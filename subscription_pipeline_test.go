package marketstream

import (
	"context"
	"errors"
	"io"
	"runtime"
	"testing"
	"time"
)

// subFixtureEvents 构造可确定产生 FINAL+UPDATE 序列的单分区事件（MaxOOO=0）：
//
//	t=1000  BTC@[0,10k)
//	t=10000 ETH@[10k,20k)  -> wm=10000，seq1 FINAL BTC@[0,10k)
//	t=11000 ETH@[10k,20k)  -> wm=11000（使宽限边界=6000）
//	t=6000  BTC@[0,10k)    -> 窗口结束 10000 > 6000 宽限内迟到，seq2 UPDATE BTC@[0,10k)
//	t=20000 SOL@[20k,30k)  -> wm=20000，seq3 FINAL ETH@[10k,20k)
//	分区 EOF -> +∞，        seq4 FINAL SOL@[20k,30k)
func subFixtureEvents() []Event {
	return []Event{
		mkEvent(0, 0, "BTC", 1_000, 100, 1),
		mkEvent(0, 1, "ETH", 10_000, 50, 1),
		mkEvent(0, 2, "ETH", 11_000, 60, 1),
		mkEvent(0, 3, "BTC", 6_000, 110, 1),
		mkEvent(0, 4, "SOL", 20_000, 1, 1),
	}
}

func subTestConfig(t *testing.T) Config {
	cfg := testConfig(t)
	cfg.MaxOutOfOrderness = 0
	cfg.AllowedLateness = 5_000
	cfg.ResultHistoryCapacity = 32
	return cfg
}

// runSubPipeline 启动带收集器（排空旧 Results 面）的处理器。
func runSubPipeline(ctx context.Context, t *testing.T, cfg Config) (*Processor, *collector, func()) {
	t.Helper()
	return runProcessor(ctx, t, cfg, []int{0})
}

// recsToFinal 把订阅记录折叠为 窗口->最新记录。
func recsToFinal(recs []ResultRecord) map[WindowKey]ResultRecord {
	return materializeRecords(recs)
}

// assertFinalAgg 严格逐字段断言最终视图。
func assertSubFixtureFinal(t *testing.T, final map[WindowKey]ResultRecord) {
	t.Helper()
	btc := WindowKey{Symbol: "BTC", Start: 0, End: 10_000}
	eth := WindowKey{Symbol: "ETH", Start: 10_000, End: 20_000}
	sol := WindowKey{Symbol: "SOL", Start: 20_000, End: 30_000}
	r, ok := final[btc]
	if !ok {
		t.Fatalf("missing %s", btc)
	}
	if r.Result.Kind != ResultUpdate {
		t.Fatalf("BTC last kind = %s want UPDATE", r.Result.Kind)
	}
	wantAgg := Aggregate{EventCount: 2, TotalQty: 2, Notional: 210, FirstTime: 1_000, LastTime: 6_000}
	if r.Result.Agg != wantAgg {
		t.Fatalf("BTC agg = %+v want %+v", r.Result.Agg, wantAgg)
	}
	if e := final[eth].Result; e.Kind != ResultFinal || e.Agg.EventCount != 2 ||
		e.Agg.TotalQty != 2 || e.Agg.Notional != 110 || e.Agg.FirstTime != 10_000 || e.Agg.LastTime != 11_000 {
		t.Fatalf("ETH final mismatch: %+v", e)
	}
	if s := final[sol].Result; s.Kind != ResultFinal || s.Agg.EventCount != 1 || s.Agg.Notional != 1 {
		t.Fatalf("SOL final mismatch: %+v", s)
	}
}

func TestProcessorSubscribeDisconnectReconnect(t *testing.T) {
	ctx := context.Background()
	cfg := subTestConfig(t)
	p, col, cleanup := runSubPipeline(ctx, t, cfg)
	defer cleanup()
	evs := subFixtureEvents()

	// 掉线前先订阅，避免漏掉任何结果；收到 seq1 后"掉线"。
	sub, err := p.SubscribeResults(0)
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	if err := p.Ingest(ctx, evs[0]); err != nil {
		t.Fatal(err)
	}
	if err := p.Ingest(ctx, evs[1]); err != nil {
		t.Fatal(err)
	}
	first := receiveN(t, sub, 1)
	if first[0].Seq != 1 || first[0].Result.Kind != ResultFinal ||
		first[0].Result.Window.Symbol != "BTC" {
		t.Fatalf("first record = %+v", first[0])
	}
	ack := sub.Ack()
	if ack != 1 || sub.Position() != 1 {
		t.Fatalf("ack=%d position=%d want 1", ack, sub.Position())
	}
	sub.Close()
	if _, err := sub.Receive(ctx); !errors.Is(err, ErrSubscriptionClosed) {
		t.Fatalf("closed subscription = %v", err)
	}

	// "重启"后继续投递剩余事件，再从确认位点续读。
	for _, e := range evs[2:] {
		if err := p.Ingest(ctx, e); err != nil {
			t.Fatal(err)
		}
	}
	if err := p.MarkComplete(ctx, 0); err != nil {
		t.Fatal(err)
	}
	if _, err := p.Checkpoint(ctx); err != nil {
		t.Fatal(err) // 屏障：全部处理完
	}
	sub2, err := p.SubscribeResults(ack)
	if err != nil {
		t.Fatalf("resubscribe: %v", err)
	}
	rest := receiveN(t, sub2, 3)
	for i, rec := range rest {
		if rec.Seq != ResultSeq(2+i) {
			t.Fatalf("resume seq[%d]=%d want %d (gap or duplicate)", i, rec.Seq, 2+i)
		}
	}
	if rest[0].Result.Kind != ResultUpdate || rest[0].Result.Window.Symbol != "BTC" {
		t.Fatalf("correction ordering lost: %+v", rest[0])
	}
	all := append(first, rest...)
	final := recsToFinal(all)
	assertSubFixtureFinal(t, final)

	// 旧输出面与订阅面结果集合一致（条数与逐字段相等）。旧通道由唯一接力 goroutine
	// 从结果历史异步搬运，屏障保证结果已登记到历史、但不保证接力已投递，因此这里先
	// 等待接力把全部结果投递完毕再比对（确定性，不依赖调度时序）。
	waitForResults(t, col, len(all))
	legacy := col.Results()
	if len(legacy) != len(all) {
		t.Fatalf("legacy results %d != subscription records %d", len(legacy), len(all))
	}
	for i, rec := range all {
		if rec.Result != legacy[i] {
			t.Fatalf("record %d divergence: sub=%+v legacy=%+v", i, rec.Result, legacy[i])
		}
	}
}

func TestProcessorSnapshotThenIncrementalEqualsContinuous(t *testing.T) {
	ctx := context.Background()
	evs := subFixtureEvents()

	// 持续消费基线：从头订阅拿到的完整记录序列。
	cfgBase := subTestConfig(t)
	pb, _, cleanupBase := runSubPipeline(ctx, t, cfgBase)
	baseSub, err := pb.SubscribeResults(0)
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range evs {
		if err := pb.Ingest(ctx, e); err != nil {
			t.Fatal(err)
		}
	}
	if err := pb.MarkComplete(ctx, 0); err != nil {
		t.Fatal(err)
	}
	baseRecs := receiveN(t, baseSub, 4)
	baseFinal := recsToFinal(baseRecs)
	cleanupBase()

	// 断线恢复路径：处理前两条后取屏障快照，再续读增量。
	cfg := subTestConfig(t)
	p, _, cleanup := runSubPipeline(ctx, t, cfg)
	defer cleanup()
	if err := p.Ingest(ctx, evs[0]); err != nil {
		t.Fatal(err)
	}
	if err := p.Ingest(ctx, evs[1]); err != nil {
		t.Fatal(err)
	}
	snap, err := p.ResultSnapshot(ctx)
	if err != nil {
		t.Fatalf("result snapshot: %v", err)
	}
	if snap.Seq != 1 {
		t.Fatalf("snapshot seq = %d want 1", snap.Seq)
	}
	if len(snap.Views) != 1 {
		t.Fatalf("snapshot views = %d want 1", len(snap.Views))
	}
	v := snap.Views[0]
	if v.Seq != 1 || v.Result.Window.Symbol != "BTC" || v.Result.Kind != ResultFinal {
		t.Fatalf("snapshot view = %+v", v)
	}

	for _, e := range evs[2:] {
		if err := p.Ingest(ctx, e); err != nil {
			t.Fatal(err)
		}
	}
	if err := p.MarkComplete(ctx, 0); err != nil {
		t.Fatal(err)
	}
	if _, err := p.Checkpoint(ctx); err != nil {
		t.Fatal(err)
	}
	incSub, err := p.SubscribeResults(snap.Seq)
	if err != nil {
		t.Fatalf("subscribe snapshot: %v", err)
	}
	inc := receiveN(t, incSub, 3)
	if inc[0].Seq != snap.Seq+1 {
		t.Fatalf("first incremental = %d want %d", inc[0].Seq, snap.Seq+1)
	}
	rebuilt := recsToFinal(append(append([]ResultRecord{}, snap.Views...), inc...))

	// 逐窗口逐字段严格一致：身份、更正关系（Kind）、聚合全字段。
	if len(rebuilt) != len(baseFinal) {
		t.Fatalf("window count rebuilt %d != baseline %d", len(rebuilt), len(baseFinal))
	}
	for k, want := range baseFinal {
		got, ok := rebuilt[k]
		if !ok {
			t.Fatalf("rebuilt missing window %s", k)
		}
		if got.Result.Window != want.Result.Window {
			t.Fatalf("window identity mismatch %+v != %+v", got.Result.Window, want.Result.Window)
		}
		if got.Result.Kind != want.Result.Kind {
			t.Fatalf("window %s kind %s != %s (correction relation changed)", k, got.Result.Kind, want.Result.Kind)
		}
		if got.Result.Agg != want.Result.Agg {
			t.Fatalf("window %s agg %+v != %+v", k, got.Result.Agg, want.Result.Agg)
		}
		if got.Seq != want.Seq {
			t.Fatalf("window %s seq %d != %d", k, got.Seq, want.Seq)
		}
	}
	assertSubFixtureFinal(t, rebuilt)
}

func TestSlowSubscriberOverflowDoesNotBlockPipeline(t *testing.T) {
	ctx := context.Background()
	cfg := subTestConfig(t)
	cfg.ResultHistoryCapacity = 3 // 极小保留范围
	p, _, cleanup := runSubPipeline(ctx, t, cfg)
	defer cleanup()
	evs := subFixtureEvents()

	// 慢订阅者：订阅后完全不读；产生远超过容量的结果。
	slow, err := p.SubscribeResults(0)
	if err != nil {
		t.Fatal(err)
	}
	before := runtime.NumGoroutine()
	// 再建大量空闲订阅者：不应带来 goroutine/后台任务增长。
	var idle []*Subscription
	for i := 0; i < 50; i++ {
		s, err := p.SubscribeResults(0)
		if err != nil {
			t.Fatalf("idle subscribe %d: %v", i, err)
		}
		idle = append(idle, s)
	}
	for _, e := range evs {
		if err := p.Ingest(ctx, e); err != nil {
			t.Fatal(err)
		}
	}
	if err := p.MarkComplete(ctx, 0); err != nil {
		t.Fatal(err)
	}
	// 管线在订阅者完全不读时仍能推进：屏障必须在有限时间内完成。
	done := make(chan *Checkpoint, 1)
	go func() {
		cp, _ := p.Checkpoint(ctx)
		done <- cp
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("pipeline blocked by slow/non-reading subscribers")
	}
	if n := runtime.NumGoroutine(); n > before+2 {
		t.Fatalf("goroutines grew with subscribers: before=%d now=%d", before, n)
	}

	// 慢订阅者读到明确可区分的溢出结论，而非静默漏过。
	if _, err := slow.Receive(ctx); !errors.Is(err, ErrResultPositionExpired) {
		t.Fatalf("slow subscriber = %v want ErrResultPositionExpired", err)
	}
	for _, s := range idle {
		if _, err := s.Receive(ctx); !errors.Is(err, ErrResultPositionExpired) {
			t.Fatalf("idle subscriber = %v want ErrResultPositionExpired", err)
		}
		s.Close()
	}

	// 恢复：重新取快照，应用视图后从快照位点续读，最终结论严格正确。
	snap, err := p.ResultSnapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	mat := NewResultMaterializer()
	for _, rec := range snap.Views {
		if !mat.Apply(rec) {
			t.Fatal("snapshot view must apply once")
		}
	}
	catchUp, err := p.SubscribeResults(snap.Seq)
	if err != nil {
		t.Fatal(err)
	}
	// 快照位点已是末尾（4 条结果中保留 3 条，最旧 seq=2），无增量；EOF 前不阻塞。
	ctx2, cancel := context.WithTimeout(ctx, 100*time.Millisecond)
	defer cancel()
	if _, err := catchUp.Receive(ctx2); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("tail subscription should block waiting, got %v", err)
	}
}

func TestPipelineFiniteStopWithNonReadingSubscriber(t *testing.T) {
	ctx := context.Background()
	cfg := subTestConfig(t)
	p, _, cleanup := runSubPipeline(ctx, t, cfg)
	defer cleanup()
	sub, err := p.SubscribeResults(0)
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range subFixtureEvents() {
		if err := p.Ingest(ctx, e); err != nil {
			t.Fatal(err)
		}
	}
	if err := p.MarkComplete(ctx, 0); err != nil {
		t.Fatal(err)
	}
	// 订阅者全程不读；Close 必须在有限时间内返回并完成资源回收。
	closed := make(chan struct{})
	go func() { _ = p.Close(); close(closed) }()
	select {
	case <-closed:
	case <-time.After(2 * time.Second):
		t.Fatal("Close did not finish in finite time with non-reading subscriber")
	}
	p.Wait()
	// 关闭后订阅仍可排空保留范围内结果，最终收到 EOF。
	recs := receiveN(t, sub, 4)
	if recs[0].Seq != 1 || recs[3].Seq != 4 {
		t.Fatalf("post-close drain = %+v", recs)
	}
	if _, err := sub.Receive(ctx); !errors.Is(err, io.EOF) {
		t.Fatalf("post-close = %v want io.EOF", err)
	}
}

// TestSubscriptionStreamConsistentAcrossOrders 通过订阅面验证：
// 不同分区交错/批大小下，逐窗口最终结果（含更正关系与全部聚合字段）严格一致。
func TestSubscriptionStreamConsistentAcrossOrders(t *testing.T) {
	ctx := context.Background()
	fix := buildInterleavedFixture()
	var baseline map[WindowKey]ResultRecord
	for ri, order := range allEventOrders(fix) {
		cfg := testConfig(t)
		cfg.ResultHistoryCapacity = 64
		p, _, cleanup := runProcessor(ctx, t, cfg, []int{0, 1})
		sub, err := p.SubscribeResults(0)
		if err != nil {
			t.Fatal(err)
		}
		ingestAll(ctx, t, p, order)
		if _, err := p.Checkpoint(ctx); err != nil {
			t.Fatal(err)
		}
		// fixture 共产生两个窗口的 FINAL。
		recs := receiveN(t, sub, 2)
		final := recsToFinal(recs)
		cleanup()
		if ri == 0 {
			baseline = final
			continue
		}
		if len(final) != len(baseline) {
			t.Fatalf("run %d windows %d != %d", ri, len(final), len(baseline))
		}
		for k, want := range baseline {
			got, ok := final[k]
			if !ok {
				t.Fatalf("run %d missing %s", ri, k)
			}
			if got.Result.Kind != want.Result.Kind || got.Result.Agg != want.Result.Agg {
				t.Fatalf("run %d window %s %+v != %+v", ri, k, got.Result, want.Result)
			}
		}
	}
}

// TestSubscribeAfterCheckpointReplayMatchesFaultFree 通过订阅面验证：
// 崩溃恢复 + 至少一次重放后，订阅到的每窗口最终结论与无故障运行严格一致。
func TestSubscribeAfterCheckpointReplayMatchesFaultFree(t *testing.T) {
	ctx := context.Background()
	fix := buildInterleavedFixture()

	// 无故障基线（经订阅面）。
	cfg := testConfig(t)
	pb, _, cleanupBase := runProcessor(ctx, t, cfg, []int{0, 1})
	baseSub, _ := pb.SubscribeResults(0)
	src := NewLocalSource(fix)
	<-pb.Feed(ctx, src)
	if _, err := pb.Checkpoint(ctx); err != nil {
		t.Fatal(err)
	}
	base := recsToFinal(receiveN(t, baseSub, 2))
	cleanupBase()

	// 崩溃 -> 检查点 -> 恢复 + 强制重放，再从订阅面取全部结果。
	p1, _, cleanup1 := runProcessor(ctx, t, cfg, []int{0, 1})
	for _, e := range fix[0] {
		p1.Ingest(ctx, e)
	}
	p1.Ingest(ctx, fix[1][0])
	p1.Ingest(ctx, fix[1][1])
	cp, err := p1.Checkpoint(ctx)
	if err != nil {
		t.Fatal(err)
	}
	cleanup1()

	p2, _, cleanup2 := runProcessor(ctx, t, cfg, []int{0, 1})
	defer cleanup2()
	if err := p2.Restore(cp); err != nil {
		t.Fatal(err)
	}
	sub, err := p2.SubscribeResults(0) // 恢复后是新的结果流，从 1 订阅
	if err != nil {
		t.Fatal(err)
	}
	src2 := NewLocalSource(fix)
	for part, off := range cp.Positions {
		src2.Seek(part, off)
	}
	src2.Seek(0, cp.Positions[0]-1) // 至少一次：重放一条已提交事件
	<-p2.Feed(ctx, src2)
	if _, err := p2.Checkpoint(ctx); err != nil {
		t.Fatal(err)
	}
	got := recsToFinal(receiveN(t, sub, 2))
	if len(got) != len(base) {
		t.Fatalf("windows %d != baseline %d", len(got), len(base))
	}
	for k, want := range base {
		g, ok := got[k]
		if !ok {
			t.Fatalf("missing window %s after recovery", k)
		}
		if g.Result.Kind != want.Result.Kind || g.Result.Agg != want.Result.Agg {
			t.Fatalf("window %s recovered %+v != %+v", k, g.Result, want.Result)
		}
	}
}
