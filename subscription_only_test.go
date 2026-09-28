package marketstream

import (
	"context"
	"errors"
	"runtime"
	"sync"
	"testing"
	"time"
)

// startNoCollector 启动处理器但【不】排空任何旧输出通道（Results/Rejections/
// Verdicts 均无人读取），用于精确复现"只用订阅面、完全不消费原有实时输出"的用法。
// 返回的清理函数保证 Close 在有限时间返回。
func startNoCollector(t *testing.T, cfg Config, parts []int) *Processor {
	t.Helper()
	p, err := NewProcessor(cfg, parts)
	if err != nil {
		t.Fatalf("NewProcessor: %v", err)
	}
	p.disableIdleTick = true
	if err := p.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	return p
}

func onlySubConfig(t *testing.T) Config {
	cfg := testConfig(t)
	cfg.MaxOutOfOrderness = 0
	cfg.AllowedLateness = 5_000
	cfg.ResultBufferSize = 1 // 旧结果通道缓冲极小，放大"无人消费"的压力
	cfg.RejectBufferSize = 1
	cfg.ResultHistoryCapacity = 16
	return cfg
}

// finalizeEvents 构造单分区事件：第 i 条（i>=1）事件时间 i*W 会最终化
// 窗口 [(i-1)W, iW)，并在 EOF 后最终化最后一个窗口。窗口 symbol 固定 S。
func finalizeEvents(n int) []Event {
	evs := make([]Event, 0, n)
	for i := 0; i < n; i++ {
		evs = append(evs, mkEvent(0, int64(i), "S", int64(i+1)*10_000, 10, 1))
	}
	return evs
}

// feedAndComplete 投递全部事件并发送分区 EOF。
func feedAndComplete(t *testing.T, p *Processor, evs []Event, part int) {
	t.Helper()
	ctx := context.Background()
	for _, e := range evs {
		if err := p.Ingest(ctx, e); err != nil {
			t.Fatalf("ingest: %v", err)
		}
	}
	if err := p.MarkComplete(ctx, part); err != nil {
		t.Fatalf("complete: %v", err)
	}
}

// TestLegacyChannelNoRedeliveryOnClose 回归：旧 Results() 通道消费者已追上全部结果
// 后再关闭处理器，接力不得因关闭唤醒而把已投递结果重发一遍（关闭无新追加时游标不回退）。
func TestLegacyChannelNoRedeliveryOnClose(t *testing.T) {
	cfg := onlySubConfig(t)
	p := startNoCollector(t, cfg, []int{0})

	var mu sync.Mutex
	var got []WindowResult
	done := make(chan struct{})
	go func() {
		for r := range p.Results() {
			mu.Lock()
			got = append(got, r)
			mu.Unlock()
		}
		close(done)
	}()

	evs := finalizeEvents(6)
	feedAndComplete(t, p, evs, 0)
	// 等待旧通道收到全部结果（接力已追上、游标位于末尾之后）。
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		mu.Lock()
		n := len(got)
		mu.Unlock()
		if n >= len(evs) {
			break
		}
		time.Sleep(time.Millisecond)
	}
	mu.Lock()
	before := len(got)
	mu.Unlock()
	if before != len(evs) {
		t.Fatalf("legacy got %d want %d before close", before, len(evs))
	}

	if err := p.Close(); err != nil {
		t.Fatal(err)
	}
	p.Wait()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("legacy Results channel did not close")
	}
	mu.Lock()
	after := len(got)
	mu.Unlock()
	if after != before {
		t.Fatalf("legacy redelivered %d extra results on close (before=%d after=%d)",
			after-before, before, after)
	}
}

// TestSubscriptionOnlyDeliversBeyondDefaultCapacity 核心场景：完全不消费原有实时
// 输出，只走订阅面；订阅者一边消费、处理一边产生，结果总量超过默认历史容量时，
// 订阅仍按位点严格持续交付（只要跟随消费就不溢出），处理链路不被卡住。
func TestSubscriptionOnlyDeliversBeyondDefaultCapacity(t *testing.T) {
	ctx := context.Background()
	cfg := onlySubConfig(t)
	cfg.ResultHistoryCapacity = defaultResultHistoryCapacity // 题目所述默认容量
	p := startNoCollector(t, cfg, []int{0})
	defer func() { _ = p.Close(); p.Wait() }()

	sub, err := p.SubscribeResults(0)
	if err != nil {
		t.Fatal(err)
	}

	const n = defaultResultHistoryCapacity + 257 // 总量超过默认容量（环形持续裁剪）
	got := make(chan ResultRecord, n)
	recvErr := make(chan error, 1)
	go func() { // 跟随消费：边产生边按位点拉取
		for want := ResultSeq(1); want <= n; want++ {
			cctx, cancel := context.WithTimeout(ctx, 2*time.Second)
			rec, rerr := sub.Receive(cctx)
			cancel()
			if rerr != nil {
				recvErr <- rerr
				return
			}
			if rec.Seq != want {
				recvErr <- errSeqGap{got: rec.Seq, want: want}
				return
			}
			got <- rec
		}
		close(got)
	}()

	feedAndComplete(t, p, finalizeEvents(n), 0)

	select {
	case err := <-recvErr:
		t.Fatalf("concurrent subscription failed: %v", err)
	default:
	}
	recs := make([]ResultRecord, 0, n)
	for rec := range got {
		recs = append(recs, rec)
	}
	if len(recs) != n || recs[0].Seq != 1 || recs[n-1].Seq != n {
		t.Fatalf("delivery incomplete: n=%d first=%d last=%d",
			len(recs), seqOrZero(recs), seqLastOrZero(recs))
	}
	if got2 := sub.Position(); got2 != ResultSeq(n) {
		t.Fatalf("position = %d want %d", got2, n)
	}
}

type errSeqGap struct{ got, want ResultSeq }

func (e errSeqGap) Error() string { return "seq gap/dup" }

func seqOrZero(r []ResultRecord) ResultSeq {
	if len(r) == 0 {
		return 0
	}
	return r[0].Seq
}

func seqLastOrZero(r []ResultRecord) ResultSeq {
	if len(r) == 0 {
		return 0
	}
	return r[len(r)-1].Seq
}

// TestSubscriptionOnlyBarriersAndCloseAreFinite 在完全不消费旧输出、且结果超过
// 默认容量时，取快照、检查点与关闭都必须在有限时间内返回。
func TestSubscriptionOnlyBarriersAndCloseAreFinite(t *testing.T) {
	cfg := onlySubConfig(t)
	cfg.ResultHistoryCapacity = 4
	p := startNoCollector(t, cfg, []int{0})

	// 不建立任何订阅、也不读任何输出，结果远超历史容量。
	feedAndComplete(t, p, finalizeEvents(100), 0)

	withTimeout(t, 2*time.Second, "ResultSnapshot", func() {
		if _, err := p.ResultSnapshot(context.Background()); err != nil {
			t.Fatalf("snapshot: %v", err)
		}
	})
	withTimeout(t, 2*time.Second, "Checkpoint", func() {
		if _, err := p.Checkpoint(context.Background()); err != nil {
			t.Fatalf("checkpoint: %v", err)
		}
	})
	withTimeout(t, 2*time.Second, "Close", func() {
		if err := p.Close(); err != nil {
			t.Fatalf("close: %v", err)
		}
	})
	p.Wait()
}

func withTimeout(t *testing.T, d time.Duration, name string, fn func()) {
	t.Helper()
	done := make(chan struct{})
	go func() { fn(); close(done) }()
	select {
	case <-done:
	case <-time.After(d):
		t.Fatalf("%s did not return within %s with unconsumed legacy output", name, d)
	}
}

// TestNonReadingSubscriberBoundedTasks 完全不读的订阅者 + 大量空闲订阅者：
// 处理链路继续推进，后台任务数量有界，落后者收到可区分的溢出结论。
func TestNonReadingSubscriberBoundedTasks(t *testing.T) {
	cfg := onlySubConfig(t)
	cfg.ResultHistoryCapacity = 8
	p := startNoCollector(t, cfg, []int{0})
	defer func() { _ = p.Close(); p.Wait() }()

	stuck, err := p.SubscribeResults(0)
	if err != nil {
		t.Fatal(err)
	}
	idle := make([]*Subscription, 0, 40)
	for i := 0; i < 40; i++ {
		s, err := p.SubscribeResults(0)
		if err != nil {
			t.Fatalf("idle sub %d: %v", i, err)
		}
		idle = append(idle, s)
	}
	before := runtime.NumGoroutine()
	feedAndComplete(t, p, finalizeEvents(60), 0)
	// 屏障完成证明循环未被任何不读的订阅者卡住。
	if _, err := p.Checkpoint(context.Background()); err != nil {
		t.Fatalf("checkpoint: %v", err)
	}
	if now := runtime.NumGoroutine(); now > before+2 {
		t.Fatalf("goroutines grew with non-reading subscribers: before=%d now=%d", before, now)
	}
	if _, err := stuck.Receive(context.Background()); !errors.Is(err, ErrResultPositionExpired) {
		t.Fatalf("stuck subscriber = %v want ErrResultPositionExpired", err)
	}
	for _, s := range idle {
		if _, err := s.Receive(context.Background()); !errors.Is(err, ErrResultPositionExpired) {
			t.Fatalf("idle subscriber = %v want ErrResultPositionExpired", err)
		}
		s.Close()
	}
}

// TestOverflowRecoverySnapThenIncremental 订阅者落后超出增量保留范围后，重新取
// 快照、再衔接增量恢复，最终每窗口视图与从头持续消费逐字段（身份/类型/聚合/位点）
// 一致。关键点：即使增量历史（环形）已裁剪掉落后订阅者需要的位点，快照视图仍保留
// 这些窗口的最新值（只要窗口还在迟到宽限内、处理循环尚未清除），因此可完整恢复。
func TestOverflowRecoverySnapThenIncremental(t *testing.T) {
	ctx := context.Background()
	cfg := onlySubConfig(t)
	cfg.ResultHistoryCapacity = 4 // 极小增量历史，快速溢出
	evs := finalizeEvents(12)

	// 基线：从头订阅、持续消费，得到完整记录序列与逐窗口最终结论（用足够大的
	// 增量历史，保证基线本身不溢出）。
	baseCfg := cfg
	baseCfg.ResultHistoryCapacity = 256
	baseP := startNoCollector(t, baseCfg, []int{0})
	baseSub, _ := baseP.SubscribeResults(0)
	feedAndComplete(t, baseP, evs, 0)
	baseline := materializeRecords(receiveN(t, baseSub, len(evs)))
	_ = baseP.Close()
	baseP.Wait()

	// 恢复路径（不在末尾 EOF 取快照，避免 +∞ 清除窗口）。
	p := startNoCollector(t, cfg, []int{0})
	defer func() { _ = p.Close(); p.Wait() }()

	lag, _ := p.SubscribeResults(0)

	// 先投递前 8 条（不 EOF）：结果数远超增量容量 4，seq1..4 已从环形裁剪 ->
	// lag 溢出；但快照物化视图独立保留了每个窗口的最新结果（含已被裁剪的窗口）。
	for _, e := range evs[:8] {
		if err := p.Ingest(ctx, e); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := p.Checkpoint(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := lag.Receive(ctx); !errors.Is(err, ErrResultPositionExpired) {
		t.Fatalf("lag = %v want expired", err)
	}
	lag.Close()

	// 取快照恢复：视图必须包含增量历史里已不存在的窗口（seq < 当前最旧位点）。
	snap, err := p.ResultSnapshot(ctx)
	if err != nil {
		t.Fatalf("snapshot: %v", err)
	}
	st := p.ResultStore()
	st.mu.Lock()
	oldest := st.oldestSeqLocked()
	st.mu.Unlock()
	foundAged := false
	for _, v := range snap.Views {
		if v.Seq < oldest {
			foundAged = true
		}
	}
	if !foundAged {
		t.Fatalf("snapshot did not recover windows aged out of incremental history (oldest=%d)", oldest)
	}

	// 从快照位点续读，再投递剩余 4 条（不 EOF），增量严格从 snap.Seq+1 衔接，
	// 且仍在容量内可完整交付。
	catchUp, err := p.SubscribeResults(snap.Seq)
	if err != nil {
		t.Fatalf("resubscribe: %v", err)
	}
	for _, e := range evs[8:] {
		if err := p.Ingest(ctx, e); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := p.Checkpoint(ctx); err != nil {
		t.Fatal(err)
	}
	st.mu.Lock()
	latest := st.seq
	st.mu.Unlock()
	inc := receiveN(t, catchUp, int(latest-snap.Seq))
	if inc[0].Seq != snap.Seq+1 {
		t.Fatalf("first incremental = %d want %d", inc[0].Seq, snap.Seq+1)
	}

	// 折叠：快照视图 + 增量。此刻仍存活的窗口必须与持续消费基线逐窗口逐字段一致
	//（身份/类型/聚合/位点），其中早期窗口正是靠快照（而非已裁剪增量）恢复的。
	got := materializeRecords(snap.Views)
	for _, rec := range inc {
		got[rec.Result.Window] = rec
	}
	for k, g := range got {
		w, ok := baseline[k]
		if !ok {
			t.Fatalf("recovered unexpected window %s", k)
		}
		if g.Result.Kind != w.Result.Kind || g.Result.Agg != w.Result.Agg || g.Seq != w.Seq {
			t.Fatalf("window %s recovered %+v(seq %d) != baseline %+v(seq %d)",
				k, g.Result, g.Seq, w.Result, w.Seq)
		}
	}
	catchUp.Close()
}

func assertMapsEqual(t *testing.T, want, got map[WindowKey]ResultRecord) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("window count %d != baseline %d", len(got), len(want))
	}
	for k, w := range want {
		g, ok := got[k]
		if !ok {
			t.Fatalf("missing window %s", k)
		}
		if g.Result.Window != w.Result.Window || g.Result.Kind != w.Result.Kind ||
			g.Result.Agg != w.Result.Agg || g.Seq != w.Seq {
			t.Fatalf("window %s mismatch:\n got=%+v (seq %d)\nwant=%+v (seq %d)",
				k, g.Result, g.Seq, w.Result, w.Seq)
		}
	}
}

// TestReconnectStrictFromAckAndRedelivery 掉线重连严格从最后确认位点续读，
// 重复确认/重复补发只生效一次；全程不消费旧输出。
func TestReconnectStrictFromAckAndRedelivery(t *testing.T) {
	ctx := context.Background()
	cfg := onlySubConfig(t)
	p := startNoCollector(t, cfg, []int{0})
	defer func() { _ = p.Close(); p.Wait() }()

	evs := subFixtureEvents()
	sub, _ := p.SubscribeResults(0)
	if err := p.Ingest(ctx, evs[0]); err != nil {
		t.Fatal(err)
	}
	if err := p.Ingest(ctx, evs[1]); err != nil {
		t.Fatal(err)
	}
	first := receiveN(t, sub, 1)
	if first[0].Seq != 1 {
		t.Fatalf("first seq %d", first[0].Seq)
	}
	ack := sub.Ack()
	if sub.Ack() != ack || sub.Position() != ack {
		t.Fatal("duplicate Ack must be idempotent")
	}
	sub.Close()
	if _, err := sub.Receive(ctx); !errors.Is(err, ErrSubscriptionClosed) {
		t.Fatalf("closed = %v", err)
	}

	// 投完剩余事件后从 ack 严格续读。
	for _, e := range evs[2:] {
		if err := p.Ingest(ctx, e); err != nil {
			t.Fatal(err)
		}
	}
	if err := p.MarkComplete(ctx, 0); err != nil {
		t.Fatal(err)
	}
	sub2, err := p.SubscribeResults(ack)
	if err != nil {
		t.Fatal(err)
	}
	rest := receiveN(t, sub2, 3)
	for i, rec := range rest {
		if rec.Seq != ResultSeq(int(ack)+1+i) {
			t.Fatalf("resume seq[%d]=%d", i, rec.Seq)
		}
	}
	// 更正链顺序：seq2 必须是 BTC 的 UPDATE。
	if rest[0].Result.Kind != ResultUpdate || rest[0].Result.Window.Symbol != "BTC" {
		t.Fatalf("correction ordering lost: %+v", rest[0])
	}

	// 重复补发：从同一 ack 再订阅，应用器只让补发至多生效一次。
	// 先把首次与续读已交付的全部结果应用一次，再让服务端从 ack 重新补发 seq2..4。
	mat := NewResultMaterializer()
	for _, rec := range append(append([]ResultRecord{}, first...), rest...) {
		if !mat.Apply(rec) {
			t.Fatalf("initial delivery seq %d must apply", rec.Seq)
		}
	}
	sub3, err := p.SubscribeResults(ack)
	if err != nil {
		t.Fatal(err)
	}
	redelivered := receiveN(t, sub3, 3)
	for _, rec := range redelivered {
		if mat.Apply(rec) {
			t.Fatalf("redelivered seq %d applied twice", rec.Seq)
		}
	}
	// 严格结论：折叠后与已知正确结果一致。
	all := append(append([]ResultRecord{}, first...), rest...)
	assertSubFixtureFinal(t, materializeRecords(all))
}

// TestSubscriptionOnlyMultiPartitionBatchesStrictEqual 不同分区交错、不同批大小
// 下，只走订阅面得到的逐窗口最终结论（身份/类型/聚合/位点）严格一致，多次运行稳定。
func TestSubscriptionOnlyMultiPartitionBatchesStrictEqual(t *testing.T) {
	fix := buildInterleavedFixture()
	var baseline map[WindowKey]ResultRecord
	for ri, order := range allEventOrders(fix) {
		cfg := testConfig(t)
		cfg.ResultHistoryCapacity = 64
		cfg.ResultBufferSize = 1
		p := startNoCollector(t, cfg, []int{0, 1})
		sub, err := p.SubscribeResults(0)
		if err != nil {
			t.Fatal(err)
		}
		ingestAll(context.Background(), t, p, order)
		if _, err := p.Checkpoint(context.Background()); err != nil {
			t.Fatal(err)
		}
		recs := receiveN(t, sub, 2)
		final := materializeRecords(recs)
		if ri == 0 {
			baseline = final
			_ = p.Close()
			p.Wait()
			continue
		}
		assertMapsEqual(t, baseline, final)
		_ = p.Close()
		p.Wait()
	}
}

// TestSubscriptionOnlyRepeatedRunsStable 同一构造多次独立运行，订阅收到的位点序列
// 与每窗口结果必须完全确定一致（消除时序相关的不稳定性）。
func TestSubscriptionOnlyRepeatedRunsStable(t *testing.T) {
	cfg := onlySubConfig(t)
	cfg.ResultHistoryCapacity = 64 // 大于结果数，保证持续订阅不溢出
	evs := finalizeEvents(30)
	var first []ResultRecord
	for run := 0; run < 5; run++ {
		p := startNoCollector(t, cfg, []int{0})
		sub, _ := p.SubscribeResults(0)
		feedAndComplete(t, p, evs, 0)
		recs := receiveN(t, sub, len(evs))
		if first == nil {
			first = recs
		} else {
			if len(recs) != len(first) {
				t.Fatalf("run %d count %d != %d", run, len(recs), len(first))
			}
			for i := range first {
				if recs[i].Seq != first[i].Seq ||
					recs[i].Result.Window != first[i].Result.Window ||
					recs[i].Result.Kind != first[i].Result.Kind ||
					recs[i].Result.Agg != first[i].Result.Agg {
					t.Fatalf("run %d unstable at %d:\n got=%+v\nwant=%+v",
						run, i, recs[i], first[i])
				}
			}
		}
		_ = p.Close()
		p.Wait()
	}
}

// TestBurstResultsDoNotBlockPipeline 突发结果量（大量事件一次性涌入、无人读旧输出）
// 下，管线持续推进、订阅在突发后可完整补齐，且并发订阅数变化不影响有界性。
func TestBurstResultsDoNotBlockPipeline(t *testing.T) {
	cfg := onlySubConfig(t)
	cfg.ResultHistoryCapacity = 32
	p := startNoCollector(t, cfg, []int{0})
	defer func() { _ = p.Close(); p.Wait() }()

	// 一个慢订阅者（突发期间完全不读）与一个事后才开始的追赶订阅者。
	slow, _ := p.SubscribeResults(0)

	evs := finalizeEvents(200)
	var wg sync.WaitGroup
	wg.Add(1)
	go func() { // 突发并发投递
		defer wg.Done()
		for _, e := range evs {
			if err := p.Ingest(context.Background(), e); err != nil {
				return
			}
		}
		_ = p.MarkComplete(context.Background(), 0)
	}()
	// 屏障必须能在突发投递期间/之后有限时间完成。
	waitBarrier := func() {
		done := make(chan struct{})
		go func() { _, _ = p.Checkpoint(context.Background()); close(done) }()
		select {
		case <-done:
		case <-time.After(2 * time.Second):
			t.Fatal("checkpoint blocked during burst")
		}
	}
	waitBarrier()
	wg.Wait()
	// 再插入屏障，确保突发的全部结果都已处理并登记后再判定慢订阅者溢出。
	if _, err := p.Checkpoint(context.Background()); err != nil {
		t.Fatalf("post-burst checkpoint: %v", err)
	}

	// 慢订阅者落后超过容量 -> 明确溢出，而非静默跳结果。
	if _, err := slow.Receive(context.Background()); !errors.Is(err, ErrResultPositionExpired) {
		t.Fatalf("slow after burst = %v want expired", err)
	}
	slow.Close()

	// 溢出恢复：取快照（EOF 后的物化视图含全部窗口最新结果，即便增量历史只剩
	// 最近 32 条），与持续消费基线逐窗口逐字段一致。
	snap, err := p.ResultSnapshot(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	got := materializeRecords(snap.Views)
	baseCfg := cfg
	baseCfg.ResultHistoryCapacity = 512
	baseP := startNoCollector(t, baseCfg, []int{0})
	baseSub, _ := baseP.SubscribeResults(0)
	feedAndComplete(t, baseP, evs, 0)
	baseline := materializeRecords(receiveN(t, baseSub, len(evs)))
	_ = baseP.Close()
	baseP.Wait()
	assertMapsEqual(t, baseline, got)
}
