package marketstream

import (
	"context"
	"errors"
	"io"
	"runtime"
	"testing"
	"time"
)

// subOnlyTestConfig 构造订阅专用模式配置：默认历史容量（1024），
// 旧输出通道小缓冲且测试全程不消费，用于证明订阅面可独立工作。
func subOnlyTestConfig(t *testing.T) Config {
	t.Helper()
	cfg := testConfig(t)
	cfg.MaxOutOfOrderness = 0
	cfg.ResultOutputMode = ResultOutputSubscriptionOnly
	cfg.ResultBufferSize = 4
	cfg.RejectBufferSize = 4
	return cfg
}

// runSubOnlyProcessor 启动订阅专用模式处理器，不启动任何旧输出收集器。
func runSubOnlyProcessor(t *testing.T, cfg Config, parts []int) *Processor {
	t.Helper()
	p, err := NewProcessor(cfg, parts)
	if err != nil {
		t.Fatalf("NewProcessor: %v", err)
	}
	p.disableIdleTick = true
	if err := p.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	t.Cleanup(func() {
		_ = p.Close()
		p.Wait()
	})
	return p
}

// burstEvents 构造单分区事件：每 N 条推进一个窗口，产生约 n/N 条 FINAL。
// 事件时间严格递增（MaxOOO=0），第 k 条事件把 wm 推到约 k*step。
func burstEvents(n int, step int64) []Event {
	evs := make([]Event, n)
	for i := range evs {
		ts := int64(i+1) * step
		evs[i] = mkEvent(0, int64(i), "BTC", ts, 100, 1)
	}
	return evs
}

// TestSubOnlyDeliveryBeyondDefaultCapacity 核心用例：
// 只使用结果订阅、完全不消费旧输出；结果持续产生且远超默认历史容量时，
// 订阅仍按位点持续交付；快照、检查点、关闭都在有限时间内正常返回。
func TestSubOnlyDeliveryBeyondDefaultCapacity(t *testing.T) {
	ctx := context.Background()
	cfg := subOnlyTestConfig(t)
	const wantResults = defaultResultHistoryCapacity * 3 // 远超默认容量
	p := runSubOnlyProcessor(t, cfg, []int{0})

	sub, err := p.SubscribeResults(0)
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}

	// 事件 t=10000,20000,...：事件 k 使 wm=k*10000，触发事件 k-1 所在窗口的 FINAL，
	// 故投递完前 k 条事件后恰有 k-1 条结果；EOF 再补最后一条，共 n 条。
	evs := burstEvents(wantResults, 10_000)
	const chunk = 256 // 远小于历史容量 1024，分块乒乓保证积压永不越界
	received := 0
	for i := 0; i < len(evs); i += chunk {
		end := i + chunk
		if end > len(evs) {
			end = len(evs)
		}
		for _, e := range evs[i:end] {
			if err := p.Ingest(ctx, e); err != nil {
				t.Fatalf("ingest: %v", err)
			}
		}
		// 前 k 条事件后恰有 k-1 条结果；订阅把本块产出的结果全部取走。
		expectAfter := end - 1
		for received < expectAfter {
			rec, err := sub.Receive(ctx)
			if err != nil {
				t.Fatalf("receive at %d/%d: %v", received, expectAfter, err)
			}
			received++
			if rec.Seq != ResultSeq(received) {
				t.Fatalf("seq gap/dup: got %d want %d", rec.Seq, received)
			}
		}
	}
	if err := p.MarkComplete(ctx, 0); err != nil {
		t.Fatalf("complete: %v", err)
	}
	// EOF 触发最后一个窗口 FINAL。
	for received < wantResults {
		rec, err := sub.Receive(ctx)
		if err != nil {
			t.Fatalf("receive tail at %d: %v", received, err)
		}
		received++
		if rec.Seq != ResultSeq(received) {
			t.Fatalf("seq gap/dup: got %d want %d", rec.Seq, received)
		}
	}

	// 检查点必须有限时间返回（旧输出完全无人消费）。
	cpDone := make(chan *Checkpoint, 1)
	go func() { cp, _ := p.Checkpoint(ctx); cpDone <- cp }()
	select {
	case cp := <-cpDone:
		if cp == nil {
			t.Fatal("nil checkpoint")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Checkpoint blocked while only subscription surface is used")
	}

	// 快照同样必须有限时间返回。
	snapDone := make(chan *ResultSnapshot, 1)
	go func() { s, _ := p.ResultSnapshot(ctx); snapDone <- s }()
	select {
	case <-snapDone:
	case <-time.After(2 * time.Second):
		t.Fatal("ResultSnapshot blocked while only subscription surface is used")
	}

	// 关闭有限时间返回；关闭后订阅收到 EOF。
	closed := make(chan struct{})
	go func() { _ = p.Close(); close(closed) }()
	select {
	case <-closed:
	case <-time.After(2 * time.Second):
		t.Fatal("Close blocked while only subscription surface is used")
	}
	if _, err := sub.Receive(ctx); !errors.Is(err, io.EOF) {
		t.Fatalf("after close = %v want io.EOF", err)
	}
}

// TestSubOnlyNonReadingAndSlowSubscribersBounded 验证：
// 慢订阅者与完全不读订阅者都不阻塞处理链路；突发结果量下
// 屏障有限时间返回，且 goroutine 数不随订阅数/结果数增长。
func TestSubOnlyNonReadingAndSlowSubscribersBounded(t *testing.T) {
	ctx := context.Background()
	cfg := subOnlyTestConfig(t)
	cfg.ResultHistoryCapacity = 16 // 小保留范围，很快溢出
	p := runSubOnlyProcessor(t, cfg, []int{0})

	// 一个完全不读的订阅 + 一大批建后即闲置的订阅者。
	dead, err := p.SubscribeResults(0)
	if err != nil {
		t.Fatal(err)
	}
	idle := make([]*Subscription, 64)
	for i := range idle {
		idle[i], err = p.SubscribeResults(0)
		if err != nil {
			t.Fatalf("idle subscribe %d: %v", i, err)
		}
	}
	before := runtime.NumGoroutine()

	// 突发：结果数远超保留范围与旧通道缓冲；全程无人读旧通道、无人读订阅。
	const burst = 200
	evs := burstEvents(burst, 10_000)
	for _, e := range evs {
		if err := p.Ingest(ctx, e); err != nil {
			t.Fatalf("ingest: %v", err)
		}
	}
	if err := p.MarkComplete(ctx, 0); err != nil {
		t.Fatalf("complete: %v", err)
	}

	// 屏障在无任何订阅消费时仍有限时间完成。
	done := make(chan struct{}, 1)
	go func() { _, _ = p.Checkpoint(ctx); done <- struct{}{} }()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Checkpoint blocked by non-reading subscribers")
	}

	// goroutine 不随订阅者数量或结果数量增长（拉模型无每订阅者任务）。
	if n := runtime.NumGoroutine(); n > before+2 {
		t.Fatalf("goroutines grew with subscribers: before=%d now=%d", before, n)
	}

	// 不读者与慢读者都收到明确可区分的溢出结论。
	if _, err := dead.Receive(ctx); !errors.Is(err, ErrResultPositionExpired) {
		t.Fatalf("non-reading subscriber = %v want ErrResultPositionExpired", err)
	}
	for i, s := range idle {
		if _, err := s.Receive(ctx); !errors.Is(err, ErrResultPositionExpired) {
			t.Fatalf("idle subscriber %d = %v want ErrResultPositionExpired", i, err)
		}
		s.Close()
	}
}

// TestSubOnlyOverflowSnapshotRecoveryEqualsContinuous 验证：
// 订阅者落后超过保留范围 -> 收到明确可区分的溢出结论 -> 重取快照、
// 应用视图、从快照位点衔接增量后，每窗口结论（身份/类型/聚合/位点）
// 与从头持续消费逐字段严格一致；缺口不会被静默跳过或补发。
func TestSubOnlyOverflowSnapshotRecoveryEqualsContinuous(t *testing.T) {
	ctx := context.Background()
	const (
		histCap = 8
		phase1  = 40 // 第一阶段结果数：远超保留范围，落后订阅必然溢出
		phase2  = 20 // 恢复后继续产生的第二阶段结果数
	)
	phase1Evs := burstEvents(phase1, 10_000)
	phase2Evs := make([]Event, phase2)
	for i := range phase2Evs {
		// 事件时间接续第一阶段；第一阶段 40 条事件只产出 39 条 FINAL，
		// 第二阶段首条事件（t=410000）先补出第 40 条，再继续推进。
		ts := int64(phase1+1+i) * 10_000
		phase2Evs[i] = mkEvent(0, int64(phase1+i), "BTC", ts, 100, 1)
	}
	allEvs := append(append([]Event{}, phase1Evs...), phase2Evs...)

	// 基线：从头持续消费（容量足够大，不溢出），EOF 后拿到全部 N 条 FINAL。
	baseCfg := subOnlyTestConfig(t)
	baseCfg.ResultHistoryCapacity = 256
	pb := runSubOnlyProcessor(t, baseCfg, []int{0})
	baseSub, _ := pb.SubscribeResults(0)
	for _, e := range allEvs {
		if err := pb.Ingest(ctx, e); err != nil {
			t.Fatal(err)
		}
	}
	if err := pb.MarkComplete(ctx, 0); err != nil {
		t.Fatal(err)
	}
	if _, err := pb.Checkpoint(ctx); err != nil {
		t.Fatal(err)
	}
	baseRecs := receiveN(t, baseSub, len(allEvs))
	baseFinal := materializeRecords(baseRecs)

	// 恢复路径：小容量，订阅后完全不读，第一阶段推进即溢出。
	cfg := subOnlyTestConfig(t)
	cfg.ResultHistoryCapacity = histCap
	p := runSubOnlyProcessor(t, cfg, []int{0})
	lag, err := p.SubscribeResults(0)
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range phase1Evs {
		if err := p.Ingest(ctx, e); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := p.Checkpoint(ctx); err != nil {
		t.Fatalf("checkpoint: %v", err)
	}
	// seq1 已滑出，Receive 立即给出可区分溢出结论。
	if _, err := lag.Receive(ctx); !errors.Is(err, ErrResultPositionExpired) {
		t.Fatalf("lag Receive = %v want ErrResultPositionExpired", err)
	}
	// 溢出结论持续、可区分，不会静默补发缺口结果。
	if _, err := lag.Receive(ctx); !errors.Is(err, ErrResultPositionExpired) {
		t.Fatalf("overflow must persist, got %v", err)
	}

	// 恢复：在第一阶段末取屏障快照，应用视图后从快照位点续读。
	snap, err := p.ResultSnapshot(ctx)
	if err != nil {
		t.Fatalf("snapshot: %v", err)
	}
	const phase1Results = phase1 - 1 // 最后一个窗口要等 EOF 才 FINAL
	if snap.Seq != ResultSeq(phase1Results) {
		t.Fatalf("snapshot seq %d want %d", snap.Seq, phase1Results)
	}
	catchUp, err := p.SubscribeResults(snap.Seq)
	if err != nil {
		t.Fatalf("resume subscribe: %v", err)
	}
	// 尚未产生第二阶段结果：衔接订阅严格等待 snap.Seq+1，不返回也不越过快照。
	waitCtx, cancel := context.WithTimeout(ctx, 80*time.Millisecond)
	if _, err := catchUp.Receive(waitCtx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("tail resume should wait, got %v", err)
	}
	cancel()

	// 第二阶段逐条产生、逐条衔接（每投一条事件恰好补/推进出一条 FINAL），
	// 模拟正常追增量速度，保证积压不越过保留范围。
	var inc []ResultRecord
	for i, e := range phase2Evs {
		if err := p.Ingest(ctx, e); err != nil {
			t.Fatal(err)
		}
		rec, rerr := catchUp.Receive(ctx)
		if rerr != nil {
			t.Fatalf("incremental receive %d: %v", i, rerr)
		}
		if rec.Seq != snap.Seq+ResultSeq(i)+1 {
			t.Fatalf("incremental[%d] seq=%d want %d", i, rec.Seq, snap.Seq+ResultSeq(i)+1)
		}
		inc = append(inc, rec)
	}
	if err := p.MarkComplete(ctx, 0); err != nil {
		t.Fatal(err)
	}
	// EOF 补第二阶段末窗口 FINAL。
	last, err := catchUp.Receive(ctx)
	if err != nil {
		t.Fatalf("incremental tail: %v", err)
	}
	if last.Seq != snap.Seq+ResultSeq(phase2)+1 {
		t.Fatalf("incremental tail seq=%d want %d", last.Seq, snap.Seq+ResultSeq(phase2)+1)
	}
	inc = append(inc, last)

	// 快照视图（第一阶段仍保留窗口）与第二阶段增量折叠，逐窗口逐字段对齐基线。
	rebuilt := materializeRecords(append(append([]ResultRecord{}, snap.Views...), inc...))
	for _, rec := range snap.Views {
		if w, ok := baseFinal[rec.Result.Window]; !ok || rec != w {
			t.Fatalf("snapshot window %+v != baseline %+v", rec, w)
		}
	}
	// 恢复者可见的窗口恰好是：快照保留窗口 + 第二阶段全部新窗口。
	wantVisible := len(snap.Views) + phase2 + 1
	if len(rebuilt) != wantVisible {
		t.Fatalf("rebuilt window count %d want %d (snapshot %d + phase2 %d)",
			len(rebuilt), wantVisible, len(snap.Views), phase2)
	}
	for k := range rebuilt {
		if _, ok := baseFinal[k]; !ok {
			t.Fatalf("rebuilt contains window %s absent in baseline", k)
		}
	}
	for k, w := range rebuilt {
		if b := baseFinal[k]; b != w {
			t.Fatalf("window %s rebuilt %+v != baseline %+v", k, w, b)
		}
	}
}

// TestSubOnlyReconnectStrictResumeAndIdempotentRedelivery 验证只走订阅面时：
// 掉线后从最后一次 Ack 严格续读；FINAL→UPDATE 更正链先后、窗口身份、
// 最终类型、聚合与位点和从头消费完全一致；重复确认/重复补发只生效一次。
func TestSubOnlyReconnectStrictResumeAndIdempotentRedelivery(t *testing.T) {
	ctx := context.Background()
	cfg := subTestConfig(t)
	cfg.ResultOutputMode = ResultOutputSubscriptionOnly
	cfg.ResultBufferSize = 2 // 旧通道极小且全程不读
	p := runSubOnlyProcessor(t, cfg, []int{0})
	evs := subFixtureEvents()

	// 从头持续消费基线：拿全 4 条记录（含 BTC 的 FINAL→UPDATE 更正链）。
	baseP := runSubOnlyProcessor(t, cfg, []int{0})
	baseSub, _ := baseP.SubscribeResults(0)
	for _, e := range evs {
		if err := baseP.Ingest(ctx, e); err != nil {
			t.Fatal(err)
		}
	}
	if err := baseP.MarkComplete(ctx, 0); err != nil {
		t.Fatal(err)
	}
	base := receiveN(t, baseSub, 4)
	if base[1].Result.Kind != ResultUpdate || base[1].Result.Window.Symbol != "BTC" {
		t.Fatalf("baseline correction chain broken: %+v", base[1])
	}

	// 重连路径：先收 seq1 后掉线。
	sub1, err := p.SubscribeResults(0)
	if err != nil {
		t.Fatal(err)
	}
	if err := p.Ingest(ctx, evs[0]); err != nil {
		t.Fatal(err)
	}
	if err := p.Ingest(ctx, evs[1]); err != nil {
		t.Fatal(err)
	}
	first := receiveN(t, sub1, 1)
	if first[0].Seq != 1 || first[0].Result != base[0].Result {
		t.Fatalf("first record %+v != baseline %+v", first[0], base[0])
	}
	ack := sub1.Ack()
	// 重复确认幂等、无副作用。
	if sub1.Ack() != ack || sub1.Position() != ack {
		t.Fatal("duplicate Ack must be idempotent")
	}
	sub1.Close()
	if _, err := sub1.Receive(ctx); !errors.Is(err, ErrSubscriptionClosed) {
		t.Fatalf("closed sub = %v", err)
	}

	// 投完剩余事件（旧通道无人消费，循环不得停）。
	for _, e := range evs[2:] {
		if err := p.Ingest(ctx, e); err != nil {
			t.Fatal(err)
		}
	}
	if err := p.MarkComplete(ctx, 0); err != nil {
		t.Fatal(err)
	}
	if _, err := p.Checkpoint(ctx); err != nil {
		t.Fatalf("checkpoint with no legacy drain: %v", err)
	}

	// 从最后确认位点严格续读：seq2..4，不重放 seq1，无缺口。
	sub2, err := p.SubscribeResults(ack)
	if err != nil {
		t.Fatalf("resubscribe: %v", err)
	}
	rest := receiveN(t, sub2, 3)
	for i, rec := range rest {
		if rec.Seq != ResultSeq(2+i) {
			t.Fatalf("resume seq[%d]=%d want %d", i, rec.Seq, 2+i)
		}
		if rec.Result != base[1+i].Result {
			t.Fatalf("resume result[%d] %+v != baseline %+v", i, rec.Result, base[1+i].Result)
		}
	}
	// 更正链顺序：seq2 必须是 BTC 的 UPDATE（在其 FINAL 之后）。
	if rest[0].Result.Kind != ResultUpdate || rest[0].Result.Window.Symbol != "BTC" {
		t.Fatalf("correction ordering lost after resume: %+v", rest[0])
	}
	all := append(first, rest...)
	assertSubFixtureFinal(t, recsToFinal(all))

	// 重复补发：从 ack-1 再订阅，seq=ack 被重发；应用器保证只生效一次。
	mat := NewResultMaterializer()
	for _, rec := range all {
		if !mat.Apply(rec) {
			t.Fatalf("fresh rec seq %d must apply", rec.Seq)
		}
	}
	redeliver, err := p.SubscribeResults(ack - 1)
	if err != nil {
		t.Fatalf("redeliver subscribe: %v", err)
	}
	dup := receiveN(t, redeliver, 4-int(ack-1))
	applied := 0
	for _, rec := range dup {
		if mat.Apply(rec) {
			applied++
		}
	}
	if applied != 0 {
		t.Fatalf("redelivered records applied %d times, want 0", applied)
	}
}

// TestSubOnlyConsistentAcrossOrdersAndBatches 验证只走订阅面、
// 不消费旧通道时，不同分区交错顺序与批大小下逐窗口结论严格一致
// （身份/最终类型/聚合全字段一致）。
func TestSubOnlyConsistentAcrossOrdersAndBatches(t *testing.T) {
	ctx := context.Background()
	fix := buildInterleavedFixture()
	var baseline map[WindowKey]ResultRecord
	for ri, order := range allEventOrders(fix) {
		cfg := testConfig(t)
		cfg.ResultOutputMode = ResultOutputSubscriptionOnly
		cfg.ResultHistoryCapacity = 64
		cfg.ResultBufferSize = 1 // 旧通道极小且全程不读
		p := runSubOnlyProcessor(t, cfg, []int{0, 1})
		sub, err := p.SubscribeResults(0)
		if err != nil {
			t.Fatal(err)
		}
		ingestAll(ctx, t, p, order)
		if _, err := p.Checkpoint(ctx); err != nil {
			t.Fatalf("checkpoint order %d: %v", ri, err)
		}
		recs := receiveN(t, sub, 2)
		final := recsToFinal(recs)
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
			if got.Result.Window != want.Result.Window ||
				got.Result.Kind != want.Result.Kind ||
				got.Result.Agg != want.Result.Agg {
				t.Fatalf("run %d window %s %+v != %+v", ri, k, got.Result, want.Result)
			}
		}
	}
}

// TestSubOnlyCheckpointReplayMatchesFaultFree 验证只走订阅面时：
// 检查点崩溃恢复 + 至少一次重放后，订阅到的完整结果序列（含每窗口
// 身份/类型/聚合/位点）与无故障运行逐字段严格一致；重放事件幂等去重。
func TestSubOnlyCheckpointReplayMatchesFaultFree(t *testing.T) {
	ctx := context.Background()
	fix := buildInterleavedFixture()
	cfg := func() Config {
		c := testConfig(t)
		c.ResultOutputMode = ResultOutputSubscriptionOnly
		c.ResultBufferSize = 2 // 旧通道全程不读
		return c
	}

	// 无故障基线：订阅面收全部结果。
	pb := runSubOnlyProcessor(t, cfg(), []int{0, 1})
	baseSub, _ := pb.SubscribeResults(0)
	<-pb.Feed(ctx, NewLocalSource(fix))
	if _, err := pb.Checkpoint(ctx); err != nil {
		t.Fatal(err)
	}
	base := receiveN(t, baseSub, 2)

	// 崩溃前：p0 全部 + p1 前两条，取检查点后关闭（不读旧通道也必须正常停止）。
	p1 := runSubOnlyProcessor(t, cfg(), []int{0, 1})
	for _, e := range fix[0] {
		if err := p1.Ingest(ctx, e); err != nil {
			t.Fatal(err)
		}
	}
	if err := p1.Ingest(ctx, fix[1][0]); err != nil {
		t.Fatal(err)
	}
	if err := p1.Ingest(ctx, fix[1][1]); err != nil {
		t.Fatal(err)
	}
	cp, err := p1.Checkpoint(ctx)
	if err != nil {
		t.Fatal(err)
	}
	closed := make(chan struct{})
	go func() { _ = p1.Close(); close(closed) }()
	select {
	case <-closed:
	case <-time.After(2 * time.Second):
		t.Fatal("pre-crash Close blocked without legacy draining")
	}
	p1.Wait()

	// 恢复：位点序列在新进程从 1 重新编号；从头订阅 + 至少一次重放。
	p2 := runSubOnlyProcessor(t, cfg(), []int{0, 1})
	if err := p2.Restore(cp); err != nil {
		t.Fatal(err)
	}
	sub, err := p2.SubscribeResults(0)
	if err != nil {
		t.Fatal(err)
	}
	src2 := NewLocalSource(fix)
	for part, off := range cp.Positions {
		src2.Seek(part, off)
	}
	src2.Seek(0, cp.Positions[0]-1) // 强制重放一条已提交事件
	<-p2.Feed(ctx, src2)
	if _, err := p2.Checkpoint(ctx); err != nil {
		t.Fatal(err)
	}
	got := receiveN(t, sub, 2)

	if len(got) != len(base) {
		t.Fatalf("record count %d != baseline %d", len(got), len(base))
	}
	for i := range base {
		if got[i].Seq != base[i].Seq {
			t.Fatalf("record %d seq %d != %d", i, got[i].Seq, base[i].Seq)
		}
		// 恢复进程从检查点水位线续推，FINAL 发出时水位线可能不同（既有
		// live 重放测试同口径）；窗口身份、最终类型、聚合值、位点必须一致。
		if got[i].Result.Window != base[i].Result.Window ||
			got[i].Result.Kind != base[i].Result.Kind ||
			got[i].Result.Agg != base[i].Result.Agg {
			t.Fatalf("record %d:\n got %+v\nwant %+v", i, got[i].Result, base[i].Result)
		}
	}
}

// TestResultOutputModeConfigCompatibility 验证默认配置是 live 模式（旧用法兼容），
// 且未知模式在 NewProcessor 处被明确拒绝。
func TestResultOutputModeConfigCompatibility(t *testing.T) {
	def := DefaultConfig()
	if def.ResultOutputMode != ResultOutputLive {
		t.Fatalf("default mode %q want live", def.ResultOutputMode)
	}
	// 零值也按 live 处理（未设置该字段的旧代码无需改动）。
	cfg := testConfig(t)
	cfg.ResultOutputMode = ""
	if _, err := NewProcessor(cfg, []int{0}); err != nil {
		t.Fatalf("zero-value mode must be accepted as live: %v", err)
	}
	bad := testConfig(t)
	bad.ResultOutputMode = "BOGUS"
	if _, err := NewProcessor(bad, []int{0}); err == nil {
		t.Fatal("unknown ResultOutputMode must fail validation")
	}
}

// TestSubOnlyLegacyChannelIsBypassAndNeverBlocks 验证订阅专用模式下旧结果通道
// 只尽力而为：缓冲满时丢旁路也不阻塞处理循环，权威结果仍全部在订阅面可消费。
func TestSubOnlyLegacyChannelIsBypassAndNeverBlocks(t *testing.T) {
	ctx := context.Background()
	cfg := subOnlyTestConfig(t)
	cfg.ResultHistoryCapacity = 128
	cfg.ResultBufferSize = 1 // 不读旧通道，第一条之后旁路即满
	p := runSubOnlyProcessor(t, cfg, []int{0})
	sub, _ := p.SubscribeResults(0)

	evs := burstEvents(20, 10_000)
	for _, e := range evs {
		if err := p.Ingest(ctx, e); err != nil {
			t.Fatal(err)
		}
	}
	if err := p.MarkComplete(ctx, 0); err != nil {
		t.Fatal(err)
	}
	// 屏障有限时间完成即证明循环从未等待旧通道。
	if _, err := p.Checkpoint(ctx); err != nil {
		t.Fatalf("checkpoint: %v", err)
	}
	// 旧通道里最多只有缓冲容量那么多条旁路数据；权威订阅面一条不少。
	if n := len(p.results); n > cap(p.results) {
		t.Fatalf("legacy channel grew beyond buffer: %d > %d", n, cap(p.results))
	}
	recs := receiveN(t, sub, len(evs))
	if recs[0].Seq != 1 || recs[len(recs)-1].Seq != ResultSeq(len(evs)) {
		t.Fatalf("authoritative subscription missing results: %+v", recs)
	}
}
