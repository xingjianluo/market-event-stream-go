package marketstream

import (
	"context"
	"errors"
	"fmt"
	"runtime"
	"sync"
	"testing"
	"time"
)

// runProcessorWithBus 启动处理器并挂接结果总线；仅排空 Rejections/Verdicts 侧通道，
// Results 通道由总线独占消费（处理器侧通道只有一个消费者）。
func runProcessorWithBus(ctx context.Context, t *testing.T, cfg Config, parts []int, bcfg ResultBusConfig) (*Processor, *ResultBus, *sideCollector, func()) {
	t.Helper()
	p, err := NewProcessor(cfg, parts)
	if err != nil {
		t.Fatalf("NewProcessor: %v", err)
	}
	p.disableIdleTick = true
	b, err := NewResultBus(bcfg)
	if err != nil {
		t.Fatalf("NewResultBus: %v", err)
	}
	side := newSideCollector(p)
	if err := p.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}
	if err := b.Attach(p); err != nil {
		t.Fatalf("Attach: %v", err)
	}
	cleanup := func() {
		_ = p.Close()
		p.Wait()
		b.Wait()
	}
	return p, b, side, cleanup
}

// sideCollector 只排空侧输出与判定通道（结果通道交给总线独占）。
type sideCollector struct {
	mu         sync.Mutex
	rejections []LateRejection
	verdicts   []Verdict
	wg         sync.WaitGroup
}

func newSideCollector(p *Processor) *sideCollector {
	c := &sideCollector{}
	c.wg.Add(2)
	go func() {
		defer c.wg.Done()
		for r := range p.Rejections() {
			c.mu.Lock()
			c.rejections = append(c.rejections, r)
			c.mu.Unlock()
		}
	}()
	go func() {
		defer c.wg.Done()
		for v := range p.Verdicts() {
			c.mu.Lock()
			c.verdicts = append(c.verdicts, v)
			c.mu.Unlock()
		}
	}()
	return c
}

func (c *sideCollector) Verdicts() []Verdict {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]Verdict(nil), c.verdicts...)
}

// collectAllSequenced 从订阅排空直到总线关闭，返回全部带位点结果。
func collectAllSequenced(t *testing.T, s *Subscription) []SequencedResult {
	t.Helper()
	out := make([]SequencedResult, 0)
	for {
		e, err := s.Next(context.Background())
		if errors.Is(err, ErrBusClosed) {
			return out
		}
		if err != nil {
			t.Fatalf("Next: %v", err)
		}
		out = append(out, e)
	}
}

// assertSequencedStrictEqual 断言两条带位点结果流逐字段严格一致
// （位点、Kind、窗口身份、水位线、Reason、全部聚合字段）。
func assertSequencedStrictEqual(t *testing.T, where string, got, want []SequencedResult) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("%s length %d != %d", where, len(got), len(want))
	}
	for i := range want {
		g, w := got[i], want[i]
		if g.Position != w.Position {
			t.Fatalf("%s [%d] position %d != %d", where, i, g.Position, w.Position)
		}
		if g.Result.Window != w.Result.Window || g.Result.Kind != w.Result.Kind ||
			g.Result.Watermark != w.Result.Watermark || g.Result.Reason != w.Result.Reason {
			t.Fatalf("%s [%d] header mismatch:\n got=%+v\nwant=%+v", where, i, g.Result, w.Result)
		}
		strictAggEqual(t, where, g.Result.Agg, w.Result.Agg)
	}
}

// e2eConfig 返回带迟到更正的确定性配置：乱序容忍 0、宽限 50s（大于用例事件时间跨度，
// 使迟到更正成立、且重投事件在去重表中仍命中）。
func e2eConfig(t *testing.T) Config {
	cfg := testConfig(t)
	cfg.WindowSize = 10_000
	cfg.AllowedLateness = 50_000 // 大于事件时间跨度，使去重表在重投时仍保留 (1,0)
	cfg.MaxOutOfOrderness = 0
	return cfg
}

// canonicalEvents 是跨用例共用的规范投递顺序（MaxOutOfOrderness=0，AllowedLateness=50000），
// 全局水位线 = 两分区最大事件时间的最小值，逐步推进如下：
//
//	p0#0 t=1000  -> w0
//	p1#0 t=2000  -> w0（wm=1000）
//	p0#1 t=5000  -> w0（wm=2000）
//	p1#1 t=11000 -> w1（wm=5000）
//	p0#2 t=21000 -> w2（wm=11000）-> w0 FINAL（位点 1）
//	p1#2 t=22000 -> w2（wm=21000）-> w1 FINAL（位点 2）
//	p0#3 t=12000 -> w1 宽限内迟到：w1 UPDATE（位点 3）
//	p1#0 t=2000  -> 同一 (1,0) 重投：去重表仍保留该 ID -> DUPLICATE，不计入、不推进水位线
//	p0#4 t=3000  -> w0 宽限内迟到：w0 UPDATE（位点 4）
//
// 关闭前结果流确定为：w0 FINAL, w1 FINAL, w1 UPDATE, w0 UPDATE；EOF 时 w2 再 FINAL。
// 宽限取 50000（大于事件时间跨度）是为了让重投的 (1,0) 在去重表中仍命中，
// 得到确定性的 DUPLICATE，而不是被当成新的迟到更正。
func canonicalEvents() []Event {
	return []Event{
		mkEvent(0, 0, "BTC", 1_000, 100, 2),
		mkEvent(1, 0, "BTC", 2_000, 105, 1),
		mkEvent(0, 1, "BTC", 5_000, 110, 3),
		mkEvent(1, 1, "BTC", 11_000, 108, 4),
		mkEvent(0, 2, "BTC", 21_000, 130, 1),
		mkEvent(1, 2, "BTC", 22_000, 135, 2), // wm=21000：w0/w1 FINAL
		mkEvent(0, 3, "BTC", 12_000, 120, 1), // w1 UPDATE
		mkEvent(1, 0, "BTC", 2_000, 105, 1),  // 同一 (1,0)：DUPLICATE
		mkEvent(0, 4, "BTC", 3_000, 95, 1),   // w0 UPDATE
	}
}

// closeAndCollect 关闭处理器、等待总线停止并排空订阅。
func closeAndCollect(t *testing.T, p *Processor, b *ResultBus, s *Subscription) []SequencedResult {
	t.Helper()
	if err := p.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	p.Wait()
	b.Wait()
	return collectAllSequenced(t, s)
}

// assertViewsContentStrictEqual 只比“结论内容”：窗口身份、最终 Kind、全部聚合字段（==）。
// 不比较 Watermark/Reason，因为同一事件集合在不同到达顺序下，窗口最终化时点的水位线
// 可以不同（这与原管线 TestInterleavingOrderAndBatchIndependent 的一致性口径一致）。
func assertViewsContentStrictEqual(t *testing.T, where string, got, want map[WindowKey]SequencedResult) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("%s window count %d != %d", where, len(got), len(want))
	}
	for k, w := range want {
		g, ok := got[k]
		if !ok {
			t.Fatalf("%s missing window %s", where, k)
		}
		if g.Position != w.Position {
			t.Fatalf("%s %s position %d != %d", where, k, g.Position, w.Position)
		}
		if g.Result.Kind != w.Result.Kind {
			t.Fatalf("%s %s kind %s != %s", where, k, g.Result.Kind, w.Result.Kind)
		}
		strictAggEqual(t, fmt.Sprintf("%s %s", where, k), g.Result.Agg, w.Result.Agg)
	}
}

// foldSequenced 把带位点结果流按窗口折叠为最终视图。
func foldSequenced(es []SequencedResult) map[WindowKey]SequencedResult { return foldView(es) }

// TestE2EInterleavingBatchStrictConsistency：同一事件集合在多种分区交错/批大小下，
// 总线位点流按窗口折叠后的最终视图逐字段严格一致（身份、Kind、聚合数值），
// 且总线位点在每次运行内都从 1 连续。注意：跨不同到达顺序，窗口创建/最终化时点可能不同，
// 因此比较的是与原管线一致的“逐窗口最终结论”不变式，而非跨顺序的发出次序。
func TestE2EInterleavingBatchStrictConsistency(t *testing.T) {
	// 事件集合（不含依赖顺序的迟到步骤），保证任何顺序下窗口在关窗前都能收到其事件。
	evs := []Event{
		mkEvent(0, 0, "BTC", 1_000, 100, 2),
		mkEvent(0, 1, "BTC", 5_000, 110, 3),
		mkEvent(0, 2, "BTC", 12_000, 120, 1),
		mkEvent(0, 3, "BTC", 22_000, 125, 2),
		mkEvent(1, 0, "BTC", 2_000, 105, 1),
		mkEvent(1, 1, "BTC", 11_000, 108, 4),
		mkEvent(1, 2, "BTC", 21_000, 130, 5),
	}
	orders := [][]Event{
		evs, // 小批交错顺序（原始）
		{ // 大批次：p0 整批再 p1 整批
			evs[0], evs[1], evs[2], evs[3], evs[4], evs[5], evs[6],
		},
	}
	// 构造两个真正不同的顺序：逐条交错 vs 分区整块。
	orders[1] = []Event{evs[0], evs[1], evs[2], evs[3], evs[4], evs[5], evs[6]}
	orders[0] = []Event{evs[4], evs[0], evs[5], evs[1], evs[6], evs[2], evs[3]}

	var baseline map[WindowKey]SequencedResult
	for ri, order := range orders {
		ctx := context.Background()
		p, b, _, cleanup := runProcessorWithBus(ctx, t, e2eConfig(t), []int{0, 1},
			ResultBusConfig{HistoryCapacity: 1024, Logger: discardLogger()})
		sub, err := b.Subscribe(0)
		if err != nil {
			t.Fatalf("subscribe: %v", err)
		}
		ingestAll(ctx, t, p, order)
		stream := closeAndCollect(t, p, b, sub)
		cleanup()
		for i, e := range stream {
			if e.Position != Position(i+1) {
				t.Fatalf("run %d position gap at %d: %d", ri, i, e.Position)
			}
		}
		view := foldSequenced(stream)
		if ri == 0 {
			baseline = view
			continue
		}
		assertViewsContentStrictEqual(t, "interleaving/batch independence", view, baseline)
	}

	// 确定值逐字段核对（严格相等）。
	w0 := WindowKey{Symbol: "BTC", Start: 0, End: 10_000}
	a0 := baseline[w0].Result.Agg
	if a0.EventCount != 3 || a0.TotalQty != 6 || a0.Notional != 200+330+105 ||
		a0.FirstTime != 1_000 || a0.LastTime != 5_000 {
		t.Fatalf("w0 agg mismatch: %+v", a0)
	}
	w1 := WindowKey{Symbol: "BTC", Start: 10_000, End: 20_000}
	a1 := baseline[w1].Result.Agg
	if a1.EventCount != 2 || a1.TotalQty != 5 || a1.Notional != 120+432 ||
		a1.FirstTime != 11_000 || a1.LastTime != 12_000 {
		t.Fatalf("w1 agg mismatch: %+v", a1)
	}
}

// TestE2EDeterministicCorrectionChain：规范顺序下，每个更正窗口的结果流是
// 严格的 FINAL -> UPDATE 链，位点递增、UPDATE 数值覆盖 FINAL、无旧值盖新值。
func TestE2EDeterministicCorrectionChain(t *testing.T) {
	ctx := context.Background()
	p, b, side, cleanup := runProcessorWithBus(ctx, t, e2eConfig(t), []int{0, 1},
		ResultBusConfig{HistoryCapacity: 1024, Logger: discardLogger()})
	defer cleanup()
	sub, err := b.Subscribe(0)
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	for _, e := range canonicalEvents() {
		if err := p.Ingest(ctx, e); err != nil {
			t.Fatalf("ingest %s: %v", e.ID, err)
		}
	}
	_ = p.MarkComplete(ctx, 0)
	_ = p.MarkComplete(ctx, 1)
	stream := closeAndCollect(t, p, b, sub)

	// 期望流（按 finalize 排序 Start,Symbol）：
	// w0 FINAL -> w1 FINAL -> w1 UPDATE(t=11000) -> w0 UPDATE(t=3000)
	wantKinds := []struct {
		w     WindowKey
		kind  ResultKind
		count int64
		qty   int64
	}{
		{WindowKey{"BTC", 0, 10_000}, ResultFinal, 3, 6},
		{WindowKey{"BTC", 10_000, 20_000}, ResultFinal, 1, 4},
		{WindowKey{"BTC", 10_000, 20_000}, ResultUpdate, 2, 5},
		{WindowKey{"BTC", 0, 10_000}, ResultUpdate, 4, 7},
		{WindowKey{"BTC", 20_000, 30_000}, ResultFinal, 2, 3},
	}
	if len(stream) != len(wantKinds) {
		t.Fatalf("stream len %d != %d: %+v", len(stream), len(wantKinds), stream)
	}
	finalSeen := make(map[WindowKey]bool)
	for i, wk := range wantKinds {
		got := stream[i]
		if got.Position != Position(i+1) {
			t.Fatalf("[%d] pos %d != %d", i, got.Position, i+1)
		}
		if got.Result.Window != wk.w || got.Result.Kind != wk.kind {
			t.Fatalf("[%d] got %s %s, want %s %s", i, got.Result.Window, got.Result.Kind, wk.w, wk.kind)
		}
		if got.Result.Agg.EventCount != wk.count || got.Result.Agg.TotalQty != wk.qty {
			t.Fatalf("[%d] agg=%+v want count=%d qty=%d", i, got.Result.Agg, wk.count, wk.qty)
		}
		if wk.kind == ResultUpdate && !finalSeen[wk.w] {
			t.Fatalf("[%d] UPDATE before FINAL for %s", i, wk.w)
		}
		finalSeen[wk.w] = true
	}
	// 重放的 p1#0 必须判为 DUPLICATE。
	var dups int
	for _, v := range side.Verdicts() {
		if v.Decision == DecisionDuplicate {
			dups++
		}
	}
	if dups != 1 {
		t.Fatalf("duplicate verdicts=%d, want 1", dups)
	}
}

// TestE2ESnapshotResumeEqualsContinuous：运行中取快照，新订阅者装视图后续读，
// 与从 1 开始不断线的订阅折叠视图逐字段严格一致。
func TestE2ESnapshotResumeEqualsContinuous(t *testing.T) {
	ctx := context.Background()
	evs := canonicalEvents()
	p, b, _, cleanup := runProcessorWithBus(ctx, t, e2eConfig(t), []int{0, 1},
		ResultBusConfig{HistoryCapacity: 1024, Logger: discardLogger()})
	defer cleanup()

	continuous, err := b.Subscribe(0)
	if err != nil {
		t.Fatalf("subscribe continuous: %v", err)
	}

	// 投递前 6 条：水位线经 p1#2(t=22000) 越过 20000，w0/w1 先后 FINAL（位点 1,2）。
	for _, e := range evs[:6] {
		if err := p.Ingest(ctx, e); err != nil {
			t.Fatalf("ingest: %v", err)
		}
	}
	waitBusTail(t, b, 2)
	snap := b.Snapshot()
	if snap.Position != 2 {
		t.Fatalf("snapshot position %d != 2", snap.Position)
	}
	snapView := make(map[WindowKey]SequencedResult)
	for _, sw := range snap.Windows {
		snapView[sw.Result.Window] = SequencedResult{Position: sw.Position, Result: sw.Result}
	}
	resumed, err := b.Subscribe(snap.Position)
	if err != nil {
		t.Fatalf("subscribe resumed: %v", err)
	}

	// 继续：DUPLICATE + 两条更正 + EOF。
	for _, e := range evs[6:] {
		if err := p.Ingest(ctx, e); err != nil {
			t.Fatalf("ingest rest: %v", err)
		}
	}
	_ = p.MarkComplete(ctx, 0)
	_ = p.MarkComplete(ctx, 1)

	cont := closeAndCollect(t, p, b, continuous)
	inc := collectAllSequenced(t, resumed) // resumed 订阅在关闭前已建立，可直接排空
	got := foldView(inc)
	for k, v := range snapView {
		if old, ok := got[k]; !ok || v.Position > old.Position {
			got[k] = v
		}
	}
	assertViewsStrictEqual(t, "e2e snapshot+incremental vs continuous", got, foldView(cont))

	// 确定值：两个窗口最终都被 UPDATE 覆盖。
	if e := got[WindowKey{"BTC", 10_000, 20_000}]; e.Result.Kind != ResultUpdate ||
		e.Result.Agg.TotalQty != 5 {
		t.Fatalf("w1 final=%+v, want UPDATE qty 5", e)
	}
	if e := got[WindowKey{"BTC", 0, 10_000}]; e.Result.Kind != ResultUpdate ||
		e.Result.Agg.TotalQty != 7 {
		t.Fatalf("w0 final=%+v, want UPDATE qty 7", e)
	}
}

// TestE2EFaultReplayViewConsistency：处理器检查点崩溃恢复（并强制重放已提交事件）后，
// 新总线折叠出的最终视图与无故障运行逐字段严格一致。
func TestE2EFaultReplayViewConsistency(t *testing.T) {
	evs := canonicalEvents()
	// 无故障基线。
	{
		ctx := context.Background()
		p, b, _, cleanup := runProcessorWithBus(ctx, t, e2eConfig(t), []int{0, 1},
			ResultBusConfig{HistoryCapacity: 1024, Logger: discardLogger()})
		sub, err := b.Subscribe(0)
		if err != nil {
			t.Fatalf("subscribe: %v", err)
		}
		for _, e := range evs {
			_ = p.Ingest(ctx, e)
		}
		_ = p.MarkComplete(ctx, 0)
		_ = p.MarkComplete(ctx, 1)
		baseline := foldSequenced(closeAndCollect(t, p, b, sub))
		cleanup()
		t.Run("recovered", func(t *testing.T) {
			runRecoveredBus(t, evs, baseline)
		})
	}
}

// runRecoveredBus 执行“检查点 -> 崩溃 -> 恢复 -> 强制重放 -> 新总线”，
// 折叠视图必须等于无故障基线，重放事件判为 DUPLICATE。
func runRecoveredBus(t *testing.T, evs []Event, baseline map[WindowKey]SequencedResult) {
	ctx := context.Background()
	cfg := e2eConfig(t)

	// 第一次：处理前 4 条（w0 三条 + w1 一条，水位线 5000）后检查点并崩溃。
	p1, err := NewProcessor(cfg, []int{0, 1})
	if err != nil {
		t.Fatalf("new p1: %v", err)
	}
	p1.disableIdleTick = true
	if err := p1.Start(ctx); err != nil {
		t.Fatalf("start: %v", err)
	}
	for _, e := range evs[:4] {
		_ = p1.Ingest(ctx, e)
	}
	cp, err := p1.Checkpoint(ctx)
	if err != nil {
		t.Fatalf("checkpoint: %v", err)
	}
	_ = p1.Close()
	p1.Wait()

	// 第二次：恢复 + 新总线。
	p2, err := NewProcessor(cfg, []int{0, 1})
	if err != nil {
		t.Fatalf("new p2: %v", err)
	}
	p2.disableIdleTick = true
	if err := p2.Restore(cp); err != nil {
		t.Fatalf("restore: %v", err)
	}
	bus, err := NewResultBus(ResultBusConfig{HistoryCapacity: 1024, Logger: discardLogger()})
	if err != nil {
		t.Fatalf("bus: %v", err)
	}
	// 侧通道收集器必须在 Start 之前装好：判定通道是有界非阻塞发送，
	// 晚注册会在通道缓冲满时丢判定（emitVerdict 不阻塞处理循环）。
	side := newSideCollector(p2)
	if err := p2.Start(ctx); err != nil {
		t.Fatalf("start2: %v", err)
	}
	if err := bus.Attach(p2); err != nil {
		t.Fatalf("attach: %v", err)
	}
	sub, err := bus.Subscribe(0)
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	// 强制重放一条已提交事件 p0#1（去重表必须命中），再投递第 5 条起剩余全部
	// （其中包含同一 (1,0) 的重复投递，再产生一条 DUPLICATE）。
	_ = p2.Ingest(ctx, evs[1]) // p0#1 t=5000，已提交 -> DUPLICATE
	for _, e := range evs[4:] {
		_ = p2.Ingest(ctx, e)
	}
	_ = p2.MarkComplete(ctx, 0)
	_ = p2.MarkComplete(ctx, 1)
	got := foldSequenced(closeAndCollect(t, p2, bus, sub))
	assertViewsContentStrictEqual(t, "fault replay folded view vs fault-free", got, baseline)

	var dups int
	for _, v := range side.Verdicts() {
		if v.Decision == DecisionDuplicate {
			dups++
		}
	}
	if dups != 2 {
		// 重放的 p0#1 与序列中显式重投的 p1#0。
		t.Fatalf("duplicate verdicts after recovery=%d, want 2", dups)
	}
}

// TestE2EFiniteStopWithIdleSubscriber：订阅者完全不读，处理器与总线仍必须在
// 有限时间内正常停止并回收 goroutine（总线从不向处理路径反压）。
func TestE2EFiniteStopWithIdleSubscriber(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cfg := e2eConfig(t)
	cfg.ResultBufferSize = 0 // 结果通道无缓冲：旧实现下会卡死处理循环，总线不能改变处理器语义之外的东西
	p, err := NewProcessor(cfg, []int{0})
	if err != nil {
		t.Fatalf("new: %v", err)
	}
	p.disableIdleTick = true
	bus, err := NewResultBus(ResultBusConfig{HistoryCapacity: 4, Logger: discardLogger()})
	if err != nil {
		t.Fatalf("bus: %v", err)
	}
	if err := p.Start(ctx); err != nil {
		t.Fatalf("start: %v", err)
	}
	if err := bus.Attach(p); err != nil {
		t.Fatalf("attach: %v", err)
	}
	// 一个永不读取的订阅者。
	idle, err := bus.Subscribe(0)
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	defer idle.Close()

	before := runtime.NumGoroutine()
	// 持续产出超过历史容量的结果：总线轮转并摘除 idle 订阅，处理循环不被阻塞。
	for i := 0; i < 20; i++ {
		if err := p.Ingest(ctx, mkEvent(0, int64(i), "BTC", int64(i)*10_000, 100, 1)); err != nil {
			t.Fatalf("ingest %d: %v", i, err)
		}
	}
	_ = p.MarkComplete(ctx, 0)

	stopped := make(chan struct{})
	go func() {
		_ = p.Close()
		p.Wait()
		bus.Wait()
		close(stopped)
	}()
	select {
	case <-stopped:
	case <-time.After(3 * time.Second):
		t.Fatal("pipeline did not stop in finite time with a fully idle subscriber")
	}
	// goroutine 应回落到关闭前水平（允许短暂抖动）。
	time.Sleep(20 * time.Millisecond)
	after := runtime.NumGoroutine()
	if after > before+1 {
		t.Fatalf("goroutine leak after stop: before=%d after=%d", before, after)
	}
	// idle 订阅在轮转后必然已过期；读取得到可区分结论。
	if _, err := idle.Next(context.Background()); !errors.Is(err, ErrPositionExpired) {
		t.Fatalf("idle Next after stop err=%v, want ErrPositionExpired", err)
	}
}

// TestE2EBurstBoundedGoroutines：突发结果流下总线只有一个后台 goroutine，
// 订阅与轮转不产生额外后台任务。
func TestE2EBurstBoundedGoroutines(t *testing.T) {
	ctx := context.Background()
	cfg := e2eConfig(t)
	p, b, _, cleanup := runProcessorWithBus(ctx, t, cfg, []int{0, 1},
		ResultBusConfig{HistoryCapacity: 8, MaxSubscribers: 4, Logger: discardLogger()})
	defer cleanup()

	big := map[int][]Event{}
	for part := 0; part < 2; part++ {
		evs := make([]Event, 300)
		for i := range evs {
			evs[i] = mkEvent(part, int64(i), "BTC", int64(i*10_000), 100, 1)
		}
		big[part] = evs
	}
	for i := 0; i < 4; i++ {
		s, err := b.Subscribe(0)
		if err != nil {
			t.Fatalf("sub %d: %v", i, err)
		}
		defer s.Close()
	}
	feedDone := p.Feed(ctx, NewLocalSource(big))
	time.Sleep(30 * time.Millisecond)
	during := runtime.NumGoroutine()
	time.Sleep(30 * time.Millisecond)
	later := runtime.NumGoroutine()
	if d := later - during; d > 8 || d < -8 {
		t.Fatalf("goroutine count unstable under burst: %d -> %d", during, later)
	}
	<-feedDone
}
