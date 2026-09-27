package marketstream

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"
)

// newTestBus 创建一个直接投喂结果的总线，返回总线、投喂通道与清理函数。
func newTestBus(t *testing.T, cap_ int, maxSubs int) (*ResultBus, chan WindowResult, func()) {
	t.Helper()
	cfg := DefaultResultBusConfig()
	cfg.HistoryCapacity = cap_
	if maxSubs > 0 {
		cfg.MaxSubscribers = maxSubs
	}
	cfg.Logger = discardLogger()
	b, err := NewResultBus(cfg)
	if err != nil {
		t.Fatalf("NewResultBus: %v", err)
	}
	in := make(chan WindowResult, 256)
	if err := b.Start(in); err != nil {
		t.Fatalf("Start: %v", err)
	}
	cleanup := func() {
		close(in)
		b.Wait()
	}
	return b, in, cleanup
}

func res(kind ResultKind, symbol string, start int64, count int64) WindowResult {
	return WindowResult{
		Kind:      kind,
		Window:    WindowKey{Symbol: symbol, Start: start, End: start + 10_000},
		Agg:       Aggregate{EventCount: count, TotalQty: count, Notional: float64(count) * 100},
		Watermark: start + 10_000,
	}
}

// drainN 从订阅拉取恰好 n 条结果，返回结果序列；阻塞超时即失败。
func drainN(t *testing.T, s *Subscription, n int) []SequencedResult {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	out := make([]SequencedResult, 0, n)
	for i := 0; i < n; i++ {
		e, err := s.Next(ctx)
		if err != nil {
			t.Fatalf("Next #%d: %v", i, err)
		}
		out = append(out, e)
	}
	return out
}

// waitBusTail 轮询等待总线位点推进到 want（投喂经 goroutine 异步生效）。
func waitBusTail(t *testing.T, b *ResultBus, want Position) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if b.CurrentPosition() >= want {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("bus tail timeout: got %d want %d", b.CurrentPosition(), want)
}

// assertSeq 断言结果序列的位点严格等于 want。
func assertSeq(t *testing.T, got []SequencedResult, want ...Position) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("result count %d != %d (%v)", len(got), len(want), seqList(got))
	}
	for i, w := range want {
		if got[i].Position != w {
			t.Fatalf("result[%d] position %d != %d (%v)", i, got[i].Position, w, seqList(got))
		}
	}
}

func seqList(rs []SequencedResult) []Position {
	out := make([]Position, len(rs))
	for i, r := range rs {
		out[i] = r.Position
	}
	return out
}

// TestBusAssignsStableMonotonicPositions：位点从 1 开始严格单调，
// 同窗口的 FINAL/UPDATE 按投喂先后成链，不会乱序。
func TestBusAssignsStableMonotonicPositions(t *testing.T) {
	b, in, cleanup := newTestBus(t, 16, 4)
	defer cleanup()

	feed := []WindowResult{
		res(ResultFinal, "BTC", 0, 1),
		res(ResultFinal, "ETH", 0, 1),
		res(ResultUpdate, "BTC", 0, 2), // BTC 更正
		res(ResultFinal, "BTC", 10_000, 1),
		res(ResultUpdate, "BTC", 0, 3), // BTC 再次更正（位点序上晚于其 FINAL）
	}
	for _, r := range feed {
		in <- r
	}
	waitBusTail(t, b, Position(len(feed)))

	sub, err := b.Subscribe(0)
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	defer sub.Close()
	got := drainN(t, sub, len(feed))
	assertSeq(t, got, 1, 2, 3, 4, 5)

	// BTC 窗口 0 的结果在序列中按 FINAL(1) -> UPDATE(3) -> UPDATE(5) 排列。
	var chain []Position
	for _, e := range got {
		if e.Result.Window.Symbol == "BTC" && e.Result.Window.Start == 0 {
			chain = append(chain, e.Position)
			if e.Position == chain[0] && e.Result.Kind != ResultFinal {
				t.Fatalf("first BTC result must be FINAL, got %s", e.Result.Kind)
			}
		}
	}
	assertPos := func(got, want []Position) {
		t.Helper()
		if len(got) != len(want) {
			t.Fatalf("correction chain %v != %v", got, want)
		}
		for i := range got {
			if got[i] != want[i] {
				t.Fatalf("correction chain %v != %v", got, want)
			}
		}
	}
	assertPos(chain, []Position{1, 3, 5})
}

// TestSubscriptionResumeAfterDisconnect：模拟订阅者处理一部分后掉线，
// 用已确认位点重新订阅，必须严格只拿到之后的结果，无重复、无遗漏、无乱序。
func TestSubscriptionResumeAfterDisconnect(t *testing.T) {
	b, in, cleanup := newTestBus(t, 64, 4)
	defer cleanup()

	for i := 1; i <= 6; i++ {
		in <- res(ResultFinal, "BTC", int64(i-1)*10_000, int64(i))
	}
	waitBusTail(t, b, 6)

	// 第一阶段：消费前 3 条并确认到位点 3。
	sub1, err := b.Subscribe(0)
	if err != nil {
		t.Fatalf("subscribe1: %v", err)
	}
	first := drainN(t, sub1, 3)
	assertSeq(t, first, 1, 2, 3)
	if pos := sub1.Ack(3); pos != 3 {
		t.Fatalf("ack = %d, want 3", pos)
	}
	if sub1.Committed() != 3 || sub1.NextPosition() != 4 {
		t.Fatalf("sub1 committed=%d next=%d", sub1.Committed(), sub1.NextPosition())
	}
	_ = sub1.Close() // 模拟进程掉线

	// 第二阶段：用持久化的位点 3 重新订阅。
	sub2, err := b.Subscribe(3)
	if err != nil {
		t.Fatalf("subscribe2: %v", err)
	}
	defer sub2.Close()
	rest := drainN(t, sub2, 3)
	assertSeq(t, rest, 4, 5, 6)

	// 此时没有更新的结果：Next 必须阻塞而非返回错误或零值。
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	if _, err := sub2.Next(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Next on caught-up sub err=%v, want deadline", err)
	}
}

// TestDuplicateAckIdempotent：重复确认、乱序确认（先大后小）都单调取最大，
// 且总线不会对同一位点补发第二次。
func TestDuplicateAckIdempotent(t *testing.T) {
	b, in, cleanup := newTestBus(t, 16, 4)
	defer cleanup()
	in <- res(ResultFinal, "BTC", 0, 1)
	in <- res(ResultFinal, "BTC", 10_000, 2)
	waitBusTail(t, b, 2)

	sub, err := b.Subscribe(0)
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	defer sub.Close()
	drainN(t, sub, 2)
	for _, p := range []Position{2, 2, 1, 2, 0} {
		if got := sub.Ack(p); got != 2 {
			t.Fatalf("Ack(%d) = %d, want committed 2", p, got)
		}
	}
	if sub.NextPosition() != 3 {
		t.Fatalf("next position = %d, want 3", sub.NextPosition())
	}

	// 在位点 2 重新订阅等价于“按同一进度重放”（调用方显式选择），
	// 但新订阅内每位点仍只交付一次；旧订阅关闭后不能再读到。
	if err := sub.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	if err := sub.Close(); err != nil { // 幂等
		t.Fatalf("second close: %v", err)
	}
	if _, err := sub.Next(context.Background()); !errors.Is(err, ErrSubscriptionClosed) {
		t.Fatalf("Next after close err=%v, want ErrSubscriptionClosed", err)
	}
}

// TestSubscribeFromFutureWaitsAtAbsolutePosition：传入超过当前尾端的位点不夹取、不跳结果，
// 保留该绝对位点并等待；日志推进到该位点后交付对应结果。
func TestSubscribeFromFutureWaitsAtAbsolutePosition(t *testing.T) {
	b, in, cleanup := newTestBus(t, 16, 4)
	defer cleanup()
	in <- res(ResultFinal, "BTC", 0, 1)
	waitBusTail(t, b, 1)

	sub, err := b.Subscribe(100) // 总线刚到 1，订阅者从 100 续读
	if err != nil {
		t.Fatalf("subscribe future: %v", err)
	}
	defer sub.Close()
	if sub.NextPosition() != 101 {
		t.Fatalf("future from must be preserved: next=%d want 101", sub.NextPosition())
	}
	// 结果 2 不会被投递给该订阅（它在 100 之前）。
	in <- res(ResultFinal, "BTC", 10_000, 2)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	if _, err := sub.Next(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("future sub must wait, err=%v", err)
	}
	// 直接把日志推进到位点 101：喂 99 条不同窗口结果。
	for i := int64(2); i < 101; i++ {
		in <- res(ResultFinal, "ETH", (i+1)*10_000, i)
	}
	got := drainN(t, sub, 1)
	if got[0].Position != 101 {
		t.Fatalf("future sub delivered pos %d, want 101", got[0].Position)
	}
}

// TestStartTwiceRejected：每个总线只能挂接一个结果源。
func TestStartTwiceRejected(t *testing.T) {
	b, in, cleanup := newTestBus(t, 16, 4)
	defer cleanup()
	if err := b.Start(in); !errors.Is(err, ErrBusStarted) {
		t.Fatalf("second start err=%v, want ErrBusStarted", err)
	}
}

// TestInvalidBusConfig：非法配置必须显式失败。
func TestInvalidBusConfig(t *testing.T) {
	if _, err := NewResultBus(ResultBusConfig{HistoryCapacity: -1, MaxSubscribers: 1}); !errors.Is(err, ErrInvalidBusConfig) {
		t.Fatalf("cap=-1 err=%v, want ErrInvalidBusConfig", err)
	}
	if _, err := NewResultBus(ResultBusConfig{HistoryCapacity: 1, MaxSubscribers: -1}); !errors.Is(err, ErrInvalidBusConfig) {
		t.Fatalf("maxSubs=-1 err=%v, want ErrInvalidBusConfig", err)
	}
	// 零值结构体等价于默认配置，保证兼容。
	if _, err := NewResultBus(ResultBusConfig{}); err != nil {
		t.Fatalf("zero config should default: %v", err)
	}
}

// TestBusDrainsHistoryAfterClose：上游关闭后，保留结果可继续排空，
// 排空后 Next 返回可区分的 ErrBusClosed；Wait 在有限时间内返回。
func TestBusDrainsHistoryAfterClose(t *testing.T) {
	cfg := DefaultResultBusConfig()
	cfg.Logger = discardLogger()
	b, err := NewResultBus(cfg)
	if err != nil {
		t.Fatalf("new bus: %v", err)
	}
	in := make(chan WindowResult, 4)
	in <- res(ResultFinal, "BTC", 0, 1)
	in <- res(ResultFinal, "BTC", 10_000, 2)
	if err := b.Start(in); err != nil {
		t.Fatalf("start: %v", err)
	}
	waitBusTail(t, b, 2)

	closed := make(chan struct{})
	go func() { close(in); close(closed) }()
	<-closed
	b.Wait()

	sub, err := b.Subscribe(0)
	if err != nil {
		t.Fatalf("subscribe after close: %v", err)
	}
	defer sub.Close()
	got := drainN(t, sub, 2)
	assertSeq(t, got, 1, 2)
	if _, err := sub.Next(context.Background()); !errors.Is(err, ErrBusClosed) {
		t.Fatalf("Next after drained err=%v, want ErrBusClosed", err)
	}
}

var _ = fmt.Sprintf

// foldView 把若干结果按窗口身份折叠为“每窗口最新一条结果”（模拟下游物化视图）。
func foldView(es []SequencedResult) map[WindowKey]SequencedResult {
	m := make(map[WindowKey]SequencedResult)
	for _, e := range es {
		if old, ok := m[e.Result.Window]; !ok || e.Position > old.Position {
			m[e.Result.Window] = e
		}
	}
	return m
}

// strictAggEqual 逐字段严格相等（不用任何 epsilon 近似判定）。
func strictAggEqual(t *testing.T, where string, got, want Aggregate) {
	t.Helper()
	if got.EventCount != want.EventCount || got.TotalQty != want.TotalQty ||
		got.Notional != want.Notional || got.FirstTime != want.FirstTime || got.LastTime != want.LastTime {
		t.Fatalf("%s agg strict mismatch:\n got=%+v\nwant=%+v", where, got, want)
	}
}

// assertViewsStrictEqual 逐窗口、逐字段（含 Kind/位点/Reason/Watermark）严格比对两个物化视图。
func assertViewsStrictEqual(t *testing.T, where string, got, want map[WindowKey]SequencedResult) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("%s view window count %d != %d", where, len(got), len(want))
	}
	for k, w := range want {
		g, ok := got[k]
		if !ok {
			t.Fatalf("%s missing window %s", where, k)
		}
		if g.Position != w.Position {
			t.Fatalf("%s window %s position %d != %d", where, k, g.Position, w.Position)
		}
		if g.Result.Kind != w.Result.Kind {
			t.Fatalf("%s window %s kind %s != %s", where, k, g.Result.Kind, w.Result.Kind)
		}
		if g.Result.Watermark != w.Result.Watermark {
			t.Fatalf("%s window %s watermark %d != %d", where, k, g.Result.Watermark, w.Result.Watermark)
		}
		if g.Result.Reason != w.Result.Reason {
			t.Fatalf("%s window %s reason %q != %q", where, k, g.Result.Reason, w.Result.Reason)
		}
		strictAggEqual(t, fmt.Sprintf("%s window %s", where, k), g.Result.Agg, w.Result.Agg)
	}
}

// buildMixedFeed 构造跨多窗口、含更正链的确定性结果序列（10 条）。
func buildMixedFeed() []WindowResult {
	return []WindowResult{
		res(ResultFinal, "BTC", 0, 1),       // 1
		res(ResultFinal, "ETH", 0, 1),       // 2
		res(ResultFinal, "BTC", 10_000, 1),  // 3
		res(ResultUpdate, "BTC", 0, 2),      // 4 更正 w0
		res(ResultFinal, "ETH", 10_000, 1),  // 5
		res(ResultUpdate, "BTC", 0, 3),      // 6 再更正 w0
		res(ResultFinal, "BTC", 20_000, 1),  // 7
		res(ResultUpdate, "ETH", 0, 2),      // 8 更正 ETH w0
		res(ResultFinal, "ETH", 20_000, 1),  // 9
		res(ResultUpdate, "BTC", 10_000, 4), // 10 更正 w1
	}
}

// TestSnapshotPlusIncrementalEqualsContinuous：快照视图 + 边界后续增量，
// 与从序号 1 连续消费得到的物化视图逐字段严格一致（含窗口身份、更正先后与聚合数值）。
func TestSnapshotPlusIncrementalEqualsContinuous(t *testing.T) {
	feed := buildMixedFeed()
	b, in, cleanup := newTestBus(t, len(feed)*2, 8)
	defer cleanup()

	// 先投放到中点 K=5 再取快照（快照发生在运行中，不是空库或结尾）。
	for i := 0; i < 5; i++ {
		in <- feed[i]
	}
	waitBusTail(t, b, 5)

	// 参照：从不断线、序号 1 开始的连续订阅。
	continuous, err := b.Subscribe(0)
	if err != nil {
		t.Fatalf("subscribe continuous: %v", err)
	}
	defer continuous.Close()

	snap := b.Snapshot()
	if snap.Position != 5 {
		t.Fatalf("snapshot position %d != 5", snap.Position)
	}
	if snap.Oldest != 0 {
		t.Fatalf("fresh stream snapshot Oldest %d != 0", snap.Oldest)
	}
	// 快照视图必须取到每个窗口截至位点 5 的最新结果：
	// BTC w0 已被位点 4 的 UPDATE 覆盖；ETH w1 是位点 5 的 FINAL。
	if e, ok := snap.Lookup(WindowKey{Symbol: "BTC", Start: 0, End: 10_000}); !ok || e.Position != 4 {
		t.Fatalf("snapshot BTC w0 = %+v ok=%v, want pos 4", e, ok)
	}
	if e, ok := snap.Lookup(WindowKey{Symbol: "ETH", Start: 10_000, End: 20_000}); !ok || e.Position != 5 {
		t.Fatalf("snapshot ETH w1 = %+v ok=%v, want pos 5", e, ok)
	}

	// 快照恢复路径：先装视图，再从快照边界续读增量。
	snapView := make(map[WindowKey]SequencedResult)
	for _, sw := range snap.Windows {
		snapView[sw.Result.Window] = SequencedResult{Position: sw.Position, Result: sw.Result}
	}
	restored, err := b.Subscribe(snap.Position)
	if err != nil {
		t.Fatalf("subscribe restored: %v", err)
	}
	defer restored.Close()

	// 投放剩余结果。
	for i := 5; i < len(feed); i++ {
		in <- feed[i]
	}
	waitBusTail(t, b, Position(len(feed)))

	inc := drainN(t, restored, len(feed)-5)
	assertSeq(t, inc, 6, 7, 8, 9, 10)
	restoredView := foldView(inc)
	for k, v := range snapView {
		if old, ok := restoredView[k]; !ok || v.Position > old.Position {
			restoredView[k] = v
		}
	}

	cont := drainN(t, continuous, len(feed))
	assertSeq(t, cont, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
	continuousView := foldView(cont)

	// 逐字段严格一致（数值用 ==，不是近似相等）。
	assertViewsStrictEqual(t, "snapshot+incremental vs continuous", restoredView, continuousView)

	// 关键窗口的确定值：BTC w0 的最新值是位点 6 的 UPDATE（count=3），
	// 不能被位点 4 的旧更正或位点 1 的 FINAL 盖住。
	btcW0 := WindowKey{Symbol: "BTC", Start: 0, End: 10_000}
	if rv := restoredView[btcW0]; rv.Position != 6 || rv.Result.Kind != ResultUpdate ||
		rv.Result.Agg.EventCount != 3 {
		t.Fatalf("BTC w0 final entry = %+v, want pos 6 UPDATE count 3", rv)
	}
	btcW1 := WindowKey{Symbol: "BTC", Start: 10_000, End: 20_000}
	if rv := restoredView[btcW1]; rv.Position != 10 || rv.Result.Agg.EventCount != 4 {
		t.Fatalf("BTC w1 final entry = %+v, want pos 10 count 4", rv)
	}
}

// TestSnapshotAfterRotationInvariant：历史轮转后，快照视图只含仍保留窗口；
// 但“快照视图 + 增量”必须等于“从快照 Oldest 边界起连续消费”，不变式仍然成立。
func TestSnapshotAfterRotationInvariant(t *testing.T) {
	const cap_ = 4
	b, in, cleanup := newTestBus(t, cap_, 4)
	defer cleanup()

	// 8 条结果全部落在不同窗口 => 轮转后视图只保留最后 4 个窗口。
	for i := 0; i < 8; i++ {
		in <- res(ResultFinal, "BTC", int64(i)*10_000, int64(i+1))
	}
	waitBusTail(t, b, 8)
	if got := b.HistoryLen(); got != cap_ {
		t.Fatalf("history len %d != cap %d", got, cap_)
	}
	if old := b.OldestPosition(); old != 4 {
		t.Fatalf("oldest %d != 4 after rotation", old)
	}

	snap := b.Snapshot()
	if snap.Position != 8 || snap.Oldest != 4 || len(snap.Windows) != 4 {
		t.Fatalf("rotated snap pos=%d oldest=%d windows=%d", snap.Position, snap.Oldest, len(snap.Windows))
	}

	// 参照：从快照的 Oldest 边界新订阅（位点 5..8）。
	fromOldest, err := b.Subscribe(snap.Oldest)
	if err != nil {
		t.Fatalf("subscribe oldest: %v", err)
	}
	defer fromOldest.Close()
	base := drainN(t, fromOldest, 4)
	refView := foldView(base)

	snapView := make(map[WindowKey]SequencedResult)
	for _, sw := range snap.Windows {
		snapView[sw.Result.Window] = SequencedResult{Position: sw.Position, Result: sw.Result}
	}
	assertViewsStrictEqual(t, "rotated snapshot vs sub-from-oldest", snapView, refView)

	// 再追加结果，验证快照 + 增量 == 从 Oldest 连续消费（两侧应用相同增量）。
	in <- res(ResultUpdate, "BTC", 70_000, 99)
	in <- res(ResultFinal, "BTC", 80_000, 1)
	waitBusTail(t, b, 10)
	incSnap, err := b.Subscribe(snap.Position)
	if err != nil {
		t.Fatalf("subscribe inc: %v", err)
	}
	defer incSnap.Close()
	for _, e := range drainN(t, incSnap, 2) {
		if old, ok := snapView[e.Result.Window]; !ok || e.Position > old.Position {
			snapView[e.Result.Window] = e
		}
	}
	for _, e := range drainN(t, fromOldest, 2) {
		refView[e.Result.Window] = e
	}
	assertViewsStrictEqual(t, "rotated snapshot+incremental", snapView, refView)
}

// TestSlowSubscriberOverflowAndRecovery：完全不读的慢订阅者在历史轮转越过其进度时
// 得到可区分的 ErrPositionExpired 并被总线摘除；总线与其他订阅不受影响，
// 慢订阅者可通过“重新取快照 + 从快照边界续读”恢复且不丢任何后续结果。
func TestSlowSubscriberOverflowAndRecovery(t *testing.T) {
	const cap_ = 4
	b, in, cleanup := newTestBus(t, cap_, 8)
	defer cleanup()

	slow, err := b.Subscribe(0)
	if err != nil {
		t.Fatalf("subscribe slow: %v", err)
	}

	fast, err := b.Subscribe(0)
	if err != nil {
		t.Fatalf("subscribe fast: %v", err)
	}
	defer fast.Close()

	// 灌满容量：slow 从不读取；fast 同步排空，保持在线。
	for i := 0; i < cap_; i++ {
		in <- res(ResultFinal, "BTC", int64(i)*10_000, int64(i+1))
	}
	waitBusTail(t, b, Position(cap_))
	assertSeq(t, drainN(t, fast, cap_), 1, 2, 3, 4) // fast 跟到位点 4
	// 再多 1 条 -> seq1 被淘汰：slow(next=1) 越过保留边界，fast(next=5) 仍在界内。
	in <- res(ResultFinal, "BTC", int64(cap_)*10_000, int64(cap_+1))
	waitBusTail(t, b, Position(cap_+1))

	// 总线主动摘除慢订阅者，订阅表不被泄漏的停滞订阅占满。
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if b.Subscribers() == 1 {
			break
		}
		time.Sleep(time.Millisecond)
	}
	if n := b.Subscribers(); n != 1 {
		t.Fatalf("slow subscriber not evicted: subs=%d", n)
	}

	// 慢订阅者下一次 Next 得到明确、可区分的过期结论，绝不静默跳过。
	if _, err := slow.Next(context.Background()); !errors.Is(err, ErrPositionExpired) {
		t.Fatalf("slow Next err=%v, want ErrPositionExpired", err)
	}
	// 快订阅者正常收到后续结果。
	assertSeq(t, drainN(t, fast, 1), 5)

	// 用更早位点重连也必须被明确拒绝，不能悄悄从中间补发。
	if _, err := b.Subscribe(0); !errors.Is(err, ErrPositionExpired) {
		t.Fatalf("subscribe before oldest err=%v, want ErrPositionExpired", err)
	}

	// 恢复路径：取当前快照 -> 装视图 -> 从快照边界续读，后续结果一个不漏。
	snap := b.Snapshot()
	snapView := make(map[WindowKey]SequencedResult)
	for _, sw := range snap.Windows {
		snapView[sw.Result.Window] = SequencedResult{Position: sw.Position, Result: sw.Result}
	}
	recovered, err := b.Subscribe(snap.Position)
	if err != nil {
		t.Fatalf("recovered subscribe: %v", err)
	}
	defer recovered.Close()
	// 参照：一个在快照点在线、从快照旧边界起持续消费且始终跟得上的订阅者。
	ref, err := b.Subscribe(snap.Oldest)
	if err != nil {
		t.Fatalf("ref subscribe: %v", err)
	}
	defer ref.Close()
	// 先排空到快照尾端（fold(headSeq..tail) 必须恰好等于快照视图）。
	refEntries := drainN(t, ref, int(snap.Position-snap.Oldest))
	assertViewsStrictEqual(t, "snapshot view == fold(retained history)",
		snapView, foldView(refEntries))

	in <- res(ResultFinal, "BTC", 90_000, 10)
	in <- res(ResultUpdate, "BTC", 40_000, 77) // 更正仍保留在历史中的旧窗口
	waitBusTail(t, b, Position(cap_+3))
	for _, e := range drainN(t, recovered, 2) {
		snapView[e.Result.Window] = e // 恢复路径上增量位点必然 > 快照位点
	}

	// 参照排空同一批增量后折叠：快照+增量必须与从旧边界不断线消费严格一致。
	refEntries = append(refEntries, drainN(t, ref, 2)...)
	assertViewsStrictEqual(t, "recovered snapshot+incremental vs continuous-from-oldest",
		snapView, foldView(refEntries))
	// 更正必须按位点序生效：补发的 BTC@40000 更正晚于其 FINAL，
	// 恢复视图中该窗口的最新值是更正值 77，而不是 FINAL 值 5。
	if e := snapView[WindowKey{Symbol: "BTC", Start: 40_000, End: 50_000}]; e.Result.Agg.EventCount != 77 {
		t.Fatalf("recovered BTC@40000 count=%d, want corrected 77", e.Result.Agg.EventCount)
	}
}

// TestMaxSubscribersCap：在线订阅者数严格有界，超出返回可区分结论；
// 关闭一个后名额立即释放。
func TestMaxSubscribersCap(t *testing.T) {
	b, _, cleanup := newTestBus(t, 16, 2)
	defer cleanup()

	s1, err := b.Subscribe(0)
	if err != nil {
		t.Fatalf("s1: %v", err)
	}
	s2, err := b.Subscribe(0)
	if err != nil {
		t.Fatalf("s2: %v", err)
	}
	if _, err := b.Subscribe(0); !errors.Is(err, ErrTooManySubscribers) {
		t.Fatalf("third subscriber err=%v, want ErrTooManySubscribers", err)
	}
	_ = s1.Close()
	waitSubs := func(want int) {
		deadline := time.Now().Add(2 * time.Second)
		for time.Now().Before(deadline) {
			if b.Subscribers() == want {
				return
			}
			time.Sleep(time.Millisecond)
		}
		t.Fatalf("subscribers = %d, want %d", b.Subscribers(), want)
	}
	waitSubs(1)
	s3, err := b.Subscribe(0)
	if err != nil {
		t.Fatalf("s3 after close: %v", err)
	}
	defer s3.Close()
	defer s2.Close()
}

// TestBoundedHistoryRetention：历史条数恒不超过容量，OldestPosition 随轮转推进，
// 位点与窗口身份在长期运行下保持稳定。
func TestBoundedHistoryRetention(t *testing.T) {
	const cap_ = 8
	b, in, cleanup := newTestBus(t, cap_, 4)
	defer cleanup()

	for round := 1; round <= 5; round++ {
		for i := 0; i < cap_; i++ {
			in <- res(ResultFinal, "BTC", int64((round-1)*cap_+i)*10_000, 1)
		}
		wantTail := Position(round * cap_)
		waitBusTail(t, b, wantTail)
		if b.HistoryLen() != cap_ {
			t.Fatalf("round %d history len %d != cap %d", round, b.HistoryLen(), cap_)
		}
		if b.CurrentPosition() != wantTail || b.OldestPosition() != wantTail-Position(cap_) {
			t.Fatalf("round %d tail=%d oldest=%d", round, b.CurrentPosition(), b.OldestPosition())
		}
	}
	// 最新订阅只能从 Oldest 起；位点 40 的结果仍可取，其窗口身份不被复用。
	sub, err := b.Subscribe(b.OldestPosition())
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	defer sub.Close()
	e, err := sub.Next(context.Background())
	if err != nil {
		t.Fatalf("next: %v", err)
	}
	if e.Position != 33 || e.Result.Window.Start != 320_000 {
		t.Fatalf("oldest retained = pos %d start %d, want 33/320000", e.Position, e.Result.Window.Start)
	}
}

// TestSnapshotLatestSurvivesRotationAcrossSameWindow：同一窗口的 FINAL 与多条 UPDATE
// 跨越环形轮转边界：被淘汰的旧更正绝不能残留在快照视图中，视图恒为保留范围内最新值；
// 且“快照 + 增量”与从 Oldest 连续消费严格一致。
func TestSnapshotLatestSurvivesRotationAcrossSameWindow(t *testing.T) {
	const cap_ = 4
	b, in, cleanup := newTestBus(t, cap_, 4)
	defer cleanup()

	// 位点 1：BTC w0 FINAL(count=1)；位点 2..5：其它窗口结果把 w0 FINAL 挤出历史。
	in <- res(ResultFinal, "BTC", 0, 1)
	for i := 1; i <= 4; i++ {
		in <- res(ResultFinal, "ETH", int64(i)*10_000, int64(i))
	}
	waitBusTail(t, b, 5)
	if old := b.OldestPosition(); old != 1 {
		t.Fatalf("oldest=%d want 1", old)
	}
	snap0 := b.Snapshot()
	if _, ok := snap0.Lookup(WindowKey{Symbol: "BTC", Start: 0, End: 10_000}); ok {
		t.Fatal("evicted BTC w0 must not appear in snapshot view")
	}

	// 现在对 w0 发两条更正（位点 6、7）。位点 6 随后也会被位点 8 挤出，7 保留。
	in <- res(ResultUpdate, "BTC", 0, 10)
	in <- res(ResultUpdate, "BTC", 0, 20)
	in <- res(ResultFinal, "ETH", 60_000, 6) // 位点 8：挤掉位点 4；位点 5..8 保留
	waitBusTail(t, b, 8)

	snap := b.Snapshot()
	e, ok := snap.Lookup(WindowKey{Symbol: "BTC", Start: 0, End: 10_000})
	if !ok {
		t.Fatal("BTC w0 with retained updates must appear in snapshot")
	}
	// 必须是最新更正位点 7、count=20，而不是被淘汰的位点 6（count=10）。
	if e.Position != 7 || e.Result.Kind != ResultUpdate || e.Result.Agg.EventCount != 20 {
		t.Fatalf("BTC w0 snapshot entry=%+v, want pos 7 UPDATE count 20", e)
	}

	// 快照 + 增量 == 从 Oldest(=4) 连续消费折叠。
	view := map[WindowKey]SequencedResult{}
	for _, sw := range snap.Windows {
		view[sw.Result.Window] = SequencedResult{Position: sw.Position, Result: sw.Result}
	}
	ref, err := b.Subscribe(snap.Oldest)
	if err != nil {
		t.Fatalf("ref subscribe: %v", err)
	}
	defer ref.Close()
	refView := foldView(drainN(t, ref, int(snap.Position-snap.Oldest)))
	assertViewsStrictEqual(t, "same-window chain across rotation", view, refView)
}

// TestConcurrentSubscribersIndependentDelivery：多个并发订阅者从不同位点独立拉取，
// 各自位点序列严格正确、互不串扰（race 检测器验证共享状态访问）。
func TestConcurrentSubscribersIndependentDelivery(t *testing.T) {
	const n = 40
	b, in, cleanup := newTestBus(t, 256, 8)
	defer cleanup()

	starts := []Position{0, 10, 20, 30}
	errCh := make(chan error, len(starts))
	var wg sync.WaitGroup
	for _, from := range starts {
		s, err := b.Subscribe(from)
		if err != nil {
			t.Fatalf("subscribe %d: %v", from, err)
		}
		wg.Add(1)
		go func(from Position, s *Subscription) {
			defer wg.Done()
			defer s.Close()
			want := from + 1
			for want <= n {
				e, err := s.Next(context.Background())
				if err != nil {
					errCh <- fmt.Errorf("from %d: %w", from, err)
					return
				}
				if e.Position != want {
					errCh <- fmt.Errorf("from %d got pos %d want %d", from, e.Position, want)
					return
				}
				s.Ack(e.Position)
				want++
			}
		}(from, s)
	}

	for i := 1; i <= n; i++ {
		in <- res(ResultFinal, "BTC", int64(i)*10_000, int64(i))
	}
	wg.Wait()
	close(errCh)
	for err := range errCh {
		t.Fatal(err)
	}
}
