package marketstream

import (
	"context"
	"testing"
)

// buildInterleavedFixture 构造两个分区的乱序行情事件（同 symbol、跨两个窗口）。
func buildInterleavedFixture() map[int][]Event {
	events := []Event{
		mkEvent(0, 0, "BTC", 1_000, 100, 2),
		mkEvent(0, 1, "BTC", 5_000, 110, 3),
		mkEvent(0, 2, "BTC", 12_000, 120, 1),
		mkEvent(0, 3, "BTC", 16_000, 130, 4),
		mkEvent(1, 0, "BTC", 2_000, 105, 1),
		mkEvent(1, 1, "BTC", 9_000, 108, 2),
		mkEvent(1, 2, "BTC", 18_000, 140, 1),
		mkEvent(1, 3, "BTC", 11_000, 115, 5), // 分区内乱序：落在窗口 [10000,20000)
	}
	part0 := []Event{events[0], events[1], events[2], events[3]}
	part1 := []Event{events[4], events[5], events[6], events[7]}
	return map[int][]Event{0: part0, 1: part1}
}

// ingestAll 按给定顺序串行投递全部事件，最后标记两个分区完成。
func ingestAll(ctx context.Context, t *testing.T, p *Processor, order []Event) {
	t.Helper()
	for _, e := range order {
		if err := p.Ingest(ctx, e); err != nil {
			t.Fatalf("Ingest %s: %v", e.ID, err)
		}
	}
	for _, id := range p.partitions {
		if err := p.MarkComplete(ctx, id); err != nil {
			t.Fatalf("MarkComplete %d: %v", id, err)
		}
	}
}

// allEventOrders 返回几种不同的到达交错顺序，但事件集合相同。
func allEventOrders(fix map[int][]Event) [][]Event {
	p0, p1 := fix[0], fix[1]
	// 顺序 A：p0 全部然后 p1 全部（大批次）
	orderA := append(append([]Event{}, p0...), p1...)
	// 顺序 B：p1 全部然后 p0 全部（批大小变化）
	orderB := append(append([]Event{}, p1...), p0...)
	// 顺序 C：逐条交错（小批次）
	orderC := make([]Event, 0, 8)
	for i := 0; i < 4; i++ {
		orderC = append(orderC, p0[i], p1[i])
	}
	return [][]Event{orderA, orderB, orderC}
}

func TestInterleavingOrderAndBatchIndependent(t *testing.T) {
	fix := buildInterleavedFixture()
	var baseline map[WindowKey]Aggregate

	for ri, order := range allEventOrders(fix) {
		ctx := context.Background()
		cfg := testConfig(t)
		p, col, cleanup := runProcessor(ctx, t, cfg, []int{0, 1})
		ingestAll(ctx, t, p, order)

		// 等待两个窗口都已发出 FINAL（异步收集），再做快照断言。
		waitForResults(t, col, 2)
		// 屏障确保全部消息处理完毕。
		cp, err := p.Checkpoint(ctx)
		if err != nil {
			t.Fatalf("checkpoint: %v", err)
		}
		t.Logf("run %d final watermark=%s, windows in state=%d, dedup=%d",
			ri, wmString(cp.Watermark), len(cp.Windows), len(cp.Dedup))

		final := make(map[WindowKey]Aggregate)
		for k, r := range col.finalResults() {
			final[k] = r.Agg
		}
		cleanup()

		if ri == 0 {
			baseline = final
			t.Logf("baseline windows=%d", len(baseline))
			continue
		}
		if len(final) != len(baseline) {
			t.Fatalf("run %d: window count %d != baseline %d", ri, len(final), len(baseline))
		}
		for k, want := range baseline {
			got, ok := final[k]
			if !ok {
				t.Fatalf("run %d: missing window %s", ri, k)
			}
			if !got.Equal(want) {
				t.Fatalf("run %d: window %s agg=%+v != baseline %+v (order-dependent result)",
					ri, k, got, want)
			}
		}
		t.Logf("run %d matches baseline across %d windows", ri, len(baseline))
	}

	// 断言两个窗口的确定值：
	// [0,10000):   count=4, qty=2+3+1+2=8, notional=200+330+105+216=851, vwap=106.375
	// [10000,20000): count=4, qty=1+4+1+5=11, notional=120+520+140+575=1355, vwap=123.1818...
	w0 := WindowKey{Symbol: "BTC", Start: 0, End: 10_000}
	w1 := WindowKey{Symbol: "BTC", Start: 10_000, End: 20_000}
	a0, ok := baseline[w0]
	if !ok {
		t.Fatalf("missing window %s", w0)
	}
	if a0.EventCount != 4 || a0.TotalQty != 8 || a0.Notional != 851 || a0.FirstTime != 1_000 || a0.LastTime != 9_000 {
		t.Fatalf("window0 agg mismatch: %+v", a0)
	}
	a1 := baseline[w1]
	if a1.EventCount != 4 || a1.TotalQty != 11 || a1.Notional != 1355 || a1.FirstTime != 11_000 || a1.LastTime != 18_000 {
		t.Fatalf("window1 agg mismatch: %+v", a1)
	}
}
