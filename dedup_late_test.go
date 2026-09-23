package marketstream

import (
	"context"
	"testing"
)

func TestDuplicateDeliveryNotCounted(t *testing.T) {
	ctx := context.Background()
	cfg := testConfig(t)
	p, col, cleanup := runProcessor(ctx, t, cfg, []int{0})
	defer cleanup()

	// 第一批：两条事件
	p.Ingest(ctx, mkEvent(0, 0, "BTC", 1_000, 100, 2))
	p.Ingest(ctx, mkEvent(0, 1, "BTC", 2_000, 110, 3))
	// 模拟"故障重放/未提交批次重投"：同样的 ID 再次投递，穿插在新事件之间。
	p.Ingest(ctx, mkEvent(0, 0, "BTC", 1_000, 100, 2)) // 重复
	p.Ingest(ctx, mkEvent(0, 2, "BTC", 3_000, 120, 1))
	p.Ingest(ctx, mkEvent(0, 1, "BTC", 2_000, 110, 3)) // 重复
	p.MarkComplete(ctx, 0)

	waitForResults(t, col, 1)
	waitForVerdicts(t, col, 5)

	verdicts := col.Verdicts()
	var dup int
	for _, v := range verdicts {
		if v.Decision == DecisionDuplicate {
			dup++
		}
	}
	if dup != 2 {
		t.Fatalf("want 2 DUPLICATE verdicts, got %d: %+v", dup, verdicts)
	}

	final := col.finalResults()
	w := WindowKey{Symbol: "BTC", Start: 0, End: 10_000}
	r, ok := final[w]
	if !ok {
		t.Fatalf("missing window result %s; got %+v", w, final)
	}
	// 只应计入 3 条（offset 0,1,2），重复事件不增加笔数/量/金额。
	if r.Agg.EventCount != 3 || r.Agg.TotalQty != 6 {
		t.Fatalf("duplicate events were counted: %+v", r.Agg)
	}
	if r.Agg.Notional != 100*2+110*3+120 {
		t.Fatalf("notional mismatch after dedup: %v", r.Agg.Notional)
	}
	t.Logf("dedup verified: count=%d qty=%d notional=%.0f, duplicates=%d",
		r.Agg.EventCount, r.Agg.TotalQty, r.Agg.Notional, dup)
}

// TestLateDataWithinAndBeyondGrace 同时覆盖宽限内 LATE_UPDATE 与超宽限 DROPPED_LATE。
func TestLateDataWithinAndBeyondGrace(t *testing.T) {
	ctx := context.Background()
	cfg := testConfig(t)
	// 窗口 10s；乱序容忍 0，使判定边界清晰：wm==分区最大事件时间。
	cfg.MaxOutOfOrderness = 0
	cfg.AllowedLateness = 5_000
	p, col, cleanup := runProcessor(ctx, t, cfg, []int{0})
	defer cleanup()

	// 单分区：用正常事件驱动水位线，迟到的 BTC 事件穿插其后（不推进分区最大时间）。
	// #0 BTC t=1000  进窗口 [0,10000)
	p.Ingest(ctx, mkEvent(0, 0, "BTC", 1_000, 100, 1))
	// #1 ETH t=10000 -> wm=10000，BTC 窗口 [0,10000) 发出 FINAL（count=1）
	p.Ingest(ctx, mkEvent(0, 1, "ETH", 10_000, 10, 1))
	waitForResults(t, col, 1)

	// #2 BTC t=5000 迟到：窗口结束 10000 > wm10000-宽限5000=5000 -> 宽限内 UPDATE
	p.Ingest(ctx, mkEvent(0, 2, "BTC", 5_000, 200, 2))
	// #3 ETH t=11000 -> wm=11000，边界 6000
	p.Ingest(ctx, mkEvent(0, 3, "ETH", 11_000, 10, 1))
	// #4 BTC t=6000 迟到：10000 > 11000-5000=6000 -> 仍在宽限内 UPDATE
	p.Ingest(ctx, mkEvent(0, 4, "BTC", 6_000, 300, 3))

	// #5 ETH t=20000 -> wm=20000，dropBoundary=15000
	p.Ingest(ctx, mkEvent(0, 5, "ETH", 20_000, 10, 1))
	// #6 BTC t=2000 迟到且超宽限：窗口结束 10000 <= 15000 -> 侧输出 DROPPED_LATE
	p.Ingest(ctx, mkEvent(0, 6, "BTC", 2_000, 400, 4))
	p.MarkComplete(ctx, 0)
	waitForVerdicts(t, col, 7)

	byOffset := map[int64]Verdict{}
	for _, v := range col.Verdicts() {
		if v.Event.Symbol == "BTC" {
			byOffset[v.Event.ID.Offset] = v
		}
	}
	if byOffset[2].Decision != DecisionLateUpdate {
		t.Fatalf("offset2 want LATE_UPDATE, got %s (%s)", byOffset[2].Decision, byOffset[2].Reason)
	}
	if byOffset[4].Decision != DecisionLateUpdate {
		t.Fatalf("offset4 want LATE_UPDATE, got %s (%s)", byOffset[4].Decision, byOffset[4].Reason)
	}
	if byOffset[6].Decision != DecisionDroppedLate {
		t.Fatalf("offset6 want DROPPED_LATE, got %s (%s)", byOffset[6].Decision, byOffset[6].Reason)
	}

	// 侧输出必须可观测且带明确原因（不静默丢弃）。
	rej := col.Rejections()
	if len(rej) != 1 {
		t.Fatalf("want exactly 1 side-output rejection, got %d: %+v", len(rej), rej)
	}
	if rej[0].Event.ID != byOffset[6].Event.ID {
		t.Fatalf("rejection event mismatch: %+v", rej[0].Event)
	}
	t.Logf("rejected late event: %s wm=%d reason=%q",
		rej[0].Event, rej[0].Watermark, rej[0].Reason)

	// UPDATE 后最终聚合：count=3（#0,#2,#4），qty=1+2+3=6，notional=100+400+900=1400。
	var last *WindowResult
	rs := col.Results()
	for i := range rs {
		if rs[i].Window.Symbol == "BTC" {
			last = &rs[i]
		}
	}
	if last == nil || last.Kind != ResultUpdate {
		t.Fatalf("last BTC result must be UPDATE, got %+v", last)
	}
	if last.Agg.EventCount != 3 || last.Agg.TotalQty != 6 || last.Agg.Notional != 1400 {
		t.Fatalf("late-updated agg mismatch: %+v", last.Agg)
	}
	t.Logf("late update corrected window: count=%d qty=%d notional=%.0f vwap=%.4f",
		last.Agg.EventCount, last.Agg.TotalQty, last.Agg.Notional, last.Agg.VWAP())
}
