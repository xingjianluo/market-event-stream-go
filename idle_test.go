package marketstream

import (
	"context"
	"testing"
	"time"
)

// TestIdlePartitionUnblocksWindowFinishing 验证：
// 慢分区停滞时，在 IdleTimeout 之前窗口不被过早关闭；
// 超时后停滞分区临时移出全局水位线，已完成窗口得以最终化；
// 分区恢复后来到的迟到数据仍按迟到语义处理，水位线不回退。
func TestIdlePartitionUnblocksWindowFinishing(t *testing.T) {
	ctx := context.Background()
	clk := newFakeClock()
	cfg := testConfig(t)
	cfg.Clock = clk
	cfg.IdleTimeout = 2 * time.Second
	cfg.MaxOutOfOrderness = 0
	p, col, cleanup := runProcessor(ctx, t, cfg, []int{0, 1})
	defer cleanup()

	// 慢分区 p1 在处理时间 t0 停住（事件时间 5000），p0 同时点亮。
	p.Ingest(ctx, mkEvent(1, 0, "BTC", 5_000, 100, 1))
	p.Ingest(ctx, mkEvent(0, 0, "BTC", 1_000, 100, 1))

	// 推进处理时间 1.5s：快分区 p0 此刻仍来数据（活动时间刷新到 t0+1.5s），
	// 慢分区 p1 已 1.5s 无事件；尚未到 IdleTimeout，窗口不过早关闭。
	clk.Advance(1500 * time.Millisecond)
	p.Ingest(ctx, mkEvent(0, 1, "BTC", 18_000, 200, 2))
	if cp, _ := p.Checkpoint(ctx); cp.Watermark != 5_000 {
		t.Fatalf("watermark should be pinned at 5000, got %s", wmString(cp.Watermark))
	}
	p.TickIdleNow(ctx)
	p.Checkpoint(ctx) // 屏障：确保 tick 已处理
	if rs := col.Results(); len(rs) != 0 {
		t.Fatalf("window must not finalize before idle timeout, got %d", len(rs))
	}

	// 再推进 600ms：p1 累计停滞 2.1s 超过 IdleTimeout 被标记空闲；
	// p0 距上次活动仅 0.6s 仍活跃。全局水位线跳到 18000，窗口最终化。
	clk.Advance(600 * time.Millisecond)
	p.TickIdleNow(ctx)
	p.Checkpoint(ctx) // 屏障：确保空闲判定与窗口最终化已完成
	waitForResults(t, col, 1)

	r := col.finalResults()[WindowKey{Symbol: "BTC", Start: 0, End: 10_000}]
	if r.Kind != ResultFinal {
		t.Fatalf("expected FINAL from unblocked window, got %s", r.Kind)
	}
	// p1#0 t=5000 与 p0#0 t=1000 同在窗口 [0,10000)，共 2 条。
	if r.Agg.EventCount != 2 || r.Agg.TotalQty != 2 {
		t.Fatalf("window agg mismatch: %+v", r.Agg)
	}

	// p1 恢复并来一条 t=6000 的迟到事件：当前 wm=18000，dropBoundary=13000，
	// 窗口结束 10000 <= 13000 => 超出宽限，明确转入侧输出而非静默丢弃。
	p.Ingest(ctx, mkEvent(1, 1, "BTC", 6_000, 300, 5))
	waitForRejections(t, col, 1) // 侧输出经接力异步投递
	waitForVerdicts(t, col, 4)
	last := col.Verdicts()[3]
	if last.Decision != DecisionDroppedLate {
		t.Fatalf("resumed late event at wm18000 should be DROPPED_LATE, got %s (%s)",
			last.Decision, last.Reason)
	}
	rej := col.Rejections()
	if len(rej) != 1 {
		t.Fatalf("want 1 side rejection, got %d", len(rej))
	}
	t.Logf("rejected resumed-late event: %s reason=%q", rej[0].Event, rej[0].Reason)

	// 恢复活动不回退水位线。
	if cp, _ := p.Checkpoint(ctx); cp.Watermark != 18_000 {
		t.Fatalf("watermark must remain 18000 after resume, got %s", wmString(cp.Watermark))
	}
	t.Log("idle unblocked window without premature close; resumed late event rejected; wm stays 18000")
}
