package marketstream

import (
	"testing"
	"time"
)

func TestWatermarkMinAcrossPartitionsAndMonotonic(t *testing.T) {
	t0 := time.Date(2026, 9, 24, 9, 0, 0, 0, time.UTC)
	tracker := NewWatermarkTracker([]int{0, 1}, 2_000, time.Second)

	if wm := tracker.Watermark(); wm != NoWatermark {
		t.Fatalf("initial watermark = %d, want NoWatermark", wm)
	}

	// 只有 p0 点亮时，未启动的 p1 以 -inf 参与最小值，阻止过早关窗。
	if _, n, adv := tracker.Observe(0, 10_000, t0); adv || n != NoWatermark {
		t.Fatalf("unobserved p1 must hold back watermark: new=%d adv=%v", n, adv)
	}
	// p1 点亮：p0 wm 8000、p1 wm 7000，全局取最小值 7000。
	_, new1, adv1 := tracker.Observe(1, 9_000, t0)
	if !adv1 || new1 != 7_000 {
		t.Fatalf("global wm must be min(8000,7000)=7000: new=%d adv=%v", new1, adv1)
	}
	// 乱序到达更早事件：不改变分区最大事件时间，水位线不推进。
	_, new2, adv2 := tracker.Observe(1, 8_500, t0)
	if adv2 || new2 != 7_000 {
		t.Fatalf("out-of-order older event must not advance: new=%d adv=%v", new2, adv2)
	}
	// 慢分区 p1 停滞时，即使快分区继续前进，全局水位线也被 p1 钉在 7000。
	_, new3, adv3 := tracker.Observe(0, 20_000, t0)
	if adv3 || new3 != 7_000 {
		t.Fatalf("stuck slow partition must pin watermark at 7000: new=%d adv=%v", new3, adv3)
	}
	// p1 追上：t=13000 -> 11000；min(p0 wm18000, p1 wm11000)=11000，推进。
	_, new4, adv4 := tracker.Observe(1, 13_000, t0)
	if !adv4 || new4 != 11_000 {
		t.Fatalf("p1 catch up should advance to 11000: new=%d adv=%v", new4, adv4)
	}
	// 全局水位线绝不回退：p1 再来一条乱序旧事件不改变结论。
	if tracker.Watermark() != 11_000 {
		t.Fatalf("watermark drifted to %d", tracker.Watermark())
	}
}

func TestWatermarkIdlePartitionUnblocks(t *testing.T) {
	clk := newFakeClock()
	tracker := NewWatermarkTracker([]int{0, 1}, 2_000, 2*time.Second)
	// 两个分区先同时点亮，p1 更慢：全局水位线被钉在 3000。
	tracker.Observe(0, 10_000, clk.Now())
	tracker.Observe(1, 5_000, clk.Now())
	if tracker.Watermark() != 3_000 {
		t.Fatalf("watermark = %d, want 3000", tracker.Watermark())
	}

	// 未到空闲超时：不变
	clk.Advance(1900 * time.Millisecond)
	if _, _, adv, _ := tracker.TickIdle(clk.Now()); adv {
		t.Fatal("must not idle before timeout")
	}
	// 超过 2s 无事件：p0 与 p1 同时停滞，二者都被标记空闲后无活跃分区，
	// 按 Flink 语义全局水位线维持（不推进，也不回退）；只有全部 EOF 才到 +inf。
	clk.Advance(200 * time.Millisecond)
	_, newWM, adv, becameIdle := tracker.TickIdle(clk.Now())
	if adv || newWM != 3_000 {
		t.Fatalf("all-idle must not move watermark: new=%d adv=%v", newWM, adv)
	}
	if len(becameIdle) != 2 {
		t.Fatalf("becameIdle=%v, want both partitions", becameIdle)
	}
	// 分区停滞但只有慢分区空闲的场景
	tracker2 := NewWatermarkTracker([]int{0, 1}, 2_000, 2*time.Second)
	c2 := newFakeClock()
	tracker2.Observe(1, 5_000, c2.Now())  // 慢分区 p1 wm 3000 -> global 3000
	c2.Advance(3 * time.Second)           // p1 停滞 3s
	tracker2.Observe(0, 20_000, c2.Now()) // 快分区 p0 此刻仍活跃，wm 18000
	_, newWM2, adv2, _ := tracker2.TickIdle(c2.Now())
	// p1 被标记空闲后只剩 p0 活跃 -> 推进到 18000
	if !adv2 || newWM2 != 18_000 {
		t.Fatalf("stuck p1 idle should let watermark reach 18000, got %d adv=%v", newWM2, adv2)
	}
	// p1 恢复活动（迟到数据）：水位线不回退
	tracker2.Observe(1, 6_000, c2.Now())
	if tracker2.Watermark() != 18_000 {
		t.Fatalf("watermark must not regress after resume, got %d", tracker2.Watermark())
	}
	if tracker2.Idle(1) {
		t.Fatal("resumed partition must not be idle")
	}
}

func TestWatermarkCompleteAll(t *testing.T) {
	t0 := time.Date(2026, 9, 24, 9, 0, 0, 0, time.UTC)
	tracker := NewWatermarkTracker([]int{0, 1}, 2_000, time.Second)
	tracker.Observe(0, 10_000, t0)
	tracker.Observe(1, 9_000, t0)

	if _, _, adv := tracker.Complete(0); adv {
		t.Fatal("completing one of two partitions must not jump watermark")
	}
	old, new, adv := tracker.Complete(1)
	if !adv || new != MaxWatermark {
		t.Fatalf("all complete should reach MaxWatermark: old=%d new=%d adv=%v", old, new, adv)
	}
}
