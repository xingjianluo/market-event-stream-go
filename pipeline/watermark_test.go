package pipeline

import (
	"testing"
	"time"
)

func ms(ms int64) time.Time { return time.UnixMilli(ms).UTC() }

func TestWatermarkGlobalMinOfActivePartitions(t *testing.T) {
	tracker := newWatermarkTracker()
	tracker.register("A")
	tracker.register("B")

	// 两个分区都未标记（-inf）：全局水位线不能推进
	if _, moved := tracker.advance(); moved {
		t.Fatalf("无任何标记时全局水位线不应推进")
	}

	tracker.observeMark("A", ms(5_000))
	if _, moved := tracker.advance(); moved {
		t.Fatalf("B 未标记时全局水位线应被 -inf 阻塞")
	}

	tracker.observeMark("B", ms(3_000))
	if g, moved := tracker.advance(); !moved || !g.Equal(ms(3_000)) {
		t.Fatalf("全局水位线应为 min(5s,3s)=3s，得到 %v moved=%v", g, moved)
	}
	tracker.observeMark("A", ms(8_000))
	if g, _ := tracker.advance(); !g.Equal(ms(3_000)) {
		t.Fatalf("A 推进不应越过慢分区 B，期望 3s 得到 %v", g)
	}
}

func TestWatermarkMonotonicAndIdle(t *testing.T) {
	tracker := newWatermarkTracker()
	tracker.register("A")
	tracker.register("B")
	tracker.observeMark("A", ms(10_000))
	tracker.observeMark("B", ms(4_000))
	if g, _ := tracker.advance(); !g.Equal(ms(4_000)) {
		t.Fatalf("期望 4s，得到 %v", g)
	}

	// 回退标记必须被拒绝（分区水位线单调）
	if _, ok := tracker.observeMark("B", ms(2_000)); ok {
		t.Fatalf("回退的水位线标记应被拒绝")
	}

	// B 停滞：全局水位线由 A 接管，不被停滞分区无限阻塞
	tracker.markIdle("B")
	if g, moved := tracker.advance(); !moved || !g.Equal(ms(10_000)) {
		t.Fatalf("B 停滞后全局水位线应为 10s，得到 %v moved=%v", g, moved)
	}

	// B 恢复并声明 12s：但 A 仍为 10s，全局 min 保持 10s，
	// 不会因 B 曾经停滞而越过 A 的真实进度（不能过早关窗）。
	tracker.observeMark("B", ms(12_000))
	if g, moved := tracker.advance(); moved || !g.Equal(ms(10_000)) {
		t.Fatalf("B 恢复到 12s 而 A 为 10s 时全局应保持 10s，得到 %v moved=%v", g, moved)
	}
	// A 跟进到 12s 后全局才推进到 12s
	tracker.observeMark("A", ms(12_000))
	if g, moved := tracker.advance(); !moved || !g.Equal(ms(12_000)) {
		t.Fatalf("A 跟进后全局水位线应为 12s，得到 %v moved=%v", g, moved)
	}

	// 全局水位线单调：所有分区再停滞也不回退
	tracker.markIdle("A")
	tracker.markIdle("B")
	if g, moved := tracker.advance(); moved || !g.Equal(ms(12_000)) {
		t.Fatalf("全停滞后全局水位线应保持 12s 不回退，得到 %v moved=%v", g, moved)
	}
}

func TestWatermarkFinishedPartitionExitsMin(t *testing.T) {
	tracker := newWatermarkTracker()
	tracker.register("A")
	tracker.register("B")
	tracker.observeMark("A", ms(9_000))
	tracker.observeMark("B", ms(6_000))
	if g, _ := tracker.advance(); !g.Equal(ms(6_000)) {
		t.Fatalf("期望初始全局 6s，得到 %v", g)
	}
	tracker.markEnd("B") // B 物理结束，退出最小值
	if g, moved := tracker.advance(); !moved || !g.Equal(ms(9_000)) {
		t.Fatalf("结束分区应退出最小值，期望 9s 得到 %v moved=%v", g, moved)
	}
}
