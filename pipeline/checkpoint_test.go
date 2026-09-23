package pipeline

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// interruptibleLogs：第一个栅栏点（A 偏移3）时只有早期事件 + 重复，
// 尚无任何越过窗口结束时间的标记，故该点 W1 必然仍 open、未触发未清理。
func interruptibleLogs() []*PartitionLog {
	a := NewPartitionLog("A")
	a.AddEvent(1, "BTC", 1_000, 100_000_000, 2)
	a.AddEvent(2, "BTC", 3_000, 102_000_000, 1)
	a.AddEvent(2, "BTC", 3_000, 102_000_000, 1) // DUPLICATE
	a.AddBarrier()                              // 故障注入点
	a.AddWatermark(12_000)
	a.AddEnd()

	b := NewPartitionLog("B")
	b.AddEvent(10, "BTC", 1_500, 98_000_000, 4)
	b.AddBarrier()
	b.AddWatermark(12_000)
	b.AddEvent(11, "BTC", 9_000, 99_000_000, 3)
	b.AddWatermark(20_000)
	b.AddBarrier()
	b.AddEvent(12, "BTC", 1_000, 10_000_000, 7)
	b.AddEnd()

	c := NewPartitionLog("C")
	c.AddIdle(0)
	c.AddEnd()
	return []*PartitionLog{a, b, c}
}

var errInjectedCrash = errors.New("注入故障：模拟进程崩溃")

// TestCheckpointRestartReplay：在第一个栅栏点同步检查点并注入崩溃，
// 随后从检查点重启跑完剩余日志，最终聚合与无故障运行一致，重放不重复计入。
func TestCheckpointRestartReplay(t *testing.T) {
	dir := t.TempDir()

	// 无故障基准
	base := runPipeline(t, testCfg(), interruptibleLogs(), testLogger(t))

	// 故障运行：第一个栅栏点检查点 -> 返回注入错误使管线停止
	crashCfg := testCfg()
	crashCfg.CheckpointDir = dir
	crashed := New(crashCfg, interruptibleLogs(), testLogger(t))
	crashed.cfg.OnBarrier = func(pid PartitionID, off int64, snap *snapshot) error {
		if len(snap.Windows) == 0 {
			t.Fatalf("故障点窗口应仍 open，快照窗口数=0")
		}
		t.Logf("故障点检查点已落盘: 全局水位线=%v 窗口数=%d 偏移=%v",
			snap.WM.Global, len(snap.Windows), snap.Offsets)
		return errInjectedCrash
	}
	err := crashed.Run(context.Background())
	if !errors.Is(err, errInjectedCrash) {
		t.Fatalf("期望注入故障错误，得到 %v", err)
	}
	// 崩溃路径输出通道已关闭，排空已产生的结果
	for range crashed.Results() {
	}
	for range crashed.SideOutput() {
	}
	if _, statErr := os.Stat(filepath.Join(dir, "checkpoint.json")); statErr != nil {
		t.Fatalf("检查点文件未生成: %v", statErr)
	}

	// 从检查点重启：偏移恢复，栅栏前已处理事件若被重放将由去重表拦截
	restartCfg := testCfg()
	restartCfg.CheckpointDir = dir
	restarted := runPipeline(t, restartCfg, interruptibleLogs(), testLogger(t))

	rb := restarted.finalOf()
	bb := base.finalOf()
	if len(rb) != len(bb) {
		t.Fatalf("重启后窗口数不一致: %d vs %d", len(rb), len(bb))
	}
	if rb[0].EventCount != bb[0].EventCount ||
		rb[0].TotalQty != bb[0].TotalQty ||
		rb[0].VwapPriceMicros != bb[0].VwapPriceMicros {
		t.Fatalf("重启后最终聚合与无故障不一致:\n重启=%+v\n基准=%+v", rb[0], bb[0])
	}
	if rb[0].EventCount != 4 {
		t.Errorf("重放导致重复计入，期望最终 4 条，得到 %d", rb[0].EventCount)
	}
	// 超范围迟到事件重启后仍必须被明确丢弃
	if d, ok := restarted.sideByID(EventID{"B", 12}); !ok || d.Disposition != DispositionLateDropped {
		t.Errorf("重启后 (B,12) 仍应 LATE_DROPPED，得到 ok=%v", ok)
	}
	t.Logf("重启后统计: %+v", restarted.stats)
}

// TestRestartFromCompletedCheckpoint：正常跑完后，最后一次栅栏检查点处于
// "窗口已清理、全部偏移已提交"状态。从它再启动应幂等收尾：
// 不重新触发已清理窗口、不重复计数、不报错。
func TestRestartFromCompletedCheckpoint(t *testing.T) {
	dir := t.TempDir()
	cfg := testCfg()
	cfg.CheckpointDir = dir
	first := runPipeline(t, cfg, interruptibleLogs(), testLogger(t))
	if len(first.finalOf()) != 1 {
		t.Fatalf("首次运行应有一个窗口，得到 %d", len(first.finalOf()))
	}

	again := runPipeline(t, cfg, interruptibleLogs(), testLogger(t))
	if len(again.results) != 0 {
		t.Fatalf("从末尾检查点幂等重启不应重新输出窗口帧，得到 %d 帧: %+v",
			len(again.results), again.results)
	}
	t.Logf("幂等重启统计（应无新增计入）: %+v", again.stats)
}

// TestPeriodicCheckpoint：配置很短的周期检查点，跑足够多事件使墙上时间
// 跨越周期，断言检查点文件被写出且管线结果正常。
func TestPeriodicCheckpoint(t *testing.T) {
	dir := t.TempDir()
	cfg := testCfg()
	cfg.CheckpointDir = dir
	cfg.CheckpointInterval = 5 * time.Millisecond

	a := NewPartitionLog("A")
	// 大量事件 + 慢一点的投递（小批），让运行时间跨越多个 5ms tick
	for i := 0; i < 3000; i++ {
		a.AddEvent(int64(i), "S", int64(i*5+100), 100, 1)
	}
	a.AddWatermark(int64(3000*5 + 10000))
	a.AddEnd()
	b := NewPartitionLog("B")
	b.AddIdle(0)
	b.AddEnd()

	c := runPipeline(t, cfg, []*PartitionLog{a, b}, nil)
	if c.stats.EventsAccepted != 3000 {
		t.Fatalf("事件应全部计入，得到 %d", c.stats.EventsAccepted)
	}
	if c.stats.CheckpointsCompleted == 0 {
		t.Fatalf("应至少完成一次周期检查点")
	}
	if _, err := os.Stat(filepath.Join(dir, "checkpoint.json")); err != nil {
		t.Fatalf("周期检查点文件未生成: %v", err)
	}
	t.Logf("周期检查点完成次数=%d", c.stats.CheckpointsCompleted)
}
