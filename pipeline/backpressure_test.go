package pipeline

import (
	"context"
	"runtime"
	"sync/atomic"
	"testing"
	"time"
)

// bigLogs 构造大量事件与窗口，用于验证慢消费者下的背压与有界完成。
func bigLogs(nEvents int) []*PartitionLog {
	a := NewPartitionLog("A")
	b := NewPartitionLog("B")
	for i := 0; i < nEvents; i++ {
		et := int64(i*100 + 100) // 跨多个 10s 窗口
		a.AddEvent(int64(i), "S", et, 100, int64(1+(i%5)))
		if i%7 == 0 {
			// 周期性推进水位线，触发窗口
			a.AddWatermark(et + 5_000)
		}
	}
	a.AddWatermark(int64(nEvents)*100 + 20_000)
	a.AddEnd()
	b.AddIdle(0)
	b.AddEnd()
	return []*PartitionLog{a, b}
}

// TestBoundedBackpressureSlowConsumer：输出通道极小、消费者故意很慢，
// 管线必须靠有界队列端到端反压并最终完成，而不是 OOM 或 goroutine 泄漏。
func TestBoundedBackpressureSlowConsumer(t *testing.T) {
	cfg := testCfg()
	cfg.InputQueueCap = 2
	cfg.OutputQueueCap = 1
	cfg.MergeBatchSize = 1

	const n = 500
	p := New(cfg, bigLogs(n), nil)
	ctx := context.Background()
	runDone := make(chan error, 1)
	go func() { runDone <- p.Run(ctx) }()

	var frames int64
	resDone := make(chan struct{})
	sideDone := make(chan struct{})
	// 结果与侧输出必须由两个 goroutine 并发消费：任一有界输出队列满都会
	// 反压到聚合器，单消费者顺序 drain 会自死锁（真实使用方也应并发消费）。
	go func() {
		defer close(resDone)
		for range p.Results() {
			atomic.AddInt64(&frames, 1)
			runtime.Gosched() // 故意很慢，制造背压
		}
	}()
	go func() {
		defer close(sideDone)
		for range p.SideOutput() {
		}
	}()
	consumeDone := func() { <-resDone; <-sideDone }

	select {
	case err := <-runDone:
		if err != nil {
			t.Fatalf("慢消费者下管线异常: %v", err)
		}
	case <-time.After(15 * time.Second):
		t.Fatalf("慢消费者下管线未能在限时内完成（疑似背压死锁/无界增长）")
	}
	consumeDone()
	if atomic.LoadInt64(&frames) == 0 {
		t.Fatalf("应至少输出一个窗口结果帧")
	}
	t.Logf("慢消费者消费结果帧=%d 统计=%+v", frames, p.Stats())
}

// TestBoundedGoroutines：运行前后 goroutine 数不应随事件量无界增长
// （源协调器与聚合器各一个，固定拓扑）。
func TestBoundedGoroutines(t *testing.T) {
	before := runtime.NumGoroutine()
	cfg := testCfg()
	c := runPipeline(t, cfg, bigLogs(300), nil)
	if c.stats.EventsAccepted < 300 {
		t.Fatalf("事件应全部计入，得到 %d", c.stats.EventsAccepted)
	}
	// 允许其它测试残留少量 goroutine，但不能随事件量增长；这里给宽松上界。
	after := runtime.NumGoroutine()
	if after > before+5 {
		t.Fatalf("goroutine 数疑似泄漏: before=%d after=%d", before, after)
	}
	t.Logf("goroutine before=%d after=%d", before, after)
}
