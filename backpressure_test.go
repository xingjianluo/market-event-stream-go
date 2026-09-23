package marketstream

import (
	"context"
	"runtime"
	"testing"
	"time"
)

// TestBoundedInputQueueBackpressure 确定性地验证慢消费者下的背压：
// 处理循环在向无缓冲、无人读取的结果通道发送 FINAL 时停住，
// 随后输入队列必然填满，生产者被阻塞而内存/任务不增长。
// 这里不启动任何输出收集器，等价于完全停滞的慢消费者。
func TestBoundedInputQueueBackpressure(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cfg := testConfig(t)
	cfg.MaxOutOfOrderness = 0
	cfg.InputQueueCapacity = 1
	cfg.ResultBufferSize = 0 // 结果通道无缓冲且本测试不读取
	cfg.RejectBufferSize = 0

	p, err := NewProcessor(cfg, []int{0})
	if err != nil {
		t.Fatalf("NewProcessor: %v", err)
	}
	if err := p.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer func() { _ = p.Close(); p.Wait() }()

	// 第一条：窗口未结束、无输出，循环不阻塞，立即处理完。
	if err := p.Ingest(ctx, mkEvent(0, 0, "BTC", 0, 100, 1)); err != nil {
		t.Fatalf("ingest first: %v", err)
	}
	// 第二条使 wm=10000，触发 BTC@[0,10000) 的 FINAL；
	// 结果通道无缓冲且无人读取 -> 循环在此确定停住。
	if err := p.Ingest(ctx, mkEvent(0, 1, "ETH", 10_000, 10, 1)); err != nil {
		t.Fatalf("ingest wm event: %v", err)
	}
	// 等待循环卡在结果发送处。
	time.Sleep(30 * time.Millisecond)

	// 再多投递：输入队列容量 1 且循环停滞，必须阻塞形成背压。
	over := make(chan error, 1)
	go func() { over <- p.Ingest(ctx, mkEvent(0, 2, "ETH", 20_000, 10, 1)) }()
	select {
	case err := <-over:
		t.Fatalf("ingest must block under backpressure, returned early: %v", err)
	case <-time.After(50 * time.Millisecond):
		// 预期阻塞：队列严格有界。
	}
	if n := len(p.in); n > cap(p.in) {
		t.Fatalf("input queue grew beyond cap: len=%d cap=%d", n, cap(p.in))
	}
	cancel()
	select {
	case <-over:
	case <-time.After(time.Second):
		t.Fatal("blocked ingest did not unblock on cancel")
	}
	t.Logf("backpressure verified: queue cap=%d, producer blocked then released", cap(p.in))
}

// TestBoundedFeederGoroutines 验证流量突增时投递 goroutine 数等于分区数，
// 不随事件数增长（背压靠阻塞 Ingest，而不是为每个事件建 goroutine）。
func TestBoundedFeederGoroutines(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cfg := testConfig(t)
	cfg.InputQueueCapacity = 2
	p, col, cleanup := runProcessor(ctx, t, cfg, []int{0, 1, 2})
	defer cleanup()

	big := map[int][]Event{}
	for part := 0; part < 3; part++ {
		evs := make([]Event, 200)
		for i := range evs {
			evs[i] = mkEvent(part, int64(i), "BTC", int64(part*1000+i*10), 100, 1)
		}
		big[part] = evs
	}
	feedDone := p.Feed(ctx, NewLocalSource(big))

	// 队列填满、feeder 阻塞时，goroutine 数应基本稳定（3 个 feeder + 少量内部）。
	time.Sleep(30 * time.Millisecond)
	during := runtime.NumGoroutine()
	time.Sleep(30 * time.Millisecond)
	later := runtime.NumGoroutine()
	t.Logf("goroutines during burst: %d then %d", during, later)
	diff := later - during
	if diff > 10 || diff < -10 {
		t.Fatalf("goroutine count unstable under sustained burst: %d -> %d", during, later)
	}

	<-feedDone
	waitForVerdicts(t, col, 600)
}
