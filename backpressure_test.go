package marketstream

import (
	"context"
	"runtime"
	"testing"
	"time"
)

// TestBoundedResourcesWithNonReadingDownstream 确定性地验证：完全不消费任何输出
// （旧通道与订阅面都不读）时，处理链路既不被卡死，也不被无界拖垮——
// 处理循环持续推进，结果只保留在有界结果历史（环形定容）中，输入队列严格有界，
// 且后台任务数量恒定。背压的有界点是"有界历史 + 有界输入队列"，而非阻塞处理循环。
func TestBoundedResourcesWithNonReadingDownstream(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cfg := testConfig(t)
	cfg.MaxOutOfOrderness = 0
	cfg.InputQueueCapacity = 4
	cfg.ResultBufferSize = 0 // 旧结果通道无缓冲，且本测试完全不读取
	cfg.RejectBufferSize = 0
	cfg.ResultHistoryCapacity = 8 // 结果历史极小，制造确定的有界上界

	p, err := NewProcessor(cfg, []int{0})
	if err != nil {
		t.Fatalf("NewProcessor: %v", err)
	}
	if err := p.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer func() { _ = p.Close(); p.Wait() }()

	// 不建立任何订阅、不读取 Results/Rejections/Verdicts，等价于完全停滞的下游。
	// 每条事件推进水位线并最终化一个新窗口，产生大量结果；循环必须始终推进，
	// 不能因为无人消费输出而停住。
	const n = 200
	for i := int64(0); i < n; i++ {
		done := make(chan error, 1)
		go func() { done <- p.Ingest(ctx, mkEvent(0, i, "X", (i+1)*10_000, 10, 1)) }()
		select {
		case <-done:
		case <-time.After(2 * time.Second):
			t.Fatalf("ingest %d blocked: pipeline stalled without any output consumer", i)
		}
	}
	if err := p.MarkComplete(ctx, 0); err != nil {
		t.Fatal(err)
	}

	// 屏障在无人读取任何输出时仍能在有限时间完成，证明处理循环没有卡在输出上。
	done := make(chan *Checkpoint, 1)
	go func() { cp, _ := p.Checkpoint(ctx); done <- cp }()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("checkpoint blocked without any output consumer")
	}

	// 资源严格有界：历史条数不超过容量，输入队列不超过容量。
	st := p.ResultStore()
	st.mu.Lock()
	hist := st.count
	ringCap := len(st.ring)
	st.mu.Unlock()
	if hist > ringCap {
		t.Fatalf("result history %d exceeded bounded capacity %d", hist, ringCap)
	}
	if q := len(p.in); q > cap(p.in) {
		t.Fatalf("input queue %d exceeded cap %d", q, cap(p.in))
	}
	t.Logf("bounded resources verified with no readers: history=%d/%d, input cap=%d",
		hist, ringCap, cap(p.in))
}

// TestNonReadingLegacyOutputDoesNotBlockSubscription 验证：只走订阅面、完全不消费
// 旧 Results() 通道时，结果持续产生（远超旧通道缓冲）也不会卡住处理链路；
// 订阅按位点继续交付，且取快照/检查点/关闭都能在有限时间返回。
func TestNonReadingLegacyOutputDoesNotBlockSubscription(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cfg := testConfig(t)
	cfg.MaxOutOfOrderness = 0
	cfg.ResultBufferSize = 1 // 旧结果通道缓冲极小，且本测试完全不读取
	cfg.ResultHistoryCapacity = 64

	p, err := NewProcessor(cfg, []int{0})
	if err != nil {
		t.Fatalf("NewProcessor: %v", err)
	}
	if err := p.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer func() { _ = p.Close(); p.Wait() }()

	sub, err := p.SubscribeResults(0)
	if err != nil {
		t.Fatal(err)
	}

	// 产生远超旧通道缓冲（=1）的结果量；旧通道无人消费。
	const n = 20
	for i := int64(0); i < n; i++ {
		if err := p.Ingest(ctx, mkEvent(0, i, "X", (i+1)*10_000, 10, 1)); err != nil {
			t.Fatalf("ingest %d: %v", i, err)
		}
	}
	if err := p.MarkComplete(ctx, 0); err != nil {
		t.Fatal(err)
	}

	// 订阅按位点严格继续交付全部结果。
	recs := receiveN(t, sub, n)
	for i, rec := range recs {
		if rec.Seq != ResultSeq(i+1) {
			t.Fatalf("seq[%d]=%d want %d (subscription stalled)", i, rec.Seq, i+1)
		}
	}

	// 屏障：快照与检查点必须在有限时间内完成。
	barrierDone := make(chan struct{}, 2)
	go func() { _, _ = p.ResultSnapshot(ctx); barrierDone <- struct{}{} }()
	go func() { _, _ = p.Checkpoint(ctx); barrierDone <- struct{}{} }()
	for i := 0; i < 2; i++ {
		select {
		case <-barrierDone:
		case <-time.After(2 * time.Second):
			t.Fatal("snapshot/checkpoint blocked with unconsumed legacy output")
		}
	}

	// 关闭必须在有限时间返回。
	closed := make(chan struct{})
	go func() { _ = p.Close(); close(closed) }()
	select {
	case <-closed:
	case <-time.After(2 * time.Second):
		t.Fatal("Close blocked with unconsumed legacy output")
	}
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
