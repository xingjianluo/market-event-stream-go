package marketstream

import (
	"context"
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"
)

// fakeClock 是可手动推进的虚拟处理时钟，用于确定性驱动空闲超时。
type fakeClock struct {
	mu sync.Mutex
	t  time.Time
}

func newFakeClock() *fakeClock {
	return &fakeClock{t: time.Date(2026, 9, 24, 9, 0, 0, 0, time.UTC)}
}

func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.t
}

func (c *fakeClock) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.t = c.t.Add(d)
}

// testLogger 打印水位线、事件时间、判定依据到测试日志（go test -v 可见）。
func testLogger(t *testing.T) *slog.Logger {
	t.Helper()
	return slog.New(slog.NewTextHandler(&testWriter{t: t}, &slog.HandlerOptions{
		Level: slog.LevelDebug,
	}))
}

type testWriter struct{ t *testing.T }

func (w *testWriter) Write(p []byte) (int, error) {
	w.t.Logf("%s", p)
	return len(p), nil
}

// discardLogger 用于不需要日志的用例。
func discardLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

// mkEvent 构造一条行情事件。
func mkEvent(part int, off int64, symbol string, ts int64, price float64, qty int64) Event {
	return Event{
		ID:        EventID{Partition: part, Offset: off},
		Symbol:    symbol,
		EventTime: ts,
		Price:     price,
		Qty:       qty,
	}
}

// testConfig 是确定性测试配置；可在此基础上覆写字段。
func testConfig(t *testing.T) Config {
	cfg := DefaultConfig()
	cfg.Logger = testLogger(t)
	cfg.IdleTimeout = 0 // 默认关闭墙钟空闲，用例按需开启虚拟时钟
	return cfg
}

// collector 后台收集输出通道，避免处理循环因通道阻塞；同时提供快照断言。
type collector struct {
	mu         sync.Mutex
	results    []WindowResult
	rejections []LateRejection
	verdicts   []Verdict
	wg         sync.WaitGroup
}

func newCollector(p *Processor) *collector {
	c := &collector{}
	c.wg.Add(3)
	go func() {
		defer c.wg.Done()
		for r := range p.Results() {
			c.mu.Lock()
			c.results = append(c.results, r)
			c.mu.Unlock()
		}
	}()
	go func() {
		defer c.wg.Done()
		for r := range p.Rejections() {
			c.mu.Lock()
			c.rejections = append(c.rejections, r)
			c.mu.Unlock()
		}
	}()
	go func() {
		defer c.wg.Done()
		for v := range p.Verdicts() {
			c.mu.Lock()
			c.verdicts = append(c.verdicts, v)
			c.mu.Unlock()
		}
	}()
	return c
}

func (c *collector) Results() []WindowResult {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]WindowResult(nil), c.results...)
}

func (c *collector) Rejections() []LateRejection {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]LateRejection(nil), c.rejections...)
}

func (c *collector) Verdicts() []Verdict {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]Verdict(nil), c.verdicts...)
}

// finalResults 返回每个窗口的最后一次结果（含 UPDATE 覆盖 FINAL 的场景）。
func (c *collector) finalResults() map[WindowKey]WindowResult {
	out := make(map[WindowKey]WindowResult)
	for _, r := range c.Results() {
		out[r.Window] = r
	}
	return out
}

// waitForResults 轮询等待收集到至少 n 条窗口结果，超时即失败。
func waitForResults(t *testing.T, c *collector, n int) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		c.mu.Lock()
		got := len(c.results)
		c.mu.Unlock()
		if got >= n {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("waitForResults timeout: got %d, want %d", len(c.Results()), n)
}

// waitForVerdicts 轮询等待收集到至少 n 条判定。
func waitForVerdicts(t *testing.T, c *collector, n int) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		c.mu.Lock()
		got := len(c.verdicts)
		c.mu.Unlock()
		if got >= n {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("waitForVerdicts timeout: got %d, want %d", len(c.Verdicts()), n)
}

// waitForRejections 轮询等待收集到至少 n 条侧输出，超时即失败。
func waitForRejections(t *testing.T, c *collector, n int) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		c.mu.Lock()
		got := len(c.rejections)
		c.mu.Unlock()
		if got >= n {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("waitForRejections timeout: got %d, want %d", len(c.Rejections()), n)
}

// runProcessor 启动处理器并收集输出，返回清理函数（屏障+关闭+等待收集结束）。
func runProcessor(ctx context.Context, t *testing.T, cfg Config, parts []int) (*Processor, *collector, func()) {
	t.Helper()
	p, err := NewProcessor(cfg, parts)
	if err != nil {
		t.Fatalf("NewProcessor: %v", err)
	}
	// 测试以虚拟时钟/显式 TickIdleNow 驱动空闲，关闭墙钟自动 ticker 避免竞态。
	p.disableIdleTick = true
	col := newCollector(p)
	if err := p.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}
	cleanup := func() {
		_ = p.Close()
		p.Wait()
		// 处理循环退出后不会再有输出；关闭通道由 GC 负责，collector 无需显式等待。
	}
	return p, col, cleanup
}
