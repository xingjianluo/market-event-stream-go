package pipeline

import (
	"context"
	"testing"
)

// TestLateBoundaryExact：精确验证水位线边界处分（W=[0,10s)，allowed=5s）：
//   - gw==end(10s)      ：事件时间<10s 属 late-in-allowed（含边界 gw==end）
//   - gw==end+allowed(15s)：事件被丢弃（gw>=cutoff 即清理）
//   - on-time 事件（gw<end 时到达）正常计入
func TestLateBoundaryExact(t *testing.T) {
	// 事件 9s 在 gw=10s 时到达 => late-in-allowed
	a := NewPartitionLog("P")
	a.AddEvent(1, "S", 9_000, 100, 1)
	a.AddWatermark(10_000) // gw==end
	a.AddBarrier()
	a.AddEvent(2, "S", 1_000, 100, 1) // 上一批 gw=10s => late-in-allowed
	a.AddWatermark(15_000)            // gw==cutoff
	a.AddBarrier()
	a.AddEvent(3, "S", 1_000, 100, 1) // gw>=15s => late-dropped
	a.AddEnd()
	c := runPipeline(t, testCfg(), []*PartitionLog{a}, testLogger(t))

	if d, ok := c.sideByID(EventID{"P", 2}); !ok || d.Disposition != DispositionLateInAllowed {
		t.Errorf("gw==end 时 1s 事件应 LATE_IN_ALLOWED，得到 %v", d.Disposition)
	}
	if d, ok := c.sideByID(EventID{"P", 3}); !ok || d.Disposition != DispositionLateDropped {
		t.Errorf("gw==end+allowed 时应 LATE_DROPPED，得到 %v", d.Disposition)
	}
	// 事件1、2 计入，事件3 丢弃 => 最终窗口 2 条
	w := c.finalOf()[0]
	if w.EventCount != 2 {
		t.Errorf("最终窗口应有 2 条（事件3 丢弃），得到 %d", w.EventCount)
	}
}

// TestMultipleSymbolsAndWindows：多品种、多滚动窗口各自独立聚合、独立触发。
func TestMultipleSymbolsAndWindows(t *testing.T) {
	a := NewPartitionLog("P")
	a.AddEvent(1, "BTC", 1_000, 100, 2)  // BTC W0
	a.AddEvent(2, "ETH", 2_000, 200, 3)  // ETH W0
	a.AddEvent(3, "BTC", 11_000, 300, 4) // BTC W1
	a.AddEvent(4, "ETH", 12_000, 400, 5) // ETH W1
	a.AddWatermark(30_000)
	a.AddEnd()
	c := runPipeline(t, testCfg(), []*PartitionLog{a}, testLogger(t))
	finals := c.finalOf()
	if len(finals) != 4 {
		t.Fatalf("应有 4 个窗口（2品种 x 2窗口），得到 %d", len(finals))
	}
	for _, w := range finals {
		if w.EventCount != 1 {
			t.Errorf("窗口 %s@%d 计数应为 1，得到 %d", w.Symbol, w.WindowStart.UnixMilli(), w.EventCount)
		}
		// 单事件窗口 VWAP==其价格
		var expectPrice int64
		switch w.Symbol {
		case "BTC":
			if w.WindowStart.UnixMilli() == 0 {
				expectPrice = 100
			} else {
				expectPrice = 300
			}
		case "ETH":
			if w.WindowStart.UnixMilli() == 0 {
				expectPrice = 200
			} else {
				expectPrice = 400
			}
		}
		if w.VwapPriceMicros != expectPrice {
			t.Errorf("窗口 %s@%d VWAP 应为 %d，得到 %d",
				w.Symbol, w.WindowStart.UnixMilli(), expectPrice, w.VwapPriceMicros)
		}
	}
}

// TestEmptyAndIdleOnly：无事件、仅停滞分区，管线正常结束、无结果、无死锁。
func TestEmptyAndIdleOnly(t *testing.T) {
	a := NewPartitionLog("A")
	a.AddIdle(0)
	a.AddEnd()
	b := NewPartitionLog("B")
	b.AddIdle(0)
	b.AddEnd()
	c := runPipeline(t, testCfg(), []*PartitionLog{a, b}, testLogger(t))
	if len(c.results) != 0 || c.stats.EventsAccepted != 0 {
		t.Fatalf("空输入不应产生结果，得到 results=%d accepted=%d",
			len(c.results), c.stats.EventsAccepted)
	}
}

// TestInvalidConfigRejected：非法配置必须被 Run 拒绝。
func TestInvalidConfigRejected(t *testing.T) {
	cases := []Config{testCfg(), testCfg(), testCfg()}
	cases[0].WindowSize = 0
	cases[1].InputQueueCap = 0
	cases[2].MergeBatchSize = 0
	for i, cfg := range cases {
		if err := New(cfg, []*PartitionLog{NewPartitionLog("X")}, nil).Run(context.Background()); err == nil {
			t.Errorf("非法配置 case %d 应返回错误", i)
		}
	}
}
