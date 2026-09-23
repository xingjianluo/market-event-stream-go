package pipeline

import (
	"testing"
)

// canonicalLogs 构造覆盖全部判定的三分区日志（窗口10s、允许迟到5s）。
// 使用显式栅栏固定脉冲边界，使每条事件的处分与 MergeBatchSize 无关。
func canonicalLogs() []*PartitionLog {
	a := NewPartitionLog("A")
	a.AddEvent(1, "BTC", 1_000, 100_000_000, 2)
	a.AddEvent(2, "BTC", 3_000, 102_000_000, 1)
	a.AddWatermark(5_000)
	a.AddEvent(2, "BTC", 3_000, 102_000_000, 1) // 重复投递 -> DUPLICATE
	a.AddEnd()

	b := NewPartitionLog("B")
	b.AddEvent(10, "BTC", 1_500, 98_000_000, 4) // 跨分区事件
	b.AddWatermark(12_000)                      // 越过 W1 end=10s
	b.AddBarrier()                              // 边界：W1 首帧确定触发
	b.AddEvent(11, "BTC", 9_000, 99_000_000, 3) // 迟到但允许 -> 修正帧
	b.AddWatermark(20_000)                      // 越过清理线 15s
	b.AddBarrier()                              // 边界：W1 确定清理
	b.AddEvent(12, "BTC", 1_000, 10_000_000, 7) // 超范围迟到 -> LATE_DROPPED
	b.AddEnd()

	c := NewPartitionLog("C")
	c.AddIdle(0) // 全程停滞，不阻塞窗口
	c.AddEnd()
	return []*PartitionLog{a, b, c}
}

func TestCanonicalScenario(t *testing.T) {
	c := runPipeline(t, testCfg(), canonicalLogs(), testLogger(t))

	// 最终窗口：事件 1,2,10 首帧(3条)，事件11 迟到修正(4条)；
	// 事件12 超范围丢弃不计入；重复的 (A,2) 只算一次。
	final := c.finalOf()
	if len(final) != 1 {
		t.Fatalf("应只有一个窗口，得到 %d: %+v", len(final), final)
	}
	w := final[0]
	if w.EventCount != 4 {
		t.Errorf("窗口最终事件数应为 4（事件1/2/10/11），得到 %d", w.EventCount)
	}
	if w.TotalQty != 10 { // 2+1+4+3
		t.Errorf("窗口总量应为 10，得到 %d", w.TotalQty)
	}
	// Σpq = 100m*2 + 102m*1 + 98m*4 + 99m*3
	//      = 200m + 102m + 392m + 297m = 991m；/10 = 99,100,000
	if w.VwapPriceMicros != 99_100_000 {
		t.Errorf("VWAP 应为 99100000，得到 %d", w.VwapPriceMicros)
	}
	if w.Revised != true {
		t.Errorf("最终帧应为迟到修正帧")
	}

	// 侧输出：三类可区分的处分
	if dup, ok := c.sideByID(EventID{"A", 2}); !ok || dup.Disposition != DispositionDuplicate {
		t.Errorf("(A,2) 应判 DUPLICATE，得到 ok=%v disp=%v", ok, dup.Disposition)
	}
	if late, ok := c.sideByID(EventID{"B", 11}); !ok || late.Disposition != DispositionLateInAllowed {
		t.Errorf("(B,11) 应判 LATE_IN_ALLOWED，得到 ok=%v disp=%v", ok, late.Disposition)
	}
	if drop, ok := c.sideByID(EventID{"B", 12}); !ok || drop.Disposition != DispositionLateDropped {
		t.Errorf("(B,12) 应判 LATE_DROPPED，得到 ok=%v disp=%v", ok, drop.Disposition)
	}

	// 统计
	if c.stats.Duplicates != 1 || c.stats.LateInAllowed != 1 || c.stats.LateDropped != 1 {
		t.Errorf("统计计数不符: %+v", c.stats)
	}
	if c.stats.EventsAccepted != 4 {
		t.Errorf("计入事件应为 4，得到 %d", c.stats.EventsAccepted)
	}
	t.Logf("最终窗口: %+v", w)
	t.Logf("统计: %+v", c.stats)
}

// TestOutOfOrderWithinPartition：分区内事件乱序（后到的事件时间更晚先投递），
// 只要在水位线标记之前到达，就正常计入且结果与顺序无关。
func TestOutOfOrderWithinPartition(t *testing.T) {
	a := NewPartitionLog("A")
	// 先投 8s 再投 1s（同窗口内乱序），水位线标记在最后
	a.AddEvent(1, "X", 8_000, 100, 10)
	a.AddEvent(2, "X", 1_000, 200, 10)
	a.AddWatermark(12_000)
	a.AddEnd()
	c := runPipeline(t, testCfg(), []*PartitionLog{a}, testLogger(t))
	w := c.finalOf()[0]
	if w.EventCount != 2 || w.TotalQty != 20 {
		t.Fatalf("乱序事件都应计入，得到 count=%d qty=%d", w.EventCount, w.TotalQty)
	}
	if w.VwapPriceMicros != 150 { // (100*10+200*10)/20
		t.Errorf("VWAP 应为 150，得到 %d", w.VwapPriceMicros)
	}
}

// TestDuplicateNotDoubleCounted：同一 EventID 多次投递只计一次，
// 即使它出现在窗口首帧前后或来自检查点重放。
func TestDuplicateNotDoubleCounted(t *testing.T) {
	a := NewPartitionLog("A")
	a.AddEvent(1, "X", 1_000, 100, 5)
	a.AddEvent(1, "X", 1_000, 100, 5) // 完全相同的重复投递
	a.AddEvent(1, "X", 1_000, 100, 5) // 再来一次
	a.AddWatermark(12_000)
	a.AddEnd()
	c := runPipeline(t, testCfg(), []*PartitionLog{a}, testLogger(t))
	w := c.finalOf()[0]
	if w.EventCount != 1 || w.TotalQty != 5 {
		t.Fatalf("重复事件只能计一次，得到 count=%d qty=%d", w.EventCount, w.TotalQty)
	}
	if c.stats.Duplicates != 2 {
		t.Errorf("应有 2 次重复判重，得到 %d", c.stats.Duplicates)
	}
}

// TestIdlePartitionDoesNotBlockWindow：停滞分区不无限阻塞其它分区窗口。
func TestIdlePartitionDoesNotBlockWindow(t *testing.T) {
	a := NewPartitionLog("A")
	a.AddEvent(1, "X", 1_000, 100, 1)
	a.AddWatermark(12_000) // A 声明越过窗口结束时间
	a.AddEnd()
	b := NewPartitionLog("B")
	b.AddIdle(0) // B 一直停滞、永不恢复
	b.AddEnd()
	c := runPipeline(t, testCfg(), []*PartitionLog{a, b}, testLogger(t))
	if c.stats.WindowsFired < 1 {
		t.Fatalf("停滞分区不应阻塞 A 的窗口触发，stats=%+v", c.stats)
	}
	if len(c.finalOf()) != 1 {
		t.Fatalf("应有一个窗口结果，得到 %d", len(c.finalOf()))
	}
}
