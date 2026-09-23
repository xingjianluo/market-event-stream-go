package pipeline

import (
	"fmt"
	"sort"
	"testing"
)

// signature 提取与到达顺序无关的最终结论指纹：
// 每个窗口的最终帧（品种/起点/计数/总量/VWAP）+ 每条侧输出的处分集合。
func signature(c collected) string {
	s := ""
	for _, w := range c.finalOf() {
		s += fmt.Sprintf("W{%s@%d n=%d q=%d v=%d rev=%v};",
			w.Symbol, w.WindowStart.UnixMilli(), w.EventCount, w.TotalQty,
			w.VwapPriceMicros, w.Revised)
	}
	// 侧输出按事件 ID 排序，处分构成集合指纹
	type kv struct {
		k string
		d LateDisposition
	}
	var all []kv
	for _, sd := range c.sides {
		all = append(all, kv{
			fmt.Sprintf("%s:%d", sd.Event.ID.Partition, sd.Event.ID.Seq),
			sd.Disposition,
		})
	}
	sort.Slice(all, func(i, j int) bool { return all[i].k < all[j].k })
	for _, x := range all {
		s += fmt.Sprintf("S{%s=%s};", x.k, x.d)
	}
	return s
}

// TestBatchSizeInvariance：改变 MergeBatchSize 不改变最终聚合与处分。
func TestBatchSizeInvariance(t *testing.T) {
	var first string
	for _, bs := range []int{1, 2, 3, 7, 64} {
		cfg := testCfg()
		cfg.MergeBatchSize = bs
		c := runPipeline(t, cfg, canonicalLogs(), nil)
		sig := signature(c)
		t.Logf("MergeBatchSize=%d => %s", bs, sig)
		if first == "" {
			first = sig
		} else if sig != first {
			t.Fatalf("批大小 %d 的结论与基准不一致:\n基准=%s\n当前=%s", bs, first, sig)
		}
	}
}

// TestPartitionOrderInvariance：调换分区注册（轮转）顺序，最终结论不变。
func TestPartitionOrderInvariance(t *testing.T) {
	logs := canonicalLogs()
	base := signature(runPipeline(t, testCfg(), logs, nil))

	// 反转分区顺序
	rev := make([]*PartitionLog, len(logs))
	for i := range logs {
		rev[i] = logs[len(logs)-1-i]
	}
	got := signature(runPipeline(t, testCfg(), rev, nil))
	if got != base {
		t.Fatalf("分区顺序改变后结论不一致:\n基准=%s\n反转=%s", base, got)
	}
}

// TestInterleavingInvariance：把"先批量投 A 再批量投 B"与"逐条交错"
// 两种日志布局对比，最终窗口聚合一致（不使用栅栏时中间帧时机可不同，
// 但同批事件的最终聚合与去重结论一致）。
func TestInterleavingInvariance(t *testing.T) {
	// 布局1：A、B 各自连续
	layoutA := func() []*PartitionLog {
		a := NewPartitionLog("A")
		a.AddEvent(1, "S", 1_000, 100, 2)
		a.AddEvent(2, "S", 2_000, 110, 2)
		a.AddWatermark(12_000)
		a.AddEnd()
		b := NewPartitionLog("B")
		b.AddEvent(3, "S", 3_000, 120, 2)
		b.AddWatermark(12_000)
		b.AddEnd()
		return []*PartitionLog{a, b}
	}
	// 布局2：内容相同，只调换分区归属顺序以改变交错形态
	layoutB := func() []*PartitionLog {
		a := NewPartitionLog("A")
		a.AddEvent(3, "S", 3_000, 120, 2)
		a.AddWatermark(12_000)
		a.AddEnd()
		b := NewPartitionLog("B")
		b.AddEvent(1, "S", 1_000, 100, 2)
		b.AddEvent(2, "S", 2_000, 110, 2)
		b.AddWatermark(12_000)
		b.AddEnd()
		return []*PartitionLog{a, b}
	}
	c1 := signature(runPipeline(t, testCfg(), layoutA(), nil))
	c2 := signature(runPipeline(t, testCfg(), layoutB(), nil))
	if c1 != c2 {
		t.Fatalf("分区交错形态不同但最终结论应一致:\n%s\nvs\n%s", c1, c2)
	}
	// VWAP = (100*2+110*2+120*2)/6 = 660/6 = 110
	c := runPipeline(t, testCfg(), layoutA(), nil)
	w := c.finalOf()[0]
	if w.VwapPriceMicros != 110 || w.EventCount != 3 {
		t.Fatalf("期望 count=3 vwap=110，得到 count=%d vwap=%d", w.EventCount, w.VwapPriceMicros)
	}
}
