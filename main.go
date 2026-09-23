// Command market-event-stream 运行一个本地行情事件流处理管线演示：
// 多个分区日志以交错、乱序、重复、迟到的方式提供事件，
// 管线按事件时间窗口聚合，输出窗口结果（含迟到修正帧）与侧输出判定。
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/xingjianluo/market-event-stream-go/pipeline"
)

func main() {
	cpDir := flag.String("checkpoint-dir", "", "检查点目录（设置后周期落盘，再次启动从该目录恢复）")
	flag.Parse()

	logger := log.New(os.Stdout, "[pipe] ", log.LstdFlags|log.Lmicroseconds)
	printf := func(format string, a ...any) { logger.Output(2, fmt.Sprintf(format, a...)) }

	cfg := pipeline.DefaultConfig()
	cfg.CheckpointDir = *cpDir
	if *cpDir != "" {
		cfg.CheckpointInterval = 200 * time.Millisecond
	}

	p := pipeline.New(cfg, buildDemoLogs(), printf)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() {
		if err := p.Run(ctx); err != nil {
			logger.Fatalf("管线失败: %v", err)
		}
	}()

	for r := range p.Results() {
		kind := "首帧"
		if r.Revised {
			kind = "修正帧"
		}
		fmt.Printf("== 窗口结果(%s) symbol=%s [%s,%s) count=%d qty=%d vwapMicros=%d 触发水位线=%s\n",
			kind, r.Symbol, r.WindowStart.Format(time.RFC3339Nano), r.WindowEnd.Format(time.RFC3339Nano),
			r.EventCount, r.TotalQty, r.VwapPriceMicros, r.FiredBy.Format(time.RFC3339Nano))
	}
	for s := range p.SideOutput() {
		fmt.Printf("## 侧输出 disposition=%s id=(%s,%d) eventTime=%s gw=%s 原因=%s\n",
			s.Disposition, s.Event.ID.Partition, s.Event.ID.Seq,
			s.Event.EventTime.Format(time.RFC3339Nano), s.GlobalWatermark.Format(time.RFC3339Nano), s.Reason)
	}
	st := p.Stats()
	fmt.Printf("统计: %+v\n", st)
}

// buildDemoLogs 构造两个活跃分区 + 一个全程停滞分区（窗口 10s，允许迟到 5s）。
// 覆盖：分区交错、分区内乱序、重复投递（同 ID 再投）、
// 迟到但允许（修正帧）、迟到超范围（明确丢弃）、停滞分区(idle)不阻塞窗口。
//
// 为了让"超范围丢弃"在任何 MergeBatchSize 下结论一致，事件5之前在
// A、B 两分区都垫了水位线标记：公平轮转会先处理 B 的 20s 标记，
// 全局水位线因此越过清理线 15s，事件5 必为 LATE_DROPPED。
func buildDemoLogs() []*pipeline.PartitionLog {
	// 分区 A
	a := pipeline.NewPartitionLog("A")
	a.AddEvent(1, "BTC", 1_000, 100_000_000, 2) // W1 正常事件
	a.AddEvent(2, "BTC", 3_000, 102_000_000, 1) // 分区内乱序，仍 on-time
	a.AddWatermark(5_000)
	a.AddEvent(2, "BTC", 3_000, 102_000_000, 1) // 重复投递 -> DUPLICATE
	a.AddWatermark(12_000)                      // 全局 gw=12s：W1 首帧触发
	a.AddEvent(3, "BTC", 9_000, 99_000_000, 3)  // gw=12s：迟到但允许 -> 修正帧
	a.AddEvent(4, "BTC", 2_000, 50_000_000, 9)  // gw=12s：仍在允许范围 -> 修正帧
	a.AddWatermark(13_000)                      // 与 B 对齐，暂不越过清理线
	a.AddIdle(13_000)                           // A 短暂停滞
	a.AddWatermark(13_000)                      // 恢复（水位线不变，保证单调）
	a.AddWatermark(20_000)                      // 与 B 的 20s 标记同轮转
	a.AddWatermark(20_000)                      // 各分区均重复声明，保证任何批大小下
	a.AddWatermark(20_000)                      // 事件5被处理前全局水位线都已达到 20s
	a.AddWatermark(20_000)                      // （源语义：标记是对"此前事件已发出"的承诺）
	a.AddEvent(5, "BTC", 1_000, 10_000_000, 7)  // gw=20s > 清理线 15s -> LATE_DROPPED
	a.AddEnd()

	// 分区 B：与 A 交错；早期事件后停滞一段再恢复
	b := pipeline.NewPartitionLog("B")
	b.AddEvent(10, "BTC", 1_500, 98_000_000, 4) // W1 正常事件（跨分区交错）
	b.AddWatermark(6_000)
	b.AddIdle(6_000) // 停滞：不再阻塞全局水位线
	b.AddWatermark(13_000)
	b.AddWatermark(20_000) // 与 A 的 20s 标记同轮轮转 -> 全局 gw=20s，清理 W1
	// B 提前没有更多数据：它以水位线标记声明进度（而不是 EOF），
	// 这样在 A 的事件5被判定时全局水位线已为 20s；
	// 随后用重复标记补足轮转位置，保证事件5之前 B 的 20s 一定先被处理。
	b.AddWatermark(20_000)
	b.AddWatermark(20_000)
	b.AddWatermark(20_000)
	b.AddEnd()

	// 分区 C：全程停滞、无数据，验证停滞分区不阻塞其它窗口收尾
	c := pipeline.NewPartitionLog("C")
	c.AddIdle(0)
	c.AddEnd()

	return []*pipeline.PartitionLog{a, b, c}
}
