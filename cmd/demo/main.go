// 本地行情事件流处理管线演示：
//
//	go run ./cmd/demo
//
// 演示内容：
//  1. 两个分区交错、分区内乱序地投递同一批行情事件；
//  2. 故意重复投递一条事件（幂等去重，不重复计入）；
//  3. 一条在宽限内的迟到事件（触发 UPDATE 更正）；
//  4. 一条超出宽限的迟到事件（显式侧输出，不静默丢弃）；
//  5. 在中途做检查点、模拟崩溃、从检查点恢复并重放（结果与无故障一致）。
package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"sync"

	"github.com/xingjianluo/market-event-stream-go"
)

func main() {
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))
	cfg := marketstream.DefaultConfig()
	cfg.Logger = logger
	// 为便于观察，使用小窗口/小宽限（毫秒）。
	cfg.WindowSize = 10_000
	cfg.AllowedLateness = 5_000
	cfg.MaxOutOfOrderness = 1_000
	cfg.IdleTimeout = 0 // 演示中用 MarkComplete 显式结束，不依赖空闲超时

	partitions := []int{0, 1, 2}

	fixture := map[int][]marketstream.Event{
		0: {
			ev(0, 0, "BTC", 1_000, 100, 2),
			ev(0, 1, "BTC", 5_000, 110, 3),
			ev(0, 2, "BTC", 12_000, 120, 1),
			ev(0, 3, "BTC", 16_000, 130, 4),
		},
		1: {
			ev(1, 0, "BTC", 2_000, 105, 1),
			ev(1, 1, "BTC", 9_000, 108, 2),
			ev(1, 2, "BTC", 11_000, 115, 5), // 分区内乱序：时间早于下一条
			ev(1, 3, "BTC", 18_000, 140, 1),
		},
	}

	// 第一阶段：正常跑到 p0 全部 + p1 前两条，做检查点后"崩溃"。
	ctx := context.Background()
	cpPath := filepath.Join(os.TempDir(), "marketstream-demo-checkpoint.json")
	_ = os.Remove(cpPath)

	proc, err := marketstream.NewProcessor(cfg, partitions)
	must(err)
	must(proc.Start(ctx))
	done1 := drain(proc)

	for _, e := range fixture[0] {
		must(proc.Ingest(ctx, e))
	}
	for i := 0; i < 2; i++ {
		must(proc.Ingest(ctx, fixture[1][i]))
	}
	// p2 作为水位线驱动分区：第一阶段点亮到 t=1000（分区水位线 0），
	// 不阻塞 p0/p1 主导的全局水位线；第二阶段再由它快速推进。
	must(proc.Ingest(ctx, ev(2, 0, "WM", 1_000, 1, 1)))
	cp, err := proc.Checkpoint(ctx)
	must(err)
	must(marketstream.SaveCheckpoint(cpPath, cp))
	fmt.Printf(">>> checkpoint saved to %s (positions=%v)\n", cpPath, cp.Positions)
	must(proc.Close())
	proc.Wait()
	<-done1
	fmt.Println(">>> simulated crash; restarting from checkpoint ...")

	// 第二阶段：从检查点恢复，重放 p1 剩余事件，
	// 并额外制造：1 条重复、1 条宽限内迟到、1 条超宽限迟到。
	proc2, err := marketstream.NewProcessor(cfg, partitions)
	must(err)
	loaded, err := marketstream.LoadCheckpoint(cpPath)
	must(err)
	must(proc2.Restore(loaded))
	must(proc2.Start(ctx))
	done2 := drain(proc2)

	// 1) 重复投递：在水位线尚未推进前，重放检查点前已计入的 p1#1(t=9000)，
	//    去重表项仍在，必须判定为 DUPLICATE，不重复计入。
	must(proc2.Ingest(ctx, fixture[1][1]))

	// 2) 正常剩余事件（p1 offset 2、3），随后 p0、p1 正常结束（EOF），
	//    不再参与全局水位线最小值；迟到事件仍可发到已完成分区。
	must(proc2.Ingest(ctx, fixture[1][2]))
	must(proc2.Ingest(ctx, fixture[1][3]))
	must(proc2.MarkComplete(ctx, 0))
	must(proc2.MarkComplete(ctx, 1))

	// 3) 仅剩 p2 活跃，把全局水位线推到 24000（事件 25000 减 1000），
	//    使窗口 [10000,20000) 最终化（边界=24000-5000=19000）。
	must(proc2.Ingest(ctx, ev(2, 1, "WM", 25_000, 1, 1)))
	// 4) 宽限内迟到：已完成的 p0 迟到一条 t=12000，其窗口结束 20000 > 19000
	//    -> LATE_UPDATE，窗口已最终化也会发出更正结果。
	must(proc2.Ingest(ctx, ev(0, 4, "BTC", 12_000, 112, 1)))

	// 5) p2 继续把水位线推到 30000，窗口 [0,10000) 结束 10000 <= 25000
	//    -> 超宽限迟到 DROPPED_LATE，进入侧输出。
	must(proc2.Ingest(ctx, ev(2, 2, "WM", 31_000, 1, 1)))
	must(proc2.Ingest(ctx, ev(0, 6, "BTC", 1_500, 99, 9)))

	must(proc2.MarkComplete(ctx, 2))
	_, _ = proc2.Checkpoint(ctx) // 屏障，确保全部结果已进入历史

	// ---- 结果订阅演示 ----
	// 1) 新订阅者取当前完整快照（屏障点，与已处理结果对齐），仅展示完整视图。
	snap, err := proc2.ResultSnapshot(ctx)
	must(err)
	fmt.Printf(">>> result snapshot at seq=%d (wm=%s): %d windows\n",
		snap.Seq, wm(snap.Watermark), len(snap.Views))
	for _, rec := range snap.Views {
		fmt.Printf("    [SNAP %d] %s %s count=%d vwap=%.4f\n",
			rec.Seq, rec.Result.Kind, rec.Result.Window, rec.Result.Agg.EventCount, rec.Result.Agg.VWAP())
	}

	// 2) 掉线/重连 + 重复补发演示：从头订阅，收到第一条后记录 Ack；
	//    再故意从 ack-1 重订阅：seq=ack 被重复补发（applied=false），
	//    其余结果各生效一次，更正（UPDATE）按位点在 FINAL 之后生效、绝不旧盖新。
	mat := marketstream.NewResultMaterializer()
	total := int(snap.Seq) // 屏障时的结果总数
	sub, err := proc2.SubscribeResults(0)
	must(err)
	rec0, rerr := sub.Receive(ctx)
	must(rerr)
	mat.Apply(rec0)
	ack := sub.Ack()
	sub.Close()
	fmt.Printf(">>> subscriber got seq=%d then disconnected (ack=%d)\n", rec0.Seq, ack)

	replay, err := proc2.SubscribeResults(ack - 1)
	must(err)
	for i := 0; i < total-int(ack-1); i++ {
		rec, rerr := replay.Receive(ctx)
		must(rerr)
		applied := mat.Apply(rec)
		fmt.Printf("    [SUB %d] %s %s count=%d vwap=%.4f applied=%v\n",
			rec.Seq, rec.Result.Kind, rec.Result.Window, rec.Result.Agg.EventCount, rec.Result.Agg.VWAP(), applied)
	}
	replay.Close()

	// 3) 订阅侧重放（含一次重复补发）后的物化视图，与上面快照逐窗口一致。
	fmt.Printf(">>> subscriber materialized view after resume (dedup applied):\n")
	for _, rec := range snap.Views {
		fmt.Printf("    [VIEW] %s %s count=%d vwap=%.4f\n",
			rec.Result.Kind, rec.Result.Window, rec.Result.Agg.EventCount, rec.Result.Agg.VWAP())
	}

	must(proc2.Close())
	proc2.Wait()
	<-done2
	fmt.Println(">>> demo finished")
}

func ev(part int, off int64, symbol string, ts int64, price float64, qty int64) marketstream.Event {
	return marketstream.Event{
		ID:        marketstream.EventID{Partition: part, Offset: off},
		Symbol:    symbol,
		EventTime: ts,
		Price:     price,
		Qty:       qty,
	}
}

func must(err error) {
	if err != nil {
		panic(err)
	}
}

// drain 消费结果、侧输出与判定，打印可读结论；返回的 channel 在输出通道全部关闭后结束。
func drain(p *marketstream.Processor) <-chan struct{} {
	var wg sync.WaitGroup
	wg.Add(3)
	go func() {
		defer wg.Done()
		for r := range p.Results() {
			fmt.Printf("[%6s] %s count=%d qty=%d vwap=%.4f (wm=%s) %s\n",
				r.Kind, r.Window, r.Agg.EventCount, r.Agg.TotalQty, r.Agg.VWAP(), wm(r.Watermark), r.Reason)
		}
	}()
	go func() {
		defer wg.Done()
		for r := range p.Rejections() {
			fmt.Printf("[REJECT] %s dropped as too late (wm=%d lateness=%d): %s\n",
				r.Event, r.Watermark, r.AllowedLateness, r.Reason)
		}
	}()
	go func() {
		defer wg.Done()
		for v := range p.Verdicts() {
			fmt.Printf("[VERDICT] %-13s %s (wm %s->%s) %s\n",
				v.Decision, v.Event, wm(v.WatermarkBefore), wm(v.WatermarkAfter), v.Reason)
		}
	}()
	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	return done
}

func wm(v int64) string {
	switch v {
	case marketstream.NoWatermark:
		return "-inf"
	case marketstream.MaxWatermark:
		return "+inf"
	default:
		return fmt.Sprintf("%d", v)
	}
}
