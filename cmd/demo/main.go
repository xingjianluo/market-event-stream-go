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
	"time"

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
	_, _ = proc2.Checkpoint(ctx) // 屏障，确保全部处理完
	must(proc2.Close())
	proc2.Wait()
	<-done2

	demoResultBus()
	fmt.Println(">>> demo finished")
}

// demoResultBus 演示“可断点续读的结果订阅与审计回放”层：
//  1. 总线为每条窗口结果分配稳定单调位点；
//  2. 订阅者处理若干条后掉线，用已确认位点重连续读，不重不漏；
//  3. 新订阅者先取完整快照，再从快照边界接增量，结论与连续消费一致；
//  4. 订阅者落后超过可保留范围时得到明确的 ErrPositionExpired，可重新取快照恢复。
func demoResultBus() {
	ctx := context.Background()
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelWarn}))
	cfg := marketstream.DefaultConfig()
	cfg.Logger = logger
	cfg.MaxOutOfOrderness = 0
	cfg.AllowedLateness = 50_000
	partitions := []int{0, 1}

	proc, err := marketstream.NewProcessor(cfg, partitions)
	must(err)
	bcfg := marketstream.DefaultResultBusConfig()
	bcfg.HistoryCapacity = 8 // 故意调小，演示有界回放与溢出结论
	bus, err := marketstream.NewResultBus(bcfg)
	must(err)
	must(proc.Start(ctx))
	must(bus.Attach(proc))

	// 审计订阅：从位点 0 开始完整消费，用作“从未断线”的参照视图。
	auditor, err := bus.Subscribe(0)
	must(err)

	// 业务订阅：先消费 3 条结果，记录确认位点后“掉线”。
	app, err := bus.Subscribe(0)
	must(err)
	events := []marketstream.Event{
		ev(0, 0, "BTC", 1_000, 100, 2),
		ev(1, 0, "BTC", 2_000, 105, 1),
		ev(0, 1, "BTC", 5_000, 110, 3),
		ev(1, 1, "BTC", 11_000, 108, 4),
		ev(0, 2, "BTC", 21_000, 130, 1),
		ev(1, 2, "BTC", 22_000, 135, 2), // 推进 wm=21000，触发 w0/w1 FINAL
		ev(0, 3, "BTC", 12_000, 120, 1), // w1 更正
		ev(0, 4, "BTC", 3_000, 95, 1),   // w0 更正
	}
	for _, e := range events {
		must(proc.Ingest(ctx, e))
	}

	printNext := func(tag string, s *marketstream.Subscription, n int) marketstream.Position {
		var committed marketstream.Position
		for i := 0; i < n; i++ {
			r, err := s.Next(ctx)
			if err != nil {
				fmt.Printf("[%s] next error: %v\n", tag, err)
				return committed
			}
			fmt.Printf("[%s] pos=%d %s %s count=%d qty=%d\n",
				tag, r.Position, r.Result.Kind, r.Result.Window,
				r.Result.Agg.EventCount, r.Result.Agg.TotalQty)
			committed = s.Ack(r.Position)
		}
		return committed
	}

	checkpoint := printNext("app-online", app, 3)
	fmt.Printf("[app] processed 3, committed position=%d; simulating disconnect ...\n", checkpoint)
	must(app.Close())

	// 重连：从已确认位点继续，只拿到之后的结果。
	app2, err := bus.Subscribe(checkpoint)
	must(err)
	for {
		r, err := app2.Next(ctx)
		if err != nil {
			break
		}
		fmt.Printf("[app-resume] pos=%d %s %s count=%d\n",
			r.Position, r.Result.Kind, r.Result.Window, r.Result.Agg.EventCount)
		app2.Ack(r.Position)
		if bus.CurrentPosition() == r.Position {
			break
		}
	}

	// 新订阅者：先取快照视图，再从快照边界接增量。
	snap := bus.Snapshot()
	fmt.Printf("[new-sub] snapshot at position=%d oldest=%d windows=%d\n",
		snap.Position, snap.Oldest, len(snap.Windows))
	for _, w := range snap.Windows {
		fmt.Printf("[new-sub] snapshot view %s %s count=%d (last pos=%d)\n",
			w.Result.Kind, w.Result.Window, w.Result.Agg.EventCount, w.Position)
	}
	fresh, err := bus.Subscribe(snap.Position)
	must(err)
	// 快照边界即当前尾端：此刻没有更新的结果，Next 会阻塞等待（这正是“续读”语义）。
	// 用一个短超时上下文演示该行为而不挂起。
	waitCtx, cancelWait := context.WithTimeout(ctx, 50*time.Millisecond)
	if _, err := fresh.Next(waitCtx); err != nil {
		fmt.Printf("[new-sub] caught up at snapshot boundary; Next blocks for new results (%v)\n", err)
	}
	cancelWait()
	must(fresh.Close())

	// 演示溢出：再制造足够多结果把小容量历史轮转越过审计订阅的进度。
	go func() {
		for i := int64(0); i < 20; i++ {
			_ = proc.Ingest(ctx, ev(int(i%2), 100+i, "BTC", 30_000+i*10_000, 100, 1))
		}
		_ = proc.MarkComplete(ctx, 0)
		_ = proc.MarkComplete(ctx, 1)
	}()
	// auditor 故意不读，直到历史轮转越界，得到明确、可区分的结论。
	for {
		_, err := auditor.Next(ctx)
		if err != nil {
			fmt.Printf("[auditor] stopped with distinguishable error: %v\n", err)
			break
		}
	}
	// 恢复方式：重新取快照 + 从快照边界续读（此处管线已 EOF，演示错误判定即可）。
	if _, err := bus.Subscribe(0); err != nil {
		fmt.Printf("[recover] subscribing at expired position rejected as expected: %v\n", err)
	}
	recoverySnap := bus.Snapshot()
	rec, err := bus.Subscribe(recoverySnap.Position)
	if err != nil {
		fmt.Printf("[recover] subscribe at snapshot boundary: %v\n", err)
	} else {
		fmt.Printf("[recover] re-attached at snapshot boundary=%d (history still covers it)\n",
			recoverySnap.Position)
		_ = rec.Close()
	}

	must(proc.Close())
	proc.Wait()
	bus.Wait()
	must(app2.Close())
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
