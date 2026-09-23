package pipeline

import (
	"context"
	"math"
	"sort"
	"time"
)

// cpReq 请求聚合器在其单线程内生成一致性快照。
type cpReq struct {
	done chan error
	snap *snapshot
}

// farFuture 是源全部投递完毕后的收尾水位线（+Inf 语义）：
// 此时不会再有任何事件到达，安全地触发并清理全部窗口。
var farFuture = time.UnixMilli(math.MaxInt64 / 4).UTC()

func (p *Pipeline) runAggregator(ctx context.Context, restored *snapshot, store *checkpointStore) {
	defer close(p.aggDone)
	closed := false
	defer func() {
		// 取消/错误退出时也关闭输出，避免消费方永远等待；正常路径已关闭则跳过。
		if !closed {
			close(p.resCh)
			close(p.sideCh)
			closed = true
		}
	}()

	emitResult := func(r WindowResult) bool {
		select {
		case p.resCh <- r:
			return true
		case <-ctx.Done():
			return false
		}
	}
	emitSide := func(r SideRecord) bool {
		select {
		case p.sideCh <- r:
			return true
		case <-ctx.Done():
			return false
		}
	}

	agg := newWindowAgg(p.cfg.WindowSize, p.cfg.AllowedLateness)
	tracker := newWatermarkTracker()
	for pid := range p.logs {
		tracker.register(pid)
	}
	offsets := make(map[PartitionID]int64, len(p.logs))
	for pid := range p.logs {
		offsets[pid] = -1
	}

	if restored != nil {
		tracker.restore(restored.WM)
		for _, st := range restored.Windows {
			agg.windows[st.key] = st
			for _, e := range st.events {
				agg.dedup.add(e.ID, st.key)
			}
		}
		for pid, off := range restored.Offsets {
			offsets[pid] = off
		}
		p.stats = restored.Stats
	}

	var ticker *time.Ticker
	var tickC <-chan time.Time
	if p.cfg.CheckpointInterval > 0 && p.cfg.CheckpointDir != "" {
		ticker = time.NewTicker(p.cfg.CheckpointInterval)
		tickC = ticker.C
		defer ticker.Stop()
	}

	closeOutputs := func() {
		if !closed {
			closed = true
			close(p.resCh)
			close(p.sideCh)
		}
	}
	fail := func(err error) {
		p.runErr = err
		p.publishStats(p.stats)
	}

	// fireAndPurge 推进全局水位线（除非 forced），触发到期窗口、清理过期窗口。
	fireAndPurge := func(gw time.Time, forced bool) bool {
		if !forced {
			if ng, moved := tracker.advance(); moved {
				gw = ng
				p.stats.WatermarksAdvance++
				p.logger("全局水位线推进 -> %s", gw.Format(time.RFC3339Nano))
			} else {
				gw = tracker.globalWM()
			}
		}
		for _, r := range agg.fireExpired(gw, p.logger) {
			p.stats.WindowsFired++
			if !emitResult(r) {
				return false
			}
		}
		n := agg.purgeExpired(gw, p.logger)
		p.stats.WindowsPurged += int64(n)
		return true
	}

	// handleOne 按投递全序处理单条消息：事件、水位线标记、EOF 的流中
	// 位置即生效顺序。返回 stop=true 表示输出失败，聚合器应退出。
	handleOne := func(msg inMsg) (stop bool) {
		// 控制消息
		switch {
		case msg.sourceDone:
			tracker.allEOF = true
			p.logger("源协调器报告所有分区日志投递完毕，准备最终冲刷")
			return false
		case msg.eof:
			tracker.markEnd(msg.p)
			p.logger("分区 %s 物理日志结束（EOF），永久退出全局水位线最小值", msg.p)
			return !fireAndPurge(time.Time{}, false)
		case msg.entry.Kind == EntryWatermark:
			at, ok := tracker.observeMark(msg.p, msg.entry.MarkTime)
			if ok {
				p.logger("分区 %s 水位线标记 -> %s", msg.p, at.Format(time.RFC3339Nano))
			} else {
				p.logger("分区 %s 水位线标记 %s 被忽略（分区水位线只能单调推进，当前=%s）",
					msg.p, msg.entry.MarkTime.Format(time.RFC3339Nano),
					at.Format(time.RFC3339Nano))
			}
			if !fireAndPurge(time.Time{}, false) {
				return true
			}
			return false
		case msg.entry.Kind == EntryIdle:
			tracker.markIdle(msg.p)
			p.logger("分区 %s 标记停滞（idle），暂时退出全局水位线最小值计算（标记时间=%s）",
				msg.p, msg.entry.MarkTime.Format(time.RFC3339Nano))
			if !fireAndPurge(time.Time{}, false) {
				return true
			}
			return false
		case msg.entry.Kind == EntryEnd:
			tracker.markEnd(msg.p)
			p.logger("分区 %s 结束标记，永久退出全局水位线最小值", msg.p)
			if !fireAndPurge(time.Time{}, false) {
				return true
			}
			return false
		case msg.barrier:
			// 栅栏对齐点：栅栏前全部事件与标记已生效，完成触发/清理后回调。
			p.logger("分区 %s 脉冲栅栏（偏移=%d）：冲刷窗口后放行后续条目",
				msg.p, msg.offset)
			if !fireAndPurge(time.Time{}, false) {
				return true
			}
			if p.cfg.OnBarrier != nil || p.cfg.CheckpointDir != "" {
				// 在聚合器单线程内直接生成一致性快照并落盘（不走 cpCh，
				// 否则回调里请求检查点会与当前处理自死锁）。
				snap := p.buildSnapshot(tracker, offsets, agg, time.Now().UTC())
				if err := store.save(snap); err != nil {
					fail(err)
					return true
				}
				if p.cfg.OnBarrier != nil {
					if err := p.cfg.OnBarrier(msg.p, msg.offset, snap); err != nil {
						if msg.ack != nil {
							close(msg.ack)
						}
						fail(err)
						return true
					}
				}
			}
			if msg.ack != nil {
				close(msg.ack)
			}
			return false
		}

		// 事件消息
		offsets[msg.p] = msg.offset
		p.stats.Received++
		gw := tracker.globalWM()
		e := msg.entry.Event
		tracker.observeEvent(msg.p, e.EventTime)
		verdict, revised, reason := agg.process(e, gw)
		rec := SideRecord{Event: e, GlobalWatermark: gw}
		switch verdict {
		case verdictOnTime:
			p.stats.EventsAccepted++
			p.logger("事件 on-time: id=(%s,%d) symbol=%s eventTime=%s gw=%s 判定=%s",
				e.ID.Partition, e.ID.Seq, e.Symbol,
				e.EventTime.Format(time.RFC3339Nano), gw.Format(time.RFC3339Nano), reason)
		case verdictLateAllowed:
			p.stats.EventsAccepted++
			p.stats.LateInAllowed++
			rec.Disposition = DispositionLateInAllowed
			rec.Reason = reason
			if !emitSide(rec) {
				fail(ctx.Err())
				return true
			}
			p.logger("事件 late-in-allowed: id=(%s,%d) symbol=%s eventTime=%s gw=%s 判定=%s",
				e.ID.Partition, e.ID.Seq, e.Symbol,
				e.EventTime.Format(time.RFC3339Nano), gw.Format(time.RFC3339Nano), reason)
			if revised.EventCount > 0 {
				if !revised.Revised {
					p.stats.WindowsFired++
				} else {
					p.stats.WindowsRevised++
				}
				if !emitResult(revised) {
					fail(ctx.Err())
					return true
				}
			}
		case verdictLateDropped:
			p.stats.LateDropped++
			rec.Disposition = DispositionLateDropped
			rec.Reason = reason
			if !emitSide(rec) {
				fail(ctx.Err())
				return true
			}
			p.logger("事件 late-dropped: id=(%s,%d) symbol=%s eventTime=%s gw=%s 判定=%s",
				e.ID.Partition, e.ID.Seq, e.Symbol,
				e.EventTime.Format(time.RFC3339Nano), gw.Format(time.RFC3339Nano), reason)
		case verdictDuplicate:
			p.stats.Duplicates++
			rec.Disposition = DispositionDuplicate
			rec.Reason = reason
			if !emitSide(rec) {
				fail(ctx.Err())
				return true
			}
			p.logger("事件 duplicate: id=(%s,%d) symbol=%s eventTime=%s gw=%s 判定=%s",
				e.ID.Partition, e.ID.Seq, e.Symbol,
				e.EventTime.Format(time.RFC3339Nano), gw.Format(time.RFC3339Nano), reason)
		}
		return false
	}

	// processSeq 处理一批已按 seq 全序排序的消息。
	processSeq := func(batch []inMsg) bool {
		sort.SliceStable(batch, func(i, j int) bool { return batch[i].seq < batch[j].seq })
		for _, msg := range batch {
			if handleOne(msg) {
				return false
			}
		}
		return true
	}

	// drain 非阻塞抽取 inputCh 中已缓冲消息。
	drain := func(first *inMsg) []inMsg {
		var batch []inMsg
		if first != nil {
			batch = append(batch, *first)
		}
		for {
			select {
			case m := <-p.inputCh:
				batch = append(batch, m)
			default:
				return batch
			}
		}
	}

	for {
		if tracker.allEOF {
			// +Inf 收尾：触发并清理所有剩余窗口。
			p.logger("使用收尾水位线 %s 冲刷全部窗口", farFuture.Format(time.RFC3339Nano))
			if !fireAndPurge(farFuture, true) {
				fail(ctx.Err())
				return
			}
			p.finalSnap = p.buildSnapshot(tracker, offsets, agg, time.Now().UTC())
			p.publishStats(p.stats)
			closeOutputs()
			return
		}

		select {
		case <-ctx.Done():
			fail(ctx.Err())
			return
		case req := <-p.cpCh:
			snap := p.buildSnapshot(tracker, offsets, agg, time.Now().UTC())
			req.snap = snap
			err := store.save(snap)
			req.done <- err
			if err == nil {
				p.stats.CheckpointsCompleted++
			}
			p.publishStats(p.stats)
		case <-tickC:
			snap := p.buildSnapshot(tracker, offsets, agg, time.Now().UTC())
			if err := store.save(snap); err != nil {
				fail(err)
				return
			}
			p.stats.CheckpointsCompleted++
			p.logger("周期检查点完成: 全局水位线=%s 偏移=%v 窗口数=%d",
				tracker.globalWM().Format(time.RFC3339Nano), offsets, len(agg.windows))
			p.publishStats(p.stats)
		case first := <-p.inputCh:
			// handleOne 在失败时已用具体错误调用 fail（注入故障或 ctx 取消），
			// 这里不能再用 fail(ctx.Err()) 覆盖，否则 Background 下会把注入
			// 错误重置为 nil。
			if !processSeq(drain(&first)) {
				return
			}
		}
	}
}

func (p *Pipeline) buildSnapshot(tracker *watermarkTracker, offsets map[PartitionID]int64,
	agg *windowAgg, at time.Time) *snapshot {
	off := make(map[PartitionID]int64, len(offsets))
	for k, v := range offsets {
		off[k] = v
	}
	return &snapshot{
		TakenAt: at,
		WM:      tracker.export(),
		Offsets: off,
		Windows: agg.exportWindows(),
		Stats:   p.stats,
	}
}
