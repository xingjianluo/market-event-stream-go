package marketstream

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sort"
	"sync"
	"time"
)

// Decision 是单条事件进入管线后的可区分判定结论。
type Decision string

const (
	// DecisionAccepted：按时事件，已计入活动窗口。
	DecisionAccepted Decision = "ACCEPTED"
	// DecisionDuplicate：事件 ID 已处理过，幂等忽略，不重复计入。
	DecisionDuplicate Decision = "DUPLICATE"
	// DecisionLateUpdate：迟到但仍在允许宽限内，窗口被更新并发出更正结果。
	DecisionLateUpdate Decision = "LATE_UPDATE"
	// DecisionDroppedLate：迟到超出允许宽限，窗口已最终化并清除，转入侧输出。
	DecisionDroppedLate Decision = "DROPPED_LATE"
)

// ResultKind 区分窗口首次/最终结果与迟到更正。
type ResultKind string

const (
	// ResultFinal：水位线越过窗口结束时间后发出的最终结果。
	ResultFinal ResultKind = "FINAL"
	// ResultUpdate：迟到事件在宽限期内更新已最终化窗口后的更正结果。
	ResultUpdate ResultKind = "UPDATE"
)

// Verdict 记录单条事件的判定依据，供日志与测试断言。
type Verdict struct {
	Event           Event
	Decision        Decision
	Reason          string
	WatermarkBefore int64
	WatermarkAfter  int64
	Window          WindowKey
}

// WindowResult 是窗口聚合输出（最终结果或迟到更正）。
type WindowResult struct {
	Kind      ResultKind
	Window    WindowKey
	Agg       Aggregate
	Watermark int64 // 发出该结果时的全局水位线
	Reason    string
}

// LateRejection 是超出允许宽限的迟到事件的侧输出记录（不静默丢弃）。
type LateRejection struct {
	Event           Event
	Watermark       int64
	AllowedLateness int64
	Reason          string
}

// inMsg 是进入串行处理循环的内部消息。
type inMsg struct {
	ev         *Event
	partition  int
	barrier    *barrierReq
	idleTick   bool
	complete   bool
	shutdown   bool
	ingestTime time.Time // 入队时刻的处理时间（用于空闲判定，避免异步处理被时钟推进污染）
}

// barrierReq 是检查点屏障的请求/应答。wantSnap 为真时，除检查点外
// 还在同一屏障点返回与"当前已处理结果"严格对齐的结果订阅快照。
type barrierReq struct {
	cp       *Checkpoint
	snap     *ResultSnapshot
	wantSnap bool
	ch       chan error
}

// Processor 是事件流处理管线：单 goroutine 串行持有全部状态，
// 输入侧为有界队列，输出侧为有界通道，提供检查点与恢复能力。
type Processor struct {
	cfg             Config
	partitions      []int
	in              chan inMsg
	results         chan WindowResult
	rejections      chan LateRejection
	verdicts        chan Verdict
	done            chan struct{}
	stopCh          chan struct{}
	closedOnce      sync.Once
	logger          *slog.Logger
	disableIdleTick bool // 测试中使用虚拟时钟时关闭墙钟自动空闲检测
	store           *ResultStore

	// 以下字段仅由处理循环 goroutine 访问。
	tracker   *WatermarkTracker
	dedup     *DedupTable
	windows   map[WindowKey]*Window
	finalized map[WindowKey]bool
	positions map[int]int64
	wm        int64
}

// NewProcessor 按配置与分区列表创建管线（尚需 Start 启动处理循环）。
func NewProcessor(cfg Config, partitions []int) (*Processor, error) {
	if err := cfg.Validate(partitions); err != nil {
		return nil, err
	}
	p := &Processor{
		cfg:        cfg,
		partitions: append([]int(nil), partitions...),
		in:         make(chan inMsg, cfg.InputQueueCapacity),
		results:    make(chan WindowResult, cfg.ResultBufferSize),
		rejections: make(chan LateRejection, cfg.RejectBufferSize),
		verdicts:   make(chan Verdict, cfg.RejectBufferSize),
		done:       make(chan struct{}),
		logger:     cfg.Logger,
		tracker:    NewWatermarkTracker(partitions, cfg.MaxOutOfOrderness, cfg.IdleTimeout),
		dedup:      NewDedupTable(),
		windows:    make(map[WindowKey]*Window),
		finalized:  make(map[WindowKey]bool),
		positions:  make(map[int]int64),
		wm:         NoWatermark,
		stopCh:     make(chan struct{}),
		store:      NewResultStore(cfg.ResultHistoryCapacity),
	}
	for _, id := range partitions {
		p.positions[id] = 0
	}
	return p, nil
}

// Start 启动串行处理循环，并启动唯一的空闲检测定时器：
// 固定间隔（IdleTimeout/4）以配置时钟当前时间驱动 TickIdle，
// 使停滞分区超时后临时移出全局水位线最小值。只有一个 ticker，
// 不会随分区数产生 goroutine；IdleTimeout=0 时关闭空闲检测。
func (p *Processor) Start(ctx context.Context) error {
	go p.loop(ctx)
	if p.cfg.IdleTimeout > 0 && !p.disableIdleTick {
		interval := p.cfg.IdleTimeout / 4
		if interval <= 0 {
			interval = time.Millisecond
		}
		go func() {
			t := time.NewTicker(interval)
			defer t.Stop()
			for {
				select {
				case <-ctx.Done():
					return
				case <-p.done:
					return
				case <-t.C:
					_ = p.TickIdleNow(ctx)
				}
			}
		}()
	}
	return nil
}

// loop 是唯一的状态持有者：所有窗口、水位线、去重、偏移更新都在此串行完成，
// 因而结果对到达顺序与批大小不变，且无需细粒度锁。
func (p *Processor) loop(ctx context.Context) {
	defer close(p.done)
	for {
		select {
		case <-ctx.Done():
			p.logger.Info("processor: context done, exiting", "err", ctx.Err())
			return
		case <-p.stopCh:
			p.logger.Info("processor: force-stopped (downstream stalled on close)")
			return
		case msg := <-p.in:
			switch {
			case msg.shutdown:
				p.purgeAllWindows()
				return
			case msg.barrier != nil:
				cp := p.snapshot()
				msg.barrier.cp = cp
				if msg.barrier.wantSnap {
					msg.barrier.snap = p.store.Snapshot()
				}
				msg.barrier.ch <- nil
			case msg.idleTick:
				p.applyIdleTick()
			case msg.complete:
				p.handleComplete(ctx, msg.partition)
			case msg.ev != nil:
				p.handleEvent(ctx, *msg.ev, msg.ingestTime)
			}
		}
	}
}

// handleEvent 处理一条事件：去重 -> 窗口归属与迟到判定 -> 水位线推进 -> 触发窗口。
func (p *Processor) handleEvent(ctx context.Context, ev Event, now time.Time) {
	partition := ev.ID.Partition
	wmBefore := p.wm
	wStart, wEnd := AssignWindow(ev.EventTime, p.cfg.WindowSize)
	wk := WindowKey{Symbol: ev.Symbol, Start: wStart, End: wEnd}

	// 1) 幂等去重：同一 (partition, offset) 永不重复计入。
	if !p.dedup.Add(ev.ID, ev.EventTime) {
		p.emitVerdict(ctx, Verdict{
			Event: ev, Decision: DecisionDuplicate,
			Reason:          fmt.Sprintf("event %s already processed; idempotent skip", ev.ID),
			WatermarkBefore: wmBefore, WatermarkAfter: wmBefore, Window: wk,
		})
		p.logger.LogAttrs(ctx, slog.LevelInfo, "event duplicate (not counted)",
			slog.String("event", ev.String()), slog.Int64("watermark", wmBefore))
		return
	}

	// 位置随实际消费推进（取最大值，回放旧偏移不回退检查点）。
	if pos := ev.ID.Offset + 1; pos > p.positions[partition] {
		p.positions[partition] = pos
	}

	// 2) 迟到判定（基于窗口结束时间与当前水位线）。
	if p.wm != NoWatermark && wEnd <= p.wm {
		// 水位线已越过窗口结束；是否还在宽限期内取决于窗口是否仍保留。
		dropBoundary := p.wm - p.cfg.AllowedLateness
		if wEnd <= dropBoundary {
			// 超出允许宽限：窗口已最终化并清除 -> 显式侧输出，绝不静默丢弃。
			rej := LateRejection{
				Event: ev, Watermark: p.wm,
				AllowedLateness: p.cfg.AllowedLateness,
				Reason: fmt.Sprintf("window %s end=%d <= watermark %d - allowedLateness %d; window finalized",
					wk, wEnd, p.wm, p.cfg.AllowedLateness),
			}
			p.sendRejection(ctx, rej)
			p.emitVerdict(ctx, Verdict{
				Event: ev, Decision: DecisionDroppedLate,
				Reason: rej.Reason, WatermarkBefore: wmBefore, WatermarkAfter: wmBefore, Window: wk,
			})
			p.logger.LogAttrs(ctx, slog.LevelWarn, "event dropped as too late (side output)",
				slog.String("event", ev.String()), slog.Int64("watermark", p.wm),
				slog.Int64("allowedLateness", p.cfg.AllowedLateness))
			// 水位线仍需观察该分区进度。
			p.observeWM(partition, ev.EventTime, now)
			p.purgeExpiredWindows(ctx)
			return
		}
		// 宽限期内的迟到：窗口仍保留，计入并发出 UPDATE 更正。
		w := p.windows[wk]
		if w == nil {
			// 极端情况下窗口缺失（理论上不会，因为边界内不清除），按超宽限处理。
			rej := LateRejection{
				Event: ev, Watermark: p.wm, AllowedLateness: p.cfg.AllowedLateness,
				Reason: fmt.Sprintf("window %s state missing during lateness grace", wk),
			}
			p.sendRejection(ctx, rej)
			p.emitVerdict(ctx, Verdict{
				Event: ev, Decision: DecisionDroppedLate,
				Reason: rej.Reason, WatermarkBefore: wmBefore, WatermarkAfter: wmBefore, Window: wk,
			})
			p.observeWM(partition, ev.EventTime, now)
			return
		}
		w.Agg.Apply(ev)
		p.observeWM(partition, ev.EventTime, now)
		p.emitVerdict(ctx, Verdict{
			Event: ev, Decision: DecisionLateUpdate,
			Reason: fmt.Sprintf("event time %d <= watermark %d but window end %d > %d - %d; within allowedLateness, updating",
				ev.EventTime, p.wm, wEnd, p.wm, p.cfg.AllowedLateness),
			WatermarkBefore: wmBefore, WatermarkAfter: p.wm, Window: wk,
		})
		p.logger.LogAttrs(ctx, slog.LevelInfo, "late event updated finalized window",
			slog.String("event", ev.String()), slog.Int64("watermark", p.wm),
			slog.String("window", wk.String()), slog.Int64("count", w.Agg.EventCount))
		p.sendResult(ctx, WindowResult{
			Kind: ResultUpdate, Window: wk, Agg: w.Agg, Watermark: p.wm,
			Reason: "late event within allowedLateness corrected the finalized window",
		})
		p.purgeExpiredWindows(ctx)
		return
	}

	// 3) 正常计入活动窗口。
	w, ok := p.windows[wk]
	if !ok {
		w = &Window{Key: wk}
		p.windows[wk] = w
	}
	w.Agg.Apply(ev)
	p.observeWM(partition, ev.EventTime, now)

	p.emitVerdict(ctx, Verdict{
		Event: ev, Decision: DecisionAccepted,
		Reason:          fmt.Sprintf("window %s is open (wm=%d); event applied", wk, p.wm),
		WatermarkBefore: wmBefore, WatermarkAfter: p.wm, Window: wk,
	})
	p.logger.LogAttrs(ctx, slog.LevelDebug, "event accepted",
		slog.String("event", ev.String()), slog.Int64("wmBefore", wmBefore),
		slog.Int64("wmAfter", p.wm), slog.String("window", wk.String()))

	p.finalizeWindows(ctx)
	p.purgeExpiredWindows(ctx)
}

// observeWM 观察分区事件时间并在全局水位线推进时记录日志。
func (p *Processor) observeWM(partition int, eventTime int64, now time.Time) {
	old, new, advanced := p.tracker.Observe(partition, eventTime, now)
	if advanced {
		p.wm = new
		p.logger.Info("watermark advanced",
			"partition", partition, "old", wmString(old), "new", wmString(new))
	}
}

// handleComplete 处理分区 EOF：标记完成，必要时推进水位线并触发/清除窗口。
func (p *Processor) handleComplete(ctx context.Context, partition int) {
	old, new, advanced := p.tracker.Complete(partition)
	if advanced {
		p.wm = new
		p.logger.Info("watermark advanced after partition completion",
			"partition", partition, "old", wmString(old), "new", wmString(new))
	}
	p.logger.LogAttrs(ctx, slog.LevelInfo, "partition complete",
		slog.Int("partition", partition), slog.Int64("watermark", p.wm))
	p.finalizeWindows(ctx)
	p.purgeExpiredWindows(ctx)
}

// finalizeWindows 发出所有结束时间已被水位线越过、尚未最终化的窗口 FINAL 结果。
func (p *Processor) finalizeWindows(ctx context.Context) {
	if p.wm == NoWatermark {
		return
	}
	keys := make([]WindowKey, 0, len(p.windows))
	for k := range p.windows {
		keys = append(keys, k)
	}
	sort.Slice(keys, func(i, j int) bool {
		if keys[i].Start != keys[j].Start {
			return keys[i].Start < keys[j].Start
		}
		return keys[i].Symbol < keys[j].Symbol
	})
	for _, k := range keys {
		if p.finalized[k] {
			continue
		}
		if k.End <= p.wm {
			w := p.windows[k]
			p.finalized[k] = true
			p.sendResult(ctx, WindowResult{
				Kind: ResultFinal, Window: k, Agg: w.Agg, Watermark: p.wm,
				Reason: fmt.Sprintf("watermark %d reached window end %d", p.wm, k.End),
			})
			p.logger.LogAttrs(ctx, slog.LevelInfo, "window finalized",
				slog.String("window", k.String()), slog.Int64("watermark", p.wm),
				slog.Int64("count", w.Agg.EventCount), slog.Float64("vwap", w.Agg.VWAP()))
		}
	}
}

// purgeExpiredWindows 清除结束时间早于 watermark-allowedLateness 的窗口，
// 释放其状态；同时裁剪去重表，保证状态有界。
func (p *Processor) purgeExpiredWindows(ctx context.Context) {
	if p.wm == NoWatermark {
		return
	}
	boundary := p.wm - p.cfg.AllowedLateness
	for k := range p.windows {
		if k.End <= boundary {
			delete(p.windows, k)
			delete(p.finalized, k)
			p.logger.LogAttrs(ctx, slog.LevelInfo, "window purged after lateness grace",
				slog.String("window", k.String()), slog.Int64("boundary", boundary))
		}
	}
	if n := p.dedup.PurgeBefore(p.wm, p.cfg.AllowedLateness); n > 0 {
		p.logger.LogAttrs(ctx, slog.LevelDebug, "dedup table pruned",
			slog.Int("removed", n), slog.Int("remaining", p.dedup.Len()))
	}
}

// purgeAllWindows 在优雅关闭（所有分区 EOF，水位线 MaxWatermark）后兜底清空。
func (p *Processor) purgeAllWindows() {
	for k := range p.windows {
		delete(p.windows, k)
		delete(p.finalized, k)
	}
}

// applyIdleTick 用配置时钟的当前时间推进空闲检测；空闲使水位线推进后，
// 必须像普通事件一样触发窗口最终化与过期清除。
func (p *Processor) applyIdleTick() {
	ctx := context.Background()
	now := p.cfg.Clock.Now()
	old, new, advanced, ids := p.tracker.TickIdle(now)
	if len(ids) > 0 {
		p.logger.Info("partitions marked idle", "partitions", ids, "idleTimeout", p.cfg.IdleTimeout)
	}
	if advanced {
		p.wm = new
		p.logger.Info("watermark advanced after idleness", "old", wmString(old), "new", wmString(new))
		p.finalizeWindows(ctx)
		p.purgeExpiredWindows(ctx)
	}
}

// snapshot 在屏障点生成一致性快照。
func (p *Processor) snapshot() *Checkpoint {
	cp := NewCheckpoint()
	for id, pos := range p.positions {
		cp.Positions[id] = pos
	}
	for id, t := range p.dedup.seen {
		cp.Dedup[encodeDedupID(id)] = t
	}
	cp.Watermark = p.wm
	cp.MaxEventTime = p.tracker.snapshotMaxEventTime()
	cp.Windows = make([]checkpointWindow, 0, len(p.windows))
	for k, w := range p.windows {
		cp.Windows = append(cp.Windows, checkpointWindow{
			Symbol: k.Symbol, Start: k.Start, End: k.End,
			Agg: checkpointAgg{
				EventCount: w.Agg.EventCount, TotalQty: w.Agg.TotalQty,
				Notional: w.Agg.Notional, FirstTime: w.Agg.FirstTime, LastTime: w.Agg.LastTime,
			},
		})
	}
	sort.Slice(cp.Windows, func(i, j int) bool {
		if cp.Windows[i].Start != cp.Windows[j].Start {
			return cp.Windows[i].Start < cp.Windows[j].Start
		}
		return cp.Windows[i].Symbol < cp.Windows[j].Symbol
	})
	return cp
}

// Feed 为本地事件源的每个分区启动一个投递 goroutine，分区内严格保序；
// 队列满时 goroutine 阻塞在 Ingest，对上游形成背压（有界 goroutine 数）。
// 分区读完后自动 MarkComplete。返回的 channel 在所有分区投递结束后关闭，
// 调用方可据此在其后插入检查点屏障。
func (p *Processor) Feed(ctx context.Context, src *LocalSource) <-chan struct{} {
	allDone := make(chan struct{})
	var wg sync.WaitGroup
	for _, id := range src.Partitions() {
		wg.Add(1)
		go func(partition int) {
			defer wg.Done()
			for {
				ev, ok, err := src.Next(ctx, partition)
				if err != nil {
					return
				}
				if !ok {
					_ = p.MarkComplete(ctx, partition)
					return
				}
				if err := p.Ingest(ctx, ev); err != nil {
					return
				}
			}
		}(id)
	}
	go func() {
		wg.Wait()
		p.logger.Info("all partitions fed")
		close(allDone)
	}()
	return allDone
}

// Ingest 向管线提交一条事件并更新该分区已完全处理位置（在串行循环内完成）；
// 输入队列满时阻塞以形成背压。
func (p *Processor) Ingest(ctx context.Context, ev Event) error {
	return p.send(ctx, inMsg{ev: &ev, partition: ev.ID.Partition, ingestTime: p.cfg.Clock.Now()})
}

// MarkComplete 通知某分区已结束。
func (p *Processor) MarkComplete(ctx context.Context, partition int) error {
	return p.send(ctx, inMsg{partition: partition, complete: true})
}

// TickIdleNow 以配置时钟的当前时间驱动一次空闲检测（测试用确定性入口）。
func (p *Processor) TickIdleNow(ctx context.Context) error {
	return p.send(ctx, inMsg{idleTick: true})
}

// send 把消息放入有界输入队列。
func (p *Processor) send(ctx context.Context, m inMsg) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-p.done:
		return errors.New("processor closed")
	case p.in <- m:
		return nil
	}
}

// Checkpoint 在处理流中插入屏障，拿到与当前已入队并处理完事件一致的快照。
func (p *Processor) Checkpoint(ctx context.Context) (*Checkpoint, error) {
	req := &barrierReq{ch: make(chan error, 1)}
	if err := p.send(ctx, inMsg{barrier: req}); err != nil {
		return nil, err
	}
	if err := <-req.ch; err != nil {
		return nil, err
	}
	return req.cp, nil
}

// Restore 用检查点恢复偏移、去重表、水位线与窗口状态。必须在 Start 之前调用。
func (p *Processor) Restore(cp *Checkpoint) error {
	if cp == nil {
		return errors.New("nil checkpoint")
	}
	for id, pos := range cp.Positions {
		if _, ok := p.positions[id]; ok {
			p.positions[id] = pos
		}
	}
	p.dedup = NewDedupTable()
	for s, t := range cp.Dedup {
		id, err := decodeDedupKey(s)
		if err != nil {
			return fmt.Errorf("restore dedup: %w", err)
		}
		p.dedup.seen[id] = t
	}
	p.windows = make(map[WindowKey]*Window)
	p.finalized = make(map[WindowKey]bool)
	for _, cw := range cp.Windows {
		k := WindowKey{Symbol: cw.Symbol, Start: cw.Start, End: cw.End}
		p.windows[k] = &Window{Key: k, Agg: Aggregate{
			EventCount: cw.Agg.EventCount, TotalQty: cw.Agg.TotalQty,
			Notional: cw.Agg.Notional, FirstTime: cw.Agg.FirstTime, LastTime: cw.Agg.LastTime,
		}}
		if cw.End <= cp.Watermark {
			p.finalized[k] = true
		}
	}
	p.tracker.restoreState(cp.MaxEventTime, cp.Watermark)
	p.wm = cp.Watermark
	// 恢复后的进程相当于一次新的结果流：位点序列从 1 重新编号，
	// 结果历史与物化视图清空，避免与崩溃前进程的记录混叠。
	// 消费位点语义是进程内的；持久化跨进程续读由订阅者在取快照后续读。
	p.store = NewResultStore(p.cfg.ResultHistoryCapacity)
	p.logger.Info("processor restored from checkpoint",
		"watermark", wmString(cp.Watermark), "windows", len(p.windows), "dedup", p.dedup.Len())
	return nil
}

// Close 优雅关闭：优先把 shutdown 排入有界输入队列（排空已入队事件）；
// 若处理循环正停滞在无人消费的输出通道上导致 shutdown 无法入队，则关闭 stopCh
// 强制解除输出阻塞并停止循环。两种路径都会等待循环退出后关闭全部输出通道。
func (p *Processor) Close() error {
	p.closedOnce.Do(func() {
		select {
		case <-p.done:
		case p.in <- inMsg{shutdown: true}:
			<-p.done
		case <-time.After(gracefulCloseTimeout):
			p.stopCancelFn()
			<-p.done
		}
		close(p.results)
		close(p.rejections)
		close(p.verdicts)
		// 关闭结果订阅存储：所有订阅被唤醒，排空保留范围内剩余结果后收到 io.EOF，
		// 即使订阅者从不读取，也没有任何后台任务需要回收（拉模型无每订阅者 goroutine）。
		p.store.Close()
	})
	return nil
}

// gracefulCloseTimeout 是向下游输出排空 shutdown 的最长等待；
// 超时说明下游已停滞，转入强制停止。
const gracefulCloseTimeout = 250 * time.Millisecond

// stopCancelFn 关闭 stopCh（仅一次），解除所有停滞在输出发送上的操作。
func (p *Processor) stopCancelFn() {
	select {
	case <-p.stopCh:
	default:
		close(p.stopCh)
	}
}

// Wait 等待处理循环退出。
func (p *Processor) Wait() { <-p.done }

// Results 返回窗口最终/更正结果通道（关闭时由消费者侧自行 drain 或停止）。
func (p *Processor) Results() <-chan WindowResult { return p.results }

// SubscribeResults 从上一次确认位点之后订阅窗口结果：返回的订阅首先交付
// Seq=after+1 的结果。after=0 从头订阅。结果按位点严格有序，FINAL/UPDATE
// 的先后关系与产生顺序完全一致；订阅为纯拉取，不建立后台 goroutine/队列。
//
// 若 after+1 已滑出有界保留范围，返回包装了 ErrResultPositionExpired 的错误，
// 调用方应改用 ResultSnapshot 取完整视图后从快照位点续读。
func (p *Processor) SubscribeResults(after ResultSeq) (*Subscription, error) {
	return p.store.Subscribe(after)
}

// ResultSnapshot 在串行处理流中插入屏障，返回与"当前已处理结果"严格对齐的
// 完整结果视图（每个窗口最新值，确定性排序）。视图应用后，用
// SubscribeResults(snap.Seq) 续读 Seq>snap.Seq 的全部增量；
// 快照+增量与从不掉线一直消费得到的结果逐窗口逐字段一致。
func (p *Processor) ResultSnapshot(ctx context.Context) (*ResultSnapshot, error) {
	req := &barrierReq{wantSnap: true, ch: make(chan error, 1)}
	if err := p.send(ctx, inMsg{barrier: req}); err != nil {
		return nil, err
	}
	if err := <-req.ch; err != nil {
		return nil, err
	}
	return req.snap, nil
}

// ResultStore 暴露底层有界结果存储（供测试与需要直接取非屏障快照的调用方使用）。
func (p *Processor) ResultStore() *ResultStore { return p.store }

// Rejections 返回超宽限迟到事件的侧输出通道。
func (p *Processor) Rejections() <-chan LateRejection { return p.rejections }

// Verdicts 返回逐条事件判定通道。
func (p *Processor) Verdicts() <-chan Verdict { return p.verdicts }

// CurrentWatermark 返回当前全局水位线（通过屏障读取，线程安全）。
func (p *Processor) CurrentWatermark(ctx context.Context) (int64, error) {
	cp, err := p.Checkpoint(ctx)
	if err != nil {
		return NoWatermark, err
	}
	return cp.Watermark, nil
}

func (p *Processor) emitVerdict(ctx context.Context, v Verdict) {
	// 订阅专用模式下判定通道只是旁路：尽力而为、绝不阻塞处理循环。
	if p.cfg.ResultOutputMode == ResultOutputSubscriptionOnly {
		select {
		case p.verdicts <- v:
		default:
		}
	} else {
		select {
		case <-ctx.Done():
		case <-p.stopCh:
		case p.verdicts <- v:
		}
	}
	p.logger.LogAttrs(ctx, slog.LevelInfo, "event decision",
		slog.String("event", v.Event.String()),
		slog.String("decision", string(v.Decision)),
		slog.Int64("eventTime", v.Event.EventTime),
		slog.Int64("wmBefore", v.WatermarkBefore),
		slog.Int64("wmAfter", v.WatermarkAfter),
		slog.String("window", v.Window.String()),
		slog.String("reason", v.Reason))
}

func (p *Processor) sendResult(ctx context.Context, r WindowResult) {
	if p.cfg.ResultOutputMode == ResultOutputSubscriptionOnly {
		// 订阅专用模式：ResultStore 是权威输出面。先登记历史（O(1)、绝不阻塞），
		// 保证只走订阅、完全不读旧通道时结果也按位点持续交付、屏障/快照/关闭不卡。
		// 旧 Results 通道退化为尽力而为的旁路，缓冲满即丢，不影响权威订阅面。
		p.store.Append(r)
		select {
		case p.results <- r:
		default:
		}
		return
	}
	select {
	case <-ctx.Done():
	case <-p.stopCh:
	case p.results <- r:
		// 仅在结果确已交付到旧输出面（Results 通道）后再登记到订阅历史，
		// 保证两个输出面看到的结果集合一致，订阅历史不会多出"未交付"结果。
		// 追加为 O(1) 且不阻塞，慢订阅者的等待不影响处理循环。
		p.store.Append(r)
	}
}

func (p *Processor) sendRejection(ctx context.Context, r LateRejection) {
	if p.cfg.ResultOutputMode == ResultOutputSubscriptionOnly {
		select {
		case p.rejections <- r:
		default:
		}
		return
	}
	select {
	case <-ctx.Done():
	case <-p.stopCh:
	case p.rejections <- r:
	}
}

// wmString 格式化水位线（含 ±∞ 语义），供日志阅读。
func wmString(wm int64) string {
	switch wm {
	case NoWatermark:
		return "-inf"
	case MaxWatermark:
		return "+inf"
	default:
		return fmt.Sprintf("%d", wm)
	}
}
