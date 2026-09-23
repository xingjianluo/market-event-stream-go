package pipeline

import (
	"context"
	"errors"
	"sync"
)

// inMsg 是源 -> 聚合器的内部消息。
type inMsg struct {
	p          PartitionID
	offset     int64
	entry      Entry
	eof        bool          // 该分区日志已读到结尾（该分区永久退出全局水位线最小值）
	sourceDone bool          // 所有分区日志均已投递完毕（协调器发出，触发最终冲刷）
	barrier    bool          // 显式脉冲栅栏
	ack        chan struct{} // 栅栏 ack：聚合器处理完栅栏前全部内容后关闭
	seq        int64         // 协调器分配的全局投递序号（确定性全序）
}

// Pipeline 是本地行情事件流处理管线。
//
// 拓扑（背压路径自右向左传播）：
//
//	确定性源协调器（单 goroutine，按分区公平轮转合并）
//	  -> 有界输入队列 inputCh
//	    -> 单个 aggregator goroutine（全部状态的唯一持有者，无锁）
//	      -> 有界结果队列 / 有界侧输出队列（慢消费者会一路反压到源）
//
// 内存与跑动任务有界性：
//   - 源与聚合器各只有一个 goroutine，不随流量/分区数无界增长任务；
//   - 已投递未处理条目数 <= inputCh 容量（背压时源阻塞，不额外缓存）；
//   - 窗口与去重条目随水位线清理（purgeExpired/dropWindow），不随历史无限累积；
//   - 输出队列有界，慢消费者通过阻塞反压，不会用无限缓冲掩盖压力。
//
// 确定性：源的轮转顺序、每轮批量、水位线都由日志内容决定，不含处理时钟，
// 因此同一批事件无论批大小、外部调度如何，聚合结论一致可复现。
type Pipeline struct {
	cfg      Config
	logs     map[PartitionID]*PartitionLog
	logOrder []*PartitionLog
	logger   func(string, ...any)

	inputCh chan inMsg
	cpCh    chan cpReq
	resCh   chan WindowResult
	sideCh  chan SideRecord

	aggDone chan struct{}
	runErr  error

	statsMu   sync.RWMutex
	stats     Stats
	finalSnap *snapshot
}

// New 用给定配置与分区日志构造管线。分区顺序即源轮转顺序。
func New(cfg Config, logs []*PartitionLog, logger func(string, ...any)) *Pipeline {
	if logger == nil {
		logger = func(string, ...any) {}
	}
	m := make(map[PartitionID]*PartitionLog, len(logs))
	for _, l := range logs {
		m[l.id] = l
	}
	return &Pipeline{
		cfg: cfg, logs: m, logOrder: logs, logger: logger,
		inputCh: make(chan inMsg, cfg.InputQueueCap),
		cpCh:    make(chan cpReq),
		resCh:   make(chan WindowResult, cfg.OutputQueueCap),
		sideCh:  make(chan SideRecord, cfg.OutputQueueCap),
		aggDone: make(chan struct{}),
	}
}

func validateConfig(c *Config) error {
	if c.WindowSize <= 0 {
		return errors.New("WindowSize 必须 > 0")
	}
	if c.AllowedLateness < 0 {
		return errors.New("AllowedLateness 不能为负")
	}
	if c.InputQueueCap <= 0 {
		return errors.New("InputQueueCap 必须 > 0")
	}
	if c.OutputQueueCap <= 0 {
		return errors.New("OutputQueueCap 必须 > 0")
	}
	if c.MaxInFlight <= 0 {
		return errors.New("MaxInFlight 必须 > 0")
	}
	if c.MergeBatchSize <= 0 {
		return errors.New("MergeBatchSize 必须 > 0")
	}
	return nil
}

// Run 启动管线并阻塞到所有分区日志消费完毕或 ctx 取消。
// 若配置了检查点目录且存在快照，则从快照恢复偏移与全部状态。
func (p *Pipeline) Run(ctx context.Context) error {
	if err := validateConfig(&p.cfg); err != nil {
		return err
	}

	store := newCheckpointStore(p.cfg.CheckpointDir)
	var restored *snapshot
	if p.cfg.CheckpointDir != "" {
		snap, err := store.load(p.cfg)
		if err != nil {
			return err
		}
		restored = snap
		if snap != nil {
			p.logger("从检查点恢复: takenAt=%s 全局水位线=%s 窗口数=%d 统计=%+v",
				snap.TakenAt.Format("2006-01-02T15:04:05.999999999Z07:00"),
				snap.WM.Global.Format("2006-01-02T15:04:05.999999999Z07:00"),
				len(snap.Windows), snap.Stats)
		}
	}

	// 恢复后从各分区已确认偏移的下一条继续；重放条目由 EventID 去重。
	startOffsets := make(map[PartitionID]int64, len(p.logOrder))
	for _, l := range p.logOrder {
		if restored != nil {
			if off, ok := restored.Offsets[l.id]; ok {
				startOffsets[l.id] = off + 1
			}
		}
	}

	go p.runCoordinator(ctx, startOffsets)
	go p.runAggregator(ctx, restored, store)

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-p.aggDone:
		return p.runErr
	}
}

// runCoordinator 是确定性本地源：
//   - 按分区注册顺序公平轮转，每轮每分区投递至多 cfg.MergeBatchSize 个条目
//     （事件与标记一视同仁），读完发 EOF；
//   - 全部条目经有界 inputCh 投递，通道满即阻塞形成背压，
//     已投递未处理条目数被 inputCh 容量严格限定；
//   - 公平轮转保证任一分区的水位线声明最多领先其它分区一个批次：
//     在"水位线标记只承诺标记之前的事件"这一有效日志前提下，
//     改批大小或轮转起点只会改变中间帧，不改变同一批事件的最终结论。
func (p *Pipeline) runCoordinator(ctx context.Context, start map[PartitionID]int64) {
	off := make(map[PartitionID]int64, len(p.logOrder))
	for _, l := range p.logOrder {
		off[l.id] = start[l.id]
	}
	// off[id] == -1 是唯一"已读完且 EOF 已发"状态。
	remaining := len(p.logOrder)

	var seq int64
	send := func(m inMsg) bool {
		m.seq = seq
		seq++
		select {
		case p.inputCh <- m:
			return true
		case <-ctx.Done():
			return false
		}
	}
	sendEOF := func(l *PartitionLog) bool {
		if !send(inMsg{p: l.id, eof: true}) {
			return false
		}
		off[l.id] = -1
		remaining--
		return true
	}

	// 投递阶段：跨分区公平轮转。读到日志末尾立即发 EOF（该分区永久退出
	// 全局水位线最小值），不影响仍在投递的其它分区；聚合器只在收到
	// sourceDone 后才做 +Inf 最终冲刷，因此短分区提前 EOF 不会提前关窗。
	for remaining > 0 {
		progressed := false
		for _, l := range p.logOrder {
			if off[l.id] < 0 {
				continue
			}
			for n := 0; n < p.cfg.MergeBatchSize; n++ {
				o := off[l.id]
				if o >= int64(len(l.entries)) {
					if !sendEOF(l) {
						return
					}
					progressed = true
					break
				}
				ent := l.entries[o]
				if ent.Kind == EntryBarrier {
					// 栅栏是全局对齐点：先把所有分区栅栏前的条目（及短分区
					// 的 EOF）排空入队，再把带 ack 的栅栏消息入队并同步等待。
					// 栅栏消息位于 inputCh 全序中，聚合器处理到它时会完成
					// 水位线推进、窗口触发/清理后再 close(ack)，因此从这里
					// 返回时栅栏后的事件一定以"栅栏已生效"的状态被判定。
					if !p.flushBeforeBarriers(off, send, func(id PartitionID) {
						off[id] = -1
						remaining--
					}) {
						return
					}
					ack := make(chan struct{})
					if !send(inMsg{p: l.id, offset: o, barrier: true, ack: ack}) {
						return
					}
					select {
					case <-ack:
					case <-ctx.Done():
						return
					}
					off[l.id] = o + 1
					progressed = true
					continue
				}
				if !send(inMsg{p: l.id, offset: o, entry: ent}) {
					return
				}
				off[l.id] = o + 1
				progressed = true
				if off[l.id] >= int64(len(l.entries)) {
					if !sendEOF(l) {
						return
					}
					break
				}
			}
		}
		if !progressed {
			return // 防御性退出（正常逻辑下不会走到）
		}
	}
	// 所有分区条目与 EOF 都已投递：发源完成信号，触发最终冲刷。
	select {
	case p.inputCh <- inMsg{sourceDone: true, seq: seq}:
	case <-ctx.Done():
	}
}

// Results 返回窗口结果通道（首帧 + 迟到修正帧，按确定性顺序）。
func (p *Pipeline) Results() <-chan WindowResult { return p.resCh }

// SideOutput 返回侧输出通道：迟到（范围内/超范围）与重复投递的判定留痕。
func (p *Pipeline) SideOutput() <-chan SideRecord { return p.sideCh }

// Checkpoint 触发一次同步检查点，返回的快照即落盘内容。
func (p *Pipeline) Checkpoint() (*snapshot, error) {
	done := make(chan error, 1)
	req := cpReq{done: done}
	select {
	case p.cpCh <- req:
		if err := <-done; err != nil {
			return nil, err
		}
		return req.snap, nil
	case <-p.aggDone:
		return nil, errors.New("pipeline 已结束")
	}
}

// Stats 返回最近一次聚合器发布的统计（结束后为最终统计）。
func (p *Pipeline) Stats() Stats {
	p.statsMu.RLock()
	defer p.statsMu.RUnlock()
	return p.stats
}

// publishStats 由聚合器（唯一写者）在每个处理步之后调用，
// 在写锁内发布统计，保证与 Stats() 的读方同步，无数据竞争。
func (p *Pipeline) publishStats(s Stats) {
	p.statsMu.Lock()
	p.stats = s
	p.statsMu.Unlock()
}

// Snapshot 返回管线最终一致性状态快照（Run 正常结束后可用）。
func (p *Pipeline) Snapshot() *snapshot { return p.finalSnap }

// flushBeforeBarriers 在全局栅栏点之前，把每个分区"下一个栅栏（不含）
// 或日志末尾"之前的全部普通条目按轮转顺序投递；读到末尾的分区立即发 EOF。
// onEOF 用于同步协调器的剩余分区计数。投递受有界 inputCh 约束，自动背压。
func (p *Pipeline) flushBeforeBarriers(off map[PartitionID]int64,
	send func(inMsg) bool, onEOF func(PartitionID)) bool {
	for {
		any := false
		for _, l := range p.logOrder {
			o := off[l.id]
			if o < 0 {
				continue
			}
			if o >= int64(len(l.entries)) {
				if !send(inMsg{p: l.id, eof: true}) {
					return false
				}
				onEOF(l.id)
				any = true
				continue
			}
			if l.entries[o].Kind == EntryBarrier {
				continue
			}
			if !send(inMsg{p: l.id, offset: o, entry: l.entries[o]}) {
				return false
			}
			off[l.id] = o + 1
			if off[l.id] >= int64(len(l.entries)) {
				if !send(inMsg{p: l.id, eof: true}) {
					return false
				}
				onEOF(l.id)
			}
			any = true
		}
		if !any {
			return true
		}
	}
}
