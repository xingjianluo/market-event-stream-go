package marketstream

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"sort"
	"sync"
)

// Position 是窗口结果在结果日志中的稳定、单调递增消费位点（序号）。
// 第一条结果序号为 1；Position(0) 表示“从头开始 / 尚无结果”。
// 从位点 P 续读会严格交付序号 > P 的结果，因此订阅者只需持久化
// “最后一条已处理结果的序号”，重连后交给 Subscribe 即可。
type Position int64

// SequencedResult 是带上稳定位点的窗口结果（FINAL 或 UPDATE 更正）。
// 位点在单个结果总线生命周期内全局单调、严格按处理器发出顺序分配，
// 因此同一窗口的“更正链”先后关系在位点序上天然保留，不会旧值盖新值。
type SequencedResult struct {
	Position Position
	Result   WindowResult
}

// SnapshotWindow 是快照视图中单个窗口的条目：该窗口在快照边界前的最新一条结果
// （FINAL，或覆盖它的最后一条 UPDATE）。
type SnapshotWindow struct {
	Position Position // 该窗口最新一条结果的序号
	Result   WindowResult
}

// ResultSnapshot 是某个位点边界上的完整结果视图。
// 不变式：把视图按窗口身份装入，再从 Subscribe(Position) 续读全部增量，
// 得到的每窗口最终状态与“从不断线、从序号 1 连续消费”严格逐字段一致。
type ResultSnapshot struct {
	Position Position         // 快照边界：结果日志中已包含的最后序号
	Oldest   Position         // 生成快照时可续读的最旧位点（最早保留结果的前一位点）
	Windows  []SnapshotWindow // 视图：每个窗口的最新结果，按 (Start, Symbol) 升序
}

// Lookup 按窗口身份（symbol+窗口起止）返回快照中的视图条目。
func (s *ResultSnapshot) Lookup(k WindowKey) (SnapshotWindow, bool) {
	i := sort.Search(len(s.Windows), func(i int) bool {
		w := s.Windows[i].Result.Window
		if w.Start != k.Start {
			return w.Start >= k.Start
		}
		return w.Symbol >= k.Symbol
	})
	if i < len(s.Windows) && s.Windows[i].Result.Window == k {
		return s.Windows[i], true
	}
	return SnapshotWindow{}, false
}

// 结果订阅相关的可区分哨兵错误。订阅者必须按错误类型选择不同恢复动作，
// 任何路径都不会静默漏掉或悄悄跳过一条结果。
var (
	// ErrPositionExpired：请求续读的位点（或订阅者当前进度）已落在可保留历史范围之外
	// （环形历史已轮转越过）。恢复方式：重新 Snapshot，应用视图后从快照边界 Subscribe 续读。
	ErrPositionExpired = errors.New("result position expired: outside retained history; take a fresh snapshot and resume")
	// ErrTooManySubscribers：在线订阅者数达到 ResultBusConfig.MaxSubscribers 上限。
	ErrTooManySubscribers = errors.New("too many result subscribers")
	// ErrSubscriptionClosed：订阅已被显式 Close（或已被总线摘除）。
	ErrSubscriptionClosed = errors.New("result subscription closed")
	// ErrBusClosed：结果总线已随上游结果通道关闭而停止；已保留结果可继续排空，之后得到此错误。
	ErrBusClosed = errors.New("result bus closed")
	// ErrBusStarted：结果总线已挂接过大结果源（每个总线只能 Start 一次）。
	ErrBusStarted = errors.New("result bus already started")
	// ErrInvalidBusConfig：结果总线配置非法。
	ErrInvalidBusConfig = errors.New("invalid result bus config")
)

const (
	// defaultHistoryCapacity 是结果历史环形缓冲的默认条数上限。
	defaultHistoryCapacity = 4096
	// defaultMaxSubscribers 是在线订阅者的默认数量上限。
	defaultMaxSubscribers = 256
)

// ResultBusConfig 是结果订阅/审计回放层的容量配置。
// 全部容量均为有界上界：慢订阅者或突发流量不会拖垮处理管线，
// 也不会使内存或后台任务随运行时间无界增长。
type ResultBusConfig struct {
	// HistoryCapacity 是结果历史保留的最大结果条数（环形，按条数有界）。
	// 同时决定：可回放范围、单订阅者落后容忍度、快照视图覆盖范围。
	HistoryCapacity int
	// MaxSubscribers 是同时在线订阅者数量上限；<=0 使用默认上限。
	MaxSubscribers int
	// Logger 注入结构化日志；为 nil 时丢弃日志。
	Logger *slog.Logger
}

// DefaultResultBusConfig 返回默认结果总线配置。
func DefaultResultBusConfig() ResultBusConfig {
	return ResultBusConfig{
		HistoryCapacity: defaultHistoryCapacity,
		MaxSubscribers:  defaultMaxSubscribers,
	}
}

// Validate 校验结果总线配置（零值字段表示“使用默认值”，由 NewResultBus 填充）。
func (c ResultBusConfig) Validate() error {
	if c.HistoryCapacity < 0 {
		return fmt.Errorf("%w: HistoryCapacity must be >= 0 (0 means default), got %d", ErrInvalidBusConfig, c.HistoryCapacity)
	}
	if c.MaxSubscribers < 0 {
		return fmt.Errorf("%w: MaxSubscribers must be >= 0 (0 means default), got %d", ErrInvalidBusConfig, c.MaxSubscribers)
	}
	return nil
}

// ResultBus 是可断点续读的结果订阅与审计回放层。
//
// 它以单 goroutine 挂接处理器的结果通道（不在处理路径上加锁或阻塞，
// 因而原有背压语义完全不变），为每条结果分配稳定位点，并在短临界区内维护：
//   - 定容环形结果历史（有界回放）；
//   - 每窗口最新结果视图（快照）；
//   - 若干按位点拉取的订阅（订阅自身不缓存结果，落后即得到可区分的过期结论）。
type ResultBus struct {
	cfg    ResultBusConfig
	logger *slog.Logger

	mu      sync.Mutex
	core    busCore
	started bool
	closed  bool
	done    chan struct{}
}

// busCore 是受 ResultBus.mu 保护的全部可变状态。
type busCore struct {
	ring    []SequencedResult // 定容环形缓冲
	headIdx int               // ring 中最旧结果的下标
	count   int               // ring 中当前条数
	headSeq Position          // ring 中最旧结果的序号（count==0 时为下一条将写入的序号）
	tail    Position          // 最新一条结果的序号

	// 每窗口在环形历史中的位点 FIFO 队列（全局按序淘汰 => 每窗口也按序淘汰），
	// 队尾即该窗口最新结果；队列总长恒等于 ring.count，故内存有界。
	views  map[WindowKey]*posQueue
	latest map[WindowKey]SequencedResult

	subs   map[int]*subState
	nextID int
}

// posQueue 是 Position 的有界 FIFO（带周期性压缩，避免切片头部长期泄漏）。
type posQueue struct {
	s []Position
	h int
}

func (q *posQueue) push(p Position) {
	if q.h > 0 {
		q.compact()
	}
	q.s = append(q.s, p)
}

func (q *posQueue) popOldest() Position {
	p := q.s[q.h]
	q.s[q.h] = 0
	q.h++
	q.compact()
	return p
}

func (q *posQueue) compact() {
	if q.h == 0 {
		return
	}
	if q.h < len(q.s)/2 && q.h < 16 {
		return
	}
	q.s = append([]Position(nil), q.s[q.h:]...)
	q.h = 0
}

func (q *posQueue) len() int { return len(q.s) - q.h }

// subState 是单个订阅的可变状态（总线与 Subscription 共享同一指针）。
type subState struct {
	id        int
	next      Position // 下一条待交付结果的序号
	committed Position // 订阅者已确认的最后位点
	wake      chan struct{}
	closed    bool // 显式 Close
	expired   bool // 历史轮转越过 next，已被摘除
}

// NewResultBus 按配置创建结果总线（尚未挂接结果源）。
// 零值字段按默认容量填充；显式负值由 Validate 拒绝。
func NewResultBus(cfg ResultBusConfig) (*ResultBus, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	if cfg.HistoryCapacity == 0 {
		cfg.HistoryCapacity = defaultHistoryCapacity
	}
	if cfg.MaxSubscribers == 0 {
		cfg.MaxSubscribers = defaultMaxSubscribers
	}
	logger := cfg.Logger
	if logger == nil {
		logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	}
	b := &ResultBus{
		cfg:    cfg,
		logger: logger,
		done:   make(chan struct{}),
	}
	b.core.ring = make([]SequencedResult, cfg.HistoryCapacity)
	b.core.headSeq = 1
	b.core.views = make(map[WindowKey]*posQueue)
	b.core.latest = make(map[WindowKey]SequencedResult)
	b.core.subs = make(map[int]*subState)
	return b, nil
}

// Start 挂接一个结果通道并启动唯一的分发 goroutine；通道关闭后总线自动停止
// （排空通道内残余结果、唤醒全部等待者并关闭 done）。每个总线只能启动一次。
func (b *ResultBus) Start(results <-chan WindowResult) error {
	b.mu.Lock()
	if b.started {
		b.mu.Unlock()
		return ErrBusStarted
	}
	b.started = true
	b.mu.Unlock()
	go b.feed(results)
	return nil
}

// Attach 把总线挂接到处理器的结果输出通道并启动。
// 挂接后结果由总线独占消费；处理器本身的背压、检查点、迟到语义均不受影响。
func (b *ResultBus) Attach(p *Processor) error {
	return b.Start(p.Results())
}

// feed 是总线唯一的后台 goroutine：数量恒为 1，不随订阅者或结果数增长。
func (b *ResultBus) feed(results <-chan WindowResult) {
	defer close(b.done)
	for r := range results {
		b.append(r)
	}
	b.mu.Lock()
	b.closed = true
	for _, st := range b.core.subs {
		select {
		case st.wake <- struct{}{}:
		default:
		}
	}
	b.mu.Unlock()
}

// append 为一条结果分配位点、写入环形历史并更新视图；历史满时按 FIFO 淘汰，
// 并主动把落后到被淘汰范围之外的订阅标记过期摘除（内存与订阅表严格有界）。
// 全程非阻塞：任何订阅者不读都不会卡住处理器或本 goroutine。
func (b *ResultBus) append(r WindowResult) {
	b.mu.Lock()
	defer b.mu.Unlock()
	c := &b.core

	if c.count == cap(c.ring) {
		evicted := c.ring[c.headIdx]
		c.ring[c.headIdx] = SequencedResult{}
		c.headIdx = (c.headIdx + 1) % cap(c.ring)
		c.count--
		c.headSeq++
		b.evictView(evicted)
	}

	seq := c.tail + 1
	entry := SequencedResult{Position: seq, Result: r}
	idx := (c.headIdx + c.count) % cap(c.ring)
	c.ring[idx] = entry
	c.count++
	c.tail = seq

	k := r.Window
	q := c.views[k]
	if q == nil {
		q = &posQueue{}
		c.views[k] = q
	}
	q.push(seq)
	c.latest[k] = entry

	b.expireSubsLocked()
	for _, st := range c.subs {
		select {
		case st.wake <- struct{}{}:
		default:
		}
	}
}

// evictView 在一条最旧结果被淘汰时，同步收缩该窗口的位点队列与最新视图。
func (b *ResultBus) evictView(ev SequencedResult) {
	k := ev.Result.Window
	q := b.core.views[k]
	if q == nil || q.len() == 0 {
		return
	}
	_ = q.popOldest() // 全局 FIFO => 队首必然是被淘汰位点
	if q.len() == 0 {
		delete(b.core.views, k)
		delete(b.core.latest, k)
	} else {
		b.core.latest[k] = b.core.at(q.s[len(q.s)-1]) // 队尾始终是该窗口最新位点
	}
}

// expireSubsLocked 摘除进度已落在新最旧位点之前的订阅，唤醒其等待并给出过期结论。
func (b *ResultBus) expireSubsLocked() {
	for id, st := range b.core.subs {
		if !st.closed && !st.expired && st.next < b.core.headSeq {
			st.expired = true
			delete(b.core.subs, id)
			select {
			case st.wake <- struct{}{}:
			default:
			}
		}
	}
}

// at 返回指定位点的结果（仅在该位点仍处于环形历史内时调用）。
func (c *busCore) at(seq Position) SequencedResult {
	idx := (c.headIdx + int(seq-c.headSeq)) % cap(c.ring)
	return c.ring[idx]
}

// Wait 等待分发 goroutine 退出。上游结果通道关闭后，本方法必然在有限时间内返回：
// 总线从不对上游反压，残余结果只是 O(通道缓冲) 的有界排空。
func (b *ResultBus) Wait() { <-b.done }

// Subscribe 从位点 from 开始订阅：严格交付序号 > from 的结果。
//   - from=0：从第一条结果（序号 1）开始；首条结果已被淘汰时同样返回过期；
//     想从“当前仍保留的最早结果”开始请传 OldestPosition()；
//   - from 早于 OldestPosition()：返回 ErrPositionExpired（请改取新快照恢复）；
//   - from 晚于当前尾端：不夹取、也不跳结果，保留该位点并阻塞等待，
//     直到日志推进到位点 from（位点是绝对序号，适合总线刚启动就提前注册的订阅者）。
func (b *ResultBus) Subscribe(from Position) (*Subscription, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	c := &b.core

	if from < c.headSeq-1 {
		return nil, fmt.Errorf("%w: requested=%d oldest=%d", ErrPositionExpired, from, c.headSeq-1)
	}
	if len(c.subs) >= b.cfg.MaxSubscribers {
		return nil, fmt.Errorf("%w: limit=%d", ErrTooManySubscribers, b.cfg.MaxSubscribers)
	}
	c.nextID++
	st := &subState{
		id:   c.nextID,
		next: from + 1,
		wake: make(chan struct{}, 1),
	}
	c.subs[st.id] = st
	sub := &Subscription{bus: b, st: st}
	b.logger.Info("result subscriber added",
		"sub", st.id, "from", from, "next", st.next, "subscribers", len(c.subs))
	return sub, nil
}

// Snapshot 在当前日志尾端生成一致性完整视图。
// 屏障语义：视图恰好包含位点 <= Position 的全部结果按窗口折叠后的最新值，
// 与 Subscribe(Position) 之后收到的增量在边界上不重不漏。
// 视图仅覆盖回放范围内仍保留结果的窗口（受 HistoryCapacity 约束）。
func (b *ResultBus) Snapshot() *ResultSnapshot {
	b.mu.Lock()
	defer b.mu.Unlock()
	c := &b.core

	snap := &ResultSnapshot{
		Position: c.tail,
		Oldest:   c.headSeq - 1,
		Windows:  make([]SnapshotWindow, 0, len(c.latest)),
	}
	for _, e := range c.latest {
		snap.Windows = append(snap.Windows, SnapshotWindow{Position: e.Position, Result: e.Result})
	}
	sort.Slice(snap.Windows, func(i, j int) bool {
		a, wj := snap.Windows[i].Result.Window, snap.Windows[j].Result.Window
		if a.Start != wj.Start {
			return a.Start < wj.Start
		}
		return a.Symbol < wj.Symbol
	})
	return snap
}

// CurrentPosition 返回结果日志当前尾端位点（最新一条结果的序号；0 表示尚无结果）。
func (b *ResultBus) CurrentPosition() Position {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.core.tail
}

// OldestPosition 返回可续读的最旧位点边界：它是仍保留的最早一条结果的前一位点，
// 传给 Subscribe 即从最早保留结果开始；传入更小的位点会得到 ErrPositionExpired。
func (b *ResultBus) OldestPosition() Position {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.core.headSeq - 1
}

// HistoryLen 返回当前环形历史中的结果条数（<= HistoryCapacity）。
func (b *ResultBus) HistoryLen() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.core.count
}

// Subscribers 返回当前在线（未关闭、未过期摘除）的订阅者数。
func (b *ResultBus) Subscribers() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return len(b.core.subs)
}

// Subscription 是一个按稳定位点拉取的结果订阅句柄。
// 订阅本身不缓存结果：所有读取直接命中总线的有界共享历史，
// 因而订阅者数量与缓冲占用都有上界，完全不读者也不会拖垮管线。
type Subscription struct {
	bus *ResultBus
	st  *subState
}

// Next 返回下一条结果。严格按位点顺序交付，且同一位点在单个订阅生命周期内
// 至多交付一次（调用方崩溃重连属于新订阅，应从已确认位点重新订阅）。
// 返回值结论可区分：
//   - 正常：位点严格递增的 SequencedResult；
//   - ErrPositionExpired：历史轮转已越过本订阅进度，必须重新取快照恢复；
//   - ErrBusClosed：上游已关闭且历史已排空（此前仍会先排空全部保留结果）；
//   - ErrSubscriptionClosed：本订阅已被 Close；
//   - ctx.Err()：等待新结果期间被取消（订阅仍有效，可继续 Next 或稍后重连）。
func (s *Subscription) Next(ctx context.Context) (SequencedResult, error) {
	for {
		s.bus.mu.Lock()
		c := &s.bus.core
		switch {
		case s.st.closed:
			s.bus.mu.Unlock()
			return SequencedResult{}, ErrSubscriptionClosed
		case s.st.expired:
			s.bus.mu.Unlock()
			return SequencedResult{}, fmt.Errorf("%w: next=%d oldest=%d",
				ErrPositionExpired, s.st.next, c.headSeq)
		case s.st.next < c.headSeq:
			// 兜底：未被主动唤醒覆盖到的摘除路径（理论上 expireSubsLocked 已处理）。
			s.st.expired = true
			delete(c.subs, s.st.id)
			s.bus.mu.Unlock()
			return SequencedResult{}, fmt.Errorf("%w: next=%d oldest=%d",
				ErrPositionExpired, s.st.next, c.headSeq)
		case s.st.next <= c.tail:
			e := c.at(s.st.next)
			s.st.next++
			s.bus.mu.Unlock()
			return e, nil
		}
		closed := s.bus.closed
		ch := s.st.wake
		s.bus.mu.Unlock()

		if closed {
			return SequencedResult{}, ErrBusClosed
		}
		select {
		case <-ctx.Done():
			return SequencedResult{}, ctx.Err()
		case <-ch:
		}
	}
}

// Ack 确认已处理到位点 pos（幂等、单调取最大、可任意重复调用），
// 返回当前已确认位点。确认只是订阅方持久化消费进度的本地记账，
// 总线从不因已确认而删除结果（淘汰只由 HistoryCapacity 决定）。
func (s *Subscription) Ack(pos Position) Position {
	s.bus.mu.Lock()
	defer s.bus.mu.Unlock()
	if pos > s.st.committed {
		s.st.committed = pos
	}
	return s.st.committed
}

// Committed 返回最后确认的位点。
func (s *Subscription) Committed() Position {
	s.bus.mu.Lock()
	defer s.bus.mu.Unlock()
	return s.st.committed
}

// NextPosition 返回下一次将交付的位点（已交付最后位点 + 1），
// 也是订阅者掉线后应持久化并重连提交的续读位点。
func (s *Subscription) NextPosition() Position {
	s.bus.mu.Lock()
	defer s.bus.mu.Unlock()
	return s.st.next
}

// Close 注销订阅并释放其在总线中的登记（幂等）。被阻塞的 Next 会得到
// ErrSubscriptionClosed。注意：关闭一个订阅不影响总线与其他订阅。
func (s *Subscription) Close() error {
	s.bus.mu.Lock()
	defer s.bus.mu.Unlock()
	if s.st.closed {
		return nil
	}
	s.st.closed = true
	delete(s.bus.core.subs, s.st.id)
	close(s.st.wake)
	return nil
}
