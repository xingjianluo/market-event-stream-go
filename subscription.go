// subscription.go：可断点续读的窗口结果订阅与审计回放。
//
// 在串行管线之外叠加一层共享的、有界的结果历史（ResultStore）：
// 每条窗口结果（FINAL/UPDATE）在产生时获得一个单调递增、永不复用的
// 稳定消费位点（ResultSeq）。订阅者按"上一次确认位点之后"拉取结果，
// 掉线/重启后可精确续读；新订阅者或落后过多的订阅者可先取某一位点的
// 完整结果快照（ResultSnapshot），再从该位点续读增量。
package marketstream

import (
	"context"
	"errors"
	"io"
	"sort"
	"sync"
)

// ResultSeq 是窗口结果的稳定消费位点：从 1 开始单调递增、绝不复用。
// 0 表示"流的起点之前"，从头订阅即 SubscribeResults(0)。
type ResultSeq int64

// ResultRecord 是带稳定位点的窗口结果，订阅/快照/审计均以它为单位。
type ResultRecord struct {
	Seq    ResultSeq
	Result WindowResult
}

// ResultSnapshot 是某位点的完整结果视图：包含该位点上每个窗口的最后一条
// 结果（FINAL 或最新 UPDATE），订阅者应用视图后从 Seq 续读增量即可。
// 视图按（窗口起点、symbol）确定性排序，便于逐字段审计比对。
type ResultSnapshot struct {
	// Seq 是快照位点：视图恰好包含所有 Seq<=该值的结果；续读从该位点开始
	//（SubscribeResults(snap.Seq) 交付 Seq>snap.Seq 的第一条增量）。
	Seq ResultSeq
	// Watermark 是快照位点时的全局水位线。
	Watermark int64
	// Views 是每个窗口的最新结果，按（窗口起点, symbol）确定性排序。
	Views []ResultRecord
}

// defaultResultHistoryCapacity 是结果历史的默认保留条数。
const defaultResultHistoryCapacity = 1024

// 订阅相关的可区分结论：
var (
	// ErrResultPositionExpired：订阅位点已落在可保留历史之外（历史被有界裁剪）。
	// 结论明确可区分，且可恢复：调用方重新取 ResultSnapshot，
	// 应用完整视图后从快照位点续读即可，不会静默漏过或跳过任何结果。
	ErrResultPositionExpired = errors.New("result position expired: beyond retained history, take a fresh snapshot and resume from it")
	// ErrInvalidResultPosition：订阅位点非法（如负数），属于调用方编程错误。
	ErrInvalidResultPosition = errors.New("invalid result subscribe position")
	// ErrSubscriptionClosed：订阅已被订阅者自己关闭。
	ErrSubscriptionClosed = errors.New("result subscription closed")
)

// ResultStore 是有界的结果历史与物化视图：处理循环单线程追加，
// 任意多个订阅者以自带游标的方式无阻塞拉读（不为订阅者建立 goroutine
// 或每订阅者队列，因而慢订阅者既不拖垮管线，也不会造成任务无界增长）。
type ResultStore struct {
	mu     sync.Mutex
	ring   []ResultRecord // 定容环形缓冲，容量即历史保留条数
	head   int            // ring 中最旧记录的下标
	count  int            // ring 中有效记录数
	seq    ResultSeq      // 已分配的最大位点（空历史时为 0）
	wm     int64          // 已追加结果中的最大水位线
	view   map[WindowKey]ResultRecord
	notify chan struct{} // 每次追加/关闭时"关闭并换新"，实现无 goroutine 广播
	closed bool
}

// NewResultStore 创建容量为 capacity 条结果的历史存储；capacity<=0 用默认容量。
func NewResultStore(capacity int) *ResultStore {
	if capacity <= 0 {
		capacity = defaultResultHistoryCapacity
	}
	return &ResultStore{
		ring:   make([]ResultRecord, capacity),
		view:   make(map[WindowKey]ResultRecord),
		wm:     NoWatermark,
		notify: make(chan struct{}),
	}
}

// oldestSeqLocked 返回当前最旧可交付位点；调用方持锁。
func (s *ResultStore) oldestSeqLocked() ResultSeq {
	if s.count == 0 {
		return s.seq + 1
	}
	return s.seq - ResultSeq(s.count) + 1
}

// recordLocked 取指定位点的记录；ok=false 表示该位点当前不在保留范围内或尚未产生。
func (s *ResultStore) recordLocked(seq ResultSeq) (ResultRecord, bool) {
	if s.count == 0 || seq < s.oldestSeqLocked() || seq > s.seq {
		return ResultRecord{}, false
	}
	off := int(seq-s.oldestSeqLocked()) % len(s.ring)
	idx := (s.head + off) % len(s.ring)
	return s.ring[idx], true
}

// broadcastLocked 唤醒所有等待中的订阅者；调用方持锁。
func (s *ResultStore) broadcastLocked() {
	old := s.notify
	s.notify = make(chan struct{})
	close(old)
}

// Append 在结果历史尾部追加一条窗口结果，分配下一个稳定位点并更新物化视图。
// O(1)，且绝不阻塞：慢订阅者只影响它自己的游标，不影响处理管线。
func (s *ResultStore) Append(r WindowResult) ResultRecord {
	s.mu.Lock()
	s.seq++
	rec := ResultRecord{Seq: s.seq, Result: r}
	if s.count == len(s.ring) {
		// 环形已满：覆盖最旧记录。若该记录正是其窗口在视图中的最新值，
		// 说明整个窗口都已滑出保留范围，同步把它从物化视图删除，
		// 保证快照内容与"历史可重放范围"严格一致（内存有界）。
		old := s.ring[s.head]
		if cur, ok := s.view[old.Result.Window]; ok && cur.Seq == old.Seq {
			delete(s.view, old.Result.Window)
		}
		s.ring[s.head] = rec
		s.head = (s.head + 1) % len(s.ring)
	} else {
		idx := (s.head + s.count) % len(s.ring)
		s.ring[idx] = rec
		s.count++
	}
	s.view[r.Window] = rec
	if r.Watermark > s.wm {
		s.wm = r.Watermark
	}
	s.broadcastLocked()
	s.mu.Unlock()
	return rec
}

// SnapshotLocked 取出当前位点的完整物化视图副本（确定性排序）；调用方持锁。
func (s *ResultStore) snapshotLocked() *ResultSnapshot {
	views := make([]ResultRecord, 0, len(s.view))
	for _, rec := range s.view {
		views = append(views, rec)
	}
	sort.Slice(views, func(i, j int) bool {
		a, b := views[i].Result.Window, views[j].Result.Window
		if a.Start != b.Start {
			return a.Start < b.Start
		}
		if a.Symbol != b.Symbol {
			return a.Symbol < b.Symbol
		}
		return a.End < b.End
	})
	return &ResultSnapshot{Seq: s.seq, Watermark: s.wm, Views: views}
}

// Snapshot 返回当前位点的完整结果视图（立即一致，不越过处理循环屏障）。
// 需要"与已处理事件严格对齐"的快照时使用 Processor.ResultSnapshot，
// 它在串行循环的屏障点取视图。
func (s *ResultStore) Snapshot() *ResultSnapshot {
	s.mu.Lock()
	snap := s.snapshotLocked()
	s.mu.Unlock()
	return snap
}

// Subscribe 从位点 after 之后开始订阅：返回的订阅首先交付 Seq=after+1 的结果。
// after=0 表示从头续读；after 为当前最新位点（或更靠后）则等待未来结果。
// 若 after+1 已滑出保留范围，返回包装了 ErrResultPositionExpired 的错误，
// 调用方应改取快照恢复；本函数本身不产生任何后台任务。
func (s *ResultStore) Subscribe(after ResultSeq) (*Subscription, error) {
	if after < 0 {
		return nil, ErrInvalidResultPosition
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if after+1 < s.oldestSeqLocked() {
		return nil, ErrResultPositionExpired
	}
	return &Subscription{
		store: s,
		next:  after + 1,
		done:  make(chan struct{}),
	}, nil
}

// Close 关闭存储：所有等待中的订阅被唤醒，排空仍在保留范围内的剩余结果后，
// Receive 返回 io.EOF。可重复调用（幂等）。
func (s *ResultStore) Close() {
	s.mu.Lock()
	if !s.closed {
		s.closed = true
		s.broadcastLocked()
	}
	s.mu.Unlock()
}

// Subscription 是订阅者侧的拉取游标：按位点严格有序地交付结果，
// 不丢、不重、不被旧值覆盖；游标状态只保存在订阅者一侧，
// 存储端不记录订阅者，因而订阅数量不带来服务端状态或后台任务。
type Subscription struct {
	store *ResultStore

	mu     sync.Mutex
	next   ResultSeq // 下一条待交付位点
	err    error     // 终结性错误（溢出等），置位后持续返回
	closed bool
	done   chan struct{} // Close 时关闭，用于中断 Receive
}

// Receive 阻塞等待下一条结果（严格按位点顺序）：
//   - 正常结果：位点严格递增，UPDATE 与 FINAL 的先后关系与产生顺序完全一致；
//   - 订阅期间落后到超出保留范围：返回 ErrResultPositionExpired（可凭快照恢复）；
//   - 存储关闭且已排空：返回 io.EOF；
//   - ctx 取消：返回 ctx.Err()；订阅被自身 Close：返回 ErrSubscriptionClosed。
func (sub *Subscription) Receive(ctx context.Context) (ResultRecord, error) {
	for {
		if err := ctx.Err(); err != nil {
			return ResultRecord{}, err
		}
		sub.mu.Lock()
		if sub.closed {
			sub.mu.Unlock()
			return ResultRecord{}, ErrSubscriptionClosed
		}
		if sub.err != nil {
			err := sub.err
			sub.mu.Unlock()
			return ResultRecord{}, err
		}
		next := sub.next
		sub.mu.Unlock()

		sub.store.mu.Lock()
		if rec, ok := sub.store.recordLocked(next); ok {
			sub.store.mu.Unlock()
			sub.mu.Lock()
			sub.next = rec.Seq + 1 // 游标前进到下一位点，保证同一条绝不重复交付
			sub.mu.Unlock()
			return rec, nil
		}
		// next 不可交付：要么已滑出保留范围（溢出），要么尚未产生（等待/EOF）。
		// 三项判定必须在同一把锁内取得，避免与 Append 竞争造成误判。
		expired := next < sub.store.oldestSeqLocked()
		closed := sub.store.closed
		ch := sub.store.notify
		sub.store.mu.Unlock()

		if expired {
			err := ErrResultPositionExpired
			sub.mu.Lock()
			if sub.err == nil {
				sub.err = err
			}
			err = sub.err
			sub.mu.Unlock()
			return ResultRecord{}, err
		}
		if closed {
			return ResultRecord{}, io.EOF
		}
		select {
		case <-ctx.Done():
			return ResultRecord{}, ctx.Err()
		case <-sub.done:
			return ResultRecord{}, ErrSubscriptionClosed
		case <-ch:
			// 存储有追加或关闭，回到循环重新判定。
		}
	}
}

// Position 返回已交付的最后一条结果位点（尚未交付任何结果时为 0）。
func (sub *Subscription) Position() ResultSeq {
	sub.mu.Lock()
	defer sub.mu.Unlock()
	return sub.next - 1
}

// Ack 返回订阅者可持久化的确认位点（语义同 Position）。
// 重复调用、重复确认都是纯读操作，幂等且无副作用；
// 重连时以该值 SubscribeResults 即可，同一结果不会被重复交付。
func (sub *Subscription) Ack() ResultSeq { return sub.Position() }

// Close 结束订阅（纯客户端游标关闭，不影响存储与其他订阅者）。
// 正在 Receive 中的调用会收到 ErrSubscriptionClosed。
func (sub *Subscription) Close() {
	sub.mu.Lock()
	if !sub.closed {
		sub.closed = true
		close(sub.done)
	}
	sub.mu.Unlock()
}

// ResultMaterializer 是订阅者侧的结果应用器：按位点保证新值不被旧值覆盖、
// 同一结果重复投递至多生效一次，便于"重复补发/重连重放"场景下安全应用。
type ResultMaterializer struct {
	mu   sync.Mutex
	last map[WindowKey]ResultSeq
}

// NewResultMaterializer 创建空的结果应用器。
func NewResultMaterializer() *ResultMaterializer {
	return &ResultMaterializer{last: make(map[WindowKey]ResultSeq)}
}

// Apply 按位点应用一条结果：仅当该窗口此前未应用过更晚或相同位点时生效。
// 返回 true 表示被应用（新窗口或更晚的更正），false 表示重复或过期补发，被忽略。
func (m *ResultMaterializer) Apply(rec ResultRecord) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	if cur, ok := m.last[rec.Result.Window]; ok && cur >= rec.Seq {
		return false
	}
	m.last[rec.Result.Window] = rec.Seq
	return true
}
