package marketstream

import "sync"

// boundedMailbox 是单写者、单接力读者的有界暂存邮箱：入队 O(1) 且绝不阻塞；
// 暂存满时覆盖最旧条目（best-effort 旁路语义）。每次入队/重置都"关闭并换新"
// 通知通道，接力 goroutine 据此无轮询唤醒。它把"处理循环"与"下游通道是否被消费"
// 彻底解耦：只要接力/消费者的暂速总体跟得上（缓冲可吸收调度抖动），条目全部送达；
// 处理循环本身永不因旁路无人消费而停住，且内存按容量严格有界。
type boundedMailbox[T any] struct {
	mu     sync.Mutex
	buf    []T
	cap    int
	notify chan struct{}
}

func newBoundedMailbox[T any](capacity int) *boundedMailbox[T] {
	if capacity <= 0 {
		capacity = 1
	}
	return &boundedMailbox[T]{
		buf:    make([]T, 0, capacity),
		cap:    capacity,
		notify: make(chan struct{}),
	}
}

// enqueue 追加一条；满则丢弃最旧条目。绝不阻塞。
func (m *boundedMailbox[T]) enqueue(v T) {
	m.mu.Lock()
	if len(m.buf) >= m.cap {
		m.buf = m.buf[1:]
	}
	m.buf = append(m.buf, v)
	old := m.notify
	m.notify = make(chan struct{})
	close(old)
	m.mu.Unlock()
}

// drain 取出并清空当前全部暂存条目。
func (m *boundedMailbox[T]) drain() []T {
	m.mu.Lock()
	out := m.buf
	m.buf = make([]T, 0, m.cap)
	m.mu.Unlock()
	return out
}

// notifyChan 返回当前通知通道。
func (m *boundedMailbox[T]) notifyChan() <-chan struct{} {
	m.mu.Lock()
	ch := m.notify
	m.mu.Unlock()
	return ch
}

// reset 清空暂存并唤醒等待者（用于进程恢复，视为新流）。
func (m *boundedMailbox[T]) reset() {
	m.mu.Lock()
	m.buf = make([]T, 0, m.cap)
	old := m.notify
	m.notify = make(chan struct{})
	close(old)
	m.mu.Unlock()
}
