package marketstream

import (
	"context"
	"sort"
	"sync"
)

// LocalSource 用内存中的分区切片模拟本地事件源（Kafka 式的分区 + 偏移模型），
// 支持按偏移回放，用于故障恢复与重复投递场景。
type LocalSource struct {
	mu       sync.Mutex
	parts    map[int][]Event
	position map[int]int64
}

// NewLocalSource 根据分区事件切片构造事件源，
// 事件 ID 会被规范化为（分区号, 切片下标），保证分区内偏移连续唯一。
func NewLocalSource(partitions map[int][]Event) *LocalSource {
	s := &LocalSource{
		parts:    make(map[int][]Event, len(partitions)),
		position: make(map[int]int64, len(partitions)),
	}
	for p, evs := range partitions {
		cp := make([]Event, len(evs))
		for i := range evs {
			e := evs[i]
			e.ID = EventID{Partition: p, Offset: int64(i)}
			cp[i] = e
		}
		s.parts[p] = cp
		s.position[p] = 0
	}
	return s
}

// Partitions 返回全部分区号（升序）。
func (s *LocalSource) Partitions() []int {
	ids := make([]int, 0, len(s.parts))
	for p := range s.parts {
		ids = append(ids, p)
	}
	sort.Ints(ids)
	return ids
}

// Next 读取指定分区当前偏移的事件并前进；ok=false 表示已到分区结尾。
// ctx 取消时返回 ctx.Err()，便于优雅停止。
func (s *LocalSource) Next(ctx context.Context, partition int) (ev Event, ok bool, err error) {
	if err := ctx.Err(); err != nil {
		return Event{}, false, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	evs, exists := s.parts[partition]
	if !exists {
		return Event{}, false, nil
	}
	pos := s.position[partition]
	if pos >= int64(len(evs)) {
		return Event{}, false, nil
	}
	ev = evs[pos]
	s.position[partition] = pos + 1
	return ev, true, nil
}

// Position 返回某分区下一条待读取的偏移。
func (s *LocalSource) Position(partition int) int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.position[partition]
}

// Seek 将某分区的读取位置重置到指定偏移（回放/重放）。
// 偏移越界时夹取到 [0, len] 区间。
func (s *LocalSource) Seek(partition int, offset int64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	evs, ok := s.parts[partition]
	if !ok {
		return
	}
	n := int64(len(evs))
	if offset < 0 {
		offset = 0
	}
	if offset > n {
		offset = n
	}
	s.position[partition] = offset
}

// Remaining 返回某分区尚未读取的事件条数。
func (s *LocalSource) Remaining(partition int) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	evs, ok := s.parts[partition]
	if !ok {
		return 0
	}
	return len(evs) - int(s.position[partition])
}
