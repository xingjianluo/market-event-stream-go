package pipeline

import "time"

// partWaterState 为单个分区的水位线状态。
type partWaterState struct {
	mark     time.Time // 最近一次 Watermark 标记声明的时间（未标记过为零值）
	marked   bool
	maxEvent time.Time // 诊断用：该分区已观测到的最大事件时间（不参与水位线推进）
	idle     bool      // EntryIdle 后暂停参与全局最小值
	finished bool      // EntryEnd 后永久退出
}

// watermarkTracker 维护各分区水位线并计算全局水位线：
// 全局水位线 = 所有活跃(active：未停滞、未结束、已标记过)分区水位线的最小值，
// 且相对历史只能单调递增。
//
// 停滞(idle)分区不参与最小值计算——避免个别分区停滞无限阻塞已完成窗口；
// 分区恢复（新的 Watermark 标记）后以其声明时间重新参与——
// 此时全局水位线不会超过该分区的声明时间，因此不会过早关闭其窗口。
//
// 水位线只由日志中的显式 Watermark 标记推进，不使用处理时钟、
// 也不按"已见到的最大事件时间"隐式推进（分区内事件也可能乱序，
// 隐式推进会把尚未读到的更早事件误判为迟到）。
type watermarkTracker struct {
	parts  map[PartitionID]*partWaterState
	global time.Time
	allEOF bool // 源已投递完所有分区日志（含各自 EOF），可做 +Inf 收尾
}

func newWatermarkTracker() *watermarkTracker {
	return &watermarkTracker{parts: make(map[PartitionID]*partWaterState)}
}

// register 注册一个已知分区。初始无标记，视为 -inf：在它发出首个
// Watermark 标记之前，全局水位线无法越过它（标准流处理语义）。
func (t *watermarkTracker) register(p PartitionID) {
	if _, ok := t.parts[p]; !ok {
		t.parts[p] = &partWaterState{}
	}
}

// observeEvent 仅记录诊断信息（已观测最大事件时间），不推进水位线。
func (t *watermarkTracker) observeEvent(p PartitionID, et time.Time) {
	s := t.parts[p]
	if s == nil {
		s = &partWaterState{}
		t.parts[p] = s
	}
	if et.After(s.maxEvent) {
		s.maxEvent = et
	}
}

// observeMark 处理水位线标记：分区恢复活跃并推进其声明进度。
// 同一分区的标记也必须单调；回退的标记被忽略并由调用方记录日志。
// 返回 (新的分区水位线, 是否被接受)。
func (t *watermarkTracker) observeMark(p PartitionID, at time.Time) (time.Time, bool) {
	s := t.parts[p]
	if s == nil {
		s = &partWaterState{}
		t.parts[p] = s
	}
	if s.marked && at.Before(s.mark) {
		return s.mark, false
	}
	s.mark = at
	s.marked = true
	s.idle = false // 新的水位线标记意味着分区恢复
	return at, true
}

// markIdle 将分区标记为停滞，暂时退出全局最小值计算。
func (t *watermarkTracker) markIdle(p PartitionID) {
	if s, ok := t.parts[p]; ok {
		s.idle = true
	}
}

// markEnd 将分区标记为结束（语义等同永久停滞）。
func (t *watermarkTracker) markEnd(p PartitionID) {
	if s, ok := t.parts[p]; ok {
		s.finished = true
		s.idle = true
	}
}

// partitionWatermark 返回指定分区当前水位线。
func (t *watermarkTracker) partitionWatermark(p PartitionID) (time.Time, bool) {
	s, ok := t.parts[p]
	if !ok || !s.marked {
		return time.Time{}, false
	}
	return s.mark, true
}

// advance 依据当前各分区状态重算全局水位线。
//
// 规则：
//   - 活跃且已标记的分区参与最小值；
//   - 活跃但从未标记的分区视为 -inf，阻塞全局水位线（尚未声明进度的
//     源可能产生任意早的事件）；
//   - idle/finished 分区不参与（停滞不阻塞、结束永久退出）；
//   - 没有任何活跃分区时维持最后已知水位线（停滞场景下仍能收尾窗口）；
//   - 结果只能单调递增。
func (t *watermarkTracker) advance() (time.Time, bool) {
	var min time.Time
	found := false
	blocked := false
	for _, s := range t.parts {
		if s.finished || s.idle {
			continue
		}
		if !s.marked {
			blocked = true
			continue
		}
		if !found || s.mark.Before(min) {
			min = s.mark
			found = true
		}
	}
	if blocked || !found {
		return t.global, false
	}
	if min.After(t.global) {
		t.global = min
		return min, true
	}
	return t.global, false
}

// global 返回当前缓存的全局水位线。
func (t *watermarkTracker) globalWM() time.Time { return t.global }

// ---- 检查点导出/恢复 ----

type wmSnapshotData struct {
	Global time.Time
	States map[PartitionID]partWaterState
}

func (t *watermarkTracker) export() wmSnapshotData {
	m := make(map[PartitionID]partWaterState, len(t.parts))
	for k, v := range t.parts {
		m[k] = *v
	}
	return wmSnapshotData{Global: t.global, States: m}
}

func (t *watermarkTracker) restore(data wmSnapshotData) {
	t.global = data.Global
	t.parts = make(map[PartitionID]*partWaterState, len(data.States))
	for k, v := range data.States {
		vv := v
		t.parts[k] = &vv
	}
}
