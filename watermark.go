package marketstream

import "time"

// NoWatermark 表示水位线尚未建立（任何事件时间都不晚于它）。
const NoWatermark int64 = -1 << 63

// MaxWatermark 表示所有分区都结束后的正无穷水位线，用于关闭全部窗口。
const MaxWatermark int64 = 1<<63 - 1

// partWatermark 是单个分区的水位线进度状态。
type partWatermark struct {
	maxEventTime int64     // 该分区已观察到的最大事件时间
	lastActivity time.Time // 最后一次收到事件的处理时间
	idle         bool      // 是否因 IdleTimeout 被临时标记为空闲
	done         bool      // 是否读到分区结尾
}

// WatermarkTracker 按"活跃分区水位线最小值"维护全局水位线，
// 保证水位线单调不减，并支持分区空闲与完成处理。
type WatermarkTracker struct {
	maxOutOfOrderness int64
	idleTimeout       time.Duration
	parts             map[int]*partWatermark
	watermark         int64
}

// NewWatermarkTracker 创建追踪器；所有分区初始为活跃但未观察到事件（NoWatermark）。
func NewWatermarkTracker(partitions []int, maxOutOfOrderness int64, idleTimeout time.Duration) *WatermarkTracker {
	t := &WatermarkTracker{
		maxOutOfOrderness: maxOutOfOrderness,
		idleTimeout:       idleTimeout,
		parts:             make(map[int]*partWatermark, len(partitions)),
		watermark:         NoWatermark,
	}
	for _, p := range partitions {
		t.parts[p] = &partWatermark{maxEventTime: NoWatermark, lastActivity: time.Time{}}
	}
	return t
}

// partitionWM 计算单个分区当前的分区水位线；未观察到事件时为 NoWatermark。
func (t *WatermarkTracker) partitionWM(p *partWatermark) int64 {
	if p.maxEventTime == NoWatermark {
		return NoWatermark
	}
	return p.maxEventTime - t.maxOutOfOrderness
}

// recompute 以活跃（非空闲、未完成）分区水位线的最小值重算全局水位线；
// 全局水位线绝不回退。没有活跃分区时维持原值（Complete 会单独处理全完成场景）。
func (t *WatermarkTracker) recompute() (oldWM, newWM int64, advanced bool) {
	oldWM = t.watermark
	cand := MaxWatermark
	active := 0
	for _, p := range t.parts {
		// 空闲与已完成分区不参与最小值；尚未产生事件的分区以 -∞ 参与，
		// 阻止窗口过早关闭（空分区会立即 EOF 解除，停滞分区由空闲超时解除）。
		if p.idle || p.done {
			continue
		}
		active++
		if w := t.partitionWM(p); w < cand {
			cand = w
		}
	}
	if active == 0 {
		cand = oldWM
	}
	newWM = oldWM
	if cand > oldWM {
		newWM = cand
	}
	t.watermark = newWM
	return oldWM, newWM, newWM > oldWM
}

// Observe 记录某分区的一条事件时间。空闲分区重新来事件即恢复活跃（不回退水位线）。
// 返回变化前后的全局水位线及是否推进。
func (t *WatermarkTracker) Observe(partition int, eventTime int64, now time.Time) (oldWM, newWM int64, advanced bool) {
	p, ok := t.parts[partition]
	if !ok {
		// 容错：未知分区（例如恢复后新增），直接登记为活跃分区。
		p = &partWatermark{maxEventTime: NoWatermark}
		t.parts[partition] = p
	}
	p.done = false
	p.idle = false
	p.lastActivity = now
	if eventTime > p.maxEventTime {
		p.maxEventTime = eventTime
	}
	return t.recompute()
}

// TickIdle 按给定处理时间把超过 IdleTimeout 未活动的非空闲分区标记为空闲，
// 返回全局水位线变化及变为空闲的分区号。停滞分区不会无限阻塞已完成窗口。
func (t *WatermarkTracker) TickIdle(now time.Time) (oldWM, newWM int64, advanced bool, becameIdle []int) {
	for id, p := range t.parts {
		if p.idle || p.done || p.maxEventTime == NoWatermark {
			continue
		}
		if t.idleTimeout > 0 && !p.lastActivity.IsZero() &&
			now.Sub(p.lastActivity) >= t.idleTimeout {
			p.idle = true
			becameIdle = append(becameIdle, id)
		}
	}
	oldWM, newWM, advanced = t.recompute()
	return
}

// Complete 标记某分区已读到结尾（EOF），后续不再参与全局水位线最小值。
// 当所有分区都完成时，全局水位线推进到 MaxWatermark 以关闭全部窗口。
func (t *WatermarkTracker) Complete(partition int) (oldWM, newWM int64, advanced bool) {
	p, ok := t.parts[partition]
	if !ok {
		return t.recompute()
	}
	p.done = true
	p.idle = false

	allDone := true
	for _, q := range t.parts {
		if !q.done {
			allDone = false
			break
		}
	}
	oldWM = t.watermark
	if allDone {
		newWM = MaxWatermark
	} else {
		_, newWM, _ = t.recompute()
	}
	if newWM > t.watermark {
		t.watermark = newWM
		advanced = true
	} else {
		newWM = t.watermark
	}
	return oldWM, newWM, advanced
}

// Watermark 返回当前全局水位线。
func (t *WatermarkTracker) Watermark() int64 { return t.watermark }

// PartitionWatermark 返回某分区当前的分区水位线；未观察到事件时为 NoWatermark。
func (t *WatermarkTracker) PartitionWatermark(partition int) int64 {
	p, ok := t.parts[partition]
	if !ok {
		return NoWatermark
	}
	return t.partitionWM(p)
}

// Idle 返回分区是否处于空闲状态。
func (t *WatermarkTracker) Idle(partition int) bool {
	if p, ok := t.parts[partition]; ok {
		return p.idle
	}
	return false
}

// Done 返回分区是否已完成。
func (t *WatermarkTracker) Done(partition int) bool {
	if p, ok := t.parts[partition]; ok {
		return p.done
	}
	return false
}

// restoreState 从检查点重建各分区的最大事件时间与全局水位线基线。
func (t *WatermarkTracker) restoreState(maxEventTime map[int]int64, globalWM int64) {
	for id, p := range t.parts {
		if mt, ok := maxEventTime[id]; ok {
			p.maxEventTime = mt
		}
	}
	if globalWM > t.watermark {
		t.watermark = globalWM
	}
}

// snapshotMaxEventTime 导出各分区最大事件时间，供检查点使用。
func (t *WatermarkTracker) snapshotMaxEventTime() map[int]int64 {
	m := make(map[int]int64, len(t.parts))
	for id, p := range t.parts {
		m[id] = p.maxEventTime
	}
	return m
}
