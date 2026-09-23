package pipeline

import "time"

// PartitionLog 是单个分区的确定性本地日志（内存），条目按追加顺序编号。
// 分区内条目偏移单调递增；源按偏移顺序投递，因此故障重放严格按原顺序重演。
type PartitionLog struct {
	id      PartitionID
	entries []Entry
}

// NewPartitionLog 构造一个分区日志。
func NewPartitionLog(id PartitionID) *PartitionLog {
	return &PartitionLog{id: id}
}

func millis(ms int64) time.Time { return time.UnixMilli(ms).UTC() }

// AddEvent 追加一条成交事件，返回该条目偏移。
func (l *PartitionLog) AddEvent(seq int64, symbol string, eventTimeMillis int64, priceMicros, qty int64) int64 {
	off := int64(len(l.entries))
	l.entries = append(l.entries, Entry{
		Kind:   EntryEvent,
		Offset: off,
		Event: Event{
			ID:          EventID{Partition: l.id, Seq: seq},
			Symbol:      symbol,
			EventTime:   millis(eventTimeMillis),
			PriceMicros: priceMicros,
			Quantity:    qty,
		},
	})
	return off
}

// AddWatermark 追加一条水位线标记：
// 该分区声明不会再产生事件时间 <= at 的事件。
func (l *PartitionLog) AddWatermark(atMillis int64) int64 {
	off := int64(len(l.entries))
	l.entries = append(l.entries, Entry{Kind: EntryWatermark, Offset: off, MarkTime: millis(atMillis)})
	return off
}

// AddIdle 追加一条停滞标记：该分区暂时不参与全局水位线最小值计算。
func (l *PartitionLog) AddIdle(atMillis int64) int64 {
	off := int64(len(l.entries))
	l.entries = append(l.entries, Entry{Kind: EntryIdle, Offset: off, MarkTime: millis(atMillis)})
	return off
}

// AddBarrier 追加一个显式脉冲栅栏，返回其偏移。
func (l *PartitionLog) AddBarrier() int64 {
	off := int64(len(l.entries))
	l.entries = append(l.entries, Entry{Kind: EntryBarrier, Offset: off})
	return off
}

// AddEnd 追加结束标记。
func (l *PartitionLog) AddEnd() int64 {
	off := int64(len(l.entries))
	l.entries = append(l.entries, Entry{Kind: EntryEnd, Offset: off})
	return off
}

// ID 返回分区 ID。
func (l *PartitionLog) ID() PartitionID { return l.id }

// Len 返回日志条目数。
func (l *PartitionLog) Len() int { return len(l.entries) }

// entryAt 返回指定偏移的条目。
func (l *PartitionLog) entryAt(offset int64) (Entry, bool) {
	if offset < 0 || offset >= int64(len(l.entries)) {
		return Entry{}, false
	}
	return l.entries[offset], true
}
