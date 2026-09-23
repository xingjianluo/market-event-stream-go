// Package marketstream 实现本地行情事件流的窗口聚合处理管线：
// 多分区事件时间窗口聚合、水位线推进、迟到处理、幂等去重、
// 有界队列背压与检查点故障恢复。
package marketstream

import "fmt"

// EventID 是事件在其分区内的唯一标识（分区号 + 偏移量）。
type EventID struct {
	Partition int
	Offset    int64
}

// String 返回 p<分区>#<偏移> 形式的标识，便于日志区分。
func (id EventID) String() string {
	return fmt.Sprintf("p%d#%d", id.Partition, id.Offset)
}

// Event 表示一条行情成交事件，时间语义为事件时间（EventTime）。
type Event struct {
	ID        EventID
	Symbol    string
	EventTime int64   // 事件时间，epoch 毫秒
	Price     float64 // 成交价
	Qty       int64   // 成交量
}

// String 返回可读的事件描述，用于测试日志与判定依据打印。
func (e Event) String() string {
	return fmt.Sprintf("%s{%s t=%d price=%.4f qty=%d}", e.ID, e.Symbol, e.EventTime, e.Price, e.Qty)
}
