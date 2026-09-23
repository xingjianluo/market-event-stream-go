// Package pipeline 实现一个本地行情事件流处理管线：
// 多分区事件源 -> 有界输入队列 -> 单线程事件时间窗口聚合器 -> 结果/侧输出。
//
// 设计要点：
//   - 水位线由各分区日志中的水位线标记(WatermarkMarker)驱动，而非处理时钟，
//     因此结果与到达顺序、批大小、调度抖动无关，可复现；
//   - 迟到事件按 "允许迟到范围内/超出范围" 给出可区分的明确结论；
//   - 所有通道均有界，实现端到端背压；
//   - 检查点记录分区偏移、各状态与窗口内容，从检查点重放后幂等聚合。
package pipeline

import "time"

// PartitionID 标识一个本地分区（对应一个日志源）。
type PartitionID string

// EventID 唯一标识一条业务事件，由 (分区, 分区内序号) 组成。
// 同一 EventID 的重复投递（重放/重发）只会计入一次。
type EventID struct {
	Partition PartitionID
	Seq       int64
}

// Event 是一条行情成交事件，时间戳为事件时间（业务时间）。
// PriceMicros 为成交价的微单位整数表示（避免浮点导致的不可复现聚合），
// 例如 12.345 元记为 12345000。
type Event struct {
	ID          EventID
	Symbol      string
	EventTime   time.Time
	PriceMicros int64
	Quantity    int64
}

// EntryKind 区分分区日志中的条目类型。
type EntryKind int

const (
	// EntryEvent 为一条业务事件。
	EntryEvent EntryKind = iota
	// EntryWatermark 声明该分区事件时间进度推进到指定时间。
	// 其语义是：该分区*不会再产生*事件时间 <= t 的事件（t 之后的迟到仍可能发生）。
	EntryWatermark
	// EntryIdle 声明该分区暂时停滞。全局水位线计算时忽略该分区，
	// 防止慢/停滞分区无限阻塞已完成窗口；分区恢复后用 Watermark 标记重新参与计算。
	EntryIdle
	// EntryEnd 声明该分区日志结束。
	EntryEnd
	// EntryBarrier 是显式脉冲栅栏：源保证栅栏之前已投递的全部条目
	// 被聚合器处理（并完成窗口触发/清理）之后，才投递栅栏之后的条目，
	// 从而在需要时把水位线生效与后续事件的处分边界做成确定、与批大小无关。
	EntryBarrier
)

// Entry 是分区日志中的一个有序条目。
type Entry struct {
	Kind EntryKind
	// Offset 为该条目在分区日志中的偏移（从 0 开始单调递增），由 NewPartitionLog 自动赋值。
	Offset int64
	// 事件条目携带的事件。
	Event Event
	// Watermark/Idle 标记携带的事件时间。
	MarkTime time.Time
}

// WindowResult 是一个事件时间滚动窗口的最终聚合结果。
type WindowResult struct {
	Symbol      string
	WindowStart time.Time
	WindowEnd   time.Time
	EventCount  int64
	TotalQty    int64
	// VwapPriceMicros 为成交量加权均价（价格微单位），
	// 用 big.Int 做整除四舍五入，保证跨平台一致。
	VwapPriceMicros int64
	// FiredBy 记录本窗口被触发/修正触发时的全局水位线，便于解释判定依据。
	FiredBy time.Time
	// Revised 为 true 表示这是迟到范围内事件修正后的再次输出（首帧之后）。
	Revised bool
}

// LateDisposition 是迟到事件的明确处理结论。
type LateDisposition string

const (
	// DispositionLateInAllowed 表示事件晚于水位线、但落在允许迟到范围内，
	// 已计入对应窗口，窗口将以修正结果重新输出。
	DispositionLateInAllowed LateDisposition = "LATE_IN_ALLOWED"
	// DispositionLateDropped 表示事件晚于水位线且超出允许迟到范围
	// （窗口已被清理），明确拒绝计入，并在侧输出留痕，绝不静默丢弃。
	DispositionLateDropped LateDisposition = "LATE_DROPPED"
	// DispositionDuplicate 表示该 EventID 已处理过，本次为重复投递，直接忽略。
	DispositionDuplicate LateDisposition = "DUPLICATE"
)

// SideRecord 记录每一条未进入正常聚合路径的事件及其判定依据。
type SideRecord struct {
	Event       Event
	Disposition LateDisposition
	// GlobalWatermark 为判定时刻的全局水位线。
	GlobalWatermark time.Time
	// Reason 为人类可读的判定依据。
	Reason string
}

// Config 为管线可配置项。
type Config struct {
	// WindowSize 为滚动事件时间窗口长度，必须 > 0。
	WindowSize time.Duration
	// AllowedLateness 为允许迟到范围：水位线越过窗口结束时间后，
	// 仍保留窗口状态并接收迟到事件的时长；超过后窗口被清理，迟到事件明确丢弃。
	AllowedLateness time.Duration
	// InputQueueCap 为聚合器输入有界队列容量（背压），必须 > 0。
	InputQueueCap int
	// OutputQueueCap 为结果/侧输出有界队列容量，必须 > 0。
	OutputQueueCap int
	// MaxInFlight 为输入在途高水位（必须 > 0）。本实现中源为单协调器，
	// 在途量已被 InputQueueCap 严格限定；该项保留用于调优/未来并行源。
	MaxInFlight int
	// MergeBatchSize 为确定性源每轮每个分区最多投递的条目数（公平轮转），
	// 必须 > 0。改变该值只改变批次切分与中间帧时机，不改变同一批事件的最终结果。
	MergeBatchSize int
	// CheckpointInterval 为周期检查点间隔（0 表示不做周期检查点，仍可手动 Checkpoint）。
	CheckpointInterval time.Duration
	// CheckpointDir 非空时，检查点快照以 JSON 持久化到该目录，用于重启恢复。
	CheckpointDir string
	// OnBarrier 非空时，聚合器在每个显式栅栏点（窗口触发/清理完成之后、
	// 放行栅栏后条目之前）同步调用它。这是确定性的中途注入点：可在此检查
	// 快照内容、注入故障（返回 error 使管线停止）。若配置了 CheckpointDir，
	// 调用前快照已原子落盘，回调拿到的 snap 即落盘内容。
	OnBarrier func(p PartitionID, offset int64, snap *snapshot) error
}

// DefaultConfig 返回一套可用的默认配置：
// 10s 事件时间窗口、5s 允许迟到、有界队列与 64 在途上限。
func DefaultConfig() Config {
	return Config{
		WindowSize:      10 * time.Second,
		AllowedLateness: 5 * time.Second,
		InputQueueCap:   128,
		OutputQueueCap:  128,
		MaxInFlight:     64,
		MergeBatchSize:  3,
	}
}

// Stats 为管线运行期的可观测计数（测试与诊断用）。
type Stats struct {
	Received             int64 // 聚合器收到的条目数（含标记）
	EventsAccepted       int64 // 正常/迟到范围内计入的事件数
	Duplicates           int64 // 判重的重复投递数
	LateInAllowed        int64 // 水位线之后、允许范围内迟到并计入的事件数
	LateDropped          int64 // 超出允许范围被明确丢弃的事件数
	WindowsFired         int64 // 窗口首帧输出次数
	WindowsRevised       int64 // 窗口修正输出次数
	WindowsPurged        int64 // 窗口清理次数
	WatermarksAdvance    int64 // 全局水位线推进次数
	CheckpointsCompleted int64 // 已完成的检查点次数
}
