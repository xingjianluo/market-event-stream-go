package marketstream

import "strconv"

// WindowKey 唯一标识某 symbol 的一个窗口实例 [Start, End)。
type WindowKey struct {
	Symbol string
	Start  int64
	End    int64
}

// String 返回便于日志阅读的窗口标识。
func (k WindowKey) String() string {
	return k.Symbol + "@[" + strconv.FormatInt(k.Start, 10) + "," + strconv.FormatInt(k.End, 10) + ")"
}

// Window 是窗口状态：键 + 当前聚合值。
type Window struct {
	Key WindowKey
	Agg Aggregate
}

// WindowStart 返回事件时间所属窗口的起始边界（对负时间也成立的向下取整）。
func WindowStart(eventTime, size int64) int64 {
	q := eventTime / size
	if eventTime%size < 0 {
		q--
	}
	return q * size
}

// AssignWindow 返回事件时间所属滚动窗口的 [start, end)。
func AssignWindow(eventTime, size int64) (start, end int64) {
	start = WindowStart(eventTime, size)
	return start, start + size
}
