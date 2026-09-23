package marketstream

// DedupTable 是事件级幂等去重表：记录已计入的事件 ID 及其事件时间。
// 对同一批事件，无论到达顺序、批大小或故障重放如何变化，同一事件 ID
// 至多计入一次。表项随水位线推进和窗口最终化而清除，保证内存有界。
type DedupTable struct {
	seen map[EventID]int64
}

// NewDedupTable 创建空去重表。
func NewDedupTable() *DedupTable {
	return &DedupTable{seen: make(map[EventID]int64)}
}

// Add 登记一个事件；若已存在则返回 false（重复投递，不重复计入）。
func (d *DedupTable) Add(id EventID, eventTime int64) (accepted bool) {
	if _, ok := d.seen[id]; ok {
		return false
	}
	d.seen[id] = eventTime
	return true
}

// Seen 判断事件是否已计入。
func (d *DedupTable) Seen(id EventID) bool {
	_, ok := d.seen[id]
	return ok
}

// PurgeBefore 清除事件时间早于 retainBefore 的条目，返回清除条数。
// 调用方传入 retainBefore = globalWM - allowedLateness，
// 与窗口最终化边界一致，使去重表随窗口关闭而有界。
func (d *DedupTable) PurgeBefore(globalWM, allowedLateness int64) int {
	retainBefore := globalWM - allowedLateness
	n := 0
	for id, t := range d.seen {
		if t < retainBefore {
			delete(d.seen, id)
			n++
		}
	}
	return n
}

// Len 返回当前条目数。
func (d *DedupTable) Len() int { return len(d.seen) }
