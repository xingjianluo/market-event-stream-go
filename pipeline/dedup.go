package pipeline

// dedupTable 记录已处理的 EventID 及其所属窗口键。
// 条目随窗口清理一并删除（dropWindow），保证内存随窗口数有界。
type dedupTable struct {
	seen map[EventID]windowKey
	// byWindow 维护窗口 -> 事件集合，使窗口清理时删除其去重条目为 O(窗口大小)。
	byWindow map[windowKey]map[EventID]struct{}
}

func newDedupTable() *dedupTable {
	return &dedupTable{
		seen:     make(map[EventID]windowKey),
		byWindow: make(map[windowKey]map[EventID]struct{}),
	}
}

// add 登记一个事件 ID；返回 false 表示此前已存在（重复投递）。
func (d *dedupTable) add(id EventID, wk windowKey) bool {
	if _, ok := d.seen[id]; ok {
		return false
	}
	d.seen[id] = wk
	set, ok := d.byWindow[wk]
	if !ok {
		set = make(map[EventID]struct{})
		d.byWindow[wk] = set
	}
	set[id] = struct{}{}
	return true
}

// has 判断事件 ID 是否已经处理过。
func (d *dedupTable) has(id EventID) bool {
	_, ok := d.seen[id]
	return ok
}

// dropWindow 删除某窗口下的全部去重条目。
func (d *dedupTable) dropWindow(wk windowKey) {
	for id := range d.byWindow[wk] {
		delete(d.seen, id)
	}
	delete(d.byWindow, wk)
}

// len 返回当前条目数（诊断用）。
func (d *dedupTable) len() int { return len(d.seen) }
