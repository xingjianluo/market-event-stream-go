package pipeline

import (
	"math/big"
	"sort"
	"time"
)

// windowKey 唯一标识一个 (品种, 窗口起点)。StartMs 为 unix 毫秒，
// 用整数而非 time.Time 作 key 以保证对齐与序列化可复现。
type windowKey struct {
	Symbol  string
	StartMs int64
}

func keyOf(symbol string, start time.Time) windowKey {
	return windowKey{Symbol: symbol, StartMs: start.UnixMilli()}
}

// eventVerdict 是聚合器对一条事件的判定。
type eventVerdict int

const (
	verdictOnTime      eventVerdict = iota // 水位线未越过窗口结束时间，正常计入
	verdictLateAllowed                     // 晚于水位线但在允许迟到范围内，计入并触发修正帧
	verdictLateDropped                     // 超出允许迟到范围（窗口已清理），明确丢弃
	verdictDuplicate                       // EventID 已处理，重复投递
)

// windowState 是单个窗口的全部状态。聚合只用整数运算（big.Int），
// 求和与加入顺序无关，因此分区交错/批大小/到达顺序都不改变结果。
type windowState struct {
	key      windowKey
	symbol   string
	start    time.Time
	end      time.Time
	count    int64
	totalQty int64
	sumPQ    *big.Int // Σ priceMicros * quantity
	fired    bool
	firedBy  time.Time
	events   []Event // 按首次计入顺序保存，供检查点恢复与诊断
}

// windowAgg 为单线程事件时间窗口聚合器所持有的全部窗口状态。
// 所有方法只在聚合器 goroutine 内被调用，无需加锁。
type windowAgg struct {
	windowSize     time.Duration
	allowedLatency time.Duration
	windows        map[windowKey]*windowState
	dedup          *dedupTable
}

func newWindowAgg(windowSize, allowedLateness time.Duration) *windowAgg {
	return &windowAgg{
		windowSize:     windowSize,
		allowedLatency: allowedLateness,
		windows:        make(map[windowKey]*windowState),
		dedup:          newDedupTable(),
	}
}

// windowStart 返回 et 所属滚动窗口的起始时间（按 unix 毫秒向下取整对齐，
// 时区统一为 UTC，保证跨环境可复现）。
func windowStart(et time.Time, size time.Duration) time.Time {
	ms := et.UnixMilli()
	sizeMs := size.Milliseconds()
	return time.UnixMilli((ms / sizeMs) * sizeMs).UTC()
}

func (a *windowAgg) getOrCreate(symbol string, start time.Time) *windowState {
	k := keyOf(symbol, start)
	st, ok := a.windows[k]
	if !ok {
		st = &windowState{
			key:    k,
			symbol: symbol,
			start:  start,
			end:    start.Add(a.windowSize),
			sumPQ:  new(big.Int),
		}
		a.windows[k] = st
	}
	return st
}

// process 对一条事件做去重 + 迟到判定并计入。
// gw 为当前全局水位线。返回判定、（迟到修正时的）结果帧与判定依据。
//
// 判定规则（窗口区间 [start, end)）：
//   - EventID 已存在于仍保留的窗口 -> DUPLICATE，忽略；
//   - gw >= end+allowedLateness    -> LATE_DROPPED：窗口已/应被清理，明确拒绝；
//   - gw >= end                    -> LATE_IN_ALLOWED：计入；若窗口已首帧输出则返回修正帧；
//   - 否则                          -> ON_TIME：正常计入。
func (a *windowAgg) process(e Event, gw time.Time) (eventVerdict, WindowResult, string) {
	start := windowStart(e.EventTime, a.windowSize)
	end := start.Add(a.windowSize)
	k := keyOf(e.Symbol, start)

	if a.dedup.has(e.ID) {
		return verdictDuplicate, WindowResult{},
			"EventID 已在窗口 " + start.Format(time.RFC3339Nano) + " 中计过，重复投递忽略"
	}

	cutoff := end.Add(a.allowedLatency)
	if !gw.Before(cutoff) { // gw >= end+allowedLateness
		return verdictLateDropped, WindowResult{},
			"事件时间 " + e.EventTime.Format(time.RFC3339Nano) +
				" 早于窗口 " + start.Format(time.RFC3339Nano) +
				" 的迟到清理线 " + cutoff.Format(time.RFC3339Nano) +
				"（当前全局水位线 " + gw.Format(time.RFC3339Nano) + "），超出允许迟到范围，明确丢弃并侧输出留痕"
	}

	st := a.getOrCreate(e.Symbol, start)
	pq := new(big.Int).Mul(big.NewInt(e.PriceMicros), big.NewInt(e.Quantity))
	st.sumPQ.Add(st.sumPQ, pq)
	st.totalQty += e.Quantity
	st.count++
	st.events = append(st.events, e)
	a.dedup.add(e.ID, k)

	late := !gw.Before(end) // gw >= end
	if !late {
		return verdictOnTime, WindowResult{},
			"事件时间 " + e.EventTime.Format(time.RFC3339Nano) + " 未晚于窗口结束时间 " +
				end.Format(time.RFC3339Nano) + "（全局水位线 " + gw.Format(time.RFC3339Nano) + "），正常计入"
	}

	// 迟到但允许：若窗口此前已首帧输出，立即给出修正帧；
	// 若窗口是事件第一次到达（水位线越过 end 后才出现首个事件），
	// 则按首帧处理（Revised=false），由 fireExpired/本次调用统一输出。
	if !st.fired {
		st.fired = true
		st.firedBy = gw
		return verdictLateAllowed, a.toResult(st, false),
			"事件时间 " + e.EventTime.Format(time.RFC3339Nano) + " 晚于窗口结束时间 " +
				end.Format(time.RFC3339Nano) + " 但在允许迟到范围（清理线 " +
				cutoff.Format(time.RFC3339Nano) + "）内，迟到计入并首帧输出"
	}
	return verdictLateAllowed, a.toResult(st, true),
		"事件时间 " + e.EventTime.Format(time.RFC3339Nano) + " 晚于窗口结束时间 " +
			end.Format(time.RFC3339Nano) + " 但在允许迟到范围内，迟到计入并输出修正帧"
}

// vwap 计算成交量加权均价（四舍五入到整数微单位），全程整数运算。
func vwap(st *windowState) int64 {
	if st.totalQty == 0 {
		return 0
	}
	num := new(big.Int).Set(st.sumPQ)
	den := big.NewInt(st.totalQty)
	q := new(big.Int).Quo(num, den)
	r := new(big.Int).Rem(num, den)
	// 统一按非负行情处理：余数*2 >= 分母则进位，保证各平台结果一致。
	if new(big.Int).Mul(r, big.NewInt(2)).Cmp(den) >= 0 {
		q.Add(q, big.NewInt(1))
	}
	return q.Int64()
}

func (a *windowAgg) toResult(st *windowState, revised bool) WindowResult {
	return WindowResult{
		Symbol:          st.symbol,
		WindowStart:     st.start,
		WindowEnd:       st.end,
		EventCount:      st.count,
		TotalQty:        st.totalQty,
		VwapPriceMicros: vwap(st),
		FiredBy:         st.firedBy,
		Revised:         revised,
	}
}

// fireExpired 输出所有 end <= gw 且尚未首帧输出的窗口。
// 结果按 (品种, 窗口起点) 排序，保证输出顺序与分区交错方式无关。
func (a *windowAgg) fireExpired(w time.Time, logger func(string, ...any)) []WindowResult {
	var out []WindowResult
	for _, st := range a.windows {
		if !st.fired && !w.Before(st.end) {
			st.fired = true
			st.firedBy = w
			out = append(out, a.toResult(st, false))
		}
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Symbol != out[j].Symbol {
			return out[i].Symbol < out[j].Symbol
		}
		return out[i].WindowStart.Before(out[j].WindowStart)
	})
	for _, r := range out {
		logger("窗口首帧触发: symbol=%s window=[%s,%s) count=%d qty=%d vwapMicros=%d 触发水位线=%s",
			r.Symbol, r.WindowStart.Format(time.RFC3339Nano), r.WindowEnd.Format(time.RFC3339Nano),
			r.EventCount, r.TotalQty, r.VwapPriceMicros, r.FiredBy.Format(time.RFC3339Nano))
	}
	return out
}

// purgeExpired 清理 end+allowedLateness <= gw 的窗口及其去重条目。
// 此后该窗口的迟到事件将得到 LATE_DROPPED 结论。
func (a *windowAgg) purgeExpired(w time.Time, logger func(string, ...any)) int {
	n := 0
	for k, st := range a.windows {
		cutoff := st.end.Add(a.allowedLatency)
		if !w.Before(cutoff) {
			a.dedup.dropWindow(k)
			delete(a.windows, k)
			n++
			logger("窗口清理: symbol=%s window=[%s,%s) 清理线=%s 当前全局水位线=%s",
				st.symbol, st.start.Format(time.RFC3339Nano), st.end.Format(time.RFC3339Nano),
				cutoff.Format(time.RFC3339Nano), w.Format(time.RFC3339Nano))
		}
	}
	return n
}

// pendingWindows 返回当前仍保留状态的窗口键（检查点/诊断用），按序排列。
func (a *windowAgg) pendingWindows() []windowKey {
	ks := make([]windowKey, 0, len(a.windows))
	for k := range a.windows {
		ks = append(ks, k)
	}
	sort.Slice(ks, func(i, j int) bool {
		if ks[i].Symbol != ks[j].Symbol {
			return ks[i].Symbol < ks[j].Symbol
		}
		return ks[i].StartMs < ks[j].StartMs
	})
	return ks
}

// ---- 检查点导出 / 恢复 ----

// exportWindows 按固定顺序返回全部窗口状态（含事件明细）。
func (a *windowAgg) exportWindows() []*windowState {
	ks := a.pendingWindows()
	out := make([]*windowState, 0, len(ks))
	for _, k := range ks {
		out = append(out, a.windows[k])
	}
	return out
}

// applyEventInto 把一条事件的数值贡献计入给定窗口（恢复/计入共用同一路径）。
func (a *windowAgg) applyEventInto(st *windowState, e Event) {
	pq := new(big.Int).Mul(big.NewInt(e.PriceMicros), big.NewInt(e.Quantity))
	st.sumPQ.Add(st.sumPQ, pq)
	st.totalQty += e.Quantity
	st.count++
	st.events = append(st.events, e)
	a.dedup.add(e.ID, st.key)
}

// restoreWindow 用快照中的一个窗口重建状态，聚合数值由事件重算得到，
// 因而即使快照格式演进，恢复后的聚合结论仍与事件明细一致。
func (a *windowAgg) restoreWindow(symbol string, start, end, firedBy time.Time, fired bool, events []Event) *windowState {
	st := &windowState{
		key: keyOf(symbol, start), symbol: symbol, start: start, end: end,
		sumPQ: new(big.Int), fired: fired, firedBy: firedBy,
	}
	for _, e := range events {
		a.applyEventInto(st, e)
	}
	a.windows[st.key] = st
	return st
}
