package marketstream

// Aggregate 是一个窗口内的聚合结论。
type Aggregate struct {
	EventCount int64   // 成交笔数
	TotalQty   int64   // 总成交量
	Notional   float64 // Σ price*qty，用于计算 VWAP
	FirstTime  int64   // 窗口内最小事件时间
	LastTime   int64   // 窗口内最大事件时间
}

// Apply 将一条事件并入聚合。空窗口由首条事件初始化 FirstTime/LastTime。
func (a *Aggregate) Apply(e Event) {
	if a.EventCount == 0 {
		a.FirstTime = e.EventTime
		a.LastTime = e.EventTime
	} else {
		if e.EventTime < a.FirstTime {
			a.FirstTime = e.EventTime
		}
		if e.EventTime > a.LastTime {
			a.LastTime = e.EventTime
		}
	}
	a.EventCount++
	a.TotalQty += e.Qty
	a.Notional += e.Price * float64(e.Qty)
}

// VWAP 返回成交量加权平均价；无成交时为 0。
func (a Aggregate) VWAP() float64 {
	if a.TotalQty == 0 {
		return 0
	}
	return a.Notional / float64(a.TotalQty)
}

// Equal 比较两个聚合是否逐字段一致，用于测试可复现断言。
func (a Aggregate) Equal(o Aggregate) bool {
	const eps = 1e-9
	return a.EventCount == o.EventCount &&
		a.TotalQty == o.TotalQty &&
		a.FirstTime == o.FirstTime &&
		a.LastTime == o.LastTime &&
		absFloat(a.Notional-o.Notional) <= eps*maxFloat(1, absFloat(o.Notional))
}

func absFloat(v float64) float64 {
	if v < 0 {
		return -v
	}
	return v
}

func maxFloat(a, b float64) float64 {
	if a > b {
		return a
	}
	return b
}
