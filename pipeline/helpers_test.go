package pipeline

import (
	"context"
	"sort"
	"testing"
	"time"
)

// testCfg 返回测试用配置：10s 窗口、5s 允许迟到、小容量有界队列。
func testCfg() Config {
	return Config{
		WindowSize:      10 * time.Second,
		AllowedLateness: 5 * time.Second,
		InputQueueCap:   8,
		OutputQueueCap:  8,
		MaxInFlight:     8,
		MergeBatchSize:  2,
	}
}

// collected 汇总一次管线运行的输出，供断言。
type collected struct {
	results []WindowResult
	sides   []SideRecord
	stats   Stats
}

// finalOf 返回每个窗口的最后一帧（首帧或最终修正帧），按 (品种,起点) 排序。
func (c collected) finalOf() []WindowResult {
	last := map[windowKey]WindowResult{}
	var order []windowKey
	for _, r := range c.results {
		k := keyOf(r.Symbol, r.WindowStart)
		if _, ok := last[k]; !ok {
			order = append(order, k)
		}
		last[k] = r
	}
	sort.Slice(order, func(i, j int) bool {
		if order[i].Symbol != order[j].Symbol {
			return order[i].Symbol < order[j].Symbol
		}
		return order[i].StartMs < order[j].StartMs
	})
	out := make([]WindowResult, 0, len(order))
	for _, k := range order {
		out = append(out, last[k])
	}
	return out
}

func (c collected) sideByID(id EventID) (SideRecord, bool) {
	for _, s := range c.sides {
		if s.Event.ID == id {
			return s, true
		}
	}
	return SideRecord{}, false
}

// runPipeline 启动并消费管线至结束，返回全部输出。
func runPipeline(t *testing.T, cfg Config, logs []*PartitionLog, logger func(string, ...any)) collected {
	t.Helper()
	p := New(cfg, logs, logger)
	var c collected
	// 结果与侧输出通道必须并发消费：任一有界输出队列满都会反压到聚合器，
	// 顺序 drain 另一个通道会自死锁。
	resDone := make(chan struct{})
	sideDone := make(chan struct{})
	go func() {
		defer close(resDone)
		for r := range p.Results() {
			c.results = append(c.results, r)
		}
	}()
	go func() {
		defer close(sideDone)
		for sd := range p.SideOutput() {
			c.sides = append(c.sides, sd)
		}
	}()
	err := p.Run(context.Background())
	if err != nil {
		t.Errorf("pipeline Run 返回错误: %v", err)
	}
	<-resDone
	<-sideDone
	c.stats = p.Stats()
	return c
}

// testLogger 返回把判定依据打印到测试日志的 logger（水位线/事件时间/理由）。
func testLogger(t *testing.T) func(string, ...any) {
	t.Helper()
	return func(format string, a ...any) {
		t.Logf(format, a...)
	}
}
