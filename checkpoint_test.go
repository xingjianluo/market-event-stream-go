package marketstream

import (
	"context"
	"path/filepath"
	"testing"
)

// runFaultFree 一次性完整处理 fixture，返回每窗口最终聚合。
func runFaultFree(t *testing.T, fix map[int][]Event) map[WindowKey]Aggregate {
	ctx := context.Background()
	cfg := testConfig(t)
	p, col, cleanup := runProcessor(ctx, t, cfg, []int{0, 1})
	defer cleanup()

	src := NewLocalSource(fix)
	done := p.Feed(ctx, src)
	<-done
	waitForResults(t, col, 2)
	final := make(map[WindowKey]Aggregate)
	for k, r := range col.finalResults() {
		final[k] = r.Agg
	}
	return final
}

func TestCheckpointRestoreReplayEqualsFaultFree(t *testing.T) {
	fix := buildInterleavedFixture()
	want := runFaultFree(t, fix)

	ctx := context.Background()
	cfg := testConfig(t)

	// ---- 第一次运行：处理 p0 全部、p1 前两条后做检查点并"崩溃" ----
	// 崩溃前的输出无需断言，仅取快照；用独立收集器排空通道即可。
	p1, _, cleanup1 := runProcessor(ctx, t, cfg, []int{0, 1})
	for _, e := range fix[0] {
		p1.Ingest(ctx, e)
	}
	p1.Ingest(ctx, fix[1][0])
	p1.Ingest(ctx, fix[1][1])
	cp, err := p1.Checkpoint(ctx)
	if err != nil {
		t.Fatalf("checkpoint: %v", err)
	}
	t.Logf("checkpoint positions=%v wm=%s windows=%d dedup=%d",
		cp.Positions, wmString(cp.Watermark), len(cp.Windows), len(cp.Dedup))

	path := filepath.Join(t.TempDir(), "cp.json")
	if err := SaveCheckpoint(path, cp); err != nil {
		t.Fatalf("save: %v", err)
	}
	cleanup1() // 模拟进程崩溃：不做任何优雅收尾

	// 持久化文件可重新读出
	loaded, err := LoadCheckpoint(path)
	if err != nil {
		t.Fatalf("load: %v", err)
	}

	// ---- 第二次运行：从检查点恢复，源 seek 到已提交偏移后重放剩余 ----
	p2, col2, cleanup2 := runProcessor(ctx, t, cfg, []int{0, 1})
	defer cleanup2()
	if err := p2.Restore(loaded); err != nil {
		t.Fatalf("restore: %v", err)
	}

	src := NewLocalSource(fix)
	for part, off := range loaded.Positions {
		src.Seek(part, off) // 正常恢复：从已提交偏移继续
	}
	// 模拟至少一次投递：p0 最后一个已提交事件未被源确认，强制重放它，
	// 该事件必须被去重表识别为 DUPLICATE，结论不得改变。
	if loaded.Positions[0] > 0 {
		src.Seek(0, loaded.Positions[0]-1)
	}
	done := p2.Feed(ctx, src)
	<-done
	waitForResults(t, col2, 2)
	// 屏障确保所有已投递事件（含重放事件）都已被串行循环处理、判定已发出；
	// 接力投递结果是异步的，收到结果数不再等价于"全部入队事件处理完毕"。
	if _, err := p2.Checkpoint(ctx); err != nil {
		t.Fatalf("barrier after recovery feed: %v", err)
	}
	// 恢复后实际处理：p0 重放 1 条（DUPLICATE）+ p1 剩余 2 条 = 3 条判定。
	waitForVerdicts(t, col2, 3)

	got := make(map[WindowKey]Aggregate)
	for k, r := range col2.finalResults() {
		got[k] = r.Agg
	}

	if len(got) != len(want) {
		t.Fatalf("window count after recovery %d != fault-free %d", len(got), len(want))
	}
	for k, w := range want {
		g, ok := got[k]
		if !ok {
			t.Fatalf("recovered run missing window %s", k)
		}
		if !g.Equal(w) {
			t.Fatalf("window %s recovered agg=%+v != fault-free %+v (recovery changed result)", k, g, w)
		}
	}

	// 恢复后不应出现超宽限拒绝（重放事件即使被去重，也不应污染结论）。
	if rj := col2.Rejections(); len(rj) != 0 {
		t.Fatalf("unexpected rejections after recovery: %+v", rj)
	}
	// 重放的已提交事件必须全部判定为 DUPLICATE。
	var dup int
	for _, v := range col2.Verdicts() {
		if v.Decision == DecisionDuplicate {
			dup++
		}
	}
	if dup == 0 {
		t.Fatal("expected replayed committed events to be deduplicated, got 0 DUPLICATE")
	}
	t.Logf("recovery matches fault-free over %d windows; replayed duplicates=%d", len(got), dup)
}

func TestLoadMissingCheckpoint(t *testing.T) {
	if _, err := LoadCheckpoint(filepath.Join(t.TempDir(), "nope.json")); err != ErrNoCheckpoint {
		t.Fatalf("want ErrNoCheckpoint, got %v", err)
	}
}
