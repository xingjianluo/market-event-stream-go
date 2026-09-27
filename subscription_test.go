package marketstream

import (
	"context"
	"errors"
	"fmt"
	"io"
	"reflect"
	"runtime"
	"sync"
	"testing"
	"time"
)

// storeRec 构造一条测试结果内容（不经过 Append，Append 会自行分配位点）。
func storeRec(symbol string, start int64, kind ResultKind, count int64, wm int64) WindowResult {
	return WindowResult{
		Kind:      kind,
		Window:    WindowKey{Symbol: symbol, Start: start, End: start + 10_000},
		Agg:       Aggregate{EventCount: count, TotalQty: count, Notional: float64(count) * 100, FirstTime: start, LastTime: start + 1},
		Watermark: wm,
	}
}

// receiveN 从订阅拉取恰好 n 条；任何错误或条数不符都立即失败。
func receiveN(t *testing.T, sub *Subscription, n int) []ResultRecord {
	t.Helper()
	out := make([]ResultRecord, 0, n)
	for i := 0; i < n; i++ {
		rec, err := sub.Receive(context.Background())
		if err != nil {
			t.Fatalf("receive %d: unexpected error %v", i, err)
		}
		out = append(out, rec)
	}
	return out
}

// materializeRecords 把记录序列逐窗口折叠成"窗口 -> 最后一条记录"。
func materializeRecords(recs []ResultRecord) map[WindowKey]ResultRecord {
	out := make(map[WindowKey]ResultRecord)
	for _, rec := range recs {
		cur, ok := out[rec.Result.Window]
		if !ok || rec.Seq > cur.Seq {
			out[rec.Result.Window] = rec
		}
	}
	return out
}

// TestStoreAppendOrderAndResume 验证位点严格递增、掉线按确认位点精确续读、
// 重复确认/重复补发不会造成重复应用。
func TestStoreAppendOrderAndResume(t *testing.T) {
	s := NewResultStore(16)
	inputs := []WindowResult{
		storeRec("BTC", 0, ResultFinal, 1, 10_000),
		storeRec("ETH", 0, ResultFinal, 1, 10_000),
		storeRec("BTC", 0, ResultUpdate, 2, 12_000), // BTC 窗口更正
		storeRec("BTC", 10_000, ResultFinal, 1, 20_000),
		storeRec("BTC", 0, ResultUpdate, 3, 13_000), // 再次更正
	}
	appended := make([]ResultRecord, 0, len(inputs))
	for _, r := range inputs {
		appended = append(appended, s.Append(r))
	}
	for i, rec := range appended {
		if rec.Seq != ResultSeq(i+1) {
			t.Fatalf("seq mismatch at %d: got %d want %d", i, rec.Seq, i+1)
		}
	}

	// 首次订阅：消费前 3 条后"掉线"，记录确认位点。
	sub1, err := s.Subscribe(0)
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	mat := NewResultMaterializer()
	for _, rec := range receiveN(t, sub1, 3) {
		if !mat.Apply(rec) {
			t.Fatalf("rec seq %d should apply", rec.Seq)
		}
	}
	confirmed := sub1.Ack()
	if confirmed != 3 {
		t.Fatalf("ack = %d want 3", confirmed)
	}
	if sub1.Ack() != confirmed || sub1.Position() != confirmed {
		t.Fatal("duplicate Ack/Position must be idempotent")
	}
	sub1.Close()

	// 重连：从确认位点之后继续，严格拿到 seq 4、5，不重放 1..3。
	sub2, err := s.Subscribe(confirmed)
	if err != nil {
		t.Fatalf("resubscribe: %v", err)
	}
	rest := receiveN(t, sub2, 2)
	if rest[0].Seq != 4 || rest[1].Seq != 5 {
		t.Fatalf("resume delivered %+v, want seq [4 5]", rest)
	}
	for _, rec := range rest {
		if !mat.Apply(rec) {
			t.Fatalf("rec seq %d should apply after reconnect", rec.Seq)
		}
	}

	// 重复补发：再次以同一位点订阅，服务端重发 4、5，应用器识别为重复不生效。
	sub3, err := s.Subscribe(confirmed)
	if err != nil {
		t.Fatalf("duplicate resubscribe: %v", err)
	}
	for _, rec := range receiveN(t, sub3, 2) {
		if mat.Apply(rec) {
			t.Fatalf("redelivered seq %d must not be applied twice", rec.Seq)
		}
	}
}

// TestSnapshotPlusIncrementalEqualsContinuous 严格比对：
// 快照 + 续读增量得到的每窗口最终记录（身份/Kind/位点/聚合）与持续消费一致。
func TestSnapshotPlusIncrementalEqualsContinuous(t *testing.T) {
	inputs := []WindowResult{
		storeRec("BTC", 0, ResultFinal, 1, 10_000),
		storeRec("BTC", 10_000, ResultFinal, 1, 20_000),
		storeRec("BTC", 0, ResultUpdate, 2, 12_000),
		storeRec("ETH", 0, ResultFinal, 1, 12_000),
		storeRec("ETH", 10_000, ResultFinal, 1, 22_000),
		storeRec("BTC", 10_000, ResultUpdate, 2, 22_000),
		storeRec("BTC", 0, ResultUpdate, 3, 14_000),
		storeRec("ETH", 0, ResultUpdate, 2, 14_000),
	}
	s := NewResultStore(64)
	var all []ResultRecord
	for _, r := range inputs {
		all = append(all, s.Append(r))
	}
	continuous := materializeRecords(all)

	// 在第 3 条之后取快照：BTC@0 已 FINAL+UPDATE，BTC@10000 已 FINAL，
	// ETH 两个窗口尚未出现（整条更正链都将走增量）。
	const cut = 3
	s2 := NewResultStore(64)
	for _, r := range inputs[:cut] {
		s2.Append(r)
	}
	snap := s2.Snapshot()
	if snap.Seq != cut {
		t.Fatalf("snapshot seq = %d want %d", snap.Seq, cut)
	}
	for i := 1; i < len(snap.Views); i++ {
		a, b := snap.Views[i-1].Result.Window, snap.Views[i].Result.Window
		if a.Start > b.Start || (a.Start == b.Start && a.Symbol > b.Symbol) {
			t.Fatalf("snapshot views not deterministically ordered: %s before %s", a, b)
		}
	}
	// 继续向 s2 追加剩余结果，再从快照位点续读增量。
	for _, r := range inputs[cut:] {
		s2.Append(r)
	}
	sub, err := s2.Subscribe(snap.Seq)
	if err != nil {
		t.Fatalf("subscribe after snapshot: %v", err)
	}
	inc := receiveN(t, sub, len(inputs)-cut)
	if inc[0].Seq != cut+1 {
		t.Fatalf("first incremental seq = %d want %d", inc[0].Seq, cut+1)
	}
	rebuilt := materializeRecords(append(append([]ResultRecord{}, snap.Views...), inc...))
	if !reflect.DeepEqual(rebuilt, continuous) {
		t.Fatalf("snapshot+incremental != continuous:\nrebuilt=%+v\ncontinuous=%+v", rebuilt, continuous)
	}
}

// TestSubscriberOverflowBeyondHistory 验证落后超出保留范围返回明确可区分结论，
// 且可通过快照恢复，不静默漏过。
func TestSubscriberOverflowBeyondHistory(t *testing.T) {
	const capN = 4
	s := NewResultStore(capN)
	for i := 0; i < 10; i++ {
		s.Append(storeRec("BTC", int64(i)*10_000, ResultFinal, int64(i+1), int64((i+1)*10_000)))
	}
	// 从头订阅：seq1 已滑出（最旧 seq = 10-4+1 = 7）。
	if _, err := s.Subscribe(0); !errors.Is(err, ErrResultPositionExpired) {
		t.Fatalf("want ErrResultPositionExpired, got %v", err)
	}
	// 非法位点可区分。
	if _, err := s.Subscribe(-1); !errors.Is(err, ErrInvalidResultPosition) {
		t.Fatalf("want ErrInvalidResultPosition, got %v", err)
	}

	// 恢复路径：快照 + 从快照位点续读，尾部位点暂无增量。
	snap := s.Snapshot()
	mat := NewResultMaterializer()
	for _, rec := range snap.Views {
		if !mat.Apply(rec) {
			t.Fatal("snapshot view must apply")
		}
	}
	sub, err := s.Subscribe(snap.Seq)
	if err != nil {
		t.Fatalf("subscribe snapshot seq: %v", err)
	}
	// 再追加一条，快照续读者精确拿到该增量。
	newRec := s.Append(storeRec("ETH", 0, ResultFinal, 9, 110_000))
	inc := receiveN(t, sub, 1)
	if inc[0].Seq != newRec.Seq || inc[0].Result.Window.Symbol != "ETH" {
		t.Fatalf("incremental after recovery = %+v", inc)
	}
	if !mat.Apply(inc[0]) {
		t.Fatal("recovered incremental must apply")
	}

	// 消费中途落后超位：先在 seq1 时订阅（不读），再让历史推进超过容量。
	s3 := NewResultStore(capN)
	s3.Append(storeRec("BTC", 0, ResultFinal, 1, 10_000))
	lag, err := s3.Subscribe(0)
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	for i := 1; i < 8; i++ {
		s3.Append(storeRec("BTC", int64(i)*10_000, ResultFinal, int64(i+1), 0))
	}
	if _, err := lag.Receive(context.Background()); !errors.Is(err, ErrResultPositionExpired) {
		t.Fatalf("mid-stream lag must yield ErrResultPositionExpired, got %v", err)
	}
	// 溢出结论持续可区分，不会悄悄返回后续结果。
	if _, err := lag.Receive(context.Background()); !errors.Is(err, ErrResultPositionExpired) {
		t.Fatalf("overflowed subscription must keep reporting expired, got %v", err)
	}
}

// TestStoreHistoryStrictlyBounded 验证历史条数与物化视图都不超过容量。
func TestStoreHistoryStrictlyBounded(t *testing.T) {
	const capN = 5
	s := NewResultStore(capN)
	for i := 0; i < 20; i++ {
		s.Append(storeRec("BTC", 0, ResultUpdate, int64(i+1), int64(i)))
	}
	if s.count != capN {
		t.Fatalf("history len = %d want %d", s.count, capN)
	}
	if len(s.ring) != capN {
		t.Fatalf("ring capacity grew: %d", capN)
	}
	if s.seq != 20 {
		t.Fatalf("latest seq = %d want 20", s.seq)
	}
	if _, err := s.Subscribe(0); !errors.Is(err, ErrResultPositionExpired) {
		t.Fatalf("seq 0 must be expired, got %v", err)
	}
	sub, err := s.Subscribe(20 - capN) // 最旧可续读位点
	if err != nil {
		t.Fatalf("subscribe at oldest: %v", err)
	}
	recs := receiveN(t, sub, capN)
	if recs[0].Seq != ResultSeq(20-capN+1) || recs[capN-1].Seq != 20 {
		t.Fatalf("retained window = %+v", recs)
	}

	// 多窗口滑出后视图条数同样有界（<=容量）。
	s4 := NewResultStore(capN)
	for i := 0; i < 20; i++ {
		s4.Append(storeRec("S", int64(i)*10_000, ResultFinal, 1, 0))
	}
	if len(s4.Snapshot().Views) > capN {
		t.Fatalf("view entries exceed capacity %d", capN)
	}
}

// TestSubscriptionWaitCloseAndCancel 验证阻塞唤醒、EOF、自身关闭与 ctx 取消可区分。
func TestSubscriptionWaitCloseAndCancel(t *testing.T) {
	s := NewResultStore(8)
	sub, err := s.Subscribe(0)
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	got := make(chan ResultRecord, 1)
	go func() {
		rec, _ := sub.Receive(context.Background())
		got <- rec
	}()
	time.Sleep(20 * time.Millisecond)
	appended := s.Append(storeRec("BTC", 0, ResultFinal, 1, 10_000))
	select {
	case rec := <-got:
		if rec.Seq != appended.Seq {
			t.Fatalf("woken with seq %d want %d", rec.Seq, appended.Seq)
		}
	case <-time.After(time.Second):
		t.Fatal("Receive did not wake on Append")
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := sub.Receive(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled Receive = %v want context.Canceled", err)
	}

	sub.Close()
	if _, err := sub.Receive(context.Background()); !errors.Is(err, ErrSubscriptionClosed) {
		t.Fatalf("closed sub Receive = %v want ErrSubscriptionClosed", err)
	}
	sub.Close() // 重复关闭幂等

	// 存储关闭后：先排空剩余结果，再 EOF。
	s.Append(storeRec("ETH", 0, ResultFinal, 1, 10_000))
	sub2, err := s.Subscribe(0)
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	s.Close()
	recs := receiveN(t, sub2, 2)
	if recs[0].Seq != 1 || recs[1].Seq != 2 {
		t.Fatalf("drain before EOF = %+v", recs)
	}
	if _, err := sub2.Receive(context.Background()); !errors.Is(err, io.EOF) {
		t.Fatalf("after drain = %v want io.EOF", err)
	}
}

// TestStoreConcurrentSubscribersStrictOrder 单写者 + 多订阅者并发：
// 每个订阅者收到的位点必须从其起点连续到末尾，无重复、无缺口、严格递增。
func TestStoreConcurrentSubscribersStrictOrder(t *testing.T) {
	const total = 500
	s := NewResultStore(total + 1) // 容量足够，所有订阅者都不应溢出
	var wg sync.WaitGroup
	starts := []ResultSeq{0, 1, 50, 250, 499}
	errs := make(chan error, len(starts))
	for i, start := range starts {
		sub, err := s.Subscribe(start)
		if err != nil {
			t.Fatalf("subscribe %d: %v", i, err)
		}
		wg.Add(1)
		go func(idx int, from ResultSeq) {
			defer wg.Done()
			want := from + 1
			for want <= total {
				rec, err := sub.Receive(context.Background())
				if err != nil {
					errs <- fmt.Errorf("subscriber %d: %w", idx, err)
					return
				}
				if rec.Seq != want {
					errs <- fmt.Errorf("subscriber %d got seq %d want %d (gap/duplicate)", idx, rec.Seq, want)
					return
				}
				if idx%2 == 0 {
					runtime.Gosched() // 交替让出，制造真实并发交错
				}
				want++
			}
			if pos := sub.Position(); pos != total {
				errs <- fmt.Errorf("subscriber %d final position %d want %d", idx, pos, total)
			}
		}(i, start)
	}
	kind := []ResultKind{ResultFinal, ResultUpdate}
	for i := 1; i <= total; i++ {
		s.Append(storeRec("BTC", int64((i%7))*10_000, kind[i%2], int64(i), 0))
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}
}

// TestMaterializerOrderingGuards 验证新值不被旧值覆盖、重复不生效。
func TestMaterializerOrderingGuards(t *testing.T) {
	m := NewResultMaterializer()
	wk := WindowKey{Symbol: "BTC", Start: 0, End: 10_000}
	r1 := ResultRecord{Seq: 1, Result: storeRec("BTC", 0, ResultFinal, 1, 0)}
	r2 := ResultRecord{Seq: 2, Result: storeRec("BTC", 0, ResultUpdate, 2, 0)}
	if !m.Apply(r1) || !m.Apply(r2) {
		t.Fatal("newer records must apply")
	}
	if m.Apply(r2) {
		t.Fatal("same record must not apply twice")
	}
	if m.Apply(r1) {
		t.Fatal("older record must not overwrite newer value")
	}
	if got := m.last[wk]; got != 2 {
		t.Fatalf("last seq = %d want 2", got)
	}
}
