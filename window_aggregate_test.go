package marketstream

import "testing"

func TestAssignWindow(t *testing.T) {
	cases := []struct {
		name      string
		ts, size  int64
		wantStart int64
		wantEnd   int64
	}{
		{"start boundary", 0, 10_000, 0, 10_000},
		{"inside window", 12_345, 10_000, 10_000, 20_000},
		{"end excluded", 10_000, 10_000, 10_000, 20_000},
		{"negative ts floors", -1, 10_000, -10_000, 0},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			start, end := AssignWindow(tc.ts, tc.size)
			if start != tc.wantStart || end != tc.wantEnd {
				t.Fatalf("AssignWindow(%d,%d) = [%d,%d), want [%d,%d)",
					tc.ts, tc.size, start, end, tc.wantStart, tc.wantEnd)
			}
		})
	}
}

func TestAggregateApplyAndVWAP(t *testing.T) {
	var a Aggregate
	a.Apply(mkEvent(0, 0, "BTC", 1_000, 100.0, 2))
	a.Apply(mkEvent(0, 1, "BTC", 1_500, 110.0, 3))

	if a.EventCount != 2 || a.TotalQty != 5 {
		t.Fatalf("unexpected count/qty: %+v", a)
	}
	if a.FirstTime != 1_000 || a.LastTime != 1_500 {
		t.Fatalf("unexpected first/last time: %+v", a)
	}
	// VWAP = (100*2 + 110*3) / 5 = 106
	if v := a.VWAP(); v < 105.999 || v > 106.001 {
		t.Fatalf("VWAP = %v, want ~106", v)
	}
}

func TestDedupTable(t *testing.T) {
	d := NewDedupTable()
	id := EventID{Partition: 1, Offset: 7}
	if !d.Add(id, 100) {
		t.Fatal("first add must be accepted")
	}
	if d.Add(id, 100) {
		t.Fatal("second add must be duplicate")
	}
	if !d.Seen(id) {
		t.Fatal("Seen should be true")
	}
	if d.Len() != 1 {
		t.Fatalf("Len = %d, want 1", d.Len())
	}
	// 早于 wm-lateness 的条目会被裁剪
	d.Add(EventID{Partition: 2, Offset: 1}, 16_000)
	n := d.PurgeBefore(20_000, 5_000) // retainBefore=15000
	if n != 1 || d.Len() != 1 {
		t.Fatalf("PurgeBefore removed=%d len=%d, want 1/1", n, d.Len())
	}
}
