package pipeline

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"time"
)

// snapshot 是管线某一时刻的完整一致性状态。
// 它只在聚合器 goroutine 内生成与消费：此时输入处理暂停，
// 因而"分区偏移 + 水位线 + 窗口内容"是同一逻辑时刻的一致快照。
type snapshot struct {
	TakenAt time.Time
	WM      wmSnapshotData
	Offsets map[PartitionID]int64 // 各分区已确认处理的最后偏移（下一条从 +1 读起）
	Windows []*windowState        // 仍保留状态的窗口（含事件明细，可完整重建聚合）
	Stats   Stats
}

const checkpointFileName = "checkpoint.json"

// ---- JSON 传输结构（map 键/内部类型做可序列化转换） ----

type partWire struct {
	Mark     time.Time `json:"mark"`
	Marked   bool      `json:"marked"`
	MaxEvent time.Time `json:"max_event"`
	Idle     bool      `json:"idle"`
	Finished bool      `json:"finished"`
}

type windowWire struct {
	Symbol  string    `json:"symbol"`
	Start   time.Time `json:"start"`
	End     time.Time `json:"end"`
	Fired   bool      `json:"fired"`
	FiredBy time.Time `json:"fired_by"`
	Events  []Event   `json:"events"`
}

type snapshotWire struct {
	Version         int                 `json:"version"`
	TakenAt         time.Time           `json:"taken_at"`
	GlobalWatermark time.Time           `json:"global_watermark"`
	Partitions      map[string]partWire `json:"partitions"`
	Offsets         map[string]int64    `json:"offsets"`
	Windows         []windowWire        `json:"windows"`
	Stats           Stats               `json:"stats"`
}

func (snap *snapshot) toWire() snapshotWire {
	w := snapshotWire{
		Version:         1,
		TakenAt:         snap.TakenAt,
		GlobalWatermark: snap.WM.Global,
		Partitions:      make(map[string]partWire, len(snap.WM.States)),
		Offsets:         make(map[string]int64, len(snap.Offsets)),
		Windows:         make([]windowWire, 0, len(snap.Windows)),
		Stats:           snap.Stats,
	}
	for p, st := range snap.WM.States {
		w.Partitions[string(p)] = partWire{Mark: st.mark, Marked: st.marked, MaxEvent: st.maxEvent, Idle: st.idle, Finished: st.finished}
	}
	for p, off := range snap.Offsets {
		w.Offsets[string(p)] = off
	}
	for _, st := range snap.Windows {
		w.Windows = append(w.Windows, windowWire{
			Symbol: st.symbol, Start: st.start, End: st.end,
			Fired: st.fired, FiredBy: st.firedBy, Events: st.events,
		})
	}
	return w
}

// checkpointStore 负责快照的持久化与读取。
type checkpointStore struct {
	dir string
}

func newCheckpointStore(dir string) *checkpointStore { return &checkpointStore{dir: dir} }

// path 返回快照文件路径；dir 为空时返回空串。
func (s *checkpointStore) path() string {
	if s.dir == "" {
		return ""
	}
	return filepath.Join(s.dir, checkpointFileName)
}

// save 原子写入一次快照（临时文件 + fsync 目录前的 rename；本地演示用）。
// dir 为空时为 no-op。
func (s *checkpointStore) save(snap *snapshot) error {
	path := s.path()
	if path == "" {
		return nil
	}
	if err := os.MkdirAll(s.dir, 0o755); err != nil {
		return err
	}
	data, err := json.MarshalIndent(snap.toWire(), "", "  ")
	if err != nil {
		return err
	}
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, data, 0o644); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}

var errNoCheckpoint = errors.New("no checkpoint")

// load 读取最近一次快照；不存在时返回 nil, nil。
// cfg 提供窗口参数（恢复出的窗口状态由事件明细重算聚合值）。
func (s *checkpointStore) load(cfg Config) (*snapshot, error) {
	path := s.path()
	if path == "" {
		return nil, nil
	}
	data, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var w snapshotWire
	if err := json.Unmarshal(data, &w); err != nil {
		return nil, err
	}
	snap := &snapshot{
		TakenAt: w.TakenAt,
		Offsets: make(map[PartitionID]int64, len(w.Offsets)),
		Windows: make([]*windowState, 0, len(w.Windows)),
		Stats:   w.Stats,
	}
	states := make(map[PartitionID]partWaterState, len(w.Partitions))
	for p, pw := range w.Partitions {
		states[PartitionID(p)] = partWaterState{
			mark: pw.Mark, marked: pw.Marked, maxEvent: pw.MaxEvent,
			idle: pw.Idle, finished: pw.Finished,
		}
	}
	snap.WM = wmSnapshotData{Global: w.GlobalWatermark, States: states}
	for p, off := range w.Offsets {
		snap.Offsets[PartitionID(p)] = off
	}
	agg := newWindowAgg(cfg.WindowSize, cfg.AllowedLateness)
	for _, ww := range w.Windows {
		st := agg.restoreWindow(ww.Symbol, ww.Start, ww.End, ww.FiredBy, ww.Fired, ww.Events)
		snap.Windows = append(snap.Windows, st)
	}
	return snap, nil
}
