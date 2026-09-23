package marketstream

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

// checkpointAgg 是聚合状态的可序列化形式。
type checkpointAgg struct {
	EventCount int64   `json:"event_count"`
	TotalQty   int64   `json:"total_qty"`
	Notional   float64 `json:"notional"`
	FirstTime  int64   `json:"first_time"`
	LastTime   int64   `json:"last_time"`
}

// checkpointWindow 是窗口状态的可序列化形式。
type checkpointWindow struct {
	Symbol string        `json:"symbol"`
	Start  int64         `json:"start"`
	End    int64         `json:"end"`
	Agg    checkpointAgg `json:"agg"`
}

// Checkpoint 是一次一致性快照：分区偏移、去重表、水位线与窗口状态，
// 在处理循环中的屏障点生成，因此彼此严格一致（无在途事件）。
type Checkpoint struct {
	Positions    map[int]int64      `json:"positions"`      // 每分区已完全处理的下一条偏移
	Dedup        map[string]int64   `json:"dedup"`          // 事件 ID 字符串 -> 事件时间
	Watermark    int64              `json:"watermark"`      // 快照时全局水位线
	MaxEventTime map[int]int64      `json:"max_event_time"` // 每分区最大事件时间（恢复水位线）
	Windows      []checkpointWindow `json:"windows"`        // 全部未清除窗口的状态
}

// NewCheckpoint 返回空检查点。
func NewCheckpoint() *Checkpoint {
	return &Checkpoint{
		Positions:    make(map[int]int64),
		Dedup:        make(map[string]int64),
		MaxEventTime: make(map[int]int64),
		Windows:      nil,
	}
}

// encodeDedupID 把事件 ID 编码为去重表的字符串键：p<分区>#<偏移>。
func encodeDedupID(id EventID) string { return id.String() }

// decodeDedupKey 把字符串键还原为事件 ID。
func decodeDedupKey(s string) (EventID, error) {
	rest, ok := strings.CutPrefix(s, "p")
	if !ok {
		return EventID{}, fmt.Errorf("invalid dedup key %q", s)
	}
	partStr, offStr, ok := strings.Cut(rest, "#")
	if !ok {
		return EventID{}, fmt.Errorf("invalid dedup key %q", s)
	}
	part, err := strconv.Atoi(partStr)
	if err != nil {
		return EventID{}, fmt.Errorf("invalid dedup key %q: %w", s, err)
	}
	off, err := strconv.ParseInt(offStr, 10, 64)
	if err != nil {
		return EventID{}, fmt.Errorf("invalid dedup key %q: %w", s, err)
	}
	return EventID{Partition: part, Offset: off}, nil
}

// SaveCheckpoint 以 JSON 原子写入（临时文件 + rename）持久化检查点，
// 避免崩溃产生半截文件。
func SaveCheckpoint(path string, cp *Checkpoint) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return fmt.Errorf("create checkpoint dir: %w", err)
	}
	tmp := path + ".tmp"
	f, err := os.Create(tmp)
	if err != nil {
		return fmt.Errorf("create temp checkpoint: %w", err)
	}
	enc := json.NewEncoder(f)
	enc.SetIndent("", "  ")
	if err := enc.Encode(cp); err != nil {
		f.Close()
		os.Remove(tmp)
		return fmt.Errorf("encode checkpoint: %w", err)
	}
	if err := f.Sync(); err != nil {
		f.Close()
		os.Remove(tmp)
		return fmt.Errorf("sync checkpoint: %w", err)
	}
	if err := f.Close(); err != nil {
		os.Remove(tmp)
		return fmt.Errorf("close checkpoint: %w", err)
	}
	if err := os.Rename(tmp, path); err != nil {
		os.Remove(tmp)
		return fmt.Errorf("rename checkpoint: %w", err)
	}
	return nil
}

// LoadCheckpoint 从 JSON 文件读取检查点；文件不存在时返回 (nil, ErrNoCheckpoint)。
func LoadCheckpoint(path string) (*Checkpoint, error) {
	f, err := os.Open(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, ErrNoCheckpoint
		}
		return nil, fmt.Errorf("open checkpoint: %w", err)
	}
	defer f.Close()
	var cp Checkpoint
	if err := json.NewDecoder(f).Decode(&cp); err != nil {
		return nil, fmt.Errorf("decode checkpoint: %w", err)
	}
	if cp.Positions == nil {
		cp.Positions = make(map[int]int64)
	}
	if cp.Dedup == nil {
		cp.Dedup = make(map[string]int64)
	}
	if cp.MaxEventTime == nil {
		cp.MaxEventTime = make(map[int]int64)
	}
	return &cp, nil
}

// ErrNoCheckpoint 表示尚不存在任何检查点（首次运行）。
var ErrNoCheckpoint = errors.New("no checkpoint file")
