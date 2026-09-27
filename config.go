package marketstream

import (
	"errors"
	"fmt"
	"log/slog"
	"os"
	"time"
)

// Config 是管线的全部可配置项（时间单位除 IdleTimeout 外均为毫秒）。
type Config struct {
	// WindowSize 是滚动事件时间窗口长度（毫秒，[start, start+WindowSize)）。
	WindowSize int64
	// AllowedLateness 是水位线越过窗口结束后，仍接受迟到更新的宽限（毫秒）。
	AllowedLateness int64
	// MaxOutOfOrderness 是单分区乱序容忍（毫秒），分区水位线 = 分区最大事件时间 - 该值。
	MaxOutOfOrderness int64
	// IdleTimeout 是分区多久没有事件后被临时视为空闲的处理时间时长；
	// 空闲分区暂时移出全局水位线最小值，恢复活动后重新计入。
	IdleTimeout time.Duration
	// InputQueueCapacity 是输入有界队列长度；队列满时 Ingest 阻塞，向上游形成背压。
	InputQueueCapacity int
	// ResultBufferSize 是窗口结果通道缓冲。
	ResultBufferSize int
	// RejectBufferSize 是判定/迟到侧输出通道缓冲。
	RejectBufferSize int
	// ResultHistoryCapacity 是结果订阅历史保留的结果条数（按条数有界裁剪，
	// 不随运行时间无限增长）；为 0 时使用默认容量。慢订阅者落后超出该范围后，
	// 订阅返回 ErrResultPositionExpired，需改取快照后续读。
	ResultHistoryCapacity int
	// Clock 注入处理时间；为 nil 时使用系统墙上时钟。
	Clock Clock
	// Logger 注入结构化日志；为 nil 时输出到 stdout。
	Logger *slog.Logger
}

// DefaultConfig 返回面向本地演示与测试的默认配置：
// 10 秒窗口、2 秒乱序容忍、5 秒迟到宽限、2 秒空闲超时、有界队列。
func DefaultConfig() Config {
	return Config{
		WindowSize:            10_000,
		AllowedLateness:       5_000,
		MaxOutOfOrderness:     2_000,
		IdleTimeout:           2 * time.Second,
		InputQueueCapacity:    256,
		ResultBufferSize:      64,
		RejectBufferSize:      64,
		ResultHistoryCapacity: defaultResultHistoryCapacity,
		Clock:                 wallClock{},
		Logger: slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
			Level: slog.LevelInfo,
		})),
	}
}

// Validate 校验配置。分区列表用于保证至少存在一个分区且编号不重复。
func (c Config) Validate(partitions []int) error {
	if c.WindowSize <= 0 {
		return fmt.Errorf("%w: WindowSize must be > 0, got %d", errInvalidConfig, c.WindowSize)
	}
	if c.AllowedLateness < 0 {
		return fmt.Errorf("%w: AllowedLateness must be >= 0, got %d", errInvalidConfig, c.AllowedLateness)
	}
	if c.MaxOutOfOrderness < 0 {
		return fmt.Errorf("%w: MaxOutOfOrderness must be >= 0, got %d", errInvalidConfig, c.MaxOutOfOrderness)
	}
	if c.IdleTimeout < 0 {
		return fmt.Errorf("%w: IdleTimeout must be >= 0, got %s", errInvalidConfig, c.IdleTimeout)
	}
	if c.InputQueueCapacity <= 0 {
		return fmt.Errorf("%w: InputQueueCapacity must be > 0, got %d", errInvalidConfig, c.InputQueueCapacity)
	}
	if c.ResultBufferSize < 0 || c.RejectBufferSize < 0 {
		return fmt.Errorf("%w: channel buffer sizes must be >= 0", errInvalidConfig)
	}
	if c.ResultHistoryCapacity < 0 {
		return fmt.Errorf("%w: ResultHistoryCapacity must be >= 0, got %d", errInvalidConfig, c.ResultHistoryCapacity)
	}
	if c.Clock == nil {
		return fmt.Errorf("%w: Clock must not be nil", errInvalidConfig)
	}
	if len(partitions) == 0 {
		return fmt.Errorf("%w: at least one partition is required", errInvalidConfig)
	}
	seen := make(map[int]struct{}, len(partitions))
	for _, p := range partitions {
		if p < 0 {
			return fmt.Errorf("%w: partition id must be >= 0, got %d", errInvalidConfig, p)
		}
		if _, dup := seen[p]; dup {
			return fmt.Errorf("%w: duplicated partition id %d", errInvalidConfig, p)
		}
		seen[p] = struct{}{}
	}
	return nil
}

// errInvalidConfig 是配置非法错误的哨兵，具体原因由包装信息给出。
var errInvalidConfig = errors.New("invalid config")
