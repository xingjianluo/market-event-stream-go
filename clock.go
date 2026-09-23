package marketstream

import "time"

// Clock 抽象处理时间来源，便于在测试中以确定性的虚拟时间驱动空闲超时。
type Clock interface {
	Now() time.Time
}

// wallClock 是默认的系统墙上时钟。
type wallClock struct{}

func (wallClock) Now() time.Time { return time.Now() }
