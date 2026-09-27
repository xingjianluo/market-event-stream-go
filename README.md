# 本地行情事件流处理管线（market-event-stream-go）

一个纯本地、零外部依赖的事件流处理管线，用内存分区事件源模拟多分区行情，
演示并验证：**按事件时间的多分区窗口聚合、水位线推进、迟到处理、幂等去重、
有界背压、检查点故障恢复，以及可断点续读的结果订阅与审计回放**。

核心目标：让乱序、重复、迟到、分区交错、到达顺序/批大小变化与故障重放，
都得到**一致且可解释**的聚合结论。

## 快速开始

```bash
# 端到端演示：交错/乱序 + 重复 + 宽限内迟到 + 超宽限迟到 + 检查点崩溃恢复
go run ./cmd/demo

# 全部单元测试（-v 可看到逐条事件的水位线、事件时间与判定依据）
go test -race -count=1 -v ./...
```

## 一、窗口语义

- 采用**事件时间**（`Event.EventTime`，epoch 毫秒）而非处理时间进行聚合，
  因此结果只取决于事件集合，不取决于何时到达。
- 滚动窗口：`start = floor(eventTime / WindowSize) * WindowSize`，窗口为
  左闭右开 `[start, start+WindowSize)`，对负时间也按向下取整处理。
- 每个 `(symbol, window)` 维护一个聚合：成交笔数、总量、名义金额（Σprice·qty）、
  窗口内最小/最大事件时间，并提供 VWAP。
- **结果与到达顺序无关**：所有状态由单个处理 goroutine 串行持有，分区如何交错、
  批大小如何变化，只要事件集合相同，逐窗口聚合结果就逐字段一致
  （见 `TestInterleavingOrderAndBatchIndependent`）。

窗口结果分两种：

| Kind   | 含义 |
|--------|------|
| FINAL  | 全局水位线到达窗口结束时间后发出的最终结果 |
| UPDATE | 已最终化窗口在迟到宽限期内被迟到事件更正后发出的结果（下游应用新值覆盖旧值） |

## 二、水位线语义

- 分区水位线：`partWM = 该分区已观察到的最大事件时间 - MaxOutOfOrderness`。
- 全局水位线 = **所有活跃分区分区水位线的最小值**，且**单调不减**。
  - 尚未观察到任何事件的已知分区以 `-∞`（`NoWatermark`）参与最小值，
    防止某个还没开始发数的分区导致窗口被**过早关闭**；空分区读到 EOF 后立即解除。
  - 分区内乱序的更早事件不改变该分区最大事件时间，因而不回退水位线。
- **停滞分区处理**：某分区在 `IdleTimeout`（处理时间）内无事件，会被临时标记为
  空闲并移出全局最小值，使其他快分区对应的**已完成窗口不被无限阻塞**；
  一旦该分区重新来事件即恢复活跃，全局水位线**绝不回退**。
  全部分区同时空闲时不会凭空推进水位线；所有分区 EOF 后水位线到 `+∞`
  （`MaxWatermark`）以关闭全部窗口。
- 水位线在四个位置推进：观察到新事件、空闲检测 tick、分区 EOF、检查点恢复。
  每次推进都会打印 `old -> new`。

## 三、迟到与一致性策略

每条事件进入管线后产生一个**可区分的判定（Verdict）**，绝不会静默丢弃：

| Decision      | 触发条件 | 处理结论 |
|---------------|----------|----------|
| `ACCEPTED`    | 事件所属窗口尚未结束 | 正常计入活动窗口 |
| `DUPLICATE`   | 事件 `(partition, offset)` 已处理过 | 幂等忽略，**不重复计入** |
| `LATE_UPDATE` | 窗口已最终化，但 `windowEnd > watermark - AllowedLateness` | 在宽限期内更新窗口，发出 `UPDATE` 更正 |
| `DROPPED_LATE`| `windowEnd <= watermark - AllowedLateness`，窗口状态已清除 | 进入**显式侧输出** `Rejections()`，附带水位线与判定原因 |

- **幂等去重**：`DedupTable` 以 `(partition, offset)` 为键。重复投递或故障重放
  同一事件至多计入一次。去重表随水位线按 `watermark - AllowedLateness` 边界裁剪，
  与窗口清除边界一致，保证内存有界。
- **迟到宽限**：窗口在发出 FINAL 后仍保留到 `windowEnd <= watermark-AllowedLateness`
  才清除；期间的迟到事件产生 UPDATE；超期事件进入侧输出而非静默丢失。
- 判定时同时记录 `WatermarkBefore/After`、事件时间、窗口与人类可读 `Reason`，
  测试日志中可直接复盘每条事件的判定依据。

## 四、背压与并发

- 单 goroutine 串行持有全部窗口/水位线/去重/偏移状态，无共享可变状态、无细粒度锁。
- 输入是容量为 `InputQueueCapacity` 的**有界队列**：队列满时上游 `Ingest` 阻塞，
  自然形成背压，慢消费者或流量突增不会造成内存或 goroutine 无界增长。
- 结果/侧输出/判定均为**有界通道**；下游停滞时处理循环阻塞在发送上，进而使
  输入队列填满并反压到生产者。
- `Feed` 为每个分区启动一个投递 goroutine（数量恒等于分区数，不随事件数增长），
  分区内严格保序，读完自动 `MarkComplete`。
- 处理时间通过 `Clock` 接口注入；测试用虚拟时钟确定性驱动空闲超时，无 `sleep` 竞态。

## 五、检查点与故障恢复

- `Checkpoint()` 在串行处理流中插入**屏障**，在屏障点原子导出一致快照：
  每分区已处理偏移、去重表、全局水位线与各分区最大事件时间、全部活动窗口状态。
- `SaveCheckpoint` 用「临时文件 + fsync + rename」原子落盘为 JSON，
  崩溃不会留下半截文件；文件不存在时 `LoadCheckpoint` 返回 `ErrNoCheckpoint`。
- `Restore(cp)` 在 `Start` 前重建全部状态。恢复后把本地事件源 `Seek` 到已提交偏移；
  即使源至少一次投递、把已提交事件重放，去重表也会将其判为 `DUPLICATE`，
  **重放后的结果与无故障运行逐窗口一致**（见
  `TestCheckpointRestoreReplayEqualsFaultFree`）。

## 六、可断点续读的结果订阅与审计回放（ResultBus）

在处理器 `Results()` 通道之上提供一层**独立、非侵入**的结果总线 `ResultBus`：
处理器的窗口/水位线/迟到/背压/检查点语义完全不变（它只是被挂接的结果通道的消费者），
总线额外提供**稳定位点订阅、断线续读、完整快照 + 增量、有界回放与容量保护**。

### 6.1 位点与订阅语义

- 总线只有一个后台 goroutine 顺序消费处理器结果，为每条 `WindowResult` 分配一个
  **全局单调、严格按发出顺序**的整数位点 `Position`（第一条为 1，`0` 表示从头）。
- `Subscribe(from)` 严格交付序号 **> from** 的结果：
  - 订阅者只需把「最后处理完的位点」持久化（`Ack` 仅做本地记账，幂等、单调取最大、
    可重复调用）；掉线/进程重启后用该位点重新订阅即可**只拿到之后的结果**；
  - `from` 晚于当前尾端时**不夹取也不跳结果**：保留该绝对位点并阻塞等待，
    直到日志推进到那里（支持总线刚启动就携带外部已提交位点提前注册）；
  - 因为位点序与发出序一致，同一窗口的 `FINAL -> UPDATE -> UPDATE` 更正链先后关系
    天然保留，**不会旧值盖新值**；单个订阅生命周期内同一位点**只交付一次**，
    不会重复应用（重连用新订阅从已确认位点开始）。
- `Next(ctx)` 为拉模型：赶上尾端时阻塞等待新结果（可用 ctx 超时/取消中断，订阅仍有效）；
  返回值结论始终**可区分**，见下表，绝不静默漏结果或悄悄跳过。

| 返回 | 含义与恢复动作 |
|------|----------------|
| `SequencedResult` | 正常交付，位点严格递增 |
| `ErrPositionExpired` | 进度已被有界历史轮转越过：**重新 `Snapshot()`，应用视图后从快照边界续读** |
| `ErrBusClosed` | 上游结果通道已关闭且保留结果已排空（关闭前会先排空） |
| `ErrSubscriptionClosed` | 本订阅已 `Close()` |
| `context 错误` | 等待期间被取消/超时；订阅仍有效，可继续 `Next` 或稍后重连 |
| `ErrTooManySubscribers` | 在线订阅者达到 `MaxSubscribers` 上限 |

### 6.2 快照与增量的严格一致关系

`Snapshot()` 在当前日志尾端（位点 `S`）给出一个一致性完整视图：每个窗口在
`<= S` 范围内的**最新一条结果**（FINAL 或最后一条 UPDATE），并返回边界 `S` 与
当时可回放的最旧位点 `Oldest`。恢复方：

1. 应用快照：把 `Windows` 按窗口身份装入自己的物化视图；
2. `Subscribe(S)`：严格接收序号 `> S` 的增量，按窗口折叠（位点大者覆盖小者）。

不变式：**快照视图 + 边界后增量 == 从不断线、从序号 1 连续消费折叠出的视图**，
逐字段严格一致（窗口身份、FINAL/UPDATE 更正关系、`EventCount/TotalQty/Notional/
FirstTime/LastTime` 全部用 `==` 比对，不用近似相等）。
新订阅者或落后太多的订阅者因此不必回放全部历史，先取快照即可廉价追平。

### 6.3 有界回放与容量保护

- 结果历史是**定容环形缓冲**（`HistoryCapacity`，按条数），运行时间再长也只保留
  最近 N 条；同时维护「每窗口位点 FIFO + 最新视图」，其总条目恒等于历史条数，内存有界。
- 订阅**不缓存结果**，读取直接命中共享历史；后台 goroutine 恒为 1 个，
  订阅者再多（`<= MaxSubscribers`）、再慢或完全不读，都**不会**增加内存或后台任务，
  也**不会反压处理管线**（处理器原有背压仅取决于它自己的有界通道，语义不变）。
- 历史轮转越过某订阅进度时，总线**主动摘除**该订阅；它的下一次 `Next/Subscribe`
  得到明确的 `ErrPositionExpired`，恢复方式就是「重新取快照 + 从快照边界续读」，
  不会静默漏掉一条结果。
- 即使所有订阅者都不读，处理器 `Close()` 后结果通道关闭，总线排空残余（仅 O(通道缓冲)）
  后在**有限时间内**停止并回收 goroutine（见 `TestE2EFiniteStopWithIdleSubscriber`）。

### 6.4 用法要点

```go
bus, _ := marketstream.NewResultBus(marketstream.DefaultResultBusConfig())
proc, _ := marketstream.NewProcessor(cfg, parts)
proc.Start(ctx)
bus.Attach(proc) // 挂接后由总线独占消费 Results()

sub, _ := bus.Subscribe(committed) // 重启时传入上次持久化的位点
for {
    r, err := sub.Next(ctx)
    if errors.Is(err, marketstream.ErrPositionExpired) {
        snap := bus.Snapshot()        // 落后太多：先取快照
        applyView(snap.Windows)       // 装入完整视图
        sub, _ = bus.Subscribe(snap.Position) // 再从边界续读
        continue
    }
    // ... 处理 r.Result ...
    committed = sub.Ack(r.Position)   // 持久化 committed
}
```

## 七、可配置项

### 7.1 处理管线（`Config`）

| 字段 | 单位 | 默认 | 含义 |
|------|------|------|------|
| `WindowSize` | 毫秒 | 10000 | 滚动事件时间窗口长度 |
| `AllowedLateness` | 毫秒 | 5000 | FINAL 后仍接受更正的迟到宽限；也是窗口/去重清除边界 |
| `MaxOutOfOrderness` | 毫秒 | 2000 | 分区水位线 = 最大事件时间 − 该值 |
| `IdleTimeout` | 处理时间 | 2s | 分区多久无事件后临时视为空闲；0 关闭空闲检测 |
| `InputQueueCapacity` | 条数 | 256 | 输入有界队列长度（背压阈值） |
| `ResultBufferSize` | 条数 | 64 | 窗口结果通道缓冲 |
| `RejectBufferSize` | 条数 | 64 | 侧输出/判定通道缓冲 |
| `Clock` | — | 系统时钟 | 处理时间来源（可注入虚拟时钟） |
| `Logger` | — | stdout | `slog` 结构化日志 |

### 7.2 结果总线（`ResultBusConfig`）

| 字段 | 单位 | 默认 | 含义 |
|------|------|------|------|
| `HistoryCapacity` | 条数 | 4096 | 有界结果历史长度：决定可回放范围、慢订阅容忍度与快照覆盖窗口数；0 取默认 |
| `MaxSubscribers` | 个 | 256 | 同时在线订阅者上限，超出 `Subscribe` 返回 `ErrTooManySubscribers`；0 取默认 |
| `Logger` | — | 丢弃 | 结果总线的 `slog` 日志 |

## 八、代码结构

| 文件 | 职责 |
|------|------|
| `event.go` | 事件与事件 ID 类型 |
| `window.go` / `aggregate.go` | 窗口分配与窗口聚合（含 VWAP） |
| `watermark.go` | 分区/全局水位线追踪、空闲与完成处理 |
| `dedup.go` | 事件级幂等去重与有界裁剪 |
| `source.go` | 内存分区事件源，支持偏移 `Seek` 回放（带锁，可并发分区读取） |
| `processor.go` | 串行处理循环、判定、窗口触发、背压、屏障、恢复、输出通道 |
| `checkpoint.go` | 检查点结构、JSON 原子持久化与 ID 编解码 |
| `resultbus.go` | 稳定位点、有界环形历史、按位点订阅/确认、快照视图与溢出摘除 |
| `config.go` / `clock.go` | 配置校验与时钟抽象 |
| `cmd/demo/main.go` | 端到端演示（含结果订阅/快照/溢出恢复段落） |

## 九、本地验证与测试场景

```bash
# 常规
go test -race -count=1 -v ./...

# 仅结果订阅/回放层（直接向总线投喂结果，快速且确定）
go test -race -count=1 -v -run 'TestBus|TestSubscription|TestSnapshot|TestSlowSubscriber|TestMaxSubscribers|TestBoundedHistory'

# 与真实处理器端到端集成（交错/快照续读/故障重放/容量回收）
go test -race -count=1 -v -run 'TestE2E'

# 可观察演示（最后一段输出 app-online / app-resume / new-sub / auditor / recover）
go run ./cmd/demo
```

结果订阅/回放相关用例：

- `TestBusAssignsStableMonotonicPositions`：位点从 1 单调，同窗口 FINAL/UPDATE 成链不乱序。
- `TestSubscriptionResumeAfterDisconnect`：掉线后用已确认位点续读，不重不漏、赶上后阻塞等待。
- `TestDuplicateAckIdempotent`：重复/乱序确认单调取最大；同一位点在单订阅内不补发两次。
- `TestSnapshotPlusIncrementalEqualsContinuous` / `TestSnapshotAfterRotationInvariant`：
  快照 + 增量与连续消费**逐字段严格一致**；轮转后不变式仍成立。
- `TestSlowSubscriberOverflowAndRecovery`：不读的慢订阅者被主动摘除并得到
  `ErrPositionExpired`，其他订阅与管线不受影响，重新取快照可无损恢复。
- `TestMaxSubscribersCap` / `TestBoundedHistoryRetention`：订阅者数与历史长度严格有界。
- `TestBusDrainsHistoryAfterClose` / `TestE2EFiniteStopWithIdleSubscriber`：
  关闭后排空保留结果再给 `ErrBusClosed`；订阅者全不读也能有限时间停止并回收 goroutine。
- `TestE2EInterleavingBatchStrictConsistency`：同一批事件在不同分区交错/批大小下，
  总线位点流折叠视图逐字段一致。
- `TestE2EDeterministicCorrectionChain`：真实处理器下确定性的 FINAL→UPDATE 更正链。
- `TestE2ESnapshotResumeEqualsContinuous`：真实处理器运行中快照 + 增量与连续消费严格一致。
- `TestE2EFaultReplayViewConsistency`：检查点崩溃恢复并强制重放已提交事件后，
  新总线视图与无故障运行一致，重放事件判为 DUPLICATE。
- `TestE2EBurstBoundedGoroutines`：突发流量下后台 goroutine 数稳定。

原有用例（窗口、水位线、迟到、去重、背压、检查点恢复）全部保留且不回归，
默认配置与 `NewProcessor/Start/Ingest/Results/Checkpoint/Restore/Close` 等既有调用方式不变。
