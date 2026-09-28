# 本地行情事件流处理管线（market-event-stream-go）

一个纯本地、零外部依赖的事件流处理管线，用内存分区事件源模拟多分区行情，
演示并验证：**按事件时间的多分区窗口聚合、水位线推进、迟到处理、幂等去重、
有界背压、检查点故障恢复，以及可断点续读的窗口结果订阅、快照衔接与审计回放**。

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
- **输出面有两种语义，由 `Config.ResultOutputMode` 选择**：
  - `ResultOutputLive`（**默认**，零值同默认，旧代码无需改动）：结果/侧输出/判定
    均为**有界通道**；下游停滞时处理循环阻塞在发送上，进而使输入队列填满并反压到
    生产者。窗口结果在写入 `Results()` 通道后才登记到订阅历史，两个输出面集合一致。
  - `ResultOutputSubscriptionOnly`（**只使用结果订阅时使用**）：`ResultStore` 是
    权威结果输出面，每条结果先以 O(1) 登记有界历史，**处理循环绝不等待旧通道**——
    完全不消费 `Results()/Rejections()/Verdicts()` 也不影响结果产生、订阅交付、
    屏障（检查点/结果快照）与关闭；旧通道退化为**尽力而为旁路**（缓冲满即丢弃旁路
    副本，权威数据在订阅面一条不少），此模式下不要把旧通道当权威输出消费。
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

## 六、可断点续读的结果订阅、快照与审计回放

在原有 `Results()` 通道之外，叠加了一层共享的、**有界的结果历史 `ResultStore`**，
为下游提供"按稳定消费位点续读 + 快照衔接增量 + 严格不重不漏"的结果订阅面。

### 6.1 位点（ResultSeq）语义

- 每条窗口结果（FINAL 或 UPDATE）在产生时获得一个 **从 1 开始、单调递增、绝不复用**
  的位点 `ResultSeq`，封装为 `ResultRecord{Seq, Result}`。
- 位点顺序就是结果的产生顺序：同一窗口的 `FINAL → UPDATE → UPDATE…` 更正链严格保序，
  **绝不会旧值盖新值**；不同窗口、不同分区交错产生的结果也有全局确定先后。
- `SubscribeResults(after)` 从 `after` 之后开始，首先交付 `Seq=after+1`。
  - `after=0`：从头订阅；
  - `after=<上次 Ack>`：掉线/重启后精确续读，不会重放已确认结果，也不会跳过任何结果；
  - `Receive()` 交付后用 `Position()` / `Ack()` 读取已交付位点（两者等价）。
    **重复 Ack 是纯读、幂等无副作用**；订阅是纯拉取游标，位点只保存在订阅者一侧，
    存储端不记录订阅者，因此不随订阅数建立任何 goroutine 或每订阅者队列。

### 6.2 重复补发与结果应用器

- 下游重连时若用旧位点重订阅，可能再次收到同一批结果（至少一次语义）。
  `ResultMaterializer` 按"每窗口已应用的最大位点"去重：位点相同或更旧的补发
  **至多生效一次、绝不覆盖更新值**，`Apply` 返回 `false` 明确告知被忽略。

### 6.3 快照 + 增量（新订阅者 / 落后太多）

- `ResultSnapshot(ctx)` 在串行处理循环的**屏障点**取视图，因此与"当前已处理结果"
  严格对齐：包含该位点上**每个窗口的最新结果**（FINAL 或最新 UPDATE），
  按（窗口起点, symbol）**确定性排序**，并带快照位点 `Seq` 与当时水位线。
- 标准衔接流程：

  ```go
  snap, _ := p.ResultSnapshot(ctx)      // 1) 取某时刻完整视图
  for _, rec := range snap.Views {      // 2) 应用完整视图（可经 Materializer 去重）
      materializer.Apply(rec)
  }
  sub, _ := p.SubscribeResults(snap.Seq) // 3) 从快照位点续读 Seq>snap.Seq 的增量
  for { rec, err := sub.Receive(ctx); ... }
  ```

- **一致性保证**：快照 + 增量折叠出的每窗口最终结论，与"从不掉线、从头持续消费"
  得到的结果**逐字段严格一致**——窗口身份、FINAL/UPDATE 更正关系、聚合全字段
  （笔数、总量、Notional、首末时间、位点）都相同，不使用任何近似比较
  （见 `TestProcessorSnapshotThenIncrementalEqualsContinuous`）。

### 6.4 有界回放、溢出与恢复

- 结果历史按 **条数** 有界（`ResultHistoryCapacity`，默认 1024），用定容环形缓冲
  保留最近 N 条；物化视图随环形覆盖同步裁剪，**内存与后台任务不随运行时间增长**。
- 订阅是拉模型且 `Append` 为 O(1) 非阻塞：**慢订阅者、完全不读的订阅者或突发结果量
  都不会阻塞处理链路**，也不会产生每订阅者队列/goroutine；存储端不记录订阅者。
- 订阅者落后到 `after+1` 已滑出保留范围时，`SubscribeResults` / `Receive` 返回
  **明确且可区分的哨兵错误 `ErrResultPositionExpired`**（区别于 `io.EOF`、
  `context.Canceled`、`ErrSubscriptionClosed`）。**绝不静默漏掉或跳过结果**；
  恢复方式唯一且确定：重新 `ResultSnapshot()` 取新视图，应用后从 `snap.Seq` 续读。
  溢出是订阅游标上的终结性结论，重复 `Receive` 仍返回同一错误，不会悄悄补发缺口。
- 即使订阅者**完全不读**，管线也能在有限时间内推进屏障（`Checkpoint()` /
  `ResultSnapshot()` / `CurrentWatermark()`）并被 `Close()` 正常停止、完成资源回收
  （无后台任务需要回收）；存储关闭后，订阅者排空保留范围内剩余结果，最终收到 `io.EOF`。
- 位点序列是**进程内**的：处理器从检查点 `Restore` 后，结果流视为新流（位点从 1
  重新编号，历史清空），跨进程续读同样走"取快照 → 从快照位点续读"。

### 6.5 只使用结果订阅（推荐的独立用法）

当下游**只按消费位点订阅、不再读取原有实时输出通道**时，把配置设为：

```go
cfg.ResultOutputMode = marketstream.ResultOutputSubscriptionOnly
```

语义与保证：

- **独立性**：结果先登记权威 `ResultStore` 再尽力旁路旧通道，处理循环**从不等待**
  `Results()/Rejections()/Verdicts()`。结果量超过 `ResultBufferSize`、
  超过默认历史容量后订阅仍按位点持续交付；同时取快照、检查点、读当前水位线与
  `Close()` 都在有限时间内返回，不会因下游不排空旧输出而停住。
- **背压与容量**：输入侧仍是有界队列背压（保护内存）；结果侧唯一的有界状态是
  定容历史环形 + 物化视图（上限 `ResultHistoryCapacity` 条）。慢/不读订阅者只会
  在自己的游标上收到溢出结论，不影响管线与其他订阅者。旧通道只是旁路，缓冲满时
  丢弃旁路副本，**不可以**在该模式下作为权威结果来源。
- **消费位点与快照的关系**：`ResultSnapshot()` 在串行循环屏障点取视图，
  `snap.Seq` 是"视图恰好覆盖 `Seq<=snap.Seq`"的位点；`SubscribeResults(snap.Seq)`
  交付 `Seq>snap.Seq` 的第一条增量。掉线重连用最后一次 `Ack()` 作为 `after`，
  严格从 `Ack()+1` 续读；重复 Ack 幂等，重复补发由 `ResultMaterializer` 保证
  同一窗口同一位点至多生效一次。
- **溢出恢复三步**：① 收到 `ErrResultPositionExpired` → ② `ResultSnapshot()`
  重取完整视图并按窗口应用（已完全滑出历史的窗口只存在于更早的快照中，
  因此长时间掉线应在溢出前定期取快照）→ ③ `SubscribeResults(snap.Seq)` 衔接后续增量。
  恢复后每个窗口身份、最终类型、关键聚合值、位点与从头持续消费逐字段一致。
- 默认模式 `ResultOutputLive` 行为完全不变：旧通道保留有界背压语义，
  结果在交付旧通道后登记历史，两个输出面结果集合与顺序一致。

## 七、可配置项（`Config`）

| 字段 | 单位 | 默认 | 含义 |
|------|------|------|------|
| `WindowSize` | 毫秒 | 10000 | 滚动事件时间窗口长度 |
| `AllowedLateness` | 毫秒 | 5000 | FINAL 后仍接受更正的迟到宽限；也是窗口/去重清除边界 |
| `MaxOutOfOrderness` | 毫秒 | 2000 | 分区水位线 = 最大事件时间 − 该值 |
| `IdleTimeout` | 处理时间 | 2s | 分区多久无事件后临时视为空闲；0 关闭空闲检测 |
| `InputQueueCapacity` | 条数 | 256 | 输入有界队列长度（背压阈值） |
| `ResultBufferSize` | 条数 | 64 | 窗口结果通道缓冲 |
| `RejectBufferSize` | 条数 | 64 | 侧输出/判定通道缓冲 |
| `ResultHistoryCapacity` | 条数 | 1024 | 结果订阅历史保留条数（环形有界）；为 0 用默认值，落后超出返回 `ErrResultPositionExpired` |
| `ResultOutputMode` | — | `""`（live） | `""`/`ResultOutputLive`：旧通道为输出面、有界背压；`ResultOutputSubscriptionOnly`：订阅面为权威输出，旧通道仅尽力旁路，不读旧通道也能独立工作 |
| `Clock` | — | 系统时钟 | 处理时间来源（可注入虚拟时钟） |
| `Logger` | — | stdout | `slog` 结构化日志 |

## 八、代码结构

| 文件 | 职责 |
|------|------|
| `event.go` | 事件与事件 ID 类型 |
| `window.go` / `aggregate.go` | 窗口分配与窗口聚合（含 VWAP） |
| `watermark.go` | 分区/全局水位线追踪、空闲与完成处理 |
| `dedup.go` | 事件级幂等去重与有界裁剪 |
| `source.go` | 内存分区事件源，支持偏移 `Seek` 回放（带锁，可并发分区读取） |
| `processor.go` | 串行处理循环、判定、窗口触发、背压、屏障、恢复、输出通道、结果订阅入口 |
| `subscription.go` | 有界结果历史、稳定位点订阅、快照物化视图、溢出结论与订阅者侧结果应用器 |
| `checkpoint.go` | 检查点结构、JSON 原子持久化与 ID 编解码 |
| `config.go` / `clock.go` | 配置校验与时钟抽象 |
| `cmd/demo/main.go` | 端到端演示 |

## 九、本地验证与测试场景

```bash
go test -race -count=1 -v ./...
```

覆盖场景与失败可区分性：

- `TestInterleavingOrderAndBatchIndependent`：3 种不同交错/批大小顺序结果逐字段一致。
- `TestWatermark*`：多分区最小值、单调不回退、空闲解除阻塞、全完成到 +∞。
- `TestDuplicateDeliveryNotCounted`：穿插重复投递，笔数/量/金额不重复计入。
- `TestLateDataWithinAndBeyondGrace`：同批中区分 `LATE_UPDATE` 与 `DROPPED_LATE`。
- `TestIdlePartitionUnblocksWindowFinishing`：超时前不关窗、超时后解除、恢复不回退。
- `TestBoundedInputQueueBackpressure` / `TestBoundedFeederGoroutines`：队列严格有界、
  生产者确实被阻塞、突发下 goroutine 数稳定。
- `TestCheckpointRestoreReplayEqualsFaultFree`：崩溃恢复 + 强制重放已提交事件，
  结果与无故障一致，重放事件判为 DUPLICATE。
- 结果订阅/审计回放（`subscription_test.go`、`subscription_pipeline_test.go`）：
  - `TestStoreAppendOrderAndResume`：位点严格递增、掉线按 Ack 精确续读、
    重复确认幂等、重复补发不重复生效；
  - `TestSnapshotPlusIncrementalEqualsContinuous` /
    `TestProcessorSnapshotThenIncrementalEqualsContinuous`：快照 + 增量与持续消费
    逐窗口逐字段严格一致（身份/更正关系/聚合/位点），含窗口整条更正链走增量的情况；
  - `TestSubscriberOverflowBeyondHistory` / `TestSlowSubscriberOverflowDoesNotBlockPipeline`：
    落后超出保留范围返回可区分的 `ErrResultPositionExpired`，取快照可恢复，
    慢/空闲订阅者不阻塞管线、不增加 goroutine；
  - `TestPipelineFiniteStopWithNonReadingSubscriber`：订阅者完全不读，
    管线仍能在有限时间内停止并回收，订阅排空后收到 `io.EOF`；
  - `TestSubscriptionStreamConsistentAcrossOrders` /
    `TestSubscribeAfterCheckpointReplayMatchesFaultFree`：不同分区交错、不同批大小、
    故障重放下订阅面的每窗口最终结论严格一致。
- **只走订阅面、完全不消费旧输出**（`subscription_only_test.go`，多次运行稳定）：
  - `TestSubOnlyDeliveryBeyondDefaultCapacity`：结果量 3 倍于默认历史容量时订阅仍按
    位点连续交付，检查点/结果快照/关闭均在有限时间内返回，关闭后收到 `io.EOF`；
  - `TestSubOnlyNonReadingAndSlowSubscribersBounded`：完全不读者与 64 个空闲订阅者、
    突发 200 条结果下屏障有限时间完成，goroutine 数不随订阅/结果增长，落后者收到
    可区分的 `ErrResultPositionExpired`；
  - `TestSubOnlyOverflowSnapshotRecoveryEqualsContinuous`：溢出结论明确且持续，
    重取快照 + 从快照位点衔接后续增量，可见窗口的身份/类型/聚合/位点与持续消费
    逐字段一致，缺口不静默跳过、增量不越位；
  - `TestSubOnlyReconnectStrictResumeAndIdempotentRedelivery`：按最后 Ack 严格续读，
    FINAL→UPDATE 更正链顺序保持，重复确认幂等、重复补发只生效一次；
  - `TestSubOnlyConsistentAcrossOrdersAndBatches` /
    `TestSubOnlyCheckpointReplayMatchesFaultFree`：不同分区交错/批大小、检查点
    崩溃 + 至少一次重放下，订阅序列逐窗口身份、最终类型、聚合与位点严格一致；
  - `TestSubOnlyLegacyChannelIsBypassAndNeverBlocks` /
    `TestResultOutputModeConfigCompatibility`：旧通道只是尽力旁路、缓冲满也不阻塞
    循环，权威订阅一条不少；默认/零值配置保持 live 旧语义，非法模式被拒绝。

### 本地验证方法

```bash
# 全量（含竞态检测），重复多轮确认无抖动
go test -race -count=10 ./...

# 只验证"只走订阅面"这一组
go test -race -count=5 -run 'TestSubOnly' -v ./...

# 端到端演示（默认 live 模式，消费旧输出面）
go run ./cmd/demo
```

所有测试通过 `slog` 打印水位线（含 `-inf/+inf`）、事件时间、窗口与判定原因，
失败断言会指出具体不一致的窗口、聚合字段或判定类别，便于区分根因。
