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
- **结果/侧输出/判定与处理循环解耦（关键）**：处理循环只把结果以 O(1)、**绝不阻塞**的方式
  写入有界结果历史（订阅面），把侧输出、逐条判定写入各自的有界暂存邮箱；再由**固定的
  三个**接力 goroutine（结果、侧输出、判定各一个）从这些有界存储搬运到原有
  `Results()` / `Rejections()` / `Verdicts()` 通道。
  因此下游**只使用结果订阅面、完全不消费原有实时输出**时，处理循环也不会停住：
  窗口结果持续产生、订阅按位点继续交付，取快照/检查点/当前状态以及 `Close()`
  都能在有限时间内完成。停滞的旧通道消费者只会让对应的接力 goroutine 停在发送上
  （接力数量固定为 3，不随订阅数/结果数增长），积压全部由有界存储承载。
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

结果存储由两部分**相互独立**的状态组成，背压/容量语义据此区分：

| 状态 | 内容 | 容量上界 | 作用 |
|------|------|----------|------|
| 环形增量历史 `ring` | 最近 N 条结果（N=`ResultHistoryCapacity`） | **严格 N 条**，突发再多也只占定长内存 | 订阅者按位点**增量回放** |
| 每窗口物化视图 `view` | 每个窗口的**最新结果**（FINAL 或最新 UPDATE） | **不同窗口数**（每窗口恰一条），不随结果条数/更正次数增长 | `ResultSnapshot()` 快照恢复 |

- **只走订阅面即可独立工作**：处理循环只做 O(1) 的非阻塞追加，不依赖任何下游是否
  读取原有 `Results()`/`Rejections()`。因此只使用结果订阅、完全不消费原有实时输出时：
  - 窗口结果持续产生，订阅按位点严格继续交付；
  - 慢订阅者、完全不读的订阅者、突发结果量都**不会拖垮处理链路**；
  - 同时刻取快照、检查点、读当前水位线以及 `Close()` 都能在**有限时间内返回**；
  - 后台任务有界：除处理循环外，只有**固定 3 个**输出接力 goroutine（结果/侧输出/判定，
    与订阅数无关），订阅本身是纯拉取游标，不建立每订阅者 goroutine/队列。
- **溢出结论明确可区分、可恢复**：订阅者落后到 `after+1` 已滑出**环形增量历史**时，
  `SubscribeResults` / `Receive` 返回哨兵错误 `ErrResultPositionExpired`
  （区别于 `io.EOF`、`context.Canceled`、`ErrSubscriptionClosed`），**绝不静默漏/跳结果**。
  恢复方式唯一且确定：重新 `ResultSnapshot()` 取完整物化视图（视图保留了每窗口最新值，
  即使该窗口的结果早已被环形历史裁剪也仍在视图中），应用视图后从 `snap.Seq` 续读增量。
  物化视图与下游结果表同构（每窗口一条），其规模随不同窗口数有界；若嵌入方有更持久的
  物化存储或按保留期回收的需要，可在外部确认后调用 `ResultStore.PruneView(boundary)`
  主动做时间界回收（`Processor` 默认不回收，以保证快照始终可完整恢复）。
- 慢订阅者停滞时只有旧输出接力 goroutine 停在通道发送上；若旧 `Results()` 通道长期
  无人消费、积压超过环形容量，接力会跳过已被裁剪的旧结果继续搬运（旧通道退化为
  "尽力而为"），而**权威、不丢不漏的结果面是订阅（ResultStore）**。
- 侧输出（`Rejections()`，超宽限迟到事件）与逐条判定（`Verdicts()`）同样通过各自的
  有界暂存邮箱 + 单一接力投递，不阻塞处理循环。邮箱容量较大（默认 4096），足以吸收
  正常调度抖动，使"总体在消费"的下游收到全部条目；完全停滞的读者超出容量后最旧条目
  被覆盖（侧输出/判定都同时写入结构化日志，不静默丢失语义）。
- 即使订阅者**完全不读**，管线也能在有限时间内推进屏障并被 `Close()` 正常停止、
  完成资源回收（无每订阅者后台任务需要回收）；存储关闭后，订阅者排空保留范围内剩余
  结果，最终收到 `io.EOF`。
- 位点序列是**进程内**的：处理器从检查点 `Restore` 后，结果流视为新流（位点从 1
  重新编号，历史/视图/侧输出暂存清空），跨进程续读同样走"取快照 → 从快照位点续读"。
- **两个输出面可独立使用**：原有 `Results()` 通道由接力从同一结果历史按位点搬运，
  保持有界通道形态；订阅历史是权威来源，不再要求"必须消费原通道"。同时使用两个面时，
  它们看到的结果集合与顺序一致（旧通道在极端停滞下为尽力而为，权威结论以订阅面为准）。

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
| `ResultHistoryCapacity` | 条数 | 1024 | 订阅面**环形增量历史**保留条数（定长有界）；为 0 用默认值。落后超出该范围返回 `ErrResultPositionExpired`，改取快照恢复。注意它只约束"增量回放"，不约束按窗口保留的快照物化视图 |
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
| `processor.go` | 串行处理循环、判定、窗口触发、背压、屏障、恢复、结果订阅入口，以及把结果历史/侧输出/判定暂存搬运到原有通道的固定接力 goroutine |
| `subscription.go` | 有界环形增量历史 + 每窗口物化视图、稳定位点订阅、快照、溢出结论与订阅者侧结果应用器 |
| `mailbox.go` | 旁路输出（侧输出/判定）用的泛型有界暂存邮箱：入队非阻塞、接力无轮询唤醒 |
| `checkpoint.go` | 检查点结构、JSON 原子持久化与 ID 编解码 |
| `config.go` / `clock.go` | 配置校验与时钟抽象 |
| `cmd/demo/main.go` | 端到端演示 |

## 九、本地验证与测试场景

```bash
# 全量（含竞态检测）
go test -race -count=1 -v ./...

# 稳定性：重复运行确认无调度相关抖动（订阅/接力已全部改为确定性同步或显式等待）
go test -race -count=20 ./...

# 只跑"只用订阅面、不消费原有输出"相关用例
go test -race -count=5 -run 'SubscriptionOnly|NonReading|Overflow|Reconnect|Burst|SlowSubscriber|FiniteStop' ./...

# 端到端演示
go run ./cmd/demo
```

**只使用结果订阅面（不消费 `Results()`）的最小用法**：

```go
sub, _ := p.SubscribeResults(lastAck) // lastAck=0 即从头订阅
for {
    rec, err := sub.Receive(ctx)
    switch {
    case errors.Is(err, ErrResultPositionExpired):
        snap, _ := p.ResultSnapshot(ctx)        // 落后太多：取完整快照
        for _, v := range snap.Views { apply(v) }
        sub, _ = p.SubscribeResults(snap.Seq)   // 再从快照位点续读增量
        lastAck = snap.Seq
        continue
    case errors.Is(err, io.EOF):
        return                                  // 处理器已优雅关闭
    case err != nil:
        return
    }
    apply(rec.Result)
    lastAck = sub.Ack()                         // 可持久化的确认位点（幂等）
}
```

该用法下无需启动任何 `Results()`/`Rejections()`/`Verdicts()` 消费者：处理循环、
屏障与关闭都不依赖它们被排空。

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
- 只用订阅面、完全不消费原有输出（`subscription_only_test.go`）：
  - `TestSubscriptionOnlyDeliversBeyondDefaultCapacity`：边产生边消费，结果量超过
    默认容量后订阅仍按位点严格交付；
  - `TestNonReadingLegacyOutputDoesNotBlockSubscription` /
    `TestSubscriptionOnlyBarriersAndCloseAreFinite`：完全不读旧输出时，订阅持续交付、
    快照/检查点/关闭都在有限时间返回；
  - `TestBoundedResourcesWithNonReadingDownstream` /
    `TestNonReadingSubscriberBoundedTasks`：完全不读者与大量空闲订阅者下历史/队列/
    goroutine 严格有界，落后者收到可区分的溢出结论；
  - `TestOverflowRecoverySnapThenIncremental`：环形增量历史已裁剪的窗口仍能由快照视图
    恢复，快照 + 增量与持续消费逐窗口逐字段（身份/类型/聚合/位点）一致；
  - `TestReconnectStrictFromAckAndRedelivery`：掉线严格按最后 Ack 续读、更正链保序、
    重复确认/重复补发只生效一次；
  - `TestSubscriptionOnlyMultiPartitionBatchesStrictEqual` /
    `TestSubscriptionOnlyRepeatedRunsStable` / `TestBurstResultsDoNotBlockPipeline`：
    不同分区交错、不同批大小、突发结果量下结论严格一致且多次运行稳定。

所有测试通过 `slog` 打印水位线（含 `-inf/+inf`）、事件时间、窗口与判定原因，
失败断言会指出具体不一致的窗口、聚合字段或判定类别，便于区分根因。
