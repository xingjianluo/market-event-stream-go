# 本地行情事件流处理管线（Go）

一个**纯本地、确定性**的行情事件流处理管线：用内存中的多分区日志模拟多分区
事件源，按**事件时间**做滚动窗口聚合（VWAP/数量/笔数），内建水位线推进、
乱序/重复/迟到处理、端到端背压与检查点恢复。

目标是让「分区交错、到达顺序、批大小、慢消费者、故障重放」都**不改变同一批
事件的聚合结论**，并且每一条未走正常路径的事件都有**可区分、可解释**的结论。

---

## 1. 快速开始

```bash
# 跑演示（构造交错/乱序/重复/三类迟到/停滞分区）
go run .

# 带检查点落盘的演示（再次启动会从检查点目录恢复）
go run . --checkpoint-dir=./.cp-demo

# 全部单元测试（含 -race 并发检测）
go test -race ./...
go test -v ./pipeline/          # 查看逐条判定日志（水位线/事件时间/理由）
```

演示输出包含两类通道：

- `窗口结果`：每个事件时间窗口的**首帧**与迟到后的**修正帧**；
- `侧输出`：`DUPLICATE` / `LATE_IN_ALLOWED` / `LATE_DROPPED` 的逐条留痕与判定理由。

---

## 2. 架构与数据流

```
PartitionLog(A) ─┐
PartitionLog(B) ─┼─> 确定性源协调器（公平轮转合并，单 goroutine）
PartitionLog(C) ─┘        │  有界 inputCh（满即背压）
                          ▼
              单个 aggregator goroutine（全部状态唯一持有者，无锁）
                    │            │
                    ▼            ▼
            有界 resCh      有界 sideCh
```

- **源协调器**：按分区注册顺序公平轮转，每轮每分区最多投递 `MergeBatchSize`
  个条目；每条消息带全局单调序号 `seq`。
- **聚合器**：把一次 select 中已缓冲的消息按 `seq` 恢复成确定性全序后逐条
  处理。因此 goroutine 调度抖动、通道缓冲深度都**无法改变处理顺序**。
- **有界队列**：输入/结果/侧输出通道全部有容量上限，慢消费者沿
  `消费者 → 聚合器 → 源` 一路反压，内存与在途任务有界。

### 为什么结果可复现？

1. 时间只用事件自带的 `EventTime` 与日志里的**显式水位线标记**，
   不使用处理时钟、墙上时间，也不按"已见到的最大事件时间"隐式推进；
2. 聚合（求和、计数、VWAP）全部使用整数 / `big.Int`，与加入顺序无关；
3. 处理顺序由协调器 `seq` 全序唯一确定；窗口首帧按 `(品种, 起点)` 排序输出；
4. 事件用 `EventID = (分区, 分区内序号)` 幂等去重，重放/重发不重复计入。

---

## 3. 窗口语义

- **滚动窗口**：`WindowStart = floor(EventTime / WindowSize)`，UTC 对齐，
  区间为左闭右开 `[start, end)`。
- **聚合值**：`count`（笔数）、`totalQty`（数量）、`VWAP`（成交量加权均价，
  `Σ priceMicros*qty / Σ qty`，整数四舍五入，全程 `big.Int` 无浮点误差）。
- **输出模型**：
  - 全局水位线 `>= end` 时输出窗口**首帧**（`Revised=false`）；
  - 允许迟到范围内再到事件，窗口状态保留并输出**修正帧**（`Revised=true`），
    结果通道里同一窗口可能有多帧，**最后一帧为最终值**；
  - 全局水位线 `>= end + AllowedLateness` 时**清理**窗口及其去重条目。

---

## 4. 水位线语义

每个分区独立维护一条水位线，**全局水位线 = 所有活跃分区水位线标记的最小值**，
且只能**单调递增**。

- **显式标记驱动**：`AddWatermark(t)` 是源的硬承诺——"本分区不会再产生
  事件时间 `<= t` 的事件"。回退的标记被拒绝并记录日志。
- **未标记 = -∞**：一个活跃但从未声明进度的分区会阻塞全局水位线
  （它可能产生任意早的事件）。
- **停滞（idle）**：`AddIdle()` 使该分区**暂时退出**最小值计算——个别分区
  停滞**不会无限阻塞**其它分区已完成的窗口；分区用新的水位线标记恢复后
  **重新参与**最小值，因此也**不会越过该分区的真实进度而过早关窗**。
- **结束（EOF/End）**：该分区永久退出最小值。只有当源把**所有分区日志**都
  投递完（`sourceDone`）才用 `+∞` 收尾水位线冲刷所有剩余窗口；短分区提前
  结束不会提前关闭长分区尚未读到的窗口。

### 脉冲栅栏 `Barrier`（确定性对齐点）

`AddBarrier()` 在日志中插入一个显式栅栏：协调器会先把所有分区栅栏前的条目
排空，再等待聚合器完成该点之前的窗口触发/清理，然后才放行栅栏后的条目。
用途：把"某个水位线先生效、某条事件后判定"做成与批大小、调度无关的确定边界
（测试与故障注入都依赖它）。不使用栅栏时，边界由自然的批次排空决定。

---

## 5. 迟到与一致性策略

事件在聚合器中按当前全局水位线 `gw` 判定，结论**四选一、互斥且可区分**：

| 判定 | 条件 | 处理 | 出口 |
|---|---|---|---|
| `ON_TIME` | `gw < end` | 正常计入窗口 | 窗口结果 |
| `LATE_IN_ALLOWED` | `end <= gw < end+AllowedLateness` | 计入并输出**修正帧** | 窗口结果 + 侧输出 |
| `LATE_DROPPED` | `gw >= end+AllowedLateness`（窗口已清理） | **明确拒绝、绝不静默丢弃** | 侧输出（含理由） |
| `DUPLICATE` | `EventID` 已处理 | 忽略 | 侧输出（含理由） |

每条侧输出都带有：事件 ID、事件时间、**判定时刻的全局水位线**、人类可读的
**判定依据**（窗口区间、清理线、为什么在范围内/超范围）。

> 边界约定：`gw == end` 即算迟到（窗口已到关闭点），进入允许迟到分支；
> `gw == end+AllowedLateness` 即超范围丢弃。见 `TestLateBoundaryExact`。

### 去重与状态有界

- 去重表条目挂在窗口上，窗口被清理时其去重条目一并删除（内存随窗口数有界，
  不随历史无限累积）。
- 已清理窗口的迟到事件得到 `LATE_DROPPED`（而不是被当成新事件）。

---

## 6. 背压与故障恢复

**背压 / 有界性**

- 输入、结果、侧输出通道均有界；源协调器与聚合器各只有一个 goroutine，
  任务数不随流量增长（见 `TestBoundedBackpressureSlowConsumer`、
  `TestBoundedGoroutines`）。
- 慢消费者在输出端阻塞，反压一路传到源，不会用无限缓冲掩盖压力。
- 注意：使用方应**并发消费** `Results()` 与 `SideOutput()`，否则一个输出
  队列被排空后另一个满会自阻塞。

**检查点 / 恢复**

- 检查点是聚合器在其单线程内生成的一致性快照：分区偏移、各分区水位线状态、
  全局水位线、全部仍保留窗口的**事件明细**、统计。快照以
  `临时文件 + rename` 原子写入 `CheckpointDir/checkpoint.json`。
- 恢复时从各分区"已确认偏移 + 1"继续读；窗口聚合值由事件明细**重算**，
  已处理条目的重放由 `EventID` 去重表幂等拦截，因此
  **重启重放后的最终聚合与无故障运行完全一致**（见
  `TestCheckpointRestartReplay`）。
- 栅栏点在配置了 `CheckpointDir` 时会自动落盘；也可通过 `OnBarrier` 回调
  在该确定点注入故障。从"全部已提交、窗口已清理"的末尾检查点重启是幂等的，
  不会重复输出窗口（见 `TestRestartFromCompletedCheckpoint`）。

---

## 7. 可配置项（`pipeline.Config`）

| 配置 | 含义 | 默认 |
|---|---|---|
| `WindowSize` | 滚动事件时间窗口长度（>0） | `10s` |
| `AllowedLateness` | 允许迟到时长；超过后窗口清理、迟到丢弃（≥0） | `5s` |
| `InputQueueCap` | 聚合器输入有界队列容量（>0） | `128` |
| `OutputQueueCap` | 结果 / 侧输出有界队列容量（>0） | `128` |
| `MaxInFlight` | 输入在途高水位（保留项，>0） | `64` |
| `MergeBatchSize` | 源每轮每分区最多投递条目数（>0）；只影响脉冲切分，不影响最终结论 | `3` |
| `CheckpointInterval` | 周期检查点间隔（0 = 不做周期检查点） | `0` |
| `CheckpointDir` | 检查点 JSON 落盘目录；空则不落盘，仍可手动检查点 | `""` |
| `OnBarrier` | 栅栏点同步回调（确定性中途注入/故障模拟） | `nil` |

### 本地日志构造 API

```go
l := pipeline.NewPartitionLog("A")
l.AddEvent(seq, "BTC", eventTimeMillis, priceMicros, qty) // 成交事件
l.AddWatermark(atMillis)   // 分区水位线标记（承诺）
l.AddIdle(atMillis)        // 分区暂时停滞，退出全局最小值
l.AddBarrier()             // 显式脉冲栅栏（确定性对齐点）
l.AddEnd()                 // 分区结束
```

---

## 8. 本地验证方法

```bash
# 全量测试（18 个用例）
go test -v ./pipeline/

# 并发安全
go test -race ./...

# 演示三种迟到结论 + 重复 + 停滞
go run .

# 检查点目录验证：第一次运行落盘，删除目录外的状态后用同一目录再跑
go run . --checkpoint-dir=./.cp-demo
go run . --checkpoint-dir=./.cp-demo   # 幂等恢复/收尾
```

测试覆盖（用例名即场景）：

| 场景 | 用例 |
|---|---|
| 水位线 min / 单调 / 停滞 / 结束 | `TestWatermarkGlobalMinOfActivePartitions`、`TestWatermarkMonotonicAndIdle`、`TestWatermarkFinishedPartitionExitsMin` |
| 分区交错 + 乱序 + 重复 + 三类迟到 | `TestCanonicalScenario`、`TestOutOfOrderWithinPartition`、`TestDuplicateNotDoubleCounted` |
| 停滞分区不阻塞 / 不过早关窗 | `TestIdlePartitionDoesNotBlockWindow` |
| 批大小 / 分区顺序 / 交错形态不变性 | `TestBatchSizeInvariance`、`TestPartitionOrderInvariance`、`TestInterleavingInvariance` |
| 迟到边界精确判定 | `TestLateBoundaryExact` |
| 多品种多窗口 / 空输入 / 非法配置 | `TestMultipleSymbolsAndWindows`、`TestEmptyAndIdleOnly`、`TestInvalidConfigRejected` |
| 背压 / goroutine 有界 | `TestBoundedBackpressureSlowConsumer`、`TestBoundedGoroutines` |
| 检查点重启重放一致性 / 幂等收尾 | `TestCheckpointRestartReplay`、`TestRestartFromCompletedCheckpoint` |

单测日志逐条打印**水位线、事件时间与判定依据**（`-v` 可见），断言失败时可直接
区分是"重复计入 / 迟到误判 / 窗口提前关闭 / 重放不一致"中的哪一类。
