# brpc 请求级时延打点与端到端分解分析 — 设计文档

- 日期：2026-08-29
- 分支：`latency-trace`
- 基准：`master @ 4047c4e0`（BRPC_REVISION 1.17.0）

---

## 1. 目标

在 brpc 的客户端与服务端各插入一组细粒度时间戳，使得**单次 RPC 的端到端时延可以被无损分解**成 33 个分解项（31 个连续区间 + 2 个派生链路项）；两端数据各自落盘后离线合并，生成一个可交互的 HTML 累积柱状图 —— 横轴按端到端时延排序，纵轴为累积时延，从而直接读出长尾时延的来源。

### 1.1 成功判据

1. 对每条成功 join 的记录，`Σ(33 个分解项) == C17 − C01`。**注意这是望远镜求和的恒等式，对任意时间戳取值都成立** —— 它验证 `merge.py` 的算术无误，**不验证埋点是否正确**（见 §10.1）。埋点正确性由判据 2–5 保证。
2. **每个点位都非零**：`ts[i] == 0` 意味着该点位从未触发。这是唯一能发现「某条代码路径漏埋」的检查。
3. **单调不减**：`ts[i] <= ts[i+1]`，捕获点位落在错误的时间顺序上。
4. **每个分解项非负**（低并发 outstanding=1 下），捕获点位落在错误的位置。
5. **每个点位遵循它自己的写入策略**：绝大多数点位是瞬时事件，只应写入一次——重复写入意味着该事件触发了两次，是 bug，必须首次写入优先（后写丢弃）。但 `write_start`/`read_start` 这一对点位的含义是「真正完成本单元这次操作的那次尝试的起点」，同一单元排在队列里时会被前面单元的尝试重入多次，只有最后一次尝试才是真正搬动本单元字节的那次，因此这一对必须最后写入优先（见 §10.1）。
6. 打点开销经 microbenchmark 实测，并据此确定最终点位集。
7. HTML 支持 1 万~10 万条记录的渲染与逐请求 hover 下钻。

### 1.2 非目标

- 不替代、不修改 rpcz 的现有行为。两套机制独立共存。
- 不覆盖 baidu_std 以外的协议（HTTP/h2、nshead 系、redis 等）。
- 不做在线聚合、不做分布式采集服务。数据靠人工汇总。
- 不追求生产环境常开。这是压测/诊断工具，默认编译期关闭。

---

## 2. 已确定的关键决策

以下决策来自与用户的逐条讨论，是本设计的输入，不再重新论证。

| # | 决策 | 理由 |
|---|---|---|
| D1 | 支持跨机测试；链路时间用 `((t_cli_wake − t_cli_write_end) − (t_srv_write_end − t_srv_wake)) / 2` | 两个括号都是进程内差值，同时免疫时钟**偏移**与**频率漂移** |
| D2 | 链路拆分同时实现「逐请求对半分」与「每连接滑窗估 offset」两种模型，HTML 下拉切换 | 二者总链路时间恒等，仅上下行劈分不同；结果差异大的请求即归因污染重的请求，本身是信号 |
| D3 | 覆盖范围：baidu_std + TCP + RDMA | RDMA 写侧完全共用 `Socket::DoWrite`，增量仅「wake」一个点位 |
| D4 | 两端各自落本地文件，人工汇总后喂给分析工具 | 零网络依赖，压测期间不引入额外流量 |
| D5 | 相邻点位之间的**每一段**都是具名分解项（共 33 项），不设「其它」兜底桶 | 长尾若出在 LB 选连接或服务端限流排队上，必须能直接看到 |
| D6 | 数据量级 1 万~10 万条；Canvas 渲染，每条一根柱子 + 命中测试 + 框选缩放 | 保真度最高，无需聚合 |
| D7 | 采用「定长记录槽 + 8 字节 handle 透传」方案 | 见 §7 的方案对比 |
| D8 | `Socket::WriteRequest` 在 `BRPC_LATENCY_TRACE` 编译开关下由 64 字节扩到 128 | 该结构体当前正好占满一个 cacheline 且零填充（`socket.cpp:2954` 有 `BAIDU_CASSERT(sizeof(WriteRequest) == 64)`），无空位可用 |
| D9 | 重试/backup request 每次 attempt 独立一条记录，带 attempt 序号 | HTML 默认只画最终成功的那次，失败 attempt 可勾选显示 |
| D10 | 同步与异步 RPC 都支持 | 区别仅在「客户端回调处理」跑在哪个执行体，点位本身不变 |
| D11 | 服务端**不**为「`svc->CallMethod()` 返回」单设点位 | 该时刻与 `S10`（`SendRpcResponse` 入口）的先后**在同步/异步实现下相反**，插入线性序列会破坏单调性。见 §4.3 |
| D12 | `srv_dispatch`（S06→S07）暂不再细拆出 `ConcurrencyLimiter` | 用户决定；若后续怀疑限流排队是长尾主因，再单独加点位 |
| D13 | 单次打点开销由 Phase 0 在 suzhou950 上实测 | 用户处无现成数据 |
| D14 | **`rpc_end` 与 brpc 的 `OnRPCEnd()` 同址；删除 `rsp_process_start` / `rsp_process_end`** | 框架不把用户回调计入 RPC 时延，本工具对齐后可与 bvar 直接对表。见 §3.3 |

---

## 3. 现状与约束

### 3.1 rpcz 为什么不能直接复用

| 方面 | rpcz 现状 | 对本需求的障碍 |
|---|---|---|
| 点位数 | `Span` 只有 5 个时间戳：`received` / `start_parse` / `start_callback` / `start_send` / `sent` | 需要的 34 个点位绝大多数不存在，且事件级点位与 write 侧点位 Span 完全没有覆盖 |
| 存储 | 每进程两个 leveldb（`id_db` + `time_db`），每 span 一次 protobuf 序列化 + 两次写（`span.cpp:689`） | 单条开销在微秒级，正好污染要测量的长尾 |
| 采样 | `bvar::Collector` 自适应限速采样 | 无法保证覆盖尾部请求 |
| 数据模型 | 树形嵌套（`repeated RpczSpan client_spans`） | 需要的是扁平定长向量 |
| 副作用 | 服务端为拿 `sent_us` 会 `bthread_id_create` + `bthread_id_join(response_id)` 阻塞响应 bthread（`baidu_rpc_protocol.cpp:414` / `:481`） | 开启后改变服务端行为，本设计必须避开 |

结论：新建独立模块，与 rpcz 并存互不影响。

### 3.2 已核实的代码事实

| 事实 | 位置 | 影响 |
|---|---|---|
| `sizeof(Socket::WriteRequest) == 64`，`BAIDU_CACHELINE_ALIGNMENT`，零填充 | `socket.cpp:310`，断言在 `socket.cpp:2954` | 加字段必然涨到 128，故走编译期开关 |
| `IOBuf` 为 32 字节（`SmallView` = 2 × `BlockRef`(16)） | `iobuf.h:82-101` | 上一行的推导依据 |
| `ReturnSuccessfulWriteRequest` 在 `butil::return_object(p)` 之前读 `p->id_wait` 与控制位 | `socket.cpp:509-517` | 在同处读 handle 安全 |
| 该函数是「WriteRequest 写完」的唯一汇聚点，快路径与 `KeepWrite` 都收敛于此 | `socket.cpp:1772` / `1871` | `write_end` 的落点 |
| `PackedPtr<Socket>` 的 extra 仅用 2 bit，余 14 bit | `socket.cpp:317-334` | 不足以放 handle（需约 40 bit），故排除「偷位」方案 |
| 客户端 `IssueRPC` 设 `wopt.id_wait = cid` | `controller.cpp:1511` | `id_wait` 已被占用，不可复用 |
| `ProcessRpcResponse` 先解 meta（`:940`）再 `bthread_id_lock`（`:950`） | `baidu_rpc_protocol.cpp:936-950` | meta 反序列化点位拿不到 handle，需先落局部变量 |
| `RpcRequestMeta` 已用 tag 1-8，tag 9 空闲 | `baidu_rpc_meta.proto:42-49` | 新字段用 tag 9 |
| rpcz 的 `trace_id`/`span_id` 填充绑死在 `IsTraceable()` 上 | `channel.cpp:539`，`baidu_rpc_protocol.cpp:671` | 复用会牵动 rpcz 采样逻辑，故新增独立字段 |
| RDMA 写侧完全共用 `Socket::Write` → `DoWrite` → `_transport->CutFromIOBufList()` | `socket.cpp:1902`，`rdma_transport.cpp:100` | 写侧点位零增量 |
| RDMA **数据面**另建一个 CQ 专用 socket（fd = `comp_channel->fd`），其 `on_edge_triggered_events = PollCq` | `rdma_endpoint.cpp:1135`，`PollCq` 在 `:1469` | 收侧 OnEdge 回调是 `PollCq` 而**非** `InputMessenger::OnNewMessages` |
| `rdma_transport.cpp:52/54` 的 if/else 只管**握手**（走 RDMA socket 的 TCP fd），与数据面无关 | `rdma_transport.cpp:44-56` 及其注释 | 不可据此推断数据面路径 |
| 轮询模式（`-rdma_use_polling`）由独立 poller 线程死循环调 `PollCq`，不经 epoll、无 OnEdge bthread | `rdma_endpoint.cpp:1733` | `wake` / `onedge_start` 两个点位在该模式下**物理上不存在** |
| RDMA 与 TCP 两条收包路径在 `InputMessenger::ProcessNewMessage` 汇合 | `rdma_endpoint.cpp:1599` | `msg_recv_done` 可共用 |
| aarch64 上 `cpuwide_time_ns()` 读 `cntvct_el0`，架构保证全系统一致 | `butil/time.h:227` | 同机跨核可比；跨机需靠 D1 的公式 |

---

## 3.3 端到端的定义与 brpc 自己的口径对齐（D14）

**`rpc_end` 取在 brpc 调用 `Controller::OnRPCEnd()` 的同一时刻，用户回调不计入端到端时延。**

起因：异步路径的实际顺序是

```
C17 rsp_process_start → 框架 OnRPCEnd() → _done->Run() → C18 rsp_process_end → C19 rpc_end
```

框架的 `OnRPCEnd()` 落在原设计「回调」区间的**内部**，于是 `C19 − C01` 比 brpc 自己的 `_end_time_us` 多出整个回调时长。同步路径无此问题（`channel.cpp` 里 `OnRPCEnd` 与 `rpc_end` 紧邻）。

**裁决（用户拍板）**：删除 `rsp_process_start` 与 `rsp_process_end` 两个点位，`rpc_end` 移到 `OnRPCEnd()` 处。

理由：回调是 brpc 判定 RPC 已经结束**之后**运行的用户代码，本就不属于 RPC 时延；框架不计入，本工具也不该计入。对齐之后，本工具的端到端可以与 brpc 的 bvar `LatencyRecorder` 直接对表 —— 两个数不一致时就是真有问题，而不是口径差异。

**代价，明确记下**：

- 回调本身的耗时**不再可见**。若回调是长尾来源，本工具看不到它；那是用户自己代码里的事，需要自行测量。
- 同步路径下，原 `cli_rpc_finish`（含唤醒发起方 bthread 的时间）不再单独成项，并入 `payload 反序列化结束 → RPC 结束` 这一段。

**连带的格式变更**（现在无数据产生，是免费改的最后窗口）：

| | 改前 | 改后 |
|---|---|---|
| 点位总数 | 36（客户端 19 + 服务端 17） | **34**（客户端 **17** + 服务端 17） |
| `sizeof(LatencyTraceRecord)` | 200 | **192**（24 + 34×4 + 28 = 188，8 字节对齐） |
| 分解项 | 35（33 区间 + 2 链路） | **33**（31 区间 + 2 链路） |

---

## 4. 点位清单（34 个）

点位 ID 在代码中为编译期常量，落盘后由分析工具按 ID 解释。`0` 表示未采集。

### 4.1 客户端（17 个）

D14：`rsp_process_start` / `rsp_process_end`（原 C17/C18，包住用户回调）已删除；`rpc_end` 落在 `Controller::OnRPCEnd()` 被调用的同一时刻，不再等 `done->Run()` / 同步调用方被唤醒之后。

| ID | 名称 | 落点 |
|---|---|---|
| C01 | `rpc_start` | `Channel::CallMethod` 入口 |
| C02 | `req_payload_ser_start` | `_serialize_request()` 调用前（`channel.cpp:592`） |
| C03 | `req_payload_ser_end` | `_serialize_request()` 返回后 |
| C04 | `req_meta_ser_start` | `PackRpcRequest` 中 `RpcMeta meta;` 构造前（`baidu_rpc_protocol.cpp:1096`） |
| C05 | `req_meta_ser_end` | `SerializeRpcHeaderAndMeta()` 返回后 |
| C06 | `write_enqueue` | `Socket::Write()` 入口（`socket.cpp:1617`） |
| C07 | `write_start` | `Socket::DoWrite()` 中 `CutFromIOBufList()` 调用前（`socket.cpp:1902`） |
| C08 | `write_end` | `Socket::DoWrite()` 中 `CutFromIOBufList()` **返回后**立即遍历本批次，对 `data` 已清空的 WriteRequest 就地打戳（见 §8.4） |
| C09 | `wake` | `epoll_wait` 返回（RDMA：CQE 被 poll 出） |
| C10 | `onedge_start` | `Transport::OnEdge` 入口（`transport.h:31`） |
| C11 | `read_start` | `InputMessenger::OnNewMessages` 循环内 `m->DoRead()` 前 |
| C12 | `msg_recv_done` | 该消息在 `ProcessNewMessage` 中切分成功 |
| C13 | `rsp_meta_deser_start` | `ProcessRpcResponse` 入口（`baidu_rpc_protocol.cpp:937`） |
| C14 | `rsp_meta_deser_end` | `ParsePbFromIOBuf(&meta, ...)` 返回后（`:940`） |
| C15 | `rsp_payload_deser_start` | response 的 `ParseFromIOBuf()` 前 |
| C16 | `rsp_payload_deser_end` | 之后 |
| C17 | `rpc_end` | `Controller::OnRPCEnd`（同步：`channel.cpp:686`；异步：`controller.cpp` 的 `EndRPC`/`DoneInBackupThread`，且必须落在 `_done->Run()` **之前**，见 §3.3） |

### 4.2 服务端（17 个）

| ID | 名称 | 落点 |
|---|---|---|
| S01 | `wake` | 同 C09 |
| S02 | `onedge_start` | 同 C10 |
| S03 | `read_start` | 同 C11 |
| S04 | `msg_recv_done` | 同 C12（**用户原清单未列，本设计补充**；无此点则「服务端收包时间」与「服务端处理排队时间」无分界） |
| S05 | `req_meta_deser_start` | `ProcessRpcRequest` 入口 |
| S06 | `req_meta_deser_end` | `ParsePbFromIOBuf(&meta, ...)` 返回后 |
| S07 | `req_payload_deser_start` | request 的 `ParseFromIOBuf()` 前 |
| S08 | `req_payload_deser_end` | 之后 |
| S09 | `service_start` | `svc->CallMethod()` 调用前 |
| S10 | `service_end` | `SendRpcResponse` 入口（`baidu_rpc_protocol.cpp:282`） |
| S11 | `rsp_payload_ser_start` | `SerializeResponse()` 前（`:331`） |
| S12 | `rsp_payload_ser_end` | 之后 |
| S13 | `rsp_meta_ser_start` | `RpcMeta meta;` 构造前（`:346`） |
| S14 | `rsp_meta_ser_end` | `SerializeRpcHeaderAndMeta()` 返回后（`:401`） |
| S15 | `write_enqueue` | `Socket::Write()` 入口 |
| S16 | `write_start` | `Socket::DoWrite()` 中 `CutFromIOBufList()` 前 |
| S17 | `write_end` | 同 C08 |


### 4.3 为什么不为「`svc->CallMethod()` 返回」设点位

服务端的 `done` 是一个包裹 `SendRpcResponse` 的 closure（`baidu_rpc_protocol.cpp:853-857`），经 `svc->CallMethod(..., done)` 传给用户实现（`:867` / `:872`）。**它何时执行完全由用户的 service 实现决定**，brpc 不介入：

- **同步实现**：`brpc::ClosureGuard done_guard(done);` 置于函数开头，作用域结束时析构并 `_done->Run()`（`closure_guard.h:37-41`）。于是 `SendRpcResponse` 在 `CallMethod` **返回之前**已经跑完。
- **异步实现**：调用 `done_guard.release()` 交出所有权后立即返回，`done->Run()` 由后续某个执行体触发。

因此两个候选点位的先后顺序在两种实现下**相反**：

| 点位 | 同步实现 | 异步实现 |
|---|---|---|
| `S09` = `svc->CallMethod()` 调用前 | 1 | 1 |
| `S10` = `SendRpcResponse` 入口 | 2 | 3 |
| （候选）`svc->CallMethod()` 返回 | 3 | 2 |

增设该候选点位仅在异步实现下有价值（可把业务处理拆为「占用框架执行体的同步部分」与「执行体已释放的异步等待」两段），但它会破坏 34 点位序列的单调性，而「相邻点位之差 = 一个分解项」与 §10 的单调性断言均以单调为前提。故不设。

`S10` 取 `SendRpcResponse` 入口在两种实现下语义一致：即「业务宣告处理完成」的时刻。若日后需要分析异步 service 的等待分解，应另设可选点位，不并入主序列。

---

## 5. 分解项（33 项）

客户端 17 点产生 16 段，其中 `C08 → C09` 一段按 D1 展开为「上行链路 + 服务端 16 段 + 下行链路」。故：

```
15 (客户端其余段) + 16 (服务端段) + 2 (链路) = 33
```

**恒等式**：由 D1 的定义，`上行 + 下行 ≡ (C09 − C08) − (S17 − S01)`，因此

```
Σ(33 项) = (C17 − C01)
```

是代数恒等 —— **而且恒等到了对任意取值都成立的地步**：把上式展开，客户端区间望远镜求和得 `(C17−C01) − (C09−C08)`，服务端得 `S17−S01`，链路两项之和按定义等于 `(C09−C08) − (S17−S01)`，三者相加时后两组彼此抵消。

**因此这条断言不能证明任何关于埋点正确性的事。** 全部时间戳为 0 时它同样成立。它的真实作用是校验 `merge.py` 的分项拆解与求和逻辑没有写错 —— 这有价值，但与「时间戳是否落在正确位置」无关。真正的埋点检查见 §10.1。

标记 ★ 的对应用户原始清单（含 2 个派生链路项），标记 ○ 的为本设计补充的间隙项（灰阶）。D14 之后 `cli_post_deser` 吸收了原来被删掉的两个点位之间的区间，改记为 ○（见 §5.3 末尾说明）。

### 5.1 客户端发送段（7 项）

| 分解项 | 区间 | 说明 |
|---|---|---|
| ○ `cli_pre_serialize` | C01→C02 | `bthread_id_lock_and_reset_range`、选项合并、Span 创建 |
| ★ `cli_req_payload_ser` | C02→C03 | 客户端 payload 序列化 |
| ○ `cli_issue_rpc` | C03→C04 | **负载均衡选 socket、建连检查、装超时定时器**。冷连接或 LB 抖动时可能很大 |
| ★ `cli_req_meta_ser` | C04→C05 | 客户端 metadata 序列化 |
| ○ `cli_pack_to_write` | C05→C06 | `PackRpcRequest` 收尾、IOBuf 拼装 |
| ★ `cli_write_queue` | C06→C07 | 客户端 write 排队（含 bthread 调度与写竞争） |
| ★ `cli_write` | C07→C08 | 客户端 write 接口 |

### 5.2 链路与服务端（18 项）

| 分解项 | 区间 | 说明 |
|---|---|---|
| ★ `link_up` | 派生 | 上行链路时间 |
| ★ `srv_wake_to_onedge` | S01→S02 | 服务端 wake → 收包 bthread 启动 |
| ★ `srv_onedge_to_read` | S02→S03 | 服务端收包 bthread 内排队 |
| ★ `srv_read` | S03→S04 | 服务端收包 |
| ★ `srv_recv_to_deser` | S04→S05 | 服务端处理排队 |
| ★ `srv_req_meta_deser` | S05→S06 | 服务端 metadata 反序列化 |
| ○ `srv_dispatch` | S06→S07 | **查 service/method、并发限制（`ConcurrencyLimiter`）、建 Controller**。限流排队即卡在此段 |
| ★ `srv_req_payload_deser` | S07→S08 | 服务端 payload 反序列化 |
| ○ `srv_to_service` | S08→S09 | 进入业务前的一次可能的 bthread 调度 |
| ★ `srv_service` | S09→S10 | 服务端服务处理 |
| ○ `srv_service_to_ser` | S10→S11 | done 进入、Controller 状态收尾 |
| ★ `srv_rsp_payload_ser` | S11→S12 | 服务端 payload 序列化 |
| ○ `srv_compress_checksum` | S12→S13 | 压缩、checksum 计算 |
| ★ `srv_rsp_meta_ser` | S13→S14 | 服务端 metadata 序列化 |
| ○ `srv_pack_to_write` | S14→S15 | 组包到入队 |
| ★ `srv_write_queue` | S15→S16 | 服务端 write 排队 |
| ★ `srv_write` | S16→S17 | 服务端 write 接口 |
| ★ `link_down` | 派生 | 下行链路时间 |

### 5.3 客户端接收段（8 项）

| 分解项 | 区间 | 说明 |
|---|---|---|
| ★ `cli_wake_to_onedge` | C09→C10 | 客户端 wake → 收包 bthread 启动 |
| ★ `cli_onedge_to_read` | C10→C11 | 客户端收包 bthread 内排队 |
| ★ `cli_read` | C11→C12 | 客户端收包 |
| ★ `cli_recv_to_deser` | C12→C13 | 客户端处理排队 |
| ★ `cli_rsp_meta_deser` | C13→C14 | 客户端 metadata 反序列化 |
| ○ `cli_lookup_cntl` | C14→C15 | 按 correlation_id 取回 Controller（`bthread_id_lock`） |
| ★ `cli_rsp_payload_deser` | C15→C16 | 客户端 payload 反序列化 |
| ○ `cli_post_deser` | C16→C17 | 错误码判定、重试决策、`SubmitSpan`、`OnRPCEnd`；同步 RPC 下含唤醒发起方 bthread 的时间。D14：原来的 `cli_post_deser`/`cli_callback`/`cli_rpc_finish` 三项因删除 `rsp_process_start`/`rsp_process_end` 两个点位而合并为这一项；用户回调本身的耗时不再计入（不可见），见 §3.3 |

合计：★ 23 项（含 2 个派生链路项），○ 10 项，共 31 个区间项 + 2 个派生项 = 33。

**关于 `*_read` / `*_write` 的命名**：这四项框住的是**传输层真正搬字节**的那一段，
名字只承诺这个，不承诺机制。写侧括的是 `_conn->CutMessageIntoFileDescriptor` /
`_transport->CutFromIOBufList`，读侧括的是 `InputMessenger::OnNewMessages` 里的 `DoRead` ——
只有在裸 TCP 上它们才分别是 `writev` 和 `readv`；RDMA 下同样的区间是 `ibv_post_send`
和 RdmaEndpoint 的收包路径，**两者都不是系统调用**，SSL 下又是第三条路径。

这四项曾叫 `*_readv` 与 `*_write_syscall`：前者继承自 C++ 点位名 `LT_C_READV_START`，
后者是分析层自己贴的类别标签。两个名字各自承诺了区间并不具备的机制，而且连承诺的方式
都不一致（一个说机制、一个说类别）。C++ 点位已同步改名为 `LT_*_READ_START`。

注意 ★ 不是 24：用户原始清单的「服务端收包排队时间」与「客户端收包排队时间」各自跨越了两个点位区间（`wake → onedge_start` 与 `onedge_start → read_start`），本设计按 D5 各拆为 2 项（22 + 2 = 24 项，见 D14 之前的版本）；D14 又把 `cli_callback` 这一项（原 ★）连同两个 ○ 项一起合并成一个 ○ 项，净减 1 项 ★、1 项 ○，故 24 → 23、11 → 10。

---

## 6. 链路时间的两种模型

对每条 join 成功的记录，令：

```
a = C08 (client write_end)      d = C09 (client wake)      —— 客户端时钟
b = S01 (server wake)           c = S17 (server write_end) —— 服务端时钟

RTT = d − a          （纯客户端时钟）
S   = c − b          （纯服务端时钟）
L   = RTT − S        （总链路时间，与时钟偏移无关，恒精确）
```

### 6.1 模型 A：逐请求对半分（默认）

```
link_up = link_down = L / 2
```

免疫时钟偏移与频率漂移。缺点：强制上下行对称，把归因噪声均摊掉。`L < 0` 的记录单独标记，HTML 中可筛选，**不静默裁剪为 0**（渲染方式见 §9.3）。

### 6.2 模型 B：每连接滑窗估 offset

设 `O = 客户端时钟 − 服务端时钟`，则 `link_up = (b + O) − a`，`link_down = d − (c + O)`，二者之和恒为 `L`，`O` 仅决定劈分。

估计方法：以 `(client_process, server_process)` 为分组键，在**滚动分箱**（tumbling，固定不重叠，默认 1 秒一箱）内取 `L` 最小的样本，用其 `O_i = a + L_i/2 − b` 作为整窗的 `O`。取最小值的依据同 NTP：链路时间最小的样本排队污染最轻，对称假设最可能成立。窗口化是为了跟踪两机时钟的**频率漂移**（未同步时典型 10~100 ppm，即每分钟 0.6~6 ms）。

> 实现说明：本节早先写作「滑动窗口」，而实现采用的是不重叠的**滚动分箱**。分箱实现简单，对本项目的采集时长足够；代价是分箱边界处 `O` 会**跳变**而非平滑过渡（当前数据下表现为 18 条记录柱顶差 1 ns，源于两侧独立取整）。若将来要长时间采集并关心时钟漂移的连续性，应改为真正的滑动窗口。此处以代码为准。

副产物：`link_up < 0` 精确指示「该请求的 `S01` 时间戳不可信」—— 服务端记录的唤醒时刻早于客户端写完时刻，因果上不可能，只能是该消息继承了更早批次的 wake 值（见 §8.2）。这是一个**探测器**而非噪声。

### 6.3 两模型的关系

总链路时间、乃至累积柱状图的**总柱高**，两模型完全一致。差异仅在上下行劈分，以及归因污染是被暴露还是被均摊。HTML 提供下拉切换与「两模型差值」列，供交叉验证。

---

## 7. 方案选型（已决）

| 方案 | 描述 | 结论 |
|---|---|---|
| **定长记录槽 + handle 透传** | 每次 RPC 分配一个定长槽，跨执行体只传 8 字节 handle | **采纳** |
| 事件流 | 每点位追加 `(trace_id, point_id, ts)` 三元组，离线 group by | 否决。`wake` 发生时消息尚未解析，**根本不知道 trace_id**，事件级点位仍需在 Socket 上暂存并二次关联 —— 简洁性优势恰在最难处失效；数据量还大 4 倍 |
| 扩展 rpcz `Span` | 加 34 个字段复用现有传递 | 否决。Span 只覆盖 process 阶段，事件级与 write 侧点位同样没有；且继承 §3.1 的全部开销 |

### 7.1 handle 载体的备选（已决 D8）

| 备选 | 结论 |
|---|---|
| `#ifdef` 下把 `WriteRequest` 扩到 128 字节 | **采纳**。默认构建一个字节不变，可上游合入 |
| per-socket 写序号 + 离线 join，零内存增长 | 否决。依赖「返还顺序 == 提交顺序」不变量；失败路径（`ReleaseAllFailedWriteRequests` / `ReturnFailedWriteRequest`）漏计一个，该连接后续所有请求的 `write_end` **永久静默错位** |
| 复用 `id_wait` 空槽 | 否决。破坏失败路径的 `bthread_id_error2`；客户端该字段已被 correlation_id 占用 |
| 复用 `_pc_and_udmsg` 指针位 | 否决。`reset_pipelined_count_and_user_message()` 在失败路径仍会读它 |
| 偷 `PackedPtr` 的 extra 位 | 否决。仅余 14 bit，handle 需约 40 bit |

---

## 8. 运行时模块

### 8.1 `src/brpc/latency_trace.{h,cpp}`

**记录结构**（POD，8 字节对齐后 192 字节）。下表按字段在结构体中的**实际声明顺序**列出（即 `merge.py` 用 `struct.unpack` 解包时必须遵循的顺序），偏移列以字节为单位、从记录起始处算起——字段顺序不是随意的排版，是 on-disk 字节布局本身：

| 字段 | 类型 | 偏移 | 说明 |
|---|---|---|---|
| `trace_id` | `uint64` | 0 | 跨进程唯一 |
| `base_counter` | `uint64` | 8 | 本进程原始计数器基准（aarch64: `cntvct_el0`；x86: TSC） |
| `slot_seq` | `uint64` | 16 | generation 校验用。**置于 `ts[]` 之前**，与实现一致 |
| `ts[34]` | `uint32 × 34` | 24 | **存「相对 `base_counter` 的偏移 + 1」**，见下方编码约定。`0` = 未采集；`0xFFFFFFFF` = 该模式下不存在（见 §8.5）；`0xFFFFFFFE` = 偏移饱和（区间 ≥ 约 42.9 秒 @100MHz） |
| `socket_id` | `uint32` | 160 | |
| `remote_ip` | `uint32` | 164 | |
| `req_size` | `uint32` | 168 | |
| `rsp_size` | `uint32` | 172 | |
| `method_id` | `uint32` | 176 | method 名走离线查表，避免变长字段；id 0 保留（未识别 method），见「方法名表」一节 |
| `error_code` | `int32` | 180 | |
| `remote_port` | `uint16` | 184 | |
| `role` | `uint8` | 186 | `LatencyTraceRole`：client=0 / server=1 |
| `attempt` | `uint8` | 187 | 重试 / backup-request 序号 |
| （尾部填充） | — | 188 | 4 字节对齐填充，凑满 192；无意义字节，不代表数据 |

`remote_ip`/`remote_port`/`role`/`attempt` 在结构体里并不相邻——它们被 `req_size`/`rsp_size`/`method_id`/`error_code` 隔开（见上表偏移列）。一个只看字段名分组、不看实际声明顺序写的 `struct.unpack` 格式串会在这四个字段上错位。

存**相对偏移**而非绝对值，把 `34 × 8` 压到 `34 × 4`。`uint32` 在 3 GHz TSC 下可表示 1.43 秒，足够覆盖单次 RPC。

存**原始计数值**，换算移至离线。计数器读取直接复用 `butil::detail::clock_cycles()`（`butil/time.h:217-266`），它已封装各架构实现，aarch64 即 `mrs cntvct_el0`。

不使用 `butil::cpuwide_time_ns()` 的理由：两个构建系统的 `WITH_CPU_FREQUENCY` **默认均为 0**（`config_brpc.sh:70`，`CMakeLists.txt:81`），此时 `cpuwide_time_ns()` 退化为 `clock_gettime(CLOCK_MONOTONIC)` 的 vDSO 调用（`butil/time.h:279-284`），比一条 `mrs` 指令贵得多。Phase 0 将实测二者差值。

**缓冲**：分片环形数组，分片数取「worker 数向上取 2 的幂」。分配槽为 `cursor.fetch_add(1)`；`handle = ((shard + 1) << 56) | seq`（**注意是 `shard + 1`，不是 `shard`**——`shard` 从 0 开始，若不加 1，`shard == 0` 且 `seq == 0` 的第一个槽会编码出 `handle == 0`，与 `LT_INVALID_HANDLE` 撞车；`Get()` 解码时相应地做 `shard = (h >> 56) - 1`。`latency_trace.h` 里 `LatencyTraceHandle` 声明处的注释仍写的是旧公式 `(shard << 56) | seq`，与 `.cpp` 的 `AllocSlot()`/`Get()` 实现不一致——这是那条注释本身的问题，不是这份设计文档说的机制；`merge.py` 不需要关心这个字段，句柄从不落盘，只存在于进程内），槽下标 `seq & mask`。每次写入前校验槽内 `slot_seq` 是否仍等于 handle 携带的 seq，不等则丢弃该次写入 —— 防止迟到的回填（例如卡住很久的 `KeepWrite`）写脏已被复用的槽。

**`base_counter` 必须是该记录时间轴的起点，而不是「槽被分配的那一刻」。** 二者在客户端碰巧一致，在服务端必然不一致：

- **客户端**：`AllocSlot` 发生在 `Channel::CallMethod` 入口，C01 紧随其后，所以「分配时刻」就是时间轴起点。
- **服务端**：`AllocSlot` 只能发生在 `ProcessRpcRequest` 解析出 `latency_trace_id` **之后**，而 S01（epoll 唤醒）、S02、S03、S04 全部发生在那**之前**。若以分配时刻为基准，这四个点位的偏移全为负。

因此 `AllocSlot` 接受基准计数值作为参数：客户端传当前时刻，服务端传它已经寄存在 `Socket` 上的 wake 时间戳（§8.2 的透传链路）。这样每条记录的 `base_counter` 都不晚于它自己的第一个点位。

**`base_counter` 永远是真实计数值，绝不能是哨兵。** 这是 §8.1 与 §8.5 之间一处必须显式对账的地方，两条决策分别成立、合起来却会互相摧毁：

- §8.1 要求服务端把 **wake 时间戳**作为 `base_counter` 传给 `AllocSlot`（因为 S01–S04 都早于建槽时刻）。
- §8.5 要求轮询模式下 wake **写哨兵**（因为那个事件根本不存在）。

两者相乘的结果是 `base_counter = 0xFFFFFFFF...`，而 `AllocSlot` 原样存下它、不做哨兵检查。于是：

| 点位类别 | 后果 |
|---|---|
| 经 `StampAt` 写入（S01–S06） | 撞上 `raw < base` 的 clamp，全部塌成 `ts = 1`。错，但不刺眼 |
| 经 `Stamp()` 写入（S09 起） | **没有 clamp**。`clock_cycles() − ~0ULL` 回绕成 `clock_cycles()+1`；而计数器是**自开机**计数，主机开机超过约 43 秒即饱和到 `0xFFFFFFFE`。**每一条轮询模式的服务端记录都显示 S09–S17 耗时约 43 秒**，确定性发生 |

**因此：`AllocSlot` 的三参重载必须拒绝哨兵基准。** 轮询模式下服务端改传 `read_start`——§8.5 自己的表格已经写明该点位在两种模式下都是真实值。哨兵只属于 `ts[]` 的某一格，永远不属于 `base_counter`。

**一条未被断言保护的隐式耦合**：轮询模式下 `base_counter` 的回退值 `read_start`、以及事件模式下从 CQ socket 拷来的 `_lt_wake`，都依赖 `InputMessenger::ProcessNewMessage` 里那段把 `Socket` 上的三个字段拷进 `InputMessageBase` 的逻辑 —— TCP 与 RDMA 两条路径调用的是同一份代码。今天成立，但**没有任何断言在守它**，而 RDMA 测试只在有 Soft-RoCE 的机器上跑、不在默认套件里。将来重构那段拷贝逻辑，可能悄无声息地打断 RDMA 侧，而默认 CI 全绿。改动 `ProcessNewMessage` 的人需要知道这条依赖。

**RDMA 事件模式的 `wake` 必须取 CQ socket 上已有的值，不能重新采样。** `Socket::OnInputEvent` 已经在真正的「epoll 返回、尚未分发」时刻把 `_lt_wake` 设好了（与 TCP 走同一套 TLS 机制）。若在 `GetAndAckEvents()` 返回**之后**重新读一次 `clock_cycles()`，得到的时刻晚于 `onedge_start`，与 TCP 的结构顺序相反 —— 而 clamp 会把这个倒挂悄悄抹平成两个 `ts = 1`，于是 `srv_wake_to_onedge` 与 `cli_wake_to_onedge` 这两个主分解项恒为零，且断言 `wake <= onedge_start` 空洞通过。正确做法是把 CQ socket 的 `_lt_wake` **拷贝**过去。

**`ts[]` 的编码约定（三个保留值 + 偏移 + 1）**：

| 值 | 含义 |
|---|---|
| `0` | 未采集 —— 该点位从未被写入 |
| `0xFFFFFFFF` | 该点位在此模式下不存在（RDMA 轮询模式，见 §8.5） |
| `0xFFFFFFFE` | 偏移饱和 —— 真实区间 ≥ 约 42.9 秒（`uint32` 在 100 MHz 下的上限）。分析工具应把它当作下界，不是精确值 |
| 其余 | 真实偏移为 `值 − 1` |

**编码必须饱和到 `0xFFFFFFFE` 而不是 `0xFFFFFFFF`**，否则一个超长区间会与「该模式下不存在」这个哨兵撞车 —— 两者的分析含义完全相反（一个是「这段特别慢」，一个是「这段不存在」），撞在一起会让 HTML 把一段 42.9 秒的真实停顿画成 N/A。

**为什么要「偏移 + 1」：`0` 唯一地表示「未打点」。** 若直接存偏移，`0` 会同时表示两件事：未打点，以及「恰好落在 `base_counter` 上」。后者不是理论可能性 —— 客户端的 `AllocSlot` 与 C01 是相邻语句，在 100 MHz（10 ns/tick）的计数器下二者极可能落在同一个 tick 内，`ts[C01]` 因而天然为 0。两义合一会让 §10.1 的「点位非零」判据对一次完全正常的采集误报。`merge.py` 读取时减 1 还原。

**`slot_seq` 的写入顺序是这套机制的全部要害，必须是**：

1. 先把 `slot_seq` 置为 `UINT64_MAX` 哨兵（「正在改写，任何 handle 都不匹配」）
2. 再 memset `ts[]`、重置元数据字段、写 `trace_id` / `base_counter` / `role`
3. **最后**以 release 语义写入真实的 `seq`

`Get()` 以 acquire 语义读 `slot_seq`。

**若把第 3 步的写入放在最前或中间，守卫就是失效的**：占坑与写入 `seq` 之间的窗口里，`slot_seq` 仍是**前一个占用者的真实 seq**，恰好那一代的迟到 handle 会「匹配成功」并写进一条正在初始化的记录 —— 正是本机制要防的那件事。

release/acquire 配对同时解决发布问题：`cursor.fetch_add` 发生在写入负载**之前**，因此 cursor 无论用什么内存序都不能充当发布点，跨线程读取（§8.1 的 `Dump`）必须依赖 `slot_seq` 这一对。

**「满了停止」不是可调策略**：关闭它会同时破坏跨端 join 的窗口对齐（见下）并重新打开下述竞态，因此其开关只能是测试专用的（`set_stop_when_full_for_test`）。

**守卫承诺的边界 —— 它保证「拒绝」，不保证「并发回收安全」。** 二者常被混为一谈：

- **保证**：一个槽已经改朝换代的 handle，`Get()` 返回 `nullptr`，而不是交出一个指向别人记录的指针。
- **不保证**：在槽被并发回收的前提下，`Stamp()` 整体是安全的。

原因是 `Stamp()` 本质是 check-then-write：

```cpp
r = Get(h);                  // 校验
if (r) r->ts[point] = ...;   // 写入 —— 这两步之间槽可能被回收
```

**这个窗口不是内存序问题，换任何 `memory_order` 都关不上。** 要真正做到并发回收安全，只有两条路：给槽加引用计数，或者根本不回收。本设计选了后者 —— 这正是「满了停止、绝不覆盖」不可调的第二个理由（第一个是跨端 join 的窗口对齐）。

**实测佐证（Task 3）**：仅用 release *store* 而不加显式 `atomic_thread_fence(release)` 是不够的 —— release store 只阻止先前的写越到它之后，不阻止后续的写越到它之前。实测每轮捕获 23,000~39,000 次不一致；补上栅栏后该通道关闭。这条不要在后续重构中被「简化」掉。

**读者侧的对称陷阱**：seqlock 式的双重校验中，`acquire` *load* 不足以约束「读负载」不被重排到「第二次读序号」之后 —— 方向正好相反，必须在两者之间插 `atomic_thread_fence(acquire)`。Task 3 的并发测试最初正是漏了这一道，症状是观测值恰好领先一个容量周期。

**缓冲满的策略：停止记录，不覆盖。** 覆盖式会让两端各自保留「最近 N 条」，两端窗口对不上；停止记录则两端都是「最早 N 条」，天然对齐。丢弃计数写入文件头。

**不起后台线程**：10 万条 × 192 B ≈ 18.3 MB，全程只写内存，`atexit` 或显式调用 `LatencyTrace::Dump()` 时一次性落盘。这消除了后台 flush 线程对被测系统的干扰。

**打点接口**：

```cpp
LT_STAMP(handle, POINT_ID)   // BRPC_LATENCY_TRACE 未定义时展开为空语句
```

**gflags**：

| flag | 默认 | 说明 |
|---|---|---|
| `-latency_trace_enabled` | `false` | 运行时总开关 |
| `-latency_trace_capacity` | `100000` | 记录条数上限 |
| `-latency_trace_dump_path` | 空 | 落盘路径；空则不落盘 |

**文件头结构 `LatencyTraceFileHeader`**（POD，128 字节固定，`merge.py` 按此顺序 `struct.unpack`；偏移以字节为单位，从文件起始处算起）：

| 字段 | 类型 | 偏移 | 说明 |
|---|---|---|---|
| `magic` | `uint64` | 0 | 见下方「magic 常量」 |
| `record_size` | `uint32` | 8 | `sizeof(LatencyTraceRecord)`，当前为 192 |
| `point_count` | `uint32` | 12 | `LT_POINT_COUNT`，当前为 34 |
| `head_counter` | `uint64` | 16 | 进程启动时采样的计数器值 |
| `head_monotonic_ns` | `int64` | 24 | 进程启动时的 `CLOCK_MONOTONIC`（见下方校准表） |
| `tail_counter` | `uint64` | 32 | dump 时采样的计数器值 |
| `tail_monotonic_ns` | `int64` | 40 | dump 时的 `CLOCK_MONOTONIC` |
| `head_realtime_ns` | `int64` | 48 | 进程启动时的 `CLOCK_REALTIME`，仅供跨机 sanity check |
| `tail_realtime_ns` | `int64` | 56 | dump 时的 `CLOCK_REALTIME`，仅供跨机 sanity check |
| `counter_freq_hz` | `double` | 64 | 权威值，由 `(tail_counter − head_counter) × 1e9 / (tail_monotonic_ns − head_monotonic_ns)` 经验计算 |
| `cntfrq_el0_hz` | `uint64` | 72 | 交叉校验用；非 aarch64 上恒为 0 |
| `record_count` | `uint64` | 80 | 实际写入的记录条数（写完记录循环后回填，见下） |
| `dropped_count` | `uint64` | 88 | 因缓冲已满而丢弃的记录数 |
| `process_tag` | `uint64` | 96 | 本进程随机 tag，即每条 `trace_id` 的高 32 位 |
| `method_table_offset` | `uint64` | 104 | 方法名表的**文件绝对偏移**（从文件起始算起，非从头部或记录区之后算起，虽然两者数值上一致，见下方「方法名表」） |
| （尾部填充） | — | 112 | 16 字节填充，凑满 128；无意义字节 |

`record_count` 与 `method_table_offset` 这两个字段不是在写头部时一次性确定的：`Dump()` 先以占位值（`0`）写出头部，再写完所有记录、统计出真实写入条数 `written` 后，回退到文件开头用真实值重写头部，再回到记录区末尾继续写方法表。这保证了即使并发流量在 dump 过程中继续调用 `AllocSlot()`，头部记录的条数也始终等于磁盘上实际存在的记录数，不会出现头部声称的条数比实际写入的多（进而 `method_table_offset` 指向记录区中间）的情况。

**magic 常量**：`LT_FILE_MAGIC` 的值是 `0x4252504C54524331ULL`。按大端字节序读这个字面量，拼出的字符串是 `"BRPLTRC1"`（`BRP` 之后**没有** `C`——不是 `"BRPCLTRC1"`）。但落盘格式是小端（本模块编译目标 x86_64 与 aarch64 都是小端主机），所以文件里从低地址到高地址实际的 8 个字节拼出的是 `"1CRTLPRB"`。`merge.py` 校验 magic 时要么按小端读出 8 字节后与 `b'1CRTLPRB'` 比较，要么把 `0x4252504C54524331` 按小端 pack 成 8 字节再比较；**不要**直接与 `b'BRPCLTRC1'`（9 字符，还多拼了个不存在的 `C`）比较——那既拼错了字符串，字节序也反了。

**方法名表**：紧跟在最后一条记录之后，起始偏移即 `method_table_offset`（= `sizeof(header) + record_count × sizeof(record)`，128 + 192 × 实际记录数）。格式为一个 `uint32 count`，随后是 `count` 个条目，每个条目是 `uint32 len` + `len` 字节原始内容（UTF-8，无结尾 `NUL`）。第 `i` 个条目（0-based）对应 `LatencyTraceRecord::method_id == i + 1` 的方法全名；`method_id == 0` 保留给「未识别 method」，因此表里没有第 0 项。

**时钟校准**：dump 文件头写入**两对**采样，进程启动时与 dump 时各取一次：

| 字段对 | 时钟 | 用途 |
|---|---|---|
| `head_monotonic_ns` / `tail_monotonic_ns` | `CLOCK_MONOTONIC` | **频率标定的唯一依据**：`freq = Δcounter / Δmonotonic`。必须用单调钟 —— 实时钟会被 NTP 步进，一次校正就能毁掉整段窗口的商 |
| `head_realtime_ns` / `tail_realtime_ns` | `CLOCK_REALTIME` | **仅供跨机 sanity check**，不参与频率计算。两台机器的单调钟各自从自己开机起算，直接相减无意义；实时钟才有共同参照 |

`merge.py` 算频率时**只能用 monotonic 那一对**。用错会引入 NTP 步进带来的系统性偏差，而这个偏差看起来完全像是计数器频率不准。

**频率必须由本模块自行标定，不能依赖 `butil`**：`BUTIL_USE_CPU_FREQUENCY=0` 时 `detail::invariant_cpu_freq` 根本不计算。标定方式为**经验测量** —— 用首尾两组 monotonic 采样求 `Δcounter / Δmonotonic`（理由见上表）。这比读 `CNTFRQ_EL0` 更可靠：它与架构无关（x86 无对应寄存器），且反映实际速率而非标称值。同时额外读取 `CNTFRQ_EL0`（aarch64）写入文件头作**交叉校验**，两者偏差超过 0.1% 时 `merge.py` 告警。

这一项是 D1 公式的正确性前提：`L = RTT − S` 用客户端时钟的时长减服务端时钟的时长，任一台机器的频率标定错误都会给 `L` 引入系统性偏差。

### 8.2 三类点位的载体与透传

| 类别 | 载体 | 说明 |
|---|---|---|
| 请求内点位 | `Controller` 存 8 字节 handle | 客户端全程可用 |
| 事件级点位<br>（C09–C11 / S01–S03） | `Socket` 增 3 个字段 → `ProcessNewMessage` 时拷入 `InputMessageBase`（增 4 字段）→ 解析出 handle 后拷入槽 | **同一批次的多条消息共享同一个 `wake` 值**。这是 §6.2 所述污染的来源，在数据中可见（时间戳相同即同批），不隐藏 |
| 写完成点位<br>（C08 / S17） | `WriteRequest` 增 8 字节 handle（`#ifdef` 内） | 时间戳在 `DoWrite` 中 `CutFromIOBufList()` 返回后就地打戳（见 §8.4），按 handle 直接回填，**不阻塞**，不使用 rpcz 那套 `bthread_id_join` |
| handle 尚不可得的点位<br>（C13/C14、S05/S06） | 先落函数局部变量，`bthread_id_lock` 取回 Controller 后一次性写入槽 | 因 `ProcessRpcResponse` 是先解 meta 再 lock（`baidu_rpc_protocol.cpp:940` / `:950`） |

`epoll_wait` 返回的时间戳存入 TLS，同一次返回的 N 个事件共享该值 —— 这是正确语义（它们确实是同一次唤醒）。

### 8.3 跨进程关联

`baidu_rpc_meta.proto` 的 `RpcRequestMeta` 增加：

```protobuf
optional uint64 latency_trace_id = 9;
```

tag 9 空闲（已核）。optional 字段向后兼容，未打补丁的对端忽略之。

不复用 rpcz 的 `trace_id`/`span_id`（tag 4/5），因其填充条件绑死在 `IsTraceable()` 亦即 `bvar::Collector` 采样上，复用将牵动 rpcz 自身行为。

`trace_id` 生成：进程启动时取一个 64 位随机 `g_process_tag`，`trace_id = (g_process_tag & 0xFFFFFFFF00000000ULL) | seq`，保证跨进程唯一。

### 8.4 `write_end` 为什么不落在 `ReturnSuccessfulWriteRequest`

初版设计把 `write_end` 放在 `ReturnSuccessfulWriteRequest()`（`socket.cpp:509`），因为那是「WriteRequest 写完」的唯一汇聚点。**这个位置会产生系统性的负值**，必须避开。

原因：`ReturnSuccessfulWriteRequest` 是 `writev` / `ibv_post_send` 返回**之后**的一个额外步骤。在 `KeepWrite` 路径上它还要先经过 `IsWriteComplete()`（`socket.cpp:1871`）。也就是说字节早已交给内核或网卡，`write_end` 却尚未打戳。在 RDMA 或 loopback 这类极快路径上，**响应的 `wake` 完全可能早于本请求的 `write_end`**，于是 `RTT = C09 − C08 < 0`，进而 `L = RTT − S` 大幅为负。这不是归因污染，是埋点位置错误。

正确落点：`Socket::DoWrite()` 中 `_transport->CutFromIOBufList()` 返回后（`socket.cpp:1902`）立即遍历本次批处理的 WriteRequest 链，对 `data` 已清空者就地打戳。这既精确表达「本请求最后一个字节交给内核/网卡的时刻」，又完全躲开 `KeepWrite` 的调度延迟。

`ReturnSuccessfulWriteRequest` 仍然是 handle 回填与记录收尾的落点，但时间戳取自上述更早、更准的位置。

### 8.5 RDMA 差异

**写侧零增量**：共用 `Socket::Write` → `StartWrite`/`KeepWrite` → `DoWrite`，仅末跳换成 `_rdma_ep->CutFromIOBufList()`（`socket.cpp:1902`，`rdma_transport.cpp:100`）。`write_end` 一律取 `ibv_post_send` 返回时刻，**不取 send CQE**：RC 的 send completion 隐含对端 ACK，会把一个单程链路时间错算进「write 接口时间」，破坏 §6 的语义。

**收侧不共用**。RDMA 数据面另建一个 CQ 专用 socket，其 OnEdge 回调是 `RdmaEndpoint::PollCq`（`rdma_endpoint.cpp:1135` / `:1469`），而不是 `InputMessenger::OnNewMessages`。且 RDMA 有两种收包模式，点位语义各不相同：

| 点位 | TCP | RDMA 事件模式 | RDMA 轮询模式（`-rdma_use_polling`） |
|---|---|---|---|
| `wake` | `epoll_wait` 返回 | `comp_channel->fd` 的 epoll 唤醒（`GetAndAckEvents`） | **不存在**：无唤醒事件 |
| `onedge_start` | `Transport::OnEdge` 入口 | 同左（回调为 `PollCq`） | **不存在**：poller 线程直接调 `PollCq`（`:1733`），无 bthread 切换 |
| `read_start` | `m->DoRead()` 前 | `ibv_poll_cq()` 前（`:1500`） | 同左 |
| `msg_recv_done` | `ProcessNewMessage` 内 | **共用**（`:1599` 调用同一函数） | **共用** |

**轮询模式下 `wake` 与 `onedge_start` 记为哨兵值而非 0**，分析工具将 `cli_wake_to_onedge` / `cli_onedge_to_read` / `srv_wake_to_onedge` / `srv_onedge_to_read` 四项判定为 N/A 并在 HTML 中显式标注「该模式下不存在」。这四项恒为零是物理真实（轮询以 CPU 占用换掉了这段延迟），但若不标注会被误读为埋点缺失。Σ 恒等式不受影响 —— N/A 项以 0 参与求和，而这些区间的真实长度确实为 0。

## 9. 分析工具 `tools/latency_trace/`

### 9.1 `merge.py`

输入两端 dump 文件，输出中间 JSON：

1. 读文件头（128 字节，字段顺序与偏移见 §8.1 的 `LatencyTraceFileHeader` 表）。**先校验 `magic`**：文件里小端读出的 8 字节应为 `b'1CRTLPRB'`（见 §8.1「magic 常量」），不匹配则拒绝该文件，不要继续解析。**频率只能由 `head_monotonic_ns` / `tail_monotonic_ns` 这一对算出**（见 §8.1 的表；realtime 那一对仅供跨机 sanity check，不参与频率计算）。**按 §8.1 的编码约定解码 `ts[i]`**：`0` 表示未采集（跳过，不要当成 0 偏移）；`0xFFFFFFFF` 表示该模式下不存在（标注 N/A，见 §8.5）；`0xFFFFFFFE` 表示饱和（区间 ≥ 约 42.9 秒，标注为下界而非精确值）；其余值的真实偏移是 `ts[i] − 1`。据此把 `base_counter + (ts[i] − 1)` 换算为各自进程时钟下的纳秒值。方法名按 `method_table_offset`（§8.1「方法名表」）读出，`method_id − 1` 索引其中的条目，`method_id == 0` 表示未识别。
2. 按 `trace_id` join 客户端与服务端记录。未配对的记录单独统计并报告（数量、原因分类）。
3. 按 §6 的两种模型分别计算 `link_up` / `link_down`。
4. 计算 33 个分解项。
5. **断言 `Σ(33 项) == C17 − C01`**，容差为 0（整数运算）。不通过则报错退出，指出违例记录。

### 9.2 `render.py`

生成单文件 HTML。

| 能力 | 说明 |
|---|---|
| 主图 | Canvas 累积柱状图；x = 按端到端时延排序的排名，y = 累积时延 |
| 配色 | 22 个具名项高饱和，11 个间隙项灰阶；遵循 `dataviz` skill 的配色规范，明暗主题各自可读 |
| hover | 命中测试 → 浮层显示该请求的 34 个时间戳与 33 个分段值 |
| 缩放 | 框选缩放到任意排名区间（用于放大尾部） |
| 链路模型 | 下拉切换模型 A / B，并提供「两模型差值」列 |
| 图例 | 点击隐藏/显示单个分段 |
| 统计 | 顶部 p50 / p90 / p99 / p99.9 各分段贡献表 |
| 筛选 | 按 `L < 0`、按 attempt、按 error_code、按 socket_id 筛选 |

---

### 9.3 负分解项的渲染

分解项可以为负，主要出现在两个链路项上（模型 B 下更常见）。**负值不被裁剪、不被隐藏、不被均摊**。

渲染按**瀑布图**语义而非普通堆叠图：令 `y_k = Σ_{i≤k} seg_i`，第 k 段占据纵向区间 `[y_{k-1}, y_k]`。若 `seg_k < 0` 则 `y_k < y_{k-1}`，该段是一条**向下回退、与前序段重叠**的带子，用斜纹半透明填充加明显描边绘制，斜纹透出下层颜色使两段同时可见。

**柱高不变量不受影响**：因 `y_33 = Σ(33 项) = C17 − C01` 是恒等式，柱子最高点始终精确等于端到端时延，与各段符号无关。

**默认全部绘制**，不过滤任何请求。含负段的柱子在基线处加红色标记；顶部显示「N 条含负分段（x%）」并可一键筛选。这样做的理由：若负值与长尾相关（例如都源于同一批 epoll 唤醒下的批处理），默认隐藏恰好会藏起最该看的那批请求。另提供两个可选视图 —— 隐藏含负段请求、负段画到零线以下不重叠。

## 10. 验证

### 10.1 Σ 恒等式检查不到什么，以及什么能检查到

初版设计把 `Σ(33 项) == C17 − C01` 当作埋点正确性的主要防线。**这是错的**，理由见 §5 的展开：该式是望远镜求和，对任意时间戳取值恒成立，全零也成立。它校验的是分析工具的算术。

以下三类真实缺陷都能在它眼皮底下通过，必须靠别的检查捕获：

| 缺陷 | Σ 恒等式 | 单调性 | 真正能捕获它的检查 |
|---|---|---|---|
| **某条代码路径漏埋**（例：SSL 与 `_conn` 分支未插 `write_start`/`write_end`，两点恒为 0） | ✅ 通过（`C08=0` 时 `link` 项吸收了差额，等式仍成立） | ✅ 通过 | **点位非零检查**。且 `RTT = C09 − 0` 会变成一个巨大的正数，链路时间随之荒谬 —— 但没有任何断言会看它 |
| **瞬时事件点位被重复写入**（例：某个只应触发一次的点位因为代码路径重入被写了两次，第二次的时刻覆盖了第一次） | ✅ 通过 | 视情况 | **写入次数检查**：瞬时事件类点位至多写一次。否则该点位的真实首次触发时刻会被后一次覆盖，污染其后所有分解项 |
| **「完成本单元的那次尝试」点位被首次写入锁死**（例：`write_start`/`read_start` 若误用首次写入优先，`KeepWrite`/多次 `DoRead` 重入时会永久锁在第一次尝试的时刻，把本该属于「排队」的时间错记成「传输」） | ✅ 通过 | ✅ 通过（`C06 ≤ C07 ≤ C08` 单调性检查不到方向反了） | 专项断言 + 人工审查；这两点的正确策略是**最后写入优先**，见下 |
| **点位落在错误位置**（例：`write_end` 落在 `ReturnSuccessfulWriteRequest`） | ✅ 通过 | 视情况 | **跨端非负检查**：`C09 ≥ C08`（负 RTT），以及低并发下每个分解项非负 |

**共同点**：这几类缺陷都产出「看起来合理」的数字，都不会让任何构建变红。它们只能被针对性的断言捕获，而不能被一条恒真的等式捕获。

**`C09 ≥ C08` 这条跨端非负检查，在生产规模的真实抓取下有极小概率被合法触发，且已被刻画（见 §13 的对应风险行）。** 90,486 次连续请求的一次实测抓取里出现 1 条违例（≈0.0011%），量级与另一次约 3,288 次的抓取（1 条违例）一致（都是「罕见、随主机负载浮动」的量级，不是某条代码路径固定触发的量级）。判别证据是该违例记录的 `C07→C08` 跨度独自坐在全体样本分布的最大值处、且分布本身平滑变薄，而不是聚集在某个固定额外开销上——这是调度延迟尾部的形状，不是埋点位置错误的形状（埋点错误会产出一簇具有相同额外开销的违例，而不是孤立的极值）。断言本身**不因此放松**，仍是本节意义上的承重检查；这一行只是说明它在这个概率量级下被触发时，读者不应把它读成 bug 信号。

**因此实现要求是按点位分策略，而不是一条对全体点位通用的规则**：`LatencyTraceBuffer::Stamp()` 对同一点位的重复写入**首次写入优先**（后续写入丢弃），适用于绝大多数点位——它们是瞬时事件，第二次写入意味着该事件触发了两次，是 bug，第一次（正确的）那次不能被冲掉。

`write_start`（`Socket::DoWrite`，被 `KeepWrite` 为同一个还未写完的 `WriteRequest` 反复重入）和 `read_start`（`InputMessenger::OnNewMessages`，在同一条尚未拼完的消息上被反复重入）是例外：它们的含义不是「这个单元的操作第一次被尝试」，而是「真正完成这个单元的那次操作的起点」。当一个单元排在队列里、前面还有别的单元占用同一个 socket 时，靠前的几次尝试服务的是*别的*单元（读侧是别的、已经在缓冲区里就绪的消息），只有最后一次尝试才真正搬动了本单元的字节。若这两个点位仍用首次写入优先，第一次尝试的时刻会被永久锁定，本该计入排队时间的间隔会被静默并入传输耗时——这正是上表第二类缺陷。两个点位实现「最后写入优先」的机制并不相同，取决于打点发生时 `LatencyTraceHandle` 是否已经存在：

- `write_start` 打点时 `WriteRequest::lt_handle` 已经存在（`Socket::Write()` 入口就已分配），`DoWrite` 每次被 `KeepWrite` 重入都能直接对同一个 handle 再打一次，因此**在 `LatencyTraceBuffer` 层**新增 `StampLast()`：语义与 `Stamp()` 相反，每次调用无条件覆盖 `ts[point]`，供 `write_start` 调用点直接使用。
- `read_start` 打点时消息尚未解析出 `trace_id`，`LatencyTraceHandle` 根本不存在（同 §8.2 的透传问题）：每次 `DoRead` 直接覆盖 `Socket::_lt_read_start` 这个原始 `uint64_t` 字段本身（不经过 `LatencyTraceBuffer`），message 被切出时才把该字段的（此时已经是最后一次 `DoRead` 的）值拷进 `InputMessageBase`，再用 `StampAt()`（首次写入优先）一次性写入 `ts[point]`——因为对 `ts[point]` 这一层而言只会写一次，「最后写入优先」的效果已经在更早的原始字段覆盖阶段完成，不需要也不能用 `StampLast()`（那一层根本没有 handle 可用）。

两种机制殊途同归：不论在哪一层实现覆盖，最终躺进 `ts[point]` 的都是「真正完成本单元这次操作」那次尝试的时刻。


| 类别 | 内容 |
|---|---|
| 不变量单测 | 逐请求断言 `Σ(33 项) == C17 − C01`。**这条只校验 merge.py 的算术**，见 §10.1 |
| 单调性单测 | 34 个点位非零且时间戳单调不减 |
| 缓冲单测 | generation 覆盖语义、handle 校验拒绝迟到写入、容量满后停止记录并正确计数 |
| microbenchmark | 单次 `LT_STAMP` 成本（Phase 0 的出口） |
| 端到端 | 起 server + client 跑 N 个请求，验证两端记录可 100% join。**低并发（每连接 outstanding=1）下**额外断言模型 A 的分解项全部非负 —— 此条件下不存在批处理归因失真，出现负值即表明埋点位置有误（参见 §8.4 的教训）。高并发下不做非负断言，只统计负值比例 |
| 回归 | 未定义 `BRPC_LATENCY_TRACE` 时，`sizeof(WriteRequest) == 64` 断言仍成立；现有测试全绿 |

---

## 11. 开销与 Phase 0

34 个点位，每个约一次计数器读 + 一次 store。aarch64 的 `cntvct_el0` 是跨时钟域的系统计数器读，量级估计在 10~40 ns（各实现差异大，**必须实测**）。全程估计 0.4~1.4 μs。

| 场景 | 端到端量级 | 打点占比（估） |
|---|---|---|
| TCP loopback | 30~60 μs | 1~5%，可接受 |
| RDMA | 5~15 μs | 3~28%，**可能改变待观察的分布形状** |

**Phase 0 已完成，结论：全量 34 点。** 实测单次打点 5.9~7.2 ns（`butil::detail::clock_cycles()` 直读计数器），34 点总开销 **0.263~0.268 μs**，占 RDMA 5~15 μs 端到端的 1.8~5.4%，远低于 10% 阈值。`lite` 点位集与 `-latency_trace_point_set` gflag **均不实现**。完整数据见 `docs/superpowers/plans/phase0-results.md`。

同时实测确认：两台 aarch64 机器的计数器经验频率分别为 100.001 MHz 与 99.994 MHz，**互差 0.007%**，远低于本节要求的 0.1% 告警阈值 —— §6 的 `L = RTT − S` 所依赖的「两端时钟频率一致」前提成立。另外 `clock_gettime(CLOCK_MONOTONIC)` 实测 23.2 ns/次，比直读计数器贵 3.6~4.5 倍，坐实了 §8.1 不使用 `cpuwide_time_ns()` 的选择。

---

## 12. 交付顺序

| Phase | 内容 | 出口判据 |
|---|---|---|
| 0 | microbenchmark 实测打点成本 | 拿到真实数字，据此确定最终点位集 |
| 1 | `latency_trace` 模块 + 单测 | 缓冲语义、handle 校验、容量策略全部通过 |
| 2 | TCP + baidu_std 全部 34 点位埋点 | 端到端跑通，34 点位齐全且单调 |
| 3 | 落盘 + `merge.py` | `Σ` 恒等断言通过，join 率 100% |
| 4 | HTML 渲染 | 1 万~10 万条可交互，hover 与缩放可用 |
| 5 | RDMA 收侧三点位（`wake` / `onedge_start` / `read_start`）在 `PollCq` 中重新实现，并区分事件模式与轮询模式；写侧零增量 | TCP / RDMA 分解对比图；轮询模式下四项正确标注为 N/A |

---

## 13. 风险与已知限制

| 风险 | 缓解 |
|---|---|
| 打点开销在 RDMA 下占比过高 | Phase 0 实测 + `lite` 点位集；同一二进制下 gflag 开关对照，量化影响 |
| 一次 wake 覆盖多条消息导致 `S01` 归因失真 | 数据中可见（同批时间戳相同）；模型 B 将其暴露为 `link_up < 0`；HTML 可筛选 |
| 两台机器的计数器频率标定不准，导致 `L = RTT − S` 系统性偏差 | 频率由首尾采样对经验标定，并与 `CNTFRQ_EL0` 交叉校验；偏差超 0.1% 时 `merge.py` 告警。见 §8.1 |
| 跨机网络本身上下行不对称 | 模型 A 强制对称，模型 B 的 `O` 会带该不对称的系统性偏差；但**两模型的总链路时间与总柱高均不受影响** |
| 追踪构建与生产构建 ABI 不同 | 文档明示；两端必须使用同一构建配置 |
| 缓冲满后停止记录导致窗口截断 | 文件头记录丢弃数；HTML 显式提示窗口是否被截断 |
| RDMA 轮询模式下收包排队相关四项恒为 0，易被误读为埋点缺失 | 记哨兵值而非 0，HTML 标注「该模式下不存在」；见 §8.5 |
| **计数器分辨率 10 ns/tick 淹没最小的分解项** | 100 MHz 计数器每 10 ns 才跳一次。一次 5 μs 的 RDMA RPC 只有约 500 个 tick 要分给 33 项，而 `srv_dispatch`、`srv_to_service`、`srv_service_to_ser`、`cli_lookup_cntl`、`cli_post_deser` 这几个间隙项的真实值可能只有几十纳秒 —— 相邻两次打点若相距不足 10 ns 会读到**同一个 tick**，该项显示为 0。缓解：这是硬件下限，无法消除；`merge.py` 对量化到 0 的项标注「低于计数器分辨率」，HTML 中以不同纹理绘制，避免被读成「这一段不耗时」。聚合视图（1 万~10 万请求的分布）中量化噪声会被平均掉，逐请求 hover 值则天生是 10 ns 的整数倍 |
| **每个区间含一次打点自身的执行时间，构成系统性正偏置** | 区间恒为 `ts[N+1] − ts[N]`，其中必然包含后一次打点自身的 5.9~7.2 ns。绝对值很小，但对上一行那几个几十纳秒的间隙项，相对偏差可达 10~20%。缓解：在 `merge.py` 与 HTML 的说明中写明该偏置的量级；不做自动扣减 —— 扣减需要假设每次打点成本恒定，而实测本身是最好情形下的值 |
| **流式响应（`SendStreamData`）未插桩，S15–S17 恒为 0** | 服务端的流式分支不经过普通的 `sock->Write()` 路径。本设计的范围是普通 RPC（D3），流式响应不在其内。缓解：§10.1 的「点位非零」判据会把它标为未采集，`merge.py` 据此排除该请求而非产出错误的写入时延。**不要**把它当成埋点缺陷去修 —— 它是范围之外，但必须可见 |
| 分解项出现负值 | 瀑布式渲染，斜纹标记，默认全部绘制并统计占比；柱高不变量不受影响。见 §9.3 |
| `write_end` 埋点位置不当会系统性制造负 RTT | 落点定在 `DoWrite` 中 `CutFromIOBufList()` 返回后，而非 `ReturnSuccessfulWriteRequest`。见 §8.4 |
| **`write_end` 之后、`wake` 之前存在一段不可观测窗口，繁忙主机上偶发产生合法的负 RTT（`C09 < C08`）** | write 系统调用真正完成写入的时刻发生在内核/网卡内部，本模块唯一能做的是「系统调用返回后，尽快在一个可被调度的线程上打戳」——`write_end` 落点已按 §8.4 校准到这个尽快点，但打戳本身仍需要该线程被调度器实际运行到。在繁忙的多核主机上，这个「系统调用已返回、打戳线程尚未被调度」的窗口偶尔会被拉长到几十微秒，长到足以让一次快速的 loopback 响应先被对端处理、其 `wake` 时间戳先于本请求迟到的 `write_end` 落盘。已用两组实测数据刻画量级：约 3,288 次连续请求中出现 1 条（早期小规模复现），90,486 次连续请求中出现 1 条，比例 ≈0.0011%，两次比例相差约 27 倍，符合「随主机负载浮动的调度尾部事件」的特征，而非「某条代码路径固定触发」的特征（后者的比例应当稳定，不随负载波动）。判别证据：90,486 条样本里唯一违例记录的 `C07→C08`（write_start→write_end）跨度为 76,220 ns，是全体样本的单一最大值，孤立坐落在一个平滑、连续变薄的分布尾部之外（中位数 6,920 ns，p99.9 为 17,150 ns）——没有第二个数值与它接近，说明这不是某个固定次生代码路径的重复开销，而是普通操作系统调度尾部「一次特别长的停顿」的形状；该记录整体端到端时延（C01→C17 = 90,730 ns）只是中位数的约 2.2 倍，并不是一次全局意义上的慢请求，异常严格局限在 `C07→C08` 这一个窗口内。两处 `C08` 打戳点都已按 §8.4 的落点核对，紧跟在 write 系统调用返回之后，之间没有能吃掉几十微秒的代码。**使用者应当怎么做**：这类记录是真实的测量结果，`write_end` 因调度延迟而偏晚，应当照常计入统计、如实报告，而不是当作坏数据静默丢弃；在 ~0.001% 这个量级下它们不会威胁任何百分位数的可信度。`C09 ≥ C08` 断言本身不放松——它仍是 §10.1 的承重检查，这一行只是把它在生产规模数据下会被小概率合法触发这件事记录下来，避免使用者第一次撞见时误判为埋点 bug。详见 `.superpowers/sdd/2026-08-29-latency-trace-instrumentation/task-a2-report.md`；另见 §10.1 该断言条目旁的指引 |
