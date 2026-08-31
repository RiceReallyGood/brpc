# 时延追踪离线分析与可视化 实现计划

> **For agentic workers:** REQUIRED SUB-SKILL: superpowers:subagent-driven-development。步骤用 `- [ ]` 复选框跟踪。

**Goal:** 把两端产出的 dump 文件合并成逐请求的 33 项时延分解，并生成一个单文件 HTML，用累积柱状图定位长尾来源。

**Architecture:** 两个 Python 脚本。`merge.py` 解析二进制 dump、按 `trace_id` join 两端记录、算出 33 个分解项、输出中间表示；`render.py` 把中间表示嵌进一个自包含的 HTML（Canvas 渲染，无外部依赖）。

**Spec:** `docs/superpowers/specs/2026-08-29-brpc-latency-trace-design.md` —— §8.1 落盘格式、§9 分析工具、§10.1 哪些判据承重、§6 两种链路模型。

## Global Constraints

- Python 3，只用标准库（`struct` / `json` / `argparse` / `statistics`）。不引入 numpy 等依赖 —— 这个工具要能在任何一台机器上直接跑。
- **落盘格式以 `src/brpc/latency_trace.h` 为准**，不以设计文档为准。文档已经错过一次（校准对、magic 注释），代码是唯一事实。解析前先核对 `sizeof` 与字段偏移。
- HTML 必须是**单文件、可离线打开**，不引用任何 CDN。
- 生成的 HTML 在明暗两种主题下都要可读。

---

## Task A: 产出第一份真实 dump

至今所有验证都是单测。这一步的价值不在于拿到测试数据，而在于**第一次看见这套埋点真正吐出来的东西** —— 预计会暴露单测结构上抓不到的问题。

**Files:** Create `tools/latency_trace/capture.sh`

- [ ] **A1. 写采集脚本**

在 `~/brpc-lt-traced` 树里起一个 echo server 和 client，跑 N 次 RPC，两端各自 dump。关键参数：
`-latency_trace_enabled=true -latency_trace_dump_path=<path>`。
server 与 client 用不同的 dump 路径。跑完后把两个文件拉回本地。

- [ ] **A2. 裸眼检查**

`hexdump -C` 看文件头前 128 字节：magic 是否为小端的 `1CRTLPRB`、`record_count` 是否等于预期、
两对校准时间戳是否合理、`counter_freq_hz` 是否约等于 100 MHz、`method_table_offset` 是否非零。
**发现任何一处对不上就停下来报告**，不要在解析器里绕过它。

- [ ] **A3. 记录观察**

把两个 dump 的实际大小、记录数、以及 A2 的每一项核对结果写进 `docs/superpowers/plans/capture-notes.md`。
后面每一步都以这份真实数据为基准。

---

## Task B: `merge.py`

**Files:** Create `tools/latency_trace/merge.py`, `tools/latency_trace/test_merge.py`

- [ ] **B1. 解析器 + 单测**

按 `latency_trace.h` 解析头、记录、方法表。解码规则（§8.1）：`ts[i]` 存的是**偏移 + 1**，
`0` = 未采集、`0xFFFFFFFF` = 该模式下不存在、`0xFFFFFFFE` = 饱和；其余真实偏移为 `ts[i] − 1`。
**这三个保留值都不能减 1。**
频率**只能**由 `head_monotonic_ns` / `tail_monotonic_ns` 这一对算出，realtime 那一对仅供跨机 sanity check。

先用 Task A 的真实文件写一条端到端的解析测试，再补边界单测（空文件、截断文件、`method_table_offset` 为 0）。

- [ ] **B2. join 与判据**

按 `trace_id` 配对两端记录。对每条配对记录施加 §10.1 的判据：每个点位非零、单调不减、
低并发下每个分解项非负、`C09 ≥ C08`。**不满足的记录不参与统计，但必须按原因分类计数并报告** ——
静默丢弃会让「埋点漏了」和「这段本来就快」变得无法区分。

- [ ] **B3. 33 个分解项与两种链路模型**

按 §5 算出 33 项。链路按 §6 的两种模型各算一份：逐请求对半分、每连接滑窗估 offset。
两者的总链路时间恒等，差异只在上下行劈分 —— 差得多的请求即归因污染重的请求，本身是信号。

- [ ] **B4. 中间表示**

输出给 `render.py` 的数据。**先测量体积**：10 万请求 × 33 项 × 4 字节 = 13.2 MB，
base64 后约 17.6 MB —— 单个 HTML 文件偏大。若超过 12 MB，改为
「尾部全量保留 + 头部降采样」并在 HTML 里注明降采样比例。

---

## Task C: `render.py`

**Files:** Create `tools/latency_trace/render.py`

- [ ] **C1. 骨架与数据嵌入**

单文件 HTML，数据以 base64 的 `Int32Array` 嵌入，页面加载时解码进 typed array。
调色板遵循 `dataviz` skill 的规范：33 段里用户原始清单的那些用高饱和色、间隙段用灰阶，
明暗主题各自可读。

- [ ] **C2. Canvas 累积柱状图**

x 轴是**虚拟 rank 轴**，视口只画其中一段。缩小时按像素列聚合，
**每列取该列内端到端最大的那条请求**（理由见下）。y 轴是累积时延。
负分段按瀑布语义画成向下回退的斜纹带（§9.3），柱顶始终等于端到端时延。

聚合取最大值对两种排序都成立：按端到端排序时同列内延迟几乎相等，取最大值保证最差的那条不被丢掉；
按 trace_id 排序时取最大值才能让尖峰可见 —— 取中位数会让一条 100ms 请求淹没在 50 条正常请求里。

- [ ] **C3. 交互:缩放与复位**

框选缩放到任意 rank 区间，一个按钮复位到全量。缩放改变的是虚拟 rank 轴的可见窗口，
放大到每列 = 1 条请求时，hover 自然落到具体请求上。

- [ ] **C4. 交互:分段开关**

图例点击可隐藏/显示任一分段。隐藏后柱子相应变矮 —— 这是**刻意的**：
它让「去掉这一段之后还剩多少」一眼可见。在图例上注明这一点，免得被当成 bug。

- [ ] **C5. 交互:hover**

命中测试后浮层显示该段名称与时延。缩小状态下额外注明「本列代表 N 条请求，显示其中最慢的一条」。

- [ ] **C6. 分位数表**

33 个分段各一行，列为 均值 / P50 / P90 / P99 / P99.9。
**统计范围跟随当前缩放的可见区间**，表头显示当前区间与条数；另设「全量」开关做对照。
点击任意列头排序，默认按 P99 降序。

- [ ] **C7. 排序模式与链路模型切换**

累积柱状图可按端到端时延排序，也可按 `trace_id` 排序（后者约等于时间序，用来看尖峰何时出现）。
链路模型下拉切换 §6 的两种。

- [ ] **C8. 用真实数据验收**

拿 Task A 的 dump 跑完整条链路，逐条核对 5 项需求。
把生成的 HTML 在浏览器里实际打开操作一遍 —— 不要只验证文件生成成功。

---

## 已知会踩的地方

- 设计文档与代码曾经不一致过两次（校准对、magic 注释）。**以 `latency_trace.h` 为准。**
- 服务端记录的 `base_counter` 是 wake 时刻而非建槽时刻，客户端是建槽时刻。两端不可直接比较。
- RDMA 轮询模式下 `wake` / `onedge_start` 是哨兵，四个依赖它们的分解项须标 N/A 而非 0。
- 流式响应的 S15–S17 恒为 0，被取消的 backup 的接收侧点位恒为 0 —— 都要能与「没测到」区分开。
