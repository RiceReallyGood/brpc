# 如何复现 latency.html

这份文档讲的是：从零开始，怎么拿到一张请求级时延分解的 HTML 图。

全流程分四步 —— **构建 → 采集 → 合并 → 渲染**。前两步需要能编译 brpc 的机器，
后两步只需要 Python 3 标准库，在哪台机器上都能跑。

```
  ┌──构建──┐   ┌──采集──┐   ┌──合并──┐   ┌──渲染──┐
  开启打点开关 →  两端各落一个 →  按 trace_id →  单文件 HTML
  重新编译       .dump 文件      join + 分解     零外部引用
   (Make)      (capture.sh)    (merge.py)     (render.py)
```

---

## 0. 最短路径：不构建，先看图

仓库里有一份真实采集的夹具（3222 条，loopback，baidu_std/TCP）。**30 秒就能出图**，
用来确认工具链本身没问题、或者先看看页面长什么样：

```bash
cd tools/latency_trace
python3 merge.py --client testdata/client.dump --server testdata/server.dump -o /tmp/ir.json
python3 render.py --ir /tmp/ir.json -o /tmp/latency.html
```

用浏览器打开 `/tmp/latency.html`。**它可以断网打开** —— 页面不引用任何 CDN、字体或外部资源。

两步也可以合成一步（`render.py` 会在进程内直接调用 `merge.py` 的函数）：

```bash
python3 render.py --client testdata/client.dump --server testdata/server.dump -o /tmp/latency.html
```

两条路径产出的 HTML **字节完全相同**（实测均为 1,915,611 字节），选哪条只看你要不要留中间文件。

> 中间的 `ir.json` 值得单独留一份：它是纯 JSON，便于你自己写脚本二次分析，
> 也便于在不重新采集的情况下反复调渲染参数。

---

## 1. 构建：打开打点开关

打点代码整体在 `#if defined(BRPC_LATENCY_TRACE)` 之后，**默认构建完全不受影响**
（`WriteRequest` 保持 64 字节，`BAIDU_CASSERT` 那条断言照常成立）。开关打开后
该结构体扩到 128 字节。

```bash
./config_brpc.sh --headers=/usr/include --libs=/usr/lib --with-latency-trace
make -j$(nproc)
```

`--with-latency-trace` 会在 `config.mk` 里加上 `-DBRPC_LATENCY_TRACE=1`。

⚠️ **三件必须知道的事：**

1. **只有 Make 支持这个开关。** CMake 和 Bazel 没有对应选项 —— 用它们构建出来的
   是无打点版本，且不会报错。
2. **开关改变了头文件里的结构体大小。** 同一棵树上来回切换配置而不 `make clean`，
   后果是**运行时堆损坏，不是编译错误**。所以建议：**一种配置一棵树**，不要原地切。
3. 关闭时 `BRPC_LATENCY_TRACE` 必须**完全不定义**，而不是定义成 0 ——
   宏用的是 `#if defined(...)`，写成 `=0` 反而会把打点打开。`config_brpc.sh` 已按此处理。

验证这次真的构建出了打点版本（别只信退出码）：

```bash
nm libbrpc.a | grep -q LatencyTraceBuffer && echo "TRACED" || echo "default"
```

---

## 2. 采集：让两端各落一个 dump

### 2.1 运行时 flags

客户端和服务端**都要**加，两边各写各的文件：

| flag | 默认 | 说明 |
|---|---|---|
| `-latency_trace_enabled` | `false` | 总开关，必须显式设为 `true` |
| `-latency_trace_dump_path` | 空 | dump 文件路径，**空则不落盘** |
| `-latency_trace_capacity` | `100000` | 环形缓冲总容量（条） |

```bash
./your_server -latency_trace_enabled=true -latency_trace_dump_path=/tmp/server.dump &
./your_client -latency_trace_enabled=true -latency_trace_dump_path=/tmp/client.dump
```

### 2.2 两个会让你拿不到数据的坑

**坑一：必须用 SIGINT 退出，不能用 SIGTERM。**
dump 是注册在 `atexit` 上的。brpc 只有在 `-graceful_quit_on_sigterm=true` 时才装
SIGTERM 处理器；否则 SIGTERM 走默认处置，进程直接死，`main()` 从不返回，
**`atexit` 不触发，文件根本不会被写出来，且没有任何报错**。

```bash
kill -INT "$PID"      # ✅ 正常落盘
kill -TERM "$PID"     # ❌ 零字节，静默
```

**坑二：容量是按分片分配的，实际值会向上取整。**
缓冲分 16 个分片，每片容量向上取到 2 的幂。所以 `-latency_trace_capacity=100000`
实际得到 **131072** 条 —— 保证「不少于所请求的」。

缓冲**写满即停，绝不覆盖**（这是两端 join 窗口对齐的前提）。写满后新请求不再被记录，
但会被计入文件头的 `dropped_count`，`merge.py` 会把它显示在页面上并在控制台告警。
**看到告警就说明你手上的数据只覆盖了压测的前一段**，加大容量重跑。

### 2.3 用 capture.sh 一键采集（loopback）

如果你像本项目一样在远端构建机上跑，`capture.sh` 把「同步 → 构建 → 起服务端 →
跑客户端 → 拉回 dump」全串起来了：

```bash
LT_BUILD_DIR=brpc-lt-traced ./tools/latency_trace/capture.sh /tmp/lt_out
```

环境变量：

| 变量 | 默认 | 说明 |
|---|---|---|
| `LT_BUILD_HOST` | `suzhou950` | ssh 目标 |
| `LT_BUILD_DIR` | `brpc-lt-traced` | 远端树，**相对路径**，不要写 `~/` |
| `CAPTURE_PORT` | `9541` | echo 服务端口 |
| `CAPTURE_REQUESTS` | `3000` | 顺序请求数 |
| `CAPTURE_TRACE_CAPACITY` | 不设 | 覆盖两端的 `-latency_trace_capacity` |

它跑的是**单 channel、阻塞、无重试**的顺序调用，即 `outstanding=1` ——
这是设计文档 §10.1 中唯一断言「每个分项都非负」成立的条件。
产出 `<out_dir>/client.dump` 和 `<out_dir>/server.dump`。

`capture.sh` **不做**的两件事：不配置远端树（`config_brpc.sh` 是一次性手工步骤，
免得两棵树抢 `config.mk`），也不跑 merge/render。

### 2.4 跨机采集

跨机时两端时钟不同步，链路时延需要靠模型推算，`merge.py` 实现了两个：

- **模型 A（默认，你定的公式）**
  `((t_client_epoll_wake − t_client_writev_end) − (t_server_writev_end − t_server_epoll_wake)) / 2`
  客户端等待时间减去服务端软件时间，平均到上下两个方向。
- **模型 B** 每连接分箱估钟差：在每个分箱内取链路时间最小的样本推 offset，
  依据同 NTP —— 最小的样本排队污染最轻，对称假设最可能成立。

**页面上可以随时切换两个模型**，不需要重新采集。两者不一致本身就是信息：
模型 B 下 `link_up` 为负是**归因被污染的探测器**，不是噪声。

跨机时把两端的 dump 收到同一台机器上再 merge 即可。`merge.py` 会检查两个文件的
realtime 采集窗口是否重叠 —— 不重叠会告警，因为 trace_id join 成功**本身不能证明**
这两份 dump 来自同一次压测。

---

## 3. 合并：merge.py

```bash
python3 merge.py --client client.dump --server server.dump -o ir.json
```

它做四件事：按 `trace_id` 关联两端记录、施加 §10.1 的四条断言、算出 33 个分项、
按两个链路模型各算一遍，写出中间表示。**只依赖 Python 3 标准库**，无 numpy。

| 参数 | 默认 | 什么时候动它 |
|---|---|---|
| `--max-mb` | `12` | base64 体积预算，超了就降采样（见下） |
| `--window-ms` | `1000` | 模型 B 的分箱大小 |
| `--low-concurrency` | 关 | **只在你确知 `outstanding=1` 时才开**，见下 |
| `--stats-only` | — | 只打报告不写文件，快速体检用 |

### 关于 `--low-concurrency`

开启后会施加「所有分项非负」的断言，并**剔除**不满足的记录。

**默认关闭是有意的。** 在真实并发下负分项是预期的批处理噪声，模型 B 下更是探测器。
无条件剔除会删掉最该看的那批请求。默认路径保留它们、统计负值比例，
让页面上的斜纹标记去表达。

### 关于降采样

超过体积预算时，**最慢的 20% 全量保留，其余按步长抽稀** —— 因为你要找的是长尾来源，
尾部保真度比头部密度值钱。

每条记录带一个权重，分位数与均值全部**加权**计算。这一点关系到结论正确性：
不加权的话，表格会把「尾部富集的保留集」当成总体，实测 P99.9 会报成真值的两倍多。
控制台会打印 `downsampling APPLIED` 及具体参数，权重之和**精确等于**原始总体条数。

---

## 4. 渲染：render.py

```bash
python3 render.py --ir ir.json -o latency.html
```

产出单个 HTML 文件，**零外部引用**，断网可开。`--title` 可改页面标题。

页面上有：

- **累积柱状图** —— 每根柱子是一个请求，按 33 个分项堆叠，柱顶即端到端时延。
  可按**端到端时延**或按 **trace_id** 排序。
- **分段开关** —— 每一分项都可单独显示/隐藏。
- **悬浮提示** —— 显示该段的名称与时延值。
- **时延统计表** —— 置顶一行是**本次采样的端到端**均值与 P50/P90/P99/P99.9，
  下面 33 行是各分项的同一组统计量。点击表头排序，默认按 P99 降序；
  端到端那一行固定在最上面，不参与排序 —— 它是下面 33 行分解的那个总量，
  不是与它们并列的第 34 项。表格**跟随 rank 轴缩放** ——
  放大到某一段区间后，表格统计的就是那段区间；也可以一键钉到全量做对照。
- **负分段用斜纹标记**，N/A 用刻度标记。

### 图表的缩放与平移

| 操作 | 效果 |
|---|---|
| 拖拽 | 平移（两个轴都可） |
| `Shift` + 拖拽 | 选中一段 rank 区间放大（**只取横向**，时延刻度不受影响） |
| `Ctrl`/`⌘` + 滚轮 | 缩放 rank 轴，以光标处为锚点 |
| `Shift` + 滚轮 | 缩放时延轴，以光标处为锚点 |
| 图下方 / 图右侧的滑动条 | 平移；滑块长度就是当前可见的比例 |
| `X −` `X +` `Y −` `Y +` | 按钮缩放，以当前视窗中心为锚点 |
| `Reset zoom` | rank 轴回到全量，时延轴回到默认区间 |

**裸滚轮不被图表捕获**，仍然正常滚页面 —— 这一页很长，图又占满宽度，
抢走滚轮会让人被卡在图上出不来。

#### 时延轴是一把固定的尺子

时延轴**只在你主动缩放它时才会变**：平移或缩放 rank 轴永远不会重新贴合它。
所以一根柱子的高度在任何窗口里含义都相同，rank 轴上相隔很远的两根柱子可以直接目测比较。

它的默认区间是**全量的 0 到 P99.9**，不是到最大值。原因是真实采集里常有一两条毫秒级长尾
压着一大片微秒级请求 —— 若按最大值定标，整页柱子会被压成贴底的一行像素
（这份数据：P99.9 是 236 µs，最大值 7.061 ms，相差 30 倍）。

超出默认区间的请求**照画不误**，只是在它跑出去的那条边上标一个小三角（▲/▼）；
`Y −` 可以一路缩出去看它们，缩到全量真实范围为止，**没有任何数据是够不到的**。
它们同样**已经计入下面每一个统计量** —— 时延轴的缩放不影响任何数字：
**缩放 rank 轴会收窄统计表**（表格设为跟随缩放时），**缩放时延轴不会**。

> 请求数很多时，一列会聚合多个请求（画其中最慢的那条作为包络），
> 页面上会标出聚合比例，并画一条逐列中位线，免得把包络误读成典型值。

---

## 5. 怎么确认这次跑对了

**这一步比前面任何一步都重要。** 这个工具最危险的失效模式不是崩溃，而是
**产出一张自洽、漂亮、但错误的图** —— 你会据此去追一个不存在的问题。

`merge.py` 的控制台输出就是验收单，逐行核对：

```
client records: 3222 (freq=100000049.543 Hz)
server records: 3222 (freq=100000023.194 Hz)
join: 3222 joined; client ids=3222 (rate=100.0000%), server ids=3222 (rate=100.0000%), only_client=0, only_server=0
rejects (excluded from statistics, counted by reason -- a record may appear under more than one reason):
  unstamped_point: 0
  non_monotonic: 0
  negative_decomposition_item: 0
  negative_rtt_late_write_end_stamp: 0
accepted: 3222
negative-item records (NOT excluded -- see --low-concurrency): 5 of 3222 accepted (0.1552%)
output records after downsampling: 3222
IR blob: 451080 raw bytes, 601440 base64 bytes (186.67 b64 bytes/record)
identity check: 3222/3222 accepted records pass Sigma(33 items) == C17-C01 exactly
```

上面是本仓库夹具的真实输出，可以直接拿来对照。

| 看什么 | 期望 | 不对时说明什么 |
|---|---|---|
| `join rate` | 接近 100% | 明显偏低 → 两端采集窗口没对齐，或缓冲溢出 |
| `dropped_count` 告警 | **不出现** | 出现 → 数据只覆盖了压测前一段，**加大容量重跑** |
| realtime 重叠告警 | **不出现** | 出现 → 这两份 dump 多半不是同一次压测的 |
| `identity check` | `N/N ... exactly` | 不全过 → 分解算术有问题，图不可信 |
| `rejects` 各项 | 尽量为 0 | 非零不一定是错，但要知道排除了多少 |
| `downsampling APPLIED` | 视情况 | 出现是正常的，确认权重和等于原始条数即可 |

⚠️ **`identity check` 全过并不等于打点位置正确。** 那条恒等式是**望远镜求和**
（相邻点位两两相减再加总，中间项全部消掉），它对任何数值都成立 —— 包括全零。
它证明的是 `merge.py` 的算术，**不是**埋点位置对不对。真正能发现埋点问题的是
真实数据下的异常分布，不是这条断言。

自检工具链本身：

```bash
python3 test_merge.py && python3 test_render.py     # 42 个测试
```

---

## 6. 已知的坑速查

| 现象 | 原因 | 处置 |
|---|---|---|
| dump 文件不存在或 0 字节 | 用了 SIGTERM | 改用 `kill -INT` |
| 记录数远少于 `capacity` | 旧版分片问题 | 已修；确认用的是当前版本 |
| 实际条数比请求的多 | 每分片向上取 2 的幂 | 正常，保证「不少于」 |
| 页面写着 100% join 却总觉得少 | 缓冲溢出 | 看 `dropped_count` 告警 |
| 图上完全没有链路段 | RDMA 轮询模式 | 已有等价锚点回退，确认用当前版本 |
| 某段柱子恰好 2.147 秒 | 时间戳溢出饱和 | 页面会标为**下界**，真实值更大 |
| CMake 构建出来没有打点 | 开关只支持 Make | 用 `config_brpc.sh --with-latency-trace` |

---

## 相关文档

- `docs/superpowers/specs/2026-08-29-brpc-latency-trace-design.md` —— 设计文档。
  §4 点位定义、§5 分项定义、§6 两个链路模型、§8 文件格式、§9 渲染契约、
  §10.1 **哪些断言真正有分量**。
- `src/brpc/latency_trace.h` —— 格式的最终权威。**代码与文档冲突时以代码为准。**
- `tools/latency_trace/README.md` —— 远端构建机（suzhou950）的用法。
