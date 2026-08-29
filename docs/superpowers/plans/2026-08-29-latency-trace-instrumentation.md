# brpc 请求级时延打点（C++ 侧）实现计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 在 brpc 的 baidu_std 协议路径上插入 36 个请求级时间戳，落盘为可离线合并的 dump 文件，使单次 RPC 的端到端时延可被无损分解为 35 项。

**Architecture:** 新增独立模块 `src/brpc/latency_trace.{h,cpp}`，提供分片环形缓冲与 8 字节 handle。请求内点位经 `Controller` 携带 handle；事件级点位先落 `Socket`、再随 `InputMessageBase` 传递；写完成点位经 `Socket::WriteRequest` 内的 handle 回填。跨进程关联靠 `RpcRequestMeta` 新增的 `latency_trace_id` 字段。全部代码由编译宏 `BRPC_LATENCY_TRACE` 控制，未定义时零改动、零开销。

**Tech Stack:** C++14（brpc 下限）、protobuf 2 语法、gflags、gtest、GNU Make（`config_brpc.sh` 生成 `config.mk`）

**Spec:** `docs/superpowers/specs/2026-08-29-brpc-latency-trace-design.md`

## Global Constraints

- 代码风格：Google C++ Style，**4 空格缩进**（brpc 惯例）
- C++14 下限；不得引入新的第三方依赖
- **未定义 `BRPC_LATENCY_TRACE` 时，编译产物必须与改动前等价**：`BAIDU_CASSERT(sizeof(WriteRequest) == 64, sizeof_write_request_is_64)`（`src/brpc/socket.cpp:2954`）必须仍然成立
- 协议侧代码只允许改 `src/brpc/policy/baidu_rpc_protocol.cpp` 与 `src/brpc/policy/baidu_rpc_meta.proto`；核心文件（`socket.cpp`、`channel.cpp`、`controller.cpp`、`input_messenger.cpp`）的改动必须是协议无关的
- 时间戳一律取 `butil::detail::clock_cycles()`（`src/butil/time.h:217`）的**原始计数值**，不在热路径做任何频率换算
- 点位 ID 常量与 §4 表格一一对应；点位总数 36（客户端 C01–C19，服务端 S01–S17）
- `RpcRequestMeta` 新字段用 **tag 9**（tag 1–8 已占用）
- 新增源文件放在 `src/brpc/` 下即被 Make 与 CMake 自动收录（`Makefile:209` 的 `wildcard`、`CMakeLists.txt:575` 的 `GLOB_RECURSE`），**无需改构建文件**
- 新增单测文件名须匹配 `test/brpc_*unittest.cpp`，即被 `test/Makefile:182` 自动收录
- **编辑在本机，构建与测试在 suzhou950，git 提交在本机。** 每次跑测试前先 `./tools/latency_trace/sync950.sh`（Task 0 产出）
- **suzhou950 上 `config_brpc.sh` 的唯一正确调用**（两个参数都不能少，理由见 Task 0 报告）：
  ```bash
  ./config_brpc.sh --headers="/usr/include/gtest /usr/include" --libs=/usr/lib64
  ```
  `--libs=/usr/lib64`：库在 lib64；写成 `/usr/lib` 时 `find_dir_of_lib_or_die` 的 `exit 1` 只杀子 shell，
  脚本静默继续并生成 `-l -l`（无库名的裸 `-l`），直到链接期才暴露。
  `--headers` 里 `/usr/include/gtest` 必须排在前：该主机装了两份 gtest，且 GCC 会把指向自身默认
  系统目录的显式 `-I` 静默降级到搜索顺序末尾，导致 `<gtest/gtest.h>` 命中 llvm-googletest 那份，
  链接时报 `llvm::raw_ostream` 未定义。
- **提交时只 `git add` 本任务明确列出的文件，禁止 `git add -A` / `git add .` / `git add -u`。**
  仓库工作区里长期存放着用户的 34 个未跟踪个人文档（`brpc_*.md`、`*.html`、`*.pptx`、
  `问题记录.txt` 等），它们是**故意不跟踪**的。Task 2 曾因 `git add -A` 把这 34 个文件
  全部提交进分支（36 files, 33188 insertions），需要 controller 做历史手术回退。
- **所有 `make` 测试目标都要带 `NEED_GPERFTOOLS=0`**：该主机未装 gperftools，而 `test/Makefile:18`
  硬编码 `NEED_GPERFTOOLS=1`，`config_brpc.sh:592` 会因此在 `config.mk` 里生成
  `$(error "Fail to find gperftools")`。见 Task 0 的裁决。
- suzhou950 的 VPN 闲置会断。长任务开始前在后台起保活：`for i in $(seq 1 240); do ssh suzhou950 'date>/dev/null'; sleep 60; done &`

---

## Task 0: 远程构建环境

**所有构建与测试都在 suzhou950 上做**（aarch64，384 核，依赖齐全）。本机（WSL x86_64）缺 protobuf / gflags / leveldb / protoc，只用来编辑代码与持有 git 历史。

已核实 suzhou950 具备：protobuf 头 + `protoc 25.1`、gflags、leveldb、openssl、`infiniband/verbs.h`、gtest、cmake、make、git、g++ 12.3.1（openEuler 24.03 SP3）。**无 rsync**，同步走 `tar | ssh`。全量同步压缩后 2.9 MB，局域网上一两秒，不必做增量。

**Files:**
- Create: `tools/latency_trace/sync950.sh`

**Interfaces:**
- Produces: `tools/latency_trace/sync950.sh` —— 把本机工作树同步到 `suzhou950:~/brpc-lt`；后续所有任务的「跑测试」步骤都以它开头

- [ ] **Step 1: 写同步脚本**

```bash
#!/bin/bash
# tools/latency_trace/sync950.sh
# Push the working tree to the remote build host. Full sync every time:
# the payload is ~3MB compressed, so incremental sync is not worth the
# risk of a stale file silently surviving.
set -euo pipefail

HOST="${LT_BUILD_HOST:-suzhou950}"
DEST="${LT_BUILD_DIR:-~/brpc-lt}"

cd "$(dirname "$0")/../.."

tar czf - \
    --exclude=.git \
    --exclude='*.o' \
    --exclude='*.so*' \
    --exclude='*.a' \
    --exclude='*.pb.cc' \
    --exclude='*.pb.h' \
    src test tools Makefile config_brpc.sh CMakeLists.txt \
  | ssh "$HOST" "mkdir -p $DEST && tar xzf - -C $DEST"

echo "synced to $HOST:$DEST"
```

`chmod +x tools/latency_trace/sync950.sh`

**注意排除 `*.pb.cc` / `*.pb.h`**：本机没有 protoc，若本地残留旧的生成文件会覆盖远端由 protoc 25.1 生成的新文件，导致极难排查的链接错误。

- [ ] **Step 2: 同步并生成 config.mk**

```bash
./tools/latency_trace/sync950.sh
ssh suzhou950 'cd ~/brpc-lt && ./config_brpc.sh --headers="/usr/include/gtest /usr/include" --libs=/usr/lib64 2>&1 | tail -20'
```

Expected: 生成 `config.mk` 与 `src/butil/config.h`，无 "Fail to find" 报错

- [ ] **Step 3: 验证基线可构建 —— 本任务的头号风险**

```bash
ssh suzhou950 'cd ~/brpc-lt && make -j64 2>&1 | tail -40'
```

Expected: 产出 `libbrpc.a` 与 `libbrpc.so`。

**若失败：** 最可能的原因是 `protoc 25.1`（protobuf 4.25）超出 `CLAUDE.md` 声明的支持范围 3.x–21.x，且 protobuf 25 的 abseil 头要求 C++17 而 brpc 默认 `-std=c++11/14`。已知的应对顺序：

1. 在 `config.mk` 的 `CXXFLAGS` 里把 `-std=c++11` 或 `-std=c++14` 改成 `-std=c++17`
2. 若仍报 abseil 相关的缺失符号，检查 `config_brpc.sh` 是否把 `-labsl_*` 系列链接进来
3. 若两步都不行，**停下来报告**，不要绕过 —— 换 protobuf 版本是影响全局的决定，需要用户拍板

- [ ] **Step 4: 验证测试工具链可用**

```bash
ssh suzhou950 'cd ~/brpc-lt/test && make NEED_GPERFTOOLS=0 brpc_controller_unittest -j64 2>&1 | tail -20 && ./brpc_controller_unittest 2>&1 | tail -5'
```

Expected: 编译通过且全部 PASS。这一步确认 gtest 链接正常、`test/Makefile` 的 `$(wildcard brpc_*unittest.cpp)` 机制可用，后续任务的 TDD 循环才有意义。

- [ ] **Step 5: 记录一条可复用的构建/测试命令**

把下面这行写进 `tools/latency_trace/README.md`，后续任务全部照抄：

```bash
./tools/latency_trace/sync950.sh && ssh suzhou950 'cd ~/brpc-lt/test && make NEED_GPERFTOOLS=0 brpc_latency_trace_unittest -j64 && ./brpc_latency_trace_unittest'
```

- [ ] **Step 6: Commit**

```bash
git add tools/latency_trace/sync950.sh tools/latency_trace/README.md
git commit -m "build: add a sync script for the suzhou950 remote build host"
```

**后续所有任务的说明：** 计划中凡写作 `cd test && make ... && ./...` 的命令，一律替换为上面 Step 5 的远程形式。编辑在本机做，构建与运行在 suzhou950 上做，git 提交在本机做。

---

## Task 1: Phase 0 — 打点成本 microbenchmark

**独立程序，不依赖 brpc**，因此可在缺依赖的 suzhou920B 上直接编译运行。这是 spec 的 D13：拿到实测数字后才能决定最终点位集。

**Files:**
- Create: `tools/latency_trace/probe/stamp_cost_probe.cpp`

**Interfaces:**
- Produces: 无代码接口。产出一份实测数据，写入 `docs/superpowers/plans/phase0-results.md`，供后续决定是否需要 `lite` 点位集。

- [ ] **Step 1: 写 probe 程序**

```cpp
// tools/latency_trace/probe/stamp_cost_probe.cpp
// Standalone. Build: g++ -O2 -std=c++14 stamp_cost_probe.cpp -o stamp_cost_probe
#include <cstdio>
#include <cstdint>
#include <cstring>
#include <ctime>
#include <vector>
#include <algorithm>

static inline uint64_t clock_cycles() {
#if defined(__aarch64__)
    uint64_t v;
    asm volatile("mrs %0, cntvct_el0" : "=r"(v));
    return v;
#elif defined(__x86_64__) || defined(__amd64__)
    unsigned lo, hi;
    __asm__ __volatile__("rdtsc" : "=a"(lo), "=d"(hi));
    return ((uint64_t)hi << 32) | lo;
#else
#error "unsupported arch"
#endif
}

static inline uint64_t cntfrq_hz() {
#if defined(__aarch64__)
    uint64_t v;
    asm volatile("mrs %0, cntfrq_el0" : "=r"(v));
    return v;
#else
    return 0;
#endif
}

static int64_t realtime_ns() {
    timespec ts;
    clock_gettime(CLOCK_REALTIME, &ts);
    return ts.tv_sec * 1000000000LL + ts.tv_nsec;
}

// Mimics LT_STAMP: bounds-checked generation compare, then one store.
struct Slot {
    uint64_t slot_seq;
    uint32_t ts[36];
};

int main() {
    const int N = 2000000;

    // 1. Empirical counter frequency, from a 200ms window.
    const uint64_t c0 = clock_cycles();
    const int64_t  r0 = realtime_ns();
    timespec nap = {0, 200000000};
    nanosleep(&nap, nullptr);
    const uint64_t c1 = clock_cycles();
    const int64_t  r1 = realtime_ns();
    const double freq = (double)(c1 - c0) * 1e9 / (double)(r1 - r0);
    printf("counter freq (empirical) = %.3f MHz\n", freq / 1e6);
    printf("counter freq (CNTFRQ_EL0) = %.3f MHz%s\n",
           cntfrq_hz() / 1e6, cntfrq_hz() ? "" : "  (n/a on this arch)");
    if (cntfrq_hz()) {
        const double d = (freq - (double)cntfrq_hz()) / (double)cntfrq_hz();
        printf("  relative deviation = %.4f%%\n", d * 100.0);
    }
    printf("counter resolution = %.2f ns/tick\n", 1e9 / freq);

    std::vector<Slot> slots(4096);
    memset(slots.data(), 0, slots.size() * sizeof(Slot));
    for (size_t i = 0; i < slots.size(); ++i) {
        slots[i].slot_seq = i;
    }

    // 2. Raw clock_cycles() only.
    int64_t t0 = realtime_ns();
    uint64_t sink = 0;
    for (int i = 0; i < N; ++i) {
        sink += clock_cycles();
    }
    int64_t t1 = realtime_ns();
    printf("clock_cycles()            = %.2f ns/op\n", (double)(t1 - t0) / N);

    // 3. clock_gettime(CLOCK_MONOTONIC) -- what brpc's cpuwide_time_ns()
    //    degrades to when BUTIL_USE_CPU_FREQUENCY=0 (the default).
    t0 = realtime_ns();
    for (int i = 0; i < N; ++i) {
        timespec ts;
        clock_gettime(CLOCK_MONOTONIC, &ts);
        sink += ts.tv_nsec;
    }
    t1 = realtime_ns();
    printf("clock_gettime(MONOTONIC)  = %.2f ns/op\n", (double)(t1 - t0) / N);

    // 4. Full LT_STAMP equivalent: counter read + generation check + store.
    const uint64_t base = clock_cycles();
    t0 = realtime_ns();
    for (int i = 0; i < N; ++i) {
        const uint64_t seq = (uint64_t)(i & 4095);
        Slot& s = slots[seq];
        if (s.slot_seq == seq) {
            s.ts[i % 36] = (uint32_t)(clock_cycles() - base);
        }
    }
    t1 = realtime_ns();
    const double per_stamp = (double)(t1 - t0) / N;
    printf("LT_STAMP equivalent       = %.2f ns/op\n", per_stamp);
    printf("=> 36 points cost         = %.2f ns (%.3f us)\n",
           per_stamp * 36, per_stamp * 36 / 1000.0);

    printf("sink=%llu\n", (unsigned long long)sink);  // defeat DCE
    return 0;
}
```

- [ ] **Step 2: 本机跑一遍确认程序正确**

```bash
g++ -O2 -std=c++14 tools/latency_trace/probe/stamp_cost_probe.cpp -o /tmp/stamp_cost_probe && /tmp/stamp_cost_probe
```

Expected: 打印出频率与四组 ns/op 数字，无崩溃。本机为 x86_64，`CNTFRQ_EL0` 一栏显示 `(n/a on this arch)`。这一步只验证程序本身，数字不作数。

- [ ] **Step 3: 在 aarch64 机器上跑**

```bash
for h in suzhou950 suzhou920B; do
  echo "===== $h ====="
  scp tools/latency_trace/probe/stamp_cost_probe.cpp $h:~/
  ssh $h 'g++ -O2 -std=c++14 ~/stamp_cost_probe.cpp -o ~/stamp_cost_probe && ~/stamp_cost_probe'
done
```

**两台都要跑，并比对计数器频率是否一致** —— 这是 D1 公式正确性的前提（见 spec §8.1）。两台的 `BogoMIPS` 都是 200.00，即 `CNTFRQ_EL0` 标称均为 100 MHz；本步骤要确认**经验测得的频率**也一致（相对偏差 < 0.1%）。若不一致，`merge.py` 必须分别使用各自的频率，且这一点要在 `phase0-results.md` 中显著标注。

- [ ] **Step 4: 记录结果并决定点位集**

创建 `docs/superpowers/plans/phase0-results.md`，记录两台机器的：计数器频率（经验值与 `CNTFRQ_EL0`、偏差）、四组 ns/op、以及推算的 36 点总开销。

判据：若 `36 点总开销 > RDMA 端到端时延的 10%`（即 > 0.5~1.5μs），则 spec §8.1 的 `lite` 点位集（约 12 点）必须实现，**追加 Task 14**（见文末「条件任务」）；否则 `-latency_trace_point_set` 这个 gflag 整体不实现，从 spec §8.1 的表中删去。

**把结论明确写进 `phase0-results.md` 的首行**，格式为 `DECISION: lite-point-set = REQUIRED` 或 `= NOT_REQUIRED`，后续任务据此判断。

- [ ] **Step 5: Commit**

```bash
git add tools/latency_trace/probe/stamp_cost_probe.cpp docs/superpowers/plans/phase0-results.md
git commit -m "bench: measure per-stamp cost and counter frequency on aarch64"
```

---

## Task 2: 点位 ID 与记录结构

**Files:**
- Create: `src/brpc/latency_trace.h`
- Test: `test/brpc_latency_trace_unittest.cpp`

**Interfaces:**
- Produces:
  - `enum LatencyTracePoint`，成员 `LT_C_RPC_START` … `LT_S_WRITE_END`，以及 `LT_POINT_COUNT = 36`
  - `struct LatencyTraceRecord`，`sizeof == 200`
  - `typedef uint64_t LatencyTraceHandle;`，`LT_INVALID_HANDLE == 0`
  - `enum LatencyTraceRole { LT_ROLE_CLIENT = 0, LT_ROLE_SERVER = 1 };`

- [ ] **Step 1: 写失败的测试**

```cpp
// test/brpc_latency_trace_unittest.cpp
#include <gtest/gtest.h>
#include "brpc/latency_trace.h"

namespace {

TEST(LatencyTraceTest, PointEnumHasExactly36Members) {
    ASSERT_EQ(36, brpc::LT_POINT_COUNT);
    // 客户端 19 个，服务端 17 个，且两段不重叠。
    ASSERT_EQ(0,  brpc::LT_C_RPC_START);
    ASSERT_EQ(18, brpc::LT_C_RPC_END);
    ASSERT_EQ(19, brpc::LT_S_WAKE);
    ASSERT_EQ(35, brpc::LT_S_WRITE_END);
}

TEST(LatencyTraceTest, RecordIsExactly200Bytes) {
    // 定长 POD。大小变化会直接改变 dump 文件格式，必须显式感知。
    ASSERT_EQ(200u, sizeof(brpc::LatencyTraceRecord));
    ASSERT_TRUE(std::is_trivially_copyable<brpc::LatencyTraceRecord>::value);
}

}  // namespace
```

- [ ] **Step 2: 跑测试确认失败**

```bash
./tools/latency_trace/sync950.sh && ssh suzhou950 'cd ~/brpc-lt/test && make NEED_GPERFTOOLS=0 brpc_latency_trace_unittest -j64'
```

Expected: 编译失败，`brpc/latency_trace.h: No such file or directory`

- [ ] **Step 3: 写最小实现**

```cpp
// src/brpc/latency_trace.h
// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#ifndef BRPC_LATENCY_TRACE_H
#define BRPC_LATENCY_TRACE_H

#include <stdint.h>
#include <type_traits>

namespace brpc {

// Timestamp points of one RPC. Order matches the timeline; the analysis
// tool treats every adjacent pair as one decomposition item, so the order
// here IS the decomposition and must stay monotonic in wall-clock terms.
// See docs/superpowers/specs/2026-08-29-brpc-latency-trace-design.md sec.4
enum LatencyTracePoint {
    // ---- client, 19 points ----
    LT_C_RPC_START = 0,
    LT_C_REQ_PAYLOAD_SER_START,
    LT_C_REQ_PAYLOAD_SER_END,
    LT_C_REQ_META_SER_START,
    LT_C_REQ_META_SER_END,
    LT_C_WRITE_ENQUEUE,
    LT_C_WRITE_START,
    LT_C_WRITE_END,
    LT_C_WAKE,
    LT_C_ONEDGE_START,
    LT_C_READV_START,
    LT_C_MSG_RECV_DONE,
    LT_C_RSP_META_DESER_START,
    LT_C_RSP_META_DESER_END,
    LT_C_RSP_PAYLOAD_DESER_START,
    LT_C_RSP_PAYLOAD_DESER_END,
    LT_C_RSP_PROCESS_START,
    LT_C_RSP_PROCESS_END,
    LT_C_RPC_END,
    // ---- server, 17 points ----
    LT_S_WAKE,
    LT_S_ONEDGE_START,
    LT_S_READV_START,
    LT_S_MSG_RECV_DONE,
    LT_S_REQ_META_DESER_START,
    LT_S_REQ_META_DESER_END,
    LT_S_REQ_PAYLOAD_DESER_START,
    LT_S_REQ_PAYLOAD_DESER_END,
    LT_S_SERVICE_START,
    LT_S_SERVICE_END,
    LT_S_RSP_PAYLOAD_SER_START,
    LT_S_RSP_PAYLOAD_SER_END,
    LT_S_RSP_META_SER_START,
    LT_S_RSP_META_SER_END,
    LT_S_WRITE_ENQUEUE,
    LT_S_WRITE_START,
    LT_S_WRITE_END,

    LT_POINT_COUNT
};

enum LatencyTraceRole {
    LT_ROLE_CLIENT = 0,
    LT_ROLE_SERVER = 1
};

// Opaque reference to one slot in the ring buffer.
// Layout: (shard << 56) | seq. Zero means "not tracing".
typedef uint64_t LatencyTraceHandle;
const LatencyTraceHandle LT_INVALID_HANDLE = 0;

// Fixed-size POD, 200 bytes. Timestamps are raw counter deltas relative
// to `base_counter`; conversion to nanoseconds happens offline.
struct LatencyTraceRecord {
    uint64_t trace_id;       // unique across processes
    uint64_t base_counter;   // raw counter at slot allocation
    uint64_t slot_seq;       // generation guard, see LatencyTraceBuffer
    uint32_t ts[LT_POINT_COUNT];  // 0 == not stamped
    uint32_t socket_id;
    uint32_t remote_ip;
    uint32_t req_size;
    uint32_t rsp_size;
    uint32_t method_id;
    int32_t  error_code;
    uint16_t remote_port;
    uint8_t  role;           // LatencyTraceRole
    uint8_t  attempt;        // retry / backup-request index
};

}  // namespace brpc

#endif  // BRPC_LATENCY_TRACE_H
```

- [ ] **Step 4: 跑测试确认通过**

```bash
./tools/latency_trace/sync950.sh && ssh suzhou950 'cd ~/brpc-lt/test && make NEED_GPERFTOOLS=0 brpc_latency_trace_unittest -j64 && ./brpc_latency_trace_unittest'
```

Expected: 2 tests PASS。若 `sizeof` 不是 200，**不要改断言去迁就**，先核对字段：`8+8+8 + 36*4 + 4*5 + 4 + 2 + 1 + 1 = 196`，8 字节对齐补到 200。

- [ ] **Step 5: Commit**

```bash
git add src/brpc/latency_trace.h test/brpc_latency_trace_unittest.cpp
git commit -m "feat(latency-trace): define the 36 trace points and the record layout"
```

---

## Task 3: 分片环形缓冲

**Files:**
- Create: `src/brpc/latency_trace.cpp`
- Modify: `src/brpc/latency_trace.h`
- Test: `test/brpc_latency_trace_unittest.cpp`

**Interfaces:**
- Consumes: Task 2 的 `LatencyTraceRecord`、`LatencyTraceHandle`、`LT_POINT_COUNT`
- Produces:
  - `class LatencyTraceBuffer`
  - `static LatencyTraceBuffer* LatencyTraceBuffer::instance();`
  - `LatencyTraceHandle AllocSlot(uint64_t trace_id, LatencyTraceRole role);`
  - `void Stamp(LatencyTraceHandle h, int point);`
  - `LatencyTraceRecord* GetForTest(LatencyTraceHandle h);`
  - `const LatencyTraceRecord* GetBySeqForTest(uint64_t global_seq) const;` —— 按全局序号遍历所有分片中已写入的槽，越界返回 `nullptr`；Task 8 起的端到端测试靠它找记录
  - `size_t recorded_count() const;` / `size_t dropped_count() const;`
  - gflags: `-latency_trace_enabled`（默认 false）、`-latency_trace_capacity`（默认 100000）

- [ ] **Step 1: 写失败的测试**

追加到 `test/brpc_latency_trace_unittest.cpp`：

```cpp
#include <gflags/gflags.h>

DECLARE_bool(latency_trace_enabled);
DECLARE_int32(latency_trace_capacity);

namespace {

class LatencyTraceBufferTest : public ::testing::Test {
protected:
    void SetUp() override {
        FLAGS_latency_trace_enabled = true;
        brpc::LatencyTraceBuffer::instance()->ResetForTest(64);
    }
    void TearDown() override {
        FLAGS_latency_trace_enabled = false;
    }
};

TEST_F(LatencyTraceBufferTest, DisabledReturnsInvalidHandle) {
    FLAGS_latency_trace_enabled = false;
    ASSERT_EQ(brpc::LT_INVALID_HANDLE,
              brpc::LatencyTraceBuffer::instance()->AllocSlot(1, brpc::LT_ROLE_CLIENT));
}

TEST_F(LatencyTraceBufferTest, AllocReturnsDistinctHandles) {
    brpc::LatencyTraceBuffer* b = brpc::LatencyTraceBuffer::instance();
    const brpc::LatencyTraceHandle h1 = b->AllocSlot(111, brpc::LT_ROLE_CLIENT);
    const brpc::LatencyTraceHandle h2 = b->AllocSlot(222, brpc::LT_ROLE_CLIENT);
    ASSERT_NE(brpc::LT_INVALID_HANDLE, h1);
    ASSERT_NE(brpc::LT_INVALID_HANDLE, h2);
    ASSERT_NE(h1, h2);
    ASSERT_EQ(111u, b->GetForTest(h1)->trace_id);
    ASSERT_EQ(222u, b->GetForTest(h2)->trace_id);
}

TEST_F(LatencyTraceBufferTest, StampWritesNonZeroDelta) {
    brpc::LatencyTraceBuffer* b = brpc::LatencyTraceBuffer::instance();
    const brpc::LatencyTraceHandle h = b->AllocSlot(7, brpc::LT_ROLE_CLIENT);
    ASSERT_EQ(0u, b->GetForTest(h)->ts[brpc::LT_C_WRITE_END]);
    b->Stamp(h, brpc::LT_C_WRITE_END);
    // The delta may legitimately be 0 if the counter has not ticked, so
    // assert on a point that is stamped after a bounded busy-wait instead.
    const uint64_t start = butil::detail::clock_cycles();
    while (butil::detail::clock_cycles() - start < 1000) {}
    b->Stamp(h, brpc::LT_C_RPC_END);
    ASSERT_GT(b->GetForTest(h)->ts[brpc::LT_C_RPC_END], 0u);
}

TEST_F(LatencyTraceBufferTest, StaleHandleIsRejectedAfterWrapAround) {
    // Capacity 64. Allocating 64 more slots must recycle the first one and
    // invalidate its handle, so a late backfill cannot corrupt a live slot.
    brpc::LatencyTraceBuffer* b = brpc::LatencyTraceBuffer::instance();
    const brpc::LatencyTraceHandle stale = b->AllocSlot(1, brpc::LT_ROLE_CLIENT);
    ASSERT_NE(brpc::LT_INVALID_HANDLE, stale);
    for (int i = 0; i < 64; ++i) {
        b->AllocSlot(1000 + i, brpc::LT_ROLE_CLIENT);
    }
    ASSERT_EQ(nullptr, b->GetForTest(stale));   // handle no longer valid
    b->Stamp(stale, brpc::LT_C_RPC_END);        // must be a silent no-op
}

TEST_F(LatencyTraceBufferTest, StopsRecordingWhenFullInsteadOfOverwriting) {
    // Spec D-decision: a full buffer stops recording rather than wrapping,
    // so both ends keep the SAME earliest-N window and stay joinable.
    brpc::LatencyTraceBuffer* b = brpc::LatencyTraceBuffer::instance();
    // Capacity is per-shard after division, and one thread only ever uses
    // one shard -- so ask for SHARD_COUNT*8 to get 8 usable slots here.
    b->ResetForTest(brpc::LatencyTraceBuffer::SHARD_COUNT * 8);
    b->set_stop_when_full(true);
    for (int i = 0; i < 8; ++i) {
        ASSERT_NE(brpc::LT_INVALID_HANDLE, b->AllocSlot(i, brpc::LT_ROLE_CLIENT));
    }
    ASSERT_EQ(brpc::LT_INVALID_HANDLE, b->AllocSlot(999, brpc::LT_ROLE_CLIENT));
    ASSERT_EQ(8u, b->recorded_count());
    ASSERT_EQ(1u, b->dropped_count());
}

}  // namespace
```

在文件顶部补 `#include "butil/time.h"`。

- [ ] **Step 2: 跑测试确认失败**

```bash
./tools/latency_trace/sync950.sh && ssh suzhou950 'cd ~/brpc-lt/test && make NEED_GPERFTOOLS=0 brpc_latency_trace_unittest -j64'
```

Expected: 编译失败，`'LatencyTraceBuffer' is not a member of 'brpc'`

- [ ] **Step 3: 写实现**

追加到 `src/brpc/latency_trace.h`（`}  // namespace brpc` 之前）：

```cpp
// Sharded ring buffer of records. One shard per group of workers keeps
// the allocation cursor off a single contended cacheline.
class LatencyTraceBuffer {
public:
    static const int SHARD_COUNT = 16;   // power of two
    static const int SHARD_SHIFT = 56;

    static LatencyTraceBuffer* instance();

    // Returns LT_INVALID_HANDLE when tracing is off or the buffer is full.
    LatencyTraceHandle AllocSlot(uint64_t trace_id, LatencyTraceRole role);

    // Silently ignores an invalid or stale handle.
    void Stamp(LatencyTraceHandle h, int point);

    // Returns nullptr when the handle is stale.
    LatencyTraceRecord* Get(LatencyTraceHandle h);
    LatencyTraceRecord* GetForTest(LatencyTraceHandle h) { return Get(h); }

    // Iterates every written slot across all shards by a flat index, so a
    // test can find records without knowing the sharding. Returns nullptr
    // once `global_seq` is past the last written slot.
    const LatencyTraceRecord* GetBySeqForTest(uint64_t global_seq) const;

    size_t recorded_count() const;
    size_t dropped_count() const;

    void set_stop_when_full(bool v) { _stop_when_full = v; }

    // Test-only: re-create the buffer with `capacity` slots in total.
    void ResetForTest(int capacity);

private:
    LatencyTraceBuffer();
    ~LatencyTraceBuffer();

    struct Shard {
        butil::atomic<uint64_t> cursor;
        LatencyTraceRecord* records;
        uint64_t mask;
        char padding[BAIDU_CACHELINE_SIZE];
    };

    Shard _shards[SHARD_COUNT];
    butil::atomic<uint64_t> _dropped;
    bool _stop_when_full;
    int _per_shard_capacity;
};
```

在头部补 `#include "butil/atomicops.h"` 与 `#include "butil/compiler_specific.h"`（`BAIDU_CACHELINE_SIZE` 定义在 `compiler_specific.h:301`，**不在** `macros.h`）。

```cpp
// src/brpc/latency_trace.cpp  (Apache 头略，照抄 latency_trace.h 的)
#include "brpc/latency_trace.h"

#include <gflags/gflags.h>
#include <new>
#include "butil/time.h"
#include "butil/logging.h"

namespace brpc {

DEFINE_bool(latency_trace_enabled, false,
            "Record per-request latency trace points. Requires the library "
            "to be built with BRPC_LATENCY_TRACE.");
DEFINE_int32(latency_trace_capacity, 100000,
             "Total number of latency trace records held in memory");

static uint64_t NextPowerOfTwo(uint64_t v) {
    uint64_t r = 1;
    while (r < v) {
        r <<= 1;
    }
    return r;
}

LatencyTraceBuffer::LatencyTraceBuffer()
    : _dropped(0), _stop_when_full(true), _per_shard_capacity(0) {
    ResetForTest(FLAGS_latency_trace_capacity);
}

LatencyTraceBuffer::~LatencyTraceBuffer() {
    for (int i = 0; i < SHARD_COUNT; ++i) {
        delete[] _shards[i].records;
        _shards[i].records = nullptr;
    }
}

void LatencyTraceBuffer::ResetForTest(int capacity) {
    const uint64_t per_shard =
        NextPowerOfTwo((capacity + SHARD_COUNT - 1) / SHARD_COUNT);
    _per_shard_capacity = (int)per_shard;
    for (int i = 0; i < SHARD_COUNT; ++i) {
        delete[] _shards[i].records;
        _shards[i].records = new LatencyTraceRecord[per_shard];
        memset(_shards[i].records, 0,
               sizeof(LatencyTraceRecord) * per_shard);
        // slot_seq starts at UINT64_MAX so that no live handle (whose seq
        // starts at 0) can ever match an untouched slot.
        for (uint64_t j = 0; j < per_shard; ++j) {
            _shards[i].records[j].slot_seq = UINT64_MAX;
        }
        _shards[i].mask = per_shard - 1;
        _shards[i].cursor.store(0, butil::memory_order_relaxed);
    }
    _dropped.store(0, butil::memory_order_relaxed);
}

LatencyTraceBuffer* LatencyTraceBuffer::instance() {
    static LatencyTraceBuffer* s = new LatencyTraceBuffer;
    return s;
}

LatencyTraceHandle LatencyTraceBuffer::AllocSlot(uint64_t trace_id,
                                                 LatencyTraceRole role) {
    if (!FLAGS_latency_trace_enabled) {
        return LT_INVALID_HANDLE;
    }
    // One shard per thread, assigned round-robin on first use. Do NOT
    // derive this from a stack address: the address of a parameter is
    // effectively constant within a thread, which happens to give the
    // right answer while looking like it hashes something.
    static butil::atomic<int> s_next_shard(0);
    static __thread int tls_shard = -1;
    if (tls_shard < 0) {
        tls_shard = s_next_shard.fetch_add(1, butil::memory_order_relaxed)
                    & (SHARD_COUNT - 1);
    }
    const int shard = tls_shard;
    Shard& sh = _shards[shard];
    const uint64_t seq = sh.cursor.fetch_add(1, butil::memory_order_relaxed);
    if (_stop_when_full && seq >= (uint64_t)_per_shard_capacity) {
        _dropped.fetch_add(1, butil::memory_order_relaxed);
        return LT_INVALID_HANDLE;
    }
    LatencyTraceRecord* r = &sh.records[seq & sh.mask];
    memset(r->ts, 0, sizeof(r->ts));
    r->trace_id = trace_id;
    r->base_counter = butil::detail::clock_cycles();
    r->role = (uint8_t)role;
    r->attempt = 0;
    r->slot_seq = seq;
    // Handle carries seq; slot_seq is the generation guard.
    return ((uint64_t)(shard + 1) << SHARD_SHIFT) | (seq & 0x00FFFFFFFFFFFFFFULL);
}

LatencyTraceRecord* LatencyTraceBuffer::Get(LatencyTraceHandle h) {
    if (h == LT_INVALID_HANDLE) {
        return nullptr;
    }
    const int shard = (int)((h >> SHARD_SHIFT) & 0xFF) - 1;
    if (shard < 0 || shard >= SHARD_COUNT) {
        return nullptr;
    }
    Shard& sh = _shards[shard];
    const uint64_t seq = h & 0x00FFFFFFFFFFFFFFULL;
    LatencyTraceRecord* r = &sh.records[seq & sh.mask];
    if (r->slot_seq != seq) {
        return nullptr;   // recycled; the handle is stale
    }
    return r;
}

void LatencyTraceBuffer::Stamp(LatencyTraceHandle h, int point) {
    LatencyTraceRecord* r = Get(h);
    if (r == nullptr || point < 0 || point >= LT_POINT_COUNT) {
        return;
    }
    r->ts[point] = (uint32_t)(butil::detail::clock_cycles() - r->base_counter);
}

size_t LatencyTraceBuffer::recorded_count() const {
    size_t n = 0;
    for (int i = 0; i < SHARD_COUNT; ++i) {
        uint64_t c = _shards[i].cursor.load(butil::memory_order_relaxed);
        n += (c > (uint64_t)_per_shard_capacity) ? _per_shard_capacity : c;
    }
    return n;
}

size_t LatencyTraceBuffer::dropped_count() const {
    return _dropped.load(butil::memory_order_relaxed);
}

}  // namespace brpc
```

**容量语义：`ResetForTest(n)` 的 `n` 是总容量，除以 `SHARD_COUNT` 后向上取 2 的幂得到每分片容量。单线程只用一个分片，所以测试要拿到 8 个可用槽必须传 `SHARD_COUNT * 8`。** 不要为了让测试通过而弱化「满了停止」的语义。

- [ ] **Step 4: 跑测试确认通过**

```bash
./tools/latency_trace/sync950.sh && ssh suzhou950 'cd ~/brpc-lt/test && make NEED_GPERFTOOLS=0 brpc_latency_trace_unittest -j64 && ./brpc_latency_trace_unittest'
```

Expected: 7 tests PASS

- [ ] **Step 5: Commit**

```bash
git add src/brpc/latency_trace.h src/brpc/latency_trace.cpp test/brpc_latency_trace_unittest.cpp
git commit -m "feat(latency-trace): sharded ring buffer with generation-guarded handles"
```

---

## Task 4: `LT_STAMP` 宏与编译开关

**Files:**
- Modify: `src/brpc/latency_trace.h`
- Test: `test/brpc_latency_trace_unittest.cpp`

**Interfaces:**
- Produces:
  - 宏 `LT_STAMP(handle, point)`
  - 宏 `LT_ALLOC(trace_id, role)`（求值为 `LatencyTraceHandle`）
  - 未定义 `BRPC_LATENCY_TRACE` 时二者展开为空语句 / `LT_INVALID_HANDLE`

- [ ] **Step 1: 写失败的测试**

```cpp
TEST(LatencyTraceMacroTest, StampCompilesAndIsNoOpWhenHandleInvalid) {
    // Must be safe to call with an invalid handle from any thread.
    LT_STAMP(brpc::LT_INVALID_HANDLE, brpc::LT_C_RPC_START);
    SUCCEED();
}

TEST(LatencyTraceMacroTest, StampRecordsWhenEnabled) {
    FLAGS_latency_trace_enabled = true;
    brpc::LatencyTraceBuffer::instance()->ResetForTest(64);
    const brpc::LatencyTraceHandle h = LT_ALLOC(42, brpc::LT_ROLE_CLIENT);
#if defined(BRPC_LATENCY_TRACE)
    ASSERT_NE(brpc::LT_INVALID_HANDLE, h);
    const uint64_t start = butil::detail::clock_cycles();
    while (butil::detail::clock_cycles() - start < 1000) {}
    LT_STAMP(h, brpc::LT_C_RPC_END);
    ASSERT_GT(brpc::LatencyTraceBuffer::instance()->GetForTest(h)
                  ->ts[brpc::LT_C_RPC_END], 0u);
#else
    ASSERT_EQ(brpc::LT_INVALID_HANDLE, h);
#endif
    FLAGS_latency_trace_enabled = false;
}
```

- [ ] **Step 2: 跑测试确认失败**

```bash
./tools/latency_trace/sync950.sh && ssh suzhou950 'cd ~/brpc-lt/test && make NEED_GPERFTOOLS=0 brpc_latency_trace_unittest -j64'
```

Expected: 编译失败，`'LT_STAMP' was not declared`

- [ ] **Step 3: 写实现**

追加到 `src/brpc/latency_trace.h` 末尾（`#endif` 之前）：

```cpp
// Compile-time switch. When BRPC_LATENCY_TRACE is not defined, every hook
// vanishes and the library is byte-for-byte equivalent to an untraced
// build -- including sizeof(Socket::WriteRequest) == 64.
#if defined(BRPC_LATENCY_TRACE)
#define LT_STAMP(handle, point)                                    \
    ::brpc::LatencyTraceBuffer::instance()->Stamp((handle), (point))
#define LT_ALLOC(trace_id, role)                                   \
    ::brpc::LatencyTraceBuffer::instance()->AllocSlot((trace_id), (role))
#else
#define LT_STAMP(handle, point) ((void)0)
#define LT_ALLOC(trace_id, role) (::brpc::LT_INVALID_HANDLE)
#endif
```

在 `config_brpc.sh` 增加 `--with-latency-trace` 选项：`Makefile` 侧照 `WITH_GLOG` 的写法（`config_brpc.sh:70` 定义默认值、`:97` 解析参数、`:484` 拼进 `CPPFLAGS`），加 `-DBRPC_LATENCY_TRACE=1`。

- [ ] **Step 4: 两种构建各跑一次**

```bash
./tools/latency_trace/sync950.sh

# 默认构建：LT_STAMP 展开为空语句，测试走 #else 分支
ssh suzhou950 'cd ~/brpc-lt && ./config_brpc.sh --headers="/usr/include/gtest /usr/include" --libs=/usr/lib64 && make -j64 \
  && cd test && make NEED_GPERFTOOLS=0 -B brpc_latency_trace_unittest -j64 && ./brpc_latency_trace_unittest'

# 追踪构建：测试走 #if 分支
ssh suzhou950 'cd ~/brpc-lt && ./config_brpc.sh --headers="/usr/include/gtest /usr/include" --libs=/usr/lib64 --with-latency-trace && make -j64 \
  && cd test && make NEED_GPERFTOOLS=0 -B brpc_latency_trace_unittest -j64 && ./brpc_latency_trace_unittest'
```

Expected: 两次都全 PASS，且 `#else` 分支与 `#if` 分支各被覆盖一次。

- [ ] **Step 5: Commit**

```bash
git add src/brpc/latency_trace.h test/brpc_latency_trace_unittest.cpp config_brpc.sh
git commit -m "feat(latency-trace): add LT_STAMP/LT_ALLOC behind BRPC_LATENCY_TRACE"
```

---

## Task 5: dump 文件格式与时钟标定

**Files:**
- Modify: `src/brpc/latency_trace.h`, `src/brpc/latency_trace.cpp`
- Test: `test/brpc_latency_trace_unittest.cpp`

**Interfaces:**
- Produces:
  - `struct LatencyTraceFileHeader`（固定 128 字节）
  - `int LatencyTraceBuffer::Dump(const char* path);`（成功返回写出的记录数，失败返回 -1）
  - gflag `-latency_trace_dump_path`（默认空，非空则 `atexit` 时自动 dump）

- [ ] **Step 1: 写失败的测试**

```cpp
TEST_F(LatencyTraceBufferTest, DumpRoundTripsHeaderAndRecords) {
    brpc::LatencyTraceBuffer* b = brpc::LatencyTraceBuffer::instance();
    b->ResetForTest(64);
    const brpc::LatencyTraceHandle h = b->AllocSlot(0xABCDEF, brpc::LT_ROLE_CLIENT);
    b->Stamp(h, brpc::LT_C_RPC_START);

    const char* path = "/tmp/brpc_lt_test.bin";
    ASSERT_GE(b->Dump(path), 1);

    FILE* fp = fopen(path, "rb");
    ASSERT_TRUE(fp != nullptr);
    brpc::LatencyTraceFileHeader hdr;
    ASSERT_EQ(1u, fread(&hdr, sizeof(hdr), 1, fp));
    ASSERT_EQ(brpc::LT_FILE_MAGIC, hdr.magic);
    ASSERT_EQ(sizeof(brpc::LatencyTraceRecord), hdr.record_size);
    ASSERT_EQ((uint32_t)brpc::LT_POINT_COUNT, hdr.point_count);
    // Empirical frequency must be a plausible clock rate, not zero.
    ASSERT_GT(hdr.counter_freq_hz, 1000000.0);
    // Head/tail calibration pairs must bracket a positive interval.
    ASSERT_GT(hdr.tail_realtime_ns, hdr.head_realtime_ns);
    ASSERT_GT(hdr.tail_counter, hdr.head_counter);

    brpc::LatencyTraceRecord rec;
    bool found = false;
    while (fread(&rec, sizeof(rec), 1, fp) == 1) {
        if (rec.trace_id == 0xABCDEF) {
            found = true;
            break;
        }
    }
    fclose(fp);
    ASSERT_TRUE(found);
    unlink(path);
}
```

- [ ] **Step 2: 跑测试确认失败**

Run: `./tools/latency_trace/sync950.sh && ssh suzhou950 'cd ~/brpc-lt/test && make NEED_GPERFTOOLS=0 brpc_latency_trace_unittest -j64'`
Expected: 编译失败，`'LatencyTraceFileHeader' is not a member of 'brpc'`

- [ ] **Step 3: 写实现**

追加到 `latency_trace.h`：

```cpp
const uint64_t LT_FILE_MAGIC = 0x4252504C54524331ULL;  // "BRPCLTRC1"

// 128 bytes, fixed. merge.py parses this verbatim.
struct LatencyTraceFileHeader {
    uint64_t magic;
    uint32_t record_size;
    uint32_t point_count;
    // Empirical calibration: freq = (tail_counter - head_counter)
    //                             * 1e9 / (tail_realtime_ns - head_realtime_ns)
    uint64_t head_counter;
    int64_t  head_realtime_ns;
    uint64_t tail_counter;
    int64_t  tail_realtime_ns;
    double   counter_freq_hz;     // authoritative, empirically measured
    uint64_t cntfrq_el0_hz;       // cross-check only; 0 on non-aarch64
    uint64_t record_count;
    uint64_t dropped_count;
    uint64_t process_tag;         // random per process, high bits of trace_id
    uint64_t method_table_offset; // byte offset of the method-name table,
                                  // written by Task 13; 0 until then
    char     padding[128 - 96];
};
```

在 `LatencyTraceBuffer` 中加 `int Dump(const char* path);` 与私有成员 `uint64_t _head_counter; int64_t _head_realtime_ns; uint64_t _process_tag;`，在构造函数中初始化。`Dump` 实现：取尾部标定对，算经验频率，写头，再顺序写出每个分片中 `slot_seq != UINT64_MAX` 的记录。

**标定窗口下限**：经验频率是 `Δcounter / Δrealtime`，窗口太短则商完全是噪声 —— 单测里 `ResetForTest` 之后立刻 `Dump`，两个采样点可能只差几微秒，算出的频率毫无意义。因此 `Dump()` 必须先检查 `tail_realtime_ns - _head_realtime_ns`，**不足 100 ms 时先 `nanosleep` 补足再取尾部采样对**。这在真实的退出时 dump 路径上永远不会触发（进程早跑过 100 ms），只在单测里生效。

`cntfrq_el0_hz` 的读取：

```cpp
static uint64_t ReadCntfrqHz() {
#if defined(__aarch64__)
    uint64_t v;
    asm volatile("mrs %0, cntfrq_el0" : "=r"(v));
    return v;
#else
    return 0;
#endif
}
```

- [ ] **Step 4: 补 atexit 自动 dump 的测试与实现**

```cpp
TEST_F(LatencyTraceBufferTest, DumpPathFlagRegistersAtexitHook) {
    // Dumping on exit is how a benchmark run gets its data without the
    // program having to call anything. Verify the hook is installed, not
    // that atexit fires (gtest cannot observe process exit).
    ASSERT_FALSE(brpc::LatencyTraceBuffer::instance()->atexit_registered());
    brpc::LatencyTraceBuffer::instance()->EnableDumpOnExit("/tmp/brpc_lt_exit.bin");
    ASSERT_TRUE(brpc::LatencyTraceBuffer::instance()->atexit_registered());
}
```

实现 `void EnableDumpOnExit(const char* path)`：保存路径，`atexit` 注册一个调用 `Dump(saved_path)` 的静态函数，用一个 `bool _atexit_registered` 防重复注册。在 `LatencyTraceBuffer` 构造函数末尾，若 `FLAGS_latency_trace_dump_path` 非空则自动调用它。

同时 `DEFINE_string(latency_trace_dump_path, "", "Dump latency trace records to this file at exit; empty disables dumping")`。

- [ ] **Step 5: 跑测试确认通过**

Run: `./tools/latency_trace/sync950.sh && ssh suzhou950 'cd ~/brpc-lt/test && make NEED_GPERFTOOLS=0 brpc_latency_trace_unittest -j64 && ./brpc_latency_trace_unittest'`
Expected: 9 tests PASS

- [ ] **Step 6: Commit**

```bash
git add src/brpc/latency_trace.h src/brpc/latency_trace.cpp test/brpc_latency_trace_unittest.cpp
git commit -m "feat(latency-trace): dump format with empirically calibrated clock rate"
```

---

## Task 6: `WriteRequest` 携带 handle

**Files:**
- Modify: `src/brpc/socket.cpp:310`（`WriteRequest` 定义）、`src/brpc/socket.cpp:2954`（断言）
- Test: `test/brpc_latency_trace_unittest.cpp`

**Interfaces:**
- Produces（均仅在 `BRPC_LATENCY_TRACE` 下存在）:
  - `Socket::WriteRequest::lt_handle`，类型 `LatencyTraceHandle`
  - `Socket::WriteOptions::lt_handle`，类型 `LatencyTraceHandle`，默认 `LT_INVALID_HANDLE`
  - `Socket::WriteOptions::lt_role`，类型 `LatencyTraceRole`，默认 `LT_ROLE_CLIENT`

  `WriteOptions` 的两个字段在本任务一并加上（`src/brpc/socket.h:372` 的 `struct WriteOptions` 及其 `:420` 的构造函数初始化列表），Task 8 与 Task 10 直接使用，不再改动该结构。

- [ ] **Step 1: 写失败的测试**

`WriteRequest` 是 `Socket` 的私有嵌套类型，测试无法直接触及。改为在 `socket.cpp` 内部加编译期断言，并在单测中断言宏的存在性：

```cpp
TEST(LatencyTraceBuildTest, WriteRequestSizeMatchesBuildMode) {
    // The real guard is the BAIDU_CASSERT inside socket.cpp; this test
    // documents the contract and fails loudly if the macro is lost.
#if defined(BRPC_LATENCY_TRACE)
    SUCCEED() << "traced build: sizeof(WriteRequest) asserted == 128 in socket.cpp";
#else
    SUCCEED() << "default build: sizeof(WriteRequest) asserted == 64 in socket.cpp";
#endif
}
```

真正的验证是下一步的编译期断言。

- [ ] **Step 2: 改断言，先让它在追踪构建下失败**

`src/brpc/socket.cpp:2954` 改为：

```cpp
#if defined(BRPC_LATENCY_TRACE)
    BAIDU_CASSERT(sizeof(WriteRequest) == 128, sizeof_write_request_is_128);
#else
    BAIDU_CASSERT(sizeof(WriteRequest) == 64, sizeof_write_request_is_64);
#endif
```

Run: `./tools/latency_trace/sync950.sh && ssh suzhou950 'cd ~/brpc-lt && ./config_brpc.sh --headers="/usr/include/gtest /usr/include" --libs=/usr/lib64 --with-latency-trace && make -j64 2>&1 | grep -i cassert'`
Expected: 编译失败 —— 此时结构体还是 64 字节，`sizeof_write_request_is_128` 断言不成立。这证明断言真的在起作用。

- [ ] **Step 3: 加字段**

`src/brpc/socket.cpp` 的 `struct BAIDU_CACHELINE_ALIGNMENT Socket::WriteRequest` 中，在 `bthread_id_t id_wait;` 之后加：

```cpp
#if defined(BRPC_LATENCY_TRACE)
    // Which trace record this write belongs to. Written at Socket::Write()
    // entry, read back when the batch drains. Lives in the second cacheline
    // so the hot fields (data/next/id_wait/control bits) stay in the first.
    LatencyTraceHandle lt_handle;
#endif
```

并在文件顶部 `#include "brpc/latency_trace.h"`。

- [ ] **Step 4: 两种构建各编一次**

```bash
./tools/latency_trace/sync950.sh
ssh suzhou950 'cd ~/brpc-lt && ./config_brpc.sh --headers="/usr/include/gtest /usr/include" --libs=/usr/lib64 --with-latency-trace && make -j64'
ssh suzhou950 'cd ~/brpc-lt && ./config_brpc.sh --headers="/usr/include/gtest /usr/include" --libs=/usr/lib64 && make -j64'
```

Expected: 两次都编译成功。第二次证明默认构建仍是 64 字节。

- [ ] **Step 5: Commit**

```bash
git add src/brpc/socket.cpp test/brpc_latency_trace_unittest.cpp
git commit -m "feat(latency-trace): carry a trace handle in WriteRequest under the build switch"
```

---

## Task 7: trace_id 生成与跨进程传递

**Files:**
- Modify: `src/brpc/policy/baidu_rpc_meta.proto:42-49`
- Modify: `src/brpc/policy/baidu_rpc_protocol.cpp`（`PackRpcRequest` 约 `:1170` 附近、`ProcessRpcRequest` 约 `:671` 附近）
- Modify: `src/brpc/latency_trace.h`, `src/brpc/latency_trace.cpp`
- Test: `test/brpc_latency_trace_unittest.cpp`

**Interfaces:**
- Consumes: Task 3 的 `AllocSlot`
- Produces:
  - `uint64_t brpc::MakeLatencyTraceId(uint64_t seq);` —— 高 32 位为进程随机标签，低 32 位为序号
  - `ControllerPrivateAccessor`（`src/brpc/details/controller_private_accessor.h:41`）新增三个方法，**后续任务只用这三个名字**：
    - `void set_latency_trace(uint64_t trace_id, LatencyTraceHandle h);`
    - `uint64_t latency_trace_id() const;`
    - `LatencyTraceHandle latency_trace_handle() const;`
  - `Controller` 相应新增私有成员 `uint64_t _lt_trace_id;` 与 `LatencyTraceHandle _lt_handle;`（均 `#if` 保护），在 `Controller::ResetPods()` 中清零

- [ ] **Step 1: 写失败的测试**

```cpp
TEST(LatencyTraceIdTest, IdsAreUniqueWithinAndAcrossProcesses) {
    const uint64_t a = brpc::MakeLatencyTraceId(1);
    const uint64_t b = brpc::MakeLatencyTraceId(2);
    ASSERT_NE(a, b);
    // High 32 bits identify the process and must be identical within it.
    ASSERT_EQ(a >> 32, b >> 32);
    // ...and must not be zero, or two untagged processes would collide.
    ASSERT_NE(0u, (uint32_t)(a >> 32));
    ASSERT_EQ(1u, (uint32_t)a);
    ASSERT_EQ(2u, (uint32_t)b);
}

TEST(LatencyTraceIdTest, MetaCarriesTraceIdOnTag9) {
    brpc::policy::RpcRequestMeta meta;
    meta.set_service_name("s");
    meta.set_method_name("m");
    meta.set_latency_trace_id(0x1234567890ABCDEFULL);
    std::string buf;
    ASSERT_TRUE(meta.SerializeToString(&buf));
    brpc::policy::RpcRequestMeta parsed;
    ASSERT_TRUE(parsed.ParseFromString(buf));
    ASSERT_TRUE(parsed.has_latency_trace_id());
    ASSERT_EQ(0x1234567890ABCDEFULL, parsed.latency_trace_id());
}
```

- [ ] **Step 2: 跑测试确认失败**

Run: `./tools/latency_trace/sync950.sh && ssh suzhou950 'cd ~/brpc-lt/test && make NEED_GPERFTOOLS=0 brpc_latency_trace_unittest -j64'`
Expected: 编译失败，`'set_latency_trace_id' is not a member` 与 `'MakeLatencyTraceId' is not a member of 'brpc'`

- [ ] **Step 3: 写实现**

`src/brpc/policy/baidu_rpc_meta.proto` 的 `RpcRequestMeta` 末尾加：

```protobuf
    // Correlates client and server latency-trace records. Independent of
    // trace_id/span_id above, which are gated on rpcz's sampler.
    optional uint64 latency_trace_id = 9;
```

`latency_trace.h` 加声明，`latency_trace.cpp` 加实现：

```cpp
uint64_t MakeLatencyTraceId(uint64_t seq) {
    static const uint32_t s_process_tag = []() {
        uint32_t t = 0;
        while (t == 0) {   // never return a zero tag
            t = (uint32_t)(butil::detail::clock_cycles() ^
                           ((uint64_t)getpid() << 16));
        }
        return t;
    }();
    return ((uint64_t)s_process_tag << 32) | (uint32_t)seq;
}
```

`PackRpcRequest`（`baidu_rpc_protocol.cpp` 约 `:1170`，紧邻现有的 `request_meta->set_trace_id(...)`）加：

```cpp
#if defined(BRPC_LATENCY_TRACE)
    if (accessor.latency_trace_id() != 0) {
        request_meta->set_latency_trace_id(accessor.latency_trace_id());
    }
#endif
```

`ProcessRpcRequest`（约 `:671`）从 meta 取出并用它 `AllocSlot`。

- [ ] **Step 4: 跑测试确认通过**

Run: `./tools/latency_trace/sync950.sh && ssh suzhou950 'cd ~/brpc-lt/test && make NEED_GPERFTOOLS=0 brpc_latency_trace_unittest -j64 && ./brpc_latency_trace_unittest'`
Expected: 10 tests PASS

- [ ] **Step 5: Commit**

```bash
git add src/brpc/policy/baidu_rpc_meta.proto src/brpc/policy/baidu_rpc_protocol.cpp \
        src/brpc/latency_trace.h src/brpc/latency_trace.cpp \
        test/brpc_latency_trace_unittest.cpp
git commit -m "feat(latency-trace): propagate latency_trace_id via RpcRequestMeta tag 9"
```

---

## Task 8: 客户端发送侧点位 C01–C08

**Files:**
- Modify: `src/brpc/controller.h`（加 `_lt_handle` 成员）、`src/brpc/controller.cpp`
- Modify: `src/brpc/channel.cpp:~470`（`CallMethod` 入口）、`:592`（`_serialize_request` 前后）
- Modify: `src/brpc/policy/baidu_rpc_protocol.cpp:1096`（meta 构造）、`SerializeRpcHeaderAndMeta` 后
- Modify: `src/brpc/socket.cpp:1617`（`Write` 入口）、`:1902`（`CutFromIOBufList` 前后）
- Test: `test/brpc_latency_trace_unittest.cpp`

**Interfaces:**
- Consumes: Task 4 的 `LT_STAMP` / `LT_ALLOC`，Task 6 的 `WriteRequest::lt_handle`，Task 7 的 `MakeLatencyTraceId`
- Produces: `Controller::_lt_handle` 与 `ControllerPrivateAccessor::latency_trace_handle()`

- [ ] **Step 1: 写失败的端到端测试**

```cpp
// 起一个真实 server + client，跑一次同步 RPC，断言 C01..C08 全部被打上。
TEST(LatencyTraceE2ETest, ClientSendPointsAreStampedAndMonotonic) {
    FLAGS_latency_trace_enabled = true;
    brpc::LatencyTraceBuffer::instance()->ResetForTest(1024);

    brpc::Server server;
    EchoServiceImpl svc;
    ASSERT_EQ(0, server.AddService(&svc, brpc::SERVER_DOESNT_OWN_SERVICE));
    ASSERT_EQ(0, server.Start(9527, nullptr));

    brpc::Channel channel;
    brpc::ChannelOptions opt;
    opt.protocol = brpc::PROTOCOL_BAIDU_STD;
    ASSERT_EQ(0, channel.Init("127.0.0.1:9527", &opt));

    test::EchoService_Stub stub(&channel);
    test::EchoRequest req;
    test::EchoResponse res;
    brpc::Controller cntl;
    req.set_message("hello");
    stub.Echo(&cntl, &req, &res, nullptr);
    ASSERT_FALSE(cntl.Failed()) << cntl.ErrorText();

    const brpc::LatencyTraceRecord* r = FindClientRecordForTest();
    ASSERT_TRUE(r != nullptr);
    for (int p = brpc::LT_C_RPC_START; p <= brpc::LT_C_WRITE_END; ++p) {
        ASSERT_GT(r->ts[p], 0u) << "point " << p << " was never stamped";
    }
    for (int p = brpc::LT_C_RPC_START; p < brpc::LT_C_WRITE_END; ++p) {
        ASSERT_LE(r->ts[p], r->ts[p + 1])
            << "point " << p << " is later than " << (p + 1);
    }
    server.Stop(0);
    server.Join();
    FLAGS_latency_trace_enabled = false;
}
```

两个测试辅助函数在本任务定义，Task 9/10/11 直接复用：

```cpp
// Scans every shard for the single record of the given role. Tests run one
// RPC at a time, so exactly one match is expected; returning nullptr on
// zero or multiple matches keeps a silently-wrong test from passing.
static const brpc::LatencyTraceRecord* FindRecordByRole(brpc::LatencyTraceRole role) {
    const brpc::LatencyTraceRecord* found = nullptr;
    int n = 0;
    brpc::LatencyTraceBuffer* b = brpc::LatencyTraceBuffer::instance();
    for (uint64_t seq = 0; ; ++seq) {
        const brpc::LatencyTraceRecord* r = b->GetBySeqForTest(seq);
        if (r == nullptr) {
            break;
        }
        if (r->role == (uint8_t)role) {
            found = r;
            ++n;
        }
    }
    return (n == 1) ? found : nullptr;
}
static const brpc::LatencyTraceRecord* FindClientRecordForTest() {
    return FindRecordByRole(brpc::LT_ROLE_CLIENT);
}
static const brpc::LatencyTraceRecord* FindServerRecordForTest() {
    return FindRecordByRole(brpc::LT_ROLE_SERVER);
}
```

`EchoServiceImpl` 需在测试文件内实现 `test::EchoService`（`test/echo.proto:55`，package 为 `test`），`Echo` 方法用 `brpc::ClosureGuard done_guard(done);` 开头并把 `request->message()` 原样写入 `response`。

- [ ] **Step 2: 跑测试确认失败**

Run: `./tools/latency_trace/sync950.sh && ssh suzhou950 'cd ~/brpc-lt/test && make NEED_GPERFTOOLS=0 brpc_latency_trace_unittest -j64 && ./brpc_latency_trace_unittest' --gtest_filter='LatencyTraceE2ETest.*'`
Expected: FAIL —— `r` 为 nullptr，或各点位全为 0

- [ ] **Step 3: 逐点位加埋点**

`controller.h` 私有区加（紧邻其它 `#if` 保护的成员）：

```cpp
#if defined(BRPC_LATENCY_TRACE)
    LatencyTraceHandle _lt_handle;
#endif
```

在 `Controller::ResetNonPods()` 或等价的重置路径中把它置为 `LT_INVALID_HANDLE`。

`channel.cpp` 的 `CallMethod` 入口（在 `cntl->set_used_by_rpc();` 之后、rpcz 的 span 创建之前）：

```cpp
#if defined(BRPC_LATENCY_TRACE)
    {
        static butil::atomic<uint64_t> s_lt_seq(0);
        const uint64_t seq = s_lt_seq.fetch_add(1, butil::memory_order_relaxed);
        const uint64_t trace_id = MakeLatencyTraceId(seq);
        ControllerPrivateAccessor accessor(cntl);
        accessor.set_latency_trace(trace_id,
                                   LT_ALLOC(trace_id, LT_ROLE_CLIENT));
        LT_STAMP(accessor.latency_trace_handle(), LT_C_RPC_START);
    }
#endif
```

`channel.cpp:592` 的 `_serialize_request` 前后分别 `LT_STAMP(h, LT_C_REQ_PAYLOAD_SER_START)` 与 `LT_C_REQ_PAYLOAD_SER_END`。

`baidu_rpc_protocol.cpp` 的 `PackRpcRequest`：`RpcMeta meta;` 前打 `LT_C_REQ_META_SER_START`，`SerializeRpcHeaderAndMeta()` 返回后打 `LT_C_REQ_META_SER_END`。

`socket.cpp:1617` 的 `Socket::Write(butil::IOBuf*, ...)`：在 `req->id_wait = opt.id_wait;` 之后加

```cpp
#if defined(BRPC_LATENCY_TRACE)
    req->lt_handle = opt.lt_handle;
    LT_STAMP(req->lt_handle, (opt.lt_role == LT_ROLE_SERVER)
                                 ? LT_S_WRITE_ENQUEUE : LT_C_WRITE_ENQUEUE);
#endif
```

`WriteOptions` 相应增加 `lt_handle` 与 `lt_role` 两个字段（同样 `#if` 保护）。

`socket.cpp:1902` 的 `DoWrite`：`CutFromIOBufList()` 调用前遍历本批次打 `LT_*_WRITE_START`；返回后遍历本批次，对 `p->data.empty()` 者打 `LT_*_WRITE_END`（**这是 spec §8.4 的关键落点，不要挪到 `ReturnSuccessfulWriteRequest`**）。

- [ ] **Step 4: 跑测试确认通过**

Run: `./tools/latency_trace/sync950.sh && ssh suzhou950 'cd ~/brpc-lt/test && make NEED_GPERFTOOLS=0 brpc_latency_trace_unittest -j64 && ./brpc_latency_trace_unittest' --gtest_filter='LatencyTraceE2ETest.*'`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add src/brpc/controller.h src/brpc/controller.cpp src/brpc/channel.cpp \
        src/brpc/socket.cpp src/brpc/socket.h \
        src/brpc/policy/baidu_rpc_protocol.cpp test/brpc_latency_trace_unittest.cpp
git commit -m "feat(latency-trace): stamp client send-side points C01-C08"
```

---

## Task 9: 事件级点位透传（C09–C12 / S01–S04）

**Files:**
- Modify: `src/brpc/socket.h`（加 3 个字段）、`src/brpc/event_dispatcher_epoll.cpp:~230`
- Modify: `src/brpc/transport.h:31`（`OnEdge` 入口）
- Modify: `src/brpc/input_message_base.h`（加 4 个字段）、`src/brpc/input_messenger.cpp`（`OnNewMessages` 循环与 `ProcessNewMessage`）
- Test: `test/brpc_latency_trace_unittest.cpp`

**Interfaces:**
- Produces:
  - `Socket::_lt_wake` / `_lt_onedge_start` / `_lt_readv_start`（原始计数值，`#if` 保护）
  - `InputMessageBase::_lt_wake` / `_lt_onedge_start` / `_lt_readv_start` / `_lt_msg_recv_done`
  - TLS 变量 `tls_lt_epoll_wake`（同一次 `epoll_wait` 返回的 N 个事件共享）

- [ ] **Step 1: 写失败的测试**

```cpp
TEST(LatencyTraceE2ETest, ReceivePointsAreStampedOnBothSides) {
    // 复用 Task 8 的 server/client 骨架，跑一次 RPC。
    // 客户端断言 C09..C12，服务端断言 S01..S04。
    const brpc::LatencyTraceRecord* c = FindClientRecordForTest();
    const brpc::LatencyTraceRecord* s = FindServerRecordForTest();
    ASSERT_TRUE(c != nullptr && s != nullptr);
    for (int p = brpc::LT_C_WAKE; p <= brpc::LT_C_MSG_RECV_DONE; ++p) {
        ASSERT_GT(c->ts[p], 0u) << "client point " << p;
    }
    for (int p = brpc::LT_S_WAKE; p <= brpc::LT_S_MSG_RECV_DONE; ++p) {
        ASSERT_GT(s->ts[p], 0u) << "server point " << p;
    }
    // 同一批次内的消息共享 wake 值，因此 wake <= onedge <= readv <= recv_done
    ASSERT_LE(s->ts[brpc::LT_S_WAKE], s->ts[brpc::LT_S_ONEDGE_START]);
    ASSERT_LE(s->ts[brpc::LT_S_ONEDGE_START], s->ts[brpc::LT_S_READV_START]);
    ASSERT_LE(s->ts[brpc::LT_S_READV_START], s->ts[brpc::LT_S_MSG_RECV_DONE]);
}
```

- [ ] **Step 2: 跑测试确认失败**

Run: `ssh suzhou950 'cd ~/brpc-lt/test && ./brpc_latency_trace_unittest --gtest_filter='*ReceivePointsAreStamped*'`
Expected: FAIL —— 各点位为 0

- [ ] **Step 3: 写实现**

`event_dispatcher_epoll.cpp` 的 `Run()` 中，`epoll_wait` 返回后、进入 `for` 循环前：

```cpp
#if defined(BRPC_LATENCY_TRACE)
        // All N events from one epoll_wait return share this timestamp.
        // That is the correct semantics: they were one wake-up.
        tls_lt_epoll_wake = butil::detail::clock_cycles();
#endif
```

`Socket::OnInputEvent`（`socket.cpp:2226`）入口把 `tls_lt_epoll_wake` 写进 `_lt_wake`。

`Transport::OnEdge`（`transport.h:31`）在 `on_edge_trigger(s.get())` 之前写 `s->_lt_onedge_start`。

`InputMessenger::OnNewMessages` 循环内 `m->DoRead()` 之前写 `m->_lt_readv_start`。

`ProcessNewMessage` 中每切出一条消息后，把 Socket 上的三个值拷进该 `InputMessageBase`，并记 `_lt_msg_recv_done`。

- [ ] **Step 4: 跑测试确认通过**

Run: `ssh suzhou950 'cd ~/brpc-lt/test && ./brpc_latency_trace_unittest --gtest_filter='*ReceivePointsAreStamped*'`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add src/brpc/socket.h src/brpc/socket.cpp src/brpc/event_dispatcher_epoll.cpp \
        src/brpc/transport.h src/brpc/input_message_base.h src/brpc/input_messenger.cpp \
        test/brpc_latency_trace_unittest.cpp
git commit -m "feat(latency-trace): thread event-level points from Socket through InputMessageBase"
```

---

## Task 10: 服务端点位 S05–S17

**Files:**
- Modify: `src/brpc/policy/baidu_rpc_protocol.cpp`：`ProcessRpcRequest`（`:671` 起）、`SendRpcResponse`（`:282` 起、`:331`、`:346`、`:401`）
- Test: `test/brpc_latency_trace_unittest.cpp`

**Interfaces:**
- Consumes: Task 9 的 `InputMessageBase` 四个字段；Task 7 的 `latency_trace_id`

- [ ] **Step 1: 写失败的测试**

```cpp
TEST(LatencyTraceE2ETest, ServerPointsAreCompleteAndMonotonic) {
    const brpc::LatencyTraceRecord* s = FindServerRecordForTest();
    ASSERT_TRUE(s != nullptr);
    for (int p = brpc::LT_S_WAKE; p <= brpc::LT_S_WRITE_END; ++p) {
        ASSERT_GT(s->ts[p], 0u) << "server point " << p << " was never stamped";
    }
    for (int p = brpc::LT_S_WAKE; p < brpc::LT_S_WRITE_END; ++p) {
        ASSERT_LE(s->ts[p], s->ts[p + 1])
            << "server point " << p << " is later than " << (p + 1);
    }
}
```

- [ ] **Step 2: 跑测试确认失败**

Run: `ssh suzhou950 'cd ~/brpc-lt/test && ./brpc_latency_trace_unittest --gtest_filter='*ServerPointsAreComplete*'`
Expected: FAIL —— S05 之后各点位为 0

- [ ] **Step 3: 写实现**

`ProcessRpcRequest`：入口把 `LT_S_REQ_META_DESER_START` 存进**局部变量**（此时 handle 未知），`ParsePbFromIOBuf(&meta, ...)` 之后存 `LT_S_REQ_META_DESER_END`；从 `meta.request().latency_trace_id()` 取出 trace_id 后 `AllocSlot`，把 Task 9 传来的四个事件级值和这两个局部值一并写入槽。之后各点位直接 `LT_STAMP`。

`SendRpcResponse` 入口打 `LT_S_SERVICE_END`；`SerializeResponse` 前后打 `LT_S_RSP_PAYLOAD_SER_START/END`；`RpcMeta meta;` 前与 `SerializeRpcHeaderAndMeta` 后打 `LT_S_RSP_META_SER_START/END`；`sock->Write(&res_buf, &wopt)` 前把 handle 塞进 `wopt.lt_handle` 并置 `wopt.lt_role = LT_ROLE_SERVER`。

- [ ] **Step 4: 跑测试确认通过**

Run: `ssh suzhou950 'cd ~/brpc-lt/test && ./brpc_latency_trace_unittest --gtest_filter='*ServerPointsAreComplete*'`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add src/brpc/policy/baidu_rpc_protocol.cpp test/brpc_latency_trace_unittest.cpp
git commit -m "feat(latency-trace): stamp server points S05-S17"
```

---

## Task 11: 客户端接收侧点位 C13–C19 与 Σ 恒等

**Files:**
- Modify: `src/brpc/latency_trace.h`（加 `const uint32_t LT_TS_NOT_APPLICABLE = 0xFFFFFFFFu;`，Task 12 会用）
- Modify: `src/brpc/policy/baidu_rpc_protocol.cpp:936-990`（`ProcessRpcResponse`）
- Modify: `src/brpc/controller.cpp`（`OnRPCReturned` / `OnRPCEnd`）、`src/brpc/channel.cpp:662`
- Test: `test/brpc_latency_trace_unittest.cpp`

**Interfaces:**
- Consumes: 前述全部

- [ ] **Step 1: 写失败的测试**

```cpp
TEST(LatencyTraceE2ETest, AllClientPointsStampedAndSumIsIdentity) {
    const brpc::LatencyTraceRecord* c = FindClientRecordForTest();
    const brpc::LatencyTraceRecord* s = FindServerRecordForTest();
    ASSERT_TRUE(c != nullptr && s != nullptr);
    for (int p = brpc::LT_C_RPC_START; p <= brpc::LT_C_RPC_END; ++p) {
        ASSERT_GT(c->ts[p], 0u) << "client point " << p;
    }
    for (int p = brpc::LT_C_RPC_START; p < brpc::LT_C_RPC_END; ++p) {
        ASSERT_LE(c->ts[p], c->ts[p + 1]) << "client point " << p;
    }

    // Sigma identity (spec sec.5): the 17 client intervals excluding
    // C08->C09, plus the 16 server intervals, plus the two link halves,
    // must equal the end-to-end span exactly.
    const int64_t e2e = (int64_t)c->ts[brpc::LT_C_RPC_END] -
                        (int64_t)c->ts[brpc::LT_C_RPC_START];
    int64_t sum = 0;
    for (int p = brpc::LT_C_RPC_START; p < brpc::LT_C_RPC_END; ++p) {
        if (p == brpc::LT_C_WRITE_END) {
            continue;   // replaced by link_up + server + link_down
        }
        sum += (int64_t)c->ts[p + 1] - (int64_t)c->ts[p];
    }
    for (int p = brpc::LT_S_WAKE; p < brpc::LT_S_WRITE_END; ++p) {
        sum += (int64_t)s->ts[p + 1] - (int64_t)s->ts[p];
    }
    const int64_t rtt = (int64_t)c->ts[brpc::LT_C_WAKE] -
                        (int64_t)c->ts[brpc::LT_C_WRITE_END];
    const int64_t srv = (int64_t)s->ts[brpc::LT_S_WRITE_END] -
                        (int64_t)s->ts[brpc::LT_S_WAKE];
    sum += (rtt - srv);   // link_up + link_down
    ASSERT_EQ(e2e, sum) << "Sigma(35 items) must equal end-to-end exactly";
    // This TCP test must never see the RDMA polling-mode sentinel; if it
    // does, the sums above are silently wrong. See Task 12 and spec sec.8.5.
    for (int p = 0; p < brpc::LT_POINT_COUNT; ++p) {
        ASSERT_NE(brpc::LT_TS_NOT_APPLICABLE, c->ts[p]) << "client point " << p;
        ASSERT_NE(brpc::LT_TS_NOT_APPLICABLE, s->ts[p]) << "server point " << p;
    }
}

TEST(LatencyTraceE2ETest, LinkTimeIsNonNegativeAtOutstandingOne) {
    // Spec sec.10: with one in-flight request per connection there is no
    // batch attribution error, so a negative link time means the
    // instrumentation is misplaced -- most likely write_end (see sec.8.4).
    const brpc::LatencyTraceRecord* c = FindClientRecordForTest();
    const brpc::LatencyTraceRecord* s = FindServerRecordForTest();
    const int64_t rtt = (int64_t)c->ts[brpc::LT_C_WAKE] -
                        (int64_t)c->ts[brpc::LT_C_WRITE_END];
    const int64_t srv = (int64_t)s->ts[brpc::LT_S_WRITE_END] -
                        (int64_t)s->ts[brpc::LT_S_WAKE];
    ASSERT_GE(rtt - srv, 0)
        << "negative total link time at outstanding=1; check write_end anchor";
}
```

- [ ] **Step 2: 跑测试确认失败**

Run: `ssh suzhou950 'cd ~/brpc-lt/test && ./brpc_latency_trace_unittest --gtest_filter='*SumIsIdentity*:*LinkTimeIsNonNegative*'`
Expected: FAIL —— C13 之后的点位为 0

- [ ] **Step 3: 写实现**

`ProcessRpcResponse`（`:936`）：入口与 `ParsePbFromIOBuf` 之后分别把 `LT_C_RSP_META_DESER_START/END` 存进**局部变量**（handle 要等 `bthread_id_lock` 之后才拿得到，见 spec §8.2）；`bthread_id_lock(cid, &cntl)` 成功后从 `cntl` 取出 handle，把两个局部值与 Task 9 传来的四个事件级值写入槽。

之后：response 的 `ParseFromIOBuf` 前后打 `LT_C_RSP_PAYLOAD_DESER_START/END`；`OnRPCReturned` / `done->Run()` 前后打 `LT_C_RSP_PROCESS_START/END`；`channel.cpp:662` 的 `cntl->OnRPCEnd(...)` 处打 `LT_C_RPC_END`。

- [ ] **Step 4: 跑测试确认通过**

Run: `./tools/latency_trace/sync950.sh && ssh suzhou950 'cd ~/brpc-lt/test && make NEED_GPERFTOOLS=0 brpc_latency_trace_unittest -j64 && ./brpc_latency_trace_unittest'`
Expected: 全部 PASS，尤其 `SumIsIdentity` 与 `LinkTimeIsNonNegative`

- [ ] **Step 5: Commit**

```bash
git add src/brpc/policy/baidu_rpc_protocol.cpp src/brpc/controller.cpp \
        src/brpc/channel.cpp test/brpc_latency_trace_unittest.cpp
git commit -m "feat(latency-trace): stamp client receive-side points and assert the sigma identity"
```

---

## Task 12: RDMA 收侧三点位

**依赖 suzhou950 恢复连通**（本机与 suzhou920B 都没有 `infiniband/verbs.h`）。若 950 长期不可用，本任务顺延，不阻塞 Task 1–11 与后续的分析工具计划。

**Files:**
- Modify: `src/brpc/rdma/rdma_endpoint.cpp:1469`（`PollCq` 入口）、`:1500`（`ibv_poll_cq` 前）、`:1599`（`ProcessNewMessage` 调用处）
- Test: `test/brpc_latency_trace_unittest.cpp`

**Interfaces:**
- Consumes: Task 9 的 `Socket::_lt_wake` / `_lt_onedge_start` / `_lt_readv_start`

- [ ] **Step 1: 写失败的测试**

```cpp
#if defined(BRPC_WITH_RDMA)
TEST(LatencyTraceRdmaTest, PollingModeMarksWakeAndOnedgeAsSentinel) {
    // Spec sec.8.5: under -rdma_use_polling there is neither an epoll
    // wake-up nor an OnEdge bthread switch, so these two points do not
    // physically exist. They must carry the sentinel, not a bogus zero,
    // so merge.py can mark the four receive-queueing items as N/A.
    const brpc::LatencyTraceRecord* s = FindServerRecordForTest();
    ASSERT_EQ(brpc::LT_TS_NOT_APPLICABLE, s->ts[brpc::LT_S_WAKE]);
    ASSERT_EQ(brpc::LT_TS_NOT_APPLICABLE, s->ts[brpc::LT_S_ONEDGE_START]);
    ASSERT_GT(s->ts[brpc::LT_S_READV_START], 0u);
}
#endif
```

`LT_TS_NOT_APPLICABLE` 已由 Task 11 在 `latency_trace.h` 中引入（`const uint32_t LT_TS_NOT_APPLICABLE = 0xFFFFFFFFu;`），本任务直接使用。

- [ ] **Step 2: 在 suzhou950 上跑，确认失败**

按 `suzhou950-soft-roce-rdma-testing` 记忆里的配方起 Soft-RoCE（需真实终端跑 `sudo`），然后：

```bash
./brpc_latency_trace_unittest --gtest_filter='LatencyTraceRdmaTest.*' \
  --rdma_use_polling=true \
  --rdma_memory_pool_initial_size_mb=32 --rdma_memory_pool_increase_size_mb=32 \
  --rdma_memory_pool_max_regions=1
```

Expected: FAIL

- [ ] **Step 3: 写实现**

`PollCq`（`rdma_endpoint.cpp:1469`）入口：事件模式下把 `GetAndAckEvents` 返回后的计数值写进 `s->_lt_wake`，并把 `PollCq` 入口计数值写进 `s->_lt_onedge_start`；轮询模式（`FLAGS_rdma_use_polling`）下两者都写 `LT_TS_NOT_APPLICABLE` 的哨兵表示。

`ibv_poll_cq`（`:1500`）之前写 `s->_lt_readv_start`。

`:1599` 调 `ProcessNewMessage` 时，Task 9 的拷贝逻辑自动生效，无需改动。

- [ ] **Step 4: 在 suzhou950 上跑，确认通过**

Run: 同 Step 2 的命令，并另跑一次 `--rdma_use_polling=false` 验证事件模式下三个点位都是真实值
Expected: 两种模式都 PASS

- [ ] **Step 5: Commit**

```bash
git add src/brpc/rdma/rdma_endpoint.cpp src/brpc/latency_trace.h \
        test/brpc_latency_trace_unittest.cpp
git commit -m "feat(latency-trace): stamp the RDMA receive path in both event and polling modes"
```

---

## Task 13: 记录元数据与重试 attempt

前面的任务只填了时间戳。`LatencyTraceRecord` 的其余字段（Task 2 定义）至今无人写入，而 HTML 的筛选功能（spec §9.2）全靠它们。spec D9 要求每次 attempt 独立成记录，也尚未实现。

**Files:**
- Modify: `src/brpc/policy/baidu_rpc_protocol.cpp`（`PackRpcRequest`、`ProcessRpcRequest`、`SendRpcResponse`、`ProcessRpcResponse`）
- Modify: `src/brpc/controller.cpp`（`IssueRPC` 的重试入口）
- Test: `test/brpc_latency_trace_unittest.cpp`

**Interfaces:**
- Consumes: Task 7 的 `ControllerPrivateAccessor::set_latency_trace` / `latency_trace_handle`；Task 3 的 `AllocSlot`
- Produces: 无新接口，只填充既有字段

- [ ] **Step 1: 写失败的测试**

```cpp
TEST(LatencyTraceMetaTest, RecordCarriesSizesEndpointAndErrorCode) {
    // Run one successful RPC first (reuse the Task 8 fixture).
    const brpc::LatencyTraceRecord* c = FindClientRecordForTest();
    const brpc::LatencyTraceRecord* s = FindServerRecordForTest();
    ASSERT_TRUE(c != nullptr && s != nullptr);

    ASSERT_GT(c->req_size, 0u) << "client never recorded the request size";
    ASSERT_GT(c->rsp_size, 0u) << "client never recorded the response size";
    ASSERT_EQ(0, c->error_code);
    ASSERT_NE(0u, c->socket_id);
    ASSERT_NE(0u, c->remote_ip);
    ASSERT_EQ(9527, c->remote_port);

    ASSERT_GT(s->req_size, 0u);
    ASSERT_NE(0u, s->socket_id);
    // Both ends must agree on the payload sizes they saw.
    ASSERT_EQ(c->req_size, s->req_size);
}

TEST(LatencyTraceMetaTest, EachRetryAttemptGetsItsOwnRecord) {
    // Spec D9: one record per attempt, numbered from 0. Point a channel at
    // a dead backend with max_retry=2 so three attempts are issued.
    FLAGS_latency_trace_enabled = true;
    brpc::LatencyTraceBuffer::instance()->ResetForTest(1024);

    brpc::Channel channel;
    brpc::ChannelOptions opt;
    opt.protocol = brpc::PROTOCOL_BAIDU_STD;
    opt.max_retry = 2;
    opt.timeout_ms = 200;
    ASSERT_EQ(0, channel.Init("127.0.0.1:9599", &opt));  // nothing listening

    test::EchoService_Stub stub(&channel);
    test::EchoRequest req;
    test::EchoResponse res;
    brpc::Controller cntl;
    req.set_message("x");
    stub.Echo(&cntl, &req, &res, nullptr);
    ASSERT_TRUE(cntl.Failed());

    std::set<int> attempts;
    brpc::LatencyTraceBuffer* b = brpc::LatencyTraceBuffer::instance();
    for (uint64_t seq = 0; ; ++seq) {
        const brpc::LatencyTraceRecord* r = b->GetBySeqForTest(seq);
        if (r == nullptr) {
            break;
        }
        if (r->role == brpc::LT_ROLE_CLIENT) {
            attempts.insert(r->attempt);
            ASSERT_NE(0, r->error_code) << "a failed attempt must carry its errno";
        }
    }
    ASSERT_EQ(3u, attempts.size()) << "expected one record per attempt";
    ASSERT_EQ(0, *attempts.begin());
    ASSERT_EQ(2, *attempts.rbegin());
    FLAGS_latency_trace_enabled = false;
}
```

- [ ] **Step 2: 跑测试确认失败**

Run: `./tools/latency_trace/sync950.sh && ssh suzhou950 'cd ~/brpc-lt/test && make NEED_GPERFTOOLS=0 brpc_latency_trace_unittest -j64 && ./brpc_latency_trace_unittest' --gtest_filter='LatencyTraceMetaTest.*'`
Expected: FAIL —— 各元数据字段为 0，且只有 1 条客户端记录

- [ ] **Step 3: 写实现**

字段填充位置：

| 字段 | 客户端 | 服务端 |
|---|---|---|
| `req_size` | `PackRpcRequest` 中 `request_body.size()` | `ProcessRpcRequest` 中 `msg->payload.size()` |
| `rsp_size` | `ProcessRpcResponse` 中 response payload 长度 | `SendRpcResponse` 中 `res_buf.size()` |
| `error_code` | `Controller::OnRPCEnd` 中 `ErrorCode()` | `SendRpcResponse` 中已算出的 `error_code` |
| `socket_id` / `remote_ip` / `remote_port` | `IssueRPC` 拿到 socket 后 | `ProcessRpcRequest` 中 `msg->socket()` |
| `method_id` | 见下 |

`method_id` 用一个进程内的字符串→序号表：`uint32_t brpc::LatencyTraceMethodId(const std::string& full_name);` 加在 `latency_trace.cpp`，内部用 `butil::FlatMap<std::string, uint32_t>` 加互斥量，首次出现时分配递增序号。**该表随 dump 一起落盘**，追加在记录之后，格式为 `uint32 count` 后跟 `count` 个 `uint32 len + bytes`，其字节偏移写入 Task 5 已预留的 `LatencyTraceFileHeader::method_table_offset`（在此之前该字段恒为 0）。

重试 attempt：`Controller::IssueRPC` 中，若 `_lt_handle` 已有值（说明这是重试而非首发），则**重新 `AllocSlot`** 拿一个新槽，`attempt` 置为当前重试次数，并把新 trace_id 写进 meta。旧槽保持原样，作为一条独立记录留在缓冲里。

- [ ] **Step 4: 跑测试确认通过**

Run: `ssh suzhou950 'cd ~/brpc-lt/test && ./brpc_latency_trace_unittest --gtest_filter='LatencyTraceMetaTest.*'`
Expected: 2 tests PASS

- [ ] **Step 5: Commit**

```bash
git add src/brpc/latency_trace.h src/brpc/latency_trace.cpp \
        src/brpc/policy/baidu_rpc_protocol.cpp src/brpc/controller.cpp \
        test/brpc_latency_trace_unittest.cpp
git commit -m "feat(latency-trace): fill record metadata and give each retry attempt its own record"
```

---

## Task 14（条件任务）: `lite` 点位集 — **已取消**

Task 1 的实测结论为 `DECISION: lite-point-set = NOT_REQUIRED`
（`docs/superpowers/plans/phase0-results.md` 首行）：36 点总开销 0.263~0.268 μs，
仅占 RDMA 5~15 μs 端到端时延的 1.8~5.4%，远低于 10% 阈值，最严的一端仍有 1.86 倍余量。
经两轮独立复核（含算术重算）后结论稳定。

因此**本任务不执行**，`-latency_trace_point_set` gflag 整体不实现，
spec §8.1 的 flags 表中该行已删除。点位数恒为 36。

---

## 后续计划

本计划只覆盖 spec 的 C++ 侧（§4、§5 的点位与分解、§8 的运行时模块、§8.5 的 RDMA）。

**离线分析与可视化（spec §6、§9）将另出一份计划**，包含 `merge.py`（时钟标定、按 `trace_id` join、两种链路模型、Σ 恒等断言）与 `render.py`（Canvas 累积柱状图、瀑布式负值渲染、hover 下钻、框选缩放）。

之所以分开：那份计划的输入是本计划 Task 5 产出的**真实 dump 文件格式与真实数据**。在拿到真实文件之前编写解析与渲染的测试，只能靠臆想的样例数据，做出来的测试是假的。Task 5 完成后即可开写第二份计划。
