# Task A capture notes: first real latency-trace dump

**Date:** 2026-09-01
**Build:** `latency-trace` @ `517d858b`, remote tree `~/brpc-lt-traced` on `suzhou950`
(aarch64, `[config: TRACED]`, `NEED_GPERFTOOLS=0`), full `make clean && make -j$(nproc)`.
**Workload:** `example/echo_c++` (`test/echo.proto`'s sibling under `example/`), baidu_std
over TCP on `127.0.0.1:9541`. One channel, sequential blocking calls (`done=nullptr`),
`max_retry=0`, `interval_ms=0` — outstanding=1 throughout, i.e. the design doc's
"low concurrency" end-to-end condition (sec.10.1).
**Driver:** `tools/latency_trace/capture.sh` (committed alongside this file). Both
processes stopped with SIGINT (see script comment — SIGTERM is a silent no-dump trap
here, since brpc only installs a SIGTERM handler when `-graceful_quit_on_sigterm=true`,
otherwise it's SIG_DFL and the atexit-registered `Dump()` never runs).

Two runs were done:

- **Run 1 (manual, pre-script verification, 3288 records/side)** — used to work out
  the SIGTERM trap and to eyeball the format before trusting it. Surfaced one
  monotonicity violation (see "Anomaly" section below).
- **Run 2 (via the finished `capture.sh`, 3169 records/side)** — the canonical capture
  this document's numbers are drawn from unless stated otherwise. Zero violations.

Both runs are consistent on everything *except* the one anomaly, which did not
reproduce in run 2 — see below for why that's expected of a scheduling race, not
grounds to dismiss it.

## File sizes and record counts (Run 2, canonical)

| | server.dump | client.dump |
|---|---|---|
| file size | 608608 bytes | 608608 bytes |
| `record_count` | 3169 | 3169 |
| requests driven | 3169 (client log confirms 3169 `Received response` lines before SIGINT) | |
| `dropped_count` | 0 | 0 |

`record_count` matches the number of RPCs actually driven exactly — no silent
under/over-count.

## Header field values (Run 2, canonical)

| Field | server.dump | client.dump |
|---|---|---|
| `magic` (LE bytes) | `31 43 52 54 4c 50 52 42` = `"1CRTLPRB"` | same |
| magic == `LT_FILE_MAGIC` (0x4252504C54524331) as little-endian u64 | **true** | **true** |
| `record_size` | 192 | 192 |
| `point_count` | 34 | 34 |
| `head_counter` | 21158345071468-ish (per-record base, see below) | — |
| `head_monotonic_ns` | 211497667701640 | 211497661009380 |
| `tail_monotonic_ns` | 211497967729630 | 211497961589990 |
| monotonic delta | 0.300028 s | 0.300581 s |
| `head_realtime_ns` | 1788209828686625970 → 2026-08-31 20:57:08.686626 UTC | 1788209828679933710 → 2026-08-31 20:57:08.679934 UTC |
| `tail_realtime_ns` | 1788209828986653970 → 2026-08-31 20:57:08.986654 UTC | 1788209828980514320 → 2026-08-31 20:57:08.980514 UTC |
| realtime delta | 0.300028 s | 0.300581 s |
| `counter_freq_hz` | 100000036.66 Hz (100.0000 MHz) | 100000099.81 Hz (100.0001 MHz) |
| `cntfrq_el0_hz` | 100000000 Hz (100.0000 MHz) | 100000000 Hz |
| cross-check delta (empirical vs. `CNTFRQ_EL0`) | -0.00004% | -0.00010% |
| `record_count` | 3169 | 3169 |
| `dropped_count` | 0 | 0 |
| `process_tag` | 1568225815 (0x5d793a17) | 4268992275 (0xfe73a713) |
| `method_table_offset` | 608576 | 608576 |
| expected `method_table_offset` (128 + 192×3169) | 608576 — **match** | 608576 — **match** |

All checks pass:

- **Magic**: bytes on disk are `1CRTLPRB`, matching the spec's note that the constant's
  big-endian spelling `"BRPLTRC1"` reads backwards on this little-endian host.
- **`record_size`/`point_count`**: 192/34, matching `sizeof(LatencyTraceRecord)`'s
  `static_assert` and `LT_POINT_COUNT`.
- **`record_count`**: matches requests driven exactly, both sides.
- **Calibration pairs**: the monotonic pair brackets ~0.30s on both sides, comfortably
  covering process start (well before any RPC) through dump time — sane. The realtime
  pair decodes to 2026-08-31 20:57:08 UTC, a plausible wall-clock instant, and its
  delta (0.300028s / 0.300581s) matches the monotonic delta almost exactly (both derived
  from the same two sampling instants, just different clocks) — no NTP-step artifact
  visible.
- **`counter_freq_hz`**: ~100.0000-100.0001 MHz on both sides, matching "this host runs
  at about 100 MHz." Cross-check against `cntfrq_el0_hz` (also 100.0000 MHz) is within
  0.0001% — nowhere near the 0.1% warning threshold.
- **`dropped_count`**: 0 on both sides — the 100000-record capacity was never remotely
  approached by 3169 records.
- **`method_table_offset`**: non-zero (608576) and exactly equal to
  `128 + record_size × record_count` = `128 + 192×3169` = 608576 on both sides. The
  method table itself (`count=1`, `["example.EchoService.Echo"]`) ends exactly at the
  file's true size (608608 = 608576 + 4-byte count + 4-byte len + 24-byte name),
  confirming no trailing garbage and no truncation.

## Record-level checks

Checked over all 3169 records on each side (Run 2):

- **`trace_id` structure**: high 32 bits / low 32 bits split confirmed. On the
  **client**, every record's high-32 equals the client's own `process_tag`
  (0xfe73a713) — expected, since the client is the trace id's originator — and the
  low-32 values are exactly the contiguous range `0..3168` with no gaps or
  duplicates (`max_retry=0`, so one attempt per RPC, one seq per attempt). On the
  **server**, every record's high-32 equals the *client's* tag, 0xfe73a713 — not the
  server's own `process_tag` (0x5d793a17) — because the server never mints its own
  trace id for a request it received; it reuses the id that arrived over the wire.
  This is the intended mechanism, not a defect: it's exactly why the join works.
- **`ts[]` non-zero where expected**: for every record, all 17 points in that record's
  own role range are stamped (never 0) — 3169/3169 coverage on all 17 client points and
  all 17 server points. For every record, all 17 points in the *other* role's range are
  exactly 0 (a client-side record never has any `S_*` point stamped, and vice versa) —
  3169/3169 both sides. No `0xFFFFFFFF` (N/A) or `0xFFFFFFFE` (saturated) sentinels
  appeared anywhere in either file — expected, since this is a plain TCP/epoll run, not
  RDMA polling mode, and no interval came close to 42.9s.
- **Monotonicity**: 0/3169 violations in Run 2 (every stamped point's decoded offset —
  `ts[i] - 1` — is non-decreasing across the point order, both roles). **Run 1 found
  exactly one violation in 3288 records — see "Anomaly" below.**
- **Other fields**: `error_code == 0`, `attempt == 0` on every record both sides (no
  retries, no failures). `req_size` constant at 13 bytes both sides (the serialized
  `"hello world"` EchoRequest). `rsp_size` mostly 44, occasionally 43 on the server
  side (a 1-byte varint-boundary artifact of protobuf's length-prefix encoding — not
  investigated further, doesn't affect any of the format checks above). Single
  `socket_id` per side (one persistent connection reused for all 3169 RPCs, as
  expected with the default connection type), consistent `remote_port` on each side
  (client always sees the server's :9541, server always sees the client's one
  ephemeral port) — confirms a single TCP connection was exercised end-to-end, not
  reconnects.

## Join

**Server trace_id set: 3169. Client trace_id set: 3169. Intersection: 3169.
Join rate: 100.0000% both directions (only-in-server: 0, only-in-client: 0).**

This is the single most load-bearing number in this document: the two dumps' trace ids
fully agree, so the cross-process join `merge.py` will depend on works on real,
independently-produced files, not just in the unit tests' single-process harness.

## Anomaly: one non-monotonic client record in Run 1 (not reproduced in Run 2)

**This is a real finding, not a documentation mismatch — reporting per the task's
instruction to stop and report rather than work around it.**

In the manual pre-script run (3288 client records), record index 954
(`trace_id=0xb66b0e43000003ba`) had `C_WRITE_END` stamped *after* `C_WAKE` in decoded
offset order:

```
C_WRITE_START               +103
C_WRITE_END                 +3275   <- later than the next point
C_WAKE                      +2870   <- earlier than the previous point
C_ONEDGE_START               +3020
```

i.e. `C09 (WAKE) < C08 (WRITE_END)` — the exact "negative RTT" signature the design
doc's sec.10.1 defect table calls out (`C09 ≥ C08` check), and this run's condition
(single channel, sequential, `outstanding=1`) is precisely the one sec.10.1 says
should make every decomposition item non-negative ("此条件下不存在批处理归因失真，
出现负值即表明埋点位置有误").

**Read of the mechanism** (not a fix, just what the code shows): per
`src/brpc/socket.cpp`'s fast-write path (~line 1810-1846), `LT_C_WRITE_END` is stamped
by the *same* bthread that issued the write, synchronously after the write syscall
returns, on the calling thread — before that thread ever blocks waiting for a
response. `LT_C_WAKE` (`src/brpc/policy/baidu_rpc_protocol.cpp:1122`) is stamped by
whichever thread/bthread services the epoll readable event for the response,
independently. On this idle 384-core loopback host, round trips are fast enough
(~20-45µs typical, per the sampled records) that if the writer's own bthread is
descheduled for even a few microseconds *between* completing the syscall and
executing the `LT_STAMP(WRITE_END)` call, the response can already have arrived and
been stamped by a different, already-running thread first. The counter reading each
stamp records is taken at the moment `Stamp()`/`StampAt()` actually executes, not at
the moment the underlying kernel event happened — so a delayed stamp call, not a
misplaced one, is a plausible explanation. This is unconfirmed; distinguishing it from
a genuine stamp-placement bug (the design doc's other listed cause of this exact
signature) needs more repro data than one occurrence.

**Frequency**: 1/3288 (~0.03%) in Run 1, 0/3169 in Run 2. Consistent with something
that depends on scheduler timing rather than a deterministic code path — expect it to
recur at a similarly low, non-zero rate on reruns, not to vanish permanently. It should
not be waved off as noise without more data: the design doc treats this exact signature
as diagnostic of a real embedding-position bug, and one clean run does not clear that.

**Recommendation for whoever picks this up next**: rerun `capture.sh` at higher volume
(tens of thousands of requests) specifically targeting this pair of points, and/or add
a targeted stress test that maximizes scheduler jitter around the client's write-then-
wait transition, before deciding whether this is expected jitter inherent to
per-thread stamping or an actual embedding-position defect. Task B's join code (§10.1)
should count this class of violation explicitly rather than silently drop the record,
per the plan's B2 instruction.

## Reproducing

```
LT_BUILD_DIR=brpc-lt-traced ./tools/latency_trace/capture.sh /tmp/lt_capture_out
hexdump -C /tmp/lt_capture_out/server.dump | head -8
hexdump -C /tmp/lt_capture_out/client.dump | head -8
```

## Run 3: committed fixture for `test_merge.py` (Task B)

**Date:** 2026-09-01, same session as Task B (`merge.py`). Run 2's dump files lived
under `/tmp` on `suzhou950` and did not survive to this task (`/tmp` is not
persistent on that host) -- `merge.py`'s test suite needs bytes it can load without
SSH access to a remote build host, so this run's dumps are committed at
`tools/latency_trace/testdata/{client,server}.dump` (618784 bytes each) rather than
regenerated on demand. Produced by the exact `capture.sh` invocation in
"Reproducing" above, same build (`latency-trace` branch, traced config, aarch64
`suzhou950`), same workload (`echo_c++`, one channel, sequential blocking calls,
`outstanding=1`) -- only the record count differs from Run 2, which is expected:
`capture.sh` stops the client once it has logged `CAPTURE_REQUESTS` (default 3000)
responses, and the exact count that accumulates before the polling loop notices and
sends SIGINT depends on scheduling, not a fixed target. Do not expect a future
re-run to reproduce 3222 exactly; expect it to reproduce the *properties* below.

| | server.dump | client.dump |
|---|---|---|
| file size | 618784 bytes | 618784 bytes |
| `record_count` | 3222 | 3222 |
| `dropped_count` | 0 | 0 |
| `counter_freq_hz` | 100000023.19 Hz | 100000049.54 Hz |
| `cntfrq_el0_hz` | 100000000 Hz | 100000000 Hz |
| `process_tag` | 3710456675 (0xdd162d63) | 1233189988 (0x498...; low 32 of every client trace_id) |
| `method_table_offset` | 618752 | 618752 |

Method table: both files, `count=1`, `{1: "example.EchoService.Echo"}` -- matches
Run 2.

**Join**: client ids 3222, server ids 3222, joined 3222, only-in-client 0,
only-in-server 0 -- **100.0000% join both directions**, same as Run 2.

**`merge.py`'s four sec.10.1 assertions, applied to all 3222 joined pairs**:

| rejection reason | count |
|---|---|
| `unstamped_point` | 0 |
| `non_monotonic` | 0 |
| `negative_decomposition_item` | 5 |
| `negative_rtt_late_write_end_stamp` | 0 |

3217/3222 (99.845%) accepted into statistics. The 5 rejects are **not** the
`C09 < C08` anomaly Run 1 surfaced (that bucket is 0 here, consistent with Run 2's
0/3169) -- they are records where `link_total = RTT - S` is slightly negative (range
-6000ns to -820ns, i.e. sub-6-microsecond), which design doc sec.6.1 explicitly
anticipates as a real, unclipped outcome of measuring two independently-clocked
hosts' round trip and one-way span this way, not a bug. See
`.superpowers/sdd/2026-08-29-latency-trace-instrumentation/task-b-report.md` for
the per-record detail and for a real merge.py bug this run caught (model B's
sliding-window offset estimator was anchoring on one of these same 5
already-rejected records before the fix).

One additional real finding: the very first request (`trace_id` low32 `0`) has
`e2e_ns` ≈ 7.06ms against a median around 30-80µs for the rest -- two orders of
magnitude higher. The decomposition attributes essentially all of it to a single
item, `srv_dispatch` (S06→S07, "查 service/method、并发限制、建 Controller") at
≈5.82ms. Not investigated further here (out of Task B's scope, which is the merger,
not the runtime) but worth flagging for whoever looks at cold-start/warm-up
behavior next: this smells like a one-time lookup or lock cost on the very first
request to hit a freshly-started service, not steady-state dispatch cost.
