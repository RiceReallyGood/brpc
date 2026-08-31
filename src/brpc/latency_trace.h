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
#include <string>
#include "butil/atomicops.h"
#include "butil/compiler_specific.h"

namespace brpc {

// Timestamp points of one RPC. Order matches the timeline; the analysis
// tool treats every adjacent pair as one decomposition item, so the order
// here IS the decomposition and must stay monotonic in wall-clock terms.
// See docs/superpowers/specs/2026-08-29-brpc-latency-trace-design.md sec.4
enum LatencyTracePoint {
    // ---- client, 17 points ----
    // D14: rpc_end lands at the same instant brpc calls Controller::
    // OnRPCEnd() -- there used to be two more client points here
    // (rsp_process_start/end, bracketing the user callback), deleted by
    // design doc sec.3.3/D14 because brpc itself does not count the
    // callback as RPC latency. Do not re-add them; re-derive the client
    // count (17, not 19) and every downstream offset (LT_S_WAKE onward,
    // LT_POINT_COUNT, LatencyTraceRecord's size) from this enum instead of
    // a stale comment if this ever changes again.
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

// Generates a trace id unique across processes: the high 32 bits are
// LatencyTraceBuffer's per-process tag (never zero, so two untagged
// processes can't collide; also what LatencyTraceFileHeader::process_tag
// records -- there is exactly one tag, reused for both purposes), the
// low 32 bits are `seq` -- this function does not read or advance any
// LatencyTraceBuffer::Shard::cursor. `seq` only needs to be unique
// within this process; see the .cpp for why its wraparound is safe.
//
// Every caller MUST source `seq` from NextLatencyTraceSeq() below, never
// from a counter of its own: two independent counters both starting near
// zero (e.g. one for a channel's first attempt, another for a retry or
// backup-request attempt) would each feed this function low bits that
// can coincide, producing two DIFFERENT physical attempts with the SAME
// trace id -- exactly the collision the whole point of this id is to
// prevent. See NextLatencyTraceSeq()'s comment.
uint64_t MakeLatencyTraceId(uint64_t seq);

// The single process-wide sequence counter backing every call to
// MakeLatencyTraceId() -- the client's first-attempt allocation
// (channel.cpp) and a retry/backup-request attempt's allocation
// (controller.cpp's IssueRPC) both call this, never a counter of their
// own, so the two call sites can never hand out the same `seq` and
// therefore can never mint the same trace id. `seq` wraps after ~4B
// calls from this process; see MakeLatencyTraceId's .cpp comment for why
// that wraparound is safe.
uint64_t NextLatencyTraceSeq();

// Interns `full_name` into a process-wide, thread-safe string->id table and
// returns the id -- stable for the life of the process, and the same string
// always maps back to the same id. Ids are assigned in first-seen order
// starting at 1; 0 is never assigned (LatencyTraceRecord::method_id defaults
// to 0, meaning "no method identified", so this table's id space cannot
// collide with that default). `method_id` alone is meaningless offline
// (it's just a small integer in a fixed-size record) -- LatencyTraceBuffer::
// Dump() appends this table's full contents after the records, and stores
// its byte offset in LatencyTraceFileHeader::method_table_offset, so an
// offline reader can resolve an id back to the name that produced it.
uint32_t LatencyTraceMethodId(const std::string& full_name);

// Opaque reference to one slot in the ring buffer.
// Layout: (shard << 56) | seq. Zero means "not tracing".
typedef uint64_t LatencyTraceHandle;
const LatencyTraceHandle LT_INVALID_HANDLE = 0;

// ts[] reserves three sentinel values out of its uint32 range (see
// LatencyTraceRecord::ts and design doc sec.8.1's table):
//   0          -- never stamped
//   0xFFFFFFFF -- this point does not exist in this record's mode (e.g.
//                 the RDMA polling-mode receive points; Task 12 writes
//                 this one, this task only reserves the name)
//   0xFFFFFFFE -- offset saturated (a real interval too long -- roughly
//                 42.9s at 100MHz -- for uint32 to hold); see EncodeOffset
//                 in the .cpp for why saturation must land here and not on
//                 0xFFFFFFFF, which means something categorically
//                 different ("not applicable" vs. "this took forever").
const uint32_t LT_TS_NOT_APPLICABLE = 0xFFFFFFFFu;

// Sentinel a caller passes as StampAt()'s `raw_counter` to mean "this
// point does not exist under the current mode" -- design doc sec.8.5:
// under RDMA's polling mode (-rdma_use_polling) there is neither an
// epoll wake-up nor an OnEdge bthread switch, so Socket::_lt_wake and
// Socket::_lt_onedge_start (both raw uint64_t counter samples, not yet
// encoded ts[] offsets) have no real value to hold. StampAt() recognizes
// this exact 64-bit value and writes LT_TS_NOT_APPLICABLE into ts[point]
// verbatim instead of computing offset = raw_counter - base_counter from
// it. All ones rather than (uint64_t)LT_TS_NOT_APPLICABLE (which would
// only occupy the low 32 bits): a real clock_cycles() reading equal to
// that smaller value is a plausible (if early) counter state -- roughly
// 43 seconds after boot at 100MHz -- so it must not be reinterpreted as
// this sentinel. All 64 bits set is not a real counter reading on any
// timescale this project cares about.
const uint64_t LT_RAW_NOT_APPLICABLE = ~static_cast<uint64_t>(0);

// Fixed-size POD, 192 bytes. Timestamps encode counter deltas (offset+1;
// see `ts` below) relative to `base_counter`; conversion to nanoseconds
// happens offline.
struct LatencyTraceRecord {
    uint64_t trace_id;       // unique across processes
    // Origin of this record's timeline -- NOT necessarily "the moment
    // the slot was allocated". Supplied by the caller of AllocSlot (see
    // its two overloads): on the client the two coincide (AllocSlot runs
    // at Channel::CallMethod's entry, immediately before C01), but the
    // server can only allocate its slot once ProcessRpcRequest has
    // parsed the metadata and recovered the trace id -- well after
    // S01-S04 (the epoll wake-up through the message being cut out of
    // the read buffer) already happened -- so the server passes the
    // wake timestamp it already parked on Socket instead. Every point on
    // a correctly-instrumented record has a raw counter value >=
    // base_counter.
    uint64_t base_counter;
    // Generation guard, see LatencyTraceBuffer. Atomic so AllocSlot can
    // publish it with release semantics and Get() can pair that with an
    // acquire load -- verified to keep this struct exactly 192 bytes and
    // trivially copyable (butil::atomic<uint64_t> matches uint64_t in both
    // size and object representation when lock-free, which it is here).
    butil::atomic<uint64_t> slot_seq;
    // Encodes offset+1 relative to base_counter, NOT the raw offset --
    // 0 unambiguously means "never stamped". A raw offset would not do:
    // AllocSlot and the C01 stamp are adjacent statements, and the
    // counter ticks once per ~10ns, so ts[LT_C_RPC_START] == 0 (offset
    // exactly 0) is a likely outcome of a perfectly correct capture on
    // the client, which would collide with the "not stamped" sentinel.
    //
    // Full decode rule -- three reserved values, everything else is
    // "offset + 1" (design doc sec.8.1/9.1 has the authoritative table):
    //   0          -- never stamped. Skip the point; do NOT treat as
    //                 offset 0.
    //   0xFFFFFFFF -- LT_TS_NOT_APPLICABLE: this point does not exist
    //                 under this record's mode (e.g. RDMA polling-mode
    //                 receive points, design doc sec.8.5). Not a
    //                 subtrahend candidate either -- do NOT decrement.
    //   0xFFFFFFFE -- offset saturated: the real interval is >= the
    //                 uint32 offset range (~42.9s at 100MHz). Treat as a
    //                 lower bound, not an exact value. Do NOT decrement.
    //   anything else -- real offset is (value - 1). Consumers --
    //                 including merge.py -- must subtract 1 to recover
    //                 it, and must do so ONLY for this last case: both
    //                 sentinels above are non-zero and must NOT be
    //                 decremented, or 0xFFFFFFFF..0xFFFFFFFE silently
    //                 becomes 0xFFFFFFFE..0xFFFFFFFD -- wrong on its own,
    //                 and it also collides the two sentinels' meanings
    //                 ("does not exist" vs. "took ~42.9s or longer").
    // See LatencyTraceBuffer::Stamp()/StampAt() for the encoding side.
    uint32_t ts[LT_POINT_COUNT];
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

// LatencyTraceRecord is ~99% of every dump's bytes and, like the file
// header above, is parsed off disk verbatim by merge.py. Unlike the file
// header, it previously had no compile-time size pin -- only a runtime
// ASSERT_EQ in a unit test that exists solely in traced (BRPC_LATENCY_TRACE)
// builds, so a size regression would only show up if that specific test
// build happened to run. Pin it the same way as the header, unconditionally.
static_assert(sizeof(LatencyTraceRecord) == 192,
              "LatencyTraceRecord is an on-disk format merge.py parses "
              "verbatim -- it must stay exactly 192 bytes");

// Spells "BRPLTRC1" -- 8 bytes, no 'C' after "BRP" -- when read as the
// big-endian byte sequence that literal actually encodes. This file
// format is little-endian (matches every target this module ships on:
// x86_64 and aarch64), so the bytes actually on disk, in file order, are
// "1CRTLPRB". A parser must byteswap this constant (or compare against
// b'1CRTLPRB', never b'BRPCLTRC1') on an LE host.
const uint64_t LT_FILE_MAGIC = 0x4252504C54524331ULL;

// 128 bytes, fixed. merge.py parses this verbatim.
struct LatencyTraceFileHeader {
    uint64_t magic;
    uint32_t record_size;
    uint32_t point_count;
    // Empirical calibration: freq = (tail_counter - head_counter)
    //                             * 1e9 / (tail_monotonic_ns - head_monotonic_ns)
    // head_monotonic_ns/tail_monotonic_ns hold CLOCK_MONOTONIC (boot-relative
    // on this host, NOT comparable across hosts). That is deliberate, not a
    // bug: computing a rate from two same-host samples is exactly what
    // CLOCK_MONOTONIC is for, and it is immune to wall-clock adjustments
    // (NTP steps, etc.) that would corrupt this calibration.
    // head_realtime_ns/tail_realtime_ns below hold CLOCK_REALTIME (wall
    // clock, Epoch-relative) purely so an offline tool can sanity-check two
    // dumps from two different hosts against each other -- do not use them
    // for the frequency calibration above.
    uint64_t head_counter;
    int64_t  head_monotonic_ns;
    uint64_t tail_counter;
    int64_t  tail_monotonic_ns;
    int64_t  head_realtime_ns;    // CLOCK_REALTIME; cross-host sanity check only
    int64_t  tail_realtime_ns;    // CLOCK_REALTIME; cross-host sanity check only
    double   counter_freq_hz;     // authoritative, empirically measured
    uint64_t cntfrq_el0_hz;       // cross-check only; 0 on non-aarch64
    uint64_t record_count;
    uint64_t dropped_count;
    uint64_t process_tag;         // random per process, high bits of trace_id
    // Byte offset (from the start of the file) of the method-name table:
    // a uint32 `count`, followed by `count` entries of uint32 `len` + `len`
    // raw bytes (no trailing NUL). Entry i (0-based) names the method whose
    // LatencyTraceRecord::method_id == i + 1 -- id 0 is never assigned (see
    // LatencyTraceMethodId), so it is intentionally absent from this table.
    uint64_t method_table_offset;
    char     padding[128 - 112];
};

// merge.py parses this struct byte-for-byte off disk; a silent size or
// layout change (a field's type changed, a member got reordered, etc.)
// would corrupt every dump file written after that point without
// tripping any test that only round-trips the header through its own
// sizeof(). Pin the contract at compile time as well.
static_assert(sizeof(LatencyTraceFileHeader) == 128,
              "LatencyTraceFileHeader is an on-disk format merge.py "
              "parses verbatim -- it must stay exactly 128 bytes");

// Sharded ring buffer of records. One shard per group of workers keeps
// the allocation cursor off a single contended cacheline in the common
// (uncontended) case.
//
// Fix-round item 1: -latency_trace_capacity must mean what it says even
// for a single-threaded producer (a synchronous benchmark client is this
// project's own motivating use case -- design doc sec.1.1), not just for
// a server whose many worker threads spread across all SHARD_COUNT
// shards. A thread is pinned to one "home" shard for life (AllocSlot's
// tls_shard), so before this fix a single-threaded caller could only
// ever fill 1/SHARD_COUNT of the configured capacity before every
// further call was dropped, even though the other 15/16 of the buffer
// sat empty -- see task-a2-report.md's "What was run" for the real
// capture that surfaced this (100000 requested, 8192 usable). AllocSlot
// now falls back to another shard once the caller's home shard is full,
// and only refuses (and counts a drop) once every shard is full. See
// AllocSlot's own comment for the mechanism and TryReserveSlot for why
// the fallback path needs a bounded reservation instead of a plain
// fetch_add.
class LatencyTraceBuffer {
public:
    static const int SHARD_COUNT = 16;   // power of two
    static const int SHARD_SHIFT = 56;

    static LatencyTraceBuffer* instance();

    // Returns LT_INVALID_HANDLE when tracing is off or every shard is
    // full (see the class comment for the home-shard-then-fallback
    // policy -- "the buffer is full" now means ALL shards are, not just
    // the caller's own). Samples the counter itself as this record's
    // timeline origin (base_counter) -- the client's case, where
    // allocation and origin coincide. See the 3-argument overload for
    // the server's case.
    LatencyTraceHandle AllocSlot(uint64_t trace_id, LatencyTraceRole role);

    // Same, but the caller supplies base_counter explicitly instead of
    // "now". Needed on the server: AllocSlot can only run once
    // ProcessRpcRequest has parsed the metadata and recovered the trace
    // id, well after S01-S04 (the epoll wake-up through the message
    // being cut out of the read buffer) already happened, so the server
    // passes the wake timestamp it already parked on Socket -- keeping
    // base_counter at or before every point actually observed for this
    // record, rather than after some of them.
    LatencyTraceHandle AllocSlot(uint64_t trace_id, LatencyTraceRole role,
                                  uint64_t base_counter);

    // Silently ignores an invalid or stale handle. First-write-wins: if
    // ts[point] already holds a non-zero value, this call is a no-op. Use
    // for points that are instantaneous events -- a second write means
    // the point fired twice, which is a bug, and the first (correct) one
    // must not be clobbered. See StampLast() for the opposite policy and
    // design doc sec.10.1 for which policy applies to which point.
    void Stamp(LatencyTraceHandle h, int point);

    // Like Stamp(), but last-write-wins: unlike Stamp(), a call here
    // always overwrites whatever `ts[point]` already holds. For points
    // whose meaning is "the start of the operation that actually
    // completed this unit" rather than "an instantaneous event" --
    // currently write_start (Socket::DoWrite, re-entered by KeepWrite for
    // every retried writev) and readv_start (InputMessenger::OnNewMessages,
    // re-entered on every DoRead of the same still-incomplete message).
    // A unit sitting behind others queued ahead of it sees the operation
    // attempted repeatedly; only the LAST attempt is the one that actually
    // moved *this* unit's bytes, so it is the correct boundary between
    // "queueing" and "syscall" time for this unit -- see design doc
    // sec.10.1. Silently ignores an invalid or stale handle, same as
    // Stamp().
    void StampLast(LatencyTraceHandle h, int point);

    // Like Stamp(), but for a timestamp that was already sampled earlier
    // (e.g. a Socket-level wake-up copied into an InputMessageBase before
    // any LatencyTraceHandle for it existed) rather than "now". Encodes
    // `raw_counter - record->base_counter` into ts[point] (see
    // LatencyTraceRecord::ts for the offset+1 encoding), through the
    // same generation-guarded Get() and the same first-write-wins rule
    // as Stamp(). `raw_counter` predating `base_counter` should no
    // longer happen in normal operation now that AllocSlot's 3-argument
    // overload lets the server pick a base_counter at or before every
    // point on its record -- if it still does (e.g. a clock-read
    // reordering), the offset is clamped to 0 rather than left to wrap
    // around; see the .cpp for why that clamp target is 0, not the
    // unstamped sentinel.
    void StampAt(LatencyTraceHandle h, int point, uint64_t raw_counter);

    // Returns nullptr when the handle is stale.
    LatencyTraceRecord* Get(LatencyTraceHandle h);
    LatencyTraceRecord* GetForTest(LatencyTraceHandle h) { return Get(h); }

    // Iterates every written slot across all shards by a flat index, so a
    // test can find records without knowing the sharding. Returns nullptr
    // once `global_seq` is past the last written slot. Walks shards in
    // FIXED index order (0, 1, ..., SHARD_COUNT-1) -- with the fix-round
    // item 1 fallback, that is NOT necessarily allocation order for a
    // single-threaded producer whose "home" shard isn't shard 0 (its
    // overflow can land in lower-numbered shards this walk visits
    // first). What stays true, and is all this function's callers may
    // rely on: each shard's written slots are still exactly its first
    // `written` entries with no gaps and no wraparound (see AllocSlot's
    // fallback comment for why), so every written record is visited
    // exactly once -- just not necessarily in the order it was recorded
    // once fallback has spread one thread's records across shards. A
    // caller that needs "most recent" should key off something in the
    // record itself (e.g. trace_id) rather than this traversal order --
    // see this file's end-to-end tests for that pattern.
    const LatencyTraceRecord* GetBySeqForTest(uint64_t global_seq) const;

    size_t recorded_count() const;
    size_t dropped_count() const;

    // The single per-process tag: written into every dumped file's header
    // (LatencyTraceFileHeader::process_tag) and reused, via
    // MakeLatencyTraceId(), as the high 32 bits of every trace id this
    // process produces. There is exactly one tag -- do not mint a second,
    // independent one for either purpose.
    uint32_t process_tag() const { return (uint32_t)_process_tag; }

    // DANGEROUS outside tests: "stop when full, never overwrite" is what
    // keeps both ends of a distributed trace on the same earliest-N
    // window so an offline join lines up. Flipping this to false makes
    // the buffer wrap and overwrite instead, which (a) breaks that
    // joinability guarantee and (b) is the only way to exercise the
    // generation-guard/recycling path in a test -- there is no
    // production caller for this and there should not be one.
    void set_stop_when_full_for_test(bool v) { _stop_when_full = v; }

    // Test-only: re-create the buffer with `capacity` slots in total.
    void ResetForTest(int capacity);

    // Writes a LatencyTraceFileHeader followed by every written record
    // (across all shards) to `path`. Returns the number of records
    // written, or -1 on I/O failure.
    int Dump(const char* path);

    // Saves `path` and registers an atexit hook that calls Dump(path) when
    // the process exits. Idempotent: only the first call registers the
    // hook. Called automatically from the constructor when
    // -latency_trace_dump_path is non-empty.
    void EnableDumpOnExit(const char* path);

    bool atexit_registered() const { return _atexit_registered; }

private:
    LatencyTraceBuffer();
    ~LatencyTraceBuffer();

    // The lone atexit callback: reaches back into the singleton to find
    // the path EnableDumpOnExit stashed, since atexit callbacks take no
    // arguments.
    static void DumpAtExitCallback();

    struct Shard {
        butil::atomic<uint64_t> cursor;
        // In-class initializer matters: LatencyTraceBuffer's constructor
        // calls ResetForTest(), whose first act per shard is `delete[]
        // records`. Without this, `records` is indeterminate on the very
        // first call and that delete corrupts the heap.
        LatencyTraceRecord* records = nullptr;
        uint64_t mask = 0;
        char padding[BAIDU_CACHELINE_SIZE];
    };

    // Bounded fetch-and-increment: reserves the next slot in `sh` (i.e.
    // returns its pre-increment cursor value in `*seq` and advances the
    // cursor by one) ONLY if doing so keeps the cursor under `capacity`;
    // returns false, leaving `sh` untouched, once the shard is already
    // full. Used for a fallback target in AllocSlot -- unlike the
    // caller's own home shard, which the fast path always increments
    // unconditionally via a plain fetch_add (see AllocSlot), a fallback
    // target can be raced by unrelated threads: other overflowing
    // threads probing the same candidate, and -- if `sh` is some OTHER
    // thread's home shard that has not filled yet -- that thread's own
    // unconditional fast-path fetch_add too. Mixing a plain fetch_add
    // and this bounded compare-exchange on the same atomic is safe
    // (every successful increment, by either mechanism, atomically
    // claims a distinct, contiguous cursor value with no gaps -- see
    // AllocSlot's fallback comment) but only THIS bounded form can
    // refuse once the shard is full instead of overshooting `capacity`
    // and corrupting the "written slots are exactly the first N, no
    // wraparound" invariant GetBySeqForTest and Dump() depend on.
    static bool TryReserveSlot(Shard& sh, int capacity, uint64_t* seq);

    Shard _shards[SHARD_COUNT];
    butil::atomic<uint64_t> _dropped;
    bool _stop_when_full;
    int _per_shard_capacity;

    // Calibration anchor, sampled once at construction. Dump() pairs this
    // with a freshly-sampled tail (counter, monotonic) to compute an
    // empirical counter frequency -- see Dump()'s comment for why the
    // window has a 100ms minimum. CLOCK_MONOTONIC, not CLOCK_REALTIME:
    // this is a same-host rate calculation and must not be perturbed by
    // wall-clock adjustments. See LatencyTraceFileHeader for the
    // CLOCK_REALTIME pair recorded purely for cross-host sanity checks.
    uint64_t _head_counter;
    int64_t _head_monotonic_ns;
    // CLOCK_REALTIME sampled adjacent to _head_monotonic_ns, purely for
    // LatencyTraceFileHeader's cross-host sanity-check pair -- never used
    // in the frequency calibration, which stays on _head_monotonic_ns.
    int64_t _head_realtime_ns;
    uint64_t _process_tag;

    bool _atexit_registered;
    std::string _dump_path;
};

}  // namespace brpc

// Compile-time switch. When BRPC_LATENCY_TRACE is not defined, LT_STAMP
// and LT_ALLOC vanish to no-ops, struct layouts affected by the switch
// (Socket::WriteRequest, RpcMeta's request-carrier wrapper, etc.) revert
// to their pre-feature sizes -- e.g. sizeof(Socket::WriteRequest) == 64 --
// and every hot path is identical to an untraced build: zero stamps
// execute. This is NOT a claim that the default library is byte-for-byte
// identical to a pre-feature build. `latency_trace.cpp` has no top-level
// `#if defined(BRPC_LATENCY_TRACE)` guard and is swept up by the default
// build's `src/brpc/*.cpp` glob, so the module still links into every
// default build, registers its (inert) gflags -- visible, doing nothing,
// in every server's /flags -- and `RpcRequestMeta` still gains its
// `latency_trace_id` field in every build, traced or not.
#if defined(BRPC_LATENCY_TRACE)
#define LT_STAMP(handle, point)                                    \
    ::brpc::LatencyTraceBuffer::instance()->Stamp((handle), (point))
// Last-write-wins counterpart of LT_STAMP() -- see
// LatencyTraceBuffer::StampLast()'s comment for which points use this
// instead of LT_STAMP().
#define LT_STAMP_LAST(handle, point)                                \
    ::brpc::LatencyTraceBuffer::instance()->StampLast((handle), (point))
#define LT_ALLOC(trace_id, role)                                   \
    ::brpc::LatencyTraceBuffer::instance()->AllocSlot((trace_id), (role))
#else
#define LT_STAMP(handle, point) ((void)0)
#define LT_STAMP_LAST(handle, point) ((void)0)
#define LT_ALLOC(trace_id, role) (::brpc::LT_INVALID_HANDLE)
#endif

#endif  // BRPC_LATENCY_TRACE_H
