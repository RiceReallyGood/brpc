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

// Generates a trace id unique across processes: the high 32 bits are
// LatencyTraceBuffer's per-process tag (never zero, so two untagged
// processes can't collide; also what LatencyTraceFileHeader::process_tag
// records -- there is exactly one tag, reused for both purposes), the
// low 32 bits are `seq`, a counter owned entirely by the caller (e.g.
// channel.cpp's `s_lt_seq`) -- this function does not read or advance
// any LatencyTraceBuffer::Shard::cursor. `seq` only needs to be unique
// within this process; see the .cpp for why its wraparound is safe.
uint64_t MakeLatencyTraceId(uint64_t seq);

// Opaque reference to one slot in the ring buffer.
// Layout: (shard << 56) | seq. Zero means "not tracing".
typedef uint64_t LatencyTraceHandle;
const LatencyTraceHandle LT_INVALID_HANDLE = 0;

// Fixed-size POD, 200 bytes. Timestamps encode counter deltas (offset+1;
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
    // acquire load -- verified to keep this struct exactly 200 bytes and
    // trivially copyable (butil::atomic<uint64_t> matches uint64_t in both
    // size and object representation when lock-free, which it is here).
    butil::atomic<uint64_t> slot_seq;
    // Encodes offset+1 relative to base_counter, NOT the raw offset --
    // 0 unambiguously means "never stamped". A raw offset would not do:
    // AllocSlot and the C01 stamp are adjacent statements, and the
    // counter ticks once per ~10ns, so ts[LT_C_RPC_START] == 0 (offset
    // exactly 0) is a likely outcome of a perfectly correct capture on
    // the client, which would collide with the "not stamped" sentinel.
    // Consumers -- including merge.py -- must subtract 1 from a non-zero
    // entry to recover the real offset. See LatencyTraceBuffer::Stamp()/
    // StampAt() (and design doc sec.8.1) for the full reasoning.
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

// merge.py parses this struct byte-for-byte off disk; a silent size or
// layout change (a field's type changed, a member got reordered, etc.)
// would corrupt every dump file written after that point without
// tripping any test that only round-trips the header through its own
// sizeof(). Pin the contract at compile time as well.
static_assert(sizeof(LatencyTraceFileHeader) == 128,
              "LatencyTraceFileHeader is an on-disk format merge.py "
              "parses verbatim -- it must stay exactly 128 bytes");

// Sharded ring buffer of records. One shard per group of workers keeps
// the allocation cursor off a single contended cacheline.
class LatencyTraceBuffer {
public:
    static const int SHARD_COUNT = 16;   // power of two
    static const int SHARD_SHIFT = 56;

    static LatencyTraceBuffer* instance();

    // Returns LT_INVALID_HANDLE when tracing is off or the buffer is
    // full. Samples the counter itself as this record's timeline origin
    // (base_counter) -- the client's case, where allocation and origin
    // coincide. See the 3-argument overload for the server's case.
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
    // ts[point] already holds a non-zero value, this call is a no-op.
    void Stamp(LatencyTraceHandle h, int point);

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
    // once `global_seq` is past the last written slot.
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

    Shard _shards[SHARD_COUNT];
    butil::atomic<uint64_t> _dropped;
    bool _stop_when_full;
    int _per_shard_capacity;

    // Calibration anchor, sampled once at construction. Dump() pairs this
    // with a freshly-sampled tail (counter, realtime) to compute an
    // empirical counter frequency -- see Dump()'s comment for why the
    // window has a 100ms minimum.
    uint64_t _head_counter;
    int64_t _head_realtime_ns;
    uint64_t _process_tag;

    bool _atexit_registered;
    std::string _dump_path;
};

}  // namespace brpc

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

#endif  // BRPC_LATENCY_TRACE_H
