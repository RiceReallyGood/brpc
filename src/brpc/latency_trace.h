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
};

}  // namespace brpc

#endif  // BRPC_LATENCY_TRACE_H
