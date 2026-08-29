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
