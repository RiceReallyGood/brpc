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

#include <gtest/gtest.h>
#include <type_traits>
#include "brpc/latency_trace.h"
#include "butil/time.h"

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

#include <gflags/gflags.h>

// DEFINE_bool/DEFINE_int32 for these flags live inside `namespace brpc` in
// latency_trace.cpp (matching the rest of this codebase, e.g. socket.cpp),
// so DECLARE must be nested the same way -- otherwise it mangles to the
// wrong symbol (::fLB::FLAGS_... instead of brpc::fLB::FLAGS_...) and the
// link fails with an undefined reference.
namespace brpc {
DECLARE_bool(latency_trace_enabled);
DECLARE_int32(latency_trace_capacity);
}  // namespace brpc

namespace {

class LatencyTraceBufferTest : public ::testing::Test {
protected:
    void SetUp() override {
        brpc::FLAGS_latency_trace_enabled = true;
        brpc::LatencyTraceBuffer::instance()->ResetForTest(64);
    }
    void TearDown() override {
        brpc::FLAGS_latency_trace_enabled = false;
    }
};

TEST_F(LatencyTraceBufferTest, DisabledReturnsInvalidHandle) {
    brpc::FLAGS_latency_trace_enabled = false;
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
    //
    // This exercises the generation guard specifically, which is orthogonal
    // to the "stop when full" production default (that's covered by
    // StopsRecordingWhenFullInsteadOfOverwriting below): with the default
    // stop-on-full policy nothing past the per-shard capacity ever gets
    // written, so `stale`'s slot would never actually be recycled and this
    // test would be checking nothing. Force wraparound explicitly so the
    // slot really gets reused and we can prove the guard rejects the old
    // handle instead of letting it corrupt the new occupant.
    brpc::LatencyTraceBuffer* b = brpc::LatencyTraceBuffer::instance();
    b->set_stop_when_full(false);
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
