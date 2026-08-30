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
#include <atomic>
#include <cstdio>
#include <thread>
#include <type_traits>
#include <unistd.h>
#include <vector>
#include "brpc/channel.h"
#include "brpc/closure_guard.h"
#include "brpc/latency_trace.h"
#include "brpc/policy/baidu_rpc_meta.pb.h"
#include "brpc/server.h"
#include "butil/time.h"
#include "echo.pb.h"

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

TEST_F(LatencyTraceBufferTest, StampIsFirstWriteWins) {
    // The property design doc sec.10.1 requires: a point already holding
    // a non-zero value is left alone, and a later write is discarded.
    // This is the fix for a real bug (see fix-round-q1q6 item 2): DoWrite
    // calls LT_STAMP(write_start) unconditionally on every KeepWrite
    // iteration, so without this rule a request needing more than one
    // writev would silently record the *last* attempt's write_start
    // instead of the first, moving real queueing time into the syscall
    // bucket.
    brpc::LatencyTraceBuffer* b = brpc::LatencyTraceBuffer::instance();
    const brpc::LatencyTraceHandle h = b->AllocSlot(8, brpc::LT_ROLE_CLIENT);
    b->Stamp(h, brpc::LT_C_WRITE_START);
    const uint32_t first = b->GetForTest(h)->ts[brpc::LT_C_WRITE_START];

    // Busy-wait so a second Stamp() call would, if it were not discarded,
    // observe a strictly later (and thus different) counter delta.
    const uint64_t start = butil::detail::clock_cycles();
    while (butil::detail::clock_cycles() - start < 1000) {}
    b->Stamp(h, brpc::LT_C_WRITE_START);
    ASSERT_EQ(first, b->GetForTest(h)->ts[brpc::LT_C_WRITE_START])
        << "second Stamp() call must be discarded, not overwrite the first";
}

TEST_F(LatencyTraceBufferTest, StampAtWritesHistoricalDeltaIntoRecord) {
    // What Task 9 actually delivers: a primitive that can backfill a
    // timestamp sampled earlier (e.g. a Socket-level wake-up copied into
    // an InputMessageBase before any LatencyTraceHandle for it existed)
    // rather than "now", by taking the raw counter value directly.
    // ts[] stores offset+1 (see LatencyTraceRecord::ts), so a 12345-tick
    // offset reads back as 12346.
    brpc::LatencyTraceBuffer* b = brpc::LatencyTraceBuffer::instance();
    const brpc::LatencyTraceHandle h = b->AllocSlot(1, brpc::LT_ROLE_SERVER);
    ASSERT_NE(brpc::LT_INVALID_HANDLE, h);
    ASSERT_EQ(0u, b->GetForTest(h)->ts[brpc::LT_S_WAKE]);

    const uint64_t base = b->GetForTest(h)->base_counter;
    b->StampAt(h, brpc::LT_S_WAKE, base + 12345);
    ASSERT_EQ(12346u, b->GetForTest(h)->ts[brpc::LT_S_WAKE]);
}

TEST_F(LatencyTraceBufferTest, StampAtIsFirstWriteWins) {
    // Same rule as Stamp() (see design doc sec.10.1), and it has to hold
    // regardless of which of the two APIs makes the first write. Encoded
    // value is offset+1, so a 100-tick offset reads back as 101.
    brpc::LatencyTraceBuffer* b = brpc::LatencyTraceBuffer::instance();
    const brpc::LatencyTraceHandle h = b->AllocSlot(2, brpc::LT_ROLE_SERVER);
    const uint64_t base = b->GetForTest(h)->base_counter;

    b->StampAt(h, brpc::LT_S_WAKE, base + 100);
    ASSERT_EQ(101u, b->GetForTest(h)->ts[brpc::LT_S_WAKE]);
    b->StampAt(h, brpc::LT_S_WAKE, base + 999);   // later StampAt: discarded
    ASSERT_EQ(101u, b->GetForTest(h)->ts[brpc::LT_S_WAKE]);
    b->Stamp(h, brpc::LT_S_WAKE);                 // later Stamp: also discarded
    ASSERT_EQ(101u, b->GetForTest(h)->ts[brpc::LT_S_WAKE]);
}

TEST_F(LatencyTraceBufferTest, StampAtRejectsStaleHandle) {
    // Mirrors StaleHandleIsRejectedAfterWrapAround below, for StampAt: a
    // handle whose slot has been recycled into a different generation
    // must be silently ignored, not written through into the new
    // occupant's record.
    brpc::LatencyTraceBuffer* b = brpc::LatencyTraceBuffer::instance();
    b->set_stop_when_full_for_test(false);
    const brpc::LatencyTraceHandle stale = b->AllocSlot(3, brpc::LT_ROLE_SERVER);
    ASSERT_NE(brpc::LT_INVALID_HANDLE, stale);
    for (int i = 0; i < 64; ++i) {
        b->AllocSlot(1000 + i, brpc::LT_ROLE_SERVER);
    }
    ASSERT_EQ(nullptr, b->GetForTest(stale));     // handle no longer valid
    b->StampAt(stale, brpc::LT_S_WAKE, 42);       // must be a silent no-op
}

TEST_F(LatencyTraceBufferTest, StampAtClampsPreBaseCounterValueToOffsetZero) {
    // A historical sample can still predate base_counter in rare cases
    // (e.g. a clock-read reordering or other instrumentation skew) even
    // though AllocSlot's 3-argument overload now lets the caller choose
    // base_counter to be at or before every point on the record's
    // timeline (see fix-round item 1) -- so this clamp is a guard
    // against an anomaly, not the server's normal path anymore.
    // raw_counter - base_counter would underflow as unsigned arithmetic
    // and produce a huge, bogus-looking positive delta; verify StampAt
    // clamps the offset to 0 instead. Encoded, offset 0 is 1 -- a real,
    // distinguishable stamp ("at or before this record's start"), not
    // the unstamped sentinel (0): collapsing it to literal 0 would
    // silently un-stamp the point and let a later write clobber it.
    brpc::LatencyTraceBuffer* b = brpc::LatencyTraceBuffer::instance();
    const brpc::LatencyTraceHandle h = b->AllocSlot(4, brpc::LT_ROLE_SERVER);
    const uint64_t base = b->GetForTest(h)->base_counter;
    ASSERT_GT(base, 0u);  // otherwise "predates" can't be constructed below
    b->StampAt(h, brpc::LT_S_WAKE, base - 1);
    ASSERT_EQ(1u, b->GetForTest(h)->ts[brpc::LT_S_WAKE])
        << "must clamp to the encoded offset-zero (1), not wrap around "
           "to a huge uint32_t, and not collide with the unstamped "
           "sentinel (0)";
}

TEST_F(LatencyTraceBufferTest,
       ExplicitBaseCounterAvoidsClampingServerReceivePoints) {
    // Fix-round item 1: the server can only call AllocSlot after
    // ProcessRpcRequest parses the trace id -- well after S01 (wake)
    // through S04 already happened. Before this fix, AllocSlot always
    // sampled base_counter at allocation time, so every one of those
    // four points would have predated base_counter and hit StampAt's
    // clamp. The 3-argument AllocSlot lets the caller supply the
    // server's actual timeline origin (the wake timestamp already
    // parked on Socket) instead, so a point captured before the slot
    // was allocated -- but at or after the true origin -- now records
    // as a real, unclamped offset instead of going through the clamp.
    brpc::LatencyTraceBuffer* b = brpc::LatencyTraceBuffer::instance();
    const uint64_t wake = butil::detail::clock_cycles();
    const brpc::LatencyTraceHandle h =
        b->AllocSlot(9, brpc::LT_ROLE_SERVER, wake);
    ASSERT_NE(brpc::LT_INVALID_HANDLE, h);
    ASSERT_EQ(wake, b->GetForTest(h)->base_counter);

    // Busy-wait so onedge_start is measurably after wake -- this models
    // a point that happened before AllocSlot's call site
    // (ProcessRpcRequest) but at/after the true origin.
    const uint64_t start = butil::detail::clock_cycles();
    while (butil::detail::clock_cycles() - start < 1000) {}
    const uint64_t onedge_start = butil::detail::clock_cycles();
    b->StampAt(h, brpc::LT_S_ONEDGE_START, onedge_start);

    const uint32_t encoded = b->GetForTest(h)->ts[brpc::LT_S_ONEDGE_START];
    ASSERT_GT(encoded, 1u)
        << "must be a real positive offset, not the clamp's offset-zero "
           "encoding (1) and not the unstamped sentinel (0)";
    ASSERT_EQ(onedge_start - wake, (uint64_t)(encoded - 1));
}

TEST_F(LatencyTraceBufferTest,
       PointStampedAtBaseCounterIsDistinguishableFromUnstamped) {
    // The motivating case for the offset+1 encoding: AllocSlot and the
    // C01 stamp are adjacent statements, and the counter ticks once per
    // ~10ns, so a client record can naturally produce an exact offset-0
    // stamp on a perfectly good capture. Verify it reads back as
    // non-zero and distinct from a point that was genuinely never
    // stamped, rather than colliding with that sentinel.
    brpc::LatencyTraceBuffer* b = brpc::LatencyTraceBuffer::instance();
    const brpc::LatencyTraceHandle h = b->AllocSlot(10, brpc::LT_ROLE_CLIENT);
    ASSERT_NE(brpc::LT_INVALID_HANDLE, h);
    const uint64_t base = b->GetForTest(h)->base_counter;

    // Stamp a point at exactly base_counter (offset 0) via StampAt so
    // the scenario is deterministic rather than racing the clock.
    b->StampAt(h, brpc::LT_C_RPC_START, base);
    const uint32_t at_base = b->GetForTest(h)->ts[brpc::LT_C_RPC_START];
    const uint32_t never_stamped = b->GetForTest(h)->ts[brpc::LT_C_RPC_END];

    ASSERT_NE(0u, at_base)
        << "offset 0 must not collide with the unstamped sentinel";
    ASSERT_EQ(0u, never_stamped);
    ASSERT_NE(at_base, never_stamped);
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
    b->set_stop_when_full_for_test(false);
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
    b->set_stop_when_full_for_test(true);
    for (int i = 0; i < 8; ++i) {
        ASSERT_NE(brpc::LT_INVALID_HANDLE, b->AllocSlot(i, brpc::LT_ROLE_CLIENT));
    }
    ASSERT_EQ(brpc::LT_INVALID_HANDLE, b->AllocSlot(999, brpc::LT_ROLE_CLIENT));
    ASSERT_EQ(8u, b->recorded_count());
    ASSERT_EQ(1u, b->dropped_count());
}

TEST_F(LatencyTraceBufferTest, GetBySeqForTestWalksWrittenSlotsInOrder) {
    // Single-threaded, so every allocation below lands on this thread's
    // one sticky shard: the flat global-seq traversal degenerates to
    // plain allocation order, which is what Task 8's end-to-end test
    // will actually depend on.
    brpc::LatencyTraceBuffer* b = brpc::LatencyTraceBuffer::instance();
    const brpc::LatencyTraceHandle h0 = b->AllocSlot(500, brpc::LT_ROLE_CLIENT);
    const brpc::LatencyTraceHandle h1 = b->AllocSlot(501, brpc::LT_ROLE_CLIENT);
    const brpc::LatencyTraceHandle h2 = b->AllocSlot(502, brpc::LT_ROLE_CLIENT);
    ASSERT_NE(brpc::LT_INVALID_HANDLE, h0);
    ASSERT_NE(brpc::LT_INVALID_HANDLE, h1);
    ASSERT_NE(brpc::LT_INVALID_HANDLE, h2);

    const brpc::LatencyTraceRecord* r0 = b->GetBySeqForTest(0);
    const brpc::LatencyTraceRecord* r1 = b->GetBySeqForTest(1);
    const brpc::LatencyTraceRecord* r2 = b->GetBySeqForTest(2);
    ASSERT_NE(nullptr, r0);
    ASSERT_NE(nullptr, r1);
    ASSERT_NE(nullptr, r2);
    ASSERT_EQ(500u, r0->trace_id);
    ASSERT_EQ(501u, r1->trace_id);
    ASSERT_EQ(502u, r2->trace_id);

    // Past the last written slot: out of bounds across every shard.
    ASSERT_EQ(nullptr, b->GetBySeqForTest(3));
}

TEST_F(LatencyTraceBufferTest, StaleHandleIsRejectedUnderConcurrentRecycling) {
    // What the generation guard actually promises -- and all it promises:
    // once a handle's slot has moved on to a different generation,
    // Get() rejects it (returns nullptr) rather than handing back a
    // pointer into that foreign generation's record. It does NOT make
    // concurrent recycling safe to write through: Stamp() is check-
    // then-write (Get() validates, then a separate statement writes
    // ts[point]), and no memory_order closes the window between those
    // two steps -- the slot can be recycled in between, in which case
    // the write lands on whoever now owns it. No fix to AllocSlot's
    // ordering can close that; only reference-counting a slot across
    // its lifetime (not implemented) or never recycling it at all
    // (which is the actual production default: stop-when-full, never
    // overwrite -- see StopsRecordingWhenFullInsteadOfOverwriting) can.
    // We still exercise Stamp() below as realistic traffic alongside
    // the check, but the assertion is specifically about what Get()
    // hands back, never about Stamp()'s write being race-free.
    //
    // Exactly ONE writer thread per shard: readers never call AllocSlot,
    // so a shard can never gain a second writer (shard assignment is a
    // sticky per-thread round robin -- only a thread's OWN first
    // AllocSlot call claims one). This rules out an entirely different,
    // unrelated hazard -- two DIFFERENT writers' write phases
    // overlapping on the literal same physical slot -- which showed up
    // in an earlier version of this test at small capacities. That
    // hazard needs two writers; single-writer-per-shard removes it
    // entirely, and at realistic production capacities (1000s, not the
    // small one used here to force wraparound quickly) it cannot occur
    // in practice either.
    //
    // The reader grabs one handle from its shard's writer and hammers
    // THAT SAME handle repeatedly (not re-fetching the latest one every
    // time) while the lone writer keeps allocating -- this gives many
    // chances for a check to land exactly when the writer wraps back
    // onto that handle's index. This models the real scenario the
    // guard exists for: a bthread that got a handle, stalled, and comes
    // back later to find its slot long since recycled by unrelated
    // traffic on the same shard.
    //
    // Single-writer-per-shard also means `seq == i` (the writer's own
    // loop counter) always, so a reader can recover the trace_id that
    // MUST belong to a given handle directly from the handle itself --
    // no separate, racy "what trace_id did the writer just assign" side
    // channel needed.
    brpc::LatencyTraceBuffer* b = brpc::LatencyTraceBuffer::instance();
    b->ResetForTest(brpc::LatencyTraceBuffer::SHARD_COUNT * 64);
    b->set_stop_when_full_for_test(false);

    const int kShards = brpc::LatencyTraceBuffer::SHARD_COUNT;
    const int kReadersPerShard = 8;
    const int kWriterIters = 300000;
    const int kMaxChecksPerHandle = 50000;
    const uint64_t kSeqMask = (((uint64_t)1) << brpc::LatencyTraceBuffer::SHARD_SHIFT) - 1;

    std::vector<std::atomic<brpc::LatencyTraceHandle>> latest(kShards);
    for (int s = 0; s < kShards; ++s) {
        latest[s].store(brpc::LT_INVALID_HANDLE, std::memory_order_relaxed);
    }
    std::atomic<int> writers_remaining(kShards);
    std::atomic<int> foreign_generation_leaks(0);

    // Diagnostic: capture full details of the FIRST bad result (if any),
    // with no I/O in the hot loop, for reporting after threads join.
    std::atomic<bool> captured(false);
    struct {
        int shard;
        brpc::LatencyTraceHandle h;
        uint64_t seq, expected_trace_id, observed_trace_id, raw_slot_seq_now;
    } diag = {};

    // Writers first: each thread's FIRST AllocSlot call claims a shard,
    // and with exactly kShards writer threads racing the same global
    // round-robin counter used by every earlier test in this binary,
    // they collectively claim all SHARD_COUNT shards, one each -- no
    // two writers ever land on the same one.
    std::vector<std::thread> writers;
    writers.reserve(kShards);
    for (int s = 0; s < kShards; ++s) {
        writers.emplace_back([&, s]() {
            for (int i = 0; i < kWriterIters; ++i) {
                const uint64_t trace_id = ((uint64_t)s << 32) | (uint32_t)i;
                const brpc::LatencyTraceHandle h =
                    b->AllocSlot(trace_id, brpc::LT_ROLE_CLIENT);
                if (h != brpc::LT_INVALID_HANDLE) {
                    b->Stamp(h, brpc::LT_C_WRITE_END);
                    latest[s].store(h, std::memory_order_release);
                }
            }
            writers_remaining.fetch_sub(1, std::memory_order_relaxed);
        });
    }

    std::vector<std::thread> readers;
    readers.reserve(kShards * kReadersPerShard);
    for (int s = 0; s < kShards; ++s) {
        for (int r = 0; r < kReadersPerShard; ++r) {
            readers.emplace_back([&, s]() {
                while (writers_remaining.load(std::memory_order_relaxed) > 0) {
                    const brpc::LatencyTraceHandle h =
                        latest[s].load(std::memory_order_acquire);
                    if (h == brpc::LT_INVALID_HANDLE) {
                        continue;
                    }
                    const uint64_t seq = h & kSeqMask;
                    const uint64_t expected_trace_id = ((uint64_t)s << 32) | seq;

                    // Hold and hammer THIS handle, not the latest one,
                    // for many checks -- see the comment above.
                    for (int k = 0; k < kMaxChecksPerHandle; ++k) {
                        b->Stamp(h, brpc::LT_C_RPC_END);  // realistic traffic

                        // Seqlock-style double-checked read of a field
                        // Get() does not itself protect (trace_id).
                        // GetForTest()'s internal load is memory_order_
                        // acquire, but an acquire LOAD only stops LATER
                        // operations (in program order) from moving
                        // BEFORE it -- it says nothing about EARLIER
                        // operations sinking AFTER it. That is exactly
                        // backwards from what this reader needs: without
                        // an explicit fence, the plain `trace_id` read
                        // below is free to be reordered (compiler or
                        // CPU) to occur AFTER the second GetForTest()
                        // call, so both sequence checks can observe the
                        // SAME still-current generation while trace_id
                        // is actually read from whatever generation
                        // replaced it in between. That reordering,
                        // undetected, produced this test's original
                        // false-positive signature: `observed_trace_id`
                        // embedding exactly `expected_seq + capacity`
                        // (one full recycle ahead), never garbage, every
                        // time -- because the read genuinely happened
                        // late, at a well-defined later point, not
                        // because of a real generation-guard bug.
                        // atomic_thread_fence(acquire) closes that: it
                        // forbids the read preceding it from being
                        // reordered past it, so the second GetForTest()
                        // call is guaranteed to observe slot_seq no
                        // earlier than the trace_id read did.
                        //
                        // Separately, `r1->trace_id` is a plain,
                        // non-atomic load racing the writer's plain,
                        // non-atomic store to the same field -- that is
                        // a data race and formally UB regardless of any
                        // fence here, independent of the reordering bug
                        // above. It is left as-is deliberately: this is
                        // exactly how Stamp() and other real callers
                        // touch a record after Get() validates it, so a
                        // "clean" atomic-only version of this test
                        // would not be testing the actual access pattern
                        // this guard has to work under. Real hardware
                        // does not tear an aligned 8-byte load/store, so
                        // this holds in practice though not in the
                        // strict standard sense.
                        const brpc::LatencyTraceRecord* r1 = b->GetForTest(h);
                        if (r1 == nullptr) {
                            break;   // recycled: move on to a fresh handle
                        }
                        const uint64_t observed_trace_id = r1->trace_id;
                        butil::atomic_thread_fence(butil::memory_order_acquire);
                        const brpc::LatencyTraceRecord* r2 = b->GetForTest(h);
                        if (r2 == nullptr) {
                            continue;   // recycled during our read: fine,
                                        // Get() correctly rejected h.
                        }
                        // Both checks agree h's generation is still
                        // current, so the guard has affirmatively
                        // vouched for this record: trace_id MUST be the
                        // one that generation published, never a
                        // foreign one. This is the guarantee under test
                        // -- not that concurrent Stamp() writes are
                        // themselves race-free.
                        if (observed_trace_id != expected_trace_id) {
                            foreign_generation_leaks.fetch_add(
                                1, std::memory_order_relaxed);
                            bool expected_flag = false;
                            if (captured.compare_exchange_strong(
                                    expected_flag, true,
                                    std::memory_order_relaxed)) {
                                diag.shard = s;
                                diag.h = h;
                                diag.seq = seq;
                                diag.expected_trace_id = expected_trace_id;
                                diag.observed_trace_id = observed_trace_id;
                                diag.raw_slot_seq_now = r1->slot_seq.load(
                                    butil::memory_order_relaxed);
                            }
                        }
                    }
                }
            });
        }
    }

    for (auto& th : writers) {
        th.join();
    }
    for (auto& th : readers) {
        th.join();
    }

    if (captured.load(std::memory_order_relaxed)) {
        fprintf(stderr,
            "StaleHandleIsRejectedUnderConcurrentRecycling: first bad "
            "result -- shard=%d h=0x%016llx expected_seq=%llu "
            "expected_trace_id=%llu observed_trace_id=%llu "
            "raw_slot_seq_now=%llu\n",
            diag.shard, (unsigned long long)diag.h,
            (unsigned long long)diag.seq,
            (unsigned long long)diag.expected_trace_id,
            (unsigned long long)diag.observed_trace_id,
            (unsigned long long)diag.raw_slot_seq_now);
    }

    ASSERT_EQ(0, foreign_generation_leaks.load())
        << "Get() vouched for a handle (two agreeing sequence checks) "
           "yet handed back a record belonging to a different "
           "generation -- it must return null instead once a slot has "
           "been recycled, never a foreign generation's data";
}

TEST_F(LatencyTraceBufferTest, DumpRoundTripsHeaderAndRecords) {
    // Hard-coded 128, not sizeof(hdr) on both sides of the fread below --
    // this is the on-disk contract merge.py parses verbatim (mirrors
    // Task 2's ASSERT_EQ(200u, sizeof(LatencyTraceRecord))). A self-
    // consistent sizeof()-vs-sizeof() round trip would keep passing even
    // if a field's type changed or members got reordered; this catches
    // that. LatencyTraceFileHeader also carries a static_assert to the
    // same effect, so a bad size fails at compile time already -- this
    // is the runtime belt to that compile-time suspender.
    ASSERT_EQ(128u, sizeof(brpc::LatencyTraceFileHeader));

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

TEST_F(LatencyTraceBufferTest, DumpPathFlagRegistersAtexitHook) {
    // Dumping on exit is how a benchmark run gets its data without the
    // program having to call anything. Verify the hook is installed, not
    // that atexit fires (gtest cannot observe process exit).
    ASSERT_FALSE(brpc::LatencyTraceBuffer::instance()->atexit_registered());
    brpc::LatencyTraceBuffer::instance()->EnableDumpOnExit("/tmp/brpc_lt_exit.bin");
    ASSERT_TRUE(brpc::LatencyTraceBuffer::instance()->atexit_registered());
}

TEST(LatencyTraceMacroTest, StampCompilesAndIsNoOpWhenHandleInvalid) {
    // Must be safe to call with an invalid handle from any thread.
    LT_STAMP(brpc::LT_INVALID_HANDLE, brpc::LT_C_RPC_START);
    SUCCEED();
}

TEST(LatencyTraceMacroTest, StampRecordsWhenEnabled) {
    brpc::FLAGS_latency_trace_enabled = true;
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
    brpc::FLAGS_latency_trace_enabled = false;
}

TEST(LatencyTraceBuildTest, WriteRequestSizeMatchesBuildMode) {
    // The real guard is the BAIDU_CASSERT inside socket.cpp; this test
    // documents the contract and fails loudly if the macro is lost.
#if defined(BRPC_LATENCY_TRACE)
    SUCCEED() << "traced build: sizeof(WriteRequest) asserted == 128 in socket.cpp";
#else
    SUCCEED() << "default build: sizeof(WriteRequest) asserted == 64 in socket.cpp";
#endif
}

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

// The points below only ever get stamped when the library is built with
// BRPC_LATENCY_TRACE -- without it, ControllerPrivateAccessor::
// set_latency_trace() is a no-op stub (see controller_private_accessor.h)
// and nothing ever calls LatencyTraceBuffer::AllocSlot() for a real RPC,
// so FindClientRecordForTest() below would always return nullptr. Keep
// this end-to-end test (and the two helpers Task 9/10/11 reuse) scoped to
// the traced build rather than asserting something that cannot be true in
// the default build.
#if defined(BRPC_LATENCY_TRACE)

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

class LatencyTraceEchoServiceImpl : public test::EchoService {
public:
    void Echo(google::protobuf::RpcController* cntl_base,
              const test::EchoRequest* request,
              test::EchoResponse* response,
              google::protobuf::Closure* done) override {
        brpc::ClosureGuard done_guard(done);
        response->set_message(request->message());
    }
};

TEST(LatencyTraceE2ETest, ClientSendPointsAreStampedAndMonotonic) {
    brpc::FLAGS_latency_trace_enabled = true;
    brpc::LatencyTraceBuffer::instance()->ResetForTest(1024);

    brpc::Server server;
    LatencyTraceEchoServiceImpl svc;
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
    brpc::FLAGS_latency_trace_enabled = false;
}

// Task 10 wires the server side: S01-S04 are the four Task-9 receive-side
// timestamps backfilled via StampAt() right after AllocSlot() in
// ProcessRpcRequest (passing the wake timestamp as base_counter -- see
// AllocSlot's 3-arg overload and design doc sec.8.1); S05-S17 are stamped
// directly (S05/S06 held in locals until the handle exists, same reasoning
// as S01-S04; S15-S17 via Socket::Write()'s generic wopt.lt_handle path).
// This single assertion range (LT_S_WAKE..LT_S_WRITE_END) therefore also
// covers the server half of what Task 9's report flagged as pending
// (formerly LatencyTraceE2ETest.ReceivePointsAreStampedOnBothSides's
// server-side assertions). The client half (C09-C12) remains Task 11's.
TEST(LatencyTraceE2ETest, ServerPointsAreCompleteAndMonotonic) {
    brpc::FLAGS_latency_trace_enabled = true;
    brpc::LatencyTraceBuffer::instance()->ResetForTest(1024);

    brpc::Server server;
    LatencyTraceEchoServiceImpl svc;
    ASSERT_EQ(0, server.AddService(&svc, brpc::SERVER_DOESNT_OWN_SERVICE));
    ASSERT_EQ(0, server.Start(9528, nullptr));

    brpc::Channel channel;
    brpc::ChannelOptions opt;
    opt.protocol = brpc::PROTOCOL_BAIDU_STD;
    ASSERT_EQ(0, channel.Init("127.0.0.1:9528", &opt));

    test::EchoService_Stub stub(&channel);
    test::EchoRequest req;
    test::EchoResponse res;
    brpc::Controller cntl;
    req.set_message("hello");
    stub.Echo(&cntl, &req, &res, nullptr);
    ASSERT_FALSE(cntl.Failed()) << cntl.ErrorText();

    const brpc::LatencyTraceRecord* s = FindServerRecordForTest();
    ASSERT_TRUE(s != nullptr);
    for (int p = brpc::LT_S_WAKE; p <= brpc::LT_S_WRITE_END; ++p) {
        ASSERT_GT(s->ts[p], 0u) << "server point " << p << " was never stamped";
    }
    for (int p = brpc::LT_S_WAKE; p < brpc::LT_S_WRITE_END; ++p) {
        ASSERT_LE(s->ts[p], s->ts[p + 1])
            << "server point " << p << " is later than " << (p + 1);
    }
    server.Stop(0);
    server.Join();
    brpc::FLAGS_latency_trace_enabled = false;
}

#endif  // defined(BRPC_LATENCY_TRACE)

}  // namespace
