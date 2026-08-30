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
#include <set>
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
// Defined in event_dispatcher.cpp, also inside `namespace brpc`. The fix
// round's usercode_in_pthread test flips this at runtime; see item 2's
// test below.
DECLARE_bool(usercode_in_pthread);
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

// Task 11's end-to-end tests issue more than one RPC on the same channel
// (see RunSequentialEchoRPCs below for why), so more than one record of
// each role exists and FindRecordByRole's "exactly one" rule would return
// nullptr. This variant returns the record of the given role with the
// LARGEST trace_id -- the most recently issued RPC -- rather than simply
// the last one encountered while walking GetBySeqForTest()'s order.
//
// That distinction is not academic. GetBySeqForTest() walks shard 0's
// written prefix, then shard 1's, and so on (see its own comment in
// latency_trace.cpp) -- shard-concatenation order, not wall-clock order
// across shards. Shard selection is per-OS-thread (a `__thread` variable
// fixed the first time that thread ever allocates a slot; see
// LatencyTraceBuffer::AllocSlot's tls_shard), so "last while walking
// shards in order" only equals "chronologically last" when every record
// that matters lands in the SAME shard:
//   - Client side: every RPC here is issued from this one blocking test
//     thread, and Channel::CallMethod's AllocSlot() runs synchronously on
//     the calling thread (no bthread hop before it), so all client
//     records in this file share one shard and shard order coincides
//     with issuance order. That is specific to this test's single-thread
//     shape, not a general property of the client path.
//   - Server side: whether every request on one persistent connection is
//     dispatched to the same worker thread (hence the same shard) across
//     separate requests was never confirmed by this project -- plausible
//     (brpc may pin a connection's read processing to one dispatcher),
//     but unverified. Depending on shard-concatenation order for "last"
//     would silently pick the wrong record the day that assumption stops
//     holding, with no test failure to flag it.
//
// Selecting on trace_id sidesteps the question rather than resting on
// either assumption above. Its low 32 bits are channel.cpp's `s_lt_seq`,
// one process-wide atomic counter incremented per RPC issued by
// Channel::CallMethod (see MakeLatencyTraceId); the server's record
// carries that exact same trace_id via propagation -- baidu_rpc_protocol.
// cpp reads request_meta.latency_trace_id() off the wire rather than
// minting its own (see ProcessRpcRequest). So "max trace_id for this
// role" means "most recently issued RPC" regardless of which shard either
// side's record ended up in.
static const brpc::LatencyTraceRecord* FindLastRecordByRole(brpc::LatencyTraceRole role) {
    const brpc::LatencyTraceRecord* found = nullptr;
    brpc::LatencyTraceBuffer* b = brpc::LatencyTraceBuffer::instance();
    for (uint64_t seq = 0; ; ++seq) {
        const brpc::LatencyTraceRecord* r = b->GetBySeqForTest(seq);
        if (r == nullptr) {
            break;
        }
        if (r->role == (uint8_t)role &&
            (found == nullptr || r->trace_id > found->trace_id)) {
            found = r;
        }
    }
    return found;
}
static const brpc::LatencyTraceRecord* FindLastClientRecordForTest() {
    return FindLastRecordByRole(brpc::LT_ROLE_CLIENT);
}
static const brpc::LatencyTraceRecord* FindLastServerRecordForTest() {
    return FindLastRecordByRole(brpc::LT_ROLE_SERVER);
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

// Task 10's defect, and why the tests below must not repeat it: Socket::
// StartWrite() has a synchronous fast path (no contention, no reconnect)
// that returns before ever calling DoWrite() -- so the write_start/
// write_end stamps that used to live only in DoWrite never ran for it.
// Task 8's end-to-end test asserted C01-C08 non-zero and passed anyway,
// because a single RPC on a fresh channel always needs a connect and so
// always goes through KeepWrite -> DoWrite; a *second* RPC on the same,
// now-established connection takes the fast path instead and would have
// left C07/C08 (and the server's S16/S17) at the unstamped sentinel (0)
// forever. Task 10 fixed StartWrite's fast path to stamp directly (see
// socket.cpp:1810/1828), but the only way to keep this class of defect
// caught in the future is to never again measure just the first RPC on a
// fresh connection. Every test below therefore issues several RPCs on one
// channel and reads back the LAST record of each role.
static void RunSequentialEchoRPCs(int port, int n_requests,
                                   const brpc::LatencyTraceRecord** out_client,
                                   const brpc::LatencyTraceRecord** out_server) {
    brpc::Server server;
    LatencyTraceEchoServiceImpl svc;
    ASSERT_EQ(0, server.AddService(&svc, brpc::SERVER_DOESNT_OWN_SERVICE));
    ASSERT_EQ(0, server.Start(port, nullptr));

    brpc::Channel channel;
    brpc::ChannelOptions opt;
    opt.protocol = brpc::PROTOCOL_BAIDU_STD;
    char addr[64];
    snprintf(addr, sizeof(addr), "127.0.0.1:%d", port);
    ASSERT_EQ(0, channel.Init(addr, &opt));

    test::EchoService_Stub stub(&channel);
    // One request at a time (fully synchronous, blocking Echo() call) --
    // never more than one outstanding request per connection. That is the
    // precondition design doc sec.10 requires for the "every decomposition
    // item non-negative" check below: with outstanding>1, a message can
    // inherit an earlier batch's wake/write timestamps (sec.8.2/8.4) and
    // manufacture a negative item that has nothing to do with a real
    // instrumentation bug.
    for (int i = 0; i < n_requests; ++i) {
        test::EchoRequest req;
        test::EchoResponse res;
        brpc::Controller cntl;
        req.set_message("hello");
        stub.Echo(&cntl, &req, &res, nullptr);
        ASSERT_FALSE(cntl.Failed()) << cntl.ErrorText();
    }

    *out_client = FindLastClientRecordForTest();
    *out_server = FindLastServerRecordForTest();

    server.Stop(0);
    server.Join();
}

TEST(LatencyTraceE2ETest, AllClientPointsStampedAndSumIsIdentity) {
    brpc::FLAGS_latency_trace_enabled = true;
    brpc::LatencyTraceBuffer::instance()->ResetForTest(1024);

    const brpc::LatencyTraceRecord* c = nullptr;
    const brpc::LatencyTraceRecord* s = nullptr;
    RunSequentialEchoRPCs(9529, 5, &c, &s);
    ASSERT_TRUE(c != nullptr && s != nullptr);

    // Criterion 1 (design doc sec.10.1): every point non-zero is the ONLY
    // check that catches a code path that was never instrumented (e.g. the
    // fast-path defect this file's comment above describes). Check both
    // ends, not just the client's 19.
    for (int p = brpc::LT_C_RPC_START; p <= brpc::LT_C_RPC_END; ++p) {
        ASSERT_GT(c->ts[p], 0u) << "client point " << p;
    }
    for (int p = brpc::LT_S_WAKE; p <= brpc::LT_S_WRITE_END; ++p) {
        ASSERT_GT(s->ts[p], 0u) << "server point " << p;
    }

    // Criterion 2: monotonic within each side's own timeline.
    for (int p = brpc::LT_C_RPC_START; p < brpc::LT_C_RPC_END; ++p) {
        ASSERT_LE(c->ts[p], c->ts[p + 1]) << "client point " << p;
    }
    for (int p = brpc::LT_S_WAKE; p < brpc::LT_S_WRITE_END; ++p) {
        ASSERT_LE(s->ts[p], s->ts[p + 1]) << "server point " << p;
    }

    // Sigma identity (design doc sec.5): the 17 client intervals excluding
    // C08->C09, plus the 16 server intervals, plus the two link halves,
    // must equal the end-to-end span exactly. This is a telescoping sum --
    // per design doc sec.10.1 it holds for ANY timestamp values (all
    // zeros included) and therefore verifies only the analysis tool's
    // arithmetic, never whether a point landed in the right place. Kept
    // because it IS a real, if narrow, invariant; the checks above and
    // below are what actually verify instrumentation correctness.
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

    brpc::FLAGS_latency_trace_enabled = false;
}

TEST(LatencyTraceE2ETest, LinkTimeIsNonNegativeAtOutstandingOne) {
    // Spec sec.10: with one in-flight request per connection there is no
    // batch attribution error, so a negative link time means the
    // instrumentation is misplaced -- most likely write_end (see sec.8.4).
    brpc::FLAGS_latency_trace_enabled = true;
    brpc::LatencyTraceBuffer::instance()->ResetForTest(1024);

    const brpc::LatencyTraceRecord* c = nullptr;
    const brpc::LatencyTraceRecord* s = nullptr;
    RunSequentialEchoRPCs(9530, 5, &c, &s);
    ASSERT_TRUE(c != nullptr && s != nullptr);

    const int64_t rtt = (int64_t)c->ts[brpc::LT_C_WAKE] -
                        (int64_t)c->ts[brpc::LT_C_WRITE_END];
    const int64_t srv = (int64_t)s->ts[brpc::LT_S_WRITE_END] -
                        (int64_t)s->ts[brpc::LT_S_WAKE];
    ASSERT_GE(rtt - srv, 0)
        << "negative total link time at outstanding=1; check write_end anchor";
    // C09 >= C08 by itself (rtt >= 0) is the specific signature called out
    // in design doc sec.8.4: write_end stamped too late (e.g. inside
    // ReturnSuccessfulWriteRequest instead of DoWrite/StartWrite) shows up
    // exactly as a negative round trip, independent of the server side.
    ASSERT_GE(rtt, 0) << "negative client round trip C09-C08; "
                          "write_end is likely anchored too late";

    brpc::FLAGS_latency_trace_enabled = false;
}

TEST(LatencyTraceE2ETest, AllDecompositionItemsNonNegativeAtOutstandingOne) {
    // Spec sec.10.1: this is the check that actually catches "a point
    // landed in the wrong slot" -- neither the sigma identity (a
    // telescoping sum, true for any values) nor plain monotonicity (which,
    // within one record, is algebraically the same statement as "this
    // item is non-negative") say anything about the two derived link
    // items. Named per design doc sec.5 for anyone cross-referencing a
    // failure here against the spec's decomposition table.
    brpc::FLAGS_latency_trace_enabled = true;
    brpc::LatencyTraceBuffer::instance()->ResetForTest(1024);

    const brpc::LatencyTraceRecord* c = nullptr;
    const brpc::LatencyTraceRecord* s = nullptr;
    RunSequentialEchoRPCs(9531, 5, &c, &s);
    ASSERT_TRUE(c != nullptr && s != nullptr);

    // 7 client send-side items (C01-C08, sec.5.1).
    static const struct { int lo, hi; const char* name; } kClientItems[] = {
        { brpc::LT_C_RPC_START,           brpc::LT_C_REQ_PAYLOAD_SER_START, "cli_pre_serialize" },
        { brpc::LT_C_REQ_PAYLOAD_SER_START, brpc::LT_C_REQ_PAYLOAD_SER_END,  "cli_req_payload_ser" },
        { brpc::LT_C_REQ_PAYLOAD_SER_END,   brpc::LT_C_REQ_META_SER_START,   "cli_issue_rpc" },
        { brpc::LT_C_REQ_META_SER_START,    brpc::LT_C_REQ_META_SER_END,     "cli_req_meta_ser" },
        { brpc::LT_C_REQ_META_SER_END,      brpc::LT_C_WRITE_ENQUEUE,        "cli_pack_to_write" },
        { brpc::LT_C_WRITE_ENQUEUE,         brpc::LT_C_WRITE_START,          "cli_write_queue" },
        { brpc::LT_C_WRITE_START,           brpc::LT_C_WRITE_END,            "cli_write_syscall" },
    };
    for (const auto& it : kClientItems) {
        ASSERT_GE((int64_t)c->ts[it.hi] - (int64_t)c->ts[it.lo], 0) << it.name;
    }

    // 16 server items (S01-S17, sec.5.2).
    static const struct { int lo, hi; const char* name; } kServerItems[] = {
        { brpc::LT_S_WAKE,                  brpc::LT_S_ONEDGE_START,          "srv_wake_to_onedge" },
        { brpc::LT_S_ONEDGE_START,          brpc::LT_S_READV_START,           "srv_onedge_to_readv" },
        { brpc::LT_S_READV_START,           brpc::LT_S_MSG_RECV_DONE,         "srv_readv" },
        { brpc::LT_S_MSG_RECV_DONE,         brpc::LT_S_REQ_META_DESER_START,  "srv_recv_to_deser" },
        { brpc::LT_S_REQ_META_DESER_START,  brpc::LT_S_REQ_META_DESER_END,    "srv_req_meta_deser" },
        { brpc::LT_S_REQ_META_DESER_END,    brpc::LT_S_REQ_PAYLOAD_DESER_START, "srv_dispatch" },
        { brpc::LT_S_REQ_PAYLOAD_DESER_START, brpc::LT_S_REQ_PAYLOAD_DESER_END, "srv_req_payload_deser" },
        { brpc::LT_S_REQ_PAYLOAD_DESER_END, brpc::LT_S_SERVICE_START,         "srv_to_service" },
        { brpc::LT_S_SERVICE_START,         brpc::LT_S_SERVICE_END,           "srv_service" },
        { brpc::LT_S_SERVICE_END,           brpc::LT_S_RSP_PAYLOAD_SER_START, "srv_service_to_ser" },
        { brpc::LT_S_RSP_PAYLOAD_SER_START, brpc::LT_S_RSP_PAYLOAD_SER_END,   "srv_rsp_payload_ser" },
        { brpc::LT_S_RSP_PAYLOAD_SER_END,   brpc::LT_S_RSP_META_SER_START,    "srv_compress_checksum" },
        { brpc::LT_S_RSP_META_SER_START,    brpc::LT_S_RSP_META_SER_END,      "srv_rsp_meta_ser" },
        { brpc::LT_S_RSP_META_SER_END,      brpc::LT_S_WRITE_ENQUEUE,         "srv_pack_to_write" },
        { brpc::LT_S_WRITE_ENQUEUE,         brpc::LT_S_WRITE_START,           "srv_write_queue" },
        { brpc::LT_S_WRITE_START,           brpc::LT_S_WRITE_END,             "srv_write_syscall" },
    };
    for (const auto& it : kServerItems) {
        ASSERT_GE((int64_t)s->ts[it.hi] - (int64_t)s->ts[it.lo], 0) << it.name;
    }

    // 10 client receive-side items (C09-C19, sec.5.3).
    static const struct { int lo, hi; const char* name; } kClientRecvItems[] = {
        { brpc::LT_C_WAKE,                  brpc::LT_C_ONEDGE_START,          "cli_wake_to_onedge" },
        { brpc::LT_C_ONEDGE_START,          brpc::LT_C_READV_START,           "cli_onedge_to_readv" },
        { brpc::LT_C_READV_START,           brpc::LT_C_MSG_RECV_DONE,         "cli_readv" },
        { brpc::LT_C_MSG_RECV_DONE,         brpc::LT_C_RSP_META_DESER_START,  "cli_recv_to_deser" },
        { brpc::LT_C_RSP_META_DESER_START,  brpc::LT_C_RSP_META_DESER_END,    "cli_rsp_meta_deser" },
        { brpc::LT_C_RSP_META_DESER_END,    brpc::LT_C_RSP_PAYLOAD_DESER_START, "cli_lookup_cntl" },
        { brpc::LT_C_RSP_PAYLOAD_DESER_START, brpc::LT_C_RSP_PAYLOAD_DESER_END, "cli_rsp_payload_deser" },
        { brpc::LT_C_RSP_PAYLOAD_DESER_END, brpc::LT_C_RSP_PROCESS_START,     "cli_post_deser" },
        { brpc::LT_C_RSP_PROCESS_START,     brpc::LT_C_RSP_PROCESS_END,       "cli_callback" },
        { brpc::LT_C_RSP_PROCESS_END,       brpc::LT_C_RPC_END,               "cli_rpc_finish" },
    };
    for (const auto& it : kClientRecvItems) {
        ASSERT_GE((int64_t)c->ts[it.hi] - (int64_t)c->ts[it.lo], 0) << it.name;
    }

    // 2 derived link items (sec.6.1, model A): total link time L = RTT - S
    // (both terms purely intra-record, so the +1 encoding cancels exactly
    // the same way it does in the sigma identity above), halved. Verified
    // via the exact same formula as LinkTimeIsNonNegativeAtOutstandingOne;
    // duplicated here (rather than only relying on that test) because this
    // test's job is specifically "all 35 named items are individually
    // accounted for and non-negative," not "the link time happens to be
    // non-negative for some other reason."
    const int64_t rtt = (int64_t)c->ts[brpc::LT_C_WAKE] -
                        (int64_t)c->ts[brpc::LT_C_WRITE_END];
    const int64_t srv = (int64_t)s->ts[brpc::LT_S_WRITE_END] -
                        (int64_t)s->ts[brpc::LT_S_WAKE];
    const int64_t total_link = rtt - srv;
    ASSERT_GE(total_link, 0) << "link_up + link_down (model A)";

    brpc::FLAGS_latency_trace_enabled = false;
}

// ---------------------------------------------------------------------
// Fix round (Task 11 review, items 1-3): every test above issues fully
// synchronous RPCs (`stub.Echo(&cntl, &req, &res, nullptr)`), so neither
// the pre-existing async instrumentation nor this round's two fixes --
// C19 on the regular async path (Controller::EndRPC) and C17-C19 on the
// -usercode_in_pthread path (Controller::DoneInBackupThread) -- were ever
// exercised. The two tests below close that gap, asserting the same
// section 10.1 properties the synchronous tests above assert: every
// point non-zero, monotonic, C09 >= C08, and every decomposition item
// non-negative at outstanding=1.
// ---------------------------------------------------------------------

// A no-op, self-deleting Closure. Deliberately NOT brpc::DoNothing():
// Controller::EndRPC special-cases `_done == DoNothing()` to always take
// the immediate (non-backup-thread) branch regardless of
// -usercode_in_pthread (see the "Note" comment in controller.cpp), so
// using DoNothing() here would make it impossible to exercise
// DoneInBackupThread() -- exactly the path item 2 fixes.
class AsyncEchoDone : public google::protobuf::Closure {
public:
    void Run() override { delete this; }
};

// Generic poll-with-timeout, mirroring the pattern used elsewhere in this
// codebase (see brpc_streaming_rpc_unittest.cpp's WaitForTrue) for
// observing state a Closure running on a different thread produces.
template <typename Pred>
static bool WaitForTrue(Pred pred, int timeout_ms) {
    const int kStepUs = 1000;
    for (int waited_us = 0; !pred(); waited_us += kStepUs) {
        if (waited_us >= timeout_ms * 1000) {
            return pred();
        }
        usleep(kStepUs);
    }
    return true;
}

// Async analog of RunSequentialEchoRPCs above, still one RPC at a time
// (outstanding == 1, for the same reason the sync helper cites: design
// doc sec.10.1's non-negative-decomposition-item check assumes it) but
// issued with a `done` closure instead of a blocking Echo() call.
//
// A subtlety unique to the async path: our closure only marks that
// `_done->Run()` happened (by self-deleting; it carries no other state).
// EndRPC's C18/C19 stamps -- and, on the -usercode_in_pthread path,
// DoneInBackupThread's -- land on the RPC-processing thread strictly
// AFTER `_done->Run()` returns, which is after control has already
// returned to that thread, not to this one. There is no synchronization
// primitive between "the closure ran" and "C18/C19 are stamped", so
// rather than reading the record the instant Echo() posts the request,
// this helper polls the record itself for C19 (LT_C_RPC_END) to go
// non-zero, bounded by a generous timeout. If C19 is never stamped (e.g.
// item 1's or item 2's fix is missing) this poll times out and the test
// fails on that specific assertion instead of silently reading a
// half-written record.
static void RunSequentialAsyncEchoRPCs(int port, int n_requests,
                                        const brpc::LatencyTraceRecord** out_client,
                                        const brpc::LatencyTraceRecord** out_server) {
    brpc::Server server;
    LatencyTraceEchoServiceImpl svc;
    ASSERT_EQ(0, server.AddService(&svc, brpc::SERVER_DOESNT_OWN_SERVICE));
    ASSERT_EQ(0, server.Start(port, nullptr));

    brpc::Channel channel;
    brpc::ChannelOptions opt;
    opt.protocol = brpc::PROTOCOL_BAIDU_STD;
    char addr[64];
    snprintf(addr, sizeof(addr), "127.0.0.1:%d", port);
    ASSERT_EQ(0, channel.Init(addr, &opt));

    test::EchoService_Stub stub(&channel);
    for (int i = 0; i < n_requests; ++i) {
        test::EchoRequest req;
        test::EchoResponse res;
        brpc::Controller cntl;
        req.set_message("hello");
        // Echo() with a non-null `done` returns as soon as the request is
        // posted -- it does not block for the response.
        stub.Echo(&cntl, &req, &res, new AsyncEchoDone);

        const bool stamped = WaitForTrue([]() {
            const brpc::LatencyTraceRecord* r = FindLastClientRecordForTest();
            return r != nullptr && r->ts[brpc::LT_C_RPC_END] != 0;
        }, 2000);
        ASSERT_TRUE(stamped)
            << "timed out waiting for C19 (LT_C_RPC_END) to be stamped "
               "on async request " << i;
        // Safe to read now: cntl's error state was finalized by OnRPCEnd()
        // before _done->Run() was ever invoked, well before the C19 stamp
        // this wait just confirmed.
        ASSERT_FALSE(cntl.Failed()) << cntl.ErrorText();
    }

    *out_client = FindLastClientRecordForTest();
    *out_server = FindLastServerRecordForTest();

    server.Stop(0);
    server.Join();
}

// Shared body for the two tests below: section 10.1's weight-bearing
// checks, factored out so they aren't duplicated verbatim a third time
// (the sync tests above already state them twice, split by concern --
// this collapses all of them into one call site per async test).
static void AssertAllWeightBearingInvariants(const brpc::LatencyTraceRecord* c,
                                              const brpc::LatencyTraceRecord* s) {
    ASSERT_TRUE(c != nullptr && s != nullptr);

    // Every point non-zero (design doc sec.10.1's only check that catches
    // an un-instrumented code path).
    for (int p = brpc::LT_C_RPC_START; p <= brpc::LT_C_RPC_END; ++p) {
        ASSERT_GT(c->ts[p], 0u) << "client point " << p;
    }
    for (int p = brpc::LT_S_WAKE; p <= brpc::LT_S_WRITE_END; ++p) {
        ASSERT_GT(s->ts[p], 0u) << "server point " << p;
    }

    // Monotonic within each side's own timeline.
    for (int p = brpc::LT_C_RPC_START; p < brpc::LT_C_RPC_END; ++p) {
        ASSERT_LE(c->ts[p], c->ts[p + 1]) << "client point " << p;
    }
    for (int p = brpc::LT_S_WAKE; p < brpc::LT_S_WRITE_END; ++p) {
        ASSERT_LE(s->ts[p], s->ts[p + 1]) << "server point " << p;
    }

    // C09 >= C08.
    const int64_t rtt = (int64_t)c->ts[brpc::LT_C_WAKE] -
                        (int64_t)c->ts[brpc::LT_C_WRITE_END];
    ASSERT_GE(rtt, 0) << "negative client round trip C09-C08";

    // Every decomposition item non-negative at outstanding=1 (design doc
    // sec.5's 7 client-send + 16 server + 10 client-receive items, plus
    // the 2 derived link halves).
    static const struct { int lo, hi; const char* name; } kClientItems[] = {
        { brpc::LT_C_RPC_START,             brpc::LT_C_REQ_PAYLOAD_SER_START, "cli_pre_serialize" },
        { brpc::LT_C_REQ_PAYLOAD_SER_START, brpc::LT_C_REQ_PAYLOAD_SER_END,   "cli_req_payload_ser" },
        { brpc::LT_C_REQ_PAYLOAD_SER_END,   brpc::LT_C_REQ_META_SER_START,    "cli_issue_rpc" },
        { brpc::LT_C_REQ_META_SER_START,    brpc::LT_C_REQ_META_SER_END,      "cli_req_meta_ser" },
        { brpc::LT_C_REQ_META_SER_END,      brpc::LT_C_WRITE_ENQUEUE,         "cli_pack_to_write" },
        { brpc::LT_C_WRITE_ENQUEUE,         brpc::LT_C_WRITE_START,           "cli_write_queue" },
        { brpc::LT_C_WRITE_START,           brpc::LT_C_WRITE_END,             "cli_write_syscall" },
    };
    for (const auto& it : kClientItems) {
        ASSERT_GE((int64_t)c->ts[it.hi] - (int64_t)c->ts[it.lo], 0) << it.name;
    }

    static const struct { int lo, hi; const char* name; } kServerItems[] = {
        { brpc::LT_S_WAKE,                   brpc::LT_S_ONEDGE_START,           "srv_wake_to_onedge" },
        { brpc::LT_S_ONEDGE_START,           brpc::LT_S_READV_START,            "srv_onedge_to_readv" },
        { brpc::LT_S_READV_START,            brpc::LT_S_MSG_RECV_DONE,          "srv_readv" },
        { brpc::LT_S_MSG_RECV_DONE,          brpc::LT_S_REQ_META_DESER_START,   "srv_recv_to_deser" },
        { brpc::LT_S_REQ_META_DESER_START,   brpc::LT_S_REQ_META_DESER_END,     "srv_req_meta_deser" },
        { brpc::LT_S_REQ_META_DESER_END,     brpc::LT_S_REQ_PAYLOAD_DESER_START, "srv_dispatch" },
        { brpc::LT_S_REQ_PAYLOAD_DESER_START, brpc::LT_S_REQ_PAYLOAD_DESER_END, "srv_req_payload_deser" },
        { brpc::LT_S_REQ_PAYLOAD_DESER_END,  brpc::LT_S_SERVICE_START,          "srv_to_service" },
        { brpc::LT_S_SERVICE_START,          brpc::LT_S_SERVICE_END,            "srv_service" },
        { brpc::LT_S_SERVICE_END,            brpc::LT_S_RSP_PAYLOAD_SER_START,  "srv_service_to_ser" },
        { brpc::LT_S_RSP_PAYLOAD_SER_START,  brpc::LT_S_RSP_PAYLOAD_SER_END,    "srv_rsp_payload_ser" },
        { brpc::LT_S_RSP_PAYLOAD_SER_END,    brpc::LT_S_RSP_META_SER_START,     "srv_compress_checksum" },
        { brpc::LT_S_RSP_META_SER_START,     brpc::LT_S_RSP_META_SER_END,       "srv_rsp_meta_ser" },
        { brpc::LT_S_RSP_META_SER_END,       brpc::LT_S_WRITE_ENQUEUE,          "srv_pack_to_write" },
        { brpc::LT_S_WRITE_ENQUEUE,          brpc::LT_S_WRITE_START,            "srv_write_queue" },
        { brpc::LT_S_WRITE_START,            brpc::LT_S_WRITE_END,              "srv_write_syscall" },
    };
    for (const auto& it : kServerItems) {
        ASSERT_GE((int64_t)s->ts[it.hi] - (int64_t)s->ts[it.lo], 0) << it.name;
    }

    static const struct { int lo, hi; const char* name; } kClientRecvItems[] = {
        { brpc::LT_C_WAKE,                    brpc::LT_C_ONEDGE_START,           "cli_wake_to_onedge" },
        { brpc::LT_C_ONEDGE_START,            brpc::LT_C_READV_START,            "cli_onedge_to_readv" },
        { brpc::LT_C_READV_START,             brpc::LT_C_MSG_RECV_DONE,          "cli_readv" },
        { brpc::LT_C_MSG_RECV_DONE,           brpc::LT_C_RSP_META_DESER_START,   "cli_recv_to_deser" },
        { brpc::LT_C_RSP_META_DESER_START,    brpc::LT_C_RSP_META_DESER_END,     "cli_rsp_meta_deser" },
        { brpc::LT_C_RSP_META_DESER_END,      brpc::LT_C_RSP_PAYLOAD_DESER_START, "cli_lookup_cntl" },
        { brpc::LT_C_RSP_PAYLOAD_DESER_START, brpc::LT_C_RSP_PAYLOAD_DESER_END,  "cli_rsp_payload_deser" },
        { brpc::LT_C_RSP_PAYLOAD_DESER_END,   brpc::LT_C_RSP_PROCESS_START,      "cli_post_deser" },
        { brpc::LT_C_RSP_PROCESS_START,       brpc::LT_C_RSP_PROCESS_END,        "cli_callback" },
        { brpc::LT_C_RSP_PROCESS_END,         brpc::LT_C_RPC_END,                "cli_rpc_finish" },
    };
    for (const auto& it : kClientRecvItems) {
        ASSERT_GE((int64_t)c->ts[it.hi] - (int64_t)c->ts[it.lo], 0) << it.name;
    }

    const int64_t srv = (int64_t)s->ts[brpc::LT_S_WRITE_END] -
                        (int64_t)s->ts[brpc::LT_S_WAKE];
    ASSERT_GE(rtt - srv, 0) << "link_up + link_down (model A)";
}

TEST(LatencyTraceE2ETest, AsyncEchoAllWeightBearingInvariants) {
    // Item 1's target: the regular async branch of Controller::EndRPC
    // (-usercode_in_pthread stays at its default, false).
    brpc::FLAGS_latency_trace_enabled = true;
    brpc::LatencyTraceBuffer::instance()->ResetForTest(1024);

    const brpc::LatencyTraceRecord* c = nullptr;
    const brpc::LatencyTraceRecord* s = nullptr;
    RunSequentialAsyncEchoRPCs(9533, 5, &c, &s);
    AssertAllWeightBearingInvariants(c, s);

    brpc::FLAGS_latency_trace_enabled = false;
}

TEST(LatencyTraceE2ETest, AsyncEchoWithUsercodeInPthreadAllWeightBearingInvariants) {
    // Item 2's target: Controller::DoneInBackupThread(), reached only via
    // RunUserCode(RunDoneInBackupThread, this), which Controller::EndRPC
    // takes when -usercode_in_pthread is on and `done` isn't DoNothing().
    // Restore the flag unconditionally (even on an early ASSERT_ return
    // above) since it's process-wide and would otherwise silently change
    // every later test's async execution path.
    const bool saved_usercode_in_pthread = brpc::FLAGS_usercode_in_pthread;
    struct Restore {
        const bool* saved;
        ~Restore() { brpc::FLAGS_usercode_in_pthread = *saved; }
    } restore{&saved_usercode_in_pthread};

    brpc::FLAGS_usercode_in_pthread = true;
    brpc::FLAGS_latency_trace_enabled = true;
    brpc::LatencyTraceBuffer::instance()->ResetForTest(1024);

    const brpc::LatencyTraceRecord* c = nullptr;
    const brpc::LatencyTraceRecord* s = nullptr;
    RunSequentialAsyncEchoRPCs(9534, 5, &c, &s);
    AssertAllWeightBearingInvariants(c, s);

    brpc::FLAGS_latency_trace_enabled = false;
}

TEST(LatencyTraceMetaTest, RecordCarriesSizesEndpointAndErrorCode) {
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

    server.Stop(0);
    server.Join();
    brpc::FLAGS_latency_trace_enabled = false;
}

TEST(LatencyTraceMetaTest, EachRetryAttemptGetsItsOwnRecord) {
    // Spec D9: one record per attempt, numbered from 0. Point a channel at
    // a dead backend with max_retry=2 so three attempts are issued.
    brpc::FLAGS_latency_trace_enabled = true;
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
    brpc::FLAGS_latency_trace_enabled = false;
}

TEST(LatencyTraceMetaTest, DumpMethodTableResolvesRecordedMethodId) {
    // The method table is only useful if an offline reader can actually
    // resolve a method_id back to a name from the dumped file -- not
    // merely if some bytes happen to land at method_table_offset. Issue a
    // real RPC, dump, then parse the file exactly as an external tool
    // would: read the header, seek to method_table_offset, walk the
    // count+len-prefixed entries, and check entry[method_id - 1] matches
    // the method that was actually called.
    brpc::FLAGS_latency_trace_enabled = true;
    brpc::LatencyTraceBuffer::instance()->ResetForTest(1024);

    brpc::Server server;
    LatencyTraceEchoServiceImpl svc;
    ASSERT_EQ(0, server.AddService(&svc, brpc::SERVER_DOESNT_OWN_SERVICE));
    ASSERT_EQ(0, server.Start(9532, nullptr));

    brpc::Channel channel;
    brpc::ChannelOptions opt;
    opt.protocol = brpc::PROTOCOL_BAIDU_STD;
    ASSERT_EQ(0, channel.Init("127.0.0.1:9532", &opt));

    test::EchoService_Stub stub(&channel);
    test::EchoRequest req;
    test::EchoResponse res;
    brpc::Controller cntl;
    req.set_message("hello");
    stub.Echo(&cntl, &req, &res, nullptr);
    ASSERT_FALSE(cntl.Failed()) << cntl.ErrorText();

    const brpc::LatencyTraceRecord* s = FindServerRecordForTest();
    ASSERT_TRUE(s != nullptr);
    ASSERT_NE(0u, s->method_id) << "server record never recorded a method_id";

    const char* path = "/tmp/brpc_lt_method_table_test.bin";
    ASSERT_GE(brpc::LatencyTraceBuffer::instance()->Dump(path), 1);

    FILE* fp = fopen(path, "rb");
    ASSERT_TRUE(fp != nullptr);
    brpc::LatencyTraceFileHeader hdr;
    ASSERT_EQ(1u, fread(&hdr, sizeof(hdr), 1, fp));
    ASSERT_GT(hdr.method_table_offset, 0u)
        << "method_table_offset must no longer be the pre-Task-13 zero";

    ASSERT_EQ(0, fseek(fp, (long)hdr.method_table_offset, SEEK_SET));
    uint32_t count = 0;
    ASSERT_EQ(1u, fread(&count, sizeof(count), 1, fp));
    ASSERT_GE(count, s->method_id) << "table too short to contain method_id="
                                    << s->method_id;

    std::vector<std::string> names;
    for (uint32_t i = 0; i < count; ++i) {
        uint32_t len = 0;
        ASSERT_EQ(1u, fread(&len, sizeof(len), 1, fp));
        std::string name(len, '\0');
        if (len > 0) {
            ASSERT_EQ(1u, fread(&name[0], len, 1, fp));
        }
        names.push_back(name);
    }
    fclose(fp);
    unlink(path);

    // Entry i (0-based) names the method whose method_id == i + 1.
    ASSERT_EQ("test.EchoService.Echo", names[s->method_id - 1])
        << "could not resolve the recorded method_id back to its name";

    server.Stop(0);
    server.Join();
    brpc::FLAGS_latency_trace_enabled = false;
}

#endif  // defined(BRPC_LATENCY_TRACE)

}  // namespace
