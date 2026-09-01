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
#include <algorithm>
#include <atomic>
#include <cerrno>
#include <cstdio>
#include <set>
#include <thread>
#include <type_traits>
#include <unistd.h>
#include <vector>
#include <sys/socket.h>
#include "brpc/channel.h"
#include "brpc/closure_guard.h"
#include "brpc/details/controller_private_accessor.h"
#include "brpc/latency_trace.h"
#include "brpc/policy/baidu_rpc_meta.pb.h"
#include "brpc/server.h"
#include "brpc/socket.h"
#include "bthread/bthread.h"
#include "butil/endpoint.h"
#include "butil/time.h"
#include "echo.pb.h"

namespace {

TEST(LatencyTraceTest, PointEnumHasExactly34Members) {
    // D14: client lost rsp_process_start/end (the two points bracketing
    // the user callback), so client is 17 points, not 19; total 34, not 36.
    ASSERT_EQ(34, brpc::LT_POINT_COUNT);
    // 客户端 17 个，服务端 17 个，且两段不重叠。
    ASSERT_EQ(0,  brpc::LT_C_RPC_START);
    ASSERT_EQ(16, brpc::LT_C_RPC_END);
    ASSERT_EQ(17, brpc::LT_S_WAKE);
    ASSERT_EQ(33, brpc::LT_S_WRITE_END);
}

TEST(LatencyTraceTest, RecordIsExactly192Bytes) {
    // 定长 POD。大小变化会直接改变 dump 文件格式，必须显式感知。
    ASSERT_EQ(192u, sizeof(brpc::LatencyTraceRecord));
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
#if defined(BRPC_LATENCY_TRACE)
// Defined in event_dispatcher_epoll.cpp with external linkage kept on
// purpose for exactly this: see the fix-round item 1 test below,
// EpollWakeIsReArmedBeforeEachCallbackInABatch.
extern __thread uint64_t tls_lt_epoll_wake;
#endif
}  // namespace brpc

#if defined(BRPC_LATENCY_TRACE)
namespace {

// Fix-round item 1: tls_lt_epoll_wake is `__thread`, so it belongs to a
// pthread, not to the dispatcher bthread that logically "owns" one
// epoll_wait() return. Socket::OnInputEvent's bthread_start_urgent()
// runs the new bthread immediately on the CURRENT pthread and queues the
// dispatcher bthread; when the dispatcher resumes to process event 2 of
// N it may resume on a *different* pthread, whose tls_lt_epoll_wake
// holds a stale or zero value (bthreads migrate, __thread does not). The
// fix (event_dispatcher_epoll.cpp) re-arms the TLS from a local captured
// once per epoll_wait() return, immediately before each callback in the
// for loop, closing the window between the write and the read.
//
// A true end-to-end reproduction -- two real Sockets becoming readable
// at the same instant, racing a real cross-pthread bthread migration in
// between their callbacks -- is not constructible deterministically:
// forcing epoll to batch two arbitrary sockets' readiness into one
// epoll_wait() return is not something userspace can command, and
// forcing a bthread migration between two specific statements is exactly
// as underspecified as the race the fix closes. What follows instead
// exercises the actual modified code in event_dispatcher_epoll.cpp
// (not a hand-copy of its shape): two fds are made readable and
// registered before a dedicated EventDispatcher is even Start()ed, so
// its first epoll_wait() deterministically batches both (n==2) into one
// Run() loop iteration. The callback for the first event mutates
// tls_lt_epoll_wake to a sentinel right before returning -- standing in
// for "a different pthread's stale copy", which is observationally
// identical to the second callback either way. Without the fix, the
// second callback reads that sentinel straight back (the TLS was armed
// once, before the loop, and nothing rewrites it in between). With the
// fix, the second callback reads the correct, shared wake value again.
struct LtWakeReArmCtx {
    std::atomic<int> call_seq{0};
    std::atomic<bool> done{false};
    uint64_t observed[2] = {0, 0};
};

class LtWakeReArmProbe {
public:
    static int OnInputEvent(void* user_data, uint32_t /*events*/,
                             const bthread_attr_t& /*thread_attr*/) {
        LtWakeReArmCtx* ctx = static_cast<LtWakeReArmCtx*>(user_data);
        const int idx = ctx->call_seq.fetch_add(1, std::memory_order_relaxed);
        if (idx < 2) {
            ctx->observed[idx] = brpc::tls_lt_epoll_wake;
        }
        if (idx == 0) {
            // Stand-in for a different pthread's stale/zero TLS copy --
            // see the block comment above.
            brpc::tls_lt_epoll_wake = 0xDEADBEEFULL;
        } else {
            ctx->done.store(true, std::memory_order_release);
        }
        return 0;
    }
    static int OnOutputEvent(void*, uint32_t, const bthread_attr_t&) {
        return 0;
    }
};

}  // namespace

TEST(LatencyTraceEventDispatcherTest,
     EpollWakeIsReArmedBeforeEachCallbackInABatch) {
    int fds_a[2] = {-1, -1};
    int fds_b[2] = {-1, -1};
    ASSERT_EQ(0, pipe(fds_a));
    ASSERT_EQ(0, pipe(fds_b));

    // Make both readable BEFORE registering them, and register both
    // BEFORE Start()ing the dispatcher -- so its very first epoll_wait()
    // call reports both together, deterministically, instead of racing a
    // live Run() loop across two separate writes. (AddConsumer uses
    // EPOLLET, but a fd already readable at ADD time still counts as one
    // rising edge and is reported on the next epoll_wait().)
    ASSERT_EQ(1, write(fds_a[1], "a", 1));
    ASSERT_EQ(1, write(fds_b[1], "b", 1));

    // EventDispatcher::Run() writes through two process-global
    // bvar::LatencyRecorder pointers that are only allocated by
    // GetGlobalEventDispatcher()'s pthread_once (InitializeGlobalDispatchers,
    // event_dispatcher.cpp) -- lazily, on the first Socket/Channel/Server
    // anywhere in the process. This test deliberately builds its own
    // standalone EventDispatcher below rather than going through the
    // global one (see the comment above), which bypasses whatever would
    // normally have triggered that init first. If this test runs before
    // any other test in the binary happens to touch a real Socket, those
    // pointers are still null and Run() would null-deref. Force the
    // pthread_once here explicitly; the dummy fd/tag arguments are
    // discarded along with the (real, but unused) global dispatcher
    // reference -- only the one-time init side effect is wanted.
    brpc::GetGlobalEventDispatcher(0, BTHREAD_TAG_DEFAULT);

    brpc::EventDispatcher dispatcher;
    LtWakeReArmCtx ctx;
    brpc::IOEventDataOptions opts{
        &LtWakeReArmProbe::OnInputEvent, &LtWakeReArmProbe::OnOutputEvent, &ctx};
    brpc::IOEventDataId data_id_a = brpc::INVALID_IO_EVENT_DATA_ID;
    brpc::IOEventDataId data_id_b = brpc::INVALID_IO_EVENT_DATA_ID;
    ASSERT_EQ(0, brpc::IOEventData::Create(&data_id_a, opts));
    ASSERT_EQ(0, brpc::IOEventData::Create(&data_id_b, opts));
    ASSERT_EQ(0, dispatcher.AddConsumer(data_id_a, fds_a[0]));
    ASSERT_EQ(0, dispatcher.AddConsumer(data_id_b, fds_b[0]));

    ASSERT_EQ(0, dispatcher.Start(nullptr));

    bool completed = false;
    for (int i = 0; i < 1000; ++i) {  // up to ~2s
        if (ctx.done.load(std::memory_order_acquire)) {
            completed = true;
            break;
        }
        usleep(2000);
    }
    ASSERT_TRUE(completed) << "both callbacks did not fire within the "
        "timeout -- see the batching precondition in the comment above "
        "this test";
    ASSERT_EQ(2, ctx.call_seq.load())
        << "test precondition failed: the two events were not delivered "
           "in the same epoll_wait() batch (n should have been 2), so "
           "this run cannot exercise the re-arm fix";
    ASSERT_NE(0u, ctx.observed[0]) << "wake was never armed for event 1";
    ASSERT_EQ(ctx.observed[0], ctx.observed[1])
        << "event 2 in the same batch observed a different "
           "tls_lt_epoll_wake than event 1 -- the TLS was not re-armed "
           "immediately before its callback, so a value written between "
           "the two calls (standing in for a cross-pthread migration) "
           "leaked through";

    close(fds_a[0]);
    close(fds_a[1]);
    close(fds_b[0]);
    close(fds_b[1]);
}
#endif  // defined(BRPC_LATENCY_TRACE)

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
    // The property design doc sec.10.1 requires for instantaneous-event
    // points (the default, and every point except write_start/read_start
    // -- see StampLastIsLastWriteWins below for that pair): a point
    // already holding a non-zero value is left alone, and a later write
    // is discarded. Using LT_C_RPC_END here deliberately -- an
    // unambiguous instantaneous event -- rather than write_start, which
    // now uses StampLast() instead of Stamp() (design doc sec.10.1; a
    // fix-round correction reversed an earlier, wrong ruling that had
    // write_start on this first-write-wins path).
    brpc::LatencyTraceBuffer* b = brpc::LatencyTraceBuffer::instance();
    const brpc::LatencyTraceHandle h = b->AllocSlot(8, brpc::LT_ROLE_CLIENT);
    b->Stamp(h, brpc::LT_C_RPC_END);
    const uint32_t first = b->GetForTest(h)->ts[brpc::LT_C_RPC_END];

    // Busy-wait so a second Stamp() call would, if it were not discarded,
    // observe a strictly later (and thus different) counter delta.
    const uint64_t start = butil::detail::clock_cycles();
    while (butil::detail::clock_cycles() - start < 1000) {}
    b->Stamp(h, brpc::LT_C_RPC_END);
    ASSERT_EQ(first, b->GetForTest(h)->ts[brpc::LT_C_RPC_END])
        << "second Stamp() call must be discarded, not overwrite the first";
}

TEST_F(LatencyTraceBufferTest, StampLastIsLastWriteWins) {
    // Mirror of StampIsFirstWriteWins above, for the opposite policy.
    // write_start's meaning is "the start of the operation that actually
    // completed this unit" -- Socket::DoWrite's lt_stamp_write_start
    // lambda calls this on every KeepWrite iteration for a request that
    // needs more than one writev, and only the LAST call's timestamp is
    // the correct queue/syscall boundary for this request (design doc
    // sec.10.1). Using LT_C_WRITE_START, the actual production call
    // site's point, rather than a generic one.
    brpc::LatencyTraceBuffer* b = brpc::LatencyTraceBuffer::instance();
    const brpc::LatencyTraceHandle h = b->AllocSlot(9, brpc::LT_ROLE_CLIENT);
    b->StampLast(h, brpc::LT_C_WRITE_START);
    const uint32_t first = b->GetForTest(h)->ts[brpc::LT_C_WRITE_START];
    ASSERT_GT(first, 0u);

    // Busy-wait so the second call observes a strictly later counter
    // delta if it takes effect.
    const uint64_t start = butil::detail::clock_cycles();
    while (butil::detail::clock_cycles() - start < 1000) {}
    b->StampLast(h, brpc::LT_C_WRITE_START);
    ASSERT_GT(b->GetForTest(h)->ts[brpc::LT_C_WRITE_START], first)
        << "second StampLast() call must overwrite the first, not be discarded";
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
    //
    // Fix-round item 1 changed what "full" means: before, one thread only
    // ever used one shard, so 8 allocations (this shard's capacity) were
    // enough to exhaust it and see a drop. Now AllocSlot falls back to
    // another shard once the caller's home shard is full (see the class
    // comment in latency_trace.h and SingleThreadFillsEntireConfigured
    // CapacityViaFallback below, which is dedicated to that fallback
    // itself) -- "full" means every shard is, so this test must actually
    // exhaust all SHARD_COUNT of them before the drop it asserts on is a
    // real one and not just an early return this rewrite failed to catch.
    brpc::LatencyTraceBuffer* b = brpc::LatencyTraceBuffer::instance();
    const int kPerShardCapacity = 8;   // already a power of two: exact,
                                        // no NextPowerOfTwo rounding to
                                        // reason about
    const int total_capacity =
        brpc::LatencyTraceBuffer::SHARD_COUNT * kPerShardCapacity;
    b->ResetForTest(total_capacity);
    b->set_stop_when_full_for_test(true);
    for (int i = 0; i < total_capacity; ++i) {
        ASSERT_NE(brpc::LT_INVALID_HANDLE, b->AllocSlot(i, brpc::LT_ROLE_CLIENT))
            << "record " << i << " of " << total_capacity << " should "
               "still fit -- either this thread's home shard or a "
               "fallback shard must have room until every shard is full";
    }
    ASSERT_EQ(brpc::LT_INVALID_HANDLE, b->AllocSlot(999, brpc::LT_ROLE_CLIENT))
        << "every shard is genuinely full now -- this one must be dropped";
    ASSERT_EQ((size_t)total_capacity, b->recorded_count());
    ASSERT_EQ(1u, b->dropped_count());
}

TEST_F(LatencyTraceBufferTest,
       SingleThreadFillsEntireConfiguredCapacityViaFallback) {
    // Fix-round item 1: -latency_trace_capacity must mean what it says
    // even for a single-threaded producer -- this project's own
    // motivating use case is a synchronous benchmark client (design doc
    // sec.1.1), and AllocSlot pins a thread to one "home" shard for
    // life. Before this fix, a single-threaded caller could only ever
    // fill 1/SHARD_COUNT of the requested capacity before every further
    // call was dropped, even with 15/16 of the buffer still empty --
    // see task-a2-report.md's "What was run" for the real capture that
    // surfaced this (100000 requested, only 8192 usable). The fix: once
    // a thread's own shard is full, AllocSlot falls back to another
    // shard instead of dropping, and only refuses once every shard is
    // full.
    brpc::LatencyTraceBuffer* b = brpc::LatencyTraceBuffer::instance();
    const int kShards = brpc::LatencyTraceBuffer::SHARD_COUNT;
    const int kPerShardCapacity = 256;   // already a power of two: exact,
                                          // no NextPowerOfTwo rounding to
                                          // reason about
    const int total_capacity = kShards * kPerShardCapacity;
    b->ResetForTest(total_capacity);
    b->set_stop_when_full_for_test(true);

    // Every call below runs on this one test thread, so it all lands on
    // a single "home" shard until fallback kicks in -- exactly the
    // single-threaded-client shape this fix targets.
    for (int i = 0; i < total_capacity; ++i) {
        ASSERT_NE(brpc::LT_INVALID_HANDLE, b->AllocSlot(i, brpc::LT_ROLE_CLIENT))
            << "record " << i << " of " << total_capacity << " should "
               "have been accepted -- either the home shard's fast path "
               "or a fallback shard must still have room";
    }
    // One past capacity: every shard is genuinely full now, so this one
    // (and only this one) must be dropped.
    ASSERT_EQ(brpc::LT_INVALID_HANDLE,
              b->AllocSlot(999999, brpc::LT_ROLE_CLIENT));

    ASSERT_EQ((size_t)total_capacity, b->recorded_count())
        << "the whole configured capacity must be usable by a single "
           "thread, not just one shard's worth of it";
    ASSERT_EQ(1u, b->dropped_count());

    // Prove the fallback actually spread records across shards, rather
    // than the counts above coincidentally matching some other way:
    // every trace_id in [0, total_capacity) must appear exactly once via
    // GetBySeqForTest's flat traversal (which is robust to allocation
    // order -- see its own comment on why fallback breaks "shard order
    // == chronological order" but not "no gaps, no wraparound").
    std::set<uint64_t> seen;
    for (uint64_t seq = 0; ; ++seq) {
        const brpc::LatencyTraceRecord* r = b->GetBySeqForTest(seq);
        if (r == nullptr) {
            break;
        }
        ASSERT_TRUE(seen.insert(r->trace_id).second)
            << "trace_id " << r->trace_id << " observed twice -- a slot "
               "was written into by two different records (a fallback "
               "correctness bug), not just visited in a different order";
    }
    ASSERT_EQ((size_t)total_capacity, seen.size());
    for (uint64_t i = 0; i < (uint64_t)total_capacity; ++i) {
        ASSERT_EQ(1u, seen.count(i))
            << "trace_id " << i << " missing -- a gap in some shard's "
               "written-slot range, breaking GetBySeqForTest's "
               "no-wraparound assumption";
    }
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
    // Task 2's ASSERT_EQ(192u, sizeof(LatencyTraceRecord))). A self-
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
    // Head/tail calibration pairs must bracket a positive interval. The
    // monotonic pair is what counter_freq_hz above is actually computed
    // from (fix-round item 6: these fields hold CLOCK_MONOTONIC, not
    // CLOCK_REALTIME -- renamed from *_realtime_ns to say so).
    ASSERT_GT(hdr.tail_monotonic_ns, hdr.head_monotonic_ns);
    ASSERT_GT(hdr.tail_counter, hdr.head_counter);
    // The genuine CLOCK_REALTIME pair (fix-round item 6) exists only for
    // an offline cross-host sanity check and plays no part in the
    // calibration above, but must still be real, moving wall-clock time.
    ASSERT_GT(hdr.tail_realtime_ns, hdr.head_realtime_ns);

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

// Fix-round item 3 (review of Task 13): Dump() used to snapshot
// hdr.record_count via recorded_count() BEFORE the record-writing loop,
// then derive method_table_offset from that early snapshot. If more
// slots finish allocating (and publish via AllocSlot's release-store to
// slot_seq) while the loop is still running, the loop -- which scans
// live slot state, not the snapshot -- writes more records than the
// snapshot counted, so the header ends up claiming a record_count (and
// therefore a method_table_offset) that does not match what actually
// landed on disk. Dump()'s own return value (`written`) is always the
// ground truth: it is the exact number of records the loop fwrote.
//
// Producing that exact race deterministically is not possible from a
// test -- but running unthrottled producers is actually
// counterproductive: this buffer's capacity is only ever in the tens or
// hundreds of thousands, and a handful of threads hammering AllocSlot()
// as fast as possible saturate that well within the first millisecond of
// Dump()'s own ~100ms calibration-window sleep, long before Dump() even
// takes its early snapshot -- at which point production has already
// stopped and there is nothing left to race. Instead, each producer
// below sleeps briefly between allocations so it keeps trickling new
// records in for the ENTIRE duration of the Dump() call (calibration
// sleep, method-table snapshot, and the record-writing loop alike)
// without ever exhausting capacity -- and the whole thing runs several
// rounds so that a race landing in the (comparatively narrow) window
// between the early snapshot and the loop's completion is caught with
// high probability even though no single round guarantees it.
TEST_F(LatencyTraceBufferTest, DumpHeaderMatchesActuallyWrittenRecordCount) {
    brpc::LatencyTraceBuffer* b = brpc::LatencyTraceBuffer::instance();

    const int kRounds = 20;
    for (int round = 0; round < kRounds; ++round) {
        b->ResetForTest(100000);
        b->set_stop_when_full_for_test(true);

        std::atomic<bool> stop(false);
        std::vector<std::thread> workers;
        for (int i = 0; i < 4; ++i) {
            workers.emplace_back([b, &stop]() {
                uint64_t local_seq = 0;
                while (!stop.load(std::memory_order_relaxed)) {
                    const uint64_t trace_id = brpc::MakeLatencyTraceId(local_seq++);
                    const brpc::LatencyTraceHandle h =
                        b->AllocSlot(trace_id, brpc::LT_ROLE_CLIENT);
                    b->Stamp(h, brpc::LT_C_RPC_START);
                    usleep(50);  // throttle -- see this test's top comment
                }
            });
        }

        char path[64];
        snprintf(path, sizeof(path), "/tmp/brpc_lt_dump_race_test_%d.bin", round);
        const int written = b->Dump(path);

        stop.store(true, std::memory_order_relaxed);
        for (auto& w : workers) {
            w.join();
        }

        ASSERT_GE(written, 0) << "Dump() failed outright, round=" << round;

        FILE* fp = fopen(path, "rb");
        ASSERT_TRUE(fp != nullptr);
        brpc::LatencyTraceFileHeader hdr;
        ASSERT_EQ(1u, fread(&hdr, sizeof(hdr), 1, fp));
        fclose(fp);
        unlink(path);

        ASSERT_EQ((uint64_t)written, hdr.record_count)
            << "round=" << round << ": hdr.record_count must match how "
               "many records Dump() actually wrote (its own return "
               "value), not an early snapshot taken before the write "
               "loop ran";
        ASSERT_EQ(sizeof(hdr) + (uint64_t)written * sizeof(brpc::LatencyTraceRecord),
                  hdr.method_table_offset)
            << "round=" << round << ": method_table_offset must point "
               "exactly past the records actually written, not past a "
               "stale pre-loop count";
    }
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
// either assumption above. Its low 32 bits come from
// NextLatencyTraceSeq(), the one process-wide counter shared by every
// attempt this process ever issues -- the first send (Channel::
// CallMethod) and any retry/backup-request attempt (Controller::
// IssueRPC) alike (see MakeLatencyTraceId; sharing that one counter is
// what the fix round's item 1 made true -- it used to be two independent
// counters); the server's record carries that exact same trace_id via
// propagation -- baidu_rpc_protocol.cpp reads request_meta.
// latency_trace_id() off the wire rather than minting its own (see
// ProcessRpcRequest). So "max trace_id for this role" means "most
// recently issued RPC" regardless of which shard either side's record
// ended up in.
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

// Fix-round items 1 and 3: unlike FindLastRecordByRole above, retry and
// backup tests below need the SERVER record belonging to one SPECIFIC
// client attempt -- not simply "the most recent server record" -- since
// more than one attempt (and therefore more than one server-side
// request) can exist for a single logical RPC. The server's `attempt`
// field is useless for this: it is only ever meaningfully set on the
// client side (see Controller::IssueRPC's `rec->attempt =
// (uint8_t)_current_call.nretry`; the server's own AllocSlot always
// leaves it 0, design doc's D9 attempt numbering was never extended to
// the server side). trace_id is what actually ties a server record back
// to the one client attempt that produced it: the server reads
// `request_meta.latency_trace_id()` off the wire rather than minting its
// own (see ProcessRpcRequest), so the two sides' trace_ids for one
// attempt are identical by construction.
static const brpc::LatencyTraceRecord* FindServerRecordForTraceId(
        uint64_t trace_id) {
    brpc::LatencyTraceBuffer* b = brpc::LatencyTraceBuffer::instance();
    for (uint64_t seq = 0; ; ++seq) {
        const brpc::LatencyTraceRecord* r = b->GetBySeqForTest(seq);
        if (r == nullptr) {
            return nullptr;
        }
        if (r->role == brpc::LT_ROLE_SERVER && r->trace_id == trace_id) {
            return r;
        }
    }
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
    // ends, not just the client's 17 (D14 dropped rsp_process_start/end).
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

    // Sigma identity (design doc sec.5): the 15 client intervals excluding
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
    ASSERT_EQ(e2e, sum) << "Sigma(33 items) must equal end-to-end exactly";

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
        { brpc::LT_C_WRITE_START,           brpc::LT_C_WRITE_END,            "cli_write" },
    };
    for (const auto& it : kClientItems) {
        ASSERT_GE((int64_t)c->ts[it.hi] - (int64_t)c->ts[it.lo], 0) << it.name;
    }

    // 16 server items (S01-S17, sec.5.2).
    static const struct { int lo, hi; const char* name; } kServerItems[] = {
        { brpc::LT_S_WAKE,                  brpc::LT_S_ONEDGE_START,          "srv_wake_to_onedge" },
        { brpc::LT_S_ONEDGE_START,          brpc::LT_S_READ_START,           "srv_onedge_to_read" },
        { brpc::LT_S_READ_START,           brpc::LT_S_MSG_RECV_DONE,         "srv_read" },
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
        { brpc::LT_S_WRITE_START,           brpc::LT_S_WRITE_END,             "srv_write" },
    };
    for (const auto& it : kServerItems) {
        ASSERT_GE((int64_t)s->ts[it.hi] - (int64_t)s->ts[it.lo], 0) << it.name;
    }

    // 8 client receive-side items (C09-C17, sec.5.3; D14 collapsed
    // cli_post_deser/cli_callback/cli_rpc_finish into one item).
    static const struct { int lo, hi; const char* name; } kClientRecvItems[] = {
        { brpc::LT_C_WAKE,                  brpc::LT_C_ONEDGE_START,          "cli_wake_to_onedge" },
        { brpc::LT_C_ONEDGE_START,          brpc::LT_C_READ_START,           "cli_onedge_to_read" },
        { brpc::LT_C_READ_START,           brpc::LT_C_MSG_RECV_DONE,         "cli_read" },
        { brpc::LT_C_MSG_RECV_DONE,         brpc::LT_C_RSP_META_DESER_START,  "cli_recv_to_deser" },
        { brpc::LT_C_RSP_META_DESER_START,  brpc::LT_C_RSP_META_DESER_END,    "cli_rsp_meta_deser" },
        { brpc::LT_C_RSP_META_DESER_END,    brpc::LT_C_RSP_PAYLOAD_DESER_START, "cli_lookup_cntl" },
        { brpc::LT_C_RSP_PAYLOAD_DESER_START, brpc::LT_C_RSP_PAYLOAD_DESER_END, "cli_rsp_payload_deser" },
        { brpc::LT_C_RSP_PAYLOAD_DESER_END, brpc::LT_C_RPC_END,               "cli_post_deser" },  // D14: collapsed cli_post_deser+cli_callback+cli_rpc_finish
    };
    for (const auto& it : kClientRecvItems) {
        ASSERT_GE((int64_t)c->ts[it.hi] - (int64_t)c->ts[it.lo], 0) << it.name;
    }

    // 2 derived link items (sec.6.1, model A): total link time L = RTT - S
    // (both terms purely intra-record, so the +1 encoding cancels exactly
    // the same way it does in the sigma identity above), halved. Verified
    // via the exact same formula as LinkTimeIsNonNegativeAtOutstandingOne;
    // duplicated here (rather than only relying on that test) because this
    // test's job is specifically "all 33 named items are individually
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
// C17 (rpc_end) on the regular async path (Controller::EndRPC) and on the
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
// EndRPC's C17 (rpc_end) stamp -- and, on the -usercode_in_pthread path,
// DoneInBackupThread's -- lands on the RPC-processing thread (D14: right
// after that thread's own OnRPCEnd() call, strictly BEFORE `_done->Run()`
// -- see controller.cpp), which is not this test thread. Posting the
// request (`stub.Echo(..., new AsyncEchoDone)`) returns as soon as the
// request is written, long before any response-side stamp exists. There
// is no synchronization primitive between "Echo() returned" and "C17 is
// stamped", so rather than reading the record the instant Echo() posts
// the request, this helper polls the record itself for C17 (LT_C_RPC_END)
// to go non-zero, bounded by a generous timeout. If C17 is never stamped
// (e.g. item 1's or item 2's fix is missing) this poll times out and the
// test fails on that specific assertion instead of silently reading a
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

        // Fix-round item 2: check the RPC's own outcome BEFORE asserting on
        // the wait's result, not after. ASSERT_TRUE is fatal (it returns
        // from this function immediately on failure), so with the old
        // order an RPC that failed outright and one that succeeded but
        // never got C17 stamped were indistinguishable -- both stopped at
        // "timed out waiting for C17" and cntl.Failed() was never reached.
        // Safe to read cntl here regardless of whether the wait timed out:
        // cntl's error state was finalized by OnRPCEnd(), which runs
        // before _done->Run() is ever invoked -- strictly before either
        // outcome below.
        ASSERT_FALSE(cntl.Failed()) << cntl.ErrorText();
        ASSERT_TRUE(stamped)
            << "timed out waiting for C17 (LT_C_RPC_END) to be stamped "
               "on async request " << i;

        // Fix-round item 1: the poll above only proves the plain,
        // non-atomic ts[LT_C_RPC_END] WORD it read is itself visible on
        // this thread -- that word is aligned and cannot tear, and the
        // wait is bounded so it cannot hang, but neither property says
        // anything about this record's EARLIER ts[] entries, written
        // (possibly from a different thread/bthread) before C17. aarch64
        // is not other-multi-copy-atomic for plain stores: this thread
        // observing the producer's last store does not imply it observes
        // every earlier store that same producer made, in the same
        // order. A stale read reads back 0, which is the SAFE direction
        // (it trips the non-zero assertion below rather than passing
        // silently) but would send someone chasing an instrumentation
        // bug that does not actually exist.
        //
        // An acquire *load* on the polled word -- e.g. loading
        // ts[LT_C_RPC_END] with acquire semantics instead of the plain
        // read WaitForTrue's predicate does today -- would NOT have been
        // enough. Acquire only orders THIS thread's later operations
        // (program order) to not move before the load; it says nothing
        // about ordering the writes that happen-before it on the
        // PRODUCER's side into visibility here. So a fence belongs
        // between "observed the sentinel" and "read the earlier fields".
        //
        // What that fence does and does not buy us, honestly: it forbids
        // the compiler from hoisting the reads that follow it (the
        // earlier ts[] entries pulled by the caller) above it, and on
        // aarch64 it lowers to a real hardware barrier (DMB ISH) that
        // narrows the practical window for those reads to observe a
        // stale value. It does NOT establish a synchronizes-with edge in
        // the C++ memory model: atomic_thread_fence only participates in
        // synchronizes-with when paired with an atomic operation on the
        // SAME object on both the read and write side. Stamp() writes
        // ts[] with plain, non-atomic stores (see design doc sec.8.1's
        // check-then-write discussion), so there is no atomic release on
        // the write side for this acquire fence to pair with -- this is
        // a barrier on the reader alone. On hardware that is not
        // multi-copy-atomic, that alone does not formally close the
        // hazard; a writer-side release would also be required for a
        // formal guarantee. We do not add one (see below), so what we
        // have here is a real, useful mitigation, not a proof: the
        // empirical evidence -- this suite passing repeatedly on the
        // actual aarch64 host -- is the only kind obtainable given that.
        //
        // Stamp() itself deliberately uses plain, unfenced stores (see
        // design doc sec.8.1) -- that is an accepted trade-off on the
        // write side, not an oversight. Do not "close the gap" by adding
        // a release fence to Stamp(): that would put an ordering
        // constraint on the hot path this project measured carefully to
        // avoid. The fence below is still worth keeping as the best
        // available reader-side mitigation.
        butil::atomic_thread_fence(butil::memory_order_acquire);
    }

    *out_client = FindLastClientRecordForTest();
    *out_server = FindLastServerRecordForTest();

    server.Stop(0);
    server.Join();
}

// Fix-round items 1 and 3: the first three of design doc sec.10.1's four
// weight-bearing checks (every point non-zero, monotonic, C09>=C08) are
// purely LOCAL to each side's own record -- they hold regardless of how
// many other requests are concurrently in flight anywhere else in the
// process. Split out from AssertAllWeightBearingInvariants below so a
// caller whose scenario does NOT satisfy the outstanding=1 precondition
// (e.g. a backup request, where the server is by construction processing
// the original and the backup at the same time) can still run these
// three without also running the fourth -- design doc sec.10.1 itself
// says the decomposition-item/link-time check requires outstanding=1
// ("高并发下不做非负断言，只统计负值比例").
static void AssertPerRecordCoreInvariants(const brpc::LatencyTraceRecord* c,
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
}

// Shared body for the two async E2E tests below: section 10.1's full set
// of weight-bearing checks (the three local ones above, plus the
// outstanding=1-only decomposition-item/link-time check), factored out
// so they aren't duplicated verbatim a third time (the sync tests above
// already state them twice, split by concern -- this collapses all of
// them into one call site per async test). Both of those tests issue one
// RPC at a time on an otherwise-idle server, so outstanding=1 genuinely
// holds; a caller that cannot make that guarantee should call
// AssertPerRecordCoreInvariants above instead -- see its comment.
static void AssertAllWeightBearingInvariants(const brpc::LatencyTraceRecord* c,
                                              const brpc::LatencyTraceRecord* s) {
    AssertPerRecordCoreInvariants(c, s);
    if (::testing::Test::HasFatalFailure()) {
        return;
    }
    const int64_t rtt = (int64_t)c->ts[brpc::LT_C_WAKE] -
                        (int64_t)c->ts[brpc::LT_C_WRITE_END];

    // Every decomposition item non-negative at outstanding=1 (design doc
    // sec.5's 7 client-send + 16 server + 8 client-receive items, plus
    // the 2 derived link halves; D14 collapsed 3 client-receive items into
    // 1, so 10 -> 8).
    static const struct { int lo, hi; const char* name; } kClientItems[] = {
        { brpc::LT_C_RPC_START,             brpc::LT_C_REQ_PAYLOAD_SER_START, "cli_pre_serialize" },
        { brpc::LT_C_REQ_PAYLOAD_SER_START, brpc::LT_C_REQ_PAYLOAD_SER_END,   "cli_req_payload_ser" },
        { brpc::LT_C_REQ_PAYLOAD_SER_END,   brpc::LT_C_REQ_META_SER_START,    "cli_issue_rpc" },
        { brpc::LT_C_REQ_META_SER_START,    brpc::LT_C_REQ_META_SER_END,      "cli_req_meta_ser" },
        { brpc::LT_C_REQ_META_SER_END,      brpc::LT_C_WRITE_ENQUEUE,         "cli_pack_to_write" },
        { brpc::LT_C_WRITE_ENQUEUE,         brpc::LT_C_WRITE_START,           "cli_write_queue" },
        { brpc::LT_C_WRITE_START,           brpc::LT_C_WRITE_END,             "cli_write" },
    };
    for (const auto& it : kClientItems) {
        ASSERT_GE((int64_t)c->ts[it.hi] - (int64_t)c->ts[it.lo], 0) << it.name;
    }

    static const struct { int lo, hi; const char* name; } kServerItems[] = {
        { brpc::LT_S_WAKE,                   brpc::LT_S_ONEDGE_START,           "srv_wake_to_onedge" },
        { brpc::LT_S_ONEDGE_START,           brpc::LT_S_READ_START,            "srv_onedge_to_read" },
        { brpc::LT_S_READ_START,            brpc::LT_S_MSG_RECV_DONE,          "srv_read" },
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
        { brpc::LT_S_WRITE_START,            brpc::LT_S_WRITE_END,              "srv_write" },
    };
    for (const auto& it : kServerItems) {
        ASSERT_GE((int64_t)s->ts[it.hi] - (int64_t)s->ts[it.lo], 0) << it.name;
    }

    static const struct { int lo, hi; const char* name; } kClientRecvItems[] = {
        { brpc::LT_C_WAKE,                    brpc::LT_C_ONEDGE_START,           "cli_wake_to_onedge" },
        { brpc::LT_C_ONEDGE_START,            brpc::LT_C_READ_START,            "cli_onedge_to_read" },
        { brpc::LT_C_READ_START,             brpc::LT_C_MSG_RECV_DONE,          "cli_read" },
        { brpc::LT_C_MSG_RECV_DONE,           brpc::LT_C_RSP_META_DESER_START,   "cli_recv_to_deser" },
        { brpc::LT_C_RSP_META_DESER_START,    brpc::LT_C_RSP_META_DESER_END,     "cli_rsp_meta_deser" },
        { brpc::LT_C_RSP_META_DESER_END,      brpc::LT_C_RSP_PAYLOAD_DESER_START, "cli_lookup_cntl" },
        { brpc::LT_C_RSP_PAYLOAD_DESER_START, brpc::LT_C_RSP_PAYLOAD_DESER_END,  "cli_rsp_payload_deser" },
        { brpc::LT_C_RSP_PAYLOAD_DESER_END,   brpc::LT_C_RPC_END,                "cli_post_deser" },  // D14: collapsed cli_post_deser+cli_callback+cli_rpc_finish
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

// ---------------------------------------------------------------------
// D14 (item 1's core semantic claim): C17 (rpc_end) must be stamped at the
// same instant brpc calls OnRPCEnd() -- strictly BEFORE `_done->Run()`
// executes, never after. None of the invariant checks above (here or in
// AssertAllWeightBearingInvariants) can observe this ordering: they only
// check that C17 eventually becomes non-zero, monotonic, and non-negative
// relative to earlier points -- properties an implementation that stamps
// C17 AFTER `_done->Run()` returns would satisfy identically, because the
// closures used everywhere else in this file (AsyncEchoDone) do nothing
// but self-delete and never touch the record. Prove the ordering directly
// by comparing raw counter reads instead of relying on any pass/fail
// derived from ts[] alone: capture the counter value at the moment the
// callback body starts running, then decode C17's own stamp back into a
// raw counter value -- base_counter + (ts[C17] - 1), the exact inverse of
// EncodeOffset (see LatencyTraceRecord::ts's comment) -- and assert the
// latter is NOT LATER than the former. A callback that also sleeps for a
// while widens the window: if C17 were (bug) stamped only after `Run()`
// completes, its decoded raw counter would land somewhere during or after
// that sleep, strictly AFTER entry_cycles, not at-or-before it.
// ---------------------------------------------------------------------

class TimingProbeAsyncDone : public google::protobuf::Closure {
public:
    TimingProbeAsyncDone(std::atomic<uint64_t>* entry_cycles,
                          std::atomic<bool>* done_flag)
        : _entry_cycles(entry_cycles), _done_flag(done_flag) {}
    void Run() override {
        _entry_cycles->store(butil::detail::clock_cycles(),
                              std::memory_order_release);
        // Widen the window a buggy "stamp after Run()" implementation
        // would land in -- see this section's block comment above.
        bthread_usleep(20 * 1000);
        _done_flag->store(true, std::memory_order_release);
        delete this;
    }
private:
    std::atomic<uint64_t>* _entry_cycles;
    std::atomic<bool>* _done_flag;
};

// Shared body: issue one async Echo with a TimingProbeAsyncDone, wait for
// the callback to finish, then assert C17's decoded raw counter is
// at-or-before the counter value sampled at the callback's own entry.
static void AssertRpcEndPrecedesAsyncCallback(int port) {
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
    test::EchoRequest req;
    test::EchoResponse res;
    brpc::Controller cntl;
    req.set_message("timing-probe");

    std::atomic<uint64_t> entry_cycles{0};
    std::atomic<bool> done_flag{false};
    stub.Echo(&cntl, &req, &res,
              new TimingProbeAsyncDone(&entry_cycles, &done_flag));

    ASSERT_TRUE(WaitForTrue([&done_flag]() {
        return done_flag.load(std::memory_order_acquire);
    }, 2000)) << "async callback never ran";
    ASSERT_FALSE(cntl.Failed()) << cntl.ErrorText();

    const brpc::LatencyTraceRecord* c = FindLastClientRecordForTest();
    ASSERT_TRUE(c != nullptr);
    ASSERT_NE(0u, c->ts[brpc::LT_C_RPC_END]) << "C17 (rpc_end) never stamped";

    const uint64_t rpc_end_raw =
        c->base_counter + (uint64_t)(c->ts[brpc::LT_C_RPC_END] - 1);
    const uint64_t entry_raw = entry_cycles.load(std::memory_order_acquire);
    ASSERT_LE(rpc_end_raw, entry_raw)
        << "C17 (rpc_end) must be stamped at/before the async callback "
           "starts running -- found it stamped AFTER the callback had "
           "already started, meaning the callback's own duration leaked "
           "into what should be pure RPC latency (design doc sec.3.3/D14)";

    server.Stop(0);
    server.Join();
}

TEST(LatencyTraceE2ETest, RpcEndPrecedesAsyncCallback) {
    // Item 1's target: the regular async branch of Controller::EndRPC
    // (-usercode_in_pthread stays at its default, false).
    brpc::FLAGS_latency_trace_enabled = true;
    brpc::LatencyTraceBuffer::instance()->ResetForTest(1024);

    AssertRpcEndPrecedesAsyncCallback(9540);

    brpc::FLAGS_latency_trace_enabled = false;
}

TEST(LatencyTraceE2ETest, RpcEndPrecedesAsyncCallbackWithUsercodeInPthread) {
    // Item 1's target: Controller::DoneInBackupThread(), reached only via
    // RunUserCode(RunDoneInBackupThread, this).
    const bool saved_usercode_in_pthread = brpc::FLAGS_usercode_in_pthread;
    struct Restore {
        const bool* saved;
        ~Restore() { brpc::FLAGS_usercode_in_pthread = *saved; }
    } restore{&saved_usercode_in_pthread};

    brpc::FLAGS_usercode_in_pthread = true;
    brpc::FLAGS_latency_trace_enabled = true;
    brpc::LatencyTraceBuffer::instance()->ResetForTest(1024);

    AssertRpcEndPrecedesAsyncCallback(9541);

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

// This round's item 2: ControllerPrivateAccessor::latency_trace_handle_for_
// response()'s fallback -- reached when a response's correlation id
// matches neither `_current_call` nor a live `_unfinished_call` -- used to
// return `_cntl->_lt_handle` (the CONTROLLER-level handle, i.e. whichever
// attempt was allocated most recently) instead of LT_INVALID_HANDLE. That
// is wrong, not merely imprecise: `_stop_when_full` (the only supported
// production setting) means slots are never recycled, so the generation
// guard the old comment invoked does NOT refuse the resulting write -- it
// silently lands a stale attempt's stamps onto whatever live record
// `_lt_handle` currently names.
//
// The actual trigger is a genuine network race -- a superseded attempt's
// response literally arriving after a retry has already been issued over
// a (possibly different) connection -- which is not deterministically
// constructible from a test. Exercise the resolver directly instead,
// against a Controller left in a REAL post-retry state by a real (failed)
// RPC: same dead-backend/max_retry=2 shape as EachRetryAttemptGetsItsOwn
// Record above, so `_current_call.nretry == 2` and `_unfinished_call ==
// nullptr` (plain retry, not backup -- backup keeps the superseded
// attempt alive in `_unfinished_call`, which is a different, already-
// covered resolution path). Ask for the handle a response for attempt 0
// -- the FIRST, long-superseded attempt, whose Call no longer exists --
// would have resolved to.
TEST(LatencyTraceMetaTest, SupersededRetryAttemptResponseHandleFallsBackToInvalid) {
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

    // Call::id() == correlation_id.value + nretry + 1 (Controller::get_id());
    // attempt 0's id is therefore call_id().value + 1.
    brpc::ControllerPrivateAccessor accessor(&cntl);
    const int64_t attempt0_id = (int64_t)cntl.call_id().value + 1;
    const brpc::LatencyTraceHandle stale_handle =
        accessor.latency_trace_handle_for_response(attempt0_id);
    ASSERT_EQ(brpc::LT_INVALID_HANDLE, stale_handle)
        << "a response for a superseded attempt whose Call is gone must "
           "resolve to LT_INVALID_HANDLE, not fall back to the live "
           "Controller-level handle -- which by now names attempt 2's "
           "record (the last one IssueRPC allocated), not attempt 0's; "
           "returning it would let this stale response's stamps overwrite "
           "attempt 2's live record";

    brpc::FLAGS_latency_trace_enabled = false;
}

// Fix-round item 1 (review of Task 13): channel.cpp's first-attempt
// allocation and controller.cpp's retry/backup-request allocation used
// to draw from two INDEPENDENT counters, each starting near zero -- so a
// process's first-ever RPC and its first-ever retry could (and, on
// alignment, would) mint the exact same trace id, breaking the
// cross-process join's uniqueness guarantee.
//
// This is deliberately NOT a test that a specific pair of trace ids
// collides -- whether the two old counters were ever exactly aligned by
// the time this test runs depends on how many RPCs/retries earlier tests
// in this same binary already issued, which this test does not control.
// Instead it asserts a property that holds if and only if every attempt
// this process ever issues -- first sends and retries/backups alike --
// draws from ONE shared, monotonically increasing counter: a retry's own
// sequence number must be strictly greater than every sequence number
// already handed out to any first-attempt allocation before it, no
// matter how much unrelated traffic (from other tests) already advanced
// either counter. Under the old, independent-counters code this fails
// deterministically: the retry-only counter's value the first time this
// test (or any earlier test) forces a retry is small (0, 1, 2, ...)
// while the first-attempt counter has already been advanced far past
// that by every other RPC-issuing test that ran first in this binary --
// so the retry's sequence number ends up SMALLER than the first-attempt
// sequence numbers already observed, not larger.
TEST(LatencyTraceMetaTest, RetryAttemptSeqContinuesInitialAttemptSeq) {
    brpc::FLAGS_latency_trace_enabled = true;
    brpc::LatencyTraceBuffer::instance()->ResetForTest(1024);

    brpc::Server server;
    LatencyTraceEchoServiceImpl svc;
    ASSERT_EQ(0, server.AddService(&svc, brpc::SERVER_DOESNT_OWN_SERVICE));
    ASSERT_EQ(0, server.Start(9535, nullptr));

    brpc::Channel channel;
    brpc::ChannelOptions opt;
    opt.protocol = brpc::PROTOCOL_BAIDU_STD;
    ASSERT_EQ(0, channel.Init("127.0.0.1:9535", &opt));
    test::EchoService_Stub stub(&channel);

    // Several ordinary (first-attempt-only) RPCs, advancing whichever
    // counter backs Channel::CallMethod's allocation, and remembering
    // the largest sequence number (trace_id's low 32 bits) any of them
    // produced.
    uint32_t max_initial_seq = 0;
    for (int i = 0; i < 5; ++i) {
        test::EchoRequest req;
        test::EchoResponse res;
        brpc::Controller cntl;
        req.set_message("x");
        stub.Echo(&cntl, &req, &res, nullptr);
        ASSERT_FALSE(cntl.Failed()) << cntl.ErrorText();
        const brpc::LatencyTraceRecord* r = FindLastClientRecordForTest();
        ASSERT_TRUE(r != nullptr);
        max_initial_seq = std::max(max_initial_seq, (uint32_t)r->trace_id);
    }
    server.Stop(0);
    server.Join();

    // Now force exactly one retry against a dead backend.
    brpc::Channel dead_channel;
    brpc::ChannelOptions dead_opt;
    dead_opt.protocol = brpc::PROTOCOL_BAIDU_STD;
    dead_opt.max_retry = 1;
    dead_opt.timeout_ms = 200;
    ASSERT_EQ(0, dead_channel.Init("127.0.0.1:9598", &dead_opt));  // nothing listening
    test::EchoService_Stub dead_stub(&dead_channel);
    test::EchoRequest req2;
    test::EchoResponse res2;
    brpc::Controller cntl2;
    req2.set_message("y");
    dead_stub.Echo(&cntl2, &req2, &res2, nullptr);
    ASSERT_TRUE(cntl2.Failed());

    const brpc::LatencyTraceRecord* retry_rec = nullptr;
    brpc::LatencyTraceBuffer* b = brpc::LatencyTraceBuffer::instance();
    for (uint64_t seq = 0; ; ++seq) {
        const brpc::LatencyTraceRecord* r = b->GetBySeqForTest(seq);
        if (r == nullptr) {
            break;
        }
        if (r->role == brpc::LT_ROLE_CLIENT && r->attempt == 1) {
            retry_rec = r;
        }
    }
    ASSERT_TRUE(retry_rec != nullptr) << "expected a retry-attempt record";

    ASSERT_GT((uint32_t)retry_rec->trace_id, max_initial_seq)
        << "the retry attempt's sequence number did not continue past "
           "every first-attempt sequence number already handed out -- "
           "the two allocation paths are drawing from separate counters "
           "and can mint colliding trace ids";

    brpc::FLAGS_latency_trace_enabled = false;
}

// Fix-round items 1 and 3 (whole-branch review of 685e6d9e): the retry and
// backup-request paths produce records but, before this test, had ZERO
// invariant assertions on them anywhere in this file -- both tests above
// only check the attempt set and (for the retry-sequence test) the trace
// id ordering. That is exactly why items 1 and 3 survived fourteen tasks
// and eight fix rounds: nothing here ever looked at whether a WINNING
// retried/backed-up attempt's record actually satisfies design doc
// sec.10.1's four weight-bearing invariants (every point non-zero,
// monotonic, C09>=C08, every decomposition item non-negative).
//
// Both existing retry tests above point at a dead backend on purpose (to
// exercise the attempt-numbering and trace-id-uniqueness properties they
// each check), so no attempt of theirs ever WINS -- there is no
// successful final record to check sec.10.1 against. This test drives a
// retry that actually succeeds: the first attempt fails with a
// retryable error code (ECONNRESET is in RpcRetryPolicy::DoRetry's
// list), the client's default retry policy reissues against the SAME
// live server over the SAME pooled connection (the response WAS
// received, so Controller::EndRPC's `error_code==0 || responded` guard
// keeps the connection in the pool for reuse -- this is a normal
// application-level failure response, not a socket-level one), and the
// second attempt succeeds -- giving a genuine winning attempt with
// attempt>0 to check.
class LatencyTraceRetryThenSucceedServiceImpl : public test::EchoService {
public:
    void Echo(google::protobuf::RpcController* cntl_base,
              const test::EchoRequest* request,
              test::EchoResponse* response,
              google::protobuf::Closure* done) override {
        brpc::ClosureGuard done_guard(done);
        brpc::Controller* cntl = static_cast<brpc::Controller*>(cntl_base);
        const int seen = _call_count.fetch_add(1, std::memory_order_relaxed);
        if (seen == 0) {
            cntl->SetFailed(ECONNRESET, "synthetic retryable failure");
            return;
        }
        response->set_message(request->message());
    }
private:
    std::atomic<int> _call_count{0};
};

TEST(LatencyTraceMetaTest, RetriedAttemptThatWinsGetsFullInvariants) {
    brpc::FLAGS_latency_trace_enabled = true;
    brpc::LatencyTraceBuffer::instance()->ResetForTest(1024);

    brpc::Server server;
    LatencyTraceRetryThenSucceedServiceImpl svc;
    ASSERT_EQ(0, server.AddService(&svc, brpc::SERVER_DOESNT_OWN_SERVICE));
    ASSERT_EQ(0, server.Start(9537, nullptr));

    brpc::Channel channel;
    brpc::ChannelOptions opt;
    opt.protocol = brpc::PROTOCOL_BAIDU_STD;
    opt.max_retry = 1;
    ASSERT_EQ(0, channel.Init("127.0.0.1:9537", &opt));

    test::EchoService_Stub stub(&channel);
    test::EchoRequest req;
    test::EchoResponse res;
    brpc::Controller cntl;
    req.set_message("retry-wins");
    stub.Echo(&cntl, &req, &res, nullptr);
    ASSERT_FALSE(cntl.Failed()) << cntl.ErrorText();
    ASSERT_EQ("retry-wins", res.message());
    ASSERT_EQ(1, cntl.retried_count()) << "expected exactly one retry";

    const brpc::LatencyTraceRecord* winner = FindLastClientRecordForTest();
    ASSERT_TRUE(winner != nullptr);
    ASSERT_EQ(1, winner->attempt)
        << "the winning attempt should be the retry (attempt 1), not the "
           "original (attempt 0) that got ECONNRESET";
    ASSERT_EQ(0, winner->error_code);

    const brpc::LatencyTraceRecord* srv =
        FindServerRecordForTraceId(winner->trace_id);
    ASSERT_TRUE(srv != nullptr)
        << "no server record shares the winning attempt's trace_id";

    // Design doc sec.10.1's four weight-bearing invariants, on the
    // WINNING attempt specifically -- this is what item 3's bug breaks:
    // C02/C03 are only stamped once, in Channel::CallMethod, for attempt
    // 0; this record is attempt 1, so before the fix ts[C02]==ts[C03]==0
    // while ts[C01]==1, failing both the non-zero and the monotonic
    // checks inside AssertAllWeightBearingInvariants (defined above in
    // this file, shared with the async E2E tests). Item 1's bug needs a
    // backup, not a retry, to manifest -- see
    // OriginalAttemptWinningOverBackupGetsFinalOutcome below for that
    // one -- but running the same full check here means a future
    // regression in either fix shows up wherever it actually reaches.
    AssertAllWeightBearingInvariants(winner, srv);

    server.Stop(0);
    server.Join();
    brpc::FLAGS_latency_trace_enabled = false;
}

// Fix-round item 2 (review of Task 13): when a backup request's ORIGINAL
// attempt responds and wins the race against its own backup (Controller
// ::EndRPC's "a previous non-backup request responded" branch), the RPC's
// true final outcome must land in the ORIGINAL attempt's own record --
// the one whose trace id the server that actually produced the winning
// response saw -- not in the backup's, which was allocated later and
// never received a response at all. Before the fix, Controller::_lt_
// handle was Controller-level rather than per-Call, so IssueRPC's own
// backup allocation clobbered it with the backup's handle, and OnRPCEnd
// (via that same handle) always stamped the final outcome onto whichever
// attempt happened to be allocated LAST -- the backup -- even when the
// backup never got a response.
//
// The mock service's first invocation (the original) sleeps long enough
// for backup_request_ms to fire a backup, then still succeeds; its
// second invocation (the backup) sleeps much longer, so the original's
// response is guaranteed to be the one that completes the RPC.
class LatencyTraceBackupRaceServiceImpl : public test::EchoService {
public:
    void Echo(google::protobuf::RpcController* /*cntl_base*/,
              const test::EchoRequest* request,
              test::EchoResponse* response,
              google::protobuf::Closure* done) override {
        brpc::ClosureGuard done_guard(done);
        const int seen = _call_count.fetch_add(1, std::memory_order_relaxed);
        if (seen == 0) {
            // The original attempt: slow enough that backup_request_ms
            // elapses and a backup fires, but still responds
            // successfully afterward -- "original wins over its backup".
            bthread_usleep(200 * 1000);
        } else {
            // The backup attempt: made to hang well past the point the
            // original's response has already ended the whole RPC (200ms
            // plus loopback overhead), without dragging server.Join()
            // below out any longer than necessary.
            bthread_usleep(1000 * 1000);
        }
        response->set_message(request->message());
    }
private:
    std::atomic<int> _call_count{0};
};

TEST(LatencyTraceMetaTest, OriginalAttemptWinningOverBackupGetsFinalOutcome) {
    brpc::FLAGS_latency_trace_enabled = true;
    brpc::LatencyTraceBuffer::instance()->ResetForTest(1024);

    brpc::Server server;
    LatencyTraceBackupRaceServiceImpl svc;
    ASSERT_EQ(0, server.AddService(&svc, brpc::SERVER_DOESNT_OWN_SERVICE));
    ASSERT_EQ(0, server.Start(9536, nullptr));

    brpc::Channel channel;
    brpc::ChannelOptions opt;
    opt.protocol = brpc::PROTOCOL_BAIDU_STD;
    ASSERT_EQ(0, channel.Init("127.0.0.1:9536", &opt));

    test::EchoService_Stub stub(&channel);
    test::EchoRequest req;
    test::EchoResponse res;
    brpc::Controller cntl;
    cntl.set_backup_request_ms(30);
    cntl.set_timeout_ms(5000);
    req.set_message("race");
    stub.Echo(&cntl, &req, &res, nullptr);
    ASSERT_FALSE(cntl.Failed()) << cntl.ErrorText();
    ASSERT_EQ("race", res.message());

    std::vector<const brpc::LatencyTraceRecord*> client_records;
    brpc::LatencyTraceBuffer* b = brpc::LatencyTraceBuffer::instance();
    for (uint64_t seq = 0; ; ++seq) {
        const brpc::LatencyTraceRecord* r = b->GetBySeqForTest(seq);
        if (r == nullptr) {
            break;
        }
        if (r->role == brpc::LT_ROLE_CLIENT) {
            client_records.push_back(r);
        }
    }
    ASSERT_EQ(2u, client_records.size())
        << "expected one record for the original attempt and one for "
           "its backup";

    const brpc::LatencyTraceRecord* original = nullptr;
    const brpc::LatencyTraceRecord* backup = nullptr;
    for (const brpc::LatencyTraceRecord* r : client_records) {
        if (r->attempt == 0) {
            original = r;
        } else if (r->attempt == 1) {
            backup = r;
        }
    }
    ASSERT_TRUE(original != nullptr && backup != nullptr);

    // The original attempt is the one that actually completed the RPC:
    // its record must carry the true (successful) final outcome, not
    // the placeholder EBACKUPREQUEST value IssueRPC speculatively wrote
    // into it at the moment the backup was sent.
    ASSERT_EQ(0, original->error_code)
        << "the original attempt's record should carry the RPC's true "
           "final outcome after it wins the race against its own backup";

    // Fix-round item 3: the LOSING backup's own record must not keep the
    // error_code == 0 it was allocated with -- that reads as "succeeded"
    // to the offline analysis even though this attempt was in fact
    // cancelled (Controller::EndRPC's "a previous non-backup request
    // responded" branch calls _current_call.OnComplete(this, ECANCELED,
    // false, false) for exactly this attempt). ECANCELED is already
    // computed at that call site; it must also be persisted into this
    // Call's own record, mirroring the prev_rec->error_code =
    // EBACKUPREQUEST write IssueRPC makes into the superseded original's
    // record at the moment a backup is issued.
    ASSERT_EQ(ECANCELED, backup->error_code)
        << "the losing backup's own record should record that it was "
           "cancelled, not keep the zero (\"success\") it was allocated "
           "with";

    // This round's item 3 (test gap, not to be confused with the "Fix-round
    // item 3" comment above from an earlier round): every assertion in this
    // test so far only checks `original` got its receive-side stamps -- it
    // never checks that `backup` did NOT. An implementation that stamps
    // BOTH handles on every response (instead of resolving the ONE handle
    // that matches the response's own attempt, per fix-round item 1) would
    // pass every assertion above and still be wrong. LT_C_WAKE is the
    // natural point to check: it is the first point only the response path
    // ever writes, so a non-zero value here can only mean this attempt's
    // handle was (incorrectly) resolved for the original's response.
    ASSERT_EQ(0u, backup->ts[brpc::LT_C_WAKE])
        << "the losing backup's own record must not receive the original's "
           "response-side stamps -- only the attempt a response actually "
           "belongs to may be stamped for it";

    // Fix-round item 1 (whole-branch review of 685e6d9e): this is the
    // exact branch item 1's bug lives in -- "original wins over its own
    // backup". Before the fix, ProcessRpcResponse read C09-C16 and
    // rsp_size through `accessor.latency_trace_handle()` (== Controller
    // ::_lt_handle), which by the time the ORIGINAL's response arrives
    // here has already been repointed at the BACKUP (IssueRPC repoints
    // it the moment it allocates the backup's slot, well before this
    // response comes back). So those eight stamps landed on `backup`
    // instead of `original`: `original`'s C09-C16 stayed zero (failing
    // the non-zero check below) while `backup` -- an attempt that never
    // received anything -- ended up with a fully plausible, internally
    // self-consistent set of receive-side timestamps that were actually
    // the winning response's. This test previously asserted only
    // error_code on each record, which is exactly why the bug survived:
    // neither of those assertions can see a receive-side stamp landing
    // on the wrong record. Check `original` -- the actual winner --
    // against its own server-side record using
    // AssertPerRecordCoreInvariants, not the full
    // AssertAllWeightBearingInvariants: this test's server has the
    // original's AND the backup's requests in flight at once (that is
    // the whole point of a backup request), so outstanding=1 does not
    // hold here and design doc sec.10.1's decomposition-item/link-time
    // check does not apply (see AssertPerRecordCoreInvariants's
    // comment). The three checks that DO still apply regardless of
    // concurrency -- every point non-zero, monotonic, C09>=C08 -- are
    // exactly the ones item 1's bug breaks: a receive-side stamp landing
    // on the wrong record leaves the RIGHT record's C09-C16 at zero
    // (failing non-zero) while leaving the WRONG record internally
    // monotonic and self-consistent, which is precisely why this needs
    // to be checked on `original` specifically, not `backup`.
    const brpc::LatencyTraceRecord* original_srv =
        FindServerRecordForTraceId(original->trace_id);
    ASSERT_TRUE(original_srv != nullptr)
        << "no server record shares the original attempt's trace_id";
    AssertPerRecordCoreInvariants(original, original_srv);

    server.Stop(0);
    server.Join();
    brpc::FLAGS_latency_trace_enabled = false;
}

// This round's item 4 (test gap): every backup-request test above only
// covers "the original wins over its own backup". The mirror direction --
// the BACKUP wins, and the original's late response (if it ever arrives)
// must be the one dropped -- had no test at all. An implementation that
// always preferred `_unfinished_call` (the original) over `_current_call`
// when resolving a response's handle -- e.g. checking `_unfinished_call`
// before `_current_call` in latency_trace_handle_for_response, the reverse
// of the order that function actually uses -- would pass every assertion
// in OriginalAttemptWinningOverBackupGetsFinalOutcome above (that test
// never exercises the backup's own response at all) while resolving the
// backup's real response onto the wrong (original) record here.
//
// The mock service's first invocation (the original) sleeps long enough
// that backup_request_ms elapses and a backup fires, then keeps sleeping
// well past the point the backup's response has already ended the whole
// RPC; its second invocation (the backup) responds immediately, so the
// backup is guaranteed to be the one that completes the RPC.
class LatencyTraceBackupWinsServiceImpl : public test::EchoService {
public:
    void Echo(google::protobuf::RpcController* /*cntl_base*/,
              const test::EchoRequest* request,
              test::EchoResponse* response,
              google::protobuf::Closure* done) override {
        brpc::ClosureGuard done_guard(done);
        const int seen = _call_count.fetch_add(1, std::memory_order_relaxed);
        if (seen == 0) {
            // The original attempt: made to hang well past the point the
            // backup's response has already ended the whole RPC (well
            // past backup_request_ms plus loopback overhead), without
            // dragging server.Join() below out any longer than necessary.
            bthread_usleep(1000 * 1000);
        }
        // The backup attempt (seen == 1): responds immediately, so it
        // wins the race against the still-sleeping original.
        response->set_message(request->message());
    }
private:
    std::atomic<int> _call_count{0};
};

TEST(LatencyTraceMetaTest, BackupAttemptWinningOverOriginalGetsFinalOutcome) {
    brpc::FLAGS_latency_trace_enabled = true;
    brpc::LatencyTraceBuffer::instance()->ResetForTest(1024);

    brpc::Server server;
    LatencyTraceBackupWinsServiceImpl svc;
    ASSERT_EQ(0, server.AddService(&svc, brpc::SERVER_DOESNT_OWN_SERVICE));
    ASSERT_EQ(0, server.Start(9538, nullptr));

    brpc::Channel channel;
    brpc::ChannelOptions opt;
    opt.protocol = brpc::PROTOCOL_BAIDU_STD;
    ASSERT_EQ(0, channel.Init("127.0.0.1:9538", &opt));

    test::EchoService_Stub stub(&channel);
    test::EchoRequest req;
    test::EchoResponse res;
    brpc::Controller cntl;
    cntl.set_backup_request_ms(30);
    cntl.set_timeout_ms(5000);
    req.set_message("backup-wins");
    stub.Echo(&cntl, &req, &res, nullptr);
    ASSERT_FALSE(cntl.Failed()) << cntl.ErrorText();
    ASSERT_EQ("backup-wins", res.message());

    std::vector<const brpc::LatencyTraceRecord*> client_records;
    brpc::LatencyTraceBuffer* b = brpc::LatencyTraceBuffer::instance();
    for (uint64_t seq = 0; ; ++seq) {
        const brpc::LatencyTraceRecord* r = b->GetBySeqForTest(seq);
        if (r == nullptr) {
            break;
        }
        if (r->role == brpc::LT_ROLE_CLIENT) {
            client_records.push_back(r);
        }
    }
    ASSERT_EQ(2u, client_records.size())
        << "expected one record for the original attempt and one for "
           "its backup";

    const brpc::LatencyTraceRecord* original = nullptr;
    const brpc::LatencyTraceRecord* backup = nullptr;
    for (const brpc::LatencyTraceRecord* r : client_records) {
        if (r->attempt == 0) {
            original = r;
        } else if (r->attempt == 1) {
            backup = r;
        }
    }
    ASSERT_TRUE(original != nullptr && backup != nullptr);

    // The backup attempt is the one that actually completed the RPC: its
    // record must carry the true (successful) final outcome.
    ASSERT_EQ(0, backup->error_code)
        << "the backup attempt's record should carry the RPC's true final "
           "outcome after it wins the race against the original";

    // The losing original's own record should keep EBACKUPREQUEST -- written
    // proactively by IssueRPC at the moment the backup was issued (before
    // either attempt's outcome is known; see IssueRPC's `prev_rec->error_code
    // = _error_code` write). This is the mirror of
    // OriginalAttemptWinningOverBackupGetsFinalOutcome's ECANCELED check:
    // there the winner is known *after* the loser already responded, so
    // EndRPC can compute and persist ECANCELED into the loser's own record;
    // here the loser (original) never responds at all within this test's
    // lifetime, so its record is never revisited after that earlier
    // EBACKUPREQUEST write.
    ASSERT_EQ(brpc::EBACKUPREQUEST, original->error_code)
        << "the losing original's own record should keep the EBACKUPREQUEST "
           "outcome IssueRPC wrote it when the backup was issued";

    // Design doc sec.10.1's four weight-bearing invariants, on the WINNING
    // (backup) attempt specifically -- resolved via
    // latency_trace_handle_for_response, not the stale Controller-level
    // _lt_handle (fix-round item 1). Checked against
    // AssertPerRecordCoreInvariants, not the full
    // AssertAllWeightBearingInvariants: this test's server has the
    // original's AND the backup's requests in flight at once, so
    // outstanding=1 does not hold and the decomposition-item/link-time
    // check does not apply (see AssertPerRecordCoreInvariants's comment).
    const brpc::LatencyTraceRecord* backup_srv =
        FindServerRecordForTraceId(backup->trace_id);
    ASSERT_TRUE(backup_srv != nullptr)
        << "no server record shares the backup attempt's trace_id";
    AssertPerRecordCoreInvariants(backup, backup_srv);

    // This round's item 4 proper: the mirror of item 3's check above. An
    // implementation that always prefers `_unfinished_call` (the original)
    // when resolving the response's handle -- the reverse bug from the one
    // item 3 covers -- would land the backup's real response-side stamps
    // onto `original` instead. Assert the LOSING original's record stayed
    // unstamped on the receive side.
    ASSERT_EQ(0u, original->ts[brpc::LT_C_WAKE])
        << "the losing original attempt's record must not receive the "
           "backup's response-side stamps -- only the attempt a response "
           "actually belongs to may be stamped for it";

    server.Stop(0);
    server.Join();
    brpc::FLAGS_latency_trace_enabled = false;
}

// Fix-round item 2 (whole-branch review of 685e6d9e): Socket::Write(
// SocketMessagePtr<>&, ...) -- used by stream/h2/RTMP/packet_guard
// writes, i.e. anything that packs into a SocketMessage rather than an
// IOBuf -- never initialized req->lt_handle. `req` comes from butil::
// get_object<WriteRequest>(), a process-wide pool this test's own
// process has already put plenty of traffic through by the time this
// runs (every earlier RPC-based test above writes through the sibling
// IOBuf overload, which correctly sets req->lt_handle to a REAL handle
// on every call), so the very next WriteRequest object this test's
// SocketMessagePtr<> write receives from that pool is essentially
// guaranteed to already hold some earlier test's real, non-zero handle
// -- not the indeterminate-but-probably-zero value a truly fresh object
// would have. Two things make that handle certainly WRONG for this
// test, not merely different: (1) it names a record from a trace-buffer
// generation the ResetForTest() call just below has already
// invalidated -- LatencyTraceBuffer's generation guard makes Stamp()/
// StampAt() on it a silent no-op rather than a crash, which is
// precisely why this bug was easy to miss in practice; (2) even in the
// FLAGS_latency_trace_enabled==false case this file's other tests
// sometimes exercise, it is simply memory this write has no business
// touching. Either way, the enqueue stamp -- taken synchronously inside
// Socket::Write(), before any actual I/O; see the LT_STAMP call
// immediately after where item 2 adds `req->lt_handle = opt.lt_handle;`
// -- never reaches this write's own, correctly-allocated record unless
// that line runs.
class LatencyTraceRawSocketMessage : public brpc::SocketMessage {
public:
    LatencyTraceRawSocketMessage(const char* str, size_t len)
        : _str(str), _len(len) {}
private:
    butil::Status AppendAndDestroySelf(butil::IOBuf* out_buf, brpc::Socket*) override {
        out_buf->append(_str, _len);
        delete this;
        return butil::Status::OK();
    }
    const char* _str;
    size_t _len;
};

TEST(LatencyTraceMetaTest, SocketMessagePathInitializesLtHandle) {
    brpc::FLAGS_latency_trace_enabled = true;
    brpc::LatencyTraceBuffer::instance()->ResetForTest(1024);
    brpc::LatencyTraceBuffer* b = brpc::LatencyTraceBuffer::instance();

    const brpc::LatencyTraceHandle h = b->AllocSlot(0x5A5AULL, brpc::LT_ROLE_CLIENT);
    ASSERT_NE(brpc::LT_INVALID_HANDLE, h);

    int fds[2];
    ASSERT_EQ(0, socketpair(AF_UNIX, SOCK_STREAM, 0, fds));
    brpc::SocketId id = 0;
    butil::EndPoint dummy;
    ASSERT_EQ(0, str2endpoint("192.168.1.26:8080", &dummy));
    brpc::SocketOptions options;
    options.fd = fds[1];
    options.remote_side = dummy;
    ASSERT_EQ(0, brpc::Socket::Create(options, &id));
    brpc::SocketUniquePtr s;
    ASSERT_EQ(0, brpc::Socket::Address(id, &s));

    brpc::Socket::WriteOptions wopt;
    wopt.lt_handle = h;
    wopt.lt_role = brpc::LT_ROLE_CLIENT;
    brpc::SocketMessagePtr<LatencyTraceRawSocketMessage> msg(
        new LatencyTraceRawSocketMessage("hi", 2));
    ASSERT_EQ(0, s->Write(msg, &wopt));

    const brpc::LatencyTraceRecord* rec = b->GetForTest(h);
    ASSERT_TRUE(rec != nullptr);
    ASSERT_GT(rec->ts[brpc::LT_C_WRITE_ENQUEUE], 0u)
        << "Socket::Write(SocketMessagePtr<>&, ...) never set "
           "req->lt_handle, so the enqueue stamp (taken synchronously, "
           "before any I/O) never reached this write's own record";

    s.reset();
    close(fds[0]);
    brpc::FLAGS_latency_trace_enabled = false;
}

TEST(LatencyTraceMetaTest, WriteStartReflectsFinalKeepWriteAttempt) {
    // Fix-round correction to design doc sec.10.1: write_start's meaning
    // is "the start of the DoWrite attempt that actually drained this
    // WriteRequest", so KeepWrite re-entering DoWrite for a request that
    // needs more than one writev must let write_start keep moving forward
    // to the LAST attempt (LatencyTraceBuffer::StampLast()), not freeze
    // on the first one. Driven genuinely end-to-end here: a payload large
    // enough that a AF_UNIX socketpair's kernel buffer cannot absorb it
    // in one writev, with the peer deliberately not reading for a while,
    // forces DoWrite to hit EAGAIN and KeepWrite to retry for real.
    brpc::FLAGS_latency_trace_enabled = true;
    brpc::LatencyTraceBuffer::instance()->ResetForTest(64);
    brpc::LatencyTraceBuffer* b = brpc::LatencyTraceBuffer::instance();
    const brpc::LatencyTraceHandle h = b->AllocSlot(0x1357ULL, brpc::LT_ROLE_CLIENT);
    ASSERT_NE(brpc::LT_INVALID_HANDLE, h);

    int fds[2];
    ASSERT_EQ(0, socketpair(AF_UNIX, SOCK_STREAM, 0, fds));
    brpc::SocketId id = 0;
    butil::EndPoint dummy;
    ASSERT_EQ(0, str2endpoint("192.168.1.27:8080", &dummy));
    brpc::SocketOptions options;
    options.fd = fds[1];
    options.remote_side = dummy;
    ASSERT_EQ(0, brpc::Socket::Create(options, &id));
    brpc::SocketUniquePtr s;
    ASSERT_EQ(0, brpc::Socket::Address(id, &s));

    // Far exceeds default SO_SNDBUF/SO_RCVBUF (typically ~200KB) for an
    // AF_UNIX socketpair, so a single writev() cannot drain it.
    const size_t payload_size = 16 * 1024 * 1024;
    butil::IOBuf iobuf;
    iobuf.append(std::string(payload_size, 'x'));

    brpc::Socket::WriteOptions wopt;
    wopt.lt_handle = h;
    wopt.lt_role = brpc::LT_ROLE_CLIENT;

    const uint64_t t_before_write = butil::detail::clock_cycles();
    ASSERT_EQ(0, s->Write(&iobuf, &wopt));

    // Deliberately do not read from fds[0] yet: this stalls KeepWrite on
    // EAGAIN (parked on epoll for EPOLLOUT, not busy-spinning) after its
    // first, necessarily-partial DoWrite attempt.
    usleep(150 * 1000);

    const uint64_t t_before_drain = butil::detail::clock_cycles();
    std::thread reader([&fds, payload_size]() {
        size_t total = 0;
        char buf[65536];
        while (total < payload_size) {
            ssize_t n = read(fds[0], buf, sizeof(buf));
            if (n <= 0) {
                break;
            }
            total += (size_t)n;
        }
    });

    const brpc::LatencyTraceRecord* rec = b->GetForTest(h);
    ASSERT_TRUE(rec != nullptr);
    bool completed = false;
    for (int i = 0; i < 2000; ++i) {  // up to ~4s
        if (rec->ts[brpc::LT_C_WRITE_END] != 0) {
            completed = true;
            break;
        }
        usleep(2000);
    }
    reader.join();
    ASSERT_TRUE(completed) << "write did not complete within the timeout -- "
        "the payload/buffer-size assumptions this test relies on to force "
        "a partial write may not hold on this host";

    ASSERT_GT(rec->ts[brpc::LT_C_WRITE_START], 0u);
    // ts[] stores offset+1 (see LatencyTraceRecord::ts).
    const uint64_t write_start_raw =
        rec->base_counter + (rec->ts[brpc::LT_C_WRITE_START] - 1);
    // The unstalled, real drain could not have completed before we
    // started reading -- the peer's buffer was full and stayed full for
    // the entire 150ms hold-off. If write_start had frozen on the first
    // (necessarily earlier, pre-stall) attempt -- the exact bug
    // first-write-wins would reintroduce here -- this would fail.
    ASSERT_GE(write_start_raw, t_before_drain)
        << "write_start must reflect the final DoWrite attempt, not an "
           "earlier one frozen in by first-write-wins";
    ASSERT_GT(write_start_raw, t_before_write);

    s.reset();
    close(fds[0]);
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

// Task 12: the RDMA receive path. Design doc sec.8.5 -- RDMA's data plane
// does not go through InputMessenger::OnNewMessages at all. A dedicated CQ
// socket's edge-triggered callback is RdmaEndpoint::PollCq
// (src/brpc/rdma/rdma_endpoint.cpp), which only rejoins the TCP path at the
// shared ProcessNewMessage() call (already covered by Task 9's threading of
// Socket::_lt_wake/_lt_onedge_start/_lt_read_start through
// InputMessageBase). Under -rdma_use_polling a standalone poller thread
// calls PollCq directly in a loop: there is no epoll wake-up and no OnEdge
// bthread switch, so `wake`/`onedge_start` do not physically exist and must
// carry LT_TS_NOT_APPLICABLE rather than a bogus zero or a bogus "now".
//
// Requires real (or Soft-RoCE) RDMA hardware reachable at -lt_rdma_test_ip
// -- the loopback address does not reach an RDMA NIC (rxe0 is bound to the
// physical interface, not `lo`). Defaults to the verified suzhou950
// Soft-RoCE address; override (or point elsewhere) with the flag.
//
// The flag/DECLARE below must sit directly in `namespace brpc`/`namespace
// brpc::rdma` (matching how this file's own DECLAREs at the top do it, for
// the same reason -- see the comment there), so this closes the anonymous
// namespace wrapping the rest of this file, declares them at the right
// scope, then reopens it: a no-op for everything else already in the file.
}  // namespace

#if BRPC_WITH_RDMA
namespace brpc {
DEFINE_string(lt_rdma_test_ip, "192.168.25.145",
              "IP of an RDMA-capable NIC (Soft-RoCE or real) to bind the "
              "RDMA receive-path test's server/client to. 127.0.0.1 does "
              "NOT work -- the RDMA device is bound to a physical NIC.");
}  // namespace brpc
namespace brpc { namespace rdma {
DECLARE_bool(rdma_use_polling);
} }  // namespace brpc::rdma
#endif  // BRPC_WITH_RDMA

namespace {

#if BRPC_WITH_RDMA

class LatencyTraceRdmaEchoServiceImpl : public test::EchoService {
public:
    void Echo(google::protobuf::RpcController* cntl_base,
              const test::EchoRequest* request,
              test::EchoResponse* response,
              google::protobuf::Closure* done) override {
        brpc::ClosureGuard done_guard(done);
        response->set_message(request->message());
    }
};

TEST(LatencyTraceRdmaTest, ReceivePathStampsWakeOnedgeReadv) {
    brpc::FLAGS_latency_trace_enabled = true;
    brpc::LatencyTraceBuffer::instance()->ResetForTest(1024);

    char addr[64];
    snprintf(addr, sizeof(addr), "%s:9539", brpc::FLAGS_lt_rdma_test_ip.c_str());

    brpc::Server server;
    LatencyTraceRdmaEchoServiceImpl svc;
    ASSERT_EQ(0, server.AddService(&svc, brpc::SERVER_DOESNT_OWN_SERVICE));
    brpc::ServerOptions sopt;
    sopt.socket_mode = brpc::SOCKET_MODE_RDMA;
    ASSERT_EQ(0, server.Start(addr, &sopt));

    brpc::Channel channel;
    brpc::ChannelOptions copt;
    copt.protocol = brpc::PROTOCOL_BAIDU_STD;
    copt.socket_mode = brpc::SOCKET_MODE_RDMA;
    ASSERT_EQ(0, channel.Init(addr, &copt));

    test::EchoService_Stub stub(&channel);
    test::EchoRequest req;
    test::EchoResponse res;
    brpc::Controller cntl;
    req.set_message("hello-rdma");
    stub.Echo(&cntl, &req, &res, nullptr);
    ASSERT_FALSE(cntl.Failed()) << cntl.ErrorText();

    const brpc::LatencyTraceRecord* s = FindServerRecordForTest();
    ASSERT_TRUE(s != nullptr);

    // read_start exists in both modes -- see design doc sec.8.5's table.
    ASSERT_GT(s->ts[brpc::LT_S_READ_START], 0u);
    ASSERT_NE(brpc::LT_TS_NOT_APPLICABLE, s->ts[brpc::LT_S_READ_START]);

    // Fix-round item 1 regression coverage. S09 (LT_S_SERVICE_START) is
    // written through plain Stamp() (see baidu_rpc_protocol.cpp and design
    // doc sec.8.1's Stamp()-vs-StampAt() table), which has NO raw<base
    // clamp -- unlike S01-S06, which go through StampAt() and would just
    // collapse to ts=1 on a bad base_counter. Before this fix-round's item
    // 1 fix, RDMA polling mode passed the wake sentinel (LT_RAW_NOT_
    // APPLICABLE, i.e. ~0ULL) straight through as base_counter, so
    // `clock_cycles() - base_counter` wrapped around and every point
    // stamped via plain Stamp() (S09 onward) saturated to 0xFFFFFFFE
    // ("took ~43s"). Assert a plausible small offset here instead --
    // this is the assertion that actually distinguishes a corrupted
    // record from a correct one; the wake/onedge_start checks below
    // cannot, because StampAt()'s clamp makes them read the same either
    // way (see the item 2 comment below).
    ASSERT_GT(s->ts[brpc::LT_S_SERVICE_START], 0u)
        << "S09 (service_start) was never stamped";
    ASSERT_LT(s->ts[brpc::LT_S_SERVICE_START], 100000000u)
        << "S09 (service_start) offset is implausibly large ("
        << s->ts[brpc::LT_S_SERVICE_START] << ") -- looks like the "
        << "~43s saturation produced by a sentinel base_counter "
        << "(design doc sec.8.1's base_counter paragraph), not a real "
        << "same-host RPC duration";

    if (brpc::rdma::FLAGS_rdma_use_polling) {
        // Sec.8.5: no epoll wake-up, no OnEdge bthread switch under
        // polling -- these two points do not exist, so merge.py can mark
        // the four receive-queueing decomposition items N/A instead of
        // reading them as missing instrumentation.
        ASSERT_EQ(brpc::LT_TS_NOT_APPLICABLE, s->ts[brpc::LT_S_WAKE]);
        ASSERT_EQ(brpc::LT_TS_NOT_APPLICABLE, s->ts[brpc::LT_S_ONEDGE_START]);
    } else {
        ASSERT_GT(s->ts[brpc::LT_S_WAKE], 0u);
        ASSERT_NE(brpc::LT_TS_NOT_APPLICABLE, s->ts[brpc::LT_S_WAKE]);
        ASSERT_GT(s->ts[brpc::LT_S_ONEDGE_START], 0u);
        ASSERT_NE(brpc::LT_TS_NOT_APPLICABLE, s->ts[brpc::LT_S_ONEDGE_START]);
        ASSERT_LE(s->ts[brpc::LT_S_WAKE], s->ts[brpc::LT_S_ONEDGE_START]);
        // Fix-round item 2 regression coverage. `ASSERT_LE` above is
        // vacuously true whether wake/onedge_start are genuinely ordered
        // OR both got clamped into equality at ts=1 by StampAt()'s
        // raw<base guard -- exactly how the inverted-order bug (design
        // doc sec.8.1's RDMA event-mode wake paragraph) passed review
        // undetected: with `wake` resampled after GetAndAckEvents() (so
        // raw wake > raw onedge_start), the clamp silently collapses
        // both to ts=1 and `wake <= onedge_start` "passes". Assert
        // strict ordering instead: under the fix, `wake` is the CQ
        // socket's genuinely earlier timestamp (copied, not resampled),
        // so onedge_start -- sampled later, at PollCq's own entry, after
        // real epoll-dispatch work already happened -- must show a real,
        // non-zero, non-clamped gap above it.
        ASSERT_GT(s->ts[brpc::LT_S_ONEDGE_START], s->ts[brpc::LT_S_WAKE])
            << "wake (" << s->ts[brpc::LT_S_WAKE] << ") and onedge_start ("
            << s->ts[brpc::LT_S_ONEDGE_START] << ") are not strictly "
            << "ordered -- looks like both were clamped to the same "
            << "value by StampAt()'s raw<base guard rather than "
            << "genuinely ordered (design doc sec.8.1's RDMA event-mode "
            << "wake paragraph)";
        ASSERT_LE(s->ts[brpc::LT_S_ONEDGE_START], s->ts[brpc::LT_S_READ_START]);
    }

    server.Stop(0);
    server.Join();
    brpc::FLAGS_latency_trace_enabled = false;
}

#endif  // BRPC_WITH_RDMA

#endif  // defined(BRPC_LATENCY_TRACE)

}  // namespace

// Every other test in this file drives brpc::FLAGS_* directly from C++
// (see e.g. brpc::FLAGS_latency_trace_enabled above), so the default
// gtest_main -- InitGoogleTest() plus RUN_ALL_TESTS(), no gflags parsing
// -- has always been enough. The RDMA receive-path test above is the
// first one that needs the *process's own command line* honored (the
// suzhou950 Soft-RoCE recipe drives it via --rdma_use_polling=... and
// --rdma_memory_pool_*_mb=...; see brpc_rdma_unittest.cpp's own main()
// for the established pattern this mirrors). Defining main() here is
// safe alongside the -lgtest_main this binary still links: the linker
// resolves `main` from this translation unit before it ever needs to
// pull the matching object out of that archive.
//
// This must stay inside the BRPC_LATENCY_TRACE guard: with the feature
// compiled out, this whole file (including the RDMA test that actually
// needs the real command line) is inert, and the untraced build must
// stay byte-for-byte equivalent to before this feature existed -- which
// means falling back to plain -lgtest_main (InitGoogleTest() +
// RUN_ALL_TESTS(), no gflags parsing) exactly as it did before this test
// was added. An unconditional main() here would make every build,
// traced or not, parse the process's command line through gflags where
// it previously parsed nothing.
#if defined(BRPC_LATENCY_TRACE)
int main(int argc, char* argv[]) {
    testing::InitGoogleTest(&argc, argv);
    GFLAGS_NAMESPACE::ParseCommandLineFlags(&argc, &argv, true);
    return RUN_ALL_TESTS();
}
#endif  // defined(BRPC_LATENCY_TRACE)
