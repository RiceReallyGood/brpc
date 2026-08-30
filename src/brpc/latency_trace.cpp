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

#include "brpc/latency_trace.h"

#include <gflags/gflags.h>
#include <new>
#include <cstdio>
#include <cstdlib>
#include <time.h>
#include "butil/time.h"
#include "butil/logging.h"
#include "butil/fast_rand.h"

namespace brpc {

DEFINE_bool(latency_trace_enabled, false,
            "Record per-request latency trace points. Requires the library "
            "to be built with BRPC_LATENCY_TRACE.");
DEFINE_int32(latency_trace_capacity, 100000,
             "Total number of latency trace records held in memory");
DEFINE_string(latency_trace_dump_path, "",
              "Dump latency trace records to this file at exit; empty "
              "disables dumping");

// Never returns zero: MakeLatencyTraceId() OR's this into the high 32
// bits of every trace id from this process, and PackRpcRequest only puts
// the field on the wire when the id is non-zero -- a zero tag combined
// with the very first seq (0) would silently look like "not tracing".
// fast_rand() is used (rather than e.g. clock_cycles() ^ getpid()) partly
// for entropy and partly because getpid() is commonly 1 inside a
// container's PID namespace, which would otherwise leave differentiation
// between containers on one host to the counter reading alone.
static uint32_t MakeProcessTag() {
    uint32_t t = 0;
    while (t == 0) {
        t = (uint32_t)butil::fast_rand();
    }
    return t;
}

// Calling LatencyTraceBuffer::instance() here cannot recurse into or
// race with the buffer's own construction: instance() lazily constructs
// the singleton via a function-local static pointer, which C++11 gives
// thread-safe one-time init for (concurrent first callers block on the
// same initialization; no re-entrant construction is possible), and
// LatencyTraceBuffer's constructor never calls MakeLatencyTraceId (or
// anything that does), so there is no path back into an unfinished
// construction.
uint64_t MakeLatencyTraceId(uint64_t seq) {
    const uint64_t tag = LatencyTraceBuffer::instance()->process_tag();
    // The low 32 bits (`seq`) wrap after ~4B calls from this process.
    // That is safe only because the ring buffer -- bounded by
    // -latency_trace_capacity, 100000 records by default -- gets dumped
    // (and its cursors reset for the next dump) long before any one
    // shard's cursor reaches anywhere near 2^32 allocations, so no two
    // records that end up in the same dump can ever share a trace id.
    // Raising -latency_trace_capacity toward 2^32 would break that.
    return (tag << 32) | (uint32_t)seq;
}

static uint64_t NextPowerOfTwo(uint64_t v) {
    uint64_t r = 1;
    while (r < v) {
        r <<= 1;
    }
    return r;
}

LatencyTraceBuffer::LatencyTraceBuffer()
    : _dropped(0), _stop_when_full(true), _per_shard_capacity(0),
      _head_counter(butil::detail::clock_cycles()),
      _head_realtime_ns(butil::monotonic_time_ns()),
      _process_tag(MakeProcessTag()),
      _atexit_registered(false) {
    ResetForTest(FLAGS_latency_trace_capacity);
    if (!FLAGS_latency_trace_dump_path.empty()) {
        EnableDumpOnExit(FLAGS_latency_trace_dump_path.c_str());
    }
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
            _shards[i].records[j].slot_seq.store(UINT64_MAX,
                                                  butil::memory_order_relaxed);
        }
        _shards[i].mask = per_shard - 1;
        _shards[i].cursor.store(0, butil::memory_order_relaxed);
    }
    _dropped.store(0, butil::memory_order_relaxed);
}

// Deliberately leaked, not a function-local static object: EnableDumpOnExit
// (called from the constructor when -latency_trace_dump_path is set) does
// its own atexit(&DumpAtExitCallback) registration from inside that
// constructor call, which runs BEFORE the compiler's own destructor
// registration for a magic-static object would happen (that registration
// happens right after construction completes, i.e. strictly later in
// program order). atexit/__cxa_atexit callbacks run in reverse order of
// registration, so a magic-static's destructor -- registered after ours
// -- would run BEFORE DumpAtExitCallback fires at real process exit,
// making the dump-at-exit path read a destroyed object. A raw `new` that
// is never deleted has no destructor registration to race against: the
// object is simply still alive, unconditionally, whenever any atexit
// callback (ours included) runs. Do not "fix" this leak with a
// function-local static.
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
    // Invalidate the slot FIRST, before touching anything else in it. The
    // previous occupant's real seq must not still be sitting in slot_seq
    // while we mutate the payload underneath it -- a stale handle for that
    // previous generation would pass Get()'s check during that window and
    // stamp a record that is midway through being reinitialized for a
    // completely different trace. UINT64_MAX can never equal any embedded
    // handle seq, so this is a hard "nothing matches" state.
    //
    // A plain release *store* here is NOT enough: release only stops
    // writes that precede it (in program order) from being reordered
    // after it -- it says nothing about the writes that follow it (the
    // payload below), which remain free to become visible to another core
    // before this one on a weak memory model. Measured on this project's
    // aarch64 build host: without the explicit fence below, a concurrent
    // stress test saw thousands of records where the read-back trace_id
    // didn't match the allocation that (per its own handle) should still
    // have owned the slot -- exactly this reordering. The fence below
    // (not the store's own order) is what actually closes the gap: it
    // forbids any write after it (the payload) from being reordered
    // ahead of any write before it (this store), in both directions of
    // observation by other threads.
    r->slot_seq.store(UINT64_MAX, butil::memory_order_relaxed);
    butil::atomic_thread_fence(butil::memory_order_release);
    memset(r->ts, 0, sizeof(r->ts));
    r->trace_id = trace_id;
    r->base_counter = butil::detail::clock_cycles();
    r->role = (uint8_t)role;
    r->attempt = 0;
    // Reset metadata left over from whichever request last owned this
    // slot -- otherwise a request that fails before these get filled in
    // (e.g. an early error) would dump with a stale socket/method/size
    // from a previous, unrelated trace.
    r->socket_id = 0;
    r->remote_ip = 0;
    r->remote_port = 0;
    r->req_size = 0;
    r->rsp_size = 0;
    r->method_id = 0;
    r->error_code = 0;
    // Publish: release-store makes every write above visible-before this
    // store to any thread that acquire-loads the same value via Get().
    // This is the only valid publication point -- `cursor.fetch_add`
    // above happens BEFORE any of this is written, so no memory_order on
    // the cursor could ever make it one; a reader on another thread (e.g.
    // Task 5's Dump) must synchronize through slot_seq instead.
    r->slot_seq.store(seq, butil::memory_order_release);
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
    // Acquire pairs with AllocSlot's release-store: seeing `seq` here
    // means every write AllocSlot made to this record before that store
    // is visible to us too, so it is safe to read (or, from Stamp, write
    // a timestamp field of) this record.
    if (r->slot_seq.load(butil::memory_order_acquire) != seq) {
        return nullptr;   // recycled, in-flight, or never written
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

const LatencyTraceRecord* LatencyTraceBuffer::GetBySeqForTest(
    uint64_t global_seq) const {
    // Walk shards in order, treating each shard's written prefix
    // (recorded_count()'s per-shard term) as one contiguous run. This is
    // only correct because the default `_stop_when_full == true` means a
    // shard's written slots are always its first `written` entries with no
    // wraparound -- overwrite-on-full is not a supported mode here.
    for (int i = 0; i < SHARD_COUNT; ++i) {
        const Shard& sh = _shards[i];
        const uint64_t c = sh.cursor.load(butil::memory_order_relaxed);
        const uint64_t written =
            (c > (uint64_t)_per_shard_capacity) ? (uint64_t)_per_shard_capacity : c;
        if (global_seq < written) {
            return &sh.records[global_seq & sh.mask];
        }
        global_seq -= written;
    }
    return nullptr;
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

static uint64_t ReadCntfrqHz() {
#if defined(__aarch64__)
    uint64_t v;
    asm volatile("mrs %0, cntfrq_el0" : "=r"(v));
    return v;
#else
    return 0;
#endif
}

int LatencyTraceBuffer::Dump(const char* path) {
    // Calibration window minimum: freq = Δcounter / Δrealtime, and a
    // window of only a few microseconds (e.g. ResetForTest() immediately
    // followed by Dump() in a unit test) makes that quotient pure noise.
    // Top the window up to at least 100ms before sampling the tail pair.
    // On a real dump-at-exit path this never triggers -- a process that
    // has run RPCs has been alive far longer than 100ms already.
    const int64_t kMinWindowNs = 100 * 1000000LL;
    int64_t elapsed_ns = butil::monotonic_time_ns() - _head_realtime_ns;
    if (elapsed_ns < kMinWindowNs) {
        const int64_t remain_ns = kMinWindowNs - elapsed_ns;
        struct timespec ts;
        ts.tv_sec = remain_ns / 1000000000LL;
        ts.tv_nsec = remain_ns % 1000000000LL;
        nanosleep(&ts, nullptr);
    }
    const uint64_t tail_counter = butil::detail::clock_cycles();
    const int64_t tail_realtime_ns = butil::monotonic_time_ns();

    LatencyTraceFileHeader hdr;
    memset(&hdr, 0, sizeof(hdr));
    hdr.magic = LT_FILE_MAGIC;
    hdr.record_size = (uint32_t)sizeof(LatencyTraceRecord);
    hdr.point_count = (uint32_t)LT_POINT_COUNT;
    hdr.head_counter = _head_counter;
    hdr.head_realtime_ns = _head_realtime_ns;
    hdr.tail_counter = tail_counter;
    hdr.tail_realtime_ns = tail_realtime_ns;
    const double dt_counter = (double)(tail_counter - _head_counter);
    const double dt_realtime_ns = (double)(tail_realtime_ns - _head_realtime_ns);
    hdr.counter_freq_hz = dt_counter * 1e9 / dt_realtime_ns;
    hdr.cntfrq_el0_hz = ReadCntfrqHz();
    hdr.record_count = recorded_count();
    hdr.dropped_count = dropped_count();
    hdr.process_tag = _process_tag;
    hdr.method_table_offset = 0;  // written by Task 13

    FILE* fp = fopen(path, "wb");
    if (fp == nullptr) {
        return -1;
    }
    // hdr.record_count above is the count this dump PROMISES to contain.
    // If any fwrite below fails partway through -- or, less obviously,
    // if the fclose() at the bottom fails to flush stdio's buffer to
    // disk even though every fwrite reported success -- that promise is
    // already wrong for whatever actually landed on disk. A header
    // claiming N records over a file holding fewer (or a file that never
    // fully hit disk at all) is worse than no file, because an offline
    // reader has no way to tell the two apart from the header alone. So
    // every failure below funnels into one `ok = false`, and the single
    // cleanup path at the end unlinks the path rather than leaving a
    // file that lies about what it contains; the caller (Dump()'s own
    // return value, or DumpAtExitCallback's log line below) is the
    // failure signal instead. One `fclose` call, one `remove` call, both
    // unconditional past this point but only the latter gated on `ok` --
    // no path here can double-close `fp` or remove a file this call
    // didn't itself create.
    bool ok = (fwrite(&hdr, sizeof(hdr), 1, fp) == 1);

    int written = 0;
    for (int i = 0; ok && i < SHARD_COUNT; ++i) {
        Shard& sh = _shards[i];
        for (int j = 0; j < _per_shard_capacity; ++j) {
            LatencyTraceRecord& r = sh.records[j];
            // Acquire-load, paired with AllocSlot's release-store: this is
            // the actual publication point (cursor.fetch_add happens
            // BEFORE the payload is written, so it cannot serve as one).
            // Seeing anything other than UINT64_MAX here guarantees every
            // payload write AllocSlot made before its release-store is
            // visible to this thread too.
            //
            // What this acquire load does NOT cover: ts[] entries the
            // owning thread writes via Stamp() concurrently with this
            // fwrite copying the record's bytes -- that is a genuine data
            // race by the formal model, with no synchronization between
            // this read and those writes. It is practically benign here:
            // each ts[] entry is a single aligned uint32_t, which does not
            // tear on this hardware, so this thread observes either the
            // pre- or post-Stamp() value for each entry, never a mix of
            // the two within one entry; and ts[point] == 0 already means
            // "not yet stamped" everywhere else this array is read, so a
            // dump that catches a record between AllocSlot and a later
            // Stamp() just looks like an earlier, still-legitimate
            // snapshot rather than corrupt data. Not fixed here -- doing
            // so would mean synchronizing every Stamp() against a
            // concurrent Dump(), which this design does not pay for.
            const uint64_t seq = r.slot_seq.load(butil::memory_order_acquire);
            if (seq == UINT64_MAX) {
                continue;   // never written
            }
            if (fwrite(&r, sizeof(r), 1, fp) != 1) {
                ok = false;
                break;
            }
            ++written;
        }
    }

    // fclose() is where stdio actually flushes its buffer to the OS/disk
    // -- a full buffer's worth of records can still be sitting in
    // userspace when the last fwrite() above returned success, and only
    // meet failure (ENOSPC, EIO, ...) here. Every fwrite succeeding is
    // therefore "the write loop succeeded", not "the file is written";
    // this check is what actually confirms the latter, and it is not
    // redundant with the per-fwrite checks above.
    if (fclose(fp) != 0) {
        ok = false;
    }

    if (!ok) {
        remove(path);
        return -1;
    }
    return written;
}

void LatencyTraceBuffer::DumpAtExitCallback() {
    // Dump-at-exit is the primary production path (see the flag's doc
    // comment) -- an I/O failure here has no caller left to report it to,
    // so it must be logged here or it is simply lost.
    LatencyTraceBuffer* b = instance();
    const int n = b->Dump(b->_dump_path.c_str());
    if (n < 0) {
        LOG(ERROR) << "Failed to dump latency trace records to \""
                   << b->_dump_path << "\"";
    }
}

void LatencyTraceBuffer::EnableDumpOnExit(const char* path) {
    _dump_path = path;
    if (!_atexit_registered) {
        atexit(&LatencyTraceBuffer::DumpAtExitCallback);
        _atexit_registered = true;
    }
}

}  // namespace brpc
