// tools/latency_trace/probe/stamp_cost_probe.cpp
// Standalone. Build: g++ -O2 -std=c++14 stamp_cost_probe.cpp -o stamp_cost_probe
#include <cstdio>
#include <cstdint>
#include <cstring>
#include <ctime>
#include <vector>
#include <algorithm>

static inline uint64_t clock_cycles() {
#if defined(__aarch64__)
    uint64_t v;
    asm volatile("mrs %0, cntvct_el0" : "=r"(v));
    return v;
#elif defined(__x86_64__) || defined(__amd64__)
    unsigned lo, hi;
    __asm__ __volatile__("rdtsc" : "=a"(lo), "=d"(hi));
    return ((uint64_t)hi << 32) | lo;
#else
#error "unsupported arch"
#endif
}

static inline uint64_t cntfrq_hz() {
#if defined(__aarch64__)
    uint64_t v;
    asm volatile("mrs %0, cntfrq_el0" : "=r"(v));
    return v;
#else
    return 0;
#endif
}

static int64_t realtime_ns() {
    timespec ts;
    clock_gettime(CLOCK_REALTIME, &ts);
    return ts.tv_sec * 1000000000LL + ts.tv_nsec;
}

// Mimics LT_STAMP: bounds-checked generation compare, then one store.
struct Slot {
    uint64_t slot_seq;
    uint32_t ts[36];
};

int main() {
    const int N = 2000000;

    // 1. Empirical counter frequency, from a 200ms window.
    const uint64_t c0 = clock_cycles();
    const int64_t  r0 = realtime_ns();
    timespec nap = {0, 200000000};
    nanosleep(&nap, nullptr);
    const uint64_t c1 = clock_cycles();
    const int64_t  r1 = realtime_ns();
    const double freq = (double)(c1 - c0) * 1e9 / (double)(r1 - r0);
    printf("counter freq (empirical) = %.3f MHz\n", freq / 1e6);
    printf("counter freq (CNTFRQ_EL0) = %.3f MHz%s\n",
           cntfrq_hz() / 1e6, cntfrq_hz() ? "" : "  (n/a on this arch)");
    if (cntfrq_hz()) {
        const double d = (freq - (double)cntfrq_hz()) / (double)cntfrq_hz();
        printf("  relative deviation = %.4f%%\n", d * 100.0);
    }
    printf("counter resolution = %.2f ns/tick\n", 1e9 / freq);

    std::vector<Slot> slots(4096);
    memset(slots.data(), 0, slots.size() * sizeof(Slot));
    for (size_t i = 0; i < slots.size(); ++i) {
        slots[i].slot_seq = i;
    }

    // 2. Raw clock_cycles() only.
    int64_t t0 = realtime_ns();
    uint64_t sink = 0;
    for (int i = 0; i < N; ++i) {
        sink += clock_cycles();
    }
    int64_t t1 = realtime_ns();
    printf("clock_cycles()            = %.2f ns/op\n", (double)(t1 - t0) / N);

    // 3. clock_gettime(CLOCK_MONOTONIC) -- what brpc's cpuwide_time_ns()
    //    degrades to when BUTIL_USE_CPU_FREQUENCY=0 (the default).
    t0 = realtime_ns();
    for (int i = 0; i < N; ++i) {
        timespec ts;
        clock_gettime(CLOCK_MONOTONIC, &ts);
        sink += ts.tv_nsec;
    }
    t1 = realtime_ns();
    printf("clock_gettime(MONOTONIC)  = %.2f ns/op\n", (double)(t1 - t0) / N);

    // 4. Full LT_STAMP equivalent: counter read + generation check + store.
    const uint64_t base = clock_cycles();
    t0 = realtime_ns();
    for (int i = 0; i < N; ++i) {
        const uint64_t seq = (uint64_t)(i & 4095);
        Slot& s = slots[seq];
        if (s.slot_seq == seq) {
            s.ts[i % 36] = (uint32_t)(clock_cycles() - base);
        }
    }
    t1 = realtime_ns();
    const double per_stamp = (double)(t1 - t0) / N;
    printf("LT_STAMP equivalent       = %.2f ns/op\n", per_stamp);
    printf("=> 36 points cost         = %.2f ns (%.3f us)\n",
           per_stamp * 36, per_stamp * 36 / 1000.0);

    printf("sink=%llu\n", (unsigned long long)sink);  // defeat DCE
    return 0;
}
