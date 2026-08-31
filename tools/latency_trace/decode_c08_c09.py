#!/usr/bin/env python3
"""
tools/latency_trace/decode_c08_c09.py

Throwaway-but-kept diagnostic script for the C09(wake) < C08(write_end)
"negative RTT" anomaly reported in docs/superpowers/plans/capture-notes.md
(task A) and investigated in task A2
(.superpowers/sdd/2026-08-29-latency-trace-instrumentation/task-a2-report.md).

Decodes only the client-side dump (role must be LT_ROLE_CLIENT for every
record in a client.dump -- this script does not handle mixed-role files)
and reports, for records where C09 < C08 in decoded-offset order:

  - the rate of occurrence
  - the distribution of the deficit C08 - C09 (in raw counter ticks and ns)
  - whether flagged records cluster by trace_id (i.e. time/position in the
    run), by `attempt`, or by unusual end-to-end latency (C17 - C01)
  - the C07 (write_start) -> C08 (write_end) interval for flagged records,
    to check whether C08 looks systematically late (vs. C07)

Byte layout is exactly design doc sec.8.1's record table, point indices are
LT_C_* / LT_S_* from src/brpc/latency_trace.h (client points occupy ts[0..16],
server points ts[17..33]; this script only reads client points).

Usage:
    python3 decode_c08_c09.py <client.dump>
"""
import struct
import sys
import statistics

HEADER_FMT = "<QIIQqQqqqdQQQQQ"  # first 112 bytes of the 128-byte header
HEADER_SIZE = 128
RECORD_SIZE = 192
POINT_COUNT = 34

# Client point indices (ts[] slots), from latency_trace.h's LatencyTracePoint enum.
C_RPC_START = 0
C_WRITE_START = 6      # C07
C_WRITE_END = 7        # C08
C_WAKE = 8              # C09
C_RPC_END = 16          # C17

TS_NOT_APPLICABLE = 0xFFFFFFFF
TS_SATURATED = 0xFFFFFFFE

RECORD_FMT = "<QQQ" + "34I" + "IIIIiiHBB4x"
assert struct.calcsize(RECORD_FMT) == RECORD_SIZE, struct.calcsize(RECORD_FMT)


def decode_ts(raw):
    """Return (offset_or_None, sentinel_str_or_None)."""
    if raw == 0:
        return None, "unstamped"
    if raw == TS_NOT_APPLICABLE:
        return None, "N/A"
    if raw == TS_SATURATED:
        return None, "saturated(>=42.9s)"
    return raw - 1, None


def read_header(f):
    data = f.read(HEADER_SIZE)
    fields = struct.unpack(HEADER_FMT, data[:112])
    keys = ["magic", "record_size", "point_count", "head_counter",
            "head_monotonic_ns", "tail_counter", "tail_monotonic_ns",
            "head_realtime_ns", "tail_realtime_ns", "counter_freq_hz",
            "cntfrq_el0_hz", "record_count", "dropped_count",
            "process_tag", "method_table_offset"]
    return dict(zip(keys, fields))


def read_records(f, count):
    for i in range(count):
        buf = f.read(RECORD_SIZE)
        if len(buf) != RECORD_SIZE:
            raise EOFError(f"short read at record {i}")
        vals = struct.unpack(RECORD_FMT, buf)
        trace_id, base_counter, slot_seq = vals[0], vals[1], vals[2]
        ts = vals[3:3 + POINT_COUNT]
        rest = vals[3 + POINT_COUNT:]
        (socket_id, remote_ip, req_size, rsp_size, method_id,
         error_code, remote_port, role, attempt) = rest
        yield {
            "index": i,
            "trace_id": trace_id,
            "base_counter": base_counter,
            "ts": ts,
            "role": role,
            "attempt": attempt,
            "error_code": error_code,
        }


def main():
    if len(sys.argv) != 2:
        print(f"usage: {sys.argv[0]} <client.dump>", file=sys.stderr)
        sys.exit(1)

    path = sys.argv[1]
    with open(path, "rb") as f:
        hdr = read_header(f)
        assert hdr["record_size"] == RECORD_SIZE
        assert hdr["point_count"] == POINT_COUNT
        freq = hdr["counter_freq_hz"]
        n = hdr["record_count"]
        print(f"file: {path}")
        print(f"record_count={n} dropped_count={hdr['dropped_count']} "
              f"freq={freq:.3f} Hz process_tag={hdr['process_tag']:#x}")

        records = list(read_records(f, n))

    non_client = [r for r in records if r["role"] != 0]
    if non_client:
        print(f"WARNING: {len(non_client)} records have role != CLIENT(0); "
              f"this script assumes a pure client.dump", file=sys.stderr)

    def ticks_to_ns(ticks):
        return ticks * 1e9 / freq

    flagged = []
    e2e_all = []
    write_span_all = []

    for r in records:
        ts = r["ts"]
        c01, _ = decode_ts(ts[C_RPC_START])
        c07, _ = decode_ts(ts[C_WRITE_START])
        c08, _ = decode_ts(ts[C_WRITE_END])
        c09, _ = decode_ts(ts[C_WAKE])
        c17, _ = decode_ts(ts[C_RPC_END])

        if c01 is not None and c17 is not None:
            e2e_all.append(c17 - c01)
        if c07 is not None and c08 is not None:
            write_span_all.append(c08 - c07)

        if c08 is None or c09 is None:
            continue  # can't compare; shouldn't happen for a TCP client dump

        if c09 < c08:
            deficit = c08 - c09
            write_span = (c08 - c07) if c07 is not None else None
            e2e = (c17 - c01) if (c01 is not None and c17 is not None) else None
            flagged.append({
                "index": r["index"],
                "trace_id": r["trace_id"],
                "attempt": r["attempt"],
                "error_code": r["error_code"],
                "deficit_ticks": deficit,
                "deficit_ns": ticks_to_ns(deficit),
                "write_span_ticks": write_span,
                "write_span_ns": ticks_to_ns(write_span) if write_span is not None else None,
                "e2e_ticks": e2e,
                "e2e_ns": ticks_to_ns(e2e) if e2e is not None else None,
                "c01": c01, "c07": c07, "c08": c08, "c09": c09, "c17": c17,
            })

    total = len(records)
    nflag = len(flagged)
    rate = nflag / total if total else 0.0
    print()
    print(f"records with C09 < C08 (negative RTT): {nflag} / {total} "
          f"= {rate*100:.5f}%")

    if not flagged:
        print("No occurrences in this run.")
        return

    deficits_ns = [x["deficit_ns"] for x in flagged]
    deficits_ns_sorted = sorted(deficits_ns)

    def pct(p):
        idx = min(len(deficits_ns_sorted) - 1, int(round(p * (len(deficits_ns_sorted) - 1))))
        return deficits_ns_sorted[idx]

    print()
    print("deficit (C08 - C09) distribution, nanoseconds:")
    print(f"  min    = {min(deficits_ns):.1f} ns")
    print(f"  p25    = {pct(0.25):.1f} ns")
    print(f"  median = {statistics.median(deficits_ns):.1f} ns")
    print(f"  p75    = {pct(0.75):.1f} ns")
    print(f"  p90    = {pct(0.90):.1f} ns")
    print(f"  max    = {max(deficits_ns):.1f} ns")
    if len(deficits_ns) > 1:
        print(f"  stdev  = {statistics.stdev(deficits_ns):.1f} ns")

    print()
    print("indices (position in run) of flagged records, and trace_id low32:")
    for x in flagged:
        print(f"  idx={x['index']:>7d}  trace_id_low32={x['trace_id'] & 0xFFFFFFFF:>8d}  "
              f"attempt={x['attempt']}  error_code={x['error_code']}  "
              f"deficit={x['deficit_ns']:.1f}ns  "
              f"write_span(C07->C08)={x['write_span_ns']:.1f}ns  "
              f"e2e(C01->C17)={x['e2e_ns']:.1f}ns  "
              f"raw: c07={x['c07']} c08={x['c08']} c09={x['c09']}")

    print()
    print("context: overall write-span (C07->C08) and e2e (C01->C17) stats, all records:")
    ws_ns = [ticks_to_ns(t) for t in write_span_all]
    e2e_ns = [ticks_to_ns(t) for t in e2e_all]
    if ws_ns:
        print(f"  write_span all: n={len(ws_ns)} min={min(ws_ns):.1f}ns "
              f"median={statistics.median(ws_ns):.1f}ns max={max(ws_ns):.1f}ns")
    if e2e_ns:
        print(f"  e2e all: n={len(e2e_ns)} min={min(e2e_ns):.1f}ns "
              f"median={statistics.median(e2e_ns):.1f}ns max={max(e2e_ns):.1f}ns")

    flagged_ws = [x["write_span_ns"] for x in flagged if x["write_span_ns"] is not None]
    flagged_e2e = [x["e2e_ns"] for x in flagged if x["e2e_ns"] is not None]
    if flagged_ws:
        print()
        print(f"  flagged write_span: n={len(flagged_ws)} min={min(flagged_ws):.1f}ns "
              f"median={statistics.median(flagged_ws):.1f}ns max={max(flagged_ws):.1f}ns")
    if flagged_e2e:
        print(f"  flagged e2e: n={len(flagged_e2e)} min={min(flagged_e2e):.1f}ns "
              f"median={statistics.median(flagged_e2e):.1f}ns max={max(flagged_e2e):.1f}ns")

    # Positional clustering: are flagged indices concentrated early in the run?
    idxs = [x["index"] for x in flagged]
    print()
    print(f"flagged record positions: min_idx={min(idxs)} max_idx={max(idxs)} "
          f"of {total} total (fraction of run covered: "
          f"{min(idxs)/total*100:.1f}% .. {max(idxs)/total*100:.1f}%)")


if __name__ == "__main__":
    main()
