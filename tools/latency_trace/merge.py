#!/usr/bin/env python3
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
"""tools/latency_trace/merge.py

Offline merger for the brpc latency-trace feature (see
docs/superpowers/specs/2026-08-29-brpc-latency-trace-design.md, sec.8-10,
and src/brpc/latency_trace.h, the authority when the two disagree).

Reads a client-side and a server-side binary dump (each produced by
LatencyTraceBuffer::Dump()), joins them by trace_id, applies the four
weight-bearing assertions from sec.10.1, computes the 33-item latency
decomposition (sec.5) under both link models (sec.6), and writes an
intermediate representation for tools/latency_trace/render.py (not yet
written -- Task C) to embed into a self-contained HTML file.

Python 3 standard library only. No numpy, no third-party dependencies --
this has to run on any machine that can read the dump files, not just a
build host.

Usage:
    python3 merge.py --client client.dump --server server.dump -o out.json
    python3 merge.py --client client.dump --server server.dump --stats-only
"""
import argparse
import base64
import json
import struct
import sys
from fractions import Fraction

# ---------------------------------------------------------------------------
# On-disk format constants -- mirror src/brpc/latency_trace.h exactly. This
# module is the second place (after check_format_doc.py) that would go
# silently wrong if that header and this file drifted apart; there is no
# runtime cross-check against the header from Python (it's a .h, not
# introspectable), so any change to the struct layout, the point enum, or
# the magic constant must be hand-mirrored here.
# ---------------------------------------------------------------------------

HEADER_SIZE = 128
RECORD_SIZE = 192
POINT_COUNT = 34

# Struct format for the first 112 significant bytes of LatencyTraceFileHeader
# (the remaining 16 are unused padding). Field order matches the struct's
# declaration order -- see design doc sec.8.1's table.
HEADER_FMT = "<QIIQqQqqqdQQQQQ"
HEADER_FIELDS = [
    "magic", "record_size", "point_count", "head_counter",
    "head_monotonic_ns", "tail_counter", "tail_monotonic_ns",
    "head_realtime_ns", "tail_realtime_ns", "counter_freq_hz",
    "cntfrq_el0_hz", "record_count", "dropped_count",
    "process_tag", "method_table_offset",
]
assert struct.calcsize(HEADER_FMT) == 112

# LT_FILE_MAGIC = 0x4252504C54524331ULL, read back as 8 little-endian bytes
# spells b"1CRTLPRB" -- see latency_trace.h's comment on LT_FILE_MAGIC and
# design doc sec.8.1's "magic constant" note for why this is NOT
# b"BRPCLTRC1" (wrong byte order AND an extra 'C' that was never in the
# constant).
LT_FILE_MAGIC_BYTES = struct.pack("<Q", 0x4252504C54524331)
assert LT_FILE_MAGIC_BYTES == b"1CRTLPRB", LT_FILE_MAGIC_BYTES

# LatencyTraceRecord, in declared-field order (design doc sec.8.1's table
# warns explicitly: remote_ip/remote_port/role/attempt are NOT adjacent in
# the struct -- req_size/rsp_size/method_id/error_code sit between
# remote_ip and remote_port). "4x" is the 4 bytes of trailing alignment
# padding that bring the record to exactly 192 bytes.
# The 9 trailing scalar fields are, in struct-declaration order: socket_id
# (u32), remote_ip (u32), req_size (u32), rsp_size (u32), method_id (u32 --
# NOT signed; a previous version of this format string used "i" here,
# which decoded any method_id >= 2^31 as negative), error_code (i32 --
# this one genuinely is signed), remote_port (u16), role (u8), attempt (u8).
RECORD_FMT = "<QQQ" + "34I" + "IIIIIiHBB4x"
assert struct.calcsize(RECORD_FMT) == RECORD_SIZE

# ts[] sentinels (latency_trace.h): 0 = never stamped, 0xFFFFFFFF = not
# applicable in this record's mode (RDMA polling), 0xFFFFFFFE = offset
# saturated (interval >= ~42.9s @ 100MHz). Neither sentinel is decremented;
# every other value's real offset is value - 1.
TS_UNSTAMPED = 0
TS_NOT_APPLICABLE = 0xFFFFFFFF
TS_SATURATED = 0xFFFFFFFE

LT_ROLE_CLIENT = 0
LT_ROLE_SERVER = 1

# Point indices, mirroring the LatencyTracePoint enum in latency_trace.h.
# Client points occupy ts[0..16], server points ts[17..33].
(
    C_RPC_START, C_REQ_PAYLOAD_SER_START, C_REQ_PAYLOAD_SER_END,
    C_REQ_META_SER_START, C_REQ_META_SER_END, C_WRITE_ENQUEUE,
    C_WRITE_START, C_WRITE_END, C_WAKE, C_ONEDGE_START, C_READV_START,
    C_MSG_RECV_DONE, C_RSP_META_DESER_START, C_RSP_META_DESER_END,
    C_RSP_PAYLOAD_DESER_START, C_RSP_PAYLOAD_DESER_END, C_RPC_END,
) = range(17)

(
    S_WAKE, S_ONEDGE_START, S_READV_START, S_MSG_RECV_DONE,
    S_REQ_META_DESER_START, S_REQ_META_DESER_END,
    S_REQ_PAYLOAD_DESER_START, S_REQ_PAYLOAD_DESER_END,
    S_SERVICE_START, S_SERVICE_END, S_RSP_PAYLOAD_SER_START,
    S_RSP_PAYLOAD_SER_END, S_RSP_META_SER_START, S_RSP_META_SER_END,
    S_WRITE_ENQUEUE, S_WRITE_START, S_WRITE_END,
) = range(17, 34)

CLIENT_POINTS = list(range(0, 17))
SERVER_POINTS = list(range(17, 34))

POINT_NAMES = {
    C_RPC_START: "C01_rpc_start", C_REQ_PAYLOAD_SER_START: "C02_req_payload_ser_start",
    C_REQ_PAYLOAD_SER_END: "C03_req_payload_ser_end", C_REQ_META_SER_START: "C04_req_meta_ser_start",
    C_REQ_META_SER_END: "C05_req_meta_ser_end", C_WRITE_ENQUEUE: "C06_write_enqueue",
    C_WRITE_START: "C07_write_start", C_WRITE_END: "C08_write_end",
    C_WAKE: "C09_wake", C_ONEDGE_START: "C10_onedge_start",
    C_READV_START: "C11_readv_start", C_MSG_RECV_DONE: "C12_msg_recv_done",
    C_RSP_META_DESER_START: "C13_rsp_meta_deser_start", C_RSP_META_DESER_END: "C14_rsp_meta_deser_end",
    C_RSP_PAYLOAD_DESER_START: "C15_rsp_payload_deser_start", C_RSP_PAYLOAD_DESER_END: "C16_rsp_payload_deser_end",
    C_RPC_END: "C17_rpc_end",
    S_WAKE: "S01_wake", S_ONEDGE_START: "S02_onedge_start", S_READV_START: "S03_readv_start",
    S_MSG_RECV_DONE: "S04_msg_recv_done", S_REQ_META_DESER_START: "S05_req_meta_deser_start",
    S_REQ_META_DESER_END: "S06_req_meta_deser_end", S_REQ_PAYLOAD_DESER_START: "S07_req_payload_deser_start",
    S_REQ_PAYLOAD_DESER_END: "S08_req_payload_deser_end", S_SERVICE_START: "S09_service_start",
    S_SERVICE_END: "S10_service_end", S_RSP_PAYLOAD_SER_START: "S11_rsp_payload_ser_start",
    S_RSP_PAYLOAD_SER_END: "S12_rsp_payload_ser_end", S_RSP_META_SER_START: "S13_rsp_meta_ser_start",
    S_RSP_META_SER_END: "S14_rsp_meta_ser_end", S_WRITE_ENQUEUE: "S15_write_enqueue",
    S_WRITE_START: "S16_write_start", S_WRITE_END: "S17_write_end",
}

# The 31 non-link decomposition items (design doc sec.5.1-5.3), in the
# canonical order they're presented in the doc: client send (7), server
# side (16), client receive (8). "starred" mirrors the doc's ★/○ marking
# (★ = user's original list item, ○ = gap item this design added) -- it's
# informational only (e.g. for render.py's color palette), not used by
# any assertion here.
#
# Each entry: (name, start_point, end_point, starred).
DECOMPOSITION_ITEMS_CLIENT_SEND = [
    ("cli_pre_serialize", C_RPC_START, C_REQ_PAYLOAD_SER_START, False),
    ("cli_req_payload_ser", C_REQ_PAYLOAD_SER_START, C_REQ_PAYLOAD_SER_END, True),
    ("cli_issue_rpc", C_REQ_PAYLOAD_SER_END, C_REQ_META_SER_START, False),
    ("cli_req_meta_ser", C_REQ_META_SER_START, C_REQ_META_SER_END, True),
    ("cli_pack_to_write", C_REQ_META_SER_END, C_WRITE_ENQUEUE, False),
    ("cli_write_queue", C_WRITE_ENQUEUE, C_WRITE_START, True),
    ("cli_write_syscall", C_WRITE_START, C_WRITE_END, True),
]

DECOMPOSITION_ITEMS_SERVER = [
    ("srv_wake_to_onedge", S_WAKE, S_ONEDGE_START, True),
    ("srv_onedge_to_readv", S_ONEDGE_START, S_READV_START, True),
    ("srv_readv", S_READV_START, S_MSG_RECV_DONE, True),
    ("srv_recv_to_deser", S_MSG_RECV_DONE, S_REQ_META_DESER_START, True),
    ("srv_req_meta_deser", S_REQ_META_DESER_START, S_REQ_META_DESER_END, True),
    ("srv_dispatch", S_REQ_META_DESER_END, S_REQ_PAYLOAD_DESER_START, False),
    ("srv_req_payload_deser", S_REQ_PAYLOAD_DESER_START, S_REQ_PAYLOAD_DESER_END, True),
    ("srv_to_service", S_REQ_PAYLOAD_DESER_END, S_SERVICE_START, False),
    ("srv_service", S_SERVICE_START, S_SERVICE_END, True),
    ("srv_service_to_ser", S_SERVICE_END, S_RSP_PAYLOAD_SER_START, False),
    ("srv_rsp_payload_ser", S_RSP_PAYLOAD_SER_START, S_RSP_PAYLOAD_SER_END, True),
    ("srv_compress_checksum", S_RSP_PAYLOAD_SER_END, S_RSP_META_SER_START, False),
    ("srv_rsp_meta_ser", S_RSP_META_SER_START, S_RSP_META_SER_END, True),
    ("srv_pack_to_write", S_RSP_META_SER_END, S_WRITE_ENQUEUE, False),
    ("srv_write_queue", S_WRITE_ENQUEUE, S_WRITE_START, True),
    ("srv_write_syscall", S_WRITE_START, S_WRITE_END, True),
]

DECOMPOSITION_ITEMS_CLIENT_RECV = [
    ("cli_wake_to_onedge", C_WAKE, C_ONEDGE_START, True),
    ("cli_onedge_to_readv", C_ONEDGE_START, C_READV_START, True),
    ("cli_readv", C_READV_START, C_MSG_RECV_DONE, True),
    ("cli_recv_to_deser", C_MSG_RECV_DONE, C_RSP_META_DESER_START, True),
    ("cli_rsp_meta_deser", C_RSP_META_DESER_START, C_RSP_META_DESER_END, True),
    ("cli_lookup_cntl", C_RSP_META_DESER_END, C_RSP_PAYLOAD_DESER_START, False),
    ("cli_rsp_payload_deser", C_RSP_PAYLOAD_DESER_START, C_RSP_PAYLOAD_DESER_END, True),
    ("cli_post_deser", C_RSP_PAYLOAD_DESER_END, C_RPC_END, False),
]

# All 31 non-link items, in canonical output order.
DECOMPOSITION_ITEMS_31 = (
    DECOMPOSITION_ITEMS_CLIENT_SEND
    + DECOMPOSITION_ITEMS_SERVER
    + DECOMPOSITION_ITEMS_CLIENT_RECV
)
assert len(DECOMPOSITION_ITEMS_31) == 31

# Names of the 4 items that are N/A (not 0) under RDMA polling mode --
# design doc sec.8.5.
NA_UNDER_POLLING = {
    "cli_wake_to_onedge", "cli_onedge_to_readv",
    "srv_wake_to_onedge", "srv_onedge_to_readv",
}

# Full 33-item canonical order (design doc sec.5): 7 client-send + link_up
# + 16 server + link_down + 8 client-recv, i.e. link_up/link_down are
# spliced in at the positions sec.5.2 puts them (first and last of the
# "link & server" block). This is purely a presentation-order convention
# for the IR/HTML; the arithmetic doesn't care about ordering.
ITEM_ORDER_33 = (
    [name for name, _, _, _ in DECOMPOSITION_ITEMS_CLIENT_SEND]
    + ["link_up"]
    + [name for name, _, _, _ in DECOMPOSITION_ITEMS_SERVER]
    + ["link_down"]
    + [name for name, _, _, _ in DECOMPOSITION_ITEMS_CLIENT_RECV]
)
assert len(ITEM_ORDER_33) == 33


# ---------------------------------------------------------------------------
# Decoding
# ---------------------------------------------------------------------------

class LatencyTraceFormatError(Exception):
    """Raised for anything that means "this is not a valid dump file",
    as opposed to a per-record data-quality issue (which is counted and
    reported, never raised)."""


def decode_ts(raw):
    """Decode one ts[] slot per latency_trace.h's encoding rule.

    Returns (kind, offset) where kind is one of:
      'unstamped'  -- raw == 0, point never fired. offset is None.
      'na'         -- raw == 0xFFFFFFFF, point does not exist in this
                      record's mode (RDMA polling). offset is None.
                      MUST NOT be decremented.
      'saturated'  -- raw == 0xFFFFFFFE, real interval >= ~42.9s. offset
                      is raw ITSELF (0xFFFFFFFE ticks), a lower bound, not
                      an exact value. latency_trace.h is explicit that
                      this sentinel "MUST NOT be decremented" the way an
                      ordinary value is -- unlike 'ok', do NOT subtract 1.
      'ok'         -- offset = raw - 1, the real offset.
    """
    if raw == TS_UNSTAMPED:
        return "unstamped", None
    if raw == TS_NOT_APPLICABLE:
        return "na", None
    if raw == TS_SATURATED:
        return "saturated", raw
    return "ok", raw - 1


def parse_header(data, path):
    if len(data) < HEADER_SIZE:
        raise LatencyTraceFormatError(
            f"{path}: file is {len(data)} bytes, shorter than the "
            f"{HEADER_SIZE}-byte header")
    raw = struct.unpack(HEADER_FMT, data[:struct.calcsize(HEADER_FMT)])
    header = dict(zip(HEADER_FIELDS, raw))
    magic_bytes = data[0:8]
    if magic_bytes != LT_FILE_MAGIC_BYTES:
        raise LatencyTraceFormatError(
            f"{path}: bad magic {magic_bytes!r}, expected {LT_FILE_MAGIC_BYTES!r} "
            f"(LT_FILE_MAGIC as little-endian bytes) -- refusing to parse "
            f"further")
    if header["record_size"] != RECORD_SIZE:
        raise LatencyTraceFormatError(
            f"{path}: header record_size={header['record_size']}, this "
            f"parser only understands {RECORD_SIZE}")
    if header["point_count"] != POINT_COUNT:
        raise LatencyTraceFormatError(
            f"{path}: header point_count={header['point_count']}, this "
            f"parser only understands {POINT_COUNT}")
    return header


def compute_freq(header, path):
    """Empirical counter frequency, computed ONLY from the monotonic
    calibration pair (head_monotonic_ns/tail_monotonic_ns) -- never from
    the realtime pair, which exists purely for cross-host sanity checks
    and would silently absorb an NTP step into what looks like a
    frequency error. See design doc sec.8.1/9.1.

    Returns (freq_hz: float, scale: Fraction) where `scale` is the exact
    rational number of nanoseconds per counter tick
    (delta_monotonic_ns / delta_counter), used for all downstream
    tick->ns conversions so that the sec.10.1/sec.5 sum identity holds
    with zero tolerance (see the module docstring in merge_records()).
    """
    delta_counter = header["tail_counter"] - header["head_counter"]
    delta_monotonic_ns = header["tail_monotonic_ns"] - header["head_monotonic_ns"]
    if delta_counter <= 0 or delta_monotonic_ns <= 0:
        raise LatencyTraceFormatError(
            f"{path}: non-positive calibration deltas (delta_counter="
            f"{delta_counter}, delta_monotonic_ns={delta_monotonic_ns}) -- "
            f"cannot compute a counter frequency")
    freq_hz = delta_counter * 1e9 / delta_monotonic_ns
    scale = Fraction(delta_monotonic_ns, delta_counter)  # ns per tick, exact
    # Cross-check against the header's own stored counter_freq_hz (computed
    # by the same formula, in the C++ runtime, from the same numbers) --
    # this should match almost exactly; a large gap would mean this parser
    # and latency_trace.cpp's Dump() disagree about the formula.
    stored = header["counter_freq_hz"]
    if stored > 0 and abs(freq_hz - stored) / stored > 1e-6:
        print(f"WARNING: {path}: recomputed counter_freq_hz ({freq_hz:.3f} Hz) "
              f"disagrees with the header's stored value ({stored:.3f} Hz) by "
              f"{abs(freq_hz - stored) / stored * 100:.4f}% -- merge.py's "
              f"formula may have drifted from latency_trace.cpp's Dump()",
              file=sys.stderr)
    # Cross-check against CNTFRQ_EL0 (aarch64 only; 0 elsewhere) -- design
    # doc sec.8.1 calls for a warning past 0.1% disagreement.
    cntfrq = header["cntfrq_el0_hz"]
    if cntfrq > 0:
        rel = abs(freq_hz - cntfrq) / cntfrq
        if rel > 0.001:
            print(f"WARNING: {path}: empirical counter_freq_hz ({freq_hz:.3f} Hz) "
                  f"disagrees with CNTFRQ_EL0 ({cntfrq} Hz) by {rel * 100:.4f}%, "
                  f"over the 0.1% sanity threshold", file=sys.stderr)
    return freq_hz, scale


def check_realtime_overlap(client_dump, server_dump):
    """The realtime calibration pair (head_realtime_ns/tail_realtime_ns)
    exists precisely so an offline tool can sanity-check that the client
    and server dump handed to it actually came from overlapping capture
    runs (design doc sec.8.1/9.1 -- "仅供跨机 sanity check"), rather than,
    say, today's client dump paired with last week's server dump that
    happens to share some trace_ids by pure coincidence of the sequence
    counter wrapping. Before this function, that check was never actually
    performed -- two dumps from unrelated runs joined "successfully" with
    no warning at all. Returns True iff the two [head,tail] realtime
    windows overlap; always also prints a WARNING to stderr when they
    don't, since a caller might otherwise proceed straight to writing the
    IR without inspecting the return value.
    """
    ch, sh = client_dump["header"], server_dump["header"]
    c_lo, c_hi = ch["head_realtime_ns"], ch["tail_realtime_ns"]
    s_lo, s_hi = sh["head_realtime_ns"], sh["tail_realtime_ns"]
    overlap = c_lo <= s_hi and s_lo <= c_hi
    if not overlap:
        print(f"WARNING: {client_dump['path']} and {server_dump['path']} "
              f"do not overlap in realtime -- client capture window is "
              f"[{c_lo}, {c_hi}] ns since epoch, server's is [{s_lo}, {s_hi}] "
              f"ns since epoch. A successful trace_id join does not by "
              f"itself prove these two dumps came from the same capture "
              f"run; this pair looks like it did not.", file=sys.stderr)
    return overlap


def parse_records(data, header, path):
    n = header["record_count"]
    records = []
    offset = HEADER_SIZE
    for i in range(n):
        chunk = data[offset:offset + RECORD_SIZE]
        if len(chunk) != RECORD_SIZE:
            raise LatencyTraceFormatError(
                f"{path}: truncated at record {i}/{n} -- expected "
                f"{RECORD_SIZE} bytes, got {len(chunk)} (file is {len(data)} "
                f"bytes total, header claims record_count={n})")
        vals = struct.unpack(RECORD_FMT, chunk)
        trace_id, base_counter, slot_seq = vals[0], vals[1], vals[2]
        ts_raw = vals[3:3 + POINT_COUNT]
        rest = vals[3 + POINT_COUNT:]
        (socket_id, remote_ip, req_size, rsp_size, method_id,
         error_code, remote_port, role, attempt) = rest
        records.append({
            "index": i,
            "trace_id": trace_id,
            "base_counter": base_counter,
            "slot_seq": slot_seq,
            "ts_raw": ts_raw,
            "socket_id": socket_id,
            "remote_ip": remote_ip,
            "req_size": req_size,
            "rsp_size": rsp_size,
            "method_id": method_id,
            "error_code": error_code,
            "remote_port": remote_port,
            "role": role,
            "attempt": attempt,
        })
        offset += RECORD_SIZE
    return records, offset


def parse_method_table(data, header, path):
    off = header["method_table_offset"]
    if off == 0:
        # Design doc doesn't define a zero offset as legal, but nothing
        # stops a corrupt/short-written file (or a synthetic test fixture)
        # from having one -- treat it as "no method table", not a crash.
        return {}
    if off + 4 > len(data):
        raise LatencyTraceFormatError(
            f"{path}: method_table_offset={off} is past end of file "
            f"({len(data)} bytes) -- cannot read table's count field")
    (count,) = struct.unpack_from("<I", data, off)
    pos = off + 4
    table = {}
    for i in range(count):
        if pos + 4 > len(data):
            raise LatencyTraceFormatError(
                f"{path}: method table truncated reading entry {i}/{count}'s "
                f"length field at byte {pos}")
        (name_len,) = struct.unpack_from("<I", data, pos)
        pos += 4
        if pos + name_len > len(data):
            raise LatencyTraceFormatError(
                f"{path}: method table truncated reading entry {i}/{count}'s "
                f"{name_len}-byte name at offset {pos}")
        name = data[pos:pos + name_len].decode("utf-8")
        pos += name_len
        # method_id == i + 1 (0 is reserved for "unidentified method").
        table[i + 1] = name
    return table


def parse_dump(path):
    with open(path, "rb") as f:
        data = f.read()
    header = parse_header(data, path)
    freq_hz, scale = compute_freq(header, path)
    records, records_end = parse_records(data, header, path)
    methods = parse_method_table(data, header, path)
    return {
        "path": path,
        "header": header,
        "freq_hz": freq_hz,
        "scale_ns_per_tick": scale,
        "records": records,
        "methods": methods,
        "file_size": len(data),
    }


# ---------------------------------------------------------------------------
# Per-record decode: absolute tick offsets for every point this record
# actually owns (its own 17-point half of ts[]).
# ---------------------------------------------------------------------------

def decode_record_points(record, point_range):
    """Returns {point_index: (kind, abs_ticks_or_None)} for every point in
    point_range. abs_ticks = base_counter + offset for kind == 'ok' or
    'saturated'; None for 'unstamped' and 'na'."""
    out = {}
    base = record["base_counter"]
    ts_raw = record["ts_raw"]
    for p in point_range:
        kind, off = decode_ts(ts_raw[p])
        if kind in ("ok", "saturated"):
            out[p] = (kind, base + off)
        else:
            out[p] = (kind, None)
    return out


# ---------------------------------------------------------------------------
# Join + assertions (design doc sec.10.1) + decomposition (sec.5/6)
# ---------------------------------------------------------------------------

REJECT_UNSTAMPED = "unstamped_point"
REJECT_NON_MONOTONIC = "non_monotonic"
REJECT_NEGATIVE_ITEM = "negative_decomposition_item"
# Named for what it is (see design doc sec.10.1's own characterization and
# capture-notes.md's anomaly write-up), not just "assertion failure": the
# true write-completion instant is inside the write syscall; the stamp
# executes afterwards on a schedulable thread and is occasionally delayed
# past the response's own wake stamp. Characterized at ~0.001-0.03% over
# tens of thousands of requests -- an inherent scheduling-delay tail, not
# a fixed per-code-path defect. The assertion is NOT relaxed for these --
# they are still excluded from the statistics, just bucketed separately so
# a reader can tell "known scheduling tail" from "something is actually
# broken" at a glance.
REJECT_NEGATIVE_RTT_LATE_WRITE_STAMP = "negative_rtt_late_write_end_stamp"

ALL_REJECT_REASONS = [
    REJECT_UNSTAMPED, REJECT_NON_MONOTONIC, REJECT_NEGATIVE_ITEM,
    REJECT_NEGATIVE_RTT_LATE_WRITE_STAMP,
]


def check_points_nonzero(points):
    """sec.10.1 assertion 1: every point in this record's own range must
    be non-'unstamped'. 'na' (RDMA polling, point genuinely doesn't exist)
    is NOT a failure -- only literal 0 (never fired) is."""
    for p, (kind, _) in points.items():
        if kind == "unstamped":
            return False
    return True


def unstamped_point_names(points):
    """The POINT_NAMES of every point in `points` that decoded as
    'unstamped' (raw 0) -- i.e. the specific evidence behind a
    REJECT_UNSTAMPED verdict. A raw 0 has (at least) two legitimate
    causes design doc sec.13 names explicitly -- streaming responses
    never stamp S15-S17 (they don't go through the ordinary sock->Write()
    path), and a cancelled backup request's receive-side points are
    likewise never reached -- as well as the un-instrumented-code-path
    possibility. The runtime writes the same plain 0 for all of them, so
    this parser genuinely cannot tell which one it is; naming exactly
    which point(s) were zero, aggregated across a whole capture (see
    merge_dumps' unstamped_point_histogram), is the most a reader can be
    given without asserting a cause this data doesn't support."""
    return [POINT_NAMES[p] for p, (kind, _) in points.items() if kind == "unstamped"]


def check_monotonic(points, point_range):
    """sec.10.1 assertion 2: decoded offsets must be non-decreasing in
    point order. 'na' points are skipped (no offset to compare -- the
    event genuinely doesn't exist under this record's mode). 'unstamped'
    points are ALSO skipped here: they decode to ticks=None (no
    comparable value), and a record with an unstamped point is already
    caught by check_points_nonzero()'s own assertion -- this function
    must not crash comparing None to an int just because some other
    assertion already condemns the record for a different, specific
    reason. A 'saturated' point's lower-bound offset still participates."""
    prev = None
    for p in point_range:
        kind, ticks = points[p]
        if kind in ("na", "unstamped"):
            continue
        if prev is not None and ticks < prev:
            return False
        prev = ticks
    return True


def merge_one(client_rec, server_rec, client_scale, server_scale, methods_client,
              low_concurrency=False):
    """Compute the full decomposition for one joined (client, server)
    record pair. Returns a dict with per-item ns values (as Fraction, for
    exact identity checking -- caller rounds to int for output), reject
    reasons (possibly empty), and diagnostic flags.

    All tick->ns conversion is via `scale` (ns per tick, an exact
    Fraction computed in compute_freq() from the integer calibration
    pair) rather than a float Hz division, so that the sec.10.1/sec.5 sum
    identity holds EXACTLY (zero tolerance, per design doc sec.9.1) rather
    than "close enough" -- see compute_freq()'s docstring. This matters
    because floating-point ns values computed independently per-item and
    then summed would not, in general, exactly equal a directly-computed
    C17-C01 in the last bit, even though the underlying algebra is an
    exact telescoping identity.

    `low_concurrency`: sec.10.1 says the "every decomposition item
    non-negative" check (assertion 3) is only a validity assertion at
    outstanding=1 -- at real capture concurrency, a negative link item is
    expected batching noise (sec.6.1) and, under model B, a legitimate
    pollution *detector* (sec.6.2), not a defect signal. So this reason is
    only added to the record's (enforced) `reasons` set when the caller
    asserts the capture was actually low-concurrency. `any_negative_item`
    is still computed and returned unconditionally, both so callers can
    report the ratio (must-fix 3 in the fix-round review) and so
    estimate_offsets_model_b()'s anchor-selection filter can still exclude
    these records from being an anchor candidate even when they are not
    being rejected outright (see that function's own comment -- this is
    the same bad-anchor bug class, and must not reopen it).
    """
    reasons = set()

    cpts = decode_record_points(client_rec, CLIENT_POINTS)
    spts = decode_record_points(server_rec, SERVER_POINTS)

    unstamped_points = []
    if not check_points_nonzero(cpts):
        reasons.add(REJECT_UNSTAMPED)
        unstamped_points += unstamped_point_names(cpts)
    if not check_points_nonzero(spts):
        reasons.add(REJECT_UNSTAMPED)
        unstamped_points += unstamped_point_names(spts)
    if not check_monotonic(cpts, CLIENT_POINTS):
        reasons.add(REJECT_NON_MONOTONIC)
    if not check_monotonic(spts, SERVER_POINTS):
        reasons.add(REJECT_NON_MONOTONIC)

    def ticks(pts, p):
        kind, v = pts[p]
        return kind, v

    def ns(pts, p, scale):
        kind, v = ticks(pts, p)
        if kind in ("ok", "saturated"):
            return Fraction(v) * scale
        return None  # unstamped or na

    # Absolute ns for every point this record owns (Fraction or None).
    c_ns = {p: ns(cpts, p, client_scale) for p in CLIENT_POINTS}
    s_ns = {p: ns(spts, p, server_scale) for p in SERVER_POINTS}

    # Points whose kind is 'saturated' (design doc sec.8.1/9.1): their ns
    # value is a lower bound, not an exact reading. Collected here so the
    # 31 items and (further below) the RTT/S/link values that used one of
    # these points as an endpoint can be flagged for render.py, instead of
    # silently rendering a ~42.9s-or-more interval as if it were exact.
    c_saturated_points = {p for p, (kind, _) in cpts.items() if kind == "saturated"}
    s_saturated_points = {p for p, (kind, _) in spts.items() if kind == "saturated"}

    def item_value(start_p, end_p):
        a = c_ns[start_p] if start_p in c_ns else s_ns[start_p]
        b = c_ns[end_p] if end_p in c_ns else s_ns[end_p]
        if a is None or b is None:
            return None  # N/A: an endpoint is unstamped or na
        return b - a

    items = {}
    for name, sp, ep, _starred in DECOMPOSITION_ITEMS_31:
        items[name] = item_value(sp, ep)

    saturated_items = [
        name for name, sp, ep, _starred in DECOMPOSITION_ITEMS_31
        if sp in c_saturated_points or sp in s_saturated_points
        or ep in c_saturated_points or ep in s_saturated_points
    ]

    # sec.10.1's cross-end nonneg check: C09 >= C08, purely client-side.
    #
    # RDMA polling mode (design doc sec.8.5) has no wake/onedge_start
    # event at all -- C09/C10 (and S01/S02) are LT_TS_NOT_APPLICABLE, not
    # 0. Falling back straight to "RTT/S is None" there would make
    # link_total None for every polling-mode record, silently dropping
    # both link items AND the identity check for the entire capture. The
    # design doc's fix is exact, not an approximation: under polling,
    # wake/onedge_start/readv_start are the SAME instant (those two
    # intervals are genuinely zero), so substituting readv_start (C11 /
    # S03 -- still a real timestamp under polling, see sec.8.5's table)
    # for wake (C09 / S01) reproduces the same RTT/S/link_total/identity
    # algebra as the non-polling case: cli_wake_to_onedge and
    # cli_onedge_to_readv (srv_ likewise) are still correctly N/A (0) in
    # `items` above, and the telescoping sum still lands on C17-C01
    # exactly because the same substitute point is used on both sides of
    # the identity (see the fix-round report for the worked-out algebra).
    c09_kind, _ = cpts[C_WAKE]
    c09 = c_ns.get(C_READV_START) if c09_kind == "na" else c_ns.get(C_WAKE)
    c08 = c_ns.get(C_WRITE_END)
    negative_rtt = False
    rtt = None
    if c08 is not None and c09 is not None:
        rtt = c09 - c08
        # The negative-RTT write-stamp-anomaly reason is specifically
        # about the ordinary (non-polling) C09 vs C08 relationship
        # (design doc sec.8.4/10.1); don't fire it off the readv_start
        # substitute, which has a different, non-anomalous relationship
        # to C08 (readv_start is a receive-side event, not a response to
        # this request's own write at all under polling).
        if rtt < 0 and c09_kind != "na":
            negative_rtt = True
            reasons.add(REJECT_NEGATIVE_RTT_LATE_WRITE_STAMP)

    s01_kind, _ = spts[S_WAKE]
    s01 = s_ns.get(S_READV_START) if s01_kind == "na" else s_ns.get(S_WAKE)
    s17 = s_ns.get(S_WRITE_END)
    srv_span = None
    if s01 is not None and s17 is not None:
        srv_span = s17 - s01

    link_total = None
    if rtt is not None and srv_span is not None:
        link_total = rtt - srv_span

    # ---- Model A: per-request halving ----
    if link_total is not None:
        link_up_a = link_total / 2
        link_down_a = link_total / 2
    else:
        link_up_a = link_down_a = None

    # ---- Model B: caller supplies the connection's estimated clock
    # offset O (see estimate_offsets_model_b()); here we only compute the
    # split given O, since O needs a whole connection's window of records
    # to estimate -- see merge_dumps() for the two-pass structure. ----
    # Placeholder; filled in by the caller once O is known.

    # sec.10.1 assertion 3: every decomposition item non-negative (valid
    # only at outstanding=1 / low concurrency -- see `low_concurrency`
    # above; at higher concurrency negative items are expected batching
    # noise, not a defect signal). N/A items (None) don't participate.
    # `any_negative_item` itself is ALWAYS computed (used for reporting
    # and for the model-B anchor filter regardless of the flag); only
    # whether it is added to the enforced `reasons` set is conditional.
    any_negative_item = False
    for name, val in items.items():
        if val is not None and val < 0:
            any_negative_item = True
    if link_total is not None and link_total < 0:
        # A negative *total* link time is itself informationally
        # interesting (sec.6.1: "L < 0 的记录单独标记...不静默裁剪为0")
        # but is not, by itself, one of the four items counted above;
        # still counts toward the nonneg-item assertion since link_up/
        # link_down would be negative too.
        any_negative_item = True
    if any_negative_item and low_concurrency:
        reasons.add(REJECT_NEGATIVE_ITEM)

    e2e = None
    if c_ns.get(C_RPC_START) is not None and c_ns.get(C_RPC_END) is not None:
        e2e = c_ns[C_RPC_END] - c_ns[C_RPC_START]

    # sec.10.1/sec.5's telescoping sum identity: Sigma(33 items) ==
    # C17 - C01, using N/A items as 0 (design doc sec.8.5: "N/A 项以 0
    # 参与求和，而这些区间的真实长度确实为 0"). Only checkable when C01,
    # C17, and the link total are all known: a record failing the
    # points-nonzero assertion may be missing exactly these, and the
    # identity is meaningless (not merely unverifiable) without them.
    identity_ok = None
    identity_lhs = None
    identity_rhs = e2e
    if e2e is not None and link_total is not None:
        total = Fraction(0)
        for name, val in items.items():
            if val is not None:
                total += val
        total += link_total  # link_up + link_down == link_total exactly
        identity_lhs = total
        identity_ok = (total == e2e)

    return {
        "reasons": reasons,
        "items_ns": items,             # name -> Fraction or None
        "rtt_ns": rtt,
        "srv_span_ns": srv_span,
        "link_total_ns": link_total,
        "link_up_a_ns": link_up_a,
        "link_down_a_ns": link_down_a,
        "e2e_ns": e2e,
        "negative_rtt": negative_rtt,
        "any_negative_item": any_negative_item,
        "saturated_items": saturated_items,
        "unstamped_points": unstamped_points,
        # The (possibly readv_start-substituted, under RDMA polling mode --
        # see the comment above rtt's computation) anchors actually used
        # for rtt/srv_span/link_total. Model B's a/b/c/d (apply_model_b(),
        # estimate_offsets_model_b()) MUST read these rather than
        # c_ns[C_WAKE]/s_ns[S_WAKE] directly: under polling those raw dict
        # entries are None (the real point is N/A), and indexing them
        # would crash exactly the polling-mode records this substitution
        # was added to stop silently under-reporting.
        "c09_anchor_ns": c09,
        "s01_anchor_ns": s01,
        "identity_ok": identity_ok,
        "identity_lhs": identity_lhs,
        "identity_rhs": identity_rhs,
        "c_ns": c_ns,
        "s_ns": s_ns,
    }


# ---------------------------------------------------------------------------
# Link model B: per-connection sliding-window offset estimate (sec.6.2).
# ---------------------------------------------------------------------------

DEFAULT_WINDOW_NS = 1_000_000_000  # 1 second, matches design doc's default


def estimate_offsets_model_b(joined, window_ns=DEFAULT_WINDOW_NS):
    """Assigns each joined record (that has a usable link_total/rtt/srv_span)
    a model-B clock offset O, per design doc sec.6.2: group by
    (client_process_tag, server_process_tag), slide a window over the
    client's rpc_start time, and within each window use the sample with
    the smallest L (=link_total) as the offset estimator:
    O_i = a + L_i/2 - b, where a = C08_ns, b = S01_ns of THAT sample.

    Mutates each entry of `joined` in place, adding "offset_b_ns" (a
    Fraction, or None if the record couldn't be windowed -- e.g. missing
    rtt/srv_span/link_total, or C01 unusable as the window key).
    """
    groups = {}
    for j in joined:
        m = j["merged"]
        if m["link_total_ns"] is None:
            j["offset_b_ns"] = None
            continue
        c01 = m["c_ns"].get(C_RPC_START)
        if c01 is None:
            j["offset_b_ns"] = None
            continue
        key = (j["client_process_tag"], j["server_process_tag"])
        groups.setdefault(key, []).append(j)

    for key, recs in groups.items():
        recs.sort(key=lambda j: j["merged"]["c_ns"][C_RPC_START])
        if not recs:
            continue
        t0 = recs[0]["merged"]["c_ns"][C_RPC_START]
        # NOTE: despite design doc sec.6.2's "滑动窗口" ("sliding window")
        # phrasing, `w` below buckets records into fixed-size,
        # non-overlapping bins of `window_ns` since t0 -- a TUMBLING
        # window, not a window that continuously slides along the time
        # axis. Every record in the same bin shares one O, and O jumps at
        # each bin boundary rather than drifting continuously; this is
        # simpler to implement and reason about, and still tracks
        # frequency drift at the same ~window_ns granularity sec.6.2 is
        # actually after, but it is not literally what "sliding" would
        # mean, and a reader translating this code back into the doc's
        # words should not expect a continuously-moving window here.
        window_index = {}
        for j in recs:
            t = j["merged"]["c_ns"][C_RPC_START]
            w = int((t - t0) // window_ns)
            window_index.setdefault(w, []).append(j)
        for w, members in window_index.items():
            # The anchor MUST come from records that already pass ALL FOUR
            # of sec.10.1's assertions -- including the non-negative-item
            # one, REGARDLESS of whether the caller is running in
            # low-concurrency mode (merge_one()'s `low_concurrency` only
            # controls whether that assertion's failure EXCLUDES a record
            # from the accepted/output set; it must not also loosen this
            # filter). Without this, argmin(L) can pick a record whose
            # negative link_total is itself one of those failures (found
            # empirically: on the real capture, the global-minimum-L
            # record was one of the 5 negative_decomposition_item
            # failures) -- anchoring the whole window's offset on a
            # record already known to be unreliable is worse than the
            # "queueing pollution" this argmin selection is trying to
            # avoid, and it systematically skewed nearly every other
            # record's link_up_b negative (a single bad anchor's bias
            # propagates to the whole window, since O is one shared
            # value). Fall back to the full member set only if every
            # record in the window fails (no clean candidate exists at
            # all).
            candidates = [j for j in members
                          if not j["merged"]["reasons"]
                          and not j["merged"]["any_negative_item"]]
            if not candidates:
                candidates = members
            best = min(candidates, key=lambda j: j["merged"]["link_total_ns"])
            a = best["merged"]["c_ns"][C_WRITE_END]
            b = best["merged"]["s01_anchor_ns"]
            L = best["merged"]["link_total_ns"]
            O = a + L / 2 - b
            for j in members:
                j["offset_b_ns"] = O


def apply_model_b(joined_entry):
    """Given offset_b_ns already assigned, compute link_up_b/link_down_b
    for one joined entry. link_up = (b+O)-a, link_down = d-(c+O); sum is
    identically link_total (design doc sec.6.2)."""
    m = joined_entry["merged"]
    O = joined_entry.get("offset_b_ns")
    if O is None or m["link_total_ns"] is None:
        m["link_up_b_ns"] = None
        m["link_down_b_ns"] = None
        return
    a = m["c_ns"][C_WRITE_END]
    b = m["s01_anchor_ns"]  # S01, or its readv_start substitute under RDMA polling
    c = m["s_ns"][S_WRITE_END]
    d = m["c09_anchor_ns"]  # C09, or its readv_start substitute under RDMA polling
    link_up_b = (b + O) - a
    link_down_b = d - (c + O)
    m["link_up_b_ns"] = link_up_b
    m["link_down_b_ns"] = link_down_b
    # Model B's own diagnostic (sec.6.2): link_up_b < 0 precisely flags
    # "this record's S01 is not trustworthy" (inherited an earlier
    # batch's wake value) -- surfaced, not hidden.
    m["model_b_link_up_negative"] = (link_up_b < 0)


# ---------------------------------------------------------------------------
# Top-level merge
# ---------------------------------------------------------------------------

def merge_dumps(client_dump, server_dump, window_ns=DEFAULT_WINDOW_NS,
                 low_concurrency=False):
    realtime_overlap_ok = check_realtime_overlap(client_dump, server_dump)

    client_recs = [r for r in client_dump["records"]]
    server_recs = [r for r in server_dump["records"]]

    client_by_role = [r for r in client_recs if r["role"] == LT_ROLE_CLIENT]
    server_by_role = [r for r in server_recs if r["role"] == LT_ROLE_SERVER]
    if len(client_by_role) != len(client_recs):
        print(f"WARNING: {client_dump['path']}: "
              f"{len(client_recs) - len(client_by_role)} record(s) have "
              f"role != CLIENT; only role==CLIENT records are used from "
              f"this file", file=sys.stderr)
    if len(server_by_role) != len(server_recs):
        print(f"WARNING: {server_dump['path']}: "
              f"{len(server_recs) - len(server_by_role)} record(s) have "
              f"role != SERVER; only role==SERVER records are used from "
              f"this file", file=sys.stderr)

    client_by_id = {}
    for r in client_by_role:
        if r["trace_id"] in client_by_id:
            print(f"WARNING: {client_dump['path']}: duplicate trace_id "
                  f"{r['trace_id']:#x} at record index {r['index']} -- "
                  f"keeping the first occurrence", file=sys.stderr)
            continue
        client_by_id[r["trace_id"]] = r

    server_by_id = {}
    for r in server_by_role:
        if r["trace_id"] in server_by_id:
            print(f"WARNING: {server_dump['path']}: duplicate trace_id "
                  f"{r['trace_id']:#x} at record index {r['index']} -- "
                  f"keeping the first occurrence", file=sys.stderr)
            continue
        server_by_id[r["trace_id"]] = r

    client_ids = set(client_by_id)
    server_ids = set(server_by_id)
    joined_ids = client_ids & server_ids
    only_client = client_ids - server_ids
    only_server = server_ids - client_ids

    client_scale = client_dump["scale_ns_per_tick"]
    server_scale = server_dump["scale_ns_per_tick"]
    client_tag = client_dump["header"]["process_tag"]
    server_tag = server_dump["header"]["process_tag"]

    joined = []
    reject_counts = {r: 0 for r in ALL_REJECT_REASONS}
    accepted = []
    any_negative_item_count = 0
    unstamped_point_histogram = {}

    for tid in joined_ids:
        crec = client_by_id[tid]
        srec = server_by_id[tid]
        m = merge_one(crec, srec, client_scale, server_scale, client_dump["methods"],
                       low_concurrency=low_concurrency)
        entry = {
            "trace_id": tid,
            "client_rec": crec,
            "server_rec": srec,
            "merged": m,
            "client_process_tag": client_tag,
            "server_process_tag": server_tag,
        }
        joined.append(entry)
        for reason in m["reasons"]:
            reject_counts[reason] += 1
        if m["any_negative_item"]:
            any_negative_item_count += 1
        for name in m["unstamped_points"]:
            unstamped_point_histogram[name] = unstamped_point_histogram.get(name, 0) + 1
        if not m["reasons"]:
            accepted.append(entry)

    estimate_offsets_model_b(joined, window_ns=window_ns)
    for j in joined:
        apply_model_b(j)

    return {
        "client_ids": client_ids,
        "server_ids": server_ids,
        "joined_ids": joined_ids,
        "only_client": only_client,
        "only_server": only_server,
        "joined": joined,
        "accepted": accepted,
        "reject_counts": reject_counts,
        "low_concurrency": low_concurrency,
        "any_negative_item_count": any_negative_item_count,
        "unstamped_point_histogram": unstamped_point_histogram,
        "realtime_overlap_ok": realtime_overlap_ok,
    }


# ---------------------------------------------------------------------------
# Intermediate representation for render.py
# ---------------------------------------------------------------------------
#
# Per accepted record we need: the 31 shared items (ns), model A's
# link_up/link_down (2), and model B's link_up/link_down (2) -- 35 int32
# values total. Storing model A and model B's full 33-item vectors
# separately (66 ints/record) would double the blob for no reason: the 31
# non-link items are identical between models by construction (design doc
# sec.6.3 -- only the link split differs), so this format stores them
# once and lets render.py recombine {31 shared items, link_up, link_down}
# per model on the fly. See report for the size measurement this is based
# on.
#
# Layout of one record's 35 int32 values, in this fixed order:
#   [0..30]  the 31 items in ITEM_ORDER_33's order with link_up/link_down
#            removed (i.e. DECOMPOSITION_ITEMS_31's order)
#   [31]     link_up, model A
#   [32]     link_down, model A
#   [33]     link_up, model B
#   [34]     link_down, model B
# N/A items are encoded as INT32_MIN (a value no real nanosecond interval
# can take); render.py must special-case it rather than plotting -2^31 ns.
NA_SENTINEL_I32 = -(2 ** 31)
INTS_PER_RECORD = len(DECOMPOSITION_ITEMS_31) + 4
assert INTS_PER_RECORD == 35


def frac_to_i32(x):
    """Returns (i32_value, clipped: bool). `clipped` is True whenever the
    int32-ns range (+-2^31, about +-2.147s) couldn't hold the real value
    and it had to be clamped -- previously computed as a local and thrown
    away; a caller now has to actually be able to ask. This is not the
    same condition as a 'saturated' ts[] sentinel (that one starts from a
    ~42.9s ticks-side lower bound and will always also clip here since
    2.147s < 42.9s), but an ordinary very-slow request's real ns value can
    trip it too, so the two are tracked and reported separately."""
    if x is None:
        return NA_SENTINEL_I32, False
    # round-half-to-even via Fraction's own rounding; values are tick-scale
    # (>= ~10ns granularity) so sub-ns rounding has no observable effect.
    v = round(x)
    clipped = False
    if v < -(2 ** 31) + 1 or v > 2 ** 31 - 1:
        # Should be exceedingly rare (an interval > ~2.1s) but don't
        # silently wrap -- clamp and report it via the return value.
        clipped = True
        v = max(-(2 ** 31) + 1, min(2 ** 31 - 1, v))
    return int(v), clipped


def record_to_i32_row(entry):
    """Returns (row, clipped_item_names) -- see frac_to_i32's docstring
    for what "clipped" means. `row` is the flat 35-int32 layout described
    above build_intermediate_representation."""
    m = entry["merged"]
    row = []
    clipped_items = []
    for name, _, _, _ in DECOMPOSITION_ITEMS_31:
        val = m["items_ns"].get(name)
        i32, was_clipped = frac_to_i32(val)
        row.append(i32)
        if was_clipped:
            clipped_items.append(name)
    for key, label in (("link_up_a_ns", "link_up_model_a"),
                        ("link_down_a_ns", "link_down_model_a"),
                        ("link_up_b_ns", "link_up_model_b"),
                        ("link_down_b_ns", "link_down_model_b")):
        i32, was_clipped = frac_to_i32(m.get(key))
        row.append(i32)
        if was_clipped:
            clipped_items.append(label)
    return row, clipped_items


def build_intermediate_representation(client_dump, server_dump, merge_result,
                                       max_b64_bytes=12 * 1024 * 1024,
                                       tail_keep_fraction=0.2):
    accepted = merge_result["accepted"]
    # Rank by end-to-end latency ascending, matching render.py's primary
    # sort mode (design doc sec.9.2's Canvas x-axis).
    accepted_sorted = sorted(
        accepted, key=lambda e: e["merged"]["e2e_ns"] if e["merged"]["e2e_ns"] is not None else Fraction(-1))

    n = len(accepted_sorted)
    raw_bytes_per_record = INTS_PER_RECORD * 4
    projected_raw_bytes = n and raw_bytes_per_record * n
    downsample_note = None
    selected = accepted_sorted

    def b64_size(count):
        raw = count * raw_bytes_per_record
        return (raw + 2) // 3 * 4  # base64 expansion, ceil(raw/3)*4

    if n > 0 and b64_size(n) > max_b64_bytes:
        # Keep the slow tail (highest e2e_ns -- what tail-latency analysis
        # is actually for) at full fidelity; downsample the head (the bulk
        # of ordinary, fast requests) by a uniform stride chosen to hit
        # the byte budget. "Uniform stride over the head" rather than
        # e.g. random sampling: reproducible given the same input, and
        # preserves the head's shape for the p50/p90 stats table.
        tail_count = max(1, int(n * tail_keep_fraction))
        head = accepted_sorted[:n - tail_count]
        tail = accepted_sorted[n - tail_count:]
        budget_records = max_b64_bytes * 3 // 4 // raw_bytes_per_record
        head_budget = max(0, budget_records - tail_count)
        if head_budget <= 0 or not head:
            stride = len(head) + 1 if head else 1
        else:
            stride = max(1, -(-len(head) // head_budget))  # ceil div
        head_kept = head[::stride]
        selected = head_kept + tail
        # Per-record weight: a head_kept row stands in for `stride`
        # originals (itself plus the stride-1 records the uniform stride
        # skipped over); a tail row stands for exactly 1 (the tail is kept
        # at full fidelity). Without this, render.py's stats table (mean,
        # P50/P90/P99...) has no way to tell it's looking at a
        # non-uniformly-sampled set, and silently treats the downsampled
        # rows as if they WERE the population -- systematically biased
        # toward the tail, since the tail is over-represented relative to
        # its true share (fix-round review's must-fix 1; see the
        # before/after numbers in the fix-round report). A record's
        # weight belongs in the IR, not something render.py has to
        # re-derive from `downsample.head_stride` plus positional
        # guessing about which records are "head" vs "tail".
        weight_by_id = {id(e): stride for e in head_kept}
        for e in tail:
            weight_by_id[id(e)] = 1
        downsample_note = {
            "applied": True,
            "reason": f"projected base64 size {b64_size(n)} bytes exceeds "
                      f"the {max_b64_bytes}-byte budget",
            "original_count": n,
            "kept_count": len(selected),
            "tail_kept_full": tail_count,
            "head_stride": stride,
            "head_kept": len(head_kept),
        }
    else:
        downsample_note = {"applied": False, "original_count": n, "kept_count": n}
        weight_by_id = {id(e): 1 for e in selected}

    blob = bytearray()
    records_meta = []
    for entry in selected:
        row, clipped_items = record_to_i32_row(entry)
        blob += struct.pack(f"<{INTS_PER_RECORD}i", *row)
        m = entry["merged"]
        crec, srec = entry["client_rec"], entry["server_rec"]
        method_id = crec["method_id"]
        method_name = client_dump["methods"].get(method_id) if method_id else None
        e2e_i32, e2e_clipped = frac_to_i32(m["e2e_ns"])
        if e2e_clipped and "e2e" not in clipped_items:
            clipped_items = clipped_items + ["e2e"]
        records_meta.append({
            "trace_id": f"{entry['trace_id']:#018x}",
            "attempt": crec["attempt"],
            "error_code": crec["error_code"],
            "method_id": method_id,
            "method_name": method_name,
            "req_size": crec["req_size"],
            "rsp_size": crec["rsp_size"],
            "socket_id_client": crec["socket_id"],
            "socket_id_server": srec["socket_id"],
            "remote_port_client": crec["remote_port"],
            "e2e_ns": e2e_i32,
            "negative_rtt": m["negative_rtt"],
            "any_negative_item": m.get("any_negative_item", False),
            "model_b_link_up_negative": m.get("model_b_link_up_negative", False),
            "link_total_negative": (m["link_total_ns"] is not None and m["link_total_ns"] < 0),
            # must-fix 1 (fix-round review): the weight this record's row
            # carries in any population statistic (mean, percentiles) --
            # see the comment above weight_by_id's construction.
            "weight": weight_by_id[id(entry)],
            # "also fix" items from the same review: which items (if any)
            # are a ts[]-saturation lower bound rather than an exact
            # reading, and which items (if any) had their ns value clamped
            # to fit int32 -- almost always the same items as saturated,
            # but not definitionally so (see frac_to_i32's docstring).
            "saturated_items": m.get("saturated_items", []),
            "clipped_items": clipped_items,
        })

    b64 = base64.b64encode(bytes(blob)).decode("ascii")

    # must-fix 3 (fix-round review): dropped_count is in every header this
    # parser already reads (parse_header/HEADER_FIELDS) but, until now,
    # was parsed and then never looked at again. The ring buffer stops
    # accepting new records once full and counts what it refused in this
    # field; if a run overflowed, join_rate_client/server above can read
    # "100%" while most of the traffic that actually happened was never
    # written to either dump at all -- 100% of what made it in, silently
    # not 100% of what ran. dropped_count == 0 is normal and expected
    # (the whole point of the ring buffer being large enough); this is
    # surfaced unconditionally so a reader isn't left trusting a join
    # rate that can't see what it doesn't have.
    client_dropped = client_dump.get("header", {}).get("dropped_count", 0)
    server_dropped = server_dump.get("header", {}).get("dropped_count", 0)

    accepted_n = len(accepted)
    # Computed over `accepted` specifically (not merge_result's
    # joined-level count, which also includes records excluded for other
    # reasons like an unstamped point) so the ratio below has a
    # consistent, well-defined denominator: "of the records this page
    # actually shows, how many have a negative segment."
    any_negative_item_count = sum(1 for e in accepted if e["merged"].get("any_negative_item"))

    item_group_sizes_31 = [
        len(DECOMPOSITION_ITEMS_CLIENT_SEND),
        len(DECOMPOSITION_ITEMS_SERVER),
        len(DECOMPOSITION_ITEMS_CLIENT_RECV),
    ]
    assert sum(item_group_sizes_31) == 31

    ir = {
        "format_version": 1,
        "point_names": [POINT_NAMES[p] for p in range(POINT_COUNT)],
        "item_names_31": [name for name, _, _, _ in DECOMPOSITION_ITEMS_31],
        "item_starred_31": [starred for _, _, _, starred in DECOMPOSITION_ITEMS_31],
        # "also fix" (fix-round review): render.py used to hardcode the
        # 7/16/8 client-send/server/client-recv split as JS constants
        # instead of reading it from the IR that actually defines the
        # ordering (DECOMPOSITION_ITEMS_31's three source lists, here).
        "item_group_sizes_31": item_group_sizes_31,
        "na_under_polling": sorted(NA_UNDER_POLLING),
        "ints_per_record": INTS_PER_RECORD,
        "na_sentinel_i32": NA_SENTINEL_I32,
        "record_layout": (
            [name for name, _, _, _ in DECOMPOSITION_ITEMS_31]
            + ["link_up_model_a", "link_down_model_a",
               "link_up_model_b", "link_down_model_b"]
        ),
        "downsample": downsample_note,
        "records_meta": records_meta,
        "items_blob_b64": b64,
        "stats": {
            "client_file": client_dump["path"],
            "server_file": server_dump["path"],
            "client_record_count": len(client_dump["records"]),
            "server_record_count": len(server_dump["records"]),
            "client_freq_hz": client_dump["freq_hz"],
            "server_freq_hz": server_dump["freq_hz"],
            "client_dropped_count": client_dropped,
            "server_dropped_count": server_dropped,
            "realtime_overlap_ok": merge_result.get("realtime_overlap_ok", True),
            "low_concurrency_mode": merge_result.get("low_concurrency", False),
            "join": {
                "client_ids": len(merge_result["client_ids"]),
                "server_ids": len(merge_result["server_ids"]),
                "joined": len(merge_result["joined_ids"]),
                "only_client": len(merge_result["only_client"]),
                "only_server": len(merge_result["only_server"]),
                # Reported "both directions", per capture-notes.md's
                # convention: joined / each side's own total id count.
                # These coincide (both 100%) only when the two sides'
                # trace_id sets are identical; either can differ from the
                # other when one side dropped records the other kept.
                # Neither denominator accounts for client_dropped_count /
                # server_dropped_count above -- a 100% rate here says
                # nothing about what the ring buffer refused before this
                # dump was ever written.
                "join_rate_client": (
                    len(merge_result["joined_ids"]) / len(merge_result["client_ids"])
                    if merge_result["client_ids"] else 0.0
                ),
                "join_rate_server": (
                    len(merge_result["joined_ids"]) / len(merge_result["server_ids"])
                    if merge_result["server_ids"] else 0.0
                ),
            },
            "reject_counts": dict(merge_result["reject_counts"]),
            "unstamped_point_histogram": dict(merge_result.get("unstamped_point_histogram", {})),
            "accepted_count": accepted_n,
            "output_record_count": len(selected),
            # must-fix 2 (fix-round review): whether/how many accepted,
            # joined records contain a negative decomposition item.
            # sec.10.1's own text makes the non-negativity assertion
            # conditional on low concurrency (outstanding=1); at real
            # capture concurrency this is expected batching noise
            # (sec.6.1) and, under model B, a genuine pollution detector
            # (sec.6.2) -- not, by itself, evidence of a defect. When
            # low_concurrency_mode is False (the default), these records
            # are NOT excluded from accepted_count/output_record_count;
            # this block reports how many there are instead of silently
            # dropping exactly the requests sec.9.3 was written to make
            # visible.
            "negative_items": {
                "count": any_negative_item_count,
                "ratio": (any_negative_item_count / accepted_n) if accepted_n else 0.0,
            },
        },
    }
    return ir, {
        "blob_raw_bytes": len(blob),
        "blob_b64_bytes": len(b64),
        "raw_bytes_per_record": raw_bytes_per_record,
        "b64_bytes_per_record": len(b64) / len(selected) if selected else 0.0,
    }


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--client", required=True, help="client-side dump file")
    ap.add_argument("--server", required=True, help="server-side dump file")
    ap.add_argument("-o", "--output", help="write the intermediate representation (JSON) here")
    ap.add_argument("--window-ms", type=float, default=1000.0,
                     help="model B sliding window size in ms (default 1000, per design doc sec.6.2)")
    ap.add_argument("--max-mb", type=float, default=12.0,
                     help="base64 blob size budget in MiB before downsampling kicks in (default 12)")
    ap.add_argument("--low-concurrency", action="store_true",
                     help="assert design doc sec.10.1's non-negative-decomposition-item check "
                          "(valid only at outstanding=1) and EXCLUDE any record that fails it "
                          "from the accepted/output set. Default off: at real capture "
                          "concurrency a negative item is expected batching noise (sec.6.1) "
                          "and, under model B, a pollution detector (sec.6.2) -- those records "
                          "are kept and counted/reported instead of silently dropped. Only pass "
                          "this for a capture you know was actually run at outstanding=1.")
    ap.add_argument("--stats-only", action="store_true",
                     help="print the report and exit without writing an IR file")
    args = ap.parse_args(argv)

    client_dump = parse_dump(args.client)
    server_dump = parse_dump(args.server)
    result = merge_dumps(client_dump, server_dump, window_ns=int(args.window_ms * 1e6),
                          low_concurrency=args.low_concurrency)

    ir, size_info = build_intermediate_representation(
        client_dump, server_dump, result,
        max_b64_bytes=int(args.max_mb * 1024 * 1024))

    stats = ir["stats"]
    j = stats["join"]
    print(f"client records: {stats['client_record_count']} "
          f"(freq={stats['client_freq_hz']:.3f} Hz)")
    print(f"server records: {stats['server_record_count']} "
          f"(freq={stats['server_freq_hz']:.3f} Hz)")
    if stats["client_dropped_count"] or stats["server_dropped_count"]:
        print(f"WARNING: ring buffer overflow -- client dropped_count="
              f"{stats['client_dropped_count']}, server dropped_count="
              f"{stats['server_dropped_count']} (records the runtime refused "
              f"because the buffer was full; NOT reflected in the join rate "
              f"below, which only ever sees what made it into the dump)",
              file=sys.stderr)
    if not stats["realtime_overlap_ok"]:
        print("WARNING: client/server realtime calibration windows do not "
              "overlap -- see the earlier warning for the exact windows; "
              "these two dumps may not be from the same capture run",
              file=sys.stderr)
    print(f"join: {j['joined']} joined; client ids={j['client_ids']} "
          f"(rate={j['join_rate_client']*100:.4f}%), "
          f"server ids={j['server_ids']} (rate={j['join_rate_server']*100:.4f}%), "
          f"only_client={j['only_client']}, only_server={j['only_server']}")
    print("rejects (excluded from statistics, counted by reason -- a record "
          "may appear under more than one reason):")
    for reason, count in stats["reject_counts"].items():
        print(f"  {reason}: {count}")
    if stats["unstamped_point_histogram"]:
        print("  unstamped_point breakdown (which point(s) were zero -- streaming "
              "responses' S15-S17, a cancelled backup's receive side, and an "
              "un-instrumented code path all write plain 0, so this cannot say "
              "which):")
        for name, count in sorted(stats["unstamped_point_histogram"].items(),
                                   key=lambda kv: -kv[1]):
            print(f"    {name}: {count}")
    print(f"accepted: {stats['accepted_count']}")
    if not stats["low_concurrency_mode"]:
        ni = stats["negative_items"]
        print(f"negative-item records (NOT excluded -- see --low-concurrency): "
              f"{ni['count']} of {stats['accepted_count']} accepted "
              f"({ni['ratio']*100:.4f}%)")
    print(f"output records after downsampling: {stats['output_record_count']}")
    if ir["downsample"]["applied"]:
        print(f"downsampling APPLIED: {ir['downsample']}")
    print(f"IR blob: {size_info['blob_raw_bytes']} raw bytes, "
          f"{size_info['blob_b64_bytes']} base64 bytes "
          f"({size_info['b64_bytes_per_record']:.2f} b64 bytes/record)")

    # Identity check (sec.9.1/10.1): assert for every ACCEPTED record.
    # Zero tolerance, exact Fraction comparison -- see merge_one()'s
    # docstring for why this is exact rather than float-approximate.
    bad_identity = []
    for entry in result["accepted"]:
        m = entry["merged"]
        if m["identity_ok"] is False:
            bad_identity.append(entry)
    if bad_identity:
        print(f"IDENTITY CHECK FAILED for {len(bad_identity)} accepted "
              f"record(s) -- Sigma(33 items) != C17-C01:", file=sys.stderr)
        for entry in bad_identity[:5]:
            m = entry["merged"]
            print(f"  trace_id={entry['trace_id']:#x} lhs={m['identity_lhs']} "
                  f"rhs={m['identity_rhs']}", file=sys.stderr)
        sys.exit(1)
    else:
        print(f"identity check: {len(result['accepted'])}/"
              f"{len(result['accepted'])} accepted records pass "
              f"Sigma(33 items) == C17-C01 exactly")

    if not args.stats_only and args.output:
        with open(args.output, "w") as f:
            json.dump(ir, f)
        print(f"wrote {args.output}")
    elif not args.stats_only:
        print("(no --output given; IR not written to disk)")

    return 0


if __name__ == "__main__":
    sys.exit(main())
