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
"""tools/latency_trace/test_merge.py

Tests for merge.py. Python 3 standard library only (unittest) -- no
third-party test framework, matching the project-wide constraint that
these tools must run anywhere without extra dependencies.

The primary case (TestRealSample) parses the committed real capture at
tools/latency_trace/testdata/{client,server}.dump -- produced by
tools/latency_trace/capture.sh against a real traced echo_c++ server and
client (see docs/superpowers/plans/capture-notes.md's "Run 3" section for
how and why these exact files are pinned) -- and asserts against the
numbers recorded there. Everything else exercises edges no single real
capture is likely to produce on demand: truncation, an empty file, a
zero method_table_offset, N/A/saturated sentinels, and one synthetic
record engineered to fail each of sec.10.1's four assertions.
"""
import os
import struct
import unittest
from fractions import Fraction

import merge

THIS_DIR = os.path.dirname(os.path.abspath(__file__))
TESTDATA_DIR = os.path.join(THIS_DIR, "testdata")
CLIENT_DUMP = os.path.join(TESTDATA_DIR, "client.dump")
SERVER_DUMP = os.path.join(TESTDATA_DIR, "server.dump")


# ---------------------------------------------------------------------------
# Synthetic fixture builder -- deliberately reuses merge.py's own format
# constants (HEADER_FMT/RECORD_FMT/HEADER_SIZE/RECORD_SIZE) rather than
# hand-rolling a second copy of the byte layout. That's intentional, not
# laziness: the on-disk FORMAT itself (are the offsets and field order
# right?) is what TestRealSample verifies, against bytes this module did
# not produce -- the C++ runtime did. These synthetic fixtures exist to
# drive merge.py's LOGIC (join, the four assertions, sentinel handling,
# truncation handling) down specific paths that one real capture won't
# reliably hit, and for that purpose building them through the same
# struct formats merge.py parses with is the correct, not circular, choice.
# ---------------------------------------------------------------------------

def make_header_bytes(record_count, method_table_offset,
                       head_counter=1_000_000, delta_counter=100_000_000,
                       head_monotonic_ns=0, delta_monotonic_ns=1_000_000_000,
                       dropped_count=0, process_tag=0xABCD1234):
    """100M ticks over 1 real second => counter_freq_hz == 1e8, a clean
    round number that makes hand-checking ns conversions in these tests
    easy (1 tick == 10ns exactly)."""
    head = dict(
        magic=0x4252504C54524331,
        record_size=merge.RECORD_SIZE,
        point_count=merge.POINT_COUNT,
        head_counter=head_counter,
        head_monotonic_ns=head_monotonic_ns,
        tail_counter=head_counter + delta_counter,
        tail_monotonic_ns=head_monotonic_ns + delta_monotonic_ns,
        head_realtime_ns=1_700_000_000_000_000_000,
        tail_realtime_ns=1_700_000_000_000_000_000 + delta_monotonic_ns,
        counter_freq_hz=delta_counter * 1e9 / delta_monotonic_ns,
        cntfrq_el0_hz=0,  # non-aarch64 test host: 0 is legal, no cross-check
        record_count=record_count,
        dropped_count=dropped_count,
        process_tag=process_tag,
        method_table_offset=method_table_offset,
    )
    packed = struct.pack(merge.HEADER_FMT, *(head[k] for k in merge.HEADER_FIELDS))
    return packed + b"\x00" * (merge.HEADER_SIZE - len(packed))


def make_record_bytes(trace_id, role, ts=None, base_counter=0, slot_seq=1,
                       socket_id=1, remote_ip=0x0100007F, req_size=13,
                       rsp_size=13, method_id=1, error_code=0,
                       remote_port=9541, attempt=0):
    """`ts` is a dict of {point_index: raw_ts_value}; every point not
    given defaults to 0 (unstamped), matching a real record's own-role
    range default and mirroring how a client dump's server-range slots
    (and vice versa) read as all-zero by construction."""
    ts_arr = [0] * merge.POINT_COUNT
    if ts:
        for p, v in ts.items():
            ts_arr[p] = v
    vals = [trace_id, base_counter, slot_seq] + ts_arr + [
        socket_id, remote_ip, req_size, rsp_size, method_id, error_code,
        remote_port, role, attempt,
    ]
    packed = struct.pack(merge.RECORD_FMT, *vals)
    assert len(packed) == merge.RECORD_SIZE
    return packed


def make_method_table_bytes(names):
    out = struct.pack("<I", len(names))
    for name in names:
        b = name.encode("utf-8")
        out += struct.pack("<I", len(b)) + b
    return out


def make_dump(records_bytes_list, method_names=("test.Service.Method",),
              **header_kwargs):
    body = b"".join(records_bytes_list)
    method_table_offset = merge.HEADER_SIZE + len(body)
    header = make_header_bytes(len(records_bytes_list), method_table_offset,
                                **header_kwargs)
    table = make_method_table_bytes(method_names)
    return header + body + table


def sequential_client_ts(start_offset_raw=1, step=10, wake_gap=10_000):
    """A fully-stamped, strictly increasing client ts[] dict covering all
    17 client points, offset+1-encoded (see decode_ts). Points advance by
    `step` raw ticks each, EXCEPT the C08 (write_end) -> C09 (wake) gap,
    which uses `wake_gap` instead: RTT (= C09 - C08, design doc sec.6) is
    a single one-point gap on the client's own timeline, while the
    server's total span (S01 -> S17) spans 16 gaps -- with a uniform
    `step` on both sides the server span is inevitably >16x the RTT,
    driving link_total = RTT - server_span negative by construction, not
    by anything under test. `wake_gap`'s generous default (10_000 raw
    ticks = 100us at this module's 10ns/tick synthetic clock) keeps RTT
    comfortably larger than any default-sized server span, so a "clean"
    record built from this helper's defaults is actually clean -- see
    test_negative_decomposition_item_via_small_negative_link_total for
    the test that deliberately narrows this gap back down."""
    ts = {}
    offset = start_offset_raw
    prev_point = None
    for p in merge.CLIENT_POINTS:
        if prev_point == merge.C_WRITE_END and p == merge.C_WAKE:
            offset = ts[merge.C_WRITE_END] + wake_gap
        ts[p] = offset
        prev_point = p
        offset += step
    return ts


def sequential_server_ts(start_offset_raw=1, step=10):
    return {p: start_offset_raw + i * step for i, p in enumerate(merge.SERVER_POINTS)}


def write_temp(tmp_path, data):
    with open(tmp_path, "wb") as f:
        f.write(data)
    return tmp_path


# ---------------------------------------------------------------------------
# Primary case: the real capture
# ---------------------------------------------------------------------------

class TestRealSample(unittest.TestCase):
    """Parses the committed real dump pair and asserts against the exact
    numbers recorded in docs/superpowers/plans/capture-notes.md's "Run 3"
    section. If these ever drift, capture-notes.md is out of date or
    merge.py's behavior changed -- both are worth knowing about, so these
    assertions are exact, not "close enough"."""

    @classmethod
    def setUpClass(cls):
        if not (os.path.exists(CLIENT_DUMP) and os.path.exists(SERVER_DUMP)):
            raise unittest.SkipTest(
                f"real capture fixtures not found at {TESTDATA_DIR} -- "
                f"regenerate with tools/latency_trace/capture.sh and commit "
                f"them, or see capture-notes.md")
        cls.client_dump = merge.parse_dump(CLIENT_DUMP)
        cls.server_dump = merge.parse_dump(SERVER_DUMP)
        # capture.sh captured this exact fixture at outstanding=1 (see its
        # own comment: "the only condition sec.10.1's end-to-end row calls
        # 'low concurrency'"), so asserting the non-negative-decomposition-
        # item check here is actually warranted, not just convenient for
        # keeping the numbers below pinned -- see merge_one()'s
        # `low_concurrency` docstring for why this defaults to off
        # elsewhere (fix-round review's must-fix 2).
        cls.result = merge.merge_dumps(cls.client_dump, cls.server_dump, low_concurrency=True)
        # And the default-off behavior, on the same fixture, for the tests
        # further down that specifically exercise it.
        cls.result_high_concurrency = merge.merge_dumps(cls.client_dump, cls.server_dump)

    def test_header_fields(self):
        ch, sh = self.client_dump["header"], self.server_dump["header"]
        self.assertEqual(ch["record_count"], 3222)
        self.assertEqual(sh["record_count"], 3222)
        self.assertEqual(ch["record_size"], 192)
        self.assertEqual(ch["point_count"], 34)
        self.assertEqual(ch["dropped_count"], 0)
        self.assertEqual(sh["dropped_count"], 0)
        self.assertEqual(ch["method_table_offset"], 618752)
        self.assertEqual(sh["method_table_offset"], 618752)
        self.assertEqual(self.client_dump["file_size"], 618784)
        self.assertEqual(self.server_dump["file_size"], 618784)

    def test_frequency_is_close_to_100mhz_and_cross_checks_agree(self):
        # Empirically ~100.0000 MHz on this aarch64 host (capture-notes.md).
        self.assertAlmostEqual(self.client_dump["freq_hz"], 1e8, delta=1e5)
        self.assertAlmostEqual(self.server_dump["freq_hz"], 1e8, delta=1e5)
        # CNTFRQ_EL0 cross-check: both sides recorded exactly 100MHz.
        self.assertEqual(self.client_dump["header"]["cntfrq_el0_hz"], 100_000_000)
        self.assertEqual(self.server_dump["header"]["cntfrq_el0_hz"], 100_000_000)

    def test_method_table(self):
        self.assertEqual(self.client_dump["methods"], {1: "example.EchoService.Echo"})
        self.assertEqual(self.server_dump["methods"], {1: "example.EchoService.Echo"})

    def test_join_is_100_percent(self):
        r = self.result
        self.assertEqual(len(r["client_ids"]), 3222)
        self.assertEqual(len(r["server_ids"]), 3222)
        self.assertEqual(len(r["joined_ids"]), 3222)
        self.assertEqual(len(r["only_client"]), 0)
        self.assertEqual(len(r["only_server"]), 0)

    def test_reject_counts_match_capture_notes(self):
        self.assertEqual(self.result["reject_counts"], {
            merge.REJECT_UNSTAMPED: 0,
            merge.REJECT_NON_MONOTONIC: 0,
            merge.REJECT_NEGATIVE_ITEM: 5,
            merge.REJECT_NEGATIVE_RTT_LATE_WRITE_STAMP: 0,
        })
        self.assertEqual(len(self.result["accepted"]), 3217)

    def test_default_high_concurrency_mode_keeps_the_5_negative_item_records(self):
        # fix-round review's must-fix 2: without --low-concurrency, the
        # same 5 records that test_reject_counts_match_capture_notes shows
        # get excluded under low_concurrency=True must instead survive
        # into `accepted` (so the renderer's hatch/red-tick/hide-negative
        # machinery has something real to draw from a plain dump run, not
        # only from a hand-edited page) -- while still being flagged and
        # counted, not silently indistinguishable from a clean record.
        r = self.result_high_concurrency
        self.assertEqual(r["reject_counts"][merge.REJECT_NEGATIVE_ITEM], 0)
        self.assertEqual(len(r["accepted"]), 3222)
        flagged = [e for e in r["accepted"] if e["merged"]["any_negative_item"]]
        self.assertEqual(len(flagged), 5)
        ir, _ = merge.build_intermediate_representation(self.client_dump, self.server_dump, r)
        self.assertEqual(ir["stats"]["negative_items"]["count"], 5)
        self.assertAlmostEqual(ir["stats"]["negative_items"]["ratio"], 5 / 3222)
        self.assertFalse(ir["stats"]["low_concurrency_mode"])
        # And every one of those 5 records' negative segment is actually
        # present in the emitted int32 blob (not clamped to 0 or dropped),
        # which is what makes the renderer's hatch path reachable at all.
        import struct as _struct
        import base64 as _base64
        blob = _base64.b64decode(ir["items_blob_b64"])
        ints_per_record = ir["ints_per_record"]
        flagged_trace_ids = {f"{e['trace_id']:#018x}" for e in flagged}
        found_negative_in_blob = 0
        for i, meta in enumerate(ir["records_meta"]):
            if meta["trace_id"] not in flagged_trace_ids:
                continue
            row = _struct.unpack_from(f"<{ints_per_record}i", blob, i * ints_per_record * 4)
            if any(v < 0 for v in row if v != ir["na_sentinel_i32"]):
                found_negative_in_blob += 1
        self.assertEqual(found_negative_in_blob, 5)

    def test_sum_identity_holds_exactly_for_every_accepted_record(self):
        # Zero tolerance, per design doc sec.9.1 -- see merge_one()'s
        # docstring for why Fraction arithmetic makes this exact rather
        # than float-approximate.
        checked = 0
        for entry in self.result["accepted"]:
            m = entry["merged"]
            self.assertIsNotNone(m["identity_ok"], msg=f"trace_id={entry['trace_id']:#x}")
            self.assertTrue(m["identity_ok"],
                             msg=f"trace_id={entry['trace_id']:#x} lhs={m['identity_lhs']} "
                                 f"rhs={m['identity_rhs']}")
            checked += 1
        self.assertEqual(checked, 3217)

    def test_negative_items_are_small_link_total_noise_not_write_stamp_anomaly(self):
        # The 5 rejects are specifically link_total < 0 (sub-6-microsecond,
        # design doc sec.6.1's expected phenomenon), not the C09<C08
        # write-stamp anomaly (that bucket is 0 -- see
        # test_reject_counts_match_capture_notes). Pin the magnitude so a
        # regression that produces wildly-negative items doesn't hide
        # behind "well, negative items are expected."
        offenders = [entry for entry in self.result["joined"]
                     if merge.REJECT_NEGATIVE_ITEM in entry["merged"]["reasons"]]
        self.assertEqual(len(offenders), 5)
        for entry in offenders:
            m = entry["merged"]
            self.assertIsNotNone(m["link_total_ns"])
            self.assertLess(m["link_total_ns"], 0)
            self.assertGreater(m["link_total_ns"], -10_000)  # > -10us
            self.assertNotIn(merge.REJECT_NEGATIVE_RTT_LATE_WRITE_STAMP, m["reasons"])

    def test_model_b_offsets_do_not_anchor_on_a_rejected_record(self):
        # Regression test for the bug this task's own run caught: the
        # model-B sliding-window anchor (argmin link_total) must be
        # chosen only from records that already passed all four sec.10.1
        # assertions. Before the fix, the anchor was one of the 5
        # negative_decomposition_item rejects (the global-minimum
        # link_total belongs to one of them), and that propagated a
        # systematic bias making ~99.9% of accepted records' model-B
        # link_up negative. After the fix it should be a rare, small
        # fraction (real per-record jitter, not a systematic skew).
        # Check the systemic-bias symptom directly rather than internals:
        neg_b = sum(1 for e in self.result["accepted"]
                    if e["merged"].get("link_up_b_ns") is not None
                    and e["merged"]["link_up_b_ns"] < 0)
        self.assertLess(neg_b / len(self.result["accepted"]), 0.05,
                         msg=f"{neg_b}/{len(self.result['accepted'])} accepted records "
                             f"have negative model-B link_up -- looks like the anchor-"
                             f"selection regression is back")

    def test_intermediate_representation_builds_and_size_matches_measurement(self):
        ir, size_info = merge.build_intermediate_representation(
            self.client_dump, self.server_dump, self.result)
        self.assertEqual(ir["stats"]["accepted_count"], 3217)
        self.assertEqual(ir["stats"]["output_record_count"], 3217)
        self.assertFalse(ir["downsample"]["applied"])
        self.assertEqual(len(ir["item_names_31"]), 31)
        self.assertEqual(len(ir["record_layout"]), 35)
        self.assertEqual(size_info["raw_bytes_per_record"], 140)
        # 3217 records * 140 bytes = 450380 raw; base64 inflates by 4/3.
        self.assertEqual(size_info["blob_raw_bytes"], 3217 * 140)
        projected_100k_b64_mb = size_info["b64_bytes_per_record"] * 100_000 / (1024 * 1024)
        # Recorded in the task report -- pin the exact real measurement so
        # a change in the item count or record layout is caught here
        # rather than only discovered when someone else does the
        # extrapolation by hand.
        self.assertGreater(projected_100k_b64_mb, 12.0,
                            msg="projection dropped below the 12MB budget -- "
                                "re-check the task report's downsampling "
                                "recommendation, it may no longer be needed")


# ---------------------------------------------------------------------------
# decode_ts(): the three sentinels plus the ordinary case
# ---------------------------------------------------------------------------

class TestDecodeTs(unittest.TestCase):
    def test_unstamped(self):
        self.assertEqual(merge.decode_ts(0), ("unstamped", None))

    def test_not_applicable_not_decremented(self):
        self.assertEqual(merge.decode_ts(0xFFFFFFFF), ("na", None))

    def test_saturated_is_not_decremented(self):
        # latency_trace.h is explicit: 0xFFFFFFFE "MUST NOT be
        # decremented" the way an ordinary value is. This used to assert
        # val == 0xFFFFFFFD (raw - 1) -- the wrong behavior, locked in by
        # a test whose own name called it "not decremented" while its
        # assertion checked the decremented value. offset must be raw
        # itself.
        kind, val = merge.decode_ts(0xFFFFFFFE)
        self.assertEqual(kind, "saturated")
        self.assertEqual(val, 0xFFFFFFFE)

    def test_ordinary_value_decrements(self):
        self.assertEqual(merge.decode_ts(1), ("ok", 0))
        self.assertEqual(merge.decode_ts(12345), ("ok", 12344))

    def test_sentinels_are_distinguishable_from_a_huge_real_offset(self):
        # 0xFFFFFFFD (one less than saturated) must decode as an ordinary
        # offset, not accidentally collide with either sentinel.
        kind, val = merge.decode_ts(0xFFFFFFFD)
        self.assertEqual(kind, "ok")
        self.assertEqual(val, 0xFFFFFFFC)


# ---------------------------------------------------------------------------
# File-format edges: truncation, empty file, magic mismatch, zero method
# table offset.
# ---------------------------------------------------------------------------

class TestFileFormatEdges(unittest.TestCase):
    def setUp(self):
        self.tmpfiles = []

    def tearDown(self):
        for p in self.tmpfiles:
            try:
                os.remove(p)
            except OSError:
                pass

    def _tmp(self, name, data):
        path = os.path.join(THIS_DIR, f"_test_tmp_{name}_{os.getpid()}.dump")
        write_temp(path, data)
        self.tmpfiles.append(path)
        return path

    def test_empty_file_raises(self):
        path = self._tmp("empty", b"")
        with self.assertRaises(merge.LatencyTraceFormatError):
            merge.parse_dump(path)

    def test_header_only_no_records_is_legal(self):
        # record_count == 0, method_table_offset points right after the
        # header (an empty table) -- a legal, if useless, file.
        data = make_dump([], method_names=())
        path = self._tmp("headeronly", data)
        d = merge.parse_dump(path)
        self.assertEqual(d["header"]["record_count"], 0)
        self.assertEqual(d["records"], [])
        self.assertEqual(d["methods"], {})

    def test_bad_magic_raises_and_does_not_attempt_records(self):
        data = bytearray(make_dump([make_record_bytes(1, merge.LT_ROLE_CLIENT,
                                                        sequential_client_ts())]))
        data[0:8] = b"GARBAGE!"
        path = self._tmp("badmagic", bytes(data))
        with self.assertRaises(merge.LatencyTraceFormatError) as cm:
            merge.parse_dump(path)
        self.assertIn("magic", str(cm.exception).lower())

    def test_truncated_mid_record_raises(self):
        rec = make_record_bytes(1, merge.LT_ROLE_CLIENT, sequential_client_ts())
        full = make_dump([rec, rec, rec])
        # Cut off partway through the 2nd record (header + 1.5 records).
        cut_at = merge.HEADER_SIZE + merge.RECORD_SIZE + merge.RECORD_SIZE // 2
        path = self._tmp("truncated", full[:cut_at])
        with self.assertRaises(merge.LatencyTraceFormatError) as cm:
            merge.parse_dump(path)
        self.assertIn("truncated", str(cm.exception).lower())

    def test_truncated_below_header_size_raises(self):
        path = self._tmp("shortheader", b"\x00" * 64)
        with self.assertRaises(merge.LatencyTraceFormatError):
            merge.parse_dump(path)

    def test_method_table_offset_zero_yields_empty_table_not_crash(self):
        rec = make_record_bytes(1, merge.LT_ROLE_CLIENT, sequential_client_ts())
        header = make_header_bytes(1, method_table_offset=0)
        data = header + rec  # no method table bytes at all
        path = self._tmp("mtzero", data)
        d = merge.parse_dump(path)
        self.assertEqual(d["methods"], {})
        self.assertEqual(len(d["records"]), 1)

    def test_wrong_record_size_in_header_raises(self):
        header = make_header_bytes(0, method_table_offset=merge.HEADER_SIZE)
        # Corrupt record_size field (offset 8, uint32) in place.
        header = bytearray(header)
        struct.pack_into("<I", header, 8, 64)  # claims 64, not 192
        path = self._tmp("wrongrecsize", bytes(header))
        with self.assertRaises(merge.LatencyTraceFormatError):
            merge.parse_dump(path)


# ---------------------------------------------------------------------------
# The four sec.10.1 assertions, and N/A-sentinel handling, via synthetic
# joined pairs (single client + single server record each).
# ---------------------------------------------------------------------------

class TestAssertionsAndSentinels(unittest.TestCase):
    def _joined_pair(self, client_ts, server_ts, trace_id=42, base_counter=0,
                      low_concurrency=False):
        crec_bytes = make_record_bytes(trace_id, merge.LT_ROLE_CLIENT, client_ts,
                                        base_counter=base_counter)
        srec_bytes = make_record_bytes(trace_id, merge.LT_ROLE_SERVER, server_ts,
                                        base_counter=base_counter)
        client_data = make_dump([crec_bytes])
        server_data = make_dump([srec_bytes])
        cpath = self._tmp("c", client_data)
        spath = self._tmp("s", server_data)
        cdump = merge.parse_dump(cpath)
        sdump = merge.parse_dump(spath)
        result = merge.merge_dumps(cdump, sdump, low_concurrency=low_concurrency)
        self.assertEqual(len(result["joined"]), 1)
        return result["joined"][0]

    def setUp(self):
        self.tmpfiles = []

    def tearDown(self):
        for p in self.tmpfiles:
            try:
                os.remove(p)
            except OSError:
                pass

    def _tmp(self, name, data):
        path = os.path.join(THIS_DIR, f"_test_tmp_{name}_{id(self)}_{os.getpid()}.dump")
        write_temp(path, data)
        self.tmpfiles.append(path)
        return path

    def test_clean_record_is_accepted(self):
        j = self._joined_pair(sequential_client_ts(), sequential_server_ts())
        self.assertEqual(j["merged"]["reasons"], set())

    def test_unstamped_point_is_rejected_and_does_not_crash_monotonic_check(self):
        ts = sequential_client_ts()
        del ts[merge.C_REQ_META_SER_END]  # C05 left at the dict-default of 0 => unstamped
        j = self._joined_pair(ts, sequential_server_ts())
        self.assertIn(merge.REJECT_UNSTAMPED, j["merged"]["reasons"])

    def test_non_monotonic_point_is_rejected(self):
        ts = sequential_client_ts()
        # Swap C03 and C02's offsets so C03 < C02 (still both "stamped").
        ts[merge.C_REQ_PAYLOAD_SER_START], ts[merge.C_REQ_PAYLOAD_SER_END] = (
            ts[merge.C_REQ_PAYLOAD_SER_END], ts[merge.C_REQ_PAYLOAD_SER_START])
        j = self._joined_pair(ts, sequential_server_ts())
        self.assertIn(merge.REJECT_NON_MONOTONIC, j["merged"]["reasons"])

    def test_negative_decomposition_item_via_small_negative_link_total(self):
        # Both sides individually monotonic and fully stamped (so neither
        # the points-nonzero nor the monotonic assertion fires) but the
        # server's total span (S01->S17, 16 gaps) exceeds the client's RTT
        # (C09-C08, a single gap deliberately narrowed via wake_gap) --
        # matching the real capture's 5 rejects (capture-notes.md's Run 3):
        # a small, purely-arithmetic negative link_total, not a stamp-order
        # defect on either individual side.
        client_ts = sequential_client_ts(step=100, wake_gap=200)  # RTT = 200 ticks = 2000ns
        server_ts_wide = sequential_server_ts(step=2000)  # span = 16*2000 = 32000 ticks

        # Default (high concurrency, fix-round review's must-fix 2): the
        # assertion is computed (any_negative_item) but NOT enforced as a
        # reject reason -- the record stays accepted.
        j_default = self._joined_pair(client_ts, server_ts_wide)
        m_default = j_default["merged"]
        self.assertIsNotNone(m_default["link_total_ns"])
        self.assertLess(m_default["link_total_ns"], 0)
        self.assertTrue(m_default["any_negative_item"])
        self.assertNotIn(merge.REJECT_NEGATIVE_ITEM, m_default["reasons"])
        self.assertEqual(m_default["reasons"], set())

        # --low-concurrency (or a caller that knows this capture really was
        # outstanding=1): same record, now excluded via REJECT_NEGATIVE_ITEM.
        j = self._joined_pair(client_ts, server_ts_wide, low_concurrency=True)
        m = j["merged"]
        self.assertIsNotNone(m["link_total_ns"])
        self.assertLess(m["link_total_ns"], 0)
        self.assertIn(merge.REJECT_NEGATIVE_ITEM, m["reasons"])
        # And this should NOT also be flagged non_monotonic or unstamped --
        # it's a clean, individually-monotonic, fully-stamped record whose
        # cross-host link arithmetic alone goes negative.
        self.assertNotIn(merge.REJECT_NON_MONOTONIC, m["reasons"])
        self.assertNotIn(merge.REJECT_UNSTAMPED, m["reasons"])

    def test_negative_rtt_write_stamp_anomaly(self):
        # C09 (wake, index 8) stamped BEFORE C08 (write_end, index 7) --
        # the exact signature capture-notes.md's Run 1 anomaly and design
        # doc sec.10.1 describe. This necessarily also violates the
        # client's own monotonicity (C08/C09 are adjacent enum indices),
        # so both reasons fire together -- that is the documented,
        # expected relationship, not a test bug (see the module docstring
        # discussion in the task report).
        ts = sequential_client_ts(step=100)
        ts[merge.C_WAKE] = ts[merge.C_WRITE_START]  # C09 way earlier than C08
        j = self._joined_pair(ts, sequential_server_ts())
        m = j["merged"]
        self.assertIn(merge.REJECT_NEGATIVE_RTT_LATE_WRITE_STAMP, m["reasons"])
        self.assertIn(merge.REJECT_NON_MONOTONIC, m["reasons"])
        self.assertTrue(m["negative_rtt"])

    def test_na_sentinel_on_wake_and_onedge_marks_dependent_items_na_not_zero(self):
        # Simulate an RDMA-polling-mode client: wake (C09) and onedge_start
        # (C10) do not exist (design doc sec.8.5), encoded as the
        # LT_TS_NOT_APPLICABLE sentinel rather than 0.
        ts = sequential_client_ts(step=1000)
        ts[merge.C_WAKE] = merge.TS_NOT_APPLICABLE
        ts[merge.C_ONEDGE_START] = merge.TS_NOT_APPLICABLE
        j = self._joined_pair(ts, sequential_server_ts(step=1000))
        m = j["merged"]
        # Not rejected for "unstamped" merely because of the N/A points --
        # N/A is a different, legitimate state (see check_points_nonzero).
        self.assertNotIn(merge.REJECT_UNSTAMPED, m["reasons"])
        self.assertIsNone(m["items_ns"]["cli_wake_to_onedge"])
        self.assertIsNone(m["items_ns"]["cli_onedge_to_readv"])
        # The other 29 items are unaffected -- still real numbers.
        for name, val in m["items_ns"].items():
            if name in ("cli_wake_to_onedge", "cli_onedge_to_readv"):
                continue
            self.assertIsNotNone(val, msg=name)
        # fix-round review's RDMA-polling fix: wake (C09) being N/A used
        # to mean RTT/link_total/the sum identity could not be computed
        # AT ALL for this record -- link_total_ns and identity_ok were
        # both None, which meant BOTH link items went N/A and every bar
        # under-reported by the whole network time, silently, while the
        # page still claimed the bar top was always exactly the
        # end-to-end latency. Design doc sec.8.5 is explicit that this is
        # fixable, not a fundamental gap: under polling, wake,
        # onedge_start and readv_start are literally the same instant
        # (those two intervals are genuinely zero), so RTT falls back to
        # C11 (readv_start) - C08 -- still a real timestamp under polling
        # -- and the identity keeps holding exactly, because
        # cli_wake_to_onedge/cli_onedge_to_readv are still correctly
        # counted as 0 (via being N/A) in the item sum. Confirmed exactly,
        # not just "not None", so a regression that produces some other
        # value doesn't slip through as "good enough":
        self.assertEqual(m["rtt_ns"], m["c_ns"][merge.C_READV_START] - m["c_ns"][merge.C_WRITE_END])
        self.assertIsNotNone(m["link_total_ns"])
        self.assertIsNotNone(m["identity_ok"])
        self.assertTrue(m["identity_ok"],
                         msg=f"lhs vs rhs mismatch: {m['identity_lhs']} != {m['identity_rhs']}")

    def test_na_sentinel_on_both_sides_does_not_crash_model_b(self):
        # Both client AND server running RDMA polling mode (design doc
        # sec.8.5 -- each side's flag is independent). Regression guard:
        # apply_model_b()/estimate_offsets_model_b() used to read
        # m["s_ns"][S_WAKE] / m["c_ns"][C_WAKE] directly, which are None
        # under polling (the real point is N/A) -- `b + O` would raise
        # TypeError the moment link_total_ns stopped being unconditionally
        # None for a polling record, which the RTT/S fallback above makes
        # it. The fix routes model B through the same c09_anchor_ns/
        # s01_anchor_ns substitutes merge_one() already computes for
        # RTT/S, so this must not crash and must produce a real split.
        ts_c = sequential_client_ts(step=1000)
        ts_c[merge.C_WAKE] = merge.TS_NOT_APPLICABLE
        ts_c[merge.C_ONEDGE_START] = merge.TS_NOT_APPLICABLE
        ts_s = sequential_server_ts(step=1000)
        ts_s[merge.S_WAKE] = merge.TS_NOT_APPLICABLE
        ts_s[merge.S_ONEDGE_START] = merge.TS_NOT_APPLICABLE
        j = self._joined_pair(ts_c, ts_s)
        m = j["merged"]
        self.assertIsNotNone(m["link_total_ns"])
        self.assertTrue(m["identity_ok"])
        self.assertIsNotNone(m["link_up_b_ns"])
        self.assertIsNotNone(m["link_down_b_ns"])
        self.assertEqual(m["link_up_b_ns"] + m["link_down_b_ns"], m["link_total_ns"])

    def test_saturated_sentinel_does_not_crash_and_is_marked_lower_bound(self):
        ts = sequential_client_ts(step=1000)
        ts[merge.C_RPC_END] = merge.TS_SATURATED  # interval to C17 >= ~42.9s
        j = self._joined_pair(ts, sequential_server_ts(step=1000))
        m = j["merged"]
        # A saturated point still participates as its lower-bound value
        # (raw - 1) -- not treated as unstamped, not treated as N/A.
        self.assertNotIn(merge.REJECT_UNSTAMPED, m["reasons"])
        self.assertIsNotNone(m["items_ns"]["cli_post_deser"])
        # The lower-bound offset is huge (0xFFFFFFFD ticks), so this
        # item's value is enormous compared to the other, real ~us-scale
        # items -- exactly why sec.8.1 says to treat it as a floor, not a
        # true reading.
        self.assertGreater(m["items_ns"]["cli_post_deser"], 10**6)


# ---------------------------------------------------------------------------
# Downsampling budget logic (design doc / plan B4).
# ---------------------------------------------------------------------------

class TestDownsampling(unittest.TestCase):
    def _make_synthetic_accepted_set(self, n):
        """A minimal fake 'accepted' list good enough for
        build_intermediate_representation(): only needs entries whose
        ["merged"] dict has the keys record_to_i32_row()/the meta builder
        read, plus e2e_ns for the sort key."""
        entries = []
        for i in range(n):
            items_ns = {name: Fraction(i) for name, _, _, _ in merge.DECOMPOSITION_ITEMS_31}
            m = {
                "items_ns": items_ns,
                "link_up_a_ns": Fraction(1), "link_down_a_ns": Fraction(1),
                "link_up_b_ns": Fraction(1), "link_down_b_ns": Fraction(1),
                "e2e_ns": Fraction(i),
                "negative_rtt": False,
                "link_total_ns": Fraction(2),
            }
            entries.append({
                "trace_id": i,
                "merged": m,
                "client_rec": {"attempt": 0, "error_code": 0, "method_id": 1,
                                "req_size": 1, "rsp_size": 1, "socket_id": 1,
                                "remote_port": 1},
                "server_rec": {"socket_id": 2},
            })
        return entries

    def test_no_downsample_when_under_budget(self):
        client_dump = {"path": "c", "methods": {1: "x"}}
        server_dump = {"path": "s"}
        accepted = self._make_synthetic_accepted_set(100)
        fake_result = {
            "client_ids": set(range(100)), "server_ids": set(range(100)),
            "joined_ids": set(range(100)), "only_client": set(), "only_server": set(),
            "reject_counts": {r: 0 for r in merge.ALL_REJECT_REASONS},
            "accepted": accepted,
        }
        client_dump.update({"freq_hz": 1e8, "records": [None] * 100})
        server_dump.update({"freq_hz": 1e8, "records": [None] * 100})
        ir, size_info = merge.build_intermediate_representation(
            client_dump, server_dump, fake_result, max_b64_bytes=12 * 1024 * 1024)
        self.assertFalse(ir["downsample"]["applied"])
        self.assertEqual(ir["stats"]["output_record_count"], 100)
        # must-fix 1: every row's weight is 1 when nothing was downsampled --
        # unweighted and weighted stats must coincide in this case.
        self.assertTrue(all(rm["weight"] == 1 for rm in ir["records_meta"]))

    def test_downsample_keeps_tail_full_and_strides_the_head(self):
        client_dump = {"path": "c", "methods": {1: "x"}}
        server_dump = {"path": "s"}
        n = 1000
        accepted = self._make_synthetic_accepted_set(n)
        fake_result = {
            "client_ids": set(range(n)), "server_ids": set(range(n)),
            "joined_ids": set(range(n)), "only_client": set(), "only_server": set(),
            "reject_counts": {r: 0 for r in merge.ALL_REJECT_REASONS},
            "accepted": accepted,
        }
        client_dump.update({"freq_hz": 1e8, "records": [None] * n})
        server_dump.update({"freq_hz": 1e8, "records": [None] * n})
        # Force a small budget so downsampling always triggers regardless
        # of the real per-record byte size. Must comfortably exceed what
        # the tail alone costs (default tail_keep_fraction=0.2 of n=1000
        # => 200 records kept in full, unconditionally, is the design's
        # own floor -- see build_intermediate_representation's docstring
        # comment on why tail fidelity outranks the byte budget) or the
        # budget could never be met even in principle, which would make
        # this a test of an impossible constraint rather than of the
        # stride logic.
        small_budget = 100_000  # comfortably above the ~37KB tail floor
        ir, size_info = merge.build_intermediate_representation(
            client_dump, server_dump, fake_result, max_b64_bytes=small_budget)
        self.assertTrue(ir["downsample"]["applied"])
        self.assertLess(ir["stats"]["output_record_count"], n)
        self.assertLessEqual(size_info["blob_b64_bytes"], small_budget)
        # The highest-e2e_ns (tail) records must all still be present --
        # since our synthetic e2e_ns == trace_id == i, the top
        # tail_kept_full trace_ids (999, 998, ...) must all appear.
        kept_ids = {rm["trace_id"] for rm in ir["records_meta"]}
        tail_n = ir["downsample"]["tail_kept_full"]
        expected_tail_ids = {f"{i:#018x}" for i in range(n - tail_n, n)}
        self.assertTrue(expected_tail_ids.issubset(kept_ids))

        # must-fix 1 (fix-round review): tail rows carry weight 1 (kept at
        # full fidelity, one row == one original record); head rows carry
        # weight == head_stride (each stands in for `stride` originals).
        # Getting this backwards, or uniformly 1, is exactly the bug that
        # made the stats table quietly report the wrong percentile.
        stride = ir["downsample"]["head_stride"]
        by_id = {rm["trace_id"]: rm for rm in ir["records_meta"]}
        for i in range(n - tail_n, n):
            self.assertEqual(by_id[f"{i:#018x}"]["weight"], 1, msg=f"tail record {i}")
        head_kept_ids = sorted(kept_ids - expected_tail_ids)
        self.assertTrue(head_kept_ids, "expected at least one head row to survive striding")
        # Every head row but the last stands in for exactly `stride`
        # originals; the last is the partial bucket and must be credited
        # with the true remainder, NOT a full stride. Crediting it fully
        # concentrates the whole rounding excess on one record sitting at
        # the head/tail seam -- right where P90 is read off -- and the
        # distortion is stride-fold locally however small it looks
        # globally (post-review finding; see the ledger).
        head_n = n - tail_n
        remainder = head_n - (len(head_kept_ids) - 1) * stride
        for tid in head_kept_ids[:-1]:
            self.assertEqual(by_id[tid]["weight"], stride, msg=f"head record {tid}")
        self.assertEqual(by_id[head_kept_ids[-1]]["weight"], remainder,
                         msg="last head row must carry the partial bucket's true count")
        self.assertLessEqual(remainder, stride)
        self.assertGreaterEqual(remainder, 1)
        # With the remainder credited correctly the weights cover the
        # original population EXACTLY -- an equality, not a bound. If this
        # ever loosens back into an inequality, the percentile table has
        # silently stopped describing the population it names.
        total_weight = sum(rm["weight"] for rm in ir["records_meta"])
        self.assertEqual(total_weight, n)


# ---------------------------------------------------------------------------
# Remaining "also fix" items from the fix-round review that were not
# already covered as part of one of the three must-fix tests above.
# ---------------------------------------------------------------------------

class TestFixRoundAlsoFixItems(unittest.TestCase):
    def setUp(self):
        self.tmpfiles = []

    def tearDown(self):
        for p in self.tmpfiles:
            try:
                os.remove(p)
            except OSError:
                pass

    def _tmp(self, name, data):
        path = os.path.join(THIS_DIR, f"_test_tmp_{name}_{id(self)}_{os.getpid()}.dump")
        write_temp(path, data)
        self.tmpfiles.append(path)
        return path

    def test_method_id_decodes_unsigned(self):
        # RECORD_FMT used to spell method_id as "i" (signed) instead of
        # "I" -- any method_id >= 2^31 decoded as a negative number. That
        # range is reachable in practice (method ids are assigned
        # sequentially from 1 in registration order -- see
        # parse_method_table -- so this needs a LOT of distinct methods,
        # but the encoding itself doesn't care why the value is large).
        big_method_id = 0x80000001  # > 2^31, would be negative if signed
        rec = make_record_bytes(1, merge.LT_ROLE_CLIENT, sequential_client_ts(),
                                 method_id=big_method_id)
        header = make_header_bytes(1, method_table_offset=merge.HEADER_SIZE)
        path = self._tmp("bigmethod", header + rec)
        d = merge.parse_dump(path)
        self.assertEqual(d["records"][0]["method_id"], big_method_id)
        self.assertGreater(d["records"][0]["method_id"], 0)

    def test_dropped_count_reaches_ir_stats(self):
        rec = make_record_bytes(1, merge.LT_ROLE_CLIENT, sequential_client_ts())
        srec = make_record_bytes(1, merge.LT_ROLE_SERVER, sequential_server_ts())
        cdata = make_dump([rec], dropped_count=42)
        sdata = make_dump([srec], dropped_count=7)
        cpath, spath = self._tmp("c", cdata), self._tmp("s", sdata)
        cdump, sdump = merge.parse_dump(cpath), merge.parse_dump(spath)
        self.assertEqual(cdump["header"]["dropped_count"], 42)
        self.assertEqual(sdump["header"]["dropped_count"], 7)
        result = merge.merge_dumps(cdump, sdump)
        ir, _ = merge.build_intermediate_representation(cdump, sdump, result)
        self.assertEqual(ir["stats"]["client_dropped_count"], 42)
        self.assertEqual(ir["stats"]["server_dropped_count"], 7)

    def test_realtime_windows_that_do_not_overlap_are_flagged(self):
        rec = make_record_bytes(1, merge.LT_ROLE_CLIENT, sequential_client_ts())
        srec = make_record_bytes(1, merge.LT_ROLE_SERVER, sequential_server_ts())
        # Two dumps whose calibration ran a day apart -- e.g. today's
        # client paired with yesterday's server dump by mistake.
        cdata = make_dump([rec])
        sdata_bytes = bytearray(make_dump([srec]))
        # head_realtime_ns / tail_realtime_ns sit right after
        # counter_freq_hz in HEADER_FMT ("<QIIQqQqqqdQQQQQ"); patch them
        # directly rather than re-deriving byte offsets by hand.
        import struct as _struct
        # HEADER_FIELDS: magic, record_size, point_count, head_counter,
        # head_monotonic_ns, tail_counter, tail_monotonic_ns,
        # head_realtime_ns, tail_realtime_ns, ... -- "<QIIQqQq" covers the
        # first 7 fields (through tail_monotonic_ns), so its calcsize is
        # exactly the byte offset where head_realtime_ns starts.
        off = _struct.calcsize("<QIIQqQq")
        one_day_ns = 24 * 3600 * 1_000_000_000
        head_rt, tail_rt = _struct.unpack_from("<qq", sdata_bytes, off)
        _struct.pack_into("<qq", sdata_bytes, off, head_rt + one_day_ns, tail_rt + one_day_ns)
        cpath, spath = self._tmp("c", cdata), self._tmp("s", bytes(sdata_bytes))
        cdump, sdump = merge.parse_dump(cpath), merge.parse_dump(spath)
        self.assertFalse(merge.check_realtime_overlap(cdump, sdump))
        result = merge.merge_dumps(cdump, sdump)
        self.assertFalse(result["realtime_overlap_ok"])
        ir, _ = merge.build_intermediate_representation(cdump, sdump, result)
        self.assertFalse(ir["stats"]["realtime_overlap_ok"])

    def test_realtime_windows_that_do_overlap_are_not_flagged(self):
        rec = make_record_bytes(1, merge.LT_ROLE_CLIENT, sequential_client_ts())
        srec = make_record_bytes(1, merge.LT_ROLE_SERVER, sequential_server_ts())
        cpath = self._tmp("c", make_dump([rec]))
        spath = self._tmp("s", make_dump([srec]))
        cdump, sdump = merge.parse_dump(cpath), merge.parse_dump(spath)
        self.assertTrue(merge.check_realtime_overlap(cdump, sdump))

    def test_item_group_sizes_31_matches_the_31_names_and_merge_pys_own_lists(self):
        # "also fix": render.py used to hardcode 7/16/8 as JS constants
        # instead of reading the split from the IR. Pin that the IR
        # actually carries a split that (a) sums to 31 and (b) matches
        # merge.py's own source-of-truth lists, so a future change to the
        # item lists is caught here rather than only in render.py at
        # runtime.
        client_dump = {"path": "c", "methods": {1: "x"}, "freq_hz": 1e8, "records": []}
        server_dump = {"path": "s", "freq_hz": 1e8, "records": []}
        fake_result = {
            "client_ids": set(), "server_ids": set(), "joined_ids": set(),
            "only_client": set(), "only_server": set(),
            "reject_counts": {r: 0 for r in merge.ALL_REJECT_REASONS},
            "accepted": [],
        }
        ir, _ = merge.build_intermediate_representation(client_dump, server_dump, fake_result)
        self.assertEqual(ir["item_group_sizes_31"],
                          [len(merge.DECOMPOSITION_ITEMS_CLIENT_SEND),
                           len(merge.DECOMPOSITION_ITEMS_SERVER),
                           len(merge.DECOMPOSITION_ITEMS_CLIENT_RECV)])
        self.assertEqual(sum(ir["item_group_sizes_31"]), 31)
        self.assertEqual(sum(ir["item_group_sizes_31"]), len(ir["item_names_31"]))

    def test_saturated_point_marks_the_items_it_touches_and_is_clipped(self):
        ts = sequential_client_ts(step=1000)
        ts[merge.C_RPC_END] = merge.TS_SATURATED  # >= ~42.9s lower bound
        crec_bytes = make_record_bytes(1, merge.LT_ROLE_CLIENT, ts)
        srec_bytes = make_record_bytes(1, merge.LT_ROLE_SERVER, sequential_server_ts(step=1000))
        cpath = self._tmp("c", make_dump([crec_bytes]))
        spath = self._tmp("s", make_dump([srec_bytes]))
        cdump, sdump = merge.parse_dump(cpath), merge.parse_dump(spath)
        result = merge.merge_dumps(cdump, sdump)
        j = result["joined"][0]
        self.assertIn("cli_post_deser", j["merged"]["saturated_items"])
        ir, _ = merge.build_intermediate_representation(cdump, sdump, result)
        meta = ir["records_meta"][0]
        self.assertIn("cli_post_deser", meta["saturated_items"])
        # The ~42.9s ticks-side lower bound cannot fit int32 ns (+-2.147s),
        # so frac_to_i32 must have clamped it -- and said so.
        self.assertIn("cli_post_deser", meta["clipped_items"])


if __name__ == "__main__":
    unittest.main()
