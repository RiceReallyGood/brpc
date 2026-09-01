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
"""tools/latency_trace/test_render.py

render.py has no browser to run its JS in during `python3 -m unittest`,
so this doesn't execute Canvas drawing -- that part of the fix-round
review's "verify in a browser" requirement was done by hand, once, with a
real headless browser, and is not repeatable here. What IS repeatable,
and what this file pins down, is the data contract the negative-segment
hatch path and the N/A path both depend on: that a record merge.py
actually produced (not hand-edited) carries the negative int32 / N/A
sentinel values those JS paths key off of, that they survive into the
rendered HTML's embedded JSON payload unmolested, and that the JS source
render.py ships still contains the branches that read them. Before the
fix-round's must-fix 2 (--low-concurrency, default off), no record with a
negative segment could reach this point at all -- see
testdata/make_synthetic_negative_na_ir.py's docstring.
"""
import json
import os
import struct
import unittest

import merge
import render

THIS_DIR = os.path.dirname(os.path.abspath(__file__))
SYNTHETIC_IR_PATH = os.path.join(THIS_DIR, "testdata", "synthetic_negative_na_ir.json")


class TestSyntheticNegativeAndNA(unittest.TestCase):
    """Exercises the hatch (negative-segment) and N/A rendering paths
    using testdata/synthetic_negative_na_ir.json -- a real merge.py
    output (see that fixture's own generator script), not a hand-authored
    IR, containing exactly one record with a negative segment (record A)
    and one with an N/A item pair (record B, RDMA polling mode)."""

    @classmethod
    def setUpClass(cls):
        if not os.path.exists(SYNTHETIC_IR_PATH):
            raise unittest.SkipTest(
                f"{SYNTHETIC_IR_PATH} not found -- regenerate with "
                f"testdata/make_synthetic_negative_na_ir.py")
        with open(SYNTHETIC_IR_PATH) as f:
            cls.ir = json.load(f)

    def _row(self, trace_id_hex):
        blob = __import__("base64").b64decode(self.ir["items_blob_b64"])
        ints = self.ir["ints_per_record"]
        for i, meta in enumerate(self.ir["records_meta"]):
            if meta["trace_id"] == trace_id_hex:
                return struct.unpack_from(f"<{ints}i", blob, i * ints * 4)
        self.fail(f"{trace_id_hex} not found in records_meta")

    def test_fixture_shape(self):
        self.assertEqual(self.ir["stats"]["output_record_count"], 2)
        ids = {m["trace_id"] for m in self.ir["records_meta"]}
        self.assertEqual(ids, {"0xa000000000000001", "0xb000000000000002"})

    def test_record_a_has_a_negative_link_item_in_the_blob(self):
        # Record A: negative link_total (design doc sec.6.1) -> both
        # model-A link items are negative. This is the exact byte pattern
        # computeNegativeFlags()/drawOneBar()'s hatch path in render.py
        # keys off of (get33Raw(...) < 0 and !== NA).
        row = self._row("0xa000000000000001")
        layout = self.ir["record_layout"]
        link_up_a = row[layout.index("link_up_model_a")]
        link_down_a = row[layout.index("link_down_model_a")]
        na = self.ir["na_sentinel_i32"]
        self.assertNotEqual(link_up_a, na)
        self.assertLess(link_up_a, 0)
        self.assertNotEqual(link_down_a, na)
        self.assertLess(link_down_a, 0)
        meta = next(m for m in self.ir["records_meta"] if m["trace_id"] == "0xa000000000000001")
        self.assertTrue(meta["any_negative_item"])
        # And NOT excluded from the output -- must-fix 2's whole point.
        self.assertIn("0xa000000000000001", {m["trace_id"] for m in self.ir["records_meta"]})

    def test_record_b_has_na_items_in_the_blob(self):
        # Record B: RDMA polling client -> cli_wake_to_onedge and
        # cli_onedge_to_readv are both N/A (design doc sec.8.5). This is
        # the byte pattern the dashed-tick / "N/A" tooltip path
        # (drawOneBar's seg.isNA branch) keys off of.
        row = self._row("0xb000000000000002")
        layout = self.ir["record_layout"]
        na = self.ir["na_sentinel_i32"]
        self.assertEqual(row[layout.index("cli_wake_to_onedge")], na)
        self.assertEqual(row[layout.index("cli_onedge_to_readv")], na)
        # But link_total/link items are real numbers (the RDMA-polling
        # fix's whole point) -- NOT also N/A.
        self.assertNotEqual(row[layout.index("link_up_model_a")], na)
        self.assertNotEqual(row[layout.index("link_down_model_a")], na)
        meta = next(m for m in self.ir["records_meta"] if m["trace_id"] == "0xb000000000000002")
        self.assertFalse(meta["any_negative_item"])

    def test_render_html_embeds_both_records_and_ships_the_reading_code(self):
        html = render.render_html(self.ir, title="synthetic fixture test")
        # The IR payload (negative ints, NA sentinel, both trace_ids)
        # actually reaches the page, not just merge.py's in-memory dict.
        self.assertIn("0xa000000000000001", html)
        self.assertIn("0xb000000000000002", html)
        self.assertIn(str(self.ir["na_sentinel_i32"]), html)
        # The JS this exact IR would run against still ships the branches
        # that read these values -- a regression that deleted/renamed the
        # hatch or N/A drawing code without updating this file would fail
        # here even though a browser was never opened.
        self.assertIn("hatchPattern", html)
        self.assertIn("seg.negative", html)
        self.assertIn("seg.isNA", html)
        self.assertIn("computeNegativeFlags", html)
        self.assertIn("NEG_A", html)
        self.assertIn("na_sentinel_i32", html)


if __name__ == "__main__":
    unittest.main()
