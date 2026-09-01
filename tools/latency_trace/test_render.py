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


# ---------------------------------------------------------------------------
# The zoom/pan/statistics upgrade: end-to-end summary row, two-axis zoom,
# scrollbars.
#
# The existing class above can only assert that certain JS *source*
# survives, because there is no JS runtime in `python3 -m unittest`.  For
# the arithmetic added here -- weighted end-to-end percentiles, and the
# zoom/pan window clamping -- that would be a weak test: the interesting
# failures are off-by-one and clamping bugs, which string matching cannot
# see.  So render.py fences its pure numeric helpers between
# LT_PURE_MATH_BEGIN/END markers, and these tests extract that block and
# run it under `node` with hand-derived expected values.  The block is
# deliberately free of DOM and of module-level state so it can run
# standalone; anything that touches `document` belongs outside the fence.
# ---------------------------------------------------------------------------
import shutil
import subprocess

PURE_BEGIN = "// ---- LT_PURE_MATH_BEGIN ----"
PURE_END = "// ---- LT_PURE_MATH_END ----"


def extract_pure_math(html):
    """The fenced block of DOM-free numeric helpers from a rendered page."""
    start = html.index(PURE_BEGIN) + len(PURE_BEGIN)
    end = html.index(PURE_END)
    return html[start:end]


class TestPureMath(unittest.TestCase):
    """Runs render.py's fenced numeric helpers under node."""

    @classmethod
    def setUpClass(cls):
        cls.node = shutil.which("node") or shutil.which("nodejs")
        if not cls.node:
            raise unittest.SkipTest("node not found -- cannot execute the page's JS")
        cls.js = extract_pure_math(render.HTML_TEMPLATE)

    def run_js(self, expr):
        """Evaluate `expr` after the fenced block; returns the parsed JSON."""
        script = self.js + "\nconsole.log(JSON.stringify(" + expr + "));\n"
        out = subprocess.run([self.node, "-e", script], capture_output=True, text=True)
        if out.returncode != 0:
            self.fail(f"node failed: {out.stderr.strip()}")
        return json.loads(out.stdout)

    # -- weightedSummary -------------------------------------------------

    def test_summary_of_1_to_10_all_weight_1(self):
        # Hand-derived from the h = p*(total-1) linear-interpolation rule
        # the existing weightedQuantile already uses: for values 1..10,
        # p50 sits at h=4.5 -> 5.5, p90 at h=8.1 -> 9.1, and so on.
        pairs = [{"v": v, "w": 1} for v in range(1, 11)]
        got = self.run_js(f"weightedSummary({json.dumps(pairs)}, 0)")
        self.assertEqual(got["n"], 10)
        self.assertEqual(got["naCount"], 0)
        self.assertAlmostEqual(got["mean"], 5.5)
        self.assertAlmostEqual(got["p50"], 5.5)
        self.assertAlmostEqual(got["p90"], 9.1)
        self.assertAlmostEqual(got["p99"], 9.91)
        self.assertAlmostEqual(got["p999"], 9.991)

    def test_summary_respects_weights(self):
        # Values [1, 2] with weights [3, 1] stand for the population
        # [1,1,1,2] (total weight 4).  Mean = (1*3 + 2*1)/4 = 1.25.
        # p50: h = 0.5*3 = 1.5, both surrounding ranks land on the value
        # 1 -> 1.  p99: h = 0.99*3 = 2.97, rank 2 -> 1 and rank 3 -> 2,
        # interpolated -> 1 + 0.97 = 1.97.  An UNWEIGHTED computation
        # over the same two rows would give mean 1.5 and p50 1.5.
        pairs = [{"v": 1, "w": 3}, {"v": 2, "w": 1}]
        got = self.run_js(f"weightedSummary({json.dumps(pairs)}, 0)")
        self.assertEqual(got["n"], 4)
        self.assertAlmostEqual(got["mean"], 1.25)
        self.assertAlmostEqual(got["p50"], 1.0)
        self.assertAlmostEqual(got["p99"], 1.97)

    def test_summary_of_nothing_is_null_not_zero(self):
        # An empty scope (e.g. a zoom window a filter emptied out) must
        # read as "no data" -- a 0 here would print as "0 ns", which is a
        # measurement claim, not an absence.
        got = self.run_js("weightedSummary([], 5)")
        self.assertEqual(got["n"], 0)
        self.assertEqual(got["naCount"], 5)
        self.assertIsNone(got["mean"])
        self.assertIsNone(got["p50"])

    # -- zoomRankWindow --------------------------------------------------

    def test_zoom_in_halves_the_window_about_the_anchor(self):
        # [0,100) zoomed in 2x about its centre -> [25,75).
        self.assertEqual(self.run_js("zoomRankWindow(0, 100, 0.5, 0.5, 100)"),
                         {"lo": 25, "hi": 75})
        # About the left edge -> the left edge stays put.
        self.assertEqual(self.run_js("zoomRankWindow(0, 100, 0.5, 0, 100)"),
                         {"lo": 0, "hi": 50})
        # About the right edge -> the right edge stays put.
        self.assertEqual(self.run_js("zoomRankWindow(0, 100, 0.5, 1, 100)"),
                         {"lo": 50, "hi": 100})

    def test_zoom_out_doubles_and_clamps_to_the_dataset(self):
        self.assertEqual(self.run_js("zoomRankWindow(25, 75, 2, 0.5, 100)"),
                         {"lo": 0, "hi": 100})
        # Already at full extent: zooming out further is a no-op, it must
        # not run off either end.
        self.assertEqual(self.run_js("zoomRankWindow(0, 100, 2, 0.5, 100)"),
                         {"lo": 0, "hi": 100})
        # Clamping at one end must not eat the width at the other: a
        # window pinned to rank 0 zoomed out 2x still grows to 2x.
        self.assertEqual(self.run_js("zoomRankWindow(0, 20, 2, 0, 100)"),
                         {"lo": 0, "hi": 40})

    def test_zoom_in_stops_at_one_request(self):
        # The x axis is a virtual rank axis; a window narrower than one
        # request has nothing to draw.
        self.assertEqual(self.run_js("zoomRankWindow(10, 11, 0.5, 0.5, 100)"),
                         {"lo": 10, "hi": 11})
        self.assertEqual(self.run_js("zoomRankWindow(10, 12, 0.5, 0.5, 100)"),
                         {"lo": 10, "hi": 11})

    # -- zoomYRange ------------------------------------------------------
    #
    # The latency axis is always an explicit range -- there is no
    # auto-fit state to return to -- so every case here is a clamp
    # inside the whole sample's extent, never a handover.

    def test_y_zoom_in_halves_about_the_anchor(self):
        b = {"lo": 0, "hi": 1000}
        got = self.run_js(f"zoomYRange({{lo:0,hi:1000}}, {json.dumps(b)}, 0.5, 500)")
        self.assertEqual(got, {"lo": 250, "hi": 750})

    def test_y_zoom_anchors_on_the_cursor(self):
        b = {"lo": 0, "hi": 1000}
        got = self.run_js(f"zoomYRange({{lo:0,hi:1000}}, {json.dumps(b)}, 0.5, 0)")
        self.assertEqual(got, {"lo": 0, "hi": 500})

    def test_y_zoom_out_stops_at_the_full_extent(self):
        # Zooming out past the whole sample would show empty space above
        # the slowest request; it stops there instead, which is also the
        # state in which nothing is cropped.
        b = {"lo": 0, "hi": 1000}
        self.assertEqual(self.run_js(f"zoomYRange({{lo:250,hi:750}}, {json.dumps(b)}, 4, 500)"),
                         {"lo": 0, "hi": 1000})
        # A zoom-out that still crops keeps cropping.
        self.assertEqual(self.run_js(f"zoomYRange({{lo:400,hi:600}}, {json.dumps(b)}, 2, 500)"),
                         {"lo": 300, "hi": 700})

    def test_y_zoom_out_near_an_edge_keeps_its_full_width(self):
        # Clamping at the top must not eat the width at the bottom --
        # zooming out from a window against the ceiling still doubles it.
        b = {"lo": 0, "hi": 1000}
        self.assertEqual(self.run_js(f"zoomYRange({{lo:800,hi:1000}}, {json.dumps(b)}, 2, 900)"),
                         {"lo": 600, "hi": 1000})

    # -- panWindow -------------------------------------------------------

    def test_pan_clamps_without_changing_width(self):
        self.assertEqual(self.run_js("panWindow(10, 20, 5, 0, 100)"), {"lo": 15, "hi": 25})
        self.assertEqual(self.run_js("panWindow(10, 20, -50, 0, 100)"), {"lo": 0, "hi": 10})
        self.assertEqual(self.run_js("panWindow(90, 100, 50, 0, 100)"), {"lo": 90, "hi": 100})


class TestUpgradeShipsInThePage(unittest.TestCase):
    """String-level regression net for the parts that need a browser."""

    def setUp(self):
        if not os.path.exists(SYNTHETIC_IR_PATH):
            raise unittest.SkipTest(f"{SYNTHETIC_IR_PATH} not found")
        with open(SYNTHETIC_IR_PATH) as f:
            self.ir = json.load(f)
        self.html = render.render_html(self.ir, title="upgrade test")

    def test_every_record_carries_the_end_to_end_value_the_row_reads(self):
        for meta in self.ir["records_meta"]:
            self.assertIn("e2e_ns", meta)
            self.assertIsInstance(meta["e2e_ns"], int)

    def test_end_to_end_row_ships(self):
        self.assertIn("computeE2eRow", self.html)
        self.assertIn("weightedSummary", self.html)
        # Pinned first and excluded from the segment column sort.
        self.assertIn("e2e-row", self.html)

    def test_two_axis_zoom_controls_ship(self):
        for el in ("xZoomInBtn", "xZoomOutBtn", "yZoomInBtn", "yZoomOutBtn",
                   "hScrollTrack", "hScrollThumb", "vScrollTrack", "vScrollThumb"):
            self.assertIn(el, self.html)

    def test_wheel_and_drag_gestures_ship(self):
        # Ctrl/cmd + wheel = x zoom, shift + wheel = y zoom (bare wheel
        # deliberately left to the page, see the gesture table).
        self.assertIn("ctrlKey", self.html)
        self.assertIn("metaKey", self.html)
        self.assertIn("shiftKey", self.html)
        self.assertIn("'wheel'", self.html)
        # Drag pans; shift+drag selects a rank range only.
        self.assertIn("yView", self.html)
        # The latency axis is a fixed reference computed over the whole
        # sample, not refitted per visible window.
        self.assertIn("Y_REF", self.html)
        self.assertIn("zoomYRange", self.html)
