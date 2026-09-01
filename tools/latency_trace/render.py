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
"""tools/latency_trace/render.py

Turns merge.py's intermediate representation (IR) into a single
self-contained HTML file: a Canvas cumulative (waterfall) bar chart over a
virtual rank axis, plus a sortable per-segment percentile table. See
docs/superpowers/specs/2026-08-29-brpc-latency-trace-design.md sec.9.2/9.3
for the contract this implements.

No external references -- no CDN, no fetched fonts, no network calls at
view time. The page must open from disk with the browser offline.

Usage:
    # end to end, from raw dumps (calls merge.py's functions in-process):
    python3 render.py --client client.dump --server server.dump -o out.html

    # from an IR file already produced by `merge.py ... -o ir.json`:
    python3 render.py --ir ir.json -o out.html
"""
import argparse
import json
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import merge as lt_merge  # noqa: E402  (path insert must come first)


def build_ir_from_dumps(client_path, server_path, window_ms, max_mb):
    client_dump = lt_merge.parse_dump(client_path)
    server_dump = lt_merge.parse_dump(server_path)
    result = lt_merge.merge_dumps(client_dump, server_dump,
                                   window_ns=int(window_ms * 1e6))
    ir, size_info = lt_merge.build_intermediate_representation(
        client_dump, server_dump, result, max_b64_bytes=int(max_mb * 1024 * 1024))

    # Same zero-tolerance identity check merge.py's CLI does (sec.9.1/10.1):
    # a broken render is much less useful than a render that refuses to
    # ship over data whose arithmetic doesn't check out.
    bad = [e for e in result["accepted"] if e["merged"]["identity_ok"] is False]
    if bad:
        print(f"IDENTITY CHECK FAILED for {len(bad)} accepted record(s) -- "
              f"refusing to render", file=sys.stderr)
        for entry in bad[:5]:
            m = entry["merged"]
            print(f"  trace_id={entry['trace_id']:#x} lhs={m['identity_lhs']} "
                  f"rhs={m['identity_rhs']}", file=sys.stderr)
        sys.exit(1)

    return ir, size_info


# ---------------------------------------------------------------------------
# HTML/CSS/JS template. IR is spliced in as a single JSON literal at
# __LT_IR_JSON__ via str.replace (not str.format -- the template is full of
# literal '{'/'}' in CSS and JS that must not be treated as format fields).
# ---------------------------------------------------------------------------

HTML_TEMPLATE = r"""<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>__LT_TITLE__</title>
<style>
  :root {
    color-scheme: light;
    --page:            #f9f9f7;
    --surface-1:       #fcfcfb;
    --text-primary:    #0b0b0b;
    --text-secondary:  #52514e;
    --text-muted:      #898781;
    --gridline:        #e1e0d9;
    --baseline:        #c3c2b7;
    --border:          rgba(11,11,11,0.10);
    --warn-bg:         #fff6e6;
    --warn-border:     #eda100;
    --status-critical: #d03b3b;
    --hue-client-send: #2a78d6;
    --hue-server:      #eb6834;
    --hue-client-recv: #1baf7a;
    --hue-link:        #4a3aa7;
    --hue-gap:         #898781;
  }
  @media (prefers-color-scheme: dark) {
    :root {
      color-scheme: dark;
      --page:            #0d0d0d;
      --surface-1:       #1a1a19;
      --text-primary:    #ffffff;
      --text-secondary:  #c3c2b7;
      --text-muted:      #898781;
      --gridline:        #2c2c2a;
      --baseline:        #383835;
      --border:          rgba(255,255,255,0.10);
      --warn-bg:         #2a2410;
      --warn-border:     #c98500;
      --status-critical: #d03b3b;
      --hue-client-send: #3987e5;
      --hue-server:      #d95926;
      --hue-client-recv: #199e70;
      --hue-link:        #9085e9;
      --hue-gap:         #898781;
    }
  }
  * { box-sizing: border-box; }
  html, body {
    margin: 0; padding: 0; background: var(--page); color: var(--text-primary);
    font-family: system-ui, -apple-system, "Segoe UI", sans-serif;
    font-size: 14px; line-height: 1.5;
  }
  .wrap { max-width: 1360px; margin: 0 auto; padding: 24px 24px 64px; }
  h1 { font-size: 20px; margin: 0 0 4px; }
  h2 { font-size: 15px; margin: 0 0 10px; }
  p.subtitle { color: var(--text-secondary); margin: 0 0 20px; max-width: 78ch; }
  .card {
    background: var(--surface-1); border: 1px solid var(--border);
    border-radius: 8px; padding: 16px 18px; margin-bottom: 18px;
  }
  .banner-grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(220px,1fr)); gap: 10px 24px; }
  .stat-row { display: flex; justify-content: space-between; gap: 12px; padding: 3px 0; border-bottom: 1px dashed var(--gridline); font-size: 13px; }
  .stat-row:last-child { border-bottom: none; }
  .stat-row .k { color: var(--text-secondary); }
  .stat-row .v { font-variant-numeric: tabular-nums; text-align: right; }
  .downsample-note { border-left: 3px solid var(--warn-border); background: var(--warn-bg); padding: 8px 12px; border-radius: 4px; margin-top: 10px; font-size: 13px; }
  .clean-note { border-left: 3px solid var(--hue-client-recv); padding: 8px 12px; border-radius: 4px; margin-top: 10px; font-size: 13px; color: var(--text-secondary); }
  .reject-table { width: 100%; border-collapse: collapse; margin-top: 8px; font-size: 13px; }
  .reject-table th, .reject-table td { text-align: left; padding: 4px 8px; border-bottom: 1px solid var(--gridline); }
  .reject-table td.n { text-align: right; font-variant-numeric: tabular-nums; }
  .controls { display: flex; flex-wrap: wrap; gap: 16px 24px; align-items: flex-end; margin-bottom: 4px; }
  .control { display: flex; flex-direction: column; gap: 4px; }
  .control label { font-size: 12px; color: var(--text-secondary); }
  select, button {
    font: inherit; padding: 6px 10px; border-radius: 6px; border: 1px solid var(--border);
    background: var(--surface-1); color: var(--text-primary); cursor: pointer;
  }
  button.primary { border-color: var(--hue-client-send); color: var(--hue-client-send); font-weight: 600; }
  button:disabled { opacity: 0.45; cursor: default; }
  .zoom-info { font-size: 12px; color: var(--text-secondary); margin-left: auto; text-align: right; }
  .legend-note { font-size: 12px; color: var(--text-secondary); margin: 8px 0 12px; }
  .legend-groups { display: grid; grid-template-columns: repeat(auto-fit, minmax(230px,1fr)); gap: 14px 20px; }
  .legend-group h3 { font-size: 12px; text-transform: uppercase; letter-spacing: .04em; color: var(--text-secondary); margin: 0 0 6px; }
  .legend-item { display: flex; align-items: center; gap: 7px; padding: 2px 0; cursor: pointer; user-select: none; font-size: 12.5px; }
  .legend-item.hidden-seg { opacity: 0.38; text-decoration: line-through; }
  .swatch { width: 12px; height: 12px; border-radius: 2px; flex: none; border: 1px solid var(--border); }
  .legend-actions { display: flex; gap: 8px; margin-bottom: 10px; }
  .legend-actions button { font-size: 12px; padding: 4px 9px; }
  .chart-area { display: flex; align-items: flex-start; gap: 6px; }
  .chart-col { flex: 1; min-width: 0; }
  .chart-wrap { position: relative; }
  #chartCanvas { width: 100%; height: 440px; display: block; cursor: grab; touch-action: none; }
  #chartCanvas.panning { cursor: grabbing; }
  #chartCanvas.boxing { cursor: crosshair; }
  #selOverlay {
    position: absolute; top: 0; height: 440px;
    background: color-mix(in srgb, var(--hue-client-send) 18%, transparent);
    border-left: 1px solid var(--hue-client-send); border-right: 1px solid var(--hue-client-send);
    pointer-events: none; display: none;
  }
  /* Scrollbars are drawn rather than native: a native one would need a
     fake oversized content element behind the canvas, which fights the
     canvas's own devicePixelRatio sizing. The thumb's LENGTH is the
     point -- it says what fraction of the data (or of the latency
     domain) the view currently covers, which a bare position indicator
     would not. */
  .lt-track { background: var(--gridline); border-radius: 5px; position: relative; touch-action: none; }
  .lt-track.disabled { opacity: 0.35; pointer-events: none; }
  .lt-thumb { position: absolute; background: var(--text-muted); border-radius: 5px; cursor: grab; }
  .lt-thumb:hover { background: var(--text-secondary); }
  .lt-thumb.dragging { background: var(--hue-client-send); cursor: grabbing; }
  #hScrollTrack { height: 10px; margin: 6px 12px 0 68px; }
  #hScrollThumb { top: 1px; bottom: 1px; }
  .vscroll-col { width: 10px; height: 440px; position: relative; flex: none; }
  #vScrollTrack { position: absolute; left: 0; right: 0; top: 12px; bottom: 26px; }
  #vScrollThumb { left: 1px; right: 1px; }
  .zoom-btns { display: flex; gap: 4px; }
  .zoom-btns button { padding: 6px 9px; font-variant-numeric: tabular-nums; }
  table.gestures { border-collapse: collapse; font-size: 12px; margin: 8px 0 12px; }
  table.gestures td { padding: 2px 14px 2px 0; color: var(--text-secondary); vertical-align: top; }
  table.gestures td.g { color: var(--text-primary); white-space: nowrap; }
  kbd {
    font: inherit; font-size: 11px; padding: 1px 5px; border-radius: 4px;
    border: 1px solid var(--border); background: var(--page); color: var(--text-primary);
  }
  #tooltip {
    position: fixed; pointer-events: none; z-index: 50; max-width: 320px;
    background: var(--surface-1); border: 1px solid var(--border); border-radius: 6px;
    padding: 8px 10px; font-size: 12.5px; box-shadow: 0 4px 18px rgba(0,0,0,0.18);
    display: none;
  }
  #tooltip .tt-title { font-weight: 600; margin-bottom: 4px; }
  #tooltip .tt-value { font-variant-numeric: tabular-nums; font-weight: 700; }
  #tooltip .tt-row { display: flex; justify-content: space-between; gap: 14px; }
  #tooltip .tt-muted { color: var(--text-secondary); }
  .neg-row { display: flex; align-items: center; gap: 10px; margin-top: 10px; font-size: 12.5px; flex-wrap: wrap; }
  .neg-row .dot { width: 9px; height: 9px; border-radius: 2px; background: var(--status-critical); flex: none; }
  table.pct { width: 100%; border-collapse: collapse; font-size: 13px; }
  table.pct th, table.pct td { padding: 6px 10px; border-bottom: 1px solid var(--gridline); text-align: right; white-space: nowrap; }
  table.pct th:first-child, table.pct td:first-child { text-align: left; }
  table.pct thead th { cursor: pointer; color: var(--text-secondary); font-weight: 600; position: sticky; top: 0; background: var(--surface-1); }
  table.pct thead th.sorted { color: var(--text-primary); }
  table.pct thead th .arrow { font-size: 10px; margin-left: 3px; }
  table.pct td.name-cell { display: flex; align-items: center; gap: 7px; }
  table.pct td.num { font-variant-numeric: tabular-nums; }
  table.pct tbody tr:hover { background: var(--gridline); }
  table.pct tbody tr.e2e-row { font-weight: 700; }
  table.pct tbody tr.e2e-row td { border-bottom: 2px solid var(--border); }
  .table-head-row { display: flex; justify-content: space-between; align-items: baseline; flex-wrap: wrap; gap: 10px; margin-bottom: 10px; }
  .table-scope { font-size: 12.5px; color: var(--text-secondary); }
  .table-wrap { max-height: 640px; overflow: auto; }
  footer { color: var(--text-muted); font-size: 12px; margin-top: 28px; }
  .pill { display: inline-block; font-size: 10px; padding: 1px 5px; border-radius: 9px; border: 1px solid var(--border); color: var(--text-secondary); margin-left: 5px; }
</style>
</head>
<body>
<div class="wrap">
  <header>
    <h1>__LT_TITLE__</h1>
    <p class="subtitle">Per-request end-to-end RPC latency, decomposed into 33 named segments
      (design doc sec.5). The bar for each request is a waterfall: segments stack in
      pipeline order and the bar's top is, by construction, exactly that request's
      end-to-end latency under model A, whether every segment is positive or not.
      Under model B the two link segments are each rounded to whole nanoseconds
      independently, so on rare records the bar top can be off by &plusmn;1ns from the
      true end-to-end value -- an artifact of that rounding, not a data error.</p>
  </header>

  <section class="card" id="qualityBanner">
    <h2>Data quality</h2>
    <div class="banner-grid" id="bannerStats"></div>
    <div id="droppedSection"></div>
    <div id="overlapSection"></div>
    <div id="rejectSection"></div>
    <div id="downsampleSection"></div>
    <div id="negativeSection"></div>
  </section>

  <section class="card">
    <h2>Chart controls</h2>
    <div class="controls">
      <div class="control">
        <label for="sortModeSel">Bar order</label>
        <select id="sortModeSel">
          <option value="latency">End-to-end latency (ascending)</option>
          <option value="trace_id">trace_id (~chronological)</option>
        </select>
      </div>
      <div class="control">
        <label for="linkModelSel">Link time model (design doc sec.6)</label>
        <select id="linkModelSel">
          <option value="A">Model A -- per-request 50/50 split</option>
          <option value="B">Model B -- per-connection clock-offset estimate</option>
        </select>
      </div>
      <div class="control">
        <label>Zoom (rank axis)</label>
        <div class="zoom-btns">
          <button id="xZoomOutBtn" title="Show twice as many requests">X &minus;</button>
          <button id="xZoomInBtn" title="Show half as many requests">X +</button>
        </div>
      </div>
      <div class="control">
        <label>Zoom (latency axis)</label>
        <div class="zoom-btns">
          <button id="yZoomOutBtn" title="Show twice the latency range">Y &minus;</button>
          <button id="yZoomInBtn" title="Show half the latency range">Y +</button>
        </div>
      </div>
      <div class="control">
        <label>&nbsp;</label>
        <button id="resetZoomBtn" disabled>Reset zoom</button>
      </div>
      <div class="control">
        <label>&nbsp;</label>
        <label style="display:flex;align-items:center;gap:6px;font-size:12.5px;color:var(--text-secondary);cursor:pointer;">
          <input type="checkbox" id="hideNegChk"> hide requests with a negative segment
        </label>
      </div>
      <div class="zoom-info" id="zoomInfo"></div>
    </div>

    <div class="legend-note">
      Click a segment below to hide/show it in the chart (it stays in the table). Hiding a
      segment <em>shortens</em> the bars that had it -- that's intended, it shows what the
      request looks like without that stage, not a rendering bug. <strong>&#9733;</strong> segments
      are the checkpoints from the project's original latency-breakdown list (shown in
      color); <strong>&#9675;</strong> segments are gap/bookkeeping intervals this design added
      between them (shown in muted gray, deliberately receding). <code>link_up</code> /
      <code>link_down</code> are derived, not directly measured -- see the model selector above.
      A segment outlined with a dashed border is a <strong>lower bound</strong>, not an exact
      reading: its underlying timestamp saturated (the real interval was too long, roughly
      42.9 seconds or more, for the on-disk encoding to hold exactly).
    </div>
    <div class="legend-actions">
      <button id="legendAllBtn">show all</button>
      <button id="legendNoneBtn">hide all</button>
    </div>
    <div class="legend-groups" id="legendGroups"></div>
  </section>

  <section class="card">
    <h2>Cumulative latency, virtual rank axis</h2>
    <p class="table-scope">When a pixel column covers more than one request, the column shows
      that column's <em>slowest</em> request (by end-to-end latency) -- the hover tooltip says
      how many requests the column stands for.</p>
    <table class="gestures">
      <tr><td class="g">drag</td><td>pan both axes</td>
          <td class="g"><kbd>Ctrl</kbd>/<kbd>&#8984;</kbd> + wheel</td><td>zoom the rank axis, anchored at the cursor</td></tr>
      <tr><td class="g"><kbd>Shift</kbd> + drag</td><td>select a rank range to zoom into (the latency scale is left alone)</td>
          <td class="g"><kbd>Shift</kbd> + wheel</td><td>zoom the latency axis, anchored at the cursor</td></tr>
      <tr><td class="g">scrollbars</td><td>pan; the thumb's length is the fraction currently in view</td>
          <td class="g">wheel alone</td><td>scrolls the page as usual -- the chart does not capture it</td></tr>
    </table>
    <p class="table-scope"><strong>The latency axis is a fixed ruler.</strong> It is set once from the whole
      sample and only ever moves when you move it -- panning or zooming the rank axis never
      rescales it -- so a bar's height means the same thing in every window, and two bars far
      apart on the rank axis can be compared directly by eye.
      Its default range is the whole sample cropped at its <strong>P99.9</strong>, not at its maximum: a
      single multi-millisecond outlier over a body of microsecond requests would otherwise
      squash every bar on the page into the bottom pixel row. The requests above that crop are
      still drawn, marked with a small triangle at the edge they run past, and
      <code>Y &minus;</code> zooms out to them -- zooming out stops at the true maximum, so nothing
      is out of reach. They are also already counted in every statistic below, which the latency
      axis never affects: <strong>zooming the rank axis narrows the statistics</strong> (while the table
      follows the zoom), <strong>zooming the latency axis does not</strong>. It is a magnifier, never a
      filter.</p>
    <div class="chart-area">
      <div class="chart-col">
        <div class="chart-wrap">
          <canvas id="chartCanvas"></canvas>
          <div id="selOverlay"></div>
        </div>
        <div class="lt-track" id="hScrollTrack"><div class="lt-thumb" id="hScrollThumb"></div></div>
      </div>
      <div class="vscroll-col">
        <div class="lt-track" id="vScrollTrack"><div class="lt-thumb" id="vScrollThumb"></div></div>
      </div>
    </div>
    <div class="neg-row" id="negCallout"></div>
  </section>

  <section class="card">
    <div class="table-head-row">
      <h2 style="margin:0;">Latency statistics</h2>
      <div class="table-scope" id="tableScopeLabel"></div>
      <button id="scopeToggleBtn"></button>
    </div>
    <div class="table-wrap">
      <table class="pct" id="pctTable">
        <thead><tr id="pctHeadRow"></tr></thead>
        <tbody id="pctBody"></tbody>
      </table>
    </div>
  </section>

  <footer>
    Generated by tools/latency_trace/render.py from
    <span id="srcFiles"></span>. Single self-contained file, no external
    references, no network calls -- safe to open from disk.
  </footer>
</div>
<div id="tooltip"></div>

<script>
'use strict';
const IR = __LT_IR_JSON__;

// ---------------------------------------------------------------------
// Pure numeric helpers -- the weighted statistics behind the percentile
// table, and the window arithmetic behind zooming and panning.
//
// This block is fenced, DOM-free, and depends on nothing but its
// arguments, so test_render.py can lift it straight out of a rendered
// page and run it under node. That matters because these are exactly
// the functions whose failures are off-by-ones and clamping mistakes --
// things asserting "the source still contains this identifier" cannot
// see. Anything touching `document`, `state` or `IR` belongs on the
// other side of the fence.
// ---------------------------------------------------------------------
// ---- LT_PURE_MATH_BEGIN ----
// must-fix 1 (fix-round review): merge.py's downsampling keeps the slow
// tail whole and strides the head to hit a byte budget (right for the
// chart -- it preserves tail shape). But an UNWEIGHTED percentile over
// that kept set treats it as if it WERE the population: on the committed
// fixture forced through --max-mb 0.2, the row labelled P50 read 34420ns
// (really the true population's 83rd percentile), P90 read the true
// 96.7th, P99 the true 99.7th -- see the fix-round report for the full
// before/after. `w` (each row's weight -- 1 for a full-fidelity row,
// `head_stride` for a strided head row, from WEIGHTS/merge.py's
// per-record "weight") corrects this: a value that stands for `w`
// original records counts `w` times, not once, in both the mean and each
// percentile. When nothing was downsampled every weight is 1 and this is
// arithmetically identical to the old unweighted computation.
function weightedQuantile(sortedPairs, prefixWeight, totalWeight, p) {
  const n = sortedPairs.length;
  if (n === 0) return null;
  if (n === 1) return sortedPairs[0].v;
  const h = p * (totalWeight - 1);
  const lo = Math.floor(h), hi = Math.ceil(h);
  const valueAtRank = (k) => {
    // Smallest index i such that prefixWeight[i] > k -- i.e. the sample
    // whose weighted "slot" (as if repeated w times) covers virtual
    // position k. Binary search since prefixWeight is non-decreasing.
    let a = 0, b = n - 1;
    while (a < b) {
      const mid = (a + b) >> 1;
      if (prefixWeight[mid] > k) b = mid; else a = mid + 1;
    }
    return sortedPairs[a].v;
  };
  const vLo = valueAtRank(lo), vHi = valueAtRank(hi);
  return lo === hi ? vLo : vLo + (vHi - vLo) * (h - lo);
}

// Mean and percentiles over `pairs` ({v, w}), plus the weight that was
// N/A. Shared by every row of the statistics table -- the end-to-end row
// and the 33 segment rows -- so the summary line and the breakdown
// underneath it can never be computed two different ways.
function weightedSummary(pairs, naWeight) {
  const sorted = pairs.slice().sort((a, b) => a.v - b.v);
  let totalWeight = 0;
  const prefixWeight = new Array(sorted.length);
  for (let i = 0; i < sorted.length; i++) { totalWeight += sorted[i].w; prefixWeight[i] = totalWeight; }
  let mean = null;
  if (totalWeight > 0) {
    let acc = 0;
    for (const o of sorted) acc += o.v * o.w;
    mean = acc / totalWeight;
  }
  return {
    // N / N-A are themselves estimates of the full accepted population's
    // counts when downsampled (sum of weight, not row count) -- see the
    // weighting note above weightedQuantile for why this can't just
    // count rows either.
    n: Math.round(totalWeight),
    naCount: Math.round(naWeight),
    mean,
    p50: weightedQuantile(sorted, prefixWeight, totalWeight, 0.50),
    p90: weightedQuantile(sorted, prefixWeight, totalWeight, 0.90),
    p99: weightedQuantile(sorted, prefixWeight, totalWeight, 0.99),
    p999: weightedQuantile(sorted, prefixWeight, totalWeight, 0.999),
  };
}

// A new [lo, hi) window on the virtual rank axis, scaled by `factor`
// (< 1 zooms in) about `anchorFrac` -- 0 = the window's left edge, 1 =
// its right edge -- clamped to [0, n]. Clamping at one end must not eat
// the width at the other: zooming out from a window pinned at rank 0
// still doubles its width rather than growing only rightwards to the
// midpoint. The floor of one request is the axis's resolution limit.
function zoomRankWindow(lo, hi, factor, anchorFrac, n) {
  if (n <= 0) return { lo: 0, hi: 0 };
  const width = Math.max(1, hi - lo);
  const anchor = lo + anchorFrac * width;
  const w = Math.max(1, Math.min(n, width * factor));
  let newLo = anchor - anchorFrac * w;
  newLo = Math.max(0, Math.min(n - w, newLo));
  const l = Math.floor(newLo);
  return { lo: l, hi: Math.max(l + 1, Math.min(n, l + Math.round(w))) };
}

// A new latency-axis window in nanoseconds, scaled by `factor` about
// `anchorNs` and clamped inside `bounds` (the whole sample's full
// extent). There is no "auto" state to fall back to: the axis is always
// an explicit range, so that panning the rank axis cannot rescale it.
// Zooming out stops when the window is the full extent -- every request
// is then on screen and there is nothing further to reveal.
function zoomYRange(cur, bounds, factor, anchorNs) {
  const range = Math.max(1e-9, cur.hi - cur.lo);
  const frac = (anchorNs - cur.lo) / range;
  const w = Math.min(bounds.hi - bounds.lo, range * factor);
  let lo = anchorNs - frac * w;
  let hi = lo + w;
  if (hi > bounds.hi) { hi = bounds.hi; lo = hi - w; }
  if (lo < bounds.lo) { lo = bounds.lo; hi = Math.min(bounds.hi, lo + w); }
  return { lo, hi };
}

// Slide a window by `delta` without changing its width, clamped so it
// stays inside [min, max].
function panWindow(lo, hi, delta, min, max) {
  const width = hi - lo;
  let l = lo + delta;
  if (l + width > max) l = max - width;
  if (l < min) l = min;
  return { lo: l, hi: l + width };
}
// ---- LT_PURE_MATH_END ----


// ---------------------------------------------------------------------
// Constants derived from the IR's fixed schema (design doc sec.5): the 31
// shared items are always client-send + server + client-recv, in that
// order (DECOMPOSITION_ITEMS_31 in merge.py). link_up/link_down are
// spliced in at positions SEND_N and SEND_N+1+SERVER_N of the canonical
// 33-item order (ITEM_ORDER_33), matching sec.5's presentation order.
//
// "also fix" (fix-round review): the split sizes (7/16/8) used to be
// hardcoded JS literals instead of being read from the IR that actually
// defines them (merge.py's DECOMPOSITION_ITEMS_CLIENT_SEND/_SERVER/
// _CLIENT_RECV) -- a change to those lists on the merge.py side wouldn't
// have been caught here until the split silently misaligned. Read from
// IR.item_group_sizes_31 instead, with a fallback to the previously-
// hardcoded values only for an older IR file that predates this field.
// ---------------------------------------------------------------------
const NAMES31 = IR.item_names_31;
const STARRED31 = IR.item_starred_31;
const GROUP_SIZES = IR.item_group_sizes_31 || [7, 16, 8];
if (GROUP_SIZES.length !== 3) {
  throw new Error('unexpected item_group_sizes_31 -- schema drift from what render.py was written against');
}
const [SEND_N, SERVER_N, RECV_N] = GROUP_SIZES;
if (NAMES31.length !== 31 || SEND_N + SERVER_N + RECV_N !== 31) {
  throw new Error('unexpected item_names_31 length -- schema drift from what render.py was written against');
}
const NAMES33 = NAMES31.slice(0, SEND_N).concat(['link_up'])
  .concat(NAMES31.slice(SEND_N, SEND_N + SERVER_N)).concat(['link_down'])
  .concat(NAMES31.slice(SEND_N + SERVER_N));
const STARRED33 = STARRED31.slice(0, SEND_N).concat([true])
  .concat(STARRED31.slice(SEND_N, SEND_N + SERVER_N)).concat([true])
  .concat(STARRED31.slice(SEND_N + SERVER_N));
const PHASE33 = new Array(SEND_N).fill('client_send').concat(['link'])
  .concat(new Array(SERVER_N).fill('server')).concat(['link'])
  .concat(new Array(RECV_N).fill('client_recv'));

function i33ToRow31Index(i33) {
  // Maps a position in the 33-item canonical order to (a) an index into
  // the 31-shared-item block of one record's 35-int row, or (b) a link
  // slot. See render.py's docstring / merge.py's record_layout comment
  // for the row layout: [0..30]=31 items, [31]=link_up_A, [32]=link_down_A,
  // [33]=link_up_B, [34]=link_down_B.
  if (i33 <= SEND_N - 1) return { kind: 'item', idx: i33 };
  if (i33 === SEND_N) return { kind: 'link_up' };
  if (i33 <= SEND_N + SERVER_N) return { kind: 'item', idx: i33 - 1 };
  if (i33 === SEND_N + SERVER_N + 1) return { kind: 'link_down' };
  return { kind: 'item', idx: i33 - 2 };
}
const ROW_MAP = [];
for (let i = 0; i < 33; i++) ROW_MAP.push(i33ToRow31Index(i));

const NA = IR.na_sentinel_i32;
const INTS = IR.ints_per_record;
const N_TOTAL = IR.records_meta.length;
// must-fix 1 (fix-round review): each record's weight -- 1 for a
// full-fidelity row, `head_stride` for a downsample-strided row -- is how
// many original accepted records this one row stands in for. Defaults to
// 1 for an older IR file that predates this field, which degrades to the
// old (correct-when-undownsampled, biased-when-downsampled) behavior
// rather than throwing.
const WEIGHTS = new Float64Array(N_TOTAL);
for (let i = 0; i < N_TOTAL; i++) {
  const w = IR.records_meta[i].weight;
  WEIGHTS[i] = (typeof w === 'number' && w > 0) ? w : 1;
}
const IS_DOWNSAMPLED = !!(IR.downsample && IR.downsample.applied);

function decodeBlobB64(b64) {
  const bin = atob(b64);
  const bytes = new Uint8Array(bin.length);
  for (let i = 0; i < bin.length; i++) bytes[i] = bin.charCodeAt(i);
  return new Int32Array(bytes.buffer);
}
const BLOB = decodeBlobB64(IR.items_blob_b64);

function rowOf(idx) { return BLOB.subarray(idx * INTS, (idx + 1) * INTS); }

function get33Raw(row, i33, model) {
  const m = ROW_MAP[i33];
  if (m.kind === 'item') return row[m.idx];
  if (m.kind === 'link_up') return row[model === 'A' ? 31 : 33];
  return row[model === 'A' ? 32 : 34]; // link_down
}

// Per-record "does this record contain a negative segment under this
// model" flag, computed once at load (not per redraw). NA items don't
// count as negative -- they're absent, not negative.
function computeNegativeFlags(model) {
  const flags = new Uint8Array(N_TOTAL);
  let count = 0;
  // The lowest point any record's running cumulative reaches, over the
  // WHOLE sample -- the floor of the latency axis. Taken here rather
  // than per visible window (and with no segment hidden) because the
  // axis is a fixed reference now: it must not move when the rank
  // window moves. Negative only when some record steps back below zero.
  let floor = 0;
  for (let idx = 0; idx < N_TOTAL; idx++) {
    const row = rowOf(idx);
    let neg = false, cum = 0;
    for (let i33 = 0; i33 < 33; i33++) {
      const v = get33Raw(row, i33, model);
      if (v === NA) continue;
      if (v < 0) neg = true;
      cum += v;
      if (cum < floor) floor = cum;
    }
    if (neg) { flags[idx] = 1; count++; }
  }
  return { flags, count, floor };
}
const NEG_A = computeNegativeFlags('A');
const NEG_B = computeNegativeFlags('B');

// ---------------------------------------------------------------------
// The latency axis is a fixed reference, computed once over the whole
// sample. It does not follow the rank window, the negative-segment
// filter, or the legend -- that is the entire point: two different rank
// windows, and two different renders of comparable data, can be read
// against the same ruler, and panning left/right never rescales the
// bars under the cursor.
//
// The default top is the whole sample's P99.9 rather than its maximum:
// on real captures a single multi-millisecond outlier over a body of
// microsecond requests flattens every bar on the page into the bottom
// pixel row. The handful of requests above it are drawn cropped, with a
// marker at the edge they run past, and Y- reaches them -- zooming out
// stops at the true maximum, so nothing is ever unreachable.
// ---------------------------------------------------------------------
const Y_REF = (() => {
  const pairs = [];
  let maxE2e = 0;
  for (let i = 0; i < N_TOTAL; i++) {
    const e = IR.records_meta[i].e2e_ns;
    pairs.push({ v: e, w: WEIGHTS[i] });
    if (e > maxE2e) maxE2e = e;
  }
  const p999 = N_TOTAL ? weightedSummary(pairs, 0).p999 : null;
  const top = Math.max(1, p999 === null ? maxE2e : p999);
  const ref = {};
  for (const [model, neg] of [['A', NEG_A], ['B', NEG_B]]) {
    const lo = Math.min(0, neg.floor);
    ref[model] = {
      base: { lo, hi: top },
      bounds: { lo, hi: Math.max(top, maxE2e, 1) },
    };
  }
  return ref;
})();
function yRefFor(model) { return Y_REF[model]; }

// ---------------------------------------------------------------------
// Sort orders over the virtual rank axis (design doc sec.9.2/9.3's
// "requirement 5": end-to-end latency, or trace_id / ~chronological).
// ---------------------------------------------------------------------
function buildOrder(keyFn) {
  const idx = new Array(N_TOTAL);
  for (let i = 0; i < N_TOTAL; i++) idx[i] = i;
  idx.sort((a, b) => { const ka = keyFn(a), kb = keyFn(b); return ka < kb ? -1 : ka > kb ? 1 : 0; });
  return idx;
}
const ORDER_LATENCY = buildOrder(i => IR.records_meta[i].e2e_ns);
const ORDER_TRACE = buildOrder(i => BigInt(IR.records_meta[i].trace_id));

function filteredOrder(base, model) {
  const flags = model === 'A' ? NEG_A.flags : NEG_B.flags;
  const out = [];
  for (const idx of base) if (!flags[idx]) out.push(idx);
  return out;
}

// ---------------------------------------------------------------------
// Color palette (dataviz skill: categorical hues for the &#9733; segments,
// grouped by pipeline phase; muted gray for &#9675; gap segments, so they
// recede rather than compete for attention). Validated with
// scripts/validate_palette.js against this exact adjacency sequence
// (blue -> gray -> ... -> violet -> orange -> ... -> violet -> aqua):
// worst adjacent CVD Delta E 13.0 (deutan), worst normal-vision Delta E
// 16.3 -- both clear the skill's target/floor. Aqua's contrast against the
// light surface is a documented sub-3:1 WARN; the legend + tooltip + table
// are the required relief channel.
// ---------------------------------------------------------------------
const PHASE_COLOR = {
  client_send: { light: '#2a78d6', dark: '#3987e5' },
  server:      { light: '#eb6834', dark: '#d95926' },
  client_recv: { light: '#1baf7a', dark: '#199e70' },
  link:        { light: '#4a3aa7', dark: '#9085e9' },
};
const GAP_COLOR = { light: '#898781', dark: '#898781' };
function isDark() { return window.matchMedia && window.matchMedia('(prefers-color-scheme: dark)').matches; }
function colorFor(i33) {
  const c = STARRED33[i33] ? PHASE_COLOR[PHASE33[i33]] : GAP_COLOR;
  return isDark() ? c.dark : c.light;
}

// ---------------------------------------------------------------------
// State
// ---------------------------------------------------------------------
const state = {
  sortMode: 'latency',
  linkModel: 'A',
  hideNeg: false,
  rankLo: 0,
  rankHi: N_TOTAL,
  // The latency axis, always an explicit {lo, hi} in nanoseconds -- it
  // never auto-fits the visible window, so bar heights are comparable
  // between any two rank windows and panning left/right does not
  // rescale anything. Starts at, and Reset returns to, Y_REF's base.
  yView: null, // filled in below, once the link model is known
  hidden: new Set(),
  tableScope: 'zoom', // 'zoom' | 'all'
  tableSort: { col: 'p99', dir: 'desc' },
};

state.yView = { lo: yRefFor(state.linkModel).base.lo, hi: yRefFor(state.linkModel).base.hi };

function currentOrder() {
  const base = state.sortMode === 'latency' ? ORDER_LATENCY : ORDER_TRACE;
  return state.hideNeg ? filteredOrder(base, state.linkModel) : base;
}

function fmtNs(ns) {
  if (ns === null || ns === undefined || Number.isNaN(ns)) return '—';
  const sign = ns < 0 ? '−' : '';
  const abs = Math.abs(ns);
  if (abs < 1000) return sign + abs.toFixed(0) + ' ns';
  if (abs < 1e6) return sign + (abs / 1000).toFixed(2) + ' µs';
  return sign + (abs / 1e6).toFixed(3) + ' ms';
}
function fmtCount(n) { return n.toLocaleString('en-US'); }
function fmtPct(x) { return (x * 100).toFixed(2) + '%'; }

// ---------------------------------------------------------------------
// Data-quality banner (design doc / task requirement: surface merge.py's
// reject counts and join stats, not just the accepted data).
// ---------------------------------------------------------------------
function renderBanner() {
  const s = IR.stats, j = s.join;
  const rows = [
    ['Client dump', s.client_file + ' (' + fmtCount(s.client_record_count) + ' records, ' + s.client_freq_hz.toFixed(1) + ' Hz)'],
    ['Server dump', s.server_file + ' (' + fmtCount(s.server_record_count) + ' records, ' + s.server_freq_hz.toFixed(1) + ' Hz)'],
    ['Joined by trace_id', fmtCount(j.joined) + ' (' + fmtPct(j.join_rate_client) + ' of client, ' + fmtPct(j.join_rate_server) + ' of server)'],
    ['Unmatched (client only / server only)', fmtCount(j.only_client) + ' / ' + fmtCount(j.only_server)],
    ['Accepted (passed all sec.10.1 checks)', fmtCount(s.accepted_count) + ' of ' + fmtCount(j.joined) + ' joined'],
    ['Shown on this page', fmtCount(s.output_record_count)],
  ];
  const grid = document.getElementById('bannerStats');
  grid.textContent = '';
  for (const [k, v] of rows) {
    const row = document.createElement('div'); row.className = 'stat-row';
    const kEl = document.createElement('span'); kEl.className = 'k'; kEl.textContent = k;
    const vEl = document.createElement('span'); vEl.className = 'v'; vEl.textContent = v;
    row.append(kEl, vEl); grid.append(row);
  }

  // must-fix 3 (fix-round review): dropped_count was parsed out of both
  // headers and then never looked at again. The ring buffer stops
  // accepting new records once full and counts what it refused there --
  // if a run overflowed, the "Joined by trace_id" row above can read
  // 100% while most of what actually ran was never written to either
  // dump at all. Surfaced prominently (not just another quiet stat row)
  // whenever either side is non-zero; also warned on stderr by merge.py's
  // own CLI (main()), since this page might not even be the first thing
  // whoever ran the capture looks at.
  const dropSec = document.getElementById('droppedSection');
  dropSec.textContent = '';
  if (s.client_dropped_count || s.server_dropped_count) {
    const dropNote = document.createElement('div');
    dropNote.className = 'downsample-note'; // reuse the warn-bg/warn-border styling
    dropNote.textContent = 'RING BUFFER OVERFLOW: the client dropped ' + fmtCount(s.client_dropped_count) +
      ' record(s) and the server dropped ' + fmtCount(s.server_dropped_count) + ' record(s) because their ' +
      'buffers were full before this capture ended. The "Joined by trace_id" rate above is computed only over ' +
      'what actually made it into these two dump files -- it says nothing about the requests dropped before ' +
      'that, and a 100% join rate here does NOT mean 100% of the traffic that ran was captured.';
    dropSec.append(dropNote);
  }

  // "also fix": the realtime calibration pair exists so this tool can
  // sanity-check the two dumps came from overlapping capture runs
  // (design doc sec.8.1/9.1) -- previously computed and then never
  // actually checked, so two dumps from unrelated runs joined "happily".
  const overlapSec = document.getElementById('overlapSection');
  overlapSec.textContent = '';
  if (s.realtime_overlap_ok === false) {
    const overlapNote = document.createElement('div');
    overlapNote.className = 'downsample-note';
    overlapNote.textContent = 'CLIENT/SERVER CLOCKS DO NOT OVERLAP: the two dumps’ realtime calibration ' +
      'windows do not overlap at all (see merge.py’s stderr warning for the exact windows). A successful ' +
      'trace_id join does not by itself prove these two dumps came from the same capture run -- this pair ' +
      'looks like it did not, and every number on this page should be treated with that in mind.';
    overlapSec.append(overlapNote);
  }

  const REASON_LABEL = {
    unstamped_point: 'a checkpoint decoded as raw 0 (never stamped) on at least one side. This has more than ' +
      'one legitimate cause the runtime cannot distinguish after the fact: an un-instrumented code path, but ' +
      'also two IN-SCOPE, EXPECTED sources of a genuine zero -- a streaming response’s S15-S17 (design doc ' +
      'sec.13: streaming does not go through the ordinary write path) and a cancelled backup request’s ' +
      'receive-side points. See the breakdown below this table for which specific point(s) were zero.',
    non_monotonic: "one side's decoded offsets went backwards within its own checkpoint sequence",
    negative_decomposition_item: 'at least one of the 31 shared segments, or the total link time, came out negative (only enforced as an exclusion in --low-concurrency mode -- see the note below the table)',
    negative_rtt_late_write_end_stamp: 'client wake (C09) preceded client write_end (C08) -- a known scheduling-delay tail (design doc sec.10.1), not necessarily a bug',
  };
  const rc = s.reject_counts;
  const totalTagged = Object.values(rc).reduce((a, b) => a + b, 0);
  const distinctExcluded = j.joined - s.accepted_count;
  const rejSec = document.getElementById('rejectSection');
  rejSec.textContent = '';
  const h = document.createElement('div'); h.className = 'stat-row';
  h.innerHTML = ''; // no innerHTML with untrusted content; build with text nodes below
  const label = document.createElement('div');
  label.style.marginTop = '10px'; label.style.fontSize = '13px'; label.style.color = 'var(--text-secondary)';
  label.textContent = distinctExcluded + ' joined record(s) (' + fmtPct(j.joined ? distinctExcluded / j.joined : 0) +
    ') were excluded from every statistic on this page for failing at least one weight-bearing check ' +
    '(sec.10.1); a record can fail more than one, so the per-reason counts below can sum to more than ' + totalTagged + ':';
  rejSec.append(label);
  const table = document.createElement('table'); table.className = 'reject-table';
  const thead = document.createElement('thead');
  const htr = document.createElement('tr');
  for (const t of ['Reason', 'Count', 'What it means']) { const th = document.createElement('th'); th.textContent = t; htr.append(th); }
  thead.append(htr); table.append(thead);
  const tbody = document.createElement('tbody');
  for (const reason of Object.keys(rc)) {
    const tr = document.createElement('tr');
    const c1 = document.createElement('td'); c1.textContent = reason;
    const c2 = document.createElement('td'); c2.className = 'n'; c2.textContent = fmtCount(rc[reason]);
    const c3 = document.createElement('td'); c3.textContent = REASON_LABEL[reason] || '';
    tr.append(c1, c2, c3); tbody.append(tr);
  }
  table.append(tbody); rejSec.append(table);

  // "also fix": name which specific point(s) were found zero among the
  // unstamped_point rejects, instead of only asserting a cause.
  const hist = s.unstamped_point_histogram || {};
  const histEntries = Object.entries(hist).sort((a, b) => b[1] - a[1]);
  if (histEntries.length) {
    const histNote = document.createElement('div');
    histNote.style.marginTop = '8px'; histNote.style.fontSize = '12.5px'; histNote.style.color = 'var(--text-secondary)';
    histNote.textContent = 'Which point(s) were zero, across the ' + fmtCount(rc.unstamped_point || 0) +
      ' unstamped_point reject(s) above (a record can contribute more than one point): ' +
      histEntries.map(([name, count]) => name + ' (' + fmtCount(count) + ')').join(', ') + '.';
    rejSec.append(histNote);
  }

  // must-fix 2 (fix-round review): whether the non-negative-item check
  // (sec.10.1 assertion 3) is being enforced as an exclusion at all.
  const modeNote = document.createElement('div');
  modeNote.style.marginTop = '10px'; modeNote.style.fontSize = '13px'; modeNote.style.color = 'var(--text-secondary)';
  if (s.low_concurrency_mode) {
    modeNote.textContent = '--low-concurrency was set: records with a negative segment were EXCLUDED above ' +
      '(counted under negative_decomposition_item), not shown on this page at all.';
  } else {
    const ni = s.negative_items || { count: 0, ratio: 0 };
    modeNote.textContent = 'Default mode (not --low-concurrency): records with a negative segment are kept, ' +
      'not excluded -- ' + fmtCount(ni.count) + ' of ' + fmtCount(s.accepted_count) + ' accepted records (' +
      fmtPct(ni.ratio) + ') have one. Design doc sec.6.1/6.2/9.3: at real capture concurrency this is expected ' +
      'batching noise, and under model B a genuine pollution detector, not by itself evidence of a defect. See ' +
      'the negative-segment callout below the chart.';
  }
  rejSec.append(modeNote);

  const dsSec = document.getElementById('downsampleSection');
  dsSec.textContent = '';
  const ds = IR.downsample;
  const note = document.createElement('div');
  if (ds.applied) {
    note.className = 'downsample-note';
    note.textContent = 'Downsampling is IN EFFECT: ' + fmtCount(ds.original_count) + ' accepted records were reduced ' +
      'to ' + fmtCount(ds.kept_count) + ' to stay under the embedded-data size budget. The slowest ' +
      fmtCount(ds.tail_kept_full) + ' (the tail, ranked by end-to-end latency) are kept at full fidelity; the ' +
      'faster head is uniformly strided by ' + ds.head_stride + '× (every ' + ds.head_stride +
      'th request kept). Anything on this page describing "N requests" describes the ' + fmtCount(ds.kept_count) +
      ' shown here, not the full ' + fmtCount(ds.original_count) + '-record capture.';
  } else {
    note.className = 'clean-note';
    note.textContent = 'No downsampling was needed -- every one of the ' + fmtCount(ds.kept_count) +
      ' accepted, joined requests appears on this page.';
  }
  dsSec.append(note);

  const negSec = document.getElementById('negativeSection');
  negSec.textContent = '';
  const negNote = document.createElement('div');
  negNote.className = 'clean-note';
  negNote.style.borderLeftColor = 'var(--status-critical)';
  negNote.textContent = fmtCount(NEG_A.count) + ' of ' + fmtCount(N_TOTAL) + ' (' + fmtPct(N_TOTAL ? NEG_A.count / N_TOTAL : 0) +
    ') shown requests contain at least one negative segment under Model A; ' + fmtCount(NEG_B.count) + ' (' +
    fmtPct(N_TOTAL ? NEG_B.count / N_TOTAL : 0) + ') under Model B. Negative segments are not clamped, hidden, or ' +
    'averaged away (design doc sec.9.3) -- the chart draws them as a hatched band that steps back over the ' +
    "previous segment, and the bar's top still equals the true end-to-end latency.";
  negSec.append(negNote);

  document.getElementById('srcFiles').textContent = s.client_file + ' + ' + s.server_file;
}
renderBanner();

// ---------------------------------------------------------------------
// Legend / segment visibility toggle (requirement 2).
// ---------------------------------------------------------------------
function groupRanges() {
  return [
    ['Client -> send', 0, SEND_N],
    ['Link (derived)', SEND_N, SEND_N + 1],
    ['Server', SEND_N + 1, SEND_N + 1 + SERVER_N],
    ['Link (derived)', SEND_N + 1 + SERVER_N, SEND_N + 2 + SERVER_N],
    ['Client <- receive', SEND_N + 2 + SERVER_N, 33],
  ];
}
function renderLegend() {
  const host = document.getElementById('legendGroups');
  host.textContent = '';
  // Merge the two "Link (derived)" ranges into one group visually since
  // they're the same 2-item family split by the server block.
  const groups = [
    ['Client -> send (' + SEND_N + ')', Array.from({length: SEND_N}, (_, i) => i)],
    ['Link (2, derived)', [SEND_N, SEND_N + 1 + SERVER_N]],
    ['Server (' + SERVER_N + ')', Array.from({length: SERVER_N}, (_, i) => SEND_N + 1 + i)],
    ['Client <- receive (' + RECV_N + ')', Array.from({length: RECV_N}, (_, i) => SEND_N + 2 + SERVER_N + i)],
  ];
  for (const [title, indices] of groups) {
    const g = document.createElement('div'); g.className = 'legend-group';
    const h = document.createElement('h3'); h.textContent = title; g.append(h);
    for (const i33 of indices) {
      const item = document.createElement('div');
      item.className = 'legend-item';
      item.dataset.i33 = String(i33);
      const sw = document.createElement('span'); sw.className = 'swatch';
      sw.style.background = colorFor(i33);
      const label = document.createElement('span');
      label.textContent = (STARRED33[i33] ? '★ ' : '○ ') + NAMES33[i33];
      item.append(sw, label);
      item.addEventListener('click', () => {
        const name = NAMES33[i33];
        if (state.hidden.has(name)) state.hidden.delete(name); else state.hidden.add(name);
        item.classList.toggle('hidden-seg', state.hidden.has(name));
        redrawAll();
      });
      g.append(item);
    }
    host.append(g);
  }
}
renderLegend();
document.getElementById('legendAllBtn').addEventListener('click', () => {
  state.hidden.clear();
  document.querySelectorAll('.legend-item').forEach(el => el.classList.remove('hidden-seg'));
  redrawAll();
});
document.getElementById('legendNoneBtn').addEventListener('click', () => {
  for (const n of NAMES33) state.hidden.add(n);
  document.querySelectorAll('.legend-item').forEach(el => el.classList.add('hidden-seg'));
  redrawAll();
});

// ---------------------------------------------------------------------
// Per-record segment geometry (nanosecond cumulative space, not pixels).
// Shared by drawing and hover hit-testing so they can never disagree.
// ---------------------------------------------------------------------
function computeBarSegments(idx) {
  const row = rowOf(idx);
  // "also fix" (fix-round review): which of this record's items are a
  // ts[]-saturation lower bound (~42.9s+) rather than an exact reading
  // (latency_trace.h's 0xFFFFFFFE sentinel) -- drawn/tooltipped as a
  // floor, not a precise value. Falls back to an empty set for an older
  // IR file that predates this field.
  const satNames = IR.records_meta[idx].saturated_items;
  const satSet = satNames && satNames.length ? new Set(satNames) : null;
  let cum = 0;
  const segs = [];
  for (let i33 = 0; i33 < 33; i33++) {
    const name = NAMES33[i33];
    if (state.hidden.has(name)) continue;
    const raw = get33Raw(row, i33, state.linkModel);
    if (raw === NA) { segs.push({ i33, name, starred: STARRED33[i33], isNA: true, cumAt: cum }); continue; }
    const before = cum, after = cum + raw;
    cum = after;
    segs.push({
      i33, name, starred: STARRED33[i33], isNA: false, valueNs: raw,
      yLo: Math.min(before, after), yHi: Math.max(before, after), negative: raw < 0,
      saturated: satSet ? satSet.has(name) : false,
    });
  }
  return { segs, finalCum: cum };
}

// ---------------------------------------------------------------------
// Chart: virtual rank axis, per-pixel-column max-aggregation, waterfall
// bars with hatched negative segments (design doc sec.9.2/9.3).
// ---------------------------------------------------------------------
const canvas = document.getElementById('chartCanvas');
const selOverlay = document.getElementById('selOverlay');
const tooltip = document.getElementById('tooltip');
let lastRender = null; // geometry cache for hover hit-testing
const hatchCache = new Map();
function hatchPattern(ctx, colorHex) {
  const key = colorHex + (isDark() ? ':d' : ':l');
  if (hatchCache.has(key)) return hatchCache.get(key);
  const size = 8;
  const off = document.createElement('canvas'); off.width = size; off.height = size;
  const octx = off.getContext('2d');
  octx.globalAlpha = 0.30; octx.fillStyle = colorHex; octx.fillRect(0, 0, size, size);
  octx.globalAlpha = 1; octx.strokeStyle = colorHex; octx.lineWidth = 1.4;
  octx.beginPath();
  octx.moveTo(0, size); octx.lineTo(size, 0);
  octx.moveTo(-size / 2, size / 2); octx.lineTo(size / 2, -size / 2);
  octx.moveTo(size / 2, size * 1.5); octx.lineTo(size * 1.5, size / 2);
  octx.stroke();
  const pat = ctx.createPattern(off, 'repeat');
  hatchCache.set(key, pat);
  return pat;
}

function drawChart() {
  const order = currentOrder();
  const nVis = order.length;
  state.rankLo = Math.max(0, Math.min(state.rankLo, nVis));
  state.rankHi = Math.max(state.rankLo, Math.min(state.rankHi, nVis));
  if (nVis === 0) state.rankHi = 0;
  const rankLo = state.rankLo, rankHi = state.rankHi;
  const visibleCount = rankHi - rankLo;

  const cssW = canvas.clientWidth || 800, cssH = canvas.clientHeight || 440;
  const dpr = window.devicePixelRatio || 1;
  const wantW = Math.max(1, Math.round(cssW * dpr)), wantH = Math.max(1, Math.round(cssH * dpr));
  if (canvas.width !== wantW || canvas.height !== wantH) { canvas.width = wantW; canvas.height = wantH; }
  const ctx = canvas.getContext('2d');
  ctx.setTransform(dpr, 0, 0, dpr, 0, 0);
  const surface = getComputedStyle(document.documentElement).getPropertyValue('--surface-1').trim() || '#fcfcfb';
  ctx.fillStyle = surface;
  ctx.fillRect(0, 0, cssW, cssH);

  const marginL = 68, marginR = 12, marginT = 12, marginB = 26;
  const plotW = Math.max(1, cssW - marginL - marginR), plotH = Math.max(1, cssH - marginT - marginB);

  if (visibleCount <= 0) {
    lastRender = null;
    ctx.fillStyle = getComputedStyle(document.documentElement).getPropertyValue('--text-secondary').trim();
    ctx.font = '13px system-ui, sans-serif';
    ctx.fillText('No requests in the current filter/zoom.', marginL, marginT + 20);
    // "also fix" (fix-round review): this early return used to skip both
    // updaters below, leaving the zoom-info line and the negative-segment
    // callout stuck on stale text from before the filter/zoom emptied out
    // (e.g. "showing all N requests" and "N of N shown requests" while the
    // chart itself says "No requests"). Call them here too so the whole
    // page agrees.
    updateZoomInfo(order, nVis);
    updateNegCallout();
    updateScrollbars();
    return;
  }

  // The latency axis does NOT come from the visible window: it is
  // state.yView, a fixed reference the user moves explicitly (Y_REF
  // above says why). Nothing computed inside this loop over the visible
  // records may feed back into the axis, or panning would rescale it.
  const model = state.linkModel;
  const yRef = yRefFor(model);
  const yDomainMin = state.yView.lo, yDomainMax = state.yView.hi;
  const yRange = Math.max(1, yDomainMax - yDomainMin);
  const yScale = plotH / yRange;
  const toY = (ns) => marginT + (yDomainMax - ns) * yScale;
  const fromY = (y) => yDomainMax - (y - marginT) / yScale;
  const baselineY = toY(0);
  // With the axis locked away from zero the baseline can fall outside
  // the plot; the per-bar negative marker then rides the nearer edge so
  // "this bar has a negative segment" stays visible instead of being
  // clipped away along with the baseline it normally sits on.
  const zeroVisible = 0 >= yDomainMin && 0 <= yDomainMax;
  const negTickY = Math.min(Math.max(baselineY, marginT + 1.5), marginT + plotH - 1.5);
  let clippedBars = 0;

  // gridlines + y labels (clean round-ish steps)
  ctx.strokeStyle = getComputedStyle(document.documentElement).getPropertyValue('--gridline').trim();
  ctx.fillStyle = getComputedStyle(document.documentElement).getPropertyValue('--text-muted').trim();
  ctx.font = '11px system-ui, sans-serif';
  ctx.textAlign = 'right'; ctx.textBaseline = 'middle';
  const steps = 5;
  for (let s = 0; s <= steps; s++) {
    const ns = yDomainMin + (yRange * s / steps);
    const y = toY(ns);
    ctx.beginPath(); ctx.moveTo(marginL, y); ctx.lineTo(marginL + plotW, y); ctx.lineWidth = 1; ctx.stroke();
    ctx.fillText(fmtNs(ns), marginL - 8, y);
  }
  if (zeroVisible) {
    ctx.strokeStyle = getComputedStyle(document.documentElement).getPropertyValue('--baseline').trim();
    ctx.beginPath(); ctx.moveTo(marginL, baselineY); ctx.lineTo(marginL + plotW, baselineY); ctx.lineWidth = 1.5; ctx.stroke();
  }

  const mode1to1 = visibleCount <= plotW;
  let colCount = 0, colRepIdx = null, colCounts = null;

  function drawOneBar(idx, x0, x1) {
    const { segs } = computeBarSegments(idx);
    let anyNeg = false;
    // Two passes, not one straight walk in pipeline order: a negative
    // segment steps back far enough to overlap more than just its
    // immediate predecessor whenever its magnitude exceeds that one
    // neighbor (rare with real data -- merge.py's own tests call these
    // "small link_total noise" -- but not impossible). Painting all
    // positive/N/A segments first and every negative segment in a
    // second, always-on-top pass guarantees the hatch stays visible
    // over its full overlap regardless of how far back it steps,
    // instead of later positive segments silently re-covering part of
    // it as the running cumulative climbs back through the same range
    // (sec.9.3's "both segments visible at once" requirement).
    for (const seg of segs) {
      if (seg.isNA) {
        const y = toY(seg.cumAt);
        ctx.save();
        ctx.strokeStyle = colorFor(seg.i33); ctx.lineWidth = 1.5; ctx.setLineDash([2, 2]);
        ctx.beginPath(); ctx.moveTo(x0, y); ctx.lineTo(x1, y); ctx.stroke();
        ctx.restore();
        continue;
      }
      if (seg.negative) { anyNeg = true; continue; }
      const pxTop = toY(seg.yHi), pxBottom = toY(seg.yLo);
      const h = Math.max(0.6, pxBottom - pxTop - 1);
      ctx.save();
      ctx.fillStyle = colorFor(seg.i33);
      ctx.fillRect(x0, pxTop, Math.max(1, x1 - x0), h);
      ctx.restore();
    }
    for (const seg of segs) {
      if (seg.isNA || !seg.negative) continue;
      const pxTop = toY(seg.yHi), pxBottom = toY(seg.yLo);
      const color = colorFor(seg.i33);
      ctx.save();
      ctx.fillStyle = hatchPattern(ctx, color);
      ctx.fillRect(x0, pxTop, Math.max(1, x1 - x0), pxBottom - pxTop);
      ctx.strokeStyle = color; ctx.lineWidth = 1.4;
      ctx.strokeRect(x0 + 0.7, pxTop + 0.7, Math.max(0, (x1 - x0) - 1.4), Math.max(0, (pxBottom - pxTop) - 1.4));
      ctx.restore();
    }
    // "also fix" (fix-round review): a ts[]-saturation lower bound
    // (~42.9s+, latency_trace.h's 0xFFFFFFFE) used to render as an exact
    // bar with no visual difference from a real reading -- specifically,
    // frac_to_i32's int32-ns clamp turns a 42.9s lower bound into an
    // exact-LOOKING 2.147s segment, which is actively misleading, not
    // just imprecise. A dashed white/dark outline marks it as a floor;
    // the tooltip (hoverAt) spells out why.
    for (const seg of segs) {
      if (seg.isNA || seg.negative || !seg.saturated) continue;
      const pxTop = toY(seg.yHi), pxBottom = toY(seg.yLo);
      ctx.save();
      ctx.strokeStyle = getComputedStyle(document.documentElement).getPropertyValue('--text-primary').trim();
      ctx.lineWidth = 1.6; ctx.setLineDash([3, 2]);
      ctx.strokeRect(x0 + 1, pxTop + 1, Math.max(0, (x1 - x0) - 2), Math.max(0, (pxBottom - pxTop) - 2));
      ctx.restore();
    }
    if (anyNeg) {
      ctx.save();
      ctx.fillStyle = getComputedStyle(document.documentElement).getPropertyValue('--status-critical').trim();
      ctx.fillRect(x0, negTickY - 1.5, Math.max(1, x1 - x0), 3);
      ctx.restore();
    }
    // Overflow markers. A bar cropped by a locked latency axis is
    // otherwise indistinguishable from one that genuinely ends at the
    // top of the plot -- which would turn a magnifier into a source of
    // wrong readings.
    let barTop = 0, barBottom = 0;
    for (const seg of segs) {
      if (seg.isNA) continue;
      if (seg.yHi > barTop) barTop = seg.yHi;
      if (seg.yLo < barBottom) barBottom = seg.yLo;
    }
    if (barTop > yDomainMax || barBottom < yDomainMin) {
      clippedBars++;
      ctx.save();
      ctx.fillStyle = getComputedStyle(document.documentElement).getPropertyValue('--text-primary').trim();
      const cx = (x0 + x1) / 2, half = Math.min(3.5, Math.max(1.5, (x1 - x0) / 2));
      if (barTop > yDomainMax) {
        ctx.beginPath();
        ctx.moveTo(cx, marginT + 1); ctx.lineTo(cx - half, marginT + 6); ctx.lineTo(cx + half, marginT + 6);
        ctx.closePath(); ctx.fill();
      }
      if (barBottom < yDomainMin) {
        const yb = marginT + plotH;
        ctx.beginPath();
        ctx.moveTo(cx, yb - 1); ctx.lineTo(cx - half, yb - 6); ctx.lineTo(cx + half, yb - 6);
        ctx.closePath(); ctx.fill();
      }
      ctx.restore();
    }
  }

  ctx.save();
  ctx.beginPath(); ctx.rect(marginL, marginT, plotW, plotH); ctx.clip();
  if (mode1to1) {
    for (let r = rankLo; r < rankHi; r++) {
      const idx = order[r];
      const x0 = marginL + (r - rankLo) / visibleCount * plotW;
      const x1 = marginL + (r - rankLo + 1) / visibleCount * plotW;
      drawOneBar(idx, x0, x1);
    }
  } else {
    colCount = Math.max(1, Math.round(plotW));
    const colMaxE2e = new Float64Array(colCount).fill(-1);
    colRepIdx = new Int32Array(colCount).fill(-1);
    colCounts = new Int32Array(colCount);
    const colVals = Array.from({ length: colCount }, () => []);
    for (let r = rankLo; r < rankHi; r++) {
      const col = Math.min(colCount - 1, Math.floor((r - rankLo) / visibleCount * colCount));
      colCounts[col]++;
      const idx = order[r];
      const e = IR.records_meta[idx].e2e_ns;
      colVals[col].push(e);
      if (e > colMaxE2e[col]) { colMaxE2e[col] = e; colRepIdx[col] = idx; }
    }
    for (let c = 0; c < colCount; c++) {
      if (colRepIdx[c] < 0) continue;
      const x0 = marginL + c / colCount * plotW, x1 = marginL + (c + 1) / colCount * plotW;
      drawOneBar(colRepIdx[c], x0, x1);
    }
    // "also fix" (fix-round review): the drawn envelope is deliberately
    // each column's SLOWEST request (tail-latency-first). At real capture
    // scale under trace_id order, a column can aggregate dozens of
    // requests, so the drawn curve sits near each column's high
    // percentile while the stats table beside it reports P50 -- an order
    // of magnitude apart, with nothing on the page explaining why. This
    // faint per-column MEDIAN line makes the gap between "drawn" and
    // "table" read as an envelope rather than a silent discrepancy; the
    // aggregation ratio itself is surfaced in the persistent zoom-info
    // line (updateZoomInfo, below).
    ctx.save();
    ctx.strokeStyle = getComputedStyle(document.documentElement).getPropertyValue('--text-secondary').trim();
    ctx.globalAlpha = 0.4;
    ctx.lineWidth = 1;
    ctx.beginPath();
    let medStarted = false;
    for (let c = 0; c < colCount; c++) {
      if (!colVals[c].length) continue;
      const arr = colVals[c].sort((a, b) => a - b);
      const med = arr[Math.floor((arr.length - 1) / 2)];
      const x = marginL + (c + 0.5) / colCount * plotW, y = toY(med);
      if (!medStarted) { ctx.moveTo(x, y); medStarted = true; } else ctx.lineTo(x, y);
    }
    ctx.stroke();
    ctx.restore();
  }
  ctx.restore();

  // x-axis caption
  ctx.textAlign = 'left'; ctx.textBaseline = 'alphabetic';
  ctx.fillStyle = getComputedStyle(document.documentElement).getPropertyValue('--text-muted').trim();
  ctx.font = '11px system-ui, sans-serif';
  const caption = (state.sortMode === 'latency' ? 'rank, sorted by end-to-end latency' : 'rank, sorted by trace_id (~chronological)')
    + '  --  showing ranks ' + fmtCount(rankLo) + '–' + fmtCount(rankHi - 1) + ' of ' + fmtCount(nVis);
  ctx.fillText(caption, marginL, cssH - 6);

  lastRender = {
    order, rankLo, rankHi, visibleCount, mode1to1, colCount, colRepIdx, colCounts,
    marginL, marginT, plotW, plotH, cssH, nVis,
    // The y mapping is cached rather than recomputed by every consumer:
    // hover hit-testing, the wheel/box-zoom anchors and the vertical
    // scrollbar all have to agree with what was actually painted, and a
    // second copy of this arithmetic would drift the moment the axis
    // could be locked.
    yDomainMin, yDomainMax, yRange, yBase: yRef.base, yBounds: yRef.bounds,
    toY, fromY, clippedBars,
  };
  updateZoomInfo(order, nVis);
  updateNegCallout();
  updateScrollbars();
}

function updateZoomInfo(order, nVis) {
  const el = document.getElementById('zoomInfo');
  const rankLo = state.rankLo, rankHi = state.rankHi;
  const isFull = rankLo === 0 && rankHi === nVis;
  const yRef = yRefFor(state.linkModel);
  const atBase = state.yView.lo === yRef.base.lo && state.yView.hi === yRef.base.hi;
  document.getElementById('resetZoomBtn').disabled = isFull && atBase;
  // The latency axis is reported separately from the rank axis because
  // the two mean different things to the numbers below: one narrows the
  // statistics, the other does not.
  let ySuffix = ' | y axis: ' + fmtNs(state.yView.lo) + '–' + fmtNs(state.yView.hi) +
    (atBase ? ' (default: the whole sample, cropped at its P99.9)' : ' (fixed)');
  const clipped = lastRender ? lastRender.clippedBars : 0;
  if (clipped > 0) {
    ySuffix += ', ' + fmtCount(clipped) + ' drawn bar(s) run past it (▲/▼) -- ' +
      'zoom the latency axis out to reach them; the statistics below already include them';
  }
  // "also fix" (fix-round review): make the column-aggregation ratio
  // persistently visible (previously only discoverable via hover, one
  // column at a time) whenever the chart is aggregating -- see the
  // median-line comment in drawChart() for why this matters.
  let aggSuffix = '';
  if (lastRender && !lastRender.mode1to1 && lastRender.colCount > 0) {
    const ratio = lastRender.visibleCount / lastRender.colCount;
    if (ratio > 1.05) {
      aggSuffix = ' -- each chart column aggregates ~' + ratio.toFixed(1) +
        ' requests (bar: slowest per column, faint line: per-column median)';
    }
  }
  if (isFull) { el.textContent = 'showing all ' + fmtCount(nVis) + ' requests' + aggSuffix + ySuffix; return; }
  // A zero-width zoom (rankLo === rankHi, reachable when a filter such as
  // hideNeg empties out the previously-zoomed range) has no records to
  // take a min/max over -- found by hand while verifying the "also fix"
  // empty-visible-set update in a real browser: without this branch the
  // min/max loop below leaves minE/maxE at their +-Infinity seed values
  // and fmtNs prints the literal string "Infinity ms".
  if (rankHi <= rankLo) {
    el.textContent = 'zoomed: ranks ' + fmtCount(rankLo) + '–' + fmtCount(rankLo) + ' (0 requests)' + aggSuffix + ySuffix;
    return;
  }
  let minE = Infinity, maxE = -Infinity;
  for (let r = rankLo; r < rankHi; r++) {
    const e = IR.records_meta[order[r]].e2e_ns;
    if (e < minE) minE = e; if (e > maxE) maxE = e;
  }
  el.textContent = 'zoomed: ranks ' + fmtCount(rankLo) + '–' + fmtCount(rankHi - 1) + ' (' + fmtCount(rankHi - rankLo) +
    ' requests, ' + fmtNs(minE) + '–' + fmtNs(maxE) + ')' + aggSuffix + ySuffix;
}

function updateNegCallout() {
  const el = document.getElementById('negCallout');
  el.textContent = '';
  const dot = document.createElement('span'); dot.className = 'dot';
  const txt = document.createElement('span');
  const count = state.linkModel === 'A' ? NEG_A.count : NEG_B.count;
  txt.textContent = 'red baseline tick = this bar has at least one negative segment. ' + fmtCount(count) +
    ' of ' + fmtCount(N_TOTAL) + ' (' + fmtPct(N_TOTAL ? count / N_TOTAL : 0) + ') shown requests, under Model ' +
    state.linkModel + '. Use "hide requests with a negative segment" above to filter them out instead.';
  el.append(dot, txt);
}

// ---------------------------------------------------------------------
// Zoom and pan (requirement 1), on two axes.
//
// The rank axis and the latency axis are not symmetric, and the code
// below keeps that asymmetry explicit: the rank window is an integer
// [lo, hi) into the current order and narrowing it narrows the
// statistics table with it, while the latency window is a nanosecond
// range that crops the drawing and nothing else. The latency axis is
// never derived from the visible records -- only the user moves it --
// so panning the rank axis leaves every bar at the height it had.
//
// Bare wheel is deliberately NOT captured: this page is long, the chart
// spans its full width, and a chart that eats the scroll wheel is a
// chart the reader has to escape from. Ctrl/Cmd+wheel (rank) and
// Shift+wheel (latency) both have defaults worth overriding here --
// browser page zoom, and horizontal scrolling of a page that has none.
// ---------------------------------------------------------------------
let drag = null;
let panFrame = 0;

function plotFrac(xCss) {
  const lr = lastRender;
  return Math.max(0, Math.min(1, (xCss - lr.marginL) / lr.plotW));
}

function schedulePanDraw() {
  // Panning redraws on every pointermove; at capture scale drawChart()
  // walks the whole visible window, so coalesce to one draw per frame
  // and leave the (much heavier) 33-segment table until the drag ends.
  if (panFrame) return;
  panFrame = requestAnimationFrame(() => { panFrame = 0; drawChart(); });
}

// A full-height band, not a box: the selection takes the rank range and
// deliberately ignores its own vertical extent, and the shape says so
// before the button comes up.
function setSelOverlay(x0, x1) {
  const lo = Math.min(x0, x1), hi = Math.max(x0, x1);
  selOverlay.style.left = lo + 'px';
  selOverlay.style.width = Math.max(0, hi - lo) + 'px';
}

canvas.addEventListener('pointerdown', (e) => {
  if (!lastRender) return;
  const rect = canvas.getBoundingClientRect();
  const x = e.clientX - rect.left, y = e.clientY - rect.top;
  canvas.setPointerCapture(e.pointerId);
  tooltip.style.display = 'none';
  if (e.shiftKey) {
    drag = { mode: 'box', x0: x, y0: y };
    canvas.classList.add('boxing');
    selOverlay.style.display = 'block';
    setSelOverlay(x, x);
  } else {
    drag = {
      mode: 'pan', x0: x, y0: y,
      rankLo0: state.rankLo, rankHi0: state.rankHi,
      yView0: { lo: state.yView.lo, hi: state.yView.hi },
      yBounds0: lastRender.yBounds, plotW0: lastRender.plotW, plotH0: lastRender.plotH,
      visibleCount0: lastRender.visibleCount, nVis0: lastRender.nVis,
    };
    canvas.classList.add('panning');
  }
});

canvas.addEventListener('pointermove', (e) => {
  const rect = canvas.getBoundingClientRect();
  const xCss = e.clientX - rect.left, yCss = e.clientY - rect.top;
  if (drag && drag.mode === 'box') { setSelOverlay(drag.x0, xCss); return; }
  if (drag && drag.mode === 'pan') {
    // Grab-and-move: dragging right shows earlier ranks, dragging down
    // shows lower latencies -- the content follows the pointer.
    const dRank = -(xCss - drag.x0) / drag.plotW0 * drag.visibleCount0;
    const w = panWindow(drag.rankLo0, drag.rankHi0, Math.round(dRank), 0, drag.nVis0);
    state.rankLo = w.lo; state.rankHi = w.hi;
    {
      const range = drag.yView0.hi - drag.yView0.lo;
      const dNs = (yCss - drag.y0) / drag.plotH0 * range;
      const v = panWindow(drag.yView0.lo, drag.yView0.hi, dNs,
                          drag.yBounds0.lo, drag.yBounds0.hi);
      state.yView = { lo: v.lo, hi: v.hi };
    }
    schedulePanDraw();
    return;
  }
  hoverAt(xCss, yCss, e.clientX, e.clientY);
});

canvas.addEventListener('pointerleave', () => { if (!drag) tooltip.style.display = 'none'; });

canvas.addEventListener('pointerup', (e) => {
  if (!drag || !lastRender) return;
  const d = drag; drag = null;
  canvas.classList.remove('panning', 'boxing');
  const rect = canvas.getBoundingClientRect();
  const x = e.clientX - rect.left, y = e.clientY - rect.top;
  if (d.mode === 'pan') {
    // A click that never moved is not a pan: recomputing the 33-segment
    // table costs real time at capture scale, and nothing changed.
    if (state.rankLo !== d.rankLo0 || state.rankHi !== d.rankHi0 ||
        state.yView.lo !== d.yView0.lo || state.yView.hi !== d.yView0.hi) redrawAll();
    return;
  }
  selOverlay.style.display = 'none';
  const lr = lastRender;
  // Rank axis only: the vertical extent of the drag is deliberately
  // ignored, so the selection cannot disturb the latency scale that
  // makes two rank windows comparable. The overlay is drawn full-height
  // to say so before the button is released.
  if (Math.abs(x - d.x0) < 4) return; // a click, not a selection
  const f0 = plotFrac(Math.min(d.x0, x)), f1 = plotFrac(Math.max(d.x0, x));
  const newLo = lr.rankLo + Math.floor(f0 * lr.visibleCount);
  const newHi = lr.rankLo + Math.max(newLo - lr.rankLo + 1, Math.ceil(f1 * lr.visibleCount));
  state.rankLo = newLo; state.rankHi = Math.min(newHi, currentOrder().length);
  redrawAll();
});

canvas.addEventListener('wheel', (e) => {
  if (!lastRender) return;
  const zoomY = e.shiftKey;
  const zoomX = !zoomY && (e.ctrlKey || e.metaKey);
  if (!zoomX && !zoomY) return; // bare wheel belongs to the page
  e.preventDefault();
  const rect = canvas.getBoundingClientRect();
  const x = e.clientX - rect.left, y = e.clientY - rect.top;
  const factor = e.deltaY < 0 ? 0.8 : 1.25;
  const lr = lastRender;
  if (zoomX) {
    const w = zoomRankWindow(state.rankLo, state.rankHi, factor, plotFrac(x), lr.nVis);
    state.rankLo = w.lo; state.rankHi = w.hi;
  } else {
    state.yView = zoomYRange(state.yView, lr.yBounds, factor, lr.fromY(y));
  }
  redrawAll();
}, { passive: false });

function zoomX(factor) {
  if (!lastRender) return;
  const w = zoomRankWindow(state.rankLo, state.rankHi, factor, 0.5, lastRender.nVis);
  state.rankLo = w.lo; state.rankHi = w.hi;
  redrawAll();
}
function zoomY(factor) {
  if (!lastRender) return;
  const lr = lastRender;
  const mid = (lr.yDomainMin + lr.yDomainMax) / 2;
  state.yView = zoomYRange(state.yView, lr.yBounds, factor, mid);
  redrawAll();
}
document.getElementById('xZoomInBtn').addEventListener('click', () => zoomX(0.5));
document.getElementById('xZoomOutBtn').addEventListener('click', () => zoomX(2));
document.getElementById('yZoomInBtn').addEventListener('click', () => zoomY(0.5));
document.getElementById('yZoomOutBtn').addEventListener('click', () => zoomY(2));
document.getElementById('resetZoomBtn').addEventListener('click', () => {
  state.rankLo = 0; state.rankHi = currentOrder().length;
  const b = yRefFor(state.linkModel).base;
  state.yView = { lo: b.lo, hi: b.hi };
  redrawAll();
});

// ---------------------------------------------------------------------
// Scrollbars (requirement 2). Drawn, not native: a native scrollbar
// would need a fake oversized element behind the canvas, which fights
// the canvas's own devicePixelRatio sizing. The thumb's LENGTH carries
// as much information as its position -- it is the fraction of the data
// (or of the latency domain) currently in view.
// ---------------------------------------------------------------------
function makeScrollbar(trackId, thumbId, opts) {
  const track = document.getElementById(trackId);
  const thumb = document.getElementById(thumbId);
  const vertical = opts.vertical;
  const lenProp = vertical ? 'height' : 'width';
  const posProp = vertical ? 'top' : 'left';
  let dragState = null;

  function trackLen() {
    const r = track.getBoundingClientRect();
    return vertical ? r.height : r.width;
  }

  function update() {
    const m = opts.model();
    if (!m || m.max - m.min <= 0 || (m.hi - m.lo) >= (m.max - m.min)) {
      track.classList.add('disabled');
      thumb.style[posProp] = '0px';
      thumb.style[lenProp] = '100%';
      return;
    }
    track.classList.remove('disabled');
    const span = m.max - m.min;
    const frac = (m.hi - m.lo) / span;
    // On the latency axis the top of the plot is the LARGEST value, so
    // the thumb's offset is measured from `max` downwards, not from
    // `min` upwards.
    const startFrac = vertical ? (m.max - m.hi) / span : (m.lo - m.min) / span;
    thumb.style[lenProp] = Math.max(6, frac * trackLen()) + 'px';
    thumb.style[posProp] = (startFrac * trackLen()) + 'px';
  }

  function panBy(pixels) {
    const m = opts.model();
    if (!m) return;
    const span = m.max - m.min;
    const len = trackLen() || 1;
    const delta = pixels / len * span * (vertical ? -1 : 1);
    const w = panWindow(m.lo, m.hi, delta, m.min, m.max);
    opts.apply(w.lo, w.hi);
  }

  thumb.addEventListener('pointerdown', (e) => {
    e.preventDefault(); e.stopPropagation();
    const m = opts.model();
    if (!m) return;
    thumb.setPointerCapture(e.pointerId);
    thumb.classList.add('dragging');
    dragState = { at: vertical ? e.clientY : e.clientX, lo: m.lo, hi: m.hi, min: m.min, max: m.max };
  });
  thumb.addEventListener('pointermove', (e) => {
    if (!dragState) return;
    const now = vertical ? e.clientY : e.clientX;
    const span = dragState.max - dragState.min;
    const len = trackLen() || 1;
    const delta = (now - dragState.at) / len * span * (vertical ? -1 : 1);
    const w = panWindow(dragState.lo, dragState.hi, delta, dragState.min, dragState.max);
    opts.apply(w.lo, w.hi);
  });
  const endDrag = () => { if (dragState) { dragState = null; thumb.classList.remove('dragging'); opts.commit(); } };
  thumb.addEventListener('pointerup', endDrag);
  thumb.addEventListener('pointercancel', endDrag);

  track.addEventListener('pointerdown', (e) => {
    // Clicking the track pages towards the click, the way a native one
    // does.
    if (e.target === thumb) return;
    const r = track.getBoundingClientRect();
    const at = vertical ? e.clientY - r.top : e.clientX - r.left;
    const thumbR = thumb.getBoundingClientRect();
    const tStart = vertical ? thumbR.top - r.top : thumbR.left - r.left;
    const tLen = vertical ? thumbR.height : thumbR.width;
    panBy(at < tStart ? -tLen : (at > tStart + tLen ? tLen : 0));
    opts.commit();
  });

  return update;
}

const updateHScroll = makeScrollbar('hScrollTrack', 'hScrollThumb', {
  vertical: false,
  model: () => lastRender ? { min: 0, max: lastRender.nVis, lo: state.rankLo, hi: state.rankHi } : null,
  apply: (lo, hi) => { state.rankLo = Math.round(lo); state.rankHi = Math.round(hi); schedulePanDraw(); },
  commit: () => redrawAll(),
});
const updateVScroll = makeScrollbar('vScrollTrack', 'vScrollThumb', {
  vertical: true,
  model: () => {
    if (!lastRender) return null;
    const b = lastRender.yBounds;
    return { min: b.lo, max: b.hi, lo: state.yView.lo, hi: state.yView.hi };
  },
  apply: (lo, hi) => { state.yView = { lo, hi }; schedulePanDraw(); },
  commit: () => drawChart(),
});
function updateScrollbars() {
  updateHScroll();
  updateVScroll();
}

// ---------------------------------------------------------------------
// Hover (requirement 3): segment name + duration; column request count
// while zoomed out enough to aggregate.
// ---------------------------------------------------------------------
function hoverAt(xCss, yCss, clientX, clientY) {
  const lr = lastRender;
  if (!lr || xCss < lr.marginL || xCss > lr.marginL + lr.plotW) { tooltip.style.display = 'none'; return; }
  const px = xCss - lr.marginL;
  let idx, count;
  if (lr.mode1to1) {
    const r = lr.rankLo + Math.min(lr.visibleCount - 1, Math.floor(px / lr.plotW * lr.visibleCount));
    idx = lr.order[r]; count = 1;
  } else {
    const col = Math.min(lr.colCount - 1, Math.max(0, Math.floor(px / lr.plotW * lr.colCount)));
    idx = lr.colRepIdx[col]; count = lr.colCounts[col];
    if (idx < 0) { tooltip.style.display = 'none'; return; }
  }
  const { segs } = computeBarSegments(idx);
  const meta = IR.records_meta[idx];

  // The y mapping comes from drawChart()'s cache, not from a second copy
  // of the same arithmetic here: with the latency axis lockable, a
  // recomputed domain would auto-fit while the painted one stayed
  // locked, and every tooltip would name the wrong segment.
  const yRange = lr.yRange;
  const nsAtCursor = lr.fromY(yCss);

  // Mirrors drawOneBar()'s two-pass paint order exactly: positives (and
  // the N/A tick) paint first in pipeline order, negatives always paint
  // last on top -- so a negative match always wins over a positive one,
  // and within each pass "last in pipeline order" wins (topmost of that
  // pass).
  let hit = null, negHit = null;
  for (const seg of segs) {
    if (seg.isNA) { if (Math.abs(nsAtCursor - seg.cumAt) < (yRange * 0.01)) hit = seg; continue; }
    if (nsAtCursor > seg.yHi || nsAtCursor < seg.yLo) continue;
    if (seg.negative) negHit = seg; else hit = seg;
  }
  if (negHit) hit = negHit;

  tooltip.textContent = '';
  const title = document.createElement('div'); title.className = 'tt-title';
  title.textContent = hit ? (hit.starred ? '★ ' : '○ ') + hit.name : 'request';
  tooltip.append(title);
  if (hit) {
    const row = document.createElement('div'); row.className = 'tt-row';
    const kEl = document.createElement('span'); kEl.className = 'tt-muted'; kEl.textContent = 'duration';
    const vEl = document.createElement('span'); vEl.className = 'tt-value';
    vEl.textContent = hit.isNA ? 'N/A (not applicable in this record\'s mode)'
      : hit.saturated ? '≥ ' + fmtNs(hit.valueNs) + '  (lower bound -- ts[] saturated at ~42.9s, not an exact reading)'
      : fmtNs(hit.valueNs) + (hit.negative ? '  (negative -- steps back)' : '');
    row.append(kEl, vEl); tooltip.append(row);
  }
  const addRow = (k, v) => {
    const row = document.createElement('div'); row.className = 'tt-row';
    const kEl = document.createElement('span'); kEl.className = 'tt-muted'; kEl.textContent = k;
    const vEl = document.createElement('span'); vEl.textContent = v;
    row.append(kEl, vEl); tooltip.append(row);
  };
  addRow('end-to-end', fmtNs(meta.e2e_ns));
  addRow('method', meta.method_name || ('#' + meta.method_id));
  addRow('trace_id', meta.trace_id);
  if (count > 1) addRow('column represents', fmtCount(count) + ' requests -- showing the slowest');

  tooltip.style.display = 'block';
  const ttRect = tooltip.getBoundingClientRect();
  let left = clientX + 14, top = clientY + 14;
  if (left + ttRect.width > window.innerWidth - 8) left = clientX - ttRect.width - 14;
  if (top + ttRect.height > window.innerHeight - 8) top = clientY - ttRect.height - 14;
  tooltip.style.left = left + 'px'; tooltip.style.top = top + 'px';
}

// ---------------------------------------------------------------------
// Statistics table (requirement 4): mean/P50/P90/P99/P99.9, for the
// sample's end-to-end latency and for each of the 33 segments, scoped to
// the current rank zoom by default, sortable by any column, defaulting
// to P99 descending (design doc / task's fixed decisions).
// ---------------------------------------------------------------------
function computeStatsRows(rankLo, rankHi, order, model) {
  const rows = [];
  for (let i33 = 0; i33 < 33; i33++) {
    const pairs = [];
    let naWeight = 0;
    for (let r = rankLo; r < rankHi; r++) {
      const idx = order[r];
      const raw = get33Raw(rowOf(idx), i33, model);
      const w = WEIGHTS[idx];
      if (raw === NA) { naWeight += w; continue; }
      pairs.push({ v: raw, w });
    }
    rows.push(Object.assign(
      { i33, name: NAMES33[i33], starred: STARRED33[i33] },
      weightedSummary(pairs, naWeight)));
  }
  return rows;
}

// The sample's end-to-end latency, summarised the same way and over the
// same scope as the segment rows above.
//
// Three things it deliberately does NOT depend on:
//   - the link model: e2e_ns is measured on the client, not assembled
//     from the two link halves, so models A and B give the same number;
//   - the legend: hiding a segment shortens the drawn bars, but the
//     request still took as long as it took;
//   - the latency-axis zoom, which crops the drawing only.
// It does follow the rank zoom, the negative-segment filter, and the
// downsampling weights, exactly as the segment rows do.
function computeE2eRow(rankLo, rankHi, order) {
  const pairs = [];
  for (let r = rankLo; r < rankHi; r++) {
    const idx = order[r];
    pairs.push({ v: IR.records_meta[idx].e2e_ns, w: WEIGHTS[idx] });
  }
  return Object.assign({ i33: -1, name: 'END-TO-END', starred: true, isE2e: true },
                       weightedSummary(pairs, 0));
}

const PCT_COLS = [
  { key: 'name', label: 'End-to-end / segment', numeric: false },
  { key: 'n', label: 'N', numeric: true },
  { key: 'naCount', label: 'N/A', numeric: true },
  { key: 'mean', label: 'Mean', numeric: true },
  { key: 'p50', label: 'P50', numeric: true },
  { key: 'p90', label: 'P90', numeric: true },
  { key: 'p99', label: 'P99', numeric: true },
  { key: 'p999', label: 'P99.9', numeric: true },
];

function renderTable() {
  const nVis = currentOrder().length;
  // "also fix" (fix-round review): this used to be a no-op ternary
  // (`state.tableScope === 'all' ? currentOrder() : currentOrder()`) --
  // both branches called the same function, so the condition did
  // nothing; tableScope only actually changes rankLo/rankHi below.
  const order = currentOrder();
  const rankLo = state.tableScope === 'all' ? 0 : state.rankLo;
  const rankHi = state.tableScope === 'all' ? nVis : state.rankHi;
  const rows = computeStatsRows(rankLo, rankHi, order, state.linkModel);
  const e2eRow = computeE2eRow(rankLo, rankHi, order);

  const sortCol = state.tableSort.col, dir = state.tableSort.dir === 'asc' ? 1 : -1;
  rows.sort((a, b) => {
    let va = a[sortCol], vb = b[sortCol];
    if (sortCol === 'name') { va = a.name; vb = b.name; return dir * va.localeCompare(vb); }
    if (va === null && vb === null) return 0;
    if (va === null) return 1; if (vb === null) return -1;
    return dir * (va - vb);
  });

  const headRow = document.getElementById('pctHeadRow');
  headRow.textContent = '';
  for (const col of PCT_COLS) {
    const th = document.createElement('th');
    th.textContent = col.label;
    if (col.key === sortCol) {
      th.classList.add('sorted');
      const arrow = document.createElement('span'); arrow.className = 'arrow';
      arrow.textContent = dir === 1 ? '▲' : '▼';
      th.append(arrow);
    }
    th.addEventListener('click', () => {
      if (state.tableSort.col === col.key) state.tableSort.dir = state.tableSort.dir === 'asc' ? 'desc' : 'asc';
      else state.tableSort = { col: col.key, dir: col.key === 'name' ? 'asc' : 'desc' };
      renderTable();
    });
    headRow.append(th);
  }

  const body = document.getElementById('pctBody');
  body.textContent = '';
  function appendRow(row) {
    const tr = document.createElement('tr');
    if (row.isE2e) tr.className = 'e2e-row';
    const nameTd = document.createElement('td'); nameTd.className = 'name-cell';
    if (!row.isE2e) {
      const sw = document.createElement('span'); sw.className = 'swatch'; sw.style.background = colorFor(row.i33);
      nameTd.append(sw);
    }
    const label = document.createElement('span');
    label.textContent = row.isE2e ? row.name : (row.starred ? '★ ' : '○ ') + row.name;
    nameTd.append(label);
    tr.append(nameTd);
    for (const col of ['n', 'naCount', 'mean', 'p50', 'p90', 'p99', 'p999']) {
      const td = document.createElement('td'); td.className = 'num';
      // The end-to-end value is a direct client-side measurement, so
      // "how many of these were N/A" has no meaning for it -- an em dash
      // rather than a 0, which would read as a count that was checked.
      td.textContent = (row.isE2e && col === 'naCount') ? '—'
        : (col === 'n' || col === 'naCount') ? fmtCount(row[col]) : fmtNs(row[col]);
      tr.append(td);
    }
    body.append(tr);
  }
  // Pinned first and deliberately outside the sort: it is the total the
  // 33 rows below decompose, not one more thing to rank against them.
  appendRow(e2eRow);
  for (const row of rows) appendRow(row);

  const scopeLabel = document.getElementById('tableScopeLabel');
  const scopeBtn = document.getElementById('scopeToggleBtn');
  // "also fix" (fix-round review): "whole dataset" on its own reads as
  // "every accepted request" -- when the negative-segment filter is on,
  // it's actually "every accepted request except those", which is a
  // materially different population for anyone reading the stats table.
  const hideNegSuffix = state.hideNeg ? ' (negative-segment requests hidden)' : '';
  // must-fix 1: state what the numbers describe. Percentiles/mean are
  // always weighted (computeStatsRows), which is a no-op when nothing
  // was downsampled (every weight is 1) but is NOT a no-op once
  // downsampling kicked in -- say so explicitly rather than leaving a
  // reader to assume "P50" means "the 50th of the rows on this page."
  const weightedSuffix = IS_DOWNSAMPLED
    ? ' -- weighted to estimate the full accepted population, not just the ' +
      fmtCount(N_TOTAL) + ' request(s) shown on this page (see the downsampling note above)'
    : '';
  if (state.tableScope === 'zoom') {
    const isFull = rankLo === 0 && rankHi === nVis;
    scopeLabel.textContent = (isFull
      ? 'scope: whole dataset (' + fmtCount(nVis) + ' requests)' + hideNegSuffix
      : 'scope: current zoom, ranks ' + fmtCount(rankLo) + '–' + fmtCount(rankHi - 1) + ' (' + fmtCount(rankHi - rankLo) + ' requests)' + hideNegSuffix)
      + weightedSuffix;
    scopeBtn.textContent = 'pin to whole-dataset stats';
  } else {
    scopeLabel.textContent = 'scope: whole dataset (' + fmtCount(nVis) + ' requests), ignoring current zoom' +
      hideNegSuffix + weightedSuffix;
    scopeBtn.textContent = 'follow zoom again';
  }
}
document.getElementById('scopeToggleBtn').addEventListener('click', () => {
  state.tableScope = state.tableScope === 'zoom' ? 'all' : 'zoom';
  renderTable();
});

// ---------------------------------------------------------------------
// Control wiring + top-level redraw
// ---------------------------------------------------------------------
function redrawAll() { drawChart(); renderTable(); }

document.getElementById('sortModeSel').addEventListener('change', (e) => {
  state.sortMode = e.target.value;
  state.rankLo = 0; state.rankHi = currentOrder().length;
  redrawAll();
});
document.getElementById('linkModelSel').addEventListener('change', (e) => {
  const wasBase = state.yView.lo === yRefFor(state.linkModel).base.lo &&
                  state.yView.hi === yRefFor(state.linkModel).base.hi;
  state.linkModel = e.target.value;
  // The two models can put the axis floor in different places (only a
  // negative segment pushes it below zero, and which segments go
  // negative is model-dependent). Follow the new model's default if the
  // axis was sitting on the old one's; otherwise keep the user's own
  // range, clamped into what the new model can show.
  const ref = yRefFor(state.linkModel);
  state.yView = wasBase
    ? { lo: ref.base.lo, hi: ref.base.hi }
    : {
        lo: Math.max(ref.bounds.lo, Math.min(state.yView.lo, ref.bounds.hi)),
        hi: Math.min(ref.bounds.hi, Math.max(state.yView.hi, ref.bounds.lo)),
      };
  redrawAll();
});
document.getElementById('hideNegChk').addEventListener('change', (e) => {
  state.hideNeg = e.target.checked;
  state.rankLo = 0; state.rankHi = currentOrder().length;
  redrawAll();
});
window.addEventListener('resize', () => drawChart());
if (window.matchMedia) {
  window.matchMedia('(prefers-color-scheme: dark)').addEventListener('change', () => redrawAll());
}

redrawAll();
</script>
</body>
</html>
"""


def render_html(ir, title=None):
    if title is None:
        stats = ir["stats"]
        title = (f"Latency trace: {stats['output_record_count']} requests, "
                 f"33-segment breakdown")
    payload = json.dumps(ir, separators=(",", ":"))
    # A stray "</script" inside a JSON string (a file path, a method name)
    # would terminate the embedding <script> tag early -- escape it the
    # standard way.
    payload = payload.replace("</", "<\\/")
    html = HTML_TEMPLATE.replace("__LT_IR_JSON__", payload)
    html = html.replace("__LT_TITLE__", title)
    return html


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__,
                                  formatter_class=argparse.RawDescriptionHelpFormatter)
    src = ap.add_argument_group("input (choose one)")
    src.add_argument("--client", help="client-side dump file (paired with --server)")
    src.add_argument("--server", help="server-side dump file (paired with --client)")
    src.add_argument("--ir", help="a JSON intermediate representation already produced by "
                                   "`merge.py ... -o ir.json`")
    ap.add_argument("-o", "--output", required=True, help="output HTML file path")
    ap.add_argument("--window-ms", type=float, default=1000.0,
                     help="model B sliding window size in ms (default 1000), only used "
                          "with --client/--server")
    ap.add_argument("--max-mb", type=float, default=12.0,
                     help="base64 blob size budget in MiB before downsampling kicks in "
                          "(default 12), only used with --client/--server")
    ap.add_argument("--title", help="override the page <title> / <h1>")
    args = ap.parse_args(argv)

    if bool(args.ir) == bool(args.client or args.server):
        ap.error("pass exactly one of --ir, or --client together with --server")
    if bool(args.client) != bool(args.server):
        ap.error("--client and --server must be given together")

    if args.ir:
        with open(args.ir) as f:
            ir = json.load(f)
        size_info = None
    else:
        ir, size_info = build_ir_from_dumps(args.client, args.server, args.window_ms, args.max_mb)

    html = render_html(ir, title=args.title)
    with open(args.output, "w") as f:
        f.write(html)

    stats = ir["stats"]
    print(f"wrote {args.output} ({len(html)} bytes)")
    print(f"records: accepted={stats['accepted_count']} shown={stats['output_record_count']}")
    if ir["downsample"]["applied"]:
        print(f"downsampling applied: {ir['downsample']}")
    if size_info:
        print(f"IR blob: {size_info['blob_raw_bytes']} raw bytes, "
              f"{size_info['blob_b64_bytes']} base64 bytes")
    return 0


if __name__ == "__main__":
    sys.exit(main())

