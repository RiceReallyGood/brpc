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
  .chart-wrap { position: relative; }
  #chartCanvas { width: 100%; height: 440px; display: block; cursor: crosshair; touch-action: none; }
  #selOverlay {
    position: absolute; top: 0; height: 440px; background: color-mix(in srgb, var(--hue-client-send) 18%, transparent);
    border-left: 1px solid var(--hue-client-send); border-right: 1px solid var(--hue-client-send);
    pointer-events: none; display: none;
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
      pipeline order and the bar's top is always exactly that request's end-to-end
      latency, whether every segment is positive or not.</p>
  </header>

  <section class="card" id="qualityBanner">
    <h2>Data quality</h2>
    <div class="banner-grid" id="bannerStats"></div>
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
    </div>
    <div class="legend-actions">
      <button id="legendAllBtn">show all</button>
      <button id="legendNoneBtn">hide all</button>
    </div>
    <div class="legend-groups" id="legendGroups"></div>
  </section>

  <section class="card">
    <h2>Cumulative latency, virtual rank axis</h2>
    <p class="table-scope">Drag a horizontal range on the chart to zoom into those ranks; the
      percentile table below follows. When a pixel column covers more than one request, the
      column shows that column's <em>slowest</em> request (by end-to-end latency) -- the
      hover tooltip says how many requests the column stands for.</p>
    <div class="chart-wrap">
      <canvas id="chartCanvas"></canvas>
      <div id="selOverlay"></div>
    </div>
    <div class="neg-row" id="negCallout"></div>
  </section>

  <section class="card">
    <div class="table-head-row">
      <h2 style="margin:0;">Per-segment statistics</h2>
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
// Constants derived from the IR's fixed schema (design doc sec.5): the 31
// shared items are always 7 client-send + 16 server + 8 client-recv, in
// that order (DECOMPOSITION_ITEMS_31 in merge.py). link_up/link_down are
// spliced in at positions 7 and 24 of the canonical 33-item order
// (ITEM_ORDER_33), matching sec.5's presentation order.
// ---------------------------------------------------------------------
const SEND_N = 7, SERVER_N = 16, RECV_N = 8;
const NAMES31 = IR.item_names_31;
const STARRED31 = IR.item_starred_31;
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
  for (let idx = 0; idx < N_TOTAL; idx++) {
    const row = rowOf(idx);
    let neg = false;
    for (let i33 = 0; i33 < 33; i33++) {
      const v = get33Raw(row, i33, model);
      if (v !== NA && v < 0) { neg = true; break; }
    }
    if (neg) { flags[idx] = 1; count++; }
  }
  return { flags, count };
}
const NEG_A = computeNegativeFlags('A');
const NEG_B = computeNegativeFlags('B');

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
  hidden: new Set(),
  tableScope: 'zoom', // 'zoom' | 'all'
  tableSort: { col: 'p99', dir: 'desc' },
};

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

  const REASON_LABEL = {
    unstamped_point: 'a checkpoint was never stamped (raw 0) on at least one side -- likely an un-instrumented code path',
    non_monotonic: "one side's decoded offsets went backwards within its own checkpoint sequence",
    negative_decomposition_item: 'at least one of the 31 shared segments, or the total link time, came out negative',
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
    return;
  }

  // y-domain: 0..maxE2e normally; extend below 0 only if the visible
  // window actually contains a record whose running cumulative dips
  // negative (rare -- see the negative-segment banner), so the axis
  // doesn't waste headroom on the common case.
  let maxE2e = 0;
  const model = state.linkModel;
  const negFlags = model === 'A' ? NEG_A.flags : NEG_B.flags;
  let minCumFloor = 0;
  for (let r = rankLo; r < rankHi; r++) {
    const idx = order[r];
    const e = IR.records_meta[idx].e2e_ns;
    if (e > maxE2e) maxE2e = e;
    if (negFlags[idx]) {
      const { segs } = computeBarSegments(idx);
      let mn = 0;
      for (const s of segs) if (!s.isNA && s.yLo < mn) mn = s.yLo;
      if (mn < minCumFloor) minCumFloor = mn;
    }
  }
  if (maxE2e <= 0) maxE2e = 1;
  const yDomainMin = minCumFloor, yDomainMax = maxE2e;
  const yRange = Math.max(1, yDomainMax - yDomainMin);
  const yScale = plotH / yRange;
  const toY = (ns) => marginT + (yDomainMax - ns) * yScale;
  const baselineY = toY(0);

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
  ctx.strokeStyle = getComputedStyle(document.documentElement).getPropertyValue('--baseline').trim();
  ctx.beginPath(); ctx.moveTo(marginL, baselineY); ctx.lineTo(marginL + plotW, baselineY); ctx.lineWidth = 1.5; ctx.stroke();

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
    if (anyNeg) {
      ctx.save();
      ctx.fillStyle = getComputedStyle(document.documentElement).getPropertyValue('--status-critical').trim();
      ctx.fillRect(x0, baselineY - 1.5, Math.max(1, x1 - x0), 3);
      ctx.restore();
    }
  }

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
    for (let r = rankLo; r < rankHi; r++) {
      const col = Math.min(colCount - 1, Math.floor((r - rankLo) / visibleCount * colCount));
      colCounts[col]++;
      const idx = order[r];
      const e = IR.records_meta[idx].e2e_ns;
      if (e > colMaxE2e[col]) { colMaxE2e[col] = e; colRepIdx[col] = idx; }
    }
    for (let c = 0; c < colCount; c++) {
      if (colRepIdx[c] < 0) continue;
      const x0 = marginL + c / colCount * plotW, x1 = marginL + (c + 1) / colCount * plotW;
      drawOneBar(colRepIdx[c], x0, x1);
    }
  }

  // x-axis caption
  ctx.textAlign = 'left'; ctx.textBaseline = 'alphabetic';
  ctx.fillStyle = getComputedStyle(document.documentElement).getPropertyValue('--text-muted').trim();
  ctx.font = '11px system-ui, sans-serif';
  const caption = (state.sortMode === 'latency' ? 'rank, sorted by end-to-end latency' : 'rank, sorted by trace_id (~chronological)')
    + '  --  showing ranks ' + fmtCount(rankLo) + '–' + fmtCount(rankHi - 1) + ' of ' + fmtCount(nVis);
  ctx.fillText(caption, marginL, cssH - 6);

  lastRender = { order, rankLo, rankHi, visibleCount, mode1to1, colCount, colRepIdx, colCounts, marginL, plotW, cssH };
  updateZoomInfo(order, nVis);
  updateNegCallout();
}

function updateZoomInfo(order, nVis) {
  const el = document.getElementById('zoomInfo');
  const rankLo = state.rankLo, rankHi = state.rankHi;
  const isFull = rankLo === 0 && rankHi === nVis;
  document.getElementById('resetZoomBtn').disabled = isFull;
  if (isFull) { el.textContent = 'showing all ' + fmtCount(nVis) + ' requests'; return; }
  let minE = Infinity, maxE = -Infinity;
  for (let r = rankLo; r < rankHi; r++) {
    const e = IR.records_meta[order[r]].e2e_ns;
    if (e < minE) minE = e; if (e > maxE) maxE = e;
  }
  el.textContent = 'zoomed: ranks ' + fmtCount(rankLo) + '–' + fmtCount(rankHi - 1) + ' (' + fmtCount(rankHi - rankLo) +
    ' requests, ' + fmtNs(minE) + '–' + fmtNs(maxE) + ')';
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
// Zoom: drag-select on the canvas (requirement 1), reset button.
// ---------------------------------------------------------------------
let dragStartPx = null;
canvas.addEventListener('pointerdown', (e) => {
  if (!lastRender) return;
  const rect = canvas.getBoundingClientRect();
  dragStartPx = e.clientX - rect.left;
  canvas.setPointerCapture(e.pointerId);
  selOverlay.style.display = 'block';
  selOverlay.style.left = dragStartPx + 'px';
  selOverlay.style.width = '0px';
});
canvas.addEventListener('pointermove', (e) => {
  const rect = canvas.getBoundingClientRect();
  const xCss = e.clientX - rect.left, yCss = e.clientY - rect.top;
  if (dragStartPx !== null) {
    const lo = Math.min(dragStartPx, xCss), hi = Math.max(dragStartPx, xCss);
    selOverlay.style.left = lo + 'px';
    selOverlay.style.width = Math.max(0, hi - lo) + 'px';
    tooltip.style.display = 'none';
    return;
  }
  hoverAt(xCss, yCss, e.clientX, e.clientY);
});
canvas.addEventListener('pointerleave', () => { if (dragStartPx === null) tooltip.style.display = 'none'; });
canvas.addEventListener('pointerup', (e) => {
  if (dragStartPx === null || !lastRender) return;
  const rect = canvas.getBoundingClientRect();
  const endPx = e.clientX - rect.left;
  const lo = Math.min(dragStartPx, endPx), hi = Math.max(dragStartPx, endPx);
  selOverlay.style.display = 'none';
  dragStartPx = null;
  if (hi - lo < 4) return; // treat as a click, not a drag
  const lr = lastRender;
  const frac0 = Math.max(0, Math.min(1, (lo - lr.marginL) / lr.plotW));
  const frac1 = Math.max(0, Math.min(1, (hi - lr.marginL) / lr.plotW));
  const newLo = lr.rankLo + Math.floor(frac0 * lr.visibleCount);
  const newHi = lr.rankLo + Math.max(newLo - lr.rankLo + 1, Math.ceil(frac1 * lr.visibleCount));
  state.rankLo = newLo; state.rankHi = Math.min(newHi, currentOrder().length);
  redrawAll();
});
document.getElementById('resetZoomBtn').addEventListener('click', () => {
  state.rankLo = 0; state.rankHi = currentOrder().length; redrawAll();
});

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

  // y-domain replicated from drawChart() to invert pixel->ns for hit
  // testing (kept in sync by construction: both read state + IR the
  // same way; recomputing here is cheap -- one record's worth of work).
  const cssH = lr.cssH, marginT = 12, marginB = 26;
  const plotH = Math.max(1, cssH - marginT - marginB);
  let maxE2e = 0;
  const negFlags = state.linkModel === 'A' ? NEG_A.flags : NEG_B.flags;
  let minCumFloor = 0;
  for (let r = lr.rankLo; r < lr.rankHi; r++) {
    const i2 = lr.order[r]; const e = IR.records_meta[i2].e2e_ns;
    if (e > maxE2e) maxE2e = e;
    if (negFlags[i2]) { const g = computeBarSegments(i2); for (const s of g.segs) if (!s.isNA && s.yLo < minCumFloor) minCumFloor = s.yLo; }
  }
  if (maxE2e <= 0) maxE2e = 1;
  const yRange = Math.max(1, maxE2e - minCumFloor);
  const yScale = plotH / yRange;
  const toY = (ns) => marginT + (maxE2e - ns) * yScale;
  const fromY = (y) => maxE2e - (y - marginT) / yScale;
  const nsAtCursor = fromY(yCss);

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
    vEl.textContent = hit.isNA ? 'N/A (not applicable in this record\'s mode)' : fmtNs(hit.valueNs) + (hit.negative ? '  (negative -- steps back)' : '');
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
// Percentile table (requirement 4): mean/P50/P90/P99/P99.9 per segment,
// scoped to the current zoom by default, sortable by any column,
// defaulting to P99 descending (design doc / task's fixed decisions).
// ---------------------------------------------------------------------
function linearPercentile(sortedAsc, p) {
  const n = sortedAsc.length;
  if (n === 0) return null;
  if (n === 1) return sortedAsc[0];
  const h = (n - 1) * p;
  const lo = Math.floor(h), hi = Math.ceil(h);
  if (lo === hi) return sortedAsc[lo];
  return sortedAsc[lo] + (sortedAsc[hi] - sortedAsc[lo]) * (h - lo);
}

function computeStatsRows(rankLo, rankHi, order, model) {
  const rows = [];
  for (let i33 = 0; i33 < 33; i33++) {
    const vals = [];
    let naCount = 0;
    for (let r = rankLo; r < rankHi; r++) {
      const idx = order[r];
      const raw = get33Raw(rowOf(idx), i33, model);
      if (raw === NA) { naCount++; continue; }
      vals.push(raw);
    }
    vals.sort((a, b) => a - b);
    const n = vals.length;
    const mean = n ? vals.reduce((s, v) => s + v, 0) / n : null;
    rows.push({
      i33, name: NAMES33[i33], starred: STARRED33[i33], n, naCount,
      mean, p50: linearPercentile(vals, 0.50), p90: linearPercentile(vals, 0.90),
      p99: linearPercentile(vals, 0.99), p999: linearPercentile(vals, 0.999),
    });
  }
  return rows;
}

const PCT_COLS = [
  { key: 'name', label: 'Segment', numeric: false },
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
  const order = state.tableScope === 'all' ? currentOrder() : currentOrder();
  const rankLo = state.tableScope === 'all' ? 0 : state.rankLo;
  const rankHi = state.tableScope === 'all' ? nVis : state.rankHi;
  const rows = computeStatsRows(rankLo, rankHi, order, state.linkModel);

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
  for (const row of rows) {
    const tr = document.createElement('tr');
    const nameTd = document.createElement('td'); nameTd.className = 'name-cell';
    const sw = document.createElement('span'); sw.className = 'swatch'; sw.style.background = colorFor(row.i33);
    const label = document.createElement('span'); label.textContent = (row.starred ? '★ ' : '○ ') + row.name;
    nameTd.append(sw, label);
    tr.append(nameTd);
    for (const col of ['n', 'naCount', 'mean', 'p50', 'p90', 'p99', 'p999']) {
      const td = document.createElement('td'); td.className = 'num';
      td.textContent = (col === 'n' || col === 'naCount') ? fmtCount(row[col]) : fmtNs(row[col]);
      tr.append(td);
    }
    body.append(tr);
  }

  const scopeLabel = document.getElementById('tableScopeLabel');
  const scopeBtn = document.getElementById('scopeToggleBtn');
  if (state.tableScope === 'zoom') {
    const isFull = rankLo === 0 && rankHi === nVis;
    scopeLabel.textContent = isFull
      ? 'scope: whole dataset (' + fmtCount(nVis) + ' requests)'
      : 'scope: current zoom, ranks ' + fmtCount(rankLo) + '–' + fmtCount(rankHi - 1) + ' (' + fmtCount(rankHi - rankLo) + ' requests)';
    scopeBtn.textContent = 'pin to whole-dataset stats';
  } else {
    scopeLabel.textContent = 'scope: whole dataset (' + fmtCount(nVis) + ' requests), ignoring current zoom';
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
  state.linkModel = e.target.value;
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

