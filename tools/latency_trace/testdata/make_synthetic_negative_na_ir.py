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
"""tools/latency_trace/testdata/make_synthetic_negative_na_ir.py

Regenerates synthetic_negative_na_ir.json, committed alongside
client.dump/server.dump for the same reason: the fix-round review found
that neither the negative-segment hatch path (fixed for real in
dfe0d0b7) nor the RDMA-polling N/A path had ever been exercised by a
dump merge.py actually produced -- both were only reachable by
hand-editing a live page. Before the fix-round's must-fix 2
(--low-concurrency, default off), every record with a negative segment
was unconditionally rejected, so even a from-scratch capture couldn't
reach the hatch path; that's what this fixture and
test_render.py::TestSyntheticNegativeAndNA now cover instead.

Built through the REAL merge.py pipeline (synthetic byte-level dump
records --> parse_dump --> merge_dumps --> build_intermediate_representation),
not a hand-authored IR -- so it's a regression test of the actual
parsing/merge contract, the same reasoning test_merge.py's own module
docstring gives for building synthetic fixtures this way.

Usage: python3 make_synthetic_negative_na_ir.py
(writes synthetic_negative_na_ir.json next to this script)
"""
import json
import os
import sys

THIS_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.dirname(THIS_DIR))
import merge  # noqa: E402
import test_merge as tm  # noqa: E402 (reuses its synthetic-fixture builders)


def build():
    # Record A: a small, purely-arithmetic negative link_total (design doc
    # sec.6.1's expected phenomenon at real concurrency) -- same recipe as
    # test_merge.py's test_negative_decomposition_item_via_small_negative_
    # link_total. Individually monotonic and fully stamped on both sides.
    a_client_ts = tm.sequential_client_ts(step=100, wake_gap=200)
    a_server_ts = tm.sequential_server_ts(step=2000)
    a_client_bytes = tm.make_record_bytes(0xA000000000000001, merge.LT_ROLE_CLIENT, a_client_ts)
    a_server_bytes = tm.make_record_bytes(0xA000000000000001, merge.LT_ROLE_SERVER, a_server_ts)

    # Record B: RDMA polling-mode client (design doc sec.8.5) -- C09/C10
    # (wake/onedge_start) are LT_TS_NOT_APPLICABLE, not 0. Exercises the
    # N/A rendering path (dashed tick, "N/A" tooltip) for
    # cli_wake_to_onedge/cli_onedge_to_read, and -- since the fix-round
    # RDMA fix -- still produces a real link_total via the read_start
    # fallback rather than going N/A itself.
    # step asymmetry (client 1000, server 100) keeps this record's
    # link_total POSITIVE (C11-C08 spans 3 client-side steps = 3000
    # ticks; the server's full S01->S17 span is 16*100 = 1600 ticks) --
    # deliberately, so this record exercises the N/A rendering path in
    # isolation, distinct from record A's negative-link_total path above.
    # (A uniform step on both sides would make this record ALSO go
    # negative via the same arithmetic sequential_client_ts's own
    # docstring warns about, conflating the two scenarios into one
    # record.)
    b_client_ts = tm.sequential_client_ts(step=1000)
    b_client_ts[merge.C_WAKE] = merge.TS_NOT_APPLICABLE
    b_client_ts[merge.C_ONEDGE_START] = merge.TS_NOT_APPLICABLE
    b_server_ts = tm.sequential_server_ts(step=100)
    b_client_bytes = tm.make_record_bytes(0xB000000000000002, merge.LT_ROLE_CLIENT, b_client_ts)
    b_server_bytes = tm.make_record_bytes(0xB000000000000002, merge.LT_ROLE_SERVER, b_server_ts)

    client_data = tm.make_dump([a_client_bytes, b_client_bytes],
                                method_names=("example.EchoService.Echo",))
    server_data = tm.make_dump([a_server_bytes, b_server_bytes],
                                method_names=("example.EchoService.Echo",))

    cpath = os.path.join(THIS_DIR, "_synthetic_c.dump.tmp")
    spath = os.path.join(THIS_DIR, "_synthetic_s.dump.tmp")
    with open(cpath, "wb") as f:
        f.write(client_data)
    with open(spath, "wb") as f:
        f.write(server_data)
    try:
        client_dump = merge.parse_dump(cpath)
        server_dump = merge.parse_dump(spath)
        # low_concurrency=False (the default merge.py now ships): record A's
        # negative link_total must NOT be excluded, or this fixture would be
        # testing the exact bug must-fix 2 fixed.
        result = merge.merge_dumps(client_dump, server_dump)
        assert len(result["accepted"]) == 2, result["reject_counts"]
        ir, _ = merge.build_intermediate_representation(client_dump, server_dump, result)
        return ir
    finally:
        os.remove(cpath)
        os.remove(spath)


if __name__ == "__main__":
    ir = build()
    out_path = os.path.join(THIS_DIR, "synthetic_negative_na_ir.json")
    with open(out_path, "w") as f:
        json.dump(ir, f, indent=1, sort_keys=True)
        f.write("\n")
    print(f"wrote {out_path}")
