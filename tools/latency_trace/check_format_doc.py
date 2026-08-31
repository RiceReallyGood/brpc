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
"""Guards against src/brpc/latency_trace.h and the design doc silently
drifting apart on the handful of numbers/names an offline parser (merge.py)
would trust blindly and get wrong in ways nothing flags.

This has already happened twice on this branch (the calibration pair
documented as CLOCK_REALTIME when the code uses monotonic, and the magic
constant's comment wrong three separate ways) -- both caught by accident,
not by any check. This script is that check.

It does NOT try to understand semantics. It extracts, from
src/brpc/latency_trace.h:
  - sizeof(LatencyTraceRecord) and sizeof(LatencyTraceFileHeader), from
    their static_assert()s
  - the LT_FILE_MAGIC constant's value
  - LatencyTracePoint's names (LT_C_*/LT_S_* only) and count

...and the corresponding numbers/names the design doc claims, then diffs
them. Any mismatch is a real, silent divergence between what merge.py's
author would read and what the on-disk format actually is -- exit
non-zero and name exactly what disagreed.

Usage: tools/latency_trace/check_format_doc.py
       (run from anywhere; paths are resolved relative to this file)
"""
import os
import re
import sys

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
REPO_ROOT = os.path.abspath(os.path.join(SCRIPT_DIR, "..", ".."))
HEADER_PATH = os.path.join(REPO_ROOT, "src", "brpc", "latency_trace.h")
DOC_PATH = os.path.join(
    REPO_ROOT, "docs", "superpowers", "specs",
    "2026-08-29-brpc-latency-trace-design.md")


class Mismatch(object):
    def __init__(self, what, code_value, doc_value):
        self.what = what
        self.code_value = code_value
        self.doc_value = doc_value

    def __str__(self):
        return "%s: code says %r, doc says %r" % (
            self.what, self.code_value, self.doc_value)


def read(path):
    with open(path, "r", encoding="utf-8") as f:
        return f.read()


# ---------------------------------------------------------------------------
# Extraction from latency_trace.h
# ---------------------------------------------------------------------------

def extract_code_sizeof(header_text, struct_name):
    """sizeof(X) == N from a static_assert(sizeof(X) == N, ...)."""
    m = re.search(
        r"static_assert\(\s*sizeof\(" + re.escape(struct_name) +
        r"\)\s*==\s*(\d+)", header_text)
    if not m:
        raise RuntimeError(
            "could not find static_assert(sizeof(%s) == N, ...) in %s" %
            (struct_name, HEADER_PATH))
    return int(m.group(1))


def extract_code_magic(header_text):
    """The LT_FILE_MAGIC literal, as the exact hex text (case-insensitive
    compare is done by the caller) -- textual match is deliberate: it
    catches a value edit on either side without this script having to
    re-derive the byte-order story itself."""
    m = re.search(
        r"const\s+uint64_t\s+LT_FILE_MAGIC\s*=\s*(0[xX][0-9A-Fa-f]+)ULL",
        header_text)
    if not m:
        raise RuntimeError(
            "could not find `const uint64_t LT_FILE_MAGIC = 0x...ULL;` in "
            "%s" % HEADER_PATH)
    return int(m.group(1), 16)


def extract_code_points(header_text):
    """Returns (ordered list of point names in 'c_snake_case'/'s_snake_case'
    form, total count) from the LatencyTracePoint enum. LT_POINT_COUNT
    itself is excluded (it's the sentinel, not a point)."""
    m = re.search(
        r"enum\s+LatencyTracePoint\s*\{(.*?)\};", header_text, re.DOTALL)
    if not m:
        raise RuntimeError(
            "could not find `enum LatencyTracePoint { ... };` in %s" %
            HEADER_PATH)
    body = m.group(1)
    # Strip // comments (the enum's leading block comment and inline notes)
    body = re.sub(r"//.*", "", body)
    names = re.findall(r"\bLT_([A-Z0-9_]+)\b", body)
    if not names or names[-1] != "POINT_COUNT":
        raise RuntimeError(
            "LatencyTracePoint enum did not end with LT_POINT_COUNT as "
            "expected -- got tail %r; check the regex against a possibly "
            "restructured enum" % (names[-3:] if len(names) >= 3 else names))
    point_names = names[:-1]  # drop POINT_COUNT sentinel
    return point_names, len(point_names)


def code_name_to_doc_name(enum_tail):
    """LT_C_REQ_PAYLOAD_SER_START's tail 'C_REQ_PAYLOAD_SER_START' ->
    ('C', 'req_payload_ser_start'), matching the design doc's per-point
    table rows (role prefix in the ID column, lowercase snake_case name in
    the name column)."""
    role, _, rest = enum_tail.partition("_")
    return role, rest.lower()


# ---------------------------------------------------------------------------
# Extraction from the design doc
# ---------------------------------------------------------------------------

def extract_doc_record_sizeof(doc_text):
    m = re.search(
        r"记录结构.*?POD.*?对齐后\s*(\d+)\s*字节", doc_text)
    if not m:
        raise RuntimeError(
            "could not find the record's sizeof claim "
            "('记录结构...POD...对齐后 N 字节') in %s" % DOC_PATH)
    return int(m.group(1))


def extract_doc_header_sizeof(doc_text):
    m = re.search(
        r"LatencyTraceFileHeader`\*\*.*?POD.*?(\d+)\s*字节固定", doc_text)
    if not m:
        raise RuntimeError(
            "could not find the file header's sizeof claim "
            "('LatencyTraceFileHeader...POD...N 字节固定') in %s" % DOC_PATH)
    return int(m.group(1))


def extract_doc_magic(doc_text):
    m = re.search(
        r"LT_FILE_MAGIC.{0,40}?(0[xX][0-9A-Fa-f]+)", doc_text)
    if not m:
        raise RuntimeError(
            "could not find an `LT_FILE_MAGIC ... 0x...` mention in %s" %
            DOC_PATH)
    return int(m.group(1), 16)


def extract_doc_points(doc_text):
    """Pulls every '| C## | `name` | ...' / '| S## | `name` | ...' row out
    of section 4 (the point list). Returns an ordered list of (role, name)
    pairs in the order the doc lists them, which should equal the enum's
    declaration order (client block, then server block)."""
    # Scope to section 4 ("## 4. ...") up to section 5 ("## 5. ...") so a
    # stray table elsewhere in the doc can't be picked up by accident.
    m = re.search(r"##\s*4\.\s.*?(?=\n##\s*5\.\s)", doc_text, re.DOTALL)
    if not m:
        raise RuntimeError(
            "could not find section 4 (point list) in %s" % DOC_PATH)
    section = m.group(0)
    rows = re.findall(
        r"^\|\s*([CS])(\d+)\s*\|\s*`([a-z0-9_]+)`\s*\|", section, re.MULTILINE)
    if not rows:
        raise RuntimeError(
            "found section 4 but no '| C01 | `name` | ...' rows in it -- "
            "table format may have changed; update the regex in %s" %
            os.path.basename(__file__))
    return [(role, name) for role, _id, name in rows]


# ---------------------------------------------------------------------------
# Comparison
# ---------------------------------------------------------------------------

def check():
    header_text = read(HEADER_PATH)
    doc_text = read(DOC_PATH)

    mismatches = []

    # 1. sizeof(LatencyTraceRecord)
    code_record_size = extract_code_sizeof(header_text, "LatencyTraceRecord")
    doc_record_size = extract_doc_record_sizeof(doc_text)
    if code_record_size != doc_record_size:
        mismatches.append(Mismatch(
            "sizeof(LatencyTraceRecord)", code_record_size, doc_record_size))

    # 2. sizeof(LatencyTraceFileHeader)
    code_header_size = extract_code_sizeof(
        header_text, "LatencyTraceFileHeader")
    doc_header_size = extract_doc_header_sizeof(doc_text)
    if code_header_size != doc_header_size:
        mismatches.append(Mismatch(
            "sizeof(LatencyTraceFileHeader)", code_header_size,
            doc_header_size))

    # 3. LT_FILE_MAGIC
    code_magic = extract_code_magic(header_text)
    doc_magic = extract_doc_magic(doc_text)
    if code_magic != doc_magic:
        mismatches.append(Mismatch(
            "LT_FILE_MAGIC", hex(code_magic), hex(doc_magic)))

    # 4. LatencyTracePoint: count and names, in order
    code_point_tails, code_point_count = extract_code_points(header_text)
    code_points = [code_name_to_doc_name(t) for t in code_point_tails]
    doc_points = extract_doc_points(doc_text)

    if code_point_count != len(doc_points):
        mismatches.append(Mismatch(
            "LatencyTracePoint count vs. section-4 row count",
            code_point_count, len(doc_points)))

    for i in range(min(len(code_points), len(doc_points))):
        code_role, code_name = code_points[i]
        doc_role, doc_name = doc_points[i]
        if code_role != doc_role or code_name != doc_name:
            mismatches.append(Mismatch(
                "point #%d (0-based)" % i,
                "%s/%s (LT_%s)" % (
                    code_role, code_name, code_point_tails[i]),
                "%s/%s" % (doc_role, doc_name)))

    if len(code_points) != len(doc_points):
        longer_side = "code" if len(code_points) > len(doc_points) else "doc"
        extra = (code_points[len(doc_points):] if longer_side == "code"
                 else doc_points[len(code_points):])
        mismatches.append(Mismatch(
            "point list length (%s has extra entries)" % longer_side,
            len(code_points), len(doc_points)))

    return mismatches


def main():
    try:
        mismatches = check()
    except RuntimeError as e:
        sys.stderr.write("check_format_doc.py: extraction failed: %s\n" % e)
        return 2

    if mismatches:
        sys.stderr.write(
            "check_format_doc.py: %d mismatch(es) between "
            "src/brpc/latency_trace.h and the design doc:\n" %
            len(mismatches))
        for m in mismatches:
            sys.stderr.write("  - %s\n" % m)
        return 1

    print("check_format_doc.py: OK -- record sizeof, header sizeof, "
          "LT_FILE_MAGIC, and all point names/order/count agree between "
          "src/brpc/latency_trace.h and the design doc.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
