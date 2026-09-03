# Remote build host (suzhou950)

This machine (WSL x86_64) cannot build brpc: it lacks protobuf / gflags /
leveldb / protoc. All building and testing happens on `suzhou950`
(aarch64, 384 cores). This machine only edits code and holds git history.

## Canonical build/test command

Every later task's "run the tests" step starts with this:

```bash
./tools/latency_trace/sync950.sh && ssh suzhou950 'cd ~/brpc-lt/test && make NEED_GPERFTOOLS=0 brpc_latency_trace_unittest -j64 && ./brpc_latency_trace_unittest'
```

Any plan step written as `cd test && make ... && ./...` should be read as
this remote form. Edit locally, build/run on suzhou950, commit locally.

`NEED_GPERFTOOLS=0` is a deliberate, permanent part of this recipe — see
"gperftools: deliberately not installed" below for why, and for what it
does and does not cover.

## Overriding host/destination: `LT_BUILD_HOST` / `LT_BUILD_DIR`

`sync950.sh` reads two optional environment variables:

- `LT_BUILD_HOST` (default `suzhou950`) — the ssh target.
- `LT_BUILD_DIR` (default `brpc-lt`, i.e. `~/brpc-lt` on the remote) — the
  destination directory, given as a path **on the remote host**.

```bash
LT_BUILD_HOST=other-host LT_BUILD_DIR=brpc-lt-2 ./tools/latency_trace/sync950.sh
```

**`LT_BUILD_DIR` must stay relative (or be an explicit remote-absolute
path), never a `~`-prefixed one, even though the default looks like it
should support that.** The default is the bare word `brpc-lt` — no
tilde — precisely so it resolves correctly: a non-interactive
`ssh host cmd` starts in the remote user's home directory, so a plain
relative path always lands under the *remote* `$HOME` with no tilde
expansion needed anywhere. If you instead write
`LT_BUILD_DIR=~/other-dir ./sync950.sh`, this local shell expands the
`~` immediately, against your *local* `$HOME`, before the script ever
runs — turning `~/other-dir` into e.g. `/home/alice/other-dir` locally,
which then gets sent to the remote host as an absolute path that has
nothing to do with the remote home. Without a guard, that would sync
"successfully" to a nonsense location on the remote filesystem with no
error at all. `sync950.sh` detects this specific case — a `LT_BUILD_DIR`
that already starts with your local `$HOME` — and refuses to run,
printing an explanation, instead of syncing to the wrong place.

## One-time setup / regenerating config.mk

`sync950.sh` only pushes build inputs — `src test tools Makefile
config_brpc.sh CMakeLists.txt cmake config.h.in RELEASE_VERSION` plus the
Bazel set (`.bazelrc BUILD.bazel MODULE.bazel WORKSPACE WORKSPACE.bzlmod
bazel registry`) — it never touches `config.mk`, so a `config.mk` generated
once in `~/brpc-lt` survives every later sync. You only need to
(re)generate it after a fresh clone of `~/brpc-lt` or if it's ever deleted:

```bash
./config_brpc.sh --headers="/usr/include/gtest /usr/include" --libs=/usr/lib64
```

**Do not use `--headers=/usr/include --libs=/usr/lib`** (the values that
look like the obvious default) — both are wrong on this host:

- `--libs=/usr/lib64`, not `/usr/lib`: gflags/protobuf/leveldb/absl `.so`/`.a`
  files live under `/usr/lib64` on this openEuler aarch64 host. `/usr/lib`
  is a near-empty separate directory. Passing `/usr/lib` makes
  `config_brpc.sh`'s `find_dir_of_lib_or_die` fail *inside a `$(...)`
  subshell*, so the `exit 1` only kills the subshell — the top-level
  script does not stop, it just silently records an empty lib dir. The
  result is a `config.mk` with bare `-l -l` (no library name) in
  `DYNAMIC_LINKINGS` where `-lgflags -lprotobuf` should be, which fails to
  link.
- `--headers="/usr/include/gtest /usr/include"`, not just `/usr/include`:
  this host has both `gtest-devel` (plain gtest, whose `.so` is what
  `-lgtest` actually links against) and `llvm-googletest` (headers under
  `/usr/include/llvm-gtest/`) installed side by side. `config_brpc.sh`
  finds `gtest/gtest.h` via `find /usr/include -path '*/gtest/gtest.h' |
  head -n1`, which is filesystem-order-dependent and on this host picks
  the llvm-gtest copy. Combined with a GCC behavior where an explicit
  `-I` that duplicates one of the compiler's own default system include
  directories (`/usr/include` is one) gets silently demoted to the end of
  the search list, the narrower `-I/usr/include/llvm-gtest/` path ends up
  searched *before* `-I/usr/include/`, so `<gtest/gtest.h>` resolves to
  LLVM's fork. LLVM's gtest fork routes assertion messages through
  `llvm::raw_ostream`, which is not in the link line, producing undefined
  references to `llvm::raw_ostream`/`raw_os_ostream` at link time — a
  test-only failure (the main `libbrpc.a`/`.so` build is unaffected,
  since it never touches gtest headers). Listing `/usr/include/gtest` as
  a search root before `/usr/include` makes the `gtest/gtest.h` lookup
  resolve to the plain copy deterministically, and `config.mk` ends up
  with a normal `HDRS=/usr/include/` (no `llvm-gtest` entry at all).

With these two arguments, `config_brpc.sh` auto-detects protobuf 25.1
(>= v22) on its own and prints:

```
[OK]   Found protobuf version 4025001 (>= v22, using C++17 with abseil)
```

and generates `CXXFLAGS=-std=c++17` plus the full `-labsl_*` link list —
**no manual edit of `config.mk` is needed or wanted.** If `config.mk` ever
needs regenerating, always go through `config_brpc.sh`; do not hand-patch
the generated file.

## gperftools: deliberately not installed

`gperftools` (providing `libtcmalloc_and_profiler`) is not installed on
suzhou950 and was not in this host's originally-verified dependency
list. `test/Makefile` hardcodes `NEED_GPERFTOOLS=1` by default, which
every test target depends on via `config.mk`'s
`$(error "Fail to find gperftools")` guard, so an un-overridden
`make brpc_<name>_unittest` fails with `"Fail to find gperftools"`.

The package does exist in the openEuler repo (`gperftools`,
`gperftools-devel`, `gperftools-libs`, confirmed via `dnf --cacheonly
search`) and the repo is reachable directly (no xray proxy needed), but
installing it needs `sudo`, and no sudo password is available in this
environment. Rather than block the whole plan on that, the canonical
recipe above passes `NEED_GPERFTOOLS=0` on the `make` command line,
which is a standard, reversible Make variable override (no file is
patched) that skips linking `-ltcmalloc_and_profiler`.

CMake has no equivalent knob: `test/CMakeLists.txt` links
`${GPERFTOOLS_LIBRARIES}` unconditionally, so the configure step fails with
`GPERFTOOLS_TCMALLOC_AND_PROFILER (ADVANCED) ... set to NOTFOUND`. The
equivalent override is to blank that cache variable on the command line:

```bash
cmake -DGPERFTOOLS_TCMALLOC_AND_PROFILER= -DBUILD_UNIT_TESTS=ON -DDOWNLOAD_GTEST=ON ..
```

`DOWNLOAD_GTEST=ON` is required too: 950 has `libgtest.so` but not the
googletest *sources*, and CMake wants sources (`add_subdirectory`). The
download is a `git clone` of googletest, so `source ~/proxy.env` first.

Bazel needs neither — it builds gtest and tcmalloc from its own module
deps — but it must run in a tree with no `*.pb.h` left over from a `make`
build, or the sandbox picks those up and fails with "generated by a newer
version of protoc". Use a second destination for it, e.g.
`LT_BUILD_DIR=brpc-bz ./tools/latency_trace/sync950.sh`.

**What this does and does not cover:** the only targets this plan needs
are `brpc_latency_trace_unittest` (all later tasks) and
`brpc_controller_unittest` (smoke check) — neither calls gperftools
profiler APIs directly, and `brpc_controller_unittest` is verified to
link and pass 5/5 with this override. It is not a safe blanket
assumption for every test binary in `test/`: any unittest that calls
gperftools APIs directly (profiler start/stop, heap profiler, etc.)
would still fail to link under `NEED_GPERFTOOLS=0`, so don't lift this
override onto an arbitrary `brpc_*_unittest` target without checking it
first.

**Consequence for benchmarks, not unit tests:** a binary built without
tcmalloc has different allocation/latency characteristics than one with
it (tcmalloc's fast paths and thread caches materially change alloc
latency). Unit test correctness is unaffected, but before running the
*real latency benchmark* (as opposed to `gtest` unit tests), gperftools
should be installed for representative numbers. That's a call for
whoever runs the benchmark, later, with a sudo-capable session — not a
blocker now.
