# Remote build host (suzhou950)

This machine (WSL x86_64) cannot build brpc: it lacks protobuf / gflags /
leveldb / protoc. All building and testing happens on `suzhou950`
(aarch64, 384 cores). This machine only edits code and holds git history.

## Canonical build/test command

Every later task's "run the tests" step starts with this:

```bash
./tools/latency_trace/sync950.sh && ssh suzhou950 'cd ~/brpc-lt/test && make brpc_latency_trace_unittest -j64 && ./brpc_latency_trace_unittest'
```

Any plan step written as `cd test && make ... && ./...` should be read as
this remote form. Edit locally, build/run on suzhou950, commit locally.

## One-time setup / regenerating config.mk

`sync950.sh` only pushes `src test tools Makefile config_brpc.sh
CMakeLists.txt` — it never touches `config.mk`, so a `config.mk` generated
once in `~/brpc-lt` survives every later sync. You only need to
(re)generate it after a fresh clone of `~/brpc-lt` or if it's ever deleted:

```bash
ssh suzhou950 'cd ~/brpc-lt && ./config_brpc.sh --headers="/usr/include/gtest /usr/include" --libs=/usr/lib64'
```

**Do not use `--headers=/usr/include --libs=/usr/lib`** (the values that
look like the obvious default) — both are wrong on this host:

- `--libs=/usr/lib64`, not `/usr/lib`: gflags/protobuf/leveldb/absl `.so`/`.a`
  files live under `/usr/lib64` on this openEuler aarch64 host. `/usr/lib`
  is a near-empty separate directory. Passing `/usr/lib` makes
  `config_brpc.sh`'s `find_dir_of_header_or_die` fail *inside a `$(...)`
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

## Known open issue: gperftools is missing on suzhou950

`test/Makefile` unconditionally sets `NEED_GPERFTOOLS=1`, which every
test target depends on via `config.mk`'s
`$(error "Fail to find gperftools")` guard. `gperftools` (providing
`libtcmalloc_and_profiler`) is not installed on suzhou950 and is not in
this host's originally-verified dependency list. It exists in the
openEuler repo (`gperftools`, `gperftools-devel`, `gperftools-libs`) and
the repo is reachable directly (no xray proxy needed), but installing it
needs `sudo`, which requires a password not available in this session.

Until this is resolved, `make brpc_<name>_unittest` on suzhou950 will
fail with `"Fail to find gperftools"`. See `task-0-report.md` in the SDD
task folder for full diagnostics and options.
