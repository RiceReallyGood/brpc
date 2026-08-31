#!/bin/bash
# tools/latency_trace/capture.sh
#
# Drives a real client/server baidu_std EchoService exchange against a
# latency-trace-instrumented (BRPC_LATENCY_TRACE=1) build on the remote
# build host, with each side dumping its ring buffer to its own file, and
# pulls both dumps back to this machine. This is the first real dump this
# feature has ever produced -- everything before this was unit tests.
#
# Usage:
#   LT_BUILD_DIR=brpc-lt-traced ./tools/latency_trace/capture.sh [out_dir]
#
# Env vars (all optional):
#   LT_BUILD_HOST      remote build host (default: suzhou950)
#   LT_BUILD_DIR        remote tree, relative to remote $HOME
#                        (default: brpc-lt-traced -- must already be a
#                        TRACED, non-RDMA config; this script does not
#                        configure it, only builds it)
#   CAPTURE_PORT        TCP port for the echo server (default: 9541)
#   CAPTURE_REQUESTS    number of sequential requests to drive on the
#                        client's single channel before stopping it
#                        (default: 3000)
#
# out_dir (positional, optional): local directory to copy the two dump
# files into (default: /tmp/lt_capture_out). Not committed -- this script
# is the reproducible artifact, not its output.
#
# What this does NOT do: configure the remote tree (see sync950.sh's
# comments -- that's a one-time `config_brpc.sh` step done by hand so two
# trees don't fight over config.mk), or run merge.py/render.py.
set -euo pipefail

HOST="${LT_BUILD_HOST:-suzhou950}"
DEST="${LT_BUILD_DIR:-brpc-lt-traced}"
PORT="${CAPTURE_PORT:-9541}"
N="${CAPTURE_REQUESTS:-3000}"
OUT_DIR="${1:-/tmp/lt_capture_out}"

cd "$(dirname "$0")/../.."

echo "== [1/6] syncing core tree to $HOST:$DEST =="
LT_BUILD_DIR="$DEST" ./tools/latency_trace/sync950.sh

echo "== [2/6] syncing example/echo_c++ (not covered by sync950.sh) =="
tar czf - example/echo_c++ \
  | ssh "$HOST" "mkdir -p \"$DEST/example\" && tar xzf - -C \"$DEST\" && \
      find \"$DEST/example/echo_c++\" -type f -exec touch {} +"

echo "== [3/6] rebuilding libbrpc.a on $HOST (clean, NEED_GPERFTOOLS=0) =="
ssh "$HOST" "cd \"$DEST\" && make clean >/tmp/lt_capture_clean.log 2>&1 && \
    make -j\"\$(nproc)\" NEED_GPERFTOOLS=0 >/tmp/lt_capture_build.log 2>&1" \
  || { echo "capture.sh: remote library build failed -- see $HOST:/tmp/lt_capture_build.log" >&2; exit 1; }

# Confirm we actually rebuilt a traced object, not a stale one from a
# previous config -- this project has hit nine distinct stale-artifact
# traps (see CLAUDE.md/plan notes), so don't just trust a zero exit code.
ssh "$HOST" "cd \"$DEST\" && nm libbrpc.a 2>/dev/null | grep -q LatencyTraceBuffer" \
  || { echo "capture.sh: libbrpc.a on $HOST has no LatencyTraceBuffer symbols -- not a traced build" >&2; exit 1; }

echo "== [4/6] building echo_c++ example against it =="
# The example's own Makefile hardcodes -std=c++14; the library and its
# headers (via config.mk's CPPFLAGS) are compiled -std=c++17, and this
# host's abseil headers do not compile clean under c++14. Bump it to
# match -- this only touches the remote, disposable copy.
ssh "$HOST" "cd \"$DEST/example/echo_c++\" && \
    sed -i 's/-std=c++14/-std=c++17/' Makefile && \
    make clean >/tmp/lt_capture_ex_clean.log 2>&1 && \
    make NEED_GPERFTOOLS=0 -j8 >/tmp/lt_capture_ex_build.log 2>&1" \
  || { echo "capture.sh: remote example build failed -- see $HOST:/tmp/lt_capture_ex_build.log" >&2; exit 1; }

echo "== [5/6] running server + client ($N sequential requests on one channel, port $PORT) =="
ssh "$HOST" bash -s -- "$DEST" "$PORT" "$N" <<'REMOTE'
set -uo pipefail
DEST="$HOME/$1"; PORT="$2"; N="$3"
cd "$DEST/example/echo_c++"

# Clean up a stale run on the same port, if any.
pkill -f "echo_server -port=$PORT " 2>/dev/null || true
sleep 0.2

rm -f "$DEST/lt_server.dump" "$DEST/lt_client.dump" \
      /tmp/lt_capture_server.log /tmp/lt_capture_client.log

# -graceful_quit_on_sigterm is NOT set: brpc only installs a SIGTERM
# handler when that flag is true, so plain SIGTERM here would hit each
# process's default disposition and kill it without ever returning from
# main() -- which means the atexit-registered Dump() never runs and the
# file is silently never written. SIGINT's handler is unconditional
# (installed the first time IsAskedToQuit() runs, which both echo_client's
# request loop and Server::RunUntilAskedToQuit() call every iteration),
# so use that instead. Verified experimentally: SIGTERM produced zero
# dump files, SIGINT produced correctly-sized ones.
./echo_server -port="$PORT" -idle_timeout_s=-1 \
    -latency_trace_enabled=true -latency_trace_dump_path="$DEST/lt_server.dump" \
    > /tmp/lt_capture_server.log 2>&1 &
SERVER_PID=$!
sleep 1
if ! kill -0 "$SERVER_PID" 2>/dev/null; then
    echo "capture.sh: echo_server died before client could connect -- see /tmp/lt_capture_server.log" >&2
    cat /tmp/lt_capture_server.log >&2
    exit 1
fi

# One channel, sequential (blocking, done=nullptr) calls, no retries --
# outstanding=1 throughout, so the steady-state write path is exercised
# request by request rather than pipelined, matching what the design doc
# (sec.10.1's end-to-end row) calls "low concurrency": the only condition
# under which every decomposition item is asserted non-negative.
./echo_client -server=127.0.0.1:"$PORT" -interval_ms=0 -timeout_ms=2000 -max_retry=0 \
    -latency_trace_enabled=true -latency_trace_dump_path="$DEST/lt_client.dump" \
    > /tmp/lt_capture_client.log 2>&1 &
CLIENT_PID=$!

# Poll the client's own log for completed round trips instead of a fixed
# sleep -- interval_ms=0 makes it run as fast as the loopback round trip
# allows, so a fixed sleep would either wildly overshoot N or undershoot
# it depending on host load.
count=0
for _ in $(seq 1 1200); do
    count=$(grep -c 'Received response' /tmp/lt_capture_client.log 2>/dev/null || true)
    count=${count:-0}
    if [ "$count" -ge "$N" ]; then break; fi
    sleep 0.05
done
if [ "$count" -lt "$N" ]; then
    echo "capture.sh: only $count/$N requests completed before timing out" >&2
fi

# SIGINT: current in-flight RPC finishes, the loop's next IsAskedToQuit()
# check exits it, main() returns, atexit fires Dump().
kill -INT "$CLIENT_PID"
wait "$CLIENT_PID" 2>/dev/null
echo "client stopped after $count logged responses (exit status $?)"

kill -INT "$SERVER_PID"
wait "$SERVER_PID" 2>/dev/null
echo "server stopped (exit status $?)"

ls -la "$DEST/lt_server.dump" "$DEST/lt_client.dump"
REMOTE

echo "== [6/6] pulling dumps back to $OUT_DIR =="
mkdir -p "$OUT_DIR"
scp "$HOST:$DEST/lt_server.dump" "$OUT_DIR/server.dump"
scp "$HOST:$DEST/lt_client.dump" "$OUT_DIR/client.dump"
echo "capture.sh: done -- $OUT_DIR/server.dump and $OUT_DIR/client.dump"
