#!/bin/bash
# tools/latency_trace/sync950.sh
# Push the working tree to the remote build host. Full sync every time:
# the payload is ~3MB compressed, so incremental sync is not worth the
# risk of a stale file silently surviving.
set -euo pipefail

HOST="${LT_BUILD_HOST:-suzhou950}"
DEST="${LT_BUILD_DIR:-~/brpc-lt}"

cd "$(dirname "$0")/../.."

tar czf - \
    --exclude=.git \
    --exclude='*.o' \
    --exclude='*.so*' \
    --exclude='*.a' \
    --exclude='*.pb.cc' \
    --exclude='*.pb.h' \
    src test tools Makefile config_brpc.sh CMakeLists.txt \
  | ssh "$HOST" "mkdir -p $DEST && tar xzf - -C $DEST"

echo "synced to $HOST:$DEST"
