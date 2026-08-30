#!/bin/bash
# tools/latency_trace/sync950.sh
# Push the working tree to the remote build host. Full sync every time:
# the payload is ~3MB compressed, so incremental sync is not worth the
# risk of a stale file silently surviving.
set -euo pipefail

HOST="${LT_BUILD_HOST:-suzhou950}"
# Relative on purpose: a non-interactive `ssh host cmd` starts in the
# remote user's home directory, so a bare relative path always resolves
# under the REMOTE $HOME with no tilde expansion involved anywhere.
DEST="${LT_BUILD_DIR:-brpc-lt}"

# Guard against the classic footgun: `LT_BUILD_DIR=~/foo ./sync950.sh`
# (unquoted, as a command-line assignment) makes *this* shell tilde-expand
# it against the LOCAL $HOME before the script ever sees it, so DEST would
# already be an absolute local-home path (e.g. /home/alice/foo) by the
# time we get here. Sent to the remote host as-is, that becomes an
# absolute path on the remote filesystem that has nothing to do with the
# remote user's home -- a silent, "successful" sync to the wrong place.
# A value that starts with our own $HOME is the fingerprint of exactly
# that mistake, so refuse it loudly instead of proceeding.
case "$DEST" in
    "$HOME" | "$HOME"/*)
        echo "sync950: LT_BUILD_DIR=\"$DEST\" starts with your LOCAL \$HOME ($HOME)." >&2
        echo "  This usually means the shell tilde-expanded it locally (e.g." >&2
        echo "  LT_BUILD_DIR=~/foo) before ssh ever ran, which would sync to that" >&2
        echo "  literal path on $HOST -- not the remote home." >&2
        echo "  Use a path relative to the REMOTE home instead (e.g." >&2
        echo "  LT_BUILD_DIR=brpc-lt-2), or an explicit remote-absolute path that" >&2
        echo "  is not under your local home." >&2
        exit 1
        ;;
esac

cd "$(dirname "$0")/../.."

# The remote clock runs ~25s AHEAD of this machine, and `tar czf` preserves
# mtimes -- so a file edited locally at T arrives stamped T, while an object
# file built remotely 20s ago is stamped T+5 by the remote clock. `make` then
# sees the object as NEWER than its source, prints "up to date", and silently
# tests a stale binary. That is a false green, and it bites hardest in the
# revert/restore cycle used to prove a test really fails.
#
# The extract step therefore touches every synced file whose mtime is within
# the last three minutes -- i.e. exactly the ones just edited. Untouched files
# keep their old mtimes, so incremental builds stay incremental instead of
# forcing a full rebuild of ~500 sources on every sync.

# GNU tar exits 1 for "some files differ" (e.g. a file changed while being
# read) -- a benign, common race under concurrent editing, not a real
# failure. Exit codes >= 2 are genuine fatal tar errors. Under `pipefail`,
# a bare pipeline would abort the whole script on tar's exit 1 even though
# the archive it produced is fine and the remote side succeeded, so we
# defer to `set +e` around just this pipeline and inspect PIPESTATUS
# ourselves instead of letting `set -e` fire on it.
set +e
tar czf - \
    --exclude=.git \
    --exclude='*.o' \
    --exclude='*.so*' \
    --exclude='*.a' \
    --exclude='*.pb.cc' \
    --exclude='*.pb.h' \
    src test tools Makefile config_brpc.sh CMakeLists.txt \
  | ssh "$HOST" "mkdir -p \"$DEST\" && tar xzf - -C \"$DEST\" && \
       cd \"$DEST\" && find src test tools -type f -newermt '-180 seconds' \
         -exec touch {} + 2>/dev/null; true"
# Capture both stages in one shot: even a bare `x=${PIPESTATUS[0]}`
# assignment is itself a "command" that resets PIPESTATUS, so splitting
# this into two separate assignment lines would silently lose index 1.
pipe_status=("${PIPESTATUS[@]}")
set -e
tar_status=${pipe_status[0]}
ssh_status=${pipe_status[1]}

if [ "$ssh_status" -ne 0 ]; then
    echo "sync950: remote mkdir/extract failed on $HOST (ssh exit $ssh_status)" >&2
    exit "$ssh_status"
fi
if [ "$tar_status" -ge 2 ]; then
    echo "sync950: tar failed while creating the archive (exit $tar_status)" >&2
    exit "$tar_status"
fi

# Report which configuration the destination tree is pinned to. Two trees
# exist so neither has to be reconfigured in place (config.mk is not a
# prerequisite of any object file, so flipping it in place silently reuses
# objects built under the previous flags). But that only helps if you can
# see which tree you just synced to -- a tree left in the wrong state once
# stays wrong silently, and has: ~/brpc-lt spent a while configured as a
# traced build because two earlier tasks reconfigured it in place.
cfg=$(ssh "$HOST" "grep -q 'BRPC_LATENCY_TRACE=1' \"$DEST\"/config.mk 2>/dev/null \
        && echo TRACED || { [ -f \"$DEST\"/config.mk ] && echo default || echo 'NOT CONFIGURED'; }")
echo "synced to $HOST:$DEST  [config: $cfg]"
