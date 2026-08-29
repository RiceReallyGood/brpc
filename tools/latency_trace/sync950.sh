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
  | ssh "$HOST" "mkdir -p \"$DEST\" && tar xzf - -C \"$DEST\""
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

echo "synced to $HOST:$DEST"
