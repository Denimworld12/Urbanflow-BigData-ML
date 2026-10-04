#!/usr/bin/env bash
# Clones the real Tier 2 results (Denimworld12/Urbanflow-BDA-data: the gold
# tables and trained model from one 243.5M-trip FHVHV run, ~340 KB) into
# external/Urbanflow-BDA-data (gitignored). `make hadoop-load` picks up its
# data/gold automatically when it is there.
#
# Never fails: if the clone is already present it does nothing, and if GitHub
# is unreachable it prints why and exits 0, so it cannot break container setup.
set -uo pipefail
cd "$(dirname "$0")/.."

URL=${TIER2_DATA_URL:-https://github.com/Denimworld12/Urbanflow-BDA-data.git}
DEST=external/Urbanflow-BDA-data

if [ -d "$DEST/.git" ]; then
  echo "  tier2 data already at $(cd "$DEST" && pwd) (git -C $DEST pull to update)"
  exit 0
fi

# GIT_TERMINAL_PROMPT=0: fail instead of waiting for a password if the repo is
# ever made private again.
mkdir -p external
if GIT_TERMINAL_PROMPT=0 git clone --quiet "$URL" "$DEST"; then
  echo "  tier2 data cloned to $(cd "$DEST" && pwd)"
else
  rm -rf "$DEST"
  echo "  skipped: could not clone $URL; the Tier 2 data is optional" >&2
fi
exit 0
