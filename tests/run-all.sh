#!/usr/bin/env bash
# Runs the whole suite and keeps the raw output, passed or failed.
set -o pipefail
mkdir -p "$OUT_DIR"
{
  echo "# raw output of: npx playwright test"
  echo "# started $(date -u +%Y-%m-%dT%H:%M:%SZ) · node $(node -v) · playwright $(npx playwright --version | cut -d' ' -f2)"
  echo
  npx playwright test "$@"
} 2>&1 | tee "$OUT_DIR/tests-raw-output.txt"
