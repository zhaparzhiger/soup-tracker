#!/usr/bin/env bash
# Do the tests fail when the server is wrong? Breaks the server on purpose, one bug at a time,
# and runs the tests that are supposed to catch it. A mutant that PASSES is a hole in the tests.
#
# Run from the repository root on the host:  bash tests/mutation-check.sh
# Needs a clean working tree in src/ (it restores src/ with git after each mutant).

set -u
cd "$(dirname "$0")/.."
OUT=test-output/mutation-check.txt
mkdir -p test-output
: > "$OUT"

if ! git diff --quiet -- src; then
  echo "src/ has uncommitted changes; commit or stash them first" >&2
  exit 2
fi

mutant() { # name, file, perl substitution, test filter
  local name=$1 file=$2 subst=$3 filter=$4
  {
    echo "=================================================================="
    echo "MUTANT: $name"
    echo "  change: $file  $subst"
  } | tee -a "$OUT"
  perl -0pi -e "$subst" "$file"
  if git diff --quiet -- "$file"; then
    echo "  !! the substitution matched nothing — mutant not applied" | tee -a "$OUT"
    return
  fi
  docker compose build api >/dev/null 2>&1 && docker compose up -d api >/dev/null 2>&1
  docker compose --profile test run --rm tests npx playwright test -g "$filter" 2>&1 |
    grep -E "^\s+(✓|✘)|^\s+[0-9]+ (passed|failed)|^\s+Error:|Expected|Received" | cut -c1-200 | tee -a "$OUT"
  git checkout -- "$file"
}

mutant "reconnect resumes from 'now' instead of from the client's cursor" \
  src/routes.ts \
  's/new RunStream\(req\.auth, run\.id, after,/new RunStream(req.auth, run.id, after > 0 ? run.last_seq : 0,/' \
  "page ==|cut off 40 times"

mutant "run lookup forgets the project filter" \
  src/repo.ts \
  's/FROM runs WHERE id = \$1 AND project_id = \$2`, \[runId, projectId\]/FROM runs WHERE id = \$1 AND \$2::uuid IS NOT NULL`, [runId, projectId]/' \
  "read, write and subscribe|never carries"

mutant "stream ignores backpressure (keeps writing to a full socket)" \
  src/stream.ts \
  's/if \(!flushed && !\(await this\.waitForDrain\(\)\)\) return;//' \
  "stops reading"

mutant "ingest does not lock the run (writers race for seq)" \
  src/repo.ts \
  's/ FOR NO KEY UPDATE'"'"'/'"'"'/' \
  "16 concurrent writers|8 times at once"

mutant "auth accepts revoked keys" \
  src/repo.ts \
  's/WHERE key_hash = \$1 AND revoked_at IS NULL/WHERE key_hash = \$1/' \
  "old key is dead|same 401"

echo "==================================================================" | tee -a "$OUT"
echo "restoring the real server" | tee -a "$OUT"
docker compose build api >/dev/null 2>&1 && docker compose up -d api >/dev/null 2>&1
git status --short -- src | tee -a "$OUT"
