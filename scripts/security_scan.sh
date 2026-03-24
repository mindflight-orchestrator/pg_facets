#!/usr/bin/env bash
# Static security checks for extension SQL and control files.
# Excludes vendored deps/ (e.g. pg_roaringbitmap). Run from repo root: ./scripts/security_scan.sh
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

fail() {
  echo "ERROR: $*" >&2
  exit 1
}

echo "==> Security scan: ${ROOT}"

if [[ ! -d sql ]]; then
  fail "expected sql/ directory"
fi

# 1) SECURITY DEFINER in first-party SQL only
if grep -rE --include='*.sql' --exclude-dir='deps' 'SECURITY[[:space:]]+DEFINER' sql/ 2>/dev/null | grep -q .; then
  echo "FAIL: SECURITY DEFINER found under sql/ (excluding deps/)"
  grep -rE --include='*.sql' --exclude-dir='deps' 'SECURITY[[:space:]]+DEFINER' sql/ || true
  exit 1
fi

# 2) superuser = true in top-level .control (must not be enabled without review)
shopt -s nullglob
for f in *.control; do
  if grep -qE '^[[:space:]]*superuser[[:space:]]*=[[:space:]]*true' "$f"; then
    fail "superuser = true in ${f} — remove or document risk"
  fi
done
shopt -u nullglob

echo "OK: security scan passed"
