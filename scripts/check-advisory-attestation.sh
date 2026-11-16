#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."

attestation=${HERMES_ADVISORY_ATTESTATION:-docs/advisory-attestation.md}
lockfile=${HERMES_ADVISORY_LOCKFILE:-Cargo.lock}
[[ -s "$attestation" ]] || { echo 'Hermes advisory attestation is missing' >&2; exit 1; }
expected=$(awk -F': ' '$1 == "lockfile-sha256" { print $2 }' "$attestation")
actual=$(shasum -a 256 "$lockfile" | awk '{print $1}')
[[ -n "$expected" && "$actual" == "$expected" ]] || {
  echo 'Hermes lockfile changed without a new advisory attestation' >&2; exit 1;
}
rg -q '^cargo-audit-version: 0\.23\.0$' "$attestation"
rg -q '^advisory-database-commit: [0-9a-f]{40}$' "$attestation"
rg -q '^audited-at-utc: [0-9]{4}-[0-9]{2}-[0-9]{2}T' "$attestation"
rg -q '^dependency-count: 37$' "$attestation"
rg -q '^vulnerability-count: 0$' "$attestation"
rg -q '^warning-count: 0$' "$attestation"
echo 'Hermes advisory attestation matches the exact 37-component root lockfile'
