#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."

baseline=${HERMES_COMPATIBILITY_BASELINE:-docs/compatibility-baseline.txt}
expected=$(mktemp "${TMPDIR:-/tmp}/hermes-expected-packages.XXXXXX")
actual=$(mktemp "${TMPDIR:-/tmp}/hermes-packages.XXXXXX")
trap 'rm -f "$expected" "$actual"' EXIT
awk -F '|' '$1 == "package" { print }' "$baseline" | LC_ALL=C sort > "$expected"
[[ -s "$expected" ]] || { echo 'empty Hermes package baseline' >&2; exit 1; }
while IFS= read -r manifest; do
  name=$(awk -F ' *= *' '/^\[package\]$/ {p=1; next} /^\[/ {p=0} p && /^name = / {gsub(/"/, "", $2); print $2; exit}' "$manifest")
  version=$(awk -F ' *= *' '/^\[package\]$/ {p=1; next} /^\[/ {p=0} p && /^version = / {gsub(/"/, "", $2); print $2; exit}' "$manifest")
  printf 'package|%s|%s|%s\n' "$manifest" "$name" "$version"
done < <(find . -name Cargo.toml -type f -not -path './target/*' -not -path './.git/*' -print | sed 's#^./##' | LC_ALL=C sort) > "$actual"
diff -u "$expected" "$actual" || { echo 'Hermes package compatibility inventory drifted' >&2; exit 1; }
while IFS='|' read -r kind fixture digest; do
  [[ "$kind" == fixture ]] || continue
  [[ -s "$fixture" ]] || { echo "missing Hermes compatibility fixture: $fixture" >&2; exit 1; }
  actual_digest=$(shasum -a 256 "$fixture" | awk '{print $1}')
  [[ "$actual_digest" == "$digest" ]] || { echo "Hermes compatibility fixture drifted: $fixture" >&2; exit 1; }
done < "$baseline"
cargo metadata --no-deps --format-version 1 --locked --offline >/dev/null
echo 'Hermes compatibility baseline closes over 13 packages and canonical empty/live checkpoints'

