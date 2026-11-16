#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."

baseline=${HERMES_SUPPLY_BASELINE:-docs/supply-chain-baseline.txt}
licenses=${HERMES_LICENSE_BASELINE:-docs/license-baseline.txt}
[[ -s "$baseline" ]] || { echo 'Hermes supply-chain baseline is missing' >&2; exit 1; }
[[ -s "$licenses" ]] || { echo 'Hermes license baseline is missing' >&2; exit 1; }
actual=$(mktemp "${TMPDIR:-/tmp}/hermes-supply.XXXXXX")
expected=$(mktemp "${TMPDIR:-/tmp}/hermes-supply-expected.XXXXXX")
trap 'rm -f "$actual" "$expected"' EXIT

awk -F '|' '$1 == "component" { print }' "$baseline" | LC_ALL=C sort > "$expected"
[[ -s "$expected" ]] || { echo 'Hermes supply inventory is empty' >&2; exit 1; }
awk '
  /^\[\[package\]\]$/ { if (name != "") print "component|" name "|" version; name=""; version=""; next }
  /^name = / { value=$0; sub(/^name = "/,"",value); sub(/"$/,"",value); name=value }
  /^version = / { value=$0; sub(/^version = "/,"",value); sub(/"$/,"",value); version=value }
  END { if (name != "") print "component|" name "|" version }
' Cargo.lock | LC_ALL=C sort > "$actual"
diff -u "$expected" "$actual" || { echo 'Hermes locked component inventory drifted' >&2; exit 1; }
rg -q '^source = "git\+' Cargo.lock && { echo 'Hermes lock contains a Git source' >&2; exit 1; }
awk '
  /^\[\[package\]\]$/ { if (registry && !checksum) exit 1; registry=0; checksum=0; next }
  /^source = "registry\+/ { registry=1 }
  /^checksum = "/ { checksum=1 }
  END { if (registry && !checksum) exit 1 }
' Cargo.lock || { echo 'Hermes registry component lacks a checksum' >&2; exit 1; }
cargo metadata --locked --offline --format-version 1 | node ../keystone/scripts/check-cargo-license-baseline.mjs "$licenses"
echo "Hermes supply profile closes over $(wc -l < "$expected" | tr -d ' ') locked components"
