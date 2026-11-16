#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
task_dir=$(mktemp -d "${TMPDIR:-/tmp}/hermes-assurance-mutants.XXXXXX")
trap 'rm -f "$task_dir"/*; rmdir "$task_dir"' EXIT

awk '!/^package\|Cargo.toml\|/' docs/compatibility-baseline.txt > "$task_dir/compatibility.txt"
if HERMES_COMPATIBILITY_BASELINE="$task_dir/compatibility.txt" bash scripts/check-compatibility-baseline.sh >"$task_dir/output" 2>&1; then
  echo 'Hermes compatibility gate accepted a missing-package mutant' >&2; exit 1
fi
sed 's/198dfe0b/298dfe0b/' docs/compatibility-baseline.txt > "$task_dir/compatibility.txt"
if HERMES_COMPATIBILITY_BASELINE="$task_dir/compatibility.txt" bash scripts/check-compatibility-baseline.sh >"$task_dir/output" 2>&1; then
  echo 'Hermes compatibility gate accepted a fixture-digest mutant' >&2; exit 1
fi
sed 's/component|hermes-ledger|0.0.1/component|hermes-ledger|0.0.2/' docs/supply-chain-baseline.txt > "$task_dir/supply.txt"
if HERMES_SUPPLY_BASELINE="$task_dir/supply.txt" bash scripts/check-supply-chain.sh >"$task_dir/output" 2>&1; then
  echo 'Hermes supply gate accepted a version-drift mutant' >&2; exit 1
fi
sed 's/license|hermes-ledger|0.0.1|MIT OR Apache-2.0/license|hermes-ledger|0.0.1|MIT/' docs/license-baseline.txt > "$task_dir/licenses.txt"
if HERMES_LICENSE_BASELINE="$task_dir/licenses.txt" bash scripts/check-supply-chain.sh >"$task_dir/output" 2>&1; then
  echo 'Hermes supply gate accepted a license-expression mutant' >&2; exit 1
fi
awk '/^lockfile-sha256:/ {$0="lockfile-sha256: 0000000000000000000000000000000000000000000000000000000000000000"} {print}' docs/advisory-attestation.md > "$task_dir/advisory.md"
if HERMES_ADVISORY_ATTESTATION="$task_dir/advisory.md" bash scripts/check-advisory-attestation.sh >"$task_dir/output" 2>&1; then
  echo 'Hermes advisory gate accepted a lockfile-digest mutant' >&2; exit 1
fi
echo 'Hermes assurance gates reject compatibility, supply, license and advisory mutants'
