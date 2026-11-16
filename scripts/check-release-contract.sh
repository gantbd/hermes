#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."

for required in CHANGELOG.md CONTRIBUTING.md LICENSE-APACHE LICENSE-MIT README.md ROADMAP.production.md SECURITY.md requirements.md specification.md rust-toolchain.toml release/github-source.json docs/advisory-attestation.md docs/compatibility.md docs/compatibility-baseline.txt docs/license-baseline.txt docs/performance.md docs/release-policy.md docs/release-readiness.md docs/soak.md docs/supply-chain.md docs/supply-chain-baseline.txt docs/threat-model.md docs/traceability.md docs/unix-peer-credentials.md; do
  [[ -s "$required" ]] || { echo "missing Hermes release-contract file: $required" >&2; exit 1; }
done
rg -q '^channel = "1\.95\.0"$' rust-toolchain.toml || { echo 'Hermes toolchain is not pinned' >&2; exit 1; }
while IFS= read -r manifest; do
  awk '/^\[package\]$/ {p=1; next} /^\[/ {p=0} p && /^publish = false$/ {ok=1} END {exit(ok ? 0 : 1)}' "$manifest" || { echo "$manifest permits publication" >&2; exit 1; }
done < <(find . -name Cargo.toml -type f -not -path './target/*' -not -path './.git/*' -print)
rg -q 'no registry or binary' docs/release-policy.md || { echo 'Hermes distribution refusal missing' >&2; exit 1; }
rg -q 'not ready' docs/release-readiness.md || { echo 'Hermes readiness refusal missing' >&2; exit 1; }
rg -q '"channel": "github-coordinated-source"' release/github-source.json || { echo 'Hermes GitHub source channel missing' >&2; exit 1; }
rg -q '"revision": "2e4faf10ec0a70805aa1db1555f93c77fcf0a1e4"' release/github-source.json || { echo 'Hermes Keystone source revision drifted' >&2; exit 1; }
rg -q '"revision": "ab5ea9d2af1027656958d9c3429d01b5291c1084"' release/github-source.json || { echo 'Hermes Aegis source revision drifted' >&2; exit 1; }
rg -q '"remoteReachabilityQualified": false' release/github-source.json || { echo 'Hermes source profile claims unverified reachability' >&2; exit 1; }
rg -q '^performance:' Makefile || { echo 'Hermes performance gate missing' >&2; exit 1; }
rg -q '^soak-process:' Makefile || { echo 'Hermes process soak gate missing' >&2; exit 1; }
rg -q '^check-peer-profiles:' Makefile || { echo 'Hermes peer-process gate missing' >&2; exit 1; }
echo 'Hermes release contract denies registry and binary publication and pins the source profile'

