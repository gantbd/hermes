# Supply-chain profile

Status: exact locked source inventory, license-declaration baseline and current
root-lockfile RustSec attestation; not an SBOM or legal/source-content review.

`make check-supply-chain` compares the complete `Cargo.lock` package set with
`supply-chain-baseline.txt`, refuses Git sources, requires a checksum for every
registry package and resolves the locked graph offline. The inventory includes
the local Hermes and Keystone packages, Aegis public-client qualification inputs
and Keystone's locked platform dependencies. `hermes-aegis` has an additional
production-closure gate that rejects Aegis authority, host or service internals;
those packages occur in the lock only because its integration test runs a real
public Aegis service.
The selected `hermes-unix-peer` adapter adds the pinned `nix` graph solely for
safe OS peer-credential retrieval; its direct dependency closure is enforced.
The engine-owned `hermes-local-client` adds no third-party component and has an
enforced public client/protocol/wire-only production closure.

The separately locked `qualification/aegis-process-authorization` workspace
contains 27 packages and may depend on Aegis service/local implementation only as
assurance scaffolding. Hermes's production `hermes-aegis` closure remains limited
to public Aegis client/protocol contracts and is enforced independently.

The same gate feeds complete locked offline Cargo metadata to Keystone's
dev-only verifier and compares every exact package license expression with
`license-baseline.txt`. A missing declaration or expression drift fails closed;
the accepted baseline remains Hermes-owned policy.

The lockfile-bound [advisory attestation](advisory-attestation.md) records a clean
audit against an exact RustSec database revision, and CI audits the current
database without ignored warnings. This detects unreviewed dependency drift,
incomplete registry provenance and known advisories at audit time. It does not
establish source/license compatibility,
artifact contents, signatures, reproducibility or provenance from a clean Cargo
home.
Those remain release blockers.
