# Changelog

All notable Hermes changes will be recorded here. Released versions will follow
Semantic Versioning once a public distribution contract is selected.

## [Unreleased]

### Added

- Added a lockfile-bound RustSec advisory attestation and live CI advisory gate.
- Added `hermes-unix-peer`, a bounded OS effective-UID/GID authenticator with
  exact subject mapping, redacted diagnostics and real-daemon unknown-peer
  refusal evidence.
- Added an optimized reference-host performance gate for append, read,
  compaction and recovery with declared p95/p99 engineering budgets.
- Added a 32-cycle two-process filesystem soak for lost append replies,
  canonical reopen, exact replay and one-event retention.
- Added repository MIT and Apache-2.0 license texts matching package metadata and
  made them mandatory release-contract artifacts.
- Added an exact locked-graph license-declaration baseline using Keystone's
  dev-only verifier; missing declarations and expression drift now fail closed.
- Added an opt-in 5,000-cycle authenticated-service soak spanning append, exact
  replay, compaction, canonical reopen and explicit retention gaps.
- Added an external canonical live V2 checkpoint fixture retaining two events
  and two independent consumer cursors.
- Extended the independent ledger model through all 100,000 five-step programs,
  reconstructing from the canonical checkpoint after every operation.
- Added the deterministic ledger core, canonical checkpoint, durable host,
  private local adapter and embedded SDK facade.
- Added the production roadmap, pinned toolchain, CI workflow, security policy
  and threat/privacy model.
- Added explicit cursor-constrained compaction, truthful read gaps, producer-key
  tombstones, V1-to-V2 checkpoint migration and durable restart evidence.
- Added representation-independent protocol DTOs, an authenticated
  command-authorized service boundary and capability-shaped append, consumer,
  compaction and observation clients with durable reopen/gap qualification.
- Added canonical bounded request, reply and readiness frames with exhaustive
  closed-algebra round trips and allocation-adversarial count/payload tests.
- Added deterministic bit-flip, byte-deletion and byte-insertion mutation over
  every public request, reply and readiness shape, requiring canonical
  re-encoding for every mutation that remains valid.
- Added a private Unix daemon with authenticated connection sessions, finite
  frames/requests/deadlines, truthful reply-loss classification and exact append
  replay after daemon plus local-store reopen.
- Added `hermes-aegis`, a public-client-only stream authorization adapter with a
  closed action mapping, reserved service resource and real allow/deny journey.
- Extended the real daemon journey across read, acknowledgement, cursor-blocked
  compaction, truthful retained gaps and restart-stable replay-history loss.
- Added exact-stream aggregate retention/backlog/stalled-cursor observations and
  service-wide fixed-cardinality residence, commit, checkpoint, usage, fencing
  and saturation signals through service, client, wire and real restart journey.
- Adopted Keystone's sticky counter arithmetic while retaining Hermes's distinct
  last-exact-value duration overflow law locally.
- Added public-protocol-only reusable inbox/outbox conformance suites and a
  fail-closed operator runbook for delivery, retention and recovery incidents.
- Qualified Moira-owned event delivery across a discarded append reply, Hermes
  process exit, durable reopen in a fresh process and exact public-wire replay.

### Security

- Producer and consumer identifiers confer no authority; durable publication
  releases success only after exact strong storage evidence.

[Unreleased]: https://github.com/gantbd/hermes/commits/main

