# Hermes production roadmap

Status: pre-release; durable kernel, OS-peer-authenticated private daemon and
process-separated Aegis adapter qualified; remote credential binding remains
open.

Hermes is the federation's bounded ordered event ledger. Production maturity
means truthful retention, replay and consumer progress behind a deployable service,
not absorbing domain schemas, orchestration, authorization policy or consensus.

## Current baseline

- Deterministic zero-runtime-dependency stream and consumer core.
- Exact-predecessor append, producer-key replay/conflict and independent monotone
  consumer cursors.
- Canonical bounded checkpoint with complete replay-index reconstruction.
- Independent 100,000-program five-step finite model with recovery after every
  step.
- Receipt-gated durable host and exclusive private-directory local adapter.
- Bounded kernel effective-UID/GID mapping into Hermes subject facts for the
  selected private local Unix profile.
- Lost-reply, actual restart, process-exit and observation-neutrality evidence.

The current profile has explicit cursor-constrained compaction plus authenticated
session, authorization-port, protocol, capability-client, wire and private-daemon
boundaries plus a public-client Aegis action/resource adapter and selected local
OS peer-credential binding. It has no remote credential profile, complete peer
chain, replication, multi-stream transaction or
production release claim. One process-separated Moira producer profile is qualified.

## Release gate H0 — contract and assurance closure

- [x] Reconcile requirements, specification, model and traceability with the
  current checkpoint and local durability evidence.
- [x] Maintain a concrete STRIDE/LINDDUN threat model and residual-risk ledger.
- [x] Pin the Rust toolchain and run `make check` in CI from a fresh checkout.
- [x] Add changelog, release policy, package compatibility baseline, locked supply
  inventory and
  release-readiness ledger with adversarial drift tests.
- [x] Add repository MIT and Apache-2.0 license texts matching package metadata.
- [x] Add digest-pinned empty V1/V2 and live V2 checkpoint fixtures.
- [x] Pin every locked package's declared license expression and reject missing
  declarations or graph/expression drift.
- [ ] Exercise the checkpoint corpus across future released versions.
- [x] Add a current RustSec advisory attestation bound to the exact root lockfile
  and a live CI audit that refuses vulnerabilities or warnings.
- [ ] Complete source-content and legal license-compatibility review.
- [x] Add deterministic public-wire codec mutation with canonical re-encode
  checks across every request, reply and readiness shape.
- [x] Extend the independent model through all 100,000 five-step programs with
  recovery after every operation.
- [x] Add an opt-in 5,000-cycle authenticated-service soak covering append,
  exact replay, compaction, canonical reopen and truthful gaps.
- [x] Add optimized append/read/compaction/recovery performance populations with
  declared p95/p99 engineering budgets and exact scope/non-SLO limitations.
- [x] Add a 32-cycle two-process filesystem campaign covering lost append
  replies, canonical reopen, exact replay and one-event retention.

Exit: one reviewed source-distribution contract and green reproducible gates.

## Release gate H1 — retention and truthful gaps

- [x] Select a closed compaction/truncation profile with retained low/high edges.
- [x] Specify cursor behavior behind retention, ahead of publication and at exact
  end-of-stream; absence must never impersonate complete history.
- [x] Decide how acknowledgements constrain compaction and how stalled consumers
  are surfaced without allowing one consumer to mutate another.
- [x] Retain bounded producer replay evidence after payload compaction, or expose
  explicit replay-history loss. Never silently re-append an old key.
- [x] Make compaction, cursor movement and replay correspondence atomic under the
  canonical checkpoint/recovery contract.
- [x] Extend the independent finite model across append, acknowledge, compact,
  read, gap and restart interleavings.

Exit: H5 gap visibility is satisfied by executable model and recovery evidence.

## Release gate H2 — native service product

- [x] Add the native service product layers:
  - [x] `hermes-protocol`, `hermes-service` and capability-shaped `hermes-client`.
  - [x] `hermes-wire` canonical bounded request/reply/readiness frames.
- [x] Expose bounded append, read page, replay result, consumer progress, gap and
  confirmed usage contracts without leaking engine internals.
- [x] Add authenticated host sessions and Aegis authorization for protected
  stream actions. Producer and consumer IDs are never authentication:
  - [x] Volatile authenticated sessions and a command-scoped authorization port.
  - [x] Concrete public-client Aegis adapter and closed action/resource mapping.
    - [x] Independent live Aegis and Hermes processes proving exact stream
      `Submit` allowance and wrong-action denial through public daemon traffic.
  - [x] Exact bounded effective UID/GID-to-subject adapter for the connected Unix
    peer, with unknown peers refused before session creation or command dispatch.
- [x] Add private local transport and selected observer/operator daemon with
  bounded frames, deadlines, lifetime ownership, health/readiness, exact-stream
  acknowledgement/compaction authority and truthful transmission uncertainty.
- [x] Preserve embedded/remote append/read/ack/compact/gap parity, exact append
  replay after reopen and explicit replay-history loss after compaction.
- [x] Add content-safe backlog, retained-edge, stalled-cursor, byte-residence,
  commit-time and saturation observations.
- [x] Own disclosure, acknowledgement and compaction command guards beside the
  public client. Presentation and Workbench reuse them without redefining
  confirmation, stale-state or uncertainty semantics.

Exit: embedded and local-daemon clients pass the same append/read/ack/gap suite.

## Release gate H3 — integration and delivery profiles

- [x] Publish reusable inbox/outbox conformance suites covering lost replies,
  conflicts, restart, independent cursors, compaction gaps and acknowledgement
  replay.
- [ ] Qualify peer consumers through public clients while keeping their schemas
  outside Hermes:
  - [x] Moira workflow-event producer with stable Moira-owned event encoding,
    lost-reply replay and public-client-only production closure.
    - [x] Process exit/reopen qualification with a discarded append reply and
      exact one-event replay over Hermes's public wire.
    - [x] Sixteen fresh peer-owned process cycles covering discarded reply,
      durable reopen, exact replay and one canonical Moira event.
    - [x] Integrated six-process durable Moira/Aegis/Chronos-gated execution recovery followed
      by exact-stream Aegis-authorized publication/readback of one canonical
      Moira result event, repeated across sixteen fresh sets.
  - [ ] Agora outbox consumer after its active public-stack refactor settles.
- [x] Add operator procedures for stalled consumers, capacity exhaustion,
  uncertain commits, compaction and recovery.
- [ ] Select additional storage, integrity or replication profiles only after the
  single-owner service contract is closed; keep their evidence separate.

Exit: one production local service and two peer integrations are qualified.

## Deliberate non-goals

- Domain event interpretation, workflow state or authorization policy.
- Silent eviction, hidden cursor advancement or automatic conflict resolution.
- Multi-stream transactions or distributed consensus in the first service profile.
- Treating storage access, producer IDs or consumer IDs as caller authority.

