# Roadmap

## Phase 0 — vocabulary and laws

- Close IDs, envelopes, limits, append outcomes, cursors, and refusal precedence.
- Define canonical equality for idempotent replay.
- Add exhaustive sequence/cursor law tests under small bounds.

## Phase 1 — volatile reference ledger

- [Complete] Implement deterministic in-memory streams and consumers.
- [Complete for selected retention profile] Add generated reference-model comparison, broader saturation,
  replay, conflict, and gap scenarios. Core replay/conflict/cursor laws are
  covered by behavior tests.
- [Complete for checkpoint V2 with V1 migration] Implement a bounded canonical codec and parser
  robustness corpus. An independent linear model compares all 100,000 programs
  in its ten-command/five-step domain, including restart after every step;
  this does not prove unbounded histories or a storage adapter.

## Phase 2 — store protocol

- [Complete] Revision-bound host envelopes and strong commit/load evidence.
- [Complete for local profile] Pre/post-publication, lost-receipt and recovery
  faults, OS writer exclusion and abrupt subprocess exit.
- [Complete] Trusted native host and bounded local directory adapter wiring.

## Phase 3 — integration

- [Implemented in process] Representation-independent protocol, authenticated
  command-authorized service and capability-shaped clients preserve append,
  replay, cursor, compaction and gap behavior through durable reopen.
- [Implemented] Canonical bounded wire transport covers the closed native
  protocol with allocation-adversarial tests.
- [Implemented] Private local daemon with bounded negotiated frames, deadlines,
  truthful reply-loss classification and append/read/ack/compact/gap parity
  across real store reopen.
- [Implemented] Shutdown-aware embedded accept loop with an identity-free wake
  connection and owner-thread/socket cleanup qualification. Production shells
  still supply storage mode, peer mapping, authorization and limits.
- [Implemented] Explicit observer/operator `hermes-daemon` application with
  create-versus-recover mode, exact OS peer mapping, signal-driven drain and
  real process qualification. Operator authority is bound to one exact stream
  for read/observe/acknowledge/compact; append remains producer-owned.
- [Qualified across independent service processes] Public-client Aegis
  authorization adapter with exact action/resource mapping, live grant issuance,
  append allowance and wrong-action denial through both public daemons.
- [Qualified] Moira workflow-event publication through Hermes public client and
  protocol only, including sixteen fresh lost-reply/process-reopen cycles with
  exact one-event replay over the durable local profile.
- Qualify Agora outbox ingestion after its active public-stack refactor settles.
- Keep domain schemas outside Hermes.
- [Implemented] Add aggregate stream backlog/stall/edge observations and
  fixed-cardinality service signals without consumer identities, domain labels
  or payload content.
- [Implemented] Reusable public-protocol-only inbox/outbox conformance suites
  cover reply loss, replay/conflict, restart, independent cursors,
  acknowledgement-gated compaction and retained gaps.
- [Documented] Operator response for stalled consumers, capacity, uncertain
  commits, compaction and fail-closed recovery.

## Phase 4 — reusable ledger surface and standalone CLI

- [Qualified] Define `hermes-tui` as a composable presentation over Hermes's public client.
  It owns bounded stream, cursor, backlog, gap, and confirmation projection, but
  no terminal lifecycle, argument parsing, daemon startup, global shortcuts, or
  process exit.
- [Qualified for the selected local profile] Provide a separate Hermes
  CLI/application composition root. The
  explicit observer/operator durable daemon is now owner-supplied; the sibling
  presentation CLI supplies observation and invokes Hermes-owned exact-confirmed
  bounded disclosure, acknowledgement and compaction guards. Guarded append
  remains a producer-specific integration concern.
- [Implemented] Keep disclosure, consumer and operator guards under Hermes
  ownership; presentation packages contain no ledger command semantics.
- [Qualified for the selected local profile] Let Workbench reuse the same TUI
  and supervise Hermes's sovereign observer/operator daemon beneath one product
  lifecycle. Hermes retains ledger, cursor, compaction, retention,
  authorization, and uncertainty authority. The selected process boundary does
  not merge Hermes's Cargo source graph with Chronos and therefore creates no
  architecture-driven Keystone source-convergence requirement.
- [Qualified for the selected observation/operator profile] Standalone
  interactive/headless and Workbench interactive/headless paths reuse the same
  public clients, presentation component, exact-confirmed disclosure,
  acknowledgement and compaction guards, durable create/reopen, and clean
  signal drain. Producer-specific append integrations remain separate adapter
  work rather than Workbench authority.

