# Traceability ledger

| Requirement | Planned implementation | Current evidence | Status |
| --- | --- | --- | --- |
| H1 ordering | exact-predecessor append over ordered stream vectors | predecessor mismatch and sequential append scenarios | Satisfied for volatile profile |
| H2 opacity | bounded payload envelope with redacted debug | payload bound/redaction scenario plus representation-independent service DTOs | Satisfied through native service boundary |
| H3 idempotency | live canonical comparison plus compacted-key tombstones | lost-reply replay, changed-content conflict and replay-history-loss scenarios | Satisfied for retention profile |
| H4 consumers | independent `(consumer, stream)` cursor map | monotone, regression, and beyond-edge scenarios | Satisfied for volatile profile |
| H5 gaps | retained/published edges and closed gap algebra | before-retention, exact-end and ahead-of-publication scenarios | Satisfied for explicit compaction profile |
| H6 commit truth | private candidate publication after exact durable receipt | synthetic failure matrix and real lost-receipt recovery | Satisfied for trusted local profile |
| H7 bounds | explicit stream/retained-event/idempotency/consumer/payload/read limits | constructor, payload, compaction, checkpoint-budget, finite-model, service, wire admission and deterministic mutation evidence | Satisfied through native service and bounded wire |
| H8 codec | V1 migration plus canonical bounded V2 retention checkpoint and V1 public wire | external goldens, malformed/limit, compaction restart, durable host and canonical single-bit/byte wire mutation tests | Satisfied for retention and public-wire profiles |
| H9 isolation | dependency-free deterministic core | source boundary and strict build graph | Satisfied for volatile core |
| H10 content-safe operation | aggregate stream health and fixed-cardinality service signals | core aggregation, service authorization, exhaustive wire and real socket/restart scenarios | Satisfied for native local service profile |
| H11 delivery conformance | public-protocol-only inbox/outbox harness | real durable service lost-reply, replay/conflict, restart, independent cursor, acknowledgement replay, compaction and gap suite | Satisfied for reusable V1 contract; peer qualifications tracked separately |
| H12 local authentication | bounded exact kernel effective-UID/GID mapping to volatile Hermes subjects | mapping bounds/duplicates/redaction, unknown-peer refusal and real daemon acceptance before domain dispatch | Satisfied for selected private local Unix profile; remote and hostile same-UID profiles open |

Release assurance additionally binds the exact 32-component root lockfile to a
clean RustSec audit at the database revision recorded in
`docs/advisory-attestation.md`. CI audits the then-current database with warnings
denied; this is known-advisory evidence, not source or legal review.

## Durable local qualification

`tests/checkpoint_behavior.rs` verifies complete replay-index reconstruction,
independent cursors, canonical order and parser refusal. The independent linear
model in `tests/finite_ledger_model.rs` compares all 100,000 declared five-step
programs with restart after every step, not arbitrary histories or storage faults.
`host/hermes-host/tests/durable_behavior.rs` covers publication gating, strong
commit/load evidence, uncertainty fencing and observation neutrality.
`adapters/hermes-local/tests/local_delivery.rs` qualifies actual filesystem replay,
conflict/cursor retention, lost receipts and refusal without reset. Keystone's
directory tests separately cover process exclusion/exit and publication faults.
The zero-dependency graph test and WASI compilation keep effects outside the core.

This is a private exclusive local Unix retention profile, not authenticated
storage, physical power-loss certification, replication or a latency SLO.

`make soak` separately runs 5,000 fresh authenticated-service lifecycles through
append, exact replay, successor append, prefix compaction, canonical reopen and
truthful before-retention gaps. It is repeated service/checkpoint evidence only;
filesystem/process evidence is covered separately. `make performance` runs
64 optimized append/read/compaction/recovery samples under the provisional
reference-host p95/p99 budgets in `docs/performance.md`; that one-stream
in-memory gate is not filesystem, daemon, load or product-SLO evidence.
`make soak-process` adds 32 fresh two-daemon cycles through lost append reply,
clean process exit, canonical durable reopen, exact replay and one-event read. It
does not claim power-loss, concurrency/load or replication coverage.

## Native service boundary

`hermes-protocol` owns representation-independent append/read/ack/cursor/compact,
gap, replay and confirmed-usage DTOs. `hermes-service` binds volatile authenticated
subjects to a command-scoped authorization port before touching the durable host;
producer and consumer IDs never enter that decision. `hermes-client` exposes
separate append, consumer, compaction and observation facades with a client-side
read ceiling. A receipt-gated in-memory-host journey proves deny-before-mutation,
exact append replay, session loss on reopen, retained-gap recovery,
replay-history loss and cursor-behind-retention refusal. `hermes-wire` exhaustively
round-trips the closed protocol, refuses truncation/trailing data, and checks
attacker-controlled counts and payload lengths before allocation. The real daemon
parity evidence is qualified below. Stream health is authorized on the exact
stream; service signals require service observe authority. Both contracts are
bounded and omit payloads, subjects, producer keys and consumer identities.

## Private local daemon

`hermes-local-client` is the engine-owned, service-free public transport client.
It depends only on Hermes's public client/protocol/wire crates and owns Unix
connection setup, negotiated frame/request limits, framing, and not-sent versus
outcome-unknown classification. Its enforced dependency closure excludes the
ledger, host, service, daemon, storage, credential mapping, Keystone, and all
presentation packages. The former `hermes-tui-local-client` package is now only
a source-compatible re-export for presentation callers.

`hermes-local-daemon` authenticates each connection into one bounded volatile
service session, advertises canonical frame/request ceilings, applies finite I/O
deadlines and removes only the socket inode it created. Connection tests separate
pre-dispatch failures from reply loss after one service outcome. A real mode-0600
socket journey over `hermes-local` proves durable append, daemon/store reopen,
exact replay, read, acknowledgment, cursor-constrained compaction, retained gaps,
restart-stable cursor/gap evidence, replay-history loss, health and owned-path
cleanup. Its final reopen also proves aggregate backlog/retention health and
service operation/commit/checkpoint/saturation signals over the canonical wire.
The selected `hermes-unix-peer` profile binds kernel effective UID/GID through a
bounded exact subject registry and proves mapped acceptance plus unknown-peer
refusal at the real daemon boundary. Remote credentials and same-UID process
separation remain open.

## Reusable delivery conformance

`hermes-conformance` depends only on `hermes-protocol` and supplies independent
outbox and inbox suites. An adopter provides an authenticated command path plus
explicit test controls that dispatch once while discarding a reply and reopen
the same durable ledger with a fresh session. The suite owns no retry,
authorization, retention, storage or transport policy. The native service runs
both suites against its real durable host; Moira and Agora qualification remain
separate H3 peer evidence.

Moira's `moira-hermes` production graph closes over `hermes-client`,
`hermes-protocol` and Moira public contracts. Its real public-service journey
dispatches one stable Moira-owned workflow transition, loses the reply, and
replays to the original sequence without a second event. This qualifies the
Moira producer mapping, not arbitrary inbox behavior or Moira's complete saga.
Moira's isolated `make qualify-hermes-process` assurance runs Hermes as a helper
process, discards the append reply, exits, reopens the same private ledger in a
fresh process and reads exactly one canonical event through public wire frames.
Its opt-in peer campaign repeats that profile across sixteen fresh directories
and process pairs, closing repeatability for the selected Moira producer segment.
Moira's separate live-chain campaign also carries a recovered, Aegis-authorized,
Chronos-validated Daimon completion into an independent Hermes daemon and reads
back one canonical event after the same Aegis authority grants exact-stream
`Submit`/`Observe`, across sixteen fresh six-process sets with Moira daemon/SQLite
reopen between execution preparation and result acceptance. This qualifies
Hermes delivery in that selected chain, not the upstream peer semantics.
Agora remains open while its public stack is being refactored.

## Aegis authorization adapter

`hermes-aegis` depends in production only on the public Aegis client/protocol and
Hermes protocol/service. Its closed mapping is Append→Submit, Read→Observe,
Acknowledge→Modify and Compact→Administer on the exact 16-byte stream resource;
service observation uses the reserved `HERMESSERVICEV1!` resource. That value is
refused as a stream in this adapter profile to prevent resource aliasing. A real
public-client journey proves exact append allowance plus wrong-action,
wrong-subject and reserved-resource denial before durable mutation.
The isolated `make qualify-aegis-process` profile then starts independent Aegis
and Hermes daemons: Hermes obtains a stream-specific `Submit` grant through the
public Aegis wire, accepts append through its own public wire and refuses read
because no `Observe` action was granted. Both sockets are removed after their
owners exit; neither engine crosses the peer's production dependency boundary.

