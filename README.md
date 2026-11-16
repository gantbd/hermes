# Hermes

Hermes is a bounded event ledger for ordered append, replay, inbox
deduplication, and independent consumer progress.

Hermes carries domain events without interpreting them. Agora, Chronos, Vellum,
Moira, Aegis, and Daimon retain ownership of their event vocabularies and state.
The repository contains a deterministic bounded volatile core and a separate
exclusive local-filesystem delivery profile.
It implements exact-predecessor append, producer-key replay/conflict detection,
explicit cursor-constrained compaction, truthful bounded read gaps, and monotone
independent consumer cursors. It makes no durable
store or crash-recovery claim by itself.

Canonical V2 checkpoints preserve retention edges, events, producer-key tombstones
and independent consumer cursors under explicit byte/field and semantic limits;
canonical V1 state upgrades on recovery. An independently implemented linear
reference model checks all 100,000 five-step programs in its declared ten-command
family, with recovery after every step.
These are pure codec/model checks, not a storage durability claim. The engine
retains zero runtime dependencies.

`host/hermes-host` is the separate trusted in-process persistence boundary.
Explicit `create_new` publishes an initial checkpoint; `open` only recovers
existing durably acknowledged state. Changed appends, cursor acknowledgments and
compactions wait for a matching durable receipt; replay, replay-history loss and identities do not
write. Uncertain commits, weak/misattributed receipts and store errors fence all
subsequent reads/mutations until reopen. Codec refusal before a store offer leaves
the live state intact. Fixed-cardinality operation counts/residence, checkpoint
commit time, confirmed usage and fencing signals contain no payload/identity
labels; disabling observation changes neither state nor results.

`adapters/hermes-local` wires this host to Keystone's bounded single-register
directory adapter. Native tests qualify append/replay/conflict, independent
cursors and lost-receipt recovery on the local filesystem. The adapter's focused
tests additionally cover OS writer exclusion, abrupt subprocess exit, staged
publication faults, private-resource checks and bounded corrupt-file refusal.
This is not physical power-loss, hostile same-UID or network-filesystem evidence.
Hosts still own authentication, authorization and exclusive store establishment;
the local storage adapter itself exposes no listener or implied authority from
producer/consumer IDs.

`hermes-protocol` now defines representation-independent append, read, gap,
consumer progress, compaction, status, confirmed-usage and content-safe
operational-observation values.
`hermes-service` owns volatile authenticated sessions and asks a command-scoped
authorization port before every durable-host operation; producer and consumer IDs
remain correlation only. `hermes-client` supplies disjoint append, consumer,
compaction and observation facades. A durable reopen test proves exact append
replay, session loss, retained gaps and replay-history loss across this boundary.
`hermes-disclosure-command`, `hermes-consumer-command`, and
`hermes-operator-command` own the exact-confirmation/current-state guards used
by standalone and Workbench shells. Presentation does not own disclosure,
acknowledgement, or compaction semantics.
`hermes-wire` carries every command, outcome and refusal in a canonical bounded
frame and validates event counts and payload lengths before allocation.
`hermes-local-daemon` adds a private mode-`0600` Unix connection profile with one
volatile authenticated session per connection, finite frame/request ceilings,
I/O deadlines, content-free health, truthful outcome-unknown classification and
inode-safe cleanup. Its real local-adapter journey proves exact append replay
after daemon and store reopen. `hermes-aegis` maps the closed stream action family
through Aegis' public client only, keeps grant handles/time private, and reserves a
service-observation resource so it cannot alias an authorized stream. An isolated
two-process profile proves a live Aegis stream-specific `Submit` grant permits
Hermes append while the absent `Observe` action denies read. The daemon
journey now covers append, exact replay, read, acknowledge, cursor-constrained
compaction, retained gaps and restart-stable replay-history loss. Production
local credential binding is supplied by `hermes-unix-peer`, which maps the
kernel-reported effective UID/GID through an exact bounded subject registry and
refuses unknown peers before session creation. Remote credentials and hostile
same-UID separation remain open. The same real-socket restart journey verifies
aggregate retained/publication edges, stalled-cursor backlog, fixed operation
residence, commit/checkpoint residence, confirmed usage and saturation without
exporting payloads or identities.

`applications/hermes-daemon` is the standalone selected local composition root.
It uses the same wire service and OS-peer adapter, requires explicit create
versus recover mode and one exact UID/GID-to-subject mapping, and selects either
an `observer` profile or an `operator` profile bound to one exact stream. The
observer grants read and observation. The operator grants read, observation,
acknowledgement and compaction only for its selected stream; append remains
denied and producer-owned. Signal shutdown wakes the accept loop without
creating a session or dispatching work.
The same exact daemon is qualified as a Workbench-supervised sovereign process
in headless and interactive modes. Workbench attaches the ordinary public
clients and reuses Hermes-owned disclosure, acknowledgement, and compaction
guards; it does not link the ledger/store implementation or acquire producer
append authority. This process-separated topology also avoids merging Hermes's
Keystone source graph with native Workbench peers.

See [requirements](requirements.md), [specification](specification.md), and the
[focused roadmap](docs/roadmap.md). The staged production program is tracked in
[ROADMAP.production.md](ROADMAP.production.md); [SECURITY.md](SECURITY.md) and the
[threat model](docs/threat-model.md) define current security claims and residual
risks.
The selected adapter and its nonclaims are documented in the
[Unix peer credential profile](docs/unix-peer-credentials.md).

## Development

```sh
make check
```

## SDK facade

`hermes-sdk-embedded` reexports the ledger engine and its receipt-gated host
boundary. It selects no durable storage or delivery adapter.

## Local delivery

Use `hermes_local::create_new(path, limits, budget)` only for privileged explicit
initialization of a **new leaf directory** under an existing trusted parent.
Use `open_existing` for all restarts. It never resets absent/corrupt state. Keep
the returned ledger alive to retain its OS writer lock. Do not copy/replace its
directory while open. An orphaned failed initial creation requires operator
inspection, not automatic deletion/reinitialization. There is no migration from
unversioned volatile state; V1 checkpoints migrate canonically to V2.

Every changed append/acknowledgment/compaction writes a whole bounded checkpoint and waits
for file and directory barriers (also `F_FULLFSYNC` for the file on macOS).
Replay/conflict/identity acknowledgment avoid writes. Observe host operation and
commit timing before tuning batching; there is no queue, hard I/O deadline or
constant-time append claim. Storage is plaintext private state, not a secret vault.

The test-only `hermes-conformance` crate exposes public-protocol inbox and outbox
suites for peer adapters. It deliberately has no engine, host, service, wire or
local-daemon dependency. For incident handling and safe retention operations,
follow the [local-service operator runbook](docs/operator-runbook.md).

