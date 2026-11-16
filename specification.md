# Initial specification

## Append

```text
append(stream, expected_predecessor, producer_key, envelope)
  -> appended(sequence) | replayed(sequence) | conflict | refusal | indeterminate
```

Validation and capacity checks occur before publication. A successful append is one
serialized state change. A producer-key replay compares the canonical event, not a
caller assertion that it is equivalent.

## Read

Reads specify a start sequence and positive bounded limit. Results declare the
retained low edge, current published edge, and a closed gap value. A start before
the retained edge returns `BeforeRetention` and begins at that edge. A start above
the next sequence returns `AheadOfPublication`. A start exactly equal to the next
sequence is an ordinary empty end-of-stream result, never a gap.

## Consumer progress

Cursor advancement cannot exceed the observed published edge or regress. Repeating
the same advancement is an identity. Cursor state is separate from domain effects;
advancing a cursor does not prove that a downstream side effect occurred.

## Retention

`compact_through(stream, sequence)` is explicit and monotone. Every registered
cursor for that stream must already point beyond the compacted range; a stalled
cursor blocks compaction and is never advanced by it. Removed envelopes become
producer-key tombstones. A later use of that key returns `ReplayHistoryLost` with
the original sequence, whether its proposed content matches or conflicts, and can
never append a second event under the key.

## Implemented volatile profile

The in-memory `Engine` uses fixed-width opaque identities and explicit limits for
streams, retained events, producer idempotency records, consumers, payload bytes,
and read batches. Producer-key replay is resolved before predecessor comparison,
so a lost reply can be closed after the stream edge has advanced. Changed
canonical envelope content returns `Conflict`; it is never treated as replay.
Payload debug formatting exposes only byte count. `Indeterminate` remains part of
the store-facing outcome algebra but is never fabricated by the volatile core.

## Canonical checkpoints

V1 remains accepted for migration. V2 writes length-prefixed `HERMESCP`, a stream
count, lexically ordered streams with retained/next edges and contiguous retained
events, lexically ordered compacted producer-key tombstones, then lexically ordered
`(consumer, stream)` cursors. Counts and sequences use
big-endian u64; byte fields have big-endian u32 lengths; identities are exactly
16 bytes. Optional causation has the closed tags 0/1. Payloads remain arbitrary
bytes. A cursor must reference an existing stream and advance through a published
event. Empty streams, duplicate producer keys, noncanonical order, invalid cursor
positions, unknown versions/tags, truncation and trailing bytes are refused.

Recovery rebuilds and validates live producer-key correspondence and validates
every tombstone against its stream retention edge. Host-supplied engine limits and
checkpoint byte/field budgets apply before publication; decoded data cannot enlarge
them. Checkpoint bytes are sensitive state,
not diagnostics, cryptographic integrity evidence or a durable acknowledgment.
Standard allocation failure is not an allocator/RSS or recoverable-OOM contract.

## Persistence-gated native host

`hermes-host` keeps one private engine and an explicitly selected checkpoint
store. `create_new` refuses existing state and commits an initial empty ledger;
`open` refuses missing/weak state rather than silently initializing it. The
host's V1 storage envelope binds the canonical checkpoint to its storage revision.
Both loaded checkpoints and commit receipts must carry durable acknowledgment
strength; a mechanism name or revision alone is insufficient evidence.

Changed append/cursor state is prepared privately and published only after the
whole checkpoint is acknowledged at the exact next storage revision. Matching
replay, conflict and repeated cursor acknowledgment do not commit again. Once a
candidate is offered to storage, any error, refusal, indeterminate outcome or bad
receipt fences the instance including reads. Reopen checks the actual winner;
it never retries the mutation automatically. `hermes-local` supplies the qualified
local directory wiring; synthetic fault tests and concrete restart/lost-receipt
tests are separate. Neither is physical power-loss certification.

## Authenticated native service

The transport-neutral service holds authenticated subjects only in bounded
volatile sessions. Before append, read, cursor, acknowledgment or compaction it
asks an external authorization port about the exact stream and action; status,
confirmed usage and operational signals require service-level observe authority.
Per-stream observation requires observe authority on that exact stream. Producer
and consumer IDs never participate in that decision. Denial occurs before the
durable host is touched. Session handles are intentionally invalid after service
restart.

The separate `hermes-unix-peer` adapter supplies the selected private-local
credential profile. It maps the kernel-reported effective UID/GID of the
connected socket through an exact bounded deployment table to a subject and
fails unknown peers before session creation. It owns no stream or authorization
policy; remote credentials and hostile same-UID separation remain outside V1.

Protocol DTOs are independent of core representation and preserve the closed
append replay/conflict/history-loss algebra, retained/published edges and read-gap
values. Capability-shaped clients separate append, consumer, compaction and
observation commands and apply a local positive read ceiling. A durable host
reopen journey proves the protocol mapping preserves retained gaps and tombstones;
the canonical bounded wire codec preserves the full closed algebra and refuses
unbounded declared payload/page growth. The qualified private daemon proves the
same append/read/ack/cursor/compact/gap and replay-history-loss behavior across
real socket plus durable-store reopen. Multi-process federation evidence remains
open. The content-free observation contract reports retention/publication edges,
retained-event count, consumer count, stalled-consumer count and maximum backlog
per authorized stream. Service-wide signals report fixed operation classes,
residence and commit time, refusals/replays/conflicts/commits, confirmed usage,
checkpoint bytes, fencing and saturation. It reports no payload, subject,
producer key or consumer identity.

## Public delivery conformance

The test-only `hermes-conformance` package depends solely on the representation-
independent protocol. Its harness requires one authenticated command path, an
explicit control that dispatches a command exactly once while discarding the
reply, and a restart control that reopens the same durable ledger with a fresh
session. The outbox suite fixes lost-reply replay, changed-input conflict,
restart and compacted-key history-loss behavior. The inbox suite fixes
independent cursor positions, acknowledgement-gated compaction, repeat
acknowledgement, retained gaps and restart behavior.

The harness controls are qualification capabilities, not production retry APIs.
The suite does not select authorization, retention, storage, transport, recovery
or consumer-side effect policy. Passing it proves correspondence to this bounded
Hermes V1 delivery contract only; it does not prove the peer's domain semantics
or end-to-end exactly-once processing.

