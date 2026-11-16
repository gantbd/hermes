# Domain model

A `StreamId` identifies an ordered sequence. Each accepted event receives the next
`EventSequence`; stream position begins at zero. Producers supply an idempotency key
and expected predecessor. Payload bytes are opaque and paired with a bounded
`SchemaId`, correlation ID, and optional causation reference.

A consumer cursor is `(ConsumerId, StreamId, next_sequence)`. Advancing it is a
monotone compare-and-set operation. Delivery is at-least-once; consumers must make
their own effects idempotent.

Each stream has a monotone retained edge and next sequence. Explicit compaction is
allowed only behind every registered cursor. Reads distinguish a request behind
retention, beyond publication, and exactly at end-of-stream. Compacted producer
keys remain as bounded tombstones, so replay-history loss is visible and an old key
can never create a second event.

Hermes distinguishes logical state from storage evidence. A store receipt proves
only the adapter mechanism and environment stated by that receipt. Timeout or lost
reply can yield an indeterminate commit that requires read-back reconciliation.

## Implemented durable local pathway

`hermes-local -> hermes-host -> pure ledger` separates filesystem wiring,
publication/recovery and domain transitions. The host prepares privately, encodes
a bounded canonical checkpoint with its storage revision, offers it through
Keystone's checkpoint port and publishes only after an exact durable receipt.
Errors or uncertain offers fence all use until explicit reopen; missing/corrupt
state never becomes a new ledger. Reopen reconstructs retained events,
producer-key tombstones and independent consumer cursors. Matching replay/conflict,
replay-history loss, identity compaction and identity cursor acknowledgment do not
write.

The local adapter owns private files, one OS writer lock and staged file/directory
barriers; it grants no domain authority. Authentication, producer/consumer access,
domain payload schemas and downstream idempotency remain host/consumer duties.
The adapter itself has no socket or UI. Full checkpoints have bounded size but
linear copy/write cost; no constant-time or physical power-loss claim is made.

The native service adds a volatile authenticated subject/session layer above the
durable host. A separate authorization port receives `(subject, action,
service-or-stream)` before dispatch. It does not receive payloads, producer keys
or consumer IDs. Service restart deliberately destroys session handles while the
ledger, retained edge, tombstones and cursors recover from durable state.

The private daemon maps one authenticated Unix connection to one such volatile
session and applies bounded frames, request count and I/O deadlines. It does not
move authorization, retention or cursor semantics into the transport. Reply loss
after dispatch is explicitly outcome-unknown and must be closed by replay or read.

RED/golden signals: five fixed operation offer/duration classes, refusals,
replays/conflicts, commit timing/failures and fencing. USE: confirmed stream/event/
cursor counts, checkpoint bytes versus configured budgets and one serialized
writer. Per-stream observation exposes only retained/published edges, retained
count, aggregate consumer/stall counts and maximum backlog. Observations contain
no identities/payloads and cannot establish success.
STRIDE/LINDDUN: replay conflict, stale predecessor, corrupt framing and bad receipts
refuse; private bounded storage narrows disclosure/DoS but is not encryption,
authenticated storage, an external rollback witness or a hostile same-UID defense.

