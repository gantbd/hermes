# Hermes local-service operator runbook

Status: V1 private single-owner service profile. These procedures do not claim
remote replication, automatic failover, or physical power-loss recovery.

## Evidence before action

Use an authenticated read-only client to capture `Status`, `Usage`, `Signals`
and the affected stream's `StreamObservation`. Preserve the exact refusal or
transport classification and the service/store logs. Identifiers correlate an
incident but confer no authority. Do not edit checkpoint files, advance a
cursor, compact a stream, or retry an outcome-unknown mutation merely to make an
alert disappear.

## Stalled consumers

A nonzero `stalled_consumers` or `max_consumer_backlog` is diagnostic evidence,
not permission to move a cursor. Identify the owning consumer outside Hermes,
compare its durable side effect with its Hermes cursor, and resume it from the
reported `next_sequence`. If the requested sequence is before `retained_from`,
stop: the explicit gap requires consumer-owned rebuild or reconciliation. Remove
or supersede a consumer only through an owning application policy; Hermes V1 has
no administrative cursor-forcing command.

## Capacity exhaustion

Classify the exact refusal: payload, stream, stream-event, idempotency, consumer,
checkpoint, or sequence capacity. Retrying unchanged input cannot create
capacity. Compaction can release retained event payload capacity only when every
registered cursor is beyond the proposed range; it intentionally retains
producer-key tombstones and cannot cure idempotency-key exhaustion. Increasing
limits requires a reviewed offline configuration and successful reopen of the
existing checkpoint. Never create a new ledger over the old path.

## Outcome-unknown commits

A lost reply after dispatch is not a refusal. Stop blind retries and preserve the
original stream, predecessor, producer, idempotency key, envelope and session
context. Reopen after any fenced host, establish a fresh authenticated session,
then resubmit the byte-for-byte equivalent append. `Replayed` closes success;
`Conflict` proves changed input under the key; `ReplayHistoryLost` proves the key
was used in compacted history but cannot reconstruct the original payload. For
acknowledgement or compaction uncertainty, read the cursor and retention edges
before deciding whether the requested state is already present.

## Compaction

Record the proposed `through` sequence and all relevant cursors before issuing
the operator command. `CompactionBlocked` is a safety result: advance consumers
only after their own effects are durable. After success, verify `retained_from`,
read from the old edge to observe `BeforeRetention`, and retain the operational
record. Compaction is not deletion of replay identity; tombstones deliberately
turn later key reuse into `ReplayHistoryLost`.

## Recovery and restart

Stop the daemon and ensure the previous process has exited before reopening the
same private directory. Use `open_existing`; never fall back to `create_new` for
missing, corrupt, busy, weak, or uncertain state. A fenced instance releases no
further reads or writes. Quarantine the directory on validation failure and
restore only a separately verified backup under the documented release profile.
After reopen, establish fresh sessions and verify status, usage, stream edges,
cursors, replay of a retained key, and any incident-specific uncertain command
before returning traffic.
