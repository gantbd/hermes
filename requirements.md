# Requirements

Status: proposed. Nothing here is a durability claim.

- **H1 — Ordered streams:** append succeeds only at the exact expected predecessor.
- **H2 — Opaque meaning:** Hermes validates envelope framing and bounds, not domain
  payload meaning.
- **H3 — Idempotency:** same producer key and identical event replays the original
  result; a different event under that key conflicts.
- **H4 — Independent consumers:** each consumer owns a monotone cursor and one
  consumer cannot acknowledge another's work.
- **H5 — Gap visibility:** bounded reads report truncation and unavailable ranges;
  absence is never silently treated as end-of-history truth.
- **H6 — Honest commit evidence:** refused, committed, and indeterminate append
  outcomes remain distinct.
- **H7 — Bounded retention:** streams, consumers, payloads, keys, read batches, and
  retained bytes have explicit ceilings and checked arithmetic.
- **H8 — Canonical records:** persisted records are versioned, deterministic, and
  decode under declared byte/work limits.
- **H9 — Effect isolation:** the core is deterministic; store adapters own I/O.

Open: compaction profiles, cryptographic integrity, replication, multi-stream
transactions, access control, durable adapter selection, and product SLOs.

