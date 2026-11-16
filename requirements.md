# Requirements

Status: selected V1 contract. Requirement-level implementation status is tracked
in `docs/traceability.md`; the retention-aware volatile core, exclusive local
durable profile and authenticated in-process service boundary are implemented.
The Moira producer additionally qualifies discarded-reply replay across actual
Hermes process exit and durable reopen using only public client/wire contracts.
This document alone is not a durability or deployment claim.

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
- **H10 — Content-safe operation:** service observations expose only bounded
  aggregate edges, backlog, operation/commit timing, capacity and saturation;
  they never expose payloads, subjects, producer keys or consumer identities.
- **H11 — Portable delivery evidence:** peer adapters can run the same public
  protocol inbox/outbox contract for outcome-unknown dispatch, exact replay,
  conflict, restart, independent cursors, acknowledgement and truthful gaps
  without depending on Hermes internals.

The selected private local Unix profile binds kernel effective UID/GID pairs to
bounded Hermes subjects. Open: remote credential binding, retention policy automation, cryptographic
integrity, replication, multi-stream transactions, the remaining peer profile,
additional durable adapters, and product SLOs.
The first selected adapter is the exclusive private local Unix directory profile;
its tested process/recovery contract is not physical power-loss certification.

