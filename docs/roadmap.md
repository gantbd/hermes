# Roadmap

## Phase 0 — vocabulary and laws

- Close IDs, envelopes, limits, append outcomes, cursors, and refusal precedence.
- Define canonical equality for idempotent replay.
- Add exhaustive sequence/cursor law tests under small bounds.

## Phase 1 — volatile reference ledger

- [Complete] Implement deterministic in-memory streams and consumers.
- [In progress] Add generated reference-model comparison, broader saturation,
  replay, conflict, and gap scenarios. Core replay/conflict/cursor laws are
  covered by behavior tests.
- Implement a bounded canonical V1 codec and parser robustness corpus.

## Phase 2 — store protocol

- Define optimistic store revisions and attributed receipts.
- Inject pre-commit, mid-commit, post-commit/pre-reply, and recovery faults.
- Add a trusted-directory adapter only after the protocol is stable.

## Phase 3 — integration

- Qualify Moira first, then Agora/Chronos outbox ingestion.
- Keep domain schemas outside Hermes.
- Add per-consumer operational metrics without domain labels or payload content.

