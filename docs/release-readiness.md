# Release readiness

Status: source-verifiable durable local retention ledger profile preparing for
public GitHub source distribution; not ready for a network service, registry
publication or production deployment.

Established evidence is recorded in `docs/traceability.md`. Repository license
texts, exact license-declaration baselines and digest-pinned empty/live checkpoint
fixtures and a lockfile-bound clean RustSec attestation are present. Current
blockers are future-version fixture exercise, source-content/legal review,
production remote credential binding, completion of multi-process peer
qualification beyond the established Aegis authorization and Moira producer
segments, power-loss qualification and any selected replication profile.
The optimized in-memory append/read/compaction/recovery budgets and opt-in
5,000-cycle service soak are green. A 32-cycle clean-exit filesystem/process
campaign is also green, and Moira independently repeats its selected producer
segment across sixteen fresh process pairs plus its integrated execution-to-event
chain across sixteen six-process sets. None of these establishes power-loss,
concurrent load, replication or product-SLO evidence.

Passing `make check` establishes only the current source profile and explicitly
does not authorize publication.

