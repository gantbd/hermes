# Hermes threat and privacy model

Status: baseline for the embedded, exclusive local-directory, authenticated
service, private local-daemon and public-client Aegis profiles.

## Assets and trust boundaries

Protected assets are event ordering, payload bytes, producer replay keys,
correlation/causation facts, consumer cursors, retained-edge evidence, checkpoint
state and operational signals. Trust boundaries exist between domain producers
and Hermes, consumers and Hermes, the deterministic engine and durable host, the
host and checkpoint adapter, and the private directory and operating system.

The current profile trusts the embedding host, its authentication adapter,
existing parent directory, current UID and non-rollback local storage. The native
service accepts verified subjects and the public-client Aegis adapter authorizes
the exact action/resource before dispatch. The selected Unix adapter binds the
kernel-reported effective UID/GID through an exact deployment-owned subject
mapping. The profile provides no encryption, replication, hostile
same-UID defense or physical-power-loss certification.

## STRIDE controls and residual risks

| Threat | Current control | Residual risk / required gate |
| --- | --- | --- |
| Producer/consumer spoofing | IDs confer no authority; selected Unix adapter maps kernel effective UID/GID before volatile subject creation and Aegis authorizes each command | Mapping provisioning, malicious kernels, remote credentials and hostile same-UID peers remain outside the profile |
| Event substitution or key aliasing | Exact envelope replay comparison and changed-input conflict | Domain schema authenticity remains outside Hermes |
| Order or cursor tampering | Exact predecessor, monotone consumer progress and cursor-blocked compaction | Retention policy selection remains an operator/service concern |
| Commit repudiation | Exact durable receipt and restart-stable replay | No durable external audit or remote delivery proof |
| Payload disclosure | Redacted `Debug`, aggregate identity-free observations, private files | Payloads and checkpoints are plaintext to the trusted host/UID; stream-level activity remains sensitive metadata |
| Denial of service | Explicit stream/event/consumer/payload/read, service session/read, daemon frame/request and I/O deadline bounds | Principal fairness and production admission policy remain open |
| Durable-state tampering | Canonical decoder, private modes, no-follow and pinned resources | No cryptographic integrity or rollback witness |
| Replay loss through retention | Compacted producer keys remain bounded tombstones | `ReplayHistoryLost` refuses silent re-append; exact replay/conflict classification is intentionally unavailable after compaction |

## LINDDUN privacy considerations

Opaque payload handling does not make metadata private. Stream activity, event
sizes, producer-key reuse, cursor lag and timing can reveal relationships and
behavior. Metrics must avoid stream, producer, consumer and correlation labels.
The native observation contract exposes aggregate stream edges/backlog only after
exact-stream observe authorization and service-wide signals only after service
observe authorization. Operators must protect checkpoint copies and diagnostic exports as sensitive
plaintext even when payload bodies are not rendered.

## Security release gates

- Every service caller is authenticated separately from producer/consumer IDs.
- Aegis authorization precedes append, read and acknowledgement disclosure.
- Gap and retention evidence is explicit; absence never claims complete history.
- Indeterminate or weak durable evidence fences the live owner.
- Compaction preserves or explicitly retires replay correspondence.
- New storage/replication profiles carry separate integrity and failure claims.

