# Private local daemon profile

Status: qualified bounded Unix append/read/ack/compact/gap profile with selected
OS peer authentication and explicit observer/operator standalone application
profiles. External supervision and producer-authority application profiles
remain open.

`hermes-local-daemon` binds only a new filesystem path, sets mode `0600` and
refuses to replace an existing entry. The embedding launcher selects the trusted
parent. An authentication adapter derives a bounded volatile subject session for
each accepted connection; producer and consumer IDs remain event/cursor data and
confer no authority.
`hermes-unix-peer` reads the connected socket's kernel effective UID/GID and maps
that exact pair through a bounded deployment registry to the volatile subject.
Unknown peers fail before readiness, session creation or durable-host dispatch.

The daemon sends a canonical readiness frame, applies finite read/write deadlines
and serves a configured finite sequence of length-prefixed bounded requests. The
service still calls its action/resource authorization port before durable-host
dispatch. A partial input frame is pre-dispatch failure. Failure to write a reply
after service execution is outcome-unknown, requiring reconnect and replay or
inspection rather than inferred success/failure.

Embedding shells may use `serve_until` with a caller-owned atomic stop flag.
The shutdown requester sets the flag and opens one wake connection; the daemon
checks the flag immediately after accept, so the wake connection is never
authenticated, assigned a session, or dispatched. The owner thread then drops
the listener and its identity-guarded socket path. This supplies lifecycle
mechanics only: storage create/recovery, peer mapping, authorization and limits
remain deployment inputs.

`hermes-daemon create|open DIRECTORY SOCKET EFFECTIVE_UID EFFECTIVE_GID
SUBJECT_HEX observer` and `hermes-daemon create|open DIRECTORY SOCKET
EFFECTIVE_UID EFFECTIVE_GID SUBJECT_HEX operator OPERATOR_STREAM_HEX` are the
general observation and operator roots. `hermes-daemon create|open DIRECTORY
SOCKET EFFECTIVE_UID EFFECTIVE_GID SUBJECT_HEX delivery DELIVERY_STREAM_HEX`
selects the minimized delivery root. All explicitly create or recover
the durable ledger and map one configured OS peer identity to one subject. The
observer grants only `Read` and `Observe`. The operator grants `Read`,
`Observe`, `Acknowledge`, and `Compact` only for the exact selected stream;
wrong-stream operations and all `Append` requests are denied. The delivery
profile grants only `Append`, `ObserveCursor`, and `Acknowledge` for one exact
stream. Cursor observation is deliberately distinct from payload `Read`, so
reconciliation authority cannot disclose events or compact retention.
`SIGINT`/`SIGTERM`
set the owner stop flag and wake the blocked accept without authenticating or
dispatching the wake connection. The process gate proves observer create/reopen
and mutation refusal plus selected-operator read, acknowledgement, compaction,
wrong-stream denial, clean shutdown and identity-guarded socket removal through
the ordinary wire protocol. The binary never infers authority from local
process ownership, presentation selection, producer IDs or consumer IDs.

Health exposes only readiness and saturated connection/outcome counters. Drop
removes the socket only if device and inode still match the entry this daemon
created. The real-socket journey covers authenticated negotiation, private mode,
durable append, exact replay, read, acknowledgment, cursor-constrained compaction,
truthful retention gaps, restart-stable cursor/gap evidence, replay-history loss,
health and cleanup. It is not a remote-network, hostile-host,
physical-power-loss, remote-credential or hostile same-UID profile.
