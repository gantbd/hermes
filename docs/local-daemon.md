# Private local daemon profile

Status: qualified bounded Unix append/read/ack/compact/gap profile with selected
OS peer authentication. Executable packaging and supervision remain open.

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

Health exposes only readiness and saturated connection/outcome counters. Drop
removes the socket only if device and inode still match the entry this daemon
created. The real-socket journey covers authenticated negotiation, private mode,
durable append, exact replay, read, acknowledgment, cursor-constrained compaction,
truthful retention gaps, restart-stable cursor/gap evidence, replay-history loss,
health and cleanup. It is not a remote-network, hostile-host,
physical-power-loss, remote-credential or hostile same-UID profile.
