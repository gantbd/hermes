# Unix peer credential profile

Status: qualified for the selected private local Unix deployment profile.

`hermes-unix-peer` asks the operating system for the effective UID and GID of the
connected Unix-domain-socket peer. A bounded deployment-owned table maps that
exact pair to a Hermes subject. Producer IDs, consumer IDs, stream IDs and Aegis
authorization policy never enter this mapping.

The mapping ceiling must be positive, the table must be nonempty and within the
ceiling, and duplicate peer pairs are refused. Missing kernel evidence and
unknown peers fail closed before the daemon emits readiness, creates a service
session or dispatches a domain command. Mapping and error `Debug` values disclose
neither UID/GID nor subject material.

The adapter supports Linux/Android peer credentials and `getpeereid` platforms
including macOS and the supported BSD family. Other Unix targets fail
authentication. The launcher owns trusted-parent/socket placement, mapping
provisioning and account lifecycle.

This profile authenticates an operating-system account boundary, not a person,
binary, container, remote machine or individual process among peers sharing the
same effective UID/GID. It supplies no passwords, tokens, certificates, account
lookup, executable attestation, revocation feed or protection from a malicious
kernel/root operator. Those profiles require separate adapters and evidence.
