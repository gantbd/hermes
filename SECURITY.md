# Security policy

## Supported versions

Hermes has not made a production release. The `main` branch is the only line
currently considered for security fixes; historical snapshots and untagged
source archives are unsupported.

## Reporting a vulnerability

Do not disclose a suspected vulnerability in a public issue, discussion, pull
request, chat room or fixture. Use GitHub private vulnerability reporting when
available, or contact the repository owner privately through their GitHub
profile to establish a confidential channel.

Include the affected revision, impact, preconditions and a minimal reproduction.
Do not attach real event payloads, producer keys, consumer identities, stream
names or unredacted durable records.

## Security boundary

Hermes orders opaque bounded events and consumer progress. It is not an
authentication provider, authorization policy engine, encrypted vault, workflow
coordinator or consensus system. Producer and consumer identifiers confer no
authority. Hosts must authenticate and authorize callers, protect durable state
and map domain schemas outside Hermes. The selected `hermes-unix-peer` adapter
authenticates only the local OS account boundary from kernel effective UID/GID;
the host owns its exact subject mapping. The concrete
[threat model](docs/threat-model.md) records current controls and residual risks.

