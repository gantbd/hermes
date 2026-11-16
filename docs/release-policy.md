# Release and distribution policy

Status: coordinated GitHub source distribution is being qualified; no registry or binary
publication is authorized.

Every Hermes package sets `publish = false`. The current checkout depends on the
exact sibling Keystone and Aegis revisions recorded in
`release/github-source.json` through local paths. Passing repository tests does
not establish a registry package, signed artifact, installer or support
lifetime. GitHub source publication is a separate, source-only channel and does
not weaken those nonclaims.

The selected release unit contains the ledger core, durable host, local storage
and Unix peer-credential adapters, daemon and embedded SDK. It excludes domain
schemas, remote authentication, authorization policy, remote transports and peer
products. The adapters' claims apply only to their documented private local
profiles.

Enabling a distribution channel requires reviewed package contents, immutable
dependency provenance, license texts and compatibility review, SBOM/advisory
evidence, installation/upgrade/removal qualification, updated security and
readiness records, and a deliberate change to the fail-closed publication gate.

