# Release and distribution policy

Status: coordinated local source distribution only; no registry or binary
publication is authorized.

Every Hermes package sets `publish = false`. The current checkout depends on a
sibling Keystone checkout through exact local paths. Passing repository tests
does not establish a self-contained source archive, remote Git dependency,
registry package, signed artifact, installer or support lifetime.

The selected release unit contains the ledger core, durable host, local storage
and Unix peer-credential adapters, daemon and embedded SDK. It excludes domain
schemas, remote authentication, authorization policy, remote transports and peer
products. The adapters' claims apply only to their documented private local
profiles.

Enabling a distribution channel requires reviewed package contents, immutable
dependency provenance, license texts and compatibility review, SBOM/advisory
evidence, installation/upgrade/removal qualification, updated security and
readiness records, and a deliberate change to the fail-closed publication gate.

