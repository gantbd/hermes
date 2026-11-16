# Compatibility profile

Status: package/source topology and canonical empty V1/V2 plus live V2
checkpoint frozen;
cross-version compatibility qualification still open.

`make check-compatibility-baseline` refuses unreviewed package-name, version or
manifest-path drift against `compatibility-baseline.txt`. The current canonical
empty and live checkpoint fixtures are external, digest-pinned and consumed by
the migration and recovery tests. The live fixture retains two events and two
independent consumer cursors. The isolated Aegis process-qualification package
is inventoried as assurance topology but does not join Hermes's root workspace or
public package graph.
Until a broader valid/invalid corpus is exercised across released versions, this
repository makes no general upgrade or downgrade compatibility promise.

`hermes-unix-peer` adds a bounded deployment mapping from OS effective UID/GID
pairs to subjects. The mapping is a source-level V1 input; no credential file or
wire format is defined.

The public Aegis adapter reserves the exact resource bytes
`HERMESSERVICEV1!` for service observation and refuses that value as a stream.
All other stream resources are their exact 16-byte stream IDs. Changing this
mapping or the Append→Submit, Read→Observe, Acknowledge→Modify and
Compact→Administer action correspondence requires explicit compatibility review
because deployed grants scope those exact values.
