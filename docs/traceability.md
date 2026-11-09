# Traceability ledger

| Requirement | Planned implementation | Current evidence | Status |
| --- | --- | --- | --- |
| H1 ordering | exact-predecessor append over ordered stream vectors | predecessor mismatch and sequential append scenarios | Satisfied for volatile profile |
| H2 opacity | bounded payload envelope with redacted debug | payload bound and redaction scenario | Satisfied for volatile profile |
| H3 idempotency | producer/key append index with canonical envelope comparison | lost-reply replay and changed-content conflict scenarios | Satisfied for volatile profile |
| H4 consumers | independent `(consumer, stream)` cursor map | monotone, regression, and beyond-edge scenarios | Satisfied for volatile profile |
| H5 gaps | bounded range result with published edge | bounded read behavior; compaction absent | Partial; truncation profile open |
| H6 commit truth | store receipt sum type | none | Open |
| H7 bounds | explicit stream/event/idempotency/consumer/payload/read limits | constructor and payload boundary evidence | Partial; broader saturation evidence open |
| H8 codec | canonical V1 records | none | Open |
| H9 isolation | dependency-free deterministic core | source boundary and strict build graph | Satisfied for volatile core |

