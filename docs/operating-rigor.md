# Operating rigor

- Event envelopes, append outcomes, cursor outcomes, refusals, and codecs are closed
  and explicitly versioned.
- All byte, cardinality, work, and sequence bounds are checked before publication.
- Refusal leaves stream and cursor state unchanged.
- Event visibility, possession, producer identity, and consumer authority are
  separate facts.
- At-least-once delivery is the baseline. Cross-system exactly-once is not claimed.
- Unknown commit evidence remains unknown until attributable read-back resolves it.
- Compaction changes retained availability, never historical sequence identity.
- Adapters report only mechanisms actually tested on declared environments.
- Missing durability, crash, fuzz, or corruption evidence stays open.

