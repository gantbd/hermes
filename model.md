# Domain model

A `StreamId` identifies an ordered sequence. Each accepted event receives the next
`EventSequence`; stream position begins at zero. Producers supply an idempotency key
and expected predecessor. Payload bytes are opaque and paired with a bounded
`SchemaId`, correlation ID, and optional causation reference.

A consumer cursor is `(ConsumerId, StreamId, next_sequence)`. Advancing it is a
monotone compare-and-set operation. Delivery is at-least-once; consumers must make
their own effects idempotent.

Hermes distinguishes logical state from storage evidence. A store receipt proves
only the adapter mechanism and environment stated by that receipt. Timeout or lost
reply can yield an indeterminate commit that requires read-back reconciliation.

