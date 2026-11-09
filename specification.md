# Initial specification

## Append

```text
append(stream, expected_predecessor, producer_key, envelope)
  -> appended(sequence) | replayed(sequence) | conflict | refusal | indeterminate
```

Validation and capacity checks occur before publication. A successful append is one
serialized state change. A producer-key replay compares the canonical event, not a
caller assertion that it is equivalent.

## Read

Reads specify a start sequence and positive bounded limit. Results declare the
returned range, current published edge, and whether earlier records were compacted.

## Consumer progress

Cursor advancement cannot exceed the observed published edge or regress. Repeating
the same advancement is an identity. Cursor state is separate from domain effects;
advancing a cursor does not prove that a downstream side effect occurred.

## Implemented volatile profile

The in-memory `Engine` uses fixed-width opaque identities and explicit limits for
streams, retained events, producer idempotency records, consumers, payload bytes,
and read batches. Producer-key replay is resolved before predecessor comparison,
so a lost reply can be closed after the stream edge has advanced. Changed
canonical envelope content returns `Conflict`; it is never treated as replay.
Payload debug formatting exposes only byte count. `Indeterminate` remains part of
the store-facing outcome algebra but is never fabricated by the volatile core.

