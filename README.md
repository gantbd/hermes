# Hermes

Hermes is the planned bounded event ledger for ordered append, replay, inbox
deduplication, and independent consumer progress.

Hermes carries domain events without interpreting them. Agora, Chronos, Vellum,
Moira, Aegis, and Daimon retain ownership of their event vocabularies and state.
The repository now contains a deterministic bounded volatile reference ledger.
It implements exact-predecessor append, producer-key replay/conflict detection,
bounded reads, and monotone independent consumer cursors. It makes no durable
store or crash-recovery claim.

The remaining first-profile work is a canonical record codec and independently
checked small-state model before any durable adapter is claimed.

See [requirements](requirements.md), [specification](specification.md), and the
[roadmap](docs/roadmap.md).

## Development

```sh
make check
```

