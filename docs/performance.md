# Performance qualification

`make performance` runs an ignored optimized acceptance test for the selected
receipt-gated in-memory ledger profile. The target records the Rust compiler, OS
and architecture, then measures 64 successful samples each of append, read,
compaction, and canonical checkpoint recovery. Setup and compilation are outside
the samples; every measured operation retains its functional assertions.

The provisional reference-host engineering budgets are p95 <= 10 ms and sample
p99 <= 25 ms for each operation class. With 64 samples nearest-rank p99 is the
maximum, not a statistically established production tail. On 2026-11-10 the
`aarch64-apple-darwin` Rust 1.95.0 optimized run produced:

| Operation | p50 ns | p95 ns | sample p99/max ns |
| --- | ---: | ---: | ---: |
| Append | 667 | 3,000 | 15,334 |
| Read | 167 | 250 | 1,250 |
| Compact | 750 | 2,167 | 2,708 |
| Recover | 209 | 459 | 1,167 |

This is a regression gate for the exact one-stream synthetic profile, not a
product SLO. The in-memory receipt store performs no filesystem sync. The
population excludes daemon transport, authentication, large payloads/streams,
concurrent producers/consumers, sustained retention pressure, allocation/RSS and
power loss. Durable filesystem and process-soak qualification remain separate.
