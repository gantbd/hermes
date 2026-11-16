# Deterministic soak profile

`make soak` runs 5,000 fresh authenticated Hermes service lifecycles. Every cycle
creates a receipt-gated in-memory ledger, appends and exactly replays an event,
appends a successor, compacts the prefix, reopens from the canonical checkpoint
and observes one retained event with an explicit before-retention gap.

The fixed campaign is opt-in so ordinary correctness tests remain fast and
focused. It detects replay-index, retention-edge, service-session and repeated
recovery regressions in the selected profile. It is not a latency benchmark,
filesystem/power-loss test, daemon/process soak, concurrent-producer proof,
replication test or product SLO.

`make soak-process` runs 32 fresh two-process durable ledger cycles. Each cycle
creates a private filesystem ledger in one daemon process, commits an append
whose reply is deliberately discarded, exits, reopens in a second daemon
process, proves the original append replays rather than duplicates, and reads
the exact one retained event. Every cycle uses fresh paths and removes them only
after both child processes exit and their sockets are gone.

The process campaign passed on 2026-11-10 in 2.64 seconds on Darwin arm64. It is
repeatability evidence for clean process exit, receipt-gated filesystem commits,
lost-reply reconciliation and canonical reopen. It is not sudden power-loss,
kill-during-fsync, concurrent producer/consumer load, retention pressure,
replication, resource-leak or product-availability evidence.
