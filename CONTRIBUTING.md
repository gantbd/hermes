# Contributing to Hermes

Hermes is source-distributed and has not made a production release. Preserve
the ledger boundaries and nonclaims in `docs/release-policy.md`,
`docs/release-readiness.md`, and `SECURITY.md`.

The repository targets the Rust toolchain pinned by `rust-toolchain.toml` and
uses the exact Keystone and Aegis source revisions recorded in
`release/github-source.json`. Run both the root and peer-profile gates before
submitting changes:

```sh
make check
make check-peer-profiles
```

The process and Unix-socket tests require an environment that permits local
subprocesses and Unix-domain sockets.

- Keep every crate non-publishable unless the release policy changes in the
  same reviewed contribution.
- Preserve explicit bounds, typed uncertainty, append-only ledger semantics,
  and content-safe diagnostics.
- Treat compatibility fixtures, attestations, traceability records, and release
  profiles as source artifacts.
- Do not commit credentials, private user data, local paths, or build output.

Report suspected vulnerabilities through `SECURITY.md`, not a public issue.
