.PHONY: check check-peer-profiles check-release-contract check-compatibility-baseline check-supply-chain check-advisory-attestation check-assurance-mutants check-sdk-boundaries fmt lint test doc soak soak-process performance qualify-aegis-process

check: check-release-contract check-compatibility-baseline check-supply-chain check-advisory-attestation check-assurance-mutants check-sdk-boundaries fmt lint test doc

check-peer-profiles: qualify-aegis-process

check-release-contract:
	bash scripts/check-release-contract.sh

check-compatibility-baseline:
	bash scripts/check-compatibility-baseline.sh

check-supply-chain:
	bash scripts/check-supply-chain.sh

check-advisory-attestation:
	bash scripts/check-advisory-attestation.sh

check-assurance-mutants:
	bash scripts/check-assurance-mutants.sh

check-sdk-boundaries:
	./scripts/check-sdk-boundaries.sh

fmt:
	cargo fmt --all -- --check

lint:
	cargo clippy --workspace --all-targets --all-features --locked --offline -- -D warnings

test:
	cargo test --workspace --all-targets --locked --offline

doc:
	cargo test --workspace --doc --locked --offline
	RUSTDOCFLAGS="-D warnings" cargo doc --workspace --no-deps --locked --offline

soak:
	cargo test -p hermes-service --test service_behavior five_thousand_service_lifecycles --locked --offline -- --ignored --nocapture

soak-process:
	cargo test -p hermes-local-daemon --test unix_listener thirty_two_process_recovery_cycles --locked --offline -- --ignored --nocapture

performance:
	rustc -Vv
	uname -srm
	cargo test -p hermes-service --test service_behavior local_candidate_performance_gate --release --locked --offline -- --ignored --nocapture

qualify-aegis-process:
	cargo fmt --manifest-path qualification/aegis-process-authorization/Cargo.toml -- --check
	cargo clippy --manifest-path qualification/aegis-process-authorization/Cargo.toml --all-targets --locked --offline -- -D warnings
	cargo test --manifest-path qualification/aegis-process-authorization/Cargo.toml --all-targets --locked --offline

