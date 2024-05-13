init:
	./misc/certs-generate.sh

fmt:
	cargo fmt -- --check

clippy:
	cargo clippy --features=local-memory --workspace -- -D warnings
	cargo clippy --features=local-persist --workspace -- -D warnings
	cargo clippy --features=distributed --workspace -- -D warnings

test:
	cargo test --features=local-memory --workspace
	cargo test --features=local-persist --workspace
	cargo test --features=distributed --workspace

pre-push: fmt clippy test

fix:
	cargo fmt
	cargo clippy --fix --allow-staged --allow-dirty --all-features --workspace -- -D warnings
