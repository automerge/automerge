.PHONY: all
all: ci

.PHONY: fmt
fmt:
	cargo fmt --all -- --check

.PHONY: clippy
clippy:
	cargo clippy --all-targets --all-features -- -D warnings

.PHONY: doc
doc:
	cargo doc --workspace --all-features

.PHONY: build
build:
	cargo build --all-targets --workspace

.PHONY: test
test: test-rust test-wasm test-js
	cargo test --workspace

.PHONY: test-rust
test-rust:
	cargo test --workspace

.PHONY: test-wasm
test-wasm:
	wasm-pack test automerge-frontend --node

.PHONY: test-js
test-js:
	cd automerge-backend-wasm && yarn release && yarn test:js

.PHONY: ci
ci: fmt clippy doc build test
