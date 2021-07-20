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

.PHONY: build-wasm
build-wasm:
	cd automerge-backend-wasm && yarn dev

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
test-js: build-wasm
	cd automerge-backend-wasm && yarn test:js

.PHONY: ci
ci: fmt clippy doc build test

.PHONY: book
book:
	cd docs && mdbook serve

.PHONY: gh-pages
gh-pages:
	mdbook build docs --dest-dir ../book
	cargo doc --workspace --all-features
	mv target/doc docsrs
	git checkout gh-pages
	git add book docsrs
	git commit -m "Update documentation"
	git push
	git checkout -
