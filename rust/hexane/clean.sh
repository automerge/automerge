#!/usr/bin/env bash

cargo clippy --all-targets --all-features -- -D warnings
cargo doc --no-deps --workspace --document-private-items
cargo fmt -- --check
