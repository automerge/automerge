# Plan: Make `hexane` `no_std`-compatible

> Deferred until v0 is removed from the codebase. The `src/v1/` modules and the
> `leb128` crate dependency are likely to shift during that removal; revisit this
> plan against the post-v0-removal state of the crate before executing.

## Context

The `hexane` crate is the columnar compression library underpinning automerge's
v1 storage format. Today it depends on `std` only by habit — no I/O, no
threading, no time, no filesystem. Going `no_std + alloc` would unlock
embedded/bare-metal consumers and tighten the dependency surface, but more
practically it's the *easy* dependency to convert if automerge itself ever heads
in that direction.

**Rough effort:** ~2–3 focused days. Mostly mechanical `std::` → `core::`/`alloc::`
swaps; one real piece of work (replacing the `leb128` dep).

## Findings from exploration

- **No std-only API surface.** Public API uses `&[u8]` and `&mut Vec<u8>`
  everywhere — `ColumnData::save()/save_to()/load()`, `v1::Column::save()/load()`,
  encoder `save_to(...)` family, `RawReader::read_next` (slice-based, despite
  the name). No `std::io::Read`/`Write` trait bounds anywhere.
  **No breaking API changes are needed.**
- **No `std::collections`, `std::io`, `std::fs`, `std::thread`, `std::time`,
  `std::error`, `std::sync::Mutex/RwLock`.** `std::sync::Arc` is the only
  `std::sync::*` use (one site: `Slab::data` at `src/slab.rs:21`).
- **`std::` imports** across `src/` only hit `ops`, `fmt`, `cmp`, `marker`,
  `iter`, `num`, `mem`, `str`, `slice`, `borrow`, `sync::Arc` — every one has a
  `core::` or `alloc::` equivalent.
- **Two real hurdles:**
  1. `leb128 = "0.2.5"` dep is `std::io::Read`/`Write`-bound and has no `no_std`
     mode upstream.
  2. `Slab::data: Arc<Vec<u8>>` requires `alloc::sync::Arc`, which needs
     `target_has_atomic = "ptr"` (true on most embedded targets; flag for niche
     bare-metal targets only).

## Implementation steps

### 1. `Cargo.toml`

- Add `[features]`:
  - `default = ["std"]`
  - `std = ["thiserror/std"]`
- Change `thiserror = "^2.0.12"` →
  `thiserror = { version = "^2.0.12", default-features = false }`. (`thiserror`
  2.0 emits `core::fmt::Display` without std; gates `Error::source` behind its
  `std` feature.)
- Remove `leb128 = "^0.2.5"` (after step 4).

### 2. Crate root (`src/lib.rs`)

- Add at top: `#![cfg_attr(not(feature = "std"), no_std)]` and
  `extern crate alloc;`.
- `pub(crate) use std::borrow::Cow;` (line 97) →
  `pub(crate) use alloc::borrow::Cow;`.
- Cfg-gate the non-wasm `__log!` arm (lines 57–64) so `println!` isn't
  referenced under `not(feature = "std")` — make it a no-op.
- Add a small alloc-prelude shim:
  `pub(crate) use alloc::{vec::Vec, string::{String, ToString}, format, vec, borrow::ToOwned};`
  (alloc prelude isn't auto-imported under `no_std`).

### 3. Mechanical `std::` → `core::`/`alloc::` swap

Across the files below — bulk of the work but each edit is trivial. Use `core::`
for `ops/fmt/cmp/marker/iter/num/mem/str/slice/borrow::Borrow`; use `alloc::`
for `borrow::Cow` and `sync::Arc`.

Files needing edits (top-of-file imports + inline `std::Foo::Bar` references):

- `src/aggregate.rs`, `src/boolean.rs`, `src/columndata.rs`, `src/cursor.rs`,
  `src/delta.rs`, `src/encoder.rs`, `src/pack.rs`, `src/raw.rs`, `src/rle.rs`,
  `src/slab.rs`, `src/slab/tree.rs`, `src/slab/writer.rs`
- All ~14 files under `src/v1/` (`btree.rs`, `column.rs`, `encoding.rs`,
  `prefix.rs`, `encoder.rs`, `delta/decoder.rs`, `delta/mod.rs`,
  `delta/indexed.rs`, `raw.rs`, `bool.rs`, `leb.rs`, `mod.rs`, …).

### 4. Replace the `leb128` dependency (the only non-trivial step)

Hexane already has a stack-buffered LEB128 *encoder* in `src/v1/leb.rs:69–114`
(`encode_signed`/`encode_unsigned`) and size helpers in `src/leb128.rs:1–20`.
Only the *decoder* and the `Vec<u8>: io::Write` bridge need replacing.

- **Add decoders** to `src/leb128.rs`:
  - `pub(crate) fn read_unsigned(buf: &mut &[u8]) -> Result<u64, LebError>` (~25 lines)
  - `pub(crate) fn read_signed(buf: &mut &[u8]) -> Result<i64, LebError>` (~30 lines)
  - `pub enum LebError { Overflow, Eof }`
- **`PackError`** at `src/pack.rs:8–30`: replace
  `InvalidNumber(#[from] leb128::read::Error)` with
  `InvalidNumber(#[from] LebError)`.
- **`Writer<'a, P> for Vec<u8>`** at `src/encoder.rs:23–46`: replace
  `leb128::write::{unsigned,signed}(self, …).unwrap()` with
  `self.extend_from_slice(&v1::leb::encode_{unsigned,signed}(…))`. The
  `.unwrap()`s become unreachable (stack encoders are infallible).
- **Update call sites** for `leb128::read::*`: ~19 sites across
  `src/v1/leb.rs:117–143`, `src/v1/mod.rs:374–553`, and
  `src/pack.rs:99,119,149`. Switch to
  `crate::leb128::read_{unsigned,signed}`.
- **Tests in `src/leb128.rs:30–79`** that use `leb128::write::*` for round-trip
  checks: switch to local stack encoder.
- Delete the dep line.

### 5. Verify

- `cargo build --no-default-features` from `rust/hexane/` — must compile.
- `cargo build --no-default-features --target thumbv7em-none-eabihf` (after
  `rustup target add`) — proves real no_std.
- `cargo test -p hexane` (default features) — full suite must still pass.
- `cargo build -p automerge` from `rust/` — automerge consumes hexane with
  default features, so this should be unaffected.
- `cargo clippy --all-targets --all-features -- -D warnings` and
  `cargo fmt -- --check` per the workspace conventions.

## Critical files

- `Cargo.toml` — feature flag, drop `leb128`, gate `thiserror`
- `src/lib.rs` — `#![no_std]`, `extern crate alloc`, log macro, alloc prelude shim
- `src/leb128.rs` — new decoder functions + `LebError`
- `src/v1/leb.rs` — rewire wrapper helpers to local decoder
- `src/encoder.rs` — `Writer for Vec<u8>` impl swap
- `src/pack.rs` — `PackError::InvalidNumber` variant
- All other `src/**/*.rs` files: mechanical `std::` → `core::`/`alloc::` import
  swaps

## Out of scope / follow-ups

- Bare-metal targets without atomics (would need `Rc` fallback for `Slab::data`)
  — defer until someone asks.
- A no_std smoke build in CI — add after the port lands and stabilizes.
- Doing the same to `automerge` itself — much harder (HashMaps, Cow-ier APIs,
  `serde` features); not part of this work.
