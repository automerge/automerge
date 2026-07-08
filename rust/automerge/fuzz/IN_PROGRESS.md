# Automerge Trace Fuzzing: In Progress

This file tracks the current implementation phase for the trace fuzzing harness described in `FUZZING.md`.

## Current phase

### Phase 2: mutation corpus loop

Goal: add a basic corpus-driven fuzz loop that mutates existing traces, replays candidates, tracks simple semantic/structural novelty, and saves interesting traces or failures. Phase 1 is complete.

## Current milestone

Build the smallest useful trace fuzzer binary in the existing fuzz crate:

```text
rust/automerge/fuzz/
```

The current binary should be runnable as:

```bash
cd rust/automerge/fuzz
cargo run --bin trace_fuzz -- generate --seed 1 --steps 20
cargo run --bin trace_fuzz -- replay corpus/trace/seeds/basic-map.amtrace
cargo run --bin trace_fuzz -- fuzz --seed 1 --iterations 1000
```

## Phase 1 checklist

### Cargo setup

- [x] Add dependencies:
  - [x] `clap` with `derive`
  - [x] `rand`
  - [x] `serde` with `derive`
  - [x] `serde_json`
- [x] Add `trace_fuzz` binary entry to `Cargo.toml`.
- [x] Keep existing `load` libFuzzer target working.

### Module skeleton

- [x] Add `src/lib.rs`.
- [x] Add `src/trace.rs`.
- [x] Use `rand::rngs::StdRng` directly from generation code.
- [x] Add `src/trace_io.rs`.
- [x] Add `src/runner.rs`.
- [x] Add `src/bin/trace_fuzz.rs`.

### Trace model

- [x] Define `Trace`.
- [x] Define `Metadata`.
- [x] Define actor/document ID wrappers or aliases.
- [x] Define initial `Step` enum.
- [x] Define initial `Op` enum.
- [x] Define simple `Value` enum.
- [x] Define symbolic object references.
- [x] Derive `Serialize` and `Deserialize` for trace types.

### Trace I/O

- [x] Implement JSON trace loading.
- [x] Implement pretty JSON trace saving.
- [x] Use `.amtrace` extension for trace files.
- [x] Include seed and generation metadata in generated traces.

### CLI

- [x] Implement `generate` subcommand.
- [x] Implement `replay` subcommand.
- [x] Add basic error reporting.
- [x] Print useful replay success/failure output.

### Generator

- [x] Use deterministic `rand::rngs::StdRng` seeded from CLI.
- [x] Generate simple root-map operations.
- [x] Generate simple object creation operations.
- [x] Generate simple list/text inserts where possible.
- [x] Ensure generated traces are reproducible from seed.

### Runner

- [x] Execute simple `Change` steps.
- [x] Support root-map puts.
- [x] Support map/list/text creation.
- [x] Support list/text insertion.
- [x] Support `SaveLoad` steps.
- [x] Check save/load roundtrip invariant.
- [x] Catch panics and report them as failures.

### Seeds

- [x] Add `corpus/trace/seeds/basic-map.amtrace`.
- [x] Add `corpus/trace/seeds/basic-list.amtrace`.
- [x] Add `corpus/trace/seeds/basic-text.amtrace`.

### Validation

- [x] `cargo run --bin trace_fuzz -- generate --seed 1 --steps 20` works.
- [x] `cargo run --bin trace_fuzz -- replay corpus/trace/seeds/basic-map.amtrace` works.
- [x] Existing `cargo fuzz` target is not broken.
- [x] `cargo test` in `rust/automerge/fuzz` passes if tests exist.

Validation completed in the nix dev shell:

```bash
cd rust/automerge/fuzz
cargo check --bin trace_fuzz
cargo check --bin load
cargo run --bin trace_fuzz -- generate --seed 1 --steps 20
cargo run --bin trace_fuzz -- replay corpus/trace/seeds/basic-map.amtrace
cargo run --bin trace_fuzz -- replay corpus/trace/seeds/basic-list.amtrace
cargo run --bin trace_fuzz -- replay corpus/trace/seeds/basic-text.amtrace
cargo test
```

## Phase 2 checklist

### Mutation

- [x] Add `src/mutate.rs`.
- [x] Implement simple step deletion.
- [x] Implement simple step duplication.
- [x] Implement random change insertion.
- [x] Implement save/load insertion.
- [x] Implement basic key/value/index mutation.
- [x] Implement appending random ops to existing changes.

### Feedback

- [x] Add `src/feedback.rs`.
- [x] Track simple semantic features.
- [x] Track simple structural buckets.
- [x] Keep candidates that discover new features or buckets.

### Fuzz loop

- [x] Add `fuzz` subcommand.
- [x] Load traces from `corpus/trace/seeds`.
- [x] Load generated traces from `corpus/trace/interesting` if present.
- [x] Mutate corpus traces deterministically from a seed.
- [x] Save interesting traces to `corpus/trace/interesting`.
- [x] Save panic/invariant failures to `corpus/trace/crashes`.
- [x] Print periodic progress reports.

### Phase 2 validation

- [x] `cargo check --bin trace_fuzz` passes.
- [x] `cargo run --bin trace_fuzz -- fuzz --seed 1 --iterations 100 --report-every 50` works.

## Coverage improvement: state-aware generation

Instead of generating only fixed root-key traces, the generator now maintains a lightweight symbolic model of known objects.

- [x] Track known map/list/text objects during generation.
- [x] Generate nested map/list/text objects under known maps.
- [x] Generate puts/deletes against known maps.
- [x] Generate inserts against known lists.
- [x] Generate splices against known text objects.
- [x] Add generated traces to the fuzz-loop startup corpus with `--generated-traces`.
- [x] Track nested object paths as a semantic feature.
- [x] Add richer structural buckets for operation mix, object depth, save/load count, fork/merge count, and actor count.

## Coverage reporting

Observational LLVM coverage reporting is now implemented behind `--coverage-dir`.

- [x] Add `src/coverage.rs`.
- [x] Add `--coverage-dir` flag.
- [x] Add `--coverage-poll-secs` flag.
- [x] Poll coverage periodically when LLVM tools and `.profraw` files are available.
- [x] Generate end-of-run `summary.json` and `report.txt` when LLVM tools are available.
- [x] Generate `summary-core.json` and `report-core.txt` for Automerge core coverage (`rust/automerge/src` + `rust/hexane/src`).
- [x] Flush LLVM profile data best-effort via dynamic lookup of `__llvm_profile_write_file`.
- [x] Gracefully continue if `llvm-profdata` / `llvm-cov` are unavailable.
- [x] Add `--max-corpus-load` to avoid loading unbounded generated corpora at startup.
- [x] Add `--coverage-retention` and `--coverage-retention-window`.
- [x] Save recent valid traces to `corpus/trace/coverage` when live coverage increases.
- [x] Load retained coverage traces from `corpus/trace/coverage` at startup.

Example coverage run:

```bash
cd rust/automerge/fuzz
./run-trace-fuzz-coverage.sh \
  --seed 1 \
  --iterations 100000 \
  --coverage-poll-secs 30
```

The script sets `RUSTFLAGS=-Cinstrument-coverage`, sets `LLVM_PROFILE_FILE` if it is not already set, and adds `--coverage-dir target/trace-coverage` unless another `--coverage-dir` is supplied.

Note: this requires `llvm-profdata` and `llvm-cov` to be available on `PATH`.

## State-aware mutation

The mutator now includes a heavily weighted state-aware insertion path.

- [x] Build a lightweight symbolic model from a trace prefix.
- [x] Track known map/list/text objects from existing trace operations.
- [x] Insert valid-ish changes at arbitrary trace positions using the prefix model.
- [x] Append valid-ish operations to existing changes using the prefix model.
- [x] Prefer state-aware mutation over destructive syntactic mutation.

Short validation run after this change:

```text
iters=200 corpus=71 valid=193 interesting=63 rejected=7
```

## Sync traces

The trace runner and mutator now include a first reliable-sync scenario.

- [x] Add `Step::Sync { left, right, rounds }`.
- [x] Execute reliable bidirectional sync using `automerge::sync::{State, SyncDoc}`.
- [x] Check hydrated document convergence after sync.
- [x] Add sync as a semantic feature and structural bucket.
- [x] Add a state-aware sync scenario mutation: fork doc 0 to doc 1, edit both, then sync.

Short validation run after this change:

```text
iters=200 corpus=97 valid=188 interesting=89 rejected=12 features=14 buckets=83
```

## Observation/read-side and diff traces

The trace runner and mutator now include read-side observation and explicit diff steps.

- [x] Add read-only `Step::Observe { doc }`.
- [x] Exercise read APIs over root and known objects: `keys`, `values`, `map_range`, `list_range`, `get`, `get_all`, `hydrate`, `parents`, and `length`.
- [x] Exercise text/list cursor APIs and text span/mark APIs where applicable.
- [x] Split diff behavior out of `Observe`.
- [x] Add `Step::SaveHeads { doc, slot }` and `HeadRef::{Empty, Current, Slot}`.
- [x] Add arbitrary-head `Step::DiffRange { doc, before, after }`.
- [x] Add AutoCommit incremental diff events: `UpdateDiffCursor`, `ResetDiffCursor`, and `DiffIncremental`.
- [x] Add observe/diff events as semantic features and structural buckets.
- [x] Add mutator scenarios for read-only observation, incremental diff, and arbitrary-head diff.

Short validation run after this change:

```text
iters=150 corpus=90 valid=142 interesting=82 rejected=8 features=20 buckets=70
```

## Rich text, marks, and list splice traces

The trace runner and mutator now include higher-leverage mutating operations which create better inputs for observation and diff APIs.

- [x] Add `Op::SpliceList { obj, index, delete, values }` using `Transactable::splice`.
- [x] Add `Op::UpdateText { obj, value }` using `Transactable::update_text` to exercise text diff code.
- [x] Add `Op::Mark { obj, start, end, name, value, expand }`.
- [x] Add `Op::Unmark { obj, start, end, name, expand }`.
- [x] Add `MarkExpand::{Before, After, Both, None}`.
- [x] Add state-aware generation/mutation for list splices, text updates, marks, and unmarks.
- [x] Add semantic features and structural buckets for list splice, text update, mark, and unmark operations.

Short fuzz run after this change:

```text
iters=100 corpus=81 valid=95 interesting=73 rejected=4 features=25 buckets=55 crashes=1
```

This immediately found repeatable Automerge panics in mark/text interactions, with crashes saved under the temporary test corpus used for the run.

## Behavioral feedback and stats logging

Novelty is now also measured from where execution actually took the documents,
not just from what the trace text says.

- [x] Collect `BehaviorStats` in the runner from the end-of-run state that the
  save/load invariant already hydrates: doc count, max heads, total changes,
  object count, max depth, text/sequence lengths, conflicted properties
  (from hydrate `conflict` flags), list/text marks, and max saved size.
- [x] Bucket the stats into `BehaviorKey`s in `feedback.rs`; a new key is a
  novelty reason (`new behavior bucket ...`) ranked above trace-syntax
  features (priority 3), which were demoted to priority 2.
- [x] Report `behavior=` in the status line.
- [x] Append machine-readable run statistics to `<corpus>/stats.jsonl`
  (override with `--stats-file`). Events: `start`, `warmup` (baseline after
  corpus replay), `report` (same cadence as the status line), `novelty`,
  `checkpoint`, `crash`, `coverage`, and `done` (includes the unhit
  `sometimes` label list). Use this for time-to-novelty curves and for
  fixed-seed A/B comparison of scheduler/mutator changes.

Short validation runs (400 iterations, fresh corpora): behavior buckets kept
growing (68/103/91 across seeds) after syntax features plateaued at 31, with
`sometimes` label counts unchanged versus the prior scheduler and no
throughput regression.

## Deferred until later phases

Do not implement these until reliable sync/read-side/rich-text traces have had some runtime testing:

- sync schedules with drop/duplicate/reorder/save-load-between-rounds,
- finer-grained coverage attribution,
- richer state-aware mutation operators,
- minimization,
- large generated corpus management.

## Notes

- Keep each phase intentionally small.
- Prefer working end-to-end behavior over broad operation coverage.
- Once replay is reliable, later phases can iterate safely on generation, mutation, and feedback.
