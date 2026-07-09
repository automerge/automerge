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

## Throughput

Profiling (perf, dwarf call graphs) showed the loop was dominated by
avoidable work rather than op application. Fixes, measured on a fixed seed
against a 480-trace reference corpus:

- [x] Mutation planning no longer materializes a full trace clone per variant:
  `position_variant_count` counts arithmetically and `apply_position_variant`
  clones the input exactly once (~11% of total time was `VmInstr`/`VmOp`
  vector clones).
- [x] `Fork` uses `AutoCommit::fork` and `Merge` merges the two live docs via
  `split_at_mut` instead of save+load round trips (~24% of total time was
  document loading; explicit `SaveLoad` steps and the end-of-run save/load
  invariant still exercise those paths).
- [x] Prefix-cache hashing uses derived `Hash` instead of serde_json
  serialization of every instruction.
- [x] `--jobs N` executes candidates on N worker threads, each with its own
  `Runner`; generation, feedback, and corpus management stay on the main
  thread, and warmup replays also go through the pool. Results are processed
  in completion order, so parallel runs are not reproducible run-to-run, but
  every saved trace remains individually replayable. Coverage/sancov builds
  force `--jobs 1` because their counters are process-global.
- [x] Status-line exec/s and event timestamps measure the fuzz loop itself,
  excluding warmup.

Single-thread: 107 -> 166 exec/s on the reference corpus, with identical
discovery counts. Parallel on a heavier corpus: 31 exec/s at `--jobs 1` to
421 exec/s at `--jobs 24` (13.6x). Short novelty-heavy runs are limited by
main-thread trace saving and batch enumeration; saturated long runs are
worker-bound and scale better. Trace timeouts (2s wall clock) fire slightly
more often under high `--jobs` due to CPU contention; they count as rejected.

## Explicit transactions and rollback

Coverage triage showed `src/transaction.rs` (the explicit `Transaction` API,
including rollback) at 0% because the runner only used `AutoCommit`.

- [x] Add `VmInstr::Transact { doc, actor, ops, commit }`: applies ops inside
  an explicit `Automerge::transaction()` on a copy of the document.
- [x] Share op application between paths: `apply_vm_op_tx` is generic over
  `ReadDoc + Transactable`, used by both `AutoCommit` changes and explicit
  transactions.
- [x] Commit path feeds the resulting changes back into the live doc through
  `AutoCommit::apply_changes`, exercising the change-application queue.
- [x] Rollback path checks the rollback-is-a-perfect-noop invariant: heads and
  hydrated state must be untouched afterwards.
- [x] Generator emits `Transact` for ~15% of changes; the mutator can toggle
  commit/rollback and position enumeration tries committed and rolled-back
  transaction variants at every step.
- [x] `sometimes!` labels in `TransactionInner::rollback`
  (`tx.rollback.{empty,ops,many_ops,first_change_of_actor}`).
- [x] Regression test: committed ops land exactly once; rollbacks leave no
  trace.

Validation: a 3,000-iteration coverage run took `transaction.rs` from 0% to
30%, `manual_transaction.rs` to 53%, and `transaction/inner.rs` to 83%. A
5,000-iteration soak at `--jobs 16` (1,900 exec/s) hit the rollback labels
thousands of times with no invariant failures.

## Sync sessions

Sync exchanges are now first-class VM state instead of one-shot reliable
loops, covering the drop/duplicate/reorder schedules planned in FUZZING.md.

- [x] Add `VmInstr::SyncSession { session, op }` over up to 8 persistent
  sessions, each holding live `sync::State`s for both sides plus in-flight
  queues of *encoded* messages (every delivery round-trips the message codec).
- [x] `VmSyncOp::{Start, Generate, Deliver, SaveStates, Finish}` with
  `VmSyncFault::{None, Drop, Duplicate, Reorder}` on delivery. Edits,
  merges, and doc save/loads interleave freely between session steps.
- [x] `SaveStates` encode/decode round-trips both sync states in place, as a
  process restart would; decode failure is an invariant violation.
- [x] `Finish` flushes in-flight messages, then syncs reliably for up to
  `rounds` rounds; if the protocol quiesces, both docs must have equal heads.
  The check is skipped when a `Fork` replaced either doc since `Start`
  (tracked by a per-doc generation counter), since resuming a sync state
  against an unrelated history is API misuse.
- [x] Generator starts/continues sessions (~1 in 11 instructions once docs
  fork); mutator can rewrite session ops and faults; features and structural
  buckets track each op and fault kind.
- [x] Regression test: a session with a dropped message, a duplicated
  delivery, a state round-trip, and a mid-session edit still converges on
  finish.

Validation: 3,000-iteration coverage run moved `sync.rs` 57% -> 78%,
`sync/state.rs` 21% -> 54%, `sync/bloom.rs` -> 85%. A 3,000-iteration smoke
at `--jobs 16` discovered all nine sync-session features with no crashes and
no false convergence failures.

## Historical forks and change transfer

- [x] Add `VmInstr::ForkAt { from, to, head }`: fork a document at saved or
  current heads via `AutoCommit::fork_at`.
- [x] Add `VmInstr::ApplyChanges { from, into, order }`: transfer the changes
  `into` is missing through `apply_changes`, delivered in an adversarial
  order (`VmApplyOrder::{InOrder, Reversed, Shuffled, Duplicated,
  DropHalf}`). Out-of-order delivery exercises the causal readiness queue;
  `DropHalf` leaves pending changes for later instructions to complete.
- [x] Completeness invariant: any complete delivery order must leave `into`
  containing all of `from`'s heads once the queue drains.
- [x] `sometimes!` labels in `ChangeQueue::pop_topo_sorted_ready`
  (`change_queue.{applied_out_of_order,still_pending,drained}`).

Validation: `change_queue.rs` coverage reached 96% (it was essentially
unexercised before, since merge always delivers in order); the queue labels
hit thousands of times per short run.

**Found a bug immediately**: `fork_at` at a document's own current heads
panics with `MissingOps` in `ChangeCollector::from_build_meta_inner` when an
actor's changes are interleaved with another actor's (sensitive to the actor
ids: `[2,0,2]`, `[0,1,0]`, `[2,1,2]` panic; `[1,0,1]`, `[0,2,0]` do not).
Minimized repro committed as an ignored test in
`rust/automerge/tests/fuzz_crashes.rs`
(`fork_at_current_heads_after_interleaved_actor_changes`), alongside a
passing guard that `fork_at` with foreign heads returns an error. Until the
bug is fixed, fuzz runs will keep saving this crash signature.

## Text encodings, rich text, and cursors

Coverage triage flagged the text subsystem (`text_diff.rs` 20%,
`text_value.rs` 27%, `cursor.rs` 9%) as almost untouched because the runner
only ever used the default encoding, canned `update_text` strings, and never
serialized cursors.

- [x] `Trace::text_encoding` (`VmTextEncoding::{CodePoint, Utf8, Utf16,
  Grapheme}`): documents are created and reloaded with the chosen encoding.
  Generated ~half the time, occasionally flipped by the mutator, and folded
  into the prefix-cache hash.
- [x] `VmOp::EditText { obj, seed }`: `update_text` with a small edit derived
  from the object's *current* text (insert/delete/replace/duplicate),
  biased toward boundary-hostile strings (astral plane, ZWJ families,
  combining marks) so the Myers diff sees near-identical before/after inputs.
- [x] `VmOp::UpdateSpans { obj, seed }`: rewrites rich content (text runs with
  mark sets, occasional block markers) via `update_spans`, driving the
  block/marks diff path.
- [x] Cursor round-trip invariant in the `Cursors` observe mode: a cursor
  round-tripped through both `to_bytes`/`try_from` and `to_string`/`try_from`
  must resolve to the same position.
- [x] Extended the hostile-string tables with decomposed accents, stacked
  combining marks, and multi-person ZWJ sequences.
- [x] Features and structural buckets track encodings, `text_edit`, and
  `update_spans`.

Validation: `text_diff.rs` 20% -> 90%, `cursor.rs` 9% -> 66%, `text_value.rs`
27% -> 44%. Regression tests cover hostile edits under all four encodings and
repeated `update_spans` rewrites. No crashes in short soak runs.

## Pathological text and text-accounting invariants

The earlier text work reached the code but fed it shallow content (whole canned
strings) and had no oracle for width/grapheme accounting. This closes both.

- [x] `HOSTILE_FRAGMENTS`: composable grapheme-breaking/fusing pieces (lone
  combining marks, ZWJ, variation selectors, skin-tone modifiers, regional
  indicators that pair into flags, tag characters, astral bases). `vm_edit_text`
  now inserts these at code-point boundaries — including *inside* an existing
  grapheme — and appends/prepends fragments that fuse with neighbouring
  clusters, plus adjacent code-point swaps. Whole-string tables gained
  skin-tone+ZWJ, multi-flag, and dangling-regional-indicator entries.
- [x] Two text-accounting invariants in the save/load pass
  (`check_text_invariants`):
  - the `Span::Text` runs from `spans()` must reconstruct `text()` (sound under
    every encoding);
  - `length()` (the internal width index) must equal the width of `text()`
    recomputed from scratch — checked only when the text has no embedded
    objects and is not grapheme-encoded (the grapheme store segments each
    spliced value independently, so re-segmenting the whole string can
    legitimately merge clusters).
  Both filter the U+FFFC object-replacement placeholder, which `spans()`
  emits for embedded objects/blocks but `text()` omits.

Both invariants were validated against all 1,463 committed known-good traces
with zero false positives before being trusted (the first two drafts *did*
false-positive on embedded objects — see below). `delete.in.multicharacter`
(delete landing mid-multi-code-unit element) is now hit routinely, and
`text_diff.rs` holds at ~92%. A clean 8,000-iteration soak found no crashes.
`text_value.rs` coverage is encoding-dependent (each trace fixes one encoding)
and accumulates across a multi-seed soak rather than in a single run.

Note: designing a sound text oracle is subtle — `text()` omits embedded
objects and block markers that `spans()` and `length()` both count, and the
grapheme encoding stores per-splice segmentation. The invariants above encode
those caveats; do not tighten them without re-validating against the corpus.

The trace fuzzer has now covered the reachable-but-unreached surface from the
original coverage triage (transactions, sync sessions, historical forks +
change transfer, text). Remaining low-coverage core files are old-format
`legacy/` and `columnar/` encoding paths, reachable only by loading crafted
bytes — the raw-byte `load` fuzz target's domain, not the trace fuzzer's.

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
