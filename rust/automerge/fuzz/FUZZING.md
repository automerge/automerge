# Automerge Trace Fuzzing Plan

This document describes a plan for building a lightweight, coverage-guided, stateful fuzzing harness for Automerge in the existing fuzz crate at `rust/automerge/fuzz`.

The goal is to complement the existing raw byte fuzz target (`fuzz_targets/load.rs`) with a higher-level trace fuzzer that generates Automerge operation histories: edits, forks, merges, saves, loads, and sync schedules. This should give us an Antithesis-like workflow: run many deterministic executions, measure whether they do something new, preserve interesting traces, and minimize failures into replayable regressions.

## Goals

- Generate structured traces of Automerge API operations.
- Exercise CRDT-specific behavior: concurrency, merge, sync, save/load, conflicts, marks, counters, lists, and text.
- Measure interestingness using a combination of:
  - compiler coverage,
  - semantic coverage,
  - structural document novelty.
- Automatically mutate and evolve a corpus of interesting traces.
- Save and minimize crashing or invariant-violating traces.
- Keep dependencies small and boring; `rand`, `serde`, and `clap` are acceptable.
- Keep the existing `cargo-fuzz` raw byte target working.

## Non-goals

- Replacing libFuzzer/raw byte fuzzing.
- Depending on proptest, arbitrary, or a large fuzzing framework.
- Building a general-purpose fuzzing framework.
- Replaying the entire generated corpus in normal CI.

## Location

Use the existing fuzz crate:

```text
rust/automerge/fuzz/
```

Suggested layout:

```text
rust/automerge/fuzz/
  Cargo.toml
  FUZZING.md
  fuzz_targets/
    load.rs                  # existing raw-byte libFuzzer target
  src/
    lib.rs
    trace.rs                 # serde-compatible trace data model and deterministic generation
    trace_io.rs              # trace load/save and metadata handling
    runner.rs                # Automerge interpreter and invariants
    mutate.rs                # trace mutation operators
    shrink.rs                # minimization
    feedback.rs              # novelty and coverage tracking
    bin/
      trace_fuzz.rs          # standalone CLI
  corpus/
    trace/
      seeds/
      interesting/
      crashes/
      minimized/
```

The crate already has its own workspace stanza:

```toml
[workspace]
members = ["."]
```

That is fine. We can treat `rust/automerge/fuzz` as a standalone fuzzing workspace and run it from that directory.

## Cargo setup

Keep the existing `load` binary:

```toml
[[bin]]
name = "load"
path = "fuzz_targets/load.rs"
test = false
doc = false
```

Add a normal trace-fuzzer binary:

```toml
[[bin]]
name = "trace_fuzz"
path = "src/bin/trace_fuzz.rs"
test = false
doc = false
```

The trace fuzzer can use a small dependency set. `rand` gives us a well-tested deterministic RNG, `serde` makes traces easier to serialize, evolve, and minimize, and `clap` keeps the CLI maintainable as modes and flags grow.

```toml
[dependencies]
clap = { version = "4", features = ["derive"] }
rand = "0.9"
serde = { version = "1", features = ["derive"] }
serde_json = "1"

automerge = { path = ".." }
```

If we decide that JSON is too noisy for hand-editing, we can keep the same `serde` data model and swap the on-disk format later.

## Command-line interface

Use `clap` derive for the CLI. The fuzzer will likely accumulate modes and flags, and using `clap` avoids spending time maintaining a custom parser.

Suggested commands:

```bash
cd rust/automerge/fuzz

cargo run --bin trace_fuzz -- generate --seed 123 --steps 500
cargo run --bin trace_fuzz -- replay corpus/trace/seeds/basic.amtrace
cargo run --bin trace_fuzz -- fuzz --iterations 100000 --seed 123
cargo run --bin trace_fuzz -- minimize corpus/trace/crashes/foo.amtrace
```

Eventually useful flags:

```text
--seed <u64>
--iterations <usize>
--steps <usize>
--corpus <path>
--max-docs <usize>
--max-actors <usize>
--minimize-on-crash
--coverage-dir <path>
```

## Trace model

The fuzzer should generate traces rather than raw bytes.

A trace is deterministic and replayable:

```rust
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct Trace {
    pub version: u32,
    pub metadata: Metadata,
    pub actors: Vec<ActorSpec>,
    pub steps: Vec<Step>,
}
```

Initial step set:

```rust
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum Step {
    ForkDoc {
        from: DocId,
        to: DocId,
    },
    Change {
        doc: DocId,
        actor: ActorId,
        ops: Vec<Op>,
    },
    Merge {
        into: DocId,
        from: DocId,
    },
    SaveLoad {
        doc: DocId,
    },
    Sync {
        left: DocId,
        right: DocId,
        rounds: u8,
    },
    Observe {
        doc: DocId,
    },
    SaveHeads {
        doc: DocId,
        slot: u8,
    },
    DiffRange {
        doc: DocId,
        before: HeadRef,
        after: HeadRef,
    },
    UpdateDiffCursor {
        doc: DocId,
    },
    ResetDiffCursor {
        doc: DocId,
    },
    DiffIncremental {
        doc: DocId,
    },
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum HeadRef {
    Empty,
    Current,
    Slot { slot: u8 },
}
```

Initial operation set:

```rust
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum Op {
    Put { obj: ObjRef, key: String, value: Scalar },
    MakeMap { obj: ObjRef, key: String },
    MakeList { obj: ObjRef, key: String },
    MakeText { obj: ObjRef, key: String },
    Insert { obj: ObjRef, index: usize, value: Scalar },
    SpliceList { obj: ObjRef, index: usize, delete: usize, values: Vec<Scalar> },
    SpliceText { obj: ObjRef, index: usize, delete: usize, value: String },
    UpdateText { obj: ObjRef, value: String },
    Mark {
        obj: ObjRef,
        start: usize,
        end: usize,
        name: String,
        value: Scalar,
        expand: MarkExpand,
    },
    Unmark {
        obj: ObjRef,
        start: usize,
        end: usize,
        name: String,
        expand: MarkExpand,
    },
    Delete { obj: ObjRef, key: String },
}

#[derive(Clone, Copy, Debug, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MarkExpand {
    Before,
    After,
    Both,
    None,
}
```

References such as `ObjRef` should be symbolic rather than raw Automerge object IDs where possible. For example:

```rust
pub enum ObjRef {
    Root,
    Known(usize),
    Path(Vec<PathElem>),
    Invalid(u8),
}
```

The runner resolves symbolic references against the current document state. Most generated references should be valid, but a small percentage should deliberately be invalid or stale to exercise error paths.

## Trace file format

Use a serde-backed, human-readable format. JSON is the simplest first choice because `serde_json` is already common in the Rust ecosystem and gives us stable, easy-to-debug traces without writing a parser by hand.

Example:

```json
{
  "version": 1,
  "metadata": {
    "seed": 12345,
    "parent": "corpus/trace/interesting/000017.amtrace",
    "reason": "new feature ConcurrentTextInsert"
  },
  "actors": [{ "bytes": [0] }, { "bytes": [1] }],
  "steps": [
    {
      "type": "change",
      "doc": 0,
      "actor": 0,
      "ops": [
        { "type": "put", "obj": "root", "key": "a", "value": { "int": 1 } },
        { "type": "make_list", "obj": "root", "key": "list" }
      ]
    },
    { "type": "fork_doc", "from": 0, "to": 1 },
    {
      "type": "change",
      "doc": 1,
      "actor": 1,
      "ops": [
        { "type": "insert", "obj": { "path": ["list"] }, "index": 0, "value": { "str": "x" } }
      ]
    },
    { "type": "merge", "into": 0, "from": 1 },
    { "type": "save_load", "doc": 0 }
  ]
}
```

Requirements:

- stable and human-readable,
- easy enough to diff,
- easy to minimize mechanically,
- tolerant of new optional metadata fields,
- strict enough that parse errors point to a useful location.

Trace structs should derive serde traits:

```rust
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct Trace {
    pub version: u32,
    pub metadata: Metadata,
    pub actors: Vec<ActorSpec>,
    pub steps: Vec<Step>,
}
```

Use tagged enums for readability:

```rust
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum Step {
    ForkDoc { from: DocId, to: DocId },
    Change { doc: DocId, actor: ActorId, ops: Vec<Op> },
    Merge { into: DocId, from: DocId },
    SaveLoad { doc: DocId },
    Sync { left: DocId, right: DocId, rounds: u8 },
    Observe { doc: DocId },
    SaveHeads { doc: DocId, slot: u8 },
    DiffRange { doc: DocId, before: HeadRef, after: HeadRef },
    UpdateDiffCursor { doc: DocId },
    ResetDiffCursor { doc: DocId },
    DiffIncremental { doc: DocId },
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum HeadRef {
    Empty,
    Current,
    Slot { slot: u8 },
}
```

`Observe` is intentionally read-only. Diff behavior is modelled by explicit events: incremental AutoCommit diff uses `UpdateDiffCursor`/`ResetDiffCursor`/`DiffIncremental`, while arbitrary-head diff uses `SaveHeads` and `DiffRange`.

The file extension can remain `.amtrace`; the contents are JSON. If hand-editability becomes more important than compatibility, we can later add a second pretty line-oriented format while keeping the serde model as the source of truth.

## Deterministic RNG

Use `rand` with an explicitly seeded deterministic RNG. This is simpler than maintaining our own PRNG and keeps generation reproducible.

```rust
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};

let mut rng = StdRng::seed_from_u64(seed);
let key_index = rng.random_range(0..8);
let add_save_load = rng.random_range(0..100) < 15;
```

Always record the seed in trace metadata so generated traces can be reproduced even if they are not saved immediately.

## Runner

The runner interprets a trace against a set of live Automerge documents.

Responsibilities:

- create initial documents,
- set actor IDs,
- execute changes,
- resolve symbolic object/key/index references,
- perform merges,
- perform save/load round trips,
- simulate sync schedules,
- collect semantic/structural feedback,
- check invariants,
- catch panics and report failures.

Trace execution should be wrapped in `catch_unwind` so panics can be saved as replayable traces.

```rust
let result = std::panic::catch_unwind(|| runner.run(&trace));
```

## Invariants

The first version should implement a small set of high-value invariants.

### Save/load round trip

For each live document:

```text
load(save(doc)) should succeed
materialized value should match original
heads should match original where applicable
saving the loaded doc should not panic
```

### Merge convergence

For compatible documents descended from the same trace root:

```text
clone A and B
merge A into B
merge B into A
materialized values should converge
```

### Sync convergence

For sync traces:

```text
run sync according to generated schedule
then run reliable sync to completion
both documents should converge
```

Schedules should eventually include:

- normal delivery,
- reordered delivery,
- duplicated delivery,
- dropped-then-retried delivery,
- save/load between sync rounds.

### No panics

Any panic during trace execution is a crash. Save and minimize the trace.

## Feedback and interestingness

A trace is worth keeping if it discovers something new.

Use three feedback channels.

### 1. Compiler coverage

Use Rust/LLVM source coverage for the authoritative coverage signal. This should serve two purposes:

1. print live progress during long fuzz runs, so we can see whether coverage is still increasing quickly;
2. write an end-of-run report, so we can inspect what source regions are and are not covered.

Example run:

```bash
cd rust/automerge/fuzz

./run-trace-fuzz-coverage.sh \
  --seed 1 \
  --iterations 100000 \
  --coverage-poll-secs 30
```

The script sets `RUSTFLAGS=-Cinstrument-coverage`, sets `LLVM_PROFILE_FILE` if it is not already set, and adds `--coverage-dir target/trace-coverage` unless another `--coverage-dir` is supplied.

The fuzzer should shell out to the LLVM tools when `--coverage-dir` is provided:

```bash
llvm-profdata merge -sparse target/trace-fuzz-*.profraw -o target/trace-coverage/current.profdata

llvm-cov export \
  target/debug/trace_fuzz \
  --instr-profile=target/trace-coverage/current.profdata \
  --summary-only
```

`llvm-cov export --summary-only` produces JSON. Since the fuzz crate can depend on `serde_json`, we can parse this and report totals for lines, regions, functions, and branches where available.

At the end of the run, also generate durable artifacts:

```text
target/trace-coverage/final.profdata
target/trace-coverage/summary.json          # llvm-cov full summary
target/trace-coverage/report.txt           # llvm-cov full report
target/trace-coverage/export.json          # full export used for post-processing
target/trace-coverage/summary-core.json     # rust/automerge/src + rust/hexane/src
target/trace-coverage/report-core.txt      # rust/automerge/src + rust/hexane/src
```

Filter out toolchain and registry paths where possible, for example with `--ignore-filename-regex='/.cargo/registry|/rustc/'`. The main metric we care about is **core coverage**, meaning `rust/automerge/src` plus `rust/hexane/src`, because Hexane is a separate crate but is exercised primarily through Automerge.

#### Live coverage progress

The hot fuzzing loop should maintain cheap in-process stats continuously:

```text
iterations
exec/s
corpus size
crashes
semantic features discovered
structural novelty buckets discovered
iterations since last interesting trace
```

Coverage polling should be less frequent because `llvm-profdata` and `llvm-cov` are not free. A good default is:

```text
poll coverage every 30 seconds, or every 10,000 executions, whichever comes later
```

A live status line should look roughly like:

```text
iters=250000 exec/s=4812 corpus=1392 crashes=0 \
features=47 buckets=312 \
lines=18421/31590 58.31% (+143/60s) \
regions=49210/91003 54.07% (+511/60s) \
last_new_cov=18s
```

Suggested internal shape:

```rust
pub struct CoverageSummary {
    pub lines_covered: u64,
    pub lines_total: u64,
    pub regions_covered: u64,
    pub regions_total: u64,
    pub functions_covered: u64,
    pub functions_total: u64,
    pub branches_covered: Option<u64>,
    pub branches_total: Option<u64>,
}

pub struct CoveragePoller {
    pub coverage_dir: PathBuf,
    pub binary: PathBuf,
    pub profraw_glob: String,
    pub last_summary: Option<CoverageSummary>,
    pub last_poll: Instant,
}
```

On each poll:

1. flush the current profile if possible;
2. merge `.profraw` files with `llvm-profdata`;
3. run `llvm-cov export --summary-only`;
4. parse the JSON summary;
5. compute deltas from the previous summary;
6. update the live status output.

#### Mid-run profile flushing

LLVM profile data is normally written at process exit. For live coverage polling, the fuzzer should try to flush counters explicitly before invoking `llvm-profdata`.

When compiled with coverage instrumentation, the LLVM profiling runtime exposes symbols like:

```rust
unsafe extern "C" {
    fn __llvm_profile_write_file() -> i32;
}
```

The fuzzer can wrap this in a best-effort helper:

```rust
pub fn flush_coverage_profile() {
    unsafe {
        let _ = __llvm_profile_write_file();
    }
}
```

This should be isolated in a small coverage module and allowed to fail gracefully. If the symbol is unavailable or the binary was not compiled with coverage instrumentation, the fuzzer should still run; it just will not have live LLVM coverage updates.

#### Coverage as corpus feedback

Coverage reporting starts observational, but should also be used for corpus retention. The current coarse approach is window-based rather than per-trace attribution:

```text
keep a recent window of valid traces when live line/region/function coverage increases
```

The relevant flags are:

```text
--coverage-retention
--coverage-retention-window 128
```

Retained traces are written to `corpus/trace/coverage` and loaded on later runs. This does not identify the exact trace that caused the increase, but it cheaply preserves the local neighborhood of traces that led to new coverage. Later we can replay the retained window trace-by-trace to attribute coverage more precisely.

Semantic and structural novelty should remain in place even after compiler coverage is available, because they capture Automerge-specific state-space exploration that source coverage alone may miss.

### 2. Semantic coverage

The runner should record Automerge-specific events/features, for example:

```rust
pub enum Feature {
    ConcurrentPutSameKey,
    ConcurrentListInsertSameIndex,
    DeleteThenMerge,
    SaveLoadAfterConflict,
    SyncWithDroppedMessage,
    SyncWithReorderedMessage,
    TextMarkOverlapping,
    TextMarkAcrossDelete,
    CounterIncrementConcurrent,
    InvalidObjectReference,
    EmptyChange,
    LargeActorId,
    DeepObjectTree,
}
```

Keep traces that discover a new feature or a new feature pair.

### 3. Structural novelty

Track coarse buckets of final document shape and execution behavior:

```text
number of documents
number of actors
number of heads
number of changes
number of objects
max object depth
number of conflicts
list length bucket
text length bucket
number of marks
number of sync messages
saved document size bucket
```

Use logarithmic buckets:

```rust
fn bucket(n: usize) -> u8 {
    if n == 0 {
        0
    } else {
        usize::BITS as u8 - n.leading_zeros() as u8
    }
}
```

Keep traces that hit a previously unseen bucket tuple.

## Mutation strategy

Main loop:

```text
load seed corpus
repeat N times:
  choose a corpus trace
  mutate it
  run it
  if it crashes: save and minimize
  else if it is interesting: save to corpus/trace/interesting
```

Mutation operators:

```rust
pub enum Mutation {
    DeleteStep,
    DuplicateStep,
    SwapAdjacentSteps,
    SplicePrefixFromAnotherTrace,
    ChangeActor,
    ChangeDoc,
    ChangeKey,
    ChangeObjRef,
    ChangeIndex,
    ChangeValue,
    InsertRandomStep,
    InsertRandomOp,
    DeleteOp,
    MakeStepConcurrent,
    AddSaveLoad,
    AddMerge,
    AddSyncRound,
}
```

Automerge-specific high-value mutations:

- fork before a change,
- convert sequential changes into concurrent changes,
- merge after conflicting changes,
- add save/load around conflicts,
- add sync with reordered messages,
- add sync with dropped and retried messages,
- duplicate sync messages,
- use stale object references,
- delete objects that later receive operations,
- insert list/text items at the same index concurrently,
- add marks that overlap deletes,
- increment counters concurrently,
- create deep object paths,
- use many actors with small changes,
- use one actor with many changes.

## Minimization

When a trace crashes or violates an invariant, minimize it automatically if requested.

Minimization strategy:

1. Remove contiguous chunks of steps.
2. Remove individual steps.
3. Remove operations inside `Change` steps.
4. Simplify values.
5. Simplify keys.
6. Replace actor/doc IDs with smaller IDs.
7. Reduce indices and ranges.
8. Reduce sync rounds and simplify schedules.

Pseudo-code:

```rust
pub fn minimize(mut trace: Trace, predicate: impl Fn(&Trace) -> bool) -> Trace {
    let mut changed = true;

    while changed {
        changed = false;

        for granularity in [64, 32, 16, 8, 4, 2, 1] {
            let mut i = 0;
            while i < trace.steps.len() {
                let candidate = trace.with_steps_removed(i, granularity);
                if predicate(&candidate) {
                    trace = candidate;
                    changed = true;
                } else {
                    i += granularity;
                }
            }
        }

        // Then try local simplifications.
    }

    trace
}
```

The predicate should check whether the minimized trace still reproduces the same class of failure:

```text
panic with same message, or
same invariant failure kind
```

## Corpus policy

Suggested corpus layout:

```text
corpus/trace/
  seeds/
    basic-map.amtrace
    basic-list.amtrace
    basic-text.amtrace
    concurrent-map-conflict.amtrace
    sync-drop-retry.amtrace
  interesting/
    cov-00000001.amtrace
    feat-00000002.amtrace
  crashes/
    panic-00000001.amtrace
    invariant-merge-divergence-00000002.amtrace
  minimized/
    panic-00000001.min.amtrace
```

Commit only:

- handwritten seeds,
- minimized crash regressions,
- a very small curated set of especially useful interesting traces.

Do not commit a large generated corpus by default.

## Regression testing

Add a normal test target or binary mode that replays committed traces:

```bash
cargo test -- replay_committed_trace_corpus
```

This should replay:

- `corpus/trace/seeds`,
- `corpus/trace/minimized`,
- optionally a curated small subset of `corpus/trace/interesting`.

Normal CI should not replay a large generated corpus.

## Implementation phases

### Phase 1: deterministic runner

Implement:

- `Trace`, `Step`, and `Op`,
- serde-based trace load/save,
- deterministic generation using seeded `rand::rngs::StdRng`,
- basic generation,
- runner for map/list/text operations,
- save/load invariant,
- merge convergence invariant,
- `generate` and `replay` commands.

This phase should already make handwritten traces easier to write, share, and replay.

### Phase 2: mutation corpus loop

Implement:

- loading seed corpus,
- mutation engine,
- fuzz loop,
- crash saving,
- semantic feature feedback,
- structural novelty feedback.

### Phase 3: minimizer

Implement:

- `minimize` command,
- automatic minimization on crash/invariant failure,
- minimized trace output.

### Phase 4: sync-heavy traces

Implement:

- sync step runner,
- network/message scheduler,
- drop/reorder/duplicate schedules,
- reliable-sync-to-completion convergence check.

### Phase 5: compiler coverage integration

Implement optional coverage workflow:

- documented `RUSTFLAGS=-Cinstrument-coverage` mode first,
- `--coverage-dir` flag,
- end-of-run `summary.json`, `report.txt`, and optional HTML report,
- periodic live polling with line/region/function deltas,
- best-effort mid-run profile flushing via LLVM profiling runtime,
- source coverage as an additional corpus-retention signal.

## Design principle

The most important choice is to fuzz Automerge at the level where the interesting state exists: operation histories, actors, forks, merges, sync schedules, and persistence boundaries.

Raw bytes are still useful for parser hardening. Trace fuzzing should target CRDT behavior.
