# Changes on the `unchecked_load` branch

Public API changes in `automerge` relative to `main`.

## Added

- **`LoadOptions::skip_hash_graph(bool)`** — load a document without
  building the change-hash graph. The op columns are still fully validated;
  change hashes, and everything derived from them, are simply not computed.
  Loads are 20–70× faster on large documents. Requires the document's
  head-index suffix (written by current `save()`); loads without it fail
  with `MissingHeadIndexes`.
- **`Automerge::rebuild_hash_graph() -> Result<(), AutomergeError>`** (and
  the `AutoCommit` mirror) — build the hash graph on a document that was
  loaded with `skip_hash_graph`, restoring full functionality. Verifies the
  recomputed head hashes against the document's declared heads.
- **`Automerge::hash_graph_is_checked() -> bool`** (and `AutoCommit`
  mirror) — whether hash-dependent APIs are currently available.
- **`AutomergeError::UncheckedHashGraph`** — returned by hash-dependent
  APIs on a document whose graph was skipped; the fix is to call
  `rebuild_hash_graph()`.

## Changed — previously-infallible methods now return `Result`

These can fail with `UncheckedHashGraph` on a skip-hash-graph document
(on documents loaded normally they never fail; callers can `?` or unwrap
by policy). On both `Automerge` and `AutoCommit` unless noted:

- `with_actor`, `set_actor` (fail if the actor collides with an existing
  actor's identity in ways only the hash graph can distinguish)
- `save_after`
- `get_changes`, `get_changes_meta`
- `get_change_by_hash`, `get_change_meta_by_hash`
- `get_changes_added`
- `get_last_local_change`
- `get_missing_deps`
- `hash_for_opid`
- `AutoCommit::isolate`
- `sync::SyncDoc::generate_sync_message` now returns
  `Result<Option<Message>, AutomergeError>` (`receive_sync_message` was
  already fallible)
- `ReadDoc::get_missing_deps` / `ReadDoc::get_change_by_hash` (trait
  methods) are fallible

The wasm and C bindings do not expose `skip_hash_graph`, so their
documents always have a checked graph; the bindings unwrap these results
internally and their public surfaces are unchanged.

## Added — the fragment-hashes state (experimental, with the fragment APIs)

- **`HashGraphState`** (`Checked` / `FragmentHashes` / `Unchecked`) and
  `hash_graph_state()` on `Automerge`/`AutoCommit` (wasm/JS:
  `hashGraphState()` returning `"checked" | "fragmentHashes" |
  "unchecked"`).
- **Save writes hash columns** (three change columns under one new
  ColumnId: a delta column of node indices, a value-metadata/value pair
  of 32-byte hashes): every fragment-level (> 0) hash plus loose commits
  and anchors, heads excluded (the head-index suffix has them). Old
  automerge versions ignore and drop the columns (verified against main
  and 0.9). Size: ~32 bytes per stored hash — tens of bytes on compacted
  docs, ≤ ~31KB on a 134k-change document.
- **Loading with `skip_hash_graph` imports the columns** and enters the
  `FragmentHashes` state: fragment APIs work immediately (no rebuild),
  hashes carried by the columns resolve, and everything else still
  errors with `UncheckedHashGraph`. The imported hashes are trusted like
  the head pairing; `rebuild_hash_graph` verifies them (and a *checked*
  load verifies the columns against the recomputed hashes outright).
- **`Fragment.members` is now `Vec<ChangeId>`** (`ChangeId { actor, seq }`,
  new experimental type): members are derivable from graph structure, so
  fragments — including `bundle_fragments`, which now builds bundles
  from nodes and only needs boundary hashes — work in the
  `FragmentHashes` state.

## Behavior

- **DEFLATE now uses the `zlib-rs` backend** of `flate2` (pure Rust,
  noticeably faster inflate on load, works on wasm). No format change.
- Document load is substantially faster on the checked path too (~10–25%):
  op indexes are built during column load in the same decode pass, and the
  change-hash computation no longer re-materializes ops for indexing.
- New structural validation on load: object-id columns are checked
  (fully-null or fully-set pairs, strictly increasing) during the load walk;
  actor-index ranges and succ/value totals are checked post-load; change
  dep indexes are bounds-checked.

## Bindings

- **wasm / JavaScript**: `load(data, { skipHashGraph: true })` and
  `rebuildHashGraph(doc)` expose the unchecked-load flow;
  hash-dependent calls on an unchecked document throw
  (`RangeError: the hash graph has not been built ...`) instead of
  panicking the wasm module. A few wasm methods that could previously
  never throw now can (`getChangesAdded`, `getLastLocalChange`,
  `generateSyncMessage`, `topoHistoryTraversal`, `saveSince`, `clone`
  and `fork` with an actor, `isolate`).

## Internal (not API, worth knowing)

- Clock queries use a rebuilt cache (forward sweep at load, dominance
  pruning at query time); clock latency on merge-heavy documents dropped
  from milliseconds to microseconds.
