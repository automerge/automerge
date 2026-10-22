# Hash retention

Which change hashes a `next` (`Retained`) document keeps, which ones a
bundle must carry, and where each requirement comes from. Written as the
starting point for an audit of this code. Each section names the functions
that implement it so the claims can be checked against the code.

## Background

- A change's hash covers its deps' hashes, so **a hash can't be computed
  for one change on its own**. Rebuilding it means rebuilding every
  unhashed ancestor back to ones whose hashes are known (`rebuild_hash` →
  `nodes_back_to_retained` in `automerge.rs` / `change_graph.rs`). Inside a
  fragment that can be the whole fragment.
- `Full` (audit mode) keeps every hash. `Retained` keeps the *retained
  set* and frees the rest when garbage collection runs
  (`hash_retention.rs`).
- A hash's **fragment level** is its count of leading zero bytes
  (`ChangeHash::fragment_level`).

## Terms

| term | meaning | code |
|---|---|---|
| fragment head | a change with level > 0; ends a fragment | `cache_fragment_inner` |
| fragment | a level-L head plus its ancestry back to the merged clock of its `deps`. Lower-level cached fragments it covers are superseded and dropped from the cache. | `FragmentNode`, `cached_fragment`, `fragment_nodes` |
| deps / boundary | a level-L head's nearest ancestors of level ≥ L, whether or not a cached fragment still heads at them. Found from history alone: each actor's latest such change below the head's clock (`leveled`), less those in another's ancestry. Arrival order changes only node order, never a fragment. | `fragment_boundary` |
| checkpoint | a level > 0 member of a fragment other than its head | `export_fragment`, `leveled_members` |
| `fragment_top` | merged clock of every cached fragment | `cache_fragment_inner` |
| loose commit | a change above `fragment_top`; exported as a level-0 fragment whose boundary is its parents | `loose_commits`, `loose_commit` |
| anchor | a covered, level-0 parent of a loose commit | `anchors_of` |
| actor tip | the latest change of an actor *in this document* | `seq_index[actor].last()` |
| fragment pins | a fragment's members' parents outside it, and each actor's last member (the receiver's tip for that actor) | `fragment_pins` |
| pin count | how many holders need a node's hash | `Pins` |

## The retained set

A `Retained` document keeps a hash when any of these holds
(`ChangeGraph::must_keep`):

1. its level is > 0 (fragment heads and checkpoints): kept for good
2. it is pinned (`Pins`, a count per node), by
   - **each cached fragment**: its fragment pins. Pinned when the fragment
     is cached, unpinned when a higher one supersedes it. A fragment's
     boundary is fixed once it is cached, so its pins are computed again
     to unpin (`unpin_fragment`) rather than stored. Its members are each
     actor's `seq_index` range between its boundary clock and its clock,
     so computing them is a scan of those ranges, not an ancestry walk.

     A new fragment can pin a change the GC already freed: a fork P inside
     fragment B whose other branch sat in a fragment concurrent with B. P
     was interior to both until a fragment covering both put B on its
     boundary and P outside it. No earlier pin could have kept it (the
     concurrent fragment can form first), so the document rebuilds those
     hashes after the commit or apply that cached the fragment
     (`take_unhashed_fragment_pins`, `rebuild_unhashed_fragment_pins`). A
     bundle's own members never take this path.
   - **each loose commit**: itself and all its parents, from when it is
     added until `fragment_top` covers it (`pin_loose`,
     `release_covered`). A new change is always above `fragment_top`, so
     the commits released are exactly the per-actor `seq_index` ranges
     between the old and new `fragment_top`. A bundle's members are pinned
     only after its hashes are recorded (`pin_deferred_loose`), and only
     those its fragments left loose.
3. it is an actor tip
4. it is one of the document's heads

GC (`retain_hashes_only`) only looks at *candidates*: hashes learned, or
unpinned to zero, since the last GC. Each one it keeps as a tip or head is
re-queued, because nothing signals when a node stops being one. A GC costs
O(candidates), not O(graph). It runs when a newly cached fragment triggers
it (`gc_retained_hashes`), unless `GcMode::Manual` defers it. An
`AutoCommit` collects only in `save` and `save_incremental`, so nothing the
next incremental save names is freed in between.

`into_retained` (loading, or leaving audit mode) computes every pin from
scratch (`rebuild_pins`) and keeps what `must_keep` says.

## Who reads a hash, and what happens if it's missing

| consumer | which hash | missing → | code |
|---|---|---|---|
| commit as the current actor | that actor's tip, added to deps if it isn't a head | **panic** (`expect`) | `transaction_args` |
| `set_actor` to an existing actor | that actor's tip | rebuilt lazily; a failed rebuild is **silently** ignored, and the panic above follows on commit | `ensure_actor_tip_hash`, `rebuild_hash` |
| `new().with_actor(X)` then loading X's history | X's tip | no lazy rebuild (doesn't go through `set_actor`) → commit panics | `get_or_create_actor_index` |
| writing a bundle: each member's parent outside the bundle | any change, *not only* fragment heads | `UnretainedHashes` error | `write_bundle_changes` |
| writing a bundle: hashes its receiver must retain | see "What a bundle carries" | `UnretainedHashes` error | `hashes_to_retain` |
| exporting a fragment | head, boundary | `expect` panic | `cached_fragment` |
| exporting a loose commit | it and its parents | the loose commit is silently left out | `loose_commit` (`filter_map` in `loose_commits`) |
| `merge` duplicate-seq check | both documents' hash at the last shared seq | check **silently skipped** | `merge` (`hash_at`) |
| `get_last_local_change_legacy` | current actor's tip | error | |
| heads, sync | heads | always held | |

## What a bundle carries

Built by `assemble_bundle` / `bundles_for_fragments` (`automerge.rs`):

- **heads**: the fragment head(s)
- **checkpoints**: level > 0 members
- **deps**: the hash of every member's parent outside the bundle
  (`write_bundle_changes`)
- **boundary**: the fragment's boundary entries that are also deps
  (`27fab1df2`), plus the pins of every fragment headed in the bundle that
  lie outside it and aren't deps already (a one-change bundle carrying a
  fragment head is the usual case). A receiver pins those when it caches
  the fragment, and may already have freed them.
- **retained**: `hashes_to_retain`, the level-0 member hashes the receiver
  must keep. This is computed *from the receiver's point of view*, since
  the writer's own retained set describes the writer's history, not the
  receiver's:
  - `retained_from(level>0 members ∪ loose members, frontier)` where
    `frontier` is the clock of the bundle's level > 0 members
  - **plus each actor's last member in the bundle** (`member_tips`). After
    loading up to this bundle, that change is the receiver's tip for the
    actor, even though it's an interior change in the writer's document.
  - **plus the pins, inside the bundle, of every fragment headed in it**.
  - Both modes name exactly that set, so audited and `next` documents
    with the same history write the same bytes. A `Retained` writer
    errors if any of it is missing.
  - `NameHashes::UntagTiny` (whole-document `save` only) leaves out level-0
    hashes of changes with ≤ `TINY_CHANGE_OPS` ops when the bundle is within
    `REHASHABLE_CHANGES` / `REHASHABLE_OPS`. The receiver rebuilds them
    (`rehash_untagged`), and only when `Bundle::is_rehashable`, which also
    requires empty deps and boundary. That holds today only because
    `UntagTiny` is used only for whole-document saves.

### Which members

`merge` and `save_after(heads)` send the changes above the receiver's
clock (`nodes_above`). When the writer has freed a hash that set would
name as a dep, it starts further back: past every unhashed change behind
those deps, and past everything descending from them, so the clock stays
causally closed. The extra changes are ones the receiver already has.
`AutoCommit::save_incremental` does the same, so a `next` chunk can be
wider than an audited one: when a change that arrived since the last
save depends on a change interior to the bundle that brought it, which
both sides have rightly freed.

## Receiving a bundle

`plan_bundle` → commit → `rehash_bundle` (`automerge.rs`):

1. records hashes for boundary entries and external deps (`hash_pairs`,
   also for bundles overlapping what the document has), heads,
   checkpoints, and the `retained` section, all in node order. Only a
   head that is new to the document becomes one of its heads.
2. rebuilds the fragment pins of changes it already had
   (`rebuild_unhashed_fragment_pins`, see "The retained set")
3. `rehash_untagged` for rehashable bundles, for every member whose hash
   the document lacks, including members it already had
4. `missing_retained_hashes`: if any node pinned since the last check, or
   any actor tip, has no hash → **panic**. The check needs the graph as
   it stands after the commit, so the bundle's changes are already in the
   document. An error would leave the document inconsistent. A failed
   `rehash_untagged` panics for the same reason. Moving the check before the
   commit would let both become errors.

Hashes recorded in step 1 that nothing pins are GC candidates, and the
next GC frees them.

## History of this problem

### Fixed: receiver tips missing from fragment bundles

Bundles named tips from the writer's full history, so a receiver that had
loaded only part of the stack lacked its own tips. The fallback was
`rehash_document_for`, which rehashed every change in the document once per
such bundle. The fix is `member_tips` in `hashes_to_retain`; the fallback is
removed and the case now panics.

`load(first)` + `load_incremental(rest)` (ms), and the size cost of
`member_tips` across each document's fragment stack:

| doc | before | after | `load(all)` | stack bytes |
|---|---:|---:|---:|---:|
| A1 | 868 | 163 | 154 | +2.8 KB (+0.25%) |
| A2 | 453 | 89 | 79 | +18.2 KB (+2.15%) |
| C1 | 32,851 | 112 | 83 | +11.9 KB (+0.85%) |
| C2 | 16,731 | 51 | 48 | +3.8 KB (+0.66%) |
| monday-meeting-notes | 24 | 12 | 10 | +5.3 KB (+2.20%) |
| essay | 145 | 136 | 115 | +34 B |
| pathological | 19,309 | 15,507 | 2,724 | +162 KB (+0.42%) |

The other egwalker and `benchmark-battery/data` documents are unchanged.
Pathological's remaining time isn't hashing (sha256 < 1% of the profile);
it goes to merging each of its 1,593 bundles into a 38 MB document.

### Fixed: a `next` document couldn't re-serve its fragments

Load a fragment stack into `next::Automerge`, then call
`bundles_for_fragments(fragments(..))`:

| doc | failing fragments | unhashed external parents |
|---|---:|---:|
| C2 | 142 of 143 | 394 |
| monday-meeting-notes | 36 of 91 | 212 |

Every missing hash is a level-0 interior change: never a fragment head, and
never in any audited fragment's boundary. A cached fragment's members run
back to the merged *clock* of its deps, so a member can depend on any
change below that clock, not only on a dep's head. Those parents' hashes
arrive as bundle deps, but they aren't in the retained set, so GC frees
them. The failure predates `27fab1df2`: `2b4f4e95b` fails the same way.

The fix is the fragment pins: each cached fragment pins its members'
outside parents and each actor's last member. Loading each document's
stack into `next`, running GC and exporting every fragment now fails on
none, by `load`, `load_incremental` and one-change bundles.

### Fixed: the fragment index depended on hash arrival order

A `next` load reported 143 fragments for C2 where the audited document has
102 (C1 380 vs 354, essay 264 vs 58). Every extra was a checkpoint of
another fragment. A fragment's boundary is fixed once its head's change is
in (its ancestors' level > 0 hashes are always known by then), but one
bundle apply recorded its hashes out of graph order: heads first, then
checkpoints sorted by hash. A checkpoint cached after its head was never
superseded. Apply now records a bundle's heads, checkpoints and retained
hashes together in node order (`commit_bundle_graph`), and `cache_fragment_inner`
debug-asserts that no cached fragment already covers a new head. Fragments
(head, level, boundary, checkpoints) from `load`, `load_incremental` and a
reloaded whole-document save of each stack equal the audited document's.

### Fixed: `Retained` writers couldn't name receiver tips

`member_tips` hashes were interior changes in the writer, so GC freed
them. Each fragment's last members are now fragment pins.

### Fixed: fragment boundaries depended on arrival order

A fragment's deps were the cached fragments its head covered when it was
cached. A concurrent higher fragment arriving first superseded lower ones
the new head should stop at, so the boundary, and the members, depended
on arrival order (seed 2 of the fuzz test: 279 members against 40). The
same rule is on `main`. Deps now come from history alone
(`fragment_boundary`).

### Fixed, found by the fuzz test

- An overlapping bundle re-added a head the document had already built on
  (`commit_bundle_graph`).
- A whole save loaded into a document that already had part of it left a
  freed parent unhashed (`rehash_untagged` covered only new members).
- `merge` and `save_after` failed when the writer had freed a dep hash;
  widening to one covering fragment rarely applied (`nodes_above`).
- Overlapping bundles didn't record their deps' hashes.
- Every save named every held hash, not just what its receiver needs (a
  first `next` save of the 10M toggle document was 360 MB).

## Where it is loud now

| situation | result |
|---|---|
| bundle leaves out a hash its receiver must retain | panic: "bundle left out N change hashes this document must retain" |
| GC finds a pinned node or actor tip without its hash | panic: "N change hashes this document must retain are already gone" |
| a holder unpins a node nobody pinned | panic: "unpinned …, which no holder pinned" |
| a fragment is cached after one that covers it (debug builds) | panic: "fragments cached out of graph order" |
| `slow_path_assertions`: a pin count differs from one recomputed from every holder | panic, naming the node |
| writing a bundle whose members depend on an unhashed change | `AutomergeError::UnretainedHashes` ("changes outside the bundle that its members depend on") |
| writing a bundle missing a hash its receiver must retain | `AutomergeError::UnretainedHashes` ("changes its receiver must retain") |
| writing a bundle missing a fragment pin outside it | `AutomergeError::UnretainedHashes` ("changes outside the bundle its fragments need") |

Still quiet, from the table above: `rebuild_hash` failures in `set_actor`,
loose commits dropped from `fragments()`, the `merge` duplicate-seq check,
and `ensure_actor_tip_hash` not running for `new().with_actor(X)`.

## Fuzz test

`automerge::retention_fuzz_tests` runs an audited and a `next` cluster of
four actors through the same random edits, partitions and syncs (`merge`,
`save_after`, whole saves, fragment stacks). It checks, as it goes:

- fragment stacks and whole saves are byte-identical in both modes;
- a `next` document holds exactly what a fresh conversion of the audited
  one holds, and reloading its stack (by `load` and `load_incremental`)
  gives the same heads, fragments and held hashes, and can commit as
  every actor;
- incremental saves are byte-identical unless the `next` document lacks a
  dep the audited chunk names, and a widened chunk carries only changes
  its archive already has; both archives' saves equal the audited save;
- no pinned change is ever without its hash, and heads never include an
  ancestor of another head.

`AUTOMERGE_FUZZ_CHANGES` and `AUTOMERGE_FUZZ_SEED` set the run; the
`small_fragments` feature makes levels count zero nibbles, so a few
thousand changes build fragments to level 3. Run long:

```
AUTOMERGE_FUZZ_CHANGES=20000 cargo test -p automerge --lib --profile wasm-test \
  --features small_fragments,slow_path_assertions retention_fuzz
```
