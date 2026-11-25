# Incoming signing admission: Verus coverage

The spec in each directory is the review surface, together with the listed
trusted adapters. The production harnesses import the actual Rust kernels;
there is no Verus dependency in the Cargo workspace. The clock theorems are
separate and do not check executable code.

| Spec | Checked |
| --- | --- |
| [Intrinsic validation and plan assembly](incoming_intrinsic/README.md) | Member/slot indexing, head/reachability/level checks, actor chains, exact reverse-edge inputs, ordered validation and evaluation, descriptor assembly, selected and blocked plan projections |
| [Incoming ancestry worklist](incoming_topology/README.md) | A duplicate-free dependency order containing every resolvable candidate |
| [Dependency walks](incoming_walk/README.md) | Exact in-group reachability, boundary discovery, and member walks through a supplied causal cut |
| [Authors and actor slots](incoming_authors/README.md) | Whole-pass propagation from exact predecessors, original causes of broken chains, actor-slot conflicts, and group-local cache reuse |
| [Boundary past](boundary_past/README.md) | Exact memo lookup, fill and union on one snapshot, and exact cutoff given the applied-clock contract |
| [Incoming groups](incoming_groups/README.md) | Delivered/canonical equality, per-actor frontiers, attachment search and proof assembly, and exact unapplied rank order |
| [Fragment selection](fragment_selection/README.md) | The greatest subset of proved fragment occurrences closed under boundary requirements |
| [Actor clocks](actor_clock/README.md) | Conditional theorems: representing a closed past by maxima, preservation of causal closure, and exact accepted-set extension |
| [Incoming completion](incoming_completion/README.md) | Apply/error gating, selected-proof publication order, exact accepted-clock update, and the post-apply pending policy |

## Remaining composition gap

There are checked implementations for the principal policies of all five
stages, but **not a full source-level proof of**
[`incoming.rs`](../automerge/src/automerge/signing/incoming.rs). The kernels'
contracts are linked by Rust adapters; Verus does not yet check that every
adapter establishes the next kernel's preconditions. In particular:

- The adapters still have to supply faithful row observations: which changes
  are applied, which provisional authors qualify, which graph/actor-table slots
  identify applied sources, and which changes/tables correspond to each rank.
  The production index and origin decoders now check the corresponding map
  construction, not the accuracy of those graph/author lookups.
- The memo now borrows a single immutable graph/candidate view and has a
  checked lookup, fill, and union. The cutoff theorem still needs a proof at
  the concrete group call site that the ancestry is resolved, has valid
  chains and no equivocation, and is queried through the applied-clock
  contract on that same snapshot.
- The per-frontier adapters must use the correct attachment and established
  author. Pending deduplication has a checked member-set comparison, but
  still depends on exact head and attachment equality and validated candidates.
  Prepared admission carries only the selected proof payloads across apply;
  the full relation between those proofs, members and graph effects is not
  yet checked at the original entry point.

Document application, Ed25519, graph/actor queries, and signature
merge/reclamation, readiness, and draft pruning are external operations; the
harnesses prove their use only under the listed per-operation contracts. A
single checked entry-point contract connecting the real adapters would be a
stronger result than the individual kernel checks passing.

Run all checks with the [pinned Verus release and Rust toolchain](incoming_topology/README.md),
from the repository root:

```sh
for harness in incoming_intrinsic incoming_topology incoming_walk incoming_authors boundary_past incoming_groups fragment_selection actor_clock incoming_completion; do
    verus --edition=2021 --crate-type=lib --cfg automerge_verus --no-cheating \
        "rust/verification/$harness/lib.rs" || exit
done
```
