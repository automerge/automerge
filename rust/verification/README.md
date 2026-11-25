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
| [Boundary past](boundary_past/README.md) | Exact candidate maxima and applied seed ancestry, including substitution of previously summarized memo entries |
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

- The adapters form the provisional actor-author catalog, tag and concatenate
  old/new candidates, and project decoded changes and graph lookups into the
  member, dependency, rank, and author views. They resolve returned origins
  and plan indices back to those same values.
- The canonical caller must interpret boundary summaries against the exact
  same graph snapshot, keep memo entries keyed by the root that they summarize,
  combine each boundary summary, and supply the applied-clock cutoff. The
  summary, traversal and clock contracts do not check this complete caller.
- The per-frontier adapters must use the correct attachment and established
  author; pending deduplication assumes the stable delivery equality documented
  in the completion harness. Plan and completion proofs rely on faithful
  selection and graph projections between stages.

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
