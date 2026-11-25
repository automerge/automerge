# Incoming signing admission: Verus coverage

The spec in each directory is the review surface, together with the listed
trusted adapters. The production harnesses import the actual Rust kernels;
there is no Verus dependency in the Cargo workspace. The clock theorems are
separate and do not check executable code.

| Spec | Checked |
| --- | --- |
| [Incoming ancestry worklist](incoming_topology/README.md) | A duplicate-free dependency order containing every resolvable candidate |
| [Dependency walks](incoming_walk/README.md) | Exact in-group reachability, boundary discovery, and member walks through a supplied causal cut |
| [Authors and actor slots](incoming_authors/README.md) | Whole-pass propagation from exact predecessors, original causes of broken chains, actor-slot conflicts, and group-local cache reuse |
| [Boundary past](boundary_past/README.md) | Exact candidate maxima and applied seed ancestry, including substitution of previously summarized memo entries |
| [Incoming groups](incoming_groups/README.md) | Delivered/canonical equality, per-actor frontiers, attachment search and proof assembly, and exact unapplied rank order |
| [Fragment selection](fragment_selection/README.md) | The greatest subset of proved fragment occurrences closed under boundary requirements |
| [Actor clocks](actor_clock/README.md) | Conditional theorems: representing a closed past by maxima, preservation of causal closure, and exact accepted-set extension |

This is **not** verification of all of [`incoming.rs`](../automerge/src/automerge/signing/incoming.rs).
Remaining executable obligations include intrinsic head, level, and actor-chain
validation; the full graph-to-canonical projection and per-group orchestration;
actual store and graph updates at completion, and pending-group lifecycle. The
verified rank projection assumes the selected member union is mapped correctly
and its unapplied dependencies are selected. The selection proof is about
already-verified descriptors and is independent of these obligations.

Run all checks with the [pinned Verus release and Rust toolchain](incoming_topology/README.md),
from the repository root:

```sh
for harness in incoming_topology incoming_walk incoming_authors boundary_past incoming_groups fragment_selection actor_clock; do
    verus --edition=2021 --crate-type=lib --cfg automerge_verus --no-cheating \
        "rust/verification/$harness/lib.rs" || exit
done
```
