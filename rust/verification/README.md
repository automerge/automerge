# Incoming signing admission: Verus coverage

The spec in each directory is the review surface, together with the listed
trusted adapters. The production harnesses import the actual Rust kernels;
there is no Verus dependency in the Cargo workspace. The clock theorems are
separate and do not check executable code.

| Spec | Checked |
| --- | --- |
| [Incoming ancestry worklist](incoming_topology/README.md) | A duplicate-free dependency order containing every resolvable candidate |
| [Dependency walks](incoming_walk/README.md) | Exact in-group reachability, boundary discovery, and member walks through a supplied causal cut |
| [Author checks](incoming_authors/README.md) | Exact predecessor lookup and group-local cache reuse under the established frontier authors |
| [Fragment selection](fragment_selection/README.md) | The greatest subset of proved fragment occurrences closed under boundary requirements |
| [Actor clocks](actor_clock/README.md) | Conditional theorems: representing a closed past by maxima, preservation of causal closure, and exact accepted-set extension |

This is **not** verification of all of [`incoming.rs`](../automerge/src/automerge/signing/incoming.rs).
Remaining executable obligations include intrinsic head, level, and actor-chain
validation; cross-change author propagation and ancestry equivocation checks;
the boundary-past memo and clocks; construction and verification of attached
proofs; the final admission plan and pending-group lifecycle; and [`complete`](../automerge/src/automerge/signing/incoming.rs)
and its calls into document and signing state. The selection proof is about
already-verified descriptors and is independent of those obligations.

Run all checks with the [pinned Verus release and Rust toolchain](incoming_topology/README.md),
from the repository root:

```sh
for harness in incoming_topology incoming_walk incoming_authors fragment_selection actor_clock; do
    verus --edition=2021 --crate-type=lib --cfg automerge_verus --no-cheating \
        "rust/verification/$harness/lib.rs" || exit
done
```
