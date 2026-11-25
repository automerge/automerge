# Incoming boundary-past memo and cutoff

[The memo spec](memo_spec.rs) describes the overlay of candidate changes and
applied leaves. A summary consists of **exactly** the applied leaves reachable
from its root, and each candidate actor's highest reachable sequence. Every
stored maximum must be attained by an actual reachable candidate. No memo
or traversal order appears in this definition.

## Checked production code

- [`boundary_memo.rs`](../../automerge/src/automerge/signing/incoming/boundary_memo.rs)
  computes that summary directly. Cache hits copy an exact summary for the
  requested key. A miss walks the candidate graph, stops at applied leaves
  or exact memo entries, and inserts the result under the requested root
  without changing another entry. The union fold visits exactly the supplied
  boundary roots and preserves old bindings. An initially empty memo satisfies
  the invariant; each checked call preserves it for the same observation.
- [`boundary_summary.rs`](../../automerge/src/automerge/signing/incoming/boundary_summary.rs)
  checks summary cloning and the exact union/maximum operations used in the
  walk and the boundary fold, using standard Rust collection specifications.
- [`boundary_cutoff.rs`](../../automerge/src/automerge/signing/incoming/boundary_cutoff.rs)
  tests an applied hash against its applied slot's maximum, and a candidate
  against its own actor's candidate maximum. It cannot use one side's maximum
  for the other side.

[Overlay composition](overlay_bridge.rs) proves that, for a dependency-closed
applied graph, the full boundary past is the union of the overlay candidate
ancestors and the inclusive pasts of exactly the applied leaves in the memo
summary. [The cutoff theorem](cutoff_spec.rs) combines this result, exact
per-actor maxima, and validated actor chains: the production cutoff test is
exact membership in that full boundary past, not an approximation.

## Trusted boundary

[`BoundaryPastMemo` in `incoming.rs`](../../automerge/src/automerge/signing/incoming.rs)
borrows the applied graph and candidate table for its entire lifetime. The
boundary walk, memo observer, and membership cutoff use those same references;
the memo cannot survive document apply or a change to the borrowed table.
The Verus harness still assumes that the adapter returns the actual metadata
for each reachable hash, that the relevant ancestry has valid chains and no
equivocation, and that `ChangeGraph::seq_clock_for_heads` provides exact applied
maxima for the given seeds. It does not prove those graph APIs or discharge all
of the cutoff theorem's premises at the concrete group-evaluation call site.
Hash/actor equality, hashing, and cloning have their usual identity contracts.

Run with the [pinned Verus toolchain](../incoming_topology/README.md):

```sh
verus --edition=2021 --crate-type=lib --cfg automerge_verus --no-cheating \
    rust/verification/boundary_past/lib.rs
```
