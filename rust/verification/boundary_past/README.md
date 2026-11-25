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
exact membership in that full boundary past, not an approximation. The
[ready-cone composition](ready_cutoff.rs) discharges the actor-chain premise
with the live rows and conflict guard, then uses the checked walks to
establish the canonical descriptor.

## Trusted boundary

[`BoundaryPastMemo` in `incoming.rs`](../../automerge/src/automerge/signing/incoming.rs)
borrows the applied graph and candidate table for its entire lifetime. The
boundary walk, memo observer, and membership cutoff use those same references;
the memo cannot survive document apply or a change to the borrowed table.
The composition assumes that the actual Rust adapters return faithful
metadata for every relevant hash and that `ChangeGraph::seq_clock_for_heads`
returns exact applied maxima for the given seeds. It derives chain validity
and non-equivocation from stage contracts rather than assuming them over
the whole pending batch. The concrete group-evaluation call site still needs
to establish the raw graph/row/clock alignment contracts.
Hash/actor equality, hashing, and cloning have their usual identity contracts.

Run with the [pinned Verus toolchain](../incoming_topology/README.md):

```sh
verus --edition=2021 --crate-type=lib --cfg automerge_verus --no-cheating \
    rust/verification/boundary_past/lib.rs
```
