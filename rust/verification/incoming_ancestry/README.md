# Ancestry required for canonical group evaluation

The claim is about the **past of one ready group head**, not the entire pending
batch: every ancestor has a complete chain, each actor/sequence slot identifies
one hash, and sequence numbers above one have the exact same-actor predecessor
as a direct dependency. Unrelated conflicts or unresolved groups are allowed.

## Checked production code

- [`actor_input.rs`](../../automerge/src/automerge/signing/incoming/actor_input.rs)
  constructs each actor row at its resolved hash's rank, preserving the
  original dependency order and the exact applied or candidate label of
  each parent. It passes those very rows to the checked propagation pass.
- [`ancestry_gate.rs`](../../automerge/src/automerge/signing/incoming/ancestry_gate.rs)
  makes both scans use **the same** slot observer. A clean batch proves its
  candidate subsets are clean without a walk. When the batch has a conflict,
  the production fallback runs the checked dependency walk and the checked
  slot scan on its exact unapplied cone. It returns a conflict exactly when
  this cone has an occupied or repeated slot. It does not open applied history.

## Composition

[Live-row facts](live_spec.rs) connect readiness and the materialized author
map: `Some(None)` at the map lookup is a live, authorless change, not a missing
or broken chain. A linked candidate has only applied or linked parents; its
real parent edges include the same-actor predecessor when its sequence is
above one. [Cone soundness](cone_spec.rs) combines these facts, historical
actor-prefix invariants, and the guard result. This supplies the actor-clock
invariant on the exact ready head's full past without imposing it on the batch.

[Cutoff composition](../boundary_past/ready_cutoff.rs) instantiates that cone
proof in the boundary-clock theorem. The observed level-walk boundary lies
inside the ready cone, and the actual overlay summary, applied clock and
candidate cutoff refer to that same cone. The hard-descriptor theorem then
uses the checked member walk and intrinsic member-level condition to produce
the canonical-fragment premise used by the [selection closure theorem](../actor_clock/README.md).
It derives decreasing ranks on candidate edges from the actor rows; the
applied rank invariant is a graph boundary contract.

## Remaining boundary

The type-level inputs are still projections of the Rust document. These
harnesses do not check the actual `ChangeGraph`/`OpSet`/`Authors` observers or
that every concrete adapter in `incoming.rs` discharges the alignment
premises: exact applied-slot metadata, exact hash/rank and parent observations,
clock maxima for the applied seeds, and matching canonical/member views. Nor
do they prove the end-to-end plan-to-document transition. The regular tests
exercise the incoming adapters, but are not proofs of their preconditions.

Run with the [pinned Verus toolchain](../incoming_topology/README.md):

```sh
verus --edition=2021 --crate-type=lib --cfg automerge_verus --no-cheating \
    rust/verification/incoming_ancestry/lib.rs
verus --edition=2021 --crate-type=lib --cfg automerge_verus --no-cheating \
    rust/verification/boundary_past/lib.rs
```
