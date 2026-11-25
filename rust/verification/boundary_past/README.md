# Incoming boundary-past summaries

Review [interpretation.rs](interpretation.rs) for what an exact summary means.
The ordinary dependency walk cannot open an applied node or a node with a
reusable memo entry. It visits the stops themselves:

- An expanded candidate contributes just its own actor and sequence.
- An applied stop contributes its hash as a seed for `ChangeGraph`.
- A memoized candidate stop contributes the saved summary of its inclusive
  past, not just its own actor and sequence.

The [production summary fold](../../automerge/src/automerge/signing/incoming/boundary_summary.rs)
is verified against [spec.rs](spec.rs): it takes the exact union of the seed
sets and the attained maximum for each candidate actor. The
[composition proof](exactness.rs) shows that, with an initially exact memo,
that walk and fold produce an exact summary of the boundary node's inclusive
past. In particular, the applied seeds need not be minimal; following their
pasts recovers exactly the applied portion of the boundary past. Overlapping
memo entries are harmless.

## Trusted boundary

- [`boundary_past_for`](../../automerge/src/automerge/signing/incoming.rs)
  translates the stable graph snapshot to the walk and fold callbacks. They
  agree about which hashes are applied or memoized; the applied graph must
  already be dependency-closed. For a newly computed result the caller inserts
  the summary under that boundary node's own hash. Cache hits copy the stored
  summary, and each evaluation pass starts with an empty memo. The raw
  `HashMap` lookup/insert wiring and `ChangeGraph` queries are outside the
  harness.
- The fold trusts the standard collection specifications and that actor
  equality, hashing and cloning agree with actor identity, and that hash
  ordering agrees with change identity.
- Turning the summary into a membership test also needs
  `ChangeGraph::seq_clock_for_heads` to return the actual applied maxima for
  the given seeds. The [actor-clock theorem](../actor_clock/README.md) explains
  why those maxima and the candidate-side maxima represent exact membership
  in a valid, non-equivocating history. This harness does not verify the
  applied clock implementation or the entire `evaluate_single_group` call.

Run with the [same toolchain as the ancestry worklist](../incoming_topology/README.md):

```sh
verus --edition=2021 --crate-type=lib --cfg automerge_verus --no-cheating \
    rust/verification/boundary_past/lib.rs
```
