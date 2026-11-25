# Causal history and actor clocks

These are theorems about the admission contracts, not verification of a
production clock or cache implementation. The reviewed definitions are in
[spec.rs](spec.rs) and [admission_spec.rs](admission_spec.rs).

- **An exact maximum represents exact membership.** Within a history with no
  conflicting hashes for an actor and sequence, and a direct predecessor edge
  at every sequence above one, compare a node against its actor's maximum in a
  parent-closed past. The comparison is true exactly when the node is in that
  past. This also works if the maxima and the query are restricted to the same
  side of a partition, as in the applied and candidate clocks in `incoming.rs`.
- **Selection preserves causal closure.** Start with a closed accepted history
  and a boundary-closed selection of canonical fragments. The union of the old
  history and the selected members is parent-closed. A boundary may be supplied
  by an overlapping fragment: following that supplier increases head level or
  moves to an earlier head at the same level, so the argument is well-founded.
- **Exact publication.** Updating the old accepted clock by taking its
  per-actor maximum with the selected members' clock accepts exactly the old
  accepted set plus those members, not any unselected change.

The connection to the runtime remains conditional: canonical descriptions must
satisfy the cut and level contracts, the selected history must satisfy the
actor-chain and uniqueness contracts, and clock updates must implement the
stated maxima. This harness does not verify `boundary_past_for` or its memo,
`AcceptedClock::include_hashes`, document application, or `complete`.

Run with the [same toolchain as the ancestry worklist](../incoming_topology/README.md):

```sh
verus --edition=2021 --crate-type=lib --no-cheating \
    rust/verification/actor_clock/lib.rs
```
