# Intrinsic validation and incoming plan assembly

This harness imports the actual production kernels. It takes immutable views
of changes, dependencies, and observations; it does not interpret change
bytes or assume that an incoming group is valid.

## Intrinsic validation

- [Member and slot index](index_spec.rs): index exactly the supplied members.
  Reject out-of-range sequences, an author away from sequence one, a repeated
  member hash, and a conflicting applied or batch actor slot. Old batch-slot
  bindings are never changed; the same hash can occur in separate deliveries.
- [Shape](shape_spec.rs): require a unique head (no in-group dependent) and a
  full traversal from that head. A loose group is a singleton. Other members
  must have strictly lower levels than a hard head. The traversal must report
  only in-group nodes, as guaranteed by the [walk contract](../incoming_walk/README.md).
- [Intra-group chains](chain_spec.rs): sort without changing the member rows.
  Each adjacent pair in an actor's run must have consecutive sequences and a
  direct predecessor edge. Report a failure using the child’s **original**
  member index. The adapter reads actor, sequence, level and dependency facts
  from the same decoded changes indexed in the first check.

## Group decisions and descriptor assembly

- [Batch and occurrence partition](batch_spec.rs): validate deliveries in
  order with a private ledger and return no partial batch on error. The later
  evaluation pass records exactly the waiting and verified occurrences in
  their original order, omits disproved old groups, and stops at the first
  fatal error. The per-occurrence caller uses the checked origin policy in
  [incoming completion](../incoming_completion/README.md).
- [Decision gate](evaluation_spec.rs): a known broken dependency wins over
  waiting for unresolved ancestry. Otherwise a ready group must pass the
  equivocation, canonicality, then attached-evidence checks. No proof is
  consulted by this driver before those preceding checks pass.
- [Canonical descriptor](canonical_spec.rs): a loose descriptor has its
  direct parents as its boundary, one member, and no checkpoints. A hard
  descriptor passes the **exact returned boundary** into the member walk and
  lists exactly the non-head, positive-level members as checkpoints. The
  adapter supplies the level-cut and boundary-past views specified by the
  [dependency walk](../incoming_walk/README.md) and [boundary memo](../boundary_past/README.md)
  harnesses.

## Graph and plan projection

- [Topology input](topology_input_spec.rs): count every unapplied dependency
  occurrence, including missing ones, and create a reverse edge only when the
  parent is another candidate. The only dependency classification supplied by
  the adapter is whether the parent is applied, at a candidate index, or
  missing. Repeated dependencies count separately.
- [Snapshot index](snapshot_spec.rs): build the exact first/last-occurrence
  maps over the projected delivery rows. Used for provisional authors,
  unapplied changes by hash, and per-delivery frontier lookup; a present `None`
  payload still occupies its key. The filter and metadata are supplied by the
  caller.
- [Plan lookup](plan_spec.rs): build exactly the inverse rank map. The
  [incoming plan harness](../incoming_plan/README.md) checks the joint member,
  proof and blocked occurrence projection from one selection. The
  [transaction materializer](../incoming_transaction/payload_spec.rs) fetches
  changes in their original planned order from the owned cohort.

Run with the [pinned Verus toolchain](../incoming_topology/README.md):

```sh
verus --edition=2021 --crate-type=lib --cfg automerge_verus --no-cheating \
    rust/verification/incoming_intrinsic/lib.rs
```
