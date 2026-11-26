# Preparing an incoming admission

The [entry contract](entry_spec.rs) is about **one owned candidate cohort**.
Validate fresh deliveries before opening pending. Keep exactly the old then
new deliveries with their original origin tags. Resolve ancestry once; pass
the same candidate slice and resolved view to evaluation and planning. An
error in validation or evaluation does not run the following phases. The
exact cohort used to plan accompanies the finished plan across document apply.

The [plan contract](prepare_spec.rs) is about **one selection of that verified
cohort**. Select directly from its borrowed fragment descriptors. For each
selected occurrence, move that occurrence's proof and include exactly its
candidate's member hashes. For each excluded occurrence, keep its original
candidate index, together with the separate waiting indices. Duplicates in
the accepted member list are allowed across overlapping selected groups;
that does not duplicate an applied change.

The [schedule contract](schedule_spec.rs) filters that same member sequence
through the applied/rank view: skip applied changes, reject a conflict in
the selected slots before yielding any plan, and sort precisely the distinct
unapplied ranks. The scheduled hashes are exactly the inverse projection of
that order. Completion does not run another selection or interpret a new
verified occurrence list.

[Plan composition](composition.rs) connects these production contracts with
canonical group evaluation. A successful plan's proposed accepted set is
**old accepted history ∪ selected members**, is dependency-closed, and has
an apply list containing every newly applied member exactly once with every
unapplied dependency earlier in the list. It derives the closure from the
actual selector mask and canonical descriptions, rather than assuming the
selected union is closed.

## Boundary

The production kernels check the ownership, ordering, and index translations
above. The composition needs faithful leaf observations of `ChangeGraph`,
`OpSet`, `Authors`, and `AcceptedClock`; the evaluated descriptors must already
be canonical and match their exact verified deliveries. The normalized rank
and slot observations must refer to those same changes and their actual
dependencies. The native adapter is not itself compiled in Verus, so the proof
chain does not yet discharge every one of those premises at
`Incoming::prepare`. The [linked transaction theorem](../incoming_transaction/README.md)
uses this plan proof to derive document effects under the primitive apply and
signing contracts.

Run with the [pinned Verus toolchain](../incoming_topology/README.md):

```sh
verus --edition=2021 --crate-type=lib --cfg automerge_verus --no-cheating \
    rust/verification/incoming_plan/lib.rs
```
