# Incoming completion and pending deliveries

## Transaction and publication order

[publication_spec.rs](publication_spec.rs) specifies the [production driver](../../automerge/src/automerge/signing/incoming/publication.rs).
A preparation error skips both application and completion. An application
error skips completion. On success, completion merges exactly the selected
proof indices in plan order, then includes the selected member list, then
reclaims evidence and removes published drafts. Pending is rebuilt afterward
against the resulting post-apply, post-publication state.

These are ordering and selection guarantees. The adapter in
[`signing.rs`](../../automerge/src/automerge/signing.rs) must plan without
mutating the document and apply the prepared schedule without mutating it on
error. On successful apply it must supply a fresh, aligned actor/graph view.
The [completion adapter](../../automerge/src/automerge/signing/incoming.rs)
projects the plan to the exact group proof and selected member list. The
behavior of merging evidence and readiness, reclaiming covered records, and
pruning drafts is still an external contract; the scheduling proof does not
verify those implementations.

## Exact accepted-clock updates

[clock_spec.rs](clock_spec.rs) specifies the [production slot fold](../../automerge/src/clock/seq_slots.rs):
for each actor, the updated slot is the maximum of the old slot and the
selected sequences for that actor. Nothing else can raise a slot. The
change-graph projection must return the actual post-apply actor slot and
positive sequence for each selected hash.

[exact_publication.rs](exact_publication.rs) connects that checked fold to the
[canonical closure theorem](../actor_clock/README.md). If the selected
members, canonical fragments, and history satisfy the upstream contracts,
reading the new clock accepts exactly **old accepted history ∪ selected
members**. In particular it cannot silently accept an unselected change by
advancing an actor maximum. The composition assumes an exact plan projection,
consistent actors after apply, and the pre-existing clock's exactness.

## Pending queue

[pending_spec.rs](pending_spec.rs) specifies the [production queue policy](../../automerge/src/automerge/signing/incoming/pending.rs):

- An invalid new delivery is an error. An old pending delivery disproved by
  newly established history is dropped and does not abort the batch.
- Recheck only the plan's remaining occurrences. Keep a delivery viable only
  if some member is unpublished, no unapplied member conflicts with an
  applied actor slot, and all frontiers with now-established authors have
  valid evidence in that delivery's own attachment. A frontier whose actor
  has no applied history does not yet cause failure.
- Keep the **first viable copy** of each equivalent delivery, in input
  order. An ineligible or disproved copy cannot hide a viable copy. Different
  attachments on identical content are not duplicates.

The reviewed adapters supply the remaining candidate indices, the post-apply
member/author/attachment queries, and delivery equality. Equality must be
stable on the intrinsically validated candidates and compare the head,
member hash set, and raw attached entries. It intentionally ignores the
preverification cache. The harness does not prove those graph lookups or
that every upstream plan projection is faithful.

Run with the [same toolchain as the ancestry worklist](../incoming_topology/README.md):

```sh
verus --edition=2021 --crate-type=lib --cfg automerge_verus --no-cheating \
    rust/verification/incoming_completion/lib.rs
```
