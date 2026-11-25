# Incoming membership, evidence, and apply order

## Membership and frontiers

The [production membership check](../../automerge/src/automerge/signing/incoming/group_membership.rs)
accepts exactly when the delivered set equals the duplicate-free result of the
canonical walk. Missing, extra, and substituted hashes are all rejected;
presentation order is immaterial. The check does not reconstruct the graph.

[frontier_spec.rs](frontier_spec.rs) specifies [frontier selection](../../automerge/src/signature/fragment/frontiers.rs):
there is exactly one obligation per **actor**, namely a hash of one of that
actor's maximum-sequence members. The proof also shows that distinct member
hashes give distinct obligations. The Rust adapter projects `(actor, seq,
hash)` from the delivered changes, then extracts and sorts the chosen hashes
without changing the set. It does not use the author or available signatures
to determine which hashes need proof.

## Group-local proof evidence

[evidence_spec.rs](evidence_spec.rs) checks the [production evidence loops](../../automerge/src/signature/fragment/evidence.rs)
and their composition:

- The search takes the first successful witness from the projected entries
  for this exact hash. If none can be verified, it yields none.
- Assembly starts with an empty store, checks each required frontier in
  order, and returns no partial proof on the first failure. Early validation
  still requires a supported attached entry for every frontier; it can defer
  verification only where the author isn't yet established. The final pass
  cannot defer any required proof.
- Given the per-frontier contract below, successful assembly contains exactly
  the required evidence for this group, without extra or cross-group entries.
  The cache reuse lemma permits retaining that same store only when all the
  originally checked authors agree with the now-established authors.

**Trusted per-frontier boundary.** `HashEntries::for_hash` projects only this
attachment's entries for the requested hash, in order and within the per-hash
bound; `as_ed25519` filters by scheme and representation. A successful Ed25519
check binds that very entry to its exact hash and the supplied author.
`SignatureStore::insert_ed25519` retains that witness under its hash and
preserves previous witnesses. The [calling code](../../automerge/src/signature/fragment.rs)
supplies the established author and the group's own attachment; in the early
pass the caller supplies the provisional author and prepared key for that same
author. These are individual lookup, crypto, and storage contracts, not an
assumption that any whole group is authenticated. Neither the Ed25519
implementation nor the caller's graph/author projection is verified here.

## Apply-order projection

[order_spec.rs](order_spec.rs) specifies the [production rank projection](../../automerge/src/automerge/signing/incoming/apply_order.rs).
It removes already-applied members, keeps each selected unapplied rank exactly
once even across overlapping groups, and sorts it in the established dependency
order. If every unapplied parent is in the selected set at a lower rank,
the proof shows every such parent comes before its dependent in the output.
The caller is responsible for projecting the exact selected member union and
for the correspondence between rank and change hash. The conflict scan runs
before the sort. This does not verify the document apply, publication, or
pending-state transition.

Run with the [same toolchain as the ancestry worklist](../incoming_topology/README.md):

```sh
verus --edition=2021 --crate-type=lib --cfg automerge_verus --no-cheating \
    rust/verification/incoming_groups/lib.rs
```
