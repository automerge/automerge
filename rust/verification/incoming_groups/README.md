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

The [`IncomingProof` wrapper](../../automerge/src/signature/fragment.rs)
owns the exact frontier list, the raw attachment, and the provisional store
created from those two inputs. No caller can supply an arbitrary store at
settlement time. [The checked scope constructor and settlement policy](cache_spec.rs)
keep those inputs together, and the fallback calls the checked fresh path
with that same attachment, frontier list, and established-author resolver.

- [Provisional policy](provisional_spec.rs) requires a typed attached signature
  at every frontier. It may defer a frontier with an unknown author, but a
  known authorless frontier is an error. The prepared-key map can be shared
  between deliveries; the proof checks that every entry was decoded under
  its key's exact author and that sharing does not share witnesses.
- [Fresh verification](scoped_spec.rs) checks the actual per-frontier calls in
  order: read that hash's bucket, resolve its author, decode that key, search
  that bucket with the exact hash and key, then retain that witness. Failure
  stops at the first failed obligation and returns no partial store.
- [Evidence theorems](provisional_soundness.rs), [fresh soundness](scoped_soundness.rs),
  and [settlement soundness](cache_spec.rs) compose those drivers with the
  leaf contracts below. Early evidence is exact for the usable provisional
  authors. A successful settled proof contains **exactly** the frontier
  witnesses, each verified for its established author and drawn from that
  delivery's own attachment, whether the cache was reused or not.

**Trusted leaves.** The raw hash-bucket projection must return exactly this
attachment's typed entries for the given hash and preserve raw presence for
error ordering. The author lookups must return the actual provisional or
established authors, respectively. Successful Ed25519 decoding/checking
must bind the precise author, hash, and signature arguments; retention must
insert a fresh witness under its hash without changing other records; store
reads and clone must preserve those records. These are individual operations,
not assumptions that a frontier or whole group has already been authenticated.
The production loops and the constructor/choice are checked; the native
`HashEntries`, graph/author lookup, crypto, and `SignatureStore` implementations
remain external. The full group-evaluation call site must still establish the
canonical-member and established-author preconditions.

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
