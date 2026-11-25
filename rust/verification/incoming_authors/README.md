# Incoming authors and actor slots

## Propagation

[chain_spec.rs](chain_spec.rs) specifies the [production propagation pass](../../automerge/src/automerge/signing/incoming/actor_chains.rs).
Every resolved change is classified exactly once:

- If a direct dependency is broken, inherit the first such dependency's
  cause, even if a matching predecessor was seen earlier. The cause eventually
  traces back to the original local failure.
- Otherwise, sequence one starts a new origin unless the actor already has
  applied history. A later change takes its origin from the first direct
  predecessor with the same actor and sequence exactly one less. If there is
  no matching predecessor, this change is itself broken.

The pass returns origin IDs rather than copying keys through the proof. An
origin can legitimately have no author; that is different from a broken chain.
The [adapter](../../automerge/src/automerge/signing/incoming.rs) translates
resolved candidates and their dependencies to topological ranks or applied
actor slots. [The checked materializer](projection_spec.rs) writes the
origin’s author or the exact cause hash under the corresponding resolved
hash. An authorless chain has a present `None` entry, not a missing entry.
It trusts the graph/actor metadata and the read-only lookup of the actual
author for each Initial root or Applied slot supplied by the adapter.

## Equivocation

[slot_spec.rs](slot_spec.rs) says that the [conflict scan](../../automerge/src/automerge/signing/incoming/actor_slots.rs)
returns no conflict exactly when all distinct unapplied candidates have
unoccupied and pairwise distinct `(actor, seq)` slots. Otherwise it reports
a hash witnessing the conflict. The proof also shows why a conflict-free
batch needs no per-ancestry recheck: every subset is conflict-free. A
conflict elsewhere in the batch does **not** invalidate an unrelated ancestry.
The caller runs the scan over the unique batch hashes, over the verified walk
of the relevant ancestry, and over the selected unapplied union. The proof
trusts those projections and that an applied-slot query uses the same stable
actor/sequence snapshot in all three cases.

## Cached proof reuse

[spec.rs](spec.rs) specifies when the [cache check](../../automerge/src/automerge/signing/incoming/author_checks.rs)
permits reusing evidence: every required frontier must have an established
author and a cached witness under that exact author. Missing authors don't
match. This assumes accurate, stable lookups, concrete identity equality, and
that the cache lookup is scoped to this delivery's verified store and frontier
hash. It does not prove construction of that store, the frontier calculation,
or cryptography.

Run with the [same toolchain as the ancestry worklist](../incoming_topology/README.md):

```sh
verus --edition=2021 --crate-type=lib --cfg automerge_verus --no-cheating \
    rust/verification/incoming_authors/lib.rs
```
