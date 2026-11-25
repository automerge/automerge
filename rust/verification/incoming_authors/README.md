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

## Source precedence

The [checked source choices](../../automerge/src/automerge/signing/incoming/author_checks.rs)
never consult a candidate author when the hash has an applied slot, including
when that slot is authorless. During provisional evaluation a registered
actor uses its history lookup (which can still say unresolved); only an
unregistered actor uses the delivery's hint or, failing that, the batch hint.
The adapter supplies exact registration, applied-slot, and author observations.
See [scoped evidence](../incoming_groups/README.md) for the checked cache choice
and its connection to the original attachment.

Run with the [same toolchain as the ancestry worklist](../incoming_topology/README.md):

```sh
verus --edition=2021 --crate-type=lib --cfg automerge_verus --no-cheating \
    rust/verification/incoming_authors/lib.rs
```
