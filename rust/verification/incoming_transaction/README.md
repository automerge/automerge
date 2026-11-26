# Linked incoming admission contract

Start here for the transaction as a whole. The checked
[`linked_document_contract`](linked.rs) composes the plan and execution proofs
into [one observational contract](document_spec.rs) over the
[production driver](../../automerge/src/automerge/signing/incoming/admission_txn.rs).

Given the canonical evaluation result and faithful document operations:

- **On error**, the document is unchanged. No preparation or apply error opens
  a signing view or runs completion. The document observation does not include
  the caller's patch log.
- **On success**, apply exactly the plan's ordered, distinct unapplied changes,
  after their dependencies. The applied hash set and document content are the
  old state extended by that exact schedule. An empty schedule skips apply.
- The accepted set is exactly **old accepted history ∪ selected members**.
  This follows from causal closure of the selected union and the checked
  clock fold on the fresh actor layout. It is not assumed as an effect of
  raising the sequence maxima.
- Merge exactly the selected proof payloads. A retained witness after cleanup
  must come from the old evidence or one of those payloads. Reclamation and
  storage limits can remove or decline to retain evidence; the theorem does
  not claim an exact set union after cleanup.
- Replace pending with **the first viable copy of each still-blocked delivery**
  in original candidate order, using the post-publication view. Install that
  exact list without reselecting or changing attachment identity.

## What is linked

[`plan_bridge.rs`](plan_bridge.rs) derives the safe, closed schedule and the
existence of every scheduled payload from the checked compiler output. The
production transaction then [materializes](payload_spec.rs) those exact
payloads from its owned cohort, using the last occurrence of a repeated hash,
just as before. It passes them directly to apply. On success it opens one
post-apply signing view, [publishes](transaction_spec.rs) the plan's actual
proofs and member list through that view, and rebuilds and installs pending
from the same plan. The driver no longer takes a callback that can implement
arbitrary completion.

## Boundary of this result

The theorem assumes the [observed compiler inputs](linked.rs): the actual
successful preparation has the checked plan projection, fed by canonical,
correctly aligned group results. The concrete Rust graph, actor, author, and
wire-attachment adapters are still not checked against every such premise.
There is no trusted claim that the whole plan is safe or exact: the linked
proof derives those properties from the compiler result and the alignment
facts.

[The primitive contracts](document_spec.rs) specify read-only preparation,
exact payload application (with no document mutation on error), interpretation
of the fresh graph and clock by hash, the individual signing effects, and
setting the pending list. Ed25519, document/CRDT application, actual graph
queries, and the signature-store/readiness/reclamation implementations remain
outside the verified transaction. In particular the theorem is conditional
on those narrow contracts; it is not an unconditional source-level proof of
all of `incoming.rs` or the document implementation.

Run with the [pinned Verus toolchain](../incoming_topology/README.md):

```sh
verus --edition=2021 --crate-type=lib --cfg automerge_verus --no-cheating \
    rust/verification/incoming_transaction/lib.rs
```
