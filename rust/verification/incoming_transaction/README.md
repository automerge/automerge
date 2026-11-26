# Linked incoming admission contract

**Start with [the abstract spec](spec.rs).** It has no Automerge types or proof
lemmas. Read `aborts`, `publishes`, and `retains_pending` for the guarantees;
the definitions above them explain canonical fragment selection and legal
apply order. Occurrences distinguish separate deliveries, even when their
contents are equal. The spec uses the post-publication viability test and
compares delivery identity, including the attachment, when retaining pending.

You do not need to read the proofs to review that contract.

## How it is connected to production

[`refinement.rs`](refinement.rs) maps the concrete transaction observations
to the abstract objects in `spec.rs`. Its checked `refines_abstract_spec`
theorem establishes those exact predicates for an abort or a successful
production transaction. It checks the mapping of the selected occurrences,
ordered apply list, proof witnesses, and pending order, not just the equality
of the accepted hash sets. The bridge uses the existing premises and does
not add assumptions about the correctness of selection or completion.

The bridge calls [`linked_document_contract`](linked.rs), which composes
[`plan_bridge.rs`](plan_bridge.rs) and the [production driver's trace](transaction_spec.rs).
The driver uses one owned cohort and plan: it materializes the scheduled
payloads, passes them directly to apply, then opens a fresh signing view and
publishes and rebuilds pending from that same plan.

## Boundary of this result

The theorem assumes the [observed compiler inputs](linked.rs): successful
preparation feeds canonical, correctly aligned group results into the checked
plan projection. The concrete Rust graph, actor, author, and wire-attachment
adapters are still not checked against every such premise. The proof derives
plan safety, causal closure, and exact publication rather than assuming them.

[The primitive contracts](document_spec.rs) specify read-only preparation,
exact payload application (with no document mutation on error), interpretation
of the fresh graph and clock by hash, the individual signing effects, and
setting the pending list. Ed25519, document/CRDT application, graph queries,
and signature-store/readiness/reclamation implementations remain outside the
verified transaction. This is a refinement proof under those contracts, not
an unconditional source-level proof of all of `incoming.rs`.

Run with the [pinned Verus toolchain](../incoming_topology/README.md):

```sh
verus --edition=2021 --crate-type=lib --cfg automerge_verus --no-cheating \
    rust/verification/incoming_transaction/lib.rs
```
