# Incoming ancestry worklist

This verifies the topological worklist used by signing-mode incoming admission,
not all of `incoming.rs`. The reviewer-facing contract is in [spec.rs](spec.rs).
[proof.rs](proof.rs) supplies lemmas for the loop invariants in the
[production worklist](../../automerge/src/automerge/signing/incoming/topology.rs).
The Verus harness imports that file directly. Normal Cargo builds run the same
executable statements, with no Verus dependency.

## Contract

A candidate is **ready after** a set of candidates if it has no dependency
outside the applied graph and the full candidate set, and all of its candidate
dependencies are in the given set. The worklist returns a sequence such that:

- Each entry is a distinct candidate and is ready after the preceding entries.
- At the end no omitted candidate is ready. In fact, every candidate in *any*
  valid dependency order appears in the result; the proof derives this by
  induction along that other order.

This doesn't specify how ties are broken. It doesn't require the candidate
input to be a DAG: cycles and anything depending on one stay unresolved, as do
missing dependencies and their descendants. Repeated dependency entries count
as separate edges for the worklist but don't affect readiness.

## Trusted boundary

The caller, [`topological_sort_unapplied`](../../automerge/src/automerge/signing/incoming.rs),
builds the indexed view and maps the resulting IDs back to hashes and ranks.
That adapter is not verified here. To interpret the result as a statement
about the document, review that:

1. The IDs are a one-to-one enumeration of the unapplied candidate hashes.
2. An applied dependency contributes nothing. Every other dependency occurrence
   increments the child's initial counter; one reverse edge is recorded if and
   only if its parent is another candidate. The counter's excess over the
   number of reverse edges is therefore exactly the number of missing occurrences.
3. Mapping IDs back preserves order; the ranks are their positions in that order.

The proof uses Verus's standard collection specifications. Author propagation,
canonical fragment derivation, group proofs, selection, and publication are
outside this proof. In particular, the returned order is about **applied**
ancestry, not about which changes are signed or accepted.

## Run

With [Verus release `0.2026.11.22.1ce6a59`](https://github.com/verus-lang/verus/releases/tag/release%2F0.2026.11.22.1ce6a59)
and its Rust toolchain (`1.100.0`) installed, run from the repository root:

```sh
verus --edition=2021 --crate-type=lib --cfg automerge_verus --no-cheating \
    rust/verification/incoming_topology/lib.rs
```

`--no-cheating` rejects proof shortcuts such as `assume` and `external_body` in
the proof. The standalone harness does not change the main crate's toolchain.
