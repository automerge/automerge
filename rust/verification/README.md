# Incoming signing admission: Verus coverage

**Review the [abstract admission spec](incoming_transaction/spec.rs) first.**
It describes the transaction without implementation types or proof lemmas.
The [checked refinement](incoming_transaction/refinement.rs) links it to the
production transaction and the stage proofs below. The production harnesses
import the actual Rust kernels; there is no Verus dependency in Cargo builds.

| Spec | Checked |
| --- | --- |
| [Intrinsic validation and plan assembly](incoming_intrinsic/README.md) | Member/slot indexing, head/reachability/level checks, actor chains, exact reverse-edge inputs, ordered validation and evaluation, descriptor assembly, selected and blocked plan projections |
| [Incoming ancestry worklist](incoming_topology/README.md) | A duplicate-free dependency order containing every resolvable candidate |
| [Dependency walks](incoming_walk/README.md) | Exact in-group reachability, boundary discovery, and member walks through a supplied causal cut |
| [Authors and actor slots](incoming_authors/README.md) | Whole-pass propagation from exact predecessors, original causes of broken chains, actor-slot conflicts, and author-source precedence |
| [Ready ancestry](incoming_ancestry/README.md) | Exact actor row projection, shared batch/cone conflict guard, and the checked derivation of clock and canonical-fragment premises on a ready head’s own past |
| [Boundary past](boundary_past/README.md) | Exact memo lookup, fill and union on one snapshot, and exact cutoff given the applied-clock contract |
| [Incoming groups](incoming_groups/README.md) | Delivered/canonical equality, per-actor frontiers, scoped provisional/fresh/cache verification and exact proof contents, and exact unapplied rank order |
| [Fragment selection](fragment_selection/README.md) | Exact hash-to-index projection and the greatest boundary-closed subset of verified occurrences |
| [Incoming plan](incoming_plan/README.md) | One tagged candidate cohort and ancestry view; exact selected proofs, members, pending indices, and a dependency-closed apply schedule |
| [Actor clocks](actor_clock/README.md) | Conditional theorems: representing a closed past by maxima, preservation of causal closure, and exact accepted-set extension |
| [Incoming completion](incoming_completion/README.md) | Publication order, exact accepted-clock update, and the post-apply pending policy |
| [Linked transaction](incoming_transaction/README.md) | Exact apply payloads, one plan through apply and completion, and a checked refinement to the abstract admission spec |

## Remaining composition gap

There is now [one checked observational contract](incoming_transaction/README.md)
connecting the prepared plan and production completion under explicit
primitive contracts. This is **not a full source-level proof of**
[`incoming.rs`](../automerge/src/automerge/signing/incoming.rs): Verus does not
yet check that every concrete graph/author/document adapter establishes the
corresponding observation or effect contract. In particular:

- The adapters still have to supply faithful row observations: which changes
  are applied, which provisional authors qualify, which graph/actor-table slots
  identify applied sources, and which changes/tables correspond to each rank.
  The production index and origin decoders now check the corresponding map
  construction, not the accuracy of those graph/author lookups.
- The production actor-row projection and shared batch/cone conflict guard
  now have contracts. A composition theorem derives complete, conflict-free
  actor chains for the ready head’s own past and uses them in the boundary
  cutoff and canonical-descriptor proofs. `incoming.rs` itself is not yet
  checked against all their alignment premises: notably that the same decoded
  `Change`/graph/author views supply those rows, exact slots, canonical walks
  and applied-clock maxima.
- Evidence preparation and settlement now carry one owned scope and check
  every per-frontier operation; prepared keys can be shared but witnesses
  cannot. The leaf graph/author, wire-bucket, and signature-store observations
  still need faithful adapters. Pending deduplication has a checked member-set
  comparison, but still depends on exact head and attachment equality and
  validated candidates. The checked prepare driver now binds the actual
  selector to one exact member/proof/pending projection and one apply order
  on the same cohort. It relies on faithful group evaluation and aligned
  graph/rank observations. The linked transaction theorem now derives exact
  document effects from that plan and the primitive operations; the remaining
  work is discharging those explicit alignment and effect premises for the
  concrete adapters.

Document application, Ed25519, graph/actor queries, and signature
merge/reclamation, readiness, and draft pruning are external operations. The
linked contract does not verify their implementations or treat successful
whole-document admission as one trusted operation.

Run all checks with the [pinned Verus release and Rust toolchain](incoming_topology/README.md),
from the repository root:

```sh
for harness in incoming_intrinsic incoming_topology incoming_walk incoming_authors incoming_ancestry boundary_past incoming_groups fragment_selection incoming_plan actor_clock incoming_completion incoming_transaction; do
    verus --edition=2021 --crate-type=lib --cfg automerge_verus --no-cheating \
        "rust/verification/$harness/lib.rs" || exit
done
```
