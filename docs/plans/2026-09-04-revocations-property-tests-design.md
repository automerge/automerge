# Property tests for `Revocations` — design

Date: 2026-09-04
Scope: unit-level property tests for `rust/automerge/src/revocation.rs` (`Revocations`).
Framework: proptest (already a dependency of `automerge`).

## Goal

Test the `Revocations` struct in isolation with model-based sequence testing:
generate random operation sequences, maintain a naive reference model alongside,
and assert structural invariants plus model agreement after every step. A small
set of algebraic laws (round-trips, idempotence) is layered on top for readable
failures.

The tests also serve as an experiment: the coupling between `revocations`
(author → heads) and `revocations_mask` (actor → max seq) is *hypothesized* to
be an invariant, not known to be one. If sequences can break it, proptest
produces the minimal counterexample and a human decides whether the code or the
invariant is wrong.

## Architecture

- **Universe:** a fixed small set of authors and actors, with a generated
  author → actors mapping that is a **partition**: each actor belongs to
  exactly one author (author/actor pairs are unique). Clocks are generated as
  small integers and adapted into `SeqClock`; author→actor mappings are adapted
  into `Authors`. If `Authors`/`SeqClock` prove hard to construct in a unit
  test, a thin test harness (test-only constructors) is acceptable.
- **Operation generator:** random sequences over
  `revoke`, `unrevoke`, `insert_actor`, `remove_actor`, `insert_mask_for`
  (modelling the `revoke_new_actor` protocol), `recompute_revocations`,
  `clear`, `extend_pending_revocations`, `pop_pending_revocation`.
  The generator models the two-step new-actor protocol: `insert_actor`
  followed by the corresponding mask insert (see P15 note).
- **Reference model:** e.g. `BTreeMap<Author, (Vec<ChangeHash>, BTreeSet<ActorIdx>)>`
  plus stored clocks; the model's mask is *derived on demand* from revoked
  authors and clocks. `is_revoked` in the model: entry absent → false;
  clock value `None` → true for all seqs; clock value `Some(v)` → `seq > v`.
- **Checking:** after every applied operation, assert the structural invariants
  (P1–P3) and model agreement (P7) over a probe grid of (actor, seq) pairs
  (all actors × seqs around the boundary values plus extremes).
- **Location:** a `#[cfg(test)]` module in `revocation.rs` or a sibling test
  module, following existing test placement in the crate.
- **Naming convention:** the P-numbers below are for this document only and
  must **not** appear in test code. Each test gets a descriptive name stating
  what it tests (e.g. `is_revoked_is_monotonic_in_seq`,
  `mask_keys_match_actors_of_revoked_authors`), and the property is documented
  clearly and concisely in a doc comment on the test itself.

## Properties

### Structural invariants (checked after every step)

- **P1 — Mask/revocations coupling (hypothesis):**
  `mask.keys() == union of actors of all revoked authors`, both directions.
  Expected to be *temporarily false* in the window between `insert_actor` and
  the follow-up mask insert for an already-revoked author (the
  `revoke_new_actor` window); the generator models this protocol and the check
  is suspended only within that window.
- **P2 — `is_empty` consistency:** `is_empty()` ⇔ no author revoked; and
  (dependent on P1) `is_empty()` ⇒ mask empty. Motivation:
  `active_revocation_clock` uses `is_empty()` to skip filtering, so a non-empty
  mask with empty `revocations` silently disables revocation on slow paths.
- **P3 — Uniqueness:** at most one revocation entry per author and one mask
  entry per actor, *preserved across index shifts*: after arbitrary
  `insert_actor`/`remove_actor`, no two entries collide onto one index and none
  are lost (entry count preserved by insert; decremented by at most 1 by
  remove).

### `is_revoked` semantics (ordering)

- **P4 — Monotonicity in seq:** `is_revoked(a, s)` ⇒ `is_revoked(a, s')` for
  all `s' ≥ s`.
- **P5 — Boundary exactness:** mask `Some(v)` → revoked iff `s > v`;
  mask `None`-valued entry → revoked for all `s`; no entry → never revoked.
- **P6 — Innocent bystander (safety):** an actor whose author is never revoked
  during the sequence is never `is_revoked`, at any seq, at any point.
- **P7 — Model agreement:** `is_revoked(a, s)` matches the reference model for
  all probe-grid pairs. Subsumes P4/P5 in principle; P4/P5 are kept separate
  for named, readable failures.

### Algebraic laws

- **P8 — Revoke/unrevoke round-trip:** from any reachable state,
  `revoke(author, …)` then `unrevoke(author)` restores the prior state for that
  author's actors; other entries untouched.
- **P9 — Revoke idempotence:** identical `revoke` twice ≡ once.
- **P10 — Last-write-wins:** two `revoke`s of the same author with different
  heads/clocks ≡ the second alone. This pins down the intended semantics.
- **P11 — Commutativity across authors:** revoking distinct authors in either
  order yields the same state. Sound because author→actor is a partition
  (enforced by the generator), so distinct authors never share actors.
- **P12 — Unrevoke of a non-revoked author is a no-op.**

### Actor index shifting

- **P13 — Insert/remove inverse:** `remove_actor(i)` ∘ `insert_actor(i)` ≡
  identity on the mask.
- **P14 — Value preservation under remap:** after any shift, each surviving
  entry carries the same value at the correctly remapped index; `is_revoked`
  answers are preserved modulo the index permutation.
- **P15 — Remove drops exactly the target:** `remove_actor(i)` removes at most
  the entry at `i` and nothing else.
- Note: `insert_actor` shifts the mask but does not touch `revocations` or
  consult `Authors`; a new actor for an already-revoked author has no mask
  until the `revoke_new_actor`-style insert runs. This is the P1 suspension
  window above.

### Recompute, pending, clear

- **P16 — Recompute is a fixpoint:** `recompute_revocations` twice with the
  same clock function ≡ once; afterwards the mask equals the mask derived
  purely from (revocations, clocks) — the strongest form of P1.
- **P17 — Pending is a set:** `pop_pending_revocation(h)` returns `true`
  exactly once per distinct extended hash, `false` thereafter; duplicate input
  hashes collapse.
- **P18 — Clear is total:** after `clear()`: `is_empty()`, mask empty, pending
  empty, `is_revoked` false for every (actor, seq).

## Priorities

Highest value relative to cost: **P1/P2** (probe the open coupling question),
**P6** (safety), **P7** (model workhorse), **P14** (index arithmetic).
P9, P12, P13 are nearly free once the generator exists.

## Error handling

Not applicable — test-only code. Proptest shrinking provides minimal
counterexample sequences; the check harness should print the failing operation
sequence and the divergent (actor, seq) probe.

## Out of scope

- Integration-level semantics (e.g. "materialized document after revoke equals
  the document built from unrevoked changes only", causal-descendant
  invisibility via real heads/clocks). These need `ChangeGraph`/`Automerge`
  and are a separate follow-up.
- `revoke_new_actor`, `rebuild_revocation_clock`, `active_revocation_clock`
  themselves (they live on `ChangeGraph`); only the `Revocations`-side protocol
  window is modelled.
