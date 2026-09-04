# Revocations Property Tests Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Model-based proptest property tests for `Revocations` in `rust/automerge/src/revocation.rs`, per the spec in `docs/plans/2026-09-04-revocations-property-tests-design.md`.

**Architecture:** A `#[cfg(test)] mod tests` inside `revocation.rs` (following the precedent in `src/author.rs`). A `Harness` drives a real `Revocations` and a naive reference model through generated operation sequences; per-step check functions assert structural invariants and model agreement. Algebraic laws are separate named proptests reusing the same harness.

**Tech Stack:** Rust, proptest ^1.7 (already a dev-dependency of the `automerge` crate — do NOT add dependencies).

## Global Constraints

- **VCS is jujutsu (`jj`), colocated with git.** Commit with `jj commit -m "..."`. There is no staging step. Never use `git commit`. A detached git HEAD is normal — do not "fix" it.
- **Test naming:** spec P-numbers (P1, P2, …) must NOT appear anywhere in test code. Test names describe what they test (e.g. `is_revoked_is_monotonic_in_seq`); each test carries a clear, concise doc comment stating its property.
- **Property failure protocol:** several properties (especially the mask/revocations coupling) are *hypotheses about the code*. If a property fails, do NOT weaken or delete the test to make it pass. Capture proptest's shrunk minimal counterexample, commit any generated `proptest-regressions/` file, and report the failure to the user for a decision. Only compilation errors and harness bugs (model wrong, not code wrong) may be fixed unilaterally.
- **All test code lives in** `rust/automerge/src/revocation.rs` in one `#[cfg(test)] mod tests`.
- **Run tests from `rust/`** (the cargo workspace root): `cargo test -p automerge --lib revocation::`.
- Universe constants (used throughout): `MAX_AUTHORS = 4`, `MAX_ACTORS = 8`, `MAX_SEQ = 8` (clock seed values are drawn from `1..MAX_SEQ`).
- The author→actor mapping is a **partition**: every actor is assigned to exactly one author at creation and never reassigned.

---

### Task 1: Test harness — universe, operations, generators, reference model

**Files:**
- Modify: `rust/automerge/src/revocation.rs` (append a `#[cfg(test)] mod tests` at the bottom)

**Interfaces:**
- Consumes: `Revocations` (same file), `crate::author::Authors`, `crate::clock::SeqClock`, `crate::op_set2::ActorIdx`, `crate::{Author, ChangeHash}`.
- Produces (used by Tasks 2–6, all inside `mod tests`):
  - `const MAX_AUTHORS: usize; const MAX_ACTORS: usize; const MAX_SEQ: u32;`
  - `fn author(n: usize) -> Author<'static>` and `fn hash(n: u8) -> ChangeHash`
  - `fn to_hashes(ns: &[u8]) -> Vec<ChangeHash>`
  - `enum Op { Revoke { author: usize, heads: Vec<u8>, clock_seed: Vec<Option<u32>> }, Unrevoke { author: usize }, InsertActor { pos: usize, author: usize }, RemoveActor { pos: usize }, Recompute, Clear, ExtendPending { hashes: Vec<u8> }, PopPending { hash: u8 } }`
  - `fn gen_clock_seed() -> impl Strategy<Value = Vec<Option<u32>>>`
  - `fn gen_op() -> impl Strategy<Value = Op>` and `fn gen_ops() -> impl Strategy<Value = Vec<Op>>`
  - `fn probe_seqs() -> impl Iterator<Item = u64>` (ascending)
  - `struct Harness { real: Revocations, authors: Authors, positions: Vec<u32>, /* … */ }` with methods `new()`, `apply(&mut self, op: &Op)`, `seq_clock(&self, seed: &[Option<u32>]) -> SeqClock`, `actors_of(&self, author_n: usize) -> Vec<usize>`, `model_mask(&self) -> BTreeMap<u32, Option<NonZeroU32>>` (keyed by stable id), `model_is_revoked(&self, idx: usize, seq: u64) -> bool`, and public fields `positions`, `author_of`, `ever_revoked`, `revoked`
  - `fn reach(ops: &[Op]) -> Harness`
  - `fn assert_same_visible_state(a: &Revocations, b: &Revocations) -> Result<(), TestCaseError>`

- [ ] **Step 1: Append the test module to `revocation.rs`**

```rust
#[cfg(test)]
mod tests {
    use super::Revocations;
    use crate::{
        author::Authors, clock::SeqClock, op_set2::ActorIdx, Author, ChangeHash,
    };
    use proptest::prelude::*;
    use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
    use std::num::NonZeroU32;

    const MAX_AUTHORS: usize = 4;
    const MAX_ACTORS: usize = 8;
    const MAX_SEQ: u32 = 8;

    /// A stable identity for an actor, unaffected by index shifts.
    type StableId = u32;

    fn author(n: usize) -> Author<'static> {
        Author::from(vec![n as u8])
    }

    fn hash(n: u8) -> ChangeHash {
        ChangeHash([n; 32])
    }

    fn to_hashes(ns: &[u8]) -> Vec<ChangeHash> {
        ns.iter().map(|n| hash(*n)).collect()
    }

    /// Seqs probed for every actor after every step: all values around the
    /// generated clock range plus extremes, ascending.
    fn probe_seqs() -> impl Iterator<Item = u64> {
        (0..=(MAX_SEQ as u64 + 1)).chain([u64::MAX])
    }

    /// One operation against the harness. Authors are drawn from a tiny
    /// universe (`0..MAX_AUTHORS`) so sequences frequently revisit the same
    /// author; actor positions and clock values are reduced modulo the live
    /// state at execution time.
    #[derive(Debug, Clone)]
    enum Op {
        Revoke {
            author: usize,
            heads: Vec<u8>,
            clock_seed: Vec<Option<u32>>,
        },
        Unrevoke {
            author: usize,
        },
        /// Insert a new actor (assigned to `author`) at `pos % (len + 1)`,
        /// applying the two-step `revoke_new_actor` protocol atomically:
        /// when the author is already revoked, the new actor immediately
        /// gets a fully-revoking mask entry. No-op at `MAX_ACTORS` actors.
        InsertActor {
            pos: usize,
            author: usize,
        },
        /// Remove the actor at `pos % len`; no-op when no actors exist.
        RemoveActor {
            pos: usize,
        },
        Recompute,
        Clear,
        ExtendPending {
            hashes: Vec<u8>,
        },
        PopPending {
            hash: u8,
        },
    }

    fn gen_clock_seed() -> impl Strategy<Value = Vec<Option<u32>>> {
        proptest::collection::vec(proptest::option::of(1u32..MAX_SEQ), MAX_ACTORS)
    }

    fn gen_op() -> impl Strategy<Value = Op> {
        prop_oneof![
            (
                0..MAX_AUTHORS,
                proptest::collection::vec(any::<u8>(), 1..3),
                gen_clock_seed()
            )
                .prop_map(|(author, heads, clock_seed)| Op::Revoke {
                    author,
                    heads,
                    clock_seed
                }),
            (0..MAX_AUTHORS).prop_map(|author| Op::Unrevoke { author }),
            (any::<usize>(), 0..MAX_AUTHORS)
                .prop_map(|(pos, author)| Op::InsertActor { pos, author }),
            any::<usize>().prop_map(|pos| Op::RemoveActor { pos }),
            Just(Op::Recompute),
            Just(Op::Clear),
            proptest::collection::vec(any::<u8>(), 0..4)
                .prop_map(|hashes| Op::ExtendPending { hashes }),
            any::<u8>().prop_map(|hash| Op::PopPending { hash }),
        ]
    }

    fn gen_ops() -> impl Strategy<Value = Vec<Op>> {
        proptest::collection::vec(gen_op(), 0..40)
    }

    /// Reference model for one revoked author: the heads it was revoked at
    /// and, per actor (by stable id), the captured seq bound. A bound of
    /// `None` means "fully revoked" (the clock at the revocation heads had
    /// no entry for the actor).
    #[derive(Debug, Clone)]
    struct ModelRevocation {
        heads: Vec<ChangeHash>,
        bounds: BTreeMap<StableId, Option<NonZeroU32>>,
    }

    /// Drives a real [`Revocations`] and a naive reference model in
    /// lockstep. Actor identity is tracked with stable ids so checks can
    /// follow an actor across `insert_actor`/`remove_actor` index shifts.
    struct Harness {
        real: Revocations,
        authors: Authors,
        /// Stable id of the actor at each current index.
        positions: Vec<StableId>,
        next_stable: StableId,
        /// Stable id -> author number. Actors are never reassigned
        /// (author->actor is a partition).
        author_of: BTreeMap<StableId, usize>,
        /// Author number -> model revocation record.
        revoked: BTreeMap<usize, ModelRevocation>,
        /// Heads recorded at revoke time -> clock, index-shifted alongside
        /// the actor set; shared by real and model in `Recompute`.
        clock_table: HashMap<Vec<ChangeHash>, SeqClock>,
        /// Model of the pending-revocation set.
        pending: HashSet<ChangeHash>,
        /// Authors revoked at any point during the run.
        ever_revoked: BTreeSet<usize>,
    }

    impl Harness {
        fn new() -> Self {
            Harness {
                real: Revocations::new(),
                authors: Authors::with_actors(0),
                positions: Vec::new(),
                next_stable: 0,
                author_of: BTreeMap::new(),
                revoked: BTreeMap::new(),
                clock_table: HashMap::new(),
                pending: HashSet::new(),
                ever_revoked: BTreeSet::new(),
            }
        }

        /// Current indices of the actors belonging to `author_n`.
        fn actors_of(&self, author_n: usize) -> Vec<usize> {
            self.positions
                .iter()
                .enumerate()
                .filter(|(_, id)| self.author_of.get(*id) == Some(&author_n))
                .map(|(i, _)| i)
                .collect()
        }

        /// Build a clock of the current length from a generated seed.
        fn seq_clock(&self, seed: &[Option<u32>]) -> SeqClock {
            SeqClock(
                (0..self.positions.len())
                    .map(|i| {
                        seed.get(i % MAX_ACTORS)
                            .copied()
                            .flatten()
                            .and_then(NonZeroU32::new)
                    })
                    .collect(),
            )
        }

        /// The model's mask: union over revoked authors of their per-actor
        /// bounds, keyed by stable id. Well-defined because authors never
        /// share actors.
        fn model_mask(&self) -> BTreeMap<StableId, Option<NonZeroU32>> {
            self.revoked
                .values()
                .flat_map(|r| r.bounds.clone())
                .collect()
        }

        fn model_is_revoked(&self, idx: usize, seq: u64) -> bool {
            let id = self.positions[idx];
            match self.model_mask().get(&id) {
                Some(None) => true,
                Some(Some(v)) => seq > v.get() as u64,
                None => false,
            }
        }

        fn apply(&mut self, op: &Op) {
            match op {
                Op::Revoke {
                    author: a,
                    heads,
                    clock_seed,
                } => {
                    let heads = to_hashes(heads);
                    let clock = self.seq_clock(clock_seed);
                    self.clock_table.insert(heads.clone(), clock.clone());
                    self.real
                        .revoke(author(*a), heads.clone(), &clock, &self.authors);
                    let bounds = self
                        .actors_of(*a)
                        .into_iter()
                        .map(|idx| (self.positions[idx], clock.get_for_actor(&idx)))
                        .collect();
                    self.revoked.insert(*a, ModelRevocation { heads, bounds });
                    self.ever_revoked.insert(*a);
                }
                Op::Unrevoke { author: a } => {
                    self.real.unrevoke(&author(*a), &self.authors);
                    self.revoked.remove(a);
                }
                Op::InsertActor { pos, author: a } => {
                    if self.positions.len() >= MAX_ACTORS {
                        return;
                    }
                    let pos = pos % (self.positions.len() + 1);
                    self.real.insert_actor(pos);
                    self.authors.insert_actor(pos);
                    self.authors.assign_author(author(*a), pos);
                    for clock in self.clock_table.values_mut() {
                        clock.rewrite_with_new_actor(pos);
                    }
                    let id = self.next_stable;
                    self.next_stable += 1;
                    self.positions.insert(pos, id);
                    self.author_of.insert(id, *a);
                    // Two-step `revoke_new_actor` protocol, applied
                    // atomically: a new actor of an already-revoked author
                    // has no entry at the revocation-heads clock, so it is
                    // fully revoked.
                    if let Some(rec) = self.revoked.get_mut(a) {
                        self.real.insert_mask_for(ActorIdx::from(pos), None);
                        rec.bounds.insert(id, None);
                    }
                }
                Op::RemoveActor { pos } => {
                    if self.positions.is_empty() {
                        return;
                    }
                    let pos = pos % self.positions.len();
                    self.real.remove_actor(pos);
                    self.authors.remove_actor(pos);
                    for clock in self.clock_table.values_mut() {
                        clock.remove_actor(pos);
                    }
                    let id = self.positions.remove(pos);
                    self.author_of.remove(&id);
                    for rec in self.revoked.values_mut() {
                        rec.bounds.remove(&id);
                    }
                }
                Op::Recompute => {
                    let table = self.clock_table.clone();
                    let len = self.positions.len();
                    self.real.recompute_revocations(&self.authors, |heads| {
                        table
                            .get(heads)
                            .cloned()
                            .unwrap_or_else(|| SeqClock::new(len))
                    });
                    for (a, rec) in self.revoked.iter_mut() {
                        let clock = table
                            .get(&rec.heads)
                            .cloned()
                            .unwrap_or_else(|| SeqClock::new(len));
                        for (idx, id) in self.positions.iter().enumerate() {
                            if self.author_of.get(id) == Some(a) {
                                rec.bounds.insert(*id, clock.get_for_actor(&idx));
                            }
                        }
                    }
                }
                Op::Clear => {
                    self.real.clear();
                    self.revoked.clear();
                    self.pending.clear();
                }
                Op::ExtendPending { hashes } => {
                    let hs = to_hashes(hashes);
                    self.real.extend_pending_revocations(hs.iter().copied());
                    self.pending.extend(hs);
                }
                Op::PopPending { hash: h } => {
                    let real = self.real.pop_pending_revocation(&hash(*h));
                    let model = self.pending.remove(&hash(*h));
                    assert_eq!(
                        real, model,
                        "pop_pending_revocation({h}) disagrees with the model"
                    );
                }
            }
        }
    }

    /// Build a harness in a reachable state by applying `ops`.
    fn reach(ops: &[Op]) -> Harness {
        let mut h = Harness::new();
        for op in ops {
            h.apply(op);
        }
        h
    }

    /// Two `Revocations` agree on their externally visible revocation
    /// state. The pending set has no accessor and is intentionally not
    /// compared here; it is covered by its own tests.
    fn assert_same_visible_state(
        a: &Revocations,
        b: &Revocations,
    ) -> Result<(), TestCaseError> {
        prop_assert_eq!(a.get_revocations(), b.get_revocations());
        prop_assert_eq!(a.get_revocation_mask(), b.get_revocation_mask());
        Ok(())
    }

    /// Hand-written sequence exercising the harness end to end: two
    /// authors, one revocation with a bounded clock, then unrevoke.
    #[test]
    fn harness_smoke_hand_written_sequence() {
        let mut h = Harness::new();
        h.apply(&Op::InsertActor { pos: 0, author: 0 });
        h.apply(&Op::InsertActor { pos: 1, author: 1 });
        h.apply(&Op::Revoke {
            author: 0,
            heads: vec![1],
            clock_seed: vec![Some(3), Some(2)],
        });
        // Actor 0 (author 0) is revoked strictly past seq 3.
        assert!(!h.real.is_revoked(ActorIdx::from(0usize), 3));
        assert!(h.real.is_revoked(ActorIdx::from(0usize), 4));
        assert_eq!(h.model_is_revoked(0, 3), false);
        assert_eq!(h.model_is_revoked(0, 4), true);
        // Actor 1 (author 1) is untouched.
        assert!(!h.real.is_revoked(ActorIdx::from(1usize), u64::MAX));
        assert!(!h.real.is_empty());
        h.apply(&Op::Unrevoke { author: 0 });
        assert!(h.real.is_empty());
        assert!(!h.real.is_revoked(ActorIdx::from(0usize), 4));
    }
}
```

- [ ] **Step 2: Run the smoke test**

Run (from `rust/`): `cargo test -p automerge --lib revocation::`
Expected: compiles; `harness_smoke_hand_written_sequence` PASSES. If it fails, first suspect the harness (e.g. a borrow or modular-arithmetic mistake), not `Revocations`; fix the harness. If `Revocations` itself appears wrong, follow the property failure protocol.

- [ ] **Step 3: Commit**

```bash
jj commit -m "test: add model-based harness for Revocations property tests"
```

---

### Task 2: Structural invariant sequence tests

**Files:**
- Modify: `rust/automerge/src/revocation.rs` (inside `mod tests`)

**Interfaces:**
- Consumes: `Harness` (fields `real`, `positions`, `revoked`; methods `apply`, `model_mask`), `gen_ops()`, `ActorIdx`, from Task 1.
- Produces: `impl Harness { fn check_mask_matches_model(&self) -> Result<(), TestCaseError>; fn check_is_empty_consistency(&self) -> Result<(), TestCaseError>; }` (reused by Task 6).

- [ ] **Step 1: Add check methods and the two proptests**

Add to `impl Harness`:

```rust
        /// The real mask must contain exactly one entry per model-mask
        /// entry, at the correctly remapped index, with the same bound.
        /// Equality of the maps also proves uniqueness: no entries have
        /// collided onto one index or been lost across shifts.
        fn check_mask_matches_model(&self) -> Result<(), TestCaseError> {
            let expected: BTreeMap<u32, Option<NonZeroU32>> = self
                .model_mask()
                .iter()
                .filter_map(|(id, bound)| {
                    let idx = self.positions.iter().position(|p| p == id)?;
                    Some((idx as u32, *bound))
                })
                .collect();
            let actual: BTreeMap<u32, Option<NonZeroU32>> = self
                .real
                .get_revocation_mask()
                .iter()
                .map(|(a, v)| (a.0, *v))
                .collect();
            prop_assert_eq!(actual, expected);
            Ok(())
        }

        /// `is_empty()` agrees with "no author is revoked", and an empty
        /// state has an empty mask — `active_revocation_clock` relies on
        /// `is_empty()` to skip filtering, so a stale mask entry while
        /// `is_empty()` is true would silently disable revocation.
        fn check_is_empty_consistency(&self) -> Result<(), TestCaseError> {
            prop_assert_eq!(self.real.is_empty(), self.revoked.is_empty());
            if self.real.is_empty() {
                prop_assert!(
                    self.real.get_revocation_mask().is_empty(),
                    "is_empty() is true but the revocation mask is not empty"
                );
            }
            Ok(())
        }
```

Add inside a `proptest! { ... }` block in `mod tests`:

```rust
        /// After every operation, the mask's keys are exactly the actors
        /// of currently revoked authors, each mapped to the bound captured
        /// at revocation time. Tests the mask/revocations coupling
        /// hypothesis; a failure here is a finding to report, not a test
        /// to fix.
        #[test]
        fn mask_keys_match_actors_of_revoked_authors(ops in gen_ops()) {
            let mut h = Harness::new();
            for op in &ops {
                h.apply(op);
                h.check_mask_matches_model()?;
            }
        }

        /// After every operation, `is_empty()` means no author is revoked,
        /// and implies the mask is empty.
        #[test]
        fn is_empty_agrees_with_revocations_and_mask(ops in gen_ops()) {
            let mut h = Harness::new();
            for op in &ops {
                h.apply(op);
                h.check_is_empty_consistency()?;
            }
        }
```

- [ ] **Step 2: Run**

Run (from `rust/`): `cargo test -p automerge --lib revocation::`
Expected: PASS — or a shrunk counterexample sequence. On failure: verify by hand whether the model or the code is wrong; if the code, follow the property failure protocol (commit the `proptest-regressions/` file, report, stop this task).

- [ ] **Step 3: Commit**

```bash
jj commit -m "test: structural invariants for Revocations (mask coupling, is_empty)"
```

---

### Task 3: `is_revoked` semantics tests

**Files:**
- Modify: `rust/automerge/src/revocation.rs` (inside `mod tests`)

**Interfaces:**
- Consumes: `Harness` (fields `real`, `positions`, `author_of`, `ever_revoked`; methods `apply`, `model_is_revoked`), `gen_ops()`, `probe_seqs()`, `ActorIdx`, from Task 1.
- Produces: nothing consumed by later tasks.

- [ ] **Step 1: Add the four proptests** (inside a `proptest!` block)

```rust
        /// `is_revoked` agrees with the reference model for every live
        /// actor and every probed seq, after every operation.
        #[test]
        fn is_revoked_matches_reference_model(ops in gen_ops()) {
            let mut h = Harness::new();
            for op in &ops {
                h.apply(op);
                for idx in 0..h.positions.len() {
                    for seq in probe_seqs() {
                        prop_assert_eq!(
                            h.real.is_revoked(ActorIdx::from(idx), seq),
                            h.model_is_revoked(idx, seq),
                            "divergence at actor index {} seq {}", idx, seq
                        );
                    }
                }
            }
        }

        /// Once an actor is revoked at some seq, it is revoked at every
        /// greater seq.
        #[test]
        fn is_revoked_is_monotonic_in_seq(ops in gen_ops()) {
            let mut h = Harness::new();
            for op in &ops {
                h.apply(op);
                for idx in 0..h.positions.len() {
                    let mut seen_revoked = false;
                    for seq in probe_seqs() {
                        let r = h.real.is_revoked(ActorIdx::from(idx), seq);
                        prop_assert!(
                            r || !seen_revoked,
                            "revocation not monotonic at actor index {} seq {}",
                            idx, seq
                        );
                        seen_revoked = seen_revoked || r;
                    }
                }
            }
        }

        /// The mask entry determines `is_revoked` exactly: no entry means
        /// never revoked; a `None` bound means revoked at every seq; a
        /// bound `v` means revoked iff `seq > v`.
        #[test]
        fn mask_boundary_is_exact(ops in gen_ops()) {
            let mut h = Harness::new();
            for op in &ops {
                h.apply(op);
                for idx in 0..h.positions.len() {
                    let mask = h.real.get_mask_for(&ActorIdx::from(idx)).copied();
                    for seq in probe_seqs() {
                        let expected = match mask {
                            None => false,
                            Some(None) => true,
                            Some(Some(v)) => seq > v.get() as u64,
                        };
                        prop_assert_eq!(
                            h.real.is_revoked(ActorIdx::from(idx), seq),
                            expected,
                            "boundary mismatch at actor index {} seq {}", idx, seq
                        );
                    }
                }
            }
        }

        /// Safety: an actor whose author is never revoked during the run
        /// is never reported revoked, at any seq, at any point.
        #[test]
        fn actors_of_never_revoked_authors_are_never_revoked(ops in gen_ops()) {
            let mut h = Harness::new();
            for op in &ops {
                h.apply(op);
                for (idx, id) in h.positions.iter().enumerate() {
                    if !h.ever_revoked.contains(&h.author_of[id]) {
                        for seq in probe_seqs() {
                            prop_assert!(
                                !h.real.is_revoked(ActorIdx::from(idx), seq),
                                "actor index {} of never-revoked author {} \
                                 reported revoked at seq {}",
                                idx, h.author_of[id], seq
                            );
                        }
                    }
                }
            }
        }
```

- [ ] **Step 2: Run**

Run (from `rust/`): `cargo test -p automerge --lib revocation::`
Expected: PASS, or shrunk counterexample — apply the property failure protocol.

- [ ] **Step 3: Commit**

```bash
jj commit -m "test: is_revoked ordering and safety properties for Revocations"
```

---

### Task 4: Algebraic laws

**Files:**
- Modify: `rust/automerge/src/revocation.rs` (inside `mod tests`)

**Interfaces:**
- Consumes: `Harness` (fields `real`, `authors`; methods `apply`, `seq_clock`), `reach`, `gen_ops`, `gen_clock_seed`, `assert_same_visible_state`, `author`, `to_hashes`, `Op`, from Task 1.
- Produces: nothing consumed by later tasks.

- [ ] **Step 1: Add the five law proptests** (inside a `proptest!` block)

```rust
        /// For an author that is not revoked, revoke followed by unrevoke
        /// restores the prior visible state.
        #[test]
        fn revoke_then_unrevoke_restores_prior_state(
            ops in gen_ops(),
            a in 0..MAX_AUTHORS,
            heads in proptest::collection::vec(any::<u8>(), 1..3),
            seed in gen_clock_seed(),
        ) {
            let mut h = reach(&ops);
            h.apply(&Op::Unrevoke { author: a });
            let before = h.real.clone();
            h.apply(&Op::Revoke { author: a, heads, clock_seed: seed });
            h.apply(&Op::Unrevoke { author: a });
            assert_same_visible_state(&h.real, &before)?;
        }

        /// Revoking with identical arguments twice equals revoking once.
        #[test]
        fn revoke_twice_with_same_args_equals_once(
            ops in gen_ops(),
            a in 0..MAX_AUTHORS,
            heads in proptest::collection::vec(any::<u8>(), 1..3),
            seed in gen_clock_seed(),
        ) {
            let mut h = reach(&ops);
            h.apply(&Op::Revoke {
                author: a,
                heads: heads.clone(),
                clock_seed: seed.clone(),
            });
            let once = h.real.clone();
            h.apply(&Op::Revoke { author: a, heads, clock_seed: seed });
            assert_same_visible_state(&h.real, &once)?;
        }

        /// Two revocations of the same author: the second alone determines
        /// the state (last write wins).
        #[test]
        fn second_revoke_of_same_author_wins(
            ops in gen_ops(),
            a in 0..MAX_AUTHORS,
            heads1 in proptest::collection::vec(any::<u8>(), 1..3),
            seed1 in gen_clock_seed(),
            heads2 in proptest::collection::vec(any::<u8>(), 1..3),
            seed2 in gen_clock_seed(),
        ) {
            let h = reach(&ops);
            let clock1 = h.seq_clock(&seed1);
            let clock2 = h.seq_clock(&seed2);
            let mut both = h.real.clone();
            both.revoke(author(a), to_hashes(&heads1), &clock1, &h.authors);
            both.revoke(author(a), to_hashes(&heads2), &clock2, &h.authors);
            let mut second_only = h.real.clone();
            second_only.revoke(author(a), to_hashes(&heads2), &clock2, &h.authors);
            assert_same_visible_state(&both, &second_only)?;
        }

        /// Revocations of distinct authors commute. Sound because the
        /// author->actor mapping is a partition: distinct authors never
        /// share an actor.
        #[test]
        fn revocations_of_distinct_authors_commute(
            ops in gen_ops(),
            a in 0..MAX_AUTHORS,
            b in 0..MAX_AUTHORS,
            heads_a in proptest::collection::vec(any::<u8>(), 1..3),
            seed_a in gen_clock_seed(),
            heads_b in proptest::collection::vec(any::<u8>(), 1..3),
            seed_b in gen_clock_seed(),
        ) {
            prop_assume!(a != b);
            let h = reach(&ops);
            let ca = h.seq_clock(&seed_a);
            let cb = h.seq_clock(&seed_b);
            let mut ab = h.real.clone();
            ab.revoke(author(a), to_hashes(&heads_a), &ca, &h.authors);
            ab.revoke(author(b), to_hashes(&heads_b), &cb, &h.authors);
            let mut ba = h.real.clone();
            ba.revoke(author(b), to_hashes(&heads_b), &cb, &h.authors);
            ba.revoke(author(a), to_hashes(&heads_a), &ca, &h.authors);
            assert_same_visible_state(&ab, &ba)?;
        }

        /// Unrevoking an author that is not revoked changes nothing.
        #[test]
        fn unrevoke_of_unrevoked_author_is_noop(
            ops in gen_ops(),
            a in 0..MAX_AUTHORS,
        ) {
            let mut h = reach(&ops);
            h.apply(&Op::Unrevoke { author: a });
            let before = h.real.clone();
            h.real.unrevoke(&author(a), &h.authors);
            assert_same_visible_state(&h.real, &before)?;
        }
```

- [ ] **Step 2: Run**

Run (from `rust/`): `cargo test -p automerge --lib revocation::`
Expected: PASS, or shrunk counterexample — apply the property failure protocol. Note: `second_revoke_of_same_author_wins` pins down *intended* semantics; a failure is a design finding for the user, not automatically a bug.

- [ ] **Step 3: Commit**

```bash
jj commit -m "test: algebraic laws for Revocations revoke/unrevoke"
```

---

### Task 5: Actor index shifting laws

**Files:**
- Modify: `rust/automerge/src/revocation.rs` (inside `mod tests`)

**Interfaces:**
- Consumes: `Harness` (fields `real`, `positions`; methods `apply`), `reach`, `gen_ops`, `probe_seqs`, `Op`, `StableId`, `ActorIdx`, from Task 1.
- Produces: nothing consumed by later tasks.

- [ ] **Step 1: Add the two shifting proptests** (inside a `proptest!` block)

```rust
        /// `remove_actor(i)` directly after `insert_actor(i)` leaves the
        /// mask unchanged (raw calls, without the new-actor protocol).
        #[test]
        fn remove_after_insert_is_mask_identity(
            ops in gen_ops(),
            pos in any::<usize>(),
        ) {
            let h = reach(&ops);
            let mut r = h.real.clone();
            let before = r.get_revocation_mask().clone();
            let pos = pos % (h.positions.len() + 1);
            r.insert_actor(pos);
            r.remove_actor(pos);
            prop_assert_eq!(r.get_revocation_mask(), &before);
        }

        /// Index shifts preserve every surviving actor's `is_revoked`
        /// answers: entries move to the remapped index with their value
        /// intact. Actors are tracked across the shift by stable id.
        #[test]
        fn shifts_preserve_is_revoked_per_actor(
            ops in gen_ops(),
            pos in any::<usize>(),
            insert in any::<bool>(),
        ) {
            let mut h = reach(&ops);
            let before: Vec<(StableId, Vec<bool>)> = h
                .positions
                .iter()
                .enumerate()
                .map(|(idx, id)| {
                    let answers = probe_seqs()
                        .map(|s| h.real.is_revoked(ActorIdx::from(idx), s))
                        .collect();
                    (*id, answers)
                })
                .collect();
            if insert {
                h.apply(&Op::InsertActor { pos, author: 0 });
            } else {
                h.apply(&Op::RemoveActor { pos });
            }
            for (id, answers) in before {
                if let Some(idx) = h.positions.iter().position(|p| *p == id) {
                    let after: Vec<bool> = probe_seqs()
                        .map(|s| h.real.is_revoked(ActorIdx::from(idx), s))
                        .collect();
                    prop_assert_eq!(
                        after, answers,
                        "answers changed for surviving actor now at index {}", idx
                    );
                }
            }
        }
```

- [ ] **Step 2: Run**

Run (from `rust/`): `cargo test -p automerge --lib revocation::`
Expected: PASS, or shrunk counterexample — apply the property failure protocol.

- [ ] **Step 3: Commit**

```bash
jj commit -m "test: actor index shifting preserves Revocations mask semantics"
```

---

### Task 6: Recompute, pending, and clear

**Files:**
- Modify: `rust/automerge/src/revocation.rs` (inside `mod tests`)

**Interfaces:**
- Consumes: `Harness` (fields `real`, `positions`; methods `apply`, `check_mask_matches_model` from Task 2), `reach`, `gen_ops`, `probe_seqs`, `assert_same_visible_state`, `hash`, `Op`, `Revocations`, `ActorIdx`, from Tasks 1–2.
- Produces: nothing consumed by later tasks.

- [ ] **Step 1: Add the three proptests** (inside a `proptest!` block)

```rust
        /// `recompute_revocations` is a fixpoint: running it twice with
        /// the same clocks equals running it once, and afterwards the mask
        /// is exactly the mask derived from (revocations, clocks).
        #[test]
        fn recompute_is_a_fixpoint(ops in gen_ops()) {
            let mut h = reach(&ops);
            h.apply(&Op::Recompute);
            let once = h.real.clone();
            h.apply(&Op::Recompute);
            assert_same_visible_state(&h.real, &once)?;
            h.check_mask_matches_model()?;
        }

        /// The pending set has set semantics: each distinct extended hash
        /// pops `true` exactly once and `false` thereafter; duplicates in
        /// the input collapse; unknown hashes pop `false`.
        #[test]
        fn pending_revocations_behave_as_a_set(
            hashes in proptest::collection::vec(any::<u8>(), 0..8),
            unknown in any::<u8>(),
        ) {
            let mut r = Revocations::new();
            r.extend_pending_revocations(hashes.iter().map(|h| hash(*h)));
            let distinct: HashSet<u8> = hashes.iter().copied().collect();
            for h in &distinct {
                prop_assert!(r.pop_pending_revocation(&hash(*h)));
                prop_assert!(!r.pop_pending_revocation(&hash(*h)));
            }
            if !distinct.contains(&unknown) {
                prop_assert!(!r.pop_pending_revocation(&hash(unknown)));
            }
        }

        /// After `clear()`: empty revocations, empty mask, empty pending
        /// set, and no actor is revoked at any seq.
        #[test]
        fn clear_resets_all_state(
            ops in gen_ops(),
            pending in proptest::collection::vec(any::<u8>(), 0..4),
        ) {
            let mut h = reach(&ops);
            h.apply(&Op::ExtendPending { hashes: pending.clone() });
            h.apply(&Op::Clear);
            prop_assert!(h.real.is_empty());
            prop_assert!(h.real.get_revocation_mask().is_empty());
            for idx in 0..h.positions.len() {
                for seq in probe_seqs() {
                    prop_assert!(!h.real.is_revoked(ActorIdx::from(idx), seq));
                }
            }
            for p in pending {
                prop_assert!(!h.real.pop_pending_revocation(&hash(p)));
            }
        }
```

- [ ] **Step 2: Run the full module one last time**

Run (from `rust/`): `cargo test -p automerge --lib revocation::`
Expected: all tests PASS (or documented, reported findings per the failure protocol). Also run `cargo clippy -p automerge --tests` if clippy is part of the normal workflow and fix any warnings in the new test module.

- [ ] **Step 3: Commit**

```bash
jj commit -m "test: recompute fixpoint, pending-set, and clear properties for Revocations"
```
