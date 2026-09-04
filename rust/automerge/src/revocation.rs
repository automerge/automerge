use std::{
    collections::{HashMap, HashSet},
    num::NonZeroU32,
};

use crate::{author::Authors, clock::SeqClock, op_set2::ActorIdx, Author, ChangeHash};

/// Record revocations of [`Author`]s in relation to a set of changes.
///
/// Each author is uniquely revoked, and the heads are the inclusive bounds at
/// which the Automerge document will materialize for this author. Any changes
/// after these changes will be considered invisible.
///
/// # Revocation Mask
///
/// A revocation mask is a mapping from each actor to a sequence number. This
/// provides data to build a clock for marking operations as invisible when
/// materializing an Automerge document.
///
/// # Pending Changes
///
/// In some cases, the [`ChangeHash`] might not yet be recorded in the change
/// graph, these can be kept track of by using
/// [`Revocations::extend_pending_revocations`], and removed using
/// [`Revocations::pop_pending_revocation`].
#[derive(Debug, Default, Clone)]
pub(crate) struct Revocations {
    /// Revocations are a map from an [`Author`] to a [`ChangeHash`] heads
    /// boundary. These [`ChangeHash`]es, and any descendants will be made
    /// invisible to the materialized document.
    revocations: HashMap<Author<'static>, Vec<ChangeHash>>,
    /// Mapping from [`ActorIdx`] to maximum sequence number allowed by the
    /// revocations.
    revocations_mask: HashMap<ActorIdx, Option<NonZeroU32>>,
    /// [`ChangeHash`] values that are not witnessed in the graph, but are being
    /// revoked for an author.
    pending_revoke: HashSet<ChangeHash>,
}

impl Revocations {
    /// Initialize the [`Revocations`] beginning with no [`Author`]s being revoked.
    pub(crate) fn new() -> Self {
        Self::default()
    }

    /// Returns `true` if no [`Author`]s have been revoked.
    pub(crate) fn is_empty(&self) -> bool {
        self.revocations.is_empty()
    }

    /// Return at which changes the [`Author`] was revoked at, if any.
    pub(crate) fn get_revocations_for_author(
        &self,
        author: &Author<'static>,
    ) -> Option<&[ChangeHash]> {
        self.revocations.get(author).map(|heads| heads.as_slice())
    }

    /// Return the set of revocations, per [`Author`].
    pub(crate) fn get_revocations(&self) -> &HashMap<Author<'static>, Vec<ChangeHash>> {
        &self.revocations
    }

    /// Revoke the given [`Author`] at the given `heads`.
    ///
    /// [`Authors`] and [`SeqClock`] are used for building the [revocation
    /// mask](Revocations#revocation-mask).
    pub(crate) fn revoke(
        &mut self,
        author: Author<'static>,
        heads: Vec<ChangeHash>,
        clock: &SeqClock,
        authors: &Authors,
    ) {
        for actor in authors.get_actors_for_author(&author) {
            self.revocations_mask
                .insert(actor.into(), clock.get_for_actor(&actor));
        }
        self.revocations.insert(author, heads);
    }

    /// Unrevoke the given [`Author`], so that their changes can be materialized
    /// in the Automerge document again.
    ///
    /// [`Authors`] is used for removing the actors from the [revocation
    /// mask](Revocations#revocation-mask).
    pub(crate) fn unrevoke(&mut self, author: &Author<'static>, authors: &Authors) {
        for a in authors.get_actors_for_author(author) {
            self.revocations_mask.remove(&a.into());
        }
        self.revocations.remove(author);
    }

    /// Returns `true` if the `actor` is revoked at the given sequence number.
    ///
    /// If the recorded mask is `None`, then the actor is revoked regardless of
    /// the sequence number.
    pub(crate) fn is_revoked(&self, actor: ActorIdx, seq: u64) -> bool {
        match self.revocations_mask.get(&actor) {
            Some(Some(v)) if (v.get() as u64) < seq => true,
            Some(None) => true,
            _ => false,
        }
    }

    /// Insert a `mask` for the given `actor`.
    pub(crate) fn insert_mask_for(&mut self, actor: ActorIdx, mask: Option<NonZeroU32>) {
        self.revocations_mask.insert(actor, mask);
    }

    /// Return the mask for the given `actor`.
    pub(crate) fn get_mask_for(&self, actor: &ActorIdx) -> Option<&Option<NonZeroU32>> {
        self.revocations_mask.get(actor)
    }

    /// Return the [revocation mask](Revocations#revocation-mask).
    pub(crate) fn get_revocation_mask(&self) -> &HashMap<ActorIdx, Option<NonZeroU32>> {
        &self.revocations_mask
    }

    /// Insert an `actor` into the [revocation mask](Revocations#revocation-mask).
    pub(crate) fn insert_actor(&mut self, actor: usize) {
        self.revocations_mask = std::mem::take(&mut self.revocations_mask)
            .into_iter()
            .map(|(a, v)| {
                let shifted = if a.0 >= actor as u32 {
                    ActorIdx(a.0 + 1)
                } else {
                    a
                };
                (shifted, v)
            })
            .collect();
    }

    /// Remove an `actor` from the [revocation mask](Revocations#revocation-mask).
    pub(crate) fn remove_actor(&mut self, actor: usize) {
        self.revocations_mask = std::mem::take(&mut self.revocations_mask)
            .into_iter()
            .filter_map(|(a, v)| match a.0.cmp(&(actor as u32)) {
                std::cmp::Ordering::Less => Some((a, v)),
                std::cmp::Ordering::Equal => None,
                std::cmp::Ordering::Greater => Some((ActorIdx(a.0 - 1), v)),
            })
            .collect();
    }

    /// Clear all [`Revocations`] state.
    pub(crate) fn clear(&mut self) {
        self.pending_revoke.clear();
        self.revocations_mask.clear();
        self.revocations.clear();
    }

    /// Recompute the [revocation mask](Revocations#revocation-mask).
    ///
    /// [`Authors`] is used for finding the recorded actors for each author,
    /// while `get_clock` is a callback for calculating the new [`SeqClock`] for
    /// updating the mask entry for each actor.
    pub(crate) fn recompute_revocations(
        &mut self,
        authors: &Authors,
        get_clock: impl Fn(&[ChangeHash]) -> SeqClock,
    ) {
        for (author, heads) in &self.revocations {
            let clock = get_clock(heads);
            for actor in authors.get_actors_for_author(author) {
                self.revocations_mask
                    .insert(actor.into(), clock.get_for_actor(&actor));
            }
        }
    }

    /// Extend the set of pending revocation changes with the given `heads`.
    ///
    /// Once a change has been witnessed, it can be removed using
    /// [`Revocations::pop_pending_revocation`].
    pub(crate) fn extend_pending_revocations(&mut self, heads: impl Iterator<Item = ChangeHash>) {
        self.pending_revoke.extend(heads)
    }

    /// Remove the `head` from the pending revocations, returning `true` if it
    /// existed in the set.
    pub(crate) fn pop_pending_revocation(&mut self, head: &ChangeHash) -> bool {
        self.pending_revoke.remove(head)
    }
}

#[cfg(test)]
mod tests {
    //! Property tests for [`Revocations`].
    //!
    //! These tests use a model-based (lockstep) strategy: a [`Harness`] steps
    //! the production [`Revocations`] and an independent reference model
    //! through a generated sequence of [`Op`]s, and after every step the
    //! properties assert that the two still agree. The reference model is a
    //! deliberately simple restatement of the intended revocation semantics,
    //! written without reference to the production implementation, so a bug
    //! in [`Revocations`] surfaces as a divergence rather than being mirrored
    //! by the model.
    //!
    //! Actors are tracked by [`StableId`] — an identity that, unlike the
    //! production [`ActorIdx`], never renumbers when actors are inserted or
    //! removed. This keeps the model correct across index shifts and is what
    //! lets the tests catch index-arithmetic mistakes in `insert_actor` /
    //! `remove_actor`.
    //!
    //! Each actor belongs to exactly one author (author/actor is a
    //! partition), and the universe of authors, actors, and sequence numbers
    //! is kept deliberately small (see [`MAX_AUTHORS`], [`MAX_ACTORS`],
    //! [`MAX_SEQ`]) so that generated sequences frequently revisit the same
    //! author and exercise collisions, re-revocation, and index shifts.

    use super::Revocations;
    use crate::{author::Authors, clock::SeqClock, op_set2::ActorIdx, Author, ChangeHash};
    use proptest::prelude::*;
    use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
    use std::num::NonZeroU32;

    /// The universe is kept deliberately small so generated [`Op`] sequences
    /// frequently revisit the same author and actor, exercising collisions,
    /// re-revocation, and index shifts rather than spreading thinly over a
    /// large space.
    const MAX_AUTHORS: usize = 4;
    const MAX_ACTORS: usize = 8;
    const MAX_SEQ: u32 = 8;

    /// A stable identity for an actor, unaffected by index shifts.
    type StableId = u32;

    /// Build the [`Author`] for author number `n` (a one-byte identity).
    fn author(n: usize) -> Author<'static> {
        Author::from(vec![n as u8])
    }

    /// Build the [`ChangeHash`] whose 32 bytes are all `n`, so a single byte
    /// names a distinct, reproducible hash in tests.
    fn hash(n: u8) -> ChangeHash {
        ChangeHash([n; 32])
    }

    /// Map a slice of hash bytes to [`ChangeHash`]es via [`hash`].
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
        /// Revoke `author` at `heads`, masking with a clock built from
        /// `clock_seed`.
        Revoke {
            author: usize,
            heads: Vec<u8>,
            clock_seed: Vec<Option<u32>>,
        },
        /// Undo a previous revocation of `author`.
        Unrevoke { author: usize },
        /// Insert a new actor (assigned to `author`) at `pos % (len + 1)`,
        /// applying the two-step `revoke_new_actor` protocol atomically:
        /// when the author is already revoked, the new actor immediately
        /// gets a fully-revoking mask entry. No-op at `MAX_ACTORS` actors.
        InsertActor { pos: usize, author: usize },
        /// Remove the actor at `pos % len`; no-op when no actors exist.
        RemoveActor { pos: usize },
        /// Recompute every revocation's mask from its stored heads,
        /// exercising [`Revocations::recompute_revocations`].
        Recompute,
        /// Drop all revocation and pending state.
        Clear,
        /// Add `hashes` to the pending-revocation set.
        ExtendPending { hashes: Vec<u8> },
        /// Remove one `hash` from the pending-revocation set, asserting the
        /// real and model sets agree on whether it was present.
        PopPending { hash: u8 },
    }

    /// Generate a per-actor clock seed of length [`MAX_ACTORS`]; each entry
    /// is `None` or a sequence value in `1..MAX_SEQ`. It is reduced to the
    /// live actor count by [`Harness::seq_clock`].
    fn gen_clock_seed() -> impl Strategy<Value = Vec<Option<u32>>> {
        proptest::collection::vec(proptest::option::of(1u32..MAX_SEQ), MAX_ACTORS)
    }

    /// Generate a single [`Op`]; out-of-range positions and values are left
    /// to be reduced against the live state at apply time.
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

    /// Generate a sequence of up to 40 [`Op`]s — the input to the lockstep
    /// properties.
    fn gen_ops() -> impl Strategy<Value = Vec<Op>> {
        proptest::collection::vec(gen_op(), 0..40)
    }

    /// Reference model for a single, revoked author.
    #[derive(Debug, Clone)]
    struct ModelRevocation {
        /// The heads that the author was revoked at.
        heads: Vec<ChangeHash>,
        /// The sequence bounds, that model the revocation mask.
        ///
        /// The `StableId` keeps track of the actor IDs, but does not require
        /// shifting index for any given actor.
        /// This ensures that the index and values of the revocation mask in
        /// [`Revocations`] do not diverge.
        bounds: BTreeMap<StableId, Option<NonZeroU32>>,
    }

    /// Drives a real [`Revocations`] and a naive reference model in
    /// lockstep. Actor identity is tracked with stable ids so checks can
    /// follow an actor across `insert_actor`/`remove_actor` index shifts.
    struct Harness {
        /// The production [`Revocations`] under test.
        real: Revocations,
        /// Author/actor bookkeeping mirroring the document's own [`Authors`],
        /// consulted by `revoke`, `insert_actor`, and `remove_actor` exactly
        /// as the production code would.
        authors: Authors,
        /// Mapping from the actor [`StableId`] to the actor index (via the
        /// index of the `Vec`).
        positions: Vec<StableId>,
        /// The next [`StableId`] to hand out; incremented on every actor
        /// insert so ids are never reused.
        next_stable: StableId,
        /// A reverse-lookup from an actor's [`StableId`] to an author's `usize`.
        /// Actors are never reassigned, since author to actor forms a partition.
        author_of: BTreeMap<StableId, usize>,
        /// Mapping from an author (represented by a `usize` index) to their
        /// reference model of revocation.
        ///
        /// There will only be an entry in the map if the author has been
        /// revoked.
        revoked: BTreeMap<usize, ModelRevocation>,
        /// Heads recorded at revoke time -> clock, index-shifted alongside
        /// the actor set; shared by [`Harness::real`] and the reference model
        /// in `Recompute`.
        clock_table: HashMap<Vec<ChangeHash>, SeqClock>,
        /// Model of the pending-revocation set.
        pending: HashSet<ChangeHash>,
        /// Authors revoked at any point during the run.
        ever_revoked: BTreeSet<usize>,
    }

    impl Harness {
        /// An empty harness: no actors, no revocations, no pending state.
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

        /// Return the revocation mask for the reference model.
        ///
        /// It is built by taking the union of every author's
        /// [`ModelRevocation::bounds`]. This is well-defined because the
        /// authors never share actors.
        fn model_mask(&self) -> BTreeMap<StableId, Option<NonZeroU32>> {
            self.revoked
                .values()
                .flat_map(|r| r.bounds.clone())
                .collect()
        }

        /// Returns `true` if the actor found at `idx` is revoked at `seq`.
        ///
        /// Note that this is a restatement of the same logic in
        /// [`Revocations::is_revoked`]. They must remain independent to
        /// exercise the production model against the reference model.
        fn model_is_revoked(&self, idx: usize, seq: u64) -> bool {
            let id = self.positions[idx];
            match self.model_mask().get(&id) {
                Some(None) => true,
                Some(Some(v)) => seq > v.get() as u64,
                None => false,
            }
        }

        /// Apply one [`Op`] to both the real [`Revocations`] and the
        /// reference model, keeping them in lockstep. Each arm updates both
        /// sides so that afterwards the model accurately describes the real
        /// state; the properties depend on this to surface any divergence.
        /// Generated positions and sequence values are reduced against the
        /// live actor set here rather than in the generators.
        fn apply(&mut self, op: &Op) {
            match op {
                Op::Revoke {
                    author: a,
                    heads,
                    clock_seed,
                } => {
                    let heads = to_hashes(heads);
                    let clock = self.seq_clock(clock_seed);
                    // `clock_table` stands in for the change graph, which does
                    // not exist in a unit test: stash this revoke's clock
                    // under its heads so `Recompute` can replay it.
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
                    // Replay the stored clocks in place of the change graph:
                    // the real side recomputes its mask from `clock_table`
                    // via the closure, and the model re-derives each
                    // revocation's bounds from the same clocks.
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

        /// The real mask must contain exactly one entry per model-mask
        /// entry, at the correctly remapped index, with the same bound.
        /// Equality of the maps also proves uniqueness: no entries have
        /// collided onto one index or been lost across shifts.
        ///
        /// Precondition: every revoked actor still has a live position, so
        /// the position lookup below is total. `RemoveActor` upholds this by
        /// purging the removed actor's bound, so the `filter_map` never
        /// silently drops a model entry.
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
    fn assert_same_visible_state(a: &Revocations, b: &Revocations) -> Result<(), TestCaseError> {
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
        assert!(!h.model_is_revoked(0, 3));
        assert!(h.model_is_revoked(0, 4));
        // Actor 1 (author 1) is untouched.
        assert!(!h.real.is_revoked(ActorIdx::from(1usize), u64::MAX));
        assert!(!h.real.is_empty());
        h.apply(&Op::Unrevoke { author: 0 });
        assert!(h.real.is_empty());
        assert!(!h.real.is_revoked(ActorIdx::from(0usize), 4));
    }

    proptest! {
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
            // Ensure the author is not revoked to exercise the revoke, then
            // unrevoke logic
            h.apply(&Op::Unrevoke { author: a });
            let before = h.real.clone();
            h.apply(&Op::Revoke { author: a, heads, clock_seed: seed });
            h.apply(&Op::Unrevoke { author: a });
            assert_same_visible_state(&h.real, &before)?;
        }

        /// Revoking with identical arguments twice equals revoking once (idempotent).
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

        /// Unrevoking an author that is not revoked changes nothing (idempotent).
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

        /// A shift preserves the `is_revoked` answers of every actor that
        /// survives it.
        ///
        /// Surviving actors are matched by their [`StableId`], not by index,
        /// so the renumbering that [`Revocations::insert_actor`] and
        /// [`Revocations::remove_actor`] perform is expected and is not
        /// counted as a change. Only a surviving actor whose answers actually
        /// differ will fail the property.
        ///
        /// The inserted or removed actor is intentionally not checked: an
        /// inserted actor has no answers from before the shift, and a removed
        /// actor no longer has an index to probe.
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
    }
}
