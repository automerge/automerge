use std::{
    collections::{HashMap, HashSet},
    num::NonZeroU32,
};

use crate::{
    actor::{ActorRefs, ActorRemoval, ActorShift, NoActorIndices},
    author::Authors,
    clock::SeqClock,
    op_set2::ActorIdx,
    Author, ChangeHash,
};

/// A record of write-frontiers for a set of [`Author`]s.
///
/// A frontier is a set of heads, i.e. [`Vec<ChangeHash>`]. A write-frontier is
/// then an inclusive bound at which the Automerge document will materialize for
/// the given author. Any changes after this frontier will be considered
/// invisible.
///
/// Each author is uniquely masked, and the heads are the inclusive bounds at
/// which the Automerge document will materialize for this author. Any changes
/// after these changes will be considered invisible.
///
/// # Frontier Mask
///
/// A frontier mask is a mapping from each actor to a sequence number. This
/// provides data to build a clock for marking operations as invisible when
/// materializing an Automerge document.
///
/// # Pending Changes
///
/// In some cases, the [`ChangeHash`] might not yet be recorded in the change
/// graph, these can be kept track of by using
/// [`WriteFrontier::extend_pending_changes`], and removed using
/// [`WriteFrontier::pop_pending_change`].
#[derive(Debug, Default, Clone)]
pub(crate) struct WriteFrontier {
    /// Frontiers are a map from an [`Author`] to a list of [`ChangeHash`] heads
    /// boundary. Any descendants of these `ChangeHash`es will be made invisible
    /// to the materialized document.
    // TODO(finto): Can this be a `HashSet<ChangeHash>` for better `contains` checks
    author_frontier: HashMap<Author<'static>, Vec<ChangeHash>>,
    /// Mapping from [`ActorIdx`] to maximum sequence number allowed by the
    /// write-frontier.
    frontier_mask: ActorRefs<HashMap<ActorIdx, SeqNumber>>,
    /// [`ChangeHash`] values that are not witnessed in the graph, but are in an
    /// author's write frontier.
    pending_changes: HashSet<ChangeHash>,
}

#[derive(Debug, Default, Clone)]
struct SeqNumber(Option<NonZeroU32>);

impl NoActorIndices for SeqNumber {}

impl SeqNumber {
    #[inline]
    fn get(&self) -> &Option<NonZeroU32> {
        &self.0
    }
}

impl WriteFrontier {
    /// Initialize the [`WriteFrontier`] beginning with no [`Author`]s being masked.
    pub(crate) fn new() -> Self {
        Self::default()
    }

    /// Returns `true` if no [`Author`]s have been masked.
    pub(crate) fn is_empty(&self) -> bool {
        self.author_frontier.is_empty()
    }

    /// Return at which changes the [`Author`] was masked at, if any.
    pub(crate) fn get_write_frontier_for_author(
        &self,
        author: &Author<'static>,
    ) -> Option<&[ChangeHash]> {
        self.author_frontier
            .get(author)
            .map(|heads| heads.as_slice())
    }

    /// Return the set of write-frontiers, per [`Author`].
    pub(crate) fn get_write_frontier(&self) -> &HashMap<Author<'static>, Vec<ChangeHash>> {
        &self.author_frontier
    }

    /// Mask the given [`Author`] at the given `heads`.
    ///
    /// [`Authors`] and [`SeqClock`] are used for building the [frontier
    /// mask](WriteFrontier#frontier-mask).
    pub(crate) fn mask(
        &mut self,
        author: Author<'static>,
        heads: Vec<ChangeHash>,
        clock: &SeqClock,
        authors: &Authors,
    ) {
        for actor in authors.get_actors_for_author(&author) {
            self.frontier_mask
                .insert(actor.into(), SeqNumber(clock.get_for_actor(&actor)));
        }
        self.author_frontier.insert(author, heads);
    }

    /// Reveal the given [`Author`], so that their changes can be materialized
    /// in the Automerge document again.
    ///
    /// [`Authors`] is used for removing the actors from the [frontier
    /// mask](WriteFrontier#frontier-mask).
    pub(crate) fn reveal(&mut self, author: &Author<'static>, authors: &Authors) {
        for a in authors.get_actors_for_author(author) {
            self.frontier_mask.remove(&a.into());
        }
        if let Some(heads) = self.author_frontier.remove(author) {
            // Stale pending heads would later trigger a spurious boundary
            // resolution (and a full mask republish) for a write-frontier that
            // no longer exists. Heads shared with another author's live
            // boundary stay pending.
            for head in heads {
                if !self.author_frontier.values().any(|hs| hs.contains(&head)) {
                    self.pending_changes.remove(&head);
                }
            }
        }
    }

    /// Insert a `mask` for the given `actor`.
    #[cfg(test)]
    pub(crate) fn insert_mask_for(&mut self, actor: ActorIdx, mask: Option<NonZeroU32>) {
        self.frontier_mask.insert(actor, SeqNumber(mask));
    }

    /// Return the mask for the given `actor`.
    pub(crate) fn get_mask_for(&self, actor: &ActorIdx) -> Option<&Option<NonZeroU32>> {
        self.frontier_mask.get(actor).map(SeqNumber::get)
    }

    /// Insert an `actor` into the [frontier mask](WriteFrontier#frontier-mask).
    pub(crate) fn insert_actor(&mut self, actor: &ActorShift) {
        self.frontier_mask.shift_actors(actor);
    }

    /// Remove an `actor` from the [frontier mask](WriteFrontier#frontier-mask).
    pub(crate) fn remove_actor(&mut self, actor: &ActorRemoval) {
        self.frontier_mask.remove(&ActorIdx::from(actor.index()));
        self.frontier_mask
            .remove_actor(actor)
            .expect("removed actor still has a mask entry")
    }

    /// Clear all [`WriteFrontier`] state.
    pub(crate) fn clear(&mut self) {
        self.pending_changes.clear();
        self.frontier_mask.clear();
        self.author_frontier.clear();
    }

    /// Recompute the [frontier mask](WriteFrontier#frontier-mask).
    ///
    /// [`Authors`] is used for finding the recorded actors for each author,
    /// while `get_clock` is a callback for calculating the new [`SeqClock`] for
    /// updating the mask entry for each actor.
    #[allow(unused)]
    fn recompute_write_frontiers(
        &mut self,
        authors: &Authors,
        get_clock: impl Fn(&[ChangeHash]) -> SeqClock,
    ) {
        for (author, heads) in &self.author_frontier {
            let clock = get_clock(heads);
            for actor in authors.get_actors_for_author(author) {
                self.frontier_mask
                    .insert(actor.into(), SeqNumber(clock.get_for_actor(&actor)));
            }
        }
    }

    /// Extend the set of pending changes with the given `heads`.
    ///
    /// Once a change has been witnessed, it can be removed using
    /// [`WriteFrontier::pop_pending_change`].
    pub(crate) fn extend_pending_changes(&mut self, heads: impl Iterator<Item = ChangeHash>) {
        self.pending_changes.extend(heads)
    }

    /// Remove the `head` from the pending changes, returning `true` if it
    /// existed in the set.
    #[allow(unused)]
    fn pop_pending_change(&mut self, head: &ChangeHash) -> bool {
        self.pending_changes.remove(head)
    }
}

#[cfg(test)]
mod tests {
    //! Property tests for [`WriteFrontier`].
    //!
    //! These tests use a model-based (lockstep) strategy: a [`Harness`] steps
    //! the production [`WriteFrontier`] and an independent reference model
    //! through a generated sequence of [`Op`]s, and after every step the
    //! properties assert that the two still agree. The reference model is a
    //! deliberately simple restatement of the intended masking semantics,
    //! written without reference to the production implementation, so a bug
    //! in [`WriteFrontier`] surfaces as a divergence rather than being mirrored
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
    //! author and exercise collisions, re-mask, and index shifts.

    use super::WriteFrontier;
    use crate::actor::{ActorIndexed, ActorRemoval, ActorShift, ActorTable};
    use crate::{author::Authors, clock::SeqClock, op_set2::ActorIdx, Author, ChangeHash};
    use proptest::prelude::*;
    use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
    use std::num::NonZeroU32;

    /// The universe is kept deliberately small so generated [`Op`] sequences
    /// frequently revisit the same author and actor, exercising collisions,
    /// re-mask, and index shifts rather than spreading thinly over a
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

    /// One operation against the harness. Authors are drawn from a tiny
    /// universe (`0..MAX_AUTHORS`) so sequences frequently revisit the same
    /// author; actor positions and clock values are reduced modulo the live
    /// state at execution time.
    #[derive(Debug, Clone)]
    enum Op {
        /// Mask `author` at `heads`, masking with a clock built from
        /// `clock_seed`.
        Mask {
            author: usize,
            heads: Vec<u8>,
            clock_seed: Vec<Option<u32>>,
        },
        /// Undo a previous mask of `author`.
        Reveal { author: usize },
        /// Insert a new actor (assigned to `author`) at `pos % (len + 1)`,
        /// applying the two-step "mask new actor"" protocol atomically:
        /// when the author is already masked, the new actor immediately
        /// gets a fully-masking entry. No-op at `MAX_ACTORS` actors.
        InsertActor { pos: usize, author: usize },
        /// Remove the actor at `pos % len`; no-op when no actors exist.
        RemoveActor { pos: usize },
        /// Recompute the write-frontier's mask from its stored heads,
        /// exercising [`WriteFrontier::recompute_write_frontiers`].
        Recompute,
        /// Clear the write-frontier and pending changes.
        Clear,
        /// Add `hashes` to the pending changes set.
        ExtendPending { hashes: Vec<u8> },
        /// Remove one `hash` from the pending changes set, asserting the
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
                .prop_map(|(author, heads, clock_seed)| Op::Mask {
                    author,
                    heads,
                    clock_seed
                }),
            (0..MAX_AUTHORS).prop_map(|author| Op::Reveal { author }),
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

    /// Reference model for a single, masked author.
    #[derive(Debug, Clone)]
    struct ModelWriteFrontier {
        /// The heads that the author was masked at.
        heads: Vec<ChangeHash>,
        /// The sequence bounds, that model the write-frontier mask.
        ///
        /// The `StableId` keeps track of the actor IDs, but does not require
        /// shifting index for any given actor.
        /// This ensures that the index and values of the write-frontier mask in
        /// [`WriteFrontier`] do not diverge.
        bounds: BTreeMap<StableId, Option<NonZeroU32>>,
    }

    /// Drives a real [`WriteFrontier`] and a naive reference model in
    /// lockstep. Actor identity is tracked with stable ids so checks can
    /// follow an actor across `insert_actor`/`remove_actor` index shifts.
    struct Harness {
        /// The production [`WriteFrontier`] under test.
        real: WriteFrontier,
        /// Author/actor bookkeeping mirroring the document's own [`Authors`],
        /// consulted by `mask`, `insert_actor`, and `remove_actor` exactly
        /// as the production code would.
        authors: Authors,
        /// Mapping from the actor [`StableId`] to the actor index (via the
        /// index of the `Vec`).
        positions: ActorIndexed<StableId>,
        /// An empty clock kept in step with the actors, used to build seeded
        /// clocks without bypassing `SeqClock`'s actor-indexed storage.
        empty_clock: SeqClock,
        /// The next [`StableId`] to hand out; incremented on every actor
        /// insert so ids are never reused.
        next_stable: StableId,
        /// A reverse-lookup from an actor's [`StableId`] to an author's `usize`.
        /// Actors are never reassigned, since author to actor forms a partition.
        author_of: BTreeMap<StableId, usize>,
        /// Mapping from an author (represented by a `usize` index) to their
        /// reference model of the writer-frontier.
        ///
        /// There will only be an entry in the map if the author has been
        /// masked.
        masked: BTreeMap<usize, ModelWriteFrontier>,
        /// Heads recorded at mask time -> clock, index-shifted alongside
        /// the actor set; shared by [`Harness::real`] and the reference model
        /// in `Recompute`.
        clock_table: HashMap<Vec<ChangeHash>, SeqClock>,
        /// Model of the pending changes set.
        pending: HashSet<ChangeHash>,
        /// Authors masked at any point during the run.
        ever_masked: BTreeSet<usize>,
    }

    impl Harness {
        /// An empty harness: no actors, no write-frontiers, no pending state.
        fn new() -> Self {
            let actors = ActorTable::new();
            Harness {
                real: WriteFrontier::new(),
                authors: Authors::with_actors(0),
                positions: ActorIndexed::new(&actors),
                empty_clock: SeqClock::new(&actors),
                next_stable: 0,
                author_of: BTreeMap::new(),
                masked: BTreeMap::new(),
                clock_table: HashMap::new(),
                pending: HashSet::new(),
                ever_masked: BTreeSet::new(),
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
            let mut clock = self.empty_clock.empty_like();
            for i in 0..self.positions.len() {
                clock.include(i, seed.get(i % MAX_ACTORS).copied().flatten());
            }
            clock
        }

        /// Return the write-frontier mask for the reference model.
        ///
        /// It is built by taking the union of every author's
        /// [`ModelWriteFrontier::bounds`]. This is well-defined because the
        /// authors never share actors.
        fn model_mask(&self) -> BTreeMap<StableId, Option<NonZeroU32>> {
            self.masked
                .values()
                .flat_map(|r| r.bounds.clone())
                .collect()
        }

        /// Apply one [`Op`] to both the real [`WriteFrontier`] and the
        /// reference model, keeping them in lockstep. Each arm updates both
        /// sides so that afterwards the model accurately describes the real
        /// state; the properties depend on this to surface any divergence.
        /// Generated positions and sequence values are reduced against the
        /// live actor set here rather than in the generators.
        fn apply(&mut self, op: &Op) {
            match op {
                Op::Mask {
                    author: a,
                    heads,
                    clock_seed,
                } => {
                    let heads = to_hashes(heads);
                    let clock = self.seq_clock(clock_seed);
                    // `clock_table` stands in for the change graph, which does
                    // not exist in a unit test: stash this mask's clock
                    // under its heads so `Recompute` can replay it.
                    self.clock_table.insert(heads.clone(), clock.clone());
                    self.real
                        .mask(author(*a), heads.clone(), &clock, &self.authors);
                    let bounds = self
                        .actors_of(*a)
                        .into_iter()
                        .map(|idx| (self.positions[idx], clock.get_for_actor(&idx)))
                        .collect();
                    self.masked.insert(*a, ModelWriteFrontier { heads, bounds });
                    self.ever_masked.insert(*a);
                }
                Op::Reveal { author: a } => {
                    self.real.reveal(&author(*a), &self.authors);
                    if let Some(rec) = self.masked.remove(a) {
                        // Mirror production: revealing clears the author's
                        // pending heads unless another live writer-frontier still
                        // references them.
                        for head in rec.heads {
                            if !self.masked.values().any(|r| r.heads.contains(&head)) {
                                self.pending.remove(&head);
                            }
                        }
                    }
                }
                Op::InsertActor { pos, author: a } => {
                    if self.positions.len() >= MAX_ACTORS {
                        return;
                    }
                    let pos = pos % (self.positions.len() + 1);
                    let shift = ActorShift::for_test(pos);
                    self.real.insert_actor(&shift);
                    self.authors.insert_actor(&shift);
                    self.authors.assign_author(author(*a), pos);
                    self.empty_clock.shift_actor(&shift);
                    for clock in self.clock_table.values_mut() {
                        clock.shift_actor(&shift);
                    }
                    let id = self.next_stable;
                    self.next_stable += 1;
                    self.positions.insert(&shift, id);
                    self.author_of.insert(id, *a);
                    // Two-step "mask new actor" protocol, applied
                    // atomically: a new actor of an already-masked author
                    // has no entry at the write-frontier's heads clock, so it is
                    // fully masked.
                    if let Some(rec) = self.masked.get_mut(a) {
                        self.real.insert_mask_for(ActorIdx::from(pos), None);
                        rec.bounds.insert(id, None);
                    }
                }
                Op::RemoveActor { pos } => {
                    if self.positions.is_empty() {
                        return;
                    }
                    let pos = pos % self.positions.len();
                    let removal = ActorRemoval::for_test(pos);
                    self.real.remove_actor(&removal);
                    self.authors.remove_actor(&removal);
                    self.empty_clock.remove_actor(&removal);
                    for clock in self.clock_table.values_mut() {
                        clock.remove_actor(&removal);
                    }
                    let id = self.positions.remove(&removal);
                    self.author_of.remove(&id);
                    for rec in self.masked.values_mut() {
                        rec.bounds.remove(&id);
                    }
                }
                Op::Recompute => {
                    // Replay the stored clocks in place of the change graph:
                    // the real side recomputes its mask from `clock_table`
                    // via the closure, and the model re-derives each
                    // write-frontier's bounds from the same clocks.
                    let table = self.clock_table.clone();
                    let empty_clock = &self.empty_clock;
                    self.real.recompute_write_frontiers(&self.authors, |heads| {
                        table
                            .get(heads)
                            .cloned()
                            .unwrap_or_else(|| empty_clock.empty_like())
                    });
                    for (a, rec) in self.masked.iter_mut() {
                        let clock = table
                            .get(&rec.heads)
                            .cloned()
                            .unwrap_or_else(|| empty_clock.empty_like());
                        for (idx, id) in self.positions.iter().enumerate() {
                            if self.author_of.get(id) == Some(a) {
                                rec.bounds.insert(*id, clock.get_for_actor(&idx));
                            }
                        }
                    }
                }
                Op::Clear => {
                    self.real.clear();
                    self.masked.clear();
                    self.pending.clear();
                }
                Op::ExtendPending { hashes } => {
                    let hs = to_hashes(hashes);
                    self.real.extend_pending_changes(hs.iter().copied());
                    self.pending.extend(hs);
                }
                Op::PopPending { hash: h } => {
                    let real = self.real.pop_pending_change(&hash(*h));
                    let model = self.pending.remove(&hash(*h));
                    assert_eq!(
                        real, model,
                        "pop_pending_change({h}) disagrees with the model"
                    );
                }
            }
        }

        /// The real mask must contain exactly one entry per model-mask
        /// entry, at the correctly remapped index, with the same bound.
        /// Equality of the maps also proves uniqueness: no entries have
        /// collided onto one index or been lost across shifts.
        ///
        /// Precondition: every masked actor still has a live position, so
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
                .frontier_mask
                .iter()
                .map(|(a, v)| (a.0, *v.get()))
                .collect();
            prop_assert_eq!(actual, expected);
            Ok(())
        }

        /// `is_empty()` agrees with "no author is masked", and an empty
        /// state has an empty mask — `Automerge::rebuild_mask` relies on
        /// `is_empty()` to drop the mask entirely, so a stale mask entry
        /// while `is_empty()` is true would silently disable masking.
        fn check_is_empty_consistency(&self) -> Result<(), TestCaseError> {
            prop_assert_eq!(self.real.is_empty(), self.masked.is_empty());
            if self.real.is_empty() {
                prop_assert!(
                    self.real.frontier_mask.is_empty(),
                    "is_empty() is true but the write-frontier mask is not empty"
                );
            }
            Ok(())
        }
    }
    /// Hand-written sequence exercising the harness end to end: two
    /// authors, one mask with a bounded clock, then reveal.
    #[test]
    fn harness_smoke_hand_written_sequence() {
        let mut h = Harness::new();
        h.apply(&Op::InsertActor { pos: 0, author: 0 });
        h.apply(&Op::InsertActor { pos: 1, author: 1 });
        h.apply(&Op::Mask {
            author: 0,
            heads: vec![1],
            clock_seed: vec![Some(3), Some(2)],
        });
        // Actor 0 (author 0) is bounded at seq 3; actor 1 (author 1) is
        // untouched.
        assert_eq!(
            h.real.get_mask_for(&ActorIdx::from(0usize)),
            Some(&NonZeroU32::new(3))
        );
        assert_eq!(h.real.get_mask_for(&ActorIdx::from(1usize)), None);
        assert!(!h.real.is_empty());
        h.check_mask_matches_model().unwrap();
        h.apply(&Op::Reveal { author: 0 });
        assert!(h.real.is_empty());
        assert_eq!(h.real.get_mask_for(&ActorIdx::from(0usize)), None);
        h.check_mask_matches_model().unwrap();
    }
    #[test]
    fn removing_masked_actor_preserves_other_bounds_and_frontier() {
        for seed in [vec![None; 3], vec![Some(2), Some(3), Some(5)]] {
            let mut h = Harness::new();
            for pos in 0..3 {
                h.apply(&Op::InsertActor { pos, author: 0 });
            }
            h.apply(&Op::Mask {
                author: 0,
                heads: vec![1],
                clock_seed: seed.clone(),
            });

            h.apply(&Op::RemoveActor { pos: 1 });

            assert_eq!(
                h.real.get_mask_for(&ActorIdx::from(0usize)),
                Some(&seed[0].and_then(NonZeroU32::new))
            );
            assert_eq!(
                h.real.get_mask_for(&ActorIdx::from(1usize)),
                Some(&seed[2].and_then(NonZeroU32::new))
            );
            assert_eq!(h.real.get_mask_for(&ActorIdx::from(2usize)), None);
            assert_eq!(
                h.real.get_write_frontier_for_author(&author(0)),
                Some(to_hashes(&[1]).as_slice())
            );
            h.check_mask_matches_model().unwrap();
            h.apply(&Op::Recompute);
            h.check_mask_matches_model().unwrap();
        }
    }

    proptest! {
        /// The central lockstep oracle: after every operation the real
        /// [`WriteFrontier`] mask agrees entry-for-entry with the reference
        /// model — the mask's keys are exactly the actors of currently
        /// masked authors (each at the bound captured at masking
        /// time) — and `is_empty()` means no author is masked with an
        /// empty mask. Production consumes this mask through
        /// `Automerge::rebuild_mask`/`ChangeGraph::max_op_for_seq`, so the
        /// mask entries (not any seq predicate) are the interface under
        /// test. Because the generated ops include mask/reveal
        /// collisions, actor index shifts, recompute, clear and
        /// pending-set traffic, the algebraic properties (idempotence,
        /// last-write-wins, commutation over distinct authors,
        /// monotonicity in seq, recompute as a fixpoint) all follow from
        /// agreement with the model. A failure here is a finding to
        /// report, not a test to fix.
        #[test]
        fn lockstep_matches_reference_model(ops in gen_ops()) {
            let mut h = Harness::new();
            for op in &ops {
                h.apply(op);
                h.check_mask_matches_model()?;
                h.check_is_empty_consistency()?;
            }
        }
    }
}
