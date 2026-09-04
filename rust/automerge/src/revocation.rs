use std::{
    collections::{HashMap, HashSet},
    num::NonZeroU32,
};

use crate::{author::Authors, clock::SeqClock, op_set2::ActorIdx, Author, ChangeHash};

#[derive(Debug, Default, Clone)]
pub(crate) struct Revocations {
    /// Revocations are a map from an [`Author`] to a [`ChangeHash`] heads
    /// boundary. These [`ChangeHash`]es, and any descendants will be made
    /// invisible to the materialized document.
    revocations: HashMap<Author<'static>, Vec<ChangeHash>>,
    /// Mapping from [`ActorIdx`] to maximum sequence number allowed by the
    /// revocations.
    ///
    /// See [`ChangeGraph::revocations`].
    revocations_mask: HashMap<ActorIdx, Option<NonZeroU32>>,
    pending_revoke: HashSet<ChangeHash>,
}

impl Revocations {
    pub(crate) fn new() -> Self {
        Self::default()
    }

    /// Returns `true` if no [`Author`]s have been revoked.
    pub(crate) fn is_empty(&self) -> bool {
        self.revocations.is_empty()
    }

    pub(crate) fn get_revocations_for_author(
        &self,
        author: &Author<'static>,
    ) -> Option<&Vec<ChangeHash>> {
        self.revocations.get(author)
    }

    pub(crate) fn get_revocations(&self) -> &HashMap<Author<'static>, Vec<ChangeHash>> {
        &self.revocations
    }

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

    pub(crate) fn unrevoke(&mut self, author: &Author<'static>, authors: &Authors) {
        for a in authors.get_actors_for_author(author) {
            self.revocations_mask.remove(&a.into());
        }
        self.revocations.remove(author);
    }

    pub(crate) fn is_revoked(&self, actor: ActorIdx, seq: u64) -> bool {
        match self.revocations_mask.get(&actor) {
            Some(Some(v)) if (v.get() as u64) < seq => true,
            Some(None) => true,
            _ => false,
        }
    }

    pub(crate) fn insert_mask_for(&mut self, actor: ActorIdx, mask: Option<NonZeroU32>) {
        self.revocations_mask.insert(actor, mask);
    }

    pub(crate) fn get_mask_for(&self, actor: &ActorIdx) -> Option<&Option<NonZeroU32>> {
        self.revocations_mask.get(actor)
    }

    pub(crate) fn get_revocation_mask(&self) -> &HashMap<ActorIdx, Option<NonZeroU32>> {
        &self.revocations_mask
    }

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

    pub(crate) fn clear(&mut self) {
        self.pending_revoke.clear();
        self.revocations_mask.clear();
        self.revocations.clear();
    }

    pub(crate) fn recompute_revocations(
        &mut self,
        authors: &Authors,
        get_clock: impl Fn(&Vec<ChangeHash>) -> SeqClock,
    ) {
        for (author, heads) in &self.revocations {
            let clock = get_clock(heads);
            for actor in authors.get_actors_for_author(author) {
                self.revocations_mask
                    .insert(actor.into(), clock.get_for_actor(&actor));
            }
        }
    }

    pub(crate) fn extend_pending_revocations(&mut self, heads: impl Iterator<Item = ChangeHash>) {
        self.pending_revoke.extend(heads)
    }

    pub(crate) fn pop_pending_revocation(&mut self, head: &ChangeHash) -> bool {
        self.pending_revoke.remove(head)
    }
}

#[cfg(test)]
mod tests {
    use super::Revocations;
    use crate::{
        author::Authors, clock::SeqClock, op_set2::ActorIdx, Author, ChangeHash,
    };
    use proptest::prelude::*;
    use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
    use std::num::NonZeroU32;

    // `allow(dead_code)` on items below marks harness pieces consumed by
    // later property tests; remove each allow once its item is in use.
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
    #[allow(dead_code)]
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
    }

    /// Build a harness in a reachable state by applying `ops`.
    #[allow(dead_code)]
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
    #[allow(dead_code)]
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
    }
}
