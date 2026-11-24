//! Hold bundles that are not causally ready, and prepare the ones that become
//! ready without consuming them.
//!
//! The bundle counterpart of [`StagedChanges`](crate::staging::StagedChanges):
//! a bundle leaves staging only through [`StagedBundles::remove_completed`],
//! once it has applied.

use std::collections::{BTreeSet, HashMap, HashSet};

use crate::storage::Bundle;
use crate::{ActorId, ChangeHash};

/// Bundles each waiting for one actor to reach a seq.
///
/// A bundle names its deps by [`ChangeId`](crate::ChangeId), so unlike a
/// staged change it can always tell when it is ready, whatever hashes the
/// document has freed.
#[derive(Debug, Clone, Default)]
pub(crate) struct StagedBundles {
    slots: Vec<Option<Staged>>,
    /// `(seq, slot)` per actor, so preparing looks only at the bundles an
    /// actor's progress could have made ready
    waiting: HashMap<ActorId, BTreeSet<(u64, usize)>>,
    /// `(heads, deps)` of each staged bundle: the same changes staged twice
    /// are held once
    keys: HashSet<(Vec<ChangeHash>, Vec<ChangeHash>)>,
}

#[derive(Debug, Clone)]
struct Staged {
    bundle: Bundle,
    waits_on: (ActorId, u64),
}

fn key(bundle: &Bundle) -> (Vec<ChangeHash>, Vec<ChangeHash>) {
    (bundle.heads().collect(), bundle.deps().to_vec())
}

impl StagedBundles {
    pub(crate) fn is_empty(&self) -> bool {
        self.keys.is_empty()
    }

    pub(crate) fn iter(&self) -> impl Iterator<Item = &Bundle> {
        self.slots.iter().flatten().map(|s| &s.bundle)
    }

    pub(crate) fn actors(&self) -> Vec<ActorId> {
        self.waiting.keys().cloned().collect()
    }

    /// Stage `bundle` until `actor` reaches `seq`.
    pub(crate) fn stage(&mut self, bundle: Bundle, waits_on: (ActorId, u64)) {
        if !self.keys.insert(key(&bundle)) {
            return;
        }
        let slot = self.slots.len();
        self.wait(slot, &waits_on);
        self.slots.push(Some(Staged { bundle, waits_on }));
    }

    /// The slots of the bundles waiting on `actors` at or below the seq
    /// `have` gives each, in staging order. Leaves staging unchanged.
    pub(crate) fn prepare(&self, actors: &[ActorId], have: impl Fn(&ActorId) -> u64) -> Vec<usize> {
        let mut ready: Vec<usize> = actors
            .iter()
            .filter_map(|a| Some((self.waiting.get(a)?, have(a))))
            .flat_map(|(w, have)| w.range(..=(have, usize::MAX)).map(|(_, slot)| *slot))
            .collect();
        ready.sort_unstable();
        ready.dedup();
        ready
    }

    pub(crate) fn get(&self, slot: usize) -> &Bundle {
        &self.staged(slot).bundle
    }

    /// Wait on the next change the bundle in `slot` lacks.
    pub(crate) fn rewait(&mut self, slot: usize, waits_on: (ActorId, u64)) {
        self.unwait(slot);
        self.wait(slot, &waits_on);
        self.slots[slot].as_mut().expect("a staged slot").waits_on = waits_on;
    }

    /// Remove the bundle in `slot` once it has applied.
    pub(crate) fn remove_completed(&mut self, slot: usize) {
        self.unwait(slot);
        let staged = self.slots[slot].take().expect("a staged slot");
        self.keys.remove(&key(&staged.bundle));
        self.compact();
    }

    /// Remove staged bundles holding or depending on `actor` at or after
    /// `seq`, together with bundles which transitively depend on those.
    pub(crate) fn remove_actor_branch_from(&mut self, actor: &ActorId, seq: u64) {
        if self.is_empty() {
            return;
        }
        let mut cutoffs = vec![(actor.clone(), seq)];
        while let Some((actor, seq)) = cutoffs.pop() {
            for slot in 0..self.slots.len() {
                if !self.slots[slot]
                    .as_ref()
                    .is_some_and(|s| names(&s.bundle, &actor, seq))
                {
                    continue;
                }
                cutoffs.extend(first_members(self.get(slot)));
                self.remove_completed(slot);
                if self.slots.is_empty() {
                    return;
                }
            }
        }
    }

    fn staged(&self, slot: usize) -> &Staged {
        self.slots[slot].as_ref().expect("a staged slot")
    }

    fn wait(&mut self, slot: usize, (actor, seq): &(ActorId, u64)) {
        self.waiting
            .entry(actor.clone())
            .or_default()
            .insert((*seq, slot));
    }

    fn unwait(&mut self, slot: usize) {
        let (actor, seq) = &self.slots[slot].as_ref().expect("a staged slot").waits_on;
        let Some(w) = self.waiting.get_mut(actor) else {
            return;
        };
        w.remove(&(*seq, slot));
        if w.is_empty() {
            self.waiting.remove(actor);
        }
    }

    fn compact(&mut self) {
        if self.keys.is_empty() {
            *self = Self::default();
        }
    }
}

/// Whether `bundle` holds or depends on `actor` at or after `seq`.
fn names(bundle: &Bundle, actor: &ActorId, seq: u64) -> bool {
    let actors = bundle.actors();
    let member = bundle
        .member_actors
        .iter()
        .zip(&bundle.member_seqs)
        .any(|(a, s)| &actors[usize::from(*a)] == actor && s.get() >= seq);
    let dep = bundle
        .dep_ids
        .iter()
        .chain(bundle.boundary.iter().map(|(_, id)| id))
        .any(|id| id.actor() == actor && id.seq() >= seq);
    member || dep
}

/// Each actor's lowest member seq in `bundle`.
fn first_members(bundle: &Bundle) -> Vec<(ActorId, u64)> {
    let mut first: HashMap<usize, u64> = HashMap::new();
    for (a, s) in bundle.member_actors.iter().zip(&bundle.member_seqs) {
        let seq = first.entry(usize::from(*a)).or_insert(u64::MAX);
        *seq = (*seq).min(s.get());
    }
    first
        .into_iter()
        .map(|(a, seq)| (bundle.actors()[a].clone(), seq))
        .collect()
}
