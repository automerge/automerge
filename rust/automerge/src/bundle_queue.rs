use std::cmp::Reverse;
use std::collections::binary_heap::PeekMut;
use std::collections::{BinaryHeap, HashMap, HashSet};

use crate::storage::Bundle;
use crate::{ActorId, ChangeHash};

/// Bundles that are not causally ready, each waiting for one actor to reach a
/// seq.
///
/// A bundle names its deps by [`ChangeId`](crate::ChangeId), so unlike a
/// queued change it can always tell when it is ready, whatever hashes the
/// document has freed.
#[derive(Debug, Clone, Default)]
pub(crate) struct BundleQueue {
    parked: Vec<Option<Bundle>>,
    /// `(seq, index into parked)` per actor, so an apply looks only at the
    /// bundles its actors could have made ready
    waiting: HashMap<ActorId, BinaryHeap<Reverse<(u64, usize)>>>,
    /// `(heads, deps)` of each parked bundle: the same changes queued twice
    /// are parked once
    keys: HashSet<(Vec<ChangeHash>, Vec<ChangeHash>)>,
}

fn key(bundle: &Bundle) -> (Vec<ChangeHash>, Vec<ChangeHash>) {
    (bundle.heads().collect(), bundle.deps().to_vec())
}

impl BundleQueue {
    pub(crate) fn is_empty(&self) -> bool {
        self.keys.is_empty()
    }

    pub(crate) fn iter(&self) -> impl Iterator<Item = &Bundle> {
        self.parked.iter().flatten()
    }

    pub(crate) fn actors(&self) -> Vec<ActorId> {
        self.waiting.keys().cloned().collect()
    }

    /// Queue `bundle` until `actor` reaches `seq`.
    pub(crate) fn park(&mut self, bundle: Bundle, (actor, seq): (ActorId, u64)) {
        if !self.keys.insert(key(&bundle)) {
            return;
        }
        let heap = self.waiting.entry(actor).or_default();
        heap.push(Reverse((seq, self.parked.len())));
        self.parked.push(Some(bundle));
    }

    /// Remove the bundles waiting on `actor` at or below `have`.
    pub(crate) fn take_ready(&mut self, actor: &ActorId, have: u64) -> Vec<Bundle> {
        let Some(heap) = self.waiting.get_mut(actor) else {
            return vec![];
        };
        let mut ready = vec![];
        while let Some(Reverse((_, i))) =
            heap.peek_mut().filter(|w| w.0 .0 <= have).map(PeekMut::pop)
        {
            ready.extend(self.parked[i].take());
        }
        if heap.is_empty() {
            self.waiting.remove(actor);
        }
        for b in &ready {
            self.keys.remove(&key(b));
        }
        self.compact();
        ready
    }

    /// Remove queued bundles holding or depending on `actor` at or after
    /// `seq`, together with bundles which transitively depend on those.
    pub(crate) fn remove_actor_branch_from(&mut self, actor: &ActorId, seq: u64) {
        if self.is_empty() {
            return;
        }
        let mut cutoffs = vec![(actor.clone(), seq)];
        while let Some((actor, seq)) = cutoffs.pop() {
            for slot in &mut self.parked {
                if !slot.as_ref().is_some_and(|b| names(b, &actor, seq)) {
                    continue;
                }
                let b = slot.take().expect("checked above");
                self.keys.remove(&key(&b));
                cutoffs.extend(first_members(&b));
            }
        }
        // a removed bundle's heap entry is skipped when it is popped
        self.waiting.retain(|_, heap| {
            heap.retain(|Reverse((_, i))| self.parked[*i].is_some());
            !heap.is_empty()
        });
        self.compact();
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
