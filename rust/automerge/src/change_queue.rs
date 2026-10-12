use std::collections::{BTreeMap, BTreeSet, HashMap, VecDeque};

use crate::{ActorId, Author, AutomergeError, Change, ChangeHash};

mod actor_queue;
use actor_queue::ActorQueue;
mod doc_state;
pub(crate) use doc_state::DocState;
mod entry_id;
use entry_id::EntryId;

/// The changes waiting to enter the document.
#[derive(Debug, Clone)]
pub(crate) struct ChangeQueue {
    /// Queued changes in arrival order.
    changes: BTreeMap<EntryId, Change>,
    next_id: EntryId,
    /// change hash → entry in `changes`.
    by_hash: HashMap<ChangeHash, EntryId>,
    /// actor → seq → entry in `changes`. Also where an actor's author is
    /// found while its first change is still queued.
    by_actor: HashMap<ActorId, ActorQueue>,
}

impl ChangeQueue {
    pub(crate) fn new() -> Self {
        Self {
            changes: BTreeMap::new(),
            next_id: EntryId::zero(),
            by_hash: HashMap::new(),
            by_actor: HashMap::new(),
        }
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.changes.is_empty()
    }

    /// Every queued change, in arrival order.
    pub(crate) fn changes(&self) -> impl Iterator<Item = &Change> {
        self.changes.values()
    }

    /// Queue `changes`, checking each against the document and against what is
    /// already queued.
    ///
    /// Either everything is queued or nothing is: an error leaves the queue as
    /// it was, except that a change conflicting with the document's own actor
    /// sequence evicts the queued branch it conflicts with, as it always has.
    ///
    /// A change already applied or already queued is skipped rather than
    /// refused, so re-receiving history is harmless.
    pub(crate) fn push(
        &mut self,
        changes: impl IntoIterator<Item = Change>,
        doc: &DocState<'_>,
    ) -> Result<(), AutomergeError> {
        let mark = self.next_id;
        let result = self.try_push(changes, doc);
        if result.is_err() {
            let unwound = self.changes.split_off(&mark);
            for change in unwound.values() {
                self.unindex(change);
            }
        }
        result
    }

    fn try_push(
        &mut self,
        changes: impl IntoIterator<Item = Change>,
        doc: &DocState<'_>,
    ) -> Result<(), AutomergeError> {
        for change in changes {
            if doc.graph.has_change(&change.hash()) || self.by_hash.contains_key(&change.hash()) {
                continue;
            }
            self.check_change(&change, doc)?;
            self.insert(change);
        }
        Ok(())
    }

    /// The checks one change must pass to be queued.
    fn check_change(&mut self, change: &Change, doc: &DocState<'_>) -> Result<(), AutomergeError> {
        if doc.has_actor_seq(change) {
            // The document already holds this actor sequence under another
            // hash. Any queued change at a later sequence belongs to the same
            // incompatible branch and can never apply.
            self.remove_actor_branch_from(change.actor_id(), change.seq().saturating_add(1));
            return Err(AutomergeError::duplicate_seq(change));
        }
        let queued = self.by_actor.get(change.actor_id());
        if queued.is_some_and(|seqs| seqs.at(change.seq()).is_some()) {
            return Err(AutomergeError::duplicate_seq(change));
        }
        if change.author().is_some() {
            let known = doc.author_of(change.actor_id()).is_some()
                || self.author_for_actor(change.actor_id()).is_some();
            if known {
                return Err(AutomergeError::duplicate_author(change));
            }
            if change.seq() != 1 {
                return Err(AutomergeError::author_on_non_initial_seq(change));
            }
        }
        Ok(())
    }

    fn insert(&mut self, change: Change) {
        let id = self.next_id;
        self.next_id = id.next();
        self.by_hash.insert(change.hash(), id);
        self.by_actor
            .entry(change.actor_id().clone())
            .or_default()
            .insert(change.seq(), id);
        self.changes.insert(id, change);
    }

    fn remove(&mut self, id: EntryId) -> Option<Change> {
        let change = self.changes.remove(&id)?;
        self.unindex(&change);
        Some(change)
    }

    fn unindex(&mut self, change: &Change) {
        self.by_hash.remove(&change.hash());
        if let Some(seqs) = self.by_actor.get_mut(change.actor_id()) {
            seqs.remove(change.seq());
            if seqs.is_empty() {
                self.by_actor.remove(change.actor_id());
            }
        }
    }

    /// The author `actor` assigns itself, if its first change is queued.
    fn author_for_actor(&self, actor: &ActorId) -> Option<Author<'_>> {
        let id = self.by_actor.get(actor)?.first()?;
        self.changes[&id].author()
    }

    /// Remove queued changes at or after an incompatible actor sequence,
    /// together with changes which transitively depend on that branch.
    pub(crate) fn remove_actor_branch_from(&mut self, actor: &ActorId, seq: u64) {
        let seeds: Vec<EntryId> = self
            .by_actor
            .get(actor)
            .map(|seqs| seqs.from_seq(seq).collect())
            .unwrap_or_default();
        if seeds.is_empty() {
            return;
        }
        let mut dependents: HashMap<ChangeHash, Vec<EntryId>> = HashMap::new();
        for (id, change) in &self.changes {
            for dep in change.deps() {
                dependents.entry(*dep).or_default().push(*id);
            }
        }
        let mut removed: BTreeSet<EntryId> = seeds.iter().copied().collect();
        let mut to_visit: VecDeque<EntryId> = seeds.into_iter().collect();
        while let Some(id) = to_visit.pop_front() {
            if let Some(children) = dependents.remove(&self.changes[&id].hash()) {
                for child in children {
                    if removed.insert(child) {
                        to_visit.push_back(child);
                    }
                }
            }
        }
        for id in removed {
            self.remove(id);
        }
    }

    /// Release every change which is causally ready, in an order which
    /// respects their dependencies.
    ///
    /// A change is causally ready when every dependency is applied or released
    /// before it in the same pass.
    pub(crate) fn pop_ready(&mut self, doc: &DocState<'_>) -> Vec<Change> {
        // Kahn's algorithm over the queue.
        let ids: Vec<EntryId> = self.changes.keys().copied().collect();
        let mut unsatisfied = vec![0u32; ids.len()];
        let mut waiting_on: HashMap<ChangeHash, Vec<usize>> = HashMap::new();
        for (index, id) in ids.iter().enumerate() {
            for dep in self.changes[id].deps() {
                if !doc.graph.has_change(dep) {
                    unsatisfied[index] += 1;
                    waiting_on.entry(*dep).or_default().push(index);
                }
            }
        }

        let mut ready: VecDeque<usize> = (0..ids.len())
            .filter(|index| unsatisfied[*index] == 0)
            .collect();
        let mut released = Vec::new();
        while let Some(index) = ready.pop_front() {
            let id = ids[index];
            released.push(id);
            let Some(dependents) = waiting_on.remove(&self.changes[&id].hash()) else {
                continue;
            };
            for dependent in dependents {
                unsatisfied[dependent] -= 1;
                if unsatisfied[dependent] == 0 {
                    ready.push_back(dependent);
                }
            }
        }

        released
            .into_iter()
            .map(|id| self.remove(id).expect("released change is queued"))
            .collect()
    }
}
