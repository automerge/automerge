//! Hold unapplied changes and prepare causally ready history without consuming it.
//!
//! Missing dependencies leave changes staged for a later attempt. Preparation
//! borrows ready contents in dependency order. The existing destructive drain
//! remains in use until borrowed application and completion are wired.

use std::{
    collections::{HashMap, HashSet, VecDeque},
    ops::ControlFlow,
};

use crate::{change_graph::ChangeGraph, ActorId, AutomergeError, Change, ChangeHash};

#[derive(Debug, Clone)]
pub(crate) struct ChangeBatch {
    changes: Vec<Change>,
    hashes: HashSet<ChangeHash>,
    incoming_actor_seqs: HashSet<(ActorId, u64)>,
}

impl ChangeBatch {
    pub(crate) fn new() -> Self {
        Self {
            changes: Vec::new(),
            hashes: HashSet::new(),
            incoming_actor_seqs: HashSet::new(),
        }
    }

    pub(crate) fn push(&mut self, change: Change) -> Result<(), AutomergeError> {
        let hash = change.hash();
        if self.hashes.contains(&hash) {
            return Ok(());
        }

        let actor_seq = (change.actor_id().clone(), change.seq());
        if self.incoming_actor_seqs.contains(&actor_seq) {
            return Err(AutomergeError::DuplicateSeqNumber(
                change.seq(),
                change.actor_id().clone(),
            ));
        }

        self.hashes.insert(hash);
        self.incoming_actor_seqs.insert(actor_seq);
        self.changes.push(change);
        Ok(())
    }
}

/// Unapplied change contents held for possible application.
///
/// Owns each staged change once, with indexes for its hash and actor/sequence.
/// Staging alone does not establish causal readiness or signing requirements.
#[derive(Debug, Clone)]
pub(crate) struct StagedChanges {
    changes: Vec<Change>,
    /// Set of hashes of all changes in the queue — O(1) contains check.
    hashes: HashSet<ChangeHash>,
    incoming_actor_seqs: HashSet<(ActorId, u64)>,
}

impl StagedChanges {
    /// Borrow all causally ready staged changes in dependency order.
    ///
    /// Dependencies may already be applied or become available earlier in this
    /// batch. Missing dependencies keep a change and its dependents out of the
    /// result, without preventing unrelated ready history from proceeding.
    ///
    /// Leaves staging unchanged, including the prepared changes. An empty result
    /// is valid. The result borrows staged contents, not the applied graph.
    pub(crate) fn prepare(&self, applied: &ChangeGraph) -> PreparedChanges<'_> {
        Preparation::new(self, applied).run()
    }

    pub(crate) fn new() -> Self {
        Self {
            changes: Vec::new(),
            hashes: HashSet::new(),
            incoming_actor_seqs: HashSet::new(),
        }
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.changes.is_empty()
    }

    pub(crate) fn iter(&self) -> impl Iterator<Item = &Change> {
        self.changes.iter()
    }

    /// Remove queued changes at or after an incompatible actor sequence,
    /// together with changes which transitively depend on that branch.
    pub(crate) fn remove_actor_branch_from(&mut self, actor: &ActorId, seq: u64) {
        let mut removed = self
            .changes
            .iter()
            .filter(|change| change.actor_id() == actor && change.seq() >= seq)
            .map(Change::hash)
            .collect::<HashSet<_>>();

        let mut dependents: HashMap<ChangeHash, Vec<ChangeHash>> = HashMap::new();
        for change in &self.changes {
            for dep in change.deps() {
                dependents.entry(*dep).or_default().push(change.hash());
            }
        }

        let mut to_remove = removed.iter().copied().collect::<VecDeque<_>>();
        while let Some(hash) = to_remove.pop_front() {
            if let Some(children) = dependents.remove(&hash) {
                for child in children {
                    if removed.insert(child) {
                        to_remove.push_back(child);
                    }
                }
            }
        }

        self.changes.retain(|change| {
            if removed.contains(&change.hash()) {
                self.hashes.remove(&change.hash());
                self.incoming_actor_seqs
                    .remove(&(change.actor_id().clone(), change.seq()));
                false
            } else {
                true
            }
        });
    }

    pub(crate) fn extend(&mut self, batch: ChangeBatch) {
        for c in batch.changes {
            let incoming_actor_seq = (c.actor_id().clone(), c.seq());
            debug_assert!(!self.incoming_actor_seqs.contains(&incoming_actor_seq));
            debug_assert!(!self.hashes.contains(&c.hash()));
            self.incoming_actor_seqs.insert(incoming_actor_seq);
            self.hashes.insert(c.hash());
            self.changes.push(c);
        }
    }

    /// Return the causally ready (according to ChangeGraph) changes in
    /// topological order, removing them from the queue
    pub(crate) fn pop_topo_sorted_ready(&mut self, change_graph: &ChangeGraph) -> Vec<Change> {
        // Kahn's algorithm: topological sort of the pool.
        let n = self.changes.len();
        let mut unsatisfied = vec![0u32; n];
        let mut waiting_on: HashMap<ChangeHash, Vec<usize>> = HashMap::new();

        for (i, c) in self.changes.iter().enumerate() {
            for dep in c.deps() {
                if !change_graph.has_change(dep) {
                    unsatisfied[i] += 1;
                    waiting_on.entry(*dep).or_default().push(i);
                }
            }
        }

        let mut ready: VecDeque<usize> = VecDeque::new();
        for (i, &count) in unsatisfied.iter().enumerate() {
            if count == 0 {
                ready.push_back(i);
            }
        }

        let mut topo_order: Vec<usize> = Vec::new();
        while let Some(idx) = ready.pop_front() {
            let hash = self.changes[idx].hash();
            topo_order.push(idx);
            if let Some(dependents) = waiting_on.remove(&hash) {
                for dep_idx in dependents {
                    unsatisfied[dep_idx] -= 1;
                    if unsatisfied[dep_idx] == 0 {
                        ready.push_back(dep_idx);
                    }
                }
            }
        }

        let mut slots = std::mem::take(&mut self.changes)
            .into_iter()
            .map(Some)
            .collect::<Vec<_>>();
        let mut topo = Vec::new();
        for idx in topo_order {
            let change = slots[idx]
                .take()
                .expect("topo_order contains invalid index");
            self.hashes.remove(&change.hash());
            self.incoming_actor_seqs
                .remove(&(change.actor_id().clone(), change.seq()));
            topo.push(change);
        }
        self.changes = slots.into_iter().flatten().collect::<Vec<_>>();

        topo
    }
}

/// Causally ready staged changes in application order.
///
/// Construct through [`StagedChanges::prepare`]. Dropping the result has no effect.
/// Keep preparation and application within one document operation. This value
/// does not retain or establish the identity of the consulted graph.
#[derive(Debug)]
pub(crate) struct PreparedChanges<'staged> {
    ordered: Vec<&'staged Change>,
}

impl<'staged> PreparedChanges<'staged> {
    /// Borrow the ready changes in dependency order.
    pub(crate) fn changes(&self) -> &[&'staged Change] {
        &self.ordered
    }
}

struct Preparation<'staged> {
    /// All staged changes, kept in staging order.
    changes: Vec<&'staged Change>,
    /// Dependencies not already applied or emitted, including missing history.
    waiting: Vec<usize>,
    /// When this hash is emitted, which changes move closer to readiness?
    dependents: HashMap<ChangeHash, Vec<usize>>,
    /// Indices into `changes` whose waiting count is zero.
    ready: VecDeque<usize>,
    /// References appended in application order.
    ordered: Vec<&'staged Change>,
}

impl<'staged> Preparation<'staged> {
    fn new(staged: &'staged StagedChanges, applied: &ChangeGraph) -> Self {
        let changes = staged.iter().collect::<Vec<_>>();

        let mut waiting = vec![0; changes.len()];
        let mut dependents: HashMap<ChangeHash, Vec<usize>> = HashMap::new();
        let mut ready = VecDeque::with_capacity(changes.len());
        for (index, change) in changes.iter().enumerate() {
            for dep in change.deps() {
                if !applied.has_change(dep) {
                    // Missing hashes never emit a notification, so their
                    // dependents remain blocked when the ready queue empties.
                    waiting[index] += 1;
                    dependents.entry(*dep).or_default().push(index);
                }
            }
            if waiting[index] == 0 {
                ready.push_back(index);
            }
        }

        let ordered = Vec::with_capacity(changes.len());

        Self {
            changes,
            waiting,
            dependents,
            ready,
            ordered,
        }
    }

    fn step(mut self) -> ControlFlow<PreparedChanges<'staged>, Self> {
        let Some(index) = self.ready.pop_front() else {
            return ControlFlow::Break(self.finish());
        };

        let change = self.changes[index];
        self.ordered.push(change);

        if let Some(dependents) = self.dependents.remove(&change.hash()) {
            for dependent in dependents {
                self.waiting[dependent] -= 1;

                if self.waiting[dependent] == 0 {
                    self.ready.push_back(dependent);
                }
            }
        }

        ControlFlow::Continue(self)
    }

    fn run(mut self) -> PreparedChanges<'staged> {
        loop {
            match self.step() {
                ControlFlow::Continue(next) => self = next,
                ControlFlow::Break(result) => return result,
            }
        }
    }

    fn finish(self) -> PreparedChanges<'staged> {
        PreparedChanges {
            ordered: self.ordered,
        }
    }
}

#[cfg(test)]
mod tests;
