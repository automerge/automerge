use std::sync::Arc;

use crate::{
    change_graph::ChangeGraph, clock::Clock, op_set2::ActorIdx, ActorId, PatchLogMismatch,
};

use super::Revocations;

/// A document-local reference to a resolved revocation clock. The identity
/// distinguishes snapshots appended at the same offset in diverging clones.
#[derive(Clone, Debug)]
pub(crate) struct RevocationState {
    index: usize,
    identity: Arc<()>,
}

impl PartialEq for RevocationState {
    fn eq(&self, other: &Self) -> bool {
        self.index == other.index && Arc::ptr_eq(&self.identity, &other.identity)
    }
}

impl Eq for RevocationState {}

#[derive(Clone, Debug)]
struct Snapshot {
    clock: Clock,
    identity: Arc<()>,
}

/// Resolved masks, not causal clocks or revocation policies. A saved mask must
/// not be re-resolved when a previously missing boundary arrives.
///
/// History is process-local and is not saved or synced. Entries are retained for
/// the document's lifetime so outstanding patch logs can still reference them.
/// Only actor-index migration changes an existing entry's representation.
#[derive(Clone, Debug)]
pub(crate) struct RevocationHistory {
    snapshots: Vec<Snapshot>,
}

impl RevocationHistory {
    pub(crate) fn new(actors: usize) -> Self {
        Self {
            snapshots: vec![Snapshot {
                clock: Clock(vec![u32::MAX; actors]),
                identity: Arc::new(()),
            }],
        }
    }

    pub(crate) fn current(&self) -> RevocationState {
        RevocationState {
            index: self.snapshots.len() - 1,
            identity: self.snapshots.last().unwrap().identity.clone(),
        }
    }

    pub(crate) fn get(&self, state: &RevocationState) -> Result<&Clock, PatchLogMismatch> {
        self.snapshots
            .get(state.index)
            .filter(|snapshot| Arc::ptr_eq(&snapshot.identity, &state.identity))
            .map(|snapshot| &snapshot.clock)
            .ok_or(PatchLogMismatch)
    }

    pub(crate) fn active(&self, revocations: &Revocations) -> Option<&Clock> {
        (!revocations.is_empty()).then(|| &self.snapshots.last().unwrap().clock)
    }

    pub(crate) fn rebuild(&mut self, graph: &ChangeGraph, revocations: &Revocations) {
        let clock: Clock = (0..graph.num_actors())
            .map(
                |actor| match revocations.get_mask_for(&ActorIdx::from(actor)) {
                    Some(mask) => mask.and_then(|seq| graph.max_op_for_seq(actor, seq)),
                    None => Some(u32::MAX),
                },
            )
            .collect();
        if self.snapshots.last().unwrap().clock != clock {
            self.snapshots.push(Snapshot {
                clock,
                identity: Arc::new(()),
            });
        }
    }

    /// Rebase the history when the empty-document load fast path replaces an
    /// op set. Normally actors migrate one at a time through insert/remove.
    pub(crate) fn migrate_actors(&mut self, before: &[ActorId], after: &[ActorId]) {
        let indexes: Vec<_> = after
            .iter()
            .map(|actor| before.binary_search(actor).ok())
            .collect();
        for snapshot in &mut self.snapshots {
            snapshot.clock = Clock(
                indexes
                    .iter()
                    .map(|index| index.map_or(u32::MAX, |index| snapshot.clock.0[index]))
                    .collect(),
            );
        }
    }

    pub(crate) fn insert_actor(&mut self, actor: usize) {
        for snapshot in &mut self.snapshots {
            // A newly discovered actor has no operations at any saved view's
            // heads. Its restrictions are published in the new current state,
            // never retroactively applied to old snapshots.
            snapshot.clock.0.insert(actor, u32::MAX);
        }
    }

    pub(crate) fn remove_actor(&mut self, actor: usize) {
        for snapshot in &mut self.snapshots {
            snapshot.clock.0.remove(actor);
        }
    }
}
