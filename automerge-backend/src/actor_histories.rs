use crate::operation_with_metadata::OperationWithMetadata;
use crate::protocol::{ActorID, Change, Clock};
use std::collections::HashMap;

/// ActorHistories is a cache for the transitive dependencies of each change
/// received from each actor. This is necessary because a change only ships its
/// direct dependencies in `deps` but we need all dependencies to determine
/// whether two operations occurrred concurrently.
#[derive(Debug, Clone, PartialEq)]
pub struct ActorHistories(HashMap<ActorID, HashMap<u32, Clock>>);

impl ActorHistories {
    pub(crate) fn new() -> ActorHistories {
        ActorHistories(HashMap::new())
    }

    /// Return the latest sequence required by `op` for actor `actor`
    fn dependency_for(&self, op: &OperationWithMetadata, actor: &ActorID) -> u32 {
        self.0
            .get(&op.actor_id)
            .and_then(|clocks| clocks.get(&op.sequence))
            .map(|c| c.at(actor))
            .unwrap_or(0)
    }

    /// Update this ActorHistories to include the changes in `change`
    pub(crate) fn add_change(&mut self, change: &Change) {
        let change_deps = change
            .dependencies
            .with_dependency(&change.actor_id, change.seq - 1);
        let transitive = self.transitive_dependencies(&change.actor_id, change.seq);
        let all_deps = transitive.upper_bound(&change_deps);
        let state = self
            .0
            .entry(change.actor_id.clone())
            .or_insert_with(HashMap::new);
        state.insert(change.seq, all_deps);
    }

    pub fn transitive_dependencies(&self, actor_id: &ActorID, seq: u32) -> Clock {
        self.0
            .get(actor_id)
            .and_then(|deps| deps.get(&seq))
            .cloned()
            .unwrap_or_else(Clock::empty)
    }

    pub fn transitive_dependencies_of_clock(&self, clock: &Clock) -> Clock {
        clock
            .into_iter()
            .fold(Clock::empty(), |clock, (actor_id, seq)| {
                clock.upper_bound(&self.transitive_dependencies(actor_id, *seq))
            })
            .upper_bound(clock)
    }

    /// Whether the two operations in question are concurrent
    pub(crate) fn are_concurrent(
        &self,
        op1: &OperationWithMetadata,
        op2: &OperationWithMetadata,
    ) -> bool {
        if op1.sequence == op2.sequence && op1.actor_id == op2.actor_id {
            return false;
        }
        self.dependency_for(op1, &op2.actor_id) < op2.sequence
            && self.dependency_for(op2, &op1.actor_id) < op1.sequence
    }
}
