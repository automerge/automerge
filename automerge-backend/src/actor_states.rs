use crate::error::AutomergeError;
use crate::protocol::{ActorID, Change, Clock};
use std::collections::HashMap;

#[derive(Debug, PartialEq, Clone)]
pub struct ActorStates {
    change_by_actor: HashMap<ActorID, Vec<Change>>,
    deps_by_actor: HashMap<ActorID, Vec<Clock>>,
    pub history: Vec<Change>,
}

impl ActorStates {
    pub(crate) fn new() -> ActorStates {
        ActorStates {
            change_by_actor: HashMap::new(),
            deps_by_actor: HashMap::new(),
            history: Vec::new(),
        }
    }

    pub fn is_concurrent(
        &self,
        actor_id1: &ActorID,
        seq1: u32,
        actor_id2: &ActorID,
        seq2: u32,
    ) -> bool {
        let clock1 = self.get_deps(actor_id1, seq1).unwrap();
        let clock2 = self.get_deps(actor_id2, seq2).unwrap();
        clock1.get(actor_id2) < seq2 && clock2.get(actor_id1) < seq1
    }

    pub fn get(&self, actor_id: &ActorID) -> Vec<&Change> {
        // FIXME - i know this can be simpler
        if let Some(changes) = self.change_by_actor.get(actor_id) {
            changes.iter().collect()
        } else {
            Vec::new()
        }
    }

    fn get_change(&self, actor_id: &ActorID, seq: u32) -> Option<&Change> {
        self.change_by_actor
            .get(actor_id)
            .and_then(|v| v.get((seq as usize) - 1))
    }

    fn get_deps(&self, actor_id: &ActorID, seq: u32) -> Option<&Clock> {
        self.deps_by_actor
            .get(actor_id)
            .and_then(|v| v.get((seq as usize) - 1))
    }

    fn transitive_deps(&self, clock: &Clock) -> Clock {
        let mut all_deps = clock.clone();
        clock
            .into_iter()
            .filter_map(|(actor_id, seq)| self.get_deps(actor_id, *seq))
            .for_each(|deps| all_deps.merge(deps));
        all_deps
    }

    pub(crate) fn add_change(&mut self, change: Change) -> Result<bool, AutomergeError> {
        if let Some(c) = self.get_change(&change.actor_id, change.seq) {
            if &change == c {
                return Ok(false);
            } else {
                return Err(AutomergeError::InvalidChange(
                    "Invalid reuse of sequence number for actor".to_string(),
                ));
            }
        }

        let deps = change.dependencies.with(&change.actor_id, change.seq - 1);
        let all_deps = self.transitive_deps(&deps);
        let actor_id = change.actor_id.clone();

        self.history.push(change.clone());

        let actor_changes = self
            .change_by_actor
            .entry(actor_id.clone())
            .or_insert_with(Vec::new);

        if (change.seq as usize) - 1 != actor_changes.len() {
            panic!(
                "cant push c={:?}:{:?} at ${:?}",
                change.actor_id,
                change.seq,
                actor_changes.len()
            );
        }

        actor_changes.push(change);

        let actor_deps = self.deps_by_actor.entry(actor_id).or_insert_with(Vec::new);

        actor_deps.push(all_deps);

        // TODO - panic if its the wrong seq!?

        Ok(true)
    }
}
