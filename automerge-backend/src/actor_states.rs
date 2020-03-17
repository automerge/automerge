use crate::error::AutomergeError;
use crate::operation_with_metadata::OperationWithMetadata;
use crate::protocol::{ActorID, Change, Clock};
use std::collections::HashMap;
use std::rc::Rc;

// ActorStates manages
//    `change_by_actor` - a seq ordered vec of changes per actor
//    `deps_by_actor` - a seq ordered vec of transitive deps per actor
//    `history` - a list of all changes received in order
// this struct is used for telling if two ops are concurrent or referencing
// historic changes

#[derive(Debug, PartialEq, Clone)]
pub struct ActorStates {
    pub history: Vec<Rc<Change>>,
    change_by_actor: HashMap<ActorID, Vec<Rc<Change>>>,
    deps_by_actor: HashMap<ActorID, Vec<Clock>>,
    // this lets me return a reference to an empty clock when needed
    // without having to do any extra allocations or copies
    // in the default path
    empty_clock: Clock,
}

impl ActorStates {
    pub(crate) fn new() -> ActorStates {
        ActorStates {
            change_by_actor: HashMap::new(),
            deps_by_actor: HashMap::new(),
            empty_clock: Clock::empty(),
            history: Vec::new(),
        }
    }

    pub fn is_concurrent(&self, op1: &OperationWithMetadata, op2: &OperationWithMetadata) -> bool {
        let clock1 = self.get_deps(&op1.actor_id, op1.seq);
        let clock2 = self.get_deps(&op2.actor_id, op2.seq);
        clock1.get(&op2.actor_id) < op2.seq && clock2.get(&op1.actor_id) < op1.seq
    }

    pub fn get(&self, actor_id: &ActorID) -> Vec<&Change> {
        self.change_by_actor
            .get(actor_id)
            .map(|vec| vec.iter().map(|c| c.as_ref()).collect())
            .unwrap_or_default()
    }

    pub fn get_change(&self, actor_id: &ActorID, seq: u32) -> Option<&Rc<Change>> {
        self.change_by_actor
            .get(actor_id)
            .and_then(|v| v.get((seq as usize) - 1))
    }

    fn get_deps(&self, actor_id: &ActorID, seq: u32) -> &Clock {
        self.get_deps_option(actor_id, seq)
            .unwrap_or(&self.empty_clock)
    }

    fn get_deps_option(&self, actor_id: &ActorID, seq: u32) -> Option<&Clock> {
        self.deps_by_actor
            .get(actor_id)
            .and_then(|v| v.get((seq as usize) - 1))
    }

    fn transitive_deps(&self, clock: &Clock) -> Clock {
        let mut all_deps = clock.clone();
        clock
            .into_iter()
            .filter_map(|(actor_id, seq)| self.get_deps_option(actor_id, *seq))
            .for_each(|deps| all_deps.merge(deps));
        all_deps
    }

    // if the change is new - return Ok(true)
    // if the change is a duplicate - dont insert and return Ok(false)
    // if the change has a dup actor:seq but is different error
    pub(crate) fn add_change(&mut self, change: Change) -> Result<bool, AutomergeError> {
        if let Some(c) = self.get_change(&change.actor_id, change.seq) {
            if &change == c.as_ref() {
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

        let rc = Rc::new(change);
        self.history.push(rc.clone());

        let actor_changes = self
            .change_by_actor
            .entry(actor_id.clone())
            .or_insert_with(Vec::new);

        if (rc.seq as usize) - 1 != actor_changes.len() {
            panic!(
                "cant push c={:?}:{:?} at ${:?}",
                rc.actor_id,
                rc.seq,
                actor_changes.len()
            );
        }

        actor_changes.push(rc);

        let actor_deps = self.deps_by_actor.entry(actor_id).or_insert_with(Vec::new);

        actor_deps.push(all_deps);

        Ok(true)
    }
}
