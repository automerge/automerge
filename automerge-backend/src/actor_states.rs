use crate::error::AutomergeError;
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
pub(crate) struct ActorStates {
    pub history: Vec<Rc<Change>>,
    change_by_actor: HashMap<ActorID, Vec<Rc<Change>>>,
    deps_by_actor: HashMap<ActorID, Vec<Clock>>,
    // this lets me return a reference to an empty clock when needed
    // without having to do any extra allocations or copies
    // in the default path
    empty_clock: Clock,
}

impl ActorStates {
    pub fn new() -> ActorStates {
        ActorStates {
            change_by_actor: HashMap::new(),
            deps_by_actor: HashMap::new(),
            empty_clock: Clock::empty(),
            history: Vec::new(),
        }
    }

    pub fn get(&self, actor_id: &ActorID) -> Vec<&Change> {
        self.change_by_actor
            .get(actor_id)
            .map(|vec| vec.iter().map(|c| c.as_ref()).collect())
            .unwrap_or_default()
    }

    fn get_change(&self, actor_id: &ActorID, seq: u64) -> Option<&Rc<Change>> {
        self.change_by_actor
            .get(actor_id)
            .and_then(|v| v.get((seq as usize) - 1))
    }

    fn get_deps_option(&self, actor_id: &ActorID, seq: u64) -> Option<&Clock> {
        self.deps_by_actor
            .get(actor_id)
            .and_then(|v| v.get((seq as usize) - 1))
    }

    fn transitive_deps(&self, change: &Change) -> Clock {
        let clock = change.deps.with(&change.actor_id, change.seq - 1);
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
    pub fn add_change(&mut self, change: &Rc<Change>) -> Result<Option<Clock>, AutomergeError> {
        if let Some(c) = self.get_change(&change.actor_id, change.seq) {
            if change.as_ref() == c.as_ref() {
                return Ok(None);
            } else {
                return Err(AutomergeError::InvalidChange(
                    "Invalid reuse of sequence number for actor".to_string(),
                ));
            }
        }

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

        actor_changes.push(change.clone());

        let all_deps = self.transitive_deps(change);

        self.deps_by_actor
            .entry(actor_id)
            .or_insert_with(Vec::new)
            .push(all_deps.clone());

        Ok(Some(all_deps))
    }
}
