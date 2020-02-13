mod protocol;
mod error;

use crate::protocol::{Change, ActorID, Clock};

pub struct Backend  {
}

pub struct Patch {}

impl Backend {
    pub fn init() -> Backend {
        Backend{}
    }

    pub fn apply_changes(&mut self, _changes: Vec<Change>) -> Patch {
        Patch{}
    }

    pub fn apply_local_change(&mut self, _change: Change) -> Patch {
        Patch{}
    }

    pub fn get_patch(&self) -> Patch {
        Patch{}
    }
    
    pub fn get_changes(&self) -> Vec<Change> {
        Vec::new()
    }

    pub fn get_changes_for_actor_id(&self, _actor_id: ActorID) -> Vec<Change> {
        Vec::new()
    }

    pub fn get_missing_changes(&self, _clock: Clock) -> Vec<Change> {
        Vec::new()
    }

    pub fn get_missing_deps(&self) -> Clock {
        Clock::empty()
    }

    pub fn merge(&mut self, _remote: &Backend) -> Patch {
        Patch{}
    }

}

#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {
        assert_eq!(2 + 2, 4);
    }
}
