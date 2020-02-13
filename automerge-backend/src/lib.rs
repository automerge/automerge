mod error;
mod protocol;

use crate::protocol::{ActorID, Change, Clock, DataType, Key, ObjectID, PrimitiveValue};

pub struct Backend {}

pub enum ElementValue {
    Primitive(PrimitiveValue),
    Link(ObjectID),
}

pub enum SequenceType {
    List,
    Text,
}

pub enum MapType {
    Map,
    Table,
}

pub enum DiffAction {
    CreateMap(ObjectID, MapType),
    CreateList(ObjectID, SequenceType),
    MaxElem(ObjectID, u32),
    RemoveMapKey(ObjectID, Key),
    SetMapKey(ObjectID, Key, PrimitiveValue, Option<DataType>),
    RemoveSequenceElement(ObjectID, u32, Option<DataType>),
    InsertSequenceElement(ObjectID, u32, ElementValue, Option<DataType>),
    SetSequenceElement(ObjectID, u32, ElementValue),
}

struct Conflict {
    actor: ActorID,
    value: ElementValue,
    datatype: Option<DataType>
}

struct Diff {
    action: DiffAction,
    conflicts: Vec<Conflict>
}

pub struct Patch {
    can_undo: bool,
    can_redo: bool,
    clock: Clock,
    deps: Clock,
    diffs: Vec<Diff>,
}

impl Patch {
    fn empty() -> Patch {
        Patch {
            can_undo: false,
            can_redo: false,
            clock: Clock::empty(),
            deps: Clock::empty(),
            diffs: Vec::new(),
        }
    }
}

impl Backend {
    pub fn init() -> Backend {
        Backend {}
    }

    pub fn apply_changes(&mut self, _changes: Vec<Change>) -> Patch {
        Patch::empty()
    }

    pub fn apply_local_change(&mut self, _change: Change) -> Patch {
        Patch::empty()
    }

    pub fn get_patch(&self) -> Patch {
        Patch::empty()
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
        Patch::empty()
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {
        assert_eq!(2 + 2, 4);
    }
}
