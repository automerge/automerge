mod error;
mod protocol;
mod patch_serialization;
use serde::Serialize;

pub use crate::protocol::{ActorID, Change, Clock, DataType, Key, ObjectID, PrimitiveValue};

pub enum ElementValue {
    Primitive(PrimitiveValue),
    Link(ObjectID),
}

#[derive(Serialize)]
pub enum SequenceType {
    #[serde(rename="list")]
    List,
    #[serde(rename="text")]
    Text,
}

#[derive(Serialize)]
pub enum MapType {
    #[serde(rename="map")]
    Map,
    #[serde(rename="table")]
    Table,
}

pub enum DiffAction {
    CreateMap(ObjectID, MapType),
    CreateList(ObjectID, SequenceType),
    MaxElem(ObjectID, u32, SequenceType),
    RemoveMapKey(ObjectID, MapType, Key),
    SetMapKey(ObjectID, MapType, Key, ElementValue, Option<DataType>),
    RemoveSequenceElement(ObjectID, SequenceType, u32),
    InsertSequenceElement(ObjectID, SequenceType, u32, ElementValue, Option<DataType>),
    SetSequenceElement(ObjectID, SequenceType, u32, ElementValue, Option<DataType>),
}

pub struct Conflict {
    pub actor: ActorID,
    pub value: ElementValue,
    pub datatype: Option<DataType>
}

pub struct Diff {
    pub action: DiffAction,
    pub conflicts: Vec<Conflict>
}

#[derive(Serialize)]
#[serde(rename_all="camelCase")]
pub struct Patch {
    pub can_undo: bool,
    pub can_redo: bool,
    pub clock: Clock,
    pub deps: Clock,
    pub diffs: Vec<Diff>,
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

#[derive(Debug, PartialEq, Clone)]
pub struct Backend  {
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
