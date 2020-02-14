mod error;
mod protocol;
use serde::{Serialize, Serializer};
use serde::ser::SerializeMap;

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

impl Serialize for Conflict {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
        where S: Serializer {
        let mut map_serializer = serializer.serialize_map(None)?;
        map_serializer.serialize_entry("actor", &self.actor)?;
        match &self.datatype {
            Some(d) => map_serializer.serialize_entry("datatype", &d)?,
            None => {}
        };
        match &self.value {
            ElementValue::Primitive(v) => map_serializer.serialize_entry("value", &v)?,
            ElementValue::Link(oid) => {
                map_serializer.serialize_entry("value", &oid)?;
                map_serializer.serialize_entry("link", &true)?;
            }
        };
        map_serializer.end()
    }
}

pub struct Diff {
    pub action: DiffAction,
    pub conflicts: Vec<Conflict>
}

impl Serialize for Diff {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
        where S: Serializer {
        let mut map_serializer = serializer.serialize_map(None)?;
        map_serializer.serialize_entry("conflicts", &self.conflicts)?;
        match &self.action {
            DiffAction::CreateMap(oid, map_type) => {
                map_serializer.serialize_entry("action", "create")?;
                map_serializer.serialize_entry("obj", &oid)?;
                map_serializer.serialize_entry("type", &map_type)?;
            },
            DiffAction::CreateList(oid, seq_type) => {
                map_serializer.serialize_entry("action", "create")?;
                map_serializer.serialize_entry("obj", &oid)?;
                map_serializer.serialize_entry("type", &seq_type)?;
            },
            DiffAction::MaxElem(oid, max, seq_type) => {
                map_serializer.serialize_entry("action", "maxElem")?;
                map_serializer.serialize_entry("obj", &oid)?;
                map_serializer.serialize_entry("value", &max)?;
                map_serializer.serialize_entry("type", &seq_type)?;
            },
            DiffAction::RemoveMapKey(oid, map_type, key) => {
                map_serializer.serialize_entry("action", "remove")?;
                map_serializer.serialize_entry("type", &map_type)?;
                map_serializer.serialize_entry("obj", &oid)?;
                map_serializer.serialize_entry("key", &key)?;
            },
            DiffAction::SetMapKey(oid, map_type, key, value, datatype) => {
                map_serializer.serialize_entry("action", "set")?;
                map_serializer.serialize_entry("type", &map_type)?;
                map_serializer.serialize_entry("obj", &oid)?;
                map_serializer.serialize_entry("key", &key)?;
                match datatype {
                    Some(dtype) => map_serializer.serialize_entry("datatype", &dtype)?,
                    None => {}
                };
                match value {
                    ElementValue::Primitive(v) => map_serializer.serialize_entry("value", &v)?,
                    ElementValue::Link(linked_oid) => {
                        map_serializer.serialize_entry("link", &true)?;
                        map_serializer.serialize_entry("value", &linked_oid)?;
                    }
                };
            },
            DiffAction::RemoveSequenceElement(oid, seq_type, index) => {
                map_serializer.serialize_entry("action", "remove")?;
                map_serializer.serialize_entry("type", &seq_type)?;
                map_serializer.serialize_entry("obj", &oid)?;
                map_serializer.serialize_entry("index", &index)?;
            }
            DiffAction::InsertSequenceElement(oid, seq_type, index, value, datatype) => {
                map_serializer.serialize_entry("action", "insert")?;
                map_serializer.serialize_entry("type", &seq_type)?;
                map_serializer.serialize_entry("obj", &oid)?;
                map_serializer.serialize_entry("index", &index)?;
                match value {
                    ElementValue::Primitive(v) => map_serializer.serialize_entry("value", &v)?,
                    ElementValue::Link(linked_oid) => {
                        map_serializer.serialize_entry("link", &true)?;
                        map_serializer.serialize_entry("value", &linked_oid)?;
                    }
                };
                match datatype {
                    Some(d) => map_serializer.serialize_entry("datatype", &d)?,
                    None => {}
                };
            },
            DiffAction::SetSequenceElement(oid, seq_type, index, value, datatype) => {
                map_serializer.serialize_entry("action", "set")?;
                map_serializer.serialize_entry("type", &seq_type)?;
                map_serializer.serialize_entry("obj", &oid)?;
                map_serializer.serialize_entry("index", &index)?;
                match value {
                    ElementValue::Primitive(v) => map_serializer.serialize_entry("value", &v)?,
                    ElementValue::Link(linked_oid) => {
                        map_serializer.serialize_entry("link", &true)?;
                        map_serializer.serialize_entry("value", &linked_oid)?;
                    }
                };
                match datatype {
                    Some(d) => map_serializer.serialize_entry("datatype", &d)?,
                    None => {}
                };
            }
        }
        map_serializer.end()
    }
}

#[derive(Serialize)]
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
