use crate::{ActorID, Clock, DataType, Key, ObjectID, PrimitiveValue, ElementID};
use serde::Serialize;

#[derive(Debug, PartialEq, Clone)]
pub enum ElementValue {
    Primitive(PrimitiveValue),
    Link(ObjectID),
}

#[derive(Debug, PartialEq, Clone, Serialize)]
pub enum SequenceType {
    #[serde(rename = "list")]
    List,
    #[serde(rename = "text")]
    Text,
}

#[derive(Debug, PartialEq, Clone, Serialize)]
pub enum MapType {
    #[serde(rename = "map")]
    Map,
    #[serde(rename = "table")]
    Table,
}

#[derive(Debug, Clone, PartialEq)]
pub enum DiffAction {
    CreateMap(ObjectID, MapType),
    CreateList(ObjectID, SequenceType),
    MaxElem(ObjectID, u32, SequenceType),
    RemoveMapKey(ObjectID, MapType, Key),
    SetMapKey(ObjectID, MapType, Key, ElementValue, Option<DataType>),
    RemoveSequenceElement(ObjectID, SequenceType, u32),
    InsertSequenceElement(ObjectID, SequenceType, u32, ElementValue, Option<DataType>, ElementID),
    SetSequenceElement(ObjectID, SequenceType, u32, ElementValue, Option<DataType>),
}

#[derive(Debug, PartialEq, Clone)]
pub struct Conflict {
    pub actor: ActorID,
    pub value: ElementValue,
    pub datatype: Option<DataType>,
}

#[derive(Debug, PartialEq, Clone)]
pub struct Diff {
    pub action: DiffAction,
    pub conflicts: Vec<Conflict>,
}

#[derive(Serialize, Debug, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct Patch {
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub actor: Option<ActorID>,
    pub can_undo: bool,
    pub can_redo: bool,
    pub clock: Clock,
    pub deps: Clock,
    pub diffs: Vec<Diff>,
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub seq: Option<u32>
}

impl Patch {
    pub fn empty() -> Patch {
        Patch {
            actor: None,
            can_undo: false,
            can_redo: false,
            clock: Clock::empty(),
            deps: Clock::empty(),
            diffs: Vec::new(),
            seq: None,
        }
    }
}
