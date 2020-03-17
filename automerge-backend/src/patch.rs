use crate::{
    ActorID, Clock, DataType, ElementID, Key, OpID, Operation, OperationWithMetadata,
    PrimitiveValue,
};
use serde::{Deserialize, Serialize, Serializer};
use std::collections::HashMap;

#[derive(Debug, PartialEq, Clone)]
pub enum ElementValue {
    Primitive(PrimitiveValue),
    Link(OpID),
}

impl ElementValue {
    pub fn object_id(&self) -> Option<OpID> {
        match self {
            ElementValue::Link(object_id) => Some(object_id.clone()),
            _ => None,
        }
    }
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
    CreateMap(OpID, MapType),
    CreateList(OpID, SequenceType),
    MaxElem(OpID, u32, SequenceType),
    RemoveMapKey(OpID, MapType, Key),
    SetMapKey(OpID, MapType, Key, ElementValue, Option<DataType>),
    RemoveSequenceElement(OpID, SequenceType, u32),
    InsertSequenceElement(
        OpID,
        SequenceType,
        u32,
        ElementValue,
        Option<DataType>,
        ElementID,
    ),
    SetSequenceElement(OpID, SequenceType, u32, ElementValue, Option<DataType>),
}

#[derive(Serialize, Deserialize, Debug, PartialEq, Clone)]
#[serde(rename_all = "camelCase", tag = "type")]
pub enum Diff2 {
    Map(MapDiff),
    Table(MapDiff),
    List(ListDiff),
    Text(ListDiff),
}

impl Diff2 {
    pub fn new() -> Diff2 {
        Diff2::Map(MapDiff {
            object_id: OpID::Root,
            props: HashMap::new(),
        })
    }

    pub fn op(&mut self, key: &Key, path: &[OperationWithMetadata], ops: &[OperationWithMetadata]) {
        match self {
            Diff2::Map(mapdiff) | Diff2::Table(mapdiff) => match path {
                [] => mapdiff.op(key, ops),
                [head, tail @ ..] => {
                    let d = mapdiff.expand(head);
                    d.op(key, tail, ops)
                }
            },
            Diff2::List(listdiff) | Diff2::Text(listdiff) => match path {
                [] => listdiff.op(key, ops),
                [head, tail @ ..] => {
                    let d = listdiff.expand(head);
                    d.op(key, tail, ops)
                }
            },
        }
    }
}

impl Default for Diff2 {
    fn default() -> Self {
        Self::new()
    }
}

impl MapDiff {
    fn expand(&mut self, metaop: &OperationWithMetadata) -> &mut Diff2 {
        let key = metaop.key().unwrap();
        let prop = self.props.entry(key.clone()).or_insert_with(HashMap::new);
        let opid = metaop.opid();
        let subprop = prop
            .entry(opid.clone())
            .or_insert_with(|| match metaop.operation {
                Operation::MakeMap { .. } => DiffLink::Link(Diff2::Map(MapDiff {
                    object_id: opid,
                    props: HashMap::new(),
                })),
                _ => panic!("no implemented"),
            });
        if let DiffLink::Link(ref mut diff2) = subprop {
            return diff2;
        }
        panic!("should not happen");
    }

    pub fn op(&mut self, key: &Key, ops: &[OperationWithMetadata]) {
        let prop = self.props.entry(key.clone()).or_insert_with(HashMap::new);
        ops.iter().for_each(|metaop| match metaop.operation {
            Operation::Set {
                ref value,
                ref datatype,
                ..
            } => {
                let key = metaop.opid();
                let val = DiffValue {
                    value: value.clone(),
                    datatype: datatype.clone(),
                };
                prop.insert(key, DiffLink::Val(val));
            }
            _ => panic!("not implemented"),
        })
    }
}

#[derive(Serialize, Deserialize, Debug, PartialEq, Clone)]
#[serde(rename_all = "camelCase", tag = "action")]
pub enum DiffEdit {
    Insert { index: u32 },
    Remove { index: u32 },
}

#[derive(Serialize, Deserialize, Debug, PartialEq, Clone)]
#[serde(rename_all = "camelCase")]
pub struct MapDiff {
    object_id: OpID,
    props: HashMap<Key, HashMap<OpID, DiffLink>>,
}

#[derive(Serialize, Deserialize, Debug, PartialEq, Clone)]
#[serde(rename_all = "camelCase")]
pub struct ListDiff {
    object_id: OpID,
    edits: Vec<DiffEdit>,
    props: HashMap<String, HashMap<OpID, DiffLink>>,
}

impl ListDiff {
    fn expand(&mut self, _metaop: &OperationWithMetadata) -> &mut Diff2 {
        panic!("not implemented");
    }

    pub fn op(&mut self, _key: &Key, _ops: &[OperationWithMetadata]) {
        panic!("not implemented");
    }
}

#[derive(Serialize, Deserialize, Debug, PartialEq, Clone)]
#[serde(rename_all = "camelCase")]
pub struct DiffValue {
    value: PrimitiveValue,
    #[serde(skip_serializing_if = "Option::is_none", default)]
    datatype: Option<DataType>,
}

#[derive(Deserialize, Debug, PartialEq, Clone)]
pub enum DiffLink {
    Link(Diff2),
    Val(DiffValue),
}

impl Serialize for DiffLink {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match self {
            DiffLink::Link(diff2) => diff2.serialize(serializer),
            DiffLink::Val(val) => val.serialize(serializer),
        }
    }
}

impl DiffAction {
    fn value(&self) -> Option<ElementValue> {
        match self {
            DiffAction::SetMapKey(_, _, _, value, _)
            | DiffAction::InsertSequenceElement(_, _, _, value, _, _)
            | DiffAction::SetSequenceElement(_, _, _, value, _) => Some(value.clone()),
            _ => None,
        }
    }
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

impl Diff {
    pub fn links(&self) -> Vec<OpID> {
        let mut oids = Vec::new();
        if let Some(oid) = self.action.value().and_then(|v| v.object_id()) {
            oids.push(oid)
        }
        for c in self.conflicts.iter() {
            if let Some(oid) = c.value.object_id() {
                oids.push(oid)
            }
        }
        oids
    }
}

#[derive(Serialize, Debug, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct Patch {
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub actor: Option<ActorID>,
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub seq: Option<u32>,
    pub can_undo: bool,
    pub can_redo: bool,
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub clock: Option<Clock>,
    pub version: u32,
    pub diffs: Diff2,
}
