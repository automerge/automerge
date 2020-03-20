use crate::{
    ActorID, Clock, DataType, ElementID, Key, ObjType, OpID, Operation, OperationWithMetadata,
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

pub enum PendingDiff {
    Seq(OperationWithMetadata, usize),
    Map(OperationWithMetadata),
    NoOp,
}

#[derive(Serialize, Deserialize, Debug, PartialEq, Clone)]
#[serde(rename_all = "camelCase")]
pub struct Diff2 {
    object_id: OpID,
    #[serde(skip_serializing_if = "Option::is_none", default)]
    edits: Option<Vec<DiffEdit>>,
    props: HashMap<Key, HashMap<OpID, DiffLink>>,
    #[serde(rename = "type")]
    obj_type: ObjType,
}

impl Diff2 {
    pub fn new() -> Diff2 {
        Diff2 {
            obj_type: ObjType::Map,
            object_id: OpID::Root,
            edits: None,
            props: HashMap::new(),
        }
    }

    pub fn add_insert(&mut self, index: usize) -> &mut Diff2 {
        self.edits
            .get_or_insert_with(Vec::new)
            .push(DiffEdit::Insert { index });
        self
    }

    pub fn add_remove(&mut self, index: usize) -> &mut Diff2 {
        self.edits
            .get_or_insert_with(Vec::new)
            .push(DiffEdit::Remove { index });
        self
    }

    pub fn add_values(&mut self, key: &Key, ops: &[OperationWithMetadata]) -> &mut Diff2 {
        let prop = self.props.entry(key.clone()).or_insert_with(HashMap::new);
        for metaop in ops.iter() {
            match metaop.operation {
                Operation::Set {
                    ref value,
                    ref datatype,
                    ..
                } => {
                    let key = metaop.opid.clone();
                    let val = DiffValue {
                        value: value.clone(),
                        datatype: datatype.clone(),
                    };
                    prop.insert(key, DiffLink::Val(val));
                }
                _ => panic!("not implemented"),
                //_ => {}
            }
        }
        self
    }

    pub fn op(
        &mut self,
        key: &Key,
        path: &[OperationWithMetadata],
        insert: Option<usize>,
        concurrent_ops: &[OperationWithMetadata],
    ) {
        match path {
            [] => {
                if let Some(index) = insert {
                    self.add_insert(index);
                }
                self.add_values(key, concurrent_ops);
            }
            [head, tail @ ..] => {
                let d = self.expand(head);
                d.op(key, tail, insert, concurrent_ops)
            }
        }
    }

    pub fn cd(&mut self, path: &[OperationWithMetadata]) -> &mut Diff2 {
        match path {
            [] => self,
            [head, tail @ ..] => {
                let d = self.expand(head);
                d.cd(tail)
            }
        }
    }

    fn expand(&mut self, metaop: &OperationWithMetadata) -> &mut Diff2 {
        let key = metaop.key();
        let prop = self.props.entry(key.clone()).or_insert_with(HashMap::new);
        let opid = metaop.opid.clone();
        let obj_type = metaop.make_type().unwrap();
        let edits = match obj_type {
            ObjType::Map | ObjType::Table => None,
            ObjType::Text | ObjType::List => Some(Vec::new()),
        };
        let link = prop.entry(opid.clone()).or_insert_with(|| {
            DiffLink::Link(Diff2 {
                obj_type,
                object_id: opid,
                edits,
                props: HashMap::new(),
            })
        });
        if let DiffLink::Link(ref mut diff) = link {
            return diff;
        }
        panic!("Tried to expand into a value {:?}", metaop);
    }
}

impl Default for Diff2 {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Serialize, Deserialize, Debug, PartialEq, Clone)]
#[serde(rename_all = "camelCase", tag = "action")]
pub enum DiffEdit {
    Insert { index: usize },
    Remove { index: usize },
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
