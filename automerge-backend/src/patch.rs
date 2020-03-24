use crate::{
    ActorID, Clock, DataType, ElementID, Key, ObjType, OpID, OpSet, Operation,
    OperationWithMetadata, PrimitiveValue,
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
    pub object_id: OpID,
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub edits: Option<Vec<DiffEdit>>,
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub props: Option<HashMap<Key, HashMap<OpID, DiffLink>>>,
    #[serde(rename = "type")]
    pub obj_type: ObjType,
}

impl Diff2 {
    pub fn new() -> Diff2 {
        Diff2 {
            obj_type: ObjType::Map,
            object_id: OpID::Root,
            edits: None,
            props: None,
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

    pub fn is_seq(&self) -> bool {
        match self.obj_type {
            ObjType::Map | ObjType::Table => false,
            ObjType::Text | ObjType::List => true,
        }
    }

    pub fn touch(&mut self) -> &mut Diff2 {
        if self.is_seq() {
            self.edits.get_or_insert_with(Vec::new);
        }
        self.props.get_or_insert_with(HashMap::new);
        self
    }

    pub fn add_child(&mut self, key: &Key, opid: &OpID, child: Diff2) -> &mut Diff2 {
        self.props
            .get_or_insert_with(HashMap::new)
            .entry(key.clone())
            .or_insert_with(HashMap::new)
            .insert(opid.clone(), DiffLink::Link(child));
        self
    }

    pub fn add_values(&mut self, key: &Key, ops: &[OperationWithMetadata]) -> &mut Diff2 {
        match ops {
            [] => {
                if self.is_seq() {
                    self.edits.get_or_insert_with(Vec::new);
                }
                self.props
                    .get_or_insert_with(HashMap::new)
                    .entry(key.clone())
                    .or_insert_with(HashMap::new);
                self
            }
            [head] => {
                if self.is_seq() {
                    self.edits.get_or_insert_with(Vec::new);
                }
                self.add_value(key, head)
            }
            [head, tail @ ..] => {
                self.add_value(key, head);
                self.add_values(key, tail) // tail recursion?
            }
        }
    }

    //    pub fn add_values2(&mut self, key: &Key, ops: &[OperationWithMetadata]) -> &mut Diff2 {
    pub fn add_value(&mut self, key: &Key, op: &OperationWithMetadata) -> &mut Diff2 {
        match op.operation {
            Operation::Set {
                ref value,
                ref datatype,
                ..
            } => {
                let prop = self
                    .props
                    .get_or_insert_with(HashMap::new)
                    .entry(key.clone())
                    .or_insert_with(HashMap::new);
                let key = op.opid.clone();
                let val = DiffValue {
                    value: value.clone(),
                    datatype: datatype.clone(),
                };
                prop.insert(key, DiffLink::Val(val));
                self
            }
            Operation::MakeMap { .. }
            | Operation::MakeTable { .. }
            | Operation::MakeList { .. }
            | Operation::MakeText { .. } => self.expand(op),
            _ => panic!("not implemented"),
            //_ => {}
        }
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

    pub fn expand_path(&mut self, path: &[(OpID, &Key, OpID)], op_set: &OpSet) -> &mut Diff2 {
        match path {
            [] => self,
            [(obj, key, target), tail @ ..] => {
                if !self.has_child(key, obj) {
                    op_set
                        .objs
                        .get(obj)
                        .and_then(|obj| obj.props.get(key))
                        .map(|ops| self.add_values(key, &ops));
                }
                let child = self.get_child(key, target).unwrap();
                child.expand_path(tail, op_set)
            }
        }
    }

    fn has_child(&self, key: &Key, target: &OpID) -> bool {
        self.props
            .as_ref()
            .and_then(|p| {
                p.get(key).and_then(|values| {
                    values
                        .iter()
                        .filter_map(|(_, link)| match link {
                            DiffLink::Link(ref diff) => {
                                if diff.object_id == *target {
                                    Some(diff)
                                } else {
                                    None
                                }
                            }
                            _ => None,
                        })
                        .next()
                })
            })
            .is_some()
    }

    fn get_child(&mut self, key: &Key, target: &OpID) -> Option<&mut Diff2> {
        self.props
            .get_or_insert_with(HashMap::new)
            .get_mut(key)
            .and_then(|values| {
                values
                    .iter_mut()
                    .filter_map(|(_, link)| match link {
                        DiffLink::Link(ref mut diff) => {
                            if diff.object_id == *target {
                                Some(diff)
                            } else {
                                None
                            }
                        }
                        _ => None,
                    })
                    .next()
            })
    }

    fn expand(&mut self, metaop: &OperationWithMetadata) -> &mut Diff2 {
        let key = metaop.key();
        let prop = self
            .props
            .get_or_insert_with(HashMap::new)
            .entry(key.clone())
            .or_insert_with(HashMap::new);
        let opid = metaop.opid.clone();
        let obj_type = metaop.make_type().unwrap();
        let link = prop.entry(opid.clone()).or_insert_with(|| {
            DiffLink::Link(Diff2 {
                obj_type,
                object_id: opid,
                edits: None,
                props: None,
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
    pub version: u64,
    pub diffs: Diff2,
}
