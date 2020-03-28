use crate::op_set::OpSet;
use crate::protocol::{
    ActorID, Clock, DataType, Key, ObjType, ObjectID, OpHandle, OpID, OpType, PrimitiveValue,
};
use serde::{Serialize, Serializer};
use std::collections::HashMap;

#[derive(Debug)]
pub enum PendingDiff {
    Seq(OpHandle, usize),
    Map(OpHandle),
    NoOp,
}

#[derive(Serialize, Debug, PartialEq, Clone)]
#[serde(rename_all = "camelCase")]
pub struct Diff {
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub edits: Option<Vec<DiffEdit>>,
    pub object_id: ObjectID,
    #[serde(rename = "type")]
    pub obj_type: ObjType,
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub props: Option<HashMap<Key, HashMap<OpID, DiffLink>>>,
}

impl Diff {
    pub fn new() -> Diff {
        Diff {
            obj_type: ObjType::Map,
            object_id: ObjectID::Root,
            edits: None,
            props: None,
        }
    }

    pub fn add_insert(&mut self, index: usize) -> &mut Diff {
        self.edits
            .get_or_insert_with(Vec::new)
            .push(DiffEdit::Insert { index });
        self
    }

    pub fn add_remove(&mut self, index: usize) -> &mut Diff {
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

    pub fn touch(&mut self) -> &mut Diff {
        if self.is_seq() {
            self.edits.get_or_insert_with(Vec::new);
        }
        self.props.get_or_insert_with(HashMap::new);
        self
    }

    pub fn add_child(&mut self, key: &Key, opid: &OpID, child: Diff) -> &mut Diff {
        self.props
            .get_or_insert_with(HashMap::new)
            .entry(key.clone())
            .or_insert_with(HashMap::new)
            .insert(opid.clone(), DiffLink::Link(child));
        self
    }

    pub fn add_values(&mut self, key: &Key, ops: &[OpHandle]) -> &mut Diff {
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
                self.add_values(key, tail) // easy to rewrite wo recurion
            }
        }
    }

    //    pub fn add_values2(&mut self, key: &Key, ops: &[OpHandle]) -> &mut Diff {
    pub fn add_value(&mut self, key: &Key, op: &OpHandle) -> &mut Diff {
        match &op.action {
            OpType::Set(_, ref datatype) => {
                let prop = self
                    .props
                    .get_or_insert_with(HashMap::new)
                    .entry(key.clone())
                    .or_insert_with(HashMap::new);
                let key = op.id.clone();
                let val = DiffValue {
                    value: op.adjusted_value(),
                    datatype: datatype.clone(),
                };
                prop.insert(key, DiffLink::Val(val));
                self
            }
            OpType::Make(_) => self.expand(key, op),
            _ => panic!("not implemented"),
            //_ => {}
        }
    }

    pub fn expand_path(
        &mut self,
        path: &[(ObjectID, Key, Key, ObjectID)],
        op_set: &OpSet,
    ) -> &mut Diff {
        match path {
            [] => self,
            [(obj, key, prop, target), tail @ ..] => {
                if !self.has_child(key, target) {
                    op_set
                        .objs
                        .get(obj)
                        .and_then(|obj| obj.props.get(prop))
                        .map(|ops| self.add_values(key, &ops));
                }
                let child = self.get_child(key, target);
                let child = child.unwrap();
                child.expand_path(tail, op_set)
            }
        }
    }

    fn has_child(&self, key: &Key, target: &ObjectID) -> bool {
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

    fn get_child(&mut self, key: &Key, target: &ObjectID) -> Option<&mut Diff> {
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

    fn expand(&mut self, key: &Key, metaop: &OpHandle) -> &mut Diff {
        //        let key = metaop.key();
        let prop = self
            .props
            .get_or_insert_with(HashMap::new)
            .entry(key.clone())
            .or_insert_with(HashMap::new);
        let child = metaop.child().unwrap();
        let obj_type = metaop.obj_type().unwrap();
        let link = prop.entry(metaop.id.clone()).or_insert_with(|| {
            DiffLink::Link(Diff {
                obj_type,
                object_id: child.clone(),
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

impl Default for Diff {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Serialize, Debug, PartialEq, Clone)]
#[serde(rename_all = "camelCase", tag = "action")]
pub enum DiffEdit {
    Insert { index: usize },
    Remove { index: usize },
}

#[derive(Serialize, Debug, PartialEq, Clone)]
#[serde(rename_all = "camelCase")]
pub struct DiffValue {
    value: PrimitiveValue,
    #[serde(skip_serializing_if = "DataType::is_undefined")]
    datatype: DataType,
}

#[derive(Debug, PartialEq, Clone)]
pub enum DiffLink {
    Link(Diff),
    Val(DiffValue),
}

impl Serialize for DiffLink {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match self {
            DiffLink::Link(diff) => diff.serialize(serializer),
            DiffLink::Val(val) => val.serialize(serializer),
        }
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
    pub diffs: Diff,
}
