use crate::error::AutomergeError;
use crate::op_handle::OpHandle;
use crate::op_set::OpSet;
use crate::ordered_set::OrderedSet;
use crate::protocol::{
    ActorID, Clock, DataType, Key, ObjType, ObjectID, OpID, OpType, PrimitiveValue,
};
use serde::ser::SerializeMap;
use serde::{Deserialize, Serialize, Serializer};

use std::collections::HashMap;

#[derive(Debug)]
pub(crate) enum PendingDiff {
    SeqSet(OpHandle),
    SeqInsert(OpHandle, usize),
    SeqRemove(OpHandle, usize),
    Map(OpHandle),
    Noop,
}

// The Diff Structure Maps on to the Patch Diffs the Frontend is expecting
// Diff {
//  object_id: 123,
//  obj_type: map,
//  edits: None,
//  props: {
//      "key1": {
//          "10@abc123":
//              DiffLink::Link(Diff {
//                  object_id: 444,
//                  obj_type: list,
//                  edits: [ DiffEdit { ... } ],
//                  props: { ... },
//              })
//          }
//      "key2": {
//          "11@abc123":
//              DiffLink::Val(DiffValue {
//                  value: 10,
//                  datatype: "counter"
//              }
//          }
//      }
// }

#[derive(Serialize, Deserialize, Debug, PartialEq, Clone)]
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
    pub(crate) fn new() -> Diff {
        Diff::init(ObjectID::Root, ObjType::Map)
    }

    pub(crate) fn init(object_id: ObjectID, obj_type: ObjType) -> Diff {
        Diff {
            obj_type,
            object_id,
            edits: None,
            props: None,
        }
    }

    pub(crate) fn add_insert(&mut self, index: usize) -> &mut Diff {
        self.edits
            .get_or_insert_with(Vec::new)
            .push(DiffEdit::Insert { index });
        self
    }

    pub(crate) fn add_remove(&mut self, index: usize) -> &mut Diff {
        self.edits
            .get_or_insert_with(Vec::new)
            .push(DiffEdit::Remove { index });
        self
    }

    pub(crate) fn is_seq(&self) -> bool {
        match self.obj_type {
            ObjType::Map | ObjType::Table => false,
            ObjType::Text | ObjType::List => true,
        }
    }

    pub(crate) fn add_child(&mut self, key: &Key, opid: &OpID, child: Diff) -> &mut Diff {
        self.prop_key(key)
            .insert(opid.clone(), DiffLink::Link(child));
        self.prop_key(key).get_mut(&opid).unwrap().get_ref()
    }

    fn touch(&mut self, key: &Key) {
        if self.is_seq() {
            self.props.get_or_insert_with(HashMap::new);
            self.edits.get_or_insert_with(Vec::new);
        } else {
            self.props
                .get_or_insert_with(HashMap::new)
                .entry(key.clone())
                .or_insert_with(HashMap::new);
        }
    }

    pub(crate) fn add_values(
        &mut self,
        key: &Key,
        ops: &[OpHandle],
        op_set: &OpSet,
    ) -> Result<&mut Diff, AutomergeError> {
        self.touch(key);

        for op in ops.iter() {
            self.add_value(key, &op, op_set)?;
        }

        Ok(self)
    }

    fn prop_key(&mut self, key: &Key) -> &mut HashMap<OpID, DiffLink> {
        self.props
            .get_or_insert_with(HashMap::new)
            .entry(key.clone())
            .or_insert_with(HashMap::new)
    }

    pub(crate) fn add_value(
        &mut self,
        key: &Key,
        op: &OpHandle,
        op_set: &OpSet,
    ) -> Result<&mut Diff, AutomergeError> {
        Ok(match &op.action {
            OpType::Link(child_id) => {
                self.add_child(&key, &op.id, op_set.construct_object(child_id)?);
                self
            }
            _ => self.add_shallow_value(key, op, op_set)?,
        })
    }

    pub(crate) fn add_shallow_value(
        &mut self,
        key: &Key,
        op: &OpHandle,
        op_set: &OpSet,
    ) -> Result<&mut Diff, AutomergeError> {
        Ok(match &op.action {
            OpType::Set(PrimitiveValue::Counter(_)) => {
                let id = op.id.clone();
                let value = DiffLink::Val(DiffValue {
                    value: op.adjusted_value(),
                    datatype: DataType::Counter,
                });
                self.prop_key(key).insert(id, value);
                self
            }
            OpType::Set(PrimitiveValue::Timestamp(_)) => {
                let id = op.id.clone();
                let value = DiffLink::Val(DiffValue {
                    value: op.adjusted_value(),
                    datatype: DataType::Timestamp,
                });
                self.prop_key(key).insert(id, value);
                self
            }
            OpType::Set(_) => {
                let id = op.id.clone();
                let value = DiffLink::Val(DiffValue {
                    value: op.adjusted_value(),
                    datatype: DataType::Undefined,
                });
                self.prop_key(key).insert(id, value);
                self
            }
            OpType::Make(obj_type) => {
                let object_id = op.id.to_object_id(); // op.child()
                self.add_child(&key, &op.id, Diff::init(object_id, *obj_type))
            }
            OpType::Link(ref object_id) => {
                let obj_type = op_set.get_obj(&object_id)?.obj_type;
                self.add_child(&key, &op.id, Diff::init(object_id.clone(), obj_type))
            }
            _ => panic!("Inc and Del should never get here"),
        })
    }

    pub(crate) fn expand_path(
        &mut self,
        path: &[&OpHandle],
        op_set: &OpSet,
    ) -> Result<&mut Diff, AutomergeError> {
        match path {
            [] => Ok(self),
            [op, tail @ ..] => {
                let key = &op.operation_key();

                if self.prop_key(key).is_empty() {
                    if let Some(ops) = op_set.get_field_ops(&op.obj, &key) {
                        for i in ops.iter() {
                            self.add_shallow_value(key, &i, op_set)?;
                        }
                    }
                }

                self.get_child(key, op)?.expand_path(tail, op_set)
            }
        }
    }

    fn get_child(&mut self, key: &Key, op: &OpHandle) -> Result<&mut Diff, AutomergeError> {
        let target = &op.child().unwrap();
        for (_, link) in self.prop_key(&key).iter_mut() {
            if let DiffLink::Link(ref mut diff) = link {
                if &diff.object_id == target {
                    return Ok(diff);
                }
            }
        }
        Err(AutomergeError::GetChildFailed(target.clone(), key.clone()))
    }

    // constructed diffs are already remapped
    pub(crate) fn already_remapped(&mut self) -> bool {
        if let Some(p) = &self.props {
            if let Some(key) = p.keys().next() {
                return key.0.parse::<usize>().is_ok();
            }
        }
        false
    }

    pub(crate) fn remap_list_keys(&mut self, op_set: &OpSet) -> Result<(), AutomergeError> {
        if self.already_remapped() {
            return Ok(());
        }
        if self.is_seq() && self.props.is_some() {
            let mut oldprops = self.props.take().unwrap_or_default();
            let mut newprops = HashMap::new();
            //let elemids = op_set.get_elem_ids(&self.object_id)?;
            let elemids = op_set.get_obj(&self.object_id).map(|o| &o.seq)?;
            for (key, keymap) in oldprops.drain() {
                let key_op = key.to_opid()?;
                let index = elemids.index_of(&key_op).ok_or_else(|| {
                    AutomergeError::MissingElement(self.object_id.clone(), key_op.clone())
                })?;
                let new_key = Key(index.to_string());
                newprops.insert(new_key, keymap);
            }
            self.props = Some(newprops);
        }
        if let Some(ref mut props) = self.props {
            for (_, keymap) in props.iter_mut() {
                for (_, link) in keymap.iter_mut() {
                    match link {
                        DiffLink::Val(_) => {}
                        DiffLink::Link(d) => d.remap_list_keys(op_set)?,
                    }
                }
            }
        }
        Ok(())
    }
}

impl Default for Diff {
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

#[derive(Serialize, Debug, PartialEq, Clone)]
#[serde(rename_all = "camelCase")]
pub struct DiffValue {
    pub(crate) value: PrimitiveValue,
    #[serde(skip_serializing_if = "DataType::is_undefined")]
    pub(crate) datatype: DataType,
}

#[derive(Serialize, Debug, PartialEq, Clone)]
#[serde(untagged)]
pub enum DiffLink {
    Link(Diff),
    Val(DiffValue),
}

impl DiffLink {
    fn get_ref(&mut self) -> &mut Diff {
        match self {
            DiffLink::Link(ref mut diff) => diff,
            DiffLink::Val(_) => panic!("DiffLink not a link()"),
        }
    }
}

#[derive(Serialize, Deserialize, Debug, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct Patch {
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub actor: Option<ActorID>,
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub seq: Option<u64>,
    pub clock: Clock,
    pub can_undo: bool,
    pub can_redo: bool,
    pub version: u64,
    #[serde(serialize_with = "Patch::top_level_serialize")]
    pub diffs: Option<Diff>,
}

impl Patch {
    // the default behavior is to return {} for an empty patch
    // this patch implementation comes with ObjectID::Root baked in so this covered
    // the top level scope where not even Root is referenced
    pub(crate) fn top_level_serialize<S>(
        maybe_diff: &Option<Diff>,
        serializer: S,
    ) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        if let Some(diff) = maybe_diff {
            diff.serialize(serializer)
        } else {
            let map = serializer.serialize_map(Some(0))?;
            map.end()
        }
    }
}
