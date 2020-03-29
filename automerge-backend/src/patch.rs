use crate::error::AutomergeError;
use crate::op_handle::OpHandle;
use crate::op_set::OpSet;
use crate::protocol::{
    ActorID, Clock, DataType, Key, ObjType, ObjectID, OpID, OpType, PrimitiveValue,
};
use serde::ser::SerializeMap;
use serde::{Serialize, Serializer};

use std::collections::HashMap;

#[derive(Debug)]
pub(crate) enum PendingDiff {
    Seq(OpHandle, usize),
    Map(OpHandle),
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

    // use in construct diff path
    pub(crate) fn add_child(&mut self, key: &Key, opid: &OpID, child: Diff) -> &mut Diff {
        self.props
            .get_or_insert_with(HashMap::new)
            .entry(key.clone())
            .or_insert_with(HashMap::new)
            .insert(opid.clone(), DiffLink::Link(child));
        self
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

    pub(crate) fn add_values(&mut self, key: &Key, ops: &[OpHandle]) -> &mut Diff {
        self.touch(key);

        for op in ops.iter() {
            self.add_value(key, &op);
        }

        self
    }

    fn prop_key(&mut self, key: &Key) -> &mut HashMap<OpID, DiffLink> {
        self.props
            .get_or_insert_with(HashMap::new)
            .entry(key.clone())
            .or_insert_with(HashMap::new)
    }

    pub(crate) fn add_value(&mut self, key: &Key, op: &OpHandle) -> &mut Diff {
        match &op.action {
            OpType::Set(_, ref datatype) => {
                let id = op.id.clone();
                self.prop_key(key).insert(
                    id,
                    DiffLink::Val(DiffValue {
                        value: op.adjusted_value(),
                        datatype: datatype.clone(),
                    }),
                );
                self
            }
            OpType::Make(obj_type) => {
                let id = op.id.clone();
                let object_id = op.id.to_object_id(); // op.child()
                let entry = self.prop_key(key).entry(id);
                let link =
                    entry.or_insert_with(|| DiffLink::Link(Diff::init(object_id, *obj_type)));
                link.get_ref()
            }
            _ => panic!("not implemented"),
        }
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
                            self.add_value(key, &i);
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

    pub(crate) fn remap_list_keys(&mut self, op_set: &OpSet) -> Result<(), AutomergeError> {
        if self.is_seq() && self.props.is_some() {
            let mut oldprops = self.props.take().unwrap_or_default();
            let mut newprops = HashMap::new();
            let elemids = op_set.get_elem_ids(&self.object_id);
            for (key, keymap) in oldprops.drain() {
                let key_op = key.to_opid()?;
                let index = elemids.iter().position(|id| id == &key_op).ok_or_else(|| {
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

impl DiffLink {
    fn get_ref(&mut self) -> &mut Diff {
        match self {
            DiffLink::Link(ref mut diff) => diff,
            DiffLink::Val(_) => panic!("DiffLink not a link()"),
        }
    }
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
