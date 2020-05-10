use crate::op_handle::OpHandle;
use serde::ser::SerializeMap;
use serde::{Deserialize, Serialize, Serializer};
use automerge_protocol::{ChangeHash, ObjType, Key, Value};

use std::collections::HashMap;

#[derive(Debug, Clone, PartialEq)]
pub(crate) enum PendingDiff {
    SeqInsert(OpHandle, usize),
    SeqRemove(OpHandle, usize),
    Set(OpHandle),
}

impl PendingDiff {

    pub fn operation_key(&self) -> Key {
        match self {
            Self::SeqInsert(op, _) => op.operation_key(),
            Self::SeqRemove(op, _) => op.operation_key(),
            Self::Set(op) => op.operation_key(),
        }
    }

    pub fn edit(&self) -> Option<DiffEdit> {
        match *self {
            Self::SeqInsert(_, index) => Some(DiffEdit::Insert { index }),
            Self::SeqRemove(_, index) => Some(DiffEdit::Remove { index }),
            _ => None,
        }
    }
}

// The Diff Structure Maps on to the Patch Diffs the Frontend is expecting
// Diff {
//  object_id: 123,
//  obj_type: map,
//  edits: None,
//  props: {
//      "key1": {
//          "10@abc123":
//              DiffLink::Diff(Diff {
//                  object_id: 444,
//                  obj_type: list,
//                  edits: [ DiffEdit { ... } ],
//                  props: { ... },
//              })
//          }
//      "key2": {
//          "11@abc123":
//              DiffLink::Value(DiffValue {
//                  value: 10,
//                  datatype: "counter"
//              }
//          }
//      }
// }


#[derive(Debug, PartialEq, Clone)]
pub enum Diff {
    Map(MapDiff),
    Seq(SeqDiff),
    Unchanged(ObjDiff),
    Value(Value),
}

impl From<MapDiff> for Diff {
    fn from(m: MapDiff) -> Self {
        Diff::Map(m)
    }
}

impl From<SeqDiff> for Diff {
    fn from(s: SeqDiff) -> Self {
        Diff::Seq(s)
    }
}

impl From<&Value> for Diff {
    fn from(v: &Value) -> Self {
        Diff::Value(v.clone())
    }
}

impl From<Value> for Diff {
    fn from(v: Value) -> Self {
        Diff::Value(v)
    }
}

impl From<&str> for Diff {
    fn from(s: &str) -> Self {
        Diff::Value(s.into())
    }
}

#[derive(Deserialize, Serialize, Debug, PartialEq, Clone)]
#[serde(rename_all = "camelCase")]
pub struct MapDiff {
    pub object_id: String,
    #[serde(rename = "type")]
    pub obj_type: ObjType,
    pub props: HashMap<String, HashMap<String, Diff>>,
}

#[derive(Deserialize, Serialize, Debug, PartialEq, Clone)]
#[serde(rename_all = "camelCase")]
pub struct SeqDiff {
    pub object_id: String,
    #[serde(rename = "type")]
    pub obj_type: ObjType,
    pub edits: Vec<DiffEdit>,
    pub props: HashMap<usize, HashMap<String, Diff>>,
}

#[derive(Deserialize, Serialize, Debug, PartialEq, Clone)]
#[serde(rename_all = "camelCase")]
pub struct ObjDiff {
    pub object_id: String,
    #[serde(rename = "type")]
    pub obj_type: ObjType,
}

#[derive(Deserialize, Serialize, Debug, PartialEq, Clone)]
#[serde(rename_all = "camelCase", tag = "action")]
pub enum DiffEdit {
    Insert { index: usize },
    Remove { index: usize },
}

#[derive(Serialize, Deserialize, Debug, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct Patch {
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub actor: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub seq: Option<u64>,
    pub clock: HashMap<String,u64>,
    pub deps: Vec<ChangeHash>,
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
