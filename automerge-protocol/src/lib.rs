pub mod error;
mod serde_impls;
mod utility_impls;

use serde::{ser::SerializeMap, Deserialize, Serialize, Serializer};
use std::collections::HashMap;

#[derive(Eq, PartialEq, Hash, Debug, Clone, PartialOrd, Ord)]
pub struct ActorID(Vec<u8>);

impl ActorID {
    pub fn random() -> ActorID {
        ActorID(uuid::Uuid::new_v4().as_bytes().to_vec())
    }

    pub fn to_bytes(&self) -> Vec<u8> {
        self.0.clone()
    }

    pub fn into_bytes(self) -> Vec<u8> {
        self.0
    }

    pub fn from_bytes(bytes: &[u8]) -> ActorID {
        ActorID(bytes.to_vec())
    }

    pub fn to_hex_string(&self) -> String {
        hex::encode(&self.0)
    }

    pub fn op_id_at(&self, seq: u64) -> OpID {
        OpID(seq, self.clone())
    }
}

#[derive(Deserialize, Serialize, Debug, Clone, PartialEq, Copy, Hash)]
#[serde(rename_all = "camelCase")]
pub enum ObjType {
    Map,
    Table,
    Text,
    List,
}

#[derive(Eq, PartialEq, Hash, Clone)]
pub struct OpID(pub u64, pub ActorID);

impl OpID {
    pub fn new(seq: u64, actor: &ActorID) -> OpID {
        OpID(seq, actor.clone())
    }

    pub fn counter(&self) -> u64 {
        self.0
    }
}

#[derive(Eq, PartialEq, Debug, Hash, Clone)]
pub enum ObjectID {
    ID(OpID),
    Root,
}

#[derive(PartialEq, Eq, Debug, Hash, Clone)]
pub enum ElementID {
    Head,
    ID(OpID),
}

impl ElementID {
    pub fn as_opid(&self) -> Option<&OpID> {
        match self {
            ElementID::Head => None,
            ElementID::ID(opid) => Some(opid),
        }
    }

    pub fn into_key(self) -> Key {
        Key::Seq(self)
    }

    pub fn not_head(&self) -> bool {
        match self {
            ElementID::Head => false,
            ElementID::ID(_) => true,
        }
    }
}

#[derive(Serialize, PartialEq, Eq, Debug, Hash, Clone)]
#[serde(untagged)]
pub enum Key {
    Map(String),
    Seq(ElementID),
}

impl Key {
    pub fn head() -> Key {
        Key::Seq(ElementID::Head)
    }

    pub fn as_element_id(&self) -> Option<ElementID> {
        match self {
            Key::Map(_) => None,
            Key::Seq(eid) => Some(eid.clone()),
        }
    }

    pub fn to_opid(&self) -> Option<OpID> {
        match self.as_element_id()? {
            ElementID::ID(id) => Some(id),
            ElementID::Head => None,
        }
    }
}

#[derive(Deserialize, Serialize, PartialEq, Debug, Clone, Copy)]
pub enum DataType {
    #[serde(rename = "counter")]
    Counter,
    #[serde(rename = "timestamp")]
    Timestamp,
    #[serde(rename = "undefined")]
    Undefined,
}

impl DataType {
    #[allow(clippy::trivially_copy_pass_by_ref)]
    pub fn is_undefined(d: &DataType) -> bool {
        match d {
            DataType::Undefined => true,
            _ => false,
        }
    }
}

// TODO I feel like a clearer name for this enum would be `ScalarValue`
#[derive(Serialize, PartialEq, Debug, Clone)]
#[serde(untagged)]
pub enum Value {
    Str(String),
    Int(i64),
    Uint(u64),
    F64(f64),
    F32(f32),
    Counter(i64),
    Timestamp(i64),
    Boolean(bool),
    Null,
}

impl Value {
    pub fn from(val: Option<Value>, datatype: Option<DataType>) -> Option<Value> {
        match datatype {
            Some(DataType::Counter) => Some(Value::Counter(val?.to_i64()?)),
            Some(DataType::Timestamp) => Some(Value::Timestamp(val?.to_i64()?)),
            _ => val,
        }
    }

    /// If this value can be coerced to an i64, return the i64 value
    pub fn to_i64(&self) -> Option<i64> {
        match self {
            Value::Int(n) => Some(*n),
            Value::Uint(n) => Some(*n as i64),
            Value::F32(n) => Some(*n as i64),
            Value::F64(n) => Some(*n as i64),
            Value::Counter(n) => Some(*n),
            Value::Timestamp(n) => Some(*n),
            _ => None,
        }
    }
}

#[derive(Serialize, Debug, PartialEq, Clone)]
pub enum RequestKey {
    Str(String),
    Num(u64),
}

#[derive(Deserialize, PartialEq, Debug, Clone)]
#[serde(rename_all = "camelCase")]
pub enum OpType {
    MakeMap,
    MakeTable,
    MakeList,
    MakeText,
    Del,
    Link,
    Inc,
    Set,
}

#[derive(Deserialize, PartialEq, Debug, Clone)]
pub struct Op {
    pub action: OpType,
    pub obj: String,
    pub key: RequestKey,
    pub child: Option<String>,
    pub value: Option<Value>,
    pub datatype: Option<DataType>,
    #[serde(default = "serde_impls::make_false")]
    pub insert: bool,
}

impl Op {
    pub fn primitive_value(&self) -> Value {
        match (self.value.as_ref().and_then(|v| v.to_i64()), self.datatype) {
            (Some(n), Some(DataType::Counter)) => Value::Counter(n),
            (Some(n), Some(DataType::Timestamp)) => Value::Timestamp(n),
            _ => self.value.clone().unwrap_or(Value::Null),
        }
    }

    pub fn obj_type(&self) -> Option<ObjType> {
        match self.action {
            OpType::MakeMap => Some(ObjType::Map),
            OpType::MakeTable => Some(ObjType::Table),
            OpType::MakeList => Some(ObjType::List),
            OpType::MakeText => Some(ObjType::Text),
            _ => None,
        }
    }

    pub fn to_i64(&self) -> Option<i64> {
        self.value.as_ref().and_then(|v| v.to_i64())
    }
}

#[derive(Eq, PartialEq, Debug, Hash, Clone, PartialOrd, Ord, Copy)]
pub struct ChangeHash(pub [u8; 32]);

#[derive(Deserialize, PartialEq, Debug, Clone)]
#[serde(rename_all = "camelCase")]
pub struct Request {
    pub actor: ActorID,
    pub seq: u64,
    pub version: u64,
    pub message: Option<String>,
    #[serde(default = "serde_impls::make_true")]
    pub undoable: bool,
    pub time: Option<i64>,
    pub deps: Option<Vec<ChangeHash>>,
    pub ops: Option<Vec<Op>>,
    pub request_type: RequestType,
}

#[derive(Deserialize, PartialEq, Debug, Clone)]
#[serde(rename_all = "camelCase")]
pub enum RequestType {
    Change,
    Undo,
    Redo,
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

#[derive(Deserialize, Serialize, Debug, PartialEq, Clone)]
#[serde(rename_all = "camelCase")]
pub struct MapDiff {
    pub object_id: ObjectID,
    #[serde(rename = "type")]
    pub obj_type: ObjType,
    pub props: HashMap<String, HashMap<OpID, Diff>>,
}

#[derive(Deserialize, Serialize, Debug, PartialEq, Clone)]
#[serde(rename_all = "camelCase")]
pub struct SeqDiff {
    pub object_id: ObjectID,
    #[serde(rename = "type")]
    pub obj_type: ObjType,
    pub edits: Vec<DiffEdit>,
    pub props: HashMap<usize, HashMap<OpID, Diff>>,
}

#[derive(Deserialize, Serialize, Debug, PartialEq, Clone)]
#[serde(rename_all = "camelCase")]
pub struct ObjDiff {
    pub object_id: ObjectID,
    #[serde(rename = "type")]
    pub obj_type: ObjType,
}

#[derive(Deserialize, Serialize, Debug, PartialEq, Clone)]
#[serde(rename_all = "camelCase", tag = "action")]
pub enum DiffEdit {
    Insert { index: usize },
    Remove { index: usize },
}

#[derive(Clone, Serialize, Deserialize, Debug, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct Patch {
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub actor: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub seq: Option<u64>,
    pub clock: HashMap<String, u64>,
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
