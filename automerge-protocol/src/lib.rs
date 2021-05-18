pub mod error;
mod serde_impls;
mod utility_impls;
use std::{collections::HashMap, convert::TryFrom, fmt};

use serde::{ser::SerializeMap, Deserialize, Serialize, Serializer};

#[derive(Eq, PartialEq, Hash, Clone, PartialOrd, Ord)]
#[cfg_attr(feature = "derive-arbitrary", derive(arbitrary::Arbitrary))]
pub struct ActorId(Vec<u8>);

impl fmt::Debug for ActorId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple("ActorID")
            .field(&hex::encode(&self.0))
            .finish()
    }
}

impl ActorId {
    pub fn random() -> ActorId {
        ActorId(uuid::Uuid::new_v4().as_bytes().to_vec())
    }

    pub fn to_bytes(&self) -> Vec<u8> {
        self.0.clone()
    }

    pub fn into_bytes(self) -> Vec<u8> {
        self.0
    }

    pub fn from_bytes(bytes: &[u8]) -> ActorId {
        ActorId(bytes.to_vec())
    }

    pub fn to_hex_string(&self) -> String {
        hex::encode(&self.0)
    }

    pub fn op_id_at(&self, seq: u64) -> OpId {
        OpId(seq, self.clone())
    }
}

#[derive(Deserialize, Serialize, Debug, Clone, PartialEq, Copy, Hash)]
#[serde(rename_all = "camelCase", untagged)]
pub enum ObjType {
    Map(MapType),
    Sequence(SequenceType),
}

impl ObjType {
    pub fn map() -> ObjType {
        ObjType::Map(MapType::Map)
    }

    pub fn table() -> ObjType {
        ObjType::Map(MapType::Table)
    }

    pub fn text() -> ObjType {
        ObjType::Sequence(SequenceType::Text)
    }

    pub fn list() -> ObjType {
        ObjType::Sequence(SequenceType::List)
    }
}

#[derive(Deserialize, Serialize, Debug, Clone, PartialEq, Copy, Hash)]
#[cfg_attr(feature = "derive-arbitrary", derive(arbitrary::Arbitrary))]
#[serde(rename_all = "camelCase")]
pub enum MapType {
    Map,
    Table,
}

#[derive(Deserialize, Serialize, Debug, Clone, PartialEq, Copy, Hash)]
#[serde(rename_all = "camelCase")]
pub enum SequenceType {
    List,
    Text,
}

#[derive(Eq, PartialEq, Hash, Clone)]
#[cfg_attr(feature = "derive-arbitrary", derive(arbitrary::Arbitrary))]
pub struct OpId(pub u64, pub ActorId);

impl OpId {
    pub fn new(seq: u64, actor: &ActorId) -> OpId {
        OpId(seq, actor.clone())
    }

    pub fn counter(&self) -> u64 {
        self.0
    }
}

#[derive(Eq, PartialEq, Debug, Hash, Clone)]
#[cfg_attr(feature = "derive-arbitrary", derive(arbitrary::Arbitrary))]
pub enum ObjectId {
    Id(OpId),
    Root,
}

#[derive(PartialEq, Eq, Debug, Hash, Clone)]
pub enum ElementId {
    Head,
    Id(OpId),
}

impl ElementId {
    pub fn as_opid(&self) -> Option<&OpId> {
        match self {
            ElementId::Head => None,
            ElementId::Id(opid) => Some(opid),
        }
    }

    pub fn into_key(self) -> Key {
        Key::Seq(self)
    }

    pub fn not_head(&self) -> bool {
        match self {
            ElementId::Head => false,
            ElementId::Id(_) => true,
        }
    }
}

#[derive(Serialize, PartialEq, Eq, Debug, Hash, Clone)]
#[serde(untagged)]
pub enum Key {
    Map(String),
    Seq(ElementId),
}

impl Key {
    pub fn head() -> Key {
        Key::Seq(ElementId::Head)
    }

    pub fn is_map_key(&self) -> bool {
        match self {
            Key::Map(_) => true,
            Key::Seq(_) => false,
        }
    }

    pub fn as_element_id(&self) -> Option<ElementId> {
        match self {
            Key::Map(_) => None,
            Key::Seq(eid) => Some(eid.clone()),
        }
    }

    pub fn to_opid(&self) -> Option<OpId> {
        match self.as_element_id()? {
            ElementId::Id(id) => Some(id),
            ElementId::Head => None,
        }
    }
}

#[derive(Deserialize, Serialize, PartialEq, Debug, Clone, Copy)]
pub enum DataType {
    #[serde(rename = "counter")]
    Counter,
    #[serde(rename = "timestamp")]
    Timestamp,
    #[serde(rename = "bytes")]
    Bytes,
    #[serde(rename = "cursor")]
    Cursor,
    #[serde(rename = "undefined")]
    Undefined,
}

impl DataType {
    #[allow(clippy::trivially_copy_pass_by_ref)]
    pub fn is_undefined(d: &DataType) -> bool {
        matches!(d, DataType::Undefined)
    }
}

#[derive(Serialize, PartialEq, Debug, Clone)]
#[serde(untagged)]
pub enum ScalarValue {
    Bytes(Vec<u8>),
    Str(String),
    Int(i64),
    Uint(u64),
    F64(f64),
    F32(f32),
    Counter(i64),
    Timestamp(i64),
    Cursor(OpId),
    Boolean(bool),
    Null,
}

impl ScalarValue {
    pub fn as_datatype(
        &self,
        datatype: DataType,
    ) -> Result<ScalarValue, error::InvalidScalarValue> {
        match (datatype, self) {
            (DataType::Counter, ScalarValue::Int(i)) => Ok(ScalarValue::Counter(*i)),
            (DataType::Counter, ScalarValue::Uint(u)) => match i64::try_from(*u) {
                Ok(i) => Ok(ScalarValue::Counter(i)),
                Err(_) => Err(error::InvalidScalarValue {
                    raw_value: self.clone(),
                    expected: "an integer".to_string(),
                    unexpected: "an integer larger than i64::max_value".to_string(),
                    datatype,
                }),
            },
            (DataType::Bytes, ScalarValue::Bytes(bytes)) => Ok(ScalarValue::Bytes(bytes.clone())),
            (DataType::Bytes, v) => Err(error::InvalidScalarValue {
                raw_value: self.clone(),
                expected: "a vec of bytes".to_string(),
                unexpected: v.to_string(),
                datatype,
            }),
            (DataType::Counter, v) => Err(error::InvalidScalarValue {
                raw_value: self.clone(),
                expected: "an integer".to_string(),
                unexpected: v.to_string(),
                datatype,
            }),
            (DataType::Timestamp, ScalarValue::Int(i)) => Ok(ScalarValue::Timestamp(*i)),
            (DataType::Timestamp, ScalarValue::Uint(u)) => match i64::try_from(*u) {
                Ok(i) => Ok(ScalarValue::Timestamp(i)),
                Err(_) => Err(error::InvalidScalarValue {
                    raw_value: self.clone(),
                    expected: "an integer".to_string(),
                    unexpected: "an integer larger than i64::max_value".to_string(),
                    datatype,
                }),
            },
            (DataType::Timestamp, v) => Err(error::InvalidScalarValue {
                raw_value: self.clone(),
                expected: "an integer".to_string(),
                unexpected: v.to_string(),
                datatype,
            }),
            (DataType::Cursor, v) => Err(error::InvalidScalarValue {
                raw_value: self.clone(),
                expected: "a cursor".to_string(),
                unexpected: v.to_string(),
                datatype,
            }),
            (DataType::Undefined, _) => Ok(self.clone()),
        }
    }

    /// If this value can be coerced to an i64, return the i64 value
    pub fn to_i64(&self) -> Option<i64> {
        match self {
            ScalarValue::Int(n) => Some(*n),
            ScalarValue::Uint(n) => Some(*n as i64),
            ScalarValue::F32(n) => Some(*n as i64),
            ScalarValue::F64(n) => Some(*n as i64),
            ScalarValue::Counter(n) => Some(*n),
            ScalarValue::Timestamp(n) => Some(*n),
            _ => None,
        }
    }

    pub fn datatype(&self) -> Option<DataType> {
        match self {
            ScalarValue::Counter(..) => Some(DataType::Counter),
            ScalarValue::Timestamp(..) => Some(DataType::Timestamp),
            _ => None,
        }
    }
}

#[derive(PartialEq, Debug, Clone)]
pub enum OpType {
    Make(ObjType),
    Del,
    Inc(i64),
    Set(ScalarValue),
}

#[derive(PartialEq, Debug, Clone)]
pub struct Op {
    pub action: OpType,
    pub obj: ObjectId,
    pub key: Key,
    pub pred: Vec<OpId>,
    pub insert: bool,
}

impl Op {
    pub fn primitive_value(&self) -> Option<ScalarValue> {
        match &self.action {
            OpType::Set(v) => Some(v.clone()),
            OpType::Inc(i) => Some(ScalarValue::Int(*i)),
            _ => None,
        }
    }

    pub fn obj_type(&self) -> Option<ObjType> {
        match self.action {
            OpType::Make(o) => Some(o),
            _ => None,
        }
    }

    pub fn to_i64(&self) -> Option<i64> {
        self.primitive_value().as_ref().and_then(|v| v.to_i64())
    }
}

#[derive(Eq, PartialEq, Hash, Clone, PartialOrd, Ord, Copy)]
pub struct ChangeHash(pub [u8; 32]);

impl fmt::Debug for ChangeHash {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple("ChangeHash")
            .field(&hex::encode(&self.0))
            .finish()
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
    Value(ScalarValue),
    Cursor(CursorDiff),
}

#[derive(Deserialize, Serialize, Debug, PartialEq, Clone)]
#[serde(rename_all = "camelCase")]
pub struct MapDiff {
    pub object_id: ObjectId,
    #[serde(rename = "type")]
    pub obj_type: MapType,
    #[serde(skip_serializing_if = "HashMap::is_empty", default)]
    pub props: HashMap<String, HashMap<OpId, Diff>>,
}

#[derive(Deserialize, Serialize, Debug, PartialEq, Clone)]
#[serde(rename_all = "camelCase")]
pub struct SeqDiff {
    pub object_id: ObjectId,
    #[serde(rename = "type")]
    pub obj_type: SequenceType,
    pub edits: Vec<DiffEdit>,
    pub props: HashMap<usize, HashMap<OpId, Diff>>,
}

#[derive(Deserialize, Serialize, Debug, PartialEq, Clone)]
#[serde(rename_all = "camelCase")]
pub struct ObjDiff {
    pub object_id: ObjectId,
    #[serde(rename = "type")]
    pub obj_type: ObjType,
}

#[derive(Debug, PartialEq, Clone)]
pub struct CursorDiff {
    pub object_id: ObjectId,
    pub elem_id: OpId,
    pub index: u32,
}

#[derive(Deserialize, Serialize, Debug, PartialEq, Clone)]
#[serde(rename_all = "camelCase", tag = "action")]
pub enum DiffEdit {
    Insert {
        index: usize,
        #[serde(rename = "elemId")]
        elem_id: ElementId,
    },
    Remove {
        index: usize,
    },
}

#[derive(Clone, Serialize, Deserialize, Debug, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct Patch {
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub actor: Option<ActorId>,
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub seq: Option<u64>,
    pub clock: HashMap<ActorId, u64>,
    pub deps: Vec<ChangeHash>,
    pub max_op: u64,
    pub pending_changes: usize,
    //    pub can_undo: bool,
    //    pub can_redo: bool,
    //    pub version: u64,
    #[serde(serialize_with = "Patch::top_level_serialize")]
    pub diffs: Option<Diff>,
}

#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct UncompressedChange {
    #[serde(rename = "ops")]
    pub operations: Vec<Op>,
    #[serde(rename = "actor")]
    pub actor_id: ActorId,
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub hash: Option<ChangeHash>,
    pub seq: u64,
    #[serde(rename = "startOp")]
    pub start_op: u64,
    pub time: i64,
    pub message: Option<String>,
    pub deps: Vec<ChangeHash>,
    #[serde(skip_serializing_if = "Vec::is_empty", default = "Default::default")]
    pub extra_bytes: Vec<u8>,
}

impl PartialEq for UncompressedChange {
    // everything but hash (its computed and not always present)
    fn eq(&self, other: &Self) -> bool {
        self.operations == other.operations
            && self.actor_id == other.actor_id
            && self.seq == other.seq
            && self.start_op == other.start_op
            && self.time == other.time
            && self.message == other.message
            && self.deps == other.deps
            && self.extra_bytes == other.extra_bytes
    }
}

impl UncompressedChange {
    pub fn op_id_of(&self, index: u64) -> Option<OpId> {
        if let Ok(index_usize) = usize::try_from(index) {
            if index_usize < self.operations.len() {
                return Some(self.actor_id.op_id_at(self.start_op + index));
            }
        }
        None
    }
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
