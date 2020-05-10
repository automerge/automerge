//! This module contains types which are deserialized from the changes which
//! are produced by the automerge JS library. Given the following code
//!
//! ```javascript
//! doc = ... // create and edit an automerge document
//! let changes = Automerge.getHistory(doc).map(h => h.change)
//! console.log(JSON.stringify(changes, null, 4))
//! ```
//!

use serde::{Deserialize, Serialize};
use std::hash::Hash;
use std::str::FromStr;

use crate::error;
use crate::error::AutomergeError;
use crate::helper;
use crate::op_handle::OpHandle;
use crate::ordered_set::OrderedSet;
use automerge_protocol::{ActorID, ChangeHash, ObjType, OpID};


#[derive(Eq, PartialEq, Debug, Hash, Clone)]
pub enum ObjectID {
    ID(OpID),
    Root,
}

impl From<&OpID> for ObjectID {
    fn from(o: &OpID) -> Self {
        ObjectID::ID(o.clone())
    }
}

impl FromStr for ObjectID {
    type Err = AutomergeError;

    fn from_str(s: &str) -> Result<ObjectID, Self::Err> {
        if s == "00000000-0000-0000-0000-000000000000" {
            Ok(ObjectID::Root)
        } else if let Ok(id) = OpID::from_str(s) {
            Ok(ObjectID::ID(id))
        } else {
            Err(AutomergeError::InvalidObjectID(s.to_string()))
        }
    }
}


impl From<OpID> for ObjectID {
    fn from(id: OpID) -> Self {
        ObjectID::ID(id)
    }
}

impl From<&str> for Key {
    fn from(s: &str) -> Self {
        Key::Map(s.into())
    }
}

impl From<OpID> for Key {
    fn from(id: OpID) -> Self {
        Key::Seq(ElementID::ID(id))
    }
}

impl From<&OpID> for Key {
    fn from(id: &OpID) -> Self {
        Key::Seq(ElementID::ID(id.clone()))
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

    pub fn as_element_id(&self) -> Result<ElementID, AutomergeError> {
        match self {
            Key::Map(_) => Err(AutomergeError::MapKeyInSeq),
            Key::Seq(eid) => Ok(eid.clone()),
        }
    }

    pub fn to_opid(&self) -> Result<OpID, AutomergeError> {
        match self.as_element_id()? {
            ElementID::ID(id) => Ok(id),
            ElementID::Head => Err(AutomergeError::HeadToOpID),
        }
    }
}

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

    pub fn adjust(self, datatype: DataType) -> Value {
        match datatype {
            DataType::Counter => {
                if let Some(n) = self.to_i64() {
                    Value::Counter(n)
                } else {
                    self
                }
            }
            DataType::Timestamp => {
                if let Some(n) = self.to_i64() {
                    Value::Timestamp(n)
                } else {
                    self
                }
            }
            _ => self,
        }
    }

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

impl From<&str> for Value {
    fn from(s: &str) -> Self {
        Value::Str(s.into())
    }
}

impl From<i64> for Value {
    fn from(n: i64) -> Self {
        Value::Int(n)
    }
}

impl From<u64> for Value {
    fn from(n: u64) -> Self {
        Value::Uint(n)
    }
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

impl From<OpID> for ElementID {
    fn from(o: OpID) -> Self {
        ElementID::ID(o)
    }
}

impl From<&OpID> for ElementID {
    fn from(o: &OpID) -> Self {
        ElementID::ID(o.clone())
    }
}

impl FromStr for ElementID {
    type Err = error::InvalidElementID;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "_head" => Ok(ElementID::Head),
            id => Ok(ElementID::ID(
                OpID::from_str(id).map_err(|_| error::InvalidElementID(id.to_string()))?,
            )),
        }
    }
}

/*
impl PartialOrd for ElementID {
    fn partial_cmp(&self, other: &ElementID) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for ElementID {
    fn cmp(&self, other: &ElementID) -> Ordering {
        match (self, other) {
            (ElementID::Head, ElementID::Head) => Ordering::Equal,
            (ElementID::Head, _) => Ordering::Less,
            (_, ElementID::Head) => Ordering::Greater,
            (ElementID::ID(self_opid), ElementID::ID(other_opid)) => self_opid.cmp(other_opid),
        }
    }
}
*/

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

#[derive(Serialize, Debug, PartialEq, Clone)]
pub enum RequestKey {
    Str(String),
    Num(u64),
}

/*
impl RequestKey {
    pub fn to_key(&self) -> Key {
        Key(format!("{:?}", self))
    }
}
*/

#[derive(Deserialize, PartialEq, Debug, Clone)]
#[serde(rename_all = "camelCase")]
pub enum ReqOpType {
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
pub struct OpRequest {
    pub action: ReqOpType,
    pub obj: String,
    pub key: RequestKey,
    pub child: Option<String>,
    pub value: Option<Value>,
    pub datatype: Option<DataType>,
    #[serde(default = "helper::make_false")]
    pub insert: bool,
}

impl OpRequest {
    pub fn primitive_value(&self) -> Value {
        match (self.value.as_ref().and_then(|v| v.to_i64()), self.datatype) {
            (Some(n), Some(DataType::Counter)) => Value::Counter(n),
            (Some(n), Some(DataType::Timestamp)) => Value::Timestamp(n),
            _ => self.value.clone().unwrap_or(Value::Null),
        }
    }

    pub(crate) fn resolve_key(
        &self,
        id: &OpID,
        ids: &mut dyn OrderedSet<OpID>,
    ) -> Result<Key, AutomergeError> {
        let key = &self.key;
        let insert = self.insert;
        let del = self.action == ReqOpType::Del;
        match key {
            RequestKey::Str(s) => Ok(Key::Map(s.clone())),
            RequestKey::Num(n) => {
                let n: usize = *n as usize;
                (if insert {
                    if n == 0 {
                        ids.insert_index(0, id.clone());
                        Some(Key::head())
                    } else {
                        ids.insert_index(n, id.clone());
                        ids.key_of(n - 1).map(|i| i.into())
                    }
                } else if del {
                    ids.remove_index(n).map(|k| k.into())
                } else {
                    ids.key_of(n).map(|i| i.into())
                })
                .ok_or(AutomergeError::IndexOutOfBounds(n))
            }
        }
    }

    pub fn obj_type(&self) -> Option<ObjType> {
        match self.action {
            ReqOpType::MakeMap => Some(ObjType::Map),
            ReqOpType::MakeTable => Some(ObjType::Table),
            ReqOpType::MakeList => Some(ObjType::List),
            ReqOpType::MakeText => Some(ObjType::Text),
            _ => None,
        }
    }

    pub fn to_i64(&self) -> Result<i64, AutomergeError> {
        self.value
            .as_ref()
            .and_then(|v| v.to_i64())
            .ok_or_else(|| AutomergeError::MissingNumberValue(self.clone()))
    }
}

#[derive(PartialEq, Debug, Clone)]
pub enum OpType {
    Make(ObjType),
    Del,
    Link(ObjectID),
    Inc(i64),
    Set(Value),
}

#[derive(PartialEq, Debug, Clone)]
pub struct UndoOperation {
    pub action: OpType,
    pub obj: ObjectID,
    pub key: Key,
}

impl UndoOperation {
    pub fn into_operation(self, pred: Vec<OpID>) -> Operation {
        Operation {
            action: self.action,
            obj: self.obj,
            key: self.key,
            insert: false,
            pred,
        }
    }
}

#[derive(PartialEq, Debug, Clone)]
pub struct Operation {
    pub action: OpType,
    pub obj: ObjectID,
    pub key: Key,
    pub pred: Vec<OpID>,
    pub insert: bool,
}

impl Operation {
    pub fn set(obj: ObjectID, key: Key, value: Value, pred: Vec<OpID>) -> Operation {
        Operation {
            action: OpType::Set(value),
            obj,
            key,
            insert: false,
            pred,
        }
    }

    pub fn insert(obj: ObjectID, key: Key, value: Value, pred: Vec<OpID>) -> Operation {
        Operation {
            action: OpType::Set(value),
            obj,
            key,
            insert: true,
            pred,
        }
    }

    pub fn inc(obj: ObjectID, key: Key, value: i64, pred: Vec<OpID>) -> Operation {
        Operation {
            action: OpType::Inc(value),
            obj,
            key,
            insert: false,
            pred,
        }
    }

    pub fn del(obj: ObjectID, key: Key, pred: Vec<OpID>) -> Operation {
        Operation {
            action: OpType::Del,
            obj,
            key,
            insert: false,
            pred,
        }
    }

    pub fn is_make(&self) -> bool {
        self.obj_type().is_some()
    }

    pub(crate) fn generate_redos(&self, overwritten: &[OpHandle]) -> Vec<UndoOperation> {
        let key = self.key.clone();

        if let OpType::Inc(value) = self.action {
            vec![UndoOperation {
                action: OpType::Inc(-value),
                obj: self.obj.clone(),
                key,
            }]
        } else if overwritten.is_empty() {
            vec![UndoOperation {
                action: OpType::Del,
                obj: self.obj.clone(),
                key,
            }]
        } else {
            overwritten.iter().map(|o| o.invert(&key)).collect()
        }
    }

    pub fn is_basic_assign(&self) -> bool {
        !self.insert
            && match self.action {
                OpType::Del | OpType::Set(_) | OpType::Inc(_) | OpType::Link(_) => true,
                _ => false,
            }
    }

    pub fn can_merge(&self, other: &Operation) -> bool {
        !self.insert && !other.insert && other.obj == self.obj && other.key == self.key
    }

    pub(crate) fn merge(&mut self, other: Operation) {
        if let OpType::Inc(delta) = other.action {
            match self.action {
                OpType::Set(Value::Counter(number)) => {
                    self.action = OpType::Set(Value::Counter(number + delta))
                }
                OpType::Inc(number) => self.action = OpType::Inc(number + delta),
                _ => {}
            } // error?
        } else {
            match other.action {
                OpType::Set(_) | OpType::Link(_) | OpType::Del => self.action = other.action,
                _ => {}
            }
        }
    }

    pub fn is_inc(&self) -> bool {
        match self.action {
            OpType::Inc(_) => true,
            _ => false,
        }
    }

    pub fn obj_type(&self) -> Option<ObjType> {
        match self.action {
            OpType::Make(t) => Some(t),
            _ => None,
        }
    }
}

#[derive(Deserialize, Serialize, PartialEq, Debug, Clone)]
pub struct Change {
    #[serde(rename = "ops")]
    pub operations: Vec<Operation>,
    #[serde(rename = "actor")]
    pub actor_id: ActorID,
    pub hash: ChangeHash,
    pub seq: u64,
    #[serde(rename = "startOp")]
    pub start_op: u64,
    pub time: i64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
    pub deps: Vec<ChangeHash>,
}

impl Change {
    pub fn max_op(&self) -> u64 {
        self.start_op + (self.operations.len() as u64) - 1
    }
}

#[derive(Deserialize, PartialEq, Debug, Clone)]
#[serde(rename_all = "camelCase")]
pub struct ChangeRequest {
    pub actor: ActorID,
    pub seq: u64,
    pub version: u64,
    pub message: Option<String>,
    #[serde(default = "helper::make_true")]
    pub undoable: bool,
    pub time: Option<i64>,
    pub deps: Option<Vec<ChangeHash>>,
    pub ops: Option<Vec<OpRequest>>,
    pub child: Option<String>,
    pub request_type: ChangeRequestType,
}

fn _true() -> bool {
    true
}

#[derive(Deserialize, PartialEq, Debug, Clone)]
#[serde(rename_all = "camelCase")]
pub enum ChangeRequestType {
    Change,
    Undo,
    Redo,
}
