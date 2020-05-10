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

use crate::error::AutomergeError;
use crate::helper;
use crate::op_handle::OpHandle;
use crate::ordered_set::OrderedSet;
use automerge_protocol::{ActorID, ChangeHash, ObjType, OpID, ObjectID, Key, DataType, Value};


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
