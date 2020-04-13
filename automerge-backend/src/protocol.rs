//! This module contains types which are deserialized from the changes which
//! are produced by the automerge JS library. Given the following code
//!
//! ```javascript
//! doc = ... // create and edit an automerge document
//! let changes = Automerge.getHistory(doc).map(h => h.change)
//! console.log(JSON.stringify(changes, null, 4))
//! ```
//!
//! The output of this can then be deserialized like so
//!
//! ```rust,no_run
//! # use automerge_backend::Change;
//! let changes_str = "<paste the contents of the output here>";
//! let changes: Vec<Change> = serde_json::from_str(changes_str).unwrap();
//! ```
use core::cmp::max;
use serde::{Deserialize, Serialize};
use std::cmp::{Ordering, PartialOrd};
use std::collections::HashMap;
use std::convert::Infallible;
use std::fmt;
use std::hash::Hash;
use std::str::FromStr;

use crate::error;
use crate::error::AutomergeError;
use crate::helper;
use crate::op_handle::OpHandle;

#[derive(Deserialize, Serialize, Debug, Clone, PartialEq, Copy, Hash)]
#[serde(rename_all = "camelCase")]
pub enum ObjType {
    Map,
    Table,
    Text,
    List,
}

#[derive(Eq, PartialEq, Debug, Hash, Clone)]
pub enum ObjectID {
    ID(OpID),
    Str(String),
    Root,
}

impl From<&str> for ObjectID {
    fn from(s: &str) -> ObjectID {
        if s == "00000000-0000-0000-0000-000000000000" {
            ObjectID::Root
        } else if let Some(id) = OpID::from_str(s).ok() {
            ObjectID::ID(id)
        } else {
            ObjectID::Str(s.to_string())
        }
    }
}

impl From<&ObjectID> for String {
    fn from(o: &ObjectID) -> String {
        match o {
            ObjectID::ID(OpID::ID(seq, actor)) => format!("{}@{}", seq, actor),
            ObjectID::Str(s) => s.clone(),
            ObjectID::Root => "00000000-0000-0000-0000-000000000000".into()  
        }
    }
}


#[derive(Eq, PartialEq, Hash, Clone)]
pub enum OpID {
    ID(u64, String),
}

impl Ord for OpID {
    fn cmp(&self, other: &Self) -> Ordering {
        match (self, other) {
            (OpID::ID(counter1, actor1), OpID::ID(counter2, actor2)) => {
                // Lamport compare
                if counter1 != counter2 {
                    counter1.cmp(&counter2)
                } else {
                    actor1.cmp(&actor2)
                }
            }
        }
    }
}

impl fmt::Debug for OpID {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.to_string().as_str())
    }
}

impl PartialOrd for OpID {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl FromStr for OpID {
    type Err = AutomergeError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let mut i = s.split('@');
        match (i.next(), i.next(), i.next()) {
            (Some(seq_str), Some(actor_str), None) => seq_str
                .parse()
                .map(|seq| OpID::ID(seq, actor_str.to_string()))
                .map_err(|_| AutomergeError::InvalidOpID(s.into())),
            _ => Err(AutomergeError::InvalidOpID(s.into())),
        }
    }
}

impl OpID {
    pub fn new(seq: u64, actor: &ActorID) -> OpID {
        OpID::ID(seq, actor.0.clone())
    }

    pub fn to_object_id(&self) -> ObjectID {
        ObjectID::ID(self.clone())
    }

    pub fn to_key(&self) -> Key {
        Key(self.to_string())
    }

    // I think object_id and op_id need to be distinct so there's not a panic here
    pub fn counter(&self) -> u64 {
        match self {
            OpID::ID(counter, _) => *counter,
        }
    }

}

impl fmt::Display for OpID {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            //OpID::Root => write!(f, "00000000-0000-0000-0000-000000000000"),
            OpID::ID(seq, actor) => write!(f, "{}@{}", seq, actor),
        }
    }
}

#[derive(Deserialize, Serialize, PartialEq, Eq, Debug, Hash, Clone)]
pub struct Key(pub String);

impl Key {
    pub fn as_element_id(&self) -> Result<ElementID, AutomergeError> {
        ElementID::from_str(&self.0).map_err(|_| AutomergeError::InvalidChange(format!("Attempted to link, set, delete, or increment an object in a list with invalid element ID {:?}", self.0)))
    }

    pub fn to_opid(&self) -> Result<OpID, AutomergeError> {
        OpID::from_str(&self.0)
    }
}

#[derive(Deserialize, Serialize, Eq, PartialEq, Hash, Debug, Clone, PartialOrd, Ord)]
pub struct ActorID(pub String);

impl ActorID {
    pub fn random() -> ActorID {
        ActorID(uuid::Uuid::new_v4().to_string())
    }
}

impl FromStr for ActorID {
    type Err = Infallible;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(ActorID(s.into()))
    }
}

#[derive(Deserialize, Serialize, PartialEq, Eq, Debug, Clone)]
pub struct Clock(pub HashMap<ActorID, u32>);

impl Default for Clock {
    fn default() -> Self {
        Self::empty()
    }
}

impl Clock {
    pub fn empty() -> Clock {
        Clock(HashMap::new())
    }

    pub fn with(&self, actor_id: &ActorID, seq: u32) -> Clock {
        let mut result = self.clone();
        result.set(actor_id, max(seq, self.get(actor_id)));
        result
    }

    pub fn without(&self, actor_id: &ActorID) -> Clock {
        let mut result = self.clone();
        result.0.remove(actor_id);
        result
    }

    pub fn merge(&mut self, other: &Clock) {
        other.into_iter().for_each(|(actor_id, seq)| {
            self.set(actor_id, max(*seq, self.get(actor_id)));
        });
    }

    pub fn subtract(&mut self, other: &Clock) {
        other.into_iter().for_each(|(actor_id, seq)| {
            if self.get(actor_id) <= *seq {
                self.0.remove(actor_id);
            }
        });
    }

    pub fn union(&self, other: &Clock) -> Clock {
        let mut result = self.clone();
        result.merge(other);
        result
    }

    pub fn set(&mut self, actor_id: &ActorID, seq: u32) {
        if seq == 0 {
            self.0.remove(actor_id);
        } else {
            self.0.insert(actor_id.clone(), seq);
        }
    }

    pub fn get(&self, actor_id: &ActorID) -> u32 {
        *self.0.get(actor_id).unwrap_or(&0)
    }

    pub fn divergent(&self, other: &Clock) -> bool {
        !self.less_or_equal(other)
    }

    fn less_or_equal(&self, other: &Clock) -> bool {
        self.into_iter()
            .all(|(actor_id, _)| self.get(actor_id) <= other.get(actor_id))
    }
}

impl PartialOrd for Clock {
    fn partial_cmp(&self, other: &Clock) -> Option<Ordering> {
        let le1 = self.less_or_equal(other);
        let le2 = other.less_or_equal(self);
        match (le1, le2) {
            (true, true) => Some(Ordering::Equal),
            (true, false) => Some(Ordering::Less),
            (false, true) => Some(Ordering::Greater),
            (false, false) => None,
        }
    }
}

impl<'a> IntoIterator for &'a Clock {
    type Item = (&'a ActorID, &'a u32);
    type IntoIter = ::std::collections::hash_map::Iter<'a, ActorID, u32>;

    fn into_iter(self) -> Self::IntoIter {
        self.0.iter()
    }
}

#[derive(Deserialize, Serialize, PartialEq, Debug, Clone)]
#[serde(untagged)]
pub enum PrimitiveValue {
    Str(String),
    Number(f64),
    Boolean(bool),
    Null,
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

    pub fn as_key(&self) -> Key {
        match self {
            ElementID::Head => Key("_head".to_string()),
            ElementID::ID(opid) => Key(opid.to_string()),
        }
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

impl RequestKey {
    pub fn to_key(&self) -> Key {
        Key(format!("{:?}", self))
    }
}

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
    pub value: Option<PrimitiveValue>,
    pub datatype: Option<DataType>,
    #[serde(default = "helper::make_false")]
    pub insert: bool,
}

impl OpRequest {
    pub fn primitive_value(&self) -> PrimitiveValue {
        self.value.clone().unwrap_or(PrimitiveValue::Null)
    }

    pub fn resolve_key(&self, id: &OpID, ids: &mut Vec<OpID>) -> Result<Key, AutomergeError> {
        let key = &self.key;
        let insert = self.insert;
        let del = self.action == ReqOpType::Del;
        match key {
            RequestKey::Str(s) => Ok(Key(s.clone())),
            RequestKey::Num(n) => {
                let n: usize = *n as usize;
                (if insert {
                    if n == 0 {
                        ids.insert(0, id.clone());
                        Some(Key("_head".to_string()))
                    } else {
                        ids.insert(n, id.clone());
                        ids.get(n - 1).map(|i| i.to_key())
                    }
                } else if del {
                    if n < ids.len() {
                        Some(ids.remove(n).to_key())
                    } else {
                        None
                    }
                } else {
                    ids.get(n).map(|i| i.to_key())
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

    pub fn number_value(&self) -> Result<f64, AutomergeError> {
        if let Some(PrimitiveValue::Number(f)) = self.value {
            Ok(f)
        } else {
            Err(AutomergeError::MissingNumberValue(self.clone()))
        }
    }
}

#[derive(PartialEq, Debug, Clone)]
pub struct ObjAlias(HashMap<String, OpID>);

impl ObjAlias {
    pub fn new() -> ObjAlias {
        ObjAlias(HashMap::new())
    }

    pub fn insert(&mut self, alias: String, id: &OpID) {
        self.0.insert(alias, id.clone());
    }

    pub fn get(&self, text: &str) -> ObjectID {
        if let Some(id) = self.0.get(text) {
            id.to_object_id()
        } else {
            ObjectID::from(text)
        }
    }
}

#[derive(PartialEq, Debug, Clone)]
pub enum OpType {
    Make(ObjType),
    Del,
    Link(ObjectID),
    Inc(f64),
    Set(PrimitiveValue, DataType),
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
                OpType::Del | OpType::Set(_, _) | OpType::Inc(_) | OpType::Link(_) => true,
                _ => false,
            }
    }

    pub fn can_merge(&self, other: &Operation) -> bool {
        !self.insert && !other.insert && other.obj == self.obj && other.key == self.key
    }

    pub(crate) fn merge(&mut self, other: Operation) {
        if let OpType::Inc(delta) = other.action {
            match self.action {
                OpType::Set(PrimitiveValue::Number(number), DataType::Counter) => {
                    self.action =
                        OpType::Set(PrimitiveValue::Number(number + delta), DataType::Counter)
                }
                OpType::Inc(number) => self.action = OpType::Inc(number + delta),
                _ => {}
            } // error?
        } else {
            match other.action {
                OpType::Set(_, _) | OpType::Link(_) | OpType::Del => self.action = other.action,
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
    pub seq: u32,
    #[serde(rename = "startOp")]
    pub start_op: u64,
    pub time: u128,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
    pub deps: Clock,
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
    pub seq: u32,
    pub version: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
    #[serde(default = "helper::make_true")]
    pub undoable: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub deps: Option<Clock>,
    #[serde(skip_serializing_if = "Option::is_none")]
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
