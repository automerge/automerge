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
use std::cmp::{Ordering, PartialOrd};
use std::collections::HashMap;
use std::fmt;
use std::str::FromStr;
use serde::de;
use serde::de::Visitor;
use serde::{Deserialize, Deserializer, Serialize, Serializer};

use crate::error::AutomergeError;
use crate::helper;
use crate::error;

#[derive(Deserialize, Serialize, Debug, Clone, PartialEq, Copy)]
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

impl ObjectID {
    pub fn to_opid(&self) -> Result<&OpID, AutomergeError> {
        match self {
            ObjectID::ID(id) => Ok(id),
            ObjectID::Str(_) => Err(AutomergeError::CantCastToOpID(self.clone())),
            ObjectID::Root => Err(AutomergeError::CantCastToOpID(self.clone())),
        }
    }

    pub fn parse(s: &str) -> ObjectID {
        if s == "00000000-0000-0000-0000-000000000000" {
            ObjectID::Root
        } else if let Some(id) = OpID::parse(s) {
            ObjectID::ID(id)
        } else {
            ObjectID::Str(s.to_string())
        }
    }
}

impl Serialize for ObjectID {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match self {
            ObjectID::ID(id) => id.serialize(serializer),
            ObjectID::Str(str) => serializer.serialize_str(str.as_str()), // FIXME - can i str.serialize(ser)
            ObjectID::Root => serializer.serialize_str("00000000-0000-0000-0000-000000000000"),
        }
    }
}

impl<'de> Deserialize<'de> for ObjectID {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        if s == "00000000-0000-0000-0000-000000000000" {
            Ok(ObjectID::Root)
        } else if let Some(id) = OpID::parse(&s) {
            Ok(ObjectID::ID(id))
        } else {
            Ok(ObjectID::Str(s))
        }
    }
}

#[derive(Eq, PartialEq, Debug, Hash, Clone)]
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

impl PartialOrd for OpID {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
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

    pub fn parse(s: &str) -> Option<OpID> {
        //        match s {
        //            "00000000-0000-0000-0000-000000000000" => Some(OpID::Root),
        //            _ => {
        let mut i = s.split('@');
        match (i.next(), i.next(), i.next()) {
            (Some(seq_str), Some(actor_str), None) => seq_str
                .parse()
                .ok()
                .map(|seq| OpID::ID(seq, actor_str.to_string())),
            _ => None,
        }
        //            }
        //        }
    }
}

impl<'de> Deserialize<'de> for OpID {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        OpID::parse(&s)
            .ok_or_else(|| de::Error::invalid_value(de::Unexpected::Str(&s), &"A valid OpID"))
    }
}

impl Serialize for OpID {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(self.to_string().as_str())
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
        OpID::parse(&self.0).ok_or_else(|| AutomergeError::InvalidOpID(self.0.clone()))
    }
}

#[derive(Deserialize, Serialize, Eq, PartialEq, Hash, Debug, Clone, PartialOrd, Ord)]
pub struct ActorID(pub String);

impl ActorID {
    pub fn random() -> ActorID {
        ActorID(uuid::Uuid::new_v4().to_string())
    }

    pub fn from_string(raw: String) -> ActorID {
        ActorID(raw)
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
    pub fn as_key(&self) -> Key {
        match self {
            ElementID::Head => Key("_head".to_string()),
            ElementID::ID(opid) => Key(opid.to_string()),
        }
    }
}

impl<'de> Deserialize<'de> for ElementID {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        ElementID::from_str(&s).map_err(|_| de::Error::custom("invalid element ID"))
    }
}

impl Serialize for ElementID {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match self {
            ElementID::Head => serializer.serialize_str("_head"),
            ElementID::ID(opid) => opid.serialize(serializer),
        }
    }
}

impl FromStr for ElementID {
    type Err = error::InvalidElementID;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "_head" => Ok(ElementID::Head),
            id => Ok(ElementID::ID(
                OpID::parse(id).ok_or_else(|| error::InvalidElementID(id.to_string()))?,
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

#[derive(Deserialize, Serialize, PartialEq, Debug, Clone)]
pub enum DataType {
    #[serde(rename = "counter")]
    Counter,
    #[serde(rename = "timestamp")]
    Timestamp,
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

impl<'de> Deserialize<'de> for RequestKey {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct RequestKeyVisitor;
        impl<'de> Visitor<'de> for RequestKeyVisitor {
            type Value = RequestKey;

            fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
                formatter.write_str("a number or string")
            }

            fn visit_u64<E>(self, value: u64) -> Result<RequestKey, E>
            where
                E: de::Error,
            {
                Ok(RequestKey::Num(value))
            }

            fn visit_str<E>(self, value: &str) -> Result<RequestKey, E>
            where
                E: de::Error,
            {
                Ok(RequestKey::Str(value.to_string()))
            }
        }
        deserializer.deserialize_any(RequestKeyVisitor)
    }
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
pub struct OpRequest {
    pub action: OpType,
    pub obj: String,
    pub key: RequestKey,
    pub child: Option<String>,
    pub value: Option<PrimitiveValue>,
    pub datatype: Option<DataType>,
    #[serde(default = "helper::make_false")]
    pub insert: bool,
}

impl OpRequest {
    pub fn primitive_value(&self) -> Result<PrimitiveValue, AutomergeError> {
        self.value
            .clone()
            .ok_or(AutomergeError::MissingPrimitiveValue)
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

    pub fn number_value(&self) -> Result<f64, AutomergeError> {
        if let Some(PrimitiveValue::Number(f)) = self.value {
            Ok(f)
        } else {
            Err(AutomergeError::MissingNumberValue)
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
            ObjectID::parse(&text)
        }
    }
}

#[derive(Deserialize, Serialize, PartialEq, Debug, Clone)]
#[serde(tag = "action")]
pub enum Operation {
    #[serde(rename = "makeMap")]
    MakeMap {
        #[serde(rename = "obj")]
        object_id: ObjectID,
        key: Key,
        pred: Vec<OpID>,
        #[serde(skip_serializing_if = "helper::is_false", default)]
        insert: bool,
    },
    #[serde(rename = "makeList")]
    MakeList {
        #[serde(rename = "obj")]
        object_id: ObjectID,
        key: Key,
        pred: Vec<OpID>,
        #[serde(skip_serializing_if = "helper::is_false", default)]
        insert: bool,
    },
    #[serde(rename = "makeText")]
    MakeText {
        #[serde(rename = "obj")]
        object_id: ObjectID,
        key: Key,
        pred: Vec<OpID>,
        #[serde(skip_serializing_if = "helper::is_false", default)]
        insert: bool,
    },
    #[serde(rename = "makeTable")]
    MakeTable {
        #[serde(rename = "obj")]
        object_id: ObjectID,
        key: Key,
        pred: Vec<OpID>,
        #[serde(skip_serializing_if = "helper::is_false", default)]
        insert: bool,
    },
    #[serde(rename = "set")]
    Set {
        #[serde(rename = "obj")]
        object_id: ObjectID,
        key: Key,
        value: PrimitiveValue,
        pred: Vec<OpID>,
        #[serde(skip_serializing_if = "Option::is_none", default)]
        datatype: Option<DataType>,
        #[serde(skip_serializing_if = "helper::is_false", default)]
        insert: bool,
    },
    #[serde(rename = "link")]
    Link {
        #[serde(rename = "obj")]
        object_id: ObjectID,
        key: Key,
        value: OpID,
        pred: Vec<OpID>,
        #[serde(skip_serializing_if = "helper::is_false", default)]
        insert: bool,
    },
    #[serde(rename = "del")]
    Delete {
        #[serde(rename = "obj")]
        object_id: ObjectID,
        key: Key,
        pred: Vec<OpID>,
    },
    #[serde(rename = "inc")]
    Increment {
        #[serde(rename = "obj")]
        object_id: ObjectID,
        key: Key,
        value: f64,
        pred: Vec<OpID>,
        #[serde(skip_serializing_if = "helper::is_false", default)]
        insert: bool,
    },
}

impl Operation {
    pub fn is_make(&self) -> bool {
        match self {
            Operation::MakeMap { .. }
            | Operation::MakeList { .. }
            | Operation::MakeText { .. }
            | Operation::MakeTable { .. } => true,
            _ => false,
        }
    }

    pub fn child(&self, opid: &OpID) -> Option<ObjectID> {
        match &self {
            Operation::MakeMap { .. }
            | Operation::MakeList { .. }
            | Operation::MakeText { .. }
            | Operation::MakeTable { .. } => Some(opid.clone().to_object_id()),
            Operation::Link { .. } => panic!("not implemented"),
            _ => None,
        }
    }

    pub fn obj_type(&self) -> Option<ObjType> {
        match self {
            Operation::MakeMap { .. } => Some(ObjType::Map),
            Operation::MakeTable { .. } => Some(ObjType::Table),
            Operation::MakeList { .. } => Some(ObjType::List),
            Operation::MakeText { .. } => Some(ObjType::Text),
            _ => None,
        }
    }

    pub fn obj(&self) -> &ObjectID {
        match self {
            Operation::MakeMap { object_id, .. }
            | Operation::MakeTable { object_id, .. }
            | Operation::MakeList { object_id, .. }
            | Operation::MakeText { object_id, .. }
//            | Operation::Insert { list_id: object_id, ..  }
            | Operation::Set { object_id, .. }
            | Operation::Link { object_id, .. }
            | Operation::Delete { object_id, .. }
            | Operation::Increment { object_id, .. } => object_id,
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
