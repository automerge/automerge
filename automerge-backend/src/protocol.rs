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
use serde::de;
use serde::de::{Error, MapAccess, Unexpected, Visitor};
use serde::ser::SerializeStruct;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::cmp::{Ordering, PartialOrd};
use std::collections::HashMap;
use std::fmt;
use std::hash::{Hash, Hasher};
use std::ops::Deref;
use std::rc::Rc;
use std::str::FromStr;

use crate::error;
use crate::error::AutomergeError;
use crate::helper;

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
            (Some(seq_str), Some(actor_str), None) => seq_str.parse().ok().map(|seq| {
                OpID::ID(seq, actor_str.to_string())
            }),
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
    #[serde(rename = "undefined")]
    Undefined,
}

impl DataType {
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
    pub fn primitive_value(&self) -> Result<PrimitiveValue, AutomergeError> {
        self.value
            .clone()
            .ok_or(AutomergeError::MissingPrimitiveValue)
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

#[derive(Clone)]
pub struct OpHandle {
    pub id: OpID,
    change: Rc<Change>,
    index: usize,
    delta: f64,
}

impl OpHandle {
    pub fn extract(change: &Rc<Change>) -> Vec<OpHandle> {
        change
            .operations
            .iter()
            .enumerate()
            .map(|(index, _)| {
                let id = OpID::ID(change.start_op + (index as u64), change.actor_id.0.clone());
                OpHandle {
                    id,
                    change: change.clone(),
                    index,
                    delta: 0.0,
                }
            })
            .collect()
    }

    pub fn adjusted_value(&self) -> PrimitiveValue {
        match &self.action {
            OpType::Set(PrimitiveValue::Number(a), DataType::Counter) => {
                PrimitiveValue::Number(a + self.delta)
            }
            OpType::Set(val, _) => val.clone(),
            _ => PrimitiveValue::Null,
        }
    }

    pub fn child(&self) -> Option<ObjectID> {
        match &self.action {
            OpType::Make(_) => Some(self.id.to_object_id()),
            OpType::Link(obj) => Some(obj.clone()),
            _ => None,
        }
    }

    pub fn operation_key(&self) -> Key {
        if self.insert {
            self.id.to_key()
        } else {
            self.key.clone()
        }
    }

    pub fn maybe_increment(&mut self, inc: &OpHandle) {
        if let OpType::Inc(amount) = inc.action {
            if inc.pred.contains(&self.id) {
                if let OpType::Set(PrimitiveValue::Number(_), DataType::Counter) = self.action {
                    self.delta += amount;
                }
            }
        }
    }

    pub fn actor_id(&self) -> &ActorID {
        &self.change.actor_id
    }

    pub fn counter(&self) -> u64 {
        self.change.start_op + (self.index as u64)
    }
}

impl fmt::Debug for OpHandle {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("OpHandle")
            .field("id", &self.id.to_string())
            .field("action", &self.action)
            .field("obj", &self.obj)
            .field("key", &self.key)
            .finish()
    }
}

impl Ord for OpHandle {
    fn cmp(&self, other: &Self) -> Ordering {
        self.id.cmp(&other.id)
    }
}

impl Hash for OpHandle {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.id.hash(state);
    }
}

impl PartialOrd for OpHandle {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl PartialEq for OpHandle {
    // FIXME - what about delta?  this could cause an issue
    fn eq(&self, other: &Self) -> bool {
        self.id.eq(&other.id)
    }
}

impl Eq for OpHandle {}

impl Deref for OpHandle {
    type Target = Operation;

    fn deref(&self) -> &Self::Target {
        &self.change.operations[self.index]
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

impl OpType {
    pub fn to_str(&self) -> &str {
        match self {
            OpType::Make(ObjType::Map) => "makeMap",
            OpType::Make(ObjType::Table) => "makeTable",
            OpType::Make(ObjType::List) => "makeList",
            OpType::Make(ObjType::Text) => "makeText",
            OpType::Del => "del",
            OpType::Link(_) => "link",
            OpType::Inc(_) => "inc",
            OpType::Set(_, _) => "set",
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

    pub fn is_basic_assign(&self) -> bool {
        !self.insert
            && match self.action {
                OpType::Del | OpType::Set(_, _) | OpType::Inc(_) | OpType::Link(_) => true,
                _ => false,
            }
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

impl Serialize for Operation {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut fields = 4;

        if self.insert {
            fields += 1
        }

        match &self.action {
            OpType::Link(_) | OpType::Inc(_) | OpType::Set(_, DataType::Undefined) => fields += 1,
            OpType::Set(_, _) => fields += 2,
            _ => {}
        }

        let mut op = serializer.serialize_struct("Operation", fields)?;
        op.serialize_field("action", &self.action.to_str())?;
        op.serialize_field("obj", &self.obj)?;
        op.serialize_field("key", &self.key)?;
        if self.insert {
            op.serialize_field("insert", &self.insert)?;
        }
        match &self.action {
            OpType::Link(child) => op.serialize_field("child", &child)?,
            OpType::Inc(n) => op.serialize_field("value", &n)?,
            OpType::Set(value, DataType::Undefined) => op.serialize_field("value", &value)?,
            OpType::Set(value, datatype) => {
                op.serialize_field("value", &value)?;
                op.serialize_field("datatype", &datatype)?;
            }
            _ => {}
        }
        op.serialize_field("pred", &self.pred)?;
        op.end()
    }
}

impl<'de> Deserialize<'de> for Operation {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        const FIELDS: &[&str] = &["ops", "deps", "message", "seq", "actor", "requestType"];
        struct OperationVisitor;
        impl<'de> Visitor<'de> for OperationVisitor {
            type Value = Operation;

            fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
                formatter.write_str("An operation object")
            }

            fn visit_map<V>(self, mut map: V) -> Result<Operation, V::Error>
            where
                V: MapAccess<'de>,
            {
                fn read_field<'de, T, M>(
                    name: &'static str,
                    data: &mut Option<T>,
                    map: &mut M,
                ) -> Result<(), M::Error>
                where
                    M: MapAccess<'de>,
                    T: Deserialize<'de>,
                {
                    if data.is_some() {
                        Err(Error::duplicate_field(name))
                    } else {
                        data.replace(map.next_value()?);
                        Ok(())
                    }
                }

                let mut action: Option<ReqOpType> = None;
                let mut obj: Option<ObjectID> = None;
                let mut key: Option<Key> = None;
                let mut pred: Option<Vec<OpID>> = None;
                let mut insert: Option<bool> = None;
                let mut datatype: Option<DataType> = None;
                let mut value: Option<PrimitiveValue> = None;
                let mut child: Option<ObjectID> = None;
                while let Some(field) = map.next_key::<String>()? {
                    match field.as_ref() {
                        "action" => read_field("action", &mut action, &mut map)?,
                        "obj" => read_field("obj", &mut obj, &mut map)?,
                        "key" => read_field("key", &mut key, &mut map)?,
                        "pred" => read_field("pred", &mut pred, &mut map)?,
                        "insert" => read_field("insert", &mut insert, &mut map)?,
                        "datatype" => read_field("datatype", &mut datatype, &mut map)?,
                        "value" => read_field("value", &mut value, &mut map)?,
                        "child" => read_field("child", &mut child, &mut map)?,
                        _ => return Err(Error::unknown_field(&field, FIELDS)),
                    }
                }
                let action = action.ok_or_else(|| Error::missing_field("action"))?;
                let obj = obj.ok_or_else(|| Error::missing_field("obj"))?;
                let key = key.ok_or_else(|| Error::missing_field("key"))?;
                let pred = pred.ok_or_else(|| Error::missing_field("pred"))?;
                let insert = insert.unwrap_or(false);
                let action = match action {
                    ReqOpType::MakeMap => OpType::Make(ObjType::Map),
                    ReqOpType::MakeTable => OpType::Make(ObjType::Table),
                    ReqOpType::MakeList => OpType::Make(ObjType::List),
                    ReqOpType::MakeText => OpType::Make(ObjType::Text),
                    ReqOpType::Del => OpType::Del,
                    ReqOpType::Link => {
                        OpType::Link(child.ok_or_else(|| Error::missing_field("pred"))?)
                    }
                    ReqOpType::Set => OpType::Set(
                        value.ok_or_else(|| Error::missing_field("value"))?,
                        datatype.unwrap_or(DataType::Undefined),
                    ),
                    ReqOpType::Inc => match value {
                        Some(PrimitiveValue::Number(f)) => Ok(OpType::Inc(f)),
                        Some(PrimitiveValue::Str(s)) => {
                            Err(Error::invalid_value(Unexpected::Str(&s), &"a number"))
                        }
                        Some(PrimitiveValue::Boolean(b)) => {
                            Err(Error::invalid_value(Unexpected::Bool(b), &"a number"))
                        }
                        Some(PrimitiveValue::Null) => {
                            Err(Error::invalid_value(Unexpected::Other("null"), &"a number"))
                        }
                        None => Err(Error::missing_field("value")),
                    }?,
                };
                Ok(Operation {
                    action,
                    obj,
                    key,
                    insert,
                    pred,
                })
            }
        }
        deserializer.deserialize_struct("Operation", &FIELDS, OperationVisitor)
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
