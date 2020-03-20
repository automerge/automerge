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
use crate::error::AutomergeError;
use core::cmp::max;
use serde::de;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::cmp::{Ordering, PartialOrd};
use std::collections::HashMap;
use std::fmt;
use std::str::FromStr;

use crate::error;

#[derive(Deserialize, Serialize, Debug, Clone, PartialEq)]
#[serde(rename_all = "camelCase")]
pub enum ObjType {
    Map,
    Table,
    Text,
    List,
}

#[derive(Eq, PartialEq, Debug, Hash, Clone)]
pub enum OpID {
    ID(u64, String),
    Root,
}

impl Ord for OpID {
    fn cmp(&self, other: &Self) -> Ordering {
        match (self, other) {
            (OpID::Root, OpID::Root) => Ordering::Equal,
            (_, OpID::Root) => Ordering::Greater,
            (OpID::Root, _) => Ordering::Less,
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

    pub fn to_key(&self) -> Key {
        Key(self.to_string())
    }

    pub fn parse(s: &str) -> Option<OpID> {
        match s {
            "00000000-0000-0000-0000-000000000000" => Some(OpID::Root),
            _ => {
                let mut i = s.split('@');
                match (i.next(), i.next(), i.next()) {
                    (Some(seq_str), Some(actor_str), None) => seq_str
                        .parse()
                        .ok()
                        .map(|seq| OpID::ID(seq, actor_str.to_string())),
                    _ => None,
                }
            }
        }
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
            OpID::Root => write!(f, "00000000-0000-0000-0000-000000000000"),
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

    pub fn merge(&mut self, other: &Clock) {
        other.into_iter().for_each(|(actor_id, seq)| {
            self.set(actor_id, max(*seq, self.get(actor_id)));
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
                OpID::parse(id).ok_or(error::InvalidElementID(id.to_string()))?,
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

#[derive(Deserialize, Serialize, PartialEq, Debug, Clone)]
#[serde(tag = "action")]
pub enum OpRequest {
    #[serde(rename = "makeMap")]
    MakeMap {
        obj: String,
        key: Key,
        child: String,
    },
    #[serde(rename = "set")]
    Set {
        obj: String,
        key: Key,
        insert: Option<bool>,
        value: PrimitiveValue,
    },
}

impl OpRequest {
    pub fn into_op(
        self,
        seq_num: u64,
        actor: &ActorID,
        objmap: &mut HashMap<String, OpID>,
    ) -> Operation {
        match self {
            OpRequest::MakeMap { obj, key, child } => {
                let opid = OpID::parse(&obj).unwrap_or_else(|| objmap.get(&obj).unwrap().clone());
                let this = OpID::ID(seq_num, actor.0.clone());
                objmap.insert(child, this);
                Operation::MakeMap {
                    object_id: opid,
                    key,
                    pred: Vec::new(),
                }
            }
            OpRequest::Set {
                obj,
                key,
                value,
                insert,
            } => {
                let opid = OpID::parse(&obj).unwrap_or_else(|| objmap.get(&obj).unwrap().clone());
                Operation::Set {
                    object_id: opid,
                    key,
                    value,
                    insert,
                    pred: Vec::new(),
                    datatype: None,
                }
            }
        }
    }
}

#[derive(Deserialize, Serialize, PartialEq, Debug, Clone)]
#[serde(tag = "action")]
pub enum Operation {
    #[serde(rename = "makeMap")]
    MakeMap {
        #[serde(rename = "obj")]
        object_id: OpID,
        key: Key,
        pred: Vec<OpID>,
    },
    #[serde(rename = "makeList")]
    MakeList {
        #[serde(rename = "obj")]
        object_id: OpID,
        key: Key,
        pred: Vec<OpID>,
    },
    #[serde(rename = "makeText")]
    MakeText {
        #[serde(rename = "obj")]
        object_id: OpID,
        key: Key,
        pred: Vec<OpID>,
    },
    #[serde(rename = "makeTable")]
    MakeTable {
        #[serde(rename = "obj")]
        object_id: OpID,
        key: Key,
        pred: Vec<OpID>,
    },
    /*
        #[serde(rename = "ins")]
        Insert {
            #[serde(rename = "obj")]
            list_id: OpID,
            key: ElementID,
            elem: u32,
        },
    */
    #[serde(rename = "set")]
    Set {
        #[serde(rename = "obj")]
        object_id: OpID,
        key: Key,
        value: PrimitiveValue,
        pred: Vec<OpID>,
        #[serde(skip_serializing_if = "Option::is_none", default)]
        datatype: Option<DataType>,
        #[serde(skip_serializing_if = "Option::is_none", default)]
        insert: Option<bool>,
    },
    #[serde(rename = "link")]
    Link {
        #[serde(rename = "obj")]
        object_id: OpID,
        key: Key,
        value: OpID,
        pred: Vec<OpID>,
    },
    #[serde(rename = "del")]
    Delete {
        #[serde(rename = "obj")]
        object_id: OpID,
        key: Key,
        pred: Vec<OpID>,
    },
    #[serde(rename = "inc")]
    Increment {
        #[serde(rename = "obj")]
        object_id: OpID,
        key: Key,
        value: f64,
        pred: Vec<OpID>,
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

    pub fn obj(&self) -> &OpID {
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
    pub time: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
    #[serde(rename = "deps")]
    pub dependencies: Clock,
}

/*
#[derive(PartialEq, Debug, Clone)]
pub struct Moment {
  pub actor_id: ActorID,
  pub seq: u32,
}
*/

#[derive(Deserialize, PartialEq, Debug, Clone)]
#[serde(rename_all = "camelCase")]
pub struct ChangeRequest {
    pub actor: ActorID,
    pub seq: u32,
    pub version: u32,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub undoable: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub dependencies: Option<Clock>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ops: Option<Vec<OpRequest>>,
    pub request_type: ChangeRequestType,
}

#[derive(Deserialize, PartialEq, Debug, Clone)]
#[serde(rename_all = "camelCase")]
pub enum ChangeRequestType {
    Change,
    Undo,
    Redo,
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json;
    use std::iter::FromIterator;

    /*
        #[test]
        fn test_deserializing_operations() {
            let json_str = r#"{
                "ops": [
                    {
                        "action": "makeMap",
                        "obj": "2ed3ffe8-0ff3-4671-9777-aa16c3e09945"
                    },
                    {
                        "action": "makeList",
                        "obj": "2ed3ffe8-0ff3-4671-9777-aa16c3e09945"
                    },
                    {
                        "action": "makeText",
                        "obj": "2ed3ffe8-0ff3-4671-9777-aa16c3e09945"
                    },
                    {
                        "action": "makeTable",
                        "obj": "2ed3ffe8-0ff3-4671-9777-aa16c3e09945"
                    },
                    {
                        "action": "ins",
                        "obj": "2ed3ffe8-0ff3-4671-9777-aa16c3e09945",
                        "key": "someactorid:6",
                        "elem": 5
                    },
                    {
                        "action": "ins",
                        "obj": "2ed3ffe8-0ff3-4671-9777-aa16c3e09945",
                        "key": "_head",
                        "elem": 6
                    },
                    {
                        "action": "set",
                        "obj": "2ed3ffe8-0ff3-4671-9777-aa16c3e09945",
                        "key": "sometimestamp",
                        "value": 123456,
                        "datatype": "timestamp"
                    },
                    {
                        "action": "set",
                        "obj": "2ed3ffe8-0ff3-4671-9777-aa16c3e09945",
                        "key": "somekeyid",
                        "value": true
                    },
                    {
                        "action": "set",
                        "obj": "2ed3ffe8-0ff3-4671-9777-aa16c3e09945",
                        "key": "somekeyid",
                        "value": 123
                    },
                    {
                        "action": "set",
                        "obj": "2ed3ffe8-0ff3-4671-9777-aa16c3e09945",
                        "key": "somekeyid",
                        "value": null
                    },
                    {
                        "action": "link",
                        "obj": "00000000-0000-0000-0000-000000000000",
                        "key": "cards",
                        "value": "2ed3ffe8-0ff3-4671-9777-aa16c3e09945"
                    },
                    {
                        "action": "del",
                        "obj": "2ed3ffe8-0ff3-4671-9777-aa16c3e09945",
                        "key": "somekey"
                    },
                    {
                        "action": "inc",
                        "obj": "2ed3ffe8-0ff3-4671-9777-aa16c3e09945",
                        "key": "somekey",
                        "value": 123
                    }
                ],
                "actor": "741e7221-11cc-4ef8-86ee-4279011569fd",
                "seq": 1,
                "deps": {
                    "someid": 0
                },
                "message": "Initialization"
            }"#;
            let change: Change = serde_json::from_str(&json_str).unwrap();
            assert_eq!(
                change,
                Change {
                    actor_id: ActorID("741e7221-11cc-4ef8-86ee-4279011569fd".to_string()),
                    operations: vec![
                        Operation::MakeMap {
                            object_id: ObjectID::ID("2ed3ffe8-0ff3-4671-9777-aa16c3e09945".to_string())
                        },
                        Operation::MakeList {
                            object_id: ObjectID::ID("2ed3ffe8-0ff3-4671-9777-aa16c3e09945".to_string())
                        },
                        Operation::MakeText {
                            object_id: ObjectID::ID("2ed3ffe8-0ff3-4671-9777-aa16c3e09945".to_string())
                        },
                        Operation::MakeTable {
                            object_id: ObjectID::ID("2ed3ffe8-0ff3-4671-9777-aa16c3e09945".to_string())
                        },
                        Operation::Insert {
                            list_id: ObjectID::ID("2ed3ffe8-0ff3-4671-9777-aa16c3e09945".to_string()),
                            key: ElementID::SpecificElementID(ActorID("someactorid".to_string()), 6),
                            elem: 5,
                        },
                        Operation::Insert {
                            list_id: ObjectID::ID("2ed3ffe8-0ff3-4671-9777-aa16c3e09945".to_string()),
                            key: ElementID::Head,
                            elem: 6,
                        },
                        Operation::Set {
                            object_id: ObjectID::ID("2ed3ffe8-0ff3-4671-9777-aa16c3e09945".to_string()),
                            key: Key("sometimestamp".to_string()),
                            value: PrimitiveValue::Number(123_456.0),
                            datatype: Some(DataType::Timestamp)
                        },
                        Operation::Set {
                            object_id: ObjectID::ID("2ed3ffe8-0ff3-4671-9777-aa16c3e09945".to_string()),
                            key: Key("somekeyid".to_string()),
                            value: PrimitiveValue::Boolean(true),
                            datatype: None
                        },
                        Operation::Set {
                            object_id: ObjectID::ID("2ed3ffe8-0ff3-4671-9777-aa16c3e09945".to_string()),
                            key: Key("somekeyid".to_string()),
                            value: PrimitiveValue::Number(123.0),
                            datatype: None,
                        },
                        Operation::Set {
                            object_id: ObjectID::ID("2ed3ffe8-0ff3-4671-9777-aa16c3e09945".to_string()),
                            key: Key("somekeyid".to_string()),
                            value: PrimitiveValue::Null,
                            datatype: None,
                        },
                        Operation::Link {
                            object_id: ObjectID::Root,
                            key: Key("cards".to_string()),
                            value: ObjectID::ID("2ed3ffe8-0ff3-4671-9777-aa16c3e09945".to_string())
                        },
                        Operation::Delete {
                            object_id: ObjectID::ID("2ed3ffe8-0ff3-4671-9777-aa16c3e09945".to_string()),
                            key: Key("somekey".to_string())
                        },
                        Operation::Increment {
                            object_id: ObjectID::ID("2ed3ffe8-0ff3-4671-9777-aa16c3e09945".to_string()),
                            key: Key("somekey".to_string()),
                            value: 123.0,
                        }
                    ],
                    seq: 1,
                    message: Some("Initialization".to_string()),
                    dependencies: Clock(HashMap::from_iter(vec![(ActorID("someid".to_string()), 0)]))
                }
            );
        }

        #[test]
        fn test_deserialize_elementid() {
            let json_str = "\"_head\"";
            let elem: ElementID = serde_json::from_str(json_str).unwrap();
            assert_eq!(elem, ElementID::Head);
        }

        #[test]
        fn test_serialize_elementid() {
            let result = serde_json::to_value(ElementID::Head).unwrap();
            assert_eq!(result, serde_json::Value::String("_head".to_string()));
        }
    */
}
