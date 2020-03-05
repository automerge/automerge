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
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::cmp::{Ordering, PartialOrd};
use std::collections::HashMap;
use std::str::FromStr;

use crate::error;

#[derive(Eq, PartialEq, Debug, Hash, Clone)]
pub enum ObjectID {
    ID(String),
    Root,
}

impl ObjectID {
    fn parse(s: &str) -> ObjectID {
        match s {
            "00000000-0000-0000-0000-000000000000" => ObjectID::Root,
            _ => ObjectID::ID(s.into()),
        }
    }
}

impl<'de> Deserialize<'de> for ObjectID {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        Ok(ObjectID::parse(&s))
    }
}

impl Serialize for ObjectID {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let id_str = match self {
            ObjectID::Root => "00000000-0000-0000-0000-000000000000",
            ObjectID::ID(id) => id,
        };
        serializer.serialize_str(id_str)
    }
}

#[derive(Deserialize, Serialize, PartialEq, Eq, Debug, Hash, Clone)]
pub struct Key(pub String);

impl Key {
    pub fn as_element_id(&self) -> Result<ElementID, error::InvalidElementID> {
        ElementID::from_str(&self.0)
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

impl Clock {
    pub fn empty() -> Clock {
        Clock(HashMap::new())
    }

    pub fn with_dependency(&self, actor_id: &ActorID, new_seq: u32) -> Clock {
        let mut result = self.0.clone();
        result.insert(actor_id.clone(), new_seq);
        Clock(result)
    }

    pub fn upper_bound(&self, other: &Clock) -> Clock {
        let mut result: HashMap<ActorID, u32> = HashMap::new();
        self.0.iter().for_each(|(actor_id, seq)| {
            result.insert(
                actor_id.clone(),
                max(*seq, *other.0.get(actor_id).unwrap_or(&(0 as u32))),
            );
        });
        other.0.iter().for_each(|(actor_id, seq)| {
            result.insert(
                actor_id.clone(),
                max(*seq, *self.0.get(actor_id).unwrap_or(&(0 as u32))),
            );
        });
        Clock(result)
    }

    pub fn is_before_or_concurrent_with(&self, other: &Clock) -> bool {
        other
            .0
            .iter()
            .all(|(actor_id, seq)| self.0.get(actor_id).unwrap_or(&0) >= seq)
    }

    pub fn seq_for(&self, actor_id: &ActorID) -> u32 {
        *self.0.get(actor_id).unwrap_or(&0)
    }

    /// Returns true if all components of `clock1` are less than or equal to those
    /// of `clock2` (both clocks given as Immutable.js Map objects). Returns false
    /// if there is at least one component in which `clock1` is greater than
    /// `clock2` (that is, either `clock1` is overall greater than `clock2`, or the
    /// clocks are incomparable).
    ///
    /// TODO This feels like it should be a PartialOrd implementation but I
    /// can't figure out quite what that should look like
    ///
    pub fn less_or_equal(&self, other: &Clock) -> bool {
        self.0.iter().chain(other.0.iter()).all(|(actor_id, _)| {
            self.0.get(actor_id).unwrap_or(&0) < other.0.get(actor_id).unwrap_or(&0)
        })
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
    SpecificElementID(ActorID, u32),
}

impl ElementID {
    pub fn as_key(&self) -> Key {
        match self {
            ElementID::Head => Key("_head".to_string()),
            ElementID::SpecificElementID(actor_id, elem) => Key(format!("{}:{}", actor_id.0, elem)),
        }
    }

    pub fn from_actor_and_elem(actor: ActorID, elem: u32) -> ElementID {
        ElementID::SpecificElementID(actor, elem)
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
            ElementID::SpecificElementID(actor_id, elem) => {
                serializer.serialize_str(&format!("{}:{}", actor_id.0, elem))
            }
        }
    }
}

impl FromStr for ElementID {
    type Err = error::InvalidElementID;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "_head" => Ok(ElementID::Head),
            id => {
                let components: Vec<&str> = id.split(':').collect();
                match components.as_slice() {
                    [actor_id, elem_str] => {
                        let elem = u32::from_str(elem_str)
                            .map_err(|_| error::InvalidElementID(id.to_string()))?;
                        Ok(ElementID::SpecificElementID(
                            ActorID((*actor_id).to_string()),
                            elem,
                        ))
                    }
                    _ => Err(error::InvalidElementID(id.to_string())),
                }
            }
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
            (
                ElementID::SpecificElementID(self_actor, self_elem),
                ElementID::SpecificElementID(other_actor, other_elem),
            ) => {
                if self_elem == other_elem {
                    self_actor.cmp(other_actor)
                } else {
                    self_elem.cmp(other_elem)
                }
            }
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
pub enum Operation {
    #[serde(rename = "makeMap")]
    MakeMap {
        #[serde(rename = "obj")]
        object_id: ObjectID,
    },
    #[serde(rename = "makeList")]
    MakeList {
        #[serde(rename = "obj")]
        object_id: ObjectID,
    },
    #[serde(rename = "makeText")]
    MakeText {
        #[serde(rename = "obj")]
        object_id: ObjectID,
    },
    #[serde(rename = "makeTable")]
    MakeTable {
        #[serde(rename = "obj")]
        object_id: ObjectID,
    },
    #[serde(rename = "ins")]
    Insert {
        #[serde(rename = "obj")]
        list_id: ObjectID,
        key: ElementID,
        elem: u32,
    },
    #[serde(rename = "set")]
    Set {
        #[serde(rename = "obj")]
        object_id: ObjectID,
        key: Key,
        value: PrimitiveValue,
        #[serde(skip_serializing_if = "Option::is_none", default)]
        datatype: Option<DataType>,
    },
    #[serde(rename = "link")]
    Link {
        #[serde(rename = "obj")]
        object_id: ObjectID,
        key: Key,
        value: ObjectID,
    },
    #[serde(rename = "del")]
    Delete {
        #[serde(rename = "obj")]
        object_id: ObjectID,
        key: Key,
    },
    #[serde(rename = "inc")]
    Increment {
        #[serde(rename = "obj")]
        object_id: ObjectID,
        key: Key,
        value: f64,
    },
}

impl Operation {
    pub fn object_id(&self) -> &ObjectID {
        match self {
            Operation::MakeMap { object_id }
            | Operation::MakeTable { object_id }
            | Operation::MakeList { object_id }
            | Operation::MakeText { object_id }
            | Operation::Insert {
                list_id: object_id, ..
            }
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
    #[serde(skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
    #[serde(rename = "deps")]
    pub dependencies: Clock,
}

#[derive(PartialEq, Debug, Clone)]
pub struct ChangeRequest {
    pub actor_id: ActorID,
    pub seq: u32,
    pub message: Option<String>,
    pub dependencies: Clock,
    pub undoable: Option<bool>,
    pub request_type: ChangeRequestType,
}

#[derive(PartialEq, Debug, Clone)]
pub enum ChangeRequestType {
    Change(Vec<Operation>),
    Undo,
    Redo,
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json;
    use std::iter::FromIterator;

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
}
