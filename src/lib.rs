use serde::{Deserialize, Serialize, Deserializer, Serializer};
use serde::de::{self, Visitor};
use std::collections::HashMap;
use std::fmt;

#[derive(Deserialize, Serialize, Eq, PartialEq, Debug)]
struct ObjectID(String);
#[derive(Deserialize, Serialize, PartialEq, Eq, Debug)]
struct Key(String);
#[derive(Deserialize, Serialize, Eq, PartialEq, Hash, Debug)]
struct ActorID(String);
#[derive(Deserialize, Serialize, PartialEq, Eq, Debug)]
struct Clock(HashMap<ActorID, u32>);

#[derive(Deserialize, Serialize, PartialEq, Debug)]
#[serde(untagged)]
enum PrimitiveValue {
    Str(String),
    Number(f64),
    Boolean(bool),
    Null,
}

#[derive(PartialEq, Eq, Debug)]
enum ElementID {
    Object(ObjectID),
    Head,
}

impl <'de> Deserialize<'de> for ElementID {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error> 
        where D: Deserializer<'de> 
    {
        struct ElementIDVisitor;
        impl <'de> Visitor<'de> for ElementIDVisitor {
            type Value = ElementID;
            
            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                formatter.write_str("'_head' or an object ID")
            }

            fn visit_str<E>(self, value: &str) -> Result<ElementID, E> 
                where E: de::Error
            {
                match value {
                    "_head" => Ok(ElementID::Head),
                    id => Ok(ElementID::Object(ObjectID(id.into())))
                }
            }
        }

        deserializer.deserialize_str(ElementIDVisitor)
    }
}

impl Serialize for ElementID {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
        where S: Serializer 
    {
        match self {
            ElementID::Head => serializer.serialize_str("_head"),
            ElementID::Object(ObjectID(id)) => serializer.serialize_str(id)
        }
    }
}

#[derive(Deserialize, Serialize, PartialEq, Debug)]
#[serde(tag="action")]
enum Operation {
    #[serde(rename="makeMap")]
    MakeMap{ 
        #[serde(rename="obj")]
        object_id: ObjectID 
    },
    #[serde(rename="makeList")]
    MakeList{ 
        #[serde(rename="obj")]
        object_id: ObjectID 
    },
    #[serde(rename="makeText")]
    MakeText{ 
        #[serde(rename="obj")]
        object_id: ObjectID 
    },
    #[serde(rename="makeTable")]
    MakeTable{ 
        #[serde(rename="obj")]
        object_id: ObjectID 
    },
    #[serde(rename="ins")]
    Insert{ 
        #[serde(rename="obj")]
        list_id: ObjectID,
        key: ElementID,
        elem: u32 
    },
    #[serde(rename="set")]
    Set{ 
        #[serde(rename="obj")]
        object_id: ObjectID,
        key: Key,
        value: PrimitiveValue 
    },
    #[serde(rename="link")]
    Link{ 
        #[serde(rename="obj")]
        object_id: ObjectID,
        key: Key,
        value: ObjectID 
    },
    #[serde(rename="del")]
    Delete{ 
        #[serde(rename="obj")]
        object_id: ObjectID,
        key: Key
    },
    #[serde(rename="inc")]
    Increment{ 
        #[serde(rename="obj")]
        object_id: ObjectID,
        value: f64
    },
}


#[derive(Deserialize, Serialize, PartialEq, Debug)]
struct Change {
    #[serde(rename="actor")]
    actor_id: ActorID,
    ops: Vec<Operation>,
    seq: u32,
    message: Option<String>
}


#[cfg(test)]
mod tests {
    use super::*;
    use serde_json;

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
                    "key": "2ed3ffe8-0ff3-4671-9777-aa16c3e09945",
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
                    "key": "somekeyid",
                    "value": "somevalue"
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
                    "value": 123
                }
            ],
            "actor": "741e7221-11cc-4ef8-86ee-4279011569fd",
            "seq": 1,
            "deps": {},
            "message": "Initialization"
        }"#;
        let change: Change = serde_json::from_str(&json_str).unwrap();
        assert_eq!(change, Change{
            actor_id: ActorID("741e7221-11cc-4ef8-86ee-4279011569fd".to_string()),
            ops: vec![
                Operation::MakeMap{object_id: ObjectID( "2ed3ffe8-0ff3-4671-9777-aa16c3e09945".to_string())},
                Operation::MakeList{object_id: ObjectID( "2ed3ffe8-0ff3-4671-9777-aa16c3e09945".to_string())},
                Operation::MakeText{object_id: ObjectID( "2ed3ffe8-0ff3-4671-9777-aa16c3e09945".to_string())},
                Operation::MakeTable{object_id: ObjectID( "2ed3ffe8-0ff3-4671-9777-aa16c3e09945".to_string())},
                Operation::Insert{
                    list_id: ObjectID( "2ed3ffe8-0ff3-4671-9777-aa16c3e09945".to_string()),
                    key: ElementID::Object(ObjectID("2ed3ffe8-0ff3-4671-9777-aa16c3e09945".to_string())),
                    elem: 5,
                },
                Operation::Insert{
                    list_id: ObjectID( "2ed3ffe8-0ff3-4671-9777-aa16c3e09945".to_string()),
                    key: ElementID::Head,
                    elem: 6,
                },
                Operation::Set{
                    object_id: ObjectID( "2ed3ffe8-0ff3-4671-9777-aa16c3e09945".to_string()),
                    key: Key("somekeyid".to_string()),
                    value: PrimitiveValue::Str("somevalue".to_string())
                },
                Operation::Set{
                    object_id: ObjectID( "2ed3ffe8-0ff3-4671-9777-aa16c3e09945".to_string()),
                    key: Key("somekeyid".to_string()),
                    value: PrimitiveValue::Boolean(true)
                },
                Operation::Set{
                    object_id: ObjectID( "2ed3ffe8-0ff3-4671-9777-aa16c3e09945".to_string()),
                    key: Key("somekeyid".to_string()),
                    value: PrimitiveValue::Number(123.0)
                },
                Operation::Set{
                    object_id: ObjectID( "2ed3ffe8-0ff3-4671-9777-aa16c3e09945".to_string()),
                    key: Key("somekeyid".to_string()),
                    value: PrimitiveValue::Null
                },
                Operation::Link{
                    object_id: ObjectID("00000000-0000-0000-0000-000000000000".to_string()),
                    key: Key("cards".to_string()),
                    value: ObjectID("2ed3ffe8-0ff3-4671-9777-aa16c3e09945".to_string())
                },
                Operation::Delete{
                    object_id: ObjectID( "2ed3ffe8-0ff3-4671-9777-aa16c3e09945".to_string()),
                    key: Key("somekey".to_string())
                },
                Operation::Increment{
                    object_id: ObjectID( "2ed3ffe8-0ff3-4671-9777-aa16c3e09945".to_string()),
                    value: 123.0
                }
            ],
            seq: 1,
            message: Some("Initialization".to_string()),
        });
    }
}
