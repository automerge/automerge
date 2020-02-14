use crate::{Conflict, Diff, DiffAction, ElementValue};
use serde::de::{Error, MapAccess, Visitor};
use serde::ser::SerializeMap;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::fmt;

impl Serialize for Conflict {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut map_serializer = serializer.serialize_map(None)?;
        map_serializer.serialize_entry("actor", &self.actor)?;
        match &self.datatype {
            Some(d) => map_serializer.serialize_entry("datatype", &d)?,
            None => {}
        };
        match &self.value {
            ElementValue::Primitive(v) => map_serializer.serialize_entry("value", &v)?,
            ElementValue::Link(oid) => {
                map_serializer.serialize_entry("value", &oid)?;
                map_serializer.serialize_entry("link", &true)?;
            }
        };
        map_serializer.end()
    }
}

//impl<'de> Deserialize<'de> for Conflict {
//fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
//where D: Deserializer<'de> {
//struct ConflictVisitor;
//impl <'de> Visitor<'de> for ConflictVisitor {
//type Value = Conflict;

//fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
//formatter.write_str("A conflict object")
//}

//fn visit_map<V>(self, mut map: V) -> Result<Conflict, V::Error>
//where V: MapAccess<'de> {
//let mut actor = None;
//let mut value = None;
//let mut datatype = None;

//while let Some((key, value)) = map.next_key()? {
//match key {
//"actor" => {
//if actor.is_some() {
//return Err(Error::duplicate_field("actor"));
//}
//actor = Some(value)
//}
//}
//}

//let actor = actor.ok_or_else(|| Error::missing_field("actor"))?;
//let value = value.ok_or_else(|| Error::missing_field("value"))?;
//Ok(Conflict{
//actor,
//value,
//datatype,
//})
//}
//}
//deserializer.deserialize_struct(
//"Conflict",
//&["actor", "value", "datatype", "link"],
//ConflictVisitor,
//)
//}
//}

impl Serialize for Diff {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut map_serializer = serializer.serialize_map(None)?;
        map_serializer.serialize_entry("conflicts", &self.conflicts)?;
        match &self.action {
            DiffAction::CreateMap(oid, map_type) => {
                map_serializer.serialize_entry("action", "create")?;
                map_serializer.serialize_entry("obj", &oid)?;
                map_serializer.serialize_entry("type", &map_type)?;
            }
            DiffAction::CreateList(oid, seq_type) => {
                map_serializer.serialize_entry("action", "create")?;
                map_serializer.serialize_entry("obj", &oid)?;
                map_serializer.serialize_entry("type", &seq_type)?;
            }
            DiffAction::MaxElem(oid, max, seq_type) => {
                map_serializer.serialize_entry("action", "maxElem")?;
                map_serializer.serialize_entry("obj", &oid)?;
                map_serializer.serialize_entry("value", &max)?;
                map_serializer.serialize_entry("type", &seq_type)?;
            }
            DiffAction::RemoveMapKey(oid, map_type, key) => {
                map_serializer.serialize_entry("action", "remove")?;
                map_serializer.serialize_entry("type", &map_type)?;
                map_serializer.serialize_entry("obj", &oid)?;
                map_serializer.serialize_entry("key", &key)?;
            }
            DiffAction::SetMapKey(oid, map_type, key, value, datatype) => {
                map_serializer.serialize_entry("action", "set")?;
                map_serializer.serialize_entry("type", &map_type)?;
                map_serializer.serialize_entry("obj", &oid)?;
                map_serializer.serialize_entry("key", &key)?;
                match datatype {
                    Some(dtype) => map_serializer.serialize_entry("datatype", &dtype)?,
                    None => {}
                };
                match value {
                    ElementValue::Primitive(v) => map_serializer.serialize_entry("value", &v)?,
                    ElementValue::Link(linked_oid) => {
                        map_serializer.serialize_entry("link", &true)?;
                        map_serializer.serialize_entry("value", &linked_oid)?;
                    }
                };
            }
            DiffAction::RemoveSequenceElement(oid, seq_type, index) => {
                map_serializer.serialize_entry("action", "remove")?;
                map_serializer.serialize_entry("type", &seq_type)?;
                map_serializer.serialize_entry("obj", &oid)?;
                map_serializer.serialize_entry("index", &index)?;
            }
            DiffAction::InsertSequenceElement(oid, seq_type, index, value, datatype) => {
                map_serializer.serialize_entry("action", "insert")?;
                map_serializer.serialize_entry("type", &seq_type)?;
                map_serializer.serialize_entry("obj", &oid)?;
                map_serializer.serialize_entry("index", &index)?;
                match value {
                    ElementValue::Primitive(v) => map_serializer.serialize_entry("value", &v)?,
                    ElementValue::Link(linked_oid) => {
                        map_serializer.serialize_entry("link", &true)?;
                        map_serializer.serialize_entry("value", &linked_oid)?;
                    }
                };
                match datatype {
                    Some(d) => map_serializer.serialize_entry("datatype", &d)?,
                    None => {}
                };
            }
            DiffAction::SetSequenceElement(oid, seq_type, index, value, datatype) => {
                map_serializer.serialize_entry("action", "set")?;
                map_serializer.serialize_entry("type", &seq_type)?;
                map_serializer.serialize_entry("obj", &oid)?;
                map_serializer.serialize_entry("index", &index)?;
                match value {
                    ElementValue::Primitive(v) => map_serializer.serialize_entry("value", &v)?,
                    ElementValue::Link(linked_oid) => {
                        map_serializer.serialize_entry("link", &true)?;
                        map_serializer.serialize_entry("value", &linked_oid)?;
                    }
                };
                match datatype {
                    Some(d) => map_serializer.serialize_entry("datatype", &d)?,
                    None => {}
                };
            }
        }
        map_serializer.end()
    }
}

#[cfg(test)]
mod tests {
    //use super::*;
    use crate::{
        DataType, Diff, DiffAction, ElementValue, Key, MapType, ObjectID, PrimitiveValue,
        SequenceType,
    };
    use serde_json;

    struct TestCase {
        name: &'static str,
        diff: Diff,
        json: serde_json::Value,
    }

    #[test]
    fn do_tests() {
        let testcases = vec![
            TestCase {
                name: "CreateMap",
                diff: Diff {
                    action: DiffAction::CreateMap(ObjectID::ID("1234".to_string()), MapType::Map),
                    conflicts: Vec::new(),
                },
                json: serde_json::from_str(
                    r#"
                    {
                        "action": "create",
                        "obj": "1234",
                        "type": "map",
                        "conflicts": []
                    }
                    "#,
                )
                .unwrap(),
            },
            TestCase {
                name: "CreateMap (table)",
                diff: Diff {
                    action: DiffAction::CreateMap(ObjectID::ID("1234".to_string()), MapType::Table),
                    conflicts: Vec::new(),
                },
                json: serde_json::from_str(
                    r#"
                    {
                        "action": "create",
                        "obj": "1234",
                        "type": "table",
                        "conflicts": []
                    }
                    "#,
                )
                .unwrap(),
            },
            TestCase {
                name: "CreateList",
                diff: Diff {
                    action: DiffAction::CreateList(
                        ObjectID::ID("1234".to_string()),
                        SequenceType::List,
                    ),
                    conflicts: Vec::new(),
                },
                json: serde_json::from_str(
                    r#"
                    {
                        "action": "create",
                        "obj": "1234",
                        "type": "list",
                        "conflicts": []
                    }
                    "#,
                )
                .unwrap(),
            },
            TestCase {
                name: "CreateText",
                diff: Diff {
                    action: DiffAction::CreateList(
                        ObjectID::ID("1234".to_string()),
                        SequenceType::Text,
                    ),
                    conflicts: Vec::new(),
                },
                json: serde_json::from_str(
                    r#"
                    {
                        "action": "create",
                        "obj": "1234",
                        "type": "text",
                        "conflicts": []
                    }
                    "#,
                )
                .unwrap(),
            },
            TestCase {
                name: "MaxElem(list)",
                diff: Diff {
                    action: DiffAction::MaxElem(
                        ObjectID::ID("1234".to_string()),
                        4,
                        SequenceType::List,
                    ),
                    conflicts: Vec::new(),
                },
                json: serde_json::from_str(
                    r#"
                    {
                        "action": "maxElem",
                        "obj": "1234",
                        "type": "list",
                        "value": 4,
                        "conflicts": []
                    }
                    "#,
                )
                .unwrap(),
            },
            TestCase {
                name: "MaxElem(text)",
                diff: Diff {
                    action: DiffAction::MaxElem(
                        ObjectID::ID("1234".to_string()),
                        4,
                        SequenceType::Text,
                    ),
                    conflicts: Vec::new(),
                },
                json: serde_json::from_str(
                    r#"
                    {
                        "action": "maxElem",
                        "obj": "1234",
                        "type": "text",
                        "value": 4,
                        "conflicts": []
                    }
                    "#,
                )
                .unwrap(),
            },
            TestCase {
                name: "RemoveMapKey(map)",
                diff: Diff {
                    action: DiffAction::RemoveMapKey(
                        ObjectID::ID("1234".to_string()),
                        MapType::Map,
                        Key("key".to_string()),
                    ),
                    conflicts: Vec::new(),
                },
                json: serde_json::from_str(
                    r#"
                    {
                        "action": "remove",
                        "obj": "1234",
                        "type": "map",
                        "key": "key",
                        "conflicts": []
                    }
                    "#,
                )
                .unwrap(),
            },
            TestCase {
                name: "RemoveMapKey(table)",
                diff: Diff {
                    action: DiffAction::RemoveMapKey(
                        ObjectID::ID("1234".to_string()),
                        MapType::Table,
                        Key("key".to_string()),
                    ),
                    conflicts: Vec::new(),
                },
                json: serde_json::from_str(
                    r#"
                    {
                        "action": "remove",
                        "obj": "1234",
                        "type": "table",
                        "key": "key",
                        "conflicts": []
                    }
                    "#,
                )
                .unwrap(),
            },
            TestCase {
                name: "SetMapKey(map)",
                diff: Diff {
                    action: DiffAction::SetMapKey(
                        ObjectID::ID("1234".to_string()),
                        MapType::Map,
                        Key("key".to_string()),
                        ElementValue::Link(ObjectID::ID("5678".to_string())),
                        None,
                    ),
                    conflicts: Vec::new(),
                },
                json: serde_json::from_str(
                    r#"
                    {
                        "action": "set",
                        "obj": "1234",
                        "type": "map",
                        "key": "key",
                        "value": "5678",
                        "link": true,
                        "conflicts": []
                    }
                    "#,
                )
                .unwrap(),
            },
            TestCase {
                name: "SetMapKey(table) with link",
                diff: Diff {
                    action: DiffAction::SetMapKey(
                        ObjectID::ID("1234".to_string()),
                        MapType::Table,
                        Key("key".to_string()),
                        ElementValue::Link(ObjectID::ID("5678".to_string())),
                        Some(DataType::Counter),
                    ),
                    conflicts: Vec::new(),
                },
                json: serde_json::from_str(
                    r#"
                    {
                        "action": "set",
                        "obj": "1234",
                        "type": "table",
                        "key": "key",
                        "value": "5678",
                        "link": true,
                        "datatype": "counter",
                        "conflicts": []
                    }
                    "#,
                )
                .unwrap(),
            },
            TestCase {
                name: "RemoveSequenceElement",
                diff: Diff {
                    action: DiffAction::RemoveSequenceElement(
                        ObjectID::ID("1234".to_string()),
                        SequenceType::List,
                        5,
                    ),
                    conflicts: Vec::new(),
                },
                json: serde_json::from_str(
                    r#"
                    {
                        "action": "remove",
                        "obj": "1234",
                        "type": "list",
                        "index": 5,
                        "conflicts": []
                    }
                    "#,
                )
                .unwrap(),
            },
            TestCase {
                name: "RemoveSequenceElement(text)",
                diff: Diff {
                    action: DiffAction::RemoveSequenceElement(
                        ObjectID::ID("1234".to_string()),
                        SequenceType::Text,
                        5,
                    ),
                    conflicts: Vec::new(),
                },
                json: serde_json::from_str(
                    r#"
                    {
                        "action": "remove",
                        "obj": "1234",
                        "type": "text",
                        "index": 5,
                        "conflicts": []
                    }
                    "#,
                )
                .unwrap(),
            },
            TestCase {
                name: "InsertSequenceElement",
                diff: Diff {
                    action: DiffAction::InsertSequenceElement(
                        ObjectID::ID("1234".to_string()),
                        SequenceType::List,
                        5,
                        ElementValue::Primitive(PrimitiveValue::Str("hi".to_string())),
                        None,
                    ),
                    conflicts: Vec::new(),
                },
                json: serde_json::from_str(
                    r#"
                    {
                        "action": "insert",
                        "obj": "1234",
                        "type": "list",
                        "index": 5,
                        "value": "hi",
                        "conflicts": []
                    }
                    "#,
                )
                .unwrap(),
            },
            TestCase {
                name: "InsertSequenceElement(text with link and datatype)",
                diff: Diff {
                    action: DiffAction::InsertSequenceElement(
                        ObjectID::ID("1234".to_string()),
                        SequenceType::Text,
                        5,
                        ElementValue::Link(ObjectID::ID("5678".to_string())),
                        Some(DataType::Timestamp),
                    ),
                    conflicts: Vec::new(),
                },
                json: serde_json::from_str(
                    r#"
                    {
                        "action": "insert",
                        "obj": "1234",
                        "type": "text",
                        "index": 5,
                        "value": "5678",
                        "link": true,
                        "conflicts": [],
                        "datatype": "timestamp"
                    }
                    "#,
                )
                .unwrap(),
            },
            TestCase {
                name: "SetSequenceElement",
                diff: Diff {
                    action: DiffAction::SetSequenceElement(
                        ObjectID::ID("1234".to_string()),
                        SequenceType::Text,
                        5,
                        ElementValue::Link(ObjectID::ID("5678".to_string())),
                        None,
                    ),
                    conflicts: Vec::new(),
                },
                json: serde_json::from_str(
                    r#"
                    {
                        "action": "set",
                        "obj": "1234",
                        "type": "text",
                        "index": 5,
                        "value": "5678",
                        "link": true,
                        "conflicts": []
                    }
                    "#,
                ).unwrap(),
            },
            TestCase {
                name: "SetSequenceElement(list with primitive and datatype)",
                diff: Diff {
                    action: DiffAction::SetSequenceElement(
                        ObjectID::ID("1234".to_string()),
                        SequenceType::List,
                        5,
                        ElementValue::Primitive(PrimitiveValue::Str("hi".to_string())),
                        Some(DataType::Counter),
                    ),
                    conflicts: Vec::new(),
                },
                json: serde_json::from_str(
                    r#"
                    {
                        "action": "set",
                        "obj": "1234",
                        "type": "list",
                        "index": 5,
                        "value": "hi",
                        "datatype": "counter",
                        "conflicts": []
                    }
                    "#,
                ).unwrap(),
            }
        ];
        for testcase in testcases {
            let serialized = serde_json::to_value(testcase.diff)
                .expect(&std::format!("Failed to deserialize {}", testcase.name));
            assert_eq!(
                testcase.json, serialized,
                "TestCase {} did not match",
                testcase.name
            );
        }
    }
}
