use crate::{
    ActorID, Conflict, DataType, Diff, DiffAction, ElementID, ElementValue, Key, MapType,
    PrimitiveValue, SequenceType, OpID
};
use serde::de::{Error, MapAccess, Unexpected, Visitor};
use serde::ser::SerializeMap;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::fmt;

/*
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

impl<'de> Deserialize<'de> for Conflict {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        const FIELDS: &[&str] = &["actor", "value", "datatype", "link"];
        struct ConflictVisitor;
        impl<'de> Visitor<'de> for ConflictVisitor {
            type Value = Conflict;

            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                formatter.write_str("A conflict object")
            }

            fn visit_map<V>(self, mut map: V) -> Result<Conflict, V::Error>
            where
                V: MapAccess<'de>,
            {
                let mut actor: Option<ActorID> = None;
                let mut value_raw: Option<PrimitiveValue> = None;
                let mut datatype: Option<DataType> = None;
                let mut link: Option<bool> = None;

                while let Some(key) = map.next_key::<String>()? {
                    match key.as_ref() {
                        "actor" => {
                            if actor.is_some() {
                                return Err(Error::duplicate_field("actor"));
                            }
                            actor = Some(map.next_value()?);
                        }
                        "datatype" => {
                            if datatype.is_some() {
                                return Err(Error::duplicate_field("datatype"));
                            }
                            datatype = Some(map.next_value()?);
                        }
                        "value" => {
                            if value_raw.is_some() {
                                return Err(Error::duplicate_field("value"));
                            }
                            value_raw = Some(map.next_value()?);
                        }
                        "link" => {
                            if link.is_some() {
                                return Err(Error::duplicate_field("link"));
                            }
                            link = Some(map.next_value()?);
                        }
                        _ => return Err(Error::unknown_field(&key, FIELDS)),
                    }
                }

                let actor = actor.ok_or_else(|| Error::missing_field("actor"))?;
                let value_raw = value_raw.ok_or_else(|| Error::missing_field("value"))?;
                let is_link = link.unwrap_or(false);
                let value = match (is_link, value_raw) {
                    (true, PrimitiveValue::Str(s)) => {
                        ElementValue::Link(OpID.parse(s).unwrap()) //FIXME - error
                    }
                    (false, v) => ElementValue::Primitive(v),
                    _ => return Err(Error::custom(
                        "Received a conflict with `link` set to true but no string in 'value' key",
                    )),
                };
                Ok(Conflict {
                    actor,
                    value,
                    datatype,
                })
            }
        }
        deserializer.deserialize_struct("Conflict", FIELDS, ConflictVisitor)
    }
}

impl Serialize for Diff {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut map_serializer = serializer.serialize_map(None)?;
        if !self.conflicts.is_empty() {
            map_serializer.serialize_entry("conflicts", &self.conflicts)?;
        }
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
            DiffAction::InsertSequenceElement(
                oid,
                seq_type,
                index,
                value,
                datatype,
                element_id,
            ) => {
                map_serializer.serialize_entry("action", "insert")?;
                map_serializer.serialize_entry("type", &seq_type)?;
                map_serializer.serialize_entry("obj", &oid)?;
                map_serializer.serialize_entry("index", &index)?;
                map_serializer.serialize_entry("elemId", &element_id)?;
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

impl<'de> Deserialize<'de> for Diff {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        const FIELDS: &[&str] = &["actor", "value", "datatype", "link"];
        struct DiffVisitor;
        impl<'de> Visitor<'de> for DiffVisitor {
            type Value = Diff;

            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                formatter.write_str("A diff object")
            }

            fn visit_map<V>(self, mut map: V) -> Result<Diff, V::Error>
            where
                V: MapAccess<'de>,
            {
                let mut object_id: Option<OpID> = None;
                let mut type_str: Option<String> = None;
                let mut seq: Option<u32> = None;
                let mut action: Option<String> = None;
                let mut key: Option<Key> = None;
                let mut value: Option<PrimitiveValue> = None;
                let mut datatype: Option<DataType> = None;
                let mut conflicts: Option<Vec<Conflict>> = None;
                let mut index: Option<u32> = None;
                let mut is_link: Option<bool> = None;
                let mut elem_id: Option<ElementID> = None;

                while let Some(map_key) = map.next_key::<String>()? {
                    match map_key.as_ref() {
                        "obj" => {
                            if object_id.is_some() {
                                return Err(Error::duplicate_field("obj"));
                            }
                            object_id = Some(map.next_value()?);
                        }
                        "type" => {
                            if type_str.is_some() {
                                return Err(Error::duplicate_field("type"));
                            }
                            type_str = Some(map.next_value()?);
                        }
                        "seq" => {
                            if seq.is_some() {
                                return Err(Error::duplicate_field("seq"));
                            }
                            seq = Some(map.next_value()?);
                        }
                        "action" => {
                            if action.is_some() {
                                return Err(Error::duplicate_field("action"));
                            }
                            action = Some(map.next_value()?);
                        }
                        "key" => {
                            if key.is_some() {
                                return Err(Error::duplicate_field("key"));
                            }
                            key = Some(map.next_value()?);
                        }
                        "value" => {
                            if value.is_some() {
                                return Err(Error::duplicate_field("value"));
                            }
                            value = Some(map.next_value()?);
                        }
                        "datatype" => {
                            if datatype.is_some() {
                                return Err(Error::duplicate_field("datatype"));
                            }
                            datatype = Some(map.next_value()?);
                        }
                        "conflicts" => {
                            if conflicts.is_some() {
                                return Err(Error::duplicate_field("conflicts"));
                            }
                            conflicts = Some(map.next_value()?);
                        }
                        "index" => {
                            if index.is_some() {
                                return Err(Error::duplicate_field("index"));
                            }
                            index = Some(map.next_value()?);
                        }
                        "link" => {
                            if is_link.is_some() {
                                return Err(Error::duplicate_field("link"));
                            }
                            is_link = Some(map.next_value()?);
                        }
                        "elemId" => {
                            if elem_id.is_some() {
                                return Err(Error::duplicate_field("elemId"));
                            }
                            elem_id = Some(map.next_value()?);
                        }
                        _ => return Err(Error::unknown_field(&map_key, FIELDS)),
                    }
                }

                let is_link = is_link.unwrap_or(false);
                let value =
                    match (is_link, value) {
                        (true, Some(PrimitiveValue::Str(s))) => {
                            let oid = match s.as_ref() {
                                "00000000-0000-0000-0000-000000000000" => OpID::Root,
                                id => OpID.parse(s).unwrap(), //FIXME unwrap
                            };
                            Some(ElementValue::Link(oid))
                        }
                        (false, Some(v)) => Some(ElementValue::Primitive(v)),
                        (_, None) => None,
                        _ => return Err(Error::custom(
                            "Received a diff with `link` set to true but no string in 'value' key",
                        )),
                    };

                let diff_action = match action {
                    Some(action_str) => match action_str.as_ref() {
                        "create" => {
                            let obj_id = object_id.ok_or_else(|| Error::missing_field("obj"))?;
                            let create_type =
                                type_str.ok_or_else(|| Error::missing_field("type"))?;
                            match create_type.as_ref() {
                                "map" => DiffAction::CreateMap(obj_id, MapType::Map),
                                "table" => DiffAction::CreateMap(obj_id, MapType::Table),
                                "list" => DiffAction::CreateList(obj_id, SequenceType::List),
                                "text" => DiffAction::CreateList(obj_id, SequenceType::Text),
                                _ => {
                                    return Err(Error::invalid_value(
                                        Unexpected::Str(&create_type),
                                        &"A valid object type",
                                    ))
                                }
                            }
                        }
                        "maxElem" => {
                            let obj_id = object_id.ok_or_else(|| Error::missing_field("obj"))?;
                            let value = value.ok_or_else(|| Error::missing_field("value"))?;
                            let seq_type_str =
                                type_str.ok_or_else(|| Error::missing_field("type"))?;
                            let seq_type = match seq_type_str.as_ref() {
                                "list" => SequenceType::List,
                                "text" => SequenceType::Text,
                                _ => {
                                    return Err(Error::invalid_value(
                                        Unexpected::Str(&seq_type_str),
                                        &"A valid sequence type",
                                    ))
                                }
                            };
                            let seq = match value {
                                ElementValue::Primitive(PrimitiveValue::Number(n)) => n as u32,
                                _ => return Err(Error::custom("Invalid value for maxElem.value")),
                            };
                            DiffAction::MaxElem(obj_id, seq, seq_type)
                        }
                        "remove" => {
                            let type_str = type_str.ok_or_else(|| Error::missing_field("type"))?;
                            let obj_id = object_id.ok_or_else(|| Error::missing_field("obj"))?;
                            match key {
                                Some(k) => {
                                    let map_type = match type_str.as_ref() {
                                        "map" => MapType::Map,
                                        "table" => MapType::Table,
                                        _ => {
                                            return Err(Error::invalid_value(
                                                Unexpected::Str(&type_str),
                                                &"A valid map type",
                                            ))
                                        }
                                    };
                                    DiffAction::RemoveMapKey(obj_id, map_type, k)
                                }
                                None => {
                                    let seq_type = match type_str.as_ref() {
                                        "list" => SequenceType::List,
                                        "text" => SequenceType::Text,
                                        _ => {
                                            return Err(Error::invalid_value(
                                                Unexpected::Str(&type_str),
                                                &"A valid sequence type",
                                            ))
                                        }
                                    };
                                    let index =
                                        index.ok_or_else(|| Error::missing_field("index"))?;
                                    DiffAction::RemoveSequenceElement(obj_id, seq_type, index)
                                }
                            }
                        }
                        "set" => {
                            let type_str = type_str.ok_or_else(|| Error::missing_field("type"))?;
                            let obj_id = object_id.ok_or_else(|| Error::missing_field("obj"))?;
                            let value = value.ok_or_else(|| Error::missing_field("value"))?;
                            match key {
                                Some(k) => {
                                    let map_type = match type_str.as_ref() {
                                        "map" => MapType::Map,
                                        "table" => MapType::Table,
                                        _ => {
                                            return Err(Error::invalid_value(
                                                Unexpected::Str(&type_str),
                                                &"A valid map type",
                                            ))
                                        }
                                    };
                                    DiffAction::SetMapKey(obj_id, map_type, k, value, datatype)
                                }
                                None => {
                                    let seq_type = match type_str.as_ref() {
                                        "list" => SequenceType::List,
                                        "text" => SequenceType::Text,
                                        _ => {
                                            return Err(Error::invalid_value(
                                                Unexpected::Str(&type_str),
                                                &"A valid sequence type",
                                            ))
                                        }
                                    };
                                    let index =
                                        index.ok_or_else(|| Error::missing_field("index"))?;
                                    DiffAction::SetSequenceElement(
                                        obj_id, seq_type, index, value, datatype,
                                    )
                                }
                            }
                        }
                        "insert" => {
                            let obj_id = object_id.ok_or_else(|| Error::missing_field("obj"))?;
                            let type_str = type_str.ok_or_else(|| Error::missing_field("type"))?;
                            let value = value.ok_or_else(|| Error::missing_field("value"))?;
                            let elem_id = elem_id.ok_or_else(|| Error::missing_field("elemId"))?;
                            let seq_type = match type_str.as_ref() {
                                "list" => SequenceType::List,
                                "text" => SequenceType::Text,
                                _ => {
                                    return Err(Error::invalid_value(
                                        Unexpected::Str(&type_str),
                                        &"A valid sequence type",
                                    ))
                                }
                            };
                            let index = index.ok_or_else(|| Error::missing_field("index"))?;
                            DiffAction::InsertSequenceElement(
                                obj_id, seq_type, index, value, datatype, elem_id,
                            )
                        }
                        _ => {
                            return Err(Error::invalid_value(
                                Unexpected::Str(&action_str),
                                &"A valid action string",
                            ))
                        }
                    },
                    None => return Err(Error::missing_field("action")),
                };

                let conflicts = conflicts.unwrap_or_default();

                Ok(Diff {
                    action: diff_action,
                    conflicts,
                })
            }
        }
        deserializer.deserialize_struct("Conflict", FIELDS, DiffVisitor)
    }
}

#[cfg(test)]
mod tests {
    //use super::*;
    use crate::{
        ActorID, Conflict, DataType, Diff, DiffAction, ElementID, ElementValue, Key, MapType,
        OpID, PrimitiveValue, SequenceType,
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
                        "type": "map"
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
                        "type": "table"
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
                        "type": "list"
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
                        "type": "text"
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
                        "value": 4
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
                        "value": 4
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
                        "key": "key"
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
                        "key": "key"
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
                        "link": true
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
                        "datatype": "counter"
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
                        "index": 5
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
                        "index": 5
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
                        ElementID::from_actor_and_elem(ActorID("someactor".to_string()), 1),
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
                        "elemId": "someactor:1"
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
                        ElementID::from_actor_and_elem(ActorID("someactor".to_string()), 1),
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
                        "datatype": "timestamp",
                        "elemId": "someactor:1"
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
                        "link": true
                    }
                    "#,
                )
                .unwrap(),
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
                        "datatype": "counter"
                    }
                    "#,
                )
                .unwrap(),
            },
        ];
        for testcase in testcases {
            let serialized = serde_json::to_value(testcase.diff.clone()).unwrap_or_else(|_| {
                panic!(std::format!("Failed to deserialize {}", testcase.name));
            });
            assert_eq!(
                testcase.json, serialized,
                "TestCase {} did not match",
                testcase.name
            );
            let deserialized: Diff = serde_json::from_value(serialized).unwrap_or_else(|_| {
                panic!(std::format!("Failed to deserialize for {}", testcase.name));
            });
            assert_eq!(
                testcase.diff, deserialized,
                "TestCase {} failed the round trip",
                testcase.name
            );
        }
    }

    #[test]
    fn test_deserialize_conflict_link() {
        let json = serde_json::from_str(
            r#"
            {
                "actor": "1234",
                "value": "someid",
                "link": true
            }
            "#,
        )
        .unwrap();
        let expected = Conflict {
            actor: ActorID("1234".to_string()),
            value: ElementValue::Link(ObjectID::ID("someid".to_string())),
            datatype: None,
        };
        let actual: Conflict = serde_json::from_value(json).unwrap();
        assert_eq!(expected, actual);
    }

    #[test]
    fn test_deserialize_conflict_nolink() {
        let json = serde_json::from_str(
            r#"
            {
                "actor": "1234",
                "value": 5,
                "datatype": "counter"
            }
            "#,
        )
        .unwrap();
        let expected = Conflict {
            actor: ActorID("1234".to_string()),
            value: ElementValue::Primitive(PrimitiveValue::Number(5.0)),
            datatype: Some(DataType::Counter),
        };
        let actual: Conflict = serde_json::from_value(json).unwrap();
        assert_eq!(expected, actual);
    }
}
*/
