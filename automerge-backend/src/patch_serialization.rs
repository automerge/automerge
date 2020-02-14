use crate::{Diff, DiffAction, ElementValue, Conflict};
use serde::{Serialize, Serializer};
use serde::ser::SerializeMap;
use serde::de::{MapAccess, Visitor, Error};
use std::fmt;

impl Serialize for Conflict {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
        where S: Serializer {
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
        where D: Deserializer<'de> {
        struct ConflictVisitor;
        impl <'de> Visitor<'de> for ConflictVisitor {
            type Value = Conflict;

            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                formatter.write_str("A conflict object")
            }

            fn visit_map<V>(self, mut map: V) -> Result<Conflict, V::Error> 
                where V: MapAccess<'de> {
                let mut actor = None;
                let mut value = None;
                let mut datatype = None;

                while let Some(key) = map.next_key()? {
                }

                let actor = actor.ok_or_else(|| Error::missing_field("actor"))?;
                let value = value.ok_or_else(|| Error::missing_field("value"))?;
                Ok(Conflict{
                    actor,
                    value,
                    datatype,
                })
            }

        }
    }
}

impl Serialize for Diff {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
        where S: Serializer {
        let mut map_serializer = serializer.serialize_map(None)?;
        map_serializer.serialize_entry("conflicts", &self.conflicts)?;
        match &self.action {
            DiffAction::CreateMap(oid, map_type) => {
                map_serializer.serialize_entry("action", "create")?;
                map_serializer.serialize_entry("obj", &oid)?;
                map_serializer.serialize_entry("type", &map_type)?;
            },
            DiffAction::CreateList(oid, seq_type) => {
                map_serializer.serialize_entry("action", "create")?;
                map_serializer.serialize_entry("obj", &oid)?;
                map_serializer.serialize_entry("type", &seq_type)?;
            },
            DiffAction::MaxElem(oid, max, seq_type) => {
                map_serializer.serialize_entry("action", "maxElem")?;
                map_serializer.serialize_entry("obj", &oid)?;
                map_serializer.serialize_entry("value", &max)?;
                map_serializer.serialize_entry("type", &seq_type)?;
            },
            DiffAction::RemoveMapKey(oid, map_type, key) => {
                map_serializer.serialize_entry("action", "remove")?;
                map_serializer.serialize_entry("type", &map_type)?;
                map_serializer.serialize_entry("obj", &oid)?;
                map_serializer.serialize_entry("key", &key)?;
            },
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
            },
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
            },
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
