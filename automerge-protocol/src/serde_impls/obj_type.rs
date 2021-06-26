// See type in map_type.rs
use serde::{de::Error, Deserialize, Deserializer, Serialize};

use crate::ObjType;

impl Serialize for ObjType {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        match self {
            ObjType::Map => serializer.serialize_str("map"),
            ObjType::Table => serializer.serialize_str("table"),
            ObjType::List => serializer.serialize_str("list"),
            ObjType::Text => serializer.serialize_str("text"),
        }
    }
}

impl<'de> Deserialize<'de> for ObjType {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        const VARIANTS: &[&str] = &["map", "table", "list", "text"];
        // TODO: Probably more efficient to deserialize to a `&str`
        let raw_type = String::deserialize(deserializer)?;
        match raw_type.as_str() {
            "map" => Ok(ObjType::Map),
            "table" => Ok(ObjType::Table),
            "list" => Ok(ObjType::List),
            "text" => Ok(ObjType::Text),
            other => Err(Error::unknown_variant(other, VARIANTS)),
        }
    }
}
