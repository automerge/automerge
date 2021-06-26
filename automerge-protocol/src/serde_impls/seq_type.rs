// See type in map_type.rs
use serde::{de::Error, Deserialize, Deserializer, Serialize};

use crate::SequenceType;

impl Serialize for SequenceType {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        match self {
            SequenceType::List => serializer.serialize_str("list"),
            SequenceType::Text => serializer.serialize_str("text"),
        }
    }
}

impl<'de> Deserialize<'de> for SequenceType {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        const VARIANTS: &[&str] = &["list", "text"];
        // TODO: Probably more efficient to deserialize to a `&str`
        let raw_type = String::deserialize(deserializer)?;
        match raw_type.as_str() {
            "list" => Ok(SequenceType::List),
            "text" => Ok(SequenceType::Text),
            other => Err(Error::unknown_variant(other, VARIANTS)),
        }
    }
}
