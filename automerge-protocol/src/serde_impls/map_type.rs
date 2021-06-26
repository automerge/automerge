// By default, msgpack-rust serializes enums
// as maps with a single K/V pair. This is unnecessary,
// so we override that decision and manually serialize
// to a string

// The downside of this is that we cannot deserialize data structures
// that use this enum b/c the msgpack deserializer will expect
// enums to be encoded as a map with a single K/V pair
// Luckily, we don't need to deserialize data structures
// that use this enum
use serde::{de::Error, Deserialize, Deserializer, Serialize};

use crate::MapType;

impl Serialize for MapType {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        match self {
            MapType::Map => serializer.serialize_str("map"),
            MapType::Table => serializer.serialize_str("table"),
        }
    }
}

impl<'de> Deserialize<'de> for MapType {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        const VARIANTS: &[&str] = &["map", "table"];
        // TODO: Probably more efficient to deserialize to a `&str`
        let raw_type = String::deserialize(deserializer)?;
        match raw_type.as_str() {
            "map" => Ok(MapType::Map),
            "table" => Ok(MapType::Table),
            other => Err(Error::unknown_variant(other, VARIANTS)),
        }
    }
}
