// See comment in map_type.rs
use serde::{de::Error, Deserialize, Deserializer, Serialize};

use crate::DataType;

impl Serialize for DataType {
    fn serialize<S>(&self, s: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        match self {
            DataType::Counter => s.serialize_str("counter"),
            DataType::Timestamp => s.serialize_str("timestamp"),
            DataType::Bytes => s.serialize_str("bytes"),
            DataType::Cursor => s.serialize_str("cursor"),
            DataType::Uint => s.serialize_str("uint"),
            DataType::Int => s.serialize_str("int"),
            DataType::F64 => s.serialize_str("float64"),
            DataType::Undefined => s.serialize_str("undefined"),
        }
    }
}

impl<'de> Deserialize<'de> for DataType {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        const VARIANTS: &[&str] = &[
            "counter",
            "timestamp",
            "bytes",
            "cursor",
            "uint",
            "int",
            "float64",
            "undefined",
        ];
        // TODO: Probably more efficient to deserialize to a `&str`
        let raw_type = String::deserialize(deserializer)?;
        match raw_type.as_str() {
            "counter" => Ok(DataType::Counter),
            "timestamp" => Ok(DataType::Timestamp),
            "bytes" => Ok(DataType::Bytes),
            "cursor" => Ok(DataType::Cursor),
            "uint" => Ok(DataType::Uint),
            "int" => Ok(DataType::Int),
            "float64" => Ok(DataType::F64),
            "undefined" => Ok(DataType::Undefined),
            other => Err(Error::unknown_variant(other, VARIANTS)),
        }
    }
}
