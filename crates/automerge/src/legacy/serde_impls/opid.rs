use std::str::FromStr;

use serde::{de, Deserialize, Deserializer, Serialize, Serializer};

use crate::legacy::OpId;

impl<'de> Deserialize<'de> for OpId {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        OpId::from_str(&s)
            .map_err(|_| de::Error::invalid_value(de::Unexpected::Str(&s), &"A valid OpID"))
    }
}

impl Serialize for OpId {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(self.to_string().as_str())
    }
}
