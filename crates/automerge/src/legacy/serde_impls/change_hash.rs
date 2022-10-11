use std::convert::TryInto;

use serde::{de, Deserialize, Deserializer, Serialize, Serializer};

use crate::ChangeHash;

impl Serialize for ChangeHash {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        hex::encode(&self.0).serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for ChangeHash {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        let vec = hex::decode(&s).map_err(|_| {
            de::Error::invalid_value(de::Unexpected::Str(&s), &"A valid hex string")
        })?;
        vec.as_slice().try_into().map_err(|_| {
            de::Error::invalid_value(de::Unexpected::Str(&s), &"A 32 byte hex encoded string")
        })
    }
}
