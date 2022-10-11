use std::str::FromStr;

use serde::{de, Deserialize, Deserializer, Serialize, Serializer};

use crate::ActorId;

impl<'de> Deserialize<'de> for ActorId {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        ActorId::from_str(&s)
            .map_err(|_| de::Error::invalid_value(de::Unexpected::Str(&s), &"A valid ActorID"))
    }
}

impl Serialize for ActorId {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(self.to_hex_string().as_str())
    }
}
