use crate::ActorID;
use serde::de;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::str::FromStr;

impl<'de> Deserialize<'de> for ActorID {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        ActorID::from_str(&s)
            .map_err(|_| de::Error::invalid_value(de::Unexpected::Str(&s), &"A valid ActorID"))
    }
}

impl Serialize for ActorID {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(self.to_hex_string().as_str())
    }
}
