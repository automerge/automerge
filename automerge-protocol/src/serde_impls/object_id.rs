use crate::{ObjectID, OpID};
use serde::{de, Deserialize, Deserializer, Serialize, Serializer};
use std::str::FromStr;

impl Serialize for ObjectID {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match self {
            ObjectID::ID(id) => id.serialize(serializer),
            ObjectID::Root => serializer.serialize_str("00000000-0000-0000-0000-000000000000"),
        }
    }
}

impl<'de> Deserialize<'de> for ObjectID {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        if s == "00000000-0000-0000-0000-000000000000" {
            Ok(ObjectID::Root)
        } else if let Ok(id) = OpID::from_str(&s) {
            Ok(ObjectID::ID(id))
        } else {
            Err(de::Error::invalid_value(
                de::Unexpected::Str(&s),
                &"A valid ObjectID",
            ))
        }
    }
}
