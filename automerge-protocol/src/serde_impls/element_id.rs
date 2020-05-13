use crate::ElementID;
use serde::{de, Deserialize, Deserializer, Serialize, Serializer};
use std::str::FromStr;

impl Serialize for ElementID {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match self {
            ElementID::ID(id) => id.serialize(serializer),
            ElementID::Head => serializer.serialize_str("_head"),
        }
    }
}

impl<'de> Deserialize<'de> for ElementID {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        ElementID::from_str(&s).map_err(|_| de::Error::custom("invalid element ID"))
    }
}
