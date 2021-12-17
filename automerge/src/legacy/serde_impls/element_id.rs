use std::str::FromStr;

use serde::{de, Deserialize, Deserializer, Serialize, Serializer};

use crate::legacy::ElementId;

impl Serialize for ElementId {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match self {
            ElementId::Id(id) => id.serialize(serializer),
            ElementId::Head => serializer.serialize_str("_head"),
        }
    }
}

impl<'de> Deserialize<'de> for ElementId {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        ElementId::from_str(&s).map_err(|_| de::Error::custom("invalid element ID"))
    }
}
