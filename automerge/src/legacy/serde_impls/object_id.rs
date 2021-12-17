use std::str::FromStr;

use serde::{de, Deserialize, Deserializer, Serialize, Serializer};

use crate::legacy::{ObjectId, OpId};

impl Serialize for ObjectId {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match self {
            ObjectId::Id(id) => id.serialize(serializer),
            ObjectId::Root => serializer.serialize_str("_root"),
        }
    }
}

impl<'de> Deserialize<'de> for ObjectId {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        if s == "_root" {
            Ok(ObjectId::Root)
        } else if let Ok(id) = OpId::from_str(&s) {
            Ok(ObjectId::Id(id))
        } else {
            Err(de::Error::invalid_value(
                de::Unexpected::Str(&s),
                &"A valid ObjectID",
            ))
        }
    }
}
