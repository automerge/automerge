use crate::{ElementId, Key};
use serde::{Deserialize, Deserializer};
use std::str::FromStr;

impl<'de> Deserialize<'de> for Key {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        if let Ok(eid) = ElementId::from_str(&s) {
            Ok(Key::Seq(eid))
        } else {
            Ok(Key::Map(s))
        }
    }
}
