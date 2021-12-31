use serde::{de, Deserialize, Deserializer};
use smol_str::SmolStr;

use crate::types::ScalarValue;

impl<'de> Deserialize<'de> for ScalarValue {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct ValueVisitor;
        impl<'de> de::Visitor<'de> for ValueVisitor {
            type Value = ScalarValue;

            fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
                formatter.write_str("a number, string, bool, or null")
            }

            fn visit_bool<E>(self, value: bool) -> Result<ScalarValue, E>
            where
                E: de::Error,
            {
                Ok(ScalarValue::Boolean(value))
            }

            fn visit_u64<E>(self, value: u64) -> Result<ScalarValue, E>
            where
                E: de::Error,
            {
                Ok(ScalarValue::Uint(value))
            }

            fn visit_i64<E>(self, value: i64) -> Result<ScalarValue, E>
            where
                E: de::Error,
            {
                Ok(ScalarValue::Int(value))
            }

            fn visit_f64<E>(self, value: f64) -> Result<ScalarValue, E>
            where
                E: de::Error,
            {
                Ok(ScalarValue::F64(value))
            }

            fn visit_str<E>(self, value: &str) -> Result<ScalarValue, E>
            where
                E: de::Error,
            {
                Ok(ScalarValue::Str(SmolStr::new(value)))
            }

            fn visit_none<E>(self) -> Result<ScalarValue, E>
            where
                E: de::Error,
            {
                Ok(ScalarValue::Null)
            }
        }
        deserializer.deserialize_any(ValueVisitor)
    }
}
