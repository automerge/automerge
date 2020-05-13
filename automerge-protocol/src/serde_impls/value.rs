use crate::Value;
use serde::{de, Deserialize, Deserializer};

impl<'de> Deserialize<'de> for Value {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct ValueVisitor;
        impl<'de> de::Visitor<'de> for ValueVisitor {
            type Value = Value;

            fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
                formatter.write_str("a number, string, bool, or null")
            }

            fn visit_bool<E>(self, value: bool) -> Result<Value, E>
            where
                E: de::Error,
            {
                Ok(Value::Boolean(value))
            }

            fn visit_u64<E>(self, value: u64) -> Result<Value, E>
            where
                E: de::Error,
            {
                Ok(Value::Uint(value))
            }

            fn visit_i64<E>(self, value: i64) -> Result<Value, E>
            where
                E: de::Error,
            {
                Ok(Value::Int(value))
            }

            fn visit_f64<E>(self, value: f64) -> Result<Value, E>
            where
                E: de::Error,
            {
                Ok(Value::F64(value))
            }

            fn visit_str<E>(self, value: &str) -> Result<Value, E>
            where
                E: de::Error,
            {
                Ok(Value::Str(value.to_string()))
            }
        }
        deserializer.deserialize_any(ValueVisitor)
    }
}
