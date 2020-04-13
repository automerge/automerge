use serde::Serialize;
use std::collections::HashMap;

#[derive(Serialize, Clone, Debug, PartialEq)]
pub enum MapType {
    Map,
    Table,
}

#[derive(Serialize, Clone, Debug, PartialEq)]
pub enum SequenceType {
    List,
    Text,
}

/// Possible values of an element of the state. Using this rather than
/// serde_json::Value because we'll probably want to make the core logic
/// independent of serde in order to be `no_std` compatible.
#[derive(Serialize, Clone, Debug, PartialEq)]
#[serde(untagged)]
pub enum Value {
    Map(HashMap<String, Value>, MapType),
    Sequence(Vec<Value>, SequenceType),
    Str(String),
    Number(f64),
    Boolean(bool),
    Null,
}

impl Value {
    pub fn from_json(json: &serde_json::Value) -> Value {
        match json {
            serde_json::Value::Object(kvs) => {
                let result: HashMap<String, Value> = kvs
                    .iter()
                    .map(|(k, v)| (k.clone(), Value::from_json(v)))
                    .collect();
                Value::Map(result, MapType::Map)
            }
            serde_json::Value::Array(vs) => Value::Sequence(vs.iter().map(Value::from_json).collect(), SequenceType::List),
            serde_json::Value::String(s) => Value::Str(s.to_string()),
            serde_json::Value::Number(n) => Value::Number(n.as_f64().unwrap_or(0.0)),
            serde_json::Value::Bool(b) => Value::Boolean(*b),
            serde_json::Value::Null => Value::Null,
        }
    }

    pub fn to_json(&self) -> serde_json::Value {
        match self {
            Value::Map(map, map_type) => {
                let result: serde_json::map::Map<String, serde_json::Value> =
                    map.iter().map(|(k, v)| (k.clone(), v.to_json())).collect();
                serde_json::Value::Object(result)
            }
            Value::Sequence(elements, seq_type) => {
                serde_json::Value::Array(elements.iter().map(|v| v.to_json()).collect())
            }
            Value::Str(s) => serde_json::Value::String(s.to_string()),
            Value::Number(n) => serde_json::Value::Number(
                serde_json::Number::from_f64(*n).unwrap_or_else(|| serde_json::Number::from(0)),
            ),
            Value::Boolean(b) => serde_json::Value::Bool(*b),
            Value::Null => serde_json::Value::Null,
        }
    }
}
