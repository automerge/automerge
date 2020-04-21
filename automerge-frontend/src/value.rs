use automerge_backend::{DataType, PrimitiveValue};
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

#[derive(Serialize, Clone, Debug, PartialEq)]
#[serde(untagged)]
pub enum Value {
    Map(HashMap<String, Value>, MapType),
    Sequence(Vec<Value>, SequenceType),
    Primitive(PrimitiveValue, DataType),
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
            serde_json::Value::Array(vs) => Value::Sequence(
                vs.iter().map(Value::from_json).collect(),
                SequenceType::List,
            ),
            serde_json::Value::String(s) => {
                Value::Primitive(PrimitiveValue::Str(s.to_string()), DataType::Undefined)
            }
            serde_json::Value::Number(n) => Value::Primitive(
                PrimitiveValue::Number(n.as_f64().unwrap_or(0.0)),
                DataType::Undefined,
            ),
            serde_json::Value::Bool(b) => {
                Value::Primitive(PrimitiveValue::Boolean(*b), DataType::Undefined)
            }
            serde_json::Value::Null => Value::Primitive(PrimitiveValue::Null, DataType::Undefined),
        }
    }

    pub fn to_json(&self) -> serde_json::Value {
        match self {
            Value::Map(map, _) => {
                let result: serde_json::map::Map<String, serde_json::Value> =
                    map.iter().map(|(k, v)| (k.clone(), v.to_json())).collect();
                serde_json::Value::Object(result)
            }
            Value::Sequence(elements, SequenceType::List) => {
                serde_json::Value::Array(elements.iter().map(|v| v.to_json()).collect())
            }
            Value::Sequence(elements, SequenceType::Text) => {
                serde_json::Value::String(elements.iter().map(|v| match v {
                    Value::Primitive(PrimitiveValue::Str(c), _) => c.as_str(),
                    _ => panic!("Non string element in text sequence"),
                }).collect())
            }
            Value::Primitive(v, _) => {
                match v {
                    PrimitiveValue::Number(n) => serde_json::Value::Number(serde_json::Number::from_f64(*n).unwrap_or_else(|| serde_json::Number::from(0))),
                    PrimitiveValue::Str(s) =>  serde_json::Value::String(s.to_string()),
                    PrimitiveValue::Boolean(b) => serde_json::Value::Bool(*b),
                    PrimitiveValue::Null => serde_json::Value::Null,
                }
            }
        }
    }
}
