use automerge_protocol as amp;
use serde::Serialize;
use std::{borrow::Borrow, collections::HashMap};

#[derive(Serialize, Clone, Debug, PartialEq)]
pub struct Conflicts(HashMap<amp::OpID, Value>);

impl From<HashMap<amp::OpID, Value>> for Conflicts {
    fn from(hmap: HashMap<amp::OpID, Value>) -> Self {
        Conflicts(hmap)
    }
}

#[derive(Serialize, Clone, Debug, PartialEq)]
#[serde(untagged)]
pub enum Value {
    Map(HashMap<String, Value>, amp::MapType),
    Sequence(Vec<Value>),
    Text(Vec<char>),
    Primitive(amp::ScalarValue),
}

impl From<amp::ScalarValue> for Value {
    fn from(val: amp::ScalarValue) -> Self {
        Value::Primitive(val)
    }
}

impl From<&amp::ScalarValue> for Value {
    fn from(val: &amp::ScalarValue) -> Self {
        val.clone().into()
    }
}

impl From<&str> for Value {
    fn from(s: &str) -> Self {
        Value::Primitive(amp::ScalarValue::Str(s.to_string()))
    }
}

impl<T> From<Vec<T>> for Value
where
    T: Into<Value>,
{
    fn from(v: Vec<T>) -> Self {
        Value::Sequence(v.into_iter().map(|t| t.into()).collect())
    }
}

impl<T, K> From<HashMap<K, T>> for Value
where
    T: Into<Value>,
    K: Borrow<str>,
{
    fn from(h: HashMap<K, T>) -> Self {
        Value::Map(
            h.into_iter()
                .map(|(k, v)| (k.borrow().to_string(), v.into()))
                .collect(),
            amp::MapType::Map,
        )
    }
}

impl Value {
    pub fn from_json(json: &serde_json::Value) -> Value {
        match json {
            serde_json::Value::Object(kvs) => {
                let result: HashMap<String, Value> = kvs
                    .iter()
                    .map(|(k, v)| (k.clone(), Value::from_json(v)))
                    .collect();
                Value::Map(result, amp::MapType::Map)
            }
            serde_json::Value::Array(vs) => {
                Value::Sequence(vs.iter().map(Value::from_json).collect())
            }
            serde_json::Value::String(s) => Value::Primitive(amp::ScalarValue::Str(s.clone())),
            serde_json::Value::Number(n) => {
                Value::Primitive(amp::ScalarValue::F64(n.as_f64().unwrap_or(0.0)))
            }
            serde_json::Value::Bool(b) => Value::Primitive(amp::ScalarValue::Boolean(*b)),
            serde_json::Value::Null => Value::Primitive(amp::ScalarValue::Null),
        }
    }

    pub fn to_json(&self) -> serde_json::Value {
        match self {
            Value::Map(map, _) => {
                let result: serde_json::map::Map<String, serde_json::Value> =
                    map.iter().map(|(k, v)| (k.clone(), v.to_json())).collect();
                serde_json::Value::Object(result)
            }
            Value::Sequence(elements) => {
                serde_json::Value::Array(elements.iter().map(|v| v.to_json()).collect())
            }
            Value::Text(chars) => serde_json::Value::String(chars.iter().collect()),
            Value::Primitive(v) => match v {
                amp::ScalarValue::F64(n) => serde_json::Value::Number(
                    serde_json::Number::from_f64(*n).unwrap_or_else(|| serde_json::Number::from(0)),
                ),
                amp::ScalarValue::F32(n) => serde_json::Value::Number(
                    serde_json::Number::from_f64(f64::from(*n))
                        .unwrap_or_else(|| serde_json::Number::from(0)),
                ),
                amp::ScalarValue::Uint(n) => {
                    serde_json::Value::Number(serde_json::Number::from(*n))
                }
                amp::ScalarValue::Int(n) => serde_json::Value::Number(serde_json::Number::from(*n)),
                amp::ScalarValue::Str(s) => serde_json::Value::String(s.to_string()),
                amp::ScalarValue::Boolean(b) => serde_json::Value::Bool(*b),
                amp::ScalarValue::Counter(c) => {
                    serde_json::Value::Number(serde_json::Number::from(*c))
                }
                amp::ScalarValue::Timestamp(t) => {
                    serde_json::Value::Number(serde_json::Number::from(*t))
                }
                amp::ScalarValue::Null => serde_json::Value::Null,
                amp::ScalarValue::Cursor(eid) => {
                    eid.to_string().into()
                }
            },
        }
    }
}

/// Convert a value to a vector of op requests that will create said value.
///
/// #Arguments
///
/// * actor         - The actor who is creating this value
/// * start_op      - The start op which will be used to generate element IDs
/// * parent_object - The ID of the "parent" object, i.e the object that will
///                   contain the newly created object
/// * key           - The property that the newly created object will populate
///                   within the parent object.
/// * insert        - Whether the op that creates this value should be insert
///
///
/// Returns a vector of the op requests which will create this value
pub(crate) fn value_to_op_requests(
    actor: &amp::ActorID,
    start_op: u64,
    parent_object: amp::ObjectID,
    key: &amp::Key,
    v: &Value,
    insert: bool,
) -> (Vec<amp::Op>, u64) {
    match v {
        Value::Sequence(vs) => {
            let list_op = amp::OpID(start_op, actor.clone());
            let make_op = amp::Op {
                action: amp::OpType::Make(amp::ObjType::list()),
                obj: parent_object,
                key: key.clone(),
                insert,
                pred: Vec::new(),
            };
            let mut op_num = start_op + 1;
            let mut result = vec![make_op];
            let mut last_elemid = amp::ElementID::Head;
            for v in vs.iter() {
                let (child_requests, new_op_num) = value_to_op_requests(
                    actor,
                    op_num,
                    amp::ObjectID::from(list_op.clone()),
                    &last_elemid.clone().into(),
                    v,
                    true,
                );
                last_elemid = amp::OpID::new(op_num, actor).into();
                op_num = new_op_num;
                result.extend(child_requests);
            }
            (result, op_num)
        }
        Value::Text(chars) => {
            let make_text_op = amp::OpID(start_op, actor.clone());
            let make_op = amp::Op {
                action: amp::OpType::Make(amp::ObjType::text()),
                obj: parent_object,
                key: key.clone(),
                insert,
                pred: Vec::new(),
            };
            let mut insert_ops: Vec<amp::Op> = Vec::new();
            let mut last_elemid = amp::ElementID::Head;
            let mut op_num = start_op + 1;
            for c in chars.iter() {
                insert_ops.push(amp::Op {
                    action: amp::OpType::Set(amp::ScalarValue::Str(c.to_string())),
                    obj: amp::ObjectID::from(make_text_op.clone()),
                    key: last_elemid.clone().into(),
                    insert: true,
                    pred: Vec::new(),
                });
                last_elemid = amp::OpID::new(op_num, actor).into();
                op_num += 1;
            }
            let mut ops = vec![make_op];
            ops.extend(insert_ops.into_iter());
            (ops, op_num)
        }
        Value::Map(kvs, map_type) => {
            let make_action = match map_type {
                amp::MapType::Map => amp::OpType::Make(amp::ObjType::map()),
                amp::MapType::Table => amp::OpType::Make(amp::ObjType::table()),
            };
            let make_op_id = amp::OpID::new(start_op, actor);
            let make_op = amp::Op {
                action: make_action,
                obj: parent_object,
                key: key.clone(),
                insert,
                pred: Vec::new(),
            };
            let mut op_num = start_op + 1;
            let mut result = vec![make_op];
            for (key, v) in kvs.iter() {
                let (child_requests, new_op_num) = value_to_op_requests(
                    actor,
                    op_num,
                    amp::ObjectID::from(make_op_id.clone()),
                    &amp::Key::from(key.as_str()),
                    v,
                    false,
                );
                op_num = new_op_num;
                result.extend(child_requests);
            }
            (result, op_num)
        }
        Value::Primitive(prim_value) => {
            let ops = vec![amp::Op {
                action: amp::OpType::Set(prim_value.clone()),
                obj: parent_object,
                key: key.clone(),
                insert,
                pred: Vec::new(),
            }];
            (ops, start_op + 1)
        }
    }
}
