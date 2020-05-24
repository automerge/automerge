use crate::PathElement;
use automerge_protocol as amp;
use maplit::hashmap;
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
    Sequence(Vec<Value>, amp::SequenceType),
    Primitive(amp::Value),
}

impl From<amp::Value> for Value {
    fn from(val: amp::Value) -> Self {
        Value::Primitive(val)
    }
}

impl From<&amp::Value> for Value {
    fn from(val: &amp::Value) -> Self {
        val.clone().into()
    }
}

impl From<&str> for Value {
    fn from(s: &str) -> Self {
        Value::Primitive(amp::Value::Str(s.to_string()))
    }
}

impl<T> From<Vec<T>> for Value
where
    T: Into<Value>,
{
    fn from(v: Vec<T>) -> Self {
        Value::Sequence(
            v.into_iter().map(|t| t.into()).collect(),
            amp::SequenceType::List,
        )
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
            serde_json::Value::Array(vs) => Value::Sequence(
                vs.iter().map(Value::from_json).collect(),
                amp::SequenceType::List,
            ),
            serde_json::Value::String(s) => Value::Primitive(amp::Value::Str(s.clone())),
            serde_json::Value::Number(n) => {
                Value::Primitive(amp::Value::F64(n.as_f64().unwrap_or(0.0)))
            }
            serde_json::Value::Bool(b) => Value::Primitive(amp::Value::Boolean(*b)),
            serde_json::Value::Null => Value::Primitive(amp::Value::Null),
        }
    }

    pub fn to_json(&self) -> serde_json::Value {
        match self {
            Value::Map(map, _) => {
                let result: serde_json::map::Map<String, serde_json::Value> =
                    map.iter().map(|(k, v)| (k.clone(), v.to_json())).collect();
                serde_json::Value::Object(result)
            }
            Value::Sequence(elements, amp::SequenceType::List) => {
                serde_json::Value::Array(elements.iter().map(|v| v.to_json()).collect())
            }
            Value::Sequence(elements, amp::SequenceType::Text) => serde_json::Value::String(
                elements
                    .iter()
                    .map(|v| match v {
                        Value::Primitive(amp::Value::Str(c)) => c.as_str(),
                        _ => panic!("Non string element in text sequence"),
                    })
                    .collect(),
            ),
            Value::Primitive(v) => match v {
                amp::Value::F64(n) => serde_json::Value::Number(
                    serde_json::Number::from_f64(*n).unwrap_or_else(|| serde_json::Number::from(0)),
                ),
                amp::Value::F32(n) => serde_json::Value::Number(
                    serde_json::Number::from_f64(f64::from(*n))
                        .unwrap_or_else(|| serde_json::Number::from(0)),
                ),
                amp::Value::Uint(n) => serde_json::Value::Number(serde_json::Number::from(*n)),
                amp::Value::Int(n) => serde_json::Value::Number(serde_json::Number::from(*n)),
                amp::Value::Str(s) => serde_json::Value::String(s.to_string()),
                amp::Value::Boolean(b) => serde_json::Value::Bool(*b),
                amp::Value::Counter(c) => serde_json::Value::Number(serde_json::Number::from(*c)),
                amp::Value::Timestamp(t) => serde_json::Value::Number(serde_json::Number::from(*t)),
                amp::Value::Null => serde_json::Value::Null,
            },
        }
    }
}

/// Convert a value to a vector of op requests that will create said value.
///
/// #Arguments
///
/// * parent_object - The ID of the "parent" object, i.e the object that will
///                   contain the newly created object
/// * key           - The property that the newly created object will populate
///                   within the parent object.
///
///
/// Returns a tuple of the op requests which will create this value, and a diff
/// which corresponds to those ops.
pub(crate) fn value_to_op_requests(
    parent_object: String,
    key: PathElement,
    v: &Value,
    insert: bool,
) -> (Vec<amp::Op>, amp::Diff) {
    match v {
        Value::Sequence(vs, seq_type) => {
            let make_action = match seq_type {
                amp::SequenceType::List => amp::OpType::MakeList,
                amp::SequenceType::Text => amp::OpType::MakeText,
            };
            let list_id = new_object_id();
            let make_op = amp::Op {
                action: make_action,
                obj: parent_object,
                key: key.to_request_key(),
                child: Some(list_id.clone()),
                value: None,
                datatype: None,
                insert,
            };
            let child_requests_and_diffs: Vec<(Vec<amp::Op>, amp::Diff)> = vs
                .iter()
                .enumerate()
                .map(|(index, v)| {
                    value_to_op_requests(list_id.clone(), PathElement::Index(index), v, true)
                })
                .collect();
            let child_requests: Vec<amp::Op> = child_requests_and_diffs
                .iter()
                .cloned()
                .flat_map(|(o, _)| o)
                .collect();
            let child_diff = amp::SeqDiff {
                edits: vs
                    .iter()
                    .enumerate()
                    .map(|(index, _)| amp::DiffEdit::Insert { index })
                    .collect(),
                object_id: list_id,
                obj_type: amp::ObjType::Sequence(*seq_type),
                props: child_requests_and_diffs
                    .into_iter()
                    .enumerate()
                    .map(|(index, (_, diff_link))| {
                        (index, hashmap! {random_op_id().to_string() => diff_link})
                    })
                    .collect(),
            };
            let mut result = vec![make_op];
            result.extend(child_requests);
            (result, amp::Diff::Seq(child_diff))
        }
        Value::Map(kvs, map_type) => {
            let make_action = match map_type {
                amp::MapType::Map => amp::OpType::MakeMap,
                amp::MapType::Table => amp::OpType::MakeTable,
            };
            let map_id = new_object_id();
            let make_op = amp::Op {
                action: make_action,
                obj: parent_object,
                key: key.to_request_key(),
                child: Some(map_id.clone()),
                value: None,
                datatype: None,
                insert,
            };
            let child_requests_and_diffs: HashMap<String, (Vec<amp::Op>, amp::Diff)> = kvs
                .iter()
                .map(|(k, v)| {
                    (
                        k.clone(),
                        value_to_op_requests(map_id.clone(), PathElement::Key(k.clone()), v, false),
                    )
                })
                .collect();
            let mut result = vec![make_op];
            let child_requests: Vec<amp::Op> = child_requests_and_diffs
                .iter()
                .flat_map(|(_, (o, _))| o)
                .cloned()
                .collect();
            let child_diff = amp::MapDiff {
                object_id: map_id,
                obj_type: amp::ObjType::Map(*map_type),
                props: child_requests_and_diffs
                    .into_iter()
                    .map(|(k, (_, diff_link))| {
                        (k, hashmap! {random_op_id().to_string() => diff_link})
                    })
                    .collect(),
            };
            result.extend(child_requests);
            (result, amp::Diff::Map(child_diff))
        }
        Value::Primitive(prim_value) => {
            let ops = vec![amp::Op {
                action: amp::OpType::Set,
                obj: parent_object,
                key: key.to_request_key(),
                child: None,
                value: Some(prim_value.clone()),
                datatype: Some(value_to_datatype(prim_value)),
                insert,
            }];
            let diff = amp::Diff::Value(prim_value.clone());
            (ops, diff)
        }
    }
}

fn new_object_id() -> String {
    amp::ObjectID::ID(random_op_id()).to_string()
}

pub(crate) fn random_op_id() -> amp::OpID {
    amp::OpID::new(1, &amp::ActorID::random())
}

fn value_to_datatype(value: &amp::Value) -> amp::DataType {
    match value {
        amp::Value::Counter(_) => amp::DataType::Counter,
        amp::Value::Timestamp(_) => amp::DataType::Timestamp,
        _ => amp::DataType::Undefined,
    }
}
