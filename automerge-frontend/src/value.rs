use crate::PathElement;
use automerge_backend as amb;
use serde::Serialize;
use std::collections::HashMap;
use maplit::hashmap;

#[derive(Serialize, Clone, Debug, PartialEq)]
pub enum MapType {
    Map,
    Table,
}

impl MapType {
    pub(crate) fn to_obj_type(&self) -> amb::ObjType {
        match self {
            MapType::Map => amb::ObjType::Map,
            MapType::Table => amb::ObjType::Table,
        }
    }
}

#[derive(Serialize, Clone, Debug, PartialEq)]
pub enum SequenceType {
    List,
    Text,
}

impl SequenceType {
    pub(crate) fn to_obj_type(&self) -> amb::ObjType {
        match self {
            SequenceType::List => amb::ObjType::List,
            SequenceType::Text => amb::ObjType::Text,
        }
    }
}

#[derive(Serialize, Clone, Debug, PartialEq)]
pub struct Conflicts(HashMap<amb::OpID, Value>);

impl From<HashMap<amb::OpID, Value>> for Conflicts {
    fn from(hmap: HashMap<amb::OpID, Value>) -> Self {
        Conflicts(hmap)
    }
}

#[derive(Serialize, Clone, Debug, PartialEq)]
#[serde(untagged)]
pub enum Value {
    Map(HashMap<String, Value>, MapType),
    Sequence(Vec<Value>, SequenceType),
    Primitive(amb::Value),
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
            serde_json::Value::String(s) => Value::Primitive(amb::Value::Str(s.clone())),
            serde_json::Value::Number(n) => {
                Value::Primitive(amb::Value::F64(n.as_f64().unwrap_or(0.0)))
            }
            serde_json::Value::Bool(b) => Value::Primitive(amb::Value::Boolean(*b)),
            serde_json::Value::Null => Value::Primitive(amb::Value::Null),
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
            Value::Sequence(elements, SequenceType::Text) => serde_json::Value::String(
                elements
                    .iter()
                    .map(|v| match v {
                        Value::Primitive(amb::Value::Str(c)) => c.as_str(),
                        _ => panic!("Non string element in text sequence"),
                    })
                    .collect(),
            ),
            Value::Primitive(v) => match v {
                amb::Value::F64(n) => serde_json::Value::Number(
                    serde_json::Number::from_f64(*n).unwrap_or_else(|| serde_json::Number::from(0)),
                ),
                amb::Value::F32(n) => serde_json::Value::Number(
                    serde_json::Number::from_f64(f64::from(*n)).unwrap_or_else(|| serde_json::Number::from(0)),
                ),
                amb::Value::Uint(n) => serde_json::Value::Number(serde_json::Number::from(*n)),
                amb::Value::Int(n) => serde_json::Value::Number(serde_json::Number::from(*n)),
                amb::Value::Str(s) => serde_json::Value::String(s.to_string()),
                amb::Value::Boolean(b) => serde_json::Value::Bool(*b),
                amb::Value::Counter(c) => {
                    serde_json::Value::Number(serde_json::Number::from(*c))
                }
                amb::Value::Timestamp(t) => {
                    serde_json::Value::Number(serde_json::Number::from(*t))
                }
                amb::Value::Null => serde_json::Value::Null,
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
) -> (Vec<amb::OpRequest>, amb::Diff) {
    match v {
        Value::Sequence(vs, seq_type) => {
            let make_action = match seq_type {
                SequenceType::List => amb::ReqOpType::MakeList,
                SequenceType::Text => amb::ReqOpType::MakeText,
            };
            let list_id = new_object_id();
            let make_op = amb::OpRequest {
                action: make_action,
                obj: parent_object,
                key: key.to_request_key(),
                child: Some(list_id.clone()),
                value: None,
                datatype: None,
                insert,
            };
            let child_requests_and_diffs: Vec<(Vec<amb::OpRequest>, amb::Diff)> = vs
                .iter()
                .enumerate()
                .map(|(index, v)| {
                    value_to_op_requests(list_id.clone(), PathElement::Index(index), v, true)
                })
                .collect();
            let child_requests: Vec<amb::OpRequest> = child_requests_and_diffs
                .iter()
                .cloned()
                .flat_map(|(o, _)| o)
                .collect();
            let child_diff = amb::SeqDiff {
                edits: vs.iter()
                        .enumerate()
                        .map(|(index, _)| amb::DiffEdit::Insert { index })
                        .collect(),
                object_id: amb::ObjectID::ID(amb::OpID(1, list_id)).to_string(),
                obj_type: match seq_type {
                    SequenceType::List => amb::ObjType::List,
                    SequenceType::Text => amb::ObjType::Text,
                },
                props: child_requests_and_diffs
                    .into_iter()
                    .enumerate()
                    .map(|(index, (_, diff_link))| {
                        (index, hashmap!{random_op_id().to_string() => diff_link})
                    })
                    .collect(),
            };
            let mut result = vec![make_op];
            result.extend(child_requests);
            (result, amb::Diff::Seq(child_diff))
        }
        Value::Map(kvs, map_type) => {
            let make_action = match map_type {
                MapType::Map => amb::ReqOpType::MakeMap,
                MapType::Table => amb::ReqOpType::MakeTable,
            };
            let map_id = new_object_id();
            let make_op = amb::OpRequest {
                action: make_action,
                obj: parent_object,
                key: key.to_request_key(),
                child: Some(map_id.clone()),
                value: None,
                datatype: None,
                insert,
            };
            let child_requests_and_diffs: HashMap<String, (Vec<amb::OpRequest>, amb::Diff)> =
                kvs.iter()
                    .map(|(k, v)| {
                        (
                            k.clone(),
                            value_to_op_requests(
                                map_id.clone(),
                                PathElement::Key(k.clone()),
                                v,
                                false,
                            ),
                        )
                    })
                    .collect();
            let mut result = vec![make_op];
            let child_requests: Vec<amb::OpRequest> = child_requests_and_diffs
                .iter()
                .flat_map(|(_, (o, _))| o)
                .cloned()
                .collect();
            let child_diff = amb::MapDiff {
                object_id: amb::ObjectID::ID(amb::OpID(1, map_id)).to_string(),
                obj_type: match map_type {
                    MapType::Map => amb::ObjType::Map,
                    MapType::Table => amb::ObjType::Table,
                },
                props: child_requests_and_diffs
                    .into_iter()
                    .map(|(k, (_, diff_link))| {
                        (k, hashmap!{random_op_id().to_string() => diff_link})
                    })
                    .collect(),
            };
            result.extend(child_requests);
            (result, amb::Diff::Map(child_diff))
        }
        Value::Primitive(prim_value) => {
            let ops = vec![amb::OpRequest {
                action: amb::ReqOpType::Set,
                obj: parent_object,
                key: key.to_request_key(),
                child: None,
                value: Some(prim_value.clone()),
                datatype: Some(prim_value.datatype()),
                insert,
            }];
            let diff = amb::Diff::Value(prim_value.clone());
            (ops, diff)
        }
    }
}

fn new_object_id() -> String {
    uuid::Uuid::new_v4().to_string()
}

pub(crate) fn random_op_id() -> amb::OpID {
    amb::OpID(1, amb::ActorID::random().0)
}
