use std::{borrow::Cow, collections::HashMap};

use amp::SortedVec;
use automerge_protocol as amp;
use serde::Serialize;
use smol_str::SmolStr;

use crate::path::PathElement;

#[derive(Serialize, Clone, Debug, PartialEq)]
pub struct Conflicts(HashMap<amp::OpId, Value>);

impl From<HashMap<amp::OpId, Value>> for Conflicts {
    fn from(hmap: HashMap<amp::OpId, Value>) -> Self {
        Conflicts(hmap)
    }
}

#[derive(Serialize, Clone, Debug, PartialEq)]
#[cfg_attr(feature = "derive-arbitrary", derive(arbitrary::Arbitrary))]
#[serde(untagged)]
pub enum Value {
    Map(HashMap<SmolStr, Value>),
    Table(HashMap<SmolStr, Value>),
    Sequence(Vec<Value>),
    /// Sequence of grapheme clusters
    Text(Vec<SmolStr>),
    Primitive(Primitive),
}

#[derive(Serialize, Clone, Debug, PartialEq)]
#[cfg_attr(feature = "derive-arbitrary", derive(arbitrary::Arbitrary))]
pub enum Primitive {
    Bytes(Vec<u8>),
    Str(SmolStr),
    Int(i64),
    Uint(u64),
    F64(f64),
    F32(f32),
    Counter(i64),
    Timestamp(i64),
    Boolean(bool),
    Cursor(Cursor),
    Null,
}

#[derive(Serialize, Clone, Debug, PartialEq)]
#[cfg_attr(feature = "derive-arbitrary", derive(arbitrary::Arbitrary))]
pub struct Cursor {
    pub index: u32,
    pub(crate) object: amp::ObjectId,
    pub(crate) elem_opid: amp::OpId,
}

impl Cursor {
    pub fn new(index: u32, obj: amp::ObjectId, op: amp::OpId) -> Cursor {
        Cursor {
            index,
            object: obj,
            elem_opid: op,
        }
    }
}

impl From<Cursor> for Value {
    fn from(c: Cursor) -> Self {
        Value::Primitive(Primitive::Cursor(c))
    }
}

impl From<&Primitive> for amp::ScalarValue {
    fn from(p: &Primitive) -> Self {
        match p {
            Primitive::Bytes(b) => amp::ScalarValue::Bytes(b.clone()),
            Primitive::Str(s) => amp::ScalarValue::Str(s.clone()),
            Primitive::Int(i) => amp::ScalarValue::Int(*i),
            Primitive::Uint(u) => amp::ScalarValue::Uint(*u),
            Primitive::F64(f) => amp::ScalarValue::F64(*f),
            Primitive::F32(f) => amp::ScalarValue::F32(*f),
            Primitive::Counter(i) => amp::ScalarValue::Counter(*i),
            Primitive::Timestamp(i) => amp::ScalarValue::Timestamp(*i),
            Primitive::Boolean(b) => amp::ScalarValue::Boolean(*b),
            Primitive::Null => amp::ScalarValue::Null,
            Primitive::Cursor(c) => amp::ScalarValue::Cursor(c.elem_opid.clone()),
        }
    }
}

impl From<Primitive> for Value {
    fn from(p: Primitive) -> Self {
        Value::Primitive(p)
    }
}

impl From<&str> for Value {
    fn from(s: &str) -> Self {
        Value::Primitive(Primitive::Str(SmolStr::new(s)))
    }
}

impl From<&amp::CursorDiff> for Primitive {
    fn from(diff: &amp::CursorDiff) -> Self {
        Primitive::Cursor(Cursor {
            index: diff.index,
            object: diff.object_id.clone(),
            elem_opid: diff.elem_id.clone(),
        })
    }
}

impl From<char> for Value {
    fn from(c: char) -> Value {
        Value::Primitive(Primitive::Str(SmolStr::new(c.to_string())))
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

impl From<i64> for Value {
    fn from(v: i64) -> Self {
        Value::Primitive(Primitive::Int(v))
    }
}

impl<T, K> From<HashMap<K, T>> for Value
where
    T: Into<Value>,
    K: AsRef<str>,
{
    fn from(h: HashMap<K, T>) -> Self {
        Value::Map(
            h.into_iter()
                .map(|(k, v)| (SmolStr::new(k), v.into()))
                .collect(),
        )
    }
}

impl AsRef<Value> for Value {
    fn as_ref(&self) -> &Value {
        self
    }
}

impl Value {
    pub fn from_json(json: &serde_json::Value) -> Value {
        match json {
            serde_json::Value::Object(kvs) => {
                let result: HashMap<SmolStr, Value> = kvs
                    .iter()
                    .map(|(k, v)| (SmolStr::new(k), Value::from_json(v)))
                    .collect();
                Value::Map(result)
            }
            serde_json::Value::Array(vs) => {
                Value::Sequence(vs.iter().map(Value::from_json).collect())
            }
            serde_json::Value::String(s) => Value::Primitive(Primitive::Str(SmolStr::new(s))),
            serde_json::Value::Number(n) => {
                Value::Primitive(Primitive::F64(n.as_f64().unwrap_or(0.0)))
            }
            serde_json::Value::Bool(b) => Value::Primitive(Primitive::Boolean(*b)),
            serde_json::Value::Null => Value::Primitive(Primitive::Null),
        }
    }

    pub fn to_json(&self) -> serde_json::Value {
        match self {
            Value::Map(map) => {
                let result: serde_json::map::Map<String, serde_json::Value> = map
                    .iter()
                    .map(|(k, v)| (k.to_string(), v.to_json()))
                    .collect();
                serde_json::Value::Object(result)
            }
            Value::Table(map) => {
                let result: serde_json::map::Map<String, serde_json::Value> = map
                    .iter()
                    .map(|(k, v)| (k.to_string(), v.to_json()))
                    .collect();
                serde_json::Value::Object(result)
            }
            Value::Sequence(elements) => {
                serde_json::Value::Array(elements.iter().map(|v| v.to_json()).collect())
            }
            Value::Text(graphemes) => serde_json::Value::String(graphemes.join("")),
            Value::Primitive(v) => match v {
                Primitive::F64(n) => serde_json::Value::Number(
                    serde_json::Number::from_f64(*n).unwrap_or_else(|| serde_json::Number::from(0)),
                ),
                Primitive::F32(n) => serde_json::Value::Number(
                    serde_json::Number::from_f64(f64::from(*n))
                        .unwrap_or_else(|| serde_json::Number::from(0)),
                ),
                Primitive::Uint(n) => serde_json::Value::Number(serde_json::Number::from(*n)),
                Primitive::Int(n) => serde_json::Value::Number(serde_json::Number::from(*n)),
                Primitive::Bytes(b) => serde_json::Value::Array(
                    b.iter()
                        .map(|byte| serde_json::Value::Number(serde_json::Number::from(*byte)))
                        .collect(),
                ),
                Primitive::Str(s) => serde_json::Value::String(s.to_string()),
                Primitive::Boolean(b) => serde_json::Value::Bool(*b),
                Primitive::Counter(c) => serde_json::Value::Number(serde_json::Number::from(*c)),
                Primitive::Timestamp(t) => serde_json::Value::Number(serde_json::Number::from(*t)),
                Primitive::Null => serde_json::Value::Null,
                Primitive::Cursor(c) => {
                    serde_json::Value::Number(serde_json::Number::from(c.index))
                }
            },
        }
    }

    pub fn get_value(&self, path: crate::Path) -> Option<Cow<'_, Self>> {
        let mut path_elements = path.elements();
        path_elements.reverse();
        self.get_value_rev_path(path_elements)
    }

    fn get_value_rev_path(&self, mut rev_path: Vec<PathElement>) -> Option<Cow<'_, Self>> {
        if let Some(element) = rev_path.pop() {
            match (self, element) {
                (Value::Map(m), PathElement::Key(k)) => {
                    m.get(&k).and_then(|v| v.get_value_rev_path(rev_path))
                }
                (Value::Table(m), PathElement::Key(k)) => {
                    m.get(&k).and_then(|v| v.get_value_rev_path(rev_path))
                }
                (Value::Sequence(s), PathElement::Index(i)) => s
                    .get(i as usize)
                    .and_then(|v| v.get_value_rev_path(rev_path)),
                (Value::Text(t), PathElement::Index(i)) => t
                    .get(i as usize)
                    .map(|v| Cow::Owned(Value::Primitive(Primitive::Str(v.clone())))),
                (Value::Map(_), PathElement::Index(_))
                | (Value::Table(_), PathElement::Index(_))
                | (Value::Sequence(_), PathElement::Key(_))
                | (Value::Text(_), PathElement::Key(_))
                | (Value::Primitive(_), PathElement::Key(_))
                | (Value::Primitive(_), PathElement::Index(_)) => None,
            }
        } else {
            Some(Cow::Borrowed(self))
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
    actor: &amp::ActorId,
    start_op: u64,
    parent_object: amp::ObjectId,
    key: &amp::Key,
    v: &Value,
    insert: bool,
) -> (Vec<amp::Op>, u64) {
    match v {
        Value::Sequence(vs) => {
            let list_op = amp::OpId(start_op, actor.clone());
            let make_op = amp::Op {
                action: amp::OpType::Make(amp::ObjType::List),
                obj: parent_object,
                key: key.clone(),
                insert,
                pred: SortedVec::new(),
            };
            let mut op_num = start_op + 1;
            let mut result = vec![make_op];
            let mut last_elemid = amp::ElementId::Head;
            for v in vs.iter() {
                let (child_requests, new_op_num) = value_to_op_requests(
                    actor,
                    op_num,
                    amp::ObjectId::from(list_op.clone()),
                    &last_elemid.clone().into(),
                    v,
                    true,
                );
                last_elemid = amp::OpId::new(op_num, actor).into();
                op_num = new_op_num;
                result.extend(child_requests);
            }
            (result, op_num)
        }
        Value::Text(chars) => {
            let make_text_op = amp::OpId(start_op, actor.clone());
            let make_op = amp::Op {
                action: amp::OpType::Make(amp::ObjType::Text),
                obj: parent_object,
                key: key.clone(),
                insert,
                pred: SortedVec::new(),
            };
            let mut insert_ops: Vec<amp::Op> = Vec::new();
            let mut last_elemid = amp::ElementId::Head;
            let mut op_num = start_op + 1;
            for c in chars.iter() {
                insert_ops.push(amp::Op {
                    action: amp::OpType::Set(amp::ScalarValue::Str(c.clone())),
                    obj: amp::ObjectId::from(make_text_op.clone()),
                    key: last_elemid.clone().into(),
                    insert: true,
                    pred: SortedVec::new(),
                });
                last_elemid = amp::OpId::new(op_num, actor).into();
                op_num += 1;
            }
            let mut ops = vec![make_op];
            ops.extend(insert_ops.into_iter());
            (ops, op_num)
        }
        Value::Map(kvs) => {
            let make_op_id = amp::OpId::new(start_op, actor);
            let make_op = amp::Op {
                action: amp::OpType::Make(amp::ObjType::Map),
                obj: parent_object,
                key: key.clone(),
                insert,
                pred: SortedVec::new(),
            };
            let mut op_num = start_op + 1;
            let mut result = vec![make_op];
            for (key, v) in kvs.iter() {
                let (child_requests, new_op_num) = value_to_op_requests(
                    actor,
                    op_num,
                    amp::ObjectId::from(make_op_id.clone()),
                    &amp::Key::from(key.as_str()),
                    v,
                    false,
                );
                op_num = new_op_num;
                result.extend(child_requests);
            }
            (result, op_num)
        }
        Value::Table(kvs) => {
            let make_op_id = amp::OpId::new(start_op, actor);
            let make_op = amp::Op {
                action: amp::OpType::Make(amp::ObjType::Table),
                obj: parent_object,
                key: key.clone(),
                insert,
                pred: SortedVec::new(),
            };
            let mut op_num = start_op + 1;
            let mut result = vec![make_op];
            for (key, v) in kvs.iter() {
                let (child_requests, new_op_num) = value_to_op_requests(
                    actor,
                    op_num,
                    amp::ObjectId::from(make_op_id.clone()),
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
                action: amp::OpType::Set(prim_value.into()),
                obj: parent_object,
                key: key.clone(),
                insert,
                pred: SortedVec::new(),
            }];
            (ops, start_op + 1)
        }
    }
}

#[cfg(test)]
mod tests {
    use maplit::hashmap;
    use pretty_assertions::assert_eq;

    use super::*;
    use crate::Path;

    #[test]
    fn get_value() {
        let v = Value::Map(hashmap! {
            "hello".into() => Value::Primitive(Primitive::Str("world".into())),
            "again".into() => Value::Sequence(vec![Value::Primitive(Primitive::Int(2))])
        });

        assert_eq!(v.get_value(Path::root()), Some(Cow::Borrowed(&v)));
        assert_eq!(
            v.get_value(Path::root().key("hello")),
            Some(Cow::Borrowed(&Value::Primitive(Primitive::Str(
                "world".into()
            ))))
        );
        assert_eq!(v.get_value(Path::root().index(0)), None);
        assert_eq!(
            v.get_value(Path::root().key("again")),
            Some(Cow::Borrowed(&Value::Sequence(vec![Value::Primitive(
                Primitive::Int(2)
            )])))
        );
        assert_eq!(
            v.get_value(Path::root().key("again").index(0)),
            Some(Cow::Borrowed(&Value::Primitive(Primitive::Int(2))))
        );
        assert_eq!(v.get_value(Path::root().key("again").index(1)), None);
    }
}
