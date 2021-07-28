mod conflicts;
mod cursor;
mod primitive;

use std::{
    borrow::Cow,
    collections::{BTreeMap, HashMap},
};

use amp::SortedVec;
use automerge_protocol as amp;
pub use conflicts::Conflicts;
pub use cursor::Cursor;
pub use primitive::Primitive;
use serde::Serialize;
use smol_str::SmolStr;

use crate::path::PathElement;

/// A composite value, composing maps, tables, lists, text and primitives.
///
/// A `Value` is the general container type for objects in the document tree.
#[derive(Serialize, Clone, Debug, PartialEq)]
#[cfg_attr(feature = "derive-arbitrary", derive(arbitrary::Arbitrary))]
#[serde(untagged)]
pub enum Value {
    /// A mapping from string keys to values.
    Map(HashMap<SmolStr, Value>),
    /// A mapping from string keys to values, using sorted keys.
    SortedMap(BTreeMap<SmolStr, Value>),
    /// A mapping from unique IDs to values.
    Table(HashMap<SmolStr, Value>),
    /// An ordered sequence of values.
    List(Vec<Value>),
    /// An ordered sequence of grapheme clusters.
    Text(Vec<SmolStr>),
    /// A primitive value.
    Primitive(Primitive),
}

impl Value {
    /// Return whether the [`Value`] is a map.
    pub fn is_map(&self) -> bool {
        matches!(self, Self::Map(_))
    }

    /// Extract the inner map in this [`Value`] if it represents a map.
    pub fn map(&self) -> Option<&HashMap<SmolStr, Value>> {
        match self {
            Self::Map(m) => Some(m),
            _ => None,
        }
    }

    /// Return whether the [`Value`] is a sorted map.
    pub fn is_sorted_map(&self) -> bool {
        matches!(self, Self::SortedMap(_))
    }

    /// Extract the inner sorted map in this [`Value`] if it represents a sorted map.
    pub fn sorted_map(&self) -> Option<&BTreeMap<SmolStr, Value>> {
        match self {
            Self::SortedMap(m) => Some(m),
            _ => None,
        }
    }

    /// Return whether the [`Value`] is a table.
    pub fn is_table(&self) -> bool {
        matches!(self, Self::Table(_))
    }

    /// Extract the inner map in this [`Value`] if it represents a table.
    pub fn table(&self) -> Option<&HashMap<SmolStr, Value>> {
        match self {
            Self::Table(m) => Some(m),
            _ => None,
        }
    }

    /// Return whether the [`Value`] is a list.
    pub fn is_list(&self) -> bool {
        matches!(self, Self::List(_))
    }

    /// Extract the elements in this [`Value`] if it represents a list.
    pub fn list(&self) -> Option<&[Value]> {
        match self {
            Self::List(m) => Some(m),
            _ => None,
        }
    }

    /// Return whether the [`Value`] is text.
    pub fn is_text(&self) -> bool {
        matches!(self, Self::Text(_))
    }

    /// Extract the graphemes in this [`Value`] if it represents text.
    pub fn text(&self) -> Option<&[SmolStr]> {
        match self {
            Self::Text(m) => Some(m),
            _ => None,
        }
    }

    /// Return whether the [`Value`] is a primitive.
    pub fn is_primitive(&self) -> bool {
        matches!(self, Self::Primitive(_))
    }

    /// Extract the [`Primitive`] in this [`Value`] if it represents a primitive.
    pub fn primitive(&self) -> Option<&Primitive> {
        match self {
            Self::Primitive(p) => Some(p),
            _ => None,
        }
    }

    /// Convert a JSON object into a [`Value`].
    pub fn from_json(json: &serde_json::Value) -> Value {
        match json {
            serde_json::Value::Object(kvs) => {
                let result: HashMap<SmolStr, Value> = kvs
                    .iter()
                    .map(|(k, v)| (SmolStr::new(k), Value::from_json(v)))
                    .collect();
                Value::Map(result)
            }
            serde_json::Value::Array(vs) => Value::List(vs.iter().map(Value::from_json).collect()),
            serde_json::Value::String(s) => Value::Primitive(Primitive::Str(SmolStr::new(s))),
            serde_json::Value::Number(n) => {
                Value::Primitive(Primitive::F64(n.as_f64().unwrap_or(0.0)))
            }
            serde_json::Value::Bool(b) => Value::Primitive(Primitive::Boolean(*b)),
            serde_json::Value::Null => Value::Primitive(Primitive::Null),
        }
    }

    /// Convert this [`Value`] into a JSON object.
    pub fn to_json(&self) -> serde_json::Value {
        match self {
            Value::Map(map) => {
                let result: serde_json::map::Map<String, serde_json::Value> = map
                    .iter()
                    .map(|(k, v)| (k.to_string(), v.to_json()))
                    .collect();
                serde_json::Value::Object(result)
            }
            Value::SortedMap(map) => {
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
            Value::List(elements) => {
                serde_json::Value::Array(elements.iter().map(|v| v.to_json()).collect())
            }
            Value::Text(graphemes) => serde_json::Value::String(graphemes.join("")),
            Value::Primitive(v) => match v {
                Primitive::F64(n) => serde_json::Value::Number(
                    serde_json::Number::from_f64(*n).unwrap_or_else(|| serde_json::Number::from(0)),
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

    /// Get the [`Value`] at the given path, if one exists.
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
                (Value::SortedMap(m), PathElement::Key(k)) => {
                    m.get(&k).and_then(|v| v.get_value_rev_path(rev_path))
                }
                (Value::Table(m), PathElement::Key(k)) => {
                    m.get(&k).and_then(|v| v.get_value_rev_path(rev_path))
                }
                (Value::List(s), PathElement::Index(i)) => s
                    .get(i as usize)
                    .and_then(|v| v.get_value_rev_path(rev_path)),
                (Value::Text(t), PathElement::Index(i)) => t
                    .get(i as usize)
                    .map(|v| Cow::Owned(Value::Primitive(Primitive::Str(v.clone())))),
                (Value::Map(_), PathElement::Index(_))
                | (Value::SortedMap(_), PathElement::Index(_))
                | (Value::Table(_), PathElement::Index(_))
                | (Value::List(_), PathElement::Key(_))
                | (Value::Text(_), PathElement::Key(_))
                | (Value::Primitive(_), PathElement::Key(_))
                | (Value::Primitive(_), PathElement::Index(_)) => None,
            }
        } else {
            Some(Cow::Borrowed(self))
        }
    }
}

impl From<Cursor> for Value {
    fn from(c: Cursor) -> Self {
        Value::Primitive(Primitive::Cursor(c))
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
        Value::List(v.into_iter().map(|t| t.into()).collect())
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
        Value::List(vs) => {
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
        Value::SortedMap(kvs) => {
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
            "again".into() => Value::List(vec![Value::Primitive(Primitive::Int(2))])
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
            Some(Cow::Borrowed(&Value::List(vec![Value::Primitive(
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
