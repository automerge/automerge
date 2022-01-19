#[cfg(test)]
use crate::{Automerge, ObjId, Value, ROOT};
#[cfg(test)]
use std::convert::TryFrom;

use crate::{Prop, ScalarValue};

mod list;
mod map;

pub use list::ListRef;
pub use map::MapRef;

#[derive(Debug, PartialEq)]
pub enum ValueRef<'a> {
    Map(MapRef<'a>),
    List(ListRef<'a>),
    Scalar(ScalarValue),
}

impl<'a> ValueRef<'a> {
    pub fn get<P: Into<Prop>>(&self, prop: P) -> Option<ValueRef<'a>> {
        match self {
            ValueRef::Map(map) => map.get(prop),
            ValueRef::List(l) => l.get(prop),
            ValueRef::Scalar(_) => None,
        }
    }

    pub fn len(&self) -> usize {
        match self {
            ValueRef::Map(map) => map.len(),
            ValueRef::List(list) => list.len(),
            ValueRef::Scalar(_) => 0,
        }
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn map(&self) -> Option<MapRef<'a>> {
        if let ValueRef::Map(map) = self {
            Some(map.clone())
        } else {
            None
        }
    }

    pub fn list(&self) -> Option<ListRef<'a>> {
        if let ValueRef::List(list) = self {
            Some(list.clone())
        } else {
            None
        }
    }

    pub fn scalar(&self) -> Option<ScalarValue> {
        if let ValueRef::Scalar(scalar) = self {
            Some(scalar.clone())
        } else {
            None
        }
    }
}

#[cfg(test)]
impl TryFrom<serde_json::Value> for Automerge {
    type Error = String;

    fn try_from(value: serde_json::Value) -> Result<Self, Self::Error> {
        use serde_json::Map;
        fn add_map(map: Map<String, serde_json::Value>, doc: &mut Automerge, obj: ObjId) {
            for (k, v) in map.into_iter() {
                match v {
                    serde_json::Value::Null => {
                        doc.set(obj.clone(), k, ()).unwrap();
                    }
                    serde_json::Value::Bool(b) => {
                        doc.set(obj.clone(), k, b).unwrap();
                    }
                    serde_json::Value::Number(n) => {
                        doc.set(obj.clone(), k, n.as_u64().unwrap())
                            .expect("no error");
                    }
                    serde_json::Value::String(s) => {
                        doc.set(obj.clone(), k, s.to_owned()).unwrap().unwrap();
                    }
                    serde_json::Value::Array(a) => {
                        let obj = doc.set(obj.clone(), k, Value::list()).unwrap().unwrap();
                        add_list(a, doc, obj)
                    }
                    serde_json::Value::Object(map) => {
                        let obj = doc.set(obj.clone(), k, Value::map()).unwrap().unwrap();
                        add_map(map, doc, obj)
                    }
                };
            }
        }

        fn add_list(list: Vec<serde_json::Value>, doc: &mut Automerge, obj: ObjId) {
            for (i, v) in list.into_iter().enumerate() {
                match v {
                    serde_json::Value::Null => {
                        doc.set(obj.clone(), i, ()).unwrap();
                    }
                    serde_json::Value::Bool(b) => {
                        doc.set(obj.clone(), i, b).unwrap();
                    }
                    serde_json::Value::Number(n) => {
                        doc.insert(obj.clone(), i, n.as_u64().unwrap())
                            .expect("no error");
                    }
                    serde_json::Value::String(s) => {
                        doc.set(obj.clone(), i, s.to_owned()).unwrap().unwrap();
                    }
                    serde_json::Value::Array(a) => {
                        let obj = doc.set(obj.clone(), i, Value::list()).unwrap().unwrap();
                        add_list(a, doc, obj)
                    }
                    serde_json::Value::Object(map) => {
                        let obj = doc.set(obj.clone(), i, Value::map()).unwrap().unwrap();
                        add_map(map, doc, obj)
                    }
                };
            }
        }

        if let serde_json::Value::Object(o) = value {
            let mut doc = Automerge::new();
            add_map(o, &mut doc, ROOT);
            Ok(doc)
        } else {
            Err("wasn't an object".to_owned())
        }
    }
}

impl From<u64> for ValueRef<'static> {
    fn from(u: u64) -> Self {
        ValueRef::Scalar(ScalarValue::Uint(u))
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use crate::Automerge;

    use super::*;

    #[test]
    fn get_map_key() {
        let doc = Automerge::try_from(json!({"a": 1})).unwrap();

        let a_val = doc.root().get("a");
        assert!(matches!(
            a_val,
            Some(ValueRef::Scalar(ScalarValue::Uint(1)))
        ));
    }

    #[test]
    fn get_nested_map() {
        let doc = Automerge::try_from(json!({"a": {"b": 1}})).unwrap();

        let b_val = doc.root().get("a").unwrap().get("b");

        assert!(matches!(
            b_val,
            Some(ValueRef::Scalar(ScalarValue::Uint(1)))
        ));
    }
}
