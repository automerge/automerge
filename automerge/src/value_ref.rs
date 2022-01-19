#[cfg(test)]
use crate::{Automerge, ObjId, ROOT};
use std::borrow::Cow;
#[cfg(test)]
use std::convert::TryFrom;

use crate::Value;
use crate::{Prop, ScalarValue};

mod list;
mod map;

pub use list::ListRef;
pub use list::ListRefMut;
pub use map::MapRef;
pub use map::MapRefMut;

#[derive(Debug, PartialEq)]
pub enum ValueRef<'a, 'h> {
    Map(MapRef<'a, 'h>),
    List(ListRef<'a, 'h>),
    Scalar(ScalarValue),
}

impl<'a, 'h> ValueRef<'a, 'h> {
    pub fn get<P: Into<Prop>>(&self, prop: P) -> Option<ValueRef<'a, 'h>> {
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

    pub fn map(&mut self) -> Option<&mut MapRef<'a, 'h>> {
        if let ValueRef::Map(map) = self {
            Some(map)
        } else {
            None
        }
    }

    pub fn list(&self) -> Option<ListRef<'a, 'h>> {
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

#[derive(Debug, PartialEq)]
pub enum ValueRefMut<'a> {
    Map(MapRefMut<'a>),
    List(ListRefMut<'a>),
    Scalar(ScalarValue),
}

impl<'a> ValueRefMut<'a> {
    pub fn get<P: Into<Prop>>(&self, prop: P) -> Option<ValueRef> {
        match self {
            ValueRefMut::Map(map) => map.get(prop),
            ValueRefMut::List(l) => l.get(prop),
            ValueRefMut::Scalar(_) => None,
        }
    }

    pub fn get_mut<P: Into<Prop>>(&mut self, prop: P) -> Option<ValueRefMut> {
        match self {
            ValueRefMut::Map(map) => map.get_mut(prop),
            ValueRefMut::List(l) => l.get_mut(prop),
            ValueRefMut::Scalar(_) => None,
        }
    }

    pub fn insert<P: Into<Prop>, V: Into<Value>>(&mut self, prop: P, value: V) {
        match self {
            ValueRefMut::Map(map) => map.insert(prop, value),
            ValueRefMut::List(list) => list.insert(prop, value),
            ValueRefMut::Scalar(_) => {}
        }
    }

    pub fn remove<P: Into<Prop>>(&mut self, prop: P) -> bool {
        match self {
            ValueRefMut::Map(map) => map.remove(prop),
            ValueRefMut::List(list) => list.remove(prop),
            ValueRefMut::Scalar(_) => false,
        }
    }

    pub fn len(&self) -> usize {
        match self {
            ValueRefMut::Map(map) => map.len(),
            ValueRefMut::List(list) => list.len(),
            ValueRefMut::Scalar(_) => 0,
        }
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn map(&self) -> Option<MapRef> {
        if let ValueRefMut::Map(map) = self {
            Some(MapRef {
                obj: map.obj.clone(),
                doc: map.doc,
                heads: Cow::Borrowed(&[]),
            })
        } else {
            None
        }
    }

    pub fn map_mut(&mut self) -> Option<&mut MapRefMut<'a>> {
        if let ValueRefMut::Map(map) = self {
            Some(map)
        } else {
            None
        }
    }

    pub fn list(&self) -> Option<ListRef> {
        if let ValueRefMut::List(list) = self {
            Some(ListRef {
                obj: list.obj.clone(),
                doc: list.doc,
                heads: Cow::Borrowed(&[]),
            })
        } else {
            None
        }
    }

    pub fn scalar(&self) -> Option<ScalarValue> {
        if let ValueRefMut::Scalar(scalar) = self {
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

impl From<u64> for ValueRef<'static, 'static> {
    fn from(u: u64) -> Self {
        ValueRef::Scalar(ScalarValue::Uint(u))
    }
}

impl From<i32> for ValueRef<'static, 'static> {
    fn from(i: i32) -> Self {
        ValueRef::Scalar(ScalarValue::Int(i as i64))
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use crate::Automerge;

    use super::*;

    #[test]
    fn get_map_key() {
        let mut doc = Automerge::try_from(json!({"a": 1})).unwrap();

        let a_val = doc.root().get("a");
        assert!(matches!(
            a_val,
            Some(ValueRef::Scalar(ScalarValue::Uint(1)))
        ));
    }

    #[test]
    fn get_nested_map() {
        let mut doc = Automerge::try_from(json!({"a": {"b": 1}})).unwrap();

        let b_val = doc.root().get("a").unwrap().get("b");

        assert!(matches!(
            b_val,
            Some(ValueRef::Scalar(ScalarValue::Uint(1)))
        ));
    }

    #[test]
    fn set_nested_map() {
        let mut doc = Automerge::new();
        let mut root = doc.root_mut();
        root.insert("a", Value::map());
        let mut a = root.get_mut("a").unwrap();
        a.insert("b", 1);

        assert_eq!(a.get("b"), Some(1.into()));
    }
}
