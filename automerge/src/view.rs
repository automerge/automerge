#[cfg(test)]
use crate::{Automerge, ObjId, ROOT};
use std::borrow::Cow;
#[cfg(test)]
use std::convert::TryFrom;

use crate::Value;
use crate::{Prop, ScalarValue};

mod list;
mod map;

pub use list::ListView;
pub use list::MutableListView;
pub use map::MapView;
pub use map::MutableMapView;

#[derive(Debug, PartialEq)]
pub enum View<'a, 'h> {
    Map(MapView<'a, 'h>),
    List(ListView<'a, 'h>),
    Scalar(ScalarValue),
}

impl<'a, 'h> View<'a, 'h> {
    pub fn get<P: Into<Prop>>(&self, prop: P) -> Option<View<'a, 'h>> {
        match self {
            View::Map(map) => map.get(prop),
            View::List(l) => l.get(prop),
            View::Scalar(_) => None,
        }
    }

    pub fn len(&self) -> usize {
        match self {
            View::Map(map) => map.len(),
            View::List(list) => list.len(),
            View::Scalar(_) => 0,
        }
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn map(&mut self) -> Option<&mut MapView<'a, 'h>> {
        if let View::Map(map) = self {
            Some(map)
        } else {
            None
        }
    }

    pub fn list(&self) -> Option<ListView<'a, 'h>> {
        if let View::List(list) = self {
            Some(list.clone())
        } else {
            None
        }
    }

    pub fn scalar(&self) -> Option<ScalarValue> {
        if let View::Scalar(scalar) = self {
            Some(scalar.clone())
        } else {
            None
        }
    }
}

#[derive(Debug, PartialEq)]
pub enum MutableView<'a> {
    Map(MutableMapView<'a>),
    List(MutableListView<'a>),
    Scalar(ScalarValue),
}

impl<'a> MutableView<'a> {
    pub fn get<P: Into<Prop>>(&self, prop: P) -> Option<View> {
        match self {
            MutableView::Map(map) => map.get(prop),
            MutableView::List(l) => l.get(prop),
            MutableView::Scalar(_) => None,
        }
    }

    pub fn get_mut<P: Into<Prop>>(&mut self, prop: P) -> Option<MutableView> {
        match self {
            MutableView::Map(map) => map.get_mut(prop),
            MutableView::List(l) => l.get_mut(prop),
            MutableView::Scalar(_) => None,
        }
    }

    pub fn insert<P: Into<Prop>, V: Into<Value>>(&mut self, prop: P, value: V) {
        match self {
            MutableView::Map(map) => map.insert(prop, value),
            MutableView::List(list) => list.insert(prop, value),
            MutableView::Scalar(_) => {}
        }
    }

    pub fn remove<P: Into<Prop>>(&mut self, prop: P) -> bool {
        match self {
            MutableView::Map(map) => map.remove(prop),
            MutableView::List(list) => list.remove(prop),
            MutableView::Scalar(_) => false,
        }
    }

    pub fn len(&self) -> usize {
        match self {
            MutableView::Map(map) => map.len(),
            MutableView::List(list) => list.len(),
            MutableView::Scalar(_) => 0,
        }
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn map(&self) -> Option<MapView> {
        if let MutableView::Map(map) = self {
            Some(MapView {
                obj: map.obj.clone(),
                doc: map.doc,
                heads: Cow::Borrowed(&[]),
            })
        } else {
            None
        }
    }

    pub fn map_mut(&mut self) -> Option<&mut MutableMapView<'a>> {
        if let MutableView::Map(map) = self {
            Some(map)
        } else {
            None
        }
    }

    pub fn list(&self) -> Option<ListView> {
        if let MutableView::List(list) = self {
            Some(ListView {
                obj: list.obj.clone(),
                doc: list.doc,
                heads: Cow::Borrowed(&[]),
            })
        } else {
            None
        }
    }

    pub fn scalar(&self) -> Option<ScalarValue> {
        if let MutableView::Scalar(scalar) = self {
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

impl From<u64> for View<'static, 'static> {
    fn from(u: u64) -> Self {
        View::Scalar(ScalarValue::Uint(u))
    }
}

impl From<i32> for View<'static, 'static> {
    fn from(i: i32) -> Self {
        View::Scalar(ScalarValue::Int(i as i64))
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
        assert!(matches!(a_val, Some(View::Scalar(ScalarValue::Uint(1)))));
    }

    #[test]
    fn get_nested_map() {
        let mut doc = Automerge::try_from(json!({"a": {"b": 1}})).unwrap();

        let b_val = doc.root().get("a").unwrap().get("b");

        assert!(matches!(b_val, Some(View::Scalar(ScalarValue::Uint(1)))));
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
