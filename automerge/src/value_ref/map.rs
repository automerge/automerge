use std::borrow::Cow;

use itertools::Itertools;

use crate::{Automerge, ChangeHash, ObjId, ObjType, Prop, Value};

use super::{list::ListRefMut, ListRef, ValueRef, ValueRefMut};

#[derive(Debug)]
pub struct MapRef<'a, 'h> {
    pub(crate) obj: ObjId,
    pub(crate) doc: &'a Automerge,
    pub(crate) heads: Cow<'h, [ChangeHash]>,
}

impl<'a, 'h> PartialEq for MapRef<'a, 'h> {
    fn eq(&self, other: &Self) -> bool {
        self.obj == other.obj
            && self.len() == other.len()
            && self
                .iter()
                .sorted_by_key(|(key, _)| key.clone())
                .zip(other.iter().sorted_by_key(|(key, _)| key.clone()))
                .all(|(a, b)| a == b)
    }
}

impl<'a, 'h> MapRef<'a, 'h> {
    pub fn len(&self) -> usize {
        self.doc.length_at(&self.obj, &self.heads)
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn get<P: Into<Prop>>(&self, key: P) -> Option<ValueRef<'a, 'h>> {
        match self.doc.value_at(&self.obj, key, &self.heads) {
            Ok(Some((value, id))) => match value {
                Value::Object(ObjType::Map) => Some(ValueRef::Map(MapRef {
                    obj: id,
                    doc: self.doc,
                    heads: self.heads.clone(),
                })),
                Value::Object(ObjType::Table) => todo!(),
                Value::Object(ObjType::List) => Some(ValueRef::List(ListRef {
                    obj: id,
                    doc: self.doc,
                    heads: self.heads.clone(),
                })),
                Value::Object(ObjType::Text) => todo!(),
                Value::Scalar(s) => Some(ValueRef::Scalar(s)),
            },
            Ok(None) | Err(_) => None,
        }
    }

    pub fn contains_key<P: Into<Prop>>(&self, key: P) -> bool {
        self.get(key).is_some()
    }

    pub fn keys(&self) -> impl Iterator<Item = String> {
        self.doc.keys_at(&self.obj, &self.heads).into_iter()
    }

    pub fn values(&self) -> impl Iterator<Item = ValueRef> {
        self.keys().map(move |key| self.get(key).unwrap())
    }

    pub fn iter(&self) -> impl Iterator<Item = (String, ValueRef)> {
        self.keys().map(move |key| {
            let v = self.get(&key).unwrap();
            (key, v)
        })
    }
}

// MapRefMut isn't allowed to travel to the past as it can't be mutated.
#[derive(Debug)]
pub struct MapRefMut<'a> {
    pub(crate) obj: ObjId,
    pub(crate) doc: &'a mut Automerge,
}

impl<'a> PartialEq for MapRefMut<'a> {
    fn eq(&self, other: &Self) -> bool {
        self.obj == other.obj
            && self.len() == other.len()
            && self
                .iter()
                .sorted_by_key(|(key, _)| key.clone())
                .zip(other.iter().sorted_by_key(|(key, _)| key.clone()))
                .all(|(a, b)| a == b)
    }
}

impl<'a> MapRefMut<'a> {
    pub fn into_immutable(self) -> MapRef<'a, 'static> {
        let heads = self.doc.get_heads();
        MapRef {
            obj: self.obj,
            doc: self.doc,
            heads: Cow::Owned(heads),
        }
    }

    pub fn len(&self) -> usize {
        self.doc.length(&self.obj)
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn get<P: Into<Prop>>(&self, key: P) -> Option<ValueRef> {
        match self.doc.value(&self.obj, key) {
            Ok(Some((value, id))) => match value {
                Value::Object(ObjType::Map) => Some(ValueRef::Map(MapRef {
                    obj: id,
                    doc: self.doc,
                    heads: Cow::Borrowed(&[]),
                })),
                Value::Object(ObjType::Table) => todo!(),
                Value::Object(ObjType::List) => Some(ValueRef::List(ListRef {
                    obj: id,
                    doc: self.doc,
                    heads: Cow::Borrowed(&[]),
                })),
                Value::Object(ObjType::Text) => todo!(),
                Value::Scalar(s) => Some(ValueRef::Scalar(s)),
            },
            Ok(None) | Err(_) => None,
        }
    }

    pub fn get_mut<P: Into<Prop>>(&mut self, key: P) -> Option<ValueRefMut> {
        match self.doc.value(&self.obj, key) {
            Ok(Some((value, id))) => match value {
                Value::Object(ObjType::Map) => Some(ValueRefMut::Map(MapRefMut {
                    obj: id,
                    doc: self.doc,
                })),
                Value::Object(ObjType::Table) => todo!(),
                Value::Object(ObjType::List) => Some(ValueRefMut::List(ListRefMut {
                    obj: id,
                    doc: self.doc,
                })),
                Value::Object(ObjType::Text) => todo!(),
                Value::Scalar(s) => Some(ValueRefMut::Scalar(s)),
            },
            Ok(None) | Err(_) => None,
        }
    }

    pub fn insert<P: Into<Prop>, V: Into<Value>>(&mut self, prop: P, value: V) {
        self.doc.set(&self.obj, prop, value).unwrap();
    }

    // TODO: change this to return the valueref that was removed, using the old heads, once
    // valueref can work in the past
    pub fn remove<P: Into<Prop>>(&mut self, prop: P) -> bool {
        let prop = prop.into();
        let exists = self.get(prop.clone()).is_some();
        self.doc.del(&self.obj, prop).unwrap();
        exists
    }

    pub fn contains_key<P: Into<Prop>>(&self, key: P) -> bool {
        self.get(key).is_some()
    }

    pub fn keys(&self) -> impl Iterator<Item = String> {
        self.doc.keys(&self.obj).into_iter()
    }

    pub fn values(&self) -> impl Iterator<Item = ValueRef> {
        self.keys().map(move |key| self.get(key).unwrap())
    }

    pub fn iter(&self) -> impl Iterator<Item = (String, ValueRef)> {
        self.keys().map(move |key| {
            let v = self.get(&key).unwrap();
            (key, v)
        })
    }
}

#[cfg(test)]
mod tests {
    use crate::ScalarValue;
    use serde_json::json;
    use std::convert::TryFrom;

    use super::*;

    #[test]
    fn test_map() {
        let mut doc = Automerge::try_from(json!({
            "a": 1,
            "b": 2,
        }))
        .unwrap();

        let root = doc.root();

        assert!(matches!(
            root.get("a"),
            Some(ValueRef::Scalar(ScalarValue::Uint(1)))
        ));

        assert!(matches!(
            root.get("b"),
            Some(ValueRef::Scalar(ScalarValue::Uint(2)))
        ));

        assert_eq!(root.len(), 2);

        assert_eq!(root.is_empty(), false);

        assert_eq!(root.contains_key("a"), true);

        assert_eq!(root.contains_key("c"), false);

        assert_eq!(root.keys().collect::<Vec<String>>(), vec!["a", "b"]);

        assert_eq!(root.values().collect::<Vec<_>>(), vec![1.into(), 2.into()]);

        assert_eq!(
            root.iter().collect::<Vec<_>>(),
            vec![("a".to_owned(), 1.into()), ("b".to_owned(), 2.into())]
        );
    }

    #[test]
    fn test_map_mut() {
        let mut doc = Automerge::try_from(json!({
            "a": 1,
            "b": 2,
        }))
        .unwrap();

        let mut root = doc.root_mut();

        assert!(matches!(
            root.get("a"),
            Some(ValueRef::Scalar(ScalarValue::Uint(1)))
        ));

        assert!(matches!(
            root.get("b"),
            Some(ValueRef::Scalar(ScalarValue::Uint(2)))
        ));

        assert_eq!(root.len(), 2);

        assert_eq!(root.is_empty(), false);

        assert_eq!(root.contains_key("a"), true);

        assert_eq!(root.contains_key("c"), false);

        assert_eq!(root.keys().collect::<Vec<String>>(), vec!["a", "b"]);

        assert_eq!(root.values().collect::<Vec<_>>(), vec![1.into(), 2.into()]);

        assert_eq!(
            root.iter().collect::<Vec<_>>(),
            vec![("a".to_owned(), 1.into()), ("b".to_owned(), 2.into())]
        );

        root.insert("c", 5);

        assert_eq!(root.len(), 3);
        assert_eq!(root.contains_key("c"), true);

        assert_eq!(root.remove("a"), true);
        assert_eq!(root.remove("a"), false);
        assert_eq!(root.len(), 2);

        let imm = root.into_immutable();
        assert_eq!(imm.contains_key("c"), true);
    }

    #[test]
    fn nested_map() {
        let mut doc = Automerge::new();
        let mut root = doc.root_mut();

        root.insert("a", Value::map());
        let mut a = root.get_mut("a").unwrap();
        let a_map = a.map_mut().unwrap();
        a_map.insert("b", 1);

        assert_eq!(a_map.contains_key("b"), true);
    }
}
