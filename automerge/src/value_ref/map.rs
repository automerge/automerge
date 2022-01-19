use itertools::Itertools;

use crate::{Automerge, ObjId, ObjType, Prop, Value};

use super::{ListRef, ValueRef};

#[derive(Debug, Clone)]
pub struct MapRef<'a> {
    pub(crate) obj: ObjId,
    pub(crate) doc: &'a Automerge,
}

impl<'a> PartialEq for MapRef<'a> {
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

impl<'a> MapRef<'a> {
    pub fn len(&self) -> usize {
        self.doc.length(&self.obj)
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn get<P: Into<Prop>>(&self, key: P) -> Option<ValueRef<'a>> {
        match self.doc.value(&self.obj, key) {
            Ok(Some((value, id))) => match value {
                Value::Object(ObjType::Map) => Some(ValueRef::Map(MapRef {
                    obj: id,
                    doc: self.doc,
                })),
                Value::Object(ObjType::Table) => todo!(),
                Value::Object(ObjType::List) => Some(ValueRef::List(ListRef {
                    obj: id,
                    doc: self.doc,
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
        let doc = Automerge::try_from(json!({
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
}
