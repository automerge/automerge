use crate::{Automerge, ObjId, ObjType, Prop, Value};

use super::MapRef;
use super::ValueRef;

#[derive(Debug, Clone)]
pub struct ListRef<'a> {
    pub(crate) obj: ObjId,
    pub(crate) doc: &'a Automerge,
}

impl<'a> PartialEq for ListRef<'a> {
    fn eq(&self, other: &Self) -> bool {
        self.obj == other.obj
            && self.len() == other.len()
            && self.iter().zip(other.iter()).all(|(a, b)| a == b)
    }
}

impl<'a> ListRef<'a> {
    pub fn len(&self) -> usize {
        self.doc.length(&self.obj)
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn get<P: Into<Prop>>(&self, prop: P) -> Option<ValueRef<'a>> {
        match self.doc.value(&self.obj, prop) {
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

    pub fn iter(&self) -> impl Iterator<Item = ValueRef> {
        (0..self.len()).map(move |i| self.get(i).unwrap())
    }
}

#[cfg(test)]
mod tests {
    use crate::ScalarValue;
    use serde_json::json;
    use std::convert::TryFrom;

    use super::*;

    #[test]
    fn test_list() {
        let doc = Automerge::try_from(json!({
            "a": [1, 2],
        }))
        .unwrap();

        let list = doc.root().get("a").unwrap().list().unwrap();

        assert_eq!(list.get(0), Some(ValueRef::Scalar(ScalarValue::Uint(1))));

        assert_eq!(list.len(), 2);

        assert_eq!(list.is_empty(), false);

        assert_eq!(list.iter().collect::<Vec<_>>(), vec![1.into(), 2.into()]);
    }
}
