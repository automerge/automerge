use crate::{Automerge, ObjId, ObjType, Prop, Value};

use super::MapRef;
use super::MapRefMut;
use super::ValueRef;
use super::ValueRefMut;

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

#[derive(Debug)]
pub struct ListRefMut<'a> {
    pub(crate) obj: ObjId,
    pub(crate) doc: &'a mut Automerge,
}

impl<'a> PartialEq for ListRefMut<'a> {
    fn eq(&self, other: &Self) -> bool {
        self.obj == other.obj
            && self.len() == other.len()
            && self.iter().zip(other.iter()).all(|(a, b)| a == b)
    }
}

impl<'a> ListRefMut<'a> {
    pub fn len(&self) -> usize {
        self.doc.length(&self.obj)
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn get<P: Into<Prop>>(&self, prop: P) -> Option<ValueRef> {
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

    pub fn get_mut<P: Into<Prop>>(&mut self, prop: P) -> Option<ValueRefMut> {
        match self.doc.value(&self.obj, prop) {
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

        assert_eq!(
            list.iter().collect::<Vec<_>>(),
            vec![1u64.into(), 2u64.into()]
        );
    }
}