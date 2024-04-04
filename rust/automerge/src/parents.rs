use crate::op_set;
use crate::op_set::OpSet;
use crate::patches::TextRepresentation;
use crate::types::{ObjId, ObjType};
use crate::{clock::Clock, exid::ExId, Prop};

/// An iterator over the "parents" of an object
///
/// The "parent" of an object in this context is the ([`ExId`], [`Prop`]) pair which specifies the
/// location of this object in the composite object which contains it. Each element in the iterator
/// is a [`Parent`], yielded in reverse order. This means that once the iterator returns `None` you
/// have reached the root of the document.
///
/// This is returned by [`crate::ReadDoc::parents`]
#[derive(Debug, Clone)]
pub struct Parents<'a> {
    pub(crate) obj: ObjId,
    pub(crate) text_rep: TextRepresentation,
    pub(crate) ops: &'a OpSet,
    pub(crate) clock: Option<Clock>,
}

impl<'a> Parents<'a> {
    /// Return the path this `Parents` represents
    ///
    /// This is _not_ in reverse order.
    pub fn path(self) -> Vec<(ExId, Prop)> {
        let mut path = self
            .map(|Parent { obj, prop, .. }| (obj, prop))
            .collect::<Vec<_>>();
        path.reverse();
        path
    }

    /// Like `path` but returns `None` if the target is not visible
    pub fn visible_path(self) -> Option<Vec<(ExId, Prop)>> {
        let mut path = Vec::new();
        for Parent {
            obj, prop, visible, ..
        } in self
        {
            if !visible {
                return None;
            }
            path.push((obj, prop))
        }
        path.reverse();
        Some(path)
    }
}

impl<'a> Iterator for Parents<'a> {
    type Item = Parent;

    fn next(&mut self) -> Option<Self::Item> {
        if self.obj.is_root() {
            return None;
        }
        let op_set::Parent {
            obj,
            typ,
            prop,
            visible,
            ..
        } = self
            .ops
            .parent_object(&self.obj, self.text_rep, self.clock.as_ref())?;
        self.obj = obj;
        let obj = self.ops.id_to_exid(self.obj.0);
        Some(Parent {
            obj,
            typ,
            prop,
            visible,
        })
    }
}

/// A component of a path to an object
#[derive(Debug, PartialEq, Eq)]
pub struct Parent {
    /// The object ID this component refers to
    pub obj: ExId,
    /// The type of the parent object
    pub typ: ObjType,
    /// The property within `obj` this component refers to
    pub prop: Prop,
    /// Whether this component is "visible"
    ///
    /// An "invisible" component is one where the property is hidden, either because it has been
    /// deleted or because there is a conflict on this (object, property) pair and this value does
    /// not win the conflict.
    pub visible: bool,
}

#[cfg(test)]
mod tests {
    use super::Parent;
    use crate::{transaction::Transactable, ObjType, Prop, ReadDoc};

    #[test]
    fn test_invisible_parents() {
        // Create a document with a list of objects, then delete one of the objects, then generate
        // a path to the deleted object.

        let mut doc = crate::AutoCommit::new();
        let list = doc
            .put_object(crate::ROOT, "list", crate::ObjType::List)
            .unwrap();
        let obj1 = doc.insert_object(&list, 0, crate::ObjType::Map).unwrap();
        let _obj2 = doc.insert_object(&list, 1, crate::ObjType::Map).unwrap();
        doc.put(&obj1, "key", "value").unwrap();
        doc.delete(&list, 0).unwrap();

        let mut parents = doc.parents(&obj1).unwrap().collect::<Vec<_>>();
        parents.reverse();
        assert_eq!(
            parents,
            vec![
                Parent {
                    obj: crate::ROOT,
                    prop: Prop::Map("list".to_string()),
                    visible: true,
                    typ: ObjType::Map,
                },
                Parent {
                    obj: list,
                    prop: Prop::Seq(0),
                    visible: false,
                    typ: ObjType::List,
                },
            ]
        );
    }
}
