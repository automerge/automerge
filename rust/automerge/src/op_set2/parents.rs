use crate::op_set2::OpSet;
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
    pub(crate) ops: &'a OpSet,
    pub(crate) clock: Option<Clock>,
}

impl Parents<'_> {
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

impl Iterator for Parents<'_> {
    type Item = Parent;

    fn next(&mut self) -> Option<Self::Item> {
        if self.obj.is_root() {
            return None;
        }
        let super::op_set::Parent {
            obj,
            typ,
            prop,
            visible,
            ..
        } = self
            .ops
            .parent_object(&self.obj, self.clock.as_ref())
            .unwrap();
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

    #[test]
    fn test_invisible_parents_of_a_deleted_last_element() {
        // Create a document whose last list element is an object, delete that element, then
        // generate a path to the object it contained. Nothing visible follows the deleted
        // element.

        let mut doc = crate::AutoCommit::new();
        let list = doc
            .put_object(crate::ROOT, "list", crate::ObjType::List)
            .unwrap();
        doc.insert(&list, 0, "a").unwrap();
        let obj = doc.insert_object(&list, 1, crate::ObjType::Map).unwrap();
        doc.delete(&list, 1).unwrap();
        let after_delete = doc.get_heads();
        // an unrelated change, so that `after_delete` is in the past
        doc.put(crate::ROOT, "unrelated", 1).unwrap();

        let expected = vec![
            Parent {
                obj: crate::ROOT,
                prop: Prop::Map("list".to_string()),
                visible: true,
                typ: ObjType::Map,
            },
            Parent {
                obj: list,
                prop: Prop::Seq(1),
                visible: false,
                typ: ObjType::List,
            },
        ];

        let mut parents = doc.parents(&obj).unwrap().collect::<Vec<_>>();
        parents.reverse();
        assert_eq!(parents, expected);

        let mut parents_at = doc
            .parents_at(&obj, &after_delete)
            .unwrap()
            .collect::<Vec<_>>();
        parents_at.reverse();
        assert_eq!(parents_at, expected);
    }
}
