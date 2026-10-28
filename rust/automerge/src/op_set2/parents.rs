use crate::op_set2::OpSet;
use crate::types::{ObjId, ObjType};
use crate::{automerge::view::ReadAt, exid::ExId, Prop};

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
    pub(crate) read: ReadAt<'a>,
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
        } = self.ops.parent_object(&self.obj, &self.read).unwrap();
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
    use crate::{transaction::Transactable, AutoCommit, ObjId, ObjType, Prop, ReadDoc, ROOT};

    /// A list whose only element is a map that has since been deleted.
    fn deleted_only_child() -> (AutoCommit, ObjId, ObjId) {
        let mut doc = AutoCommit::new();
        let list = doc.put_object(ROOT, "items", ObjType::List).unwrap();
        let child = doc.insert_object(&list, 0, ObjType::Map).unwrap();
        doc.commit();
        doc.delete(&list, 0).unwrap();
        doc.commit();
        (doc, list, child)
    }

    fn deleted_link(list: &ObjId) -> Vec<Parent> {
        vec![
            Parent {
                obj: list.clone(),
                typ: ObjType::List,
                prop: Prop::Seq(0),
                visible: false,
            },
            Parent {
                obj: ROOT,
                typ: ObjType::Map,
                prop: Prop::Map("items".to_string()),
                visible: true,
            },
        ]
    }

    #[test]
    fn parents_of_deleted_only_child() {
        // No visible element follows the deleted child, which previously made
        // the slow sequence lookup disagree with the fast one.
        let (doc, list, child) = deleted_only_child();
        let parents = doc.parents(&child).unwrap().collect::<Vec<_>>();
        assert_eq!(parents, deleted_link(&list));
    }

    #[test]
    fn historical_parents_of_deleted_only_child() {
        let (mut doc, list, child) = deleted_only_child();
        let deleted_heads = doc.get_heads();
        // Make `deleted_heads` historical rather than current.
        doc.put(ROOT, "unrelated", 1).unwrap();
        doc.commit();
        let parents = doc
            .parents_at(&child, &deleted_heads)
            .unwrap()
            .collect::<Vec<_>>();
        assert_eq!(parents, deleted_link(&list));
    }

    #[test]
    fn historical_parents_before_child_creation_are_invisible() {
        let mut doc = AutoCommit::new();
        let list = doc.put_object(ROOT, "items", ObjType::List).unwrap();
        doc.commit();
        let before = doc.get_heads();
        let list_child = doc.insert_object(&list, 0, ObjType::Map).unwrap();
        let map_child = doc.put_object(ROOT, "m", ObjType::Map).unwrap();
        doc.commit();

        let parents = doc
            .parents_at(&list_child, &before)
            .unwrap()
            .collect::<Vec<_>>();
        assert_eq!(parents, deleted_link(&list));

        let parents = doc
            .parents_at(&map_child, &before)
            .unwrap()
            .collect::<Vec<_>>();
        assert_eq!(
            parents,
            vec![Parent {
                obj: ROOT,
                typ: ObjType::Map,
                prop: Prop::Map("m".to_string()),
                visible: false,
            }]
        );
    }

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
