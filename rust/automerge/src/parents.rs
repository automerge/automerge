use crate::op_set;
use crate::op_set::OpSet;
use crate::types::{ListEncoding, ObjId};
use crate::{exid::ExId, Prop};

#[derive(Debug)]
pub struct Parents<'a> {
    pub(crate) obj: ObjId,
    pub(crate) ops: &'a OpSet,
}

impl<'a> Parents<'a> {
    // returns the path to the object
    // works even if the object or a parent has been deleted
    pub fn path(&mut self) -> Vec<(ExId, Prop)> {
        let mut path = self
            .map(|Parent { obj, prop, .. }| (obj, prop))
            .collect::<Vec<_>>();
        path.reverse();
        path
    }

    // returns the path to the object
    // if the object or one of its parents has been deleted or conflicted out
    // returns none
    pub fn visible_path(&mut self) -> Option<Vec<(ExId, Prop)>> {
        let mut path = Vec::new();
        for Parent { obj, prop, visible } in self {
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
            None
        } else if let Some(op_set::Parent { obj, key, visible }) = self.ops.parent_object(&self.obj)
        {
            self.obj = obj;
            Some(Parent {
                obj: self.ops.id_to_exid(self.obj.0),
                prop: self.ops.export_key(self.obj, key, ListEncoding::List),
                visible,
            })
        } else {
            None
        }
    }
}

#[derive(Debug, PartialEq, Eq)]
pub struct Parent {
    pub obj: ExId,
    pub prop: Prop,
    pub visible: bool,
}
