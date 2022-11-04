use crate::op_set::OpSet;
use crate::types::ObjId;
use crate::{exid::ExId, Prop};

#[derive(Debug)]
pub struct Parents<'a> {
    pub(crate) obj: ObjId,
    pub(crate) ops: &'a OpSet,
}

impl<'a> Parents<'a> {
    pub fn path(&mut self) -> Vec<(ExId, Prop)> {
        let mut path = self.map(|(id, prop, _)| (id, prop)).collect::<Vec<_>>();
        path.reverse();
        path
    }

    pub fn visible_path(&mut self) -> Option<Vec<(ExId, Prop)>> {
        let mut path = Vec::new();
        for (id, prop, vis) in self {
            if !vis {
                return None;
            }
            path.push((id, prop))
        }
        path.reverse();
        Some(path)
    }
}

impl<'a> Iterator for Parents<'a> {
    type Item = (ExId, Prop, bool);

    fn next(&mut self) -> Option<Self::Item> {
        if self.obj.is_root() {
            None
        } else if let Some((obj, key, visible)) = self.ops.parent_object(&self.obj) {
            self.obj = obj;
            Some((
                self.ops.id_to_exid(self.obj.0),
                self.ops.export_key(self.obj, key),
                visible,
            ))
        } else {
            None
        }
    }
}
