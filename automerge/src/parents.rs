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
        let mut path = self.collect::<Vec<_>>();
        path.reverse();
        path
    }
}

impl<'a> Iterator for Parents<'a> {
    type Item = (ExId, Prop);

    fn next(&mut self) -> Option<Self::Item> {
        if self.obj.is_root() {
            None
        } else if let Some((obj, key)) = self.ops.parent_object(&self.obj) {
            self.obj = obj;
            Some((
                self.ops.id_to_exid(self.obj.0),
                self.ops.export_key(self.obj, key),
            ))
        } else {
            None
        }
    }
}
