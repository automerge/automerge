use crate::op_set::OpSet;
use crate::types::{ObjId, Prop};

#[derive(Debug)]
pub struct Path<'a> {
    pub(crate) obj: ObjId,
    pub(crate) op_set: &'a OpSet,
}

impl<'a> Iterator for Path<'a> {
    type Item = Prop;

    fn next(&mut self) -> Option<Self::Item> {
        if let Some((obj, prop)) = self.op_set.parent_prop(&self.obj) {
            self.obj = obj;
            Some(prop)
        } else {
            None
        }
    }
}
