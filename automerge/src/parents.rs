use crate::exid::ExId;
use crate::op_set::OpSet;
use crate::Prop;

#[derive(Debug)]
pub struct Parents<'a> {
    pub(crate) obj: ExId,
    pub(crate) doc: &'a OpSet,
}

impl<'a> Iterator for Parents<'a> {
    type Item = (ExId, Prop);

    fn next(&mut self) -> Option<Self::Item> {
        if let Some((obj, prop)) = self.doc.parent_object(&self.obj) {
            self.obj = obj.clone();
            Some((obj, prop))
        } else {
            None
        }
    }
}
