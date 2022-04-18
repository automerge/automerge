use crate::{exid::ExId, Automerge, Prop};

pub struct Parents<'a> {
    pub(crate) obj: ExId,
    pub(crate) doc: &'a Automerge,
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
