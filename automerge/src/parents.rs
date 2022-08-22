use crate::{exid::ExId, types::ObjId, Automerge, Prop};

#[derive(Debug)]
pub struct Parents<'a> {
    pub(crate) obj: ObjId,
    pub(crate) doc: &'a Automerge,
}

impl<'a> Iterator for Parents<'a> {
    type Item = (ExId, Prop);

    fn next(&mut self) -> Option<Self::Item> {
        if let Some((obj, key)) = self.doc.parent_object(self.obj) {
            self.obj = obj;
            Some((self.doc.id_to_exid(obj.0), self.doc.export_key(obj, key)))
        } else {
            None
        }
    }
}
