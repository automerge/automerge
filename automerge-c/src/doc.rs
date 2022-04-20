use automerge as am;
use std::collections::BTreeSet;
use std::ops::{Deref, DerefMut};

use crate::result::AMobjId;
use automerge::transaction::Transactable;

/// \struct AMdoc
/// \brief A JSON-like CRDT.
#[derive(Clone)]
pub struct AMdoc {
    body: am::AutoCommit,
    obj_ids: BTreeSet<AMobjId>,
}

impl AMdoc {
    pub fn new(body: am::AutoCommit) -> Self {
        Self {
            body: body,
            obj_ids: BTreeSet::new(),
        }
    }

    pub fn insert_object(
        &mut self,
        obj: &am::ObjId,
        index: usize,
        value: am::ObjType,
    ) -> Result<&AMobjId, am::AutomergeError> {
        match self.body.insert_object(obj, index, value) {
            Ok(ex_id) => {
                let obj_id = AMobjId::new(ex_id);
                self.obj_ids.insert(obj_id.clone());
                match self.obj_ids.get(&obj_id) {
                    Some(obj_id) => Ok(obj_id),
                    None => Err(am::AutomergeError::Fail),
                }
            }
            Err(e) => Err(e),
        }
    }

    pub fn put_object<O: AsRef<am::ObjId>, P: Into<am::Prop>>(
        &mut self,
        obj: O,
        prop: P,
        value: am::ObjType,
    ) -> Result<&AMobjId, am::AutomergeError> {
        match self.body.put_object(obj, prop, value) {
            Ok(ex_id) => {
                let obj_id = AMobjId::new(ex_id);
                self.obj_ids.insert(obj_id.clone());
                match self.obj_ids.get(&obj_id) {
                    Some(obj_id) => Ok(obj_id),
                    None => Err(am::AutomergeError::Fail),
                }
            }
            Err(e) => Err(e),
        }
    }

    pub fn drop_obj_id(&mut self, obj_id: &AMobjId) -> bool {
        self.obj_ids.remove(obj_id)
    }
}

impl Deref for AMdoc {
    type Target = am::AutoCommit;

    fn deref(&self) -> &Self::Target {
        &self.body
    }
}

impl DerefMut for AMdoc {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.body
    }
}

impl From<AMdoc> for *mut AMdoc {
    fn from(b: AMdoc) -> Self {
        Box::into_raw(Box::new(b))
    }
}
