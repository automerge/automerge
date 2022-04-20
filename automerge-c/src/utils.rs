use crate::{AMobjId, AMresult};
use automerge as am;
use std::ops::Deref;

impl Deref for AMobjId {
    type Target = am::ObjId;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

#[allow(clippy::not_unsafe_ptr_arg_deref)]
impl From<*const AMobjId> for AMobjId {
    fn from(obj_id: *const AMobjId) -> Self {
        unsafe { obj_id.as_ref().unwrap_or(AMobjId(am::ROOT)) }
    }
}

impl<'a> From<AMresult<'a>> for *mut AMresult<'a> {
    fn from(b: AMresult<'a>) -> Self {
        Box::into_raw(Box::new(b))
    }
}
