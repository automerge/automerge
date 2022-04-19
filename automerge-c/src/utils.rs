use crate::{AMobj, AMresult};
use automerge as am;
use std::ops::Deref;

impl Deref for AMobj {
    type Target = am::ObjId;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

#[allow(clippy::not_unsafe_ptr_arg_deref)]
impl From<*const AMobj> for AMobj {
    fn from(obj: *const AMobj) -> Self {
        unsafe { obj.as_ref().cloned().unwrap_or(AMobj(am::ROOT)) }
    }
}

impl From<AMresult> for *mut AMresult {
    fn from(b: AMresult) -> Self {
        Box::into_raw(Box::new(b))
    }
}
