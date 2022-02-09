use automerge as am;
use std::ops::{Deref, DerefMut};

#[derive(Clone)]
pub struct AMdoc(am::Automerge);

impl AMdoc {
    pub fn create(handle: am::Automerge) -> AMdoc {
        AMdoc(handle)
    }
}

impl Deref for AMdoc {
    type Target = am::Automerge;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for AMdoc {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl From<AMdoc> for *mut AMdoc {
    fn from(b: AMdoc) -> Self {
        Box::into_raw(Box::new(b))
    }
}
