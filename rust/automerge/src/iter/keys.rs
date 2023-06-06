use std::fmt;

use crate::op_set::OpSet;

use super::TopOps;

/// Iterator created by the [`crate::ReadDoc::keys()`] and [`crate::ReadDoc::keys_at()`] methods
#[derive(Default)]
pub struct Keys<'a> {
    pub(crate) iter: Option<(TopOps<'a>, &'a OpSet)>,
}

impl<'a> fmt::Debug for Keys<'a> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Keys").finish()
    }
}

impl<'a> Iterator for Keys<'a> {
    type Item = String;

    fn next(&mut self) -> Option<Self::Item> {
        self.iter
            .as_mut()
            .and_then(|(i, op_set)| i.next().map(|top| op_set.to_string(top.op.elemid_or_key())))
    }
}
