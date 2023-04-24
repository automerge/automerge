use std::fmt;

use crate::exid::ExId;
use crate::types::Clock;
use crate::value::Value;
use crate::Automerge;

use super::TopOps;

/// Iterator created by the [`crate::ReadDoc::values()`] and [`crate::ReadDoc::values_at()`] methods
#[derive(Default)]
pub struct Values<'a> {
    iter: Option<(TopOps<'a>, &'a Automerge, Option<Clock>)>,
}

impl<'a> Values<'a> {
    pub(crate) fn new(iter: TopOps<'a>, doc: &'a Automerge, clock: Option<Clock>) -> Self {
        Self {
            iter: Some((iter, doc, clock)),
        }
    }
}

impl<'a> fmt::Debug for Values<'a> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Values").finish()
    }
}

impl<'a> Iterator for Values<'a> {
    type Item = (Value<'a>, ExId);

    fn next(&mut self) -> Option<Self::Item> {
        self.iter
            .as_mut()
            .and_then(|(i, doc, clock)| i.next().map(|op| doc.export_value(op, clock.as_ref())))
    }
}
