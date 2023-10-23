use std::fmt;

use crate::exid::ExId;
use crate::types::Clock;
use crate::value::Value;

use super::TopOps;

/// Iterator created by the [`crate::ReadDoc::values()`] and [`crate::ReadDoc::values_at()`] methods
#[derive(Default)]
pub struct Values<'a> {
    iter: Option<(TopOps<'a>, Option<Clock>)>,
}

impl<'a> Values<'a> {
    pub(crate) fn new(iter: TopOps<'a>, clock: Option<Clock>) -> Self {
        Self {
            iter: Some((iter, clock)),
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
            .and_then(|(i, clock)| i.next().map(|top| top.op.tagged_value(clock.as_ref())))
    }
}
