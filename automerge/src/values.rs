use crate::{exid::ExId, Value};
use std::ops::RangeFull;

use crate::{query, Automerge};

#[derive(Debug)]
pub struct Values<'a> {
    range: Option<query::ListRange<'a, RangeFull>>,
    doc: &'a Automerge,
}

impl<'a> Values<'a> {
    pub(crate) fn new(doc: &'a Automerge, range: Option<query::ListRange<'a, RangeFull>>) -> Self {
        Self { range, doc }
    }
}

impl<'a> Iterator for Values<'a> {
    type Item = (Value<'a>, ExId);

    fn next(&mut self) -> Option<Self::Item> {
        self.range
            .as_mut()?
            .next()
            .map(|(_idx, value, id)| (value, self.doc.id_to_exid(id)))
    }
}
