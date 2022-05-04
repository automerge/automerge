use crate::{exid::ExId, Value};

use crate::{query, Automerge};
use std::ops::RangeBounds;

#[derive(Debug)]
pub struct ListRange<'a, R: RangeBounds<usize>> {
    range: Option<query::ListRange<'a, R>>,
    doc: &'a Automerge,
}

impl<'a, R: RangeBounds<usize>> ListRange<'a, R> {
    pub(crate) fn new(doc: &'a Automerge, range: Option<query::ListRange<'a, R>>) -> Self {
        Self { range, doc }
    }
}

impl<'a, R: RangeBounds<usize>> Iterator for ListRange<'a, R> {
    type Item = (usize, Value<'a>, ExId);

    fn next(&mut self) -> Option<Self::Item> {
        self.range
            .as_mut()?
            .next()
            .map(|(idx, value, id)| (idx, value, self.doc.id_to_exid(id)))
    }
}
