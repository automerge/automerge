use crate::{exid::ExId, Value};
use std::ops::RangeBounds;

use crate::{query, Automerge};

#[derive(Debug)]
pub struct ListRangeAt<'a, R: RangeBounds<usize>> {
    range: Option<query::ListRangeAt<'a, R>>,
    doc: &'a Automerge,
}

impl<'a, R: RangeBounds<usize>> ListRangeAt<'a, R> {
    pub(crate) fn new(doc: &'a Automerge, range: Option<query::ListRangeAt<'a, R>>) -> Self {
        Self { range, doc }
    }
}

impl<'a, R: RangeBounds<usize>> Iterator for ListRangeAt<'a, R> {
    type Item = (usize, Value<'a>, ExId);

    fn next(&mut self) -> Option<Self::Item> {
        self.range
            .as_mut()?
            .next()
            .map(|(key, value, id)| (key, value, self.doc.id_to_exid(id)))
    }
}
