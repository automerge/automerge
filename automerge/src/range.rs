use crate::{exid::ExId, Value};
use std::ops::RangeBounds;

use crate::{query, Automerge};

pub struct Range<'a, R: RangeBounds<String>> {
    range: Option<query::Range<'a, R>>,
    doc: &'a Automerge,
}

impl<'a, R: RangeBounds<String>> Range<'a, R> {
    pub(crate) fn new(doc: &'a Automerge, range: Option<query::Range<'a, R>>) -> Self {
        Self { range, doc }
    }
}

impl<'a, R: RangeBounds<String>> Iterator for Range<'a, R> {
    type Item = (String, Value<'a>, ExId);

    fn next(&mut self) -> Option<Self::Item> {
        self.range
            .as_mut()?
            .next()
            .map(|(key, value, id)| (self.doc.to_string(key), value, self.doc.id_to_exid(id)))
    }
}

impl<'a, R: RangeBounds<String>> DoubleEndedIterator for Range<'a, R> {
    fn next_back(&mut self) -> Option<Self::Item> {
        self.range
            .as_mut()?
            .next_back()
            .map(|(key, value, id)| (self.doc.to_string(key), value, self.doc.id_to_exid(id)))
    }
}
