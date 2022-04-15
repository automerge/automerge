use crate::{exid::ExId, Value};
use std::ops::RangeBounds;

use crate::{query, Automerge, Prop};

pub struct Range<'a, R: RangeBounds<Prop>> {
    range: Option<query::Range<'a, R>>,
    doc: &'a Automerge,
}

impl<'a, R: RangeBounds<Prop>> Range<'a, R> {
    pub(crate) fn new(doc: &'a Automerge, range: Option<query::Range<'a, R>>) -> Self {
        Self { range, doc }
    }
}

impl<'a, R: RangeBounds<Prop>> Iterator for Range<'a, R> {
    type Item = (String, Value<'a>, ExId);

    fn next(&mut self) -> Option<Self::Item> {
        self.range
            .as_mut()?
            .next()
            .map(|(key, value, id)| (self.doc.to_string(key), value, self.doc.id_to_exid(id)))
    }
}

impl<'a, R: RangeBounds<Prop>> DoubleEndedIterator for Range<'a, R> {
    fn next_back(&mut self) -> Option<Self::Item> {
        self.range
            .as_mut()?
            .next_back()
            .map(|(key, value, id)| (self.doc.to_string(key), value, self.doc.id_to_exid(id)))
    }
}
