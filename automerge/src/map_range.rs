use crate::{exid::ExId, Value};
use std::ops::RangeBounds;

use crate::{query, Automerge};

#[derive(Debug)]
pub struct MapRange<'a, R: RangeBounds<String>> {
    range: Option<query::MapRange<'a, R>>,
    doc: &'a Automerge,
}

impl<'a, R: RangeBounds<String>> MapRange<'a, R> {
    pub(crate) fn new(doc: &'a Automerge, range: Option<query::MapRange<'a, R>>) -> Self {
        Self { range, doc }
    }
}

impl<'a, R: RangeBounds<String>> Iterator for MapRange<'a, R> {
    type Item = (&'a str, Value<'a>, ExId);

    fn next(&mut self) -> Option<Self::Item> {
        self.range
            .as_mut()?
            .next()
            .map(|(key, value, id)| (key, value, self.doc.id_to_exid(id)))
    }
}

impl<'a, R: RangeBounds<String>> DoubleEndedIterator for MapRange<'a, R> {
    fn next_back(&mut self) -> Option<Self::Item> {
        self.range
            .as_mut()?
            .next_back()
            .map(|(key, value, id)| (key, value, self.doc.id_to_exid(id)))
    }
}
