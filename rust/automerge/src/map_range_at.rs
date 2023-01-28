use crate::{exid::ExId, Value};
use std::ops::RangeBounds;

use crate::{query, Automerge};

/// An iterator over the keys and values of a map object as at a particuar heads
///
/// This is returned by the [`crate::ReadDoc::map_range_at`] method
#[derive(Debug)]
pub struct MapRangeAt<'a, R: RangeBounds<String>> {
    range: Option<query::MapRangeAt<'a, R>>,
    doc: &'a Automerge,
}

impl<'a, R: RangeBounds<String>> MapRangeAt<'a, R> {
    pub(crate) fn new(doc: &'a Automerge, range: Option<query::MapRangeAt<'a, R>>) -> Self {
        Self { range, doc }
    }
}

impl<'a, R: RangeBounds<String>> Iterator for MapRangeAt<'a, R> {
    type Item = (&'a str, Value<'a>, ExId);

    fn next(&mut self) -> Option<Self::Item> {
        self.range
            .as_mut()?
            .next()
            .map(|(key, value, id)| (key, value, self.doc.id_to_exid(id)))
    }
}

impl<'a, R: RangeBounds<String>> DoubleEndedIterator for MapRangeAt<'a, R> {
    fn next_back(&mut self) -> Option<Self::Item> {
        self.range
            .as_mut()?
            .next_back()
            .map(|(key, value, id)| (key, value, self.doc.id_to_exid(id)))
    }
}
