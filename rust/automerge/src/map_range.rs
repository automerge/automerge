use crate::{exid::ExId, Value};
use std::ops::RangeBounds;

use crate::{query, Automerge};

/// An iterator over the keys and values of a map object
///
/// This is returned by the [`crate::ReadDoc::map_range`] method
#[derive(Debug)]
pub struct MapRange<'a, R: RangeBounds<String>> {
    range: MapRangeQuery<'a, R>,
    doc: &'a Automerge,
}

#[derive(Debug)]
enum MapRangeQuery<'a, R: RangeBounds<String>> {
    MapRange(query::MapRange<'a, R>),
    MapRangeAt(query::MapRangeAt<'a, R>),
    None,
}

impl<'a, R: RangeBounds<String>> MapRange<'a, R> {
    pub(crate) fn new(doc: &'a Automerge) -> Self {
        Self {
            range: MapRangeQuery::None,
            doc,
        }
    }
    pub(crate) fn with_map_range(self, query: query::MapRange<'a, R>) -> Self {
        let range = MapRangeQuery::MapRange(query);
        Self {
            range,
            doc: self.doc,
        }
    }
    pub(crate) fn with_map_range_at(self, query: query::MapRangeAt<'a, R>) -> Self {
        let range = MapRangeQuery::MapRangeAt(query);
        Self {
            range,
            doc: self.doc,
        }
    }
}

impl<'a, R: RangeBounds<String>> Iterator for MapRange<'a, R> {
    type Item = (&'a str, Value<'a>, ExId);

    fn next(&mut self) -> Option<Self::Item> {
        match &mut self.range {
            MapRangeQuery::MapRange(query) => query
                .next()
                .map(|(key, value, id)| (key, value, self.doc.id_to_exid(id))),
            MapRangeQuery::MapRangeAt(query) => query
                .next()
                .map(|(key, value, id)| (key, value, self.doc.id_to_exid(id))),
            MapRangeQuery::None => None,
        }
    }
}

impl<'a, R: RangeBounds<String>> DoubleEndedIterator for MapRange<'a, R> {
    fn next_back(&mut self) -> Option<Self::Item> {
        match &mut self.range {
            MapRangeQuery::MapRange(query) => query
                .next_back()
                .map(|(key, value, id)| (key, value, self.doc.id_to_exid(id))),
            MapRangeQuery::MapRangeAt(query) => query
                .next_back()
                .map(|(key, value, id)| (key, value, self.doc.id_to_exid(id))),
            MapRangeQuery::None => None,
        }
    }
}
