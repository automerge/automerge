use crate::{exid::ExId, Value};

use crate::{query, Automerge};
use std::ops::RangeBounds;

/// An iterator over the elements of a list object
///
/// This is returned by the [`crate::ReadDoc::list_range`] method
#[derive(Debug)]
pub struct ListRange<'a, R: RangeBounds<usize>> {
    range: ListRangeQuery<'a, R>,
    doc: &'a Automerge,
}

#[derive(Debug)]
enum ListRangeQuery<'a, R: RangeBounds<usize>> {
    ListRange(query::ListRange<'a, R>),
    ListRangeAt(query::ListRangeAt<'a, R>),
    None,
}

impl<'a, R: RangeBounds<usize>> ListRange<'a, R> {
    pub(crate) fn new(doc: &'a Automerge) -> Self {
        Self {
            range: ListRangeQuery::None,
            doc,
        }
    }

    pub(crate) fn with_list_range(self, query: query::ListRange<'a, R>) -> Self {
        let range = ListRangeQuery::ListRange(query);
        Self {
            range,
            doc: self.doc,
        }
    }

    pub(crate) fn with_list_range_at(self, query: query::ListRangeAt<'a, R>) -> Self {
        let range = ListRangeQuery::ListRangeAt(query);
        Self {
            range,
            doc: self.doc,
        }
    }
}

impl<'a, R: RangeBounds<usize>> Iterator for ListRange<'a, R> {
    type Item = (usize, Value<'a>, ExId);

    fn next(&mut self) -> Option<Self::Item> {
        match &mut self.range {
            ListRangeQuery::ListRange(query) => query
                .next()
                .map(|(idx, value, id)| (idx, value, self.doc.id_to_exid(id))),
            ListRangeQuery::ListRangeAt(query) => query
                .next()
                .map(|(idx, value, id)| (idx, value, self.doc.id_to_exid(id))),
            ListRangeQuery::None => None,
        }
    }
}
