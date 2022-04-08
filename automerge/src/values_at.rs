use crate::{exid::ExId, Value};
use std::ops::RangeFull;

use crate::{query, Automerge};

pub struct ValuesAt<'a> {
    range: Option<query::RangeAt<'a, RangeFull>>,
    doc: &'a Automerge,
}

impl<'a> ValuesAt<'a> {
    pub(crate) fn new(doc: &'a Automerge, range: Option<query::RangeAt<'a, RangeFull>>) -> Self {
        Self { range, doc }
    }
}

impl<'a> Iterator for ValuesAt<'a> {
    type Item = (String, Value<'a>, ExId);

    fn next(&mut self) -> Option<Self::Item> {
        self.range
            .as_mut()?
            .next()
            .map(|(key, value, id)| (self.doc.to_string(key), value, self.doc.id_to_exid(id)))
    }
}
