use crate::{exid::ExId, Value};
use std::ops::RangeBounds;

use crate::{query, Automerge, Prop};

pub struct RangeAt<'a, 'k, R: RangeBounds<Prop>> {
    range: Option<query::RangeAt<'k, R>>,
    doc: &'a Automerge,
}

impl<'a, 'k, 'm, R: RangeBounds<Prop>> RangeAt<'a, 'k, R> {
    pub(crate) fn new(doc: &'a Automerge, range: Option<query::RangeAt<'k, R>>) -> Self {
        Self { range, doc }
    }
}

impl<'a, 'k, 'm, R: RangeBounds<Prop>> Iterator for RangeAt<'a, 'k, R> {
    type Item = (String, Value, ExId);

    fn next(&mut self) -> Option<Self::Item> {
        self.range
            .as_mut()?
            .next()
            .map(|(key, value, id)| (self.doc.to_string(key), value, self.doc.id_to_exid(id)))
    }
}
