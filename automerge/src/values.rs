use crate::{exid::ExId, Value};
use std::ops::RangeFull;

use crate::{query, Automerge};

pub struct Values<'a, 'k> {
    range: Option<query::Range<'k, RangeFull>>,
    doc: &'a Automerge,
}

impl<'a, 'k> Values<'a, 'k> {
    pub(crate) fn new(doc: &'a Automerge, range: Option<query::Range<'k, RangeFull>>) -> Self {
        Self { range, doc }
    }
}

impl<'a, 'k> Iterator for Values<'a, 'k> {
    type Item = (String, Value, ExId);

    fn next(&mut self) -> Option<Self::Item> {
        self.range
            .as_mut()?
            .next()
            .map(|(key, value, id)| (self.doc.to_string(key), value, self.doc.id_to_exid(id)))
    }
}
