use crate::op_set::B;
use crate::{query, Automerge};

pub struct Keys<'a, 'k> {
    keys: Option<query::Keys<'k, B>>,
    doc: &'a Automerge,
}

impl<'a, 'k> Keys<'a, 'k> {
    pub(crate) fn new(doc: &'a Automerge, keys: Option<query::Keys<'k, B>>) -> Self {
        Self { keys, doc }
    }
}

impl<'a, 'k> Iterator for Keys<'a, 'k> {
    type Item = String;

    fn next(&mut self) -> Option<Self::Item> {
        self.keys
            .as_mut()?
            .next()
            .map(|key| self.doc.to_string(key))
    }
}

impl<'a, 'k> DoubleEndedIterator for Keys<'a, 'k> {
    fn next_back(&mut self) -> Option<Self::Item> {
        self.keys
            .as_mut()?
            .next_back()
            .map(|key| self.doc.to_string(key))
    }
}
