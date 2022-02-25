use crate::op_set::B;
use crate::{query, Automerge};

pub struct KeysAt<'a, 'k> {
    keys: Option<query::KeysAt<'k, B>>,
    doc: &'a Automerge,
}

impl<'a, 'k> KeysAt<'a, 'k> {
    pub(crate) fn new(doc: &'a Automerge, keys: Option<query::KeysAt<'k, B>>) -> Self {
        Self { keys, doc }
    }
}

impl<'a, 'k> Iterator for KeysAt<'a, 'k> {
    type Item = String;

    fn next(&mut self) -> Option<Self::Item> {
        self.keys
            .as_mut()?
            .next()
            .map(|key| self.doc.to_string(key))
    }
}

impl<'a, 'k> DoubleEndedIterator for KeysAt<'a, 'k> {
    fn next_back(&mut self) -> Option<Self::Item> {
        self.keys
            .as_mut()?
            .next()
            .map(|key| self.doc.to_string(key))
    }
}
