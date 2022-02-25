use crate::{query::IterKeysAt, Automerge};

pub struct KeysAt<'a, 'k, const B: usize> {
    keys: Option<IterKeysAt<'k, B>>,
    doc: &'a Automerge,
}

impl<'a, 'k, const B: usize> KeysAt<'a, 'k, B> {
    pub(crate) fn new(doc: &'a Automerge, keys: Option<IterKeysAt<'k, B>>) -> Self {
        Self { keys, doc }
    }
}

impl<'a, 'k, const B: usize> Iterator for KeysAt<'a, 'k, B> {
    type Item = String;

    fn next(&mut self) -> Option<Self::Item> {
        self.keys
            .as_mut()?
            .next()
            .map(|key| self.doc.to_string(key))
    }
}

impl<'a, 'k, const B: usize> DoubleEndedIterator for KeysAt<'a, 'k, B> {
    fn next_back(&mut self) -> Option<Self::Item> {
        self.keys
            .as_mut()?
            .next()
            .map(|key| self.doc.to_string(key))
    }
}
