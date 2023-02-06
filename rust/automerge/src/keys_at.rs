use crate::{query, Automerge};

/// An iterator over the keys of an object at a particular point in history
///
/// This is returned by [`crate::ReadDoc::keys_at`] method. The returned item is either the keys of a map,
/// or the encoded element IDs of a sequence.
#[derive(Debug)]
pub struct KeysAt<'a, 'k> {
    keys: Option<query::KeysAt<'k>>,
    doc: &'a Automerge,
}

impl<'a, 'k> KeysAt<'a, 'k> {
    pub(crate) fn new(doc: &'a Automerge, keys: Option<query::KeysAt<'k>>) -> Self {
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
