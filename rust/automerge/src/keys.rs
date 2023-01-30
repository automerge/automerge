use crate::{query, Automerge};

/// An iterator over the keys of an object
///
/// This is returned by [`crate::ReadDoc::keys`] and method. The returned item is either
/// the keys of a map, or the encoded element IDs of a sequence.
#[derive(Debug)]
pub struct Keys<'a, 'k> {
    keys: Option<query::Keys<'k>>,
    doc: &'a Automerge,
}

impl<'a, 'k> Keys<'a, 'k> {
    pub(crate) fn new(doc: &'a Automerge, keys: Option<query::Keys<'k>>) -> Self {
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
