use crate::{types::Key, Automerge};

pub struct Keys<'a> {
    index: usize,
    keys: Vec<Key>,
    doc: &'a Automerge,
}

impl<'a> Keys<'a> {
    pub(crate) fn new(doc: &'a Automerge, keys: Vec<Key>) -> Self {
        Self {
            index: 0,
            keys,
            doc,
        }
    }
}

impl<'a> Iterator for Keys<'a> {
    type Item = String;

    fn next(&mut self) -> Option<Self::Item> {
        let n = self.keys.get(self.index)?;
        self.index += 1;
        Some(self.doc.to_string(*n))
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let num_left = self.keys.len() - self.index;
        (num_left, Some(num_left))
    }
}

impl<'a> ExactSizeIterator for Keys<'a> {}
