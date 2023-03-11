use crate::{query, Automerge};

/// An iterator over the keys of an object
///
/// This is returned by [`crate::ReadDoc::keys`] and method. The returned item is either
/// the keys of a map, or the encoded element IDs of a sequence.
#[derive(Debug)]
pub struct Keys<'a> {
    keys: KeyQuery<'a>,
    doc: &'a Automerge,
}

#[derive(Debug)]
enum KeyQuery<'a> {
    Keys(query::Keys<'a>),
    KeysAt(query::KeysAt<'a>),
    None,
}

impl<'a> Keys<'a> {
    pub(crate) fn with_keys(self, query: query::Keys<'a>) -> Self {
        Self {
            keys: KeyQuery::Keys(query),
            doc: self.doc,
        }
    }

    pub(crate) fn with_keys_at(self, query: query::KeysAt<'a>) -> Self {
        Self {
            keys: KeyQuery::KeysAt(query),
            doc: self.doc,
        }
    }

    pub(crate) fn new(doc: &'a Automerge) -> Self {
        let keys = KeyQuery::None;
        Self { keys, doc }
    }
}

impl<'a> Iterator for Keys<'a> {
    type Item = String;

    fn next(&mut self) -> Option<Self::Item> {
        match &mut self.keys {
            KeyQuery::Keys(keys) => keys.next().map(|key| self.doc.to_string(key)),
            KeyQuery::KeysAt(keys_at) => keys_at.next().map(|key| self.doc.to_string(key)),
            KeyQuery::None => None,
        }
    }
}

impl<'a> DoubleEndedIterator for Keys<'a> {
    fn next_back(&mut self) -> Option<Self::Item> {
        match &mut self.keys {
            KeyQuery::Keys(keys) => keys.next_back().map(|key| self.doc.to_string(key)),
            KeyQuery::KeysAt(keys_at) => keys_at.next_back().map(|key| self.doc.to_string(key)),
            KeyQuery::None => None,
        }
    }
}
