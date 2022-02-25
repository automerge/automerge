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
        let key = self.keys.as_mut()?.next()?;
        Some(self.doc.to_string(key))
    }
}
