use crate::{query::IterKeys, Automerge};

pub struct Keys<'a, 'k, const B: usize> {
    keys: Option<IterKeys<'k, B>>,
    doc: &'a Automerge,
}

impl<'a, 'k, const B: usize> Keys<'a, 'k, B> {
    pub(crate) fn new(doc: &'a Automerge, keys: Option<IterKeys<'k, B>>) -> Self {
        Self { keys, doc }
    }
}

impl<'a, 'k, const B: usize> Iterator for Keys<'a, 'k, B> {
    type Item = String;

    fn next(&mut self) -> Option<Self::Item> {
        let key = self.keys.as_mut()?.next()?;
        Some(self.doc.to_string(key))
    }
}
