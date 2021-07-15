use smol_str::SmolStr;

use crate::{state_tree::StateTreeText, Value};

#[derive(Clone, Debug)]
pub struct TextRef<'a> {
    stt: &'a StateTreeText,
}

impl<'a> TextRef<'a> {
    pub(crate) fn new(stt: &'a StateTreeText) -> Self {
        Self { stt }
    }

    pub fn len(&self) -> usize {
        self.stt.graphemes.len()
    }

    pub fn is_empty(&self) -> bool {
        self.stt.graphemes.is_empty()
    }

    pub fn get(&self, index: usize) -> Option<&SmolStr> {
        self.stt
            .graphemes
            .get(index)
            .map(|(_, mg)| mg.default_grapheme())
    }

    pub fn iter(&self) -> impl Iterator<Item = &SmolStr> {
        self.stt.graphemes.iter().map(|mg| mg.default_grapheme())
    }

    pub fn value(&self) -> Value {
        let mut v = Vec::new();
        for e in self.stt.graphemes.iter() {
            v.push(e.default_grapheme().clone())
        }
        Value::Text(v)
    }
}
