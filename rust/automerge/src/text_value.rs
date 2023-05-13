use core::fmt::Debug;

use crate::sequence_tree::SequenceTree;

#[cfg(not(target_family = "wasm"))]
#[derive(Clone, PartialEq)]
pub struct TextValue(SequenceTree<char>);

#[cfg(target_family = "wasm")]
#[derive(Clone, PartialEq)]
pub struct TextValue(SequenceTree<u16>);

#[cfg(not(target_family = "wasm"))]
impl TextValue {
    pub(crate) fn new(s: &str) -> Self {
        let mut v = SequenceTree::new();
        for ch in s.chars() {
            v.push(ch)
        }
        Self(v)
    }

    pub(crate) fn splice(&mut self, index: usize, value: &str) {
        for (n, ch) in value.chars().enumerate() {
            self.0.insert(index + n, ch)
        }
    }

    pub fn make_string(&self) -> String {
        self.0.iter().collect()
    }

    pub(crate) fn width(s: &str) -> usize {
        s.chars().count()
    }
}

#[cfg(target_family = "wasm")]
impl TextValue {
    pub(crate) fn new(s: &str) -> Self {
        let mut v = SequenceTree::new();
        for ch in s.encode_utf16() {
            v.push(ch)
        }
        Self(v)
    }

    pub(crate) fn splice(&mut self, index: usize, value: &str) {
        for (n, ch) in value.encode_utf16().enumerate() {
            self.0.insert(index + n, ch)
        }
    }

    pub fn make_string(&self) -> String {
        let bytes: Vec<_> = self.0.iter().cloned().collect();
        String::from_utf16_lossy(bytes.as_slice())
    }

    pub(crate) fn width(s: &str) -> usize {
        s.encode_utf16().count()
    }
}

impl TextValue {
    pub fn len(&self) -> usize {
        self.0.len()
    }

    pub fn remove(&mut self, index: usize) {
        self.0.remove(index);
    }
}

impl Debug for TextValue {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("TextValue")
            .field(&self.make_string())
            .finish()
    }
}

impl From<&str> for TextValue {
    fn from(s: &str) -> Self {
        TextValue::new(s)
    }
}

impl From<&TextValue> for String {
    fn from(s: &TextValue) -> Self {
        s.make_string()
    }
}
