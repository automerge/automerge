use crate::sequence_tree::SequenceTree;
use cfg_if::cfg_if;
use core::fmt::Debug;

cfg_if! {
    if #[cfg(feature = "utf8-indexing")] {
        #[derive(Clone, PartialEq, Default)]
        pub struct TextValue(SequenceTree<u8>);

        impl TextValue {
            pub(crate) fn new(s: &str) -> Self {
                let mut v = SequenceTree::new();
                for ch in s.bytes() {
                    v.push(ch)
                }
                Self(v)
            }

            pub(crate) fn splice(&mut self, index: usize, value: &str) {
                for (n, ch) in value.bytes().enumerate() {
                    self.0.insert(index + n, ch)
                }
            }

            pub(crate) fn splice_text_value(&mut self, index: usize, value: &TextValue) {
                for (n, ch) in value.chars().enumerate() {
                    self.0.insert(index + n, ch)
                }
            }

            pub fn make_string(&self) -> String {
                let bytes: Vec<_> = self.0.iter().cloned().collect();
                String::from_utf8_lossy(bytes.as_slice()).to_string()
            }

            pub(crate) fn width(s: &str) -> usize {
                s.len()
            }

            pub(crate) fn chars(&self) -> impl Iterator<Item = u8> + '_ {
                self.0.iter().cloned()
            }
        }
    } else if #[cfg(feature = "utf16-indexing")] {
        #[derive(Clone, PartialEq, Default)]
        pub struct TextValue(SequenceTree<u16>);

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

            pub(crate) fn splice_text_value(&mut self, index: usize, value: &TextValue) {
                for (n, ch) in value.chars().enumerate() {
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

            pub(crate) fn chars(&self) -> impl Iterator<Item = u16> + '_ {
                self.0.iter().cloned()
            }
        }
    } else {
        #[derive(Clone, PartialEq, Default)]
        pub struct TextValue(SequenceTree<char>);

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

            pub(crate) fn splice_text_value(&mut self, index: usize, value: &TextValue) {
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

            pub fn chars(&self) -> impl Iterator<Item = char> + '_ {
                self.0.iter().cloned()
            }
        }
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

impl From<String> for TextValue {
    fn from(s: String) -> Self {
        TextValue::new(&s)
    }
}

impl From<&TextValue> for String {
    fn from(s: &TextValue) -> Self {
        s.make_string()
    }
}
