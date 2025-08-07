use crate::{sequence_tree::SequenceTree, TextEncoding};
use cfg_if::cfg_if;

impl TextEncoding {
    /// Return the default text encoding for the platform you are building for.
    ///
    /// This is set to
    ///
    /// * UTF-8 code units if the `utf8-indexing` feature is enabled.
    /// * UTF-16 code units if the `utf16-indexing` feature is enabled (as it is in the automerge-wasm build)
    /// * Unicode code points if neither feature is enabled.
    pub fn platform_default() -> Self {
        cfg_if! {
            if #[cfg(feature = "utf8-indexing")] {
                    Self::Utf8CodeUnit
            } else if #[cfg(feature = "utf16-indexing")] {
                    Self::Utf16CodeUnit
            } else {
                    Self::UnicodeCodePoint
            }
        }
    }
}

pub(crate) trait TextValue {
    type Elem;
    fn new(s: &str) -> Self;
    fn splice(&mut self, index: usize, value: &str);
    fn splice_text_value(&mut self, index: usize, value: &SequenceTree<Self::Elem>);
    fn make_string(&self) -> String;
    fn len(&self) -> usize;
    fn remove(&mut self, index: usize);
}

#[derive(Debug, Clone, PartialEq)]
pub struct Utf8(SequenceTree<u8>);

impl TextValue for Utf8 {
    type Elem = u8;
    fn new(s: &str) -> Self {
        let mut v = SequenceTree::new();
        for ch in s.bytes() {
            v.push(ch)
        }
        Self(v)
    }

    fn splice(&mut self, index: usize, value: &str) {
        for (n, ch) in value.bytes().enumerate() {
            self.0.insert(index + n, ch)
        }
    }

    fn splice_text_value(&mut self, index: usize, value: &SequenceTree<Self::Elem>) {
        for (n, ch) in value.iter().cloned().enumerate() {
            self.0.insert(index + n, ch)
        }
    }

    fn make_string(&self) -> String {
        let bytes: Vec<_> = self.0.iter().cloned().collect();
        String::from_utf8_lossy(bytes.as_slice()).to_string()
    }

    fn len(&self) -> usize {
        self.0.len()
    }

    fn remove(&mut self, index: usize) {
        self.0.remove(index);
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct CodePoint(SequenceTree<char>);
impl TextValue for CodePoint {
    type Elem = char;

    fn new(s: &str) -> Self {
        let mut v = SequenceTree::new();
        for ch in s.chars() {
            v.push(ch)
        }
        Self(v)
    }

    fn splice(&mut self, index: usize, value: &str) {
        for (n, ch) in value.chars().enumerate() {
            self.0.insert(index + n, ch)
        }
    }

    fn splice_text_value(&mut self, index: usize, value: &SequenceTree<char>) {
        for (n, ch) in value.iter().copied().enumerate() {
            self.0.insert(index + n, ch)
        }
    }

    fn make_string(&self) -> String {
        self.0.iter().collect()
    }

    fn len(&self) -> usize {
        self.0.len()
    }

    fn remove(&mut self, index: usize) {
        self.0.remove(index);
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct Utf16(SequenceTree<u16>);
impl TextValue for Utf16 {
    type Elem = u16;

    fn new(s: &str) -> Self {
        let mut v = SequenceTree::new();
        for ch in s.encode_utf16() {
            v.push(ch)
        }
        Self(v)
    }

    fn splice(&mut self, index: usize, value: &str) {
        for (n, ch) in value.encode_utf16().enumerate() {
            self.0.insert(index + n, ch)
        }
    }

    fn splice_text_value(&mut self, index: usize, value: &SequenceTree<Self::Elem>) {
        for (n, ch) in value.iter().copied().enumerate() {
            self.0.insert(index + n, ch)
        }
    }

    fn make_string(&self) -> String {
        let bytes: Vec<_> = self.0.iter().cloned().collect();
        String::from_utf16_lossy(bytes.as_slice())
    }

    fn len(&self) -> usize {
        self.0.len()
    }

    fn remove(&mut self, index: usize) {
        self.0.remove(index);
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct Grapheme(SequenceTree<String>);

impl TextValue for Grapheme {
    type Elem = String;

    fn new(s: &str) -> Self {
        let mut v = SequenceTree::new();
        for ch in unicode_segmentation::UnicodeSegmentation::graphemes(s, true) {
            v.push(ch.to_string())
        }
        Self(v)
    }

    fn splice(&mut self, index: usize, value: &str) {
        for (n, ch) in unicode_segmentation::UnicodeSegmentation::graphemes(value, true).enumerate()
        {
            self.0.insert(index + n, ch.to_string())
        }
    }

    fn splice_text_value(&mut self, index: usize, value: &SequenceTree<Self::Elem>) {
        for (n, ch) in value.iter().cloned().enumerate() {
            self.0.insert(index + n, ch)
        }
    }

    fn make_string(&self) -> String {
        self.0.iter().fold(String::new(), |acc, s| acc + s)
    }

    fn len(&self) -> usize {
        self.0.len()
    }

    fn remove(&mut self, index: usize) {
        self.0.remove(index);
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum ConcreteTextValue {
    Utf8CodeUnit(Utf8),
    Utf16CodeUnit(Utf16),
    CodePoint(CodePoint),
    Grapheme(Grapheme),
}

impl ConcreteTextValue {
    pub fn new(s: &str, text_encoding: TextEncoding) -> Self {
        match text_encoding {
            TextEncoding::Utf8CodeUnit => Self::Utf8CodeUnit(Utf8::new(s)),
            TextEncoding::Utf16CodeUnit => Self::Utf16CodeUnit(Utf16::new(s)),
            TextEncoding::UnicodeCodePoint => Self::CodePoint(CodePoint::new(s)),
            TextEncoding::GraphemeCluster => Self::Grapheme(Grapheme::new(s)),
        }
    }

    pub fn make_string(&self) -> String {
        match self {
            Self::Utf8CodeUnit(u) => u.make_string(),
            Self::Utf16CodeUnit(u) => u.make_string(),
            Self::CodePoint(u) => u.make_string(),
            Self::Grapheme(u) => u.make_string(),
        }
    }

    pub(crate) fn splice_text_value(
        &mut self,
        index: usize,
        other: &ConcreteTextValue,
    ) -> Result<(), error::MismatchedEncoding> {
        match (self, other) {
            (ConcreteTextValue::Utf16CodeUnit(this), ConcreteTextValue::Utf16CodeUnit(other)) => {
                this.splice_text_value(index, &other.0);
                Ok(())
            }
            (ConcreteTextValue::Utf8CodeUnit(this), ConcreteTextValue::Utf8CodeUnit(other)) => {
                this.splice_text_value(index, &other.0);
                Ok(())
            }
            (ConcreteTextValue::CodePoint(this), ConcreteTextValue::CodePoint(other)) => {
                this.splice_text_value(index, &other.0);
                Ok(())
            }
            (ConcreteTextValue::Grapheme(this), ConcreteTextValue::Grapheme(other)) => {
                this.splice_text_value(index, &other.0);
                Ok(())
            }
            _ => Err(error::MismatchedEncoding),
        }
    }

    pub(crate) fn splice(&mut self, index: usize, value: &str) {
        match self {
            Self::Utf8CodeUnit(u) => u.splice(index, value),
            Self::Utf16CodeUnit(u) => u.splice(index, value),
            Self::CodePoint(u) => u.splice(index, value),
            Self::Grapheme(u) => u.splice(index, value),
        }
    }

    pub(crate) fn len(&self) -> usize {
        match self {
            Self::Utf8CodeUnit(u) => u.len(),
            Self::Utf16CodeUnit(u) => u.len(),
            Self::CodePoint(u) => u.len(),
            Self::Grapheme(u) => u.len(),
        }
    }

    pub(crate) fn remove(&mut self, index: usize) {
        match self {
            Self::Utf8CodeUnit(u) => u.remove(index),
            Self::Utf16CodeUnit(u) => u.remove(index),
            Self::CodePoint(u) => u.remove(index),
            Self::Grapheme(u) => u.remove(index),
        }
    }
}

pub(crate) mod error {
    #[derive(Debug, thiserror::Error)]
    #[error("mismatched encoding")]
    pub(crate) struct MismatchedEncoding;
}
