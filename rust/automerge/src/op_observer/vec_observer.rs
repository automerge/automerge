#![allow(dead_code)]

use core::fmt::Debug;

use crate::{marks::Mark, ObjId, OpObserver, Prop, ReadDoc, ScalarValue, Value};

use crate::sequence_tree::SequenceTree;

use crate::op_observer::BranchableObserver;
use crate::op_observer::{Patch, PatchAction};

#[derive(Debug, Copy, Clone, PartialEq)]
pub enum TextRepresentation {
    Array,
    String,
}

impl TextRepresentation {
    pub fn is_array(&self) -> bool {
        matches!(self, TextRepresentation::Array)
    }

    pub fn is_string(&self) -> bool {
        matches!(self, TextRepresentation::String)
    }
}

impl std::default::Default for TextRepresentation {
    fn default() -> Self {
        TextRepresentation::Array
    }
}

pub(crate) trait TextIndex {
    type Item: Debug + PartialEq + Clone;
    type Iter<'a>: Iterator<Item = Self::Item>;

    fn chars(text: &str) -> Self::Iter<'_>;
}

#[derive(Debug, Clone, Default)]
struct VecOpObserverInner<T: TextIndex> {
    pub(crate) patches: Vec<Patch<T::Item>>,
    pub(crate) text_rep: TextRepresentation,
}

#[derive(Debug, Clone, Default)]
pub struct VecOpObserver(VecOpObserverInner<Utf8TextIndex>);

#[derive(Debug, Clone, Default)]
pub struct VecOpObserver16(VecOpObserverInner<Utf16TextIndex>);

#[derive(Debug, Clone, Default)]
pub(crate) struct Utf16TextIndex;

#[derive(Debug, Clone, Default)]
pub(crate) struct Utf8TextIndex;

impl TextIndex for Utf8TextIndex {
    type Item = char;
    type Iter<'a> = std::str::Chars<'a>;

    fn chars(text: &str) -> Self::Iter<'_> {
        text.chars()
    }
}

impl TextIndex for Utf16TextIndex {
    type Item = u16;
    type Iter<'a> = std::str::EncodeUtf16<'a>;

    fn chars(text: &str) -> Self::Iter<'_> {
        text.encode_utf16()
    }
}

pub trait HasPatches {
    type Patches;

    fn take_patches(&mut self) -> Self::Patches;
    fn with_text_rep(self, text_rep: TextRepresentation) -> Self;
    fn set_text_rep(&mut self, text_rep: TextRepresentation);
    fn get_text_rep(&self) -> TextRepresentation;
}

impl HasPatches for VecOpObserver {
    type Patches = Vec<Patch<char>>;

    fn take_patches(&mut self) -> Self::Patches {
        std::mem::take(&mut self.0.patches)
    }

    fn with_text_rep(mut self, text_rep: TextRepresentation) -> Self {
        self.0.text_rep = text_rep;
        self
    }

    fn set_text_rep(&mut self, text_rep: TextRepresentation) {
        self.0.text_rep = text_rep;
    }

    fn get_text_rep(&self) -> TextRepresentation {
        self.0.text_rep
    }
}

impl HasPatches for VecOpObserver16 {
    type Patches = Vec<Patch<u16>>;

    fn take_patches(&mut self) -> Self::Patches {
        std::mem::take(&mut self.0.patches)
    }

    fn with_text_rep(mut self, text_rep: TextRepresentation) -> Self {
        self.0.text_rep = text_rep;
        self
    }

    fn set_text_rep(&mut self, text_rep: TextRepresentation) {
        self.0.text_rep = text_rep;
    }

    fn get_text_rep(&self) -> TextRepresentation {
        self.0.text_rep
    }
}

impl<T: TextIndex> VecOpObserverInner<T> {
    fn get_path<R: ReadDoc>(&mut self, doc: &R, obj: &ObjId) -> Option<Vec<(ObjId, Prop)>> {
        match doc.parents(obj) {
            Ok(parents) => parents.visible_path(),
            Err(e) => {
                log!("error generating patch : {:?}", e);
                None
            }
        }
    }

    fn maybe_append(&mut self, obj: &ObjId) -> Option<&mut PatchAction<T::Item>> {
        match self.patches.last_mut() {
            Some(Patch {
                obj: tail_obj,
                action,
                ..
            }) if obj == tail_obj => Some(action),
            _ => None,
        }
    }
}

impl<T: TextIndex> OpObserver for VecOpObserverInner<T> {
    fn insert<R: ReadDoc>(
        &mut self,
        doc: &R,
        obj: ObjId,
        index: usize,
        tagged_value: (Value<'_>, ObjId),
        conflict: bool,
    ) {
        let value = (tagged_value.0.to_owned(), tagged_value.1);
        if let Some(PatchAction::Insert {
            index: tail_index,
            values,
            ..
        }) = self.maybe_append(&obj)
        {
            let range = *tail_index..=*tail_index + values.len();
            if range.contains(&index) {
                values.insert(index - *tail_index, value);
                return;
            }
        }
        if let Some(path) = self.get_path(doc, &obj) {
            let mut values = SequenceTree::new();
            values.push(value);
            let action = PatchAction::Insert {
                index,
                values,
                conflict,
            };
            self.patches.push(Patch { obj, path, action });
        }
    }

    fn splice_text<R: ReadDoc>(&mut self, doc: &R, obj: ObjId, index: usize, value: &str) {
        if self.text_rep == TextRepresentation::Array {
            for (offset, c) in value.chars().map(ScalarValue::from).enumerate() {
                log!(
                    "-- internal insert index={} value={:?}",
                    index + offset,
                    value
                );
                let value = (c.into(), ObjId::Root);
                self.insert(doc, obj.clone(), index + offset, value, false);
            }
            return;
        }
        if let Some(PatchAction::SpliceText {
            index: tail_index,
            value: prev_value,
            ..
        }) = self.maybe_append(&obj)
        {
            let range = *tail_index..=*tail_index + prev_value.len();
            if range.contains(&index) {
                let i = index - *tail_index;
                for (n, ch) in T::chars(value).enumerate() {
                    prev_value.insert(i + n, ch)
                }
                return;
            }
        }
        if let Some(path) = self.get_path(doc, &obj) {
            let mut v = SequenceTree::new();
            for ch in T::chars(value) {
                v.push(ch)
            }
            let action = PatchAction::SpliceText { index, value: v };
            self.patches.push(Patch { obj, path, action });
        }
    }

    fn delete_seq<R: ReadDoc>(&mut self, doc: &R, obj: ObjId, index: usize, length: usize) {
        match self.maybe_append(&obj) {
            Some(PatchAction::SpliceText {
                index: tail_index,
                value,
                ..
            }) => {
                let range = *tail_index..*tail_index + value.len();
                if range.contains(&index) && range.contains(&(index + length - 1)) {
                    for _ in 0..length {
                        value.remove(index - *tail_index);
                    }
                    return;
                }
            }
            Some(PatchAction::Insert {
                index: tail_index,
                values,
                ..
            }) => {
                let range = *tail_index..*tail_index + values.len();
                if range.contains(&index) && range.contains(&(index + length - 1)) {
                    for _ in 0..length {
                        values.remove(index - *tail_index);
                    }
                    return;
                }
            }
            Some(PatchAction::DeleteSeq {
                index: tail_index,
                length: tail_length,
                ..
            }) => {
                if index == *tail_index {
                    *tail_length += length;
                    return;
                }
            }
            _ => {}
        }
        if let Some(path) = self.get_path(doc, &obj) {
            let action = PatchAction::DeleteSeq { index, length };
            self.patches.push(Patch { obj, path, action })
        }
    }

    fn delete_map<R: ReadDoc>(&mut self, doc: &R, obj: ObjId, key: &str) {
        if let Some(path) = self.get_path(doc, &obj) {
            let action = PatchAction::DeleteMap {
                key: key.to_owned(),
            };
            self.patches.push(Patch { obj, path, action })
        }
    }

    fn put<R: ReadDoc>(
        &mut self,
        doc: &R,
        obj: ObjId,
        prop: Prop,
        tagged_value: (Value<'_>, ObjId),
        conflict: bool,
    ) {
        let expose = false;
        if let Some(path) = self.get_path(doc, &obj) {
            let value = (tagged_value.0.to_owned(), tagged_value.1);
            let action = match prop {
                Prop::Map(key) => PatchAction::PutMap {
                    key,
                    value,
                    expose,
                    conflict,
                },
                Prop::Seq(index) => PatchAction::PutSeq {
                    index,
                    value,
                    expose,
                    conflict,
                },
            };
            self.patches.push(Patch { obj, path, action })
        }
    }

    fn expose<R: ReadDoc>(
        &mut self,
        doc: &R,
        obj: ObjId,
        prop: Prop,
        tagged_value: (Value<'_>, ObjId),
        conflict: bool,
    ) {
        let expose = true;
        if let Some(path) = self.get_path(doc, &obj) {
            let value = (tagged_value.0.to_owned(), tagged_value.1);
            let action = match prop {
                Prop::Map(key) => PatchAction::PutMap {
                    key,
                    value,
                    expose,
                    conflict,
                },
                Prop::Seq(index) => PatchAction::PutSeq {
                    index,
                    value,
                    expose,
                    conflict,
                },
            };
            self.patches.push(Patch { obj, path, action })
        }
    }

    fn increment<R: ReadDoc>(
        &mut self,
        doc: &R,
        obj: ObjId,
        prop: Prop,
        tagged_value: (i64, ObjId),
    ) {
        if let Some(path) = self.get_path(doc, &obj) {
            let value = tagged_value.0;
            let action = PatchAction::Increment { prop, value };
            self.patches.push(Patch { obj, path, action })
        }
    }

    fn mark<'a, R: ReadDoc, M: Iterator<Item = Mark<'a>>>(
        &mut self,
        doc: &'a R,
        obj: ObjId,
        mark: M,
    ) {
        if let Some(PatchAction::Mark { marks, .. }) = self.maybe_append(&obj) {
            for m in mark {
                marks.push(m.into_owned())
            }
            return;
        }
        if let Some(path) = self.get_path(doc, &obj) {
            let marks: Vec<_> = mark.map(|m| m.into_owned()).collect();
            if !marks.is_empty() {
                let action = PatchAction::Mark { marks };
                self.patches.push(Patch { obj, path, action });
            }
        }
    }

    fn unmark<R: ReadDoc>(&mut self, doc: &R, obj: ObjId, key: &str, start: usize, end: usize) {
        if let Some(path) = self.get_path(doc, &obj) {
            let action = PatchAction::Unmark {
                key: key.to_string(),
                start,
                end,
            };
            self.patches.push(Patch { obj, path, action });
        }
    }

    fn text_as_seq(&self) -> bool {
        self.text_rep == TextRepresentation::Array
    }
}

impl<T: TextIndex> BranchableObserver for VecOpObserverInner<T> {
    fn merge(&mut self, other: &Self) {
        self.patches.extend_from_slice(other.patches.as_slice())
    }

    fn branch(&self) -> Self {
        VecOpObserverInner {
            patches: vec![],
            text_rep: self.text_rep,
        }
    }
}

impl OpObserver for VecOpObserver {
    fn insert<R: ReadDoc>(
        &mut self,
        doc: &R,
        obj: ObjId,
        index: usize,
        tagged_value: (Value<'_>, ObjId),
        conflict: bool,
    ) {
        self.0.insert(doc, obj, index, tagged_value, conflict)
    }

    fn splice_text<R: ReadDoc>(&mut self, doc: &R, obj: ObjId, index: usize, value: &str) {
        self.0.splice_text(doc, obj, index, value)
    }

    fn delete_seq<R: ReadDoc>(&mut self, doc: &R, obj: ObjId, index: usize, length: usize) {
        self.0.delete_seq(doc, obj, index, length)
    }

    fn delete_map<R: ReadDoc>(&mut self, doc: &R, obj: ObjId, key: &str) {
        self.0.delete_map(doc, obj, key)
    }

    fn put<R: ReadDoc>(
        &mut self,
        doc: &R,
        obj: ObjId,
        prop: Prop,
        tagged_value: (Value<'_>, ObjId),
        conflict: bool,
    ) {
        self.0.put(doc, obj, prop, tagged_value, conflict)
    }

    fn expose<R: ReadDoc>(
        &mut self,
        doc: &R,
        obj: ObjId,
        prop: Prop,
        tagged_value: (Value<'_>, ObjId),
        conflict: bool,
    ) {
        self.0.expose(doc, obj, prop, tagged_value, conflict)
    }

    fn increment<R: ReadDoc>(
        &mut self,
        doc: &R,
        obj: ObjId,
        prop: Prop,
        tagged_value: (i64, ObjId),
    ) {
        self.0.increment(doc, obj, prop, tagged_value)
    }

    fn mark<'a, R: ReadDoc, M: Iterator<Item = Mark<'a>>>(
        &mut self,
        doc: &'a R,
        obj: ObjId,
        mark: M,
    ) {
        self.0.mark(doc, obj, mark)
    }

    fn unmark<R: ReadDoc>(&mut self, doc: &R, obj: ObjId, key: &str, start: usize, end: usize) {
        self.0.unmark(doc, obj, key, start, end)
    }

    fn text_as_seq(&self) -> bool {
        self.0.text_as_seq()
    }
}

impl OpObserver for VecOpObserver16 {
    fn insert<R: ReadDoc>(
        &mut self,
        doc: &R,
        obj: ObjId,
        index: usize,
        tagged_value: (Value<'_>, ObjId),
        conflict: bool,
    ) {
        self.0.insert(doc, obj, index, tagged_value, conflict)
    }

    fn splice_text<R: ReadDoc>(&mut self, doc: &R, obj: ObjId, index: usize, value: &str) {
        self.0.splice_text(doc, obj, index, value)
    }

    fn delete_seq<R: ReadDoc>(&mut self, doc: &R, obj: ObjId, index: usize, length: usize) {
        self.0.delete_seq(doc, obj, index, length)
    }

    fn delete_map<R: ReadDoc>(&mut self, doc: &R, obj: ObjId, key: &str) {
        self.0.delete_map(doc, obj, key)
    }

    fn put<R: ReadDoc>(
        &mut self,
        doc: &R,
        obj: ObjId,
        prop: Prop,
        tagged_value: (Value<'_>, ObjId),
        conflict: bool,
    ) {
        self.0.put(doc, obj, prop, tagged_value, conflict)
    }

    fn expose<R: ReadDoc>(
        &mut self,
        doc: &R,
        obj: ObjId,
        prop: Prop,
        tagged_value: (Value<'_>, ObjId),
        conflict: bool,
    ) {
        self.0.expose(doc, obj, prop, tagged_value, conflict)
    }

    fn increment<R: ReadDoc>(
        &mut self,
        doc: &R,
        obj: ObjId,
        prop: Prop,
        tagged_value: (i64, ObjId),
    ) {
        self.0.increment(doc, obj, prop, tagged_value)
    }

    fn mark<'a, R: ReadDoc, M: Iterator<Item = Mark<'a>>>(
        &mut self,
        doc: &'a R,
        obj: ObjId,
        mark: M,
    ) {
        self.0.mark(doc, obj, mark)
    }

    fn unmark<R: ReadDoc>(&mut self, doc: &R, obj: ObjId, key: &str, start: usize, end: usize) {
        self.0.unmark(doc, obj, key, start, end)
    }

    fn text_as_seq(&self) -> bool {
        self.0.text_as_seq()
    }
}

impl BranchableObserver for VecOpObserver {
    fn merge(&mut self, other: &Self) {
        self.0.merge(&other.0)
    }

    fn branch(&self) -> Self {
        VecOpObserver(self.0.branch())
    }
}

impl BranchableObserver for VecOpObserver16 {
    fn merge(&mut self, other: &Self) {
        self.0.merge(&other.0)
    }

    fn branch(&self) -> Self {
        VecOpObserver16(self.0.branch())
    }
}
