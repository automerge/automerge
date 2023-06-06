use core::fmt::Debug;

use crate::{ObjId, Prop, ReadDoc, Value};

use super::{Patch, PatchAction};
use crate::{marks::Mark, sequence_tree::SequenceTree};

#[derive(Debug, Clone, Default)]
pub(crate) struct PatchBuilder {
    pub(crate) patches: Vec<Patch>,
}

impl PatchBuilder {
    pub(crate) fn get_path<R: ReadDoc>(
        &mut self,
        doc: &R,
        obj: &ObjId,
    ) -> Option<Vec<(ObjId, Prop)>> {
        match doc.parents(obj) {
            Ok(parents) => parents.visible_path(),
            Err(e) => {
                log!("error generating patch : {:?}", e);
                None
            }
        }
    }

    fn maybe_append(&mut self, obj: &ObjId) -> Option<&mut PatchAction> {
        match self.patches.last_mut() {
            Some(Patch {
                obj: tail_obj,
                action,
                ..
            }) if obj == tail_obj => Some(action),
            _ => None,
        }
    }

    pub(crate) fn take_patches(&mut self) -> Vec<Patch> {
        std::mem::take(&mut self.patches)
    }

    pub(crate) fn insert<R: ReadDoc>(
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

    pub(crate) fn splice_text<R: ReadDoc>(
        &mut self,
        doc: &R,
        obj: ObjId,
        index: usize,
        value: &str,
    ) {
        if let Some(PatchAction::SpliceText {
            index: tail_index,
            value: prev_value,
            ..
        }) = self.maybe_append(&obj)
        {
            let range = *tail_index..=*tail_index + prev_value.len();
            if range.contains(&index) {
                let i = index - *tail_index;
                prev_value.splice(i, value);
                return;
            }
        }
        if let Some(path) = self.get_path(doc, &obj) {
            let action = PatchAction::SpliceText {
                index,
                value: value.into(),
            };
            self.patches.push(Patch { obj, path, action });
        }
    }

    pub(crate) fn delete_seq<R: ReadDoc>(
        &mut self,
        doc: &R,
        obj: ObjId,
        index: usize,
        length: usize,
    ) {
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

    pub(crate) fn delete_map<R: ReadDoc>(&mut self, doc: &R, obj: ObjId, key: &str) {
        if let Some(path) = self.get_path(doc, &obj) {
            let action = PatchAction::DeleteMap {
                key: key.to_owned(),
            };
            self.patches.push(Patch { obj, path, action })
        }
    }

    pub(crate) fn put<R: ReadDoc>(
        &mut self,
        doc: &R,
        obj: ObjId,
        prop: Prop,
        tagged_value: (Value<'_>, ObjId),
        conflict: bool,
    ) {
        if let Some(path) = self.get_path(doc, &obj) {
            let value = (tagged_value.0.to_owned(), tagged_value.1);
            let action = match prop {
                Prop::Map(key) => PatchAction::PutMap {
                    key,
                    value,
                    conflict,
                },
                Prop::Seq(index) => PatchAction::PutSeq {
                    index,
                    value,
                    conflict,
                },
            };
            self.patches.push(Patch { obj, path, action })
        }
    }

    pub(crate) fn increment<R: ReadDoc>(
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

    pub(crate) fn mark<'a, R: ReadDoc, M: Iterator<Item = Mark<'a>>>(
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

    // FIXME
    pub(crate) fn flag_conflict<R: ReadDoc>(&mut self, _doc: &R, _objid: ObjId, _prop: Prop) {}
}

impl AsMut<PatchBuilder> for PatchBuilder {
    fn as_mut(&mut self) -> &mut Self {
        self
    }
}
