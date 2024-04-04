use core::fmt::Debug;
use std::collections::HashMap;
use std::sync::Arc;

use crate::marks::MarkSet;
use crate::read::ReadDocInternal;
use crate::{ObjId, Prop, ReadDoc, Value};

use super::{Patch, PatchAction};
use crate::{marks::Mark, sequence_tree::SequenceTree};

#[derive(Debug, Clone)]
pub(crate) struct PatchBuilder<'a, R> {
    patches: Vec<Patch>,
    last_mark_set: Option<Arc<MarkSet>>, // keep this around for a quick pointer equality test
    visible_paths: Option<HashMap<ObjId, Vec<(ObjId, Prop)>>>,
    doc: &'a R,
}

impl<'a, R: ReadDocInternal> PatchBuilder<'a, R> {
    pub(crate) fn new(doc: &'a R, patches_size_hint: Option<usize>) -> Self {
        // If we are expecting a lot of patches then precompute all the visible
        // paths up front to avoid doing many seek operations in the `Parents`
        // iterator in `Self::get_path`
        let path_lookup = if patches_size_hint.map(|n| n > 100).unwrap_or(false) {
            Some(doc.live_obj_paths())
        } else {
            None
        };
        Self {
            patches: Vec::new(),
            last_mark_set: None,
            visible_paths: path_lookup,
            doc,
        }
    }
}

impl<'a, R: ReadDoc> PatchBuilder<'a, R> {
    pub(crate) fn get_path(&mut self, obj: &ObjId) -> Option<Vec<(ObjId, Prop)>> {
        if let Some(visible_paths) = &self.visible_paths {
            visible_paths.get(obj).cloned()
        } else {
            match self.doc.parents(obj) {
                Ok(parents) => parents.visible_path(),
                Err(e) => {
                    log!("error generating patch : {:?}", e);
                    None
                }
            }
        }
    }

    pub(crate) fn take_patches(&mut self) -> Vec<Patch> {
        std::mem::take(&mut self.patches)
    }

    pub(crate) fn insert(
        &mut self,
        obj: ObjId,
        index: usize,
        tagged_value: (Value<'_>, ObjId),
        conflict: bool,
    ) {
        let value = (tagged_value.0.to_owned(), tagged_value.1, conflict);
        if let Some(PatchAction::Insert {
            index: tail_index,
            values,
            ..
        }) = maybe_append(&mut self.patches, &obj)
        {
            let range = *tail_index..=*tail_index + values.len();
            if range.contains(&index) {
                values.insert(index - *tail_index, value);
                return;
            }
        }
        if let Some(path) = self.get_path(&obj) {
            let mut values = SequenceTree::new();
            values.push(value);
            let action = PatchAction::Insert { index, values };
            self.push(Patch { obj, path, action });
        }
    }

    fn push(&mut self, patch: Patch) {
        self.patches.push(patch);
        self.last_mark_set = None;
    }

    pub(crate) fn splice_text(
        &mut self,
        obj: ObjId,
        index: usize,
        value: &str,
        marks: Option<Arc<MarkSet>>,
    ) {
        if let Some(PatchAction::SpliceText {
            index: tail_index,
            value: prev_value,
            ..
        }) = maybe_append(&mut self.patches, &obj)
        {
            let range = *tail_index..=*tail_index + prev_value.len();
            if marks == self.last_mark_set && range.contains(&index) {
                let i = index - *tail_index;
                prev_value.splice(i, value);
                return;
            }
        }
        if let Some(path) = self.get_path(&obj) {
            let action = PatchAction::SpliceText {
                index,
                value: value.into(),
                marks: marks.as_deref().cloned(),
            };
            self.push(Patch { obj, path, action });
            self.last_mark_set = marks;
        }
    }

    pub(crate) fn delete_seq(&mut self, obj: ObjId, index: usize, length: usize) {
        match maybe_append(&mut self.patches, &obj) {
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
                    if value.len() == 0 {
                        self.patches.pop();
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
                    if values.len() == 0 {
                        self.patches.pop();
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
        if let Some(path) = self.get_path(&obj) {
            let action = PatchAction::DeleteSeq { index, length };
            self.push(Patch { obj, path, action })
        }
    }

    pub(crate) fn delete_map(&mut self, obj: ObjId, key: &str) {
        if let Some(path) = self.get_path(&obj) {
            let action = PatchAction::DeleteMap {
                key: key.to_owned(),
            };
            self.push(Patch { obj, path, action })
        }
    }

    pub(crate) fn put(
        &mut self,
        obj: ObjId,
        prop: Prop,
        tagged_value: (Value<'_>, ObjId),
        conflict: bool,
    ) {
        if let Some(path) = self.get_path(&obj) {
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
            self.push(Patch { obj, path, action })
        }
    }

    pub(crate) fn increment(&mut self, obj: ObjId, prop: Prop, tagged_value: (i64, ObjId)) {
        if let Some(path) = self.get_path(&obj) {
            let value = tagged_value.0;
            let action = PatchAction::Increment { prop, value };
            self.push(Patch { obj, path, action })
        }
    }

    pub(crate) fn mark<'b, 'c, M: Iterator<Item = Mark<'c>>>(&mut self, obj: ObjId, mark: M) {
        if let Some(PatchAction::Mark { marks, .. }) = maybe_append(&mut self.patches, &obj) {
            for m in mark {
                marks.push(m.into_owned())
            }
            return;
        }
        if let Some(path) = self.get_path(&obj) {
            let marks: Vec<_> = mark.map(|m| m.into_owned()).collect();
            if !marks.is_empty() {
                let action = PatchAction::Mark { marks };
                self.push(Patch { obj, path, action });
            }
        }
    }

    pub(crate) fn flag_conflict(&mut self, obj: ObjId, prop: Prop) {
        let conflict = match maybe_append(&mut self.patches, &obj) {
            Some(PatchAction::PutMap { key, conflict, .. })
                if Some(key.as_str()) == prop.as_str() =>
            {
                Some(conflict)
            }
            Some(PatchAction::PutSeq {
                index, conflict, ..
            }) if Some(*index) == prop.as_index() => Some(conflict),
            _ => None,
        };
        if let Some(conflict) = conflict {
            *conflict = true
        } else if let Some(path) = self.get_path(&obj) {
            let action = PatchAction::Conflict { prop };
            self.push(Patch { obj, path, action });
        }
    }
}

impl<'a, R> AsMut<PatchBuilder<'a, R>> for PatchBuilder<'a, R> {
    fn as_mut(&mut self) -> &mut Self {
        self
    }
}

fn maybe_append<'a>(patches: &'a mut [Patch], obj: &ObjId) -> Option<&'a mut PatchAction> {
    match patches.last_mut() {
        Some(Patch {
            obj: tail_obj,
            action,
            ..
        }) if obj == tail_obj => Some(action),
        _ => None,
    }
}
