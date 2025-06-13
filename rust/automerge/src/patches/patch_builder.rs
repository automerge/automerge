use core::fmt::Debug;
use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use crate::exid::ExId;
use crate::marks::MarkSet;
use crate::text_value::ConcreteTextValue;
use crate::types::Clock;
use crate::types::ObjId;
use crate::{Automerge, Prop, Value};

use super::{Patch, PatchAction, TextRepresentation};
use crate::{marks::Mark, sequence_tree::SequenceTree};

#[derive(Debug, Clone)]
pub(crate) struct PatchBuilder<'a> {
    patches: Vec<Patch>,
    last_mark_set: Option<Arc<MarkSet>>, // keep this around for a quick pointer equality test
    path_map: BTreeMap<ObjId, (Prop, ObjId)>,
    visible_paths: Option<HashMap<ExId, Vec<(ExId, Prop)>>>,
    text_rep: TextRepresentation,
    clock: Option<Clock>,
    doc: &'a Automerge,
}

impl<'a> PatchBuilder<'a> {
    pub(crate) fn new(
        doc: &'a Automerge,
        path_map: BTreeMap<ObjId, (Prop, ObjId)>,
        clock: Option<Clock>,
        patches_size_hint: Option<usize>,
        text_rep: TextRepresentation,
    ) -> Self {
        // If we are expecting a lot of patches then precompute all the visible
        // paths up front to avoid doing many seek operations in the `Parents`
        // iterator in `Self::get_path`
        let path_lookup =
            if path_map.is_empty() && patches_size_hint.map(|n| n > 100).unwrap_or(false) {
                Some(doc.visible_obj_paths(clock.clone()))
            } else {
                None
            };
        Self {
            patches: Vec::new(),
            last_mark_set: None,
            visible_paths: path_lookup,
            path_map,
            doc,
            clock,
            text_rep,
        }
    }
}

impl PatchBuilder<'_> {
    #[inline(never)]
    fn build_path(&mut self, obj: &ExId) -> Option<Vec<(ExId, Prop)>> {
        let mut path = vec![];
        let mut obj = obj.to_internal_obj();
        while !obj.is_root() {
            let (o, p) = if let Some((prop, parent)) = self.path_map.get(&obj) {
                (*parent, prop.clone())
            } else {
                let parent =
                    self.doc
                        .ops()
                        .parent_object(&obj, self.text_rep, self.clock.as_ref())?;
                if !parent.visible {
                    return None;
                }
                self.path_map.insert(obj, (parent.prop.clone(), parent.obj));
                (parent.obj, parent.prop)
            };
            obj = o;
            let parent_obj = self.doc.ops().id_to_exid(obj.0);
            path.push((parent_obj, p));
        }
        path.reverse();
        Some(path)
    }

    #[inline(never)]
    pub(crate) fn get_path(&mut self, obj: &ExId) -> Option<Vec<(ExId, Prop)>> {
        if let Some(visible_paths) = &self.visible_paths {
            visible_paths.get(obj).cloned()
        } else {
            self.build_path(obj)
        }
    }

    pub(crate) fn take_patches(&mut self) -> Vec<Patch> {
        std::mem::take(&mut self.patches)
    }

    pub(crate) fn insert(
        &mut self,
        obj: ExId,
        index: usize,
        tagged_value: (Value<'_>, ExId),
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
        obj: ExId,
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
                value: ConcreteTextValue::new(value, self.text_rep),
                marks: marks.as_deref().cloned(),
            };
            self.push(Patch { obj, path, action });
            self.last_mark_set = marks;
        }
    }

    pub(crate) fn delete_seq(&mut self, obj: ExId, index: usize, length: usize) {
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

    pub(crate) fn delete_map(&mut self, obj: ExId, key: &str) {
        if let Some(path) = self.get_path(&obj) {
            let action = PatchAction::DeleteMap {
                key: key.to_owned(),
            };
            self.push(Patch { obj, path, action })
        }
    }

    pub(crate) fn put(
        &mut self,
        obj: ExId,
        prop: Prop,
        tagged_value: (Value<'_>, ExId),
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

    pub(crate) fn increment(&mut self, obj: ExId, prop: Prop, tagged_value: (i64, ExId)) {
        if let Some(path) = self.get_path(&obj) {
            let value = tagged_value.0;
            let action = PatchAction::Increment { prop, value };
            self.push(Patch { obj, path, action })
        }
    }

    pub(crate) fn mark<M: Iterator<Item = Mark>>(&mut self, obj: ExId, mark: M) {
        if let Some(PatchAction::Mark { marks, .. }) = maybe_append(&mut self.patches, &obj) {
            for m in mark {
                marks.push(m)
            }
            return;
        }
        if let Some(path) = self.get_path(&obj) {
            let marks: Vec<_> = mark./*map(|m| m.into_owned()).*/collect();
            if !marks.is_empty() {
                let action = PatchAction::Mark { marks };
                self.push(Patch { obj, path, action });
            }
        }
    }

    pub(crate) fn flag_conflict(&mut self, obj: ExId, prop: Prop) {
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

impl<'a> AsMut<PatchBuilder<'a>> for PatchBuilder<'a> {
    fn as_mut(&mut self) -> &mut Self {
        self
    }
}

fn maybe_append<'a>(patches: &'a mut [Patch], obj: &ExId) -> Option<&'a mut PatchAction> {
    match patches.last_mut() {
        Some(Patch {
            obj: tail_obj,
            action,
            ..
        }) if obj == tail_obj => Some(action),
        _ => None,
    }
}
