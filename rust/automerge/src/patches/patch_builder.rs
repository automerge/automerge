use core::fmt::Debug;
use std::collections::{BTreeMap, HashSet};
use std::sync::Arc;

use crate::exid::ExId;
use crate::iter::SpanInternal;
use crate::marks::MarkSet;
use crate::text_value::ConcreteTextValue;
use crate::types::{Clock, ObjId, ObjType};
use crate::{Automerge, Prop, TextEncoding, Value};

use super::{Event, Patch, PatchAction};
use crate::{marks::Mark, sequence_tree::SequenceTree};

#[derive(Debug, Clone)]
pub(crate) struct PatchBuilder<'a> {
    patches: Vec<Patch>,
    last_mark_set: Option<Arc<MarkSet>>, // keep this around for a quick pointer equality test
    path_map: BTreeMap<ObjId, (Prop, ObjId)>,
    seen: HashSet<ObjId>,
    text_encoding: TextEncoding,
    clock: Option<Clock>,
    doc: &'a Automerge,
}

impl<'a> PatchBuilder<'a> {
    pub(crate) fn new(
        doc: &'a Automerge,
        path_map: BTreeMap<ObjId, (Prop, ObjId)>,
        clock: Option<Clock>,
        text_encoding: TextEncoding,
    ) -> Self {
        // If we are expecting a lot of patches then precompute all the visible
        // paths up front to avoid doing many seek operations in the `Parents`
        // iterator in `Self::get_path`
        Self {
            patches: Vec::new(),
            last_mark_set: None,
            path_map,
            seen: HashSet::new(),
            doc,
            clock,
            text_encoding,
        }
    }
}

impl PatchBuilder<'_> {
    pub(crate) fn log_event(&mut self, doc: &Automerge, exid: ExId, event: &Event) {
        match event {
            Event::PutMap {
                key,
                value,
                id,
                conflict,
            } => {
                let opid = doc.id_to_exid(*id);
                self.put(exid, key.into(), (value.into(), opid), *conflict);
            }
            Event::DeleteMap { key } => {
                self.delete_map(exid, key);
            }
            Event::IncrementMap { key, n, id } => {
                let opid = doc.id_to_exid(*id);
                self.increment(exid, key.into(), (*n, opid));
            }
            Event::FlagConflictMap { key } => {
                self.flag_conflict(exid, key.into());
            }
            Event::PutSeq {
                index,
                value,
                id,
                conflict,
            } => {
                let opid = doc.id_to_exid(*id);
                self.put(exid, index.into(), (value.into(), opid), *conflict);
            }
            Event::Insert {
                index,
                value,
                id,
                conflict,
                //marks,
            } => {
                let opid = doc.id_to_exid(*id);
                self.insert(
                    exid,
                    *index,
                    (value.into(), opid),
                    *conflict,
                    //marks.clone(),
                );
            }
            Event::DeleteSeq { index, num } => {
                self.delete_seq(exid, *index, *num);
            }
            Event::IncrementSeq { index, n, id } => {
                let opid = doc.id_to_exid(*id);
                self.increment(exid, index.into(), (*n, opid));
            }
            Event::FlagConflictSeq { index } => {
                self.flag_conflict(exid, index.into());
            }
            Event::Splice { index, text, marks } => {
                self.splice_text(exid, *index, text, marks.clone());
            }
            Event::Mark { marks } => self.mark(exid, marks.clone().into_iter()),
        }
    }

    fn update_path_map(&mut self, parent_id: ObjId, parent_type: ObjType) {
        match parent_type {
            ObjType::List => {
                for item in self.doc.ops.list_range(&parent_id, .., self.clock.clone()) {
                    if item.value.is_object() {
                        let prop = Prop::from(item.index);
                        self.path_map.insert(ObjId(item.op_id()), (prop, parent_id));
                    }
                }
            }
            ObjType::Text => {
                for span in self.doc.ops.spans(&parent_id, self.clock.clone()) {
                    if let SpanInternal::Obj(id, index, _) = span {
                        let prop = Prop::from(index);
                        self.path_map.insert(ObjId(id), (prop, parent_id));
                    }
                }
            }
            _ => {
                for item in self.doc.ops.map_range(&parent_id, .., self.clock.clone()) {
                    if item.value.is_object() {
                        self.path_map.insert(
                            ObjId(item.op_id()),
                            (Prop::from(item.key.to_string()), parent_id),
                        );
                    }
                }
            }
        }
    }

    #[inline(never)]
    pub(crate) fn get_path(&mut self, obj: &ExId) -> Option<Vec<(ExId, Prop)>> {
        let mut path = vec![];
        let mut obj = obj.to_internal_obj();
        while !obj.is_root() {
            let (p, o) = if let Some(r) = self.path_map.get(&obj).cloned() {
                r
            } else {
                let parent_id = self.doc.ops().object_parent(&obj)?;
                let parent_type = self.doc.ops().object_type(&parent_id)?;
                if self.seen.contains(&obj) {
                    return None;
                }
                self.seen.insert(obj);
                self.update_path_map(parent_id, parent_type);
                self.path_map.get(&obj).cloned()?
            };
            obj = o;
            let parent_obj = self.doc.ops().id_to_exid(obj.0);
            path.push((parent_obj, p));
        }
        path.reverse();
        Some(path)
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
                value: ConcreteTextValue::new(value, self.text_encoding),
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
