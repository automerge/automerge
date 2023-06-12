use core::fmt::Debug;
use std::sync::Arc;

use crate::marks::RichText;
use crate::{ObjId, Parents, Prop, Value};

use super::{Patch, PatchAction};
use crate::{marks::Mark, sequence_tree::SequenceTree};

#[derive(Debug, Clone)]
pub(crate) struct PatchBuilder {
    patches: Vec<Patch>,
    last_mark_set: Option<Arc<RichText>>, // keep this around for a quick pointer equality test
}

impl PatchBuilder {
    pub(crate) fn new() -> Self {
        Self {
            patches: Vec::new(),
            last_mark_set: None,
        }
    }

    pub(crate) fn take_patches(&mut self) -> Vec<Patch> {
        std::mem::take(&mut self.patches)
    }

    pub(crate) fn insert(
        &mut self,
        parents: Parents<'_>,
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
        let mut values = SequenceTree::new();
        values.push(value);
        let action = PatchAction::Insert { index, values };
        self.finish(parents, obj, action);
    }

    pub(crate) fn splice_text(
        &mut self,
        parents: Parents<'_>,
        obj: ObjId,
        index: usize,
        value: &str,
        marks: Option<Arc<RichText>>,
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
        let action = PatchAction::SpliceText {
            index,
            value: value.into(),
            marks: marks.as_deref().cloned(),
        };
        self.finish(parents, obj, action);
        self.last_mark_set = marks;
    }

    pub(crate) fn delete_seq(
        &mut self,
        parents: Parents<'_>,
        obj: ObjId,
        index: usize,
        length: usize,
    ) {
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
        let action = PatchAction::DeleteSeq { index, length };
        self.finish(parents, obj, action);
    }

    pub(crate) fn delete_map(&mut self, parents: Parents<'_>, obj: ObjId, key: &str) {
        let action = PatchAction::DeleteMap {
            key: key.to_owned(),
        };
        self.finish(parents, obj, action);
    }

    pub(crate) fn put(
        &mut self,
        parents: Parents<'_>,
        obj: ObjId,
        prop: Prop,
        tagged_value: (Value<'_>, ObjId),
        conflict: bool,
    ) {
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
        self.finish(parents, obj, action);
    }

    pub(crate) fn increment(
        &mut self,
        parents: Parents<'_>,
        obj: ObjId,
        prop: Prop,
        tagged_value: (i64, ObjId),
    ) {
        let value = tagged_value.0;
        let action = PatchAction::Increment { prop, value };
        self.finish(parents, obj, action);
    }

    pub(crate) fn mark<'a, 'b, M: Iterator<Item = Mark<'b>>>(
        &mut self,
        parents: Parents<'a>,
        obj: ObjId,
        mark: M,
    ) {
        if let Some(PatchAction::Mark { marks, .. }) = maybe_append(&mut self.patches, &obj) {
            for m in mark {
                marks.push(m.into_owned())
            }
            return;
        }
        let marks: Vec<_> = mark.map(|m| m.into_owned()).collect();
        if !marks.is_empty() {
            let action = PatchAction::Mark { marks };
            self.finish(parents, obj, action);
        }
    }

    pub(crate) fn flag_conflict(&mut self, parents: Parents<'_>, obj: ObjId, prop: Prop) {
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
        } else {
            let action = PatchAction::Conflict { prop };
            self.finish(parents, obj, action);
        }
    }

    fn finish(&mut self, parents: Parents<'_>, obj: ObjId, action: PatchAction) {
        let mut patch = Patch {
            obj,
            action,
            path: vec![],
        };
        for p in parents {
            // parent was deleted - dont make a patch
            if !p.visible {
                return;
            }
            patch.path.push((p.obj, p.prop));
        }
        patch.path.reverse();
        self.patches.push(patch);
        self.last_mark_set = None;
    }
}

impl AsMut<PatchBuilder> for PatchBuilder {
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
