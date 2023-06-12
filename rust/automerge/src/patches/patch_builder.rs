use core::fmt::Debug;
use std::sync::Arc;

use crate::block::Block;
use crate::marks::RichText;
use crate::{Cursor, ObjId, Prop, ReadDoc, Value};

use super::{Patch, PatchAction};
use crate::{marks::Mark, sequence_tree::SequenceTree};

#[derive(Debug, Clone, Default)]
pub(crate) struct PatchBuilder {
    patches: Vec<Patch>,
    last_mark_set: Option<Arc<RichText>>, // keep this around for a quick pointer equality test
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

    pub(crate) fn take_patches(&mut self) -> Vec<Patch> {
        std::mem::take(&mut self.patches)
    }

    pub(crate) fn insert<R: ReadDoc>(
        &mut self,
        InsertArgs {
            doc,
            obj,
            index,
            tagged_value,
            conflict,
            marks,
            block_id,
        }: InsertArgs<'_, '_, R>,
    ) {
        if let Some(block) = tagged_value.0.to_block() {
            if let Some(cursor) = block_id {
                return self.split_block(doc, obj, index, block, cursor, conflict);
            }
        }
        let value = (tagged_value.0.to_owned(), tagged_value.1, conflict);
        if let Some(PatchAction::Insert {
            index: tail_index,
            values,
            ..
        }) = maybe_append(&mut self.patches, &obj)
        {
            let range = *tail_index..=*tail_index + values.len();
            if marks == self.last_mark_set && range.contains(&index) {
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
                marks: marks.as_deref().cloned(),
            };
            self.push(Patch { obj, path, action });
            self.last_mark_set = marks;
        }
    }

    fn push(&mut self, patch: Patch) {
        self.patches.push(patch);
        self.last_mark_set = None;
    }

    pub(crate) fn splice_text<R: ReadDoc>(
        &mut self,
        doc: &R,
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
        if let Some(path) = self.get_path(doc, &obj) {
            let action = PatchAction::SpliceText {
                index,
                value: value.into(),
                marks: marks.as_deref().cloned(),
            };
            self.push(Patch { obj, path, action });
            self.last_mark_set = marks;
        }
    }

    pub(crate) fn delete_seq<R: ReadDoc>(
        &mut self,
        doc: &R,
        obj: ObjId,
        index: usize,
        length: usize,
        block_id: Option<Cursor>,
    ) {
        if let Some(block) = block_id {
            return self.join_block(doc, obj, index, block);
        }
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
            self.push(Patch { obj, path, action })
        }
    }

    pub(crate) fn delete_map<R: ReadDoc>(&mut self, doc: &R, obj: ObjId, key: &str) {
        if let Some(path) = self.get_path(doc, &obj) {
            let action = PatchAction::DeleteMap {
                key: key.to_owned(),
            };
            self.push(Patch { obj, path, action })
        }
    }

    pub(crate) fn put<R: ReadDoc>(
        &mut self,
        doc: &R,
        obj: ObjId,
        prop: Prop,
        tagged_value: (Value<'_>, ObjId),
        conflict: bool,
        block_id: Option<Cursor>,
    ) {
        if let Some(block) = tagged_value.0.to_block() {
            if let Some(cursor) = block_id {
                if let Prop::Seq(index) = prop {
                    return self.update_block(doc, obj, index, block, cursor, conflict);
                }
            }
        }
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
            self.push(Patch { obj, path, action })
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
            self.push(Patch { obj, path, action })
        }
    }

    pub(crate) fn mark<'a, 'b, R: ReadDoc, M: Iterator<Item = Mark<'b>>>(
        &mut self,
        doc: &'a R,
        obj: ObjId,
        mark: M,
    ) {
        if let Some(PatchAction::Mark { marks, .. }) = maybe_append(&mut self.patches, &obj) {
            for m in mark {
                marks.push(m.into_owned())
            }
            return;
        }
        if let Some(path) = self.get_path(doc, &obj) {
            let marks: Vec<_> = mark.map(|m| m.into_owned()).collect();
            if !marks.is_empty() {
                let action = PatchAction::Mark { marks };
                self.push(Patch { obj, path, action });
            }
        }
    }

    pub(crate) fn flag_conflict<R: ReadDoc>(&mut self, doc: &R, obj: ObjId, prop: Prop) {
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
        } else if let Some(path) = self.get_path(doc, &obj) {
            let action = PatchAction::Conflict { prop };
            self.push(Patch { obj, path, action });
        }
    }

    fn split_block<R: ReadDoc>(
        &mut self,
        doc: &R,
        obj: ObjId,
        index: usize,
        block: Block,
        cursor: Cursor,
        conflict: bool,
    ) {
        if let Some(path) = self.get_path(doc, &obj) {
            let name = block.name;
            let parents = block.parents;
            let action = PatchAction::SplitBlock {
                index,
                name,
                parents,
                cursor,
                conflict,
            };
            self.push(Patch { obj, path, action })
        }
    }

    fn join_block<R: ReadDoc>(&mut self, doc: &R, obj: ObjId, index: usize, cursor: Cursor) {
        if let Some(path) = self.get_path(doc, &obj) {
            let action = PatchAction::JoinBlock { index, cursor };
            self.push(Patch { obj, path, action })
        }
    }

    pub(crate) fn update_block<R: ReadDoc>(
        &mut self,
        doc: &R,
        obj: ObjId,
        index: usize,
        block: Block,
        cursor: Cursor,
        conflict: bool,
    ) {
        match maybe_append(&mut self.patches, &obj) {
            Some(PatchAction::SplitBlock {
                cursor: tail_cursor,
                name,
                parents,
                ..
            }) if *tail_cursor == cursor => {
                *name = block.name;
                *parents = block.parents;
                return;
            }
            _ => {}
        }
        if let Some(path) = self.get_path(doc, &obj) {
            let name = block.name;
            let parents = block.parents;
            let action = PatchAction::UpdateBlock {
                index,
                name,
                parents,
                cursor,
                conflict,
            };
            self.push(Patch { obj, path, action })
        }
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

pub(crate) struct InsertArgs<'a, 'b, R: ReadDoc> {
    pub(crate) doc: &'a R,
    pub(crate) obj: ObjId,
    pub(crate) index: usize,
    pub(crate) tagged_value: (Value<'b>, ObjId),
    pub(crate) conflict: bool,
    pub(crate) marks: Option<Arc<RichText>>,
    pub(crate) block_id: Option<Cursor>,
}
