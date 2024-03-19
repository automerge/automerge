use core::fmt::Debug;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use itertools::Itertools;

use crate::clock::Clock;
use crate::marks::RichText;
use crate::{Automerge, Block, Cursor, ObjId, Parents, Prop, ReadDoc, Value};

use super::{Patch, PatchAction};
use crate::{marks::Mark, sequence_tree::SequenceTree};

#[derive(Debug, Clone)]
pub(crate) struct PatchBuilder<'a> {
    patches: Vec<Patch>,
    last_mark_set: Option<Arc<RichText>>, // keep this around for a quick pointer equality test
    osd: &'a crate::op_set::OpSetData,
    block_objs: HashSet<crate::types::ObjId>,
}

impl<'a> PatchBuilder<'a> {
    pub(crate) fn new(osd: &'a crate::op_set::OpSetData) -> Self {
        Self {
            patches: Vec::new(),
            last_mark_set: None,
            osd,
            block_objs: HashSet::new(),
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
        if self.block_objs.contains(&obj.to_internal_obj()) {
            return;
        }
        if self.block_objs.contains(&tagged_value.1.to_internal_obj()) {
            return;
        }
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
        if self.block_objs.contains(&obj.to_internal_obj()) {
            return;
        }
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
        if self.block_objs.contains(&obj.to_internal_obj()) {
            return;
        }
        if self.block_objs.contains(&tagged_value.1.to_internal_obj()) {
            return;
        }
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

    pub(crate) fn mark<'b, 'c, M: Iterator<Item = Mark<'c>>>(
        &mut self,
        parents: Parents<'b>,
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

    pub(crate) fn split_block(
        &mut self,
        doc: &Automerge,
        parents: Parents<'_>,
        obj: ObjId,
        created_at: Option<&Clock>,
        index: usize,
        block_id: crate::types::ObjId,
        elem: crate::types::OpId,
    ) {
        if let Some(block) =
            load_split_block(doc, &mut self.block_objs, block_id, created_at)
        {
            let action = PatchAction::SplitBlock {
                index,
                cursor: Cursor::new(elem, self.osd),
                conflict: false,
                parents: block.parents().to_vec(),
                block_type: block.block_type().to_string(),
                attrs: block.attrs().clone(),
            };
            self.finish(parents, obj, action);
        }
    }

    pub(crate) fn join_block(
        &mut self,
        parents: Parents<'_>,
        obj: ObjId,
        joined_block_id: crate::types::ObjId,
        index: usize,
    ) {
        self.block_objs.insert(joined_block_id);
        let action = PatchAction::JoinBlock { index };
        self.finish(parents, obj, action);
    }

    pub(crate) fn hydrate_update_block(
        &mut self,
        doc: &Automerge,
        parents: Parents<'_>,
        obj: ObjId,
        index: usize,
        before_block_id: crate::types::ObjId,
        after_block_id: crate::types::ObjId,
    ) {
        let crate::automerge::diff::UpdateBlockDiff {
            new_type,
            new_parents,
            new_attrs,
            composed_obj_ids,
        } = crate::automerge::diff::load_update_block_diff(index, doc, before_block_id, after_block_id);
        self.block_objs.extend(composed_obj_ids);
        if new_type.is_some() || new_parents.is_some() || new_attrs.is_some() {
            let action = PatchAction::UpdateBlock {
                index,
                new_block_type: new_type,
                new_block_parents: new_parents,
                new_attrs,
            };
            self.finish(parents, obj, action);
        }
    }

    pub(crate) fn update_block(
        &mut self,
        parents: Parents<'_>,
        obj: ObjId,
        index: usize,
        new_block_id: crate::types::ObjId,
        new_parents_id: crate::types::ObjId,
        new_attrs_id: crate::types::ObjId,
        new_type: Option<String>,
        new_parents: Option<Vec<String>>,
        new_attrs: Option<HashMap<String, crate::ScalarValue>>,
    ) {
        self.block_objs.insert(new_block_id);
        self.block_objs.insert(new_parents_id);
        self.block_objs.insert(new_attrs_id);
        let action = PatchAction::UpdateBlock {
            index,
            new_block_type: new_type,
            new_block_parents: new_parents,
            new_attrs,
        };
        self.finish(parents, obj, action);
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

impl<'a> AsMut<PatchBuilder<'a>> for PatchBuilder<'a> {
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

fn load_split_block(
    doc: &Automerge,
    hidden_blocks: &mut HashSet<crate::types::ObjId>,
    block_obj_id: crate::types::ObjId,
    clock: Option<&Clock>,
) -> Option<Block> {
    hidden_blocks.insert(block_obj_id);
    let Some(block_ops) = doc.ops().iter_obj(&block_obj_id) else {
        return None;
    };
    // Don't log objects in the block to the patch log
    for op_idx in block_ops {
        let op = op_idx.as_op(doc.osd());
        if let crate::types::OpType::Make(_) = op.action() {
            hidden_blocks.insert(op.id().into());
        }
    }
    let block_val = doc.hydrate_map(&block_obj_id, clock);
    crate::block::hydrate_block(block_val)
}
