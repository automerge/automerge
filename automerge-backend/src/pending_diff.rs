use std::{
    collections::{HashMap, HashSet},
    mem,
};

use automerge_protocol as amp;

use crate::{
    internal::{Key, ObjectId, OpId, InternalOpType},
    object_store::ObjState,
    op_handle::OpHandle,
    actor_map::ActorMap,
};

#[derive(Debug, Clone, PartialEq)]
pub(crate) enum PendingDiff {
    SeqInsert(OpHandle, usize, OpId),
    SeqUpdate(OpHandle, usize, OpId),
    SeqRemove(OpHandle, usize),
    Set(OpHandle),
    CursorChange(Key),
}

impl PendingDiff {
    pub fn operation_key(&self) -> Key {
        match self {
            Self::SeqInsert(op, ..) => op.operation_key(),
            Self::SeqUpdate(op, ..) => op.operation_key(),
            Self::SeqRemove(op, ..) => op.operation_key(),
            Self::Set(op) => op.operation_key(),
            Self::CursorChange(k) => k.clone(),
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub(super) struct PendingDiffs(pub(super) HashMap<ObjectId, Vec<PendingDiff>>);

impl PendingDiffs {
    pub(super) fn new() -> PendingDiffs {
        PendingDiffs(HashMap::new())
    }

    pub(super) fn append_diffs(&mut self, oid: &ObjectId, mut diffs: Vec<PendingDiff>) {
        self.0.entry(oid.clone()).or_default().append(&mut diffs)
    }

    pub(super) fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    pub(super) fn changed_object_ids(&self) -> impl Iterator<Item = &ObjectId> {
        self.0.keys()
    }

    pub(super) fn get_mut(&mut self, obj_id: &ObjectId) -> Option<&mut Vec<PendingDiff>> {
        self.0.get_mut(obj_id)
    }

    fn gen_map_diff(
        &self,
        obj_id: &ObjectId,
        obj: &ObjState,
        pending: &[PendingDiff],
        actors: &ActorMap,
        map_type: amp::MapType,
    ) -> amp::MapDiff {
        let mut props = HashMap::new();
        // I may have duplicate keys - I do this to make sure I visit each one only once
        let keys: HashSet<_> = pending.iter().map(|p| p.operation_key()).collect();
        for key in keys.iter() {
            let key_string = actors.key_to_string(key);
            let mut opid_to_value = HashMap::new();
            for op in obj.props.get(&key).iter().flat_map(|i| i.iter()) {
                let link = match op.action {
                    InternalOpType::Set(ref value) => self.gen_value_diff(op, value),
                    InternalOpType::Make(_) => {
                        // FIXME
                        self.gen_obj_diff(&op.id.into(), actors)
                    }
                    _ => panic!("del or inc found in field_operations"),
                };
                opid_to_value.insert(actors.export_opid(&op.id), link);
            }
            props.insert(key_string, opid_to_value);
        }
        amp::MapDiff {
            object_id: actors.export_obj(obj_id),
            obj_type: map_type,
            props,
        }
    }

    fn gen_value_diff(&self, op: &OpHandle, value: &amp::ScalarValue) -> amp::Diff {
        match value {
            amp::ScalarValue::Cursor(oid) => {
                // .expect() is okay here because we check that the cursr exists at the start of
                // `OpSet::apply_op()`
                let cursor_state = self
                    .cursors
                    .values()
                    .flatten()
                    .find(|c| c.element_opid == *oid)
                    .expect("missing cursor");
                amp::Diff::Cursor(amp::CursorDiff {
                    object_id: cursor_state.referred_object_id.clone(),
                    index: cursor_state.index as u32,
                    elem_id: oid.clone(),
                })
            }
            _ => op.adjusted_value().into(),
        }
    }
}

#[derive(Debug)]
pub(super) struct Edits(Vec<amp::DiffEdit>);

impl Edits {
    pub(super) fn new() -> Edits {
        Edits(Vec::new())
    }

    /// Append an edit to this sequence, collapsing it into the last edit if possible.
    ///
    /// The collapsing handles conversion of a sequence of inserts to a multi-insert.
    pub(super) fn append_edit(&mut self, edit: amp::DiffEdit) {
        if let Some(mut last) = self.0.last_mut() {
            match (&mut last, edit) {
                (
                    amp::DiffEdit::SingleElementInsert {
                        index,
                        elem_id,
                        op_id,
                        value: amp::Diff::Value(value),
                    },
                    amp::DiffEdit::SingleElementInsert {
                        index: next_index,
                        elem_id: next_elem_id,
                        op_id: next_op_id,
                        value: amp::Diff::Value(next_value),
                    },
                ) if *index + 1 == next_index
                    && elem_id.as_opid() == Some(op_id)
                    && next_elem_id.as_opid() == Some(&next_op_id)
                    && op_id.delta(&next_op_id, 1) =>
                {
                    *last = amp::DiffEdit::MultiElementInsert {
                        index: *index,
                        elem_id: elem_id.clone(),
                        values: vec![mem::replace(value, amp::ScalarValue::Null), next_value],
                    };
                }
                (
                    amp::DiffEdit::MultiElementInsert {
                        index,
                        elem_id,
                        values,
                    },
                    amp::DiffEdit::SingleElementInsert {
                        index: next_index,
                        elem_id: next_elem_id,
                        op_id,
                        value: amp::Diff::Value(value),
                    },
                ) if *index + (values.len() as u64) == next_index
                    && next_elem_id.as_opid() == Some(&op_id)
                    && elem_id
                        .as_opid()
                        .unwrap()
                        .delta(&op_id, values.len() as u64) =>
                {
                    values.push(value);
                }
                (
                    amp::DiffEdit::Remove { index, count },
                    amp::DiffEdit::Remove {
                        index: new_index,
                        count: new_count,
                    },
                ) if *index == new_index => *count += new_count,
                (_, edit) => self.0.push(edit),
            }
        } else {
            self.0.push(edit)
        }
    }

    pub(super) fn into_vec(self) -> Vec<amp::DiffEdit> {
        self.0
    }
}
