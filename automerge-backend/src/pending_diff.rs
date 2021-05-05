use automerge_protocol as amp;

use crate::{
    internal::{Key, OpId},
    op_handle::OpHandle,
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

pub(super) struct Edits(Vec<amp::DiffEdit>);

impl Edits {
    pub(super) fn new() -> Edits {
        Edits(Vec::new())
    }

    pub(super) fn append_edit(&mut self, edit: amp::DiffEdit) {
        self.0.push(edit)
    }

    pub(super) fn to_vec(self) -> Vec<amp::DiffEdit> {
        self.0
    }
}
