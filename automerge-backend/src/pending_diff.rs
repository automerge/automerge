use crate::actor_map::ActorMap;
use crate::internal::{Key, OpId};
use crate::op_handle::OpHandle;
use automerge_protocol as amp;

#[derive(Debug, Clone, PartialEq)]
pub(crate) enum PendingDiff {
    SeqInsert(OpHandle, usize, OpId),
    SeqRemove(OpHandle, usize),
    Set(OpHandle),
    CursorChange(Key),
}

impl PendingDiff {
    pub fn operation_key(&self) -> Key {
        match self {
            Self::SeqInsert(op, _, _) => op.operation_key(),
            Self::SeqRemove(op, _) => op.operation_key(),
            Self::Set(op) => op.operation_key(),
            Self::CursorChange(k) => k.clone(),
        }
    }

    pub fn edit(&self, actors: &ActorMap) -> Option<amp::DiffEdit> {
        match *self {
            Self::SeqInsert(_, index, opid) => Some(amp::DiffEdit::Insert {
                index,
                elem_id: actors.export_opid(&opid).into(),
            }),
            Self::SeqRemove(_, index) => Some(amp::DiffEdit::Remove { index }),
            _ => None,
        }
    }
}
