use crate::op_handle::OpHandle;
use automerge_protocol as amp;

#[derive(Debug, Clone, PartialEq)]
pub(crate) enum PendingDiff {
    SeqInsert(OpHandle, usize),
    SeqRemove(OpHandle, usize),
    Set(OpHandle),
}

impl PendingDiff {
    pub fn operation_key(&self) -> amp::Key {
        match self {
            Self::SeqInsert(op, _) => op.operation_key(),
            Self::SeqRemove(op, _) => op.operation_key(),
            Self::Set(op) => op.operation_key(),
        }
    }

    pub fn edit(&self) -> Option<amp::DiffEdit> {
        match *self {
            Self::SeqInsert(_, index) => Some(amp::DiffEdit::Insert { index }),
            Self::SeqRemove(_, index) => Some(amp::DiffEdit::Remove { index }),
            _ => None,
        }
    }
}
