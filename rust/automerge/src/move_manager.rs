use std::collections::{HashMap, HashSet};
use crate::types::{OpId, Op, ObjId};
mod document_tree;
mod winner_indicator;

use document_tree::DocumentTree;
use winner_indicator::WinnerIndicator;

pub(crate) struct MoveManager<'a> {
    op_log: Vec<LogEntry<'a>>,
    tree: DocumentTree,
    cycle_safe_operation_map: HashMap<OpId, bool>, // we can't use HashSet
    winner_indicator: WinnerIndicator,
}

struct LogEntry<'a> {
    op: &'a Op,
    parent_id: ObjId,
}

impl MoveManager {
    pub(crate) fn new () -> Self {
        Self {
            op_log: Vec::new(),
            tree: DocumentTree::new(),
            cycle_safe_operation_map: HashMap::new(),
            winner_indicator: WinnerIndicator::new(),
        }
    }

    pub(crate) fn is_visible(&self, op_id: OpId) -> bool {
        self.cycle_safe_operation_map.get(&op_id).map_or(true, |&is_cycle_safe| {
            is_cycle_safe && self.winner_indicator.is_winner(op_id)
        })
    }

    pub(crate) fn do_op(&self, op: Op) {
        todo!()
    }

    fn undo_op(&self, op: Op) {
        todo!()
    }

    fn redo_op(&self, op: Op) {
        todo!()
    }

}