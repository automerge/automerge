use crate::types::{OpId, ObjId};
use std::collections::{HashMap, HashSet};

pub(super) struct WinnerIndicator {
    obj_id_to_move_stack: HashMap<ObjId, Vec<OpId>>,
    winner_set: HashSet<OpId>,
}

impl WinnerIndicator {
    pub(super) fn new() -> Self {
        Self {
            obj_id_to_move_stack: HashMap::new(),
            winner_set: HashSet::new(),
        }
    }

    pub(super) fn is_winner(&self, op_id: OpId) -> bool {
        self.winner_set.contains(&op_id)
    }

    pub(super) fn undo_op_for_obj(&mut self, obj_id: ObjId) {
        self.obj_id_to_move_stack.get_mut(&obj_id).and_then(|stack| {
            if Some(op_id) = stack.pop() {
                self.winner_set.remove(&op_id);
            }
            if Some(op_id) = stack.last() {
                self.winner_set.insert(*op_id);
            }
            Some(())
        });
    }

    pub(super) fn redo_op_for_obj(&mut self, obj_id: ObjId, op_id: OpId) {
        if Some(stack) = self.obj_id_to_move_stack.get_mut(&obj_id) {
            if Some(last_op_id) = stack.last() {
                winner_set.remove(last_op_id);
            }
            stack.push(op_id);
            self.winner_set.insert(op_id);
        } else {
            self.obj_id_to_move_stack.insert(obj_id, vec![op_id]);
            self.winner_set.insert(op_id);
        }
    }
}

