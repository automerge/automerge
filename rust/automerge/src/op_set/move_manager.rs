use crate::types::{ObjId, Op, OpId};
use std::collections::{HashMap, HashSet};

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct MoveManager {
    op_log: Vec<LogEntry>,
    tree: DocumentTree,
    cycle_safe_operation_map: HashMap<OpId, bool>,
    // we can't use HashSet for this
    winner_indicator: WinnerIndicator,
}

impl MoveManager {
    #[allow(dead_code)]
    pub(crate) fn new() -> Self {
        Self {
            op_log: Vec::new(),
            tree: DocumentTree::new(),
            cycle_safe_operation_map: HashMap::new(),
            winner_indicator: WinnerIndicator::new(),
        }
    }

    #[allow(dead_code)]
    pub(crate) fn is_visible(&self, op_id: OpId) -> bool {
        self.cycle_safe_operation_map
            .get(&op_id)
            .map_or(true, |&is_cycle_safe| {
                is_cycle_safe && self.winner_indicator.is_winner(op_id)
            })
    }

    #[allow(dead_code)]
    pub(crate) fn do_op(&self, _op: Op) {
        todo!()
    }

    #[allow(dead_code)]
    fn undo_op(&self, _op: Op) {
        todo!()
    }

    #[allow(dead_code)]
    fn redo_op(&self, _op: Op) {
        todo!()
    }
}

#[derive(Debug, Clone, PartialEq)]
struct LogEntry {
    op: Op,
    parent_id: ObjId,
}

#[derive(Debug, Clone, PartialEq)]
struct DocumentTree {
    parent_map: HashMap<ObjId, Option<ObjId>>,
}

impl DocumentTree {
    #[allow(dead_code)]
    pub(crate) fn new() -> Self {
        Self {
            parent_map: HashMap::new(),
        }
    }

    #[allow(dead_code)]
    pub(crate) fn insert(&mut self, obj_id: ObjId, parent_id: ObjId) {
        self.parent_map.insert(obj_id, Some(parent_id));
    }

    #[allow(dead_code)]
    pub(crate) fn remove(&mut self, obj_id: ObjId) {
        self.parent_map.remove(&obj_id);
    }

    #[allow(dead_code)]
    pub(crate) fn get_parent(&self, obj_id: ObjId) -> Option<ObjId> {
        self.parent_map.get(&obj_id).cloned().unwrap_or(None)
    }

    #[allow(dead_code)]
    pub(crate) fn is_ancestor_of(&self, ancestor_id: ObjId, descendant_id: ObjId) -> bool {
        let mut current_id = descendant_id;
        while let Some(parent_id) = self.get_parent(current_id) {
            if parent_id == ancestor_id {
                return true;
            }
            current_id = parent_id;
        } // reach root or garbage
        false
    }

    #[allow(dead_code)]
    pub(crate) fn update_parent(&mut self, obj_id: ObjId, new_parent_id: ObjId) {
        self.parent_map.insert(obj_id, Some(new_parent_id));
    }
}

#[derive(Debug, Clone, PartialEq)]
struct WinnerIndicator {
    obj_id_to_move_stack: HashMap<ObjId, Vec<OpId>>,
    winner_set: HashSet<OpId>,
}

impl WinnerIndicator {
    #[allow(dead_code)]
    pub(crate) fn new() -> Self {
        Self {
            obj_id_to_move_stack: HashMap::new(),
            winner_set: HashSet::new(),
        }
    }

    #[allow(dead_code)]
    pub(crate) fn is_winner(&self, op_id: OpId) -> bool {
        self.winner_set.contains(&op_id)
    }

    #[allow(dead_code)]
    pub(crate) fn undo_op_for_obj(&mut self, obj_id: ObjId) {
        if let Some(stack) = self.obj_id_to_move_stack.get_mut(&obj_id) {
            if let Some(op_id) = stack.pop() {
                self.winner_set.remove(&op_id);
            }
            if let Some(op_id) = stack.last() {
                self.winner_set.insert(*op_id);
            }
        }
    }

    #[allow(dead_code)]
    pub(crate) fn redo_op_for_obj(&mut self, obj_id: ObjId, op_id: OpId) {
        if let Some(stack) = self.obj_id_to_move_stack.get_mut(&obj_id) {
            if let Some(last_op_id) = stack.last() {
                self.winner_set.remove(last_op_id);
            }
            stack.push(op_id);
            self.winner_set.insert(op_id);
        } else {
            self.obj_id_to_move_stack.insert(obj_id, vec![op_id]);
            self.winner_set.insert(op_id);
        }
    }
}
