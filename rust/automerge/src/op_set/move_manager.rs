use crate::op_set::OpSetMetadata;
use crate::types::{ObjId, Op, OpId};
use crate::OpType;
use std::cmp::Ordering;
use std::collections::HashMap;
use std::hash::{Hash, Hasher};

pub(crate) type ValidityChangesMap = HashMap<(ObjId, usize), bool>;

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct MoveManager {
    op_log: Vec<LogEntry>,
    doc_tree: DocumentTree,
    opid_to_log_index: HashMap<OpId, usize>,
    move_op_stacks: HashMap<ObjId, Vec<usize>>,
}

impl Default for MoveManager {
    fn default() -> Self {
        MoveManager::new()
    }
}

impl MoveManager {
    pub(crate) fn new() -> Self {
        Self {
            op_log: Vec::new(),
            doc_tree: DocumentTree::new(),
            move_op_stacks: HashMap::new(),
            opid_to_log_index: HashMap::new(),
        }
    }

    pub(crate) fn start_validity_check<'a>(
        &'a mut self,
        id: OpId,
        m: &'a OpSetMetadata,
    ) -> ValidityChecker<'a> {
        ValidityChecker::new(self, id, m)
    }

    fn get_logs_length(&self) -> usize {
        self.op_log.len()
    }

    fn insert_log_entry_at(&mut self, entry: LogEntry, index: usize) {
        if let Some(id) = entry.move_id {
            self.move_op_stacks.entry(id).or_insert(Vec::new());
        }
        self.opid_to_log_index.insert(entry.id, index);
        self.op_log.insert(index, entry);
    }

    fn update_log_index(&mut self, opid: OpId, new_index: usize) {
        self.opid_to_log_index.insert(opid, new_index);
    }

    fn get_log_entry_at(&self, idx: usize) -> &LogEntry {
        self.op_log.get(idx).expect("OpLog index out of bound")
    }

    fn get_mut_log_entry_at(&mut self, idx: usize) -> &mut LogEntry {
        self.op_log.get_mut(idx).expect("OpLog index out of bound")
    }

    fn get_log_entry_by_opid(&mut self, opid: &OpId) -> &LogEntry {
        self.opid_to_log_index
            .get(opid)
            .and_then(|idx| self.op_log.get(*idx))
            .expect("OpLog not found")
    }

    fn get_mut_log_entry_by_opid(&mut self, opid: &OpId) -> &mut LogEntry {
        self.opid_to_log_index
            .get(opid)
            .and_then(|idx| self.op_log.get_mut(*idx))
            .expect("OpLog not found")
    }

    fn pop_concurrent_move(&mut self, move_id: ObjId, changes: &mut ValidityChangesMap) {
        let move_op_stack = self.move_op_stacks.get_mut(&move_id).unwrap();
        move_op_stack.pop();
        self.set_top_move_validity(move_id, true, changes);
    }

    fn push_concurrent_move(
        &mut self,
        move_id: ObjId,
        new_idx: usize,
        changes: &mut ValidityChangesMap,
    ) {
        self.set_top_move_validity(move_id, false, changes);
        let move_op_stack = self.move_op_stacks.get_mut(&move_id).unwrap();
        move_op_stack.push(new_idx);
        self.set_top_move_validity(move_id, true, changes);
    }

    fn set_top_move_validity(
        &mut self,
        move_id: ObjId,
        validity: bool,
        changes: &mut ValidityChangesMap,
    ) {
        let move_op_stack = self.move_op_stacks.get(&move_id).unwrap();
        if let Some(top_idx) = move_op_stack.last() {
            let top_move_op_entry = self.get_mut_log_entry_at(*top_idx);
            top_move_op_entry.validity = validity;
            changes.insert(
                (
                    top_move_op_entry.op_tree_id.unwrap(),
                    top_move_op_entry.op_tree_index.unwrap(),
                ),
                top_move_op_entry.validity,
            );
        }
    }

    fn set_validity(&mut self, opid: &OpId, validity: bool, changes: &mut ValidityChangesMap) {
        let log_entry = self.get_mut_log_entry_by_opid(opid);
        log_entry.validity = validity;
        changes.insert(
            (
                log_entry.op_tree_id.unwrap(),
                log_entry.op_tree_index.unwrap(),
            ),
            log_entry.validity,
        );
    }

    fn store_parent_for_op(&mut self, opid: &OpId, child_id: ObjId, parent_id: Option<ObjId>) {
        let log_entry = self.get_mut_log_entry_by_opid(opid);
        log_entry.parent_map.insert(child_id, parent_id);
        self.doc_tree.update_parent(child_id, None);
    }

    fn revert(&mut self, op: &Op, idx: usize, validity_changes: &mut ValidityChangesMap) {
        let log_entry = self.get_log_entry_at(idx);
        let parent_map = &log_entry.parent_map.clone();
        for (obj_id, parent_id) in parent_map {
            self.doc_tree.update_parent(*obj_id, *parent_id);
        }
        if let OpType::Move(_, _) = op.action {
            let move_id = op.move_id.expect("Move operation must have move_id");
            self.pop_concurrent_move(move_id, validity_changes);
        }
    }

    fn apply(&mut self, op: &Op, idx: usize, validity_changes: &mut ValidityChangesMap) {
        let apply_log = self.get_log_entry_at(idx);
        let op_tree_id = apply_log.op_tree_id;
        // handle deletion and overwrites
        op.pred.iter().for_each(|pred_id| {
            let pred_log = self.get_log_entry_by_opid(pred_id);
            let obj_id = match pred_log.move_id {
                Some(id) => id,
                None => (*pred_id).into(),
            };
            let curr_parent = self.doc_tree.get_parent(obj_id);
            self.store_parent_for_op(&op.id, obj_id, curr_parent);
            self.doc_tree.update_parent(obj_id, None);
        });

        // handle move and make
        match op.action {
            OpType::Make(_) => {
                let curr_parent = self.doc_tree.get_parent(op.id.into());
                self.store_parent_for_op(&op.id, op.id.into(), curr_parent);
                self.doc_tree
                    .update_parent(op.id.into(), Some(op_tree_id.unwrap()));
            }
            OpType::Move(_, _) => {
                let move_id = op.move_id.expect("Move operation must have move_id");
                if self.doc_tree.is_ancestor_of(move_id, op_tree_id.unwrap()) {
                    self.set_validity(&op.id, false, validity_changes);
                    let parent_id = self.doc_tree.get_parent(move_id);
                    self.store_parent_for_op(&op.id, move_id, parent_id);
                    return;
                }
                //self.parent_maps[idx].insert(ObjId(op.id), self.doc_tree.get_parent(ObjId(op.id)));
                self.doc_tree
                    .update_parent(move_id, Some(op_tree_id.unwrap()));
                self.push_concurrent_move(move_id, idx, validity_changes);
            }
            _ => {}
        }
    }
}

#[derive(Debug, Clone)]
pub(crate) struct LogEntry {
    pub(crate) id: OpId,
    pub(crate) op_tree_id: Option<ObjId>, // None if it is a delete op
    pub(crate) op_tree_index: Option<usize>, // None if it is a delete op
    move_id: Option<ObjId>,               // None if it is not a move operation
    validity: bool,                       // always true if it is not a move operation
    parent_map: HashMap<ObjId, Option<ObjId>>,
}

impl Hash for LogEntry {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.id.hash(state);
    }
}

impl PartialEq for LogEntry {
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id
    }
}

impl LogEntry {
    pub(crate) fn new(
        id: OpId,
        op_tree_id: Option<ObjId>,
        op_tree_index: Option<usize>,
        move_id: Option<ObjId>,
    ) -> Self {
        Self {
            id,
            op_tree_id,
            op_tree_index,
            move_id,
            validity: true,
            parent_map: HashMap::new(),
        }
    }
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

    pub(crate) fn get_parent(&self, obj_id: ObjId) -> Option<ObjId> {
        self.parent_map.get(&obj_id).cloned().unwrap_or(None)
    }

    pub(crate) fn update_parent(&mut self, obj_id: ObjId, new_parent_id: Option<ObjId>) {
        self.parent_map.insert(obj_id, new_parent_id);
    }

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
}

pub(crate) struct ValidityChecker<'a> {
    manager: &'a mut MoveManager,
    new_id: OpId,
    m: &'a OpSetMetadata,
}

impl<'a> ValidityChecker<'a> {
    fn new(manager: &'a mut MoveManager, id: OpId, m: &'a OpSetMetadata) -> Self {
        Self {
            manager,
            new_id: id,
            m,
        }
    }

    pub(crate) fn get_logs_with_greater_ids(&self) -> Vec<&LogEntry> {
        let mut idx = self.manager.get_logs_length();
        let mut ops = Vec::new();
        if idx == 0 {
            return ops;
        } else {
            idx -= 1;
        }
        let mut curr_entry = self.manager.get_log_entry_at(idx);
        while let Ordering::Greater = self.m.lamport_cmp(curr_entry.id, self.new_id) {
            ops.push(curr_entry);
            if idx == 0 {
                break;
            }
            idx -= 1;
            curr_entry = self.manager.get_log_entry_at(idx);
        }
        ops
    }

    pub(crate) fn update_validity(
        &mut self,
        ops_with_greater_ids: Vec<&Op>,
        new_op: &Op,
        new_log: LogEntry,
    ) -> ValidityChangesMap {
        let mut validity_changes: ValidityChangesMap = HashMap::new();
        let mut idx = self.manager.get_logs_length();

        // Restore
        for op in &ops_with_greater_ids {
            idx -= 1;
            self.manager.revert(op, idx, &mut validity_changes);
        }

        // Apply
        self.manager.insert_log_entry_at(new_log, idx);
        self.manager.apply(new_op, idx, &mut validity_changes);

        // Reapply
        for op in ops_with_greater_ids {
            idx += 1;
            self.manager.apply(op, idx, &mut validity_changes);
            self.manager.update_log_index(op.id, idx);
        }

        validity_changes
    }
}
