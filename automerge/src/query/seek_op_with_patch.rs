use crate::op_tree::{OpSetMetadata, OpTreeNode};
use crate::query::{binary_search_by, QueryResult, TreeQuery};
use crate::types::{ElemId, Key, Op, HEAD};
use std::cmp::Ordering;
use std::fmt::Debug;

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct SeekOpWithPatch<const B: usize> {
    op: Op,
    pub pos: usize,
    pub succ: Vec<usize>,
    found: bool,
    pub seen: usize,
    last_seen: Option<ElemId>,
    pub values: Vec<Op>,
    pub had_value_before: bool,
}

impl<const B: usize> SeekOpWithPatch<B> {
    pub fn new(op: &Op) -> Self {
        SeekOpWithPatch {
            op: op.clone(),
            succ: vec![],
            pos: 0,
            found: false,
            seen: 0,
            last_seen: None,
            values: vec![],
            had_value_before: false,
        }
    }

    fn lesser_insert(&self, op: &Op, m: &OpSetMetadata) -> bool {
        op.insert && m.lamport_cmp(op.id, self.op.id) == Ordering::Less
    }

    fn greater_opid(&self, op: &Op, m: &OpSetMetadata) -> bool {
        m.lamport_cmp(op.id, self.op.id) == Ordering::Greater
    }

    fn is_target_insert(&self, op: &Op) -> bool {
        op.insert && op.elemid() == self.op.key.elemid()
    }

    /// Keeps track of the number of visible list elements we have seen. Increments `self.seen` if
    /// operation `e` associates a visible value with a list element, and if we have not already
    /// counted that list element (this ensures that if a list element has several values, i.e.
    /// a conflict, then it is still only counted once).
    fn count_visible(&mut self, e: &Op) {
        if e.elemid() == self.op.elemid() {
            return;
        }
        if e.insert {
            self.last_seen = None
        }
        if e.visible() && self.last_seen.is_none() {
            self.seen += 1;
            self.last_seen = e.elemid()
        }
    }
}

impl<const B: usize> TreeQuery<B> for SeekOpWithPatch<B> {
    fn query_node_with_metadata(
        &mut self,
        child: &OpTreeNode<B>,
        m: &OpSetMetadata,
    ) -> QueryResult {
        if self.found {
            return QueryResult::Descend;
        }
        match self.op.key {
            // Special case for insertion at the head of the list (`e == HEAD` is only possible for
            // an insertion operation). Skip over any list elements whose elemId is greater than
            // the opId of the operation being inserted.
            Key::Seq(e) if e == HEAD => {
                while self.pos < child.len() {
                    let op = child.get(self.pos).unwrap();
                    if op.insert && m.lamport_cmp(op.id, self.op.id) == Ordering::Less {
                        break;
                    }
                    self.count_visible(&op);
                    self.pos += 1;
                }
                QueryResult::Finish
            }

            // Updating a list: search for the tree node that contains the new operation's
            // reference element (i.e. the element we're updating or inserting after)
            Key::Seq(e) => {
                if self.found || child.index.ops.contains(&e.0) {
                    QueryResult::Descend
                } else {
                    self.pos += child.len();

                    // When we skip over a subtree, we need to count the number of visible list
                    // elements we're skipping over. Each node stores the number of visible
                    // elements it contains. However, it could happen that a visible element is
                    // split across two tree nodes. To avoid double-counting in this situation, we
                    // subtract one if the last visible element also appears in this tree node.
                    let mut num_vis = child.index.len;
                    if num_vis > 0 {
                        // FIXME: I think this is wrong: we should subtract one only if this
                        // subtree contains a *visible* (i.e. empty succs) operation for the list
                        // element with elemId `last_seen`; this will subtract one even if all
                        // values for this list element have been deleted in this subtree.
                        if child.index.has(&self.last_seen) {
                            num_vis -= 1;
                        }
                        self.seen += num_vis;

                        // FIXME: this is also wrong: `last_seen` needs to be the elemId of the
                        // last *visible* list element in this subtree, but I think this returns
                        // the last operation's elemId regardless of whether it's visible or not.
                        // This will lead to incorrect counting if `last_seen` is not visible: it's
                        // not counted towards `num_vis`, so we shouldn't be subtracting 1.
                        self.last_seen = child.last().elemid();
                    }
                    QueryResult::Next
                }
            }

            // Updating a map: operations appear in sorted order by key
            Key::Map(_) => {
                // Search for the place where we need to insert the new operation. First find the
                // first op with a key >= the key we're updating
                self.pos = binary_search_by(child, |op| m.key_cmp(&op.key, &self.op.key));
                while self.pos < child.len() {
                    // Iterate over any existing operations for the same key; stop when we reach an
                    // operation with a different key
                    let op = child.get(self.pos).unwrap();
                    if op.key != self.op.key {
                        break;
                    }

                    // Keep track of any ops we're overwriting and any conflicts on this key
                    if self.op.overwrites(op) {
                        self.succ.push(self.pos);
                    } else if op.visible() {
                        self.values.push(op.clone());
                    }

                    // Ops for the same key should be in ascending order of opId, so we break when
                    // we reach an op with an opId greater than that of the new operation
                    if m.lamport_cmp(op.id, self.op.id) == Ordering::Greater {
                        break;
                    }
                    self.pos += 1;
                }

                // For the purpose of reporting conflicts, we also need to take into account any
                // ops for the same key that appear after the new operation
                let mut later_pos = self.pos;
                while later_pos < child.len() {
                    let op = child.get(later_pos).unwrap();
                    if op.key != self.op.key {
                        break;
                    }
                    // No need to check if `self.op.overwrites(op)` because an operation's `preds`
                    // must always have lower Lamport timestamps than that op itself, and the ops
                    // here all have greater opIds than the new op
                    if op.visible() {
                        self.values.push(op.clone());
                    }
                    later_pos += 1;
                }
                QueryResult::Finish
            }
        }
    }

    // Only called when operating on a sequence (list/text) object, since updates of a map are
    // handled in `query_node_with_metadata`.
    fn query_element_with_metadata(&mut self, e: &Op, m: &OpSetMetadata) -> QueryResult {
        let result = if !self.found {
            // First search for the referenced list element (i.e. the element we're updating, or
            // after which we're inserting)
            if self.is_target_insert(e) {
                self.found = true;
                if self.op.overwrites(e) {
                    self.succ.push(self.pos);
                }
                if e.visible() {
                    self.had_value_before = true;
                }
            }
            self.pos += 1;
            QueryResult::Next
        } else {
            // Once we've found the reference element, keep track of any ops that we're overwriting
            let overwritten = self.op.overwrites(e);
            if overwritten {
                self.succ.push(self.pos);
            }

            // If the new op is an insertion, skip over any existing list elements whose elemId is
            // greater than the ID of the new insertion
            if self.op.insert {
                if self.lesser_insert(e, m) {
                    // Insert before the first existing list element whose elemId is less than that
                    // of the new insertion
                    QueryResult::Finish
                } else {
                    self.pos += 1;
                    QueryResult::Next
                }
            } else if e.insert {
                // If the new op is an update of an existing list element, the first insertion op
                // we encounter after the reference element indicates the end of the reference elem
                QueryResult::Finish
            } else {
                // When updating an existing list element, keep track of any conflicts on this list
                // element. We also need to remember if the list element had any visible elements
                // prior to applying the new operation: if not, the new operation is resurrecting
                // a deleted list element, so it looks like an insertion in the patch.
                if e.visible() {
                    self.had_value_before = true;
                    if !overwritten {
                        self.values.push(e.clone());
                    }
                }

                // We now need to put the ops for the same list element into ascending order, so we
                // skip over any ops whose ID is less than that of the new operation.
                if !self.greater_opid(e, m) {
                    self.pos += 1;
                }
                QueryResult::Next
            }
        };

        // The patch needs to know the list index of each operation, so we count the number of
        // visible list elements up to the insertion position of the new operation
        if result == QueryResult::Next {
            self.count_visible(e);
        }
        result
    }
}
