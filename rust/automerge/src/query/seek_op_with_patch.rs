use crate::op_tree::{OpSetMetadata, OpTreeNode};
use crate::query::{binary_search_by, QueryResult, TreeQuery};
use crate::types::{Key, ListEncoding, Op, HEAD};
use std::cmp::Ordering;
use std::fmt::Debug;

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct SeekOpWithPatch<'a> {
    op: Op,
    pub(crate) pos: usize,
    pub(crate) succ: Vec<usize>,
    found: bool,
    encoding: ListEncoding,
    pub(crate) seen: usize,
    pub(crate) last_width: usize,
    last_seen: Option<Key>,
    pub(crate) values: Vec<&'a Op>,
    pub(crate) had_value_before: bool,
}

impl<'a> SeekOpWithPatch<'a> {
    pub(crate) fn new(op: &Op, encoding: ListEncoding) -> Self {
        SeekOpWithPatch {
            op: op.clone(),
            succ: vec![],
            pos: 0,
            found: false,
            encoding,
            seen: 0,
            last_width: 0,
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
            self.seen += e.width(self.encoding);
            self.last_seen = Some(e.elemid_or_key())
        }
    }
}

impl<'a> TreeQuery<'a> for SeekOpWithPatch<'a> {
    fn query_node_with_metadata(
        &mut self,
        child: &'a OpTreeNode,
        m: &OpSetMetadata,
        ops: &[Op],
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
                    let op = &ops[child.get(self.pos).unwrap()];
                    if op.insert && m.lamport_cmp(op.id, self.op.id) == Ordering::Less {
                        break;
                    }
                    self.count_visible(op);
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

                    let mut num_vis = child.index.visible_len(self.encoding);
                    self.seen += num_vis;

                    let last_elemid = ops[child.last()].elemid_or_key();
                    if child.index.has_visible(&last_elemid) {
                        self.last_seen = Some(last_elemid);
                    }
                    QueryResult::Next
                }
            }

            // Updating a map: operations appear in sorted order by key
            Key::Map(_) => {
                let start = binary_search_by(child, ops, |op| m.key_cmp(&op.key, &self.op.key));
                self.pos = start;
                QueryResult::Skip(start)
            }
        }
    }

    // Only called when operating on a sequence (list/text) object, since updates of a map are
    // handled in `query_node_with_metadata`.
    fn query_element_with_metadata(&mut self, e: &'a Op, m: &OpSetMetadata) -> QueryResult {
        match self.op.key {
            Key::Map(_) => {
                if !self.found {
                    // Iterate over any existing operations for the same key; stop when we reach an
                    // operation with a different key
                    if e.key != self.op.key {
                        return QueryResult::Finish;
                    }

                    // Keep track of any ops we're overwriting and any conflicts on this key
                    if self.op.overwrites(e) {
                        // when we encounter an increment op we also want to find the counter for
                        // it.
                        if self.op.is_inc() && e.is_counter() && e.visible() {
                            self.values.push(e);
                        }
                        self.succ.push(self.pos);
                        self.last_width = e.width(self.encoding);

                        if e.visible() {
                            self.had_value_before = true;
                        }
                    } else if e.visible() {
                        self.values.push(e);
                    }

                    // Ops for the same key should be in ascending order of opId, so we break when
                    // we reach an op with an opId greater than that of the new operation
                    if m.lamport_cmp(e.id, self.op.id) == Ordering::Greater {
                        self.found = true;
                        return QueryResult::Next;
                    }

                    self.pos += 1;
                } else {
                    // For the purpose of reporting conflicts, we also need to take into account any
                    // ops for the same key that appear after the new operation

                    if e.key != self.op.key {
                        return QueryResult::Finish;
                    }
                    // No need to check if `self.op.overwrites(op)` because an operation's `preds`
                    // must always have lower Lamport timestamps than that op itself, and the ops
                    // here all have greater opIds than the new op
                    if e.visible() {
                        self.values.push(e);
                    }
                }
                QueryResult::Next
            }
            Key::Seq(_) => {
                let result = if !self.found {
                    // First search for the referenced list element (i.e. the element we're updating, or
                    // after which we're inserting)
                    if self.is_target_insert(e) {
                        self.found = true;
                        if self.op.overwrites(e) {
                            // when we encounter an increment op we also want to find the counter for
                            // it.
                            if self.op.is_inc() && e.is_counter() && e.visible() {
                                self.values.push(e);
                            }
                            self.succ.push(self.pos);
                            self.last_width = e.width(self.encoding);
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
                        // when we encounter an increment op we also want to find the counter for
                        // it.
                        if self.op.is_inc() && e.is_counter() && e.visible() {
                            self.values.push(e);
                        }
                        self.succ.push(self.pos);
                        self.last_width = e.width(self.encoding);
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
                                self.values.push(e);
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
    }
}

#[cfg(test)]
mod tests {
    use super::{super::seek_op::tests::optree_with_only_internally_visible_ops, SeekOpWithPatch};
    use crate::{
        op_tree::B,
        types::{ListEncoding, ObjId},
    };

    #[test]
    fn test_insert_on_internal_only_nodes() {
        let (set, new_op) = optree_with_only_internally_visible_ops();

        let q = SeekOpWithPatch::new(&new_op, ListEncoding::List);
        let q = set.search(&ObjId::root(), q);

        // we've inserted `B - 1` elements for "a", so the index should be `B`
        assert_eq!(q.pos, B);
    }
}
