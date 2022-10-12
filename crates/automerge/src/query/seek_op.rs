use crate::op_tree::{OpSetMetadata, OpTreeNode};
use crate::query::{binary_search_by, QueryResult, TreeQuery};
use crate::types::{Key, Op, HEAD};
use std::cmp::Ordering;
use std::fmt::Debug;

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct SeekOp<'a> {
    /// the op we are looking for
    op: &'a Op,
    /// The position to insert at
    pub(crate) pos: usize,
    /// The indices of ops that this op overwrites
    pub(crate) succ: Vec<usize>,
    /// whether a position has been found
    found: bool,
    /// The found start position of the key if there is one yet (for map objects).
    start: Option<usize>,
}

impl<'a> SeekOp<'a> {
    pub(crate) fn new(op: &'a Op) -> Self {
        SeekOp {
            op,
            succ: vec![],
            pos: 0,
            found: false,
            start: None,
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
}

impl<'a> TreeQuery<'a> for SeekOp<'a> {
    fn query_node_with_metadata(&mut self, child: &OpTreeNode, m: &OpSetMetadata) -> QueryResult {
        if self.found {
            return QueryResult::Descend;
        }
        match self.op.key {
            Key::Seq(HEAD) => {
                while self.pos < child.len() {
                    let op = child.get(self.pos).unwrap();
                    if op.insert && m.lamport_cmp(op.id, self.op.id) == Ordering::Less {
                        break;
                    }
                    self.pos += 1;
                }
                QueryResult::Finish
            }
            Key::Seq(e) => {
                if child.index.ops.contains(&e.0) {
                    QueryResult::Descend
                } else {
                    self.pos += child.len();
                    QueryResult::Next
                }
            }
            Key::Map(_) => {
                if let Some(start) = self.start {
                    if self.pos + child.len() >= start {
                        // skip empty nodes
                        if child.index.visible_len() == 0 {
                            self.pos += child.len();
                            QueryResult::Next
                        } else {
                            QueryResult::Descend
                        }
                    } else {
                        self.pos += child.len();
                        QueryResult::Next
                    }
                } else {
                    // in the root node find the first op position for the key
                    let start = binary_search_by(child, |op| m.key_cmp(&op.key, &self.op.key));
                    self.start = Some(start);
                    self.pos = start;
                    QueryResult::Skip(start)
                }
            }
        }
    }

    fn query_element_with_metadata(&mut self, e: &Op, m: &OpSetMetadata) -> QueryResult {
        match self.op.key {
            Key::Map(_) => {
                // don't bother looking at things past our key
                if e.key != self.op.key {
                    return QueryResult::Finish;
                }

                if self.op.overwrites(e) {
                    self.succ.push(self.pos);
                }

                if m.lamport_cmp(e.id, self.op.id) == Ordering::Greater {
                    return QueryResult::Finish;
                }

                self.pos += 1;
                QueryResult::Next
            }
            Key::Seq(_) => {
                if !self.found {
                    if self.is_target_insert(e) {
                        self.found = true;
                        if self.op.overwrites(e) {
                            self.succ.push(self.pos);
                        }
                    }
                    self.pos += 1;
                    QueryResult::Next
                } else {
                    // we have already found the target
                    if self.op.overwrites(e) {
                        self.succ.push(self.pos);
                    }
                    if self.op.insert {
                        if self.lesser_insert(e, m) {
                            QueryResult::Finish
                        } else {
                            self.pos += 1;
                            QueryResult::Next
                        }
                    } else if e.insert || self.greater_opid(e, m) {
                        QueryResult::Finish
                    } else {
                        self.pos += 1;
                        QueryResult::Next
                    }
                }
            }
        }
    }
}
