#![allow(dead_code)]

use crate::op_tree::{OpSetMetadata, OpTreeNode};
use crate::query::{binary_search_by, QueryResult, TreeQuery};
use crate::{Key, Op, HEAD};
use std::cmp::Ordering;
use std::fmt::Debug;

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct SeekOp<const B: usize> {
    op: Op,
    pub pos: usize,
    pub succ: Vec<usize>,
    found: bool,
}

impl<const B: usize> SeekOp<B> {
    pub fn new(op: &Op) -> Self {
        SeekOp {
            op: op.clone(),
            succ: vec![],
            pos: 0,
            found: false,
        }
    }

    fn different_obj(&self, op: &Op) -> bool {
        op.obj != self.op.obj
    }

    fn lesser_insert(&self, op: &Op, m: &OpSetMetadata) -> bool {
        op.insert && m.lamport_cmp(op.id, self.op.id) == Ordering::Less
    }

    fn greater_opid(&self, op: &Op, m: &OpSetMetadata) -> bool {
        m.lamport_cmp(op.id, self.op.id) == Ordering::Greater
    }

    fn is_target_insert(&self, op: &Op) -> bool {
        if !op.insert {
            return false;
        }
        if self.op.insert {
            op.elemid() == self.op.key.elemid()
        } else {
            op.elemid() == self.op.elemid()
        }
    }
}

impl<const B: usize> TreeQuery<B> for SeekOp<B> {
    fn query_node_with_metadata(
        &mut self,
        child: &OpTreeNode<B>,
        m: &OpSetMetadata,
    ) -> QueryResult {
        if self.found {
            return QueryResult::Decend;
        }
        match self.op.key {
            Key::Seq(e) if e == HEAD => {
                self.pos = binary_search_by(child, |op| {
                    m.lamport_cmp(op.obj.0, self.op.obj.0)
                    //.then_with(|| m.key_cmp(&op.key, &self.op.key))
                    //.then_with(|| m.lamport_cmp(op.id, self.op.id))
                });
                while self.pos < child.len() {
                    let op = child.get(self.pos).unwrap();
                    if op.obj != self.op.obj {
                        break;
                    }
                    if self.op.overwrites(op) {
                        self.succ.push(self.pos);
                    }
                    if op.insert && m.lamport_cmp(op.id, self.op.id) == Ordering::Less {
                        break;
                    }
                    self.pos += 1;
                }
                QueryResult::Finish
            }
            Key::Seq(e) => {
                if self.found || child.index.ops.contains(&e.0) {
                    QueryResult::Decend
                } else {
                    self.pos += child.len();
                    QueryResult::Next
                }
            }
            Key::Map(_) => {
                self.pos = binary_search_by(child, |op| {
                    m.lamport_cmp(op.obj.0, self.op.obj.0)
                        .then_with(|| m.key_cmp(&op.key, &self.op.key))
                    //.then_with(|| m.lamport_cmp(op.id, self.op.id))
                });
                while self.pos < child.len() {
                    let op = child.get(self.pos).unwrap();
                    if op.obj != self.op.obj {
                        break;
                    }
                    if op.key != self.op.key {
                        break;
                    }
                    if self.op.overwrites(op) {
                        self.succ.push(self.pos);
                    }
                    if m.lamport_cmp(op.id, self.op.id) == Ordering::Greater {
                        break;
                    }
                    self.pos += 1;
                }
                QueryResult::Finish
            }
        }
    }

    fn query_element_with_metadata(&mut self, e: &Op, m: &OpSetMetadata) -> QueryResult {
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
            if self.op.overwrites(e) {
                self.succ.push(self.pos);
            }
            if self.op.insert {
                if self.different_obj(e) || self.lesser_insert(e, m) {
                    QueryResult::Finish
                } else {
                    self.pos += 1;
                    QueryResult::Next
                }
            } else if e.insert || self.different_obj(e) || self.greater_opid(e, m) {
                QueryResult::Finish
            } else {
                self.pos += 1;
                QueryResult::Next
            }
        }
    }
}
