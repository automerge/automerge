use crate::op_tree::OpTreeNode;
use crate::query::{binary_search_by, QueryResult, TreeQuery};
use crate::{ElemId, Key, Op};
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

    fn lesser_insert(&self, op: &Op) -> bool {
        op.insert && op.id.cmp(&self.op.id) == Ordering::Less
    }

    fn greater_opid(&self, op: &Op) -> bool {
        op.id.cmp(&self.op.id) == Ordering::Greater
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
    fn query_node(&mut self, child: &OpTreeNode<B>) -> QueryResult {
        if self.found {
            return QueryResult::Decend;
        }
        match &self.op.key {
            Key::Seq(ElemId::Head) => {
                while self.pos < child.len() {
                    let op = child.get(self.pos).unwrap();
                    if self.op.overwrites(op) {
                        self.succ.push(self.pos);
                    }
                    if op.insert && op.id.cmp(&self.op.id) == Ordering::Less {
                        break;
                    }
                    self.pos += 1;
                }
                QueryResult::Finish
            }
            Key::Seq(ElemId::Id(id)) => {
                if self.found || child.index.ops.contains(id) {
                    QueryResult::Decend
                } else {
                    self.pos += child.len();
                    QueryResult::Next
                }
            }
            Key::Map(_) => {
                self.pos = binary_search_by(child, |op| op.key.cmp(&self.op.key));
                while self.pos < child.len() {
                    let op = child.get(self.pos).unwrap();
                    if op.key != self.op.key {
                        break;
                    }
                    if self.op.overwrites(op) {
                        self.succ.push(self.pos);
                    }
                    if op.id.cmp(&self.op.id) == Ordering::Greater {
                        break;
                    }
                    self.pos += 1;
                }
                QueryResult::Finish
            }
        }
    }

    fn query_element(&mut self, e: &Op) -> QueryResult {
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
                if self.different_obj(e) || self.lesser_insert(e) {
                    QueryResult::Finish
                } else {
                    self.pos += 1;
                    QueryResult::Next
                }
            } else if e.insert || self.different_obj(e) || self.greater_opid(e) {
                QueryResult::Finish
            } else {
                self.pos += 1;
                QueryResult::Next
            }
        }
    }
}
