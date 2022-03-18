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
            Key::Seq(e) if e == HEAD => {
                while self.pos < child.len() {
                    let op = child.get(self.pos).unwrap();
                    if self.op.overwrites(op) {
                        self.succ.push(self.pos);
                    }
                    if op.insert && m.lamport_cmp(op.id, self.op.id) == Ordering::Less {
                        break;
                    }
                    self.count_visible(&op);
                    self.pos += 1;
                }
                QueryResult::Finish
            }

            Key::Seq(e) => {
                if self.found || child.index.ops.contains(&e.0) {
                    QueryResult::Descend
                } else {
                    self.pos += child.len();

                    let mut num_vis = child.index.len;
                    if num_vis > 0 {
                        // num vis is the number of keys in the index
                        // minus one if we're counting last_seen
                        // let mut num_vis = s.keys().count();
                        if child.index.has(&self.last_seen) {
                            num_vis -= 1;
                        }
                        self.seen += num_vis;
                        self.last_seen = child.last().elemid();
                    }
                    QueryResult::Next
                }
            }

            Key::Map(_) => {
                self.pos = binary_search_by(child, |op| m.key_cmp(&op.key, &self.op.key));
                while self.pos < child.len() {
                    let op = child.get(self.pos).unwrap();
                    if op.key != self.op.key {
                        break;
                    }
                    if self.op.overwrites(op) {
                        self.succ.push(self.pos);
                    } else if op.visible() {
                        self.values.push(op.clone());
                    }
                    if m.lamport_cmp(op.id, self.op.id) == Ordering::Greater {
                        break;
                    }
                    self.pos += 1;
                }

                let mut later_pos = self.pos;
                while later_pos < child.len() {
                    let op = child.get(later_pos).unwrap();
                    if op.key != self.op.key {
                        break;
                    }
                    if op.visible() {
                        self.values.push(op.clone());
                    }
                    later_pos += 1;
                }
                QueryResult::Finish
            }
        }
    }

    fn query_element_with_metadata(&mut self, e: &Op, m: &OpSetMetadata) -> QueryResult {
        let result = if !self.found {
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
                if self.lesser_insert(e, m) {
                    QueryResult::Finish
                } else {
                    self.pos += 1;
                    QueryResult::Next
                }
            } else if e.insert {
                QueryResult::Finish
            } else if self.greater_opid(e, m) {
                if e.visible() {
                    self.values.push(e.clone());
                }
                QueryResult::Next
            } else {
                self.pos += 1;
                QueryResult::Next
            }
        };

        if result == QueryResult::Next {
            self.count_visible(e);
        }
        result
    }
}
