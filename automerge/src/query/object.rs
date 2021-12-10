#![allow(dead_code)]

use crate::op_tree::OpTreeNode;
use crate::query::{is_visible, visible_op, CounterData, QueryResult, TreeQuery};
use crate::{AutomergeError, ElemId, Key, ObjId, Op, OpId};
use std::collections::HashMap;
use std::fmt::Debug;

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct Object<const B: usize> {
    obj: ObjId,
    target: usize,
    index: usize,
    seen: usize,
    pub last_insert_pos: usize,
    pub last_elem: Option<ElemId>,
    pub ops: Vec<Op>,
    pub ops_pos: Vec<usize>,
    pub pos: usize,
    last_seen: Option<ElemId>,
    counters: HashMap<OpId, CounterData>,
}

impl<const B: usize> Object<B> {
    pub fn new(obj: ObjId) -> Self {
        Object {
            obj,
            target: 0,
            index: 0,
            seen: 0,
            last_seen: None,
            ops: vec![],
            ops_pos: vec![],
            pos: 0,
            last_insert_pos: 0,
            last_elem: None,
            counters: HashMap::new(),
        }
    }

    pub fn done(&self) -> bool {
        self.seen > self.target
    }

    pub fn key(&self) -> Result<Key, AutomergeError> {
        if let Some(e) = self.last_elem {
            Ok(Key::Seq(e))
        } else {
            Err(AutomergeError::InvalidIndex(self.target))
        }
    }
}

impl<const B: usize> TreeQuery<B> for Object<B> {
    fn query_node(&mut self, child: &OpTreeNode<B>) -> QueryResult {
        if let Some(mut num_vis) = child.index.lens.get(&self.obj).copied() {
            // num vis is the number of keys in the index
            // minus one if we're counting last_seen
            // let mut num_vis = s.keys().count();
            if child.index.has(&self.obj, &self.last_seen) {
                num_vis -= 1;
            }
            if self.seen + num_vis > self.target {
                QueryResult::Decend
            } else {
                self.index += child.len();
                self.pos += child.len();
                self.seen += num_vis;
                self.last_seen = child.last().elemid();
                QueryResult::Next
            }
        } else {
            self.index += child.len();
            self.pos += child.len();
            QueryResult::Next
        }
    }

    fn query_element(&mut self, element: &Op) -> QueryResult {
        if element.obj != self.obj {
            if self.seen > self.target {
                return QueryResult::Finish;
            }
        } else {
            if element.insert {
                if self.seen > self.target {
                    return QueryResult::Finish;
                };
                self.last_elem = element.elemid();
                self.last_insert_pos = self.pos;
                self.last_seen = None
            }
            let visible = is_visible(element, self.pos, &mut self.counters);
            if visible && self.last_seen.is_none() {
                self.seen += 1;
                self.last_seen = element.elemid()
            }
            if self.seen == self.target + 1 && visible {
                let (vpos, vop) = visible_op(element, self.pos, &self.counters);
                self.ops.push(vop);
                self.ops_pos.push(vpos);
            }
        }
        self.pos += 1;
        QueryResult::Next
    }
}
