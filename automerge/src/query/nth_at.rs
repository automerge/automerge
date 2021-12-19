#![allow(dead_code)]
#![allow(unused_imports)]

use crate::op_tree::OpTreeNode;
use crate::query::{is_visible, visible_op, CounterData, QueryResult, TreeQuery};
use crate::{AutomergeError, Clock, ElemId, Key, ObjId, Op, OpId};
use std::collections::HashMap;
use std::fmt::Debug;

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct NthAt<const B: usize> {
    obj: ObjId,
    clock: Clock,
    target: usize,
    seen: usize,
    last_seen: Option<ElemId>,
    last_elem: Option<ElemId>,
    counters: HashMap<OpId, CounterData>,
    pub ops: Vec<Op>,
    pub ops_pos: Vec<usize>,
    pub pos: usize,
}

impl<const B: usize> NthAt<B> {
    pub fn new(obj: ObjId, target: usize, clock: Clock) -> Self {
        NthAt {
            obj,
            clock,
            target,
            seen: 0,
            last_seen: None,
            ops: vec![],
            ops_pos: vec![],
            pos: 0,
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

impl<const B: usize> TreeQuery<B> for NthAt<B> {
    fn query_node(&mut self, _child: &OpTreeNode<B>) -> QueryResult {
        unimplemented!();
    }

    fn query_element(&mut self, _element: &Op) -> QueryResult {
        unimplemented!();
    }
}
