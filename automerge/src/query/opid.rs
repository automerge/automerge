#![allow(dead_code)]

use crate::op_tree::OpTreeNode;
use crate::query::{CounterData, QueryResult, TreeQuery};
use crate::{Op, OpId};
use std::collections::HashMap;
use std::fmt::Debug;

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct OpIdQuery {
    target: OpId,
    index: usize,
    finish: bool,
    pub ops: Vec<Op>,
    counters: HashMap<OpId, CounterData>,
}

impl OpIdQuery {
    pub fn new(target: OpId) -> Self {
        OpIdQuery {
            target,
            index: 0,
            finish: false,
            ops: vec![],
            counters: HashMap::new(),
        }
    }
}

impl<const B: usize> TreeQuery<B> for OpIdQuery {
    fn query_node(&mut self, child: &OpTreeNode<B>) -> QueryResult {
        if child.index.ops.contains(&self.target) {
            QueryResult::Decend
        } else {
            self.index += child.len();
            QueryResult::Next
        }
    }

    fn query_element(&mut self, element: &Op) -> QueryResult {
        if element.id == self.target {
            self.finish = true;
            QueryResult::Finish
        } else {
            self.index += 1;
            QueryResult::Next
        }
    }
}
