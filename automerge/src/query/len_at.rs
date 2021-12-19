#![allow(dead_code)]

use crate::op_tree::OpTreeNode;
use crate::query::{QueryResult, TreeQuery};
use crate::{Clock, ObjId};
use std::fmt::Debug;

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct LenAt<const B: usize> {
    obj: ObjId,
    clock: Clock,
    pub len: usize,
}

impl<const B: usize> LenAt<B> {
    pub fn new(obj: ObjId, clock: Clock) -> Self {
        LenAt { obj, clock, len: 0 }
    }
}

impl<const B: usize> TreeQuery<B> for LenAt<B> {
    fn query_node(&mut self, _child: &OpTreeNode<B>) -> QueryResult {
        unimplemented!();
    }
}
