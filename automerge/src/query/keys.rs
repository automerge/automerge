use crate::op_tree::OpTreeNode;
use crate::query::{QueryResult, TreeQuery};
use crate::types::Key;
use std::fmt::Debug;

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct Keys<const B: usize> {
    pub keys: Vec<Key>,
}

impl<const B: usize> Keys<B> {
    pub fn new() -> Self {
        Keys { keys: vec![] }
    }
}

impl<const B: usize> TreeQuery<B> for Keys<B> {
    fn query_node(&mut self, child: &OpTreeNode<B>) -> QueryResult {
        let mut last = None;
        for i in 0..child.len() {
            let op = child.get(i).unwrap();
            if Some(op.key) != last && op.visible() {
                self.keys.push(op.key);
                last = Some(op.key);
            }
        }
        QueryResult::Finish
    }
}
