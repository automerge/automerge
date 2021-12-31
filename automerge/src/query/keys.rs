use crate::op_tree::OpTreeNode;
use crate::query::{QueryResult, TreeQuery, VisWindow};
use crate::types::Key;
use std::fmt::Debug;

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct Keys<const B: usize> {
    pub keys: Vec<Key>,
    window: VisWindow,
}

impl<const B: usize> Keys<B> {
    pub fn new() -> Self {
        Keys {
            keys: vec![],
            window: Default::default(),
        }
    }
}

impl<const B: usize> TreeQuery<B> for Keys<B> {
    fn query_node(&mut self, child: &OpTreeNode<B>) -> QueryResult {
        let mut last = None;
        for i in 0..child.len() {
            let op = child.get(i).unwrap();
            let visible = self.window.visible(op, i);
            if Some(op.key) != last && visible {
                self.keys.push(op.key);
                last = Some(op.key);
            }
        }
        QueryResult::Finish
    }
}
