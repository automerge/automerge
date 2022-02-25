use crate::op_tree::OpTreeNode;
use crate::types::Key;
use std::fmt::Debug;

#[derive(Debug)]
pub(crate) struct IterKeys<'a, const B: usize> {
    index: usize,
    last_key: Option<Key>,
    index_back: usize,
    last_key_back: Option<Key>,
    root_child: &'a OpTreeNode<B>,
}

impl<'a, const B: usize> IterKeys<'a, B> {
    pub(crate) fn new(root_child: &'a OpTreeNode<B>) -> Self {
        Self {
            index: 0,
            last_key: None,
            index_back: root_child.len(),
            last_key_back: None,
            root_child,
        }
    }
}

impl<'a, const B: usize> Iterator for IterKeys<'a, B> {
    type Item = Key;

    fn next(&mut self) -> Option<Self::Item> {
        for i in self.index..self.index_back {
            let op = self.root_child.get(i)?;
            self.index += 1;
            if Some(op.key) != self.last_key && op.visible() {
                self.last_key = Some(op.key);
                return Some(op.key);
            }
        }
        None
    }
}

impl<'a, const B: usize> DoubleEndedIterator for IterKeys<'a, B> {
    fn next_back(&mut self) -> Option<Self::Item> {
        for i in (self.index..self.index_back).rev() {
            let op = self.root_child.get(i)?;
            self.index_back -= 1;
            if Some(op.key) != self.last_key_back && op.visible() {
                self.last_key_back = Some(op.key);
                return Some(op.key);
            }
        }
        None
    }
}
