use crate::op_tree::OpTreeNode;
use crate::query::VisWindow;
use crate::types::{Clock, Key};
use std::fmt::Debug;

#[derive(Debug)]
pub(crate) struct IterKeysAt<'a, const B: usize> {
    clock: Clock,
    window: VisWindow,
    index: usize,
    last_key: Option<Key>,
    index_back: usize,
    last_key_back: Option<Key>,
    root_child: &'a OpTreeNode<B>,
}

impl<'a, const B: usize> IterKeysAt<'a, B> {
    pub(crate) fn new(root_child: &'a OpTreeNode<B>, clock: Clock) -> Self {
        Self {
            clock,
            window: VisWindow::default(),
            index: 0,
            last_key: None,
            index_back: root_child.len(),
            last_key_back: None,
            root_child,
        }
    }
}

impl<'a, const B: usize> Iterator for IterKeysAt<'a, B> {
    type Item = Key;

    fn next(&mut self) -> Option<Self::Item> {
        for i in self.index..self.root_child.len() {
            let op = self.root_child.get(i)?;
            let visible = self.window.visible_at(op, i, &self.clock);
            self.index += 1;
            if Some(op.key) != self.last_key && visible {
                self.last_key = Some(op.key);
                return Some(op.key);
            }
        }
        None
    }
}

impl<'a, const B: usize> DoubleEndedIterator for IterKeysAt<'a, B> {
    fn next_back(&mut self) -> Option<Self::Item> {
        for i in self.index..self.index_back {
            let op = self.root_child.get(i)?;
            let visible = self.window.visible_at(op, i, &self.clock);
            self.index_back -= 1;
            if Some(op.key) != self.last_key_back && visible {
                self.last_key_back = Some(op.key);
                return Some(op.key);
            }
        }
        None
    }
}
