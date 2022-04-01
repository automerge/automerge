use crate::op_tree::OpTreeNode;
use crate::query::VisWindow;
use crate::types::{Clock, Key};
use std::fmt::Debug;

#[derive(Debug)]
pub(crate) struct KeysAt<'a> {
    clock: Clock,
    window: VisWindow,
    index: usize,
    last_key: Option<Key>,
    index_back: usize,
    last_key_back: Option<Key>,
    root_child: &'a OpTreeNode,
}

impl<'a> KeysAt<'a> {
    pub(crate) fn new(root_child: &'a OpTreeNode, clock: Clock) -> Self {
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

impl<'a> Iterator for KeysAt<'a> {
    type Item = Key;

    fn next(&mut self) -> Option<Self::Item> {
        for i in self.index..self.root_child.len() {
            let op = self.root_child.get(i)?;
            let visible = self.window.visible_at(op, i, &self.clock);
            self.index += 1;
            if Some(op.elemid_or_key()) != self.last_key && visible {
                self.last_key = Some(op.elemid_or_key());
                return Some(op.elemid_or_key());
            }
        }
        None
    }
}

impl<'a> DoubleEndedIterator for KeysAt<'a> {
    fn next_back(&mut self) -> Option<Self::Item> {
        for i in self.index..self.index_back {
            let op = self.root_child.get(i)?;
            let visible = self.window.visible_at(op, i, &self.clock);
            self.index_back -= 1;
            if Some(op.elemid_or_key()) != self.last_key_back && visible {
                self.last_key_back = Some(op.elemid_or_key());
                return Some(op.elemid_or_key());
            }
        }
        None
    }
}
