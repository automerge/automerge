use crate::op_tree::OpTreeInternal;
use crate::types::Key;
use std::fmt::Debug;

#[derive(Debug)]
pub(crate) struct Keys<'a> {
    index: usize,
    last_key: Option<Key>,
    index_back: usize,
    last_key_back: Option<Key>,
    op_tree: &'a OpTreeInternal,
}

impl<'a> Keys<'a> {
    pub(crate) fn new(op_tree: &'a OpTreeInternal) -> Self {
        Self {
            index: 0,
            last_key: None,
            index_back: op_tree.len(),
            last_key_back: None,
            op_tree,
        }
    }
}

impl<'a> Iterator for Keys<'a> {
    type Item = Key;

    fn next(&mut self) -> Option<Self::Item> {
        for i in self.index..self.index_back {
            let op = self.op_tree.get(i)?;
            self.index += 1;
            if Some(op.elemid_or_key()) != self.last_key && op.visible() {
                self.last_key = Some(op.elemid_or_key());
                return Some(op.elemid_or_key());
            }
        }
        None
    }
}

impl<'a> DoubleEndedIterator for Keys<'a> {
    fn next_back(&mut self) -> Option<Self::Item> {
        for i in (self.index..self.index_back).rev() {
            let op = self.op_tree.get(i)?;
            self.index_back -= 1;
            if Some(op.elemid_or_key()) != self.last_key_back && op.visible() {
                self.last_key_back = Some(op.elemid_or_key());
                return Some(op.elemid_or_key());
            }
        }
        None
    }
}
