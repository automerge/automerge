use super::VisWindow;
use crate::op_tree::OpTreeNode;
use crate::types::{Clock, Key, OpId};
use crate::Value;
use std::fmt::Debug;
use std::ops::RangeBounds;

#[derive(Debug)]
pub(crate) struct ListRangeAt<'a, R: RangeBounds<usize>> {
    range: R,
    index: usize,
    pos: usize,
    last_key: Option<Key>,
    next_result: Option<(usize, Value<'a>, OpId)>,
    index_back: usize,
    root_child: &'a OpTreeNode,
    clock: Clock,
    window: VisWindow,
}

impl<'a, R: RangeBounds<usize>> ListRangeAt<'a, R> {
    pub(crate) fn new(range: R, clock: Clock, root_child: &'a OpTreeNode) -> Self {
        Self {
            range,
            index: 0, // FIXME root_child.seek_to_pos(range.start)
            pos: 0,   // FIXME range.start
            last_key: None,
            next_result: None,
            index_back: root_child.len(),
            root_child,
            clock,
            window: VisWindow::default(),
        }
    }
}

impl<'a, R: RangeBounds<usize>> Iterator for ListRangeAt<'a, R> {
    type Item = (usize, Value<'a>, OpId);

    fn next(&mut self) -> Option<Self::Item> {
        // FIXME if self.pos > range.end { return None }
        let mut result = None;
        for i in self.index..self.index_back {
            let op = self.root_child.get(i)?;
            let visible = self.window.visible_at(op, i, &self.clock);
            self.index += 1;
            if visible {
                if self.range.contains(&self.pos) {
                    result = self.next_result.replace((self.pos, op.value(), op.id));
                }
                if Some(op.key) != self.last_key {
                    self.last_key = Some(op.key);
                    self.pos += 1;
                    if result.is_some() {
                        return result;
                    }
                }
            }
        }
        self.next_result.take()
    }
}
