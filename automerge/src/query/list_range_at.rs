use super::VisWindow;
use crate::exid::ExId;
use crate::op_tree::OpTreeNode;
use crate::types::{Clock, ElemId, OpId};
use crate::values::ValueIter;
use crate::{Automerge, Value};
use std::fmt::Debug;
use std::ops::RangeBounds;

#[derive(Debug)]
pub(crate) struct ListRangeAt<'a, R: RangeBounds<usize>> {
    range: R,
    index: usize,
    pos: usize,
    last_elemid: Option<ElemId>,
    next_result: Option<(usize, Value<'a>, OpId)>,
    index_back: usize,
    root_child: &'a OpTreeNode,
    clock: Clock,
    window: VisWindow,
}

impl<'a, R: RangeBounds<usize>> ValueIter<'a> for ListRangeAt<'a, R> {
    fn next_value(&mut self, doc: &'a Automerge) -> Option<(Value<'a>, ExId)> {
        self.next().map(|(_, val, id)| (val, doc.id_to_exid(id)))
    }
}

impl<'a, R: RangeBounds<usize>> ListRangeAt<'a, R> {
    pub(crate) fn new(range: R, clock: Clock, root_child: &'a OpTreeNode) -> Self {
        Self {
            range,
            index: 0, // FIXME root_child.seek_to_pos(range.start)
            pos: 0,   // FIXME range.start
            last_elemid: None,
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
        for i in self.index..self.index_back {
            let op = self.root_child.get(i)?;
            let visible = self.window.visible_at(op, i, &self.clock);
            self.index += 1;
            if visible {
                if op.elemid() != self.last_elemid {
                    self.last_elemid = op.elemid();
                    self.pos += 1;
                    if self.range.contains(&(self.pos - 1)) {
                        let result = self.next_result.replace((self.pos - 1, op.value(), op.id));
                        if result.is_some() {
                            return result;
                        }
                    }
                } else if self.pos > 0 && self.range.contains(&(self.pos - 1)) {
                    self.next_result = Some((self.pos - 1, op.value(), op.id));
                }
            }
        }
        self.next_result.take()
    }
}
