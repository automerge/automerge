use crate::exid::ExId;
use crate::op_set::OpSet;
use crate::op_tree::OpTreeInternal;
use crate::types::{ElemId, OpId};
use crate::values::ValueIter;
use crate::{Automerge, Value};
use std::fmt::Debug;
use std::ops::RangeBounds;

#[derive(Debug)]
pub(crate) struct ListRange<'a, R: RangeBounds<usize>> {
    range: R,
    index: usize,
    pos: usize,
    last_elemid: Option<ElemId>,
    next_result: Option<(usize, Value<'a>, OpId)>,
    index_back: usize,
    op_tree: &'a OpTreeInternal,
    opset: &'a OpSet,
}

impl<'a, R: RangeBounds<usize>> ListRange<'a, R> {
    pub(crate) fn new(range: R, op_tree: &'a OpTreeInternal, opset: &'a OpSet) -> Self {
        Self {
            range,
            index: 0, // FIXME root_child.seek_to_pos(range.start)
            pos: 0,   // FIXME range.start
            last_elemid: None,
            next_result: None,
            index_back: op_tree.len(),
            op_tree,
            opset,
        }
    }
}

impl<'a, R: RangeBounds<usize>> ValueIter<'a> for ListRange<'a, R> {
    fn next_value(&mut self, doc: &'a Automerge) -> Option<(Value<'a>, ExId)> {
        self.next().map(|(_, val, id)| (val, doc.id_to_exid(id)))
    }
}

impl<'a, R: RangeBounds<usize>> Iterator for ListRange<'a, R> {
    type Item = (usize, Value<'a>, OpId);

    // FIXME: this is fine if we're scanning everything (see values()) but could be much more efficient
    // if we're scanning a narrow range on a large sequence ... we should be able to seek to the starting
    // point and stop at the end point and not needless scan all the ops before and after the range
    fn next(&mut self) -> Option<Self::Item> {
        for i in self.index..self.index_back {
            let op = self.op_tree.get(i)?;
            self.index += 1;
            if op.visible() {
                if op.elemid() != self.last_elemid {
                    self.last_elemid = op.elemid();
                    self.pos += 1;
                    if self.range.contains(&(self.pos - 1)) {
                        let value = if op.is_move() {
                            Value::Object(self.opset.object_type(&op.get_move_source()).unwrap())
                        } else {
                            op.value()
                        };
                        let result = self.next_result.replace((self.pos - 1, value, op.id));
                        if result.is_some() {
                            return result;
                        }
                    }
                } else if self.pos > 0 && self.range.contains(&(self.pos - 1)) {
                    let value = if op.is_move() {
                        Value::Object(self.opset.object_type(&op.get_move_source()).unwrap())
                    } else {
                        op.value()
                    };
                    self.next_result = Some((self.pos - 1, value, op.id));
                }
            }
        }
        self.next_result.take()
    }
}
