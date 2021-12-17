use std::borrow::Cow;

use crate::legacy as amp;
use amp::SortedVec;

use crate::internal::InternalOpType;

/// The same as amp::Op except the `action` is an `InternalOpType`. This allows us to expand
/// collections of `amp::Op` into `ExpandedOp`s and remove optypes which perform multiple
/// operations (`amp::OpType::MultiSet` and `amp::OpType::Del`)
#[derive(Debug, PartialEq)]
pub struct ExpandedOp<'a> {
    pub(crate) action: InternalOpType,
    pub obj: Cow<'a, amp::ObjectId>,
    pub key: Cow<'a, amp::Key>,
    pub pred: Cow<'a, SortedVec<amp::OpId>>,
    pub insert: bool,
}

/// An iterator which expands `amp::OpType::MultiSet` and `amp::OpType::Del` operations into
/// multiple `amp::InternalOpType`s
pub(super) struct ExpandedOpIterator<'a> {
    offset: usize,
    ops: &'a [amp::Op],
    expand_count: Option<usize>,
    op_num: u64,
}

impl<'a> Iterator for ExpandedOpIterator<'a> {
    type Item = ExpandedOp<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.offset >= self.ops.len() {
            None
        } else {
            self.op_num += 1;
            let op = &self.ops[self.offset];
            let action = match &op.action {
                amp::OpType::Set(v) => InternalOpType::Set(v.clone()),
                amp::OpType::Make(ot) => InternalOpType::Make(*ot),
                amp::OpType::Inc(i) => InternalOpType::Inc(*i),
                amp::OpType::Del(count) => {
                    if count.get() == 1 {
                        InternalOpType::Del
                    } else {
                        assert_eq!(
                            op.pred.len(),
                            1,
                            "multiOp deletion must have exactly one pred"
                        );
                        let index = if let Some(c) = self.expand_count {
                            if c == count.get() as usize - 1 {
                                // the last
                                self.offset += 1;
                                self.expand_count = None;
                            } else {
                                // somewhere in the middle
                                self.expand_count = Some(c + 1);
                            }
                            c
                        } else {
                            // first one of the series
                            self.expand_count = Some(1);
                            0
                        };
                        let pred = op.pred.get(0).unwrap().increment_by(index as u64);
                        let key = op.key.increment_by(index as u64).unwrap();
                        return Some(ExpandedOp {
                            action: InternalOpType::Del,
                            insert: op.insert,
                            pred: Cow::Owned(vec![pred].into()),
                            key: Cow::Owned(key),
                            obj: Cow::Borrowed(&op.obj),
                        });
                    }
                }
            };
            self.offset += 1;
            Some(ExpandedOp {
                action,
                insert: op.insert,
                pred: Cow::Borrowed(&op.pred),
                key: Cow::Borrowed(&op.key),
                obj: Cow::Borrowed(&op.obj),
            })
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        (self.ops.len() - self.offset, None)
    }
}

impl<'a> ExpandedOpIterator<'a> {
    pub(super) fn new(ops: &'a [amp::Op], start_op: u64) -> ExpandedOpIterator<'a> {
        ExpandedOpIterator {
            ops,
            offset: 0,
            expand_count: None,
            op_num: start_op - 1,
        }
    }
}
