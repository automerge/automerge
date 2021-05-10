use std::borrow::Cow;

use automerge_protocol as amp;

use crate::internal::InternalOpType;

/// The same as amp::Op except the `action` is an `InternalOpType`. This allows us to expand
/// collections of `amp::Op` into `ExpandedOp`s and remove optypes which perform multiple
/// operations (`amp::OpType::MultiSet` and `amp::OpType::Del`)
#[derive(Debug)]
pub struct ExpandedOp<'a> {
    pub(crate) action: InternalOpType,
    pub obj: Cow<'a, amp::ObjectId>,
    pub key: Cow<'a, amp::Key>,
    pub pred: Cow<'a, [amp::OpId]>,
    pub insert: bool,
}

/// An iterator which expands `amp::OpType::MultiSet` and `amp::OpType::Del` operations into
/// multiple `amp::InternalOpType`s
pub(super) struct ExpandedOpIterator<'a> {
    offset: usize,
    ops: &'a [amp::Op],
    expand_count: Option<usize>,
}

impl<'a> Iterator for ExpandedOpIterator<'a> {
    type Item = ExpandedOp<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.offset >= self.ops.len() {
            None
        } else {
            let op = &self.ops[self.offset];
            let action = match &op.action {
                amp::OpType::Set(v) => InternalOpType::Set(v.clone()),
                amp::OpType::Make(ot) => InternalOpType::Make(*ot),
                amp::OpType::Inc(i) => InternalOpType::Inc(*i),
                amp::OpType::Del(count) => {
                    if count.get() == 1 {
                        InternalOpType::Del
                    } else {
                        let index = if let Some(c) = self.expand_count {
                            if c == count.get() as usize - 1 {
                                // the last
                                self.offset += 1;
                                self.expand_count = None;
                                c
                            } else {
                                // somewhere in the middle
                                self.expand_count = Some(c + 1);
                                c + 1
                            }
                        } else {
                            // first one of the series
                            self.expand_count = Some(0);
                            0
                        };
                        let pred = op.pred[0].increment_by(index as u64);
                        let key = op.key.increment_by(index as u64).unwrap();
                        return Some(ExpandedOp {
                            action: InternalOpType::Del,
                            insert: op.insert,
                            pred: Cow::Owned(vec![pred]),
                            key: Cow::Owned(key),
                            obj: Cow::Borrowed(&op.obj),
                        });
                    }
                }
                amp::OpType::MultiSet(values) => {
                    let expanded_offset = match self.expand_count {
                        None => {
                            self.expand_count = Some(0);
                            0
                        }
                        Some(o) => o,
                    };
                    if let Some(v) = values.get(expanded_offset) {
                        self.expand_count = Some(expanded_offset + 1);
                        return Some(ExpandedOp {
                            action: InternalOpType::Set(v.clone()),
                            insert: op.insert,
                            pred: Cow::Borrowed(&op.pred),
                            key: Cow::Borrowed(&op.key),
                            obj: Cow::Borrowed(&op.obj),
                        });
                    } else {
                        self.offset += 1;
                        self.expand_count = None;
                        return self.next();
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
}

impl<'a> ExpandedOpIterator<'a> {
    pub(super) fn new(ops: &'a [amp::Op]) -> ExpandedOpIterator<'a> {
        ExpandedOpIterator {
            ops,
            offset: 0,
            expand_count: None,
        }
    }
}
