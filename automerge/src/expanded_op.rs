use std::borrow::Cow;

use crate::legacy as amp;
use amp::{ActorId, ElementId, Key, OpId, SortedVec};

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
    actor: ActorId,
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
                amp::OpType::MultiSet(values) => {
                    assert!(op.pred.is_empty(), "multi-insert pred must be empty");
                    let expanded_offset = match self.expand_count {
                        None => {
                            self.expand_count = Some(0);
                            0
                        }
                        Some(o) => o,
                    };

                    let key = if expanded_offset == 0 {
                        Cow::Borrowed(&op.key)
                    } else {
                        Cow::Owned(Key::Seq(ElementId::Id(OpId(
                            self.op_num - 1,
                            self.actor.clone(),
                        ))))
                    };

                    if expanded_offset == values.len() - 1 {
                        self.offset += 1;
                        self.expand_count = None;
                    } else {
                        self.expand_count = Some(expanded_offset + 1);
                    }

                    let v = values.get(expanded_offset).unwrap();
                    return Some(ExpandedOp {
                        action: InternalOpType::Set(v.clone()),
                        insert: op.insert,
                        pred: Cow::Borrowed(&op.pred),
                        key,
                        obj: Cow::Borrowed(&op.obj),
                    });
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
    pub(super) fn new(ops: &'a [amp::Op], start_op: u64, actor: ActorId) -> ExpandedOpIterator<'a> {
        ExpandedOpIterator {
            ops,
            offset: 0,
            expand_count: None,
            op_num: start_op - 1,
            actor,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{convert::TryInto, num::NonZeroU32, str::FromStr};

    use amp::{ObjectId, Op, OpType, ScalarValue, SortedVec};
    use pretty_assertions::assert_eq;

    use super::*;

    #[test]
    fn expand_multi_set() {
        let actor = ActorId::from_str("7f12a4d3567c4257af34f216aa16fe48").unwrap();
        let ops = [Op {
            action: OpType::MultiSet(
                vec![
                    ScalarValue::Uint(1),
                    ScalarValue::Uint(2),
                    ScalarValue::Uint(3),
                ]
                .try_into()
                .unwrap(),
            ),
            obj: ObjectId::Id(OpId(1, actor.clone())),
            key: Key::Seq(ElementId::Head),
            pred: SortedVec::new(),
            insert: true,
        }];
        let expanded_ops = ExpandedOpIterator::new(&ops, 2, actor.clone()).collect::<Vec<_>>();
        assert_eq!(
            expanded_ops,
            vec![
                ExpandedOp {
                    action: InternalOpType::Set(ScalarValue::Uint(1)),
                    obj: Cow::Owned(ObjectId::Id(OpId(1, actor.clone()))),
                    key: Cow::Owned(Key::Seq(ElementId::Head)),
                    pred: Cow::Owned(SortedVec::new()),
                    insert: true
                },
                ExpandedOp {
                    action: InternalOpType::Set(ScalarValue::Uint(2)),
                    obj: Cow::Owned(ObjectId::Id(OpId(1, actor.clone()))),
                    key: Cow::Owned(Key::Seq(ElementId::Id(OpId(2, actor.clone())))),
                    pred: Cow::Owned(SortedVec::new()),
                    insert: true
                },
                ExpandedOp {
                    action: InternalOpType::Set(ScalarValue::Uint(3)),
                    obj: Cow::Owned(ObjectId::Id(OpId(1, actor.clone()))),
                    key: Cow::Owned(Key::Seq(ElementId::Id(OpId(3, actor)))),
                    pred: Cow::Owned(SortedVec::new()),
                    insert: true
                },
            ]
        );
    }

    #[test]
    fn expand_multi_set_double() {
        let actor = ActorId::from_str("7f12a4d3567c4257af34f216aa16fe48").unwrap();
        let ops = [
            Op {
                action: OpType::MultiSet(
                    vec![
                        ScalarValue::Uint(1),
                        ScalarValue::Uint(2),
                        ScalarValue::Uint(3),
                    ]
                    .try_into()
                    .unwrap(),
                ),
                obj: ObjectId::Id(OpId(1, actor.clone())),
                key: Key::Seq(ElementId::Head),
                pred: SortedVec::new(),
                insert: true,
            },
            Op {
                action: OpType::MultiSet(
                    vec![
                        ScalarValue::Str("hi".into()),
                        ScalarValue::Str("world".into()),
                    ]
                    .try_into()
                    .unwrap(),
                ),
                obj: ObjectId::Id(OpId(1, actor.clone())),
                key: Key::Seq(ElementId::Id(OpId(4, actor.clone()))),
                pred: SortedVec::new(),
                insert: true,
            },
        ];
        let expanded_ops = ExpandedOpIterator::new(&ops, 2, actor.clone()).collect::<Vec<_>>();
        assert_eq!(
            expanded_ops,
            vec![
                ExpandedOp {
                    action: InternalOpType::Set(ScalarValue::Uint(1)),
                    obj: Cow::Owned(ObjectId::Id(OpId(1, actor.clone()))),
                    key: Cow::Owned(Key::Seq(ElementId::Head)),
                    pred: Cow::Owned(SortedVec::new()),
                    insert: true
                },
                ExpandedOp {
                    action: InternalOpType::Set(ScalarValue::Uint(2)),
                    obj: Cow::Owned(ObjectId::Id(OpId(1, actor.clone()))),
                    key: Cow::Owned(Key::Seq(ElementId::Id(OpId(2, actor.clone())))),
                    pred: Cow::Owned(SortedVec::new()),
                    insert: true
                },
                ExpandedOp {
                    action: InternalOpType::Set(ScalarValue::Uint(3)),
                    obj: Cow::Owned(ObjectId::Id(OpId(1, actor.clone()))),
                    key: Cow::Owned(Key::Seq(ElementId::Id(OpId(3, actor.clone())))),
                    pred: Cow::Owned(SortedVec::new()),
                    insert: true
                },
                ExpandedOp {
                    action: InternalOpType::Set(ScalarValue::Str("hi".into())),
                    obj: Cow::Owned(ObjectId::Id(OpId(1, actor.clone()))),
                    key: Cow::Owned(Key::Seq(ElementId::Id(OpId(4, actor.clone())))),
                    pred: Cow::Owned(SortedVec::new()),
                    insert: true
                },
                ExpandedOp {
                    action: InternalOpType::Set(ScalarValue::Str("world".into())),
                    obj: Cow::Owned(ObjectId::Id(OpId(1, actor.clone()))),
                    key: Cow::Owned(Key::Seq(ElementId::Id(OpId(5, actor)))),
                    pred: Cow::Owned(SortedVec::new()),
                    insert: true
                },
            ]
        );
    }

    #[test]
    fn expand_multi_del() {
        let actor = ActorId::from_str("7f12a4d3567c4257af34f216aa16fe48").unwrap();
        let pred = OpId(1, actor.clone());
        let ops = [Op {
            action: OpType::Del(NonZeroU32::new(3).unwrap()),
            obj: ObjectId::Id(OpId(1, actor.clone())),
            key: Key::Seq(ElementId::Id(OpId(1, actor.clone()))),
            pred: vec![pred].into(),
            insert: true,
        }];
        let expanded_ops = ExpandedOpIterator::new(&ops, 2, actor.clone()).collect::<Vec<_>>();
        assert_eq!(
            expanded_ops,
            vec![
                ExpandedOp {
                    action: InternalOpType::Del,
                    obj: Cow::Owned(ObjectId::Id(OpId(1, actor.clone()))),
                    key: Cow::Owned(Key::Seq(ElementId::Id(OpId(1, actor.clone())))),
                    pred: Cow::Owned(vec![OpId(1, actor.clone())].into()),
                    insert: true
                },
                ExpandedOp {
                    action: InternalOpType::Del,
                    obj: Cow::Owned(ObjectId::Id(OpId(1, actor.clone()))),
                    key: Cow::Owned(Key::Seq(ElementId::Id(OpId(2, actor.clone())))),
                    pred: Cow::Owned(vec![OpId(2, actor.clone())].into()),
                    insert: true
                },
                ExpandedOp {
                    action: InternalOpType::Del,
                    obj: Cow::Owned(ObjectId::Id(OpId(1, actor.clone()))),
                    key: Cow::Owned(Key::Seq(ElementId::Id(OpId(3, actor.clone())))),
                    pred: Cow::Owned(vec![OpId(3, actor)].into()),
                    insert: true
                },
            ]
        );
    }
}
