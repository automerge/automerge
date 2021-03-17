use crate::internal::InternalOpType;
use automerge_protocol as amp;

/// The same as amp::Op except the `action` is an `InternalOpType`. This allows us to expand
/// collections of `amp::Op` into `ExpandedOp`s and remove optypes which perform multiple
/// operations (`amp::OpType::MultiSet` and `amp::OpType::Del`)
#[derive(Debug)]
pub(super) struct ExpandedOp<'a> {
    pub action: InternalOpType,
    pub obj: &'a amp::ObjectId,
    pub key: &'a amp::Key,
    pub pred: &'a [amp::OpId],
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
                amp::OpType::Del => InternalOpType::Del,
                amp::OpType::Make(ot) => InternalOpType::Make(ot.clone()),
                amp::OpType::Inc(i) => InternalOpType::Inc(*i),
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
                        InternalOpType::Set(v.clone())
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
                pred: &op.pred,
                key: &op.key,
                obj: &op.obj,
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
