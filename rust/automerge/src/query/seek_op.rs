use crate::op_tree::{OpSetMetadata, OpTreeNode};
use crate::query::{binary_search_by, QueryResult, TreeQuery};
use crate::types::{Key, Op, HEAD};
use std::cmp::Ordering;
use std::fmt::Debug;

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct SeekOp<'a> {
    /// the op we are looking for
    op: &'a Op,
    /// The position to insert at
    pub(crate) pos: usize,
    /// The indices of ops that this op overwrites
    pub(crate) succ: Vec<usize>,
    /// whether a position has been found
    found: bool,
}

impl<'a> SeekOp<'a> {
    pub(crate) fn new(op: &'a Op) -> Self {
        SeekOp {
            op,
            succ: vec![],
            pos: 0,
            found: false,
        }
    }

    fn lesser_insert(&self, op: &Op, m: &OpSetMetadata) -> bool {
        op.insert && m.lamport_cmp(op.id, self.op.id) == Ordering::Less
    }

    fn greater_opid(&self, op: &Op, m: &OpSetMetadata) -> bool {
        m.lamport_cmp(op.id, self.op.id) == Ordering::Greater
    }

    fn is_target_insert(&self, op: &Op) -> bool {
        op.insert && op.elemid() == self.op.key.elemid()
    }
}

impl<'a> TreeQuery<'a> for SeekOp<'a> {
    fn query_node_with_metadata(
        &mut self,
        child: &OpTreeNode,
        m: &OpSetMetadata,
        ops: &[Op],
    ) -> QueryResult {
        if self.found {
            return QueryResult::Descend;
        }
        match self.op.key {
            Key::Seq(HEAD) => {
                while self.pos < child.len() {
                    let op = &ops[child.get(self.pos).unwrap()];
                    if op.insert && m.lamport_cmp(op.id, self.op.id) == Ordering::Less {
                        break;
                    }
                    self.pos += 1;
                }
                QueryResult::Finish
            }
            Key::Seq(e) => {
                if child.index.ops.contains(&e.0) {
                    QueryResult::Descend
                } else {
                    self.pos += child.len();
                    QueryResult::Next
                }
            }
            Key::Map(_) => {
                let start = binary_search_by(child, ops, |op| m.key_cmp(&op.key, &self.op.key));
                self.pos = start;
                QueryResult::Skip(start)
            }
        }
    }

    fn query_element_with_metadata(&mut self, e: &Op, m: &OpSetMetadata) -> QueryResult {
        match self.op.key {
            Key::Map(_) => {
                // don't bother looking at things past our key
                if e.key != self.op.key {
                    return QueryResult::Finish;
                }

                if self.op.overwrites(e) {
                    self.succ.push(self.pos);
                }

                if m.lamport_cmp(e.id, self.op.id) == Ordering::Greater {
                    return QueryResult::Finish;
                }

                self.pos += 1;
                QueryResult::Next
            }
            Key::Seq(_) => {
                if !self.found {
                    if self.is_target_insert(e) {
                        self.found = true;
                        if self.op.overwrites(e) {
                            self.succ.push(self.pos);
                        }
                    }
                    self.pos += 1;
                    QueryResult::Next
                } else {
                    // we have already found the target
                    if self.op.overwrites(e) {
                        self.succ.push(self.pos);
                    }
                    if self.op.insert {
                        if self.lesser_insert(e, m) {
                            QueryResult::Finish
                        } else {
                            self.pos += 1;
                            QueryResult::Next
                        }
                    } else if e.insert || self.greater_opid(e, m) {
                        QueryResult::Finish
                    } else {
                        self.pos += 1;
                        QueryResult::Next
                    }
                }
            }
        }
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use crate::{
        op_set::OpSet,
        op_tree::B,
        query::SeekOp,
        types::{Key, ObjId, Op, OpId},
        ActorId, ScalarValue,
    };

    /// Create an optree in which the only visible ops are on the boundaries of the nodes,
    /// i.e. the visible elements are in the internal nodes. Like so
    ///
    /// ```notrust
    ///
    ///                      .----------------------.
    ///                      | id   |  key  |  succ |
    ///                      | B    |  "a"  |       |
    ///                      | 2B   |  "b"  |       |
    ///                      '----------------------'
    ///                           /      |      \
    ///  ;------------------------.      |       `------------------------------------.
    ///  | id     | op     | succ |      |       | id            | op     | succ      |
    ///  | 0      |set "a" |  1   |      |       | 2B + 1        |set "c" |  2B + 2   |
    ///  | 1      |set "a" |  2   |      |       | 2B + 2        |set "c" |  2B + 3   |
    ///  | 2      |set "a" |  3   |      |       ...
    ///  ...                             |       | 3B            |set "c" |           |
    ///  | B - 1  |set "a" |  B   |      |       '------------------------------------'
    ///  '--------'--------'------'      |
    ///                                  |
    ///                      .-----------------------------.
    ///                      | id         |  key  |  succ  |
    ///                      | B + 1      |  "b"  | B + 2  |
    ///                      | B + 2      |  "b"  | B + 3  |
    ///                      ....
    ///                      | B + (B - 1 |  "b"  |   2B   |
    ///                      '-----------------------------'
    /// ```
    ///
    /// The important point here is that the leaf nodes contain no visible ops for keys "a" and
    /// "b".
    ///
    /// # Returns
    ///
    /// The opset in question and an op which should be inserted at the next position after the
    /// internally visible ops.
    pub(crate) fn optree_with_only_internally_visible_ops() -> (OpSet, Op) {
        let mut set = OpSet::new();
        let actor = set.m.actors.cache(ActorId::random());
        let a = set.m.props.cache("a".to_string());
        let b = set.m.props.cache("b".to_string());
        let c = set.m.props.cache("c".to_string());

        let mut counter = 0;
        // For each key insert `B` operations with the `pred` and `succ` setup such that the final
        // operation for each key is the only visible op.
        for key in [a, b, c] {
            for iteration in 0..B {
                // Generate a value to insert
                let keystr = set.m.props.get(key);
                let val = keystr.repeat(iteration + 1);

                // Only the last op is visible
                let pred = if iteration == 0 {
                    Default::default()
                } else {
                    set.m
                        .sorted_opids(vec![OpId::new(counter - 1, actor)].into_iter())
                };

                // only the last op is visible
                let succ = if iteration == B - 1 {
                    Default::default()
                } else {
                    set.m
                        .sorted_opids(vec![OpId::new(counter, actor)].into_iter())
                };

                let op = Op {
                    id: OpId::new(counter, actor),
                    action: crate::OpType::Put(ScalarValue::Str(val.into())),
                    key: Key::Map(key),
                    succ,
                    pred,
                    insert: false,
                };
                set.insert(counter as usize, &ObjId::root(), op);
                counter += 1;
            }
        }

        // Now try and create an op which inserts at the next index of 'a'
        let new_op = Op {
            id: OpId::new(counter, actor),
            action: crate::OpType::Put(ScalarValue::Str("test".into())),
            key: Key::Map(a),
            succ: Default::default(),
            pred: set
                .m
                .sorted_opids(std::iter::once(OpId::new(B as u64 - 1, actor))),
            insert: false,
        };
        (set, new_op)
    }

    #[test]
    fn seek_on_page_boundary() {
        let (set, new_op) = optree_with_only_internally_visible_ops();

        let q = SeekOp::new(&new_op);
        let q = set.search(&ObjId::root(), q);

        // we've inserted `B - 1` elements for "a", so the index should be `B`
        assert_eq!(q.pos, B);
    }
}
