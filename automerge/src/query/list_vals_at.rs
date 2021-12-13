#![allow(dead_code)]

use crate::op_tree::{OpSetMetadata, OpTreeNode};
use crate::query::{binary_search_by, is_visible, visible_op, QueryResult, TreeQuery};
use crate::{ElemId, ObjId, Op, Clock};
use std::fmt::Debug;

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct ListValsAt {
    obj: ObjId,
    clock: Clock,
    last_elem: Option<ElemId>,
    pub ops: Vec<Op>,
}

impl ListValsAt {
    pub fn new(obj: ObjId, clock: Clock) -> Self {
        ListValsAt {
            obj,
            clock,
            last_elem: None,
            ops: vec![],
        }
    }
}

impl<const B: usize> TreeQuery<B> for ListValsAt {
    fn query_node_with_metadata(
        &mut self,
        child: &OpTreeNode<B>,
        m: &OpSetMetadata,
    ) -> QueryResult {
        let start = binary_search_by(child, |op| m.lamport_cmp(op.obj.0, self.obj.0));
        let mut counters = Default::default();
        for pos in start..child.len() {
            let op = child.get(pos).unwrap();
            if op.obj != self.obj {
                break;
            }
            if op.insert {
                self.last_elem = None;
            }
            if self.last_elem.is_none() && is_visible(op, pos, &mut counters) {
                for (_, vop) in visible_op(op, pos, &counters) {
                    self.last_elem = vop.elemid();
                    self.ops.push(vop);
                }
            }
        }
        //QueryResult::Finish
        unimplemented!()
    }
}
