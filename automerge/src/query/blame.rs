use crate::query::{OpSetMetadata, QueryResult, TreeQuery};
use crate::types::{ElemId, Op, OpType, ScalarValue};
use crate::clock::Clock;
use std::collections::HashMap;
use std::fmt::Debug;

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct Blame<const B: usize> {
    pos: usize,
    seen: usize,
    last_seen: Option<ElemId>,
    last_insert: Option<ElemId>,
    seen_at_this_mark: Option<ElemId>,
    seen_at_last_mark: Option<ElemId>,
    ops: Vec<Op>,
    points: Vec<Clock>,
    changed: bool,
}

impl<const B: usize> Blame<B> {
    pub fn new(points: Vec<Clock>) -> Self {
        Blame {
            pos: 0,
            seen: 0,
            last_seen: None,
            last_insert: None,
            seen_at_last_mark: None,
            seen_at_this_mark: None,
            changed: false,
            points: Vec::new(),
            ops: Vec::new(),
        }
    }
}

impl<const B: usize> TreeQuery<B> for Blame<B> {
    /*
    fn query_node(&mut self, _child: &OpTreeNode<B>) -> QueryResult {
        unimplemented!()
    }
    */

    fn query_element_with_metadata(&mut self, element: &Op, m: &OpSetMetadata) -> QueryResult {
        // find location to insert
        // mark or set
        if element.succ.is_empty() {
            if let OpType::MarkBegin(_) = &element.action {
                let pos = self
                    .ops
                    .binary_search_by(|probe| m.lamport_cmp(probe.id, element.id))
                    .unwrap_err();
                self.ops.insert(pos, element.clone());
            }
            if let OpType::MarkEnd(_) = &element.action {
                self.ops.retain(|op| op.id != element.id.prev());
            }
        }
        if element.insert {
            self.last_seen = None;
            self.last_insert = element.elemid();
        }
        if self.last_seen.is_none() && element.visible() {
            //self.check_marks();
            self.seen += 1;
            self.last_seen = element.elemid();
            self.seen_at_this_mark = element.elemid();
        }
        self.pos += 1;
        QueryResult::Next
    }
}
