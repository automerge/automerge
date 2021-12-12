#![allow(dead_code)]

use crate::op_tree::{OpSetMetadata, OpTreeNode};
use crate::query::{binary_search_by, is_visible, CounterData, QueryResult, TreeQuery};
use crate::{Key, Op, OpId};
use std::collections::HashMap;
use std::fmt::Debug;

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct SeekOp<const B: usize> {
    op: Op,
    pos: usize,
    found: bool,
    counters: HashMap<OpId, CounterData>,
}

impl<const B: usize> SeekOp<B> {
    pub fn new(op: Op) -> Self {
        SeekOp {
            op,
            pos: 0,
            found: false,
            counters: Default::default(),
        }
    }

    pub fn is_visible(&mut self, element: &Op) -> bool {
        is_visible(element, self.pos, &mut self.counters)
    }
}

impl<const B: usize> TreeQuery<B> for SeekOp<B> {
    fn query_node_with_metadata(
        &mut self,
        child: &OpTreeNode<B>,
        m: &OpSetMetadata,
    ) -> QueryResult {
        if self.found { return QueryResult::Decend }
        match self.op.key {
          Key::Seq(e) => 
            if child.index.ops.contains(&e.0) {
                QueryResult::Decend
            } else {
                self.pos += child.len();
                QueryResult::Next
            }
          Key::Map(_) => {
            self.pos = binary_search_by(child, |op|
               m.lamport_cmp(op.obj.0, self.op.obj.0)
                .then_with(|| m.key_cmp(&op.key, &self.op.key))
                .then_with(|| m.lamport_cmp(op.id, self.op.id))
            );
            QueryResult::Finish
          }
        }
    }

    fn query_element(&mut self, element: &Op) -> QueryResult {
        if element.obj != self.op.obj {
            if self.found {
                return QueryResult::Finish;
            }
        } else {
            if element.insert {
                if !self.found {
                    return QueryResult::Finish;
                };
                //self.last_seen = None;
                //self.last_insert = element.elemid();
            }
            //if self.last_seen.is_none() && self.is_visible(element) {
                //self.seen += 1;
                //self.last_seen = element.elemid()
            //}
        }
        self.pos += 1;
        QueryResult::Next
    }
}
