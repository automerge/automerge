#![allow(dead_code)]

use crate::op_tree::{OpSetMetadata, OpTreeNode};
use crate::query::{binary_search_by, is_visible, CounterData, QueryResult, TreeQuery};
use crate::{Clock, Key, ObjId, OpId};
use std::collections::HashMap;
use std::fmt::Debug;

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct KeysAt<const B: usize> {
    obj: ObjId,
    clock: Clock,
    pub keys: Vec<Key>,
    pos: usize,
    counters: HashMap<OpId, CounterData>,
}

impl<const B: usize> KeysAt<B> {
    pub fn new(obj: ObjId, clock: Clock) -> Self {
        KeysAt {
            obj,
            clock,
            pos: 0,
            keys: vec![],
            counters: Default::default(),
        }
    }
}

impl<const B: usize> TreeQuery<B> for KeysAt<B> {
    fn query_node_with_metadata(
        &mut self,
        child: &OpTreeNode<B>,
        m: &OpSetMetadata,
    ) -> QueryResult {
        self.pos = binary_search_by(child, |op| {
            m.lamport_cmp(op.obj.0, self.obj.0)
            //.then_with(|| m.key_cmp(&op.key, &self.op.key))
        });
        let mut last = None;
        while self.pos < child.len() {
            let op = child.get(self.pos).unwrap();
            if op.obj != self.obj {
                break;
            }
            let visible = is_visible(op, self.pos, &mut self.counters);
            if Some(op.key) != last && visible {
                self.keys.push(op.key);
                last = Some(op.key);
            }
            self.pos += 1;
        }
        //QueryResult::Finish
        unimplemented!();
    }
}
