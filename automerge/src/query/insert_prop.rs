use crate::op_tree::{OpSetMetadata, OpTreeNode};
use crate::query::{binary_search_by, QueryResult, TreeQuery};
use crate::types::{Key, Op};
use std::fmt::Debug;

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct InsertProp<'a> {
    key: Key,
    pub(crate) ops: Vec<&'a Op>,
    pub(crate) ops_pos: Vec<usize>,
    pub(crate) pos: usize,
    start: Option<usize>,
}

impl<'a> InsertProp<'a> {
    pub(crate) fn new(prop: usize) -> Self {
        InsertProp {
            key: Key::Map(prop),
            ops: vec![],
            ops_pos: vec![],
            pos: 0,
            start: None,
        }
    }
}

impl<'a> TreeQuery<'a> for InsertProp<'a> {
    fn cache_lookup_map(&mut self, cache: &crate::object_data::MapOpsCache) -> bool {
        if let Some((last_key, last_pos)) = cache.last {
            if last_key == self.key {
                self.start = Some(last_pos);
            }
        }
        // don't have all of the result yet
        false
    }

    fn cache_update_map(&self, cache: &mut crate::object_data::MapOpsCache) {
        cache.last = None
    }

    fn query_node_with_metadata(
        &mut self,
        child: &'a OpTreeNode,
        m: &OpSetMetadata,
    ) -> QueryResult {
        let start = if let Some(start) = self.start {
            debug_assert!(binary_search_by(child, |op| m.key_cmp(&op.key, &self.key)) >= start);
            start
        } else {
            binary_search_by(child, |op| m.key_cmp(&op.key, &self.key))
        };
        self.start = Some(start);
        self.pos = start;
        for pos in start..child.len() {
            let op = child.get(pos).unwrap();
            if op.key != self.key {
                break;
            }
            if op.visible() {
                self.ops.push(op);
                self.ops_pos.push(pos);
            }
            self.pos += 1;
        }
        QueryResult::Finish
    }
}
