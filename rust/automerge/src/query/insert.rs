use crate::error::AutomergeError;
use crate::marks::MarkSet;
use crate::marks::MarkStateMachine;
use crate::op_tree::OpTreeNode;
use crate::query::{ListState, MarkMap, OpSetMetadata, OpTree, QueryResult, TreeQuery};
use crate::types::{Clock, Key, ListEncoding, Op, HEAD};
use std::fmt::Debug;

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct InsertNth<'a> {
    idx: ListState,
    valid: Option<usize>,
    clock: Option<Clock>,
    last_valid_insert: Option<Key>,
    marks: MarkMap<'a>,
}

impl<'a> InsertNth<'a> {
    pub(crate) fn new(target: usize, encoding: ListEncoding, clock: Option<Clock>) -> Self {
        let idx = ListState::new(encoding, target);
        if target == 0 {
            InsertNth {
                idx,
                valid: Some(0),
                last_valid_insert: Some(Key::Seq(HEAD)),
                clock,
                marks: Default::default(),
            }
        } else {
            InsertNth {
                idx,
                valid: None,
                last_valid_insert: None,
                clock,
                marks: Default::default(),
            }
        }
    }

    pub(crate) fn marks(&self, m: &OpSetMetadata) -> Option<MarkSet> {
        let mut marks = MarkStateMachine::default();
        for (id, mark_data) in self.marks.iter() {
            marks.mark_begin(*id, mark_data, m);
        }
        marks.current().cloned()
    }

    pub(crate) fn pos(&self) -> usize {
        self.valid.unwrap_or(self.idx.pos())
    }

    pub(crate) fn key(&self) -> Result<Key, AutomergeError> {
        self.last_valid_insert
            .ok_or(AutomergeError::InvalidIndex(self.idx.target()))
    }
}

impl<'a> TreeQuery<'a> for InsertNth<'a> {
    fn equiv(&mut self, other: &Self) -> bool {
        self.pos() == other.pos() && self.key() == other.key()
    }

    fn can_shortcut_search(&mut self, tree: &'a OpTree) -> bool {
        if let Some(last) = &tree.last_insert {
            if last.index + last.width == self.idx.target() {
                self.valid = Some(last.pos + 1);
                self.last_valid_insert = Some(last.key);
                return true;
            }
        }
        false
    }

    fn query_node(&mut self, child: &'a OpTreeNode, ops: &'a [Op]) -> QueryResult {
        self.idx.check_if_node_is_clean(child);
        if self.clock.is_none() {
            self.idx.process_node(child, ops, Some(&mut self.marks))
        } else {
            QueryResult::Descend
        }
    }

    fn query_element(&mut self, element: &'a Op) -> QueryResult {
        self.marks.process(element);
        let key = element.elemid_or_key();
        let visible = element.visible_at(self.clock.as_ref());
        // an insert after we're done - could be a valid insert point
        if element.insert && self.valid.is_none() && self.idx.done() {
            self.valid = Some(self.idx.pos());
        }
        // sticky marks
        if self.valid.is_some() && element.valid_mark_anchor() {
            self.last_valid_insert = Some(key);
            self.valid = None;
        }
        if visible {
            if self.valid.is_some() {
                return QueryResult::Finish;
            }
            self.last_valid_insert = Some(key);
        }
        self.idx.process_op(element, key, visible);
        QueryResult::Next
    }
}
