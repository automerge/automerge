use crate::error::AutomergeError;
use crate::marks::{MarkSet, MarkStateMachine};
use crate::op_set::Op;
use crate::op_tree::{OpTree, OpTreeNode};
use crate::query::{Index, ListState, OpSetData, QueryResult, RichTextQueryState, TreeQuery};
use crate::types::{Clock, Key, ListEncoding};
use std::fmt::Debug;
use std::sync::Arc;

/// The Nth query walks the tree to find the n-th Node. It skips parts of the tree where it knows
/// that the nth node can not be in them
#[derive(Debug, Clone)]
pub(crate) struct Nth<'a> {
    list_state: ListState,
    clock: Option<Clock>,
    marks: Option<RichTextQueryState<'a>>,
    // TODO: put osd in all queries - take out of API
    osd: &'a OpSetData,
    pub(crate) ops: Vec<Op<'a>>,
    pub(crate) ops_pos: Vec<usize>,
}

impl<'a> Nth<'a> {
    pub(crate) fn new(
        target: usize,
        encoding: ListEncoding,
        clock: Option<Clock>,
        osd: &'a OpSetData,
    ) -> Self {
        Nth {
            list_state: ListState::new(encoding, target + 1),
            clock,
            marks: None,
            osd,
            ops: vec![],
            ops_pos: vec![],
        }
    }

    pub(crate) fn with_marks(mut self) -> Self {
        self.marks = Some(Default::default());
        self
    }

    pub(crate) fn marks(&self) -> Option<Arc<MarkSet>> {
        let mut marks = MarkStateMachine::default();
        if let Some(m) = &self.marks {
            for (id, mark_data) in m.iter() {
                marks.mark_begin(*id, mark_data, self.osd);
            }
        }
        marks.current().cloned()
    }

    /// Get the key
    pub(crate) fn key(&self) -> Result<Key, AutomergeError> {
        // the query collects the ops so we can use that to get the key they all use
        if let Some(e) = self.ops.first().and_then(|op| op.elemid()) {
            Ok(Key::Seq(e))
        } else {
            Err(AutomergeError::InvalidIndex(
                self.list_state.target().saturating_sub(1),
            ))
        }
    }

    pub(crate) fn index(&self) -> usize {
        self.list_state.last_index()
    }

    pub(crate) fn pos(&self) -> usize {
        self.list_state.pos()
    }
}

impl<'a> TreeQuery<'a> for Nth<'a> {
    fn can_shortcut_search(&mut self, tree: &'a OpTree, osd: &'a OpSetData) -> bool {
        if self.marks.is_some() {
            // we could cache marks data but we're not now
            return false;
        }
        if let Some(last) = &tree.last_insert {
            if last.index == self.list_state.target().saturating_sub(1) {
                if let Some(idx) = tree.internal.get(last.pos) {
                    self.list_state.seek(last);
                    self.ops.push(idx.as_op(osd));
                    self.ops_pos.push(last.pos);
                    return true;
                }
            }
        }
        false
    }

    fn query_node(
        &mut self,
        child: &'a OpTreeNode,
        index: &'a Index,
        osd: &OpSetData,
    ) -> QueryResult {
        self.list_state.check_if_node_is_clean(index);
        if self.clock.is_none() {
            self.list_state
                .process_node(child, index, osd, self.marks.as_mut())
        } else {
            QueryResult::Descend
        }
    }

    fn query_element(&mut self, op: Op<'a>) -> QueryResult {
        if op.insert() && self.list_state.done() {
            QueryResult::Finish
        } else {
            if let Some(m) = self.marks.as_mut() {
                m.process(op, self.clock.as_ref())
            }
            let visible = op.visible_at(self.clock.as_ref());
            let key = op.elemid_or_key();
            self.list_state.process_op(op, key, visible);
            if visible && self.list_state.done() {
                self.ops.push(op);
                self.ops_pos.push(self.list_state.pos().saturating_sub(1));
            }
            QueryResult::Next
        }
    }
}
