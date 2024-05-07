use crate::marks::MarkSet;
use crate::op_set::{Op, OpSetData};
use crate::op_tree::OpTreeNode;
use crate::query::{Index, ListState, QueryResult, RichTextQueryState, TreeQuery};
use crate::types::Clock;
use crate::types::{ListEncoding, OpId};
use std::cmp::Ordering;
use std::sync::Arc;

/// Search for an OpId in a tree.  /// Returns the index of the operation in the tree.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct OpIdSearch<'a> {
    list_state: ListState,
    clock: Option<&'a Clock>,
    target: SearchTarget<'a>,
    marks: RichTextQueryState<'a>,
}

#[derive(Clone, PartialEq)]
enum SearchTarget<'a> {
    OpId(
        // The opid we are looking for
        OpId,
        // If we are performing an inset, this is the operation we are inserting
        Option<Op<'a>>,
    ),
    Op(Op<'a>),
    Complete(usize),
}

impl std::fmt::Debug for SearchTarget<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SearchTarget::OpId(id, to_insert) => {
                write!(f, "OpId({:?}, {:?})", id, to_insert.map(|op| op.id()))
            }
            SearchTarget::Op(op) => write!(f, "Op({:?})", op.id()),
            SearchTarget::Complete(n) => write!(f, "Complete({})", n),
        }
    }
}

impl<'a> OpIdSearch<'a> {
    pub(crate) fn opid(target: OpId, encoding: ListEncoding, clock: Option<&'a Clock>) -> Self {
        OpIdSearch {
            list_state: ListState::new(encoding, usize::MAX),
            clock,
            target: SearchTarget::OpId(target, None),
            marks: Default::default(),
        }
    }

    pub(crate) fn op(op: Op<'a>, encoding: ListEncoding) -> Self {
        // this will only be called with list ops
        let elemid = op
            .key()
            .elemid()
            .expect("map op passed to query::OpIdSearch");
        let target = match elemid.is_head() {
            true => SearchTarget::Op(op),
            false => SearchTarget::OpId(elemid.0, Some(op)),
        };
        OpIdSearch {
            list_state: ListState::new(encoding, usize::MAX),
            clock: None,
            target,
            marks: Default::default(),
        }
    }

    /// Get the index of the operation, if found.
    pub(crate) fn found(&self) -> Option<usize> {
        match &self.target {
            SearchTarget::Complete(n) => Some(*n),
            _ => None,
        }
    }

    pub(crate) fn pos(&self) -> usize {
        self.list_state.pos()
    }

    pub(crate) fn index(&self) -> usize {
        self.list_state.index()
    }

    pub(crate) fn index_for(&self, op: Op<'a>) -> usize {
        if self.list_state.was_last_seen(op.elemid_or_key()) {
            self.list_state.last_index()
        } else {
            self.list_state.index()
        }
    }

    pub(crate) fn marks(&self, osd: &OpSetData) -> Option<Arc<MarkSet>> {
        MarkSet::from_query_state(&self.marks, osd)
    }
}

impl<'a> TreeQuery<'a> for OpIdSearch<'a> {
    fn query_node(
        &mut self,
        child: &'a OpTreeNode,
        index: &'a Index,
        osd: &'a OpSetData,
    ) -> QueryResult {
        self.list_state.check_if_node_is_clean(index);
        if self.clock.is_some() {
            QueryResult::Descend
        } else {
            match &self.target {
                SearchTarget::OpId(id, _) if !index.ops.contains(id) => {
                    self.list_state
                        .process_node(child, index, osd, Some(&mut self.marks));
                    QueryResult::Next
                }
                _ => QueryResult::Descend,
            }
        }
    }

    fn query_element(&mut self, op: Op<'a>) -> QueryResult {
        match self.target {
            SearchTarget::OpId(target, None) => {
                self.marks.process(op, self.clock);
                if op.id() == &target {
                    self.target = SearchTarget::Complete(self.list_state.pos());
                    return QueryResult::Finish;
                }
            }
            SearchTarget::OpId(target, Some(op2)) => {
                self.marks.process(op, self.clock);
                if op.id() == &target {
                    if op2.insert() {
                        self.target = SearchTarget::Op(op2);
                    } else {
                        self.target = SearchTarget::Complete(self.list_state.pos());
                        return QueryResult::Finish;
                    }
                }
            }
            SearchTarget::Op(op2) => {
                if op.insert() && op.lamport_cmp(*op2.id()) == Ordering::Less {
                    self.target = SearchTarget::Complete(self.list_state.pos());
                    return QueryResult::Finish;
                }
                self.marks.process(op, self.clock);
            }
            SearchTarget::Complete(_) => return QueryResult::Finish, // this should never happen
        }
        let elemid_or_key = op.elemid_or_key();
        let visible_at = op.visible_at(self.clock);
        self.list_state.process_op(op, elemid_or_key, visible_at);
        QueryResult::Next
    }
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct SimpleOpIdSearch<'a> {
    target: OpId,
    op: Op<'a>,
    pub(crate) pos: usize,
    found: bool,
}

impl<'a> SimpleOpIdSearch<'a> {
    pub(crate) fn op(op: Op<'a>) -> Self {
        let elemid = op
            .key()
            .elemid()
            .expect("map op passed to query::OpIdSearch");
        //let target = match elemid.is_head() {
        //    true => SearchTarget::Op(op),
        //    false => SearchTarget::OpId(elemid.0, Some(op)),
        //};
        SimpleOpIdSearch {
            target: elemid.0,
            op,
            pos: 0,
            found: elemid.is_head(),
        }
    }
}

impl<'a> TreeQuery<'a> for SimpleOpIdSearch<'a> {
    fn query_node(&mut self, child: &OpTreeNode, index: &Index, _osd: &OpSetData) -> QueryResult {
        if self.found || index.ops.contains(&self.target) {
            QueryResult::Descend
        } else {
            self.pos += child.len();
            QueryResult::Next
        }
    }

    fn query_element(&mut self, op: Op<'a>) -> QueryResult {
        if !self.found {
            if op.id() == &self.target {
                self.found = true;
                if !self.op.insert() {
                    return QueryResult::Finish;
                }
            }
        } else if op.insert() && op.lamport_cmp(*self.op.id()) == Ordering::Less {
            return QueryResult::Finish;
        }
        self.pos += 1;
        QueryResult::Next
    }
}
