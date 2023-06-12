use crate::marks::RichText;
use crate::op_tree::OpTreeNode;
use crate::query::OpSetMetadata;
use crate::query::{ListState, QueryResult, RichTextQueryState, TreeQuery};
use crate::types::Clock;
use crate::types::{ListEncoding, Op, OpId};
use std::cmp::Ordering;
use std::rc::Rc;

/// Search for an OpId in a tree.  /// Returns the index of the operation in the tree.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct OpIdSearch<'a> {
    idx: ListState,
    clock: Option<&'a Clock>,
    target: SearchTarget<'a>,
    marks: RichTextQueryState<'a>,
}

#[derive(Debug, Clone, PartialEq)]
enum SearchTarget<'a> {
    OpId(OpId, Option<&'a Op>),
    Op(&'a Op),
    Complete(usize),
}

impl<'a> OpIdSearch<'a> {
    pub(crate) fn opid(target: OpId, encoding: ListEncoding, clock: Option<&'a Clock>) -> Self {
        OpIdSearch {
            idx: ListState::new(encoding, usize::MAX),
            clock,
            target: SearchTarget::OpId(target, None),
            marks: Default::default(),
        }
    }

    pub(crate) fn op(op: &'a Op, encoding: ListEncoding) -> Self {
        // this will only be called with list ops
        let elemid = op.key.elemid().expect("map op passed to query::OpIdSearch");
        let target = match elemid.is_head() {
            true => SearchTarget::Op(op),
            false => SearchTarget::OpId(elemid.0, Some(op)),
        };
        OpIdSearch {
            idx: ListState::new(encoding, usize::MAX),
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
        self.idx.pos()
    }

    pub(crate) fn index(&self) -> usize {
        self.idx.index()
    }

    pub(crate) fn index_for(&self, op: &Op) -> usize {
        if self.idx.was_last_seen(op.elemid_or_key()) {
            self.idx.last_index()
        } else {
            self.idx.index()
        }
    }

    pub(crate) fn marks(&self, m: &OpSetMetadata) -> Option<Rc<RichText>> {
        RichText::from_query_state(&self.marks, m)
    }
}

impl<'a> TreeQuery<'a> for OpIdSearch<'a> {
    fn query_node(&mut self, child: &'a OpTreeNode, ops: &'a [Op]) -> QueryResult {
        self.idx.check_if_node_is_clean(child);
        if self.clock.is_some() {
            QueryResult::Descend
        } else {
            match &self.target {
                SearchTarget::OpId(id, _) if !child.index.ops.contains(id) => {
                    self.idx.process_node(child, ops, Some(&mut self.marks));
                    QueryResult::Next
                }
                _ => QueryResult::Descend,
            }
        }
    }

    fn query_element_with_metadata(&mut self, element: &'a Op, m: &OpSetMetadata) -> QueryResult {
        self.marks.process(element);
        match self.target {
            SearchTarget::OpId(target, None) => {
                if element.id == target {
                    self.target = SearchTarget::Complete(self.idx.pos());
                    return QueryResult::Finish;
                }
            }
            SearchTarget::OpId(target, Some(op)) => {
                if element.id == target {
                    if op.insert {
                        self.target = SearchTarget::Op(op);
                    } else {
                        self.target = SearchTarget::Complete(self.idx.pos());
                        return QueryResult::Finish;
                    }
                }
            }
            SearchTarget::Op(op) => {
                if element.insert && m.lamport_cmp(element.id, op.id) == Ordering::Less {
                    self.target = SearchTarget::Complete(self.idx.pos());
                    return QueryResult::Finish;
                }
            }
            SearchTarget::Complete(_) => return QueryResult::Finish, // this should never happen
        }
        self.idx.process_op(
            element,
            element.elemid_or_key(),
            element.visible_at(self.clock),
        );
        QueryResult::Next
    }
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct SimpleOpIdSearch<'a> {
    target: OpId,
    op: &'a Op,
    pub(crate) pos: usize,
    found: bool,
}

impl<'a> SimpleOpIdSearch<'a> {
    pub(crate) fn op(op: &'a Op) -> Self {
        let elemid = op.key.elemid().expect("map op passed to query::OpIdSearch");
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
    fn query_node(&mut self, child: &OpTreeNode, _ops: &[Op]) -> QueryResult {
        if self.found || child.index.ops.contains(&self.target) {
            QueryResult::Descend
        } else {
            self.pos += child.len();
            QueryResult::Next
        }
    }

    fn query_element_with_metadata(&mut self, element: &'a Op, m: &OpSetMetadata) -> QueryResult {
        if !self.found {
            if element.id == self.target {
                self.found = true;
                if !self.op.insert {
                    return QueryResult::Finish;
                }
            }
        } else if element.insert && m.lamport_cmp(element.id, self.op.id) == Ordering::Less {
            return QueryResult::Finish;
        }
        self.pos += 1;
        QueryResult::Next
    }
}
