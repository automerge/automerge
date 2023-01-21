use crate::{
    op_tree::OpTreeNode,
    types::{ElemId, ListEncoding, Op, OpId},
};

use super::{QueryResult, TreeQuery};

/// Lookup the index in the list that this elemid occupies, includes hidden elements.
#[derive(Clone, Debug)]
pub(crate) struct ElemIdPos {
    elem_opid: OpId,
    pos: usize,
    found: bool,
    encoding: ListEncoding,
}

impl ElemIdPos {
    pub(crate) fn new(elemid: ElemId, encoding: ListEncoding) -> Self {
        if elemid.is_head() {
            Self {
                elem_opid: elemid.0,
                pos: 0,
                found: true,
                encoding,
            }
        } else {
            Self {
                elem_opid: elemid.0,
                pos: 0,
                found: false,
                encoding,
            }
        }
    }

    pub(crate) fn index(&self) -> Option<usize> {
        if self.found {
            Some(self.pos)
        } else {
            None
        }
    }
}

impl<'a> TreeQuery<'a> for ElemIdPos {
    fn query_node(&mut self, child: &OpTreeNode, _ops: &[Op]) -> QueryResult {
        if self.found {
            return QueryResult::Finish;
        }
        // if index has our element then we can continue
        if child.index.has_op(&self.elem_opid) {
            // element is in this node somewhere
            QueryResult::Descend
        } else {
            // not in this node, try the next one
            self.pos += child.index.visible_len(self.encoding);
            QueryResult::Next
        }
    }

    fn query_element(&mut self, element: &crate::types::Op) -> QueryResult {
        if self.found {
            return QueryResult::Finish;
        }
        if element.elemid() == Some(ElemId(self.elem_opid)) {
            // this is it
            self.found = true;
            return QueryResult::Finish;
        } else if element.visible() {
            self.pos += element.width(self.encoding);
        }
        QueryResult::Next
    }
}
