use crate::{
    op_tree::OpTreeNode,
    types::{ElemId, Key, ListEncoding},
};

use super::{QueryResult, TreeQuery};

/// Lookup the index in the list that this elemid occupies.
#[derive(Clone, Debug)]
pub(crate) struct ElemIdPos {
    elemid: ElemId,
    pos: usize,
    found: bool,
    encoding: ListEncoding,
}

impl ElemIdPos {
    pub(crate) fn new(elemid: ElemId, encoding: ListEncoding) -> Self {
        Self {
            elemid,
            pos: 0,
            found: false,
            encoding,
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
    fn query_node(&mut self, child: &OpTreeNode) -> QueryResult {
        // if index has our element then we can continue
        if child.index.has_visible(&Key::Seq(self.elemid)) {
            // element is in this node somewhere
            QueryResult::Descend
        } else {
            // not in this node, try the next one
            self.pos += child.index.visible_len(self.encoding);
            QueryResult::Next
        }
    }

    fn query_element(&mut self, element: &crate::types::Op) -> QueryResult {
        if element.elemid() == Some(self.elemid) {
            // this is it
            self.found = true;
            return QueryResult::Finish;
        } else if element.visible() {
            self.pos += element.width(self.encoding);
        }
        QueryResult::Next
    }
}
