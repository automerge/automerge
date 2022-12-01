use crate::{
    op_tree::OpTreeNode,
    types::{ElemId, Key},
};

use super::{QueryResult, TreeQuery};

/// Lookup the index in the list that this elemid occupies.
pub(crate) struct ElemIdPos {
    elemid: ElemId,
    pos: usize,
    found: bool,
}

impl ElemIdPos {
    pub(crate) fn new(elemid: ElemId) -> Self {
        Self {
            elemid,
            pos: 0,
            found: false,
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
            self.pos += child.index.visible_len(false);
            QueryResult::Next
        }
    }

    fn query_element(&mut self, element: &crate::types::Op) -> QueryResult {
        if element.elemid() == Some(self.elemid) {
            // this is it
            self.found = true;
            return QueryResult::Finish;
        } else if element.visible() {
            self.pos += 1;
        }
        QueryResult::Next
    }
}
