use crate::{op_tree::OpTreeNode, types::ElemId};

use super::{QueryResult, TreeQuery};

pub(crate) struct ElemIdPos {
    elemid: ElemId,
    pos: usize,
    found: bool,
}

impl ElemIdPos {
    pub fn new(elemid: ElemId) -> Self {
        Self {
            elemid,
            pos: 0,
            found: false,
        }
    }

    pub fn index(&self) -> Option<usize> {
        if self.found {
            Some(self.pos)
        } else {
            None
        }
    }
}

impl<const B: usize> TreeQuery<B> for ElemIdPos {
    fn query_node(&mut self, child: &OpTreeNode<B>) -> QueryResult {
        dbg!(child, &self.elemid);
        // if index has our element then we can cont
        if child.index.has(&Some(self.elemid)) {
            // element is in this node somewhere
            QueryResult::Descend
        } else {
            // not in this node, try the next one
            self.pos += child.index.len;
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
