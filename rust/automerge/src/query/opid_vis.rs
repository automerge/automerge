use crate::op_tree::OpTreeNode;
use crate::query::{QueryResult, TreeQuery};
use crate::types::{Key, Op, OpId};

/// Search for an OpId in a tree.
/// Returns the index of the operation in the tree.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct OpIdVisSearch {
    target: OpId,
    found: bool,
    pub(crate) visible: bool,
    key: Option<Key>,
}

impl OpIdVisSearch {
    pub(crate) fn new(target: OpId) -> Self {
        OpIdVisSearch {
            target,
            found: false,
            visible: true,
            key: None,
        }
    }

    pub(crate) fn key(&self) -> &Option<Key> {
        &self.key
    }
}

impl<'a> TreeQuery<'a> for OpIdVisSearch {
    fn query_node(&mut self, child: &OpTreeNode) -> QueryResult {
        if child.index.ops.contains(&self.target) {
            QueryResult::Descend
        } else {
            QueryResult::Next
        }
    }

    fn query_element(&mut self, element: &Op) -> QueryResult {
        if element.id == self.target {
            self.found = true;
            self.key = Some(element.elemid_or_key());
            if element.visible() {
                QueryResult::Next
            } else {
                self.visible = false;
                QueryResult::Finish
            }
        } else if self.found {
            if self.key != Some(element.elemid_or_key()) {
                QueryResult::Finish
            } else if element.visible() {
                self.visible = false;
                QueryResult::Finish
            } else {
                QueryResult::Next
            }
        } else {
            QueryResult::Next
        }
    }
}
