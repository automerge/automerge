use get_size::GetSize;

use super::TreeQuery;

#[derive(Clone, Debug)]
pub(crate) struct StatsQuery {
    index_size: usize,
    ops_size: usize,
}

impl StatsQuery {
    pub(crate) fn new() -> Self {
        Self{ 
            index_size: 0,
            ops_size: 0,
        }
    }

    pub(crate) fn result(self) -> OpTreeStats {
        OpTreeStats {
            index_size: self.index_size,
            ops_size: self.ops_size,
        }
    }
}

pub(crate) struct OpTreeStats {
    pub(crate) index_size: usize,
    pub(crate) ops_size: usize,
}


impl<'a> TreeQuery<'a> for StatsQuery {
    fn query_node(
        &mut self,
        child: &'a crate::op_tree::OpTreeNode,
        _ops: &'a [crate::types::Op],
    ) -> super::QueryResult {
        self.index_size += child.index.get_size();
        super::QueryResult::Descend
    }

    fn query_element(&mut self, element: &'a crate::types::Op) -> super::QueryResult {
        self.ops_size += element.get_size();
        super::QueryResult::Next
    }
}
