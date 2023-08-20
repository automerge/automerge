use crate::op_tree::OpTreeNode;
use crate::query::OpSetMetadata;
use crate::query::{QueryResult, TreeQuery};
use crate::types::{Key, Op, OpId};
use crate::OpType;
use crate::ScalarValue;
use std::cmp::Ordering;
use std::fmt::Debug;

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct MapMove<'a> {
    key: Key,
    pub(crate) pos: usize,
    // predecessors of the move operation
    pub(crate) src_pred: Vec<&'a Op>,
    // positions of the predecessors
    pub(crate) src_pred_pos: Vec<usize>,
    // id of the moved element
    pub(crate) target_move_id: Option<OpId>,
    // None if it moves a scalar value
    pub(crate) scalar_value: Option<ScalarValue>,
}

impl<'a> MapMove<'a> {
    pub(crate) fn new(key: Key) -> Self {
        MapMove {
            key,
            src_pred: vec![],
            src_pred_pos: vec![],
            pos: 0,
            target_move_id: None,
            scalar_value: None,
        }
    }
}

impl<'a> TreeQuery<'a> for MapMove<'a> {
    fn query_node_with_metadata(
        &mut self,
        child: &OpTreeNode,
        m: &OpSetMetadata,
        ops: &[Op],
    ) -> QueryResult {
        let cmp = m.key_cmp(&ops[child.last()].key, &self.key);
        if cmp == Ordering::Less || (cmp == Ordering::Equal && !child.index.has_visible(&self.key))
        {
            self.pos += child.len();
            QueryResult::Next
        } else {
            QueryResult::Descend
        }
    }

    fn query_element_with_metadata(&mut self, element: &'a Op, m: &OpSetMetadata) -> QueryResult {
        match m.key_cmp(&element.key, &self.key) {
            Ordering::Greater => QueryResult::Finish,
            Ordering::Equal => {
                if element.visible() {
                    self.src_pred.push(element);
                    self.src_pred_pos.push(self.pos);
                    match &element.action {
                        OpType::Make(_) => {
                            self.target_move_id = Some(element.id);
                        }
                        OpType::Put(val) => {
                            self.target_move_id = Some(element.id);
                            self.scalar_value = Some(val.clone());
                        }
                        OpType::Move(scalar, _) => {
                            self.target_move_id = element.move_id.map(|id| id.0);
                            self.scalar_value = Some(scalar.clone());
                        }
                        // TODO: check if we need to handle other cases
                        _ => {}
                    }
                }
                self.pos += 1;
                QueryResult::Next
            }
            Ordering::Less => {
                self.pos += 1;
                QueryResult::Next
            }
        }
    }
}
