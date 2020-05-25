use crate::op::Operation;
use crate::op_type::OpType;
use automerge_protocol as amp;

#[derive(PartialEq, Debug, Clone)]
pub struct UndoOperation {
    pub action: OpType,
    pub obj: amp::ObjectID,
    pub key: amp::Key,
}

impl UndoOperation {
    pub fn into_operation(self, pred: Vec<amp::OpID>) -> Operation {
        Operation {
            action: self.action,
            obj: self.obj,
            key: self.key,
            insert: false,
            pred,
        }
    }
}
