use automerge_protocol::{OpID, ObjectID, Key, OpType, Operation};


#[derive(PartialEq, Debug, Clone)]
pub struct UndoOperation {
    pub action: OpType,
    pub obj: ObjectID,
    pub key: Key,
}

impl UndoOperation {
    pub fn into_operation(self, pred: Vec<OpID>) -> Operation {
        Operation {
            action: self.action,
            obj: self.obj,
            key: self.key,
            insert: false,
            pred,
        }
    }
}

