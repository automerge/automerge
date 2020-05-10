//! This module contains types which are deserialized from the changes which
//! are produced by the automerge JS library. Given the following code
//!
//! ```javascript
//! doc = ... // create and edit an automerge document
//! let changes = Automerge.getHistory(doc).map(h => h.change)
//! console.log(JSON.stringify(changes, null, 4))
//! ```
//!

use serde::{Deserialize, Serialize};

use crate::helper;
use automerge_protocol::{ActorID, ChangeHash, OpID, ObjectID, Key, OpRequest, OpType, Operation};


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

#[derive(Deserialize, Serialize, PartialEq, Debug, Clone)]
pub struct Change {
    #[serde(rename = "ops")]
    pub operations: Vec<Operation>,
    #[serde(rename = "actor")]
    pub actor_id: ActorID,
    pub hash: ChangeHash,
    pub seq: u64,
    #[serde(rename = "startOp")]
    pub start_op: u64,
    pub time: i64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
    pub deps: Vec<ChangeHash>,
}

impl Change {
    pub fn max_op(&self) -> u64 {
        self.start_op + (self.operations.len() as u64) - 1
    }
}

#[derive(Deserialize, PartialEq, Debug, Clone)]
#[serde(rename_all = "camelCase")]
pub struct ChangeRequest {
    pub actor: ActorID,
    pub seq: u64,
    pub version: u64,
    pub message: Option<String>,
    #[serde(default = "helper::make_true")]
    pub undoable: bool,
    pub time: Option<i64>,
    pub deps: Option<Vec<ChangeHash>>,
    pub ops: Option<Vec<OpRequest>>,
    pub child: Option<String>,
    pub request_type: ChangeRequestType,
}

fn _true() -> bool {
    true
}

#[derive(Deserialize, PartialEq, Debug, Clone)]
#[serde(rename_all = "camelCase")]
pub enum ChangeRequestType {
    Change,
    Undo,
    Redo,
}
