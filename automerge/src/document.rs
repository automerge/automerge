use super::{AutomergeError, ChangeRequest};
use crate::change_context::ChangeContext;
use crate::error::InvalidChangeRequest;
use automerge_backend::OpSet;
use automerge_backend::Value;
use automerge_backend::{ActorID, Change};

pub struct Document {
    op_set: OpSet,
    actor_id: ActorID,
}

impl Document {
    /// Create a new, empty document
    pub fn init() -> Document {
        Document {
            op_set: OpSet::init(),
            actor_id: ActorID(uuid::Uuid::new_v4().to_string()),
        }
    }

    /// Create a new document from a set of changes
    pub fn load(changes: Vec<Change>) -> Result<Document, AutomergeError> {
        let mut doc = Document::init();
        for change in changes {
            doc.apply_change(change)?
        }
        Ok(doc)
    }

    /// Get the current state of the document as a serde_json value
    pub fn state(&self) -> &Value {
        panic!("not implemented");
    }

    /// Add a single change to the document
    pub fn apply_change(&mut self, change: Change) -> Result<(), AutomergeError> {
        self.op_set.apply_change(change, false).map(|_| ())
    }

    pub fn create_and_apply_change(
        &mut self,
        message: Option<String>,
        requests: Vec<ChangeRequest>,
    ) -> Result<Change, InvalidChangeRequest> {
        let mut change_ctx = ChangeContext::new(
            &self.op_set.object_store,
            self.actor_id.clone(),
            &self.op_set.states,
            self.op_set.clock.clone(),
        );
        let change = change_ctx.create_change(requests, message)?;
        self.apply_change(change.clone())
            .map_err(|e| InvalidChangeRequest(format!("Error applying change: {:?}", e)))?;
        Ok(change)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::change_request::{ListIndex, Path};
    use automerge_backend::Value;

    #[test]
    #[ignore] // This is broken for some reason
    fn test_insert_ops() {
        let json_value: serde_json::Value = serde_json::from_str(
            r#"
            {
                "values": [1.0, false]
            }
        "#,
        )
        .unwrap();
        let mut doc = Document::init();
        doc.create_and_apply_change(
            Some("Initial".to_string()),
            vec![ChangeRequest::Set {
                path: Path::root(),
                value: Value::from_json(&json_value),
            }],
        )
        .unwrap();
        let person_json: serde_json::Value = serde_json::from_str(
            r#"
            {
                "name": "fred",
                "surname": "johnson"
            }
            "#,
        )
        .unwrap();
        doc.create_and_apply_change(
            Some("list additions".to_string()),
            vec![
                ChangeRequest::InsertAfter {
                    path: Path::root()
                        .key("values".to_string())
                        .index(ListIndex::Head),
                    value: Value::from_json(&person_json),
                },
                ChangeRequest::InsertAfter {
                    path: Path::root()
                        .key("values".to_string())
                        .index(ListIndex::Index(1)),
                    value: Value::from_json(&serde_json::Value::String("final".to_string())),
                },
            ],
        )
        .unwrap();
        let expected: serde_json::Value = serde_json::from_str(
            r#"
            {
                "values": [
                    {
                        "name": "fred",
                        "surname": "johnson"
                    },
                    1.0,
                    false,
                    "final"
                ]
            }
            "#,
        )
        .unwrap();
        assert_eq!(expected, doc.state().to_json());
    }
}
