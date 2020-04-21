use automerge_backend::{
    ActorID, ChangeRequest, ChangeRequestType, Clock, DataType, ObjectID, Patch, PrimitiveValue,
    RequestKey, Backend
};

mod change_context;
mod error;
mod value;

pub use error::{AutomergeFrontendError, InvalidInitialStateError};
pub use value::{MapType, SequenceType, Value};

pub struct Frontend {
    actor_id: ActorID,
    backend: Backend,
    state: Value,
}

impl Frontend {
    pub fn new() -> Self {
        return Frontend {
            actor_id: ActorID::random(),
            backend: Backend::init(),
            state: Value::Primitive(PrimitiveValue::Null, DataType::Undefined),
        };
    }

    pub fn new_with_initial_state(inital_state: Value) -> Result<Self, InvalidInitialStateError> {
        match inital_state {
            Value::Map(kvs, MapType::Map) => {
                let init_ops = kvs.iter().flat_map(|(k, v)| {
                    change_context::value_to_op_requests(
                        ObjectID::Root.to_string(),
                        PathElement::Key(k.to_string()),
                        v,
                        false,
                    )
                }).collect();
                let mut front = Frontend::new();
                let init_change_request = ChangeRequest {
                    actor: front.actor_id.clone(),
                    seq: 1,
                    version: 0,
                    message: Some("Initialization".to_string()),
                    undoable: false,
                    deps: Some(Clock::empty()),
                    ops: Some(init_ops),
                    request_type: ChangeRequestType::Change,
                };
                // Unwrap here is fine because it should be impossible to
                // cause an error applying a local change from a `Value`. If
                // that happens we've made an error, not the user.
                let patch = front.backend.apply_local_change(init_change_request).unwrap(); 
                front.apply_patch(patch).unwrap();
                Ok(front)
            }
            _ => Err(InvalidInitialStateError::InitialStateMustBeMap),
        }
    }

    pub fn state(&self) -> &Value {
        &self.state
    }

    pub fn apply_patch(&mut self, patch: Patch) -> Result<(), AutomergeFrontendError> {
        let mut change_ctx = change_context::ChangeContext::new();
        if let Some(diff) = patch.diffs {
            change_ctx.apply_diff(&diff)?;
        };
        let new_state = change_ctx.value_for_object(&ObjectID::Root).unwrap();
        self.state = new_state;
        Ok(())
    }
}

enum PathElement {
    Key(String),
    Index(usize),
}

impl PathElement {
    pub(crate) fn to_request_key(&self) -> RequestKey {
        match self {
            PathElement::Key(s) => RequestKey::Str(s.into()),
            PathElement::Index(i) => RequestKey::Num(*i as u64),
        }
    }
}

//enum LocalOperation {
    //Set(Value),
    //Delete,
//}

//struct LocalChange {
    //path: Vec<PathElement>,
    //operation: LocalOperation,
//}

//#[cfg(test)]
//mod tests {}
