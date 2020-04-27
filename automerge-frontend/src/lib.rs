use automerge_backend::{
    ActorID, Backend, ChangeRequest, ChangeRequestType, Clock, ObjectID, Patch
};

mod object;
mod change_context;
mod error;
mod value;
mod mutation;

use object::Object;
pub use error::{AutomergeFrontendError, InvalidInitialStateError};
use std::{cell::RefCell, collections::HashMap};
pub use value::{Conflicts, MapType, PrimitiveValue, SequenceType, Value};
pub use mutation::{MutableDocument, LocalChange, Path};
use mutation::PathElement;

pub struct Frontend {
    pub actor_id: ActorID,
    backend: Backend,
    objects: RefCell<HashMap<ObjectID, Object>>,
    state: Value,
}

impl Frontend {
    pub fn new() -> Self {
        let mut objects = HashMap::new();
        objects.insert(
            ObjectID::Root,
            Object::Map(ObjectID::Root, HashMap::new(), MapType::Map),
        );
        return Frontend {
            actor_id: ActorID::random(),
            backend: Backend::init(),
            objects: RefCell::new(objects),
            state: Value::Map(HashMap::new(), MapType::Map),
        };
    }

    pub fn new_with_initial_state(inital_state: Value) -> Result<Self, InvalidInitialStateError> {
        match inital_state {
            Value::Map(kvs, MapType::Map) => {
                let init_ops = kvs
                    .iter()
                    .flat_map(|(k, v)| {
                        value::value_to_op_requests(
                            ObjectID::Root.to_string(),
                            PathElement::Key(k.to_string()),
                            v,
                            false,
                        )
                    })
                    .collect();
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
                let patch = front
                    .backend
                    .apply_local_change(init_change_request)
                    .unwrap();
                front.apply_patch(patch).unwrap();
                Ok(front)
            }
            _ => Err(InvalidInitialStateError::InitialStateMustBeMap),
        }
    }

    pub fn state(&self) -> &Value {
        &self.state
    }

    pub fn change<F>(&mut self, change_closure: F) -> Result<Option<ChangeRequest>, AutomergeFrontendError>
    where
        F: FnOnce(&mut dyn MutableDocument) -> Result<(), AutomergeFrontendError>,
    {
        let mut change_ctx = change_context::ChangeContext::new(self.objects.get_mut());
        let mut mutation_tracker = mutation::MutationTracker::new(&mut change_ctx);
        change_closure(&mut mutation_tracker)?;
        let change_request = mutation_tracker.change_request();
        let new_state = change_ctx.commit()?;
        self.state = new_state;
        Ok(change_request)
    }

    pub fn apply_patch(&mut self, patch: Patch) -> Result<(), AutomergeFrontendError> {
        let objects = self.objects.get_mut();
        let mut change_ctx = change_context::ChangeContext::new(objects);
        if let Some(diff) = patch.diffs {
            change_ctx.apply_diff(&diff)?;
        };
        let new_state = change_ctx.commit()?;
        self.state = new_state;
        Ok(())
    }
}
