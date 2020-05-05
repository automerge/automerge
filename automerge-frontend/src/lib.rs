use automerge_backend::{
    ActorID, Backend, ChangeRequest, ChangeRequestType, Clock, ObjectID, Patch,
};

mod object;
mod change_context;
mod error;
mod value;
mod mutation;

use object::Object;
pub use error::{AutomergeFrontendError, InvalidInitialStateError};
use std::{cell::RefCell, collections::HashMap, rc::Rc};
pub use value::{Conflicts, MapType, PrimitiveValue, SequenceType, Value};
pub use mutation::{MutableDocument, LocalChange, Path};
use mutation::PathElement;

struct InFlightRequest {
    state_before: HashMap<ObjectID, Rc<Object>>,
    seq: u64
}

pub struct Frontend {
    pub actor_id: ActorID,
    pub seq: u64,
    backend: Backend,
    /// The current state of this frontend
    objects: RefCell<HashMap<ObjectID, Rc<Object>>>,
    /// The current state of this frontend as a `Value`
    cached_state: Value,
    /// Requests which we have optimistically applied but haven't yet 
    /// received a patch from the backend for
    in_flight_requests: Vec<InFlightRequest>,
    /// The highest version number we've received from the backend
    version: u64,
}

impl Frontend {
    pub fn new() -> Self {
        let mut objects = HashMap::new();
        objects.insert(
            ObjectID::Root,
            Rc::new(Object::Map(ObjectID::Root, HashMap::new(), MapType::Map)),
        );
        return Frontend {
            actor_id: ActorID::random(),
            seq: 0,
            backend: Backend::init(),
            objects: RefCell::new(objects),
            cached_state: Value::Map(HashMap::new(), MapType::Map),
            in_flight_requests: Vec::new(),
            version: 0,
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
                        ).0
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
        &self.cached_state
    }

    pub fn change<F>(&mut self, message: Option<String>, change_closure: F) -> Result<Option<ChangeRequest>, AutomergeFrontendError>
    where
        F: FnOnce(&mut dyn MutableDocument) -> Result<(), AutomergeFrontendError>,
    {
        let mut change_ctx = change_context::ChangeContext::new(self.objects.get_mut());
        let mut mutation_tracker = mutation::MutationTracker::new(&mut change_ctx);
        change_closure(&mut mutation_tracker)?;
        let maybe_change_request = match mutation_tracker.ops() {
            Some(ops) => {
                let new_state = change_ctx.commit()?;
                self.cached_state = new_state;
                self.seq += 1;
                let change_request = ChangeRequest{
                    actor: self.actor_id.clone(),
                    seq: self.seq,
                    version: self.version,
                    message,
                    undoable: false,
                    deps: None,
                    ops: Some(ops),
                    request_type: ChangeRequestType::Change,
                };
                Some(change_request)
            }
            None => None
        };
        Ok(maybe_change_request)
    }

    pub fn apply_patch(&mut self, patch: Patch) -> Result<(), AutomergeFrontendError> {
        let objects = self.objects.get_mut();
        let mut change_ctx = change_context::ChangeContext::new(objects);
        if let Some(diff) = patch.diffs {
            change_ctx.apply_diff(&diff)?;
        };
        let new_state = change_ctx.commit()?;
        self.cached_state = new_state;
        self.version = std::cmp::max(self.version, patch.version);
        Ok(())
    }
}
