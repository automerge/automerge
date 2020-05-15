use automerge_protocol::{ActorID, ChangeRequest, ChangeRequestType, ObjectID, OpRequest, Patch};

mod change_context;
mod error;
mod mutation;
mod object;
mod value;

pub use error::{AutomergeFrontendError, InvalidInitialStateError};
use mutation::PathElement;
pub use mutation::{LocalChange, MutableDocument, Path};
use object::Object;
use std::convert::TryFrom;
use std::time;
use std::{collections::HashMap, rc::Rc};
pub use value::{Conflicts, MapType, SequenceType, Value};

/// Tracks the possible states of the frontend
///
/// What does this mean and why do we need it? The reason the frontend/backend
/// split exists in the first place is that we want to quickly apply local
/// changes (local in this sense means something like "on the UI thread") on a
/// low latency local cache whilst also shipping those same changes off to a
/// backend, which can reconcile them with historical changes and new changes
/// received over the network - work which may be more compute intensive and
/// so have to high a latency to be acceptable on the UI thread.
///
/// This frontend/backend split implies that we need to optimistically apply
/// local changes somehow. In order to do this we immediately apply changes to
/// a copy of the local state (state being the HashMap<ObjectID, Object>) and
/// add the sequence number of the new change to a list of in flight requests.
/// In detail the logic looks like this:
///
/// When we receive a patch from the backend:
/// 1. Check that if the patch is for our actor ID then the sequence number of
///    the patch is the same as the sequence number of the oldest in flight
///    request.
/// 2. Apply the patch to the `reconciled_state` of the current state
/// 3. If there are no in flight requests remaining then transition from
///    the `WaitingForInFlightRequests` state to the `Reconciled` state,
///    moving the `reconciled_state` into the `Reconciled` enum branch
#[derive(Clone)]
enum FrontendState {
    WaitingForInFlightRequests {
        in_flight_requests: Vec<u64>,
        reconciled_objects: HashMap<ObjectID, Rc<Object>>,
        optimistically_updated_objects: HashMap<ObjectID, Rc<Object>>,
    },
    Reconciled {
        objects: HashMap<ObjectID, Rc<Object>>,
    },
}

impl FrontendState {
    /// Apply a patch received from the backend to this frontend state,
    /// returns the updated cached value and a new `FrontendState` which
    /// replaces this one
    fn apply_remote_patch(
        self,
        self_actor: &ActorID,
        patch: &Patch,
    ) -> Result<(Value, Self), AutomergeFrontendError> {
        match self {
            FrontendState::WaitingForInFlightRequests {
                in_flight_requests,
                mut reconciled_objects,
                optimistically_updated_objects,
            } => {
                // Check that if the patch is for out actor ID then it is not
                // out of order
                if let (Some(patch_actor), Some(patch_seq)) = (&patch.actor, patch.seq) {
                    if self_actor == &ActorID::from(patch_actor.as_str())
                        && in_flight_requests[0] != patch_seq
                    {
                        return Err(AutomergeFrontendError::MismatchedSequenceNumber);
                    }
                }
                let mut change_ctx = change_context::ChangeContext::new(&mut reconciled_objects);
                if let Some(diff) = &patch.diffs {
                    change_ctx.apply_diff(&diff)?;
                }
                let new_state = change_ctx.commit()?;
                // unwrap should be fine here as `in_flight_requests` should never have zero lenght
                // given the following code
                let (_, new_in_flight_requests) = in_flight_requests.split_first().unwrap();
                Ok((
                    new_state,
                    match new_in_flight_requests[..] {
                        [] => FrontendState::Reconciled {
                            objects: reconciled_objects,
                        },
                        _ => FrontendState::WaitingForInFlightRequests {
                            in_flight_requests: new_in_flight_requests.into(),
                            reconciled_objects,
                            optimistically_updated_objects,
                        },
                    },
                ))
            }
            FrontendState::Reconciled { mut objects } => {
                let mut change_ctx = change_context::ChangeContext::new(&mut objects);
                if let Some(diff) = &patch.diffs {
                    change_ctx.apply_diff(&diff)?;
                };
                let new_state = change_ctx.commit()?;
                Ok((new_state, FrontendState::Reconciled { objects }))
            }
        }
    }

    fn get_object_id(&self, path: &Path) -> Option<ObjectID> {
        let objects = match self {
            FrontendState::WaitingForInFlightRequests {
                optimistically_updated_objects,
                ..
            } => optimistically_updated_objects,
            FrontendState::Reconciled { objects, .. } => objects,
        };
        mutation::resolve_path(path, objects).and_then(|o| o.id())
    }

    /// Apply a patch
    pub fn optimistically_apply_change<F>(
        self,
        change_closure: F,
        seq: u64,
    ) -> Result<(Option<Vec<OpRequest>>, FrontendState, Value), AutomergeFrontendError>
    where
        F: FnOnce(&mut dyn MutableDocument) -> Result<(), AutomergeFrontendError>,
    {
        match self {
            FrontendState::WaitingForInFlightRequests {
                mut in_flight_requests,
                reconciled_objects,
                mut optimistically_updated_objects,
            } => {
                let mut change_ctx =
                    change_context::ChangeContext::new(&mut optimistically_updated_objects);
                let mut mutation_tracker = mutation::MutationTracker::new(&mut change_ctx);
                change_closure(&mut mutation_tracker)?;
                let ops = mutation_tracker.ops();
                let new_value = change_ctx.commit()?;
                in_flight_requests.push(seq);
                Ok((
                    ops,
                    FrontendState::WaitingForInFlightRequests {
                        in_flight_requests,
                        optimistically_updated_objects,
                        reconciled_objects,
                    },
                    new_value,
                ))
            }
            FrontendState::Reconciled { objects } => {
                let mut optimistically_updated_objects = objects.clone();
                let mut change_ctx =
                    change_context::ChangeContext::new(&mut optimistically_updated_objects);
                let mut mutation_tracker = mutation::MutationTracker::new(&mut change_ctx);
                change_closure(&mut mutation_tracker)?;
                let ops = mutation_tracker.ops();
                let new_value = change_ctx.commit()?;
                let in_flight_requests = vec![seq];
                Ok((
                    ops,
                    FrontendState::WaitingForInFlightRequests {
                        in_flight_requests,
                        optimistically_updated_objects,
                        reconciled_objects: objects,
                    },
                    new_value,
                ))
            }
        }
    }
}

pub struct Frontend {
    pub actor_id: ActorID,
    pub seq: u64,
    /// The current state of the frontend, see the description of
    /// `FrontendState` for details. It's an `Option` to allow consuming it
    /// using Option::take whilst behind a mutable reference.
    state: Option<FrontendState>,
    /// The highest version number we've received from the backend
    version: u64,
    /// A cache of the value of this frontend
    cached_value: Value,
}

impl Default for Frontend {
    fn default() -> Self {
        Self::new()
    }
}

impl Frontend {
    pub fn new() -> Self {
        let mut objects = HashMap::new();
        objects.insert(
            ObjectID::Root,
            Rc::new(Object::Map(ObjectID::Root, HashMap::new(), MapType::Map)),
        );
        Frontend {
            actor_id: ActorID::random(),
            seq: 0,
            state: Some(FrontendState::Reconciled { objects }),
            version: 0,
            cached_value: Value::Map(HashMap::new(), MapType::Map),
        }
    }

    pub fn new_with_initial_state(
        initial_state: Value,
    ) -> Result<(Self, ChangeRequest), InvalidInitialStateError> {
        match &initial_state {
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
                        .0
                    })
                    .collect();
                let mut front = Frontend::new();

                let init_change_request = ChangeRequest {
                    actor: front.actor_id.clone(),
                    time: system_time(),
                    seq: 1,
                    version: 0,
                    message: Some("Initialization".to_string()),
                    undoable: false,
                    deps: None,
                    ops: Some(init_ops),
                    request_type: ChangeRequestType::Change,
                };
                // Unwrap here is fine because it should be impossible to
                // cause an error applying a local change from a `Value`. If
                // that happens we've made an error, not the user.
                front
                    .change(Some("initialization".into()), |doc| {
                        doc.add_change(LocalChange::set(Path::root(), initial_state))
                    })
                    .unwrap(); // This should never error
                Ok((front, init_change_request))
            }
            _ => Err(InvalidInitialStateError::InitialStateMustBeMap),
        }
    }

    pub fn state(&self) -> &Value {
        &self.cached_value
    }

    pub fn change<F>(
        &mut self,
        message: Option<String>,
        change_closure: F,
    ) -> Result<Option<ChangeRequest>, AutomergeFrontendError>
    where
        F: FnOnce(&mut dyn MutableDocument) -> Result<(), AutomergeFrontendError>,
    {
        // TODO this leaves the `state` as `None` if there's an error, it shouldn't
        let (ops, new_state, new_value) = self
            .state
            .take()
            .unwrap()
            .optimistically_apply_change(change_closure, self.seq + 1)?;
        self.state = Some(new_state);
        if ops.is_none() {
            return Ok(None);
        }
        self.seq += 1;
        self.cached_value = new_value;
        let change_request = ChangeRequest {
            actor: self.actor_id.clone(),
            seq: self.seq,
            time: system_time(),
            version: self.version,
            message,
            undoable: true,
            deps: None,
            ops,
            request_type: ChangeRequestType::Change,
        };
        Ok(Some(change_request))
    }

    pub fn apply_patch(&mut self, patch: Patch) -> Result<(), AutomergeFrontendError> {
        // TODO this leaves the `state` as `None` if there's an error, it shouldn't
        let (new_cached_value, new_state) = self
            .state
            .take()
            .unwrap()
            .apply_remote_patch(&self.actor_id, &patch)?;
        self.state = Some(new_state);
        self.cached_value = new_cached_value;
        self.version = std::cmp::max(self.version, patch.version);
        Ok(())
    }

    pub fn get_object_id(&self, path: &Path) -> Option<ObjectID> {
        // So gross, this is just a quick hack before refactoring to a proper
        // state machine
        self.state.clone().and_then(|s| s.get_object_id(path))
    }
}

fn system_time() -> Option<i64> {
    // TODO note this can fail as SystemTime is not monotonic, also
    // it's a system call so it's not no_std compatible. Finally,
    // it doesn't handle system times before 1970 (which should be
    // very rare one imagines).
    time::SystemTime::now()
        .duration_since(time::UNIX_EPOCH)
        .ok()
        .and_then(|d| i64::try_from(d.as_millis()).ok())
}
