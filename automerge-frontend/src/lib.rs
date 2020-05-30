use automerge_protocol::{ActorID, MapType, ObjectID, Op, OpID, Patch, Request, RequestType};

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
pub use value::{Conflicts, Value};

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
#[derive(Clone, Debug)]
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
    /// returns the updated cached value (if it has changed) and a new
    /// `FrontendState` which replaces this one
    fn apply_remote_patch(
        self,
        self_actor: &ActorID,
        patch: &Patch,
    ) -> Result<(Option<Value>, Self), AutomergeFrontendError> {
        match self {
            FrontendState::WaitingForInFlightRequests {
                in_flight_requests,
                mut reconciled_objects,
                optimistically_updated_objects,
            } => {
                let mut new_in_flight_requests = in_flight_requests;
                // If the actor ID and seq exist then this is patch corresponds
                // to a local change (i.e it came from Backend::apply_local_change
                if let (Some(patch_actor), Some(patch_seq)) = (&patch.actor, patch.seq) {
                    // If this is a local change corresponding to our actor then we
                    // need to match it against in flight requests
                    if self_actor == patch_actor {
                        // Check that if the patch is for our actor ID then it is not
                        // out of order
                        if new_in_flight_requests[0] != patch_seq {
                            return Err(AutomergeFrontendError::MismatchedSequenceNumber);
                        }
                        // unwrap should be fine here as `in_flight_requests` should never have zero length
                        // because we transition to reconciled state when that happens
                        let (_, remaining_requests) = new_in_flight_requests.split_first().unwrap();
                        new_in_flight_requests = remaining_requests.iter().copied().collect();
                    }
                }
                let mut change_ctx = change_context::ChangeContext::new(&mut reconciled_objects);
                if let Some(diff) = &patch.diffs {
                    change_ctx.apply_diff(&diff)?;
                }
                let new_cached_state = change_ctx.commit()?;
                Ok(match new_in_flight_requests[..] {
                    [] => (
                        Some(new_cached_state),
                        FrontendState::Reconciled {
                            objects: reconciled_objects,
                        },
                    ),
                    _ => (
                        None,
                        FrontendState::WaitingForInFlightRequests {
                            in_flight_requests: new_in_flight_requests,
                            reconciled_objects,
                            optimistically_updated_objects,
                        },
                    ),
                })
            }
            FrontendState::Reconciled { mut objects } => {
                let mut change_ctx = change_context::ChangeContext::new(&mut objects);
                if let Some(diff) = &patch.diffs {
                    change_ctx.apply_diff(&diff)?;
                };
                let new_state = change_ctx.commit()?;
                Ok((Some(new_state), FrontendState::Reconciled { objects }))
            }
        }
    }

    fn get_object_id(&self, path: &Path) -> Option<ObjectID> {
        self.get_object(path).and_then(|o| o.id())
    }

    fn get_object(&self, path: &Path) -> Option<Rc<Object>> {
        let objects = match self {
            FrontendState::WaitingForInFlightRequests {
                optimistically_updated_objects,
                ..
            } => optimistically_updated_objects,
            FrontendState::Reconciled { objects, .. } => objects,
        };
        mutation::resolve_path(path, objects)
    }

    /// Apply a patch
    pub fn optimistically_apply_change<F>(
        self,
        change_closure: F,
        seq: u64,
    ) -> Result<(Option<Vec<Op>>, FrontendState, Value), AutomergeFrontendError>
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

    fn in_flight_requests(&self) -> Vec<u64> {
        match self {
            FrontendState::WaitingForInFlightRequests {
                in_flight_requests, ..
            } => in_flight_requests.clone(),
            _ => Vec::new(),
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
    pub version: u64,
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
    ) -> Result<(Self, Request), InvalidInitialStateError> {
        match &initial_state {
            Value::Map(kvs, MapType::Map) => {
                let init_ops = kvs
                    .iter()
                    .flat_map(|(k, v)| {
                        value::value_to_op_requests(
                            ObjectID::Root,
                            PathElement::Key(k.to_string()),
                            v,
                            false,
                        )
                        .0
                    })
                    .collect();
                let mut front = Frontend::new();

                let init_change_request = Request {
                    actor: front.actor_id.clone(),
                    time: system_time(),
                    seq: 1,
                    version: 0,
                    message: Some("Initialization".to_string()),
                    undoable: false,
                    deps: None,
                    ops: Some(init_ops),
                    request_type: RequestType::Change,
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
    ) -> Result<Option<Request>, AutomergeFrontendError>
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
        let change_request = Request {
            actor: self.actor_id.clone(),
            seq: self.seq,
            time: system_time(),
            version: self.version,
            message,
            undoable: true,
            deps: None,
            ops,
            request_type: RequestType::Change,
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
        if let Some(new_cached_value) = new_cached_value {
            self.cached_value = new_cached_value;
        };
        self.version = std::cmp::max(self.version, patch.version);
        if let Some(seq) = patch.clock.get(&self.actor_id) {
            if *seq > self.seq {
                self.seq = *seq;
            }
        }
        Ok(())
    }

    pub fn get_object_id(&self, path: &Path) -> Option<ObjectID> {
        self.state.as_ref().and_then(|s| s.get_object_id(path))
    }

    pub fn in_flight_requests(&self) -> Vec<u64> {
        self.state
            .as_ref()
            .map(|s| s.in_flight_requests())
            .unwrap_or_default()
    }

    /// Gets the set of values for `path`, returns None if the path does not
    /// exist
    pub fn get_conflicts(&self, path: &Path) -> Option<HashMap<OpID, Value>> {
        self.state
            .as_ref()
            .and_then(|s| s.get_object(&path.parent()))
            .and_then(|o| match (&*o, path.name()) {
                (Object::Map(_, vals, _), Some(PathElement::Key(k))) => {
                    vals.get(k.as_str()).map(|values| values.conflicts())
                }
                (Object::Sequence(_, vals, _), Some(PathElement::Index(i))) => vals
                    .get(*i)
                    .and_then(|mvalues| mvalues.as_ref().map(|values| values.conflicts())),
                _ => None,
            })
    }

    pub fn get_value(&self, path: &Path) -> Option<Value> {
        self.state
            .as_ref()
            .and_then(|s| s.get_object(path))
            .map(|o| o.value())
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
