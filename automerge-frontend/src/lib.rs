use automerge_protocol::{
    ActorID, ChangeHash, MapType, ObjectID, Op, OpID, Patch, UncompressedChange,
};

mod error;
mod mutation;
mod path;
mod state_tree;
mod value;

pub use error::{
    AutomergeFrontendError, InvalidChangeRequest, InvalidInitialStateError, InvalidPatch,
};
pub use mutation::{LocalChange, MutableDocument};
pub use path::Path;
use path::PathElement;
use state_tree::ResolvedPath;
use std::collections::HashMap;
use std::convert::TryFrom;
use std::error::Error;
use std::time;
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
/// a copy of the local state (state being an instance of [StateTree]) and
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
        reconciled_root_state: state_tree::StateTree,
        optimistically_updated_root_state: state_tree::StateTree,
        max_op: u64,
    },
    Reconciled {
        root_state: state_tree::StateTree,
        max_op: u64,
        deps_of_last_received_patch: Vec<ChangeHash>,
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
    ) -> Result<(Option<Value>, Self), InvalidPatch> {
        match self {
            FrontendState::WaitingForInFlightRequests {
                in_flight_requests,
                reconciled_root_state,
                optimistically_updated_root_state,
                max_op,
            } => {
                let mut new_in_flight_requests = in_flight_requests;
                // If the actor ID and seq exist then this patch corresponds
                // to a local change (i.e it came from Backend::apply_local_change
                // so we don't need to apply it, we just need to remove it from
                // the in_flight_requests vector
                if let (Some(patch_actor), Some(patch_seq)) = (&patch.actor, patch.seq) {
                    // If this is a local change corresponding to our actor then we
                    // need to match it against in flight requests
                    if self_actor == patch_actor {
                        // Check that if the patch is for our actor ID then it is not
                        // out of order
                        if new_in_flight_requests[0] != patch_seq {
                            return Err(InvalidPatch::MismatchedSequenceNumber {
                                expected: new_in_flight_requests[0],
                                actual: patch_seq,
                            });
                        }
                        // unwrap should be fine here as `in_flight_requests` should never have zero length
                        // because we transition to reconciled state when that happens
                        let (_, remaining_requests) = new_in_flight_requests.split_first().unwrap();
                        new_in_flight_requests = remaining_requests.iter().copied().collect();
                    }
                }
                let new_reconciled_root_state = if let Some(diff) = &patch.diffs {
                    reconciled_root_state.apply_diff(diff)?
                } else {
                    reconciled_root_state
                };
                Ok(match new_in_flight_requests[..] {
                    [] => (
                        Some(new_reconciled_root_state.value()),
                        FrontendState::Reconciled {
                            root_state: new_reconciled_root_state,
                            max_op: patch.max_op,
                            deps_of_last_received_patch: patch.deps.clone(),
                        },
                    ),
                    _ => (
                        None,
                        FrontendState::WaitingForInFlightRequests {
                            in_flight_requests: new_in_flight_requests,
                            reconciled_root_state: new_reconciled_root_state,
                            optimistically_updated_root_state,
                            max_op,
                        },
                    ),
                })
            }
            FrontendState::Reconciled { root_state, .. } => {
                let new_root_state = if let Some(diff) = &patch.diffs {
                    root_state.apply_diff(diff)?
                } else {
                    root_state
                };
                Ok((
                    Some(new_root_state.value()),
                    FrontendState::Reconciled {
                        root_state: new_root_state,
                        max_op: patch.max_op,
                        deps_of_last_received_patch: patch.deps.clone(),
                    },
                ))
            }
        }
    }

    fn get_object_id(&self, path: &Path) -> Option<ObjectID> {
        self.resolve_path(path).and_then(|r| r.object_id())
    }

    fn get_value(&self, path: &Path) -> Option<Value> {
        self.resolve_path(path).map(|r| r.default_value())
    }

    fn resolve_path(&self, path: &Path) -> Option<ResolvedPath> {
        let root = match self {
            FrontendState::WaitingForInFlightRequests {
                optimistically_updated_root_state,
                ..
            } => optimistically_updated_root_state,
            FrontendState::Reconciled { root_state, .. } => root_state,
        };
        root.resolve_path(path)
    }

    /// Apply a patch. The change closure will be passed a `MutableDocument`
    /// which it can use to query the document state and make changes. It
    /// can also throw an error of type `E`. If an error is thrown in the
    /// closure no chnages are made and the error is returned.
    pub fn optimistically_apply_change<F, E>(
        self,
        actor: &ActorID,
        change_closure: F,
        seq: u64,
    ) -> Result<OptimisticChangeResult, E>
    where
        E: Error,
        F: FnOnce(&mut dyn MutableDocument) -> Result<(), E>,
    {
        match self {
            FrontendState::WaitingForInFlightRequests {
                mut in_flight_requests,
                reconciled_root_state,
                optimistically_updated_root_state,
                max_op,
            } => {
                let mut mutation_tracker = mutation::MutationTracker::new(
                    optimistically_updated_root_state,
                    max_op,
                    actor.clone(),
                );
                change_closure(&mut mutation_tracker)?;
                let new_root_state = mutation_tracker.state.clone();
                let new_value = new_root_state.value();
                in_flight_requests.push(seq);
                Ok(OptimisticChangeResult {
                    ops: mutation_tracker.ops(),
                    new_state: FrontendState::WaitingForInFlightRequests {
                        in_flight_requests,
                        optimistically_updated_root_state: new_root_state,
                        reconciled_root_state,
                        max_op: mutation_tracker.max_op,
                    },
                    new_value,
                    deps: Vec::new(),
                })
            }
            FrontendState::Reconciled {
                root_state,
                max_op,
                deps_of_last_received_patch,
            } => {
                let mut mutation_tracker =
                    mutation::MutationTracker::new(root_state.clone(), max_op, actor.clone());
                change_closure(&mut mutation_tracker)?;
                let new_root_state = mutation_tracker.state.clone();
                let new_value = new_root_state.value();
                let in_flight_requests = vec![seq];
                Ok(OptimisticChangeResult {
                    ops: mutation_tracker.ops(),
                    new_state: FrontendState::WaitingForInFlightRequests {
                        in_flight_requests,
                        optimistically_updated_root_state: new_root_state,
                        reconciled_root_state: root_state,
                        max_op: mutation_tracker.max_op,
                    },
                    new_value,
                    deps: deps_of_last_received_patch,
                })
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

    fn max_op(&self) -> u64 {
        match self {
            FrontendState::WaitingForInFlightRequests { max_op, .. } => *max_op,
            FrontendState::Reconciled { max_op, .. } => *max_op,
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
        let root_state = state_tree::StateTree::new();
        Frontend {
            actor_id: ActorID::random(),
            seq: 0,
            state: Some(FrontendState::Reconciled {
                root_state,
                max_op: 0,
                deps_of_last_received_patch: Vec::new(),
            }),
            cached_value: Value::Map(HashMap::new(), MapType::Map),
        }
    }

    pub fn new_with_initial_state(
        initial_state: Value,
    ) -> Result<(Self, UncompressedChange), InvalidInitialStateError> {
        match &initial_state {
            Value::Map(kvs, MapType::Map) => {
                let mut front = Frontend::new();
                let (init_ops, _) =
                    kvs.iter()
                        .fold((Vec::new(), 1), |(mut ops, max_op), (k, v)| {
                            let (more_ops, max_op) = value::value_to_op_requests(
                                &front.actor_id,
                                max_op,
                                ObjectID::Root,
                                &k.into(),
                                v,
                                false,
                            );
                            ops.extend(more_ops);
                            (ops, max_op)
                        });

                let init_change_request = UncompressedChange {
                    actor_id: front.actor_id.clone(),
                    start_op: 1,
                    time: system_time().unwrap_or(0),
                    seq: 1,
                    message: Some("Initialization".to_string()),
                    deps: Vec::new(),
                    operations: init_ops,
                    extra_bytes: Vec::new(),
                };
                // Unwrap here is fine because it should be impossible to
                // cause an error applying a local change from a `Value`. If
                // that happens we've made an error, not the user.
                front.change(Some("initialization".into()), |doc| {
                    doc.add_change(LocalChange::set(Path::root(), initial_state))
                        .map_err(|_| InvalidInitialStateError::InitialStateMustBeMap)
                })?;
                Ok((front, init_change_request))
            }
            _ => Err(InvalidInitialStateError::InitialStateMustBeMap),
        }
    }

    pub fn state(&self) -> &Value {
        &self.cached_value
    }

    pub fn change<F, E>(
        &mut self,
        message: Option<String>,
        change_closure: F,
    ) -> Result<Option<UncompressedChange>, E>
    where
        E: Error,
        F: FnOnce(&mut dyn MutableDocument) -> Result<(), E>,
    {
        let start_op = self.state.as_ref().unwrap().max_op() + 1;
        // TODO this leaves the `state` as `None` if there's an error, it shouldn't
        let change_result = self.state.take().unwrap().optimistically_apply_change(
            &self.actor_id,
            change_closure,
            self.seq + 1,
        )?;
        self.state = Some(change_result.new_state);
        if let Some(ops) = change_result.ops {
            self.seq += 1;
            self.cached_value = change_result.new_value;
            let change = UncompressedChange {
                start_op,
                actor_id: self.actor_id.clone(),
                seq: self.seq,
                time: system_time().unwrap_or(0),
                message,
                deps: change_result.deps,
                operations: ops,
                extra_bytes: Vec::new(),
            };
            Ok(Some(change))
        } else {
            Ok(None)
        }
    }

    pub fn apply_patch(&mut self, patch: Patch) -> Result<(), InvalidPatch> {
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
            .and_then(|s| s.resolve_path(path))
            .map(|o| o.values())
    }

    pub fn get_value(&self, path: &Path) -> Option<Value> {
        self.state.as_ref().and_then(|s| s.get_value(path))
    }

    /// Returns the value given by path, if it exists
    pub fn value_at_path(&self, path: &Path) -> Option<Value> {
        self.state
            .as_ref()
            .and_then(|s| s.resolve_path(&path))
            .map(|o| o.default_value())
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

struct OptimisticChangeResult {
    ops: Option<Vec<Op>>,
    new_state: FrontendState,
    new_value: Value,
    deps: Vec<ChangeHash>,
}
