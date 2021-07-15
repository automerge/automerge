use automerge_protocol as amp;
use automerge_protocol::{ActorId, ChangeHash, ObjectId, Op, OpId, Patch};
use value_ref::RootRef;

mod error;
mod mutation;
mod path;
mod state_tree;
mod value;
pub mod value_ref;

use std::{collections::HashMap, convert::TryFrom, error::Error, fmt::Debug};

pub use error::{
    AutomergeFrontendError, InvalidChangeRequest, InvalidInitialStateError, InvalidPatch,
};
pub use mutation::{LocalChange, MutableDocument};
pub use path::Path;
use path::PathElement;
use state_tree::ResolvedPath;
pub use value::{Conflicts, Cursor, Primitive, Value};

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
    /// The backend is processing some requests so we need to keep an optimistic version of the
    /// state.
    WaitingForInFlightRequests {
        /// The sequence numbers of in flight changes.
        in_flight_requests: Vec<u64>,
        /// The root state that the backend tracks.
        reconciled_root_state: state_tree::StateTree,
        /// The optimistic version of the root state that the user manipulates.
        optimistically_updated_root_state: state_tree::StateTree,
        /// A flag to track whether this state has seen a patch from the backend that represented
        /// changes from another actor.
        ///
        /// If this is true then our optimistic state will not equal the reconciled state so we may
        /// need to do extra work when moving to the reconciled state.
        seen_non_local_patch: bool,
        /// The maximum operation observed.
        max_op: u64,
    },
    /// The backend has processed all changes and we no longer wait for anything.
    Reconciled {
        /// The root state that the backend tracks.
        reconciled_root_state: state_tree::StateTree,
        /// A copy of the reconciled root state that we keep to be able to undo changes a user
        /// makes when changing the state.
        reconciled_root_state_copy_for_rollback: state_tree::StateTree,
        /// The maximum operation observed.
        max_op: u64,
        /// The dependencies of the last received patch.
        deps_of_last_received_patch: Vec<ChangeHash>,
    },
}

impl FrontendState {
    /// Apply a patch received from the backend to this frontend state,
    /// returns the updated cached value (if it has changed) and a new
    /// `FrontendState` which replaces this one
    fn apply_remote_patch(
        &mut self,
        self_actor: &ActorId,
        patch: Patch,
    ) -> Result<(), InvalidPatch> {
        match self {
            FrontendState::WaitingForInFlightRequests {
                in_flight_requests,
                reconciled_root_state,
                optimistically_updated_root_state,
                seen_non_local_patch,
                max_op: _,
            } => {
                let mut new_in_flight_requests = in_flight_requests.clone();
                // If the actor ID and seq exist then this patch corresponds
                // to a local change (i.e it came from Backend::apply_local_change
                // so we don't need to apply it, we just need to remove it from
                // the in_flight_requests vector
                let mut is_local = false;
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
                        is_local = true;
                        // unwrap should be fine here as `in_flight_requests` should never have zero length
                        // because we transition to reconciled state when that happens
                        let (_, remaining_requests) = new_in_flight_requests.split_first().unwrap();
                        new_in_flight_requests = remaining_requests.iter().copied().collect();
                    }
                }
                let checked_diff = reconciled_root_state.check_diff(patch.diffs)?;

                reconciled_root_state.apply_diff(checked_diff);
                if new_in_flight_requests.is_empty() {
                    if *seen_non_local_patch {
                        *optimistically_updated_root_state = reconciled_root_state.clone();
                    }
                    *self = FrontendState::Reconciled {
                        reconciled_root_state: std::mem::take(reconciled_root_state),
                        reconciled_root_state_copy_for_rollback: std::mem::take(
                            optimistically_updated_root_state,
                        ),
                        max_op: patch.max_op,
                        deps_of_last_received_patch: patch.deps,
                    }
                } else {
                    *in_flight_requests = new_in_flight_requests;
                    *seen_non_local_patch = *seen_non_local_patch || !is_local;
                    // don't update max_op as we have progressed since then
                }
                Ok(())
            }
            FrontendState::Reconciled {
                reconciled_root_state,
                reconciled_root_state_copy_for_rollback,
                max_op,
                deps_of_last_received_patch,
            } => {
                let checked_diff = reconciled_root_state.check_diff(patch.diffs)?;

                reconciled_root_state.apply_diff(checked_diff.clone());
                // quicker and cheaper to apply the diff again than to clone the large root state
                reconciled_root_state_copy_for_rollback.apply_diff(checked_diff);
                *max_op = patch.max_op;
                *deps_of_last_received_patch = patch.deps;
                Ok(())
            }
        }
    }

    fn get_object_id(&self, path: &Path) -> Option<ObjectId> {
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
            FrontendState::Reconciled {
                reconciled_root_state,
                ..
            } => reconciled_root_state,
        };
        root.resolve_path(path)
    }

    /// Apply a patch. The change closure will be passed a `MutableDocument`
    /// which it can use to query the document state and make changes. It
    /// can also throw an error of type `E`. If an error is thrown in the
    /// closure no chnages are made and the error is returned.
    pub fn optimistically_apply_change<F, O, E>(
        &mut self,
        actor: &ActorId,
        change_closure: F,
        seq: u64,
    ) -> Result<OptimisticChangeResult<O>, E>
    where
        E: Error,
        F: FnOnce(&mut dyn MutableDocument) -> Result<O, E>,
    {
        match self {
            FrontendState::WaitingForInFlightRequests {
                in_flight_requests,
                reconciled_root_state: _,
                optimistically_updated_root_state,
                seen_non_local_patch: _,
                max_op,
            } => {
                let mut mutation_tracker = mutation::MutationTracker::new(
                    optimistically_updated_root_state,
                    *max_op,
                    actor.clone(),
                );
                // TODO: somehow handle rolling back the optimistic state if the closure gives an
                // error
                let result = match change_closure(&mut mutation_tracker) {
                    Ok(result) => result,
                    Err(e) => {
                        // reset the original state
                        mutation_tracker.rollback();
                        return Err(e);
                    }
                };
                *max_op = mutation_tracker.max_op;
                let ops = mutation_tracker.ops();
                if !ops.is_empty() {
                    // we actually have made a change so expect it to be sent to the backend
                    in_flight_requests.push(seq);
                }

                Ok(OptimisticChangeResult {
                    ops,
                    deps: Vec::new(),
                    closure_result: result,
                })
            }
            FrontendState::Reconciled {
                reconciled_root_state,
                reconciled_root_state_copy_for_rollback,
                max_op,
                deps_of_last_received_patch,
            } => {
                let mut mutation_tracker = mutation::MutationTracker::new(
                    reconciled_root_state_copy_for_rollback,
                    *max_op,
                    actor.clone(),
                );
                let result = match change_closure(&mut mutation_tracker) {
                    Ok(result) => result,
                    Err(e) => {
                        // reset the original state
                        mutation_tracker.rollback();
                        return Err(e);
                    }
                };
                *max_op = mutation_tracker.max_op;
                let ops = mutation_tracker.ops();
                let in_flight_requests = vec![seq];
                let deps = deps_of_last_received_patch.clone();
                if !ops.is_empty() {
                    *self = FrontendState::WaitingForInFlightRequests {
                        in_flight_requests,
                        optimistically_updated_root_state: std::mem::take(
                            reconciled_root_state_copy_for_rollback,
                        ),
                        seen_non_local_patch: false,
                        reconciled_root_state: std::mem::take(reconciled_root_state),
                        max_op: *max_op,
                    }
                } else {
                    // the old and new states should be equal since we have no operations
                    debug_assert_eq!(
                        *reconciled_root_state_copy_for_rollback,
                        *reconciled_root_state
                    );
                    // we can remain in the reconciled frontend state since we didn't make a change
                };
                Ok(OptimisticChangeResult {
                    ops,
                    deps,
                    closure_result: result,
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

    fn value(&self) -> Value {
        match self {
            FrontendState::WaitingForInFlightRequests {
                optimistically_updated_root_state,
                ..
            } => optimistically_updated_root_state.value(),
            FrontendState::Reconciled {
                reconciled_root_state,
                ..
            } => reconciled_root_state.value(),
        }
    }

    fn value_ref(&self) -> RootRef {
        match self {
            FrontendState::WaitingForInFlightRequests {
                optimistically_updated_root_state,
                ..
            } => optimistically_updated_root_state.value_ref(),
            FrontendState::Reconciled {
                reconciled_root_state,
                ..
            } => reconciled_root_state.value_ref(),
        }
    }
}

pub struct Frontend {
    pub actor_id: ActorId,
    pub seq: u64,
    /// The current state of the frontend, see the description of
    /// `FrontendState` for details. It's an `Option` to allow consuming it
    /// using Option::take whilst behind a mutable reference.
    state: FrontendState,
    /// A cache of the value of this frontend
    cached_value: Option<Value>,
    /// A function for generating timestamps
    timestamper: Box<dyn Fn() -> Option<i64>>,
}

impl Debug for Frontend {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::result::Result<(), std::fmt::Error> {
        let Frontend {
            actor_id,
            seq,
            state,
            cached_value,
            timestamper: _,
        } = self;
        {
            let mut builder = f.debug_struct("Frontend");
            let _ = builder.field("actor_id", &actor_id);
            let _ = builder.field("seq", &seq);
            let _ = builder.field("state", &state);
            let _ = builder.field("cached_value", &cached_value);
            builder.finish()
        }
    }
}

#[cfg(feature = "std")]
impl Default for Frontend {
    fn default() -> Self {
        Self::new()
    }
}

impl Frontend {
    #[cfg(feature = "std")]
    pub fn new() -> Self {
        let system_time = || {
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .ok()
                .and_then(|d| i64::try_from(d.as_millis()).ok())
        };
        Self::new_with_timestamper(Box::new(system_time))
    }

    #[cfg(feature = "std")]
    pub fn new_with_actor_id(actor_id: &[u8]) -> Self {
        let system_time = || {
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .ok()
                .and_then(|d| i64::try_from(d.as_millis()).ok())
        };
        Self::new_with_timestamper_and_actor_id(Box::new(system_time), actor_id)
    }

    pub fn new_with_timestamper(t: Box<dyn Fn() -> Option<i64>>) -> Self {
        Self::new_with_timestamper_and_actor_id(t, uuid::Uuid::new_v4().as_bytes())
    }

    pub fn new_with_timestamper_and_actor_id(
        t: Box<dyn Fn() -> Option<i64>>,
        actor_id: &[u8],
    ) -> Self {
        let root_state = state_tree::StateTree::new();
        Frontend {
            actor_id: ActorId::from(actor_id),
            seq: 0,
            state: FrontendState::Reconciled {
                reconciled_root_state: root_state.clone(),
                reconciled_root_state_copy_for_rollback: root_state,
                max_op: 0,
                deps_of_last_received_patch: Vec::new(),
            },
            cached_value: None,
            timestamper: t,
        }
    }

    #[cfg(feature = "std")]
    pub fn new_with_initial_state(
        initial_state: Value,
    ) -> Result<(Self, amp::Change), InvalidInitialStateError> {
        match &initial_state {
            Value::Map(kvs) => {
                let mut front = Frontend::new();
                let (init_ops, _) =
                    kvs.iter()
                        .fold((Vec::new(), 1), |(mut ops, max_op), (k, v)| {
                            let (more_ops, max_op) = value::value_to_op_requests(
                                &front.actor_id,
                                max_op,
                                ObjectId::Root,
                                &amp::Key::Map(k.clone()),
                                v,
                                false,
                            );
                            ops.extend(more_ops);
                            (ops, max_op)
                        });

                let init_change_request = amp::Change {
                    actor_id: front.actor_id.clone(),
                    start_op: 1,
                    time: (front.timestamper)().unwrap_or(0),
                    seq: 1,
                    message: Some("Initialization".to_string()),
                    hash: None,
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

    pub fn state(&mut self) -> &Value {
        if let Some(ref v) = self.cached_value {
            v
        } else {
            let value = self.state.value();
            self.cached_value = Some(value);
            self.cached_value.as_ref().unwrap()
        }
    }

    pub fn value_ref(&self) -> RootRef {
        self.state.value_ref()
    }

    pub fn change<F, O, E>(
        &mut self,
        message: Option<String>,
        change_closure: F,
    ) -> Result<(O, Option<amp::Change>), E>
    where
        E: Error,
        F: FnOnce(&mut dyn MutableDocument) -> Result<O, E>,
    {
        let start_op = self.state.max_op() + 1;
        let change_result =
            self.state
                .optimistically_apply_change(&self.actor_id, change_closure, self.seq + 1)?;
        self.cached_value = None;
        if !change_result.ops.is_empty() {
            self.seq += 1;
            let change = amp::Change {
                start_op,
                actor_id: self.actor_id.clone(),
                seq: self.seq,
                time: (self.timestamper)().unwrap_or(0),
                message,
                hash: None,
                deps: change_result.deps,
                operations: change_result.ops,
                extra_bytes: Vec::new(),
            };
            Ok((change_result.closure_result, Some(change)))
        } else {
            Ok((change_result.closure_result, None))
        }
    }

    pub fn apply_patch(&mut self, patch: Patch) -> Result<(), InvalidPatch> {
        self.cached_value = None;
        if let Some(seq) = patch.clock.get(&self.actor_id) {
            if *seq > self.seq {
                self.seq = *seq;
            }
        }
        self.state.apply_remote_patch(&self.actor_id, patch)?;
        Ok(())
    }

    pub fn get_object_id(&self, path: &Path) -> Option<ObjectId> {
        self.state.get_object_id(path)
    }

    pub fn in_flight_requests(&self) -> Vec<u64> {
        self.state.in_flight_requests()
    }

    /// Gets the set of values for `path`, returns None if the path does not
    /// exist
    pub fn get_conflicts(&self, path: &Path) -> Option<HashMap<OpId, Value>> {
        self.state.resolve_path(path).map(|o| o.values())
    }

    /// Returns the value given by path, if it exists
    pub fn get_value(&self, path: &Path) -> Option<Value> {
        self.state.get_value(path)
    }
}

struct OptimisticChangeResult<O> {
    ops: Vec<Op>,
    deps: Vec<ChangeHash>,
    closure_result: O,
}
