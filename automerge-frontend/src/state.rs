use std::{error::Error, num::NonZeroU64};

use automerge_protocol as amp;

use crate::{
    mutation::MutationTracker,
    state_tree::{OptimisticStateTree, ResolvedPath, StateTree},
    value_ref::RootRef,
    InvalidPatch, MutableDocument, Path, Value,
};

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
pub(crate) enum FrontendState {
    /// The backend is processing some requests so we need to keep an optimistic version of the
    /// state.
    WaitingForInFlightRequests {
        /// The sequence numbers of in flight changes.
        in_flight_requests: Vec<NonZeroU64>,
        /// The optimistic version of the root state that the user manipulates.
        optimistic_root_state: OptimisticStateTree,
        /// Queued patches to be applied when we have no more in-flight requests.
        queued_diffs: Vec<amp::RootDiff>,
        /// A flag to track whether this state has seen a patch from the backend that represented
        /// changes from another actor.
        ///
        /// If this is true then our optimistic state will not equal the reconciled state so we may
        /// need to do extra work when moving to the reconciled state.
        seen_non_local_patch: bool,
        /// The maximum operation observed.
        max_op: NonZeroU64,
    },
    /// The backend has processed all changes and we no longer wait for anything.
    Reconciled {
        /// The root state that the backend tracks.
        reconciled_root_state: StateTree,
        /// The maximum operation observed.
        max_op: Option<NonZeroU64>,
        /// The dependencies of the last received patch.
        deps_of_last_received_patch: Vec<amp::ChangeHash>,
    },
}

impl FrontendState {
    /// Apply a patch received from the backend to this frontend state,
    /// returns the updated cached value (if it has changed) and a new
    /// `FrontendState` which replaces this one
    pub(crate) fn apply_remote_patch(
        &mut self,
        self_actor: &amp::ActorId,
        mut patch: amp::Patch,
    ) -> Result<(), InvalidPatch> {
        match self {
            FrontendState::WaitingForInFlightRequests {
                in_flight_requests,
                optimistic_root_state,
                queued_diffs,
                seen_non_local_patch,
                ..
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

                if new_in_flight_requests.is_empty() {
                    let max_op = patch.max_op;
                    let deps_of_last_received_patch = std::mem::take(&mut patch.deps);

                    let reconciled_root_state = if *seen_non_local_patch {
                        // seen at least one patch that won't have changes in the optimistic state

                        // Undo all of our changes
                        optimistic_root_state.rollback_all();

                        // get the reconciled state back out
                        let mut reconciled_root_state = optimistic_root_state.take_state();

                        queued_diffs.push(patch.diffs);

                        // apply all the patches to the reconciled state
                        //
                        // TODO: maybe try and apply diffs to each other rather than queueing them
                        // to compress them, then we only need to apply one
                        for diff in queued_diffs.drain(..) {
                            let checked_diff = reconciled_root_state.check_diff(diff)?;

                            reconciled_root_state.apply_diff(checked_diff);
                        }

                        reconciled_root_state
                    } else {
                        optimistic_root_state.take_state()
                    };

                    *self = FrontendState::Reconciled {
                        reconciled_root_state,
                        max_op,
                        deps_of_last_received_patch,
                    }
                } else {
                    queued_diffs.push(patch.diffs);
                    *in_flight_requests = new_in_flight_requests;
                    *seen_non_local_patch = *seen_non_local_patch || !is_local;
                    // don't update max_op as we have progressed since then
                }
                Ok(())
            }
            FrontendState::Reconciled {
                reconciled_root_state,
                max_op,
                deps_of_last_received_patch,
            } => {
                let checked_diff = reconciled_root_state.check_diff(patch.diffs)?;

                reconciled_root_state.apply_diff(checked_diff);

                *max_op = patch.max_op;
                *deps_of_last_received_patch = patch.deps;
                Ok(())
            }
        }
    }

    pub(crate) fn get_object_id(&self, path: &Path) -> Option<amp::ObjectId> {
        self.resolve_path(path).and_then(|r| r.object_id())
    }

    pub(crate) fn get_value(&self, path: &Path) -> Option<Value> {
        self.resolve_path(path).map(|r| r.default_value())
    }

    pub(crate) fn resolve_path(&self, path: &Path) -> Option<ResolvedPath> {
        let root = match self {
            FrontendState::WaitingForInFlightRequests {
                optimistic_root_state,
                ..
            } => optimistic_root_state,
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
    pub(crate) fn optimistically_apply_change<F, O, E>(
        &mut self,
        actor: &amp::ActorId,
        change_closure: F,
        seq: NonZeroU64,
    ) -> Result<OptimisticChangeResult<O>, E>
    where
        E: Error,
        F: FnOnce(&mut dyn MutableDocument) -> Result<O, E>,
    {
        match self {
            FrontendState::WaitingForInFlightRequests {
                in_flight_requests,
                optimistic_root_state,
                max_op,
                ..
            } => {
                let mut mutation_tracker =
                    MutationTracker::new(optimistic_root_state, Some(*max_op), actor.clone());

                let result = match change_closure(&mut mutation_tracker) {
                    Ok(result) => result,
                    Err(e) => {
                        // reset the original state
                        mutation_tracker.cancel();
                        return Err(e);
                    }
                };

                let (ops, mt_max_op) = mutation_tracker.commit();
                *max_op = mt_max_op.unwrap();
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
                max_op,
                deps_of_last_received_patch,
            } => {
                let mut optimistic_root_state =
                    OptimisticStateTree::new(std::mem::take(reconciled_root_state));

                let mut mutation_tracker =
                    MutationTracker::new(&mut optimistic_root_state, *max_op, actor.clone());

                let result = match change_closure(&mut mutation_tracker) {
                    Ok(result) => result,
                    Err(e) => {
                        // reset the original state
                        mutation_tracker.cancel();
                        // ensure we reinstate the reconciled_root_state
                        *reconciled_root_state = optimistic_root_state.take_state();
                        return Err(e);
                    }
                };

                let (ops, mt_max_op) = mutation_tracker.commit();
                *max_op = mt_max_op;

                let in_flight_requests = vec![seq];
                let deps = deps_of_last_received_patch.clone();

                if !ops.is_empty() {
                    *self = FrontendState::WaitingForInFlightRequests {
                        in_flight_requests,
                        optimistic_root_state,
                        queued_diffs: Vec::new(),
                        seen_non_local_patch: false,
                        max_op: max_op.unwrap(),
                    }
                } else {
                    // we can remain in the reconciled frontend state since we didn't make a change

                    // ensure we reinstate the reconciled_root_state
                    *reconciled_root_state = optimistic_root_state.take_state();
                };
                Ok(OptimisticChangeResult {
                    ops,
                    deps,
                    closure_result: result,
                })
            }
        }
    }

    pub(crate) fn in_flight_requests(&self) -> Vec<NonZeroU64> {
        match self {
            FrontendState::WaitingForInFlightRequests {
                in_flight_requests, ..
            } => in_flight_requests.clone(),
            _ => Vec::new(),
        }
    }

    pub(crate) fn max_op(&self) -> Option<NonZeroU64> {
        match self {
            FrontendState::WaitingForInFlightRequests { max_op, .. } => Some(*max_op),
            FrontendState::Reconciled { max_op, .. } => *max_op,
        }
    }

    pub(crate) fn value(&self) -> Value {
        match self {
            FrontendState::WaitingForInFlightRequests {
                optimistic_root_state,
                ..
            } => optimistic_root_state.value(),
            FrontendState::Reconciled {
                reconciled_root_state,
                ..
            } => reconciled_root_state.value(),
        }
    }

    pub(crate) fn value_ref(&self) -> RootRef {
        match self {
            FrontendState::WaitingForInFlightRequests {
                optimistic_root_state,
                ..
            } => optimistic_root_state.value_ref(),
            FrontendState::Reconciled {
                reconciled_root_state,
                ..
            } => reconciled_root_state.value_ref(),
        }
    }
}

pub(crate) struct OptimisticChangeResult<O> {
    pub(crate) ops: Vec<amp::Op>,
    pub(crate) deps: Vec<amp::ChangeHash>,
    pub(crate) closure_result: O,
}
