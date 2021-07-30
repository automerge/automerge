use std::{collections::HashMap, convert::TryFrom, error::Error, fmt::Debug, num::NonZeroU64};

use automerge_protocol as amp;
use automerge_protocol::{ActorId, ObjectId, OpId, Patch};

use crate::{
    error::{InvalidInitialStateError, InvalidPatch},
    mutation::{LocalChange, MutableDocument},
    path::Path,
    state::FrontendState,
    state_tree::StateTree,
    value,
    value::Value,
    value_ref::RootRef,
};

pub struct Frontend {
    pub actor_id: ActorId,
    pub seq: Option<NonZeroU64>,
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
        let root_state = StateTree::new();
        Frontend {
            actor_id: ActorId::from(actor_id),
            seq: None,
            state: FrontendState::Reconciled {
                reconciled_root_state: root_state,
                max_op: None,
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
                let (init_ops, _) = kvs.iter().fold(
                    (Vec::new(), NonZeroU64::new(1).unwrap()),
                    |(mut ops, max_op), (k, v)| {
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
                    },
                );

                let init_change_request = amp::Change {
                    actor_id: front.actor_id.clone(),
                    start_op: NonZeroU64::new(1).unwrap(),
                    time: (front.timestamper)().unwrap_or(0),
                    seq: NonZeroU64::new(1).unwrap(),
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
        let start_op =
            NonZeroU64::new(self.state.max_op().map(|nzu| nzu.get()).unwrap_or_default() + 1)
                .unwrap();
        let change_result = self.state.optimistically_apply_change(
            &self.actor_id,
            change_closure,
            NonZeroU64::new(self.seq.map(|nzu| nzu.get()).unwrap_or_default() + 1).unwrap(),
        )?;
        self.cached_value = None;
        if !change_result.ops.is_empty() {
            self.seq = NonZeroU64::new(self.seq.map(|nzu| nzu.get()).unwrap_or_default() + 1);
            let change = amp::Change {
                start_op,
                actor_id: self.actor_id.clone(),
                seq: NonZeroU64::new(self.seq.map(|nzu| nzu.get()).unwrap_or_default()).unwrap(),
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
            if (*seq).get() > self.seq.map(|nzu| nzu.get()).unwrap_or_default() {
                self.seq = Some(*seq);
            }
        }
        self.state.apply_remote_patch(&self.actor_id, patch)?;
        Ok(())
    }

    pub fn get_object_id(&self, path: &Path) -> Option<ObjectId> {
        self.state.get_object_id(path)
    }

    pub fn in_flight_requests(&self) -> Vec<NonZeroU64> {
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
