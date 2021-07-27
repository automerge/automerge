use std::convert::TryFrom;

use automerge_protocol::ActorId;

use super::Schema;

/// Options for building a [`Frontend`](crate::Frontend).
#[derive(Debug, Clone)]
pub struct Options<T> {
    pub schema: Schema,
    pub actor_id: ActorId,
    pub timestamper: T,
}

impl Default for Options<fn() -> Option<i64>> {
    fn default() -> Self {
        Self {
            schema: Schema::default(),
            actor_id: ActorId::random(),
            timestamper: system_time,
        }
    }
}

/// Use the default timestamp since the Unix Epoch.
pub fn system_time() -> Option<i64> {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .ok()
        .and_then(|d| i64::try_from(d.as_millis()).ok())
}
