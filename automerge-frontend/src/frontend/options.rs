use std::convert::TryFrom;

use automerge_protocol::ActorId;

use super::Schema;

/// Options for building a [`Frontend`](crate::Frontend).
#[derive(Debug, Clone)]
pub struct Options {
    /// The schema for the frontend to use.
    ///
    /// The default is an empty [`Schema`].
    pub schema: Schema,
    /// The actor id to appear in changes from this frontend.
    ///
    /// The default is [`ActorId::random`].
    pub actor_id: ActorId,
    /// The timestamp function for this frontend.
    ///
    /// The default is [`system_time`].
    pub timestamper: fn() -> Option<i64>,
}

impl Default for Options {
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
