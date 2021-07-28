use std::convert::TryFrom;

use automerge_protocol::ActorId;

use super::RootSchema;

/// Options for building a [`Frontend`](crate::Frontend).
#[derive(Debug, Clone)]
pub struct Options {
    /// The schema for the frontend to use.
    ///
    /// The default is an empty [`Schema`].
    pub(crate) schema: Option<RootSchema>,
    /// The actor id to appear in changes from this frontend.
    ///
    /// The default is [`ActorId::random`].
    pub(crate) actor_id: ActorId,
    /// The timestamp function for this frontend.
    ///
    /// The default is [`system_time`].
    pub(crate) timestamper: fn() -> Option<i64>,
}

impl Options {
    pub fn set_schema(&mut self, schema: RootSchema) -> &mut Self {
        self.schema = Some(schema);
        self
    }

    pub fn with_schema(mut self, schema: RootSchema) -> Self {
        self.schema = Some(schema);
        self
    }

    pub fn set_actor_id<A: Into<ActorId>>(&mut self, actor_id: A) -> &mut Self {
        self.actor_id = actor_id.into();
        self
    }

    pub fn with_actor_id<A: Into<ActorId>>(mut self, actor_id: A) -> Self {
        self.actor_id = actor_id.into();
        self
    }

    pub fn set_timestamper(&mut self, timestamper: fn() -> Option<i64>) -> &mut Self {
        self.timestamper = timestamper;
        self
    }

    pub fn with_timestamper(mut self, timestamper: fn() -> Option<i64>) -> Self {
        self.timestamper = timestamper;
        self
    }
}

impl Default for Options {
    fn default() -> Self {
        Self {
            schema: None,
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
