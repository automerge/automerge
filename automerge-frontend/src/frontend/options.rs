use std::convert::TryFrom;

use automerge_protocol::ActorId;

use crate::schema::RootSchema;

/// Options for building a [`Frontend`](crate::Frontend).
#[derive(Debug, Clone)]
pub struct Options {
    /// The schema for the frontend to use.
    ///
    /// The default is an empty [`RootSchema`].
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
    /// Set the schema.
    pub fn set_schema<S: Into<RootSchema>>(&mut self, schema: S) -> &mut Self {
        self.schema = Some(schema.into());
        self
    }

    /// Set the schema.
    pub fn with_schema<S: Into<RootSchema>>(mut self, schema: S) -> Self {
        self.schema = Some(schema.into());
        self
    }

    /// Set the actor id.
    pub fn set_actor_id<A: Into<ActorId>>(&mut self, actor_id: A) -> &mut Self {
        self.actor_id = actor_id.into();
        self
    }

    /// Set the actor id.
    pub fn with_actor_id<A: Into<ActorId>>(mut self, actor_id: A) -> Self {
        self.actor_id = actor_id.into();
        self
    }

    /// Set the timestamper function.
    ///
    /// The timestamper should produce values relative to the [Unix Epoch](https://en.wikipedia.org/wiki/Unix_time).
    ///
    /// For current discussion on the resolution provided see [this issue](https://github.com/automerge/automerge/issues/357).
    /// Do note, however, that the default timestamper [`system_time`] uses millisecond resolution.
    pub fn set_timestamper(&mut self, timestamper: fn() -> Option<i64>) -> &mut Self {
        self.timestamper = timestamper;
        self
    }

    /// Set the timestamper function.
    ///
    /// The timestamper should produce values relative to the [Unix Epoch](https://en.wikipedia.org/wiki/Unix_time).
    ///
    /// For current discussion on the resolution provided see [this issue](https://github.com/automerge/automerge/issues/357).
    /// Do note, however, that the default timestamper [`system_time`] uses millisecond resolution.
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
///
/// This produces millisecond resolution.
pub fn system_time() -> Option<i64> {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .ok()
        .and_then(|d| i64::try_from(d.as_millis()).ok())
}
