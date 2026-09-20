//! Actors and the indices which refer to them.
//!
//! Use [`ActorTable`] when you require the sorted set of recorded actors for a
//! document.
//!
//! Use [`ActorIndexed`] when you require tracking a value for a given
//! [`ActorId`] inside the [`ActorTable`].
//!
//! Use [`HasActorIndices`] and [`ActorRefs`] when a value, or collection of
//! values, uses an [`ActorId`] in the [`ActorTable`], e.g. [`OpId`].
//!
//! [`ActorId`]: crate::types::ActorId
//! [`OpId`]: crate::types::OpId

mod index;
mod reference;
mod table;

pub(crate) use index::ActorIndexed;
pub(crate) use reference::{ActorRefs, HasActorIndices, NoActorIndices};
pub(crate) use table::{ActorInsert, ActorRemoval, ActorShift, ActorTable};
