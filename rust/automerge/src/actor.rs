//! Actors and the indices which refer to them.
//!
//! Use [`ActorTable`] when you require the sorted set of recorded actors for a
//! document.
//!
//! Use [`ActorIndexed`] when you require tracking a value for a given
//! [`ActorId`] inside the [`ActorTable`].

mod index;
mod table;

pub(crate) use index::ActorIndexed;
pub(crate) use table::{ActorInsert, ActorRemoval, ActorShift, ActorTable};
