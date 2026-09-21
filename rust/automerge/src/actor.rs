//! Actors and the indices which refer to them.
//!
//! Use [`ActorTable`] when you require the sorted set of recorded actors for a
//! document.

mod table;

pub(crate) use table::{ActorInsert, ActorTable};
