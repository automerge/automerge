//! Actors and the indices which refer to them.
//!
//! Use [`ActorTable`] when you require the sorted set of recorded actors for a
//! document.
//!
//! Use [`ActorList`] when you require a list of actors, that has no particular
//! constraints on it. Usually, these [`ActorId`]s come from an external source
//! where the constraints on the [`ActorTable`] may not hold.
//!
//! Use the [`ActorIds`] trait when either an [`ActorTable`] or an [`ActorList`]
//! will suffice.
//!
//! Use [`ActorIndexed`] when you require tracking a value for a given
//! [`ActorId`] inside the [`ActorTable`].
//!
//! Use [`HasActorIndices`] and [`ActorRefs`] when a value, or collection of
//! values, uses an [`ActorId`] in the [`ActorTable`], e.g. [`OpId`].
//!
//! [`OpId`]: crate::types::OpId

mod index;
mod list;
mod reference;
mod table;

pub(crate) use index::ActorIndexed;
pub(crate) use list::ActorList;
pub(crate) use reference::{ActorRefs, HasActorIndices, NoActorIndices};
pub(crate) use table::{ActorInsert, ActorRemoval, ActorShift, ActorTable};

use crate::ActorId;

/// Anything which maps actor indices to [`ActorId`]s: a stored
/// [`ActorList`] or a live [`ActorTable`].
///
/// Code which only needs to *read* [`ActorId`]s by index - change
/// reconstruction, change encoding - is written against this trait so it can
/// run over a chunk's stored list before the list's order has been checked, as
/// well as over a document's live table.
pub(crate) trait ActorIds {
    /// Return the number [`ActorId`]s in the stored collection.
    fn len(&self) -> usize;
    /// Return the [`ActorId`] at the given `index`.
    ///
    /// # Panics
    ///
    /// Indexing into collections will generally panic if the `index` exceeds
    /// the size of the collection, i.e. `index > self.len()`.
    fn actor(&self, index: usize) -> &ActorId;
}

impl ActorIds for ActorList {
    fn len(&self) -> usize {
        ActorList::len(self)
    }

    fn actor(&self, index: usize) -> &ActorId {
        &self[index]
    }
}

impl ActorIds for ActorTable {
    fn len(&self) -> usize {
        ActorTable::len(self)
    }

    fn actor(&self, index: usize) -> &ActorId {
        &self[index]
    }
}
