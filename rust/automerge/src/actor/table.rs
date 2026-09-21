//! The set of actors known to a document, kept in lexicographic order.
use std::ops::Deref;

use crate::ActorId;

/// The set of [`ActorId`]s that are recorded for a document.
///
/// Every actor known to a document lives in an [`ActorTable`], kept in
/// lexicographic order. An actor's position in that table is its *actor
/// index*, and actor indices - not [`ActorId`]s - are what
/// the rest of the crate stores: in `OpId`s, in clocks, in the change graph,
/// in the op set's columns. Two things rely on the table being sorted:
///
/// * `OpId` ordering tie-breaks on the actor index, which only agrees with
///   ordering by actor id when the table is sorted, and
/// * looking up an actor binary-searches the table.
///
/// [`ActorTable`] owns insertion and lookup so that those two facts are
/// maintained in one place rather than at every call site.
///
/// # Construction
///
/// - [`ActorTable::new`] creates a new, empty table.
/// - [`ActorTable::from_sorted`] creates a new table from a given vector of
///   actors. If the actors are not unique and sorted, then an [`UnsortedActors`]
///   error is returned.
/// - [`ActorTable::from_document_order`] creates a new table from an existing
///   document's actor set, which is expected to be in sorted order.
///
/// # Lookup
///
/// [`ActorTable::lookup`] returns the index of the given [`ActorId`], if it
/// exists. This is performed efficiently using a binary-search.
///
/// # Insertion and Removal
///
/// - [`ActorTable::insert`] attempts to insert an [`ActorId`] into the table.
///   The return value reports whether the actor was already existing, or if it
///   was inserted, with the index of the actor's position in both cases.
/// - [`ActorTable::remove`] removes the [`ActorId`] found at the given index,
///   returning the [`ActorId`] that was in that position.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct ActorTable {
    table: Vec<ActorId>,
}

/// Result of [`ActorTable::insert`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ActorInsert {
    /// The actor was already present at this index.
    Existing(usize),
    /// The actor was inserted at this index; every structure indexed by actor
    /// must shift indices `>= idx` up by one.
    Inserted(usize),
}

/// Returns `true` if the `table` of actors is uniquely sorted.
fn is_uniquely_sorted(table: &[ActorId]) -> bool {
    table.windows(2).all(|w| w[0] < w[1])
}

impl ActorTable {
    /// Construct an empty [`ActorTable`].
    pub(crate) fn new() -> Self {
        Self::default()
    }

    /// Adopt a document's actor list in the order it was stored.
    ///
    /// A document's op columns index actors by position in the stored list,
    /// so the indices are consistent with the columns. [`Self::lookup`] and
    /// `OpId` ordering additionally rely on that order being sorted, which
    /// documents saved by this implementation guarantee.
    pub(crate) fn from_document_order(table: Vec<ActorId>) -> Self {
        debug_assert!(is_uniquely_sorted(&table));
        Self { table }
    }

    /// The index of `actor`, if present.
    pub(crate) fn lookup(&self, actor: &ActorId) -> Option<usize> {
        self.table.binary_search(actor).ok()
    }

    /// Insert `actor` if it is not already present, reporting where it lives.
    pub(crate) fn insert(&mut self, actor: ActorId) -> ActorInsert {
        match self.table.binary_search(&actor) {
            Ok(idx) => ActorInsert::Existing(idx),
            Err(idx) => {
                self.table.insert(idx, actor);
                ActorInsert::Inserted(idx)
            }
        }
    }

    /// Remove the actor at `idx`. Structures indexed by actor must shift
    /// indices `> idx` down by one.
    ///
    /// # Panics
    ///
    /// Panics if `index` is out of bounds.
    pub(crate) fn remove(&mut self, idx: usize) -> ActorId {
        self.table.remove(idx)
    }
}

impl Deref for ActorTable {
    type Target = [ActorId];

    fn deref(&self) -> &[ActorId] {
        &self.table
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn actor(b: u8) -> ActorId {
        ActorId::from(&[b; 4][..])
    }

    #[test]
    fn from_document_order_preserves_order_and_lookup() {
        let t = ActorTable::from_document_order(vec![actor(1), actor(2), actor(3)]);
        assert_eq!(t.len(), 3);
        assert_eq!(t.lookup(&actor(2)), Some(1));
    }

    #[test]
    fn insert_keeps_order_and_reports_position() {
        let mut t = ActorTable::new();
        assert_eq!(t.insert(actor(5)), ActorInsert::Inserted(0));
        assert_eq!(t.insert(actor(1)), ActorInsert::Inserted(0));
        assert_eq!(t.insert(actor(9)), ActorInsert::Inserted(2));
        assert_eq!(t.insert(actor(5)), ActorInsert::Existing(1));
        assert_eq!(&*t, &[actor(1), actor(5), actor(9)]);
        assert_eq!(t.lookup(&actor(9)), Some(2));
        assert_eq!(t.lookup(&actor(7)), None);
    }

    #[test]
    fn remove_returns_actor() {
        let mut t = ActorTable::from_document_order(vec![actor(1), actor(2)]);
        assert_eq!(t.remove(0), actor(1));
        assert_eq!(&*t, &[actor(2)]);
    }
}
