//! The set of actors known to a document, and the tokens which witness
//! changes to it.
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
///   If the [`ActorId`] is newly inserted, an [`ActorShift`] token is provided
///   to update other sets that mirror the actor's indices.
/// - [`ActorTable::remove`] removes the [`ActorId`] found at the given index,
///   returning the [`ActorId`] that was in that position.
///   An [`ActorRemoval`] token is also provided to update other sets that
///   mirror the actor's indices.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct ActorTable {
    table: Vec<ActorId>,
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

    /// Return the index of the [`ActorId`], if present.
    pub(crate) fn lookup(&self, actor: &ActorId) -> Option<usize> {
        self.table.binary_search(actor).ok()
    }

    /// Insert the [`ActorId`] into the table.
    ///
    /// [`ActorInsert`] reports whether the [`ActorId`] already existed, and the
    /// index of where it lives in the table.
    ///
    /// If the insertion was new, the [`ActorShift`] token must be used in other
    /// actor-ordered tables.
    pub(crate) fn insert(&mut self, actor: ActorId) -> ActorInsert {
        match self.table.binary_search(&actor) {
            Ok(idx) => ActorInsert::Existing(idx),
            Err(idx) => {
                self.table.insert(idx, actor);
                ActorInsert::Inserted(ActorShift(idx))
            }
        }
    }

    /// Remove the actor at `idx`. Structures indexed by actor must shift
    /// indices `> idx` down by one.
    ///
    /// # Panics
    ///
    /// Panics if `index` is out of bounds.
    pub(crate) fn remove(&mut self, idx: usize) -> (ActorId, ActorRemoval) {
        (self.table.remove(idx), ActorRemoval(idx))
    }
}

impl Deref for ActorTable {
    type Target = [ActorId];

    fn deref(&self) -> &[ActorId] {
        &self.table
    }
}

/// Result of [`ActorTable::insert`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ActorInsert {
    /// The actor was already present at this index.
    Existing(usize),
    /// The actor was inserted; every structure indexed by actor must apply
    /// the shift.
    Inserted(ActorShift),
}

impl ActorInsert {
    #[cfg(test)]
    pub(crate) fn index(&self) -> usize {
        match self {
            Self::Existing(idx) => *idx,
            Self::Inserted(shift) => shift.index(),
        }
    }
}

/// Evidence that an actor was inserted into an [`ActorTable`] at
/// [`Self::index`], obtainable only from [`ActorTable::insert`].
///
/// Every structure which stores actor indices must be told about the
/// insertion so it can move indices `>= index` up by one; taking this token
/// as the argument ties those updates to a real table mutation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct ActorShift(usize);

impl ActorShift {
    /// Fabricate a shift for tests which model an actor-indexed structure in
    /// isolation from an actual table.
    #[cfg(test)]
    pub(crate) fn for_test(index: usize) -> Self {
        Self(index)
    }

    /// Return the inner index value.
    pub(crate) fn index(&self) -> usize {
        self.0
    }

    /// Map an actor index stored before the insertion to its new value.
    pub(super) fn apply(&self, idx: usize) -> usize {
        if idx >= self.0 {
            idx + 1
        } else {
            idx
        }
    }
}

/// Evidence that the actor at [`Self::index`] was removed from an
/// [`ActorTable`], obtainable only from [`ActorTable::remove`].
///
/// Every structure which stores actor indices must be told so it can drop
/// references to the removed actor and move indices `> index` down by one.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct ActorRemoval(usize);

impl ActorRemoval {
    /// Fabricate a removal for tests which model an actor-indexed structure
    /// in isolation from an actual table.
    #[cfg(test)]
    pub(crate) fn for_test(index: usize) -> Self {
        Self(index)
    }

    /// Return the inner index value.
    pub(crate) fn index(&self) -> usize {
        self.0
    }

    /// Map an actor index stored before the removal to its new value, or
    /// `None` if it referred to the removed actor.
    pub(super) fn apply(&self, idx: usize) -> Option<usize> {
        match idx.cmp(&self.0) {
            std::cmp::Ordering::Greater => Some(idx - 1),
            std::cmp::Ordering::Equal => None,
            std::cmp::Ordering::Less => Some(idx),
        }
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
        assert_eq!(t.insert(actor(5)).index(), 0);
        assert_eq!(t.insert(actor(1)).index(), 0);
        assert_eq!(t.insert(actor(9)).index(), 2);
        assert_eq!(t.insert(actor(5)), ActorInsert::Existing(1));
        assert_eq!(&*t, &[actor(1), actor(5), actor(9)]);
        assert_eq!(t.lookup(&actor(9)), Some(2));
        assert_eq!(t.lookup(&actor(7)), None);
    }

    #[test]
    fn shift_moves_indices_at_or_after_insertion() {
        let mut t = ActorTable::from_document_order(vec![actor(1), actor(9)]);
        let ActorInsert::Inserted(shift) = t.insert(actor(5)) else {
            panic!("expected insertion")
        };
        assert_eq!(shift.index(), 1);
        assert_eq!(shift.apply(0), 0);
        assert_eq!(shift.apply(1), 2);
        assert_eq!(shift.apply(5), 6);
    }

    #[test]
    fn removal_drops_removed_and_moves_later_indices_down() {
        let mut t = ActorTable::from_document_order(vec![actor(1), actor(5), actor(9)]);
        let (removed, removal) = t.remove(1);
        assert_eq!(removed, actor(5));
        assert_eq!(&*t, &[actor(1), actor(9)]);
        assert_eq!(removal.index(), 1);
        assert_eq!(removal.apply(0), Some(0));
        assert_eq!(removal.apply(1), None);
        assert_eq!(removal.apply(2), Some(1));
    }
}
