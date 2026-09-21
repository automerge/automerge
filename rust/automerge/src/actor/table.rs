//! The set of actors known to a document, and the tokens which witness
//! changes to it.
use std::ops::Index;

use crate::{op_set2::ActorIdx, ActorId};

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
/// - [`ActorTable::from_sorted`] creates a table from a vector of actors. If
///   the actors are not unique and sorted an [`UnsortedActors`] error is
///   returned. A chunk's stored list is promoted through
///   [`ActorList::into_table`](super::ActorList::into_table), which is this
///   check by another name.
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

    /// A table holding exactly `actors`, in sorted order, for tests which
    /// want to start from a known set.
    #[cfg(test)]
    pub(crate) fn from_actors(actors: impl IntoIterator<Item = ActorId>) -> Self {
        let mut table = Self::new();
        for actor in actors {
            table.insert(actor);
        }
        table
    }

    /// Return the number of actors in the table.
    pub(crate) fn len(&self) -> usize {
        self.table.len()
    }

    /// Return `true` is the table is empty.
    pub(crate) fn is_empty(&self) -> bool {
        self.table.is_empty()
    }

    /// Get the [`ActorId`] at the given index, returning `None` if the `index`
    /// was out of bounds.
    pub(crate) fn get(&self, index: usize) -> Option<&ActorId> {
        self.table.get(index)
    }

    /// Return an [`ExactSizeIterator`] over the table of actors.
    pub(crate) fn iter(&self) -> impl ExactSizeIterator<Item = &ActorId> {
        self.table.iter()
    }

    /// Return the underlying [`Vec`] of actors.
    pub(crate) fn to_vec(&self) -> Vec<ActorId> {
        self.table.clone()
    }

    /// Adopt an actor list which is already in strictly increasing order.
    ///
    /// # Errors
    ///
    /// Returns [`UnsortedActors`] if `table` was not in lexicographic order.
    pub(crate) fn from_sorted(table: Vec<ActorId>) -> Result<Self, UnsortedActors> {
        if is_uniquely_sorted(&table) {
            Ok(Self { table })
        } else {
            Err(UnsortedActors)
        }
    }

    /// Adopt an actor list in whatever order it is in, for a reconstruction-
    /// only op set (see [`super::ActorList::for_reconstruction`]).
    ///
    /// The result must never become a live document's table: `lookup` and
    /// `OpId` ordering are only meaningful once the table is sorted. It is
    /// scoped to the `actor` module so that [`super::ActorList`] is the only
    /// type which can reach it.
    pub(super) fn unchecked(table: Vec<ActorId>) -> Self {
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

// FIXME: We should probably only use ActorIdx
impl Index<usize> for ActorTable {
    type Output = ActorId;

    fn index(&self, index: usize) -> &Self::Output {
        &self.table[index]
    }
}

impl Index<ActorIdx> for ActorTable {
    type Output = ActorId;

    fn index(&self, index: ActorIdx) -> &Self::Output {
        &self.table[usize::from(index)]
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

/// The actor IDs passed to [`ActorTable::from_sorted`] were not in strictly
/// increasing order.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error("actor table is not sorted")]
pub(crate) struct UnsortedActors;

#[cfg(test)]
mod tests {
    use super::*;

    fn actor(b: u8) -> ActorId {
        ActorId::from(&[b; 4][..])
    }

    #[test]
    fn from_sorted_accepts_strictly_increasing() {
        let t = ActorTable::from_sorted(vec![actor(1), actor(2), actor(3)]).unwrap();
        assert_eq!(t.len(), 3);
        assert_eq!(t.lookup(&actor(2)), Some(1));
    }

    #[test]
    fn from_sorted_accepts_empty() {
        assert!(ActorTable::from_sorted(vec![]).is_ok());
    }

    #[test]
    fn from_sorted_rejects_unsorted() {
        assert_eq!(
            ActorTable::from_sorted(vec![actor(2), actor(1)]),
            Err(UnsortedActors)
        );
    }

    #[test]
    fn from_sorted_rejects_duplicates() {
        assert_eq!(
            ActorTable::from_sorted(vec![actor(1), actor(1)]),
            Err(UnsortedActors)
        );
    }

    #[test]
    fn insert_keeps_order_and_reports_position() {
        let mut t = ActorTable::new();
        assert_eq!(t.insert(actor(5)).index(), 0);
        assert_eq!(t.insert(actor(1)).index(), 0);
        assert_eq!(t.insert(actor(9)).index(), 2);
        assert_eq!(t.insert(actor(5)), ActorInsert::Existing(1));
        assert_eq!(t.to_vec(), [actor(1), actor(5), actor(9)]);
        assert_eq!(t.lookup(&actor(9)), Some(2));
        assert_eq!(t.lookup(&actor(7)), None);
    }

    #[test]
    fn shift_moves_indices_at_or_after_insertion() {
        let mut t = ActorTable::from_actors([actor(1), actor(9)]);
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
        let mut t = ActorTable::from_actors([actor(1), actor(5), actor(9)]);
        let (removed, removal) = t.remove(1);
        assert_eq!(removed, actor(5));
        assert_eq!(t.to_vec(), [actor(1), actor(9)]);
        assert_eq!(removal.index(), 1);
        assert_eq!(removal.apply(0), Some(0));
        assert_eq!(removal.apply(1), None);
        assert_eq!(removal.apply(2), Some(1));
    }
}
