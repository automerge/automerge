//! A chunk's actor list as stored, before its order has been checked.
use crate::{op_set2::ActorIdx, ActorId};

use super::table::{ActorTable, UnsortedActors};

/// The actor list carried by a stored chunk (a document, a bundle), in the
/// order the writer stored it.
///
/// Positions in this list are what the chunk's own columns index into; that
/// consistency is all a stored list promises. Whether the order is sorted
/// depends on the writer: the current implementation always writes sorted
/// lists, but documents written by older JavaScript clients may have stored
/// actors in first-seen order, and a bundle from any other client need not be
/// sorted either.
///
/// Reconstructing a chunk's changes only needs the positions to be consistent,
/// so that path works from an [`ActorList`] directly. Anything which relies on
/// the order - a live [`ActorTable`], `OpId` comparison, actor lookup - must
/// first go through [`ActorList::into_table`], which checks it.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct ActorList {
    list: Vec<ActorId>,
}

impl ActorList {
    /// Adopt a stored actor list as-is.
    pub(crate) fn from_stored(list: Vec<ActorId>) -> Self {
        Self { list }
    }

    /// Promote to a live [`ActorTable`], if - and only if - the stored order
    /// is the canonical one.
    ///
    /// # Errors
    ///
    /// Returns [`UnsortedActors`] if the list is not strictly increasing. The
    /// chunk's columns are then only fit for reconstructing its changes, not
    /// for adopting directly.
    pub(crate) fn into_table(self) -> Result<ActorTable, UnsortedActors> {
        ActorTable::from_sorted(self.list)
    }

    /// Wrap as an [`ActorTable`] *without* checking the order, for an op set
    /// that exists only to reconstruct the chunk's changes.
    ///
    /// That op set never answers lookups or compares `OpId`s, which are the
    /// operations that depend on the table being sorted; it is only iterated
    /// and its actor indices resolved back to ids. The result must not be
    /// adopted as a live document's table; use [`Self::into_table`] for that.
    pub(crate) fn for_reconstruction(self) -> ActorTable {
        ActorTable::unchecked(self.list)
    }

    /// Return the number of actors in the list.
    pub(crate) fn len(&self) -> usize {
        self.list.len()
    }

    /// Return an [`ExactSizeIterator`] over the list of actors.
    pub(crate) fn iter(&self) -> impl ExactSizeIterator<Item = &ActorId> {
        self.list.iter()
    }

    /// Return a reference to the underlying slice of actors.
    pub(crate) fn as_slice(&self) -> &[ActorId] {
        &self.list
    }
}

impl std::ops::Index<usize> for ActorList {
    type Output = ActorId;

    fn index(&self, index: usize) -> &ActorId {
        &self.list[index]
    }
}

impl std::ops::Index<ActorIdx> for ActorList {
    type Output = ActorId;

    fn index(&self, index: ActorIdx) -> &ActorId {
        &self.list[usize::from(index)]
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn actor(b: u8) -> ActorId {
        ActorId::from(&[b; 4][..])
    }

    #[test]
    fn stored_order_is_preserved() {
        let list = ActorList::from_stored(vec![actor(9), actor(1)]);
        assert_eq!(list.len(), 2);
        assert_eq!(list[0], actor(9));
        assert_eq!(list[ActorIdx(1)], actor(1));
        assert_eq!(
            list.iter().cloned().collect::<Vec<_>>(),
            [actor(9), actor(1)]
        );
    }

    #[test]
    fn into_table_accepts_sorted() {
        let table = ActorList::from_stored(vec![actor(1), actor(9)])
            .into_table()
            .unwrap();
        assert_eq!(table.lookup(&actor(9)), Some(1));
    }

    #[test]
    fn into_table_rejects_unsorted_and_duplicates() {
        assert_eq!(
            ActorList::from_stored(vec![actor(9), actor(1)]).into_table(),
            Err(UnsortedActors)
        );
        assert_eq!(
            ActorList::from_stored(vec![actor(1), actor(1)]).into_table(),
            Err(UnsortedActors)
        );
    }
}
