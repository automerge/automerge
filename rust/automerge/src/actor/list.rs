//! A chunk's actor list as stored, before it has been checked.
use crate::ActorId;

use super::table::{ActorTable, UnsortedActors};

/// The actor list carried by a stored chunk (a document, a bundle), in the
/// order the writer stored it.
///
/// Positions in this list are what the chunk's own columns index into. A
/// stored list is expected to be in the same sorted order an [`ActorTable`]
/// keeps - that is what every writer produces - but having only just been
/// parsed, it has not been checked yet. Anything which relies on the order
/// goes through [`ActorList::into_table`], which checks it.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct ActorList {
    list: Vec<ActorId>,
}

impl ActorList {
    /// Adopt a stored actor list as-is.
    pub(crate) fn from_stored(list: Vec<ActorId>) -> Self {
        Self { list }
    }

    /// Promote to a live [`ActorTable`].
    ///
    /// # Errors
    ///
    /// Returns [`UnsortedActors`] if the list is not strictly increasing,
    /// which means the chunk was not written by a conforming implementation.
    pub(crate) fn into_table(self) -> Result<ActorTable, UnsortedActors> {
        ActorTable::from_sorted(self.list)
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
        assert_eq!(list.as_slice(), [actor(9), actor(1)]);
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
