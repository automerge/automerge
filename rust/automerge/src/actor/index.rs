//! One value per actor, kept in step with the [`ActorTable`].
use std::ops::Deref;

use super::table::{ActorRemoval, ActorShift, ActorTable};

/// One `T` per actor in an [`ActorTable`], addressed by actor index.
///
/// Several structures keep exactly one slot per actor: the per-actor
/// sequence numbers of a clock, the list of a given actor's changes, the
/// author an actor writes as. All of them must grow and shrink in lockstep
/// with the [actor table], and a slot's position must always be the same index
/// the table uses for that actor.
///
/// `ActorIndexed` enforces that pairing:
///
/// * it can only be created with a length taken from an [`ActorTable`] (or
///   from another `ActorIndexed`, which got its length the same way), and
/// * its length can only change by presenting the [`ActorShift`] /
///   [`ActorRemoval`] token that the table issued for the corresponding
///   insertion or removal, which also fixes *where* the slot goes.
///
/// Read access is via `Deref<Target = [T]>`, so indexing and iteration work
/// as for a slice.
///
/// To mutate the elements inside an [`ActorIndexed`], use
/// [`ActorIndexed::as_mut_slice`].
///
/// This is the *positional* half of keeping actor indices consistent. The
/// other half - collections whose elements merely *contain* actor indices,
/// such as the change graph's per-change actor column or the op set's actor
/// columns - cannot be a `Vec` of slots and instead rewrite each element
/// with [`ActorShift::apply`] / [`ActorRemoval::apply`].
///
/// # Construction
///
/// - [`ActorIndexed::new`] creates a new index, the same length as an
///   [`ActorTable`], with default values in every slot.
/// - [`ActorIndexed::new_like`] creates a new [`ActorIndexed`] based on another
///   one, with default values in every slot.
/// - [`ActorIndexed::from_slots_of`] creates a new [`ActorIndexed`] based on
///   another one, building the values using the given closure over the index and
///   the value from the other index.
///
/// # Mutation
///
/// - [`ActorIndexed::insert`] inserts the new element into the position defined
///   by [`ActorShift`], which can only be obtained by modifying an
///   [`ActorTable`].
/// - [`ActorIndexed::remove`] removes an element from position defined by
///   [`ActorRemoval`], which can only be obtained by modifying an
///   [`ActorTable`], and returns the element.
/// - [`ActorIndexed::as_mut_slice`] provides a mutable reference to the
///   underlying slice, so that the elements can modified in-place.
///
/// [`ActorTable`]: crate::actor::ActorTable
/// [actor table]: crate::actor::ActorTable
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub(crate) struct ActorIndexed<T> {
    slots: Vec<T>,
}

impl<T> ActorIndexed<T> {
    /// One default slot per actor in `actors`.
    pub(crate) fn new(actors: &ActorTable) -> Self
    where
        T: Default,
    {
        Self::build_from(actors, |_| T::default())
    }

    /// One slot per actor in `actors`, filled by `f(actor_index)`.
    fn build_from(actors: &ActorTable, f: impl FnMut(usize) -> T) -> Self {
        Self {
            slots: (0..actors.len()).map(f).collect(),
        }
    }

    /// One default slot per actor covered by `other`, which itself got its
    /// length from an actor table.
    pub(crate) fn new_like<U>(other: &ActorIndexed<U>) -> Self
    where
        T: Default,
    {
        Self::from_slots_of(other, |_, _| T::default())
    }

    /// One slot per actor covered by `other`, computed from `other`'s slot
    /// for the same actor by `f(actor_index, &slot)`.
    pub(crate) fn from_slots_of<U>(
        other: &ActorIndexed<U>,
        mut f: impl FnMut(usize, &U) -> T,
    ) -> Self {
        Self {
            slots: other
                .slots
                .iter()
                .enumerate()
                .map(|(i, u)| f(i, u))
                .collect(),
        }
    }

    /// Adopt `slots` as-is, for tests which model the actor table
    /// implicitly.
    #[cfg(test)]
    pub(crate) fn from_slots_for_test(slots: Vec<T>) -> Self {
        Self { slots }
    }

    /// Add the slot for an actor just inserted into the actor table.
    #[inline]
    pub(crate) fn insert(&mut self, shift: &ActorShift, element: T) {
        self.slots.insert(shift.index(), element);
    }

    /// Drop the slot of an actor just removed from the actor table.
    #[inline]
    pub(crate) fn remove(&mut self, removal: &ActorRemoval) -> T {
        self.slots.remove(removal.index())
    }

    /// Mutable access to the slots. Their number cannot change through this.
    #[inline]
    pub(crate) fn as_mut_slice(&mut self) -> &mut [T] {
        &mut self.slots
    }
}

impl<T> Deref for ActorIndexed<T> {
    type Target = [T];

    fn deref(&self) -> &[T] {
        &self.slots
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::actor::table::ActorInsert;
    use crate::ActorId;

    fn actor(b: u8) -> ActorId {
        ActorId::from(&[b; 4][..])
    }

    #[test]
    fn actor_indexed_tracks_table_length() {
        let mut table = ActorTable::from_actors([actor(1), actor(9)]);
        let mut slots: ActorIndexed<u8> = ActorIndexed::build_from(&table, |i| i as u8 * 10);
        assert_eq!(&*slots, &[0, 10]);

        let ActorInsert::Inserted(shift) = table.insert(actor(5)) else {
            panic!("expected insertion")
        };
        slots.insert(&shift, 99);
        assert_eq!(&*slots, &[0, 99, 10]);
        assert_eq!(slots.len(), table.len());

        let (_, removal) = table.remove(0);
        assert_eq!(slots.remove(&removal), 0);
        assert_eq!(&*slots, &[99, 10]);
        assert_eq!(slots.len(), table.len());

        let like: ActorIndexed<Option<u8>> = ActorIndexed::new_like(&slots);
        assert_eq!(like.len(), 2);
        assert!(like.iter().all(Option::is_none));
    }
}
