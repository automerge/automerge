//! Values which contain actor indices, and collections of them.

use std::collections::{HashMap, HashSet};
use std::hash::Hash;
use std::ops::{Deref, DerefMut};

use super::table::{ActorRemoval, ActorShift};

/// A value which *contains* actor indices and must be re-indexed when the
/// [actor table] changes.
///
/// Implementors describe where their actor indices live, whether in a value, or
/// within a collection of values.
/// The updating of those indices is done by using [`ActorShift`] and
/// [`ActorRemoval`].
///
/// Collections of such values are held in an [`ActorRefs`], which applies the
/// tokens to every element.
///
/// [`actor table`]: crate::actor::ActorTable
pub(crate) trait HasActorIndices: Sized {
    /// Re-index after the actor described by `shift` was inserted.
    fn shifted(self, shift: &ActorShift) -> Self;

    /// Re-index after the actor described by `removal` was removed, or
    /// `None` if this value referred to that actor.
    fn removed(self, removal: &ActorRemoval) -> Option<Self>;
}

impl HasActorIndices for usize {
    fn shifted(self, shift: &ActorShift) -> Self {
        shift.apply(self)
    }

    fn removed(self, removal: &ActorRemoval) -> Option<Self> {
        removal.apply(self)
    }
}

/// Marks a type which carries no actor indices, so re-indexing leaves it
/// unchanged. Lets such a type sit alongside index-bearing ones in an
/// [`ActorRefs`] (typically as a `HashMap` value).
pub(crate) trait NoActorIndices {}

impl<T: NoActorIndices> HasActorIndices for T {
    fn shifted(self, _: &ActorShift) -> Self {
        self
    }

    fn removed(self, _: &ActorRemoval) -> Option<Self> {
        Some(self)
    }
}

impl<A: HasActorIndices, B: HasActorIndices> HasActorIndices for (A, B) {
    fn shifted(self, shift: &ActorShift) -> Self {
        (self.0.shifted(shift), self.1.shifted(shift))
    }

    fn removed(self, removal: &ActorRemoval) -> Option<Self> {
        Some((self.0.removed(removal)?, self.1.removed(removal)?))
    }
}

/// A collection whose elements contain actor indices.
///
/// Wrapping a `Vec`, `HashMap` or `HashSet` of [`HasActorIndices`] values makes
/// the field announce that it has to react to actor table changes, and provides
/// [`ActorRefs::shift_actors`] and [`ActorRefs::remove_actor`] to do so for
/// every element at once. Everything else about the collection is reachable
/// through `Deref` and `DerefMut`.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub(crate) struct ActorRefs<C>(pub(crate) C);

/// An actor was removed from the table while an [`ActorRefs`] still held a
/// value referring to it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error("removed actor is still referenced")]
pub(crate) struct StillReferenced;

impl<C> Deref for ActorRefs<C> {
    type Target = C;

    fn deref(&self) -> &C {
        &self.0
    }
}

impl<C> DerefMut for ActorRefs<C> {
    fn deref_mut(&mut self) -> &mut C {
        &mut self.0
    }
}

impl<T: HasActorIndices + Clone> ActorRefs<Vec<T>> {
    pub(crate) fn shift_actors(&mut self, shift: &ActorShift) {
        self.0 = std::mem::take(&mut self.0)
            .into_iter()
            .map(|t| t.shifted(shift))
            .collect();
    }

    /// Fails, leaving `self` untouched, if any element refers to the removed
    /// actor.
    pub(crate) fn remove_actor(&mut self, removal: &ActorRemoval) -> Result<(), StillReferenced> {
        let updated = self
            .0
            .iter()
            .map(|t| t.clone().removed(removal).ok_or(StillReferenced))
            .collect::<Result<Vec<_>, _>>()?;
        self.0 = updated;
        Ok(())
    }
}

impl<K, V> ActorRefs<HashMap<K, V>>
where
    K: HasActorIndices + Copy + Eq + Hash,
    V: HasActorIndices + Clone,
{
    pub(crate) fn shift_actors(&mut self, shift: &ActorShift) {
        self.0 = std::mem::take(&mut self.0)
            .into_iter()
            .map(|(k, v)| (k.shifted(shift), v.shifted(shift)))
            .collect();
    }

    /// Fails, leaving `self` untouched, if any key or value refers to the
    /// removed actor.
    pub(crate) fn remove_actor(&mut self, removal: &ActorRemoval) -> Result<(), StillReferenced> {
        let updated = self
            .0
            .iter()
            .map(|(k, v)| (*k, v.clone()).removed(removal).ok_or(StillReferenced))
            .collect::<Result<HashMap<_, _>, _>>()?;
        self.0 = updated;
        Ok(())
    }
}

impl<T> ActorRefs<HashSet<T>>
where
    T: HasActorIndices + Copy + Eq + Hash,
{
    pub(crate) fn shift_actors(&mut self, shift: &ActorShift) {
        self.0 = std::mem::take(&mut self.0)
            .into_iter()
            .map(|t| t.shifted(shift))
            .collect();
    }

    /// Fails, leaving `self` untouched, if any element refers to the removed
    /// actor.
    pub(crate) fn remove_actor(&mut self, removal: &ActorRemoval) -> Result<(), StillReferenced> {
        let updated = self
            .0
            .iter()
            .map(|t| t.removed(removal).ok_or(StillReferenced))
            .collect::<Result<HashSet<_>, _>>()?;
        self.0 = updated;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::actor::table::{ActorInsert, ActorTable};
    use crate::ActorId;

    fn actor(b: u8) -> ActorId {
        ActorId::from(&[b; 4][..])
    }

    #[test]
    fn actor_refs_shift_and_remove_every_element() {
        let mut table = ActorTable::from_actors([actor(1), actor(9)]);
        // Elements refer to actor indices 0 and 1.
        let mut refs: ActorRefs<Vec<usize>> = ActorRefs(vec![0, 1, 1]);

        let ActorInsert::Inserted(shift) = table.insert(actor(5)) else {
            panic!("expected insertion")
        };
        refs.shift_actors(&shift);
        assert_eq!(&*refs, &[0, 2, 2], "indices at/after insertion move up");

        // Removing an actor nobody refers to succeeds and re-indexes.
        let (_, removal) = table.remove(1);
        refs.remove_actor(&removal).unwrap();
        assert_eq!(&*refs, &[0, 1, 1]);

        // Removing a referenced actor fails and leaves the collection intact.
        let (_, removal) = table.remove(1);
        assert_eq!(refs.remove_actor(&removal), Err(StillReferenced));
        assert_eq!(&*refs, &[0, 1, 1]);
    }

    #[test]
    fn actor_refs_hashmap_reindexes_keys_and_values() {
        let mut table = ActorTable::from_actors([actor(1), actor(9)]);
        let mut refs: ActorRefs<HashMap<usize, usize>> = ActorRefs(HashMap::from([(0, 1), (1, 0)]));

        let ActorInsert::Inserted(shift) = table.insert(actor(0)) else {
            panic!("expected insertion")
        };
        refs.shift_actors(&shift);
        assert_eq!(refs.0, HashMap::from([(1, 2), (2, 1)]));

        let (_, removal) = table.remove(0);
        refs.remove_actor(&removal).unwrap();
        assert_eq!(refs.0, HashMap::from([(0, 1), (1, 0)]));
    }
}
