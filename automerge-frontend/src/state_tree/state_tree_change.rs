use super::StateTreeComposite;
use automerge_protocol as amp;

/// Represents a change to the state tree. This is used to represent values which have changed
/// (the type T usually ends up being either a MultiValue or a StateTreeComposite) along with
/// changes that need to be made to indexes maintained by the state tree (the object id -> object
/// value index for example).
#[derive(Clone)]
pub struct StateTreeChange<T> {
    value: T,
    index_updates: Option<im_rc::HashMap<amp::ObjectID, StateTreeComposite>>,
}

impl<T> StateTreeChange<T> {
    pub(super) fn pure(value: T) -> StateTreeChange<T> {
        StateTreeChange {
            value,
            index_updates: None,
        }
    }

    pub(super) fn value(&self) -> &T {
        &self.value
    }

    pub(super) fn index_updates(
        &self,
    ) -> Option<&im_rc::HashMap<amp::ObjectID, StateTreeComposite>> {
        self.index_updates.as_ref()
    }

    pub(super) fn map<F, G>(self, f: F) -> StateTreeChange<G>
    where
        F: FnOnce(T) -> G,
    {
        StateTreeChange {
            value: f(self.value),
            index_updates: self.index_updates,
        }
    }

    pub(super) fn fallible_map<F, G, E>(self, f: F) -> Result<StateTreeChange<G>, E>
    where
        F: FnOnce(T) -> Result<G, E>,
    {
        Ok(StateTreeChange {
            value: f(self.value)?,
            index_updates: self.index_updates,
        })
    }

    pub(super) fn and_then<F, G>(self, f: F) -> StateTreeChange<G>
    where
        F: FnOnce(T) -> StateTreeChange<G>,
    {
        let diff = f(self.value);
        let index_updates = Self::merge_updates(self.index_updates, diff.index_updates);
        StateTreeChange {
            value: diff.value,
            index_updates,
        }
    }

    pub(super) fn fallible_and_then<F, G, E>(self, f: F) -> Result<StateTreeChange<G>, E>
    where
        F: FnOnce(T) -> Result<StateTreeChange<G>, E>,
    {
        let diff = f(self.value)?;
        let updates = Self::merge_updates(self.index_updates, diff.index_updates);
        Ok(StateTreeChange {
            value: diff.value,
            index_updates: updates,
        })
    }

    pub(super) fn with_updates(
        self,
        updates: Option<im_rc::HashMap<amp::ObjectID, StateTreeComposite>>,
    ) -> StateTreeChange<T> {
        StateTreeChange {
            value: self.value,
            index_updates: Self::merge_updates(self.index_updates, updates),
        }
    }

    fn merge_updates(
        before: Option<im_rc::HashMap<amp::ObjectID, StateTreeComposite>>,
        after: Option<im_rc::HashMap<amp::ObjectID, StateTreeComposite>>,
    ) -> Option<im_rc::HashMap<amp::ObjectID, StateTreeComposite>> {
        match (before, after) {
            (Some(before), Some(after)) => Some(after.union(before)),
            (Some(before), None) => Some(before),
            (None, Some(after)) => Some(after),
            (None, None) => None,
        }
    }
}
