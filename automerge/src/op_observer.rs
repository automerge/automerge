use crate::exid::ExId;
use crate::parents::Parents;
use crate::Prop;
use crate::Value;

use std::fmt::Debug;

/// Capture operations into a [`Vec`] and store them as patches.
#[derive(Default, Debug, Clone)]
pub struct OpObserver {
    pub(crate) patches: Vec<Patch>,
}

impl OpObserver {
    /// Take the current list of patches, leaving the internal list empty and ready for new
    /// patches.
    pub fn take_patches(&mut self) -> Vec<Patch> {
        std::mem::take(&mut self.patches)
    }

    pub(crate) fn merge(&mut self, other: Self) {
        self.patches.extend(other.patches)
    }

    pub fn insert(
        &mut self,
        obj_id: ExId,
        parents: Parents<'_>,
        index: usize,
        (value, id): (Value<'_>, ExId),
    ) {
        let mut path = parents.collect::<Vec<_>>();
        path.reverse();
        self.patches.push(Patch::Insert {
            obj: obj_id,
            path,
            index,
            value: (value.into_owned(), id),
        });
    }

    pub fn put(
        &mut self,
        obj: ExId,
        parents: Parents<'_>,
        key: Prop,
        (value, id): (Value<'_>, ExId),
        conflict: bool,
    ) {
        let mut path = parents.collect::<Vec<_>>();
        path.reverse();
        self.patches.push(Patch::Put {
            obj,
            path,
            key,
            value: (value.into_owned(), id),
            conflict,
        });
    }

    pub fn increment(&mut self, obj: ExId, parents: Parents<'_>, key: Prop, tagged_value: (i64, ExId)) {
        let mut path = parents.collect::<Vec<_>>();
        path.reverse();
        self.patches.push(Patch::Increment {
            obj,
            path,
            key,
            value: tagged_value,
        });
    }

    pub fn delete(&mut self, obj: ExId, parents: Parents<'_>, key: Prop) {
        let mut path = parents.collect::<Vec<_>>();
        path.reverse();
        self.patches.push(Patch::Delete { obj, path, key })
    }
}

/// A notification to the application that something has changed in a document.
#[derive(Debug, Clone, PartialEq)]
pub enum Patch {
    /// Associating a new value with a key in a map, or an existing list element
    Put {
        /// The object that was put into.
        obj: ExId,
        path: Vec<(ExId, Prop)>,
        /// The key that the new value was put at.
        key: Prop,
        /// The value that was put, and the id of the operation that put it there.
        value: (Value<'static>, ExId),
        /// Whether this put conflicts with another.
        conflict: bool,
    },
    /// Inserting a new element into a list/text
    Insert {
        /// The object that was inserted into.
        obj: ExId,
        path: Vec<(ExId, Prop)>,
        /// The index that the new value was inserted at.
        index: usize,
        /// The value that was inserted, and the id of the operation that inserted it there.
        value: (Value<'static>, ExId),
    },
    /// Incrementing a counter.
    Increment {
        /// The object that was incremented in.
        obj: ExId,
        path: Vec<(ExId, Prop)>,
        /// The key that was incremented.
        key: Prop,
        /// The amount that the counter was incremented by, and the id of the operation that
        /// did the increment.
        value: (i64, ExId),
    },
    /// Deleting an element from a list/text
    Delete {
        /// The object that was deleted from.
        obj: ExId,
        path: Vec<(ExId, Prop)>,
        /// The key that was deleted.
        key: Prop,
    },
}
