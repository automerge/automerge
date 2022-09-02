use crate::exid::ExId;
use crate::Parents;
use crate::Prop;
use crate::Value;

/// An observer of operations applied to the document.
pub trait OpObserver: Default + Clone {
    /// A new value has been inserted into the given object.
    ///
    /// - `objid`: the object that has been inserted into.
    /// - `index`: the index the new value has been inserted at.
    /// - `tagged_value`: the value that has been inserted and the id of the operation that did the
    /// insert.
    fn insert(
        &mut self,
        parents: Parents<'_>,
        objid: ExId,
        index: usize,
        tagged_value: (Value<'_>, ExId),
    );

    /// A new value has been put into the given object.
    ///
    /// - `objid`: the object that has been put into.
    /// - `prop`: the prop that the value as been put at.
    /// - `tagged_value`: the value that has been put into the object and the id of the operation
    /// that did the put.
    /// - `conflict`: whether this put conflicts with other operations.
    fn put(
        &mut self,
        parents: Parents<'_>,
        objid: ExId,
        prop: Prop,
        tagged_value: (Value<'_>, ExId),
        conflict: bool,
    );

    /// A counter has been incremented.
    ///
    /// - `objid`: the object that contains the counter.
    /// - `prop`: they prop that the chounter is at.
    /// - `tagged_value`: the amount the counter has been incremented by, and the the id of the
    /// increment operation.
    fn increment(
        &mut self,
        parents: Parents<'_>,
        objid: ExId,
        prop: Prop,
        tagged_value: (i64, ExId),
    );

    /// A value has beeen deleted.
    ///
    /// - `objid`: the object that has been deleted in.
    /// - `prop`: the prop of the value that has been deleted.
    fn delete(&mut self, parents: Parents<'_>, objid: ExId, prop: Prop);

    /// Merge data with an other observer
    ///
    /// - `other`: Another Op Observer of the same type
    fn merge(&mut self, other: &Self);

    /// Branch off to begin a transaction - allows state information to be coppied if needed
    ///
    /// - `other`: Another Op Observer of the same type
    fn branch(&self) -> Self {
        Self::default()
    }
}

impl OpObserver for () {
    fn insert(
        &mut self,
        _parents: Parents<'_>,
        _objid: ExId,
        _index: usize,
        _tagged_value: (Value<'_>, ExId),
    ) {
    }

    fn put(
        &mut self,
        _parents: Parents<'_>,
        _objid: ExId,
        _prop: Prop,
        _tagged_value: (Value<'_>, ExId),
        _conflict: bool,
    ) {
    }

    fn increment(
        &mut self,
        _parents: Parents<'_>,
        _objid: ExId,
        _prop: Prop,
        _tagged_value: (i64, ExId),
    ) {
    }

    fn delete(&mut self, _parents: Parents<'_>, _objid: ExId, _prop: Prop) {}

    fn merge(&mut self, _other: &Self) {}
}

/// Capture operations into a [`Vec`] and store them as patches.
#[derive(Default, Debug, Clone)]
pub struct VecOpObserver {
    patches: Vec<Patch>,
}

impl VecOpObserver {
    /// Take the current list of patches, leaving the internal list empty and ready for new
    /// patches.
    pub fn take_patches(&mut self) -> Vec<Patch> {
        std::mem::take(&mut self.patches)
    }
}

impl OpObserver for VecOpObserver {
    fn insert(
        &mut self,
        mut parents: Parents<'_>,
        obj: ExId,
        index: usize,
        (value, id): (Value<'_>, ExId),
    ) {
        let path = parents.path();
        self.patches.push(Patch::Insert {
            obj,
            path,
            index,
            value: (value.into_owned(), id),
        });
    }

    fn put(
        &mut self,
        mut parents: Parents<'_>,
        obj: ExId,
        prop: Prop,
        (value, id): (Value<'_>, ExId),
        conflict: bool,
    ) {
        let path = parents.path();
        self.patches.push(Patch::Put {
            obj,
            path,
            prop,
            value: (value.into_owned(), id),
            conflict,
        });
    }

    fn increment(
        &mut self,
        mut parents: Parents<'_>,
        obj: ExId,
        prop: Prop,
        tagged_value: (i64, ExId),
    ) {
        let path = parents.path();
        self.patches.push(Patch::Increment {
            obj,
            path,
            prop,
            value: tagged_value,
        });
    }

    fn delete(&mut self, mut parents: Parents<'_>, obj: ExId, prop: Prop) {
        let path = parents.path();
        self.patches.push(Patch::Delete { obj, path, prop })
    }

    fn merge(&mut self, other: &Self) {
        self.patches.extend_from_slice(other.patches.as_slice())
    }
}

/// A notification to the application that something has changed in a document.
#[derive(Debug, Clone, PartialEq)]
pub enum Patch {
    /// Associating a new value with a prop in a map, or an existing list element
    Put {
        /// path to the object
        path: Vec<(ExId, Prop)>,
        /// The object that was put into.
        obj: ExId,
        /// The prop that the new value was put at.
        prop: Prop,
        /// The value that was put, and the id of the operation that put it there.
        value: (Value<'static>, ExId),
        /// Whether this put conflicts with another.
        conflict: bool,
    },
    /// Inserting a new element into a list/text
    Insert {
        /// path to the object
        path: Vec<(ExId, Prop)>,
        /// The object that was inserted into.
        obj: ExId,
        /// The index that the new value was inserted at.
        index: usize,
        /// The value that was inserted, and the id of the operation that inserted it there.
        value: (Value<'static>, ExId),
    },
    /// Incrementing a counter.
    Increment {
        /// path to the object
        path: Vec<(ExId, Prop)>,
        /// The object that was incremented in.
        obj: ExId,
        /// The prop that was incremented.
        prop: Prop,
        /// The amount that the counter was incremented by, and the id of the operation that
        /// did the increment.
        value: (i64, ExId),
    },
    /// Deleting an element from a list/text
    Delete {
        /// path to the object
        path: Vec<(ExId, Prop)>,
        /// The object that was deleted from.
        obj: ExId,
        /// The prop that was deleted.
        prop: Prop,
    },
}
