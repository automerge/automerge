use crate::exid::ExId;
use crate::Automerge;
use crate::Prop;
use crate::Value;

/// An observer of operations applied to the document.
pub trait OpObserver {
    /// A new value has been inserted into the given object.
    ///
    /// - `doc`: a handle to the doc after the op has been inserted, can be used to query information
    /// - `objid`: the object that has been inserted into.
    /// - `index`: the index the new value has been inserted at.
    /// - `tagged_value`: the value that has been inserted and the id of the operation that did the
    /// insert.
    fn insert(
        &mut self,
        doc: &Automerge,
        objid: ExId,
        index: usize,
        tagged_value: (Value<'_>, ExId),
    );

    fn splice_text(&mut self, _doc: &Automerge, _objid: ExId, _index: usize, _value: &str) {
        panic!("splice_text not implemented in observer")
    }

    fn splice_text_utf16(
        &mut self,
        _doc: &Automerge,
        _objid: ExId,
        _index: (usize, usize),
        _len: (usize, usize),
        _value: &str,
    ) {
        panic!("splice_text_utf16 not supported in observer")
    }

    /// A new value has been put into the given object.
    ///
    /// - `doc`: a handle to the doc after the op has been inserted, can be used to query information
    /// - `objid`: the object that has been put into.
    /// - `prop`: the prop that the value as been put at.
    /// - `tagged_value`: the value that has been put into the object and the id of the operation
    /// that did the put.
    /// - `conflict`: whether this put conflicts with other operations.
    fn put(
        &mut self,
        doc: &Automerge,
        objid: ExId,
        prop: Prop,
        tagged_value: (Value<'_>, ExId),
        conflict: bool,
    );

    /// When a delete op exposes a previously conflicted value
    /// Similar to a put op - except for maps, lists and text, edits
    /// may already exist and need to be queried
    ///
    /// - `doc`: a handle to the doc after the op has been inserted, can be used to query information
    /// - `objid`: the object that has been put into.
    /// - `prop`: the prop that the value as been put at.
    /// - `tagged_value`: the value that has been put into the object and the id of the operation
    /// that did the put.
    /// - `conflict`: whether this put conflicts with other operations.
    fn expose(
        &mut self,
        doc: &Automerge,
        objid: ExId,
        prop: Prop,
        tagged_value: (Value<'_>, ExId),
        conflict: bool,
    );

    /// Flag a new conflict on a value without changing it
    ///
    /// - `doc`: a handle to the doc after the op has been inserted, can be used to query information
    /// - `objid`: the object that has been put into.
    /// - `prop`: the prop that the value as been put at.
    fn flag_conflict(&mut self, doc: &Automerge, objid: ExId, prop: Prop);

    /// A counter has been incremented.
    ///
    /// - `doc`: a handle to the doc after the op has been inserted, can be used to query information
    /// - `objid`: the object that contains the counter.
    /// - `prop`: they prop that the chounter is at.
    /// - `tagged_value`: the amount the counter has been incremented by, and the the id of the
    /// increment operation.
    fn increment(&mut self, doc: &Automerge, objid: ExId, prop: Prop, tagged_value: (i64, ExId));

    /// A value has beeen deleted.
    ///
    /// - `doc`: a handle to the doc after the op has been inserted, can be used to query information
    /// - `objid`: the object that has been deleted in.
    /// - `prop`: the prop of the value that has been deleted.
    fn delete(&mut self, doc: &Automerge, objid: ExId, prop: Prop);

    fn delete_utf16(
        &mut self,
        _doc: &Automerge,
        _objid: ExId,
        _index: (usize, usize),
        _len: (usize, usize),
    ) {
        panic!("delete_utf16 not supported in observer")
    }

    /// Branch of a new op_observer later to be merged
    ///
    /// Called by AutoCommit when creating a new transaction.  Observer branch
    /// will be merged on `commit()` or thrown away on `rollback()`
    ///
    fn branch(&self) -> Self;

    /// Merge observed information from a transaction.
    ///
    /// Called by AutoCommit on `commit()`
    ///
    /// - `other`: Another Op Observer of the same type
    fn merge(&mut self, other: &Self);
}

impl OpObserver for () {
    fn insert(
        &mut self,
        _doc: &Automerge,
        _objid: ExId,
        _index: usize,
        _tagged_value: (Value<'_>, ExId),
    ) {
    }

    fn splice_text(&mut self, _doc: &Automerge, _objid: ExId, _index: usize, _value: &str) {}

    fn put(
        &mut self,
        _doc: &Automerge,
        _objid: ExId,
        _prop: Prop,
        _tagged_value: (Value<'_>, ExId),
        _conflict: bool,
    ) {
    }

    fn expose(
        &mut self,
        _doc: &Automerge,
        _objid: ExId,
        _prop: Prop,
        _tagged_value: (Value<'_>, ExId),
        _conflict: bool,
    ) {
    }

    fn flag_conflict(&mut self, _doc: &Automerge, _objid: ExId, _prop: Prop) {}

    fn increment(
        &mut self,
        _doc: &Automerge,
        _objid: ExId,
        _prop: Prop,
        _tagged_value: (i64, ExId),
    ) {
    }

    fn delete(&mut self, _doc: &Automerge, _objid: ExId, _prop: Prop) {}

    fn merge(&mut self, _other: &Self) {}

    fn branch(&self) -> Self {}
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
    fn insert(&mut self, doc: &Automerge, obj: ExId, index: usize, (value, id): (Value<'_>, ExId)) {
        if let Ok(mut p) = doc.parents(&obj) {
            self.patches.push(Patch::Insert {
                obj,
                path: p.path(),
                index,
                value: (value.into_owned(), id),
            });
        }
    }

    fn splice_text(&mut self, doc: &Automerge, obj: ExId, index: usize, value: &str) {
        if let Ok(mut p) = doc.parents(&obj) {
            self.patches.push(Patch::Splice {
                obj,
                path: p.path(),
                index,
                value: value.to_string(),
            })
        }
    }

    fn put(
        &mut self,
        doc: &Automerge,
        obj: ExId,
        prop: Prop,
        (value, id): (Value<'_>, ExId),
        conflict: bool,
    ) {
        if let Ok(mut p) = doc.parents(&obj) {
            self.patches.push(Patch::Put {
                obj,
                path: p.path(),
                prop,
                value: (value.into_owned(), id),
                conflict,
            });
        }
    }

    fn expose(
        &mut self,
        doc: &Automerge,
        obj: ExId,
        prop: Prop,
        (value, id): (Value<'_>, ExId),
        conflict: bool,
    ) {
        if let Ok(mut p) = doc.parents(&obj) {
            self.patches.push(Patch::Expose {
                obj,
                path: p.path(),
                prop,
                value: (value.into_owned(), id),
                conflict,
            });
        }
    }

    fn flag_conflict(&mut self, mut _doc: &Automerge, _obj: ExId, _prop: Prop) {}

    fn increment(&mut self, doc: &Automerge, obj: ExId, prop: Prop, tagged_value: (i64, ExId)) {
        if let Ok(mut p) = doc.parents(&obj) {
            self.patches.push(Patch::Increment {
                obj,
                path: p.path(),
                prop,
                value: tagged_value,
            });
        }
    }

    fn delete(&mut self, doc: &Automerge, obj: ExId, prop: Prop) {
        if let Ok(mut p) = doc.parents(&obj) {
            self.patches.push(Patch::Delete {
                obj,
                path: p.path(),
                prop,
            })
        }
    }

    fn merge(&mut self, other: &Self) {
        self.patches.extend_from_slice(other.patches.as_slice())
    }

    fn branch(&self) -> Self {
        Self::default()
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
    /// Exposing (via delete) an old but conflicted value with a prop in a map, or a list element
    Expose {
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
    /// Inserting a new element into a list
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
    /// Splicing a text object
    Splice {
        /// path to the object
        path: Vec<(ExId, Prop)>,
        /// The object that was inserted into.
        obj: ExId,
        /// The index that the new value was inserted at.
        index: usize,
        /// The value that was spliced
        value: String,
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
