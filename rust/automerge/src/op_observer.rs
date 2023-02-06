use crate::exid::ExId;
use crate::Prop;
use crate::ReadDoc;
use crate::Value;

mod compose;
pub use compose::compose;

/// An observer of operations applied to the document.
pub trait OpObserver {
    /// A new value has been inserted into the given object.
    ///
    /// - `doc`: a handle to the doc after the op has been inserted, can be used to query information
    /// - `objid`: the object that has been inserted into.
    /// - `index`: the index the new value has been inserted at.
    /// - `tagged_value`: the value that has been inserted and the id of the operation that did the
    /// insert.
    fn insert<R: ReadDoc>(
        &mut self,
        doc: &R,
        objid: ExId,
        index: usize,
        tagged_value: (Value<'_>, ExId),
    );

    /// Some text has been spliced into a text object
    fn splice_text<R: ReadDoc>(&mut self, _doc: &R, _objid: ExId, _index: usize, _value: &str);

    /// A new value has been put into the given object.
    ///
    /// - `doc`: a handle to the doc after the op has been inserted, can be used to query information
    /// - `objid`: the object that has been put into.
    /// - `prop`: the prop that the value as been put at.
    /// - `tagged_value`: the value that has been put into the object and the id of the operation
    /// that did the put.
    /// - `conflict`: whether this put conflicts with other operations.
    fn put<R: ReadDoc>(
        &mut self,
        doc: &R,
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
    fn expose<R: ReadDoc>(
        &mut self,
        doc: &R,
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
    fn flag_conflict<R: ReadDoc>(&mut self, _doc: &R, _objid: ExId, _prop: Prop) {}

    /// A counter has been incremented.
    ///
    /// - `doc`: a handle to the doc after the op has been inserted, can be used to query information
    /// - `objid`: the object that contains the counter.
    /// - `prop`: they prop that the chounter is at.
    /// - `tagged_value`: the amount the counter has been incremented by, and the the id of the
    /// increment operation.
    fn increment<R: ReadDoc>(
        &mut self,
        doc: &R,
        objid: ExId,
        prop: Prop,
        tagged_value: (i64, ExId),
    );

    /// A map value has beeen deleted.
    ///
    /// - `doc`: a handle to the doc after the op has been inserted, can be used to query information
    /// - `objid`: the object that has been deleted in.
    /// - `prop`: the prop to be deleted
    fn delete<R: ReadDoc>(&mut self, doc: &R, objid: ExId, prop: Prop) {
        match prop {
            Prop::Map(k) => self.delete_map(doc, objid, &k),
            Prop::Seq(i) => self.delete_seq(doc, objid, i, 1),
        }
    }

    /// A map value has beeen deleted.
    ///
    /// - `doc`: a handle to the doc after the op has been inserted, can be used to query information
    /// - `objid`: the object that has been deleted in.
    /// - `key`: the map key to be deleted
    fn delete_map<R: ReadDoc>(&mut self, doc: &R, objid: ExId, key: &str);

    /// A one or more list values have beeen deleted.
    ///
    /// - `doc`: a handle to the doc after the op has been inserted, can be used to query information
    /// - `objid`: the object that has been deleted in.
    /// - `index`: the index of the deletion
    /// - `num`: the number of sequential elements deleted
    fn delete_seq<R: ReadDoc>(&mut self, doc: &R, objid: ExId, index: usize, num: usize);

    /// Whether to call sequence methods or `splice_text` when encountering changes in text
    ///
    /// Returns `false` by default
    fn text_as_seq(&self) -> bool {
        false
    }
}

/// An observer which can be branched
///
/// This is used when observing operations in a transaction. In this case `branch` will be called
/// at the beginning of the transaction to return a new observer and then `merge` will be called
/// with the branched observer as `other` when the transaction is comitted.
pub trait BranchableObserver {
    /// Branch of a new op_observer later to be merged
    ///
    /// Called when creating a new transaction.  Observer branch will be merged on `commit()` or
    /// thrown away on `rollback()`
    fn branch(&self) -> Self;

    /// Merge observed information from a transaction.
    ///
    /// Called by AutoCommit on `commit()`
    ///
    /// - `other`: Another Op Observer of the same type
    fn merge(&mut self, other: &Self);
}

impl OpObserver for () {
    fn insert<R: ReadDoc>(
        &mut self,
        _doc: &R,
        _objid: ExId,
        _index: usize,
        _tagged_value: (Value<'_>, ExId),
    ) {
    }

    fn splice_text<R: ReadDoc>(&mut self, _doc: &R, _objid: ExId, _index: usize, _value: &str) {}

    fn put<R: ReadDoc>(
        &mut self,
        _doc: &R,
        _objid: ExId,
        _prop: Prop,
        _tagged_value: (Value<'_>, ExId),
        _conflict: bool,
    ) {
    }

    fn expose<R: ReadDoc>(
        &mut self,
        _doc: &R,
        _objid: ExId,
        _prop: Prop,
        _tagged_value: (Value<'_>, ExId),
        _conflict: bool,
    ) {
    }

    fn increment<R: ReadDoc>(
        &mut self,
        _doc: &R,
        _objid: ExId,
        _prop: Prop,
        _tagged_value: (i64, ExId),
    ) {
    }

    fn delete_map<R: ReadDoc>(&mut self, _doc: &R, _objid: ExId, _key: &str) {}

    fn delete_seq<R: ReadDoc>(&mut self, _doc: &R, _objid: ExId, _index: usize, _num: usize) {}
}

impl BranchableObserver for () {
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
    fn insert<R: ReadDoc>(
        &mut self,
        doc: &R,
        obj: ExId,
        index: usize,
        (value, id): (Value<'_>, ExId),
    ) {
        if let Ok(p) = doc.parents(&obj) {
            self.patches.push(Patch::Insert {
                obj,
                path: p.path(),
                index,
                value: (value.into_owned(), id),
            });
        }
    }

    fn splice_text<R: ReadDoc>(&mut self, doc: &R, obj: ExId, index: usize, value: &str) {
        if let Ok(p) = doc.parents(&obj) {
            self.patches.push(Patch::Splice {
                obj,
                path: p.path(),
                index,
                value: value.to_string(),
            })
        }
    }

    fn put<R: ReadDoc>(
        &mut self,
        doc: &R,
        obj: ExId,
        prop: Prop,
        (value, id): (Value<'_>, ExId),
        conflict: bool,
    ) {
        if let Ok(p) = doc.parents(&obj) {
            self.patches.push(Patch::Put {
                obj,
                path: p.path(),
                prop,
                value: (value.into_owned(), id),
                conflict,
            });
        }
    }

    fn expose<R: ReadDoc>(
        &mut self,
        doc: &R,
        obj: ExId,
        prop: Prop,
        (value, id): (Value<'_>, ExId),
        conflict: bool,
    ) {
        if let Ok(p) = doc.parents(&obj) {
            self.patches.push(Patch::Expose {
                obj,
                path: p.path(),
                prop,
                value: (value.into_owned(), id),
                conflict,
            });
        }
    }

    fn increment<R: ReadDoc>(&mut self, doc: &R, obj: ExId, prop: Prop, tagged_value: (i64, ExId)) {
        if let Ok(p) = doc.parents(&obj) {
            self.patches.push(Patch::Increment {
                obj,
                path: p.path(),
                prop,
                value: tagged_value,
            });
        }
    }

    fn delete_map<R: ReadDoc>(&mut self, doc: &R, obj: ExId, key: &str) {
        if let Ok(p) = doc.parents(&obj) {
            self.patches.push(Patch::Delete {
                obj,
                path: p.path(),
                prop: Prop::Map(key.to_owned()),
                num: 1,
            })
        }
    }

    fn delete_seq<R: ReadDoc>(&mut self, doc: &R, obj: ExId, index: usize, num: usize) {
        if let Ok(p) = doc.parents(&obj) {
            self.patches.push(Patch::Delete {
                obj,
                path: p.path(),
                prop: Prop::Seq(index),
                num,
            })
        }
    }
}

impl BranchableObserver for VecOpObserver {
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
        /// number of items deleted (for seq)
        num: usize,
    },
}
