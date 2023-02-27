use crate::exid::ExId;
use crate::marks::Mark;
use crate::Prop;
use crate::ReadDoc;
use crate::Value;

mod compose;
mod patch;
mod toggle_observer;
mod vec_observer;
pub use compose::compose;
pub use patch::{Patch, PatchAction};
pub use toggle_observer::ToggleObserver;
pub use vec_observer::{HasPatches, TextRepresentation, VecOpObserver, VecOpObserver16};

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
        conflict: bool,
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

    fn mark<'a, R: ReadDoc, M: Iterator<Item = Mark<'a>>>(
        &mut self,
        doc: &'a R,
        objid: ExId,
        mark: M,
    );

    fn unmark<R: ReadDoc>(&mut self, doc: &R, objid: ExId, key: &str, start: usize, end: usize);

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
        _conflict: bool,
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

    fn mark<'a, R: ReadDoc, M: Iterator<Item = Mark<'a>>>(
        &mut self,
        _doc: &'a R,
        _objid: ExId,
        _mark: M,
    ) {
    }

    fn unmark<R: ReadDoc>(
        &mut self,
        _doc: &R,
        _objid: ExId,
        _key: &str,
        _start: usize,
        _end: usize,
    ) {
    }

    fn delete_map<R: ReadDoc>(&mut self, _doc: &R, _objid: ExId, _key: &str) {}

    fn delete_seq<R: ReadDoc>(&mut self, _doc: &R, _objid: ExId, _index: usize, _num: usize) {}
}

impl BranchableObserver for () {
    fn merge(&mut self, _other: &Self) {}
    fn branch(&self) -> Self {}
}
