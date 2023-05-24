use crate::exid::ExId;

use crate::marks::Mark;
use crate::{Prop, ReadDoc, Value};

mod patch;
mod vec_observer;
pub use patch::{Patch, PatchAction};
pub use vec_observer::{TextRepresentation, VecOpObserver};

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

    fn compare(&self, _other: &Self) -> bool {
        panic!("no implemented!")
    }
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

    fn delete_map<R: ReadDoc>(&mut self, _doc: &R, _objid: ExId, _key: &str) {}

    fn delete_seq<R: ReadDoc>(&mut self, _doc: &R, _objid: ExId, _index: usize, _num: usize) {}
}
