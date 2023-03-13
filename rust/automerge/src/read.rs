use crate::ScalarValue;
use crate::{
    error::AutomergeError, exid::ExId, keys::Keys, list_range::ListRange, map_range::MapRange,
    marks::Mark, parents::Parents, values::Values, Change, ChangeHash, ObjType, Prop, Value,
};
use std::borrow::Cow;

use std::ops::RangeBounds;

/// Methods for reading values from an automerge document
///
/// Many of the methods on this trait have an alternate `*_at` version which
/// takes an additional argument of `&[ChangeHash]`. This allows you to retrieve
/// the value at a particular point in the document history identified by the
/// given change hashes.
pub trait ReadDoc {
    /// Get the parents of an object in the document tree.
    ///
    /// See the documentation for [`Parents`] for more details.
    ///
    /// ### Errors
    ///
    /// Returns an error when the id given is not the id of an object in this document.
    /// This function does not get the parents of scalar values contained within objects.
    ///
    /// ### Experimental
    ///
    /// This function may in future be changed to allow getting the parents from the id of a scalar
    /// value.
    fn parents<O: AsRef<ExId>>(&self, obj: O) -> Result<Parents<'_>, AutomergeError>;

    /// Get the path to an object
    ///
    /// "path" here means the sequence of `(object Id, key)` pairs which leads
    /// to the object in question.
    ///
    /// ### Errors
    ///
    /// * If the object ID `obj` is not in the document
    fn path_to_object<O: AsRef<ExId>>(&self, obj: O) -> Result<Vec<(ExId, Prop)>, AutomergeError>;

    /// Get the keys of the object `obj`.
    ///
    /// For a map this returns the keys of the map.
    /// For a list this returns the element ids (opids) encoded as strings.
    fn keys<O: AsRef<ExId>>(&self, obj: O) -> Keys<'_>;

    /// Iterate over the keys and values of the map `obj` in the given range.
    ///
    /// If the object correspoding to `obj` is a list then this will return an empty iterator
    ///
    /// The returned iterator yields `(key, value, exid)` tuples, where the
    /// third element is the ID of the operation which created the value.
    fn map_range<O: AsRef<ExId>, R: RangeBounds<String>>(
        &self,
        obj: O,
        range: R,
    ) -> MapRange<'_, R>;

    /// Iterate over the indexes and values of the list or text `obj` in the given range.
    ///
    /// The reuturned iterator yields `(index, value, exid)` tuples, where the third
    /// element is the ID of the operation which created the value.
    fn list_range<O: AsRef<ExId>, R: RangeBounds<usize>>(
        &self,
        obj: O,
        range: R,
    ) -> ListRange<'_, R>;

    /// Iterate over the values in a map, list, or text object
    ///
    /// The returned iterator yields `(value, exid)` tuples, where the second element
    /// is the ID of the operation which created the value.
    fn values<O: AsRef<ExId>>(&self, obj: O) -> Values<'_>;

    fn values2<O: AsRef<ExId>>(&self, _obj: O) -> Cow<'_, ScalarValue> {
        todo!()
    }
    fn values3<O: AsRef<ExId>>(&self, _obj: O) -> Value<'_> {
        todo!()
    }
    fn values4<O: AsRef<ExId>>(&self, _obj: O) -> Values<'_> {
        todo!()
    }

    /// Get the length of the given object.
    ///
    /// If the given object is not in this document this method will return `0`
    fn length<O: AsRef<ExId>>(&self, obj: O) -> usize;

    /// Get the type of this object, if it is an object.
    fn object_type<O: AsRef<ExId>>(&self, obj: O) -> Result<ObjType, AutomergeError>;

    /// Get all marks on a current sequence
    fn marks<O: AsRef<ExId>>(&self, obj: O) -> Result<Vec<Mark<'_>>, AutomergeError>;

    /// Get the string represented by the given text object.
    fn text<O: AsRef<ExId>>(&self, obj: O) -> Result<String, AutomergeError>;

    /// Get a value out of the document.
    ///
    /// This returns a tuple of `(value, object ID)`. This is for two reasons:
    ///
    /// 1. If `value` is an object (represented by `Value::Object`) then the ID
    ///    is the ID of that object. This can then be used to retrieve nested
    ///    values from the document.
    /// 2. Even if `value` is a scalar, the ID represents the operation which
    ///    created the value. This is useful if there are conflicting values for
    ///    this key as each value is tagged with the ID.
    ///
    /// In the case of a key which has conflicting values, this method will
    /// return a single arbitrarily chosen value. This value will be chosen
    /// deterministically on all nodes. If you want to get all the values for a
    /// key use [`Self::get_all`].
    fn get<O: AsRef<ExId>, P: Into<Prop>>(
        &self,
        obj: O,
        prop: P,
    ) -> Result<Option<(Value<'_>, ExId)>, AutomergeError>;

    /// Get all conflicting values out of the document at this prop that conflict.
    ///
    /// If there are multiple conflicting values for a given key this method
    /// will return all of them, with each value tagged by the ID of the
    /// operation which created it.
    fn get_all<O: AsRef<ExId>, P: Into<Prop>>(
        &self,
        obj: O,
        prop: P,
    ) -> Result<Vec<(Value<'_>, ExId)>, AutomergeError>;

    /// Get the hashes of the changes in this document that aren't transitive dependencies of the
    /// given `heads`.
    fn get_missing_deps(&self, heads: &[ChangeHash]) -> Vec<ChangeHash>;

    /// Get a change by its hash.
    fn get_change_by_hash(&self, hash: &ChangeHash) -> Option<&Change>;
}
