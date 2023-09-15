use crate::{
    branch::{BranchScope, OpRef2},
    docref::DocRef,
    error::AutomergeError,
    exid::ExId,
    iter::{Keys, ListRange, MapRange, Values},
    marks::Mark,
    parents::Parents,
    Cursor, ObjType, Prop, Value,
};

use std::ops::RangeBounds;

/// Methods for reading values from an automerge document
///
/// Many of the methods on this trait have an alternate `*_at` version which
/// takes an additional argument of `&[ChangeHash]`. This allows you to retrieve
/// the value at a particular point in the document history identified by the
/// given change hashes.
pub trait ReadDocV2 {
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
    fn v2_parents<O: AsRef<ExId>>(
        &self,
        obj: O,
        branch: OpRef2<'_>,
    ) -> Result<Parents<'_>, AutomergeError>;

    /// Get the keys of the object `obj`.
    ///
    /// For a map this returns the keys of the map.
    /// For a list this returns the element ids (opids) encoded as strings.
    fn v2_keys<O: AsRef<ExId>>(&self, obj: O, branch: OpRef2<'_>) -> Keys<'_>;

    /// Iterate over the keys and values of the map `obj` in the given range.
    ///
    /// If the object correspoding to `obj` is a list then this will return an empty iterator
    ///
    /// The returned iterator yields `(key, value, exid)` tuples, where the
    /// third element is the ID of the operation which created the value.
    fn v2_map_range<'a, O: AsRef<ExId>, R: RangeBounds<String> + 'a>(
        &'a self,
        obj: O,
        range: R,
        branch: OpRef2<'_>,
    ) -> MapRange<'a, R>;

    /// Iterate over the indexes and values of the list or text `obj` in the given range.
    ///
    /// The reuturned iterator yields `(index, value, exid)` tuples, where the third
    /// element is the ID of the operation which created the value.
    fn v2_list_range<O: AsRef<ExId>, R: RangeBounds<usize>>(
        &self,
        obj: O,
        range: R,
        branch: OpRef2<'_>,
    ) -> ListRange<'_, R>;

    /// Iterate over the values in a map, list, or text object
    ///
    /// The returned iterator yields `(value, exid)` tuples, where the second element
    /// is the ID of the operation which created the value.
    fn v2_values<O: AsRef<ExId>>(&self, obj: O, branch: OpRef2<'_>) -> Values<'_>;

    /// Get the length of the given object.
    ///
    /// If the given object is not in this document this method will return `0`
    fn v2_length<O: AsRef<ExId>>(&self, obj: O, branch: OpRef2<'_>) -> usize;

    /// Get the type of this object, if it is an object.
    fn v2_object_type<O: AsRef<ExId>>(&self, obj: O) -> Result<ObjType, AutomergeError>;

    /// Get all marks on a current sequence
    fn v2_marks<O: AsRef<ExId>>(
        &self,
        obj: O,
        branch: OpRef2<'_>,
    ) -> Result<Vec<Mark<'_>>, AutomergeError>;

    /// Get the string represented by the given text object.
    fn v2_text<O: AsRef<ExId>>(&self, obj: O, branch: OpRef2<'_>)
        -> Result<String, AutomergeError>;

    /// Obtain the stable address (Cursor) for a `usize` position in a Sequence (either `Self::List` or `Self::Text`).
    ///
    /// Example use cases:
    /// 1. User cursor tracking, to maintain contextual position while merging remote changes.
    /// 2. Indexing sentences in a text field.
    ///
    /// To reverse the operation, see [`Self::v2_get_cursor_position`].
    fn v2_get_cursor<O: AsRef<ExId>>(
        &self,
        obj: O,
        position: usize,
        branch: OpRef2<'_>,
    ) -> Result<Cursor, AutomergeError>;

    /// Translate Cursor in a Sequence into an absolute position of type `usize`.
    ///
    /// Applicable only for Sequences (either `Self::List` or `Self::Text`).
    ///
    /// To reverse the operation, see [`Self::v2_get_cursor`].
    fn v2_get_cursor_position<O: AsRef<ExId>>(
        &self,
        obj: O,
        cursor: &Cursor,
        branch: OpRef2<'_>,
    ) -> Result<usize, AutomergeError>;

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
    /// key use [`Self::v2_get_all`].
    fn v2_get<O: AsRef<ExId>, P: Into<Prop>>(
        &self,
        obj: O,
        prop: P,
        branch: OpRef2<'_>,
    ) -> Result<Option<(Value<'_>, ExId)>, AutomergeError>;

    /// Get all conflicting values out of the document at this prop that conflict.
    ///
    /// If there are multiple conflicting values for a given key this method
    /// will return all of them, with each value tagged by the ID of the
    /// operation which created it.
    fn v2_get_all<O: AsRef<ExId>, P: Into<Prop>>(
        &self,
        obj: O,
        prop: P,
        branch: OpRef2<'_>,
    ) -> Result<Vec<(Value<'_>, ExId)>, AutomergeError>;
}

impl<D: DocRef + BranchScope> ReadDocV2 for D {
    fn v2_parents<O: AsRef<ExId>>(
        &self,
        obj: O,
        branch: OpRef2<'_>,
    ) -> Result<Parents<'_>, AutomergeError> {
        let branch = branch.into();
        let clock = self.scope_branch(&branch);
        self.doc_ref().parents_for(obj.as_ref(), clock)
    }

    fn v2_keys<O: AsRef<ExId>>(&self, obj: O, branch: OpRef2<'_>) -> Keys<'_> {
        let branch = branch.into();
        let clock = self.scope_branch(&branch);
        self.doc_ref().keys_for(obj.as_ref(), clock)
    }

    fn v2_map_range<'a, O: AsRef<ExId>, R: RangeBounds<String> + 'a>(
        &'a self,
        obj: O,
        range: R,
        branch: OpRef2<'_>,
    ) -> MapRange<'a, R> {
        let branch = branch.into();
        let clock = self.scope_branch(&branch);
        self.doc_ref().map_range_for(obj.as_ref(), range, clock)
    }

    fn v2_list_range<O: AsRef<ExId>, R: RangeBounds<usize>>(
        &self,
        obj: O,
        range: R,
        branch: OpRef2<'_>,
    ) -> ListRange<'_, R> {
        let branch = branch.into();
        let clock = self.scope_branch(&branch);
        self.doc_ref().list_range_for(obj.as_ref(), range, clock)
    }

    fn v2_values<O: AsRef<ExId>>(&self, obj: O, branch: OpRef2<'_>) -> Values<'_> {
        let branch = branch.into();
        let clock = self.scope_branch(&branch);
        self.doc_ref().values_for(obj.as_ref(), clock)
    }

    fn v2_length<O: AsRef<ExId>>(&self, obj: O, branch: OpRef2<'_>) -> usize {
        let branch = branch.into();
        let clock = self.scope_branch(&branch);
        self.doc_ref().length_for(obj.as_ref(), clock)
    }

    fn v2_object_type<O: AsRef<ExId>>(&self, obj: O) -> Result<ObjType, AutomergeError> {
        self.doc_ref().exid_to_obj(obj.as_ref()).map(|obj| obj.typ)
    }

    fn v2_marks<O: AsRef<ExId>>(
        &self,
        obj: O,
        branch: OpRef2<'_>,
    ) -> Result<Vec<Mark<'_>>, AutomergeError> {
        let branch = branch.into();
        let clock = self.scope_branch(&branch);
        self.doc_ref().marks_for(obj.as_ref(), clock)
    }

    fn v2_text<O: AsRef<ExId>>(
        &self,
        obj: O,
        branch: OpRef2<'_>,
    ) -> Result<String, AutomergeError> {
        let branch = branch.into();
        let clock = self.scope_branch(&branch);
        self.doc_ref().text_for(obj.as_ref(), clock)
    }

    fn v2_get_cursor<O: AsRef<ExId>>(
        &self,
        obj: O,
        position: usize,
        branch: OpRef2<'_>,
    ) -> Result<Cursor, AutomergeError> {
        let branch = branch.into();
        let clock = self.scope_branch(&branch);
        self.doc_ref().get_cursor_for(obj.as_ref(), position, clock)
    }

    fn v2_get_cursor_position<O: AsRef<ExId>>(
        &self,
        obj: O,
        cursor: &Cursor,
        branch: OpRef2<'_>,
    ) -> Result<usize, AutomergeError> {
        let branch = branch.into();
        let clock = self.scope_branch(&branch);
        self.doc_ref()
            .get_cursor_position_for(obj.as_ref(), cursor, clock)
    }

    fn v2_get<O: AsRef<ExId>, P: Into<Prop>>(
        &self,
        obj: O,
        prop: P,
        branch: OpRef2<'_>,
    ) -> Result<Option<(Value<'_>, ExId)>, AutomergeError> {
        let branch = branch.into();
        let clock = self.scope_branch(&branch);
        self.doc_ref().get_for(obj.as_ref(), prop.into(), clock)
    }

    fn v2_get_all<O: AsRef<ExId>, P: Into<Prop>>(
        &self,
        obj: O,
        prop: P,
        branch: OpRef2<'_>,
    ) -> Result<Vec<(Value<'_>, ExId)>, AutomergeError> {
        let branch = branch.into();
        let clock = self.scope_branch(&branch);
        self.doc_ref().get_all_for(obj.as_ref(), prop.into(), clock)
    }
}
