use crate::{
    cursor::{CursorPosition, MoveCursor},
    error::{AutomergeError, ViewAtError},
    exid::ExId,
    hydrate,
    marks::{Mark, MarkSet},
    op_set2::Parents,
    Change, ChangeHash, Cursor, ObjType, Prop, TextEncoding, Value, ROOT,
};

use crate::iter::{DocIter, Keys, ListRange, MapRange, Spans, Values};

use std::ops::RangeBounds;

/// Methods for reading values from an automerge document
///
/// Many of the methods on this trait have an alternate `*_at` version which
/// takes an additional argument of `&[ChangeHash]`. This allows you to retrieve
/// the value at a particular point in the document history identified by the
/// given change hashes.
///
/// Alternatively, you can use [`view_at`](Self::view_at) to create a view of
/// the document at a specific point in history, and then use the normal methods
/// on that view.
pub trait ReadDoc {
    /// The type returned by [`view_at`](Self::view_at).
    type ViewAt<'a>: ReadDoc
    where
        Self: 'a;

    /// Returns a view of this document at the given heads.
    ///
    /// The returned view implements [`ReadDoc`], so you can use all the normal
    /// read methods on it. Each method will return values as they existed at
    /// the point in history identified by `heads`.
    ///
    /// This is more ergonomic than using the `*_at` methods when you need to
    /// perform multiple reads at the same point in history.
    ///
    /// # Errors
    ///
    /// Returns an error if any of the provided heads do not exist in the
    /// document's change graph.
    ///
    /// # Example
    ///
    /// ```
    /// use automerge::{AutoCommit, ReadDoc, ROOT};
    /// use automerge::transaction::Transactable;
    ///
    /// let mut doc = AutoCommit::new();
    /// doc.put(&ROOT, "key", "value1").unwrap();
    /// let heads1 = doc.get_heads();
    ///
    /// doc.put(&ROOT, "key", "value2").unwrap();
    ///
    /// // View the document at the earlier point in history
    /// let view = doc.view_at(&heads1).unwrap();
    /// let (value, _) = view.get(&ROOT, "key").unwrap().unwrap();
    /// // value is "value1", not "value2"
    /// ```
    fn view_at(&self, heads: &[ChangeHash]) -> Result<Self::ViewAt<'_>, ViewAtError>;

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

    /// Get the parents of the object `obj` as at `heads`
    ///
    /// See [`Self::parents()`]
    fn parents_at<O: AsRef<ExId>>(
        &self,
        obj: O,
        heads: &[ChangeHash],
    ) -> Result<Parents<'_>, AutomergeError>;

    /// Get the keys of the object `obj`.
    ///
    /// For a map this returns the keys of the map.
    /// For a list this returns the element ids (opids) encoded as strings.
    fn keys<O: AsRef<ExId>>(&self, obj: O) -> Keys<'_>;

    /// Get the keys of the object `obj` as at `heads`
    ///
    /// See [`Self::keys()`]
    fn keys_at<O: AsRef<ExId>>(&self, obj: O, heads: &[ChangeHash]) -> Keys<'_>;

    /// Iterate over the object `obj` as at `heads`
    ///
    /// See [`Self::iter()`]
    fn iter_at<O: AsRef<ExId>>(&self, obj: O, heads: Option<&[ChangeHash]>) -> DocIter<'_>;

    /// Iterate over all the objects in the document
    ///
    /// The returned iterator will iterate over
    /// [`DocObjItem`](crate::iter::DocObjItem)s, which are basically an (object
    /// ID, property) pair. The property is represented by either a
    /// [`MapRangeItem`](crate::iter::MapRangeItem),
    /// [`ListRangeItem`](crate::iter::ListRangeItem), or a text span
    /// represented by a [`ValueRef`](crate::ValueRef) depending on the type of
    /// the object. The iterator iterates in "causal" order, which means that
    /// objects which were created "first" in the documents history are visited
    /// first, which in turn means that parent objects are visited before their
    /// children.
    fn iter(&self) -> DocIter<'_> {
        self.iter_at(&ROOT, None)
    }

    /// Iterate over the keys and values of the map `obj` in the given range.
    ///
    /// If the object correspoding to `obj` is a list then this will return an empty iterator
    ///
    /// The returned iterator yields `(key, value, exid)` tuples, where the
    /// third element is the ID of the operation which created the value.
    fn map_range<'a, O: AsRef<ExId>, R: RangeBounds<String> + 'a>(
        &'a self,
        obj: O,
        range: R,
    ) -> MapRange<'a>;

    /// Iterate over the keys and values of the map `obj` in the given range as
    /// at `heads`
    ///
    /// If the object correspoding to `obj` is a list then this will return an empty iterator
    ///
    /// The returned iterator yields `(key, value, exid)` tuples, where the
    /// third element is the ID of the operation which created the value.
    ///
    /// See [`Self::map_range()`]
    fn map_range_at<'a, O: AsRef<ExId>, R: RangeBounds<String> + 'a>(
        &'a self,
        obj: O,
        range: R,
        heads: &[ChangeHash],
    ) -> MapRange<'a>;

    /// Iterate over the indexes and values of the list or text `obj` in the given range.
    ///
    /// The reuturned iterator yields `(index, value, exid)` tuples, where the third
    /// element is the ID of the operation which created the value.
    fn list_range<O: AsRef<ExId>, R: RangeBounds<usize>>(&self, obj: O, range: R) -> ListRange<'_>;

    /// Iterate over the indexes and values of the list or text `obj` in the given range as at `heads`
    ///
    /// The returned iterator yields `(index, value, exid)` tuples, where the third
    /// element is the ID of the operation which created the value.
    ///
    /// See [`Self::list_range()`]
    fn list_range_at<O: AsRef<ExId>, R: RangeBounds<usize>>(
        &self,
        obj: O,
        range: R,
        heads: &[ChangeHash],
    ) -> ListRange<'_>;

    /// Iterate over the values in a map, list, or text object
    ///
    /// The returned iterator yields `(value, exid)` tuples, where the second element
    /// is the ID of the operation which created the value.
    fn values<O: AsRef<ExId>>(&self, obj: O) -> Values<'_>;

    /// Iterate over the values in a map, list, or text object as at `heads`
    ///
    /// The returned iterator yields `(value, exid)` tuples, where the second element
    /// is the ID of the operation which created the value.
    ///
    /// See [`Self::values()`]
    fn values_at<O: AsRef<ExId>>(&self, obj: O, heads: &[ChangeHash]) -> Values<'_>;

    /// Get the length of the given object.
    ///
    /// If the given object is not in this document this method will return `0`
    fn length<O: AsRef<ExId>>(&self, obj: O) -> usize;

    /// Get the length of the given object as at `heads`
    ///
    /// If the given object is not in this document this method will return `0`
    ///
    /// See [`Self::length()`]
    fn length_at<O: AsRef<ExId>>(&self, obj: O, heads: &[ChangeHash]) -> usize;

    /// Get the type of this object, if it is an object.
    fn object_type<O: AsRef<ExId>>(&self, obj: O) -> Result<ObjType, AutomergeError>;

    /// Get all marks on a current sequence
    fn marks<O: AsRef<ExId>>(&self, obj: O) -> Result<Vec<Mark>, AutomergeError>;

    /// Get all marks on a sequence at a given heads
    fn marks_at<O: AsRef<ExId>>(
        &self,
        obj: O,
        heads: &[ChangeHash],
    ) -> Result<Vec<Mark>, AutomergeError>;

    fn get_marks<O: AsRef<ExId>>(
        &self,
        obj: O,
        index: usize,
        heads: Option<&[ChangeHash]>,
    ) -> Result<MarkSet, AutomergeError>;

    /// Get the string represented by the given text object.
    fn text<O: AsRef<ExId>>(&self, obj: O) -> Result<String, AutomergeError>;

    /// Get the string represented by the given text object as at `heads`, see
    /// [`Self::text()`]
    fn text_at<O: AsRef<ExId>>(
        &self,
        obj: O,
        heads: &[ChangeHash],
    ) -> Result<String, AutomergeError>;

    /// Return the sequence of text and block markers in the text object `obj`
    fn spans<O: AsRef<ExId>>(&self, obj: O) -> Result<Spans<'_>, AutomergeError>;

    /// Return the sequence of text and block markers in the text object `obj` as at `heads`
    fn spans_at<O: AsRef<ExId>>(
        &self,
        obj: O,
        heads: &[ChangeHash],
    ) -> Result<Spans<'_>, AutomergeError>;

    /// Obtain the stable address (Cursor) for a [`usize`] position in a Sequence (either [`ObjType::List`] or [`ObjType::Text`]).
    ///
    /// **This is equivalent to [`Self::get_cursor_moving()`] with `move_cursor` = `MoveCursor::After`.**
    fn get_cursor<O: AsRef<ExId>, I: Into<CursorPosition>>(
        &self,
        obj: O,
        position: I,
        at: Option<&[ChangeHash]>,
    ) -> Result<Cursor, AutomergeError>;

    /// Obtain the stable address (Cursor) for a [`usize`] position in a Sequence (either [`ObjType::List`] or [`ObjType::Text`]).
    ///
    /// # Use cases
    /// - User cursor tracking, to maintain contextual position while merging remote changes.
    /// - Indexing sentences in a text field.
    ///
    /// # Cursor movement
    ///
    /// `move_cursor` determines how the cursor resolves its position if the item originally referenced at the given position is **removed** in later versions of the document. See [`MoveCursor`] for more details.
    ///
    /// # Start/end cursors
    /// If you'd like a cursor which follows the start (`position = 0`) or end (`position = sequence.length`) of the sequence, pass `CursorPosition::Start` or `CursorPosition::End` respectively.
    ///
    /// Conceptually, start cursors behaves like a cursor pointed an index of `-1`. End cursors behave like a cursor pointed at `sequence.length`.
    ///
    /// Note that `move_cursor` does not affect start/end cursors, as the start/end positions can never be removed.
    ///
    /// To translate a cursor into a position, see [`Self::get_cursor_position()`].
    fn get_cursor_moving<O: AsRef<ExId>, I: Into<CursorPosition>>(
        &self,
        obj: O,
        position: I,
        at: Option<&[ChangeHash]>,
        move_cursor: MoveCursor,
    ) -> Result<Cursor, AutomergeError>;

    /// Translate Cursor in a Sequence into an absolute position of type [`usize`].
    ///
    /// Applicable only for Sequences (either [`ObjType::List`] or [`ObjType::Text`]).
    ///
    /// To reverse the operation, see [`Self::get_cursor()`].
    fn get_cursor_position<O: AsRef<ExId>>(
        &self,
        obj: O,
        cursor: &Cursor,
        at: Option<&[ChangeHash]>,
    ) -> Result<usize, AutomergeError>;

    /// Get a value out of the document.
    ///
    /// This returns a tuple of `(value, object ID)`. This is for two reasons:
    ///
    /// 1. If `value` is an object (represented by [`Value::Object`]) then the ID
    ///    is the ID of that object. This can then be used to retrieve nested
    ///    values from the document.
    /// 2. Even if `value` is a scalar, the ID represents the operation which
    ///    created the value. This is useful if there are conflicting values for
    ///    this key as each value is tagged with the ID.
    ///
    /// In the case of a key which has conflicting values, this method will
    /// return a single arbitrarily chosen value. This value will be chosen
    /// deterministically on all nodes. If you want to get all the values for a
    /// key use [`Self::get_all()`].
    fn get<O: AsRef<ExId>, P: Into<Prop>>(
        &self,
        obj: O,
        prop: P,
    ) -> Result<Option<(Value<'_>, ExId)>, AutomergeError>;

    /// Get the value of the given key as at `heads`, see [`Self::get()`]
    fn get_at<O: AsRef<ExId>, P: Into<Prop>>(
        &self,
        obj: O,
        prop: P,
        heads: &[ChangeHash],
    ) -> Result<Option<(Value<'_>, ExId)>, AutomergeError>;

    fn hydrate<O: AsRef<ExId>>(
        &self,
        obj: O,
        heads: Option<&[ChangeHash]>,
    ) -> Result<hydrate::Value, AutomergeError>;

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

    /// Get all possibly conflicting values for a key as at `heads`
    ///
    /// See [`Self::get_all()`]
    fn get_all_at<O: AsRef<ExId>, P: Into<Prop>>(
        &self,
        obj: O,
        prop: P,
        heads: &[ChangeHash],
    ) -> Result<Vec<(Value<'_>, ExId)>, AutomergeError>;

    /// Get the hashes of the changes in this document that aren't transitive dependencies of the
    /// given `heads`.
    fn get_missing_deps(&self, heads: &[ChangeHash]) -> Vec<ChangeHash>;

    /// Get a change by its hash.
    fn get_change_by_hash(&self, hash: &ChangeHash) -> Option<Change>;

    /// Return some statistics about the document
    fn stats(&self) -> Stats;

    fn text_encoding(&self) -> TextEncoding;
}

/// Statistics about the document
///
/// This is returned by [`ReadDoc::stats()`]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Stats {
    /// The number of operations in the document
    pub num_ops: u64,
    /// The number of changes in the change graph for the document
    pub num_changes: u64,
    /// The number of actors in the document
    pub num_actors: u64,
    /// package name from cargo.toml ("automerge");
    pub cargo_package_name: &'static str,
    /// package version from cargo.toml
    pub cargo_package_version: &'static str,
    /// version of rustc used to compile this
    pub rustc_version: &'static str,
}
