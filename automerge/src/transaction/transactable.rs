use std::ops::RangeBounds;

use crate::exid::ExId;
use crate::query;
use crate::{
    AutomergeError, ChangeHash, Keys, KeysAt, ListRange, ListRangeAt, MapRange, MapRangeAt,
    ObjType, Parents, Prop, ScalarValue, Value, Values,
};

/// A way of mutating a document within a single change.
pub trait Transactable {
    /// Get the number of pending operations in this transaction.
    fn pending_ops(&self) -> usize;

    /// Set the value of property `P` to value `V` in object `obj`.
    ///
    /// # Errors
    ///
    /// This will return an error if
    /// - The object does not exist
    /// - The key is the wrong type for the object
    /// - The key does not exist in the object
    fn put<O: AsRef<ExId>, P: Into<Prop>, V: Into<ScalarValue>>(
        &mut self,
        obj: O,
        prop: P,
        value: V,
    ) -> Result<(), AutomergeError>;

    /// Set the value of property `P` to the new object `V` in object `obj`.
    ///
    /// # Returns
    ///
    /// The id of the object which was created.
    ///
    /// # Errors
    ///
    /// This will return an error if
    /// - The object does not exist
    /// - The key is the wrong type for the object
    /// - The key does not exist in the object
    fn put_object<O: AsRef<ExId>, P: Into<Prop>>(
        &mut self,
        obj: O,
        prop: P,
        object: ObjType,
    ) -> Result<ExId, AutomergeError>;

    /// Insert a value into a list at the given index.
    fn insert<O: AsRef<ExId>, V: Into<ScalarValue>>(
        &mut self,
        obj: O,
        index: usize,
        value: V,
    ) -> Result<(), AutomergeError>;

    /// Insert an object into a list at the given index.
    fn insert_object<O: AsRef<ExId>>(
        &mut self,
        obj: O,
        index: usize,
        object: ObjType,
    ) -> Result<ExId, AutomergeError>;

    /// Set a mark within a range on a list
    #[allow(clippy::too_many_arguments)]
    fn mark<O: AsRef<ExId>>(
        &mut self,
        obj: O,
        start: usize,
        expand_start: bool,
        end: usize,
        expand_end: bool,
        mark: &str,
        value: ScalarValue,
    ) -> Result<(), AutomergeError>;

    fn unmark<O: AsRef<ExId>>(&mut self, obj: O, mark: O) -> Result<(), AutomergeError>;

    /// Increment the counter at the prop in the object by `value`.
    fn increment<O: AsRef<ExId>, P: Into<Prop>>(
        &mut self,
        obj: O,
        prop: P,
        value: i64,
    ) -> Result<(), AutomergeError>;

    /// Delete the value at prop in the object.
    fn delete<O: AsRef<ExId>, P: Into<Prop>>(
        &mut self,
        obj: O,
        prop: P,
    ) -> Result<(), AutomergeError>;

    fn splice<O: AsRef<ExId>, V: IntoIterator<Item = ScalarValue>>(
        &mut self,
        obj: O,
        pos: usize,
        del: usize,
        vals: V,
    ) -> Result<(), AutomergeError>;

    /// Like [`Self::splice`] but for text.
    fn splice_text<O: AsRef<ExId>>(
        &mut self,
        obj: O,
        pos: usize,
        del: usize,
        text: &str,
    ) -> Result<(), AutomergeError> {
        let vals = text.chars().map(|c| c.into());
        self.splice(obj, pos, del, vals)
    }

    /// Get the keys of the given object, it should be a map.
    fn keys<O: AsRef<ExId>>(&self, obj: O) -> Keys<'_, '_>;

    /// Get the keys of the given object at a point in history.
    fn keys_at<O: AsRef<ExId>>(&self, obj: O, heads: &[ChangeHash]) -> KeysAt<'_, '_>;

    fn map_range<O: AsRef<ExId>, R: RangeBounds<String>>(
        &self,
        obj: O,
        range: R,
    ) -> MapRange<'_, R>;

    fn map_range_at<O: AsRef<ExId>, R: RangeBounds<String>>(
        &self,
        obj: O,
        range: R,
        heads: &[ChangeHash],
    ) -> MapRangeAt<'_, R>;

    fn list_range<O: AsRef<ExId>, R: RangeBounds<usize>>(
        &self,
        obj: O,
        range: R,
    ) -> ListRange<'_, R>;

    fn list_range_at<O: AsRef<ExId>, R: RangeBounds<usize>>(
        &self,
        obj: O,
        range: R,
        heads: &[ChangeHash],
    ) -> ListRangeAt<'_, R>;

    fn values<O: AsRef<ExId>>(&self, obj: O) -> Values<'_>;

    fn values_at<O: AsRef<ExId>>(&self, obj: O, heads: &[ChangeHash]) -> Values<'_>;

    /// Get the length of the given object.
    fn length<O: AsRef<ExId>>(&self, obj: O) -> usize;

    /// Get the length of the given object at a point in history.
    fn length_at<O: AsRef<ExId>>(&self, obj: O, heads: &[ChangeHash]) -> usize;

    /// Get type for object
    fn object_type<O: AsRef<ExId>>(&self, obj: O) -> Option<ObjType>;

    /// Get the string that this text object represents.
    fn text<O: AsRef<ExId>>(&self, obj: O) -> Result<String, AutomergeError>;

    /// Get the string that this text object represents at a point in history.
    fn text_at<O: AsRef<ExId>>(
        &self,
        obj: O,
        heads: &[ChangeHash],
    ) -> Result<String, AutomergeError>;

    /// test spans api for mark/span experiment
    fn spans<O: AsRef<ExId>>(&self, obj: O) -> Result<Vec<query::Span<'_>>, AutomergeError>;

    /// test raw_spans api for mark/span experiment
    fn raw_spans<O: AsRef<ExId>>(&self, obj: O) -> Result<Vec<query::SpanInfo>, AutomergeError>;

    /// test attribute api for mark/span experiment
    fn attribute<O: AsRef<ExId>>(
        &self,
        obj: O,
        baseline: &[ChangeHash],
        change_sets: &[Vec<ChangeHash>],
    ) -> Result<Vec<query::ChangeSet>, AutomergeError>;

    /// test attribute api for mark/span experiment
    fn attribute2<O: AsRef<ExId>>(
        &self,
        obj: O,
        baseline: &[ChangeHash],
        change_sets: &[Vec<ChangeHash>],
    ) -> Result<Vec<query::ChangeSet2>, AutomergeError>;

    /// Get the value at this prop in the object.
    fn get<O: AsRef<ExId>, P: Into<Prop>>(
        &self,
        obj: O,
        prop: P,
    ) -> Result<Option<(Value<'_>, ExId)>, AutomergeError>;

    /// Get the value at this prop in the object at a point in history.
    fn get_at<O: AsRef<ExId>, P: Into<Prop>>(
        &self,
        obj: O,
        prop: P,
        heads: &[ChangeHash],
    ) -> Result<Option<(Value<'_>, ExId)>, AutomergeError>;

    fn get_all<O: AsRef<ExId>, P: Into<Prop>>(
        &self,
        obj: O,
        prop: P,
    ) -> Result<Vec<(Value<'_>, ExId)>, AutomergeError>;

    fn get_all_at<O: AsRef<ExId>, P: Into<Prop>>(
        &self,
        obj: O,
        prop: P,
        heads: &[ChangeHash],
    ) -> Result<Vec<(Value<'_>, ExId)>, AutomergeError>;

    /// Get the parents of an object in the document tree.
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

    fn path_to_object<O: AsRef<ExId>>(&self, obj: O) -> Result<Vec<(ExId, Prop)>, AutomergeError> {
        let mut path = self.parents(obj.as_ref().clone())?.collect::<Vec<_>>();
        path.reverse();
        Ok(path)
    }
}
