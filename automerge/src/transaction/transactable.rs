use std::ops::RangeBounds;

use crate::exid::ExId;
use crate::{
    AutomergeError, ChangeHash, Keys, KeysAt, ObjType, Prop, Range, RangeAt, ScalarValue, Value,
    Values, ValuesAt,
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
    fn insert_object(
        &mut self,
        obj: &ExId,
        index: usize,
        object: ObjType,
    ) -> Result<ExId, AutomergeError>;

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
    fn keys<O: AsRef<ExId>>(&self, obj: O) -> Keys;

    /// Get the keys of the given object at a point in history.
    fn keys_at<O: AsRef<ExId>>(&self, obj: O, heads: &[ChangeHash]) -> KeysAt;

    fn range<O: AsRef<ExId>, R: RangeBounds<Prop>>(&self, obj: O, range: R) -> Range<R>;

    fn range_at<O: AsRef<ExId>, R: RangeBounds<Prop>>(
        &self,
        obj: O,
        range: R,
        heads: &[ChangeHash],
    ) -> RangeAt<R>;

    fn values<O: AsRef<ExId>>(&self, obj: O) -> Values;

    fn values_at<O: AsRef<ExId>>(&self, obj: O, heads: &[ChangeHash]) -> ValuesAt;

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

    /// Get the value at this prop in the object.
    fn get<O: AsRef<ExId>, P: Into<Prop>>(
        &self,
        obj: O,
        prop: P,
    ) -> Result<Option<(Value, ExId)>, AutomergeError>;

    /// Get the value at this prop in the object at a point in history.
    fn get_at<O: AsRef<ExId>, P: Into<Prop>>(
        &self,
        obj: O,
        prop: P,
        heads: &[ChangeHash],
    ) -> Result<Option<(Value, ExId)>, AutomergeError>;

    fn get_all<O: AsRef<ExId>, P: Into<Prop>>(
        &self,
        obj: O,
        prop: P,
    ) -> Result<Vec<(Value, ExId)>, AutomergeError>;

    fn get_all_at<O: AsRef<ExId>, P: Into<Prop>>(
        &self,
        obj: O,
        prop: P,
        heads: &[ChangeHash],
    ) -> Result<Vec<(Value, ExId)>, AutomergeError>;

    /// Get the object id of the object that contains this object and the prop that this object is
    /// at in that object.
    fn parent_object<O: AsRef<ExId>>(&self, obj: O) -> Option<(ExId, Prop)>;

    fn path_to_object<O: AsRef<ExId>>(&self, obj: O) -> Vec<(ExId, Prop)>;
}
