use crate::exid::ExId;
use crate::{AutomergeError, ChangeHash, Keys, KeysAt, Prop, Value};
use unicode_segmentation::UnicodeSegmentation;

/// A way of mutating a document within a single change.
pub trait Transactable {
    /// Get the number of pending operations in this transaction.
    fn pending_ops(&self) -> usize;

    /// Set the value of property `P` to value `V` in object `obj`.
    ///
    /// # Returns
    ///
    /// The opid of the operation which was created, or None if this operation doesn't change the
    /// document
    ///
    /// # Errors
    ///
    /// This will return an error if
    /// - The object does not exist
    /// - The key is the wrong type for the object
    /// - The key does not exist in the object
    fn set<P: Into<Prop>, V: Into<Value>, O: AsRef<ExId>>(
        &mut self,
        obj: O,
        prop: P,
        value: V,
    ) -> Result<Option<ExId>, AutomergeError>;

    /// Insert a value into a list at the given index.
    fn insert<V: Into<Value>, O: AsRef<ExId>>(
        &mut self,
        obj: O,
        index: usize,
        value: V,
    ) -> Result<Option<ExId>, AutomergeError>;

    /// Increment the counter at the prop in the object by `value`.
    fn inc<P: Into<Prop>, O: AsRef<ExId>>(
        &mut self,
        obj: O,
        prop: P,
        value: i64,
    ) -> Result<(), AutomergeError>;

    /// Delete the value at prop in the object.
    fn del<P: Into<Prop>, O: AsRef<ExId>>(&mut self, obj: O, prop: P)
        -> Result<(), AutomergeError>;

    /// Splice new elements into the given sequence. Returns a vector of the OpIds used to insert
    /// the new elements.
    fn splice<O: AsRef<ExId>>(
        &mut self,
        obj: O,
        pos: usize,
        del: usize,
        vals: Vec<Value>,
    ) -> Result<Vec<ExId>, AutomergeError>;

    /// Like [`Self::splice`] but for text.
    fn splice_text<O: AsRef<ExId>>(
        &mut self,
        obj: O,
        pos: usize,
        del: usize,
        text: &str,
    ) -> Result<Vec<ExId>, AutomergeError> {
        let mut vals = vec![];
        for c in text.to_owned().graphemes(true) {
            vals.push(c.into());
        }
        self.splice(obj, pos, del, vals)
    }

    /// Get the keys of the given object, it should be a map.
    fn keys<O: AsRef<ExId>>(&self, obj: O) -> Keys;

    /// Get the keys of the given object at a point in history.
    fn keys_at<O: AsRef<ExId>>(&self, obj: O, heads: &[ChangeHash]) -> KeysAt;

    /// Get the length of the given object.
    fn length<O: AsRef<ExId>>(&self, obj: O) -> usize;

    /// Get the length of the given object at a point in history.
    fn length_at<O: AsRef<ExId>>(&self, obj: O, heads: &[ChangeHash]) -> usize;

    /// Get the string that this text object represents.
    fn text<O: AsRef<ExId>>(&self, obj: O) -> Result<String, AutomergeError>;

    /// Get the string that this text object represents at a point in history.
    fn text_at<O: AsRef<ExId>>(
        &self,
        obj: O,
        heads: &[ChangeHash],
    ) -> Result<String, AutomergeError>;

    /// Get the value at this prop in the object.
    fn value<P: Into<Prop>, O: AsRef<ExId>>(
        &self,
        obj: O,
        prop: P,
    ) -> Result<Option<(Value, ExId)>, AutomergeError>;

    /// Get the value at this prop in the object at a point in history.
    fn value_at<P: Into<Prop>, O: AsRef<ExId>>(
        &self,
        obj: O,
        prop: P,
        heads: &[ChangeHash],
    ) -> Result<Option<(Value, ExId)>, AutomergeError>;

    fn values<P: Into<Prop>, O: AsRef<ExId>>(
        &self,
        obj: O,
        prop: P,
    ) -> Result<Vec<(Value, ExId)>, AutomergeError>;

    fn values_at<P: Into<Prop>, O: AsRef<ExId>>(
        &self,
        obj: O,
        prop: P,
        heads: &[ChangeHash],
    ) -> Result<Vec<(Value, ExId)>, AutomergeError>;
}
