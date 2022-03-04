use crate::exid::ExId;
use crate::query;
use crate::{AutomergeError, ChangeHash, Keys, KeysAt, ObjType, Prop, ScalarValue, Value};
use unicode_segmentation::UnicodeSegmentation;

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
    fn set<O: AsRef<ExId>, P: Into<Prop>, V: Into<ScalarValue>>(
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
    fn set_object<O: AsRef<ExId>, P: Into<Prop>>(
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

    /// Increment the counter at the prop in the object by `value`.
    fn inc<O: AsRef<ExId>, P: Into<Prop>>(
        &mut self,
        obj: O,
        prop: P,
        value: i64,
    ) -> Result<(), AutomergeError>;

    /// Delete the value at prop in the object.
    fn del<O: AsRef<ExId>, P: Into<Prop>>(&mut self, obj: O, prop: P)
        -> Result<(), AutomergeError>;

    fn splice<O: AsRef<ExId>>(
        &mut self,
        obj: O,
        pos: usize,
        del: usize,
        vals: Vec<ScalarValue>,
    ) -> Result<(), AutomergeError>;

    /// Like [`Self::splice`] but for text.
    fn splice_text<O: AsRef<ExId>>(
        &mut self,
        obj: O,
        pos: usize,
        del: usize,
        text: &str,
    ) -> Result<(), AutomergeError> {
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

    /// Get the string that this text object represents.
    fn list<O: AsRef<ExId>>(&self, obj: O) -> Result<Vec<(Value, ExId)>, AutomergeError>;

    /// Get the string that this text object represents at a point in history.
    fn list_at<O: AsRef<ExId>>(
        &self,
        obj: O,
        heads: &[ChangeHash],
    ) -> Result<Vec<(Value, ExId)>, AutomergeError>;

    /// test spans api for mark/span experiment
    fn spans<O: AsRef<ExId>>(&self, obj: O) -> Result<Vec<query::Span>, AutomergeError>;

    /// test raw_spans api for mark/span experiment
    fn raw_spans<O: AsRef<ExId>>(&self, obj: O) -> Result<Vec<query::SpanInfo>, AutomergeError>;

    /// test blame api for mark/span experiment
    fn blame<O: AsRef<ExId>>(
        &self,
        obj: O,
        baseline: &[ChangeHash],
        change_sets: &[Vec<ChangeHash>],
    ) -> Result<Vec<query::ChangeSet>, AutomergeError>;

    /// Get the value at this prop in the object.
    fn value<O: AsRef<ExId>, P: Into<Prop>>(
        &self,
        obj: O,
        prop: P,
    ) -> Result<Option<(Value, ExId)>, AutomergeError>;

    /// Get the value at this prop in the object at a point in history.
    fn value_at<O: AsRef<ExId>, P: Into<Prop>>(
        &self,
        obj: O,
        prop: P,
        heads: &[ChangeHash],
    ) -> Result<Option<(Value, ExId)>, AutomergeError>;

    fn values<O: AsRef<ExId>, P: Into<Prop>>(
        &self,
        obj: O,
        prop: P,
    ) -> Result<Vec<(Value, ExId)>, AutomergeError>;

    fn values_at<O: AsRef<ExId>, P: Into<Prop>>(
        &self,
        obj: O,
        prop: P,
        heads: &[ChangeHash],
    ) -> Result<Vec<(Value, ExId)>, AutomergeError>;
}
