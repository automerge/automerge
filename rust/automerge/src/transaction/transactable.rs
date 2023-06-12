use crate::exid::ExId;
use crate::marks::{ExpandMark, Mark};
use crate::{AutomergeError, ChangeHash, Cursor, ObjType, Prop, ReadDoc, ScalarValue};

/// A way of mutating a document within a single change.
pub trait Transactable: ReadDoc {
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

    /// replace a section of a list. If `del` is positive then N values
    /// are deleted after position `pos` and the new values inserted. If
    /// it is negative then N values are deleted before position `pos` instead.
    fn splice<O: AsRef<ExId>, V: IntoIterator<Item = ScalarValue>>(
        &mut self,
        obj: O,
        pos: usize,
        del: isize,
        vals: V,
    ) -> Result<(), AutomergeError>;

    /// Like [`Self::splice`] but for text.
    fn splice_text<O: AsRef<ExId>>(
        &mut self,
        obj: O,
        pos: usize,
        del: isize,
        text: &str,
    ) -> Result<(), AutomergeError>;

    /// Mark a sequence
    fn mark<O: AsRef<ExId>>(
        &mut self,
        obj: O,
        mark: Mark<'_>,
        expand: ExpandMark,
    ) -> Result<(), AutomergeError>;

    /// Remove a Mark from a sequence
    fn unmark<O: AsRef<ExId>>(
        &mut self,
        obj: O,
        key: &str,
        start: usize,
        end: usize,
        expand: ExpandMark,
    ) -> Result<(), AutomergeError>;

    fn split_block<S: Into<String>, O: AsRef<ExId>, P: IntoIterator<Item = S>>(
        &mut self,
        obj: O,
        index: usize,
        name: S,
        parents: P,
    ) -> Result<Cursor, AutomergeError>;

    fn join_block<O: AsRef<ExId>>(
        &mut self,
        obj: O,
        block_id: &Cursor,
    ) -> Result<(), AutomergeError>;

    fn update_block<S: Into<String>, O: AsRef<ExId>, P: IntoIterator<Item = S>>(
        &mut self,
        obj: O,
        block_id: &Cursor,
        name: S,
        parents: P,
    ) -> Result<(), AutomergeError>;

    /// The heads this transaction will be based on
    fn base_heads(&self) -> Vec<ChangeHash>;
}
