use crate::exid::ExId;
use crate::marks::{ExpandMark, Mark};
use crate::{AutomergeError, ChangeHash, ObjType, Prop, ReadDoc, ScalarValue};

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

    fn split_block<O>(&mut self, obj: O, index: usize) -> Result<ExId, AutomergeError>
    where
        O: AsRef<ExId>;

    fn join_block<O: AsRef<ExId>>(&mut self, text: O, index: usize) -> Result<(), AutomergeError>;

    fn replace_block<O>(&mut self, text: O, index: usize) -> Result<ExId, AutomergeError>
    where
        O: AsRef<ExId>;

    /// The heads this transaction will be based on
    fn base_heads(&self) -> Vec<ChangeHash>;

    /// Update the value of a string
    ///
    /// This will calculate a diff between the current value and the new value and
    /// then convert that diff into calls to {@link splice}. This will produce results
    /// which don't merge as well as directly capturing the user input actions, but
    /// sometimes it's not possible to capture user input and this is the best you
    /// can do.
    fn update_text<S: AsRef<str>>(&mut self, obj: &ExId, new_text: S)
        -> Result<(), AutomergeError>;
}
