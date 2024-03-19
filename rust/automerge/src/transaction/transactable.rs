use std::borrow::{Borrow, Cow};
use std::collections::HashMap;

use unicode_segmentation::UnicodeSegmentation;

use crate::exid::ExId;
use crate::marks::{ExpandMark, Mark};
use crate::text_value::TextValue;
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

    fn split_block<'a, O>(
        &mut self,
        obj: O,
        index: usize,
        args: NewBlock<'a>,
    ) -> Result<ExId, AutomergeError>
    where
        O: AsRef<ExId>;

    fn join_block<O: AsRef<ExId>>(&mut self, text: O, index: usize) -> Result<(), AutomergeError>;

    fn update_block<'p, O>(
        &mut self,
        text: O,
        index: usize,
        new_block: NewBlock<'p>,
    ) -> Result<(), AutomergeError>
    where
        O: AsRef<ExId>;

    fn update_blocks<'a, O: AsRef<ExId>, I: IntoIterator<Item = BlockOrText<'a>>>(
        &mut self,
        text: O,
        new_text: I,
    ) -> Result<(), AutomergeError>;

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

#[derive(Debug, PartialEq, Clone)]
pub enum BlockOrText<'a> {
    Block(crate::Block),
    Text(Cow<'a, str>),
}

impl<'a> BlockOrText<'a> {
    pub(crate) fn width(&self) -> usize {
        match self {
            BlockOrText::Block(b) => 1,
            BlockOrText::Text(t) => t.graphemes(true).map(|c| TextValue::width(c)).sum(),
        }
    }
}

#[derive(Debug)]
pub struct NewBlock<'a> {
    pub(crate) block_type: &'a str,
    pub(crate) parents: Vec<String>,
    pub(crate) attrs: HashMap<String, ScalarValue>,
}

impl<'a> NewBlock<'a> {
    pub fn new(block_type: &'a str) -> NewBlock<'a> {
        NewBlock {
            block_type,
            parents: Vec::new(),
            attrs: HashMap::new(),
        }
    }

    pub fn with_parents<I: IntoIterator, >(self, parents: I) -> NewBlock<'a> 
    where
        I::Item: std::borrow::Borrow<str>
    {
        NewBlock {
            block_type: self.block_type,
            parents: parents.into_iter().map(|s| s.borrow().to_string()).collect(),
            attrs: self.attrs,
        }
    }

    pub fn with_attr(self, key: &str, value: ScalarValue) -> NewBlock<'a> {
        let mut attrs = self.attrs;
        attrs.insert(key.to_string(), value);
        NewBlock {
            block_type: self.block_type,
            parents: self.parents,
            attrs,
        }
    }

    pub fn with_attrs(self, attrs: HashMap<String, ScalarValue>) -> NewBlock<'a> {
        NewBlock {
            block_type: self.block_type,
            parents: self.parents,
            attrs,
        }
    }
}
