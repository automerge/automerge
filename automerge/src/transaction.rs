use crate::exid::ExId;
use crate::AutomergeError;
use crate::{Automerge, ChangeHash, Prop, Value};

mod inner;
pub(crate) use inner::TransactionInner;

/// A transaction on a document.
/// Transactions group operations into a single change so that no other operations can happen
/// in-between.
///
/// Created from [`Automerge::tx`].
#[derive(Debug)]
pub struct Transaction<'a> {
    pub(crate) inner: TransactionInner,
    pub(crate) doc: &'a mut Automerge,
}

impl<'a> Transaction<'a> {
    pub fn pending_ops(&self) -> usize {
        self.inner.pending_ops()
    }

    /// Commit the operations performed in this transaction, returning the hashes corresponding to
    /// the new heads.
    pub fn commit(self, message: Option<String>, time: Option<i64>) -> Vec<ChangeHash> {
        self.inner.commit(self.doc, message, time)
    }

    /// Undo the operations added in this transaction, returning the number of cancelled
    /// operations.
    pub fn rollback(self) -> usize {
        self.inner.rollback(self.doc)
    }

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
    pub fn set<P: Into<Prop>, V: Into<Value>>(
        &mut self,
        obj: &ExId,
        prop: P,
        value: V,
    ) -> Result<Option<ExId>, AutomergeError> {
        self.inner.set(self.doc, obj, prop, value)
    }

    pub fn insert<V: Into<Value>>(
        &mut self,
        obj: &ExId,
        index: usize,
        value: V,
    ) -> Result<Option<ExId>, AutomergeError> {
        self.inner.insert(self.doc, obj, index, value)
    }

    pub fn inc<P: Into<Prop>>(
        &mut self,
        obj: &ExId,
        prop: P,
        value: i64,
    ) -> Result<(), AutomergeError> {
        self.inner.inc(self.doc, obj, prop, value)
    }

    pub fn del<P: Into<Prop>>(&mut self, obj: &ExId, prop: P) -> Result<(), AutomergeError> {
        self.inner.del(self.doc, obj, prop)
    }

    /// Splice new elements into the given sequence. Returns a vector of the OpIds used to insert
    /// the new elements
    pub fn splice(
        &mut self,
        obj: &ExId,
        pos: usize,
        del: usize,
        vals: Vec<Value>,
    ) -> Result<Vec<ExId>, AutomergeError> {
        self.inner.splice(self.doc, obj, pos, del, vals)
    }

    pub fn splice_text(
        &mut self,
        obj: &ExId,
        pos: usize,
        del: usize,
        text: &str,
    ) -> Result<Vec<ExId>, AutomergeError> {
        self.inner.splice_text(self.doc, obj, pos, del, text)
    }

    pub fn keys(&self, obj: &ExId) -> Vec<String> {
        self.doc.keys(obj)
    }

    pub fn keys_at(&self, obj: &ExId, heads: &[ChangeHash]) -> Vec<String> {
        self.doc.keys_at(obj, heads)
    }

    pub fn length(&self, obj: &ExId) -> usize {
        self.doc.length(obj)
    }

    pub fn length_at(&self, obj: &ExId, heads: &[ChangeHash]) -> usize {
        self.doc.length_at(obj, heads)
    }

    pub fn text(&self, obj: &ExId) -> Result<String, AutomergeError> {
        self.doc.text(obj)
    }

    pub fn text_at(&self, obj: &ExId, heads: &[ChangeHash]) -> Result<String, AutomergeError> {
        self.doc.text_at(obj, heads)
    }

    pub fn value<P: Into<Prop>>(
        &self,
        obj: &ExId,
        prop: P,
    ) -> Result<Option<(Value, ExId)>, AutomergeError> {
        self.doc.value(obj, prop)
    }

    pub fn value_at<P: Into<Prop>>(
        &self,
        obj: &ExId,
        prop: P,
        heads: &[ChangeHash],
    ) -> Result<Option<(Value, ExId)>, AutomergeError> {
        self.doc.value_at(obj, prop, heads)
    }

    pub fn values<P: Into<Prop>>(
        &self,
        obj: &ExId,
        prop: P,
    ) -> Result<Vec<(Value, ExId)>, AutomergeError> {
        self.doc.values(obj, prop)
    }

    pub fn values_at<P: Into<Prop>>(
        &self,
        obj: &ExId,
        prop: P,
        heads: &[ChangeHash],
    ) -> Result<Vec<(Value, ExId)>, AutomergeError> {
        self.doc.values_at(obj, prop, heads)
    }
}
