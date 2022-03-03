use crate::exid::ExId;
use crate::{Automerge, ChangeHash, KeysAt, ObjType, Prop, ScalarValue, Value};
use crate::{AutomergeError, Keys};

use super::{CommitOptions, Transactable, TransactionInner};

/// A transaction on a document.
/// Transactions group operations into a single change so that no other operations can happen
/// in-between.
///
/// Created from [`Automerge::transaction`].
///
/// ## Drop
///
/// This transaction should be manually committed or rolled back. If not done manually then it will
/// be rolled back when it is dropped. This is to prevent the document being in an unsafe
/// intermediate state.
/// This is consistent with `?` error handling.
#[derive(Debug)]
pub struct Transaction<'a> {
    // this is an option so that we can take it during commit and rollback to prevent it being
    // rolled back during drop.
    pub(crate) inner: Option<TransactionInner>,
    pub(crate) doc: &'a mut Automerge,
}

impl<'a> Transaction<'a> {
    /// Get the heads of the document before this transaction was started.
    pub fn get_heads(&self) -> Vec<ChangeHash> {
        self.doc.get_heads()
    }

    /// Commit the operations performed in this transaction, returning the hashes corresponding to
    /// the new heads.
    pub fn commit(mut self) -> Vec<ChangeHash> {
        self.inner.take().unwrap().commit(self.doc, None, None)
    }

    /// Commit the operations in this transaction with some options.
    ///
    /// ```
    /// # use automerge::transaction::CommitOptions;
    /// # use automerge::transaction::Transactable;
    /// # use automerge::ROOT;
    /// # use automerge::Automerge;
    /// # use automerge::ObjType;
    /// # use std::time::SystemTime;
    /// let mut doc = Automerge::new();
    /// let mut tx = doc.transaction();
    /// tx.make(ROOT, "todos", ObjType::List).unwrap();
    /// let now = SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).unwrap().as_secs() as
    /// i64;
    /// tx.commit_with(CommitOptions::default().with_message("Create todos list").with_time(now));
    /// ```
    pub fn commit_with(mut self, options: CommitOptions) -> Vec<ChangeHash> {
        self.inner
            .take()
            .unwrap()
            .commit(self.doc, options.message, options.time)
    }

    /// Undo the operations added in this transaction, returning the number of cancelled
    /// operations.
    pub fn rollback(mut self) -> usize {
        self.inner.take().unwrap().rollback(self.doc)
    }
}

impl<'a> Transactable for Transaction<'a> {
    /// Get the number of pending operations in this transaction.
    fn pending_ops(&self) -> usize {
        self.inner.as_ref().unwrap().pending_ops()
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
    fn set<O: AsRef<ExId>, P: Into<Prop>, V: Into<ScalarValue>>(
        &mut self,
        obj: O,
        prop: P,
        value: V,
    ) -> Result<(), AutomergeError> {
        self.inner
            .as_mut()
            .unwrap()
            .set(self.doc, obj.as_ref(), prop, value)
    }

    fn make<O: AsRef<ExId>, P: Into<Prop>, V: Into<ObjType>>(
        &mut self,
        obj: O,
        prop: P,
        value: V,
    ) -> Result<ExId, AutomergeError> {
        self.inner
            .as_mut()
            .unwrap()
            .make(self.doc, obj.as_ref(), prop, value)
    }

    fn insert<O: AsRef<ExId>, V: Into<ScalarValue>>(
        &mut self,
        obj: O,
        index: usize,
        value: V,
    ) -> Result<(), AutomergeError> {
        self.inner
            .as_mut()
            .unwrap()
            .insert(self.doc, obj.as_ref(), index, value)
    }

    fn make_insert<V: Into<ObjType>>(
        &mut self,
        obj: &ExId,
        index: usize,
        value: V,
    ) -> Result<ExId, AutomergeError> {
        self.inner
            .as_mut()
            .unwrap()
            .make_insert(self.doc, obj, index, value)
    }

    fn inc<O: AsRef<ExId>, P: Into<Prop>>(
        &mut self,
        obj: O,
        prop: P,
        value: i64,
    ) -> Result<(), AutomergeError> {
        self.inner
            .as_mut()
            .unwrap()
            .inc(self.doc, obj.as_ref(), prop, value)
    }

    fn del<O: AsRef<ExId>, P: Into<Prop>>(
        &mut self,
        obj: O,
        prop: P,
    ) -> Result<(), AutomergeError> {
        self.inner
            .as_mut()
            .unwrap()
            .del(self.doc, obj.as_ref(), prop)
    }

    /// Splice new elements into the given sequence. Returns a vector of the OpIds used to insert
    /// the new elements
    fn splice<O: AsRef<ExId>>(
        &mut self,
        obj: O,
        pos: usize,
        del: usize,
        vals: Vec<Value>,
    ) -> Result<Vec<ExId>, AutomergeError> {
        self.inner
            .as_mut()
            .unwrap()
            .splice(self.doc, obj.as_ref(), pos, del, vals)
    }

    fn keys<O: AsRef<ExId>>(&self, obj: O) -> Keys {
        self.doc.keys(obj)
    }

    fn keys_at<O: AsRef<ExId>>(&self, obj: O, heads: &[ChangeHash]) -> KeysAt {
        self.doc.keys_at(obj, heads)
    }

    fn length<O: AsRef<ExId>>(&self, obj: O) -> usize {
        self.doc.length(obj)
    }

    fn length_at<O: AsRef<ExId>>(&self, obj: O, heads: &[ChangeHash]) -> usize {
        self.doc.length_at(obj, heads)
    }

    fn text<O: AsRef<ExId>>(&self, obj: O) -> Result<String, AutomergeError> {
        self.doc.text(obj)
    }

    fn text_at<O: AsRef<ExId>>(
        &self,
        obj: O,
        heads: &[ChangeHash],
    ) -> Result<String, AutomergeError> {
        self.doc.text_at(obj, heads)
    }

    fn value<O: AsRef<ExId>, P: Into<Prop>>(
        &self,
        obj: O,
        prop: P,
    ) -> Result<Option<(Value, ExId)>, AutomergeError> {
        self.doc.value(obj, prop)
    }

    fn value_at<O: AsRef<ExId>, P: Into<Prop>>(
        &self,
        obj: O,
        prop: P,
        heads: &[ChangeHash],
    ) -> Result<Option<(Value, ExId)>, AutomergeError> {
        self.doc.value_at(obj, prop, heads)
    }

    fn values<O: AsRef<ExId>, P: Into<Prop>>(
        &self,
        obj: O,
        prop: P,
    ) -> Result<Vec<(Value, ExId)>, AutomergeError> {
        self.doc.values(obj, prop)
    }

    fn values_at<O: AsRef<ExId>, P: Into<Prop>>(
        &self,
        obj: O,
        prop: P,
        heads: &[ChangeHash],
    ) -> Result<Vec<(Value, ExId)>, AutomergeError> {
        self.doc.values_at(obj, prop, heads)
    }
}

// If a transaction is not commited or rolled back manually then it can leave the document in an
// intermediate state.
// This defaults to rolling back the transaction to be compatible with `?` error returning before
// reaching a call to `commit`.
impl<'a> Drop for Transaction<'a> {
    fn drop(&mut self) {
        if let Some(txn) = self.inner.take() {
            txn.rollback(self.doc);
        }
    }
}
