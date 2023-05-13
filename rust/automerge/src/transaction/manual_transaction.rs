use std::ops::RangeBounds;

use crate::exid::ExId;
use crate::iter::{Keys, ListRange, MapRange, Values};
use crate::marks::{ExpandMark, Mark};
use crate::op_observer::BranchableObserver;
use crate::AutomergeError;
use crate::{
    Automerge, ChangeHash, Cursor, ObjType, OpObserver, Prop, ReadDoc, ScalarValue, Value,
};

use super::{observation, CommitOptions, Transactable, TransactionArgs, TransactionInner};

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
pub struct Transaction<'a, Obs: observation::Observation> {
    // this is an option so that we can take it during commit and rollback to prevent it being
    // rolled back during drop.
    inner: Option<TransactionInner>,
    // As with `inner` this is an `Option` so we can `take` it during `commit`
    observation: Option<Obs>,
    doc: &'a mut Automerge,
}

impl<'a, Obs: observation::Observation> Transaction<'a, Obs> {
    pub(crate) fn new(doc: &'a mut Automerge, args: TransactionArgs, obs: Obs) -> Self {
        Self {
            inner: Some(TransactionInner::new(args)),
            doc,
            observation: Some(obs),
        }
    }

    /// Get the hash of the change that contains the given opid.
    ///
    /// Returns none if the opid:
    /// - is the root object id
    /// - does not exist in this document
    /// - is for an operation in this transaction
    pub fn hash_for_opid(&self, opid: &ExId) -> Option<ChangeHash> {
        self.doc.hash_for_opid(opid)
    }
}

impl<'a> Transaction<'a, observation::UnObserved> {
    pub(crate) fn empty(
        doc: &'a mut Automerge,
        args: TransactionArgs,
        opts: CommitOptions,
    ) -> ChangeHash {
        TransactionInner::empty(doc, args, opts.message, opts.time)
    }
}

impl<'a, Obs: OpObserver + BranchableObserver> Transaction<'a, observation::Observed<Obs>> {
    pub fn observer(&mut self) -> &mut Obs {
        self.observation.as_mut().unwrap().observer()
    }
}

impl<'a, Obs: observation::Observation> Transaction<'a, Obs> {
    /// Get the heads of the document before this transaction was started.
    pub fn get_heads(&self) -> Vec<ChangeHash> {
        self.doc.get_heads()
    }

    /// Commit the operations performed in this transaction, returning the hashes corresponding to
    /// the new heads.
    pub fn commit(mut self) -> Obs::CommitResult {
        let tx = self.inner.take().unwrap();
        let hash = tx.commit(self.doc, None, None);
        let obs = self.observation.take().unwrap();
        obs.make_result(hash)
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
    /// tx.put_object(ROOT, "todos", ObjType::List).unwrap();
    /// let now = SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).unwrap().as_secs() as
    /// i64;
    /// tx.commit_with(CommitOptions::default().with_message("Create todos list").with_time(now));
    /// ```
    pub fn commit_with(mut self, options: CommitOptions) -> Obs::CommitResult {
        let tx = self.inner.take().unwrap();
        let hash = tx.commit(self.doc, options.message, options.time);
        let obs = self.observation.take().unwrap();
        obs.make_result(hash)
    }

    /// Undo the operations added in this transaction, returning the number of cancelled
    /// operations.
    pub fn rollback(mut self) -> usize {
        self.inner.take().unwrap().rollback(self.doc)
    }

    fn do_tx<F, O>(&mut self, f: F) -> O
    where
        F: FnOnce(&mut TransactionInner, &mut Automerge, Option<&mut Obs::Obs>) -> O,
    {
        let tx = self.inner.as_mut().unwrap();
        if let Some(obs) = self.observation.as_mut() {
            f(tx, self.doc, obs.observer())
        } else {
            f(tx, self.doc, None)
        }
    }
}

impl<'a, Obs: observation::Observation> ReadDoc for Transaction<'a, Obs> {
    fn keys<O: AsRef<ExId>>(&self, obj: O) -> Keys<'_> {
        self.doc.keys(obj)
    }

    fn keys_at<O: AsRef<ExId>>(&self, obj: O, heads: &[ChangeHash]) -> Keys<'_> {
        self.doc.keys_at(obj, heads)
    }

    fn map_range<'b, O: AsRef<ExId>, R: RangeBounds<String> + 'b>(
        &'b self,
        obj: O,
        range: R,
    ) -> MapRange<'b, R> {
        self.doc.map_range(obj, range)
    }

    fn map_range_at<'b, O: AsRef<ExId>, R: RangeBounds<String> + 'b>(
        &'b self,
        obj: O,
        range: R,
        heads: &[ChangeHash],
    ) -> MapRange<'b, R> {
        self.doc.map_range_at(obj, range, heads)
    }

    fn list_range<O: AsRef<ExId>, R: RangeBounds<usize>>(
        &self,
        obj: O,
        range: R,
    ) -> ListRange<'_, R> {
        self.doc.list_range(obj, range)
    }

    fn list_range_at<O: AsRef<ExId>, R: RangeBounds<usize>>(
        &self,
        obj: O,
        range: R,
        heads: &[ChangeHash],
    ) -> ListRange<'_, R> {
        self.doc.list_range_at(obj, range, heads)
    }

    fn values<O: AsRef<ExId>>(&self, obj: O) -> Values<'_> {
        self.doc.values(obj)
    }

    fn values_at<O: AsRef<ExId>>(&self, obj: O, heads: &[ChangeHash]) -> Values<'_> {
        self.doc.values_at(obj, heads)
    }

    fn length<O: AsRef<ExId>>(&self, obj: O) -> usize {
        self.doc.length(obj)
    }

    fn length_at<O: AsRef<ExId>>(&self, obj: O, heads: &[ChangeHash]) -> usize {
        self.doc.length_at(obj, heads)
    }

    fn object_type<O: AsRef<ExId>>(&self, obj: O) -> Result<ObjType, AutomergeError> {
        self.doc.object_type(obj)
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

    fn get_cursor<O: AsRef<ExId>>(
        &self,
        obj: O,
        position: usize,
        at: Option<&[ChangeHash]>,
    ) -> Result<Cursor, AutomergeError> {
        self.doc.get_cursor(obj, position, at)
    }

    fn get_cursor_position<O: AsRef<ExId>>(
        &self,
        obj: O,
        address: &Cursor,
        at: Option<&[ChangeHash]>,
    ) -> Result<usize, AutomergeError> {
        self.doc.get_cursor_position(obj, address, at)
    }

    fn marks<O: AsRef<ExId>>(&self, obj: O) -> Result<Vec<Mark<'_>>, AutomergeError> {
        self.doc.marks(obj)
    }

    fn marks_at<O: AsRef<ExId>>(
        &self,
        obj: O,
        heads: &[ChangeHash],
    ) -> Result<Vec<Mark<'_>>, AutomergeError> {
        self.doc.marks_at(obj, heads)
    }

    fn get<O: AsRef<ExId>, P: Into<Prop>>(
        &self,
        obj: O,
        prop: P,
    ) -> Result<Option<(Value<'_>, ExId)>, AutomergeError> {
        self.doc.get(obj, prop)
    }

    fn get_at<O: AsRef<ExId>, P: Into<Prop>>(
        &self,
        obj: O,
        prop: P,
        heads: &[ChangeHash],
    ) -> Result<Option<(Value<'_>, ExId)>, AutomergeError> {
        self.doc.get_at(obj, prop, heads)
    }

    fn get_all<O: AsRef<ExId>, P: Into<Prop>>(
        &self,
        obj: O,
        prop: P,
    ) -> Result<Vec<(Value<'_>, ExId)>, AutomergeError> {
        self.doc.get_all(obj, prop)
    }

    fn get_all_at<O: AsRef<ExId>, P: Into<Prop>>(
        &self,
        obj: O,
        prop: P,
        heads: &[ChangeHash],
    ) -> Result<Vec<(Value<'_>, ExId)>, AutomergeError> {
        self.doc.get_all_at(obj, prop, heads)
    }

    fn parents<O: AsRef<ExId>>(&self, obj: O) -> Result<crate::Parents<'_>, AutomergeError> {
        self.doc.parents(obj)
    }

    fn parents_at<O: AsRef<ExId>>(
        &self,
        obj: O,
        heads: &[ChangeHash],
    ) -> Result<crate::Parents<'_>, AutomergeError> {
        self.doc.parents_at(obj, heads)
    }

    fn get_missing_deps(&self, heads: &[ChangeHash]) -> Vec<ChangeHash> {
        self.doc.get_missing_deps(heads)
    }

    fn get_change_by_hash(&self, hash: &ChangeHash) -> Option<&crate::Change> {
        self.doc.get_change_by_hash(hash)
    }
}

impl<'a, Obs: observation::Observation> Transactable for Transaction<'a, Obs> {
    /// Get the number of pending operations in this transaction.
    fn pending_ops(&self) -> usize {
        self.inner.as_ref().unwrap().pending_ops()
    }

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
    ) -> Result<(), AutomergeError> {
        self.do_tx(|tx, doc, obs| tx.put(doc, obs, obj.as_ref(), prop, value))
    }

    fn put_object<O: AsRef<ExId>, P: Into<Prop>>(
        &mut self,
        obj: O,
        prop: P,
        value: ObjType,
    ) -> Result<ExId, AutomergeError> {
        self.do_tx(|tx, doc, obs| tx.put_object(doc, obs, obj.as_ref(), prop, value))
    }

    fn insert<O: AsRef<ExId>, V: Into<ScalarValue>>(
        &mut self,
        obj: O,
        index: usize,
        value: V,
    ) -> Result<(), AutomergeError> {
        self.do_tx(|tx, doc, obs| tx.insert(doc, obs, obj.as_ref(), index, value))
    }

    fn insert_object<O: AsRef<ExId>>(
        &mut self,
        obj: O,
        index: usize,
        value: ObjType,
    ) -> Result<ExId, AutomergeError> {
        self.do_tx(|tx, doc, obs| tx.insert_object(doc, obs, obj.as_ref(), index, value))
    }

    fn increment<O: AsRef<ExId>, P: Into<Prop>>(
        &mut self,
        obj: O,
        prop: P,
        value: i64,
    ) -> Result<(), AutomergeError> {
        self.do_tx(|tx, doc, obs| tx.increment(doc, obs, obj.as_ref(), prop, value))
    }

    fn delete<O: AsRef<ExId>, P: Into<Prop>>(
        &mut self,
        obj: O,
        prop: P,
    ) -> Result<(), AutomergeError> {
        self.do_tx(|tx, doc, obs| tx.delete(doc, obs, obj.as_ref(), prop))
    }

    /// Splice new elements into the given sequence. Returns a vector of the OpIds used to insert
    /// the new elements
    fn splice<O: AsRef<ExId>, V: IntoIterator<Item = ScalarValue>>(
        &mut self,
        obj: O,
        pos: usize,
        del: usize,
        vals: V,
    ) -> Result<(), AutomergeError> {
        self.do_tx(|tx, doc, obs| tx.splice(doc, obs, obj.as_ref(), pos, del, vals))
    }

    fn splice_text<O: AsRef<ExId>>(
        &mut self,
        obj: O,
        pos: usize,
        del: usize,
        text: &str,
    ) -> Result<(), AutomergeError> {
        self.do_tx(|tx, doc, obs| tx.splice_text(doc, obs, obj.as_ref(), pos, del, text))
    }

    fn mark<O: AsRef<ExId>>(
        &mut self,
        obj: O,
        mark: Mark<'_>,
        expand: ExpandMark,
    ) -> Result<(), AutomergeError> {
        self.do_tx(|tx, doc, obs| tx.mark(doc, obs, obj.as_ref(), mark, expand))
    }

    fn unmark<O: AsRef<ExId>>(
        &mut self,
        obj: O,
        name: &str,
        start: usize,
        end: usize,
        expand: ExpandMark,
    ) -> Result<(), AutomergeError> {
        self.do_tx(|tx, doc, obs| tx.unmark(doc, obs, obj.as_ref(), name, start, end, expand))
    }

    fn base_heads(&self) -> Vec<ChangeHash> {
        self.doc.get_heads()
    }
}

// If a transaction is not commited or rolled back manually then it can leave the document in an
// intermediate state.
// This defaults to rolling back the transaction to be compatible with `?` error returning before
// reaching a call to `commit`.
impl<'a, Obs: observation::Observation> Drop for Transaction<'a, Obs> {
    fn drop(&mut self) {
        if let Some(txn) = self.inner.take() {
            txn.rollback(self.doc);
        }
    }
}
