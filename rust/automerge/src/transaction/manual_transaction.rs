use std::ops::RangeBounds;

use crate::automerge::{Automerge, Parents, ReadDoc};
use crate::cursor::{CursorPosition, MoveCursor};
use crate::exid::ExId;
use crate::iter::{DocIter, Keys, ListRange, MapRange, Span, Spans, Values};
use crate::marks::{ExpandMark, Mark, MarkSet, UpdateSpansConfig};
use crate::patches::PatchLog;
use crate::types::{Clock, ScalarValue};
use crate::{hydrate, AutomergeError};
use crate::{ChangeHash, Cursor, ObjType, Prop, Value};

use super::{CommitOptions, Transactable, TransactionArgs, TransactionInner};

/// A transaction on a document.
/// Transactions group operations into a single change so that no other operations can happen
/// in-between.
///
/// Created from [`Automerge::transaction()`].
///
/// ## Drop
///
/// This transaction should be manually committed or rolled back. If not done manually then it will
/// be rolled back when it is dropped. This is to prevent the document being in an unsafe
/// intermediate state.
/// This is consistent with [`?`][std::ops::Try] error handling.
#[derive(Debug)]
pub struct Transaction<'a> {
    // this is an option so that we can take it during commit and rollback to prevent it being
    // rolled back during drop.
    inner: Option<TransactionInner>,
    patch_log: PatchLog,
    doc: &'a mut Automerge,
}

impl<'a> Transaction<'a> {
    pub(crate) fn new(
        doc: &'a mut Automerge,
        args: TransactionArgs,
        mut patch_log: PatchLog,
    ) -> Self {
        patch_log.migrate_actors(&doc.ops().actors).unwrap(); // we forked and merged so there will be no mismatch
        Self {
            inner: Some(TransactionInner::new(args)),
            doc,
            patch_log,
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

impl<'a> Transaction<'a> {
    pub(crate) fn empty(
        doc: &'a mut Automerge,
        args: TransactionArgs,
        opts: CommitOptions,
    ) -> ChangeHash {
        TransactionInner::empty(doc, args, opts.message, opts.time)
    }
}

impl Transaction<'_> {
    /// Get the heads of the document before this transaction was started.
    pub fn get_heads(&self) -> Vec<ChangeHash> {
        self.doc.get_heads()
    }

    /// Commit the operations performed in this transaction, returning the hashes corresponding to
    /// the new heads.
    pub fn commit(mut self) -> (Option<ChangeHash>, PatchLog) {
        let tx = self.inner.take().unwrap();
        let hash = tx.commit(self.doc, None, None);
        // TODO - remove this clone
        (hash, self.patch_log.clone())
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
    pub fn commit_with(mut self, options: CommitOptions) -> (Option<ChangeHash>, PatchLog) {
        let tx = self.inner.take().unwrap();
        let hash = tx.commit(self.doc, options.message, options.time);
        // TODO - remove this clone
        (hash, self.patch_log.clone())
    }

    /// Undo the operations added in this transaction, returning the number of cancelled
    /// operations.
    pub fn rollback(mut self) -> usize {
        self.inner.take().unwrap().rollback(self.doc)
    }

    fn do_tx<F, O>(&mut self, f: F) -> O
    where
        F: FnOnce(&mut TransactionInner, &mut Automerge, &mut PatchLog) -> O,
    {
        let tx = self.inner.as_mut().unwrap();
        f(tx, self.doc, &mut self.patch_log)
    }

    fn get_scope(&self, heads: Option<&[ChangeHash]>) -> Option<Clock> {
        if let Some(h) = heads {
            Some(self.doc.clock_at(h))
        } else {
            self.inner.as_ref().and_then(|i| i.get_scope().clone())
        }
    }
}

impl ReadDoc for Transaction<'_> {
    type ViewAt<'a>
        = crate::view_at::AutomergeAt<'a>
    where
        Self: 'a;

    fn view_at(&self, heads: &[ChangeHash]) -> Result<Self::ViewAt<'_>, crate::error::ViewAtError> {
        // Note: view_at sees the document state, not uncommitted transaction changes, which
        // is fine, because view_at requires heads, which means uncommitted transaction changes
        // are not relevant to the view at the specified heads.
        crate::view_at::AutomergeAt::new(self.doc, heads)
    }

    fn keys<O: AsRef<ExId>>(&self, obj: O) -> Keys<'_> {
        self.doc.keys_for(obj.as_ref(), self.get_scope(None))
    }

    fn keys_at<O: AsRef<ExId>>(&self, obj: O, heads: &[ChangeHash]) -> Keys<'_> {
        self.doc.keys_for(obj.as_ref(), self.get_scope(Some(heads)))
    }

    fn iter_at<O: AsRef<ExId>>(&self, obj: O, heads: Option<&[ChangeHash]>) -> DocIter<'_> {
        self.doc.iter_for(obj.as_ref(), self.get_scope(heads))
    }

    fn map_range<'b, O: AsRef<ExId>, R: RangeBounds<String> + 'b>(
        &'b self,
        obj: O,
        range: R,
    ) -> MapRange<'b> {
        self.doc
            .map_range_for(obj.as_ref(), range, self.get_scope(None))
    }

    fn map_range_at<'b, O: AsRef<ExId>, R: RangeBounds<String> + 'b>(
        &'b self,
        obj: O,
        range: R,
        heads: &[ChangeHash],
    ) -> MapRange<'b> {
        self.doc
            .map_range_for(obj.as_ref(), range, self.get_scope(Some(heads)))
    }

    fn list_range<O: AsRef<ExId>, R: RangeBounds<usize>>(&self, obj: O, range: R) -> ListRange<'_> {
        self.doc
            .list_range_for(obj.as_ref(), range, self.get_scope(None))
    }

    fn list_range_at<O: AsRef<ExId>, R: RangeBounds<usize>>(
        &self,
        obj: O,
        range: R,
        heads: &[ChangeHash],
    ) -> ListRange<'_> {
        self.doc
            .list_range_for(obj.as_ref(), range, self.get_scope(Some(heads)))
    }

    fn values<O: AsRef<ExId>>(&self, obj: O) -> Values<'_> {
        self.doc.values_for(obj.as_ref(), self.get_scope(None))
    }

    fn values_at<O: AsRef<ExId>>(&self, obj: O, heads: &[ChangeHash]) -> Values<'_> {
        self.doc
            .values_for(obj.as_ref(), self.get_scope(Some(heads)))
    }

    fn length<O: AsRef<ExId>>(&self, obj: O) -> usize {
        self.doc.length_for(obj.as_ref(), self.get_scope(None))
    }

    fn length_at<O: AsRef<ExId>>(&self, obj: O, heads: &[ChangeHash]) -> usize {
        self.doc
            .length_for(obj.as_ref(), self.get_scope(Some(heads)))
    }

    fn object_type<O: AsRef<ExId>>(&self, obj: O) -> Result<ObjType, AutomergeError> {
        self.doc.object_type(obj)
    }

    fn text<O: AsRef<ExId>>(&self, obj: O) -> Result<String, AutomergeError> {
        self.doc.text_for(obj.as_ref(), self.get_scope(None))
    }

    fn text_at<O: AsRef<ExId>>(
        &self,
        obj: O,
        heads: &[ChangeHash],
    ) -> Result<String, AutomergeError> {
        self.doc.text_for(obj.as_ref(), self.get_scope(Some(heads)))
    }

    fn spans<O: AsRef<ExId>>(&self, obj: O) -> Result<Spans<'_>, AutomergeError> {
        self.doc.spans_for(obj.as_ref(), self.get_scope(None))
    }

    fn spans_at<O: AsRef<ExId>>(
        &self,
        obj: O,
        heads: &[ChangeHash],
    ) -> Result<Spans<'_>, AutomergeError> {
        self.doc
            .spans_for(obj.as_ref(), self.get_scope(Some(heads)))
    }

    fn get_cursor<O: AsRef<ExId>, I: Into<CursorPosition>>(
        &self,
        obj: O,
        position: I,
        at: Option<&[ChangeHash]>,
    ) -> Result<Cursor, AutomergeError> {
        self.doc.get_cursor_for(
            obj.as_ref(),
            position.into(),
            self.get_scope(at),
            MoveCursor::After,
        )
    }

    fn get_cursor_moving<O: AsRef<ExId>, I: Into<CursorPosition>>(
        &self,
        obj: O,
        position: I,
        at: Option<&[ChangeHash]>,
        move_cursor: MoveCursor,
    ) -> Result<Cursor, AutomergeError> {
        self.doc.get_cursor_for(
            obj.as_ref(),
            position.into(),
            self.get_scope(at),
            move_cursor,
        )
    }

    fn get_cursor_position<O: AsRef<ExId>>(
        &self,
        obj: O,
        address: &Cursor,
        at: Option<&[ChangeHash]>,
    ) -> Result<usize, AutomergeError> {
        self.doc
            .get_cursor_position_for(obj.as_ref(), address, self.get_scope(at))
    }

    fn marks<O: AsRef<ExId>>(&self, obj: O) -> Result<Vec<Mark>, AutomergeError> {
        self.doc.marks_for(obj.as_ref(), self.get_scope(None))
    }

    fn marks_at<O: AsRef<ExId>>(
        &self,
        obj: O,
        heads: &[ChangeHash],
    ) -> Result<Vec<Mark>, AutomergeError> {
        self.doc
            .marks_for(obj.as_ref(), self.get_scope(Some(heads)))
    }

    fn hydrate<O: AsRef<ExId>>(
        &self,
        obj: O,
        heads: Option<&[ChangeHash]>,
    ) -> Result<hydrate::Value, AutomergeError> {
        self.doc.hydrate_obj(obj.as_ref(), heads)
    }

    fn get_marks<O: AsRef<ExId>>(
        &self,
        obj: O,
        index: usize,
        heads: Option<&[ChangeHash]>,
    ) -> Result<MarkSet, AutomergeError> {
        self.doc
            .get_marks_for(obj.as_ref(), index, self.get_scope(heads))
    }

    fn get<O: AsRef<ExId>, P: Into<Prop>>(
        &self,
        obj: O,
        prop: P,
    ) -> Result<Option<(Value<'_>, ExId)>, AutomergeError> {
        self.doc
            .get_for(obj.as_ref(), prop.into(), self.get_scope(None))
    }

    fn get_at<O: AsRef<ExId>, P: Into<Prop>>(
        &self,
        obj: O,
        prop: P,
        heads: &[ChangeHash],
    ) -> Result<Option<(Value<'_>, ExId)>, AutomergeError> {
        self.doc
            .get_for(obj.as_ref(), prop.into(), self.get_scope(Some(heads)))
    }

    fn get_all<O: AsRef<ExId>, P: Into<Prop>>(
        &self,
        obj: O,
        prop: P,
    ) -> Result<Vec<(Value<'_>, ExId)>, AutomergeError> {
        self.doc
            .get_all_for(obj.as_ref(), prop.into(), self.get_scope(None))
    }

    fn get_all_at<O: AsRef<ExId>, P: Into<Prop>>(
        &self,
        obj: O,
        prop: P,
        heads: &[ChangeHash],
    ) -> Result<Vec<(Value<'_>, ExId)>, AutomergeError> {
        self.doc
            .get_all_for(obj.as_ref(), prop.into(), self.get_scope(Some(heads)))
    }

    fn parents<O: AsRef<ExId>>(&self, obj: O) -> Result<Parents<'_>, AutomergeError> {
        self.doc.parents_for(obj.as_ref(), self.get_scope(None))
    }

    fn parents_at<O: AsRef<ExId>>(
        &self,
        obj: O,
        heads: &[ChangeHash],
    ) -> Result<Parents<'_>, AutomergeError> {
        self.doc
            .parents_for(obj.as_ref(), self.get_scope(Some(heads)))
    }

    fn get_missing_deps(&self, heads: &[ChangeHash]) -> Vec<ChangeHash> {
        self.doc.get_missing_deps(heads)
    }

    fn get_change_by_hash(&self, hash: &ChangeHash) -> Option<crate::Change> {
        self.doc.get_change_by_hash(hash)
    }

    fn stats(&self) -> crate::read::Stats {
        self.doc.stats()
    }

    fn text_encoding(&self) -> crate::TextEncoding {
        self.doc.text_encoding()
    }
}

impl Transactable for Transaction<'_> {
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
        self.do_tx(|tx, doc, hist| tx.put(doc, hist, obj.as_ref(), prop, value))
    }

    fn put_object<O: AsRef<ExId>, P: Into<Prop>>(
        &mut self,
        obj: O,
        prop: P,
        value: ObjType,
    ) -> Result<ExId, AutomergeError> {
        self.do_tx(|tx, doc, hist| tx.put_object(doc, hist, obj.as_ref(), prop, value))
    }

    fn insert<O: AsRef<ExId>, V: Into<ScalarValue>>(
        &mut self,
        obj: O,
        index: usize,
        value: V,
    ) -> Result<(), AutomergeError> {
        self.do_tx(|tx, doc, hist| tx.insert(doc, hist, obj.as_ref(), index, value))
    }

    fn insert_object<O: AsRef<ExId>>(
        &mut self,
        obj: O,
        index: usize,
        value: ObjType,
    ) -> Result<ExId, AutomergeError> {
        self.do_tx(|tx, doc, hist| tx.insert_object(doc, hist, obj.as_ref(), index, value))
    }

    fn increment<O: AsRef<ExId>, P: Into<Prop>>(
        &mut self,
        obj: O,
        prop: P,
        value: i64,
    ) -> Result<(), AutomergeError> {
        self.do_tx(|tx, doc, hist| tx.increment(doc, hist, obj.as_ref(), prop, value))
    }

    fn delete<O: AsRef<ExId>, P: Into<Prop>>(
        &mut self,
        obj: O,
        prop: P,
    ) -> Result<(), AutomergeError> {
        self.do_tx(|tx, doc, hist| tx.delete(doc, hist, obj.as_ref(), prop))
    }

    /// Splice new elements into the given sequence. Returns a vector of the OpIds used to insert
    /// the new elements
    fn splice<O: AsRef<ExId>, V: IntoIterator<Item = ScalarValue>>(
        &mut self,
        obj: O,
        pos: usize,
        del: isize,
        vals: V,
    ) -> Result<(), AutomergeError> {
        self.do_tx(|tx, doc, hist| tx.splice(doc, hist, obj.as_ref(), pos, del, vals))?;
        Ok(())
    }

    fn splice_text<O: AsRef<ExId>>(
        &mut self,
        obj: O,
        pos: usize,
        del: isize,
        text: &str,
    ) -> Result<(), AutomergeError> {
        self.do_tx(|tx, doc, hist| tx.splice_text(doc, hist, obj.as_ref(), pos, del, text))?;
        Ok(())
    }

    fn mark<O: AsRef<ExId>>(
        &mut self,
        obj: O,
        mark: Mark,
        expand: ExpandMark,
    ) -> Result<(), AutomergeError> {
        self.do_tx(|tx, doc, hist| tx.mark(doc, hist, obj.as_ref(), mark, expand))
    }

    fn unmark<O: AsRef<ExId>>(
        &mut self,
        obj: O,
        name: &str,
        start: usize,
        end: usize,
        expand: ExpandMark,
    ) -> Result<(), AutomergeError> {
        self.do_tx(|tx, doc, hist| tx.unmark(doc, hist, obj.as_ref(), name, start, end, expand))
    }

    fn split_block<'p, O>(&mut self, obj: O, index: usize) -> Result<ExId, AutomergeError>
    where
        O: AsRef<ExId>,
    {
        self.do_tx(|tx, doc, hist| tx.split_block(doc, hist, obj.as_ref(), index))
    }

    fn join_block<O>(&mut self, text: O, index: usize) -> Result<(), AutomergeError>
    where
        O: AsRef<ExId>,
    {
        self.do_tx(|tx, doc, hist| tx.join_block(doc, hist, text.as_ref(), index))
    }

    fn replace_block<'p, O>(&mut self, text: O, index: usize) -> Result<ExId, AutomergeError>
    where
        O: AsRef<ExId>,
    {
        self.do_tx(|tx, doc, hist| tx.replace_block(doc, hist, text.as_ref(), index))
    }

    fn base_heads(&self) -> Vec<ChangeHash> {
        self.inner
            .as_ref()
            .map(|d| d.get_deps())
            .unwrap_or_default()
    }

    fn update_text<S: AsRef<str>>(
        &mut self,
        obj: &ExId,
        new_text: S,
    ) -> Result<(), AutomergeError> {
        self.do_tx(|tx, doc, hist| crate::text_diff::myers_diff(doc, tx, hist, obj, new_text))
    }

    fn update_spans<O: AsRef<ExId>, I: IntoIterator<Item = Span>>(
        &mut self,
        text: O,
        config: UpdateSpansConfig,
        new_text: I,
    ) -> Result<(), AutomergeError> {
        self.do_tx(move |tx, doc, hist| {
            crate::text_diff::myers_block_diff(doc, tx, hist, text.as_ref(), new_text, &config)
        })
    }

    fn update_object<O: AsRef<ExId>>(
        &mut self,
        obj: O,
        new_value: &crate::hydrate::Value,
    ) -> Result<(), crate::error::UpdateObjectError> {
        self.do_tx(move |tx, doc, hist| tx.update_object(doc, hist, obj.as_ref(), new_value))
    }
}

impl Drop for Transaction<'_> {
    /// If a transaction is not commited or rolled back manually then it can leave the document in
    /// an intermediate state.
    /// This defaults to rolling back the transaction to be compatible with [`?`][std::ops::Try]
    /// error returning before reaching a call to [`Self::commit()`].
    fn drop(&mut self) {
        if let Some(txn) = self.inner.take() {
            txn.rollback(self.doc);
        }
    }
}
