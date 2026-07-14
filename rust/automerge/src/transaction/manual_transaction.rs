use crate::exid::ExId;
use crate::patches::PatchLog;
use crate::{automerge::Automerge, AutomergeError};
use crate::{ChangeHash, ChangeId};

use super::{CommitOptions, TransactionArgs, TransactionInner};

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
    pub(crate) fn new(doc: &'a mut Automerge, args: TransactionArgs, patch_log: PatchLog) -> Self {
        Self {
            inner: Some(TransactionInner::new(args)),
            doc,
            patch_log,
        }
    }

    /// Get the [`ChangeId`] of the change that contains the given opid.
    ///
    /// Returns none if the opid:
    /// - is the root object id
    /// - does not exist in this document
    /// - is for an operation in this transaction
    pub fn change_id_for_opid(&self, opid: &ExId) -> Option<ChangeId> {
        self.doc.change_id_for_opid(opid)
    }

    /// See [`Automerge::get_hash_for_change_id`]
    pub fn get_hash_for_change_id(
        &self,
        id: &ChangeId,
    ) -> Result<Option<ChangeHash>, AutomergeError> {
        self.doc.get_hash_for_change_id(id)
    }

    /// See [`Automerge::get_change_id_for_hash`]
    pub fn get_change_id_for_hash(
        &self,
        hash: &ChangeHash,
    ) -> Result<Option<ChangeId>, AutomergeError> {
        self.doc.get_change_id_for_hash(hash)
    }

    /// See [`Automerge::get_change_ids_for_hashes`]
    pub fn get_change_ids_for_hashes(
        &self,
        hashes: &[ChangeHash],
    ) -> Result<Vec<ChangeId>, AutomergeError> {
        self.doc.get_change_ids_for_hashes(hashes)
    }

    /// See [`Automerge::get_hashes_for_change_ids`]
    pub fn get_hashes_for_change_ids(
        &self,
        ids: &[ChangeId],
    ) -> Result<Vec<ChangeHash>, AutomergeError> {
        self.doc.get_hashes_for_change_ids(ids)
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
    pub fn get_heads(&self) -> Vec<ChangeId> {
        self.doc.get_heads()
    }

    /// Commit the operations performed in this transaction, returning the id of
    /// the new change.
    pub fn commit(mut self) -> (Option<ChangeId>, PatchLog) {
        let tx = self.inner.take().unwrap();
        let hash =
            super::commit_transaction(tx, self.doc, &mut self.patch_log, CommitOptions::default());
        let id = hash.map(|h| {
            self.doc
                .get_change_id_for_hash(&h)
                .expect("hash of a newly committed change is always known")
                .expect("newly committed change must be in the document")
        });
        // TODO - remove this clone
        (id, self.patch_log.clone())
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
    pub fn commit_with(mut self, options: CommitOptions) -> (Option<ChangeId>, PatchLog) {
        let tx = self.inner.take().unwrap();
        let hash = super::commit_transaction(tx, self.doc, &mut self.patch_log, options);
        let id = hash.map(|h| {
            self.doc
                .get_change_id_for_hash(&h)
                .expect("hash of a newly committed change is always known")
                .expect("newly committed change must be in the document")
        });
        // TODO - remove this clone
        (id, self.patch_log.clone())
    }

    /// Undo the operations added in this transaction, returning the number of cancelled
    /// operations.
    pub fn rollback(mut self) -> usize {
        self.patch_log.finish_transaction(&self.doc.ops().actors);
        self.inner.take().unwrap().rollback(self.doc)
    }

    fn do_tx<F, O>(&mut self, f: F) -> O
    where
        F: FnOnce(&mut TransactionInner, &mut Automerge, &mut PatchLog) -> O,
    {
        let tx = self.inner.as_mut().unwrap();
        f(tx, self.doc, &mut self.patch_log)
    }

    fn get_scope(
        &self,
        heads: Option<&[ChangeId]>,
    ) -> Result<Option<crate::types::Clock>, AutomergeError> {
        if let Some(h) = heads {
            // a transaction is in flight: its pending ops are in the op
            // set but not under the graph's heads, so the current-heads
            // shortcut in `Automerge::clock_for_ids` must not be used here
            let nodes = self.doc.nodes_for_change_ids(h)?;
            Ok(Some(self.doc.change_graph.clock_for_nodes(nodes)))
        } else {
            Ok(self.inner.as_ref().and_then(|i| i.get_scope().clone()))
        }
    }

    pub(crate) fn batch_init_root_map(
        &mut self,
        value: &crate::hydrate::Map,
    ) -> Result<(), AutomergeError> {
        self.do_tx(move |tx, doc, hist| tx.batch_init_root_map(doc, hist, value))
    }
}

super::impl_read_doc_for_tx!(Transaction<'_>);
super::impl_transactable_for_tx!(Transaction<'_>);

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
