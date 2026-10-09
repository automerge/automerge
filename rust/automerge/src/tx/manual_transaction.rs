use super::TxDoc;
use crate::exid::ExId;
use crate::ChangeHash;
use crate::{automerge::Automerge, AutomergeError};

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
pub struct Transaction<
    'a,
    H: crate::hash_retention::HashRetention = crate::hash_retention::Retained,
> {
    // this is an option so that we can take it during commit and rollback to prevent it being
    // rolled back during drop.
    inner: Option<TransactionInner>,
    doc: &'a mut Automerge<H>,
}

impl<'a, H: crate::hash_retention::HashRetention> Transaction<'a, H> {
    pub(crate) fn new(doc: &'a mut Automerge<H>, args: TransactionArgs) -> Self {
        Self {
            inner: Some(TransactionInner::new(args)),
            doc,
        }
    }

    /// Get the hash of the change that contains the given opid.
    ///
    /// Returns none if the opid:
    /// - is the root object id
    /// - does not exist in this document
    /// - is for an operation in this transaction
    pub fn hash_for_opid(&self, opid: &ExId) -> Result<Option<ChangeHash>, AutomergeError> {
        self.doc.hash_for_opid(opid)
    }
}

impl<'a, H: crate::hash_retention::HashRetention> Transaction<'a, H> {
    pub(crate) fn empty(
        doc: &'a mut Automerge<H>,
        args: TransactionArgs,
        opts: CommitOptions,
    ) -> ChangeHash {
        TransactionInner::empty(doc, args, opts.message, opts.time)
    }
}

impl<H: crate::hash_retention::HashRetention> Transaction<'_, H> {
    /// Get the heads of the document before this transaction was started.
    pub fn get_heads(&self) -> Vec<crate::ChangeId> {
        self.doc.get_heads()
    }

    /// Commit the operations performed in this transaction, returning the
    /// [`crate::ChangeId`] of the change it created (if any).
    pub fn commit(mut self) -> Option<crate::ChangeId> {
        let hash = self.inner.take().unwrap().commit(self.doc, None, None)?;
        self.doc
            .hash_to_change_id(&hash)
            .expect("hash of a newly committed change is always known")
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
    pub fn commit_with(mut self, options: CommitOptions) -> Option<crate::ChangeId> {
        let hash = self
            .inner
            .take()
            .unwrap()
            .commit(self.doc, options.message, options.time)?;
        self.doc
            .hash_to_change_id(&hash)
            .expect("hash of a newly committed change is always known")
    }

    pub(crate) fn commit_hash(mut self, options: CommitOptions) -> Option<ChangeHash> {
        self.inner
            .take()
            .unwrap()
            .commit(self.doc, options.message, options.time)
    }

    pub(crate) fn doc_ref(&self) -> &Automerge<H> {
        self.doc
    }

    /// Undo the operations added in this transaction, returning the number of cancelled
    /// operations.
    pub fn rollback(mut self) -> usize {
        self.inner.take().unwrap().rollback(self.doc)
    }

    pub(crate) fn batch_init_root_map(
        &mut self,
        value: &crate::hydrate::Map,
    ) -> Result<(), AutomergeError> {
        self.do_tx(move |tx, doc| tx.batch_init_root_map(doc, value))
    }
}

impl<H: crate::hash_retention::HashRetention> super::TxDoc for Transaction<'_, H> {
    type Retention = H;

    fn doc(&self) -> &Automerge<H> {
        self.doc
    }

    fn inner(&self) -> Option<&TransactionInner> {
        self.inner.as_ref()
    }

    fn parts_mut(&mut self) -> (&mut TransactionInner, &mut Automerge<H>) {
        (self.inner.as_mut().expect("an open transaction"), self.doc)
    }
}

impl<H: crate::hash_retention::HashRetention> Drop for Transaction<'_, H> {
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
