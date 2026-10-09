use crate::exid::ExId;
use crate::hash_retention::Full;
use crate::tx::{self, CommitOptions};
use crate::ChangeHash;

use super::Automerge;

pub use crate::tx::{BlockOrText, Failure};

/// The result of a transaction run with [`Automerge::transact()`].
pub type Result<O, E> = std::result::Result<Success<O>, Failure<E>>;

/// The result of a successful, and committed, transaction.
#[derive(Debug)]
pub struct Success<O> {
    /// The result of the transaction.
    pub result: O,
    /// The hash of the change, will be `None` if the transaction did not create any operations
    pub hash: Option<ChangeHash>,
}

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
pub struct Transaction<'a>(pub(crate) tx::Transaction<'a, Full>);

impl Transaction<'_> {
    /// Get the hash of the change that contains the given opid.
    ///
    /// Returns none if the opid:
    /// - is the root object id
    /// - does not exist in this document
    /// - is for an operation in this transaction
    pub fn hash_for_opid(&self, opid: &ExId) -> Option<ChangeHash> {
        self.0.hash_for_opid(opid).ok().flatten()
    }

    /// Get the heads of the document before this transaction was started.
    pub fn get_heads(&self) -> Vec<ChangeHash> {
        self.0.doc_ref().get_head_hashes()
    }

    /// Commit the operations performed in this transaction, returning the hash of the change
    /// created (if any).
    pub fn commit(self) -> Option<ChangeHash> {
        self.0.commit_hash(CommitOptions::default())
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
    pub fn commit_with(self, options: CommitOptions) -> Option<ChangeHash> {
        self.0.commit_hash(options)
    }

    /// Undo the operations added in this transaction, returning the number of cancelled
    /// operations.
    pub fn rollback(self) -> usize {
        self.0.rollback()
    }
}

impl<'a> super::AuditWrapper for Transaction<'a> {
    type Next = tx::Transaction<'a, Full>;

    fn next(&self) -> &Self::Next {
        &self.0
    }

    fn next_mut(&mut self) -> &mut Self::Next {
        &mut self.0
    }

    fn doc(&self) -> &crate::automerge::Automerge<Full> {
        self.0.doc_ref()
    }
}

/// A transaction which owns the document it is editing.
///
/// Created from [`Automerge::into_transaction()`]. The document is returned when the
/// transaction is committed or rolled back.
#[derive(Debug)]
pub struct OwnedTransaction(pub(crate) tx::OwnedTransaction<Full>);

impl OwnedTransaction {
    /// Get the hash of the change that contains the given opid.
    ///
    /// Returns none if the opid:
    /// - is the root object id
    /// - does not exist in this document
    /// - is for an operation in this transaction
    pub fn hash_for_opid(&self, opid: &ExId) -> Option<ChangeHash> {
        self.0.hash_for_opid(opid).ok().flatten()
    }

    /// Get the heads of the document before this transaction was started.
    pub fn get_heads(&self) -> Vec<ChangeHash> {
        self.0.doc_ref().get_head_hashes()
    }

    /// Commit the transaction, returning the document and the hash of the change it created
    /// (if any).
    pub fn commit(self) -> (Automerge, Option<ChangeHash>) {
        let (doc, hash) = self.0.commit_hash(CommitOptions::default());
        (Automerge(doc), hash)
    }

    /// Commit with options.
    pub fn commit_with(self, options: CommitOptions) -> (Automerge, Option<ChangeHash>) {
        let (doc, hash) = self.0.commit_hash(options);
        (Automerge(doc), hash)
    }

    /// Rollback the transaction, returning the document and number of cancelled ops.
    pub fn rollback(self) -> (Automerge, usize) {
        let (doc, cancelled) = self.0.rollback();
        (Automerge(doc), cancelled)
    }
}

impl super::AuditWrapper for OwnedTransaction {
    type Next = tx::OwnedTransaction<Full>;

    fn next(&self) -> &Self::Next {
        &self.0
    }

    fn next_mut(&mut self) -> &mut Self::Next {
        &mut self.0
    }

    fn doc(&self) -> &crate::automerge::Automerge<Full> {
        self.0.doc_ref()
    }
}
