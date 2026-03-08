use crate::automerge::Automerge;
use crate::exid::ExId;
use crate::patches::PatchLog;
use crate::ChangeHash;

use super::{CommitOptions, TransactionInner};

/// A transaction that **owns** the `Automerge` document.
///
/// Like [`super::Transaction`], this groups operations into a single change. The difference is
/// ownership: `Transaction<'a>` borrows `&'a mut Automerge`, while `OwnedTransaction` consumes
/// it, making the type `'static` and `Send`. This is useful when lifetimes cannot be tracked
/// across an API boundary (e.g. FFI, async runtimes, or storing a transaction in a struct that
/// must be `'static`).
///
/// Created via [`Automerge::into_transaction`](crate::Automerge::into_transaction).
#[derive(Debug)]
pub struct OwnedTransaction {
    // This is always `Some` — it's `Option` only because the shared `impl_transactable_for_tx!`
    // macro (also used by `Transaction<'a>`, which needs `Option` for its `Drop` impl) accesses
    // `self.inner` directly and expects it to be an Option<TransactionInner>
    inner: Option<TransactionInner>,
    patch_log: PatchLog,
    doc: Automerge,
}

// Compile-time assertion that OwnedTransaction is Send.
const _: () = {
    fn _assert_send<T: Send>() {}
    fn _assert_owned_tx() {
        _assert_send::<OwnedTransaction>()
    }
};

impl OwnedTransaction {
    /// Create a new transaction, consuming the document.
    pub(crate) fn new(
        mut doc: Automerge,
        patch_log: Option<PatchLog>,
        heads: Option<&[ChangeHash]>,
    ) -> Self {
        let args = doc.transaction_args(heads);
        let mut patch_log = patch_log.unwrap_or_else(PatchLog::inactive);
        patch_log.migrate_actors(&doc.ops().actors).unwrap();
        Self {
            inner: Some(TransactionInner::new(args)),
            patch_log,
            doc,
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

    /// Get the heads of the document before this transaction was started.
    pub fn get_heads(&self) -> Vec<ChangeHash> {
        self.doc.get_heads()
    }

    /// Commit the transaction, returning the document, commit hash, and patch log.
    ///
    /// Unlike [`super::Transaction::commit`], no `PatchLog` clone is needed — it is moved out.
    pub fn commit(mut self) -> (Automerge, Option<ChangeHash>, PatchLog) {
        let tx = self.inner.take().unwrap();
        let hash = tx.commit(&mut self.doc, None, None);
        (self.doc, hash, self.patch_log)
    }

    /// Commit with options.
    pub fn commit_with(
        mut self,
        options: CommitOptions,
    ) -> (Automerge, Option<ChangeHash>, PatchLog) {
        let tx = self.inner.take().unwrap();
        let hash = tx.commit(&mut self.doc, options.message, options.time);
        (self.doc, hash, self.patch_log)
    }

    /// Rollback the transaction, returning the document and number of cancelled ops.
    pub fn rollback(mut self) -> (Automerge, usize) {
        let cancelled = self.inner.take().unwrap().rollback(&mut self.doc);
        (self.doc, cancelled)
    }

    fn do_tx<F, O>(&mut self, f: F) -> O
    where
        F: FnOnce(&mut TransactionInner, &mut Automerge, &mut PatchLog) -> O,
    {
        let tx = self.inner.as_mut().unwrap();
        f(tx, &mut self.doc, &mut self.patch_log)
    }

    fn get_scope(&self, heads: Option<&[ChangeHash]>) -> Option<crate::types::Clock> {
        if let Some(h) = heads {
            Some(self.doc.clock_at(h))
        } else {
            self.inner.as_ref().and_then(|i| i.get_scope().clone())
        }
    }
}

super::impl_read_doc_for_tx!(OwnedTransaction);
super::impl_transactable_for_tx!(OwnedTransaction);

#[cfg(test)]
mod tests {
    use crate::transaction::{CommitOptions, Transactable};
    use crate::{Automerge, ObjType, PatchLog, ReadDoc, ROOT};

    #[test]
    fn put_and_get_roundtrip() {
        let doc = Automerge::new();
        let mut tx = doc.into_transaction(None, None);
        tx.put(ROOT, "key", "value").unwrap();
        let (doc, hash, _) = tx.commit();
        assert!(hash.is_some());
        assert_eq!(
            doc.get(ROOT, "key").unwrap().unwrap().0.to_str().unwrap(),
            "value"
        );
    }

    #[test]
    fn read_during_transaction() {
        let doc = Automerge::new();
        let mut tx = doc.into_transaction(None, None);
        tx.put(ROOT, "a", "1").unwrap();
        // ReadDoc works on the transaction itself
        let (val, _) = tx.get(ROOT, "a").unwrap().unwrap();
        assert_eq!(val.to_str().unwrap(), "1");
        tx.commit();
    }

    #[test]
    fn nested_objects() {
        let doc = Automerge::new();
        let mut tx = doc.into_transaction(None, None);
        let list = tx.put_object(ROOT, "items", ObjType::List).unwrap();
        tx.insert(&list, 0, "first").unwrap();
        tx.insert(&list, 1, "second").unwrap();
        let (doc, hash, _) = tx.commit();
        assert!(hash.is_some());
        assert_eq!(doc.length(list), 2);
    }

    #[test]
    fn commit_with_options() {
        let doc = Automerge::new();
        let mut tx = doc.into_transaction(None, None);
        tx.put(ROOT, "x", 42).unwrap();
        let (doc, hash, _) = tx.commit_with(CommitOptions::default().with_message("test commit"));
        assert!(hash.is_some());
        let change = doc.get_change_by_hash(&hash.unwrap()).unwrap();
        assert_eq!(change.message().map(|s| s.as_str()), Some("test commit"));
    }

    #[test]
    fn rollback_discards_ops() {
        let mut doc = Automerge::new();
        {
            let mut tx = doc.transaction();
            tx.put(ROOT, "keep", "yes").unwrap();
            tx.commit();
        }
        let doc = doc.into_transaction(None, None);
        // Haven't written anything, just rollback
        let (doc, cancelled) = doc.rollback();
        assert_eq!(cancelled, 0);
        assert_eq!(
            doc.get(ROOT, "keep").unwrap().unwrap().0.to_str().unwrap(),
            "yes"
        );
    }

    #[test]
    fn rollback_undoes_writes() {
        let doc = Automerge::new();
        let mut tx = doc.into_transaction(None, None);
        tx.put(ROOT, "gone", "soon").unwrap();
        let (doc, cancelled) = tx.rollback();
        assert_eq!(cancelled, 1);
        assert!(doc.get(ROOT, "gone").unwrap().is_none());
    }

    #[test]
    fn owned_transaction_at() {
        let mut doc = Automerge::new();

        // Make a first change
        let mut tx = doc.transaction();
        tx.put(ROOT, "v", 1).unwrap();
        tx.commit();
        let heads_v1 = doc.get_heads();

        // Make a second change
        let mut tx = doc.transaction();
        tx.put(ROOT, "v", 2).unwrap();
        tx.commit();

        // Start an owned transaction isolated at v1 heads
        let mut tx = doc.into_transaction(None, Some(&heads_v1));
        // Should see v=1, not v=2
        let (val, _) = tx.get(ROOT, "v").unwrap().unwrap();
        assert_eq!(val.to_i64().unwrap(), 1);

        tx.put(ROOT, "from_v1", true).unwrap();
        let (doc, hash, _) = tx.commit();
        assert!(hash.is_some());
        assert!(doc.get(ROOT, "from_v1").unwrap().is_some());
    }

    #[test]
    fn log_patches() {
        let doc = Automerge::new();
        let mut tx = doc.into_transaction(Some(PatchLog::active()), None);
        tx.put(ROOT, "patched", "yes").unwrap();
        let (doc, _, mut patch_log) = tx.commit();
        let patches = doc.make_patches(&mut patch_log);
        // We should have at least one patch from the put
        assert!(!patches.is_empty());
    }

    #[test]
    fn get_heads_returns_pre_tx_heads() {
        let mut doc = Automerge::new();
        let mut tx = doc.transaction();
        tx.put(ROOT, "a", 1).unwrap();
        tx.commit();
        let heads = doc.get_heads();

        let tx = doc.into_transaction(None, None);
        assert_eq!(tx.get_heads(), heads);
        tx.commit();
    }

    #[test]
    fn pending_ops() {
        let doc = Automerge::new();
        let mut tx = doc.into_transaction(None, None);
        assert_eq!(tx.pending_ops(), 0);
        tx.put(ROOT, "a", 1).unwrap();
        assert_eq!(tx.pending_ops(), 1);
        tx.put(ROOT, "b", 2).unwrap();
        assert_eq!(tx.pending_ops(), 2);
        tx.commit();
    }

    #[test]
    fn empty_commit_returns_none_hash() {
        let doc = Automerge::new();
        let tx = doc.into_transaction(None, None);
        let (_, hash, _) = tx.commit();
        assert!(hash.is_none());
    }
}
