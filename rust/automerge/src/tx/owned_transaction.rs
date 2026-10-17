use crate::automerge::Automerge;
use crate::exid::ExId;
use crate::{AutomergeError, ChangeHash};

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
pub struct OwnedTransaction<
    H: crate::hash_retention::HashRetention = crate::hash_retention::Retained,
> {
    inner: TransactionInner,
    doc: Automerge<H>,
}

// Compile-time assertion that OwnedTransaction is Send.
const _: () = {
    fn _assert_send<T: Send>() {}
    fn _assert_owned_tx() {
        _assert_send::<OwnedTransaction>()
    }
};

impl<H: crate::hash_retention::HashRetention> OwnedTransaction<H> {
    /// Create a new transaction, consuming the document.
    pub(crate) fn new(
        mut doc: Automerge<H>,
        heads: Option<&[crate::ChangeId]>,
    ) -> Result<Self, AutomergeError> {
        if let Some(h) = heads {
            // fail fast: committing records these heads' hashes as deps
            doc.resolve_heads(h)?;
        }
        let args = doc.transaction_args(heads);
        Ok(Self {
            inner: TransactionInner::new(args),
            doc,
        })
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

    /// Get the heads of the document before this transaction was started.
    pub fn get_heads(&self) -> Vec<crate::ChangeId> {
        self.doc.get_heads()
    }

    /// Commit the transaction, returning the document and the id of the
    /// change it created (if any).
    pub fn commit(self) -> (Automerge<H>, Option<crate::ChangeId>) {
        self.commit_with(CommitOptions::default())
    }

    /// Commit with options.
    pub fn commit_with(self, options: CommitOptions) -> (Automerge<H>, Option<crate::ChangeId>) {
        let (doc, hash) = self.commit_hash(options);
        let id = hash.map(|h| {
            doc.hash_to_change_id(&h)
                .expect("hash of a newly committed change is always known")
                .expect("newly committed change must be in the document")
        });
        (doc, id)
    }

    pub(crate) fn commit_hash(self, options: CommitOptions) -> (Automerge<H>, Option<ChangeHash>) {
        let Self { inner, mut doc } = self;
        let hash = inner.commit(&mut doc, options.message, options.time);
        (doc, hash)
    }

    pub(crate) fn doc_ref(&self) -> &Automerge<H> {
        &self.doc
    }

    /// Rollback the transaction, returning the document and number of cancelled ops.
    pub fn rollback(self) -> (Automerge<H>, usize) {
        let Self { inner, mut doc } = self;
        let cancelled = inner.rollback(&mut doc);
        (doc, cancelled)
    }
}

impl<H: crate::hash_retention::HashRetention> super::TxDoc for OwnedTransaction<H> {
    type Retention = H;

    fn doc(&self) -> &Automerge<H> {
        &self.doc
    }

    fn inner(&self) -> Option<&TransactionInner> {
        Some(&self.inner)
    }

    fn parts_mut(&mut self) -> (&mut TransactionInner, &mut Automerge<H>) {
        (&mut self.inner, &mut self.doc)
    }
}

#[cfg(test)]
mod tests {
    use crate::automerge::Automerge;
    use crate::read::ReadDoc;
    use crate::tx::{CommitOptions, Transactable};
    use crate::{ObjType, ROOT};

    #[test]
    fn put_and_get_roundtrip() {
        let doc = Automerge::new();
        let mut tx = doc.into_transaction(None).unwrap();
        tx.put(ROOT, "key", "value").unwrap();
        let (doc, hash) = tx.commit();
        assert!(hash.is_some());
        assert_eq!(
            doc.get(ROOT, "key").unwrap().unwrap().0.as_str().unwrap(),
            "value"
        );
    }

    #[test]
    fn read_during_transaction() {
        let doc = Automerge::new();
        let mut tx = doc.into_transaction(None).unwrap();
        tx.put(ROOT, "a", "1").unwrap();
        // ReadDoc works on the transaction itself
        let (val, _) = tx.get(ROOT, "a").unwrap().unwrap();
        assert_eq!(val.as_str().unwrap(), "1");
        tx.commit();
    }

    #[test]
    fn nested_objects() {
        let doc = Automerge::new();
        let mut tx = doc.into_transaction(None).unwrap();
        let list = tx.put_object(ROOT, "items", ObjType::List).unwrap();
        tx.insert(&list, 0, "first").unwrap();
        tx.insert(&list, 1, "second").unwrap();
        let (doc, hash) = tx.commit();
        assert!(hash.is_some());
        assert_eq!(doc.length(list), 2);
    }

    #[test]
    fn commit_with_options() {
        let doc = Automerge::new();
        let mut tx = doc.into_transaction(None).unwrap();
        tx.put(ROOT, "x", 42).unwrap();
        let (doc, id) = tx.commit_with(CommitOptions::default().with_message("test commit"));
        assert!(id.is_some());
        let hash = doc
            .change_id_to_hash(&id.unwrap())
            .unwrap()
            .expect("committed change resolves");
        let change = doc.get_change_by_hash(&hash).unwrap();
        assert_eq!(change.unwrap().message(), Some("test commit"));
    }

    #[test]
    fn rollback_discards_ops() {
        let mut doc = Automerge::new();
        {
            let mut tx = doc.transaction();
            tx.put(ROOT, "keep", "yes").unwrap();
            tx.commit();
        }
        let doc = doc.into_transaction(None).unwrap();
        // Haven't written anything, just rollback
        let (doc, cancelled) = doc.rollback();
        assert_eq!(cancelled, 0);
        assert_eq!(
            doc.get(ROOT, "keep").unwrap().unwrap().0.as_str().unwrap(),
            "yes"
        );
    }

    #[test]
    fn rollback_undoes_writes() {
        let doc = Automerge::new();
        let mut tx = doc.into_transaction(None).unwrap();
        tx.put(ROOT, "gone", "soon").unwrap();
        let (doc, cancelled) = tx.rollback();
        assert_eq!(cancelled, 1);
        assert!(doc.get(ROOT, "gone").unwrap().is_none());
    }

    #[test]
    fn rollback_restores_clean_dirty_state() {
        let mut doc = Automerge::new();
        doc.ops_mut().clear_dirty();

        let mut tx = doc.transaction();
        tx.put(ROOT, "gone", "soon").unwrap();
        assert_eq!(tx.rollback(), 1);

        assert_eq!(
            doc.ops().dirty_positions().collect::<Vec<_>>(),
            Vec::<usize>::new()
        );
    }

    #[test]
    fn rollback_preserves_preexisting_dirty_state() {
        let mut doc = Automerge::new();
        doc.ops_mut().clear_dirty();

        let mut tx = doc.transaction();
        tx.put(ROOT, "keep", "yes").unwrap();
        tx.commit();
        let before = doc.ops().dirty_positions().collect::<Vec<_>>();
        assert!(!before.is_empty());

        let mut tx = doc.transaction();
        tx.put(ROOT, "gone", "soon").unwrap();
        assert_eq!(tx.rollback(), 1);

        assert_eq!(doc.ops().dirty_positions().collect::<Vec<_>>(), before);
    }

    #[test]
    fn owned_transaction_at() {
        let mut doc = Automerge::new();
        // Deterministic hashes: a fragment-head second commit would free
        // `heads_v1`'s hash and make isolating at it fail.
        doc.set_actor(crate::ActorId::from(&b"otx"[..]));
        let t0 = || crate::tx::CommitOptions::default().with_time(0);

        // Make a first change
        let mut tx = doc.transaction();
        tx.put(ROOT, "v", 1).unwrap();
        tx.commit_with(t0());
        let heads_v1 = doc.get_heads();

        // Make a second change
        let mut tx = doc.transaction();
        tx.put(ROOT, "v", 2).unwrap();
        tx.commit_with(t0());
        assert_eq!(
            doc.get_head_hashes()[0].fragment_level(),
            0,
            "the pinned actor must keep the second commit loose"
        );

        // Start an owned transaction isolated at v1 heads
        let mut tx = doc.into_transaction(Some(&heads_v1)).unwrap();
        // Should see v=1, not v=2
        let (val, _) = tx.get(ROOT, "v").unwrap().unwrap();
        assert_eq!(val.as_i64().unwrap(), 1);

        tx.put(ROOT, "from_v1", true).unwrap();
        let (doc, hash) = tx.commit();
        assert!(hash.is_some());
        assert!(doc.get(ROOT, "from_v1").unwrap().is_some());
    }

    #[test]
    fn get_heads_returns_pre_tx_heads() {
        let mut doc = Automerge::new();
        let mut tx = doc.transaction();
        tx.put(ROOT, "a", 1).unwrap();
        tx.commit();
        let heads = doc.get_heads();

        let tx = doc.into_transaction(None).unwrap();
        assert_eq!(tx.get_heads(), heads);
        tx.commit();
    }

    #[test]
    fn pending_ops() {
        let doc = Automerge::new();
        let mut tx = doc.into_transaction(None).unwrap();
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
        let tx = doc.into_transaction(None).unwrap();
        let (_, hash) = tx.commit();
        assert!(hash.is_none());
    }
}
