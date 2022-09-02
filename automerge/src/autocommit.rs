use std::ops::RangeBounds;

use crate::exid::ExId;
use crate::op_observer::OpObserver;
use crate::transaction::{CommitOptions, Transactable};
use crate::{
    sync, Keys, KeysAt, ListRange, ListRangeAt, MapRange, MapRangeAt, ObjType, Parents, ScalarValue,
};
use crate::{
    transaction::TransactionInner, ActorId, Automerge, AutomergeError, Change, ChangeHash, Prop,
    Value, Values,
};

/// An automerge document that automatically manages transactions.
#[derive(Debug, Clone)]
pub struct AutoCommitWithObs<Obs: OpObserver> {
    doc: Automerge,
    transaction: Option<(Obs, TransactionInner)>,
    pub op_observer: Obs,
}

pub type AutoCommit = AutoCommitWithObs<()>;

impl<O: OpObserver> Default for AutoCommitWithObs<O> {
    fn default() -> Self {
        let op_observer = O::default();
        AutoCommitWithObs {
            doc: Automerge::new(),
            transaction: None,
            op_observer,
        }
    }
}

impl AutoCommit {
    pub fn new() -> AutoCommit {
        AutoCommitWithObs {
            doc: Automerge::new(),
            transaction: None,
            op_observer: (),
        }
    }
}

impl<Obs: OpObserver> AutoCommitWithObs<Obs> {
    pub fn with_observer<Obs2: OpObserver>(self, op_observer: Obs2) -> AutoCommitWithObs<Obs2> {
        AutoCommitWithObs {
            doc: self.doc,
            transaction: self.transaction.map(|(_, t)| (op_observer.branch(), t)),
            op_observer,
        }
    }

    /// Get the inner document.
    #[doc(hidden)]
    pub fn document(&mut self) -> &Automerge {
        self.ensure_transaction_closed();
        &self.doc
    }

    pub fn with_actor(mut self, actor: ActorId) -> Self {
        self.ensure_transaction_closed();
        self.doc.set_actor(actor);
        self
    }

    pub fn set_actor(&mut self, actor: ActorId) -> &mut Self {
        self.ensure_transaction_closed();
        self.doc.set_actor(actor);
        self
    }

    pub fn get_actor(&self) -> &ActorId {
        self.doc.get_actor()
    }

    fn ensure_transaction_open(&mut self) {
        if self.transaction.is_none() {
            self.transaction = Some((self.op_observer.branch(), self.doc.transaction_inner()));
        }
    }

    pub fn fork(&mut self) -> Self {
        self.ensure_transaction_closed();
        Self {
            doc: self.doc.fork(),
            transaction: self.transaction.clone(),
            op_observer: self.op_observer.clone(),
        }
    }

    pub fn fork_at(&mut self, heads: &[ChangeHash]) -> Result<Self, AutomergeError> {
        self.ensure_transaction_closed();
        Ok(Self {
            doc: self.doc.fork_at(heads)?,
            transaction: self.transaction.clone(),
            op_observer: self.op_observer.clone(),
        })
    }

    pub fn ensure_transaction_closed(&mut self) {
        if let Some((current, tx)) = self.transaction.take() {
            self.op_observer.merge(&current);
            tx.commit(&mut self.doc, None, None);
        }
    }

    pub fn load(data: &[u8]) -> Result<Self, AutomergeError> {
        // passing a () observer here has performance implications on all loads
        // if we want an autocommit::load() method that can be observered we need to make a new method
        // fn observed_load() ?
        let doc = Automerge::load(data)?;
        let op_observer = Obs::default();
        Ok(Self {
            doc,
            transaction: None,
            op_observer,
        })
    }

    pub fn load_incremental(&mut self, data: &[u8]) -> Result<usize, AutomergeError> {
        self.ensure_transaction_closed();
        // TODO - would be nice to pass None here instead of &mut ()
        self.doc
            .load_incremental_with(data, Some(&mut self.op_observer))
    }

    pub fn apply_changes(
        &mut self,
        changes: impl IntoIterator<Item = Change>,
    ) -> Result<(), AutomergeError> {
        self.ensure_transaction_closed();
        self.doc
            .apply_changes_with(changes, Some(&mut self.op_observer))
    }

    /// Takes all the changes in `other` which are not in `self` and applies them
    pub fn merge<Obs2: OpObserver>(
        &mut self,
        other: &mut AutoCommitWithObs<Obs2>,
    ) -> Result<Vec<ChangeHash>, AutomergeError> {
        self.ensure_transaction_closed();
        other.ensure_transaction_closed();
        self.doc
            .merge_with(&mut other.doc, Some(&mut self.op_observer))
    }

    pub fn save(&mut self) -> Vec<u8> {
        self.ensure_transaction_closed();
        self.doc.save()
    }

    pub fn save_nocompress(&mut self) -> Vec<u8> {
        self.ensure_transaction_closed();
        self.doc.save_nocompress()
    }

    // should this return an empty vec instead of None?
    pub fn save_incremental(&mut self) -> Vec<u8> {
        self.ensure_transaction_closed();
        self.doc.save_incremental()
    }

    pub fn get_missing_deps(&mut self, heads: &[ChangeHash]) -> Vec<ChangeHash> {
        self.ensure_transaction_closed();
        self.doc.get_missing_deps(heads)
    }

    pub fn get_last_local_change(&mut self) -> Option<&Change> {
        self.ensure_transaction_closed();
        self.doc.get_last_local_change()
    }

    pub fn get_changes(
        &mut self,
        have_deps: &[ChangeHash],
    ) -> Result<Vec<&Change>, AutomergeError> {
        self.ensure_transaction_closed();
        self.doc.get_changes(have_deps)
    }

    pub fn get_change_by_hash(&mut self, hash: &ChangeHash) -> Option<&Change> {
        self.ensure_transaction_closed();
        self.doc.get_change_by_hash(hash)
    }

    pub fn get_changes_added<'a>(&mut self, other: &'a mut Self) -> Vec<&'a Change> {
        self.ensure_transaction_closed();
        other.ensure_transaction_closed();
        self.doc.get_changes_added(&other.doc)
    }

    pub fn import(&self, s: &str) -> Result<ExId, AutomergeError> {
        self.doc.import(s)
    }

    pub fn dump(&mut self) {
        self.ensure_transaction_closed();
        self.doc.dump()
    }

    pub fn generate_sync_message(&mut self, sync_state: &mut sync::State) -> Option<sync::Message> {
        self.ensure_transaction_closed();
        self.doc.generate_sync_message(sync_state)
    }

    pub fn receive_sync_message(
        &mut self,
        sync_state: &mut sync::State,
        message: sync::Message,
    ) -> Result<(), AutomergeError> {
        self.ensure_transaction_closed();
        self.doc.receive_sync_message(sync_state, message)
    }

    /// Return a graphviz representation of the opset.
    ///
    /// # Arguments
    ///
    /// * objects: An optional list of object IDs to display, if not specified all objects are
    ///            visualised
    #[cfg(feature = "optree-visualisation")]
    pub fn visualise_optree(&self, objects: Option<Vec<ExId>>) -> String {
        self.doc.visualise_optree(objects)
    }

    /// Get the current heads of the document.
    ///
    /// This closes the transaction first, if one is in progress.
    pub fn get_heads(&mut self) -> Vec<ChangeHash> {
        self.ensure_transaction_closed();
        self.doc.get_heads()
    }

    pub fn commit(&mut self) -> ChangeHash {
        self.commit_with(CommitOptions::default())
    }

    /// Commit the current operations with some options.
    ///
    /// ```
    /// # use automerge::transaction::CommitOptions;
    /// # use automerge::transaction::Transactable;
    /// # use automerge::ROOT;
    /// # use automerge::AutoCommit;
    /// # use automerge::ObjType;
    /// # use std::time::SystemTime;
    /// let mut doc = AutoCommit::new();
    /// doc.put_object(&ROOT, "todos", ObjType::List).unwrap();
    /// let now = SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).unwrap().as_secs() as
    /// i64;
    /// doc.commit_with(CommitOptions::default().with_message("Create todos list").with_time(now));
    /// ```
    pub fn commit_with(&mut self, options: CommitOptions) -> ChangeHash {
        // ensure that even no changes triggers a change
        self.ensure_transaction_open();
        let (current, tx) = self.transaction.take().unwrap();
        self.op_observer.merge(&current);
        tx.commit(&mut self.doc, options.message, options.time)
    }

    pub fn rollback(&mut self) -> usize {
        self.transaction
            .take()
            .map(|(_, tx)| tx.rollback(&mut self.doc))
            .unwrap_or(0)
    }
}

impl<Obs: OpObserver> Transactable for AutoCommitWithObs<Obs> {
    fn pending_ops(&self) -> usize {
        self.transaction
            .as_ref()
            .map(|(_, t)| t.pending_ops())
            .unwrap_or(0)
    }

    // KeysAt::()
    // LenAt::()
    // PropAt::()
    // NthAt::()

    fn keys<O: AsRef<ExId>>(&self, obj: O) -> Keys<'_, '_> {
        self.doc.keys(obj)
    }

    fn keys_at<O: AsRef<ExId>>(&self, obj: O, heads: &[ChangeHash]) -> KeysAt<'_, '_> {
        self.doc.keys_at(obj, heads)
    }

    fn map_range<O: AsRef<ExId>, R: RangeBounds<String>>(
        &self,
        obj: O,
        range: R,
    ) -> MapRange<'_, R> {
        self.doc.map_range(obj, range)
    }

    fn map_range_at<O: AsRef<ExId>, R: RangeBounds<String>>(
        &self,
        obj: O,
        range: R,
        heads: &[ChangeHash],
    ) -> MapRangeAt<'_, R> {
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
    ) -> ListRangeAt<'_, R> {
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

    fn object_type<O: AsRef<ExId>>(&self, obj: O) -> Option<ObjType> {
        self.doc.object_type(obj)
    }

    // set(obj, prop, value) - value can be scalar or objtype
    // del(obj, prop)
    // inc(obj, prop, value)
    // insert(obj, index, value)

    /// Set the value of property `P` to value `V` in object `obj`.
    ///
    /// # Returns
    ///
    /// The opid of the operation which was created, or None if this operation doesn't change the
    /// document or create a new object.
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
        self.ensure_transaction_open();
        let (current, tx) = self.transaction.as_mut().unwrap();
        tx.put(&mut self.doc, current, obj.as_ref(), prop, value)
    }

    fn put_object<O: AsRef<ExId>, P: Into<Prop>>(
        &mut self,
        obj: O,
        prop: P,
        value: ObjType,
    ) -> Result<ExId, AutomergeError> {
        self.ensure_transaction_open();
        let (current, tx) = self.transaction.as_mut().unwrap();
        tx.put_object(&mut self.doc, current, obj.as_ref(), prop, value)
    }

    fn insert<O: AsRef<ExId>, V: Into<ScalarValue>>(
        &mut self,
        obj: O,
        index: usize,
        value: V,
    ) -> Result<(), AutomergeError> {
        self.ensure_transaction_open();
        let (current, tx) = self.transaction.as_mut().unwrap();
        tx.insert(&mut self.doc, current, obj.as_ref(), index, value)
    }

    fn insert_object<O: AsRef<ExId>>(
        &mut self,
        obj: O,
        index: usize,
        value: ObjType,
    ) -> Result<ExId, AutomergeError> {
        self.ensure_transaction_open();
        let (current, tx) = self.transaction.as_mut().unwrap();
        tx.insert_object(&mut self.doc, current, obj.as_ref(), index, value)
    }

    fn increment<O: AsRef<ExId>, P: Into<Prop>>(
        &mut self,
        obj: O,
        prop: P,
        value: i64,
    ) -> Result<(), AutomergeError> {
        self.ensure_transaction_open();
        let (current, tx) = self.transaction.as_mut().unwrap();
        tx.increment(&mut self.doc, current, obj.as_ref(), prop, value)
    }

    fn delete<O: AsRef<ExId>, P: Into<Prop>>(
        &mut self,
        obj: O,
        prop: P,
    ) -> Result<(), AutomergeError> {
        self.ensure_transaction_open();
        let (current, tx) = self.transaction.as_mut().unwrap();
        tx.delete(&mut self.doc, current, obj.as_ref(), prop)
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
        self.ensure_transaction_open();
        let (current, tx) = self.transaction.as_mut().unwrap();
        tx.splice(&mut self.doc, current, obj.as_ref(), pos, del, vals)
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

    // TODO - I need to return these OpId's here **only** to get
    // the legacy conflicts format of { [opid]: value }
    // Something better?
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

    fn parents<O: AsRef<ExId>>(&self, obj: O) -> Result<Parents<'_>, AutomergeError> {
        self.doc.parents(obj)
    }
}
