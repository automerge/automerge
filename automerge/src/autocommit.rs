use crate::exid::ExId;
use crate::transaction::{CommitOptions, Transactable};
use crate::types::Patch;
use crate::{
    change::export_change, transaction::TransactionInner, ActorId, Automerge, AutomergeError,
    Change, ChangeHash, Prop, Value,
};
use crate::{sync, Keys, KeysAt, ObjType, ScalarValue};

/// An automerge document that automatically manages transactions.
#[derive(Debug, Clone)]
pub struct AutoCommit {
    doc: Automerge,
    transaction: Option<TransactionInner>,
}

impl Default for AutoCommit {
    fn default() -> Self {
        Self::new()
    }
}

impl AutoCommit {
    pub fn new() -> Self {
        Self {
            doc: Automerge::new(),
            transaction: None,
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

    pub fn enable_patches(&mut self, enable: bool) {
        self.doc.enable_patches(enable)
    }

    pub fn pop_patches(&mut self) -> Vec<Patch> {
        self.doc.pop_patches()
    }

    fn ensure_transaction_open(&mut self) {
        if self.transaction.is_none() {
            let actor = self.doc.get_actor_index();

            let seq = self.doc.states.entry(actor).or_default().len() as u64 + 1;
            let mut deps = self.doc.get_heads();
            if seq > 1 {
                let last_hash = self.get_hash(actor, seq - 1).unwrap();
                if !deps.contains(&last_hash) {
                    deps.push(last_hash);
                }
            }

            self.transaction = Some(TransactionInner {
                actor,
                seq,
                start_op: self.doc.max_op + 1,
                time: 0,
                message: None,
                extra_bytes: Default::default(),
                hash: None,
                operations: vec![],
                deps,
            });
        }
    }

    fn get_hash(&mut self, actor: usize, seq: u64) -> Result<ChangeHash, AutomergeError> {
        self.doc
            .states
            .get(&actor)
            .and_then(|v| v.get(seq as usize - 1))
            .and_then(|&i| self.doc.history.get(i))
            .map(|c| c.hash)
            .ok_or(AutomergeError::InvalidSeq(seq))
    }

    fn update_history(&mut self, change: Change) -> usize {
        self.doc.max_op = std::cmp::max(self.doc.max_op, change.start_op + change.len() as u64 - 1);

        self.update_deps(&change);

        let history_index = self.doc.history.len();

        self.doc
            .states
            .entry(self.doc.ops.m.actors.cache(change.actor_id().clone()))
            .or_default()
            .push(history_index);

        self.doc.history_index.insert(change.hash, history_index);
        self.doc.history.push(change);

        history_index
    }

    fn update_deps(&mut self, change: &Change) {
        for d in &change.deps {
            self.doc.deps.remove(d);
        }
        self.doc.deps.insert(change.hash);
    }

    pub fn fork(&mut self) -> Self {
        self.ensure_transaction_closed();
        Self {
            doc: self.doc.fork(),
            transaction: self.transaction.clone(),
        }
    }

    fn ensure_transaction_closed(&mut self) {
        if let Some(tx) = self.transaction.take() {
            self.update_history(export_change(
                tx,
                &self.doc.ops.m.actors,
                &self.doc.ops.m.props,
            ));
        }
    }

    pub fn load(data: &[u8]) -> Result<Self, AutomergeError> {
        let doc = Automerge::load(data)?;
        Ok(Self {
            doc,
            transaction: None,
        })
    }

    pub fn load_incremental(&mut self, data: &[u8]) -> Result<usize, AutomergeError> {
        self.ensure_transaction_closed();
        self.doc.load_incremental(data)
    }

    pub fn apply_changes(&mut self, changes: Vec<Change>) -> Result<(), AutomergeError> {
        self.ensure_transaction_closed();
        self.doc.apply_changes(changes)
    }

    /// Takes all the changes in `other` which are not in `self` and applies them
    pub fn merge(&mut self, other: &mut Self) -> Result<Vec<ChangeHash>, AutomergeError> {
        self.ensure_transaction_closed();
        other.ensure_transaction_closed();
        self.doc.merge(&mut other.doc)
    }

    pub fn save(&mut self) -> Vec<u8> {
        self.ensure_transaction_closed();
        self.doc.save()
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

    pub fn get_changes(&mut self, have_deps: &[ChangeHash]) -> Vec<&Change> {
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

    pub fn dump(&self) {
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

    #[cfg(feature = "optree-visualisation")]
    pub fn visualise_optree(&self) -> String {
        self.doc.visualise_optree()
    }

    /// Get the current heads of the document.
    ///
    /// This closes the transaction first, if one is in progress.
    pub fn get_heads(&mut self) -> Vec<ChangeHash> {
        self.ensure_transaction_closed();
        self.doc.get_heads()
    }

    pub fn commit(&mut self) -> ChangeHash {
        // ensure that even no changes triggers a change
        self.ensure_transaction_open();
        let tx = self.transaction.take().unwrap();
        tx.commit(&mut self.doc, None, None)
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
    /// doc.set_object(&ROOT, "todos", ObjType::List).unwrap();
    /// let now = SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).unwrap().as_secs() as
    /// i64;
    /// doc.commit_with(CommitOptions::default().with_message("Create todos list").with_time(now));
    /// ```
    pub fn commit_with(&mut self, options: CommitOptions) -> ChangeHash {
        self.ensure_transaction_open();
        let tx = self.transaction.take().unwrap();
        tx.commit(&mut self.doc, options.message, options.time)
    }

    pub fn rollback(&mut self) -> usize {
        self.transaction
            .take()
            .map(|tx| tx.rollback(&mut self.doc))
            .unwrap_or(0)
    }
}

impl Transactable for AutoCommit {
    fn pending_ops(&self) -> usize {
        self.transaction
            .as_ref()
            .map(|t| t.pending_ops())
            .unwrap_or(0)
    }

    // KeysAt::()
    // LenAt::()
    // PropAt::()
    // NthAt::()

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
    fn set<O: AsRef<ExId>, P: Into<Prop>, V: Into<ScalarValue>>(
        &mut self,
        obj: O,
        prop: P,
        value: V,
    ) -> Result<(), AutomergeError> {
        self.ensure_transaction_open();
        let tx = self.transaction.as_mut().unwrap();
        tx.set(&mut self.doc, obj.as_ref(), prop, value)
    }

    fn set_object<O: AsRef<ExId>, P: Into<Prop>>(
        &mut self,
        obj: O,
        prop: P,
        value: ObjType,
    ) -> Result<ExId, AutomergeError> {
        self.ensure_transaction_open();
        let tx = self.transaction.as_mut().unwrap();
        tx.set_object(&mut self.doc, obj.as_ref(), prop, value)
    }

    fn insert<O: AsRef<ExId>, V: Into<ScalarValue>>(
        &mut self,
        obj: O,
        index: usize,
        value: V,
    ) -> Result<(), AutomergeError> {
        self.ensure_transaction_open();
        let tx = self.transaction.as_mut().unwrap();
        tx.insert(&mut self.doc, obj.as_ref(), index, value)
    }

    fn insert_object(
        &mut self,
        obj: &ExId,
        index: usize,
        value: ObjType,
    ) -> Result<ExId, AutomergeError> {
        self.ensure_transaction_open();
        let tx = self.transaction.as_mut().unwrap();
        tx.insert_object(&mut self.doc, obj, index, value)
    }

    fn inc<O: AsRef<ExId>, P: Into<Prop>>(
        &mut self,
        obj: O,
        prop: P,
        value: i64,
    ) -> Result<(), AutomergeError> {
        self.ensure_transaction_open();
        let tx = self.transaction.as_mut().unwrap();
        tx.inc(&mut self.doc, obj.as_ref(), prop, value)
    }

    fn del<O: AsRef<ExId>, P: Into<Prop>>(
        &mut self,
        obj: O,
        prop: P,
    ) -> Result<(), AutomergeError> {
        self.ensure_transaction_open();
        let tx = self.transaction.as_mut().unwrap();
        tx.del(&mut self.doc, obj.as_ref(), prop)
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
        let tx = self.transaction.as_mut().unwrap();
        tx.splice(&mut self.doc, obj.as_ref(), pos, del, vals)
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
