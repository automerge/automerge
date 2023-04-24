use std::ops::RangeBounds;

use crate::exid::ExId;
use crate::history::History;
use crate::hydrate;
use crate::iter::{Keys, ListRange, MapRange, Values};
use crate::marks::{ExpandMark, Mark};
use crate::op_observer::{OpObserver, TextRepresentation};
use crate::sync::SyncDoc;
use crate::transaction::{CommitOptions, Transactable};
use crate::{sync, ObjType, Parents, ReadDoc, ScalarValue};
use crate::{
    transaction::TransactionInner, ActorId, Automerge, AutomergeError, Change, ChangeHash, Cursor,
    Prop, Value,
};

/// An automerge document that automatically manages transactions.
///
/// An `AutoCommit` can optionally manage an [`OpObserver`]. This observer will be notified of all
/// changes made by both remote and local changes. The type parameter `O` tracks whether this
/// document is observed or not.
///
/// ## Creating, loading, merging and forking documents
///
/// A new document can be created with [`Self::new`], which will create a document with a random
/// [`ActorId`]. Existing documents can be loaded with [`Self::load`].
///
/// If you have two documents and you want to merge the changes from one into the other you can use
/// [`Self::merge`].
///
/// If you have a document you want to split into two concurrent threads of execution you can use
/// [`Self::fork`]. If you want to split a document from ealier in its history you can use
/// [`Self::fork_at`].
///
/// ## Reading values
///
/// [`Self`] implements [`ReadDoc`], which provides methods for reading values from the document.
///
/// ## Modifying a document
///
/// This type implements [`Transactable`] directly, so you can modify it using methods from [`Transactable`].
///
/// ## Synchronization
///
/// To synchronise call [`Self::sync`] which returns an implementation of [`SyncDoc`]
///
#[derive(Debug, Clone)]
pub struct AutoCommit {
    pub(crate) doc: Automerge,
    transaction: Option<(History, TransactionInner)>,
    history: History,
    diff_cursor: Vec<ChangeHash>,
}

/// An autocommit document with no observer
///
/// See [`AutoCommit`]
impl Default for AutoCommit {
    fn default() -> Self {
        AutoCommit {
            doc: Automerge::new(),
            transaction: None,
            history: History::innactive(),
            diff_cursor: Vec::new(),
        }
    }
}

impl AutoCommit {
    pub fn new() -> AutoCommit {
        AutoCommit::default()
    }

    pub fn load(data: &[u8]) -> Result<Self, AutomergeError> {
        let doc = Automerge::load(data)?;
        Ok(Self {
            doc,
            transaction: None,
            history: History::innactive(),
            diff_cursor: Vec::new(),
        })
    }

    /// Erases the diff cursor created by [`Self::update_diff_cursor`] and no
    /// longer indexes changes to the document.
    pub fn reset_diff_cursor(&mut self) {
        self.ensure_transaction_closed();
        self.history = History::innactive();
        self.diff_cursor = Vec::new();
    }

    /// Sets the [`Self::diff_cursor`] to current heads of the document and will begin
    /// building an index with every change moving forward.
    ///
    /// If [`Self::diff`] is called with [`Self::diff_cursor`] as `before` and
    /// [`Self::get_heads`] as `after` - the index will be used
    ///
    /// If the cursor is no longer needed it can be reset with
    /// [`Self::reset_diff_cursor`]
    pub fn update_diff_cursor(&mut self) {
        self.ensure_transaction_closed();
        self.history.set_active(true);
        self.history.truncate();
        self.diff_cursor = self.doc.get_heads();
    }

    /// Returns the cursor set by [`Self::update_diff_cursor`]
    pub fn diff_cursor(&self) -> Vec<ChangeHash> {
        self.diff_cursor.clone()
    }

    /// Generates a diff from `before` to `after` for the given observer.
    ///
    /// By default the diff requires a sequental scan of all the ops in the doc.
    ///
    /// To do a fast indexed diff `before` must equal [`Self::diff_cursor`] and
    /// `after` must equal [`Self::get_heads`]. The diff cursor is managed with
    /// [`Self::update_diff_cursor`] and [`Self::reset_diff_cursor`]
    ///
    /// Managing the diff index has a small but non-zero overhead.  It should be
    /// disabled if no longer needed.  If a signifigantly large change is applied
    /// to the document it may be faster to reset the index before applying it,
    /// doing an unindxed diff afterwards and then reenable the index.
    ///
    /// # Arguments
    ///
    /// * `before` - heads from [`Self::get_heads()`] at beginning point in the documents history
    /// * `after` - heads from [`Self::get_heads()`] at ending point in the documents history.
    /// #           If `None` is passed the current heads will be used.
    /// * `obs` - An [`OpObserver`] to observe the changes from before to after.
    ///
    /// This function returns the observer passed in for convience
    ///
    /// Note: `before` and `after` do not have to be chronological.  Document state can move backward.
    /// Normal use might look like:
    ///
    /// # Example
    ///
    /// ```
    /// use automerge::{ AutoCommit, VecOpObserver };
    ///
    /// let mut doc = AutoCommit::new(); // or AutoCommit::load(data)
    /// // make some changes - use and update the index
    /// let heads = doc.get_heads();
    /// let diff_cursor = doc.diff_cursor();
    /// let patches = doc.diff(&diff_cursor, &heads, VecOpObserver::default()).take_patches();
    /// doc.update_diff_cursor();
    /// ```
    ///
    /// See [`Self::diff_incremental`] for encapsulating this pattern.
    pub fn diff<Obs: OpObserver, As: AsMut<Obs>>(
        &mut self,
        before: &[ChangeHash],
        after: &[ChangeHash],
        mut obs: As,
    ) -> As {
        self.ensure_transaction_closed();
        let heads = self.doc.get_heads();
        if after == heads && before == self.diff_cursor && self.history.is_active() {
            self.history.observe(obs.as_mut(), &self.doc, None);
        } else if before.is_empty() && after == heads {
            self.doc.observe_current_state(obs.as_mut());
        } else {
            self.doc.observe_diff(before, after, obs.as_mut());
        }
        obs
    }

    /// This is a convience function that encapsulates the following common pattern
    /// ```
    /// use automerge::{AutoCommit, VecOpObserver};
    /// let mut doc = AutoCommit::new();
    /// // make some changes
    /// let heads = doc.get_heads();
    /// let diff_cursor = doc.diff_cursor();
    /// let observer = doc.diff(&diff_cursor, &heads, VecOpObserver::default());
    /// doc.update_diff_cursor();
    /// ```
    pub fn diff_incremental<Obs: OpObserver + Default + AsMut<Obs>>(&mut self) -> Obs {
        self.ensure_transaction_closed();
        let heads = self.doc.get_heads();
        let diff_cursor = self.diff_cursor();
        let observer = self.diff(&diff_cursor, &heads, Obs::default());
        self.update_diff_cursor();
        observer
    }

    pub fn fork(&mut self) -> Self {
        self.ensure_transaction_closed();
        Self {
            doc: self.doc.fork(),
            transaction: self.transaction.clone(),
            history: self.history.clone(),
            diff_cursor: self.diff_cursor.clone(),
        }
    }

    pub fn fork_at(&mut self, heads: &[ChangeHash]) -> Result<Self, AutomergeError> {
        self.ensure_transaction_closed();
        Ok(Self {
            doc: self.doc.fork_at(heads)?,
            transaction: self.transaction.clone(),
            history: self.history.clone(),
            diff_cursor: self.diff_cursor.clone(),
        })
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
            let args = self.doc.transaction_args();
            let inner = TransactionInner::new(args);
            self.transaction = Some((self.history.branch(), inner))
        }
    }

    fn ensure_transaction_closed(&mut self) {
        if let Some((history, tx)) = self.transaction.take() {
            self.history.merge(history);
            tx.commit(&mut self.doc, None, None);
        }
    }

    /// Load an incremental save of a document.
    ///
    /// Unlike `load` this imports changes into an existing document. It will work with both the
    /// output of [`Self::save`] and [`Self::save_incremental`]
    ///
    /// The return value is the number of ops which were applied, this is not useful and will
    /// change in future.
    pub fn load_incremental(&mut self, data: &[u8]) -> Result<usize, AutomergeError> {
        self.ensure_transaction_closed();
        self.doc.load_incremental_inner(data, &mut self.history)
    }

    pub fn apply_changes(
        &mut self,
        changes: impl IntoIterator<Item = Change>,
    ) -> Result<(), AutomergeError> {
        self.ensure_transaction_closed();
        self.doc.apply_changes_inner(changes, &mut self.history)
    }

    /// Takes all the changes in `other` which are not in `self` and applies them
    pub fn merge(&mut self, other: &mut AutoCommit) -> Result<Vec<ChangeHash>, AutomergeError> {
        self.ensure_transaction_closed();
        other.ensure_transaction_closed();
        self.doc.merge_inner(&mut other.doc, &mut self.history)
    }

    /// Save the entirety of this document in a compact form.
    pub fn save(&mut self) -> Vec<u8> {
        self.ensure_transaction_closed();
        self.doc.save()
    }

    /// Save this document, but don't run it through DEFLATE afterwards
    pub fn save_and_verify(&mut self) -> Result<Vec<u8>, AutomergeError> {
        self.ensure_transaction_closed();
        self.doc.save_and_verify()
    }

    /// Save this document, but don't run it through DEFLATE afterwards
    pub fn save_nocompress(&mut self) -> Vec<u8> {
        self.ensure_transaction_closed();
        self.doc.save_nocompress()
    }

    /// Save the changes since the last call to [Self::save`]
    ///
    /// The output of this will not be a compressed document format, but a series of individual
    /// changes. This is useful if you know you have only made a small change since the last `save`
    /// and you want to immediately send it somewhere (e.g. you've inserted a single character in a
    /// text object).
    pub fn save_incremental(&mut self) -> Vec<u8> {
        self.ensure_transaction_closed();
        self.doc.save_incremental()
    }

    pub fn get_missing_deps(&mut self, heads: &[ChangeHash]) -> Vec<ChangeHash> {
        self.ensure_transaction_closed();
        self.doc.get_missing_deps(heads)
    }

    /// Get the last change made by this documents actor ID
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

    /// Get changes in `other` that are not in `self
    pub fn get_changes_added<'a>(&mut self, other: &'a mut Self) -> Vec<&'a Change> {
        self.ensure_transaction_closed();
        other.ensure_transaction_closed();
        self.doc.get_changes_added(&other.doc)
    }

    #[doc(hidden)]
    pub fn import(&self, s: &str) -> Result<(ExId, ObjType), AutomergeError> {
        self.doc.import(s)
    }

    #[doc(hidden)]
    pub fn import_obj(&self, s: &str) -> Result<ExId, AutomergeError> {
        self.doc.import_obj(s)
    }

    #[doc(hidden)]
    pub fn dump(&mut self) {
        self.ensure_transaction_closed();
        self.doc.dump()
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

    pub fn set_text_rep(&mut self, text_rep: TextRepresentation) {
        self.doc.set_text_rep(text_rep)
    }

    pub fn get_text_rep(&mut self) -> TextRepresentation {
        self.doc.get_text_rep()
    }

    pub fn with_text_rep(mut self, text_rep: TextRepresentation) -> Self {
        self.doc.set_text_rep(text_rep);
        self
    }

    /// Commit any uncommitted changes
    ///
    /// Returns `None` if there were no operations to commit
    pub fn commit(&mut self) -> Option<ChangeHash> {
        self.commit_with(CommitOptions::default())
    }

    /// Commit the current operations with some options.
    ///
    /// Returns `None` if there were no operations to commit
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
    pub fn commit_with(&mut self, options: CommitOptions) -> Option<ChangeHash> {
        // ensure that even no changes triggers a change
        self.ensure_transaction_open();
        let (history, tx) = self.transaction.take().unwrap();
        self.history.merge(history);
        tx.commit(&mut self.doc, options.message, options.time)
    }

    /// Remove any changes that have been made in the current transaction from the document
    pub fn rollback(&mut self) -> usize {
        self.transaction
            .take()
            .map(|(_, tx)| tx.rollback(&mut self.doc))
            .unwrap_or(0)
    }

    /// Generate an empty change
    ///
    /// The main reason to do this is if you wish to create a "merge commit" which has all the
    /// current heads of the documents as dependencies but you have no new operations to create.
    ///
    /// Because this structure is an "autocommit" there may actually be outstanding operations to
    /// submit. If this is the case this function will create two changes, one with the outstanding
    /// operations and a new one with no operations. The returned `ChangeHash` will always be the
    /// hash of the empty change.
    pub fn empty_change(&mut self, options: CommitOptions) -> ChangeHash {
        self.ensure_transaction_closed();
        let args = self.doc.transaction_args();
        TransactionInner::empty(&mut self.doc, args, options.message, options.time)
    }

    /// An implementation of [`crate::sync::SyncDoc`] for this autocommit
    ///
    /// This ensures that any outstanding transactions for this document are committed before
    /// taking part in the sync protocol
    pub fn sync(&mut self) -> impl SyncDoc + '_ {
        self.ensure_transaction_closed();
        SyncWrapper { inner: self }
    }

    pub fn hydrate(&self, heads: Option<&[ChangeHash]>) -> hydrate::Value {
        self.doc.hydrate(heads)
    }
}

impl ReadDoc for AutoCommit {
    fn parents<O: AsRef<ExId>>(&self, obj: O) -> Result<Parents<'_>, AutomergeError> {
        self.doc.parents(obj)
    }

    fn parents_at<O: AsRef<ExId>>(
        &self,
        obj: O,
        heads: &[ChangeHash],
    ) -> Result<Parents<'_>, AutomergeError> {
        self.doc.parents_at(obj, heads)
    }

    fn keys<O: AsRef<ExId>>(&self, obj: O) -> Keys<'_> {
        self.doc.keys(obj)
    }

    fn keys_at<O: AsRef<ExId>>(&self, obj: O, heads: &[ChangeHash]) -> Keys<'_> {
        self.doc.keys_at(obj, heads)
    }

    fn map_range<'a, O: AsRef<ExId>, R: RangeBounds<String> + 'a>(
        &'a self,
        obj: O,
        range: R,
    ) -> MapRange<'a, R> {
        self.doc.map_range(obj, range)
    }

    fn map_range_at<'a, O: AsRef<ExId>, R: RangeBounds<String> + 'a>(
        &'a self,
        obj: O,
        range: R,
        heads: &[ChangeHash],
    ) -> MapRange<'a, R> {
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

    fn get_missing_deps(&self, heads: &[ChangeHash]) -> Vec<ChangeHash> {
        self.doc.get_missing_deps(heads)
    }

    fn get_change_by_hash(&self, hash: &ChangeHash) -> Option<&Change> {
        self.doc.get_change_by_hash(hash)
    }
}

impl Transactable for AutoCommit {
    fn pending_ops(&self) -> usize {
        self.transaction
            .as_ref()
            .map(|(_, t)| t.pending_ops())
            .unwrap_or(0)
    }

    fn put<O: AsRef<ExId>, P: Into<Prop>, V: Into<ScalarValue>>(
        &mut self,
        obj: O,
        prop: P,
        value: V,
    ) -> Result<(), AutomergeError> {
        self.ensure_transaction_open();
        let (history, tx) = self.transaction.as_mut().unwrap();
        tx.put(&mut self.doc, history, obj.as_ref(), prop, value)
    }

    fn put_object<O: AsRef<ExId>, P: Into<Prop>>(
        &mut self,
        obj: O,
        prop: P,
        value: ObjType,
    ) -> Result<ExId, AutomergeError> {
        self.ensure_transaction_open();
        let (history, tx) = self.transaction.as_mut().unwrap();
        tx.put_object(&mut self.doc, history, obj.as_ref(), prop, value)
    }

    fn insert<O: AsRef<ExId>, V: Into<ScalarValue>>(
        &mut self,
        obj: O,
        index: usize,
        value: V,
    ) -> Result<(), AutomergeError> {
        self.ensure_transaction_open();
        let (history, tx) = self.transaction.as_mut().unwrap();
        tx.insert(&mut self.doc, history, obj.as_ref(), index, value)
    }

    fn insert_object<O: AsRef<ExId>>(
        &mut self,
        obj: O,
        index: usize,
        value: ObjType,
    ) -> Result<ExId, AutomergeError> {
        self.ensure_transaction_open();
        let (history, tx) = self.transaction.as_mut().unwrap();
        tx.insert_object(&mut self.doc, history, obj.as_ref(), index, value)
    }

    fn increment<O: AsRef<ExId>, P: Into<Prop>>(
        &mut self,
        obj: O,
        prop: P,
        value: i64,
    ) -> Result<(), AutomergeError> {
        self.ensure_transaction_open();
        let (history, tx) = self.transaction.as_mut().unwrap();
        tx.increment(&mut self.doc, history, obj.as_ref(), prop, value)
    }

    fn delete<O: AsRef<ExId>, P: Into<Prop>>(
        &mut self,
        obj: O,
        prop: P,
    ) -> Result<(), AutomergeError> {
        self.ensure_transaction_open();
        let (history, tx) = self.transaction.as_mut().unwrap();
        tx.delete(&mut self.doc, history, obj.as_ref(), prop)
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
        let (history, tx) = self.transaction.as_mut().unwrap();
        tx.splice(&mut self.doc, history, obj.as_ref(), pos, del, vals)
    }

    fn splice_text<O: AsRef<ExId>>(
        &mut self,
        obj: O,
        pos: usize,
        del: usize,
        text: &str,
    ) -> Result<(), AutomergeError> {
        self.ensure_transaction_open();
        let (history, tx) = self.transaction.as_mut().unwrap();
        tx.splice_text(&mut self.doc, history, obj.as_ref(), pos, del, text)
    }

    fn mark<O: AsRef<ExId>>(
        &mut self,
        obj: O,
        mark: Mark<'_>,
        expand: ExpandMark,
    ) -> Result<(), AutomergeError> {
        self.ensure_transaction_open();
        let (history, tx) = self.transaction.as_mut().unwrap();
        tx.mark(&mut self.doc, history, obj.as_ref(), mark, expand)
    }

    fn unmark<O: AsRef<ExId>>(
        &mut self,
        obj: O,
        key: &str,
        start: usize,
        end: usize,
        expand: ExpandMark,
    ) -> Result<(), AutomergeError> {
        self.ensure_transaction_open();
        let (history, tx) = self.transaction.as_mut().unwrap();
        tx.unmark(
            &mut self.doc,
            history,
            obj.as_ref(),
            key,
            start,
            end,
            expand,
        )
    }

    fn base_heads(&self) -> Vec<ChangeHash> {
        self.doc.get_heads()
    }
}

// A wrapper we return from `AutoCommit::sync` to ensure that transactions are closed before we
// start syncing
struct SyncWrapper<'a> {
    inner: &'a mut AutoCommit,
}

impl<'a> SyncDoc for SyncWrapper<'a> {
    fn generate_sync_message(&self, sync_state: &mut sync::State) -> Option<sync::Message> {
        self.inner.doc.generate_sync_message(sync_state)
    }

    fn receive_sync_message(
        &mut self,
        sync_state: &mut sync::State,
        message: sync::Message,
    ) -> Result<(), AutomergeError> {
        self.inner.ensure_transaction_closed();
        self.inner
            .doc
            .receive_sync_message_inner(sync_state, message, &mut self.inner.history)
    }

    fn receive_sync_message_with<Obs: OpObserver>(
        &mut self,
        sync_state: &mut sync::State,
        message: sync::Message,
        op_observer: &mut Obs,
    ) -> Result<(), AutomergeError> {
        let mut history = History::active();
        self.inner
            .doc
            .receive_sync_message_inner(sync_state, message, &mut history)?;
        if self.inner.history.is_active() {
            self.inner.history.merge(history.clone());
        }
        history.observe(op_observer, &self.inner.doc, None);
        Ok(())
    }
}
