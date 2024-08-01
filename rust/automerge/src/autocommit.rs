use std::ops::RangeBounds;

use crate::automerge::SaveOptions;
use crate::automerge::{current_state, diff};
use crate::exid::ExId;
use crate::iter::Spans;
use crate::iter::{Keys, ListRange, MapRange, Values};
use crate::marks::{ExpandMark, Mark, MarkSet};
use crate::patches::{PatchLog, TextRepresentation};
use crate::sync::SyncDoc;
use crate::transaction::{CommitOptions, Transactable};
use crate::types::Clock;
use crate::{hydrate, OnPartialLoad};
use crate::{sync, ObjType, Parents, Patch, ReadDoc, ScalarValue};
use crate::{
    transaction::TransactionInner, ActorId, Automerge, AutomergeError, Change, ChangeHash, Cursor,
    Prop, Value,
};
use crate::{LoadOptions, VerificationMode};

/// An automerge document that automatically manages transactions.
///
/// ## Creating, loading, merging and forking documents
///
/// A new document can be created with [`Self::new()`], which will create a document with a random
/// [`ActorId`]. Existing documents can be loaded with [`Self::load()`].
///
/// If you have two documents and you want to merge the changes from one into the other you can use
/// [`Self::merge()`].
///
/// If you have a document you want to split into two concurrent threads of execution you can use
/// [`Self::fork()`]. If you want to split a document from ealier in its history you can use
/// [`Self::fork_at()`].
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
/// To synchronise call [`Self::sync()`] which returns an implementation of [`SyncDoc`]
///
/// ## Patches, maintaining materialized views
///
/// [`AutoCommit`] allows you to generate [`Patch`]es representing changes to the current state of
/// the document which you can use to maintain a materialized view of the current state. There are
/// several ways to use this. See the documentation on [`Self::diff()`] for more details, but the key
/// point to remember is that [`AutoCommit`] manages an internal "diff cursor" for you. This is a
/// representation of the heads of the document last time you called [`Self::diff_incremental()`]
/// but you can also manage it directly using [`Self::update_diff_cursor()`] and
/// [`Self::reset_diff_cursor()`].
#[derive(Debug, Clone)]
pub struct AutoCommit {
    pub(crate) doc: Automerge,
    transaction: Option<(PatchLog, TransactionInner)>,
    patch_log: PatchLog,
    diff_cursor: Vec<ChangeHash>,
    diff_cache: Option<(OpRange, Vec<Patch>)>,
    save_cursor: Vec<ChangeHash>,
    isolation: Option<Vec<ChangeHash>>,
}

/// An autocommit document with an inactive [`PatchLog`]
///
/// See [`AutoCommit`]
impl Default for AutoCommit {
    fn default() -> Self {
        AutoCommit {
            doc: Automerge::new(),
            transaction: None,
            patch_log: PatchLog::inactive(TextRepresentation::default()),
            diff_cursor: Vec::new(),
            diff_cache: None,
            save_cursor: Vec::new(),
            isolation: None,
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
            patch_log: PatchLog::inactive(TextRepresentation::default()),
            diff_cursor: Vec::new(),
            diff_cache: None,
            save_cursor: Vec::new(),
            isolation: None,
        })
    }

    pub fn load_unverified_heads(data: &[u8]) -> Result<Self, AutomergeError> {
        let doc = Automerge::load_unverified_heads(data)?;
        Ok(Self {
            doc,
            transaction: None,
            patch_log: PatchLog::inactive(TextRepresentation::default()),
            diff_cursor: Vec::new(),
            diff_cache: None,
            save_cursor: Vec::new(),
            isolation: None,
        })
    }

    #[deprecated(since = "0.5.2", note = "use `load_with_options` instead")]
    pub fn load_with(
        data: &[u8],
        on_error: OnPartialLoad,
        mode: VerificationMode,
    ) -> Result<Self, AutomergeError> {
        Self::load_with_options(
            data,
            LoadOptions::new()
                .on_partial_load(on_error)
                .verification_mode(mode),
        )
    }

    pub fn load_with_options(
        data: &[u8],
        options: LoadOptions<'_>,
    ) -> Result<Self, AutomergeError> {
        let doc = Automerge::load_with_options(data, options)?;
        Ok(Self {
            doc,
            transaction: None,
            patch_log: PatchLog::inactive(TextRepresentation::default()),
            diff_cursor: Vec::new(),
            diff_cache: None,
            save_cursor: Vec::new(),
            isolation: None,
        })
    }

    /// Erases the diff cursor created by [`Self::update_diff_cursor()`] and no
    /// longer indexes changes to the document.
    pub fn reset_diff_cursor(&mut self) {
        self.ensure_transaction_closed();
        self.patch_log = PatchLog::inactive(TextRepresentation::default());
        self.diff_cursor = Vec::new();
    }

    /// Sets the [`Self::diff_cursor()`] to current heads of the document and will begin
    /// building an index with every change moving forward.
    ///
    /// If [`Self::diff()`] is called with [`Self::diff_cursor()`] as `before` and
    /// [`Self::get_heads`()] as `after` - the index will be used
    ///
    /// If the cursor is no longer needed it can be reset with
    /// [`Self::reset_diff_cursor()`]
    pub fn update_diff_cursor(&mut self) {
        self.ensure_transaction_closed();
        self.patch_log.set_active(true);
        self.patch_log.truncate();
        self.diff_cursor = self.doc.get_heads();
    }

    /// Returns the cursor set by [`Self::update_diff_cursor()`]
    pub fn diff_cursor(&self) -> Vec<ChangeHash> {
        self.diff_cursor.clone()
    }

    /// Generate the patches recorded in `patch_log`
    pub fn make_patches(&self, patch_log: &mut PatchLog) -> Vec<Patch> {
        self.doc.make_patches(patch_log)
    }

    /// Generates a diff from `before` to `after`
    ///
    /// By default the diff requires a sequental scan of all the ops in the doc.
    ///
    /// To do a fast indexed diff `before` must equal [`Self::diff_cursor()`] and
    /// `after` must equal [`Self::get_heads()`]. The diff cursor is managed with
    /// [`Self::update_diff_cursor()`] and [`Self::reset_diff_cursor()`]
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
    ///
    /// Note: `before` and `after` do not have to be chronological.  Document state can move backward.
    /// Normal use might look like:
    ///
    /// # Example
    ///
    /// ```
    /// use automerge::{ AutoCommit };
    ///
    /// let mut doc = AutoCommit::new(); // or AutoCommit::load(data)
    /// // make some changes - use and update the index
    /// let heads = doc.get_heads();
    /// let diff_cursor = doc.diff_cursor();
    /// let patches = doc.diff(&diff_cursor, &heads);
    /// doc.update_diff_cursor();
    /// ```
    ///
    /// See [`Self::diff_incremental()`] for encapsulating this pattern.
    pub fn diff(&mut self, before: &[ChangeHash], after: &[ChangeHash]) -> Vec<Patch> {
        self.ensure_transaction_closed();
        let range = OpRange::new(before, after);
        if let Some((r, patches)) = &self.diff_cache {
            if r == &range {
                // we could skip this clone and return &[Patch]
                return patches.clone();
            }
        }
        let heads = self.doc.get_heads();
        let patches = if range.after() == heads
            && range.before() == self.diff_cursor
            && self.patch_log.is_active()
        {
            self.patch_log.make_patches(&self.doc)
        } else if range.before().is_empty() && range.after() == heads {
            let mut patch_log = PatchLog::active(self.patch_log.text_rep());
            // This if statement is only active if the current heads are the same as `after`
            // so we don't need to tell the patch log to target a specific heads and consequently
            // it wll be able to generate patches very fast as it doesn't need to make any clocks
            patch_log.heads = None;
            current_state::log_current_state_patches(&self.doc, &mut patch_log);
            patch_log.make_patches(&self.doc)
        } else {
            let before_clock = self.doc.clock_at(range.before());
            let after_clock = self.doc.clock_at(range.after());
            let mut patch_log = PatchLog::active(self.patch_log.text_rep());
            patch_log.heads = Some(range.after().to_vec());
            diff::log_diff(&self.doc, &before_clock, &after_clock, &mut patch_log);
            patch_log.make_patches(&self.doc)
        };
        self.diff_cache = Some((range, patches));
        self.diff_cache.as_ref().unwrap().1.clone()
    }

    /// This is a convience function that encapsulates the following common pattern
    /// ```
    /// use automerge::AutoCommit;
    /// let mut doc = AutoCommit::new();
    /// // make some changes
    /// let heads = doc.get_heads();
    /// let diff_cursor = doc.diff_cursor();
    /// let patches = doc.diff(&diff_cursor, &heads);
    /// doc.update_diff_cursor();
    /// ```
    pub fn diff_incremental(&mut self) -> Vec<Patch> {
        self.ensure_transaction_closed();
        let heads = self.doc.get_heads();
        let diff_cursor = self.diff_cursor();
        let patches = self.diff(&diff_cursor, &heads);
        self.update_diff_cursor();
        patches
    }

    pub fn fork(&mut self) -> Self {
        self.ensure_transaction_closed();
        Self {
            doc: self.doc.fork(),
            transaction: self.transaction.clone(),
            patch_log: PatchLog::inactive(self.patch_log.text_rep()),
            diff_cursor: vec![],
            diff_cache: None,
            save_cursor: vec![],
            isolation: None,
        }
    }

    pub fn fork_at(&mut self, heads: &[ChangeHash]) -> Result<Self, AutomergeError> {
        self.ensure_transaction_closed();
        Ok(Self {
            doc: self.doc.fork_at(heads)?,
            transaction: self.transaction.clone(),
            patch_log: PatchLog::inactive(self.patch_log.text_rep()),
            diff_cursor: vec![],
            diff_cache: None,
            save_cursor: vec![],
            isolation: None,
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

    pub fn isolate(&mut self, heads: &[ChangeHash]) {
        self.ensure_transaction_closed();
        self.patch_to(heads);
        self.isolation = Some(heads.to_vec())
    }

    pub fn integrate(&mut self) {
        self.ensure_transaction_closed();
        self.patch_to(self.doc.get_heads().as_slice());
        self.isolation = None;
    }

    fn ensure_transaction_open(&mut self) {
        if self.transaction.is_none() {
            let args = self.doc.transaction_args(self.isolation.as_deref());
            let inner = TransactionInner::new(args);
            self.transaction = Some((self.patch_log.branch(), inner))
        }
    }

    fn ensure_transaction_closed(&mut self) {
        if let Some((patch_log, tx)) = self.transaction.take() {
            self.patch_log.merge(patch_log);
            let hash = tx.commit(&mut self.doc, None, None);
            if self.isolation.is_some() && hash.is_some() {
                self.isolation = hash.map(|h| vec![h])
            }
        }
    }

    /// Load an incremental save of a document.
    ///
    /// Unlike [`Self::load()`] this imports changes into an existing document. It will work with both
    /// the output of [`Self::save()`] and [`Self::save_incremental()`]
    ///
    /// The return value is the number of ops which were applied, this is not useful and will
    /// change in future.
    pub fn load_incremental(&mut self, data: &[u8]) -> Result<usize, AutomergeError> {
        self.ensure_transaction_closed();
        if self.isolation.is_some() {
            self.doc
                .load_incremental_log_patches(data, &mut PatchLog::null())
        } else {
            self.doc
                .load_incremental_log_patches(data, &mut self.patch_log)
        }
    }

    pub fn apply_changes(
        &mut self,
        changes: impl IntoIterator<Item = Change>,
    ) -> Result<(), AutomergeError> {
        self.ensure_transaction_closed();
        if self.isolation.is_some() {
            self.doc
                .apply_changes_log_patches(changes, &mut PatchLog::null())
        } else {
            self.doc
                .apply_changes_log_patches(changes, &mut self.patch_log)
        }
    }

    /// Takes all the changes in `other` which are not in `self` and applies them
    pub fn merge(&mut self, other: &mut AutoCommit) -> Result<Vec<ChangeHash>, AutomergeError> {
        self.ensure_transaction_closed();
        other.ensure_transaction_closed();
        if self.isolation.is_some() {
            self.doc
                .merge_and_log_patches(&mut other.doc, &mut PatchLog::null())
        } else {
            self.doc
                .merge_and_log_patches(&mut other.doc, &mut self.patch_log)
        }
    }

    /// Save the entirety of this document in a compact form.
    pub fn save(&mut self) -> Vec<u8> {
        self.save_with_options(SaveOptions::default())
    }

    pub fn save_with_options(&mut self, options: SaveOptions) -> Vec<u8> {
        self.ensure_transaction_closed();
        let bytes = self.doc.save_with_options(options);
        if !bytes.is_empty() {
            self.save_cursor = self.doc.get_heads()
        }
        bytes
    }

    /// Save the document and attempt to load it before returning - slow!
    pub fn save_and_verify(&mut self) -> Result<Vec<u8>, AutomergeError> {
        let bytes = self.save();
        Self::load(&bytes)?;
        Ok(bytes)
    }

    /// Save this document, but don't run it through DEFLATE afterwards
    pub fn save_nocompress(&mut self) -> Vec<u8> {
        self.save_with_options(SaveOptions {
            deflate: false,
            ..Default::default()
        })
    }

    /// Save the changes since the last call to [`Self::save()`]
    ///
    /// The output of this will not be a compressed document format, but a series of individual
    /// changes. This is useful if you know you have only made a small change since the last [`Self::save()`]
    /// and you want to immediately send it somewhere (e.g. you've inserted a single character in a
    /// text object).
    pub fn save_incremental(&mut self) -> Vec<u8> {
        self.ensure_transaction_closed();
        let bytes = self.doc.save_after(&self.save_cursor);
        if !bytes.is_empty() {
            self.save_cursor = self.doc.get_heads()
        }
        bytes
    }

    /// Save everything which is not a (transitive) dependency of `heads`
    pub fn save_after(&mut self, heads: &[ChangeHash]) -> Vec<u8> {
        self.ensure_transaction_closed();
        self.doc.save_after(heads)
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

    pub fn get_changes(&mut self, have_deps: &[ChangeHash]) -> Vec<&Change> {
        self.ensure_transaction_closed();
        self.doc.get_changes(have_deps)
    }

    pub fn get_change_by_hash(&mut self, hash: &ChangeHash) -> Option<&Change> {
        self.ensure_transaction_closed();
        self.doc.get_change_by_hash(hash)
    }

    /// Get changes in `other` that are not in `self`
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
        if let Some(i) = &self.isolation {
            i.clone()
        } else {
            self.doc.get_heads()
        }
    }

    pub fn set_text_rep(&mut self, text_rep: TextRepresentation) {
        self.patch_log.set_text_rep(text_rep)
    }

    pub fn get_text_rep(&mut self) -> TextRepresentation {
        self.patch_log.text_rep()
    }

    pub fn with_text_rep(mut self, text_rep: TextRepresentation) -> Self {
        self.patch_log.set_text_rep(text_rep);
        self
    }

    /// Commit any uncommitted changes
    ///
    /// Returns [`None`] if there were no operations to commit
    pub fn commit(&mut self) -> Option<ChangeHash> {
        self.commit_with(CommitOptions::default())
    }

    /// Commit the current operations with some options.
    ///
    /// Returns [`None`] if there were no operations to commit
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
        let (patch_log, tx) = self.transaction.take().unwrap();
        self.patch_log.merge(patch_log);
        let hash = tx.commit(&mut self.doc, options.message, options.time);
        if self.isolation.is_some() && hash.is_some() {
            self.isolation = hash.map(|h| vec![h])
        }
        hash
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
    /// operations and a new one with no operations. The returned [`ChangeHash`] will always be the
    /// hash of the empty change.
    pub fn empty_change(&mut self, options: CommitOptions) -> ChangeHash {
        self.ensure_transaction_closed();
        let args = self.doc.transaction_args(None);
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

    /// Get the hash of the change that contains the given `opid`.
    ///
    /// Returns [`None`] if the `opid`:
    /// - Is the root object id
    /// - Does not exist in this document
    /// - Is for an operation in a transaction
    pub fn hash_for_opid(&self, opid: &ExId) -> Option<ChangeHash> {
        self.doc.hash_for_opid(opid)
    }

    fn get_scope(&self, heads: Option<&[ChangeHash]>) -> Option<Clock> {
        // heads arg takes priority
        if let Some(h) = heads {
            return Some(self.doc.clock_at(h));
        }
        match (&self.isolation, &self.transaction) {
            // then look at in progress isolated transaction
            (Some(_), Some((_, t))) => t.get_scope().clone(),
            // then look at clock for isolation
            (Some(i), None) => Some(self.doc.clock_at(i)),
            _ => None,
        }
    }

    fn patch_to(&mut self, after: &[ChangeHash]) {
        // we may be isolated so we dont use self.doc.get_heads()
        let before = self.get_heads();
        if before.as_slice() != after {
            let before_clock = self.doc.clock_at(&before);
            let after_clock = self.doc.clock_at(after);
            diff::log_diff(&self.doc, &before_clock, &after_clock, &mut self.patch_log);
        }
    }

    /// Whether the peer represented by `other` has all the changes we have
    pub fn has_our_changes(&mut self, state: &crate::sync::State) -> bool {
        self.ensure_transaction_closed();
        self.doc.has_our_changes(state)
    }
}

impl ReadDoc for AutoCommit {
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

    fn keys<O: AsRef<ExId>>(&self, obj: O) -> Keys<'_> {
        self.doc.keys_for(obj.as_ref(), self.get_scope(None))
    }

    fn keys_at<O: AsRef<ExId>>(&self, obj: O, heads: &[ChangeHash]) -> Keys<'_> {
        self.doc.keys_for(obj.as_ref(), self.get_scope(Some(heads)))
    }

    fn map_range<'a, O: AsRef<ExId>, R: RangeBounds<String> + 'a>(
        &'a self,
        obj: O,
        range: R,
    ) -> MapRange<'a, R> {
        self.doc
            .map_range_for(obj.as_ref(), range, self.get_scope(None))
    }

    fn map_range_at<'a, O: AsRef<ExId>, R: RangeBounds<String> + 'a>(
        &'a self,
        obj: O,
        range: R,
        heads: &[ChangeHash],
    ) -> MapRange<'a, R> {
        self.doc
            .map_range_for(obj.as_ref(), range, self.get_scope(Some(heads)))
    }

    fn list_range<O: AsRef<ExId>, R: RangeBounds<usize>>(
        &self,
        obj: O,
        range: R,
    ) -> ListRange<'_, R> {
        self.doc
            .list_range_for(obj.as_ref(), range, self.get_scope(None))
    }

    fn list_range_at<O: AsRef<ExId>, R: RangeBounds<usize>>(
        &self,
        obj: O,
        range: R,
        heads: &[ChangeHash],
    ) -> ListRange<'_, R> {
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

    fn marks<O: AsRef<ExId>>(&self, obj: O) -> Result<Vec<Mark<'_>>, AutomergeError> {
        self.doc.marks_for(obj.as_ref(), self.get_scope(None))
    }

    fn marks_at<O: AsRef<ExId>>(
        &self,
        obj: O,
        heads: &[ChangeHash],
    ) -> Result<Vec<Mark<'_>>, AutomergeError> {
        self.doc
            .marks_for(obj.as_ref(), self.get_scope(Some(heads)))
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

    fn get_cursor<O: AsRef<ExId>>(
        &self,
        obj: O,
        position: usize,
        at: Option<&[ChangeHash]>,
    ) -> Result<Cursor, AutomergeError> {
        self.doc
            .get_cursor_for(obj.as_ref(), position, self.get_scope(at))
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

    fn hydrate<O: AsRef<ExId>>(
        &self,
        obj: O,
        heads: Option<&[ChangeHash]>,
    ) -> Result<hydrate::Value, AutomergeError> {
        self.doc.hydrate_obj(obj.as_ref(), heads)
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

    fn get_missing_deps(&self, heads: &[ChangeHash]) -> Vec<ChangeHash> {
        self.doc.get_missing_deps(heads)
    }

    fn get_change_by_hash(&self, hash: &ChangeHash) -> Option<&Change> {
        self.doc.get_change_by_hash(hash)
    }

    fn stats(&self) -> crate::read::Stats {
        self.doc.stats()
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
        let (patch_log, tx) = self.transaction.as_mut().unwrap();
        tx.put(&mut self.doc, patch_log, obj.as_ref(), prop, value)
    }

    fn put_object<O: AsRef<ExId>, P: Into<Prop>>(
        &mut self,
        obj: O,
        prop: P,
        value: ObjType,
    ) -> Result<ExId, AutomergeError> {
        self.ensure_transaction_open();
        let (patch_log, tx) = self.transaction.as_mut().unwrap();
        tx.put_object(&mut self.doc, patch_log, obj.as_ref(), prop, value)
    }

    fn insert<O: AsRef<ExId>, V: Into<ScalarValue>>(
        &mut self,
        obj: O,
        index: usize,
        value: V,
    ) -> Result<(), AutomergeError> {
        self.ensure_transaction_open();
        let (patch_log, tx) = self.transaction.as_mut().unwrap();
        tx.insert(&mut self.doc, patch_log, obj.as_ref(), index, value)
    }

    fn insert_object<O: AsRef<ExId>>(
        &mut self,
        obj: O,
        index: usize,
        value: ObjType,
    ) -> Result<ExId, AutomergeError> {
        self.ensure_transaction_open();
        let (patch_log, tx) = self.transaction.as_mut().unwrap();
        tx.insert_object(&mut self.doc, patch_log, obj.as_ref(), index, value)
    }

    fn increment<O: AsRef<ExId>, P: Into<Prop>>(
        &mut self,
        obj: O,
        prop: P,
        value: i64,
    ) -> Result<(), AutomergeError> {
        self.ensure_transaction_open();
        let (patch_log, tx) = self.transaction.as_mut().unwrap();
        tx.increment(&mut self.doc, patch_log, obj.as_ref(), prop, value)
    }

    fn delete<O: AsRef<ExId>, P: Into<Prop>>(
        &mut self,
        obj: O,
        prop: P,
    ) -> Result<(), AutomergeError> {
        self.ensure_transaction_open();
        let (patch_log, tx) = self.transaction.as_mut().unwrap();
        tx.delete(&mut self.doc, patch_log, obj.as_ref(), prop)
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
        self.ensure_transaction_open();
        let (patch_log, tx) = self.transaction.as_mut().unwrap();
        tx.splice(&mut self.doc, patch_log, obj.as_ref(), pos, del, vals)
    }

    fn splice_text<O: AsRef<ExId>>(
        &mut self,
        obj: O,
        pos: usize,
        del: isize,
        text: &str,
    ) -> Result<(), AutomergeError> {
        self.ensure_transaction_open();
        let (patch_log, tx) = self.transaction.as_mut().unwrap();
        tx.splice_text(&mut self.doc, patch_log, obj.as_ref(), pos, del, text)?;
        Ok(())
    }

    fn mark<O: AsRef<ExId>>(
        &mut self,
        obj: O,
        mark: Mark<'_>,
        expand: ExpandMark,
    ) -> Result<(), AutomergeError> {
        self.ensure_transaction_open();
        let (patch_log, tx) = self.transaction.as_mut().unwrap();
        tx.mark(&mut self.doc, patch_log, obj.as_ref(), mark, expand)
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
        let (patch_log, tx) = self.transaction.as_mut().unwrap();
        tx.unmark(
            &mut self.doc,
            patch_log,
            obj.as_ref(),
            key,
            start,
            end,
            expand,
        )
    }

    fn split_block<'p, O>(&mut self, obj: O, index: usize) -> Result<ExId, AutomergeError>
    where
        O: AsRef<ExId>,
    {
        self.ensure_transaction_open();
        let (patch_log, tx) = self.transaction.as_mut().unwrap();
        tx.split_block(&mut self.doc, patch_log, obj.as_ref(), index)
    }

    fn join_block<O: AsRef<ExId>>(&mut self, text: O, index: usize) -> Result<(), AutomergeError> {
        self.ensure_transaction_open();
        let (patch_log, tx) = self.transaction.as_mut().unwrap();
        tx.join_block(&mut self.doc, patch_log, text.as_ref(), index)
    }

    fn replace_block<'p, O>(&mut self, text: O, index: usize) -> Result<ExId, AutomergeError>
    where
        O: AsRef<ExId>,
    {
        self.ensure_transaction_open();
        let (patch_log, tx) = self.transaction.as_mut().unwrap();
        tx.replace_block(&mut self.doc, patch_log, text.as_ref(), index)
    }

    fn base_heads(&self) -> Vec<ChangeHash> {
        if let Some(i) = &self.isolation {
            i.clone()
        } else {
            self.doc.get_heads()
        }
    }

    fn update_text<S: AsRef<str>>(
        &mut self,
        obj: &ExId,
        new_text: S,
    ) -> Result<(), AutomergeError> {
        self.ensure_transaction_open();
        let (patch_log, tx) = self.transaction.as_mut().unwrap();
        crate::text_diff::myers_diff(&mut self.doc, tx, patch_log, obj, new_text)
    }

    fn update_spans<'a, O: AsRef<ExId>, I: IntoIterator<Item = crate::BlockOrText<'a>>>(
        &mut self,
        text: O,
        new_text: I,
    ) -> Result<(), AutomergeError> {
        self.ensure_transaction_open();
        let (patch_log, tx) = self.transaction.as_mut().unwrap();
        crate::text_diff::myers_block_diff(&mut self.doc, tx, patch_log, text.as_ref(), new_text)
    }

    fn update_object<O: AsRef<ExId>>(
        &mut self,
        obj: O,
        new_value: &crate::hydrate::Value,
    ) -> Result<(), crate::error::UpdateObjectError> {
        self.ensure_transaction_open();
        let (patch_log, tx) = self.transaction.as_mut().unwrap();
        tx.update_object(&mut self.doc, patch_log, obj.as_ref(), new_value)
    }
}

// A wrapper we return from [`AutoCommit::sync()`] to ensure that transactions are closed before we
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
        if self.inner.isolation.is_some() {
            self.inner.doc.receive_sync_message_log_patches(
                sync_state,
                message,
                &mut PatchLog::null(),
            )
        } else {
            self.inner.doc.receive_sync_message_log_patches(
                sync_state,
                message,
                &mut self.inner.patch_log,
            )
        }
    }

    // I dont like this function - it makes sense on automerge but not autocommit
    // FIXME
    fn receive_sync_message_log_patches(
        &mut self,
        sync_state: &mut sync::State,
        message: sync::Message,
        patch_log: &mut PatchLog,
    ) -> Result<(), AutomergeError> {
        self.inner
            .doc
            .receive_sync_message_log_patches(sync_state, message, patch_log)
    }
}

#[derive(Debug, Clone, PartialEq)]
struct OpRange {
    before_len: usize,
    hashes: Vec<ChangeHash>,
}

impl OpRange {
    fn new(before: &[ChangeHash], after: &[ChangeHash]) -> Self {
        let mut hashes = Vec::with_capacity(before.len() + after.len());
        hashes.extend(before);
        hashes.extend(after);
        let range = Self {
            before_len: before.len(),
            hashes,
        };
        assert_eq!(before, range.before());
        assert_eq!(after, range.after());
        range
    }

    fn before(&self) -> &[ChangeHash] {
        &self.hashes[0..self.before_len]
    }

    fn after(&self) -> &[ChangeHash] {
        &self.hashes[self.before_len..]
    }
}

#[cfg(test)]
mod tests {

    fn is_send<S: Send>() {}

    #[test]
    fn test_autocommit_is_send() {
        is_send::<super::AutoCommit>();
    }
}
