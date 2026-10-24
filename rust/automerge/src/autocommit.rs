use std::collections::HashMap;
use std::ops::RangeBounds;

use crate::author::Author;
use crate::automerge::view::ReadAt;
use crate::automerge::view::Viewpoint;
use crate::automerge::LoadOptions;
use crate::automerge::SaveOptions;
use crate::automerge::{Automerge, EnableAuditModeError};
use crate::change_graph::Fragment;
use crate::exid::ExId;
use crate::hash_retention::{Full, HashRetention, Retained};
use crate::op_set2::ChangeMetadata;
use crate::patches::PatchAccumulator;
use crate::tx::{CommitOptions, ScopedRead, TxDoc};
use crate::types::{ObjId, ObjMeta};
use crate::VerificationMode;
use crate::{tx::TransactionInner, ActorId, AutomergeError, Change, ChangeHash, ChangeId};
use crate::{AnonymizeError, Bundle, OnPartialLoad, TextEncoding};
use crate::{ObjType, Patch};
use std::borrow::Cow;

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
/// To synchronise, hand [`Self::document()`] or [`Self::document_mut()`]
/// to a replication protocol such as `automerge-sync`.
///
/// ## Patches, maintaining materialized views
///
/// [`AutoCommit`] allows you to generate [`Patch`]es representing changes to the current state of
/// the document which you can use to maintain a materialized view of the current state. There are
/// several ways to use this. See the documentation on [`Self::diff_incremental()`] for more details, but the key
/// point to remember is that [`AutoCommit`] manages an internal "diff cursor" for you. This is a
/// representation of the heads of the document last time you called [`Self::diff_incremental()`]
/// but you can also manage it directly using [`Self::update_diff_cursor()`] and
/// [`Self::reset_diff_cursor()`].
///
/// ## Authors and Actors
///
/// See the ["Authors and Actors"](`Automerge#authors-and-actors`) docs.
#[derive(Debug, Clone)]
pub struct AutoCommit<H: crate::hash_retention::HashRetention = crate::hash_retention::Retained> {
    pub(crate) doc: Automerge<H>,
    transaction: Option<TransactionInner>,
    diff_cursor: Viewpoint,
    /// keyed on the visibility epoch too: a write-frontier change alters what
    /// the same heads show
    diff_cache: Option<(OpRange, ObjId, bool, u64, Vec<Patch>)>,
    save_cursor: Vec<ChangeId>,
    pub(crate) isolation: Option<Vec<ChangeId>>,
}

impl Default for AutoCommit<Retained> {
    fn default() -> Self {
        AutoCommit::from_doc(Automerge::new().with_manual_gc(), Vec::new())
    }
}

impl<H: HashRetention> AutoCommit<H> {
    pub(crate) fn from_doc(doc: Automerge<H>, save_cursor: Vec<ChangeId>) -> Self {
        AutoCommit {
            doc,
            transaction: None,
            diff_cursor: Viewpoint::default(),
            diff_cache: None,
            save_cursor,
            isolation: None,
        }
    }

    pub(crate) fn load_as(data: &[u8], options: LoadOptions) -> Result<Self, AutomergeError> {
        let options = options.gc_or(crate::automerge::GcMode::Manual);
        let doc = Automerge::<H>::load_with_options_and_mark_validation(
            data,
            options,
            crate::storage::load::MarkOrderValidation::Validate,
        )?;
        let save_cursor = if H::AUDIT == crate::automerge::AuditMode::Enabled {
            Vec::new()
        } else {
            doc.get_heads()
        };
        Ok(Self::from_doc(doc, save_cursor))
    }

    fn map_doc<H2: HashRetention>(
        mut self,
        f: impl FnOnce(Automerge<H>) -> Automerge<H2>,
    ) -> AutoCommit<H2> {
        self.ensure_transaction_closed();
        let AutoCommit {
            doc,
            transaction: _,
            diff_cursor,
            diff_cache,
            save_cursor,
            isolation,
        } = self;
        AutoCommit {
            doc: f(doc),
            transaction: None,
            diff_cursor,
            diff_cache,
            save_cursor,
            isolation,
        }
    }

    pub fn diff_opset(&self, other: &Self) -> Result<(), AutomergeError> {
        self.doc.diff_opset(&other.doc)
    }

    /// Return a copy of this document with its data anonymized.
    pub fn anonymize(&mut self) -> Result<Self, AnonymizeError> {
        self.ensure_transaction_closed();
        Ok(Self::from_doc(
            self.doc.anonymize()?.with_manual_gc(),
            Vec::new(),
        ))
    }

    /// Resets the diff cursor to the empty document: the next
    /// [`Self::diff_incremental()`] returns the whole current state.
    pub fn reset_diff_cursor(&mut self) {
        self.ensure_transaction_closed();
        self.doc.clear_dirty();
        self.diff_cursor = Viewpoint::default();
    }

    /// Sets the diff cursor to the current view, so the next
    /// [`Self::diff_incremental()`] reports only what changes after this call,
    /// including write-frontier changes and entering or leaving isolation.
    pub fn update_diff_cursor(&mut self) {
        self.ensure_transaction_closed();
        self.doc.clear_dirty();
        self.diff_cursor = self.viewpoint();
    }

    /// Returns the cursor set by [`Self::update_diff_cursor()`]
    pub fn diff_cursor(&self) -> Vec<ChangeId> {
        self.diff_cursor.heads().to_vec()
    }

    /// The document's current [`Viewpoint`], at the isolated heads when isolated.
    pub fn viewpoint(&mut self) -> Viewpoint {
        self.ensure_transaction_closed();
        match &self.isolation {
            Some(heads) => self.doc.viewpoint_at(heads.clone()),
            None => self.doc.viewpoint(),
        }
    }

    /// See [`Automerge::mask_author`]. Call [`Self::update_diff_cursor`]
    /// beforehand to get the resulting patches from
    /// [`Self::diff_incremental`].
    pub fn mask_author(&mut self, author: Author<'static>, heads: &[ChangeId]) {
        self.ensure_transaction_closed();
        self.doc.mask_author(author, heads);
    }

    /// See [`Automerge::reveal_author`].
    pub fn reveal_author(&mut self, author: &Author<'static>) {
        self.ensure_transaction_closed();
        self.doc.reveal_author(author);
    }

    /// See [`Automerge::set_write_frontier`].
    pub fn set_write_frontier(&mut self, write_frontier: HashMap<Author<'static>, Vec<ChangeId>>) {
        self.ensure_transaction_closed();
        self.doc.set_write_frontier(write_frontier);
    }

    pub fn get_write_frontier(&self) -> HashMap<Author<'static>, Vec<ChangeId>> {
        self.doc.get_write_frontier()
    }

    /// Whether `author` is masked, e.g. so an application can disable editing
    /// for a masked local author.
    pub fn is_author_masked(&self, author: &Author<'_>) -> bool {
        self.doc.is_author_masked(author)
    }

    /// Generates a diff from `before` to `after`
    ///
    /// Each side is heads, read under the current write-frontier, or a
    /// [`Viewpoint`], read under its own so that masking and revealing authors
    /// is reported even when the heads did not move.
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
    /// * `before` - heads or a [`Viewpoint`] at the beginning point
    /// * `after` - heads or a [`Viewpoint`] at the ending point
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
    ///
    /// Returns [`AutomergeError::InvalidChangeId`] if `before` or `after` names a change this
    /// document does not have.
    pub fn diff(
        &mut self,
        before: impl Into<Viewpoint>,
        after: impl Into<Viewpoint>,
    ) -> Result<Vec<Patch>, AutomergeError> {
        self.diff_inner(ObjMeta::root(), before.into(), after.into(), true)
    }

    fn diff_inner(
        &mut self,
        obj: ObjMeta,
        before: Viewpoint,
        after: Viewpoint,
        recursive: bool,
    ) -> Result<Vec<Patch>, AutomergeError> {
        self.ensure_transaction_closed();
        let heads_only = before.write_frontier.is_none() && after.write_frontier.is_none();
        let range = OpRange::new(before.heads(), after.heads());
        let epoch = self.doc.visibility_epoch();
        if let Some((r, id, rec, e, patches)) = &self.diff_cache {
            if heads_only && r == &range && id == &obj.id && *rec == recursive && *e == epoch {
                // we could skip this clone and return &[Patch]
                return Ok(patches.clone());
            }
        }
        // the dirty bits were cleared at the cursor; it and `before` must
        // both have seen today's mask
        let before_sees_today = before.write_frontier.is_none() || before.epoch == Some(epoch);
        let patches = if self.doc.is_current(&after)
            && obj.id.is_root()
            && recursive
            && before.heads() == self.diff_cursor.heads()
            && self.diff_cursor.epoch == Some(epoch)
            && before_sees_today
        {
            self.doc
                .dirty_diff_patches(&self.diff_cursor)
                .expect("dirty diff should support AutoCommit cursor intervals")
        } else if range.before().is_empty() && self.doc.is_current(&after) {
            let mut patch_accumulator = PatchAccumulator::event_log();
            // `after` is current, so patches need no heads clock, which is much faster
            patch_accumulator.heads_clock = None;
            self.doc
                .log_current_state(obj, &mut patch_accumulator, recursive);
            patch_accumulator.make_patches(&self.doc)
        } else {
            self.doc.diff_obj_inner(obj, &before, &after, recursive)?
        };
        if heads_only {
            self.diff_cache = Some((range, obj.id, recursive, epoch, patches.clone()));
        }
        Ok(patches)
    }

    /// [`Self::diff()`] starting at `obj`; with `recursive` false, child
    /// objects are not diffed.
    pub fn diff_obj(
        &mut self,
        obj: &ExId,
        before: impl Into<Viewpoint>,
        after: impl Into<Viewpoint>,
        recursive: bool,
    ) -> Result<Vec<Patch>, AutomergeError> {
        let meta = self.doc.exid_to_obj(obj)?;
        self.diff_inner(meta, before.into(), after.into(), recursive)
    }

    /// Returns patches updating the view observed at the diff cursor to the
    /// current view, then advances the cursor and begins tracking again.
    ///
    /// Write-frontier changes are reported even when the heads did not move:
    /// unlike [`Self::diff()`], the before side is read under the
    /// write-frontier the cursor was taken with.
    ///
    /// Before the cursor is set, or after [`Self::reset_diff_cursor()`], this
    /// returns the whole current state. Call
    /// [`Self::update_diff_cursor()`] to start tracking from an existing view
    /// without returning its patches.
    pub fn diff_incremental(&mut self) -> Vec<Patch> {
        let after = self.viewpoint();
        let before = std::mem::take(&mut self.diff_cursor);
        let patches = if after.heads() != self.doc.get_heads() {
            // the dirty diff only covers intervals ending at the document's heads
            self.doc
                .diff(&before, &after)
                .expect("the diff cursor and heads are this document's own change ids")
        } else {
            self.doc
                .dirty_diff_patches_and_clear(&before)
                .expect("dirty diff should support AutoCommit incremental intervals")
        };
        self.diff_cursor = after;
        patches
    }

    pub fn fork(&mut self) -> Self {
        self.ensure_transaction_closed();
        Self {
            doc: self.doc.fork(),
            transaction: self.transaction.clone(),
            diff_cursor: Viewpoint::default(),
            diff_cache: None,
            save_cursor: vec![],
            isolation: None,
        }
    }

    pub fn fork_at(&mut self, heads: &[ChangeId]) -> Result<Self, AutomergeError> {
        self.ensure_transaction_closed();
        Ok(Self {
            doc: self.doc.fork_at(heads)?,
            transaction: self.transaction.clone(),
            diff_cursor: Viewpoint::default(),
            diff_cache: None,
            save_cursor: vec![],
            isolation: None,
        })
    }

    /// Get the inner document.
    #[doc(hidden)]
    pub fn document(&mut self) -> &Automerge<H> {
        self.ensure_transaction_closed();
        &self.doc
    }

    /// Get the inner document mutably, committing any open transaction first.
    #[doc(hidden)]
    pub fn document_mut(&mut self) -> &mut Automerge<H> {
        self.ensure_transaction_closed();
        &mut self.doc
    }

    /// Set the actor id for this document.
    pub fn with_actor(mut self, actor: ActorId) -> Self {
        self.ensure_transaction_closed();
        self.doc.set_actor(actor);
        self
    }

    /// Set the actor id for this document.
    pub fn set_actor(&mut self, actor: ActorId) -> &mut Self {
        self.ensure_transaction_closed();
        self.doc.set_actor(actor);
        self
    }

    pub fn with_author(mut self, author: Option<Author<'static>>) -> Self {
        self.ensure_transaction_closed();
        self.doc.set_author(author);
        self
    }

    pub fn set_author(&mut self, author: Option<Author<'static>>) -> &mut Self {
        self.ensure_transaction_closed();
        self.doc.set_author(author);
        self
    }

    pub fn get_actor(&self) -> &ActorId {
        self.doc.get_actor()
    }

    pub fn get_actors_for_author(&self, author: &Author<'_>) -> Vec<ActorId> {
        self.doc.get_actors_for_author(author)
    }

    pub fn get_author_for_actor(&self, actor: &ActorId) -> Option<Author<'_>> {
        self.doc.get_author_for_actor(actor)
    }

    pub fn get_author(&self) -> Option<&Author<'static>> {
        self.doc.get_author()
    }

    pub fn get_authors(&self) -> &[Author<'static>] {
        self.doc.get_authors()
    }

    pub fn isolate(&mut self, heads: &[ChangeId]) -> Result<(), AutomergeError> {
        self.ensure_transaction_closed();
        // changes committed in isolation record these heads' hashes as deps
        self.doc.change_ids_to_hashes(heads)?;
        self.patch_to(heads);
        self.isolation = Some(heads.to_vec());
        Ok(())
    }

    pub fn integrate(&mut self) {
        self.ensure_transaction_closed();
        self.patch_to(&self.doc.get_heads());
        self.isolation = None;
    }

    pub(crate) fn ensure_transaction_open(&mut self) {
        if self.transaction.is_none() {
            let args = self.doc.transaction_args(self.isolation.as_deref());
            self.transaction = Some(TransactionInner::new(args));
        }
    }

    pub(crate) fn ensure_transaction_closed(&mut self) {
        if let Some(tx) = self.transaction.take() {
            let hash = tx.commit(&mut self.doc, None, None);
            if self.isolation.is_some() {
                if let Some(h) = hash {
                    let id = self
                        .doc
                        .hash_to_change_id(&h)
                        .expect("just-committed hashes are always known")
                        .expect("the just-committed change is in the document");
                    self.isolation = Some(vec![id]);
                }
            }
        }
    }

    /// Load an incremental save of a document.
    ///
    /// Unlike [`Self::load()`] this imports changes into an existing document. It will work with both
    /// the output of [`Self::save()`] and [`Self::save_incremental()`]
    ///
    /// See [`Automerge::load_incremental`] for what happens to chunks whose dependencies are
    /// missing.
    ///
    /// The return value is the number of ops which were applied, this is not useful and will
    /// change in future.
    pub fn load_incremental(&mut self, data: &[u8]) -> Result<usize, AutomergeError> {
        self.ensure_transaction_closed();
        self.doc.load_incremental(data)
    }

    /// See [`Automerge::apply_changes`].
    pub fn apply_changes(
        &mut self,
        changes: impl IntoIterator<Item = Change> + Clone,
    ) -> Result<(), AutomergeError> {
        self.ensure_transaction_closed();
        self.doc.apply_changes(changes)
    }

    pub fn apply_changes_batch(
        &mut self,
        changes: impl IntoIterator<Item = Change> + Clone,
    ) -> Result<(), AutomergeError> {
        self.ensure_transaction_closed();
        self.doc.apply_changes_batch(changes)
    }

    /// Takes all the changes in `other` which are not in `self` and applies them
    pub fn merge<H2: HashRetention>(
        &mut self,
        other: &mut AutoCommit<H2>,
    ) -> Result<Vec<ChangeId>, AutomergeError> {
        self.ensure_transaction_closed();
        other.ensure_transaction_closed();
        self.doc.merge(&mut other.doc)
    }

    /// Save the entirety of this document in a compact form.
    pub fn save(&mut self) -> Vec<u8> {
        self.save_with_options(SaveOptions::default())
    }

    /// [`Self::save`] with [`SaveOptions`].
    pub fn save_with_options(&mut self, options: SaveOptions) -> Vec<u8> {
        self.ensure_transaction_closed();
        if options.format == crate::automerge::SaveFormat::Legacy {
            self.doc.remove_unused_actors(false);
        }
        let bytes = self.doc.save_with_options(options);
        if !bytes.is_empty() {
            self.save_cursor = self.doc.get_heads()
        }
        self.collect_garbage();
        bytes
    }

    /// Save the document and attempt to load it before returning - slow!
    pub fn save_and_verify(&mut self) -> Result<Vec<u8>, AutomergeError> {
        let bytes = self.save();
        Self::load_as(&bytes, LoadOptions::new())?;
        Ok(bytes)
    }

    #[cfg(test)]
    pub fn debug_cmp(&self, other: &Self) {
        self.doc.debug_cmp(&other.doc);
    }

    #[cfg(test)]
    pub(crate) fn validate_top_index(&self) -> bool {
        self.doc.ops.validate_top_index()
    }

    /// Save this document, but don't run it through DEFLATE afterwards
    pub fn save_nocompress(&mut self) -> Vec<u8> {
        self.save_with_options(SaveOptions {
            deflate: false,
            ..Default::default()
        })
    }

    /// The changes since the last save; empty when there are none.
    pub fn save_incremental(&mut self) -> Vec<u8> {
        self.save_incremental_with_options(SaveOptions::default())
    }

    /// [`Self::save_incremental`] with [`SaveOptions`].
    pub fn save_incremental_with_options(&mut self, options: SaveOptions) -> Vec<u8> {
        self.ensure_transaction_closed();
        let bytes = self
            .doc
            .save_after_with_options(&self.save_cursor, options)
            .expect("the save cursor names changes this document has");
        if !bytes.is_empty() {
            self.save_cursor = self.doc.get_heads()
        }
        self.collect_garbage();
        bytes
    }

    fn collect_garbage(&mut self) {
        // changes committed in isolation name the isolation heads' hashes as deps
        if self.isolation.is_none() {
            self.doc.gc();
        }
    }

    pub fn is_empty(&self) -> bool {
        self.doc.is_empty()
    }

    /// Everything which is not a (transitive) dependency of `heads`
    pub fn save_after(&mut self, heads: &[ChangeId]) -> Result<Vec<u8>, AutomergeError> {
        self.save_after_with_options(heads, SaveOptions::default())
    }

    /// [`Self::save_after`] with [`SaveOptions`].
    pub fn save_after_with_options(
        &mut self,
        heads: &[ChangeId],
        options: SaveOptions,
    ) -> Result<Vec<u8>, AutomergeError> {
        self.ensure_transaction_closed();
        self.doc.save_after_with_options(heads, options)
    }

    pub fn get_missing_deps(
        &mut self,
        heads: &[ChangeId],
    ) -> Result<Vec<ChangeHash>, AutomergeError> {
        self.ensure_transaction_closed();
        self.doc.get_missing_deps(heads)
    }

    /// The last change made by this document's actor, as a change chunk
    pub fn get_last_local_change_legacy(&mut self) -> Result<Option<Change>, AutomergeError> {
        self.ensure_transaction_closed();
        self.doc.get_last_local_change_legacy()
    }

    /// The last change made by this document's actor, as a bundle
    pub fn get_last_local_change(&mut self) -> Result<Option<Bundle>, AutomergeError> {
        self.ensure_transaction_closed();
        self.doc.get_last_local_change()
    }

    pub fn get_changes(&mut self, have_deps: &[ChangeId]) -> Result<Vec<Change>, AutomergeError> {
        self.ensure_transaction_closed();
        self.doc.get_changes(have_deps)
    }

    pub fn get_changes_meta(
        &mut self,
        have_deps: &[ChangeId],
    ) -> Result<Vec<ChangeMetadata<'_>>, AutomergeError> {
        self.ensure_transaction_closed();
        self.doc.get_changes_meta(have_deps)
    }

    pub fn get_change_by_hash(
        &mut self,
        hash: &ChangeHash,
    ) -> Result<Option<Change>, AutomergeError> {
        self.ensure_transaction_closed();
        self.doc.get_change_by_hash(hash)
    }

    /// See [`Automerge::get_change_by_id`].
    pub fn get_change_by_id(&mut self, id: &ChangeId) -> Result<Option<Change>, AutomergeError> {
        self.ensure_transaction_closed();
        self.doc.get_change_by_id(id)
    }

    pub fn get_change_meta_by_hash(
        &mut self,
        hash: &ChangeHash,
    ) -> Result<Option<ChangeMetadata<'_>>, AutomergeError> {
        self.ensure_transaction_closed();
        self.doc.get_change_meta_by_hash(hash)
    }

    /// [`Self::get_changes_added`] as changes rather than a bundle.
    pub fn get_changes_added_legacy<H2: HashRetention>(
        &mut self,
        other: &mut AutoCommit<H2>,
    ) -> Result<Vec<Change>, AutomergeError> {
        self.ensure_transaction_closed();
        other.ensure_transaction_closed();
        self.doc.get_changes_added_legacy(&other.doc)
    }

    pub fn get_changes_added<H2: HashRetention>(
        &mut self,
        other: &mut AutoCommit<H2>,
    ) -> Result<Option<Bundle>, AutomergeError> {
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

    /// See [`Automerge::audit_mode`]
    #[doc(hidden)]
    pub fn audit_mode(&self) -> crate::automerge::AuditMode {
        self.doc.audit_mode()
    }

    pub fn fragments<R: RangeBounds<usize>>(&self, levels: R) -> Vec<Fragment> {
        self.doc.fragments(levels)
    }

    pub fn get_fragment(&self, head: ChangeHash) -> Option<Fragment> {
        self.doc.get_fragment(head)
    }

    /// See [`Automerge::fragments_after`].
    pub fn fragments_after(&self, heads: &[ChangeId]) -> Result<Vec<Fragment>, AutomergeError> {
        self.doc.fragments_after(heads)
    }

    /// See [`Automerge::bundle_after`].
    pub fn bundle_after(&self, heads: &[ChangeId]) -> Result<Vec<Vec<u8>>, AutomergeError> {
        self.doc.bundle_after(heads)
    }

    pub fn bundles_for_fragments<I: IntoIterator<Item = Fragment>>(
        &self,
        fragments: I,
    ) -> Result<Vec<Vec<u8>>, AutomergeError> {
        self.doc.bundles_for_fragments(fragments)
    }

    /// Get the current heads of the document.
    ///
    /// This closes the transaction first, if one is in progress.
    pub fn get_heads(&mut self) -> Vec<ChangeId> {
        self.ensure_transaction_closed();
        if let Some(i) = &self.isolation {
            i.clone()
        } else {
            self.doc.get_heads()
        }
    }

    /// The heads of this document as [`ChangeHash`]es, committing any open
    /// transaction first.
    pub fn get_head_hashes(&mut self) -> Vec<ChangeHash> {
        self.ensure_transaction_closed();
        self.doc.get_head_hashes()
    }

    /// Whether this document contains the change identified by `id`.
    pub fn has_change_id(&self, id: &ChangeId) -> bool {
        self.doc.has_change_id(id)
    }

    /// See [`Automerge::change_id_to_hash`]
    pub fn change_id_to_hash(&self, id: &ChangeId) -> Result<Option<ChangeHash>, AutomergeError> {
        self.doc.change_id_to_hash(id)
    }

    /// See [`Automerge::hash_to_change_id`]
    pub fn hash_to_change_id(&self, hash: &ChangeHash) -> Result<Option<ChangeId>, AutomergeError> {
        self.doc.hash_to_change_id(hash)
    }

    /// See [`Automerge::hashes_to_change_ids`]
    pub fn hashes_to_change_ids(
        &self,
        hashes: &[ChangeHash],
    ) -> Result<Vec<ChangeId>, AutomergeError> {
        self.doc.hashes_to_change_ids(hashes)
    }

    /// See [`Automerge::change_ids_to_hashes`]
    pub fn change_ids_to_hashes(
        &self,
        ids: &[ChangeId],
    ) -> Result<Vec<ChangeHash>, AutomergeError> {
        self.doc.change_ids_to_hashes(ids)
    }

    /// Commit any uncommitted changes
    ///
    /// Returns [`None`] if there were no operations to commit
    pub fn commit(&mut self) -> Option<ChangeId> {
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
    pub fn commit_with(&mut self, options: CommitOptions) -> Option<ChangeId> {
        // ensure that even no changes triggers a change
        self.ensure_transaction_open();
        let tx = self.transaction.take().unwrap();
        let hash = tx.commit(&mut self.doc, options.message, options.time);
        let id = hash.map(|h| {
            self.doc
                .hash_to_change_id(&h)
                .expect("hash of a newly committed change is always known")
                .expect("newly committed change must be in the document")
        });
        if self.isolation.is_some() && id.is_some() {
            self.isolation = id.clone().map(|i| vec![i])
        }
        id
    }

    /// Remove any changes that have been made in the current transaction from the document
    pub fn rollback(&mut self) -> usize {
        self.transaction
            .take()
            .map(|tx| tx.rollback(&mut self.doc))
            .unwrap_or(0)
    }

    /// Generate an empty change
    ///
    /// The main reason to do this is if you wish to create a "merge commit" which has all the
    /// current heads of the documents as dependencies but you have no new operations to create.
    ///
    /// Because this structure is an "autocommit" there may actually be outstanding operations to
    /// submit. If this is the case this function will create two changes, one with the outstanding
    /// operations and a new one with no operations. The returned id is always that of the
    /// empty change.
    pub fn empty_change(&mut self, options: CommitOptions) -> ChangeId {
        self.ensure_transaction_closed();
        let args = self.doc.transaction_args(None);
        let hash = TransactionInner::empty(&mut self.doc, args, options.message, options.time);
        self.doc
            .hash_to_change_id(&hash)
            .expect("hash of a newly created change is always known")
            .expect("newly created change must be in the document")
    }

    /// See [`Automerge::hash_for_opid`]
    ///
    /// Also returns `Ok(None)` for operations in the open transaction.
    pub fn hash_for_opid(&self, opid: &ExId) -> Result<Option<ChangeHash>, AutomergeError> {
        self.doc.hash_for_opid(opid)
    }

    fn patch_to(&mut self, after: &[ChangeId]) {
        let before = self.get_heads();
        if before.as_slice() != after {
            // a view transition can touch any object
            self.doc.ops_mut().mark_all_dirty();
        }
    }
}

impl AutoCommit<Retained> {
    pub fn new() -> Self {
        AutoCommit::default()
    }

    pub fn new_with_encoding(encoding: TextEncoding) -> Self {
        Self::from_doc(
            Automerge::new_with_encoding(encoding).with_manual_gc(),
            Vec::new(),
        )
    }

    pub fn load(data: &[u8]) -> Result<Self, AutomergeError> {
        Self::load_with_options(data, LoadOptions::new())
    }

    pub fn load_unverified_heads(data: &[u8]) -> Result<Self, AutomergeError> {
        Self::load_with_options(
            data,
            LoadOptions::new().verification_mode(VerificationMode::DontCheck),
        )
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

    pub fn load_with_options(data: &[u8], options: LoadOptions) -> Result<Self, AutomergeError> {
        Self::load_as(data, options)
    }

    /// See [`Automerge::enable_audit_mode`].
    #[doc(hidden)]
    pub fn enable_audit_mode(mut self) -> Result<AutoCommit<Full>, EnableAuditModeError<Self>> {
        self.ensure_transaction_closed();
        match self.doc.verified_hashes() {
            Ok(hashes) => Ok(self.map_doc(|doc| doc.into_full_with(hashes))),
            Err(error) => Err(EnableAuditModeError {
                error,
                doc: Box::new(self),
            }),
        }
    }
}

impl AutoCommit<Full> {
    /// See [`Automerge::disable_audit_mode`].
    #[doc(hidden)]
    pub fn disable_audit_mode(self) -> AutoCommit<Retained> {
        self.map_doc(Automerge::disable_audit_mode)
    }
}

impl<H: HashRetention> ScopedRead for AutoCommit<H> {
    type Retention = H;

    fn doc(&self) -> &Automerge<H> {
        &self.doc
    }

    fn get_scope(&self, heads: Option<&[ChangeId]>) -> Result<ReadAt<'_>, AutomergeError> {
        if let Some(h) = heads {
            // pending ops are in the op set but not under the graph's heads, so
            // with a transaction in flight current heads must not read unscoped
            return if self.transaction.is_none() {
                self.doc.read_at(Some(h))
            } else {
                Ok(self.doc.read_scoped(Cow::Owned(self.doc.visible(h)?)))
            };
        }
        Ok(match (&self.isolation, &self.transaction) {
            (Some(_), Some(t)) => match t.get_scope() {
                Some(scope) => self.doc.read_scoped(Cow::Borrowed(scope)),
                None => self.doc.read_current(),
            },
            (Some(i), None) => self
                .doc
                .read_at(Some(i))
                .expect("isolation ids are always known"),
            _ => self.doc.read_current(),
        })
    }
}

impl<H: HashRetention> TxDoc for AutoCommit<H> {
    fn inner(&self) -> Option<&TransactionInner> {
        self.transaction.as_ref()
    }

    fn parts_mut(&mut self) -> (&mut TransactionInner, &mut Automerge<H>) {
        self.ensure_transaction_open();
        (self.transaction.as_mut().unwrap(), &mut self.doc)
    }

    fn tx_base_heads(&self) -> Vec<ChangeHash> {
        if let Some(i) = &self.isolation {
            self.doc
                .change_ids_to_hashes(i)
                .expect("isolation ids were validated when isolating")
        } else {
            self.doc.get_head_hashes()
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
struct OpRange {
    before_len: usize,
    ids: Vec<ChangeId>,
}

impl OpRange {
    fn new(before: &[ChangeId], after: &[ChangeId]) -> Self {
        let mut ids = Vec::with_capacity(before.len() + after.len());
        ids.extend_from_slice(before);
        ids.extend_from_slice(after);
        let range = Self {
            before_len: before.len(),
            ids,
        };
        assert_eq!(before, range.before());
        assert_eq!(after, range.after());
        range
    }

    fn before(&self) -> &[ChangeId] {
        &self.ids[0..self.before_len]
    }

    fn after(&self) -> &[ChangeId] {
        &self.ids[self.before_len..]
    }
}

#[cfg(test)]
mod tests {
    use crate::{tx::Transactable, ObjType, ROOT};

    use super::AutoCommit;

    fn is_send<S: Send>() {}

    fn assert_incremental_matches_diff<H: crate::hash_retention::HashRetention>(
        doc: &mut AutoCommit<H>,
    ) {
        let before = doc.diff_cursor();
        let after = doc.get_heads();
        let expected = doc.diff(&before, &after).unwrap();
        let actual = doc.diff_incremental();
        crate::patches::effect::assert_patches_have_same_effect(
            doc.document(),
            &before,
            &after,
            "incremental diff",
            &actual,
            "cursor diff",
            &expected,
        );
        assert!(doc.diff_incremental().is_empty());
    }

    #[test]
    fn test_autocommit_is_send() {
        is_send::<AutoCommit>();
    }

    #[test]
    fn diff_incremental_public_style_local_mutations() {
        let mut doc = AutoCommit::new();
        doc.put(ROOT, "map", 1).unwrap();
        let list = doc.put_object(ROOT, "list", ObjType::List).unwrap();
        doc.insert(&list, 0, "a").unwrap();
        let text = doc.put_object(ROOT, "text", ObjType::Text).unwrap();
        doc.splice_text(&text, 0, 0, "abc").unwrap();
        assert_incremental_matches_diff(&mut doc);

        doc.put(ROOT, "map", 2).unwrap();
        doc.put(&list, 0, "A").unwrap();
        doc.splice_text(&text, 1, 1, "B").unwrap();
        assert_incremental_matches_diff(&mut doc);
    }

    #[test]
    fn diff_with_cursor_uses_dirty_diff_without_consuming_dirty_bits() {
        let mut doc = AutoCommit::new();
        doc.put(ROOT, "base", 1).unwrap();
        let list = doc.put_object(ROOT, "list", ObjType::List).unwrap();
        doc.insert(&list, 0, "a").unwrap();
        doc.update_diff_cursor();
        let before = doc.diff_cursor();

        doc.put(ROOT, "base", 2).unwrap();
        doc.put(&list, 0, "A").unwrap();
        let after = doc.get_heads();

        let patches = doc.diff(&before, &after).unwrap();
        assert!(!patches.is_empty());
        assert!(doc.document().ops().dirty_runs().next().is_some());
        let incremental = doc.diff_incremental();
        crate::patches::effect::assert_patches_have_same_effect(
            doc.document(),
            &before,
            &after,
            "incremental diff",
            &incremental,
            "cursor diff",
            &patches,
        );
        assert!(doc.document().ops().dirty_runs().next().is_none());
    }

    #[test]
    fn diff_incremental_public_style_object_creation_ordering_fallback() {
        let mut doc = AutoCommit::new();
        doc.put(ROOT, "base", 1).unwrap();
        doc.update_diff_cursor();

        let list = doc.put_object(ROOT, "list", ObjType::List).unwrap();
        doc.insert(&list, 0, "a").unwrap();
        doc.put(ROOT, "base", 2).unwrap();
        assert_incremental_matches_diff(&mut doc);
    }

    #[test]
    fn diff_incremental_public_style_apply_changes() {
        let mut source = AutoCommit::new();
        source.put(ROOT, "key", 1).unwrap();
        let changes = source.get_changes(&[]).unwrap();

        let mut doc = AutoCommit::new();
        doc.apply_changes(changes).unwrap();
        assert_incremental_matches_diff(&mut doc);
    }

    #[test]
    fn diff_incremental_public_style_apply_changes_batch() {
        let mut source = AutoCommit::new();
        source.put(ROOT, "key", 1).unwrap();
        let list = source.put_object(ROOT, "list", ObjType::List).unwrap();
        source.insert(&list, 0, "a").unwrap();
        let changes = source.get_changes(&[]).unwrap();

        let mut doc = AutoCommit::new();
        doc.apply_changes_batch(changes).unwrap();
        assert_incremental_matches_diff(&mut doc);
    }

    #[test]
    fn diff_incremental_public_style_load_incremental() {
        let mut source = AutoCommit::new();
        source.put(ROOT, "base", 1).unwrap();
        let base_heads = source.get_heads();
        let base_data = source.save();

        let mut doc = AutoCommit::new();
        doc.load_incremental(&base_data).unwrap();
        assert_incremental_matches_diff(&mut doc);

        source.put(ROOT, "later", 2).unwrap();
        let data = source.save_after(&base_heads).unwrap();
        doc.load_incremental(&data).unwrap();
        assert_incremental_matches_diff(&mut doc);
    }

    #[test]
    fn diff_incremental_public_style_merge() {
        let mut source = AutoCommit::new();
        source.put(ROOT, "key", 1).unwrap();

        let mut doc = AutoCommit::new();
        doc.merge(&mut source).unwrap();
        assert_incremental_matches_diff(&mut doc);
    }

    #[test]
    fn diff_incremental_public_style_sync_receive() {
        let mut source = AutoCommit::new().enable_audit_mode().unwrap();
        source.put(ROOT, "key", 1).unwrap();

        // what receiving a v2 sync message does
        let mut doc = AutoCommit::new().enable_audit_mode().unwrap();
        doc.load_incremental(&source.save()).unwrap();
        assert_incremental_matches_diff(&mut doc);
    }

    #[test]
    fn diff_incremental_public_style_reset_and_rollback() {
        let mut doc = AutoCommit::new();
        doc.put(ROOT, "key", 1).unwrap();
        assert_incremental_matches_diff(&mut doc);

        let heads = doc.get_heads();
        let expected = doc.diff(&[], &heads).unwrap();
        doc.reset_diff_cursor();
        let incremental = doc.diff_incremental();
        crate::patches::effect::assert_patches_have_same_effect(
            doc.document(),
            &[],
            &heads,
            "incremental diff",
            &incremental,
            "full diff",
            &expected,
        );
        assert!(doc.diff_incremental().is_empty());

        doc.put(ROOT, "key", 2).unwrap();
        assert_eq!(doc.rollback(), 1);
        assert!(doc.diff_incremental().is_empty());

        doc.put(ROOT, "key", 3).unwrap();
        assert_incremental_matches_diff(&mut doc);
    }

    #[test]
    fn diff_incremental_public_style_isolate_and_integrate() {
        let mut doc = AutoCommit::new();
        let beginning = doc.get_heads();

        let name = doc.put_object(ROOT, "name", ObjType::Text).unwrap();
        doc.splice_text(&name, 0, 0, "name").unwrap();

        doc.isolate(&beginning).unwrap();
        let color = doc.put_object(ROOT, "color", ObjType::Text).unwrap();
        doc.splice_text(&color, 0, 0, "red").unwrap();
        doc.integrate();
        assert_incremental_matches_diff(&mut doc);

        doc.commit();
        doc.isolate(&beginning).unwrap();
        let color = doc.put_object(ROOT, "color", ObjType::Text).unwrap();
        doc.splice_text(&color, 0, 0, "unset").unwrap();
        doc.commit();
        doc.integrate();
        assert_incremental_matches_diff(&mut doc);
    }
}
