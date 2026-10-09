use std::ops::RangeBounds;

use crate::automerge::EnableAuditModeError;
use crate::exid::ExId;
use crate::hash_retention::{Full, Retained};
use crate::op_set2::ChangeMetadata;
use crate::patches::Patch;
use crate::storage::VerificationMode;
use crate::tx::CommitOptions;
use crate::{
    ActorId, AnonymizeError, Author, AutomergeError, Change, ChangeHash, ChangeId, ObjType,
};
use crate::{OnPartialLoad, TextEncoding};

use super::automerge::{Automerge, Fragment, LoadOptions, SaveOptions};

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
/// [`Self`] implements [`ReadDoc`](crate::ReadDoc), which provides methods for reading values from the document.
///
/// ## Modifying a document
///
/// This type implements [`Transactable`](crate::transaction::Transactable) directly, so you can modify it using methods from [`Transactable`](crate::transaction::Transactable).
///
/// ## Synchronization
///
/// To synchronise use the `automerge-sync` crate, whose `AutoCommitSync` trait provides `sync()`.
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
///
/// ## Authors and Actors
///
/// See the ["Authors and Actors"](`Automerge#authors-and-actors`) docs.
#[derive(Debug, Clone)]
pub struct AutoCommit(
    pub(crate) crate::autocommit::AutoCommit<Full>,
    /// reported by `get_heads` until a commit moves the isolation on
    Option<IsolatedAt>,
);

#[derive(Debug, Clone)]
struct IsolatedAt {
    given: Vec<ChangeHash>,
    resolved: Vec<ChangeId>,
}

impl AutoCommit {
    fn wrap(doc: crate::autocommit::AutoCommit<Full>) -> Self {
        AutoCommit(doc, None)
    }
}

impl Default for AutoCommit {
    fn default() -> Self {
        Self::new()
    }
}

impl AutoCommit {
    pub fn new() -> AutoCommit {
        Self::new_with_encoding(TextEncoding::platform_default())
    }

    pub fn diff_opset(&self, other: &AutoCommit) -> Result<(), AutomergeError> {
        self.0.diff_opset(&other.0)
    }

    pub fn new_with_encoding(encoding: TextEncoding) -> AutoCommit {
        Self::wrap(crate::autocommit::AutoCommit::<Full>::from_doc(
            crate::automerge::Automerge::<Full>::empty(encoding).with_manual_gc(),
            Vec::new(),
        ))
    }

    /// Return a copy of this document with its data anonymized.
    pub fn anonymize(&mut self) -> Result<Self, AnonymizeError> {
        Ok(Self::wrap(self.0.anonymize()?))
    }

    pub fn load(data: &[u8]) -> Result<Self, AutomergeError> {
        Self::load_with_options(data, LoadOptions::default())
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
        Ok(Self::wrap(crate::autocommit::AutoCommit::<Full>::load_as(
            data,
            options.into_next(),
        )?))
    }

    /// Erases the diff cursor created by [`Self::update_diff_cursor()`] and no
    /// longer indexes changes to the document.
    pub fn reset_diff_cursor(&mut self) {
        self.0.reset_diff_cursor()
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
        self.0.update_diff_cursor()
    }

    /// Returns the cursor set by [`Self::update_diff_cursor()`]
    pub fn diff_cursor(&self) -> Vec<ChangeHash> {
        let ids = self.0.diff_cursor();
        super::hashes_of(&self.0.doc, &ids)
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
        let before = super::known_ids(&self.0.doc, before);
        let after = super::known_ids(&self.0.doc, after);
        self.0.diff(&before, &after).unwrap_or_default()
    }

    /// Generates a diff from `before` to `after` for a given `object`
    ///
    /// By default the diff requires a sequental scan of all the ops in the doc.
    ///
    /// [Self::diff()] is the equivelent to [Self::diff_obj(&ROOT, before, after)]
    ///
    /// Managing the diff index has a small but non-zero overhead.  It should be
    /// disabled if no longer needed.  If a signifigantly large change is applied
    /// to the document it may be faster to reset the index before applying it,
    /// doing an unindxed diff afterwards and then reenable the index.
    ///
    /// # Arguments
    ///
    /// * `obj` - The object to start the diff at.
    /// * `before` - heads from [`Self::get_heads()`] at beginning point in the documents history
    /// * `after` - heads from [`Self::get_heads()`] at ending point in the documents history.
    /// * `recursive` - if false, do not also diff child objects
    ///
    /// Note: `before` and `after` do not have to be chronological.  Document state can move backward.
    pub fn diff_obj(
        &mut self,
        obj: &ExId,
        before: &[ChangeHash],
        after: &[ChangeHash],
        recursive: bool,
    ) -> Result<Vec<Patch>, AutomergeError> {
        let before = super::known_ids(&self.0.doc, before);
        let after = super::known_ids(&self.0.doc, after);
        self.0.diff_obj(obj, &before, &after, recursive)
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
        self.0.diff_incremental()
    }

    pub fn fork(&mut self) -> Self {
        Self::wrap(self.0.fork())
    }

    pub fn fork_at(&mut self, heads: &[ChangeHash]) -> Result<Self, AutomergeError> {
        let ids = super::strict_ids(&self.0.doc, heads)?;
        Ok(Self::wrap(self.0.fork_at(&ids)?))
    }

    /// Get the inner document.
    #[doc(hidden)]
    pub fn document(&mut self) -> &Automerge {
        Automerge::from_next_ref(self.0.document())
    }

    pub fn with_actor(mut self, actor: ActorId) -> Self {
        self.0.set_actor(actor);
        self
    }

    pub fn set_actor(&mut self, actor: ActorId) -> &mut Self {
        self.0.set_actor(actor);
        self
    }

    pub fn with_author(mut self, author: Option<Author<'static>>) -> Self {
        self.0.set_author(author);
        self
    }

    pub fn set_author(&mut self, author: Option<Author<'static>>) -> &mut Self {
        self.0.set_author(author);
        self
    }

    pub fn get_actor(&self) -> &ActorId {
        self.0.get_actor()
    }

    pub fn get_actors_for_author(&self, author: &Author<'_>) -> Vec<ActorId> {
        self.0.get_actors_for_author(author)
    }

    pub fn get_author_for_actor(&self, actor: &ActorId) -> Option<Author<'_>> {
        self.0.get_author_for_actor(actor)
    }

    pub fn get_author(&self) -> Option<&Author<'static>> {
        self.0.get_author()
    }

    pub fn get_authors(&self) -> &[Author<'static>] {
        self.0.get_authors()
    }

    pub fn isolate(&mut self, heads: &[ChangeHash]) {
        let ids = super::known_ids(&self.0.doc, heads);
        // isolating at heads the document has cannot fail
        let _ = self.0.isolate(&ids);
        self.1 = Some(IsolatedAt {
            given: heads.to_vec(),
            resolved: ids,
        });
    }

    pub fn integrate(&mut self) {
        self.0.integrate();
        self.1 = None;
    }

    /// Load an incremental save of a document.
    ///
    /// Unlike [`Self::load()`] this imports changes into an existing document. It will work with both
    /// the output of [`Self::save()`] and [`Self::save_incremental()`]
    ///
    /// The return value is the number of ops which were applied, this is not useful and will
    /// change in future.
    pub fn load_incremental(&mut self, data: &[u8]) -> Result<usize, AutomergeError> {
        self.0.load_incremental(data)
    }

    pub fn apply_changes(
        &mut self,
        changes: impl IntoIterator<Item = Change> + Clone,
    ) -> Result<(), AutomergeError> {
        self.0.apply_changes(changes)
    }

    pub fn apply_changes_batch(
        &mut self,
        changes: impl IntoIterator<Item = Change> + Clone,
    ) -> Result<(), AutomergeError> {
        self.0.apply_changes_batch(changes)
    }

    /// Takes all the changes in `other` which are not in `self` and applies them
    pub fn merge(&mut self, other: &mut AutoCommit) -> Result<Vec<ChangeHash>, AutomergeError> {
        self.0.merge(&mut other.0)?;
        Ok(self.get_heads())
    }

    /// Save the entirety of this document in a compact form.
    pub fn save(&mut self) -> Vec<u8> {
        self.save_with_options(SaveOptions::default())
    }

    pub fn save_with_options(&mut self, options: SaveOptions) -> Vec<u8> {
        self.0.save_with_options(options.into_next())
    }

    /// Save the document and attempt to load it before returning - slow!
    pub fn save_and_verify(&mut self) -> Result<Vec<u8>, AutomergeError> {
        let bytes = self.save();
        Self::load(&bytes)?;
        Ok(bytes)
    }

    #[cfg(test)]
    pub fn debug_cmp(&self, other: &Self) {
        self.0.debug_cmp(&other.0)
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
        self.0
            .save_incremental_with_options(SaveOptions::default().into_next())
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    /// Save everything which is not a (transitive) dependency of `heads`
    pub fn save_after(&mut self, heads: &[ChangeHash]) -> Vec<u8> {
        let ids = super::known_ids(self.0.document(), heads);
        self.0
            .save_after_with_options(&ids, SaveOptions::default().into_next())
            .unwrap_or_default()
    }

    pub fn get_missing_deps(&mut self, heads: &[ChangeHash]) -> Vec<ChangeHash> {
        self.0
            .document()
            .missing_deps_with_queued(heads)
            .unwrap_or_default()
    }

    /// Get the last change made by this documents actor ID
    pub fn get_last_local_change(&mut self) -> Option<Change> {
        self.0.get_last_local_change_legacy().ok().flatten()
    }

    pub fn get_changes(&mut self, have_deps: &[ChangeHash]) -> Vec<Change> {
        let ids = super::known_ids(self.0.document(), have_deps);
        self.0.get_changes(&ids).unwrap_or_default()
    }

    pub fn get_changes_meta(&mut self, have_deps: &[ChangeHash]) -> Vec<ChangeMetadata<'_>> {
        let ids = super::known_ids(self.0.document(), have_deps);
        self.0.get_changes_meta(&ids).unwrap_or_default()
    }

    pub fn get_change_by_hash(&mut self, hash: &ChangeHash) -> Option<Change> {
        self.0.get_change_by_hash(hash).ok().flatten()
    }

    pub fn get_change_meta_by_hash(&mut self, hash: &ChangeHash) -> Option<ChangeMetadata<'_>> {
        self.0.get_change_meta_by_hash(hash).ok().flatten()
    }

    /// Get changes in `other` that are not in `self`
    pub fn get_changes_added(&mut self, other: &mut Self) -> Vec<Change> {
        let theirs = other.0.document();
        self.0.document().changes_added_by_hash(theirs)
    }

    #[doc(hidden)]
    pub fn import(&self, s: &str) -> Result<(ExId, ObjType), AutomergeError> {
        self.0.import(s)
    }

    #[doc(hidden)]
    pub fn import_obj(&self, s: &str) -> Result<ExId, AutomergeError> {
        self.0.import_obj(s)
    }

    #[doc(hidden)]
    pub fn dump(&mut self) {
        self.0.dump()
    }

    /// EXPERIMENTAL: Return the fragments covering the document history at
    /// the given levels, ordered oldest to newest.
    ///
    /// This is an experimental API, it may change or be removed without
    /// warning.
    #[doc(hidden)]
    pub fn fragments<R: RangeBounds<usize>>(&self, levels: R) -> Vec<Fragment> {
        self.0
            .fragments(levels)
            .into_iter()
            .map(|f| Fragment::from_next(&self.0.doc, f))
            .collect()
    }

    /// EXPERIMENTAL: Return the fragment with the given head hash, if any.
    ///
    /// This is an experimental API, it may change or be removed without
    /// warning.
    #[doc(hidden)]
    pub fn get_fragment(&self, head: ChangeHash) -> Option<Fragment> {
        self.0
            .get_fragment(head)
            .map(|f| Fragment::from_next(&self.0.doc, f))
    }

    /// Get the current heads of the document.
    ///
    /// This closes the transaction first, if one is in progress.
    pub fn get_heads(&mut self) -> Vec<ChangeHash> {
        if self.0.isolation.is_none() {
            return self.0.get_head_hashes();
        }
        let ids = self.0.get_heads();
        match &self.1 {
            Some(IsolatedAt { given, resolved }) if *resolved == ids => given.clone(),
            _ => super::hashes_of(&self.0.doc, &ids),
        }
    }

    /// Commit any uncommitted changes
    ///
    /// Returns [`None`] if there were no operations to commit
    pub fn commit(&mut self) -> Option<ChangeHash> {
        let id = self.0.commit()?;
        Some(super::hashes_of(&self.0.doc, &[id])[0])
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
        let id = self.0.commit_with(options)?;
        Some(super::hashes_of(&self.0.doc, &[id])[0])
    }

    /// Remove any changes that have been made in the current transaction from the document
    pub fn rollback(&mut self) -> usize {
        self.0.rollback()
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
        let id = self.0.empty_change(options);
        super::hashes_of(&self.0.doc, &[id])[0]
    }

    /// Get the hash of the change that contains the given `opid`.
    ///
    /// Returns [`None`] if the `opid`:
    /// - Is the root object id
    /// - Does not exist in this document
    /// - Is for an operation in a transaction
    pub fn hash_for_opid(&self, opid: &ExId) -> Option<ChangeHash> {
        self.0.hash_for_opid(opid).ok().flatten()
    }

    /// EXPERIMENTAL: Encode each fragment as a change set.
    ///
    /// This is an experimental API, it may change or be removed without
    /// warning.
    #[doc(hidden)]
    pub fn change_sets_for_fragments<I: IntoIterator<Item = Fragment>>(
        &self,
        fragments: I,
    ) -> Result<Vec<Vec<u8>>, AutomergeError> {
        Automerge::from_next_ref(&self.0.doc).change_sets_for_fragments(fragments)
    }

    /// Convert to the [`next`](crate::next) API, which keeps only the hashes a
    /// document needs and names history by [`ChangeId`].
    pub fn into_next(self) -> crate::next::AutoCommit {
        self.0.disable_audit_mode()
    }

    /// The document as it stands, without committing an open transaction,
    /// for the `automerge-sync` crate.
    #[doc(hidden)]
    pub fn __document(&self) -> &Automerge {
        Automerge::from_next_ref(&self.0.doc)
    }

    /// The document, with any open transaction committed first, for the
    /// `automerge-sync` crate.
    #[doc(hidden)]
    pub fn __document_mut(&mut self) -> &mut Automerge {
        Automerge::from_next_mut(self.0.document_mut())
    }
}

impl super::AuditWrapper for AutoCommit {
    type Next = crate::autocommit::AutoCommit<Full>;

    fn next(&self) -> &Self::Next {
        &self.0
    }

    fn next_mut(&mut self) -> &mut Self::Next {
        &mut self.0
    }

    fn doc(&self) -> &crate::automerge::Automerge<Full> {
        &self.0.doc
    }
}

impl crate::autocommit::AutoCommit<Full> {
    /// Convert to the audited API.
    pub fn into_audit(self) -> AutoCommit {
        AutoCommit::wrap(self)
    }
}

impl crate::autocommit::AutoCommit<Retained> {
    /// Convert to the audited API, recomputing and keeping the hash of every
    /// change in the document's history.
    ///
    /// Fails if a hash the document already held does not match the
    /// change it names; the document then comes back unchanged, inside
    /// the error.
    pub fn into_audit(self) -> Result<AutoCommit, EnableAuditModeError<Self>> {
        self.enable_audit_mode().map(AutoCommit::wrap)
    }
}
