use std::ops::RangeBounds;

use crate::automerge::EnableAuditModeError;
use crate::exid::ExId;
use crate::hash_retention::{Full, Retained};
use crate::op_set2::ChangeMetadata;
use crate::patches::Patch;
use crate::storage::VerificationMode;
use crate::tx::CommitOptions;
use crate::{hydrate, ActorId, Author, AutomergeError, Change, ChangeHash, ObjType, TextEncoding};
use crate::{OnPartialLoad, StringMigration};

use super::transaction::{self, Failure, OwnedTransaction, Success, Transaction};

/// Options to pass to [`Automerge::load_with_options()`] and [`crate::AutoCommit::load_with_options()`]
#[derive(Debug)]
pub struct LoadOptions {
    on_partial_load: OnPartialLoad,
    verification_mode: VerificationMode,
    string_migration: StringMigration,
    text_encoding: TextEncoding,
    author: Option<Author<'static>>,
}

impl LoadOptions {
    pub fn new() -> LoadOptions {
        LoadOptions::default()
    }

    /// What to do when loading a document partially succeeds
    ///
    /// The default is [`OnPartialLoad::Error`]
    pub fn on_partial_load(self, on_partial_load: OnPartialLoad) -> Self {
        Self {
            on_partial_load,
            ..self
        }
    }

    /// Whether to verify the head hashes after loading
    ///
    /// The default is [`VerificationMode::Check`]
    pub fn verification_mode(self, verification_mode: VerificationMode) -> Self {
        Self {
            verification_mode,
            ..self
        }
    }

    /// Whether to convert [`ScalarValue::Str`](crate::ScalarValue::Str)s in the loaded document to [`ObjType::Text`]
    ///
    /// Until version 2.1.0 of the javascript library strings (as in, the native string of the JS
    /// runtime) were represented in the document as [`ScalarValue::Str`](crate::ScalarValue::Str) and there was a special
    /// JS class called `Text` which users were expected to use for [`ObjType::Text`]. In `2.1.0`
    /// we changed this so that native strings were represented as [`ObjType::Text`] and
    /// [`ScalarValue::Str`](crate::ScalarValue::Str) was represented as a special `RawString` class. This means
    /// that upgrading the application code to use the new API would require either
    ///
    /// a) Maintaining two code paths in the application to deal with both `string` and `RawString`
    ///    types
    /// b) Writing a migration script to convert all `RawString` types to `string`
    ///
    /// The latter is logic which is the same for all applications so we implement it in the
    /// library for convenience. The way this works is that after loading the document we iterate
    /// through all visible [`ScalarValue::Str`](crate::ScalarValue::Str) values and emit a change which creates a new
    /// [`ObjType::Text`] at the same path with the same content.
    pub fn migrate_strings(self, migration: StringMigration) -> Self {
        Self {
            string_migration: migration,
            ..self
        }
    }

    pub fn text_encoding(self, text_encoding: TextEncoding) -> Self {
        Self {
            text_encoding,
            ..self
        }
    }

    pub fn author(self, author: Author<'static>) -> Self {
        Self {
            author: Some(author),
            ..self
        }
    }
}

impl LoadOptions {
    pub(crate) fn into_next(self) -> crate::automerge::LoadOptions {
        let options = crate::automerge::LoadOptions::new()
            .on_partial_load(self.on_partial_load)
            .verification_mode(self.verification_mode)
            .migrate_strings(self.string_migration)
            .text_encoding(self.text_encoding);
        match self.author {
            Some(author) => options.author(author),
            None => options,
        }
    }
}

impl std::default::Default for LoadOptions {
    fn default() -> Self {
        Self {
            on_partial_load: OnPartialLoad::Error,
            verification_mode: VerificationMode::Check,
            string_migration: StringMigration::NoMigration,
            text_encoding: TextEncoding::platform_default(),
            author: None,
        }
    }
}

/// Options to pass to [`Automerge::save_with_options()`] and [`crate::AutoCommit::save_with_options()`]
#[derive(Debug)]
pub struct SaveOptions {
    /// Whether to apply DEFLATE compression to the RLE encoded columns in the document
    pub deflate: bool,
    /// Whether to save changes which we do not have the dependencies for
    pub retain_orphans: bool,
}

impl SaveOptions {
    pub(crate) fn into_next(self) -> crate::automerge::SaveOptions {
        crate::automerge::SaveOptions {
            deflate: self.deflate,
            retain_orphans: self.retain_orphans,
            format: crate::automerge::SaveFormat::Legacy,
        }
    }
}

impl std::default::Default for SaveOptions {
    fn default() -> Self {
        Self {
            deflate: true,
            retain_orphans: true,
        }
    }
}

/// EXPERIMENTAL: A section of the change graph identified by its head hash.
///
/// This is an experimental API, it may change or be removed without warning.
#[doc(hidden)]
#[derive(Debug, PartialEq, Clone)]
pub struct Fragment {
    pub head: ChangeHash,
    pub level: usize,
    pub boundary: Vec<ChangeHash>,
    pub checkpoints: Vec<ChangeHash>,
    pub members: Vec<ChangeHash>,
}

impl Fragment {
    pub(crate) fn into_next(
        self,
        doc: &crate::automerge::Automerge<Full>,
    ) -> crate::change_graph::Fragment {
        crate::change_graph::Fragment {
            head: self.head,
            level: self.level,
            boundary: self.boundary,
            checkpoints: self.checkpoints,
            members: super::known_ids(doc, &self.members),
        }
    }

    pub(crate) fn from_next(
        doc: &crate::automerge::Automerge<Full>,
        f: crate::change_graph::Fragment,
    ) -> Self {
        Self {
            head: f.head,
            level: f.level,
            boundary: f.boundary,
            checkpoints: f.checkpoints,
            members: super::hashes_of(doc, &f.members),
        }
    }
}

/// An automerge document which does not manage transactions for you.
///
/// ## Creating, loading, merging and forking documents
///
/// A new document can be created with [`Self::new()`], which will create a document with a random
/// [`ActorId`]. Existing documents can be loaded with [`Self::load()`], or [`Self::load_with_options()`].
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
/// ## Modifying a document (Transactions)
///
/// [`Automerge`] provides an interface for viewing and modifying automerge documents which does
/// not manage transactions for you. To create changes you use either [`Automerge::transaction()`] or
/// [`Automerge::transact()`] (or the `_with` variants).
///
/// ## Sync
///
/// The sync protocol lives in the `automerge-sync` crate, which implements its `SyncDoc` trait
/// for this type.
///
/// ## Authors and Actors
///
/// It's often useful to be able to know who made some change. For this purpose Automerge has the
/// concept of an "author ID". An author ID is an opaque byte array which can be associated with a
/// change. This author ID will become part of the document history so you can later examine the
/// changes in a document and see which author made the change.
///
/// Author IDs are set on document construction. If you don't set an author then changes produced
/// by the document will have no author ([`Change::author`] will return `None`).
///
/// ### Example
///
/// ```rust
/// # use automerge::{Author, Automerge, AutomergeError, ROOT, transaction::Transactable};
/// let author = Author::from(vec![1,2,3]);
/// let mut doc = Automerge::new().with_author(Some(author.clone()));
/// doc.transact(|tx| {
///     tx.put(ROOT, "foo", "bar")?;
///     Ok::<_, AutomergeError>(())
/// }).unwrap();
/// let change = doc.get_last_local_change().unwrap();
/// assert_eq!(change.author().unwrap(), author);
/// ```
///
/// ### Relationship to Actor IDs
///
/// Every automerge commit has an "actor ID", which represents a sequential execution. Actor IDs
/// should be considered a low level implementation detail and as much as possible should be left
/// to automerge to manage.
///
/// Prior to the introduction of author IDs, actor IDs were often used in applications to determine
/// authorship. New code should migrate to using author IDs. If you do need to map from an actor ID
/// to an author ID you can use [`Automerge::get_author_for_actor`].
#[derive(Debug, Clone)]
#[repr(transparent)]
pub struct Automerge(pub(crate) crate::automerge::Automerge<Full>);

impl Automerge {
    /// Create a new document with a random actor id.
    pub fn new() -> Self {
        Self(crate::automerge::Automerge::<Full>::empty(
            TextEncoding::platform_default(),
        ))
    }

    pub fn anonymize(&self) -> Result<Self, crate::AnonymizeError> {
        Ok(Self(self.0.anonymize()?))
    }

    /// Overwrite the keys of the root object with the values from `value`
    ///
    /// This is useful to initialize an empty document with a large initial
    /// value. Note that existing keys which are not in `value` are left as is
    pub fn init_from_hydrate(&mut self, value: &crate::hydrate::Map) -> Result<(), AutomergeError> {
        self.0.init_from_hydrate(value)
    }

    pub fn new_with_encoding(encoding: TextEncoding) -> Self {
        Self(crate::automerge::Automerge::<Full>::empty(encoding))
    }

    /// Whether this document has any operations
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    /// Set the actor id for this document.
    pub fn with_actor(mut self, actor: ActorId) -> Self {
        self.0.set_actor(actor);
        self
    }

    /// Set the actor id for this document.
    pub fn set_actor(&mut self, actor: ActorId) -> &mut Self {
        self.0.set_actor(actor);
        self
    }

    /// Set the actor id for this document.
    pub fn with_author(mut self, author: Option<Author<'static>>) -> Self {
        self.0.set_author(author);
        self
    }

    /// Set the author for this document.
    ///
    /// The [`Author`] is only changed, if `author` differs from
    /// [`Automerge::get_author`]. If the author does change then a new
    /// [`ActorId`] is generated for that author. Notably, this could be a
    /// different [`ActorId`] compared to previous edits for the same author.
    ///
    /// If you are using authors *never* manually manage the [`ActorId`].
    pub fn set_author(&mut self, author: Option<Author<'static>>) -> &mut Self {
        self.0.set_author(author);
        self
    }

    /// Get the current author of this document.
    pub fn get_author(&self) -> Option<&Author<'static>> {
        self.0.get_author()
    }

    pub fn get_actors_for_author(&self, author: &Author<'_>) -> Vec<ActorId> {
        self.0.get_actors_for_author(author)
    }

    pub fn get_authors(&self) -> &[Author<'static>] {
        self.0.get_authors()
    }

    pub fn get_author_for_actor(&self, actor: &ActorId) -> Option<Author<'_>> {
        self.0.get_author_for_actor(actor)
    }

    /// Get the current actor id of this document.
    pub fn get_actor(&self) -> &ActorId {
        self.0.get_actor()
    }

    /// Start a transaction.
    pub fn transaction(&mut self) -> Transaction<'_> {
        Transaction(self.0.transaction())
    }

    /// Start a transaction isolated at a given heads
    pub fn transaction_at(
        &mut self,
        heads: &[ChangeHash],
    ) -> Result<Transaction<'_>, AutomergeError> {
        let ids = super::known_ids(&self.0, heads);
        Ok(Transaction(self.0.transaction_at(&ids)?))
    }

    /// Start a transaction that owns the document, consuming `self`.
    ///
    /// This is useful when the transaction must be `'static` (e.g. storing across an FFI
    /// boundary or in a struct that requires `'static`). The document is returned when the
    /// transaction is committed or rolled back.
    ///
    /// # Arguments
    /// * `heads` - An optional set of heads to isolate this transaction at, or `None` to use the
    ///   current heads of the document
    pub fn into_transaction(
        self,
        heads: Option<&[ChangeHash]>,
    ) -> Result<OwnedTransaction, AutomergeError> {
        let ids = heads.map(|h| super::known_ids(&self.0, h));
        Ok(OwnedTransaction(self.0.into_transaction(ids.as_deref())?))
    }

    /// Run a transaction on this document in a closure, automatically handling commit or rollback
    /// afterwards.
    pub fn transact<F, O, E>(&mut self, f: F) -> transaction::Result<O, E>
    where
        F: FnOnce(&mut Transaction<'_>) -> Result<O, E>,
    {
        self.transact_with_impl(None::<&dyn Fn(&O) -> CommitOptions>, f)
    }

    /// Like [`Self::transact()`] but with a function for generating the commit options.
    pub fn transact_with<F, O, E, C>(&mut self, c: C, f: F) -> transaction::Result<O, E>
    where
        F: FnOnce(&mut Transaction<'_>) -> Result<O, E>,
        C: FnOnce(&O) -> CommitOptions,
    {
        self.transact_with_impl(Some(c), f)
    }

    /// Generate an empty change
    ///
    /// The main reason to do this is if you want to create a "merge commit", which is a change
    /// that has all the current heads of the document as dependencies.
    pub fn empty_commit(&mut self, opts: CommitOptions) -> ChangeHash {
        let id = self.0.empty_commit(opts);
        super::hashes_of(&self.0, &[id])[0]
    }

    /// Fork this document at the current point for use by a different actor.
    ///
    /// This will create a new actor ID for the forked document
    pub fn fork(&self) -> Self {
        Self(self.0.fork())
    }

    /// Fork this document at the given heads
    ///
    /// This will create a new actor ID for the forked document
    pub fn fork_at(&self, heads: &[ChangeHash]) -> Result<Self, AutomergeError> {
        let ids = super::strict_ids(&self.0, heads)?;
        Ok(Self(self.0.fork_at(&ids)?))
    }

    pub fn diff_opset(&self, other: &Self) -> Result<(), AutomergeError> {
        self.0.diff_opset(&other.0)
    }

    /// Load a document.
    pub fn load(data: &[u8]) -> Result<Self, AutomergeError> {
        Self::load_with_options(data, LoadOptions::default())
    }

    /// Load a document without verifying the head hashes
    ///
    /// This is useful for debugging as it allows you to examine a corrupted document.
    pub fn load_unverified_heads(data: &[u8]) -> Result<Self, AutomergeError> {
        Self::load_with_options(
            data,
            LoadOptions::new().verification_mode(VerificationMode::DontCheck),
        )
    }

    /// Load a document, with options
    ///
    /// # Arguments
    /// * `data` - The data to load
    /// * `options` - The options to use when loading
    #[tracing::instrument(skip(data), err)]
    pub fn load_with_options(data: &[u8], options: LoadOptions) -> Result<Self, AutomergeError> {
        Ok(Self(
            crate::automerge::Automerge::<Full>::load_with_options_and_mark_validation(
                data,
                options.into_next(),
                crate::storage::load::MarkOrderValidation::Validate,
            )?,
        ))
    }

    /// Best-effort rescue for documents which fail strict loading.
    ///
    /// This returns only the current hydrated value and does not preserve the original change graph.
    pub fn rescue(data: &[u8]) -> Result<hydrate::Value, AutomergeError> {
        crate::automerge::Automerge::rescue(data)
    }

    /// Get a set of [`Patch`]es which materialize the current state of the document
    ///
    /// This is a convienence method for [`doc.diff(&[], current_heads)`][diff]
    ///
    /// [diff]: Self::diff()
    pub fn current_state(&self) -> Vec<Patch> {
        self.0.current_state()
    }

    /// Load an incremental save of a document.
    ///
    /// Unlike [`Self::load()`] this imports changes into an existing document. It will work with
    /// both the output of [`Self::save()`] and [`Self::save_after()`]
    ///
    /// The return value is the number of ops which were applied, this is not useful and will
    /// change in future.
    pub fn load_incremental(&mut self, data: &[u8]) -> Result<usize, AutomergeError> {
        self.0.load_incremental(data)
    }

    /// Apply changes to this document.
    ///
    /// This is idempotent in the sense that if a change has already been applied it will be
    /// ignored.
    pub fn apply_changes(
        &mut self,
        changes: impl IntoIterator<Item = Change> + Clone,
    ) -> Result<(), AutomergeError> {
        self.0.apply_changes(changes)
    }

    /// Takes all the changes in `other` which are not in `self` and applies them
    pub fn merge(&mut self, other: &mut Self) -> Result<Vec<ChangeHash>, AutomergeError> {
        self.0.merge(&mut other.0)?;
        Ok(self.get_heads())
    }

    /// Save the entirety of this document in a compact form.
    pub fn save_with_options(&self, options: SaveOptions) -> Vec<u8> {
        self.0.save_with_options(options.into_next())
    }

    #[cfg(test)]
    pub fn debug_cmp(&self, other: &Self) {
        self.0.debug_cmp(&other.0)
    }

    /// Save the entirety of this document in a compact form.
    pub fn save(&self) -> Vec<u8> {
        self.save_with_options(SaveOptions::default())
    }

    /// Save the document and attempt to load it before returning - slow!
    pub fn save_and_verify(&self) -> Result<Vec<u8>, AutomergeError> {
        let bytes = self.save();
        Self::load(&bytes)?;
        Ok(bytes)
    }

    /// Save this document, but don't run it through `DEFLATE` afterwards
    pub fn save_nocompress(&self) -> Vec<u8> {
        self.save_with_options(SaveOptions {
            deflate: false,
            ..Default::default()
        })
    }

    /// Save the changes since the given heads
    ///
    /// The output of this will not be a compressed document format, but a series of individual
    /// changes. This is useful if you know you have only made a small change since the last
    /// [`Self::save()`] and you want to immediately send it somewhere (e.g. you've inserted a
    /// single character in a text object).
    pub fn save_after(&self, heads: &[ChangeHash]) -> Vec<u8> {
        let ids = super::known_ids(&self.0, heads);
        self.0
            .save_after_with_options(&ids, SaveOptions::default().into_next())
            .unwrap_or_default()
    }

    /// Get the last change this actor made to the document.
    pub fn get_last_local_change(&self) -> Option<Change> {
        self.0.get_last_local_change_legacy().ok().flatten()
    }

    #[doc(hidden)]
    pub fn import(&self, s: &str) -> Result<(ExId, ObjType), AutomergeError> {
        self.0.import(s)
    }

    #[doc(hidden)]
    pub fn import_obj(&self, s: &str) -> Result<ExId, AutomergeError> {
        self.0.import_obj(s)
    }

    pub fn dump(&self) {
        self.0.dump()
    }

    /// Create patches representing the change in the current state of the document between the
    /// `before` and `after` heads.  If the arguments are reverse it will observe the same changes
    /// in the opposite order.
    pub fn diff(&self, before_heads: &[ChangeHash], after_heads: &[ChangeHash]) -> Vec<Patch> {
        let before = super::known_ids(&self.0, before_heads);
        let after = super::known_ids(&self.0, after_heads);
        self.0.diff(&before, &after).unwrap_or_default()
    }

    /// Create patches representing the change in the current state of an object
    /// in the document between the `before_heads` and `after_heads` heads. If
    /// the arguments are reverse it will observe the same changes in the
    /// opposite order.
    ///
    /// # Arguments
    ///
    /// * `obj` - The object to start the diff at.
    /// * `before_heads` - heads from [`Self::get_heads()`] at beginning point
    ///   in the documents history
    /// * `after_heads` - heads from [`Self::get_heads()`] at ending point in
    ///   the documents history.
    /// * `recursive` - if false, do not also diff child objects
    ///
    /// Note: `before_heads` and `after_heads` do not have to be chronological.
    /// Document state can move backward.
    pub fn diff_obj(
        &self,
        obj: &ExId,
        before_heads: &[ChangeHash],
        after_heads: &[ChangeHash],
        recursive: bool,
    ) -> Result<Vec<Patch>, AutomergeError> {
        let before = super::known_ids(&self.0, before_heads);
        let after = super::known_ids(&self.0, after_heads);
        self.0.diff_obj(obj, &before, &after, recursive)
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
            .map(|f| Fragment::from_next(&self.0, f))
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
            .map(|f| Fragment::from_next(&self.0, f))
    }

    /// Get the heads of this document.
    pub fn get_heads(&self) -> Vec<ChangeHash> {
        self.0.get_head_hashes()
    }

    pub fn get_changes(&self, have_deps: &[ChangeHash]) -> Vec<Change> {
        let ids = super::known_ids(&self.0, have_deps);
        self.0.get_changes(&ids).unwrap_or_default()
    }

    pub fn get_changes_meta(&self, have_deps: &[ChangeHash]) -> Vec<ChangeMetadata<'_>> {
        let ids = super::known_ids(&self.0, have_deps);
        self.0.get_changes_meta(&ids).unwrap_or_default()
    }

    pub fn get_change_meta_by_hash(&self, hash: &ChangeHash) -> Option<ChangeMetadata<'_>> {
        self.0.get_change_meta_by_hash(hash).ok().flatten()
    }

    /// Get changes in `other` that are not in `self`
    pub fn get_changes_added(&self, other: &Self) -> Vec<Change> {
        self.0.changes_added_by_hash(&other.0)
    }

    /// Get the hash of the change that contains the given `opid`.
    ///
    /// Returns [`None`] if the `opid`:
    /// - is the root object id
    /// - does not exist in this document
    pub fn hash_for_opid(&self, exid: &ExId) -> Option<ChangeHash> {
        self.0.hash_for_opid(exid).ok().flatten()
    }

    pub fn hydrate(&self, heads: Option<&[ChangeHash]>) -> hydrate::Value {
        let ids = heads.map(|h| super::known_ids(&self.0, h));
        self.0
            .hydrate(ids.as_deref())
            .expect("known heads always resolve")
    }

    pub fn text_encoding(&self) -> TextEncoding {
        self.0.text_encoding()
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
        let fragments: Vec<_> = fragments
            .into_iter()
            .map(|f| f.into_next(&self.0))
            .collect();
        self.0.change_sets_for_fragments(fragments)
    }

    /// Apply a batch of changes to this document. See [`Self::apply_changes()`].
    pub fn apply_changes_batch(
        &mut self,
        changes: impl IntoIterator<Item = Change> + Clone,
    ) -> Result<(), AutomergeError> {
        self.0.apply_changes_batch(changes)
    }

    /// Convert to the [`next`](crate::next) API, which keeps only the hashes a
    /// document needs and names history by [`ChangeId`](crate::ChangeId).
    pub fn into_next(self) -> crate::next::Automerge {
        self.0.disable_audit_mode()
    }

    /// The next document behind this one, for the `automerge-sync` crate.
    #[doc(hidden)]
    pub fn __next(&self) -> &crate::automerge::Automerge<Full> {
        &self.0
    }

    /// The next document behind this one, for the `automerge-sync` crate.
    #[doc(hidden)]
    pub fn __next_mut(&mut self) -> &mut crate::automerge::Automerge<Full> {
        &mut self.0
    }

    pub(crate) fn from_next_mut(doc: &mut crate::automerge::Automerge<Full>) -> &mut Self {
        // SAFETY: `Automerge` is a `repr(transparent)` wrapper around the next type
        unsafe { &mut *(doc as *mut crate::automerge::Automerge<Full> as *mut Self) }
    }

    pub(crate) fn from_next_ref(doc: &crate::automerge::Automerge<Full>) -> &Self {
        // SAFETY: `Automerge` is a `repr(transparent)` wrapper around the next type
        unsafe { &*(doc as *const crate::automerge::Automerge<Full> as *const Self) }
    }

    fn transact_with_impl<F, O, E, C>(&mut self, c: Option<C>, f: F) -> transaction::Result<O, E>
    where
        F: FnOnce(&mut Transaction<'_>) -> Result<O, E>,
        C: FnOnce(&O) -> CommitOptions,
    {
        let mut tx = self.transaction();
        match f(&mut tx) {
            Ok(result) => {
                let hash = match c {
                    Some(c) => {
                        let options = c(&result);
                        tx.commit_with(options)
                    }
                    None => tx.commit(),
                };
                Ok(Success { result, hash })
            }
            Err(error) => Err(Failure {
                error,
                cancelled: tx.rollback(),
            }),
        }
    }
}

impl Default for Automerge {
    fn default() -> Self {
        Self::new()
    }
}

impl super::AuditWrapper for Automerge {
    type Next = crate::automerge::Automerge<Full>;

    fn next(&self) -> &Self::Next {
        &self.0
    }

    fn next_mut(&mut self) -> &mut Self::Next {
        &mut self.0
    }

    fn doc(&self) -> &crate::automerge::Automerge<Full> {
        &self.0
    }
}

impl crate::automerge::Automerge<Full> {
    /// Convert to the audited API.
    pub fn into_audit(self) -> Automerge {
        Automerge(self)
    }
}

impl crate::automerge::Automerge<Retained> {
    /// Convert to the audited API, recomputing and keeping the hash of every
    /// change in the document's history.
    ///
    /// Fails if a hash the document already held does not match the
    /// change it names; the document then comes back unchanged, inside
    /// the error.
    pub fn into_audit(self) -> Result<Automerge, EnableAuditModeError<Self>> {
        self.enable_audit_mode().map(Automerge)
    }
}
