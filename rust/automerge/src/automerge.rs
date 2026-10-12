use std::collections::{BTreeSet, HashMap, HashSet};
use std::env;
use std::fmt::Debug;
use std::num::NonZeroU64;
use std::ops::RangeBounds;

use itertools::Itertools;

use crate::author::{Author, Authors};
pub(crate) use crate::op_set2::change::ChangeCollector;
pub(crate) use crate::op_set2::types::ScalarValue;
pub(crate) use crate::op_set2::{
    ChangeMetadata, KeyRef, OpQuery, OpQueryTerm, OpSet, OpType, Parents,
};
pub(crate) use crate::read::ReadDoc;

use crate::actor::{ActorInsert, ActorRemoval, ActorShift, ActorTable, HasActorIndices};
use crate::change_graph::ChangeGraph;
use crate::change_queue::{ChangeQueue, DocState, QueuedChunk};
use crate::clock::Clock;
use crate::config::Config;
use crate::cursor::{CursorPosition, MoveCursor, OpCursor};
use crate::exid::ExId;
use crate::hydrate;
use crate::iter::{DiffIter, DocIter, Keys, ListRange, MapRange, Spans, Values};
use crate::marks::{Mark, MarkAccumulator, MarkSet};
use crate::patches::{Patch, PatchLog};
use crate::signatures::{
    self, Signature, SignatureInput, SignatureMode, SignatureRef, SignatureReport, SignatureStore,
};
use crate::storage::document::ReconstructError;
use crate::storage::{self, change, load, Bundle, CompressConfig, Document, VerificationMode};
use crate::transaction::{
    self, CommitOptions, Failure, OwnedTransaction, Success, Transactable, Transaction,
    TransactionArgs,
};
use crate::types::{ActorId, ChangeHash, ObjId, ObjMeta, OpId, SequenceType, TextEncoding, Value};
use crate::write_frontier::WriteFrontier;
use crate::{AutomergeError, Change, Cursor, Fragment, ObjType, Prop};
use std::borrow::Cow;
use view::{ClockRange, Mask, ReadAt, VisibleClock};

pub(crate) mod current_state;
pub(crate) mod view;
mod visibility;

// FIXME
//#[cfg(test)]
//mod tests;

#[cfg(test)]
mod rollback_tests;

#[derive(Debug, Clone, PartialEq)]
pub(crate) enum Actor {
    Unused(ActorId),
    Cached(usize),
}

impl Actor {
    /// React to `removed` having been removed from the actor table. If it
    /// was this actor, fall back to holding the id directly.
    fn remove_actor(&mut self, removal: &ActorRemoval, removed: ActorId) {
        if let Actor::Cached(idx) = self {
            match idx.removed(removal) {
                Some(new_idx) => *idx = new_idx,
                None => *self = Actor::Unused(removed),
            }
        }
    }

    fn shift(&mut self, shift: &ActorShift) {
        if let Actor::Cached(idx) = self {
            *idx = idx.shifted(shift);
        }
    }
}

/// Outcome of [`Automerge::update_history`].
///
/// Every mutation path must consume this: when a change resolves a pending
/// write-frontier boundary, the new visibility has to be published through
/// [`Automerge::republish_mask`] once the op set is consistent again.
#[must_use = "a resolved pending boundary must be republished"]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum HistoryUpdate {
    /// The change did not resolve a pending write-frontier boundary.
    Unchanged,
    /// The change was a pending write-frontier boundary: the caller must publish
    /// the visibility change once the op set is consistent again.
    BoundaryResolved,
}

/// What to do when loading a document partially succeeds
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OnPartialLoad {
    /// Ignore the error and return the loaded changes
    Ignore,
    /// Fail the entire load
    Error,
}

/// Whether to convert [`ScalarValue::Str`]s in the loaded document to [`ObjType::Text`]
#[derive(Debug)]
pub enum StringMigration {
    /// Don't convert anything
    NoMigration,
    /// Convert all strings to text
    ConvertToText,
}

#[derive(Debug)]
pub struct LoadOptions<'a> {
    on_partial_load: OnPartialLoad,
    verification_mode: VerificationMode,
    string_migration: StringMigration,
    patch_log: Option<&'a mut PatchLog>,
    text_encoding: TextEncoding,
    author: Option<Author<'static>>,
    write_frontier: HashMap<Author<'static>, Vec<ChangeHash>>,
    signature_mode: SignatureMode,
    /// Signatures this load starts with, which an empty document
    /// rebuilding itself through `load` passes in so that signatures it already held
    /// still count towards what the incoming history needs.
    signatures: Vec<Signature>,
}

impl<'a> LoadOptions<'a> {
    pub fn new() -> LoadOptions<'static> {
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

    /// A [`PatchLog`] to log the changes required to materialize the current state of the
    ///
    /// The default is to not log patches
    pub fn patch_log(self, patch_log: &'a mut PatchLog) -> Self {
        Self {
            patch_log: Some(patch_log),
            ..self
        }
    }

    /// Whether to convert [`ScalarValue::Str`]s in the loaded document to [`ObjType::Text`]
    ///
    /// Until version 2.1.0 of the javascript library strings (as in, the native string of the JS
    /// runtime) were represented in the document as [`ScalarValue::Str`] and there was a special
    /// JS class called `Text` which users were expected to use for [`ObjType::Text`]. In `2.1.0`
    /// we changed this so that native strings were represented as [`ObjType::Text`] and
    /// [`ScalarValue::Str`] was represented as a special `RawString` class. This means
    /// that upgrading the application code to use the new API would require either
    ///
    /// a) Maintaining two code paths in the application to deal with both `string` and `RawString`
    ///    types
    /// b) Writing a migration script to convert all `RawString` types to `string`
    ///
    /// The latter is logic which is the same for all applications so we implement it in the
    /// library for convenience. The way this works is that after loading the document we iterate
    /// through all visible [`ScalarValue::Str`] values and emit a change which creates a new
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

    /// The write-frontier to apply to the loaded document.
    ///
    /// See [`Automerge::set_write_frontier`].
    pub fn write_frontier(self, write_frontier: HashMap<Author<'static>, Vec<ChangeHash>>) -> Self {
        Self {
            write_frontier,
            ..self
        }
    }

    /// Apply a [`Config`] to the loaded document.
    ///
    /// The same settings as [`Automerge::new_with_config`], for the document a
    /// load produces. Equivalent to setting [`Self::text_encoding`] and
    /// [`Self::signature_mode`] individually.
    pub fn config(self, config: Config) -> Self {
        self.text_encoding(config.encoding())
            .signature_mode(config.signatures())
    }

    /// Configure whether loaded changes are materialized immediately or held
    /// until a signature covers them.
    pub fn signature_mode(self, signature_mode: SignatureMode) -> Self {
        Self {
            signature_mode,
            ..self
        }
    }

    /// Signatures the loading document already holds.
    pub(crate) fn signatures(self, signatures: Vec<Signature>) -> Self {
        Self { signatures, ..self }
    }
}

impl std::default::Default for LoadOptions<'static> {
    fn default() -> Self {
        Self {
            on_partial_load: OnPartialLoad::Error,
            verification_mode: VerificationMode::Check,
            patch_log: None,
            string_migration: StringMigration::NoMigration,
            text_encoding: TextEncoding::platform_default(),
            author: None,
            write_frontier: HashMap::new(),
            signature_mode: SignatureMode::Legacy,
            signatures: Vec::new(),
        }
    }
}

/// An automerge document which does not manage transactions for you.
///
/// ## Creating, loading, merging and forking documents
///
/// A new document can be created with [`Self::new()`], which will create a document with a random
/// [`ActorId`]. Existing documents can be loaded with [`Self::load()`], or [`Self::load_with()`].
///
/// If you have two documents and you want to merge the changes from one into the other you can use
/// [`Self::merge()`] or [`Self::merge_and_log_patches()`].
///
/// If you have a document you want to split into two concurrent threads of execution you can use
/// [`Self::fork()`]. If you want to split a document from ealier in its history you can use
/// [`Self::fork_at()`].
///
/// ## Reading values
///
/// [`Self`] implements [`ReadDoc`], which provides methods for reading values from the document.
///
/// ## Modifying a document (Transactions)
///
/// [`Automerge`] provides an interface for viewing and modifying automerge documents which does
/// not manage transactions for you. To create changes you use either [`Automerge::transaction()`] or
/// [`Automerge::transact()`] (or the `_with` variants).
///
/// ## Sync
///
/// This type implements [`crate::sync::SyncDoc`]
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
/// ## Masking and Revealing Author Changes
///
/// Use [`Automerge::mask_author`] to restrict an author's contribution to
/// the materialized view of a document. The supplied heads define that
/// author's write-frontier: their changes within the causal history of
/// those heads, including the heads themselves, remain eligible to
/// contribute to the view. Their other changes are hidden.
///
/// If any of the write-frontier's heads have not yet been received, all
/// of the author's changes are hidden until the complete frontier can
/// be resolved. This can happen when the write-frontier is received
/// separately from the changes it references.
///
/// Use [`Automerge::reveal_author`] to remove the author's write-frontier
/// restriction. Their changes are then subject to the document's usual
/// visibility rules.
///
/// Masking affects the materialized view, not the stored history:
/// hidden changes remain in the change graph.
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
pub struct Automerge {
    /// History received but not yet applied, the signature records held, and
    /// the mode which says what those records must cover.
    pub(crate) queue: ChangeQueue,
    /// Graph of changes
    pub(crate) change_graph: ChangeGraph,
    pub(crate) authors: Authors,
    /// Authors whose changes are hidden past a heads boundary.
    ///
    /// The visibility based on these policies is derived in one place as
    /// [`Automerge::visibility`].
    write_frontier: WriteFrontier,
    /// Visibility derived from the write-frontier, the change graph, and the
    /// author set. Written only by [`Automerge::republish_mask`], registration,
    /// and actor column shifts.
    visibility: visibility::FrontierVisibility,
    /// Current dependencies of this document (heads hashes).
    deps: HashSet<ChangeHash>,
    /// The set of operations that form this document.
    pub(crate) ops: OpSet,
    /// The current actor.
    actor: Actor,
    /// The current author.
    author: Option<Author<'static>>,
}

impl Automerge {
    /// Create a new document with a random actor id.
    pub fn new() -> Self {
        Self::new_with_encoding(TextEncoding::platform_default())
    }

    /// Return a copy of this document with its data anonymized using a fresh random seed.
    ///
    // Anonymization replaces actor IDs, map keys, mark names, scalar values, change metadata, and
    // extra bytes. It retains the change graph, operation and object types, sequence positions,
    // scalar string and byte-value lengths, and the UTF-8/UTF-16 width of each character. The
    // intention is to make a document that has very similar performance characteristics to the
    // original. This makes it useful when you want to send a document which is causing performance
    // problems to someone to diagnose.
    //
    // That said, the data scrubbing here is best-effort. The anonymized document still reveals
    // a bunch of information about editing patterns. A determined adversary could probably still
    // learn a great deal from such a document. The intended use is really for sending documents
    // to mostly trusted parties who are helping with bug fixing (e.g. library maintainers).
    pub fn anonymize(&self) -> Result<Self, crate::AnonymizeError> {
        crate::anonymize::anonymize(self)
    }

    /// Overwrite the keys of the root object with the values from `value`
    ///
    /// This is useful to initialize an empty document with a large initial
    /// value. Note that existing keys which are not in `value` are left as is
    pub fn init_from_hydrate(&mut self, value: &crate::hydrate::Map) -> Result<(), AutomergeError> {
        let mut tx = self.transaction();
        tx.batch_init_root_map(value)?;
        tx.commit();
        Ok(())
    }

    /// Create a new document configured by `config`.
    pub fn new_with_config(config: Config) -> Self {
        let mut doc = Self::new_with_encoding(config.encoding());
        doc.queue = ChangeQueue::new(config.signatures());
        doc
    }

    pub fn new_with_encoding(encoding: TextEncoding) -> Self {
        let ops = OpSet::new(encoding);
        let change_graph = ChangeGraph::new(&ops.actors);
        let authors = Authors::new(&ops.actors);
        let write_frontier = WriteFrontier::new();
        let visibility = visibility::FrontierVisibility::new(
            &write_frontier,
            &change_graph,
            &authors,
            &ops.actors,
        );
        Automerge {
            queue: ChangeQueue::new(SignatureMode::Legacy),
            change_graph,
            authors,
            write_frontier,
            visibility,
            ops,
            deps: Default::default(),
            actor: Actor::Unused(ActorId::random()),
            author: None,
        }
    }

    // TODO(finto): document that there are various invariants on the
    // relationships between OpSet, ChangeGraph, and Authors.
    pub(crate) fn from_parts(
        ops: OpSet,
        change_graph: ChangeGraph,
        authors: Authors,
        write_frontier: HashMap<Author<'static>, Vec<ChangeHash>>,
    ) -> Self {
        let deps = change_graph.heads().collect();
        let write_frontier = WriteFrontier::from(write_frontier);
        let visibility = visibility::FrontierVisibility::new(
            &write_frontier,
            &change_graph,
            &authors,
            &ops.actors,
        );
        let mut doc = Automerge {
            queue: ChangeQueue::new(SignatureMode::Legacy),
            change_graph,
            authors,
            write_frontier,
            visibility,
            ops,
            deps,
            actor: Actor::Unused(ActorId::random()),
            author: None,
        };
        doc.remove_unused_actors(false);
        if doc.mask_hides_existing_ops() {
            doc.ops.recompute_indexes(doc.visible_current().clock());
        }
        doc
    }

    /// Whether the mask hides any op the document currently holds.
    ///
    /// A mask bounds each masked actor at the counter of its last op before
    /// the write-frontier. When that is also the actor's last op overall, the
    /// bound restricts only ops that do not exist yet: indexes built without
    /// the mask are correct under it, while a fresh op by that actor is still
    /// hidden by [`Automerge::transaction_args`].
    fn mask_hides_existing_ops(&self) -> bool {
        self.visibility.mask().is_some_and(|mask| {
            mask.clock()
                .counters()
                .enumerate()
                .any(|(actor, bound)| u64::from(bound) < self.change_graph.max_op_for_actor(actor))
        })
    }

    pub(crate) fn ops_mut(&mut self) -> &mut OpSet {
        &mut self.ops
    }

    pub(crate) fn ops(&self) -> &OpSet {
        &self.ops
    }

    /// The actors known to this document, in canonical (sorted) order.
    pub(crate) fn actors(&self) -> &ActorTable {
        &self.ops.actors
    }

    pub(crate) fn changes(&self) -> &ChangeGraph {
        &self.change_graph
    }

    /// Whether this document has any operations
    pub fn is_empty(&self) -> bool {
        self.change_graph.is_empty() && self.queue.is_empty()
    }

    pub(crate) fn actor_id(&self) -> &ActorId {
        match &self.actor {
            Actor::Unused(id) => id,
            Actor::Cached(idx) => self.ops.get_actor(*idx),
        }
    }

    /// Set the actor id for this document.
    pub fn with_actor(mut self, actor: ActorId) -> Self {
        self.set_actor(actor);
        self
    }

    /// Set the actor id for this document.
    pub fn set_actor(&mut self, actor: ActorId) -> &mut Self {
        match self.ops.lookup_actor(&actor) {
            Some(idx) => self.actor = Actor::Cached(idx),
            None => self.actor = Actor::Unused(actor),
        }
        self
    }

    /// Set the write-frontier for this document.
    pub fn with_write_frontier(
        mut self,
        write_frontier: HashMap<Author<'static>, Vec<ChangeHash>>,
    ) -> Self {
        self.set_write_frontier(write_frontier);
        self
    }

    /// For each [`Author`] key in the map, mask all changes for that author
    /// after their corresponding heads value.
    ///
    /// This clears all previous write-frontier state and publishes the resulting
    /// visibility exactly once, even when `write_frontier` is empty.
    pub fn set_write_frontier(
        &mut self,
        write_frontier: HashMap<Author<'static>, Vec<ChangeHash>>,
    ) {
        self.set_write_frontier_log_patches(write_frontier, &mut PatchLog::inactive())
            .expect("a fresh patch log belongs to any document");
    }

    /// Like [`Automerge::set_write_frontier`], but records the visibility
    /// transition from the old policy to the new one in `log`.
    ///
    /// Returns [`PatchLogMismatch`] if the log's actors are incompatible with
    /// this document, without changing write-frontier.
    ///
    /// [`PatchLogMismatch`]: crate::PatchLogMismatch
    pub fn set_write_frontier_log_patches(
        &mut self,
        write_frontier: HashMap<Author<'static>, Vec<ChangeHash>>,
        log: &mut PatchLog,
    ) -> Result<(), crate::PatchLogMismatch> {
        self.republish_mask(log, |policy| *policy = WriteFrontier::from(write_frontier))
    }

    /// Set the actor id for this document.
    pub fn with_author(mut self, author: Option<Author<'static>>) -> Self {
        self.set_author(author);
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
        if author.as_ref() != self.get_author() {
            self.author = author;
            // TODO: re-use old actors
            self.actor = Actor::Unused(ActorId::random());
        }
        self
    }

    /// Get the current author of this document.
    pub fn get_author(&self) -> Option<&Author<'static>> {
        self.author.as_ref()
    }

    /// Mask all changes made by `author` after the `from` heads.
    ///
    /// Heads not yet present in the document are recorded as pending, and until
    /// they are witnessed the author is hidden entirely.
    ///
    /// Changes beyond a masked author's write-frontier — including this
    /// document's own author — are still recorded and synced but hidden.
    /// Positions in sequence operations refer to the visible document.
    ///
    /// # Errors
    ///
    /// Returns [`PatchLogMismatch`] if the log's actors are incompatible with
    /// this document, without changing the write frontier.
    ///
    /// [`PatchLogMismatch`]: crate::PatchLogMismatch
    pub fn mask_author(
        &mut self,
        author: Author<'static>,
        from: &[ChangeHash],
        log: &mut PatchLog,
    ) -> Result<(), crate::PatchLogMismatch> {
        self.republish_mask(log, |policy| policy.mask_author(author, from.to_vec()))
    }

    /// Reveal the `author`, restoring their changes.
    ///
    /// # Errors
    ///
    /// Returns [`PatchLogMismatch`] if the log's actors are incompatible with
    /// this document, without changing the write-frontier.
    ///
    /// [`PatchLogMismatch`]: crate::PatchLogMismatch
    pub fn reveal_author(
        &mut self,
        author: &Author<'static>,
        log: &mut PatchLog,
    ) -> Result<(), crate::PatchLogMismatch> {
        self.republish_mask(log, |policy| policy.reveal_author(author))
    }

    pub fn signature_mode(&self) -> SignatureMode {
        self.queue.mode()
    }

    pub(crate) fn is_authenticated(&self) -> bool {
        self.queue.mode() == SignatureMode::Authenticated
    }

    /// Apply the history of `chunks`, after whatever this document's signature
    /// mode requires of them.
    ///
    /// This is where the two modes part, and it is the only place they part on
    /// the way in. A legacy document requires nothing of a chunk, so its changes
    /// are queued one by one and each applies as soon as it is causally ready.
    /// An authenticated one requires each chunk to be covered by signatures,
    /// which is why the grouping was kept: the chunk is queued whole and applies
    /// whole, once it is both causally ready and verified.
    fn admit_chunks(
        &mut self,
        chunks: Vec<load::StreamChunk>,
        patch_log: &mut PatchLog,
    ) -> Result<(), AutomergeError> {
        if !self.is_authenticated() {
            let changes = chunks.into_iter().flat_map(|chunk| chunk.changes);
            return self.apply_changes_batch_log_patches_trusted(changes, patch_log);
        }
        // Decode and structurally check each chunk before touching the document,
        // so a stream which fails part way through has not already changed it.
        let mut queued = Vec::with_capacity(chunks.len());
        for chunk in chunks {
            queued.push(QueuedChunk::authenticated(chunk.raw, chunk.changes)?);
        }
        self.enqueue(queued, patch_log)?;
        self.apply_ready(patch_log)?;
        // Admitted history is authenticated, so whatever it unblocks may now form.
        self.form_newly_covered_fragments();
        Ok(())
    }

    pub fn add_signatures(
        &mut self,
        signatures: impl IntoIterator<Item = SignatureInput>,
        patch_log: &mut PatchLog,
    ) -> Result<SignatureReport, AutomergeError> {
        let signatures = signatures.into_iter().map(SignatureInput::into_signature);
        let (queue, doc) = self.split_queue();
        queue.add_signatures(signatures, &doc);
        // Admission forms whatever these signatures unblocked, and does it after
        // any history it admitted is in the graph, which is the only order that
        // sees both the local commits waiting on a signature and the fragments
        // the admitted history completes.
        let applied = self.apply_ready(patch_log)?;
        self.form_newly_covered_fragments();
        Ok(SignatureReport::new(
            applied.authenticated_chunks,
            applied.hashes,
            self.queue.authenticated_chunks(),
        ))
    }

    /// The queue and the view of the document it consults, borrowed apart.
    ///
    /// This is just here to work around the borrow checker. The [`ChangeQueue`]
    /// needs to mutate some fields on [`Automerge`] so this method splits the
    /// [`ChangeQueue`] borrow from the fields that [`ChangeQueue`] needs (which
    /// are in [`DocState`]).
    pub(crate) fn split_queue(&mut self) -> (&mut ChangeQueue, DocState<'_>) {
        let Self {
            queue,
            change_graph,
            authors,
            ops,
            ..
        } = self;
        (
            queue,
            DocState {
                graph: change_graph,
                authors,
                ops,
            },
        )
    }

    /// The detached signatures this document retains.
    pub(crate) fn signatures(&self) -> &SignatureStore {
        self.queue.signatures()
    }

    /// Encode the retained signatures for `hashes` as a detached signature chunk.
    pub(crate) fn signature_chunk_for<I>(&self, hashes: I) -> Vec<u8>
    where
        I: IntoIterator<Item = ChangeHash>,
    {
        let hashes: Vec<_> = hashes.into_iter().collect();
        let signatures: Vec<_> = hashes
            .iter()
            .flat_map(|hash| {
                self.queue
                    .signatures()
                    .signatures_for(hash)
                    .map(move |bytes| SignatureRef {
                        change_hash: *hash,
                        bytes,
                    })
            })
            .collect();
        if signatures.is_empty() {
            Vec::new()
        } else {
            storage::SignatureChunk::encode(signatures)
        }
    }

    /// Apply `update` to the write-frontier policy and publish the result.
    ///
    /// Re-derives the visibility from the updated policy, rebuilds the op-set
    /// indexes under the new mask, and records the change from the current
    /// visibility to the new one as a single transition in `log`. If the
    /// derived mask is unchanged, the rebuild and transition are skipped.
    ///
    /// This is the only path that changes visibility in a way that affects
    /// existing ops. The other writers of `visibility`, actor registration and
    /// actor column shifts, only touch actors that have no ops yet.
    pub(crate) fn republish_mask(
        &mut self,
        log: &mut PatchLog,
        update: impl FnOnce(&mut WriteFrontier),
    ) -> Result<(), crate::PatchLogMismatch> {
        log.transition_to(self, |d| d.visible_current())?;
        update(&mut self.write_frontier);
        let visibility = visibility::FrontierVisibility::new(
            &self.write_frontier,
            &self.change_graph,
            &self.authors,
            self.actors(),
        );
        // An update that leaves the mask as it was (e.g. setting an empty
        // policy on a document that had none, or re-witnessing a resolved
        // boundary) changes no visibility: the indexes are already correct
        // and the first transition already bound the log, so the expensive
        // reindex can be skipped.
        let changed = visibility.mask() != self.visibility.mask();
        self.visibility = visibility;
        if changed {
            let after = self.visible_current();
            self.ops.recompute_indexes(after.clock());
            log.transition_to(self, |_| after)?;
        }
        Ok(())
    }

    /// The active visibility mask, if any author is masked.
    pub(crate) fn mask(&self) -> Option<&Mask> {
        self.visibility.mask()
    }

    /// Return the write-frontier, per [`Author`].
    pub fn get_write_frontier(&self) -> HashMap<Author<'static>, Vec<ChangeHash>> {
        self.write_frontier.get_write_frontier().clone()
    }

    /// Whether `author` is currently masked, e.g. so an application can
    /// disable editing for a masked local author.
    pub fn is_author_masked(&self, author: &Author<'_>) -> bool {
        self.write_frontier.is_author_masked(author)
    }

    /// Record that `actor` belongs to `author` and re-derive the visibility.
    ///
    /// Must be called before `actor` records any ops. Because the actor has no
    /// ops yet, the derived mask cannot change the visibility of anything in
    /// the op set, so no index rebuild is needed.
    ///
    /// Used by [`Automerge::transaction_args`] so that a new actor of a masked
    /// author is entered into the mask before the transaction opens.
    fn register_actor(&mut self, author: Author<'static>, actor: usize) {
        debug_assert_eq!(self.change_graph.seq_for_actor(actor), 0);
        self.authors.assign_author(author, actor);
        self.rederive_visibility();
    }

    /// Bulk form of [`Automerge::register_actor`] for the batch importer:
    /// assign all authors, derive once. Registering nothing derives
    /// nothing: the mask already equals its derivation (an actor inserted
    /// without an author gets `u32::MAX` from the column shift, exactly
    /// what `derive` gives an unmasked actor).
    pub(crate) fn register_actors(
        &mut self,
        pairs: impl IntoIterator<Item = (Author<'static>, usize)>,
    ) {
        let mut pairs = pairs.into_iter().peekable();
        let should_derive = pairs.peek().is_some();
        for (author, actor) in pairs {
            debug_assert_eq!(self.change_graph.seq_for_actor(actor), 0);
            self.authors.assign_author(author, actor);
        }
        if should_derive {
            self.rederive_visibility();
        }
    }

    /// Re-derive `visibility` from the unchanged policy, without an
    /// index rebuild. Sound only when no op's visibility can have changed
    /// (registration of zero-op actors).
    fn rederive_visibility(&mut self) {
        self.visibility = visibility::FrontierVisibility::new(
            &self.write_frontier,
            &self.change_graph,
            &self.authors,
            self.actors(),
        );
    }

    /// Ensure that the current visibility mask is the same as the mask that
    /// would be derived by the current state of the document, i.e. the
    /// visibility mask is not lagging behind.
    ///
    /// This is used in `op_set2::change::batch` to ensure that the document
    /// state has not diverged before applying the incoming changes.
    pub(crate) fn assert_mask_derived(&self) {
        // Compare only the mask: `pending` is intentionally stale between
        // derivations so `update_history` can recognise a boundary it was
        // waiting for (see the note there).
        debug_assert_eq!(
            self.visibility.mask(),
            visibility::FrontierVisibility::new(
                &self.write_frontier,
                &self.change_graph,
                &self.authors,
                self.actors(),
            )
            .mask()
        );
    }

    pub fn get_actors_for_author(&self, author: &Author<'_>) -> Vec<ActorId> {
        self.authors
            .get_actors_for_author(author)
            .filter_map(|idx| self.ops.actors.get(idx).cloned())
            .collect()
    }

    pub fn get_authors(&self) -> &[Author<'static>] {
        self.authors.get_authors()
    }

    pub fn get_author_for_actor(&self, actor: &ActorId) -> Option<Author<'_>> {
        let actor_index = self.ops.actors.lookup(actor)?;
        self.authors.get_author_for_actor(actor_index)
    }

    /// Get the current actor id of this document.
    pub fn get_actor(&self) -> &ActorId {
        match &self.actor {
            Actor::Unused(actor) => actor,
            Actor::Cached(index) => self.ops.get_actor(*index),
        }
    }

    /// Remove the actor at index `actor`, which must have no changes in the
    /// document, from every actor-indexed structure.
    pub(crate) fn remove_actor(&mut self, actor: usize) {
        let (removed, removal) = self.ops.remove_actor(actor);
        self.actor.remove_actor(&removal, removed);
        self.change_graph.remove_actor(&removal);
        self.authors.remove_actor(&removal);
        self.visibility.remove_actor(&removal);
    }

    pub(crate) fn assert_no_unused_actors(&self, panic: bool) {
        if self.ops.actors.len() != self.change_graph.actor_ids().count() {
            let unused = self.change_graph.unused_actors().collect::<Vec<_>>();
            log!("AUTOMERGE :: unused actor found when none expected");
            log!(" :: ops={}", self.ops.actors.len());
            log!(" :: graph={}", self.change_graph.all_actor_ids().count());
            log!(" :: unused={:?}", unused);
            log!(" :: actors={:?}", self.ops.actors);
            assert!(!panic);
        }
    }

    pub(crate) fn remove_unused_actors(&mut self, panic: bool) {
        if panic {
            self.assert_no_unused_actors(cfg!(debug_assertions));
        }

        // remove the offending actors
        while let Some(idx) = self.change_graph.unused_actors().last() {
            self.remove_actor(idx);
        }
    }

    fn get_or_create_actor_index(&mut self) -> usize {
        match &self.actor {
            Actor::Unused(actor) => {
                let index = self.put_actor(actor.clone());
                self.actor = Actor::Cached(index);
                index
            }
            Actor::Cached(index) => *index,
        }
    }

    fn get_actor_index(&self) -> Option<usize> {
        match &self.actor {
            Actor::Unused(_) => None,
            Actor::Cached(index) => Some(*index),
        }
    }

    /// Start a transaction.
    pub fn transaction(&mut self) -> Transaction<'_> {
        let patch_log = PatchLog::inactive();
        let args = self.transaction_args(None);
        Transaction::new(self, args, patch_log)
    }

    /// Start a transaction which records changes in a [`PatchLog`]
    ///
    /// Returns [`PatchLogMismatch`](crate::PatchLogMismatch) if `patch_log` does not belong to
    /// this document. This probably means a patch log created for one document was reused with
    /// another document.
    pub fn transaction_log_patches(
        &mut self,
        mut patch_log: PatchLog,
    ) -> Result<Transaction<'_>, crate::PatchLogMismatch> {
        let args = self.transaction_args(None);
        patch_log.begin_transaction(self, &args)?;
        Ok(Transaction::new(self, args, patch_log))
    }

    /// Start a transaction isolated at a given heads
    ///
    /// Returns [`PatchLogMismatch`](crate::PatchLogMismatch) if `patch_log` does not belong to
    /// this document. This probably means a patch log created for one document was reused with
    /// another document.
    pub fn transaction_at(
        &mut self,
        mut patch_log: PatchLog,
        heads: &[ChangeHash],
    ) -> Result<Transaction<'_>, crate::PatchLogMismatch> {
        let args = self.transaction_args(Some(heads));
        patch_log.begin_transaction(self, &args)?;
        Ok(Transaction::new(self, args, patch_log))
    }

    /// Start a transaction that owns the document, consuming `self`.
    ///
    /// This is useful when the transaction must be `'static` (e.g. storing across an FFI
    /// boundary or in a struct that requires `'static`). The document is returned when the
    /// transaction is committed or rolled back.
    ///
    /// # Arguments
    /// * `patch_log` - An optional [`PatchLog`] to log the changes in this transaction to
    /// * `heads` - An optional set of heads to isolate this transaction at, or `None` to use the
    ///   current heads of the document
    ///
    /// Returns [`PatchLogMismatch`](crate::PatchLogMismatch) if `patch_log` does not belong to
    /// this document. This probably means a patch log created for one document was reused with
    /// another document.
    pub fn into_transaction(
        self,
        patch_log: Option<PatchLog>,
        heads: Option<&[ChangeHash]>,
    ) -> Result<OwnedTransaction, crate::PatchLogMismatch> {
        OwnedTransaction::new(self, patch_log, heads)
    }

    pub(crate) fn transaction_args(&mut self, heads: Option<&[ChangeHash]>) -> TransactionArgs {
        let actor_index;
        let seq;
        let mut deps;
        let scope;
        match heads {
            Some(heads) => {
                deps = heads.to_vec();
                let isolation = self.isolate_actor(heads);
                actor_index = isolation.actor_index;
                seq = isolation.seq;
                scope = Some(isolation.clock);
            }
            None => {
                actor_index = self.get_or_create_actor_index();
                seq = self.change_graph.seq_for_actor(actor_index) + 1;
                deps = self.get_heads();
                scope = None;
                if seq > 1 {
                    let last_hash = self.get_hash(actor_index, seq - 1).unwrap();
                    if !deps.contains(&last_hash) {
                        deps.push(last_hash);
                    }
                }
            }
        }

        // A local change claims this actor sequence. Any queued change at the
        // same or a later sequence belongs to an incompatible actor branch;
        // retaining it would allow save() to encode duplicate sequence numbers.
        let actor = self.ops.actors[actor_index].clone();
        self.queue.remove_actor_branch_from(&actor, seq);

        let start_op = NonZeroU64::MIN.saturating_add(self.change_graph.max_op());
        let author = if seq == 1 { self.author.clone() } else { None };
        // If the author is part of the write-frontier, assign the actor to the
        // author now, entering the fresh actor into the mask at the author's
        // boundary. Otherwise, no mask entry is needed, and the committed
        // change still carries the author footer.
        if let Some(author) = &author {
            if self.is_author_masked(author) {
                self.register_actor(author.clone(), actor_index);
            }
        }
        let masked = self
            .visibility
            .hides(&OpId::new(start_op.get(), actor_index));
        TransactionArgs {
            actor_index,
            seq,
            start_op,
            deps,
            scope,
            author,
            masked,
        }
    }

    #[cfg(test)]
    pub(crate) fn save_checkpoint(&self) -> std::collections::HashMap<&'static str, Vec<u8>> {
        self.ops.save_checkpoint()
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
        // FIXME
        self.transact_with_impl(Some(c), f)
    }

    fn transact_with_impl<F, O, E, C>(&mut self, c: Option<C>, f: F) -> transaction::Result<O, E>
    where
        F: FnOnce(&mut Transaction<'_>) -> Result<O, E>,
        C: FnOnce(&O) -> CommitOptions,
    {
        let mut tx = self.transaction();
        let result = f(&mut tx);
        match result {
            Ok(result) => {
                let (hash, patch_log) = if let Some(c) = c {
                    let commit_options = c(&result);
                    tx.commit_with(commit_options)
                } else {
                    tx.commit()
                };
                Ok(Success {
                    result,
                    hash,
                    patch_log,
                })
            }
            Err(error) => Err(Failure {
                error,
                cancelled: tx.rollback(),
            }),
        }
    }

    /// Run a transaction on this document in a closure, collecting patches, automatically handling commit or rollback
    /// afterwards.
    ///
    /// The collected patches are available in the return value of [`Transaction::commit()`]
    pub fn transact_and_log_patches<F, O, E>(&mut self, f: F) -> transaction::Result<O, E>
    where
        F: FnOnce(&mut Transaction<'_>) -> Result<O, E>,
    {
        self.transact_and_log_patches_with_impl(None::<&dyn Fn(&O) -> CommitOptions>, f)
    }

    /// Like [`Self::transact_and_log_patches()`] but with a function for generating the commit options
    pub fn transact_and_log_patches_with<F, O, E, C>(
        &mut self,
        c: C,
        f: F,
    ) -> transaction::Result<O, E>
    where
        F: FnOnce(&mut Transaction<'_>) -> Result<O, E>,
        C: FnOnce(&O) -> CommitOptions,
    {
        self.transact_and_log_patches_with_impl(Some(c), f)
    }

    fn transact_and_log_patches_with_impl<F, O, E, C>(
        &mut self,
        c: Option<C>,
        f: F,
    ) -> transaction::Result<O, E>
    where
        F: FnOnce(&mut Transaction<'_>) -> Result<O, E>,
        C: FnOnce(&O) -> CommitOptions,
    {
        let mut tx = self
            .transaction_log_patches(PatchLog::active())
            .expect("new patch log should not mismatch");
        let result = f(&mut tx);
        match result {
            Ok(result) => {
                let (hash, history) = if let Some(c) = c {
                    let commit_options = c(&result);
                    tx.commit_with(commit_options)
                } else {
                    tx.commit()
                };
                Ok(Success {
                    result,
                    hash,
                    patch_log: history,
                })
            }
            Err(error) => Err(Failure {
                error,
                cancelled: tx.rollback(),
            }),
        }
    }

    /// Generate an empty change
    ///
    /// The main reason to do this is if you want to create a "merge commit", which is a change
    /// that has all the current heads of the document as dependencies.
    pub fn empty_commit(&mut self, opts: CommitOptions) -> ChangeHash {
        // No patch log is recorded for an empty change, so migrate a throwaway one.
        let args = self.transaction_args(None);
        Transaction::empty(self, args, opts)
    }

    /// Fork this document at the current point for use by a different actor.
    ///
    /// This will create a new actor ID for the forked document
    pub fn fork(&self) -> Self {
        let mut f = self.clone();
        f.set_actor(ActorId::random());
        f
    }

    /// Fork this document at the given heads
    ///
    /// This will create a new actor ID for the forked document.
    ///
    /// Like [`Automerge::fork`], the process-local write-frontier policy is
    /// carried into the fork: a masked author stays hidden in the forked
    /// document. A boundary that is not reachable from `heads` is simply
    /// pending in the fork.
    pub fn fork_at(&self, heads: &[ChangeHash]) -> Result<Self, AutomergeError> {
        let mut seen = HashSet::new();
        let mut heads = heads
            .iter()
            .filter(|head| seen.insert(**head))
            .copied()
            .collect::<Vec<_>>();
        let mut hashes = vec![];
        while let Some(hash) = heads.pop() {
            if !self.change_graph.has_change(&hash) {
                return Err(AutomergeError::InvalidHash(hash));
            }
            for dep in self.change_graph.deps_for_hash(&hash) {
                if seen.insert(dep) {
                    heads.push(dep);
                }
            }
            hashes.push(hash);
        }
        let mut f = Self::new_with_encoding(self.text_encoding());
        f.set_actor(ActorId::random());
        f.queue = self.queue.fork();
        let changes = self.get_changes_by_hashes(hashes.into_iter().rev())?;
        f.apply_changes_batch_log_patches_trusted(changes, &mut PatchLog::inactive())?;
        // The replay formed a fragment at every level it met, as though every
        // change were authenticated; only the signatures decide.
        f.rebuild_fragment_index();
        f.set_write_frontier(self.get_write_frontier());
        Ok(f)
    }

    pub(crate) fn get_changes_by_hashes<I>(&self, hashes: I) -> Result<Vec<Change>, AutomergeError>
    where
        I: IntoIterator<Item = ChangeHash>,
    {
        ChangeCollector::for_hashes(&self.ops, &self.change_graph, hashes)
    }

    pub(crate) fn exid_to_opid(&self, id: &ExId) -> Result<OpId, AutomergeError> {
        match id {
            ExId::Root => Ok(OpId::new(0, 0)),
            ExId::Id(ctr, actor, idx) => {
                let opid = if self.ops.get_actor_safe(*idx) == Some(actor) {
                    OpId::new(*ctr, *idx)
                } else if let Some(backup_idx) = self.ops.lookup_actor(actor) {
                    OpId::new(*ctr, backup_idx)
                } else {
                    return Err(AutomergeError::InvalidObjId(id.to_string()));
                };
                Ok(opid)
            }
        }
    }

    pub(crate) fn get_obj_meta(&self, id: ObjId) -> Result<ObjMeta, AutomergeError> {
        if id.is_root() {
            Ok(ObjMeta::root())
        } else if let Some(typ) = self.ops.object_type(&id) {
            Ok(ObjMeta { id, typ })
        } else {
            Err(AutomergeError::NotAnObject)
        }
    }

    pub(crate) fn op_cursor_to_opid(
        &self,
        cursor: &OpCursor,
        clock: Option<&Clock>,
    ) -> Result<OpId, AutomergeError> {
        if let Some(idx) = self.ops.lookup_actor(&cursor.actor) {
            let opid = OpId::new(cursor.ctr, idx);
            match clock {
                Some(clock) if !clock.covers(&opid) => {
                    Err(AutomergeError::InvalidCursor(Cursor::Op(cursor.clone())))
                }
                _ => Ok(opid),
            }
        } else {
            Err(AutomergeError::InvalidCursor(Cursor::Op(cursor.clone())))
        }
    }

    pub(crate) fn exid_to_obj(&self, id: &ExId) -> Result<ObjMeta, AutomergeError> {
        let opid = self.exid_to_opid(id)?;
        let obj = ObjId(opid);
        self.get_obj_meta(obj)
    }

    pub(crate) fn id_to_exid(&self, id: OpId) -> ExId {
        self.ops.id_to_exid(id)
    }

    pub fn diff_opset(&self, other: &Self) -> Result<(), AutomergeError> {
        let (ops_meta1, ops_out1) = self.ops.export();
        let (ops_meta2, ops_out2) = other.ops.export();
        if ops_meta1 != ops_meta2 {
            let specs: std::collections::BTreeSet<_> = ops_meta1
                .0
                .iter()
                .chain(ops_meta2.0.iter())
                .map(|c| c.spec())
                .collect();
            for s in specs {
                let d1 = ops_meta1
                    .0
                    .iter()
                    .find(|c| c.spec() == s)
                    .map(|c| c.data())
                    .unwrap_or(0..0);
                let d2 = ops_meta2
                    .0
                    .iter()
                    .find(|c| c.spec() == s)
                    .map(|c| c.data())
                    .unwrap_or(0..0);
                let d1 = &ops_out1[d1];
                let d2 = &ops_out2[d2];
                if d1 != d2 {
                    log!(" s={:?}|{:?} ", s.id(), s.col_type());
                    log!(" {:?} ", d1);
                    log!(" {:?} ", d2);
                }
            }
        }
        Ok(())
    }

    /// Load a document.
    pub fn load(data: &[u8]) -> Result<Self, AutomergeError> {
        Self::load_with_options(data, Default::default())
    }

    /// Load a document without verifying the head hashes
    ///
    /// This is useful for debugging as it allows you to examine a corrupted document.
    pub fn load_unverified_heads(data: &[u8]) -> Result<Self, AutomergeError> {
        Self::load_with_options(
            data,
            LoadOptions {
                verification_mode: VerificationMode::DontCheck,
                ..Default::default()
            },
        )
    }

    /// Load a document, with options
    ///
    /// # Arguments
    /// * `data` - The data to load
    /// * `on_error` - What to do if the document is only partially loaded. This can happen if some
    ///                prefix of `data` contains valid data.
    /// * `mode` - Whether to verify the head hashes after loading
    /// * `patch_log` - A [`PatchLog`] to log the changes required to materialize the current state of
    ///                 the document once loaded
    #[deprecated(since = "0.5.2", note = "Use `load_with_options` instead")]
    #[tracing::instrument(skip(data), err)]
    pub fn load_with(
        data: &[u8],
        on_error: OnPartialLoad,
        mode: VerificationMode,
        patch_log: &mut PatchLog,
    ) -> Result<Self, AutomergeError> {
        Self::load_with_options(
            data,
            LoadOptions::new()
                .on_partial_load(on_error)
                .verification_mode(mode)
                .patch_log(patch_log),
        )
    }

    /// Load a document, with options
    ///
    /// # Arguments
    /// * `data` - The data to load
    /// * `options` - The options to use when loading
    #[tracing::instrument(skip(data), err)]
    pub fn load_with_options(
        data: &[u8],
        options: LoadOptions<'_>,
    ) -> Result<Self, AutomergeError> {
        Self::load_with_options_and_mark_validation(
            data,
            options,
            load::MarkOrderValidation::Validate,
        )
    }

    /// Best-effort rescue for documents which fail strict loading.
    ///
    /// This returns only the current hydrated value and does not preserve the original change graph.
    pub fn rescue(data: &[u8]) -> Result<hydrate::Value, AutomergeError> {
        Ok(Self::load_with_options_and_mark_validation(
            data,
            Default::default(),
            load::MarkOrderValidation::AllowInvalid,
        )?
        .hydrate(None))
    }

    fn load_with_options_and_mark_validation(
        data: &[u8],
        mut options: LoadOptions<'_>,
        mark_order: load::MarkOrderValidation,
    ) -> Result<Self, AutomergeError> {
        if data.is_empty() {
            tracing::trace!("no data, initializing empty document");
            let mut doc = Self::new_with_encoding(options.text_encoding);
            doc.queue = ChangeQueue::new(options.signature_mode);
            let mut doc = doc.with_author(options.author);
            if !options.write_frontier.is_empty() {
                doc.set_write_frontier(options.write_frontier);
            }
            return Ok(doc);
        }
        tracing::trace!("loading first chunk");
        let (remaining, first_chunk) = storage::Chunk::parse(storage::parse::Input::new(data))
            .map_err(|e| load::Error::Parse(Box::new(e)))?;
        if !first_chunk.checksum_valid() {
            return Err(load::Error::BadChecksum.into());
        }

        let mut changes = vec![];

        let mut queue = ChangeQueue::with_signatures(
            options.signature_mode,
            std::mem::take(&mut options.signatures),
        );
        let mut first_chunk_was_doc = false;
        // If `first_chunk` is `storage::Chunk::Document`, then these
        // write-frontier is consumed. For every other variant, the
        // write-frontier is applied after the match.
        let mut write_frontier = Some(options.write_frontier);
        let mut am = match first_chunk {
            storage::Chunk::Document(d) => {
                tracing::trace!("first chunk is document chunk, inflating");
                first_chunk_was_doc = true;
                match d.reconstruct(
                    options.verification_mode,
                    options.text_encoding,
                    write_frontier.take().unwrap_or_default(),
                ) {
                    Ok(doc) => doc,
                    Err(ReconstructError::InvalidMarkOrderDoc {
                        doc,
                        error_message: _,
                    }) if mark_order.allows_invalid() => *doc,
                    // Same policy violation, same error as the apply path.
                    Err(ReconstructError::AuthorOnNonInitialSeq(seq, actor)) => {
                        return Err(AutomergeError::AuthorOnNonInitialSeq(seq, actor))
                    }
                    Err(e) => return Err(load::Error::InflateDocument(Box::new(e)).into()),
                }
            }
            storage::Chunk::Change(stored_change) => {
                tracing::trace!("first chunk is change chunk");
                changes.push(
                    Change::new_from_unverified(stored_change.into_owned(), None)
                        .map_err(|e| load::Error::InvalidChangeColumns(Box::new(e)))?,
                );
                Self::new_with_encoding(options.text_encoding)
            }
            storage::Chunk::Signatures(chunk) => {
                tracing::trace!("first chunk is a detached signature chunk");
                queue.extend_signatures(chunk.into_signatures());
                Self::new_with_encoding(options.text_encoding)
            }
            storage::Chunk::Bundle(bundle) => {
                tracing::trace!("first chunk is change chunk");
                let bundle = Bundle::new_from_unverified(bundle.into_owned())
                    .map_err(|e| load::Error::InvalidBundleColumn(Box::new(e)))?;
                let bundle_changes = bundle
                    .to_changes()
                    .map_err(|e| load::Error::InvalidBundleChange(Box::new(e)))?;
                changes.extend(bundle_changes);
                Self::new_with_encoding(options.text_encoding)
            }
            storage::Chunk::CompressedChange(stored_change, compressed) => {
                tracing::trace!("first chunk is compressed change");
                changes.push(
                    Change::new_from_unverified(
                        stored_change.into_owned(),
                        Some(compressed.into_owned()),
                    )
                    .map_err(|e| load::Error::InvalidChangeColumns(Box::new(e)))?,
                );
                Self::new_with_encoding(options.text_encoding)
            }
        };
        am.queue = queue;
        let rest = remaining.reset().unconsumed_bytes();
        let stream =
            load::read_chunk_stream(rest, options.text_encoding, &am.change_graph, mark_order);
        if let Some(error) = stream.error {
            if options.on_partial_load == OnPartialLoad::Error {
                return Err(error.into());
            }
            tracing::warn!(whole_chunks = stream.chunks.len(), err = ?error, "partial load");
        }
        am.queue.extend_signatures(stream.signatures);

        let mut chunks = Vec::with_capacity(stream.chunks.len() + 1);
        if !changes.is_empty() {
            // The first chunk was parsed above to decide what kind of load this
            // is, so it is reunited with the rest here. Its own bytes are the
            // payload minus what is left to read.
            chunks.push(load::StreamChunk {
                raw: data[..data.len() - rest.len()].to_vec(),
                changes: std::mem::take(&mut changes),
            });
        }
        chunks.extend(stream.chunks);
        am.admit_chunks(chunks, &mut PatchLog::inactive())?;
        // A document chunk is reconstructed before the mode and the signatures are
        // known, so its index was built as though every change were
        // authenticated.
        am.rebuild_fragment_index();

        // Only allow missing deps if the first chunk was a document chunk
        // See https://github.com/automerge/automerge/pull/599#issuecomment-1549667472
        if !am.queue.is_empty()
            && !first_chunk_was_doc
            && options.on_partial_load == OnPartialLoad::Error
        {
            return Err(AutomergeError::MissingDeps);
        }

        if let StringMigration::ConvertToText = options.string_migration {
            am.convert_scalar_strings_to_text()?;
        }
        // For non-document chunks the write-frontier was not threaded through
        // `reconstruct`; apply them now, before any current-state patches are
        // logged, so the logged state is masked.
        if let Some(write_frontier) = write_frontier.take() {
            if !write_frontier.is_empty() {
                am.set_write_frontier(write_frontier);
            }
        }
        if let Some(patch_log) = options.patch_log {
            if patch_log.is_active() {
                am.log_current_state(ObjMeta::root(), patch_log, true);
            }
        }
        Ok(am.with_author(options.author))
    }

    /// Create the patches from a [`PatchLog`]
    ///
    /// See the documentation for [`PatchLog`] for more details on this
    pub fn make_patches(&self, patch_log: &mut PatchLog) -> Vec<Patch> {
        patch_log.make_patches(self)
    }

    /// Get a set of [`Patch`]es which materialize the current state of the document
    ///
    /// This is a convienence method for [`doc.diff(&[], current_heads)`][diff]
    ///
    /// [diff]: Self::diff()
    pub fn current_state(&self) -> Vec<Patch> {
        let mut patch_log = PatchLog::active();
        self.log_current_state(ObjMeta::root(), &mut patch_log, true);
        patch_log.make_patches(self)
    }

    /// Load an incremental save of a document.
    ///
    /// Unlike [`Self::load()`] this imports changes into an existing document. It will work with
    /// both the output of [`Self::save()`] and [`Self::save_after()`]
    ///
    /// The return value is the number of ops which were applied, this is not useful and will
    /// change in future.
    pub fn load_incremental(&mut self, data: &[u8]) -> Result<usize, AutomergeError> {
        self.load_incremental_log_patches(data, &mut PatchLog::inactive())
    }

    /// Like [`Self::load_incremental()`] but log the changes to the current state of the document
    /// to [`PatchLog`]
    pub fn load_incremental_log_patches(
        &mut self,
        data: &[u8],
        patch_log: &mut PatchLog,
    ) -> Result<usize, AutomergeError> {
        if self.is_empty() {
            let existing_signatures = self
                .queue
                .signatures()
                .iter()
                .map(SignatureRef::to_owned)
                .collect::<Vec<_>>();
            let mut doc = Self::load_with_options(
                data,
                LoadOptions::new()
                    .text_encoding(self.text_encoding())
                    .on_partial_load(OnPartialLoad::Ignore)
                    .verification_mode(VerificationMode::Check)
                    .signature_mode(self.queue.mode())
                    // Signatures held before this call still count towards what
                    // the incoming history needs, which is what lets a signature
                    // arrive ahead of the change it covers.
                    .signatures(existing_signatures),
            )?;
            // Ensure that the author, actor, and write-frontier state are carried
            // over for an empty document.
            doc.set_author(self.get_author().cloned());
            doc = doc.with_actor(self.actor_id().clone());
            doc.set_write_frontier(self.get_write_frontier());
            // Finish the log's pending segment against the OLD document
            // before it is replaced: its observed view and events name this
            // document's actors and views, which cannot be resolved once
            // `self` is the loaded document.
            patch_log.transition_to(self, |d| d.visible_current())?;
            if patch_log.is_active() {
                doc.log_current_state(ObjMeta::root(), patch_log, true);
            }
            *self = doc;
            return Ok(self.ops.len());
        }
        let stream = load::read_chunk_stream(
            data,
            self.text_encoding(),
            &self.change_graph,
            load::MarkOrderValidation::Validate,
        );
        if let Some(error) = &stream.error {
            tracing::warn!(whole_chunks = stream.chunks.len(), err = ?error, "partial load");
        }
        self.queue.extend_signatures(stream.signatures);
        let start = self.ops.len();
        self.admit_chunks(stream.chunks, patch_log)?;
        Ok(self.ops.len() - start)
    }

    pub(crate) fn log_current_state(
        &self,
        obj: ObjMeta,
        patch_log: &mut PatchLog,
        recursive: bool,
    ) {
        let path_map = patch_log
            .record(
                self,
                |events| {
                    let clock = ClockRange::to(self.read_current());
                    DiffIter::log(self, obj, clock, events, recursive)
                },
                |d| d.visible_current(),
            )
            .expect("patch log actors must match the document when binding a view");
        patch_log.path_hint(path_map);
    }

    /// Apply changes to this document.
    ///
    /// This is idempotent in the sense that if a change has already been applied it will be
    /// ignored.
    pub fn apply_changes(
        &mut self,
        changes: impl IntoIterator<Item = Change> + Clone,
    ) -> Result<(), AutomergeError> {
        self.apply_changes_log_patches(changes, &mut PatchLog::inactive())
    }

    /// Like [`Self::apply_changes()`] but log the resulting changes to the current state of the
    /// document to `patch_log`
    pub fn apply_changes_log_patches<I: IntoIterator<Item = Change> + Clone>(
        &mut self,
        changes: I,
        patch_log: &mut PatchLog,
    ) -> Result<(), AutomergeError> {
        self.apply_changes_batch_log_patches(changes, patch_log)
    }

    /// Takes all the changes in `other` which are not in `self` and applies them
    pub fn merge(&mut self, other: &mut Self) -> Result<Vec<ChangeHash>, AutomergeError> {
        self.merge_and_log_patches(other, &mut PatchLog::inactive())
    }

    /// Takes all the changes in `other` which are not in `self` and applies them whilst logging
    /// the resulting changes to the current state of the document to `patch_log`
    pub fn merge_and_log_patches(
        &mut self,
        other: &mut Self,
        patch_log: &mut PatchLog,
    ) -> Result<Vec<ChangeHash>, AutomergeError> {
        // TODO: Make this fallible and figure out how to do this transactionally
        let changes = self.get_changes_added(other);
        tracing::trace!(changes=?changes.iter().map(|c| c.hash()).collect::<Vec<_>>(), "merging new changes");
        self.apply_changes_log_patches(changes, patch_log)?;
        Ok(self.get_heads())
    }

    /// EXPERIMENTAL: Write the set of changes in `hashes` to a "bundle"
    ///
    /// A "bundle" is a compact representation of a set of changes which uses
    /// the same compression tricks as the document encoding we use in
    /// [`Automerge::save`].
    ///
    /// This is an experimental API, the bundle format is still subject to change
    /// and so should not be used in production just yet.
    pub fn bundle<I>(&self, hashes: I) -> Result<Bundle, AutomergeError>
    where
        I: IntoIterator<Item = ChangeHash>,
    {
        Bundle::for_hashes(&self.ops, &self.change_graph, hashes)
    }

    /// Save the entirety of this document in a compact form.
    pub fn save_with_options(&self, options: SaveOptions) -> Vec<u8> {
        self.assert_no_unused_actors(true);
        let doc = Document::new(&self.ops, &self.change_graph, options.compress());
        let mut bytes = doc.into_bytes();

        if options.retain_orphans {
            // Queued history is history this document holds and cannot apply
            // yet, whether it waits for a dependency or a signature. Each chunk
            // goes back as it arrived, after the document chunk rather than
            // inside it, so a load makes the same admission decision again
            // rather than taking it on trust.
            for queued in self.queue.chunks() {
                bytes.extend(queued.raw_bytes());
            }
        }
        if !self.queue.signatures().is_empty() {
            bytes.extend(storage::SignatureChunk::encode(
                self.queue.signatures().iter(),
            ));
        }
        bytes
    }

    #[cfg(test)]
    pub fn debug_cmp(&self, other: &Self) {
        self.ops.debug_cmp(&other.ops);
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
        self.save_after_reporting_tail(heads).bytes
    }

    /// The history after `heads`, and whether that is all of it.
    ///
    /// An authenticated save withholds history it cannot authenticate, so a
    /// caller tracking a cursor needs to know whether what it got was the whole
    /// tail. That is a fact about the export, so it comes back from the export
    /// rather than from a second question put to the document.
    pub(crate) fn save_after_reporting_tail(&self, heads: &[ChangeHash]) -> SavedTail {
        if self.is_authenticated() {
            return self.export_fragments_after(heads);
        }
        let changes = self.get_changes(heads);
        let mut bytes = vec![];
        let mut hashes = Vec::new();
        for c in changes {
            bytes.extend(c.raw_bytes());
            hashes.push(c.hash());
        }
        bytes.extend(self.signature_chunk_for(hashes));
        // A legacy export withholds nothing: it emits unsigned changes too.
        SavedTail {
            bytes,
            complete: true,
        }
    }

    fn export_fragments_after(&self, heads: &[ChangeHash]) -> SavedTail {
        let mut wanted: BTreeSet<ChangeHash> =
            self.get_changes(heads).iter().map(Change::hash).collect();
        if wanted.is_empty() {
            return SavedTail {
                bytes: Vec::new(),
                complete: true,
            };
        }
        let covering: Vec<Fragment> = self
            .all_fragments(..)
            .into_iter()
            .filter(|fragment| fragment.members.iter().any(|hash| wanted.contains(hash)))
            .collect();
        // Which of the wanted changes no exportable fragment holds: an unsigned
        // tail is in no fragment at all, because enumeration starts from the
        // exportable frontier.
        for fragment in &covering {
            for member in &fragment.members {
                wanted.remove(member);
            }
        }
        let offered = covering.len();
        let bundles = self.bundle_fragments(covering);
        // A fragment can still be dropped here, having been offered: coverage
        // puts it in the cover, but exporting it needs a signature on each of its
        // own actor maxima, which is a stronger thing.
        let complete = wanted.is_empty() && bundles.len() == offered;
        SavedTail {
            bytes: bundles.concat(),
            complete,
        }
    }

    /// Filter the changes down to those that are not transitive dependencies of the heads.
    ///
    /// Thus a graph with these heads has not seen the remaining changes.
    pub(crate) fn filter_changes(
        &self,
        heads: &[ChangeHash],
        changes: &mut BTreeSet<ChangeHash>,
    ) -> Result<(), AutomergeError> {
        let heads = heads
            .iter()
            .filter(|hash| self.has_change(hash))
            .copied()
            .collect::<Vec<_>>();

        self.change_graph.remove_ancestors(changes, &heads);

        Ok(())
    }

    /// Get the last change this actor made to the document.
    pub fn get_last_local_change(&self) -> Option<Change> {
        let actor = self.get_actor_index()?;
        let seq = self.change_graph.seq_for_actor(actor);
        let hash = self.change_graph.get_hash_for_actor_seq(actor, seq).ok()?;
        self.get_change_by_hash(&hash)
    }

    pub(crate) fn clock_range(
        &self,
        before: &[ChangeHash],
        after: &[ChangeHash],
    ) -> ClockRange<'static> {
        ClockRange::between(self.visible(before), self.visible(after))
    }

    /// The clock a read at `heads` observes: causal(heads) ∩ mask.
    pub(crate) fn visible(&self, heads: &[ChangeHash]) -> VisibleClock {
        VisibleClock::new(self.change_graph.clock_at(heads), self.mask())
    }

    /// The visible clock at the current heads.
    pub(crate) fn visible_current(&self) -> VisibleClock {
        self.visible(&self.get_heads())
    }

    /// Return a [`ReadAt`] that depends on the provided `heads`.
    ///
    /// If `heads` is `None` or is the current set of heads, then a current
    /// read ([`ReadAt::current`]) is returned, carrying the active mask.
    ///
    /// Otherwise, a historical snapshot ([`ReadAt::at`]) is taken with the
    /// provided `heads`.
    pub(crate) fn read_at(&self, heads: Option<&[ChangeHash]>) -> ReadAt<'_> {
        match heads {
            Some(h) if !self.change_graph.heads_are_current(h) => {
                ReadAt::at(Cow::Owned(self.visible(h)))
            }
            _ => self.read_current(),
        }
    }

    /// Return a [`ReadAt`] for the current state of the document, carrying
    /// the active write-frontier [`Mask`], if any.
    ///
    /// Equivalent to `doc.read_at(None)`.
    pub(crate) fn read_current(&self) -> ReadAt<'_> {
        ReadAt::current(self.mask().map(Cow::Borrowed))
    }

    /// Read at the given [`VisibleClock`], taking the indexed fast path when the
    /// given clock is the current visible clock.
    ///
    /// Use this only for a view that was fully resolved against the committed
    /// document, such as a patch log's observed view. Do not use it while a
    /// transaction is open or for an isolation scope: in both cases the indexes
    /// can include ops that the clock does not, so the fast path would expose
    /// them. Use [`Self::read_scoped`] there instead.
    pub(crate) fn read_visible<'a>(&'a self, v: &'a VisibleClock) -> ReadAt<'a> {
        if *v == self.visible_current() {
            self.read_current()
        } else {
            ReadAt::at(Cow::Borrowed(v))
        }
    }

    /// Read at a caller-held scope, which is always a historical snapshot.
    ///
    /// This is the producer for in-flight-transaction scopes: pending ops are
    /// indexed but absent from the graph heads, and a masked isolated actor's
    /// scope can equal [`Automerge::visible_current`], so the fast paths of
    /// [`Automerge::read_at`] and [`Automerge::read_visible`] must not apply.
    pub(crate) fn read_scoped<'a>(&self, scope: Cow<'a, VisibleClock>) -> ReadAt<'a> {
        ReadAt::at(scope)
    }

    fn get_isolated_actor_index(&mut self, level: usize) -> usize {
        if level == 0 {
            self.get_or_create_actor_index()
        } else {
            let base_actor = self.get_actor();
            let new_actor = base_actor.with_concurrency(level);
            self.put_actor(new_actor)
        }
    }

    pub(crate) fn isolate_actor(&mut self, heads: &[ChangeHash]) -> Isolation {
        let mut actor_index = self.get_isolated_actor_index(0);
        let mut clock = self.visible(heads);

        for i in 1.. {
            let max_op = self.change_graph.max_op_for_actor(actor_index);
            // Only actors whose latest change is in the isolation scope can safely
            // advance their sequence under it. Masked actors can also be selected,
            // but their counter is not isolated: doing so would unmask their own
            // ops. A fresh actor belongs to a masked local author if `transaction_args`
            // would mark it masked.
            if max_op == 0 || clock.covers(&OpId::new(max_op, actor_index)) {
                let masked = self.visibility.hides(&OpId::new(max_op + 1, actor_index))
                    || (max_op == 0
                        && self.author.as_ref().is_some_and(|a| {
                            self.write_frontier
                                .get_write_frontier_for_author(a)
                                .is_some()
                        }));
                if !masked {
                    clock.isolate(actor_index);
                }
                break;
            }
            actor_index = self.get_isolated_actor_index(i);
            // need to recompute the clock b/c the actor indexes may have changed
            clock = self.visible(heads);
        }

        let seq = self.change_graph.seq_for_actor(actor_index) + 1;

        Isolation {
            actor_index,
            seq,
            clock,
        }
    }

    fn get_hash(&self, actor: usize, seq: u64) -> Result<ChangeHash, AutomergeError> {
        self.change_graph.get_hash_for_actor_seq(actor, seq)
    }

    fn signed_frontier(&self) -> HashMap<usize, u64> {
        signatures::signed_frontier(self.queue.signatures(), &self.change_graph, &self.authors)
    }

    /// A test for whether a change is authenticated, which reads the signed
    /// frontier once.
    ///
    /// In legacy mode nothing is required to be authenticated, so everything is
    /// covered. That is deliberately vacuous rather than meaningless: it is what
    /// lets the export paths ask one question instead of branching on the mode,
    /// since a legacy document's exportable history is simply all of it.
    ///
    /// Callers asking about more than one change should hold on to this rather
    /// than calling [`Self::is_covered`] repeatedly, which rebuilds the frontier
    /// each time.
    fn covered_predicate(&self) -> impl Fn(&ChangeHash) -> bool + '_ {
        let frontier = self.is_authenticated().then(|| self.signed_frontier());
        move |hash: &ChangeHash| {
            let Some(frontier) = &frontier else {
                return true;
            };
            self.change_graph
                .actor_seq_for_hash(hash)
                .is_some_and(|(actor, seq)| frontier.get(&actor).is_some_and(|max| *max >= seq))
        }
    }

    /// Whether `hash` is authenticated: at or below its actor's signed frontier,
    /// or anything at all in legacy mode.
    fn is_covered(&self, hash: &ChangeHash) -> bool {
        self.covered_predicate()(hash)
    }

    /// The changes this document holds which are not authenticated.
    ///
    /// Per actor, everything above its signed frontier. Enumerated that way rather
    /// than by scanning the document, because the answer is a short tail.
    pub(crate) fn unsigned_changes(&self) -> impl Iterator<Item = ChangeHash> + '_ {
        let frontier = self.signed_frontier();
        self.change_graph
            .actor_ids()
            .collect::<Vec<_>>()
            .into_iter()
            .flat_map(move |actor| {
                let signed = frontier.get(&actor).copied().unwrap_or(0);
                self.change_graph.hashes_for_actor_above(actor, signed)
            })
    }

    /// The heads of the authenticated part of this document.
    ///
    /// Walks back from the document's heads over whatever is not covered, so the
    /// result is the frontier of history which may be exported. Bounded by the
    /// uncovered tail rather than the document.
    fn exportable_heads(&self) -> Vec<ChangeHash> {
        let covered = self.covered_predicate();
        let mut heads = BTreeSet::new();
        let mut seen = BTreeSet::new();
        let mut stack: Vec<ChangeHash> = self.get_heads();
        while let Some(hash) = stack.pop() {
            if !seen.insert(hash) {
                continue;
            }
            if covered(&hash) {
                heads.insert(hash);
            } else {
                stack.extend(self.change_graph.deps(&hash));
            }
        }
        heads.into_iter().collect()
    }

    /// Rebuild the fragment index now that this document knows what is covered.
    ///
    /// History replayed or reconstructed into a document before its signature mode
    /// was set has an index built as though all of it were authenticated. That
    /// would offer an unsigned local commit's fragment as exportable, and in
    /// absorbing the signed history beneath it, make that history unexportable
    /// too. Rebuilding restores the index the history would have produced.
    fn rebuild_fragment_index(&mut self) {
        // A skip, not a different path: with every change covered, a legacy
        // rebuild selects every fragment the index already holds.
        if !self.is_authenticated() {
            return;
        }
        let may_form = signatures::covered_fragments(
            &self.change_graph,
            &self.authors,
            self.queue.signatures(),
        );
        self.change_graph.rebuild_fragment_index(&may_form);
    }

    /// Form the fragments of deferred changes which have become authenticated.
    fn form_newly_covered_fragments(&mut self) {
        let frontier = self.signed_frontier();
        let ready: BTreeSet<ChangeHash> = self
            .change_graph
            .deferred_fragments()
            .filter(|hash| {
                self.change_graph
                    .actor_seq_for_hash(hash)
                    .is_some_and(|(actor, seq)| frontier.get(&actor).is_some_and(|m| *m >= seq))
            })
            .collect();
        if !ready.is_empty() {
            self.change_graph.form_deferred_fragments(&ready);
        }
    }

    /// Record `change` in the history.
    ///
    /// `form_fragment` says whether its fragment may form now. A fragment is only
    /// formed for history the document may export: in authenticated mode a local
    /// commit is not that until it is signed, and forming its fragment early would
    /// supersede the lower-level fragments it absorbs, withdrawing history which
    /// *was* exportable.
    ///
    /// Returns [`HistoryUpdate::BoundaryResolved`] if the change was a pending
    /// write-frontier boundary; witnessing one updates the stored seq-mask, but
    /// never the visibility [`Mask`] — publishing that is the caller's single
    /// [`Automerge::republish_mask`] step after the op set is consistent.
    pub(crate) fn update_history(&mut self, change: &Change, form_fragment: bool) -> HistoryUpdate {
        self.update_deps(change);

        let actor_index = self
            .ops
            .lookup_actor(change.actor_id())
            .expect("Change's actor not already in the document");

        let hash = self
            .change_graph
            .add_change(change, actor_index, &mut self.authors, form_fragment)
            .expect("Change's deps should already be in the document");

        // This change was a pending boundary head. Report it so the caller
        // republishes the visibility once the op set is consistent.
        if self.visibility.is_pending(&hash) {
            HistoryUpdate::BoundaryResolved
        } else {
            HistoryUpdate::Unchanged
        }
    }

    pub(crate) fn put_actor_ref(&mut self, actor: &ActorId) -> usize {
        self.put_actor(actor.clone())
    }

    /// Ensure `actor` is in the actor table and return its index, shifting
    /// every actor-indexed structure if it had to be inserted.
    pub(crate) fn put_actor(&mut self, actor: ActorId) -> usize {
        match self.ops.insert_actor(actor) {
            ActorInsert::Existing(idx) => idx,
            ActorInsert::Inserted(shift) => {
                self.change_graph.insert_actor(&shift);
                self.actor.shift(&shift);
                self.authors.insert_actor(&shift);
                self.visibility.insert_actor(&shift);
                shift.index()
            }
        }
    }

    fn update_deps(&mut self, change: &Change) {
        for d in change.deps() {
            self.deps.remove(d);
        }
        self.deps.insert(change.hash());
    }

    #[doc(hidden)]
    pub fn import(&self, s: &str) -> Result<(ExId, ObjType), AutomergeError> {
        let obj = self.import_obj(s)?;
        if obj == ExId::Root {
            Ok((ExId::Root, ObjType::Map))
        } else {
            let obj_type = self
                .object_type(&obj)
                .map_err(|_| AutomergeError::InvalidObjId(s.to_owned()))?;
            Ok((obj, obj_type))
        }
    }

    #[doc(hidden)]
    pub fn import_obj(&self, s: &str) -> Result<ExId, AutomergeError> {
        if s == "_root" {
            Ok(ExId::Root)
        } else {
            let n = s
                .find('@')
                .ok_or_else(|| AutomergeError::InvalidObjIdFormat(s.to_owned()))?;
            let counter = s[0..n]
                .parse()
                .map_err(|_| AutomergeError::InvalidObjIdFormat(s.to_owned()))?;
            let actor = ActorId::from(hex::decode(&s[(n + 1)..]).unwrap());
            let actor = self
                .ops
                .lookup_actor(&actor)
                .ok_or_else(|| AutomergeError::InvalidObjId(s.to_owned()))?;
            let obj = ExId::Id(counter, self.ops.get_actor(actor).clone(), actor);
            Ok(obj)
        }
    }

    pub fn dump(&self) {
        /*
                log!(
                    "  {:12} {:3} {:12} {:12} {:12} {:12} {:12}",
                    "id",
                    "ins",
                    "obj",
                    "key",
                    "value",
                    "pred",
                    "succ"
                );
        */
        self.ops.dump();
        /*
                for op in self.ops.iter() {
                    let id = self.to_short_string(op.id);
                    let obj = self.to_short_string(op.obj);
                    let key = match op.key {
                        KeyRef::Map(n) => n.to_owned(),
                        KeyRef::Seq(n) => self.to_short_string(n),
                    };
                    let value: String = match op.op_type() {
                        OpType::Put(value) => format!("{}", value),
                        OpType::Make(obj) => format!("make({})", obj),
                        OpType::Increment(obj) => format!("inc({})", obj),
                        OpType::Delete => format!("del{}", 0),
                        OpType::MarkBegin(_, MarkData { name, value }) => {
                            format!("mark({},{})", name, value)
                        }
                        OpType::MarkEnd(_) => "/mark".to_string(),
                    };
                    //let pred: Vec<_> = op.pred().map(|id| self.to_short_string(id)).collect();
                    let succ: Vec<_> = op.succ().map(|id| self.to_short_string(id)).collect();
                    let insert = match op.insert {
                        true => "t",
                        false => "f",
                    };
                    log!(
                        //"  {:12} {:3} {:12} {:12} {:12} {:12?} {:12?}",
                        "  {:12} {:3} {:12} {:12} {:12} {:12?}",
                        id,
                        insert,
                        obj,
                        key,
                        value,
                        //pred,
                        succ
                    );
                }
        */
    }

    /// Create patches representing the change in the current state of the document between the
    /// `before` and `after` heads.  If the arguments are reverse it will observe the same changes
    /// in the opposite order.
    pub fn diff(&self, before_heads: &[ChangeHash], after_heads: &[ChangeHash]) -> Vec<Patch> {
        let clock = self.clock_range(before_heads, after_heads);
        let mut patch_log = PatchLog::active();
        patch_log
            .record(
                self,
                |events| {
                    DiffIter::log(self, ObjMeta::root(), clock, events, true);
                },
                |d| d.visible(after_heads),
            )
            .expect("a fresh patch log belongs to any document");
        patch_log.make_patches(self)
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
        let obj = self.exid_to_obj(obj.as_ref())?;
        let clock = self.clock_range(before_heads, after_heads);
        let mut patch_log = PatchLog::active();
        patch_log
            .record(
                self,
                |events| {
                    DiffIter::log(self, obj, clock, events, recursive);
                },
                |d| d.visible(after_heads),
            )
            .expect("a fresh patch log belongs to any document");
        Ok(patch_log.make_patches(self))
    }

    /// EXPERIMENTAL: Return the fragments covering the document history at
    /// the given levels, ordered oldest to newest.
    ///
    /// This is an experimental API, it may change or be removed without
    /// warning.
    #[doc(hidden)]
    pub fn fragments<R: RangeBounds<usize>>(&self, levels: R) -> Vec<Fragment> {
        self.all_fragments(levels)
    }

    /// Every fragment this document may export.
    ///
    /// Enumeration starts from the exportable frontier, which in legacy mode is
    /// the document's heads and in authenticated mode stops short of history no
    /// signature covers. The index holds only fragments whose history is
    /// authenticated, because forming one is deferred until then, so every
    /// fragment returned here is exportable by construction — there is no
    /// per-fragment check, and no way for an unsigned commit to withdraw history
    /// which was already exportable.
    fn all_fragments<R: RangeBounds<usize>>(&self, levels: R) -> Vec<Fragment> {
        let heads = self.exportable_heads();
        // these produce fragments newest to oldest
        let mut fragments: Vec<_> = self.change_graph.fragments(&heads, levels).collect();
        // but we want to return them oldest to newest
        fragments.reverse();
        fragments
    }

    /// EXPERIMENTAL: Return the fragment with the given head hash, if any.
    ///
    /// This is an experimental API, it may change or be removed without
    /// warning.
    #[doc(hidden)]
    pub fn get_fragment(&self, head: ChangeHash) -> Option<Fragment> {
        let fragment = self.change_graph.get_fragment(head)?;
        // The index holds only fragments which may be exported, but a level-0
        // head can be asked for by hash whether or not it is in the cover, so an
        // unsigned local commit is turned away here.
        if !self.is_covered(&fragment.head) {
            None
        } else {
            Some(fragment)
        }
    }

    /// EXPERIMENTAL: Encode each fragment as bytes, either as a single change
    /// (level-0 fragments with one member) or as a bundle.
    ///
    /// This is an experimental API, it may change or be removed without
    /// warning.
    #[doc(hidden)]
    pub fn bundle_fragments<I: IntoIterator<Item = Fragment>>(&self, fragments: I) -> Vec<Vec<u8>> {
        let covered = self.covered_predicate();
        fragments
            .into_iter()
            .filter_map(|supplied| {
                // Fragment metadata is caller-controlled at this API boundary.
                // Always authorize and encode the canonical graph fragment so
                // signed `members` cannot be paired with an unsigned `head`.
                let fragment = self.change_graph.get_fragment(supplied.head)?;
                if fragment != supplied {
                    return None;
                }
                if !covered(&fragment.head) {
                    return None;
                }
                // Encoded last so that a fragment this document cannot
                // authenticate is withheld rather than sent unusable.
                let signatures = self.fragment_signature_chunk(&fragment)?;
                let mut bytes =
                    if fragment.head.fragment_level() == 0 && fragment.members.len() == 1 {
                        // there should be a unwrap().into_owned()/to_vec() to avoid a memory copy
                        self.get_change_by_hash(&fragment.head)?.bytes().to_vec()
                    } else {
                        self.bundle(fragment.members.clone()).ok()?.bytes().to_vec()
                    };
                bytes.extend(signatures);
                Some(bytes)
            })
            .collect()
    }

    /// Get the heads of this document.
    pub fn get_heads(&self) -> Vec<ChangeHash> {
        let mut deps: Vec<_> = self.deps.iter().copied().collect();
        deps.sort_unstable();
        deps
    }

    pub fn get_changes(&self, have_deps: &[ChangeHash]) -> Vec<Change> {
        ChangeCollector::exclude_hashes(&self.ops, &self.change_graph, have_deps)
    }

    pub fn get_changes_meta(&self, have_deps: &[ChangeHash]) -> Vec<ChangeMetadata<'_>> {
        ChangeCollector::exclude_hashes_meta(
            &self.ops,
            &self.change_graph,
            &self.authors,
            have_deps,
        )
    }

    pub fn get_change_meta_by_hash(&self, hash: &ChangeHash) -> Option<ChangeMetadata<'_>> {
        ChangeCollector::meta_for_hashes(&self.ops, &self.change_graph, &self.authors, [*hash])
            .ok()?
            .pop()
    }

    /// Get changes in `other` that are not in `self`
    pub fn get_changes_added(&self, other: &Self) -> Vec<Change> {
        // Depth-first traversal from the heads through the dependency graph,
        // until we reach a change that is already present in other
        let mut stack: Vec<_> = other.get_heads();
        tracing::trace!(their_heads=?stack, "finding changes to merge");
        let mut seen_hashes = HashSet::new();
        let mut added_change_hashes = Vec::new();
        while let Some(hash) = stack.pop() {
            if !seen_hashes.contains(&hash) && !self.has_change(&hash) {
                seen_hashes.insert(hash);
                added_change_hashes.push(hash);
                stack.extend(other.change_graph.deps_for_hash(&hash));
            }
        }
        // Return those changes in the reverse of the order in which the depth-first search
        // found them. This is not necessarily a topological sort, but should usually be close.
        added_change_hashes.reverse();

        // safe to unwrap here b/c added_changes all came from the change_graph
        other.get_changes_by_hashes(added_change_hashes).unwrap()
    }

    /// Get the hash of the change that contains the given `opid`.
    ///
    /// Returns [`None`] if the `opid`:
    /// - is the root object id
    /// - does not exist in this document
    pub fn hash_for_opid(&self, exid: &ExId) -> Option<ChangeHash> {
        match exid {
            ExId::Root => None,
            ExId::Id(..) => {
                let opid = self.exid_to_opid(exid).ok()?;
                self.change_graph.opid_to_hash(opid)
            }
        }
    }

    fn calculate_marks(&self, obj: &ExId, read: ReadAt<'_>) -> Result<Vec<Mark>, AutomergeError> {
        let obj = self.exid_to_obj(obj.as_ref())?;

        let Some(seq_type) = obj.typ.as_sequence_type() else {
            // Really we should return an error here but we don't in order to stay
            // compatibile with older implementations
            return Ok(Vec::new());
        };

        // present-time text marks come straight from the mark and text
        // indexes — no op materialization (the text index carries text
        // widths, so lists still take the walk below)
        if read.historical().is_none() && seq_type == SequenceType::Text {
            let fast = self.ops().calculate_marks_fast(&obj.id);
            #[cfg(feature = "slow_path_assertions")]
            {
                let slow = self.calculate_marks_slow(&obj, read.borrow(), seq_type);
                assert_eq!(fast, slow, "indexed marks != walked marks");
            }
            return Ok(fast);
        }

        Ok(self.calculate_marks_slow(&obj, read, seq_type))
    }

    fn calculate_marks_slow(
        &self,
        obj: &crate::types::ObjMeta,
        read: ReadAt<'_>,
        seq_type: SequenceType,
    ) -> Vec<Mark> {
        let mut top_ops = self.ops().top_ops(&obj.id, read).marks();

        let mut index = 0;
        let mut acc = MarkAccumulator::default();
        let mut last_marks = None;
        let mut mark_len = 0;
        let mut mark_index = 0;
        while let Some(o) = top_ops.next() {
            let marks = top_ops.get_marks();
            let len = o.width(seq_type, self.text_encoding());
            if last_marks.as_ref() != marks {
                match last_marks.as_ref() {
                    Some(m) if mark_len > 0 => acc.add(mark_index, mark_len, m),
                    _ => (),
                }
                last_marks = marks.cloned();
                mark_index = index;
                mark_len = 0;
            }
            mark_len += len;
            index += len;
        }
        match last_marks.as_ref() {
            Some(m) if mark_len > 0 => acc.add(mark_index, mark_len, m),
            _ => (),
        }
        acc.into_iter_no_unmark().collect()
    }

    pub fn hydrate(&self, heads: Option<&[ChangeHash]>) -> hydrate::Value {
        let read = self.read_at(heads);
        self.hydrate_map(&ObjId::root(), &read)
    }

    /// Test oracle: the current view materialised through the filtered
    /// (non-indexed) path. Never use for production reads.
    ///
    /// `read_at(Some(heads))` collapses to the indexed `ReadAt::Current`
    /// when `heads` are current, so tests cannot force the filtered path
    /// through heads; this method reads under `ReadAt::At` explicitly.
    #[doc(hidden)]
    pub fn read_forced_slow_hydrate(&self) -> hydrate::Value {
        self.hydrate_map(
            &ObjId::root(),
            &ReadAt::at(Cow::Owned(self.visible_current())),
        )
    }

    pub(crate) fn hydrate_obj_for(
        &self,
        obj: &crate::ObjId,
        read: ReadAt<'_>,
    ) -> Result<hydrate::Value, AutomergeError> {
        let obj = self.exid_to_obj(obj)?;
        Ok(match obj.typ {
            ObjType::Map | ObjType::Table => self.hydrate_map(&obj.id, &read),
            ObjType::List => self.hydrate_list(&obj.id, &read),
            ObjType::Text => self.hydrate_text(&obj.id, &read),
        })
    }

    pub(crate) fn parents_for<'a>(
        &'a self,
        obj: &ExId,
        read: ReadAt<'a>,
    ) -> Result<Parents<'a>, AutomergeError> {
        let obj = self.exid_to_obj(obj)?;
        // FIXME - now that we have blocks a correct text_rep is relevent
        Ok(self.ops.parents(obj.id, read))
    }

    pub(crate) fn keys_for(&self, obj: &ExId, read: ReadAt<'_>) -> Keys<'_> {
        self.exid_to_obj(obj)
            .ok()
            .map(|obj| self.ops.keys(&obj.id, read))
            .unwrap_or_default()
    }

    pub(crate) fn iter_for<'a>(&'a self, obj: &ExId, read: ReadAt<'a>) -> DocIter<'a> {
        self.exid_to_obj(obj)
            .ok()
            .map(|obj| DocIter::new(self, obj, read))
            .unwrap_or_else(|| DocIter::empty(self.text_encoding()))
    }

    pub(crate) fn map_range_for<'a, R: RangeBounds<String> + 'a>(
        &'a self,
        obj: &ExId,
        range: R,
        read: ReadAt<'a>,
    ) -> MapRange<'a> {
        self.exid_to_obj(obj)
            .ok()
            .map(|obj| self.ops.map_range(&obj.id, range, read))
            .unwrap_or_else(MapRange::empty)
    }

    pub(crate) fn list_range_for<'a, R: RangeBounds<usize>>(
        &'a self,
        obj: &ExId,
        range: R,
        read: ReadAt<'a>,
    ) -> ListRange<'a> {
        self.exid_to_obj(obj)
            .ok()
            .map(|obj| self.ops.list_range(&obj.id, range, read))
            .unwrap_or_else(ListRange::empty)
    }

    pub(crate) fn values_for(&self, obj: &ExId, read: ReadAt<'_>) -> Values<'_> {
        self.exid_to_obj(obj)
            .ok()
            .map(|obj| Values::new(&self.ops, self.ops.top_ops(&obj.id, read.borrow())))
            .unwrap_or_default()
    }

    pub(crate) fn length_for(&self, obj: &ExId, read: ReadAt<'_>) -> usize {
        // FIXME - is doc.length() for a text always the string length?
        self.exid_to_obj(obj)
            .map(|obj| self.ops.seq_length(&obj.id, self.text_encoding(), &read))
            .unwrap_or(0)
    }

    pub(crate) fn text_for(&self, obj: &ExId, read: ReadAt<'_>) -> Result<String, AutomergeError> {
        let obj = self.exid_to_obj(obj)?;
        Ok(self.ops.text(&obj.id, read))
    }

    pub(crate) fn spans_for<'a>(
        &'a self,
        obj: &ExId,
        read: ReadAt<'a>,
    ) -> Result<Spans<'a>, AutomergeError> {
        let obj = self.exid_to_obj(obj)?;
        Ok(Spans::new(self.ops.spans(&obj.id, read)))
    }

    pub(crate) fn get_cursor_for(
        &self,
        obj: &ExId,
        position: CursorPosition,
        read: ReadAt<'_>,
        move_cursor: MoveCursor,
    ) -> Result<Cursor, AutomergeError> {
        let obj = self.exid_to_obj(obj)?;
        let Some(seq_type) = obj.typ.as_sequence_type() else {
            return Err(AutomergeError::InvalidOp(obj.typ));
        };
        match position {
            CursorPosition::Start => Ok(Cursor::Start),
            CursorPosition::End => Ok(Cursor::End),
            CursorPosition::Index(i) => {
                let found = self.ops.seek_ops_by_index(&obj.id, i, seq_type, &read);

                if let Some(op) = found.ops.last() {
                    Ok(Cursor::Op(OpCursor::new(op.id, &self.ops, move_cursor)))
                } else {
                    Err(AutomergeError::InvalidIndex(i))
                }
            }
        }
    }

    pub(crate) fn get_cursor_position_for(
        &self,
        obj: &ExId,
        cursor: &Cursor,
        read: ReadAt<'_>,
    ) -> Result<usize, AutomergeError> {
        match cursor {
            Cursor::Start => Ok(0),
            Cursor::End => Ok(self.length_for(obj, read)),
            Cursor::Op(op) => {
                let obj_meta = self.exid_to_obj(obj)?;

                let Some(seq_type) = obj_meta.typ.as_sequence_type() else {
                    return Err(AutomergeError::InvalidCursor(cursor.clone()));
                };

                let opid = self.op_cursor_to_opid(op, read.historical_slow())?;

                let found = self
                    .ops
                    .seek_list_opid(&obj_meta.id, opid, seq_type, &read)
                    .ok_or_else(|| AutomergeError::InvalidCursor(cursor.clone()))?;

                match op.move_cursor {
                    // `MoveCursor::After` mimics the original behavior of cursors.
                    //
                    // The original behavior was to just return the `FoundOpId::index` found by
                    // `OpSetInternal::seek_list_opid()`.
                    //
                    // This index always corresponds to the:
                    // - index of the item itself (if it's visible at `clock`)
                    // - next index of visible item that **was also visible at the time of cursor creation**
                    //   (if the item is not visible at `clock`).
                    // - or `sequence.length` if none of the next items are visible at `clock`.
                    MoveCursor::After => Ok(found.index),
                    MoveCursor::Before => {
                        // `MoveCursor::Before` behaves like `MoveCursor::After` but in the opposite direction:
                        //
                        // - if the item is visible at `clock`, just return its index
                        // - if the item isn't visible at `clock`, find the index of the **previous** item
                        //   that's visible at `clock` that was also visible at the time of cursor creation.
                        // - if none of the previous items are visible (or the index of the original item is 0),
                        //   our index is `0`.
                        if found.visible || found.index == 0 {
                            Ok(found.index)
                        } else {
                            // FIXME: this should probably be an `OpSet` query
                            // also this implementation is likely very inefficient

                            // current implementation walks upwards through `key` of op pointed to by cursor
                            // and checks if `key` is visible by using `seek_list_opid()`.

                            let mut key = found
                                .op.key.elemid()
                                .expect("failed to retrieve initial cursor op key for MoveCursor::Before")
                                .0;

                            loop {
                                let f = self.ops.seek_list_opid(&obj_meta.id, key, seq_type, &read);

                                match f {
                                    Some(f) => {
                                        if f.visible {
                                            return Ok(f.index);
                                        }

                                        key = f
                                            .op
                                            .key
                                            .elemid()
                                            .expect(
                                                "failed to retrieve op key in MoveCursor::Before",
                                            )
                                            .0;
                                    }
                                    // reached when we've gone before the beginning of the sequence
                                    None => break Ok(0),
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    pub(crate) fn marks_for(
        &self,
        obj: &ExId,
        read: ReadAt<'_>,
    ) -> Result<Vec<Mark>, AutomergeError> {
        self.calculate_marks(obj, read)
    }

    pub(crate) fn get_for(
        &self,
        obj: &ExId,
        prop: Prop,
        read: ReadAt<'_>,
    ) -> Result<Option<(Value<'_>, ExId)>, AutomergeError> {
        let obj = self.exid_to_obj(obj)?;
        let op = match (obj.typ, prop) {
            (ObjType::Map | ObjType::Table, Prop::Map(key)) => self
                .ops
                .seek_ops_by_map_key(&obj.id, &key, &read)
                .ops
                .into_iter()
                .next_back()
                .map(|op| op.tagged_value(self.ops())),
            (ObjType::List | ObjType::Text, Prop::Seq(i)) => {
                let seq_type = obj
                    .typ
                    .as_sequence_type()
                    .expect("list and text must have a sequence type");
                self.ops
                    .seek_ops_by_index(&obj.id, i, seq_type, &read)
                    .ops
                    .into_iter()
                    .next_back()
                    .map(|op| op.tagged_value(self.ops()))
            }
            _ => return Err(AutomergeError::InvalidOp(obj.typ)),
        };
        Ok(op)
    }

    pub(crate) fn get_all_for<O: AsRef<ExId>, P: Into<Prop>>(
        &self,
        obj: O,
        prop: P,
        read: ReadAt<'_>,
    ) -> Result<Vec<(Value<'_>, ExId)>, AutomergeError> {
        let prop = prop.into();
        let obj = self.exid_to_obj(obj.as_ref())?;
        let values = match (obj.typ, prop) {
            (ObjType::Map | ObjType::Table, Prop::Map(key)) => self
                .ops
                .seek_ops_by_map_key(&obj.id, &key, &read)
                .ops
                .into_iter()
                .map(|op| op.tagged_value(self.ops()))
                .collect::<Vec<_>>(),
            (ObjType::List | ObjType::Text, Prop::Seq(i)) => {
                let seq_type = obj
                    .typ
                    .as_sequence_type()
                    .expect("list and text must have a sequence type");
                self.ops
                    .seek_ops_by_index(&obj.id, i, seq_type, &read)
                    .ops
                    .into_iter()
                    .map(|op| op.tagged_value(self.ops()))
                    .collect::<Vec<_>>()
            }
            _ => return Err(AutomergeError::InvalidOp(obj.typ)),
        };
        // this is a test to make sure opid and exid are always sorting the same way
        assert_eq!(
            values.iter().map(|v| &v.1).collect::<Vec<_>>(),
            values.iter().map(|v| &v.1).sorted().collect::<Vec<_>>()
        );
        Ok(values)
    }

    pub(crate) fn get_marks_for<O: AsRef<ExId>>(
        &self,
        obj: O,
        index: usize,
        read: ReadAt<'_>,
    ) -> Result<MarkSet, AutomergeError> {
        let obj = self.exid_to_obj(obj.as_ref())?;

        if read.historical().is_none() && obj.typ == ObjType::Text {
            let fast = self.ops().get_marks_fast(&obj.id, index);
            #[cfg(feature = "slow_path_assertions")]
            {
                let slow = self.get_marks_slow(&obj, index, read);
                assert_eq!(fast, slow, "indexed marks != walked marks");
            }
            return Ok(fast);
        }

        Ok(self.get_marks_slow(&obj, index, read))
    }

    fn get_marks_slow(
        &self,
        obj: &crate::types::ObjMeta,
        index: usize,
        read: ReadAt<'_>,
    ) -> MarkSet {
        let Some(seq_type) = obj.typ.as_sequence_type() else {
            return MarkSet::default();
        };
        let mut iter = self.ops.top_ops(&obj.id, read).marks();
        let mut pos = 0;
        while let Some(op) = iter.next() {
            pos += op.width(seq_type, self.text_encoding());
            if pos > index {
                return match iter.get_marks() {
                    Some(arc) => arc.as_ref().clone().without_unmarks(),
                    None => MarkSet::default(),
                };
            }
        }
        MarkSet::default()
    }

    fn convert_scalar_strings_to_text(&mut self) -> Result<(), AutomergeError> {
        struct Conversion {
            obj_id: ExId,
            prop: Prop,
            text: smol_str::SmolStr,
        }
        let mut to_convert = Vec::new();
        for (obj, ops) in self.ops.iter_objs() {
            match obj.typ {
                ObjType::Map | ObjType::List => {
                    for op in ops.visible_slow(None) {
                        //if !op.visible() {
                        //    continue;
                        //}
                        if let OpType::Put(ScalarValue::Str(s)) = op.op_type() {
                            let prop = match op.key {
                                KeyRef::Map(prop) => Prop::Map(prop.into()),
                                KeyRef::Seq(_) => {
                                    let Some(found) = self.ops.seek_list_opid(
                                        &obj.id,
                                        op.id,
                                        SequenceType::List,
                                        &self.read_current(),
                                    ) else {
                                        continue;
                                    };
                                    Prop::Seq(found.index)
                                }
                            };
                            to_convert.push(Conversion {
                                obj_id: self.ops.id_to_exid(obj.id.0),
                                prop,
                                text: smol_str::SmolStr::from(s),
                            })
                        }
                    }
                }
                _ => {}
            }
        }

        if !to_convert.is_empty() {
            let mut tx = self.transaction();
            for Conversion { obj_id, prop, text } in to_convert {
                let text_id = tx.put_object(obj_id, prop, ObjType::Text)?;
                tx.splice_text(&text_id, 0, 0, &text)?;
            }
            tx.commit();
        }

        Ok(())
    }

    /// Whether the peer represented by `other` has all the changes we have
    pub fn has_our_changes(&self, other: &crate::sync::State) -> bool {
        other.shared_heads == self.get_heads()
    }

    pub(crate) fn has_change(&self, head: &ChangeHash) -> bool {
        self.change_graph.has_change(head)
    }

    /// The first hash on each path back from `start` which is neither applied nor queued,
    /// traversing through the dependencies of queued changes on the way.
    pub(crate) fn missing_deps_from(
        &self,
        start: impl Iterator<Item = ChangeHash>,
    ) -> Vec<ChangeHash> {
        let queued_changes = self
            .queue
            .changes()
            .map(|change| (change.hash(), change))
            .collect::<HashMap<_, _>>();

        let mut missing = HashSet::new();
        let mut seen = HashSet::new();
        let mut stack = start.collect::<Vec<_>>();

        while let Some(hash) = stack.pop() {
            if self.has_change(&hash) || !seen.insert(hash) {
                continue;
            }

            if let Some(change) = queued_changes.get(&hash) {
                stack.extend(change.deps().iter().copied());
            } else {
                missing.insert(hash);
            }
        }

        let mut missing = missing.into_iter().collect::<Vec<_>>();
        missing.sort();
        missing
    }

    pub fn text_encoding(&self) -> TextEncoding {
        self.ops.text_encoding
    }
}

impl ReadDoc for Automerge {
    fn parents<O: AsRef<ExId>>(&self, obj: O) -> Result<Parents<'_>, AutomergeError> {
        self.parents_for(obj.as_ref(), self.read_current())
    }

    fn parents_at<O: AsRef<ExId>>(
        &self,
        obj: O,
        heads: &[ChangeHash],
    ) -> Result<Parents<'_>, AutomergeError> {
        self.parents_for(obj.as_ref(), self.read_at(Some(heads)))
    }

    fn keys<O: AsRef<ExId>>(&self, obj: O) -> Keys<'_> {
        self.keys_for(obj.as_ref(), self.read_current())
    }

    fn keys_at<O: AsRef<ExId>>(&self, obj: O, heads: &[ChangeHash]) -> Keys<'_> {
        self.keys_for(obj.as_ref(), self.read_at(Some(heads)))
    }

    fn iter_at<O: AsRef<ExId>>(&self, obj: O, heads: Option<&[ChangeHash]>) -> DocIter<'_> {
        //let obj = self.exid_to_obj(obj.as_ref()).unwrap();
        self.iter_for(obj.as_ref(), self.read_at(heads))
    }

    fn map_range<'a, O: AsRef<ExId>, R: RangeBounds<String> + 'a>(
        &'a self,
        obj: O,
        range: R,
    ) -> MapRange<'a> {
        self.map_range_for(obj.as_ref(), range, self.read_current())
    }

    fn map_range_at<'a, O: AsRef<ExId>, R: RangeBounds<String> + 'a>(
        &'a self,
        obj: O,
        range: R,
        heads: &[ChangeHash],
    ) -> MapRange<'a> {
        self.map_range_for(obj.as_ref(), range, self.read_at(Some(heads)))
    }

    fn list_range<O: AsRef<ExId>, R: RangeBounds<usize>>(&self, obj: O, range: R) -> ListRange<'_> {
        self.list_range_for(obj.as_ref(), range, self.read_current())
    }

    fn list_range_at<O: AsRef<ExId>, R: RangeBounds<usize>>(
        &self,
        obj: O,
        range: R,
        heads: &[ChangeHash],
    ) -> ListRange<'_> {
        self.list_range_for(obj.as_ref(), range, self.read_at(Some(heads)))
    }

    fn values<O: AsRef<ExId>>(&self, obj: O) -> Values<'_> {
        self.values_for(obj.as_ref(), self.read_current())
    }

    fn values_at<O: AsRef<ExId>>(&self, obj: O, heads: &[ChangeHash]) -> Values<'_> {
        self.values_for(obj.as_ref(), self.read_at(Some(heads)))
    }

    fn length<O: AsRef<ExId>>(&self, obj: O) -> usize {
        self.length_for(obj.as_ref(), self.read_current())
    }

    fn length_at<O: AsRef<ExId>>(&self, obj: O, heads: &[ChangeHash]) -> usize {
        self.length_for(obj.as_ref(), self.read_at(Some(heads)))
    }

    fn text<O: AsRef<ExId>>(&self, obj: O) -> Result<String, AutomergeError> {
        self.text_for(obj.as_ref(), self.read_current())
    }

    fn spans<O: AsRef<ExId>>(&self, obj: O) -> Result<Spans<'_>, AutomergeError> {
        self.spans_for(obj.as_ref(), self.read_current())
    }

    fn spans_at<O: AsRef<ExId>>(
        &self,
        obj: O,
        heads: &[ChangeHash],
    ) -> Result<Spans<'_>, AutomergeError> {
        self.spans_for(obj.as_ref(), self.read_at(Some(heads)))
    }

    fn get_cursor<O: AsRef<ExId>, I: Into<CursorPosition>>(
        &self,
        obj: O,
        position: I,
        at: Option<&[ChangeHash]>,
    ) -> Result<Cursor, AutomergeError> {
        self.get_cursor_for(
            obj.as_ref(),
            position.into(),
            self.read_at(at),
            MoveCursor::After,
        )
    }

    fn get_cursor_moving<O: AsRef<ExId>, I: Into<CursorPosition>>(
        &self,
        obj: O,
        position: I,
        at: Option<&[ChangeHash]>,
        move_cursor: MoveCursor,
    ) -> Result<Cursor, AutomergeError> {
        self.get_cursor_for(obj.as_ref(), position.into(), self.read_at(at), move_cursor)
    }

    fn get_cursor_position<O: AsRef<ExId>>(
        &self,
        obj: O,
        cursor: &Cursor,
        at: Option<&[ChangeHash]>,
    ) -> Result<usize, AutomergeError> {
        self.get_cursor_position_for(obj.as_ref(), cursor, self.read_at(at))
    }

    fn text_at<O: AsRef<ExId>>(
        &self,
        obj: O,
        heads: &[ChangeHash],
    ) -> Result<String, AutomergeError> {
        self.text_for(obj.as_ref(), self.read_at(Some(heads)))
    }

    fn marks<O: AsRef<ExId>>(&self, obj: O) -> Result<Vec<Mark>, AutomergeError> {
        self.marks_for(obj.as_ref(), self.read_current())
    }

    fn marks_at<O: AsRef<ExId>>(
        &self,
        obj: O,
        heads: &[ChangeHash],
    ) -> Result<Vec<Mark>, AutomergeError> {
        self.marks_for(obj.as_ref(), self.read_at(Some(heads)))
    }

    fn hydrate<O: AsRef<ExId>>(
        &self,
        obj: O,
        heads: Option<&[ChangeHash]>,
    ) -> Result<hydrate::Value, AutomergeError> {
        let obj = self.exid_to_obj(obj.as_ref())?;
        let read = self.read_at(heads);
        Ok(match obj.typ {
            ObjType::List => self.hydrate_list(&obj.id, &read),
            ObjType::Text => self.hydrate_text(&obj.id, &read),
            _ => self.hydrate_map(&obj.id, &read),
        })
    }

    fn get_marks<O: AsRef<ExId>>(
        &self,
        obj: O,
        index: usize,
        heads: Option<&[ChangeHash]>,
    ) -> Result<MarkSet, AutomergeError> {
        self.get_marks_for(obj.as_ref(), index, self.read_at(heads))
    }

    fn get<O: AsRef<ExId>, P: Into<Prop>>(
        &self,
        obj: O,
        prop: P,
    ) -> Result<Option<(Value<'_>, ExId)>, AutomergeError> {
        self.get_for(obj.as_ref(), prop.into(), self.read_current())
    }

    fn get_at<O: AsRef<ExId>, P: Into<Prop>>(
        &self,
        obj: O,
        prop: P,
        heads: &[ChangeHash],
    ) -> Result<Option<(Value<'_>, ExId)>, AutomergeError> {
        self.get_for(obj.as_ref(), prop.into(), self.read_at(Some(heads)))
    }

    fn get_all<O: AsRef<ExId>, P: Into<Prop>>(
        &self,
        obj: O,
        prop: P,
    ) -> Result<Vec<(Value<'_>, ExId)>, AutomergeError> {
        self.get_all_for(obj.as_ref(), prop.into(), self.read_current())
    }

    fn get_all_at<O: AsRef<ExId>, P: Into<Prop>>(
        &self,
        obj: O,
        prop: P,
        heads: &[ChangeHash],
    ) -> Result<Vec<(Value<'_>, ExId)>, AutomergeError> {
        self.get_all_for(obj.as_ref(), prop.into(), self.read_at(Some(heads)))
    }

    fn object_type<O: AsRef<ExId>>(&self, obj: O) -> Result<ObjType, AutomergeError> {
        let obj = obj.as_ref();
        let opid = self.exid_to_opid(obj)?;
        let typ = self.ops.object_type(&ObjId(opid));
        typ.ok_or_else(|| AutomergeError::InvalidObjId(obj.to_string()))
    }

    fn get_missing_deps(&self, heads: &[ChangeHash]) -> Vec<ChangeHash> {
        let queued = self.queue.changes().map(|change| change.hash());
        self.missing_deps_from(queued.chain(heads.iter().copied()))
    }

    fn get_change_by_hash(&self, hash: &ChangeHash) -> Option<Change> {
        ChangeCollector::for_hashes(&self.ops, &self.change_graph, [*hash])
            .ok()?
            .pop()
    }

    fn stats(&self) -> crate::read::Stats {
        let num_changes = self.change_graph.len() as u64;
        let num_ops = self.ops.len() as u64;
        let num_actors = self.ops.actors.len() as u64;
        let cargo_package_name = env!("CARGO_PKG_NAME");
        let cargo_package_version = env!("CARGO_PKG_VERSION");
        let rustc_version = env!("CARGO_PKG_RUST_VERSION");
        crate::read::Stats {
            num_changes,
            num_ops,
            num_actors,
            cargo_package_name,
            cargo_package_version,
            rustc_version,
        }
    }

    fn text_encoding(&self) -> TextEncoding {
        self.ops.text_encoding
    }
}

impl Default for Automerge {
    fn default() -> Self {
        Self::new()
    }
}

/// Options to pass to [`Automerge::save_with_options()`] and [`crate::AutoCommit::save_with_options()`]
#[derive(Debug)]
pub struct SaveOptions {
    /// Whether to apply DEFLATE compression to the RLE encoded columns in the document
    pub deflate: bool,
    /// Whether to save history which cannot be applied yet: changes whose
    /// dependencies are missing, and chunks still waiting to be authenticated
    pub retain_orphans: bool,
}

impl SaveOptions {
    fn compress(&self) -> CompressConfig {
        if self.deflate {
            CompressConfig::Threshold(change::DEFLATE_MIN_SIZE)
        } else {
            CompressConfig::None
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

/// The result of saving the history after some heads.
#[derive(Debug)]
pub(crate) struct SavedTail {
    pub(crate) bytes: Vec<u8>,
    /// Whether every change after those heads is in `bytes`.
    ///
    /// False when an authenticated export withheld some of them, which is what
    /// stops an incremental save from moving its cursor past history its peer
    /// never received.
    pub(crate) complete: bool,
}

#[derive(Debug)]
pub(crate) struct Isolation {
    actor_index: usize,
    seq: u64,
    clock: VisibleClock,
}
