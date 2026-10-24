use std::cmp::Ordering;
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

use crate::actor::{ActorInsert, ActorRemoval, ActorShift, HasActorIndices};
use crate::change_graph::{BundleDep, BundleMember, ChangeGraph};
use crate::change_queue::ChangeQueue;
use crate::clock::Clock;
use crate::cursor::{CursorPosition, MoveCursor, OpCursor};
use crate::exid::ExId;
use crate::hash_retention::{Full, HashRetention, Retained};
use crate::iter::{DiffIter, DocIter, Keys, ListRange, MapRange, Spans, Values};
use crate::marks::{Mark, MarkAccumulator, MarkSet};
use crate::op_set2::change::bundle::BundleApply;
use crate::patches::{Patch, PatchAccumulator};
use crate::storage::{self, change, load, Bundle, CompressConfig, Document, VerificationMode};
use crate::tx::{
    self, CommitOptions, Failure, OwnedTransaction, ScopedRead, Success, Transactable, Transaction,
    TransactionArgs,
};

use crate::change_graph::Fragment;
use crate::hydrate;
use crate::types::{ActorId, ChangeHash, ObjId, ObjMeta, OpId, SequenceType, TextEncoding, Value};
use crate::write_frontier::{FrontierHead, WriteFrontier};
use crate::{AutomergeError, Change, ChangeId, Cursor, MalformedBundle, ObjType, Prop};
use std::borrow::Cow;
use view::{ClockRange, Mask, ReadAt, Viewpoint, VisibleClock};

pub(crate) mod current_state;
mod dirty_diff;
#[cfg(test)]
mod retention_fuzz_tests;
#[cfg(test)]
mod save_format_tests;
pub(crate) mod view;
mod visibility;

#[derive(Debug, Clone, PartialEq)]
pub(crate) enum Actor {
    Unused(ActorId),
    Cached(usize),
}

impl Actor {
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

/// What to do when loading a document partially succeeds
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OnPartialLoad {
    /// Ignore the error and return the loaded changes
    Ignore,
    /// Fail the entire load
    Error,
}

/// Whether a document keeps the hash of every change.
///
/// With [`AuditMode::Disabled`] (the default), operations that need
/// arbitrary historical hashes, such as the hash-based sync protocol, return
/// [`AutomergeError::AuditModeRequired`]. With [`AuditMode::Enabled`] every
/// change hash is verified and kept, and loading costs a full rehash.
/// Without it, only loading a bundle (what [`Automerge::save`] writes)
/// skips the rehash.
///
/// Convert with [`Automerge::enable_audit_mode`] and
/// [`Automerge::disable_audit_mode`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum AuditMode {
    Enabled,
    #[default]
    Disabled,
}

/// When a document frees change hashes it no longer needs.
///
/// [`GcMode::Auto`] keeps memory flat, but a later `save_incremental` may
/// then have to include more history than it otherwise would.
/// [`GcMode::Manual`] defers freeing until [`Automerge::gc`];
/// [`crate::AutoCommit`] uses it and collects after each `save_incremental`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum GcMode {
    /// Free covered hashes as soon as a fragment forms. The default.
    #[default]
    Auto,
    /// Free them only when [`Automerge::gc`] is called.
    Manual,
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
pub struct LoadOptions {
    on_partial_load: OnPartialLoad,
    verification_mode: VerificationMode,
    string_migration: StringMigration,
    text_encoding: TextEncoding,
    gc: Option<GcMode>,
    author: Option<Author<'static>>,
    write_frontier: WriteFrontier,
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

    /// The [`GcMode`] to load the document in.
    ///
    /// Unset, an [`Automerge`] loads in [`GcMode::Auto`] and a
    /// [`crate::AutoCommit`] in [`GcMode::Manual`]; setting it here
    /// overrides both.
    pub fn gc(self, gc: GcMode) -> Self {
        Self {
            gc: Some(gc),
            ..self
        }
    }

    /// Load in [`GcMode::Manual`] — see [`Self::gc`].
    pub fn with_manual_gc(self) -> Self {
        self.gc(GcMode::Manual)
    }

    pub(crate) fn gc_or(mut self, default: GcMode) -> Self {
        self.gc = Some(self.gc.unwrap_or(default));
        self
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
    pub fn write_frontier(self, write_frontier: HashMap<Author<'static>, Vec<ChangeId>>) -> Self {
        self.write_frontier_boundaries(WriteFrontier::from_ids(write_frontier))
    }

    pub(crate) fn write_frontier_boundaries(self, write_frontier: WriteFrontier) -> Self {
        Self {
            write_frontier,
            ..self
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
            gc: None,
            author: None,
            write_frontier: WriteFrontier::new(),
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
/// The sync protocol in the `automerge-sync` crate operates on this type.
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
/// If some of the write-frontier's heads have not yet been received, only
/// the author's changes within the causal history of the known heads are
/// eligible to contribute. As missing heads arrive, more changes may become
/// eligible. If no heads are known, all of the author's changes are hidden.
/// This can happen when the write-frontier is received separately from the
/// changes it references.
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
/// # use automerge::{Author, AutomergeError, ROOT};
/// # use automerge::next::{transaction::Transactable, Automerge};
/// let author = Author::from(vec![1,2,3]);
/// let mut doc = Automerge::new().with_author(Some(author.clone()));
/// doc.transact(|tx| {
///     tx.put(ROOT, "foo", "bar")?;
///     Ok::<_, AutomergeError>(())
/// }).unwrap();
/// let change = doc.get_last_local_change_legacy().unwrap().unwrap();
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
pub struct Automerge<H: HashRetention = Retained> {
    /// The list of unapplied changes that are not causally ready.
    pub(crate) queue: ChangeQueue,
    /// Graph of changes
    pub(crate) change_graph: ChangeGraph<H>,
    authors: Authors,
    /// Authors whose changes are hidden past a heads boundary.
    write_frontier: WriteFrontier,
    /// Visibility derived from the write-frontier, the change graph, and the
    /// author set. Written only by [`Automerge::republish_mask`], registration,
    /// and actor column shifts.
    visibility: visibility::FrontierVisibility,
    visibility_epoch: u64,
    actor_generation: view::ActorGeneration,
    /// The set of operations that form this document.
    pub(crate) ops: OpSet,
    /// The current actor.
    actor: Actor,
    /// Cursor for dirty-bit incremental diffs.
    diff_cursor: Viewpoint,
    /// The current author.
    author: Option<Author<'static>>,
}

impl<H: HashRetention> Automerge<H> {
    pub(crate) fn empty(encoding: TextEncoding) -> Self {
        let ops = OpSet::new(encoding);
        Automerge {
            queue: ChangeQueue::new(),
            change_graph: ChangeGraph::new(&ops.actors),
            authors: Authors::new(&ops.actors),
            write_frontier: WriteFrontier::new(),
            visibility: visibility::FrontierVisibility::default(),
            visibility_epoch: 0,
            actor_generation: view::ActorGeneration::new(),
            ops,
            actor: Actor::Unused(ActorId::random()),
            diff_cursor: Viewpoint::default(),
            author: None,
        }
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
        crate::anonymize::anonymize_doc(self)
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

    pub(crate) fn from_parts(
        ops: OpSet,
        change_graph: ChangeGraph<H>,
        authors: Authors,
        write_frontier: WriteFrontier,
    ) -> Self {
        let mut doc = Automerge {
            queue: ChangeQueue::new(),
            change_graph,
            authors,
            write_frontier,
            visibility: visibility::FrontierVisibility::default(),
            visibility_epoch: 0,
            actor_generation: view::ActorGeneration::new(),
            ops,
            actor: Actor::Unused(ActorId::random()),
            diff_cursor: Viewpoint::default(),
            author: None,
        };
        doc.remove_unused_actors(false);
        doc.pin_boundaries_if_hashes_may_be_freed();
        doc.rederive_visibility();
        if doc.mask_hides_existing_ops() {
            doc.ops
                .recompute_indexes(doc.visible_current().clock(), |_| false);
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

    pub(crate) fn changes(&self) -> &ChangeGraph<H> {
        &self.change_graph
    }

    pub(crate) fn clear_dirty(&mut self) {
        self.ops.clear_dirty();
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
            Some(idx) => {
                self.ensure_actor_tip_hash(idx);
                self.actor = Actor::Cached(idx)
            }
            None => self.actor = Actor::Unused(actor),
        }
        self
    }

    /// Replace the write-frontier: hide each author's changes past their heads.
    pub fn with_write_frontier(
        mut self,
        write_frontier: HashMap<Author<'static>, Vec<ChangeId>>,
    ) -> Self {
        self.set_write_frontier(write_frontier);
        self
    }

    /// Replace the write-frontier: hide each author's changes past their heads.
    ///
    /// Rebuilds the document's indexes when the visible set changes.
    pub fn set_write_frontier(&mut self, write_frontier: HashMap<Author<'static>, Vec<ChangeId>>) {
        self.republish_mask(|policy| *policy = WriteFrontier::from_ids(write_frontier));
    }

    fn ensure_actor_tip_hash(&mut self, actor_idx: usize) {
        let seq = self.change_graph.seq_for_actor(actor_idx);
        if seq == 0 {
            return;
        }
        let id = self.change_id_at(actor_idx, seq);
        let Some(node) = self.node_for_change_id(&id) else {
            return;
        };
        if self.change_graph.hash_for_node(node).is_some() {
            return;
        }
        if let Some(hash) = self.rebuild_hash(node) {
            let gc_owed = self.change_graph.record_node_hash(node, hash);
            if gc_owed {
                self.change_graph.gc_after_batch();
            }
        }
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

    /// Hide every change by `author` that is not in the history of `heads`.
    ///
    /// Until every one of `heads` is in the document the author is hidden
    /// entirely. Hidden changes, including this document's own author's, are
    /// still recorded, saved and synced; sequence positions refer to the
    /// visible document. Take a [`Self::viewpoint`] beforehand and call
    /// [`Self::diff`] to get the resulting patches.
    ///
    /// Rebuilds the document's indexes when the visible set changes.
    pub fn mask_author(&mut self, author: Author<'static>, heads: &[ChangeId]) {
        let heads = heads.iter().cloned().map(FrontierHead::Id).collect();
        self.republish_mask(|policy| policy.mask_author(author, heads));
    }

    /// Stop hiding `author`'s changes.
    ///
    /// Rebuilds the document's indexes when the visible set changes.
    pub fn reveal_author(&mut self, author: &Author<'static>) {
        self.republish_mask(|policy| policy.reveal_author(author));
    }

    pub(crate) fn mask_author_boundaries(
        &mut self,
        author: Author<'static>,
        heads: Vec<FrontierHead>,
    ) {
        self.republish_mask(|policy| policy.mask_author(author, heads));
    }

    pub(crate) fn set_write_frontier_boundaries(&mut self, write_frontier: WriteFrontier) {
        self.republish_mask(|policy| *policy = write_frontier);
    }

    /// The only path that changes the visibility of existing ops: the
    /// indexes are rebuilt and every op whose visibility flipped is marked
    /// dirty, so an incremental diff reports it. Returns whether it rebuilt.
    pub(crate) fn republish_mask(&mut self, update: impl FnOnce(&mut WriteFrontier)) -> bool {
        update(&mut self.write_frontier);
        self.pin_boundaries_if_hashes_may_be_freed();
        let visibility = visibility::FrontierVisibility::new(
            &self.write_frontier,
            &self.change_graph,
            &self.authors,
            &self.ops.actors,
        );
        if visibility.mask() == self.visibility.mask() {
            self.visibility = visibility;
            return false;
        }
        let before = std::mem::replace(&mut self.visibility, visibility);
        let after = self.visible_current();
        let now = &self.visibility;
        self.ops
            .recompute_indexes(after.clock(), |id| before.hides(id) != now.hides(id));
        self.visibility_epoch += 1;
        true
    }

    /// Changes whenever the write-frontier changes what any heads show.
    pub(crate) fn visibility_epoch(&self) -> u64 {
        self.visibility_epoch
    }

    /// Before an apply indexes its ops: new actors' authors join the mask, so
    /// the ops of a masked author's new actor are hidden from the start.
    pub(crate) fn register_incoming_authors(
        &mut self,
        authors: impl IntoIterator<Item = (usize, Author<'static>)>,
    ) {
        if self.write_frontier.is_empty() {
            return;
        }
        let mut registered = false;
        for (actor, author) in authors {
            if self.authors.get_author_for_actor(actor).is_none() {
                self.authors.assign_author(author, actor);
                registered = true;
            }
        }
        if registered {
            self.rederive_visibility();
        }
    }

    /// A hash boundary the graph now holds becomes an id, so freeing the hash
    /// later cannot make it pending again.
    // An audited document never frees a hash, and pinning clones an actor id
    // per boundary: large policies spend most of a load doing it.
    fn pin_boundaries_if_hashes_may_be_freed(&mut self) {
        if matches!(H::AUDIT, AuditMode::Disabled) {
            self.pin_resolved_boundaries();
        }
    }

    fn pin_resolved_boundaries(&mut self) {
        for boundary in self.write_frontier.heads_mut() {
            if let FrontierHead::Hash(hash) = boundary {
                if let Some(node) = self.change_graph.node_by_hash(hash) {
                    *boundary =
                        FrontierHead::Id(self.change_graph.change_id(node, &self.ops.actors));
                }
            }
        }
    }

    /// Before `changes` reach the change graph, which may free their hashes.
    pub(crate) fn pin_incoming_boundaries(&mut self, changes: &[Change]) {
        let actors = &self.ops.actors;
        for boundary in self.write_frontier.heads_mut() {
            if let FrontierHead::Hash(hash) = boundary {
                if let Some(c) = changes.iter().find(|c| c.hash() == *hash) {
                    let idx = actors.lookup(c.actor_id()).unwrap_or(0);
                    *boundary = FrontierHead::Id(ChangeId::from_doc_seq(
                        c.seq(),
                        c.actor_id().clone(),
                        idx,
                    ));
                }
            }
        }
    }

    pub(crate) fn debug_assert_mask_indexes(&self) {
        #[cfg(debug_assertions)]
        if self.mask().is_some() {
            self.ops
                .assert_indexes_under(self.visible_current().clock());
        }
    }

    pub(crate) fn republish_if_pending(&mut self) {
        if self.visibility.has_pending() {
            self.republish_mask(|_| {});
        }
    }

    /// The mask, if it hides an op of the incoming changes, given as
    /// `(actor index, max op)`.
    pub(crate) fn mask_hiding(
        &self,
        changes: impl IntoIterator<Item = (usize, u64)>,
    ) -> Option<Clock> {
        let mask = self.mask()?;
        changes
            .into_iter()
            .any(|(actor, max_op)| mask.hides(&OpId::new(max_op, actor)))
            .then(|| mask.clock().clone())
    }

    /// The active visibility mask, if any author is masked.
    pub(crate) fn mask(&self) -> Option<&Mask> {
        self.visibility.mask()
    }

    /// The write-frontier, per [`Author`]. A boundary named by a hash this
    /// document has never held is left out.
    pub fn get_write_frontier(&self) -> HashMap<Author<'static>, Vec<ChangeId>> {
        self.write_frontier.ids()
    }

    pub(crate) fn write_frontier(&self) -> &WriteFrontier {
        &self.write_frontier
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

    /// Re-derive `visibility` from the unchanged policy, without an
    /// index rebuild. Sound only when no op's visibility can have changed
    /// (registration of zero-op actors).
    fn rederive_visibility(&mut self) {
        self.visibility = visibility::FrontierVisibility::new(
            &self.write_frontier,
            &self.change_graph,
            &self.authors,
            &self.ops.actors,
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
        let actor_index = self.ops.lookup_actor(actor)?;
        self.authors.get_author_for_actor(actor_index)
    }

    /// Get the current actor id of this document.
    pub fn get_actor(&self) -> &ActorId {
        match &self.actor {
            Actor::Unused(actor) => actor,
            Actor::Cached(index) => self.ops.get_actor(*index),
        }
    }

    /// The actor must have no changes in the document.
    pub(crate) fn remove_actor(&mut self, actor: usize) {
        let (removed, removal) = self.ops.remove_actor(actor);
        self.actor.remove_actor(&removal, removed);
        self.change_graph.remove_actor(&removal);
        self.authors.remove_actor(&removal);
        self.visibility.remove_actor(&removal);
        self.actor_generation.bump();
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
    pub fn transaction(&mut self) -> Transaction<'_, H> {
        let args = self.transaction_args(None);
        Transaction::new(self, args)
    }

    /// Start a transaction isolated at the given heads.
    pub fn transaction_at(
        &mut self,
        heads: &[ChangeId],
    ) -> Result<Transaction<'_, H>, AutomergeError> {
        self.change_ids_to_hashes(heads)?;
        let args = self.transaction_args(Some(heads));
        Ok(Transaction::new(self, args))
    }

    /// Start a transaction that owns the document, consuming `self`.
    pub fn into_transaction(
        self,
        heads: Option<&[ChangeId]>,
    ) -> Result<OwnedTransaction<H>, AutomergeError> {
        OwnedTransaction::new(self, heads)
    }

    pub(crate) fn transaction_args(&mut self, heads: Option<&[ChangeId]>) -> TransactionArgs {
        let actor_index;
        let seq;
        let mut deps;
        let scope;
        match heads {
            Some(heads) => {
                deps = self
                    .change_ids_to_hashes(heads)
                    .expect("isolation ids were validated when isolating");
                let isolation = self.isolate_actor(heads);
                actor_index = isolation.actor_index;
                seq = isolation.seq;
                scope = Some(isolation.clock);
            }
            None => {
                actor_index = self.get_or_create_actor_index();
                seq = self.change_graph.seq_for_actor(actor_index) + 1;
                deps = self.get_head_hashes();
                scope = None;
                if seq > 1 {
                    let last_hash = self
                        .get_hash(&self.change_id_at(actor_index, seq - 1))
                        .expect("hash of the current actor's last change is always known");
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

    /// Run a transaction on this document in a closure, automatically handling commit or rollback
    /// afterwards.
    pub fn transact<F, O, E>(&mut self, f: F) -> tx::Result<O, E>
    where
        F: FnOnce(&mut Transaction<'_, H>) -> Result<O, E>,
    {
        self.transact_with_impl(None::<&dyn Fn(&O) -> CommitOptions>, f)
    }

    /// Like [`Self::transact()`] but with a function for generating the commit options.
    pub fn transact_with<F, O, E, C>(&mut self, c: C, f: F) -> tx::Result<O, E>
    where
        F: FnOnce(&mut Transaction<'_, H>) -> Result<O, E>,
        C: FnOnce(&O) -> CommitOptions,
    {
        // FIXME
        self.transact_with_impl(Some(c), f)
    }

    fn transact_with_impl<F, O, E, C>(&mut self, c: Option<C>, f: F) -> tx::Result<O, E>
    where
        F: FnOnce(&mut Transaction<'_, H>) -> Result<O, E>,
        C: FnOnce(&O) -> CommitOptions,
    {
        let mut tx = self.transaction();
        let result = f(&mut tx);
        match result {
            Ok(result) => {
                let change_id = if let Some(c) = c {
                    let commit_options = c(&result);
                    tx.commit_with(commit_options)
                } else {
                    tx.commit()
                };
                Ok(Success { result, change_id })
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
    pub fn empty_commit(&mut self, opts: CommitOptions) -> ChangeId {
        let args = self.transaction_args(None);
        let hash = Transaction::empty(self, args, opts);
        self.hash_to_change_id(&hash)
            .expect("hash of a newly created change is always known")
            .expect("newly created change must be in the document")
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
    /// This will create a new actor ID for the forked document. The
    /// write-frontier is carried over; a boundary not reachable from `heads`
    /// is pending in the fork.
    ///
    /// Returns an error if any of `heads` is not a change in this document.
    pub fn fork_at(&self, heads: &[ChangeId]) -> Result<Self, AutomergeError> {
        let mut nodes = Vec::with_capacity(heads.len());
        for id in heads {
            let node = self
                .node_for_change_id(id)
                .ok_or_else(|| AutomergeError::InvalidChangeId(id.to_string()))?;
            nodes.push(node);
        }
        let mut f = Self::empty(self.text_encoding());
        f.set_gc_mode(self.gc_mode());
        let clock = self.change_graph.calculate_clock(nodes);
        if let Some(bundle) = self.bundle_nodes(self.change_graph.nodes_covered_by(&clock), &[])? {
            f.apply_bundle(bundle)?;
        }
        f.set_write_frontier_boundaries(self.write_frontier.clone());
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

    pub(crate) fn load_with_options_and_mark_validation(
        data: &[u8],
        options: LoadOptions,
        mark_order: load::MarkOrderValidation,
    ) -> Result<Self, AutomergeError> {
        if data.is_empty() {
            tracing::trace!("no data, initializing empty document");
            let mut doc = Self::empty(options.text_encoding).with_author(options.author);
            if !options.write_frontier.is_empty() {
                doc.set_write_frontier_boundaries(options.write_frontier);
            }
            return Ok(doc);
        }
        tracing::trace!("loading first chunk");
        let (remaining, first_chunk) = storage::Chunk::parse(storage::parse::Input::new(data))
            .map_err(|e| load::Error::Parse(Box::new(e)))?;
        if !first_chunk.checksum_valid() {
            return Err(load::Error::BadChecksum.into());
        }
        // only a bundle names the hashes a document outside audit mode keeps
        if H::AUDIT != AuditMode::Enabled
            && !matches!(first_chunk, storage::Chunk::BundleColumns(..))
        {
            return Automerge::<Full>::load_with_options_and_mark_validation(
                data, options, mark_order,
            )
            .map(H::from_full);
        }

        let mut changes: Vec<load::LoadedChunk> = vec![];
        let mut first_chunk_was_doc = false;
        // a document chunk takes the write-frontier while reconstructing; any
        // other first chunk gets it once everything is applied
        let mut write_frontier = Some(options.write_frontier);
        let mut am = match first_chunk {
            storage::Chunk::Document(d) => {
                tracing::trace!("first chunk is document chunk, inflating");
                first_chunk_was_doc = true;
                d.reconstruct(
                    options.verification_mode,
                    options.text_encoding,
                    mark_order.allows_invalid(),
                    write_frontier.take().unwrap_or_default(),
                )
                .map_err(|e| match e {
                    crate::storage::document::ReconstructError::AuthorOnNonInitialSeq(
                        seq,
                        actor,
                    ) => AutomergeError::AuthorOnNonInitialSeq(seq, actor),
                    e => load::Error::InflateDocument(Box::new(e)).into(),
                })
                .map(H::from_full)?
            }
            storage::Chunk::Change(stored_change) => {
                tracing::trace!("first chunk is change chunk");
                changes.push(load::LoadedChunk::Change(Box::new(
                    Change::new_from_unverified(stored_change.into_owned(), None)
                        .map_err(|e| load::Error::InvalidChangeColumns(Box::new(e)))?,
                )));
                Self::empty(options.text_encoding)
            }
            storage::Chunk::BundleV0(bundle) => {
                tracing::trace!("first chunk is a BundleV0 chunk");
                let bundle_changes = bundle
                    .to_changes()
                    .map_err(|e| load::Error::InvalidBundleV0(Box::new(e)))?;
                changes.extend(
                    bundle_changes
                        .into_iter()
                        .map(|c| load::LoadedChunk::Change(Box::new(c))),
                );
                Self::empty(options.text_encoding)
            }
            storage::Chunk::BundleColumns(bundle, _) => {
                tracing::trace!("first chunk is a bundle-columns chunk");
                // a leading bundle is a document save, trailing orphans and all
                first_chunk_was_doc = true;
                changes.push(load::LoadedChunk::Bundle(bundle));
                Self::empty(options.text_encoding)
            }
            storage::Chunk::CompressedChange(stored_change, compressed) => {
                tracing::trace!("first chunk is compressed change");
                changes.push(load::LoadedChunk::Change(Box::new(
                    Change::new_from_unverified(
                        stored_change.into_owned(),
                        Some(compressed.into_owned()),
                    )
                    .map_err(|e| load::Error::InvalidChangeColumns(Box::new(e)))?,
                )));
                Self::empty(options.text_encoding)
            }
        };
        // before applying, or fragments forming during the load would GC
        am.change_graph.set_gc_mode(options.gc.unwrap_or_default());
        tracing::trace!("loading change chunks");
        // a partial load must still apply the chunks that parsed, the first included
        let rest = match load::load_changes(
            remaining.reset(),
            options.text_encoding,
            &am.change_graph,
            mark_order,
        ) {
            load::LoadedChanges::Complete(c) => c,
            load::LoadedChanges::Partial { error, .. }
                if options.on_partial_load == OnPartialLoad::Error =>
            {
                return Err(error.into());
            }
            load::LoadedChanges::Partial { loaded, .. } => loaded,
        };
        am.apply_loaded(changes.into_iter().chain(rest).collect())?;
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
        if let Some(write_frontier) = write_frontier.take() {
            if !write_frontier.is_empty() {
                am.set_write_frontier_boundaries(write_frontier);
            }
        }
        Ok(am.with_author(options.author))
    }

    /// Get a set of [`Patch`]es which materialize the current state of the document
    ///
    /// This is a convienence method for [`doc.diff(&[], current_heads)`][diff]
    ///
    /// [diff]: Self::diff()
    pub fn current_state(&self) -> Vec<Patch> {
        self.diff(&[], &self.get_heads())
            .expect("diffing to the current heads never fails")
    }

    /// Load an incremental save of a document.
    ///
    /// Unlike [`Self::load()`] this imports changes into an existing document. It will work with
    /// both the output of [`Self::save()`] and [`Self::save_after()`]
    ///
    /// Into an empty document this is [`Self::load()`], which takes chunks in any order.
    /// Otherwise chunks apply in the order given: legacy changes whose dependencies are missing
    /// are queued as by [`Self::apply_changes`], but a bundle is rejected as by
    /// [`Self::apply_bundle`], failing the call with chunks before it applied and none after.
    ///
    /// The return value is the number of ops which were applied, this is not useful and will
    /// change in future.
    pub fn load_incremental(&mut self, data: &[u8]) -> Result<usize, AutomergeError> {
        if self.is_empty() {
            let mut doc = Self::load_with_options_and_mark_validation(
                data,
                LoadOptions::new()
                    .text_encoding(self.text_encoding())
                    .on_partial_load(OnPartialLoad::Ignore)
                    .verification_mode(VerificationMode::Check),
                load::MarkOrderValidation::Validate,
            )?;
            // Ensure that the author, actor, write-frontier and GC mode are
            // carried over for an empty document.
            doc.set_gc_mode(self.gc_mode());
            doc.set_author(self.get_author().cloned());
            doc = doc.with_actor(self.actor_id().clone());
            doc.set_write_frontier_boundaries(self.write_frontier.clone());
            doc.ops_mut().mark_all_dirty();
            *self = doc;
            return Ok(self.ops.len());
        }
        let changes = match load::load_changes(
            storage::parse::Input::new(data),
            self.text_encoding(),
            &self.change_graph,
            load::MarkOrderValidation::Validate,
        ) {
            load::LoadedChanges::Complete(c) => c,
            load::LoadedChanges::Partial { error, loaded, .. } => {
                tracing::warn!(successful_chunks=loaded.len(), err=?error, "partial load");
                loaded
            }
        };
        let start = self.ops.len();
        self.apply_loaded(changes)?;
        Ok(self.ops.len() - start)
    }

    pub(crate) fn log_current_state(
        &self,
        obj: ObjMeta,
        patch_accumulator: &mut PatchAccumulator,
        recursive: bool,
    ) {
        let clock = ClockRange::to(self.read_current());
        let path_map = DiffIter::log(self, obj, clock, patch_accumulator, recursive);
        patch_accumulator.path_hint(path_map);
    }

    fn apply_loaded(&mut self, chunks: Vec<load::LoadedChunk>) -> Result<(), AutomergeError> {
        // into an empty document a bundle may arrive before its deps
        let any_order = self.change_graph.is_empty();
        let mut waiting: Vec<Bundle> = vec![];
        let mut pending: Vec<Change> = vec![];
        for chunk in chunks {
            match chunk {
                load::LoadedChunk::Change(c) => pending.push(*c),
                load::LoadedChunk::Bundle(b) => {
                    if !pending.is_empty() {
                        self.apply_changes(std::mem::take(&mut pending))?;
                    }
                    if any_order && !self.has_bundle_deps(&b) {
                        waiting.push(*b);
                        continue;
                    }
                    self.apply_bundle(*b)?;
                    self.apply_waiting(&mut waiting)?;
                }
            }
        }
        if !pending.is_empty() {
            self.apply_changes(pending)?;
        }
        self.apply_waiting(&mut waiting)?;
        // what still waits fails as it would have in order
        for b in waiting {
            self.apply_bundle(b)?;
        }
        Ok(())
    }

    fn apply_waiting(&mut self, waiting: &mut Vec<Bundle>) -> Result<(), AutomergeError> {
        // the latest to wait usually needed the one just applied
        while let Some(i) = waiting.iter().rposition(|b| self.has_bundle_deps(b)) {
            let b = waiting.remove(i);
            self.apply_bundle(b)?;
        }
        Ok(())
    }

    /// Whether `bundle` names only changes this document has, and extends
    /// each actor's sequence without a gap. O(members).
    fn has_bundle_deps(&self, bundle: &Bundle) -> bool {
        let known = |id: &ChangeId| self.node_for_change_id(id).is_some();
        if !bundle.dep_ids.iter().all(known) || !bundle.boundary.iter().all(|(_, id)| known(id)) {
            return false;
        }
        let clock = self.change_graph.current_seq_clock();
        let mut seen = vec![false; bundle.actors().len()];
        bundle
            .member_actors
            .iter()
            .zip(&bundle.member_seqs)
            .all(|(actor, seq)| {
                let actor = usize::from(*actor);
                if std::mem::replace(&mut seen[actor], true) {
                    return true;
                }
                let have = self
                    .ops
                    .lookup_actor(&bundle.actors()[actor])
                    .and_then(|idx| clock.get_for_actor(&idx))
                    .map_or(0, |s| s.get() as u64);
                seq.get() <= have + 1
            })
    }

    /// Apply changes to this document.
    ///
    /// This is idempotent in the sense that if a change has already been applied it will be
    /// ignored.
    ///
    /// A change whose dependencies are missing is queued until they arrive. A change names its
    /// dependencies only by hash, so once this document has freed the hash of a change some
    /// incoming change depends on, it cannot tell that dependency from one it has never seen and
    /// fails with [`AutomergeError::AuditModeRequired`] without applying it. Send such changes
    /// as bundles, which also name their dependencies by [`ChangeId`], or use
    /// [`AuditMode::Enabled`].
    pub fn apply_changes(
        &mut self,
        changes: impl IntoIterator<Item = Change> + Clone,
    ) -> Result<(), AutomergeError> {
        let result = self.apply_changes_batch(changes);
        if result.is_ok() {
            self.assert_no_unused_actors(cfg!(debug_assertions));
        }
        result
    }

    /// Takes all the changes in `other` which are not in `self` and applies them
    pub fn merge<H2: HashRetention>(
        &mut self,
        other: &mut Automerge<H2>,
    ) -> Result<Vec<ChangeId>, AutomergeError> {
        if let Some(bundle) = self.get_changes_added(other)? {
            tracing::trace!(heads=?bundle.heads().collect::<Vec<_>>(), "merging new changes");
            self.apply_bundle(bundle)?;
        }
        Ok(self.get_heads())
    }

    /// Save the entirety of this document in a compact form.
    ///
    /// A whole-document bundle by default; see [`SaveFormat`].
    pub fn save_with_options(&self, options: SaveOptions) -> Vec<u8> {
        self.save_with(options, NameHashes::UntagTiny)
    }

    pub(crate) fn save_with(&self, options: SaveOptions, names: NameHashes) -> Vec<u8> {
        let mut bytes = if options.format == SaveFormat::Legacy {
            // the actor table is written verbatim, so unused actors would be saved
            self.assert_no_unused_actors(cfg!(debug_assertions));
            Document::new(&self.ops, &self.change_graph, options.compress()).into_bytes()
        } else if self.change_graph.is_empty() {
            // a bundle needs a head; empty bytes load back as an empty document
            Vec::new()
        } else {
            let bundle = self
                .bundle_document_with(names)
                .expect("a document's own changes can always be made into a bundle");
            options.encode(&bundle)
        };

        // a bundle must be causally closed, so orphans follow as change chunks
        if options.retain_orphans {
            for orphaned in self.queue.iter() {
                bytes.extend(orphaned.raw_bytes());
            }
        }
        bytes
    }

    #[cfg(test)]
    pub fn debug_cmp<H2: HashRetention>(&self, other: &Automerge<H2>) {
        self.ops.debug_cmp(&other.ops);
    }

    /// Save the document and attempt to load it before returning - slow!
    pub fn save_and_verify(&self) -> Result<Vec<u8>, AutomergeError> {
        let bytes = self.save();
        Self::load_with_options_and_mark_validation(
            &bytes,
            Default::default(),
            load::MarkOrderValidation::Validate,
        )?;
        Ok(bytes)
    }

    /// Save this document, but don't run it through `DEFLATE` afterwards
    pub fn save_nocompress(&self) -> Vec<u8> {
        self.save_with_options(SaveOptions {
            deflate: false,
            ..Default::default()
        })
    }

    /// The whole document as a single bundle.
    pub fn bundle_document(&self) -> Result<Bundle, AutomergeError> {
        self.bundle_document_with(NameHashes::All)
    }

    pub(crate) fn bundle_document_with(&self, names: NameHashes) -> Result<Bundle, AutomergeError> {
        let nodes = self.change_graph.all_nodes();
        let members = self.change_graph.bundle_members(&nodes);
        let storage = Bundle::storage_for_document(&self.ops, &self.change_graph, &members)?;
        let heads = self.get_head_hashes();
        let external_deps: [ChangeHash; 0] = [];
        self.assemble_bundle(&heads, &external_deps, &members, storage, names)
    }

    /// The fragments, loose commits included, holding a change `heads`
    /// does not cover, in causal order. A fragment that also holds older
    /// changes is included whole. [`Self::bundles_for_fragments`] encodes
    /// them as a stack that [`Self::load_incremental`] takes in order. Ids
    /// this document does not know are ignored.
    pub fn fragments_after(&self, heads: &[ChangeId]) -> Result<Vec<Fragment>, AutomergeError> {
        let covered = self.seq_clock_for_ids_lossy(heads);
        Ok(self
            .change_graph
            .fragments_after(&self.get_head_hashes(), &covered, &self.ops.actors))
    }

    /// [`Self::fragments_after`] as encoded bundles; empty when `heads`
    /// covers the document. [`Self::save_after`] gives just the new
    /// changes as one bundle.
    pub fn bundle_after(&self, heads: &[ChangeId]) -> Result<Vec<Vec<u8>>, AutomergeError> {
        self.bundles_for_fragments(self.fragments_after(heads)?)
    }

    /// The changes `heads` does not already cover, as one bundle; `None`
    /// when there are none. Ids this document does not know are ignored.
    pub(crate) fn single_bundle_after(
        &self,
        heads: &[ChangeId],
    ) -> Result<Option<Bundle>, AutomergeError> {
        let clock = self.seq_clock_for_ids_lossy(heads);
        let fresh = self.change_graph.nodes_above(clock);
        let boundary = self.change_ids_to_hashes_lossy(heads);
        self.bundle_nodes(fresh, &boundary)
    }

    fn bundle_nodes(
        &self,
        nodes: Vec<crate::change_graph::NodeIdx>,
        boundary: &[ChangeHash],
    ) -> Result<Option<Bundle>, AutomergeError> {
        if nodes.is_empty() {
            return Ok(None);
        }
        let members = self.change_graph.bundle_members(&nodes);
        let storage = Bundle::storage_for_nodes(&self.ops, &self.change_graph, &members)?;
        let heads = self
            .change_graph
            .heads_of(&nodes)
            .into_iter()
            .map(|n| {
                self.change_graph
                    .hash_for_node(n)
                    .ok_or(AutomergeError::AuditModeRequired)
            })
            .collect::<Result<Vec<_>, _>>()?;
        let boundary: Vec<ChangeHash> = boundary
            .iter()
            .copied()
            .filter(|h| {
                self.change_graph
                    .node_by_hash(h)
                    .is_some_and(|n| nodes.binary_search(&n).is_err())
            })
            .collect();
        Ok(Some(self.assemble_bundle(
            &heads,
            &boundary,
            &members,
            storage,
            NameHashes::All,
        )?))
    }

    /// [`Self::save_with_options`] with the defaults.
    pub fn save(&self) -> Vec<u8> {
        self.save_with_options(SaveOptions::default())
    }

    /// The changes since `heads`, as one fragment — or, with
    /// [`SaveFormat::Legacy`], as a series of change chunks.
    /// Empty when `heads` already covers the document.
    pub fn save_after(&self, heads: &[ChangeId]) -> Result<Vec<u8>, AutomergeError> {
        self.save_after_with_options(heads, SaveOptions::default())
    }

    /// [`Self::save_after`] with [`SaveOptions`].
    pub fn save_after_with_options(
        &self,
        heads: &[ChangeId],
        options: SaveOptions,
    ) -> Result<Vec<u8>, AutomergeError> {
        if options.format == SaveFormat::Legacy {
            let mut bytes = vec![];
            for c in self.get_changes(heads)? {
                bytes.extend(c.raw_bytes());
            }
            return Ok(bytes);
        }
        Ok(self
            .single_bundle_after(heads)?
            .map(|b| options.encode(&b))
            .unwrap_or_default())
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
            .map(|hash| Ok(self.change_graph.has_change(hash)?.then_some(*hash)))
            .filter_map(|r| r.transpose())
            .collect::<Result<Vec<_>, AutomergeError>>()?;

        self.change_graph.remove_ancestors(changes, &heads)?;

        Ok(())
    }

    /// The last change this actor made, as a change chunk.
    ///
    /// May return [`AutomergeError::AuditModeRequired`] outside audit mode.
    pub fn get_last_local_change_legacy(&self) -> Result<Option<Change>, AutomergeError> {
        let Some(id) = self.last_local_change_id() else {
            return Ok(None);
        };
        let hash = self
            .change_graph
            .get_hash_for_change_id(&id, &self.ops.actors)?;
        self.get_change_by_hash(&hash)
    }

    /// The last change this actor made, as a bundle.
    ///
    /// `None` when this actor has not committed anything.
    pub fn get_last_local_change(&self) -> Result<Option<Bundle>, AutomergeError> {
        let Some(id) = self.last_local_change_id() else {
            return Ok(None);
        };
        let Some(node) = self.node_for_change_id(&id) else {
            return Ok(None);
        };
        // widened past parents whose hashes were freed, as `save_after` is
        let nodes = self.change_graph.nodes_back_to_retained([node]);
        let deps: Vec<ChangeHash> = nodes
            .iter()
            .flat_map(|n| self.change_graph.parent_hashes(*n))
            .collect();
        self.bundle_nodes(nodes, &deps)
    }

    fn last_local_change_id(&self) -> Option<ChangeId> {
        let actor = self.get_actor_index()?;
        let seq = self.change_graph.seq_for_actor(actor);
        (seq != 0).then(|| self.change_id_at(actor, seq))
    }

    /// The clock a read at `heads` observes: causal(heads) ∩ mask.
    pub(crate) fn visible(&self, heads: &[ChangeId]) -> Result<VisibleClock, AutomergeError> {
        let nodes = self.nodes_for_change_ids(heads)?;
        Ok(VisibleClock::new(
            self.change_graph.clock_for_nodes(nodes),
            self.mask(),
        ))
    }

    pub(crate) fn visible_current(&self) -> VisibleClock {
        VisibleClock::new(self.change_graph.current_clock(), self.mask())
    }

    /// A current read when `heads` is `None` or the current heads, else a
    /// historical one. Only sound while no transaction is open: a read around
    /// one must use [`Self::read_scoped`].
    pub(crate) fn read_at(&self, heads: Option<&[ChangeId]>) -> Result<ReadAt<'_>, AutomergeError> {
        let Some(heads) = heads else {
            return Ok(self.read_current());
        };
        let nodes = self.nodes_for_change_ids(heads)?;
        if self.change_graph.nodes_are_heads(&nodes) {
            Ok(self.read_current())
        } else {
            Ok(ReadAt::at(Cow::Owned(VisibleClock::new(
                self.change_graph.clock_for_nodes(nodes),
                self.mask(),
            ))))
        }
    }

    /// A read at `clock`, which already carries any mask, or of the current
    /// state when `None`.
    pub(crate) fn read_clock(&self, clock: Option<Clock>) -> ReadAt<'_> {
        match clock {
            Some(clock) => ReadAt::at(Cow::Owned(VisibleClock::new(clock, None))),
            None => self.read_current(),
        }
    }

    /// Return a [`ReadAt`] for the current state of the document, carrying
    /// the active write-frontier [`Mask`], if any.
    ///
    /// Equivalent to `doc.read_at(None)`.
    pub(crate) fn read_current(&self) -> ReadAt<'_> {
        ReadAt::current(self.mask().map(Cow::Borrowed))
    }

    /// Always a historical read: a transaction's pending ops are indexed but
    /// not under the graph's heads, and a masked isolated actor's scope can
    /// equal the current visible clock, so the indexed fast path must not
    /// apply.
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

    pub(crate) fn isolate_actor(&mut self, heads: &[ChangeId]) -> Isolation {
        let mut actor_index = self.get_isolated_actor_index(0);
        let mut clock = self
            .visible(heads)
            .expect("validated ids always have clocks");

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
                        && self
                            .author
                            .as_ref()
                            .is_some_and(|a| self.write_frontier.is_author_masked(a)));
                if !masked {
                    clock.isolate(actor_index);
                }
                break;
            }
            actor_index = self.get_isolated_actor_index(i);
            // need to recompute the clock b/c the actor indexes may have changed
            clock = self
                .visible(heads)
                .expect("validated ids always have clocks");
        }

        let seq = self.change_graph.seq_for_actor(actor_index) + 1;

        Isolation {
            actor_index,
            seq,
            clock,
        }
    }

    fn get_hash(&self, id: &ChangeId) -> Result<ChangeHash, AutomergeError> {
        self.change_graph
            .get_hash_for_change_id(id, &self.ops.actors)
    }

    pub(crate) fn update_history_batch(&mut self, changes: &[Change]) {
        let owes_gc = self
            .change_graph
            .add_changes(
                changes
                    .iter()
                    .map(|c| (c, self.ops.lookup_actor(c.actor_id()).unwrap())),
                &mut self.authors,
            )
            .unwrap();
        if owes_gc | self.rebuild_unhashed_fragment_pins() {
            self.change_graph.gc_after_batch();
        }
    }

    pub(crate) fn update_history(&mut self, change: &Change) {
        let actor_index = self
            .ops
            .lookup_actor(change.actor_id())
            .expect("Change's actor not already in the document");

        let owes_gc = self
            .change_graph
            .add_change(change, actor_index, &mut self.authors)
            .expect("Change's deps should already be in the document");
        if owes_gc | self.rebuild_unhashed_fragment_pins() {
            self.change_graph.gc_after_batch();
        }
    }

    fn rebuild_unhashed_fragment_pins(&mut self) -> bool {
        let pins = self.change_graph.take_unhashed_fragment_pins();
        self.rebuild_hashes(pins)
    }

    /// Records the hash of each of `nodes` that lacks one, rebuilt from
    /// the document's ops. Whether a GC is owed.
    fn rebuild_hashes(&mut self, mut nodes: Vec<crate::change_graph::NodeIdx>) -> bool {
        nodes.retain(|n| self.change_graph.hash_for_node(*n).is_none());
        if nodes.is_empty() {
            return false;
        }
        // one rehash back to the retained hashes for all of them
        let span = self
            .change_graph
            .nodes_back_to_retained(nodes.iter().copied());
        let hashes = self
            .rehash(span.clone())
            .expect("rebuilding the hashes of changes this document has");
        let mut owes_gc = false;
        for node in nodes {
            let pos = span
                .binary_search(&node)
                .expect("a node is in its own ancestry");
            owes_gc |= self.change_graph.record_node_hash(node, hashes[pos]);
        }
        owes_gc
    }

    fn apply_actor_shift(&mut self, shift: &ActorShift) {
        self.change_graph.insert_actor(shift);
        self.authors.insert_actor(shift);
        self.actor.shift(shift);
        self.visibility.insert_actor(shift);
        self.actor_generation.bump();
    }

    /// Insert every actor in `actors` the document lacks, returning the ones
    /// inserted for [`Self::undo_actor_refs`].
    pub(crate) fn put_actor_refs(&mut self, actors: &[ActorId]) -> Vec<ActorId> {
        let inserted = self.ops.insert_actors(actors);
        for (_, shift) in &inserted {
            self.apply_actor_shift(shift);
        }
        inserted.into_iter().map(|(actor, _)| actor).collect()
    }

    /// Undo a [`Self::put_actor_refs`].
    pub(crate) fn undo_actor_refs(&mut self, added: &[ActorId]) {
        let mut idxs: Vec<usize> = added
            .iter()
            .filter_map(|a| self.ops.lookup_actor(a))
            .collect();
        idxs.sort_unstable();
        for idx in idxs.into_iter().rev() {
            self.remove_actor(idx);
        }
    }

    fn put_actor(&mut self, actor: ActorId) -> usize {
        match self.ops.insert_actor(actor) {
            ActorInsert::Existing(idx) => idx,
            ActorInsert::Inserted(shift) => {
                self.apply_actor_shift(&shift);
                shift.index()
            }
        }
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
                        OpType::MarkBegin(_, crate::op_set2::types::MarkData { name, value }) => {
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

    /// Patches from `before` to `after`, each given as heads or a
    /// [`Viewpoint`]. Heads are read under the current write-frontier; a
    /// viewpoint under its own, so masking and revealing authors is reported
    /// even when the heads did not move. Reversing the arguments observes the
    /// same changes in the opposite order.
    ///
    /// Returns [`AutomergeError::InvalidChangeId`] if either side names a
    /// change this document does not have.
    pub fn diff(
        &self,
        before: impl Into<Viewpoint>,
        after: impl Into<Viewpoint>,
    ) -> Result<Vec<Patch>, AutomergeError> {
        self.diff_obj_inner(ObjMeta::root(), &before.into(), &after.into(), true)
    }

    pub(crate) fn diff_obj_inner(
        &self,
        obj: ObjMeta,
        before: &Viewpoint,
        after: &Viewpoint,
        recursive: bool,
    ) -> Result<Vec<Patch>, AutomergeError> {
        let before = self.visible_for_viewpoint(before)?;
        let clock = if self.is_current(after) {
            ClockRange::between_current(before, self.read_current())
        } else {
            ClockRange::between(before, self.visible_for_viewpoint(after)?)
        };
        let mut patch_accumulator = PatchAccumulator::new(clock.read_after().historical().cloned());
        DiffIter::log(self, obj, clock, &mut patch_accumulator, recursive);
        Ok(patch_accumulator.make_patches(self))
    }

    /// Whether `viewpoint` reads exactly what the current indexes hold.
    pub(crate) fn is_current(&self, viewpoint: &Viewpoint) -> bool {
        viewpoint.heads == self.get_heads()
            && (viewpoint.write_frontier.is_none()
                || viewpoint.epoch == Some(self.visibility_epoch))
    }

    /// The document's current [`Viewpoint`].
    pub fn viewpoint(&self) -> Viewpoint {
        let mut view = self.viewpoint_at(self.get_heads());
        view.cache = Some((self.actor_generation.get(), self.visible_current()));
        view.epoch = Some(self.visibility_epoch);
        view
    }

    /// `heads` under the current write-frontier.
    pub(crate) fn viewpoint_at(&self, heads: Vec<ChangeId>) -> Viewpoint {
        Viewpoint {
            heads,
            write_frontier: Some(self.write_frontier.clone()),
            pending: Some(self.visibility.pending.clone()),
            ..Default::default()
        }
    }

    pub(crate) fn visible_for_viewpoint(
        &self,
        view: &Viewpoint,
    ) -> Result<VisibleClock, AutomergeError> {
        if let Some((generation, clock)) = &view.cache {
            if *generation == self.actor_generation.get() {
                return Ok(clock.clone());
            }
        }
        let Some(write_frontier) = &view.write_frontier else {
            return self.visible(&view.heads);
        };
        let nodes = self.nodes_for_change_ids(&view.heads)?;
        let graph = &self.change_graph;
        let history = view
            .pending
            .is_none()
            .then(|| graph.calculate_clock(nodes.clone()));
        let visibility = visibility::FrontierVisibility::resolving(
            write_frontier,
            graph,
            &self.authors,
            &self.ops.actors,
            |boundary, node| match &view.pending {
                Some(pending) => !pending.contains(boundary),
                None => {
                    let (actor, seq) = graph.actor_seq(node);
                    history
                        .as_ref()
                        .and_then(|h| h.get_for_actor(&actor))
                        .is_some_and(|s| s.get() >= seq)
                }
            },
        );
        Ok(VisibleClock::new(
            self.change_graph.clock_for_nodes(nodes),
            visibility.mask(),
        ))
    }

    /// The patches since the previous call (or since the empty document),
    /// including write-frontier changes.
    pub fn diff_incremental(&mut self) -> Vec<Patch> {
        let before = self.diff_cursor.clone();
        let patches = self.dirty_diff_patches(&before);
        self.clear_dirty();
        self.diff_cursor = self.viewpoint();
        patches
    }

    /// [`Self::diff`] starting at `obj`; with `recursive` false, child objects
    /// are not diffed.
    pub fn diff_obj(
        &self,
        obj: &ExId,
        before: impl Into<Viewpoint>,
        after: impl Into<Viewpoint>,
        recursive: bool,
    ) -> Result<Vec<Patch>, AutomergeError> {
        let obj = self.exid_to_obj(obj.as_ref())?;
        self.diff_obj_inner(obj, &before.into(), &after.into(), recursive)
    }

    /// This document's [`AuditMode`].
    #[doc(hidden)]
    pub fn audit_mode(&self) -> AuditMode {
        H::AUDIT
    }

    pub fn fragments<R: RangeBounds<usize>>(&self, levels: R) -> Vec<Fragment> {
        self.change_graph
            .fragments(&self.get_head_hashes(), levels, &self.ops.actors)
    }

    pub fn get_fragment(&self, head: ChangeHash) -> Option<Fragment> {
        self.change_graph.get_fragment(head, &self.ops.actors)
    }

    fn fragment_nodes(
        &self,
        f: &Fragment,
    ) -> Result<Vec<crate::change_graph::NodeIdx>, AutomergeError> {
        let mut nodes = f
            .members
            .iter()
            .map(|id| self.node_for_change_id(id))
            .collect::<Option<Vec<_>>>()
            .ok_or(AutomergeError::InvalidFragment(
                "fragment references an unknown change",
            ))?;
        nodes.sort_unstable();
        nodes.dedup();
        Ok(nodes)
    }

    /// Build a bundle from the changes it delivers (`heads`) and the
    /// set covering everything before it (`boundary`).
    pub fn make_bundle(
        &self,
        heads: &[ChangeId],
        boundary: &[ChangeId],
    ) -> Result<Bundle, AutomergeError> {
        let unknown = || AutomergeError::InvalidFragment("bundle names an unknown change");
        let node_of = |id: &ChangeId| self.node_for_change_id(id).ok_or_else(unknown);
        let head_nodes = heads.iter().map(node_of).collect::<Result<Vec<_>, _>>()?;
        let boundary_nodes = boundary
            .iter()
            .map(node_of)
            .collect::<Result<Vec<_>, _>>()?;

        let clock = self.change_graph.calculate_clock(boundary_nodes.clone());
        let members = self.change_graph.members_between(head_nodes, &clock);

        let hash_of = |n: &crate::change_graph::NodeIdx| {
            self.change_graph.hash_for_node(*n).ok_or_else(unknown)
        };
        // a head another head depends on is not one
        let head_hashes = self
            .change_graph
            .heads_of(&members)
            .iter()
            .map(hash_of)
            .collect::<Result<Vec<_>, _>>()?;
        let boundary_hashes = boundary_nodes
            .iter()
            .map(hash_of)
            .collect::<Result<Vec<_>, _>>()?;

        let members = self.change_graph.bundle_members(&members);
        let storage = Bundle::storage_for_nodes(&self.ops, &self.change_graph, &members)?;
        self.assemble_bundle(
            &head_hashes,
            &boundary_hashes,
            &members,
            storage,
            NameHashes::All,
        )
    }

    /// [`Self::make_bundle`] for a [`Fragment`].
    pub fn make_bundle_from_fragment(&self, f: &Fragment) -> Result<Bundle, AutomergeError> {
        let unknown = || AutomergeError::InvalidFragment("fragment names an unknown change");
        let id_of = |h: &ChangeHash| {
            self.change_graph
                .node_by_hash(h)
                .map(|n| self.change_graph.change_id(n, &self.ops.actors))
                .ok_or_else(unknown)
        };
        let heads = vec![id_of(&f.head)?];
        let boundary = f
            .boundary
            .iter()
            .map(id_of)
            .collect::<Result<Vec<_>, _>>()?;

        let bundle = self.make_bundle(&heads, &boundary)?;

        #[cfg(debug_assertions)]
        {
            let sorted = |mut v: Vec<ChangeHash>| {
                v.sort_unstable();
                v
            };
            debug_assert_eq!(
                sorted(bundle.checkpoints.iter().map(|(_, h)| *h).collect()),
                sorted(f.checkpoints.clone()),
                "derived checkpoints disagree with the fragment's",
            );
            debug_assert_eq!(
                sorted(bundle.boundary.iter().map(|(h, _)| *h).collect()),
                sorted(
                    f.boundary
                        .iter()
                        .copied()
                        .filter(|h| bundle.deps().contains(h))
                        .collect()
                ),
                "derived boundary disagrees with the fragment's",
            );
            let mut derived: Vec<ChangeId> = bundle
                .member_actors
                .iter()
                .zip(bundle.member_seqs.iter())
                .map(|(a, s)| ChangeId::new(*s, bundle.actors()[usize::from(*a)].clone(), 0))
                .collect();
            let mut declared = f.members.clone();
            derived.sort();
            declared.sort();
            debug_assert_eq!(
                derived, declared,
                "derived members disagree with the fragment's"
            );
        }

        Ok(bundle)
    }

    #[doc(hidden)]
    pub fn bundle_for_fragment(&self, f: &Fragment) -> Result<Bundle, AutomergeError> {
        let nodes = self.fragment_nodes(f)?;
        let members = self.change_graph.bundle_members(&nodes);
        let storage = Bundle::storage_for_nodes(&self.ops, &self.change_graph, &members)?;
        self.assemble_bundle(&[f.head], &f.boundary, &members, storage, NameHashes::All)
    }

    fn assemble_bundle(
        &self,
        heads: &[ChangeHash],
        boundary: &[ChangeHash],
        members: &crate::change_graph::Members<'_>,
        storage: crate::storage::BundleStorage<'static, crate::storage::change::Verified>,
        names: NameHashes,
    ) -> Result<Bundle, AutomergeError> {
        let crate::change_graph::ReceiverHashes { retained, outside } =
            self.change_graph.hashes_to_retain(members, names)?;
        let retained: Vec<(usize, ChangeHash)> = retained
            .into_iter()
            .filter(|(_, h)| h.fragment_level() == 0 && !heads.contains(h))
            .collect();
        let unknown = || AutomergeError::InvalidFragment("fragment references an unknown change");
        let member_index = |h: &ChangeHash| -> Option<usize> {
            members.position(self.change_graph.node_by_hash(h)?)
        };
        let heads = heads
            .iter()
            .map(|h| member_index(h).map(|i| (*h, i)))
            .collect::<Option<Vec<_>>>()
            .ok_or_else(unknown)?;
        let mut checkpoints = self
            .change_graph
            .leveled_members(members)
            .filter(|(_, h)| !heads.iter().any(|(head, _)| head == h))
            .map(|(n, h)| members.position(n).map(|i| (i, h)))
            .collect::<Option<Vec<_>>>()
            .ok_or_else(unknown)?;
        checkpoints.sort_unstable_by_key(|(_, h)| *h);
        let change_id = |h: &ChangeHash| -> Option<ChangeId> {
            let n = self.change_graph.node_by_hash(h)?;
            Some(self.change_graph.change_id(n, &self.ops.actors))
        };
        // an entry no member depends on is behind another entry, and a
        // fragment's boundary lists every earlier fragment it covers; but
        // the receiver needs the outside hashes its fragments pin that
        // aren't deps already
        let deps: HashSet<&ChangeHash> = storage.deps().iter().collect();
        let mut boundary: Vec<ChangeHash> = boundary
            .iter()
            .filter(|h| deps.contains(h))
            .chain(outside.iter().filter(|h| !deps.contains(h)))
            .copied()
            .collect();
        boundary.sort_unstable();
        boundary.dedup();
        let boundary = boundary
            .iter()
            .map(|h| change_id(h).map(|id| (*h, id)))
            .collect::<Option<Vec<_>>>()
            .ok_or_else(unknown)?;
        let dep_ids = storage
            .deps()
            .iter()
            .map(change_id)
            .collect::<Option<Vec<_>>>()
            .ok_or_else(unknown)?;
        // the bundle's actors are a sorted subset of the document's
        let mut actor_index =
            vec![crate::op_set2::types::ActorIdx::default(); self.ops.actors.len()];
        for (i, actor) in storage.actors.iter().enumerate() {
            let doc_index = self.ops.actors.lookup(actor).ok_or_else(unknown)?;
            actor_index[doc_index] = crate::op_set2::types::ActorIdx::from(i);
        }
        let (member_actors, member_seqs) = self.change_graph.actors_and_seqs(members, &actor_index);

        Ok(Bundle {
            heads,
            checkpoints,
            retained,
            boundary,
            dep_ids,
            member_actors,
            member_seqs,
            storage,
        })
    }

    pub fn bundles_for_fragments<I: IntoIterator<Item = Fragment>>(
        &self,
        fragments: I,
    ) -> Result<Vec<Vec<u8>>, AutomergeError> {
        let fragments: Vec<Fragment> = fragments.into_iter().collect();
        let nodes = fragments
            .iter()
            .map(|f| self.fragment_nodes(f))
            .collect::<Result<Vec<_>, _>>()?;
        let members: Vec<_> = nodes
            .iter()
            .map(|n| self.change_graph.bundle_members(n))
            .collect();
        // one shared pass rather than a document walk per bundle
        let storages = Bundle::storage_for_node_sets(&self.ops, &self.change_graph, &members)?;
        fragments
            .iter()
            .zip(members.iter())
            .zip(storages)
            .map(|((f, n), storage)| {
                Ok(self
                    .assemble_bundle(&[f.head], &f.boundary, n, storage, NameHashes::All)?
                    .bytes())
            })
            .collect()
    }

    /// Apply a bundle's changes. To apply one bundle to several documents,
    /// clone it.
    ///
    /// Bundles are not queued: one depending on a change this document lacks
    /// fails with [`AutomergeError::MissingDeps`] and changes nothing, and
    /// must be applied again once its dependencies have been.
    pub fn apply_bundle(&mut self, bundle: Bundle) -> Result<(), AutomergeError> {
        let result = self.apply_bundle_inner(bundle);
        if result.is_ok() {
            self.assert_no_unused_actors(cfg!(debug_assertions));
        }
        result
    }

    fn apply_bundle_inner(&mut self, bundle: Bundle) -> Result<(), AutomergeError> {
        H::apply_bundle(self, bundle)?;
        if self.queue.is_empty() {
            return Ok(());
        }
        // a queued change the bundle delivered would otherwise apply twice
        let graph = &self.change_graph;
        let actors = &self.ops.actors;
        self.queue.remove_present(|c| {
            graph.lookup_change_for_apply(&c.hash(), &c.id(), actors)
                == crate::change_graph::ApplyLookup::Present
        });
        self.apply_changes_batch(std::iter::empty())
    }

    /// Rebuilds and hashes every member instead of trusting the bundle's
    /// metadata.
    pub(crate) fn apply_bundle_verified(&mut self, bundle: Bundle) -> Result<(), AutomergeError> {
        let hashes = bundle.audit::<ChangeHash>()?;
        // checked before applying, so a rejected bundle leaves no trace
        for dep in bundle.deps() {
            if !self.change_graph.has_change(dep)? {
                return Err(AutomergeError::MissingDeps);
            }
        }
        self.apply_bundle_columns(bundle, Some(hashes))
    }

    /// `member_hashes`, when given, are every member's verified hash, and
    /// what the bundle says about the document is checked, not trusted.
    pub(crate) fn apply_bundle_columns(
        &mut self,
        mut bundle: Bundle,
        member_hashes: Option<Vec<ChangeHash>>,
    ) -> Result<(), AutomergeError> {
        let bundle_ops = bundle.take_bundle_ops()?;
        let bundle = &bundle;
        let Some(plan) = self.plan_bundle(bundle, member_hashes)? else {
            return Ok(());
        };

        // failures below must undo the actor table; the graph is only
        // touched once nothing can fail
        let hashed = plan.kept_hashes.is_some();
        let (added, actor_map) = self.add_bundle_actors(bundle);
        let staged = (|| -> Result<_, AutomergeError> {
            let clock = self.change_graph.current_clock();
            let mask = self.bundle_mask(bundle, &actor_map)?;
            // ops the clock covers belong to skipped members and are dropped
            let ops = BundleApply::new(
                bundle,
                actor_map.clone(),
                &clock,
                plan.overlap,
                &self.ops,
                bundle_ops,
                mask,
            )?;
            let resolved = ops.resolve(self)?;
            let graph = self.commit_bundle_graph(bundle, plan, &actor_map)?;
            Ok((ops, resolved, graph))
        })();
        let (ops, resolved, owes_gc) = staged.inspect_err(|_| self.undo_actor_refs(&added))?;
        ops.commit(self, resolved);
        self.debug_assert_mask_indexes();
        self.republish_if_pending();
        let mut owes_gc = owes_gc | self.rebuild_unhashed_fragment_pins();
        if !hashed {
            let missing = self.change_graph.missing_retained_hashes();
            debug_assert!(
                missing.is_empty(),
                "bundle left out {} change hashes this document must retain",
                missing.len()
            );
            owes_gc |= self.rebuild_hashes(missing);
        }
        if owes_gc {
            self.change_graph.gc_after_batch();
        }
        Ok(())
    }

    /// Everything [`Self::apply_bundle_columns`] decides about `bundle`
    /// before writing to the document; `None` when the document already
    /// has every member.
    pub(crate) fn plan_bundle<'b>(
        &self,
        bundle: &'b Bundle,
        member_hashes: Option<Vec<ChangeHash>>,
    ) -> Result<Option<BundlePlan<'b>>, AutomergeError> {
        let untagged = member_hashes.is_none() && bundle.is_rehashable();
        let plan = self.plan_bundle_with(bundle, member_hashes)?;
        if !untagged || plan.is_none() {
            return Ok(plan);
        }
        // a tiny whole document may leave out hashes for us to rebuild;
        // hashing it before anything is written also checks it
        self.plan_bundle_with(bundle, Some(bundle.audit::<ChangeHash>()?))
    }

    fn plan_bundle_with<'b>(
        &self,
        bundle: &'b Bundle,
        member_hashes: Option<Vec<ChangeHash>>,
    ) -> Result<Option<BundlePlan<'b>>, AutomergeError> {
        let member_actors = &bundle.member_actors;
        let member_seqs = &bundle.member_seqs;
        let num_members = member_seqs.len();

        // parsing validated the bundle's shape, so its indexes are in bounds
        let member_id = |i: usize| -> ChangeId {
            ChangeId::new(
                member_seqs[i],
                bundle.actors()[usize::from(member_actors[i])].clone(),
                0,
            )
        };

        // changes arrive in per-actor order, so a member is already here
        // exactly when this clock covers it
        let seq_clock = self.change_graph.current_seq_clock();

        // new members must extend their actor's sequence without gaps
        let mut keep = vec![false; num_members];
        let mut kept_index = vec![usize::MAX; num_members];
        let mut num_kept = 0;
        let mut next_seq: Vec<Option<u64>> = vec![None; bundle.actors().len()];
        // a kept member may pin a member this document freed the hash of
        let mut freed = Vec::new();
        for i in 0..num_members {
            let actor = usize::from(member_actors[i]);
            let seq = member_seqs[i].get();
            let have = self
                .ops
                .lookup_actor(&bundle.actors()[actor])
                .and_then(|idx| seq_clock.get_for_actor(&idx))
                .map(|s| s.get() as u64)
                .unwrap_or(0);
            let next = next_seq[actor].unwrap_or(have + 1);
            match seq.cmp(&next) {
                Ordering::Less => {
                    // already have this change, unless the actor forked
                    let node = self.node_for_change_id(&member_id(i));
                    if let (Some(hashes), Some(node)) = (&member_hashes, node) {
                        match self.change_graph.hash_for_node(node) {
                            Some(h) if h != hashes[i] => {
                                let actor = bundle.actors()[actor].clone();
                                return Err(AutomergeError::DuplicateSeqNumber(seq, actor));
                            }
                            Some(_) => {}
                            None => freed.push((node, hashes[i])),
                        }
                    }
                    continue;
                }
                Ordering::Greater => return Err(AutomergeError::MissingDeps),
                Ordering::Equal => {}
            }
            next_seq[actor] = Some(next + 1);
            keep[i] = true;
            kept_index[i] = num_kept;
            num_kept += 1;
        }

        if num_kept == 0 {
            return Ok(None);
        }
        let overlap = num_kept < num_members;
        let kept_hashes: Option<Vec<ChangeHash>> = member_hashes.map(|hashes| {
            (0..num_members)
                .filter(|i| keep[*i])
                .map(|i| hashes[i])
                .collect()
        });
        let check_external = |graph: &ChangeGraph<H>, node, hash| {
            if kept_hashes.is_some() && graph.hash_for_node(node) != Some(hash) {
                Err(AutomergeError::from(MalformedBundle::Invalid(
                    "an external change id names another change",
                )))
            } else {
                Ok(())
            }
        };

        // only the commit below appends to the graph, so the kept members'
        // nodes are known before they exist
        let base = self.change_graph.len() as u32;
        let member_node = |i: usize| -> Option<crate::change_graph::NodeIdx> {
            if keep[i] {
                Some(crate::change_graph::NodeIdx(base + kept_index[i] as u32))
            } else {
                self.node_for_change_id(&member_id(i))
            }
        };

        let mut hash_pairs: Vec<(crate::change_graph::NodeIdx, ChangeHash)> = freed;
        hash_pairs.reserve(bundle.boundary.len() + bundle.dep_ids.len());
        for (hash, id) in &bundle.boundary {
            let node = self
                .node_for_change_id(id)
                .ok_or(AutomergeError::MissingDeps)?;
            check_external(&self.change_graph, node, *hash)?;
            hash_pairs.push((node, *hash));
        }

        // a declared dep the document lacks is `MissingDeps` even if no
        // member references it
        let mut ext_nodes = Vec::with_capacity(bundle.dep_ids.len());
        for (i, dep_id) in bundle.dep_ids.iter().enumerate() {
            let node = self
                .node_for_change_id(dep_id)
                .ok_or(AutomergeError::MissingDeps)?;
            check_external(&self.change_graph, node, bundle.deps()[i])?;
            hash_pairs.push((node, bundle.deps()[i]));
            ext_nodes.push(node);
        }

        let mut graph_members: Vec<BundleMember<'_>> = Vec::new();
        if overlap {
            let members = bundle.changes()?;
            graph_members.reserve(num_kept);
            for (i, m) in members.iter().enumerate() {
                if !keep[i] {
                    continue;
                }
                let mut deps = Vec::with_capacity(m.deps.len());
                for d in &m.deps {
                    let d = *d as usize;
                    deps.push(if d >= num_members {
                        let node = ext_nodes
                            .get(d - num_members)
                            .ok_or(MalformedBundle::Invalid("bad dep index"))?;
                        BundleDep::Node(*node)
                    } else if keep[d] {
                        BundleDep::Member(kept_index[d])
                    } else {
                        BundleDep::Node(member_node(d).ok_or(AutomergeError::MissingDeps)?)
                    });
                }
                graph_members.push(BundleMember {
                    actor: usize::from(member_actors[i]),
                    seq: m.seq,
                    max_op: m.max_op,
                    num_ops: 1 + m.max_op - m.start_op,
                    timestamp: m.timestamp,
                    message: m.message.as_ref().map(|s| s.to_string()),
                    extra: Cow::Borrowed(m.extra.as_ref()),
                    deps,
                });
            }
        }

        let mut head_nodes = Vec::with_capacity(bundle.heads.len());
        for (hash, index) in &bundle.heads {
            head_nodes.push((
                member_node(*index).ok_or(MalformedBundle::Invalid(
                    "bundle head is not a member of the bundle",
                ))?,
                *hash,
            ));
        }
        let checkpoint_nodes: Vec<_> = bundle
            .checkpoints
            .iter()
            .filter_map(|(i, hash)| member_node(*i).map(|n| (n, *hash)))
            .collect();
        let retained_nodes: Vec<_> = bundle
            .retained
            .iter()
            .filter_map(|(i, hash)| member_node(*i).map(|n| (n, *hash)))
            .collect();
        self.check_hash_claims(&[&hash_pairs, &head_nodes, &checkpoint_nodes, &retained_nodes])?;

        Ok(Some(BundlePlan {
            base,
            overlap,
            kept_hashes,
            graph_members,
            ext_nodes,
            hash_pairs,
            head_nodes,
            checkpoint_nodes,
            retained_nodes,
        }))
    }

    /// Rejects claims that give one change two hashes or one hash two
    /// changes, among themselves or against the document.
    fn check_hash_claims(
        &self,
        claims: &[&[(crate::change_graph::NodeIdx, ChangeHash)]],
    ) -> Result<(), AutomergeError> {
        let mut claims: Vec<_> = claims.iter().flat_map(|c| c.iter().copied()).collect();
        claims.sort_unstable();
        claims.dedup();
        let conflict = || MalformedBundle::Invalid("a change hash names two changes").into();
        if claims.windows(2).any(|w| w[0].0 == w[1].0) {
            return Err(conflict());
        }
        claims.sort_unstable_by_key(|(_, h)| *h);
        if claims.windows(2).any(|w| w[0].1 == w[1].1) {
            return Err(conflict());
        }
        let graph = &self.change_graph;
        for (node, hash) in &claims {
            if graph.node_by_hash(hash).is_some_and(|n| n != *node)
                || graph.hash_for_node(*node).is_some_and(|h| h != *hash)
            {
                return Err(conflict());
            }
            // fragment-level hashes are never freed
            let freed = (node.0 as usize) < graph.len() && graph.hash_for_node(*node).is_none();
            if freed && hash.fragment_level() > 0 {
                return Err(MalformedBundle::Invalid(
                    "a freed change claims a fragment-level hash",
                )
                .into());
            }
        }
        Ok(())
    }

    /// Adds `bundle`'s actors; returns those that were new, for
    /// [`Self::undo_actor_refs`], and the bundle-to-document actor map.
    pub(crate) fn add_bundle_actors(&mut self, bundle: &Bundle) -> (Vec<ActorId>, Vec<usize>) {
        let added = self.put_actor_refs(bundle.actors());
        let actor_map: Vec<usize> = bundle
            .actors()
            .iter()
            .map(|a| self.ops.lookup_actor(a).expect("actor was just inserted"))
            .collect();
        (added, actor_map)
    }

    /// The write-frontier mask for `bundle`'s ops, when it hides some.
    pub(crate) fn bundle_mask(
        &mut self,
        bundle: &Bundle,
        actor_map: &[usize],
    ) -> Result<Option<Clock>, AutomergeError> {
        let member_seqs = &bundle.member_seqs;
        let member_actors = &bundle.member_actors;
        if !self.write_frontier.is_empty() && member_seqs.iter().any(|s| s.get() == 1) {
            let first_authors: Vec<_> = bundle
                .changes()?
                .iter()
                .filter(|c| c.seq == 1)
                .filter_map(|c| {
                    let author = crate::change::decode_author_footer(&c.extra)?;
                    Some((actor_map[c.actor], author.into_owned()))
                })
                .collect();
            self.register_incoming_authors(first_authors);
        }
        let restricted = |actor: usize| self.mask().is_some_and(|m| m.restricts(actor));
        Ok(
            if member_actors
                .iter()
                .any(|a| restricted(actor_map[usize::from(*a)]))
            {
                let members: Vec<_> = bundle
                    .changes()?
                    .iter()
                    .map(|c| (actor_map[c.actor], c.max_op))
                    .collect();
                self.mask_hiding(members)
            } else {
                None
            },
        )
    }

    /// Adds the planned members to the change graph and records the
    /// hashes the bundle names. Returns whether a GC is owed.
    pub(crate) fn commit_bundle_graph(
        &mut self,
        bundle: &Bundle,
        plan: BundlePlan<'_>,
        actor_map: &[usize],
    ) -> Result<bool, AutomergeError> {
        let BundlePlan {
            base,
            overlap,
            kept_hashes,
            mut graph_members,
            ext_nodes,
            hash_pairs,
            head_nodes,
            checkpoint_nodes,
            retained_nodes,
        } = plan;
        if overlap {
            for m in &mut graph_members {
                m.actor = actor_map[m.actor];
            }
        }
        let mut owes_gc = if overlap {
            self.change_graph.add_bundle_members(
                graph_members,
                kept_hashes.as_deref(),
                &mut self.authors,
                &self.ops.actors,
            )
        } else {
            self.change_graph.add_bundle_members_cols(
                &bundle.change_cols(),
                &bundle.member_actors,
                &bundle.member_seqs,
                actor_map,
                &ext_nodes,
                kept_hashes.as_deref(),
                &mut self.authors,
                &self.ops.actors,
            )
        }?;
        for (node, hash) in hash_pairs {
            owes_gc |= self.change_graph.record_node_hash(node, hash);
        }
        // in node order, so the fragment index matches one built from the
        // whole graph
        let mut members: Vec<_> = head_nodes
            .into_iter()
            .map(|(n, h)| (n, h, n.0 >= base))
            .chain(checkpoint_nodes.into_iter().map(|(n, h)| (n, h, false)))
            .chain(retained_nodes.into_iter().map(|(n, h)| (n, h, false)))
            .collect();
        members.sort_unstable_by_key(|(n, ..)| *n);
        for (node, hash, head) in members {
            owes_gc |= if head {
                self.change_graph.record_fragment_head(node, hash)
            } else {
                self.change_graph.record_node_hash(node, hash)
            };
        }
        self.change_graph.pin_deferred_loose();
        Ok(owes_gc)
    }
}

/// What [`Automerge::plan_bundle`] decides about a bundle.
pub(crate) struct BundlePlan<'b> {
    base: u32,
    overlap: bool,
    kept_hashes: Option<Vec<ChangeHash>>,
    graph_members: Vec<BundleMember<'b>>,
    ext_nodes: Vec<crate::change_graph::NodeIdx>,
    hash_pairs: Vec<(crate::change_graph::NodeIdx, ChangeHash)>,
    head_nodes: Vec<(crate::change_graph::NodeIdx, ChangeHash)>,
    checkpoint_nodes: Vec<(crate::change_graph::NodeIdx, ChangeHash)>,
    retained_nodes: Vec<(crate::change_graph::NodeIdx, ChangeHash)>,
}

impl<H: HashRetention> Automerge<H> {
    /// Test support: panics if the op columns are out of order, their
    /// indexes are stale, or they do not reproduce the document's history.
    #[doc(hidden)]
    pub fn validate_document(&self) {
        assert!(self.ops.validate_op_order(), "op columns out of order");
        self.ops.validate_indexes();

        let mut redoc = Automerge::new();
        redoc
            .apply_changes(self.get_changes(&[]).expect("change reconstruction"))
            .expect("replaying reconstructed changes");
        assert_eq!(
            self.get_heads(),
            redoc.get_heads(),
            "hash round-trip diverges"
        );
    }

    /// Every change's hash, in node order.
    fn compute_hashes(&self) -> Result<Vec<ChangeHash>, AutomergeError> {
        let inflate = |e: Box<dyn std::error::Error + Send + Sync + 'static>| {
            AutomergeError::Load(load::Error::InflateDocument(e))
        };
        let mut collector = ChangeCollector::try_new(self.change_graph.iter(), &self.ops.actors)
            .map_err(|e| inflate(Box::new(e)))?
            .hashing();
        let mut iter = self.ops.iter();
        while let Some(op) = iter.try_next().map_err(|e| inflate(Box::new(e)))? {
            let op_id = op.id;
            let op_succ = op.succ();
            collector.process_op(op);
            for id in op_succ {
                collector.process_succ(op_id, id);
            }
        }
        let collected = collector
            .collect_hashes()
            .map_err(|e| inflate(Box::new(e)))?;
        Ok(collected.hashes)
    }

    fn map_graph<H2: HashRetention>(
        self,
        f: impl FnOnce(ChangeGraph<H>) -> ChangeGraph<H2>,
    ) -> Automerge<H2> {
        let Automerge {
            queue,
            change_graph,
            authors,
            write_frontier,
            visibility,
            visibility_epoch,
            actor_generation,
            ops,
            actor,
            diff_cursor,
            author,
        } = self;
        Automerge {
            queue,
            change_graph: f(change_graph),
            authors,
            write_frontier,
            visibility,
            visibility_epoch,
            actor_generation,
            ops,
            actor,
            diff_cursor,
            author,
        }
    }

    /// The hash of `node`, rehashing from the nearest retained hashes if it
    /// was freed. Slow.
    pub(crate) fn rebuild_hash(&self, node: crate::change_graph::NodeIdx) -> Option<ChangeHash> {
        if let Some(h) = self.change_graph.hash_for_node(node) {
            return Some(h);
        }
        let nodes = self.change_graph.nodes_back_to_retained([node]);
        let pos = nodes.binary_search(&node).ok()?;
        self.rehash(nodes)?.get(pos).copied()
    }

    /// `nodes` must be sorted and include every unhashed ancestor.
    fn rehash(&self, nodes: Vec<crate::change_graph::NodeIdx>) -> Option<Vec<ChangeHash>> {
        let members = crate::change_graph::Members::new(&nodes);
        let storage = Bundle::storage_for_nodes(&self.ops, &self.change_graph, &members).ok()?;
        let changes = storage.to_changes().ok()?;
        Some(changes.iter().map(|c| c.hash()).collect())
    }

    /// Free change hashes that are no longer needed; see [`GcMode`].
    pub fn gc(&mut self) {
        self.change_graph.run_gc();
    }

    /// This document's [`GcMode`].
    pub fn gc_mode(&self) -> GcMode {
        self.change_graph.gc_mode()
    }

    /// Switch this document's [`GcMode`].
    ///
    /// Moving to [`GcMode::Auto`] runs any GC that [`GcMode::Manual`]
    /// deferred.
    pub fn set_gc_mode(&mut self, mode: GcMode) {
        self.change_graph.set_gc_mode(mode);
        if mode == GcMode::Auto && self.change_graph.gc_owed() {
            self.change_graph.run_gc();
        }
    }

    pub fn with_manual_gc(mut self) -> Self {
        self.set_gc_mode(GcMode::Manual);
        self
    }

    /// Get the heads of this document.
    ///
    /// The heads are the [`ChangeId`]s of the changes which have no
    /// successors in this document. Pass them to the `*_at` methods of
    /// [`crate::ReadDoc`] to read historical values.
    pub fn get_heads(&self) -> Vec<ChangeId> {
        self.change_graph.head_change_ids(&self.ops.actors)
    }

    /// The heads of this document as sorted [`ChangeHash`]es. Available
    /// whatever the audit mode.
    pub fn get_head_hashes(&self) -> Vec<ChangeHash> {
        self.change_graph.heads().collect()
    }

    pub(crate) fn node_for_change_id(&self, id: &ChangeId) -> Option<crate::change_graph::NodeIdx> {
        self.change_graph.node_for_change_id(id, &self.ops.actors)
    }

    pub(crate) fn change_id_at(&self, actor_idx: usize, seq: u64) -> ChangeId {
        ChangeId::from_doc_seq(seq, self.ops.actors[actor_idx].clone(), actor_idx)
    }

    pub(crate) fn nodes_for_change_ids(
        &self,
        ids: &[ChangeId],
    ) -> Result<Vec<crate::change_graph::NodeIdx>, AutomergeError> {
        ids.iter()
            .map(|id| {
                self.node_for_change_id(id)
                    .ok_or_else(|| AutomergeError::InvalidChangeId(id.to_string()))
            })
            .collect()
    }

    /// Get the hash of the change identified by `id`.
    ///
    /// Returns `Ok(None)` if no change by that `(actor, seq)` is in this
    /// document, and [`AutomergeError::AuditModeRequired`] if the change
    /// is present but its hash is not retained outside audit mode.
    pub fn change_id_to_hash(&self, id: &ChangeId) -> Result<Option<ChangeHash>, AutomergeError> {
        let Some(node) = self.node_for_change_id(id) else {
            return Ok(None);
        };
        self.change_graph
            .hash_for_node(node)
            .ok_or(AutomergeError::AuditModeRequired)
            .map(Some)
    }

    /// Get the [`ChangeId`] of the change with the given hash.
    ///
    /// Returns `Ok(None)` if the change is definitively not in this
    /// document, and [`AutomergeError::AuditModeRequired`] if the hash is
    /// not retained and so we cannot tell.
    pub fn hash_to_change_id(&self, hash: &ChangeHash) -> Result<Option<ChangeId>, AutomergeError> {
        self.change_graph
            .change_id_for_hash(hash, &self.ops.actors)
            .map_err(|_| AutomergeError::AuditModeRequired)
    }

    /// Convert a slice of hashes into [`ChangeId`]s.
    ///
    /// Errors with [`AutomergeError::MissingHash`] if a hash is not
    /// present in this document, and with
    /// [`AutomergeError::AuditModeRequired`] if a hash is not retained.
    pub fn hashes_to_change_ids(
        &self,
        hashes: &[ChangeHash],
    ) -> Result<Vec<ChangeId>, AutomergeError> {
        hashes
            .iter()
            .map(|h| {
                self.hash_to_change_id(h)?
                    .ok_or(AutomergeError::MissingHash(*h))
            })
            .collect()
    }

    /// Convert a slice of [`ChangeId`]s into hashes.
    ///
    /// Errors with [`AutomergeError::InvalidChangeId`] if an id is not
    /// present in this document, and with
    /// [`AutomergeError::AuditModeRequired`] if a change's hash is not
    /// retained outside audit mode.
    pub fn change_ids_to_hashes(
        &self,
        ids: &[ChangeId],
    ) -> Result<Vec<ChangeHash>, AutomergeError> {
        ids.iter()
            .map(|id| {
                self.change_id_to_hash(id)?
                    .ok_or_else(|| AutomergeError::InvalidChangeId(id.to_string()))
            })
            .collect()
    }

    /// [`Self::change_ids_to_hashes`], skipping ids this document does not
    /// know or no longer has a hash for.
    pub(crate) fn change_ids_to_hashes_lossy(&self, ids: &[ChangeId]) -> Vec<ChangeHash> {
        ids.iter()
            .filter_map(|id| self.change_id_to_hash(id).ok().flatten())
            .collect()
    }

    /// Whether this document contains the change identified by `id`.
    pub fn has_change_id(&self, id: &ChangeId) -> bool {
        self.node_for_change_id(id).is_some()
    }

    /// The change identified by `id`, or `None` if this document does not
    /// contain it.
    ///
    /// Outside audit mode this can be slow.
    pub fn get_change_by_id(&self, id: &ChangeId) -> Result<Option<Change>, AutomergeError> {
        let Some(node) = self.node_for_change_id(id) else {
            return Ok(None);
        };
        let nodes = self.change_graph.nodes_back_to_retained([node]);
        let members = crate::change_graph::Members::new(&nodes);
        let changes = Bundle::storage_for_nodes(&self.ops, &self.change_graph, &members)?
            .to_changes()
            .map_err(|e| AutomergeError::DecodeBundle(Box::new(e)))?;
        Ok(changes
            .into_iter()
            .find(|c| c.actor_id() == id.actor() && c.seq() == id.seq()))
    }

    /// The changes not covered by `have_deps`. Ids this document does not
    /// know are ignored.
    pub fn get_changes(&self, have_deps: &[ChangeId]) -> Result<Vec<Change>, AutomergeError> {
        let clock = self.seq_clock_for_ids_lossy(have_deps);
        ChangeCollector::exclude_seq_clock(&self.ops, &self.change_graph, clock)
    }

    /// [`Self::get_changes`] as metadata.
    pub fn get_changes_meta(
        &self,
        have_deps: &[ChangeId],
    ) -> Result<Vec<ChangeMetadata<'_>>, AutomergeError> {
        let have_deps = self.change_ids_to_hashes_lossy(have_deps);
        ChangeCollector::exclude_hashes_meta(
            &self.ops,
            &self.change_graph,
            &self.authors,
            &have_deps,
        )
    }

    fn seq_clock_for_ids_lossy(&self, ids: &[ChangeId]) -> crate::clock::SeqClock {
        let nodes = ids
            .iter()
            .filter_map(|id| self.node_for_change_id(id))
            .collect();
        self.change_graph.calculate_clock(nodes)
    }

    pub fn get_change_meta_by_hash(
        &self,
        hash: &ChangeHash,
    ) -> Result<Option<ChangeMetadata<'_>>, AutomergeError> {
        match ChangeCollector::meta_for_hashes(
            &self.ops,
            &self.change_graph,
            &self.authors,
            [*hash],
        ) {
            Ok(mut metas) => Ok(metas.pop()),
            Err(AutomergeError::AuditModeRequired) => Err(AutomergeError::AuditModeRequired),
            Err(_) => Ok(None),
        }
    }

    /// [`Self::get_changes_added`] as change chunks.
    ///
    /// May return [`AutomergeError::AuditModeRequired`] outside audit mode.
    pub fn get_changes_added_legacy<H2: HashRetention>(
        &self,
        other: &Automerge<H2>,
    ) -> Result<Vec<Change>, AutomergeError> {
        match self.get_changes_added(other)? {
            Some(bundle) => bundle.to_changes(),
            None => Ok(vec![]),
        }
    }

    /// Get changes in `other` that are not in `self`
    pub fn get_changes_added<H2: HashRetention>(
        &self,
        other: &Automerge<H2>,
    ) -> Result<Option<Bundle>, AutomergeError> {
        // per-actor sequences are linear, so a change in `other` is new
        // exactly when our seq clock does not cover it
        let ours = self.change_graph.current_seq_clock();
        let theirs = other.change_graph.current_seq_clock();
        let mut exclude = other.change_graph.empty_clock();
        for (actor_idx, seq) in ours.iter() {
            let Some(seq) = seq else { continue };
            if let Some(other_idx) = other.ops.lookup_actor(&self.ops.actors[actor_idx]) {
                let Some(cap) = theirs.get_for_actor(&other_idx) else {
                    continue;
                };
                let shared = seq.get().min(cap.get());
                // a divergent hash at a shared seq means the actor equivocated
                let hash_at = |seq: u64| -> Option<(ChangeHash, ChangeHash)> {
                    let id = self.change_id_at(actor_idx, seq);
                    Some((
                        self.change_graph
                            .get_hash_for_change_id(&id, &self.ops.actors)
                            .ok()?,
                        other
                            .change_graph
                            .get_hash_for_change_id(&id, &other.ops.actors)
                            .ok()?,
                    ))
                };
                if let Some((a, b)) = hash_at(shared as u64) {
                    if a != b {
                        let seq = (1..=shared as u64)
                            .find(|s| matches!(hash_at(*s), Some((a, b)) if a != b))
                            .unwrap_or(shared as u64);
                        return Err(AutomergeError::DuplicateSeqNumber(
                            seq,
                            self.ops.actors[actor_idx].clone(),
                        ));
                    }
                }
                exclude.include(other_idx, Some(shared));
            }
        }
        let nodes = other.change_graph.nodes_above(exclude);
        let boundary = self.get_head_hashes();
        other.bundle_nodes(nodes, &boundary)
    }

    /// Get the hash of the change that contains the given `opid`.
    ///
    /// Returns `Ok(None)` if the `opid`:
    /// - is the root object id
    /// - does not exist in this document
    ///
    /// Returns [`AutomergeError::AuditModeRequired`] if the change is in
    /// this document but its hash is not retained.
    pub fn hash_for_opid(&self, exid: &ExId) -> Result<Option<ChangeHash>, AutomergeError> {
        match exid {
            ExId::Root => Ok(None),
            ExId::Id(..) => {
                let Ok(opid) = self.exid_to_opid(exid) else {
                    return Ok(None);
                };
                let Some(id) = self.change_graph.opid_to_change_id(opid, &self.ops.actors) else {
                    return Ok(None);
                };
                Ok(Some(
                    self.change_graph
                        .get_hash_for_change_id(&id, &self.ops.actors)?,
                ))
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
            let fast = self
                .ops()
                .calculate_marks_fast(&obj.id, read.historical_slow());
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

    pub fn hydrate(&self, heads: Option<&[ChangeId]>) -> Result<hydrate::Value, AutomergeError> {
        let read = self.read_at(heads)?;
        Ok(self.hydrate_map(&ObjId::root(), &read))
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
            .unwrap_or_default()
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
            .unwrap_or_default()
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
            let fast = self
                .ops()
                .get_marks_fast(&obj.id, index, read.historical_slow());
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

    /// Whether the document contains `hash`.
    pub fn has_change(&self, hash: &ChangeHash) -> Result<bool, AutomergeError> {
        Ok(self.change_graph.has_change(hash)?)
    }

    /// Every change hash reachable from `have`, in topological order;
    /// with `have` empty, the whole document.
    pub fn change_hashes(
        &self,
        have: &[ChangeHash],
    ) -> Result<std::borrow::Cow<'_, [ChangeHash]>, AutomergeError> {
        Ok(self.change_graph.get_hashes(have)?)
    }

    /// The number of changes in the document.
    pub fn num_changes(&self) -> usize {
        self.change_graph.len()
    }

    /// The hashes `hash` directly depends on.
    pub fn change_deps(&self, hash: &ChangeHash) -> Result<Vec<ChangeHash>, AutomergeError> {
        self.change_graph
            .deps(hash)
            .collect::<Result<Vec<_>, _>>()
            .map_err(Into::into)
    }

    /// The changes named by `hashes`, in topological order.
    pub fn changes_by_hash(&self, hashes: &[ChangeHash]) -> Result<Vec<Change>, AutomergeError> {
        self.get_changes_by_hashes(hashes.iter().copied())
    }

    /// Remove from `of` every hash which is an ancestor of `heads`
    /// (`heads` this document does not have are ignored).
    pub fn remove_ancestors(
        &self,
        heads: &[ChangeHash],
        of: &mut BTreeSet<ChangeHash>,
    ) -> Result<(), AutomergeError> {
        self.filter_changes(heads, of)
    }

    /// The hashes reachable from `from` which this document is missing.
    ///
    /// Unlike [`ReadDoc::get_missing_deps`] the starting points need not
    /// be present here — they are typically another peer's heads.
    pub fn missing_deps(&self, from: &[ChangeHash]) -> Result<Vec<ChangeHash>, AutomergeError> {
        self.missing_deps_from(from.iter().copied())
    }

    /// [`Self::missing_deps`], also starting from every queued change.
    pub fn missing_deps_with_queued(
        &self,
        from: &[ChangeHash],
    ) -> Result<Vec<ChangeHash>, AutomergeError> {
        let queued = self.queue.iter().map(|change| change.hash());
        self.missing_deps_from(queued.chain(from.iter().copied()))
    }

    /// The first hash on each path back from `start` which is neither applied nor queued,
    /// traversing through the dependencies of queued changes on the way.
    pub(crate) fn missing_deps_from(
        &self,
        start: impl Iterator<Item = ChangeHash>,
    ) -> Result<Vec<ChangeHash>, AutomergeError> {
        let queued_changes = self
            .queue
            .iter()
            .map(|change| (change.hash(), change))
            .collect::<HashMap<_, _>>();

        let mut missing = HashSet::new();
        let mut seen = HashSet::new();
        let mut stack = start.collect::<Vec<_>>();

        while let Some(hash) = stack.pop() {
            if self.has_change(&hash)? || !seen.insert(hash) {
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
        Ok(missing)
    }

    pub fn text_encoding(&self) -> TextEncoding {
        self.ops.text_encoding
    }
}

impl Automerge<Retained> {
    /// Create a new document with a random actor id.
    pub fn new() -> Self {
        Self::empty(TextEncoding::platform_default())
    }

    pub fn new_with_encoding(encoding: TextEncoding) -> Self {
        Self::empty(encoding)
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
    /// * `options` - The options to use when loading
    #[tracing::instrument(skip(data), err)]
    pub fn load_with_options(data: &[u8], options: LoadOptions) -> Result<Self, AutomergeError> {
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
        Self::load_with_options_and_mark_validation(
            data,
            Default::default(),
            load::MarkOrderValidation::AllowInvalid,
        )?
        .hydrate(None)
    }

    /// Convert to a document that keeps every change hash.
    ///
    /// Costs about as much as a full load. Fails with [`AutomergeError::InvalidHash`] if a hash the
    /// document already held does not match its change; the document then
    /// comes back unchanged, inside the error.
    #[doc(hidden)]
    pub fn enable_audit_mode(self) -> Result<Automerge<Full>, EnableAuditModeError<Self>> {
        match self.verified_hashes() {
            Ok(hashes) => Ok(self.into_full_with(hashes)),
            Err(error) => Err(EnableAuditModeError {
                error,
                doc: Box::new(self),
            }),
        }
    }

    pub(crate) fn into_full_with(self, hashes: Vec<ChangeHash>) -> Automerge<Full> {
        self.map_graph(|graph| graph.into_full(hashes))
    }

    pub(crate) fn verified_hashes(&self) -> Result<Vec<ChangeHash>, AutomergeError> {
        let hashes = self.compute_hashes()?;
        self.change_graph
            .verify_hashes(&hashes)
            .map_err(AutomergeError::InvalidHash)?;
        Ok(hashes)
    }
}

impl Automerge<Full> {
    /// Convert to a document that keeps only the hashes it needs.
    #[doc(hidden)]
    pub fn disable_audit_mode(mut self) -> Automerge<Retained> {
        // before interior hashes are freed
        self.pin_resolved_boundaries();
        self.map_graph(ChangeGraph::into_retained)
    }

    /// Reverse depth-first from `other`'s heads, which is not always
    /// topological; the order the audited API returns.
    pub(crate) fn changes_added_by_hash(&self, other: &Self) -> Vec<Change> {
        let mut stack = other.get_head_hashes();
        let mut seen = HashSet::new();
        let mut added = Vec::new();
        while let Some(hash) = stack.pop() {
            if !seen.contains(&hash) && !self.change_graph.has_change(&hash).unwrap_or(false) {
                seen.insert(hash);
                added.push(hash);
                stack.extend(other.change_graph.deps(&hash).flatten());
            }
        }
        added.reverse();
        other
            .get_changes_by_hashes(added)
            .expect("every hash came from other's graph")
    }

    pub(crate) fn hash_of_id(&self, id: &ChangeId) -> Option<ChangeHash> {
        let node = self.node_for_change_id(id)?;
        self.change_graph.hash_for_node(node)
    }
}

impl<H: HashRetention> Automerge<H> {
    pub(crate) fn object_type<O: AsRef<ExId>>(&self, obj: O) -> Result<ObjType, AutomergeError> {
        let obj = obj.as_ref();
        let opid = self.exid_to_opid(obj)?;
        let typ = self.ops.object_type(&ObjId(opid));
        typ.ok_or_else(|| AutomergeError::InvalidObjId(obj.to_string()))
    }

    pub(crate) fn get_missing_deps(
        &self,
        heads: &[ChangeId],
    ) -> Result<Vec<ChangeHash>, AutomergeError> {
        self.missing_deps_with_queued(&self.change_ids_to_hashes(heads)?)
    }

    pub(crate) fn get_change_by_hash(
        &self,
        hash: &ChangeHash,
    ) -> Result<Option<Change>, AutomergeError> {
        match ChangeCollector::for_hashes(&self.ops, &self.change_graph, [*hash]) {
            Ok(mut changes) => Ok(changes.pop()),
            Err(AutomergeError::AuditModeRequired) => Err(AutomergeError::AuditModeRequired),
            Err(_) => Ok(None),
        }
    }

    pub(crate) fn stats(&self) -> crate::read::Stats {
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
}

impl<H: HashRetention> ScopedRead for Automerge<H> {
    type Retention = H;

    fn doc(&self) -> &Automerge<H> {
        self
    }

    #[inline]
    fn get_scope(&self, heads: Option<&[ChangeId]>) -> Result<ReadAt<'_>, AutomergeError> {
        match heads {
            None => Ok(self.read_current()),
            Some(_) => self.read_at(heads),
        }
    }
}

impl Default for Automerge<Retained> {
    fn default() -> Self {
        Self::new()
    }
}

/// The format a save writes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum SaveFormat {
    /// A bundle, which keeps the document's fragment structure. Readers
    /// which predate it, including those that wrote BundleV0, cannot load
    /// it.
    #[default]
    Default,
    /// A document chunk, which every release can load. Does not keep the
    /// fragment structure.
    Legacy,
}

/// Which level-0 hashes a bundle names.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum NameHashes {
    /// All but tiny changes', when the load can rehash those cheaply.
    UntagTiny,
    /// Every one, so the load rehashes nothing.
    All,
}

/// A failed `enable_audit_mode`, with the document unchanged.
#[derive(Debug)]
pub struct EnableAuditModeError<D> {
    pub error: AutomergeError,
    pub doc: Box<D>,
}

impl<D> std::fmt::Display for EnableAuditModeError<D> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Display::fmt(&self.error, f)
    }
}

impl<D: std::fmt::Debug> std::error::Error for EnableAuditModeError<D> {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&self.error)
    }
}

/// Options to pass to [`Automerge::save_with_options()`] and [`crate::AutoCommit::save_with_options()`]
#[derive(Debug)]
pub struct SaveOptions {
    /// Whether to apply DEFLATE compression to the RLE encoded columns in the document
    pub deflate: bool,
    /// Whether to save changes which we do not have the dependencies for
    pub retain_orphans: bool,
    /// See [`SaveFormat`].
    pub format: SaveFormat,
}

impl SaveOptions {
    fn compress(&self) -> CompressConfig {
        if self.deflate {
            CompressConfig::Threshold(change::DEFLATE_MIN_SIZE)
        } else {
            CompressConfig::None
        }
    }

    fn encode(&self, bundle: &Bundle) -> Vec<u8> {
        if self.deflate {
            bundle.bytes()
        } else {
            bundle.bytes_uncompressed()
        }
    }
}

impl std::default::Default for SaveOptions {
    fn default() -> Self {
        Self {
            format: SaveFormat::default(),
            deflate: true,
            retain_orphans: true,
        }
    }
}

#[derive(Debug)]
pub(crate) struct Isolation {
    actor_index: usize,
    seq: u64,
    clock: VisibleClock,
}

#[cfg(test)]
mod dirty_diff_tests {
    use std::ops::Range;

    use crate::{
        exid::ExId,
        hash_retention::{Full, HashRetention},
        marks::{ExpandMark, Mark},
        op_set2::types::Action,
        patches::effect::assert_patches_have_same_effect,
        tx::{Transactable, Transaction},
        types::ObjId,
        ActorId, ChangeId, ObjType, ScalarValue, ROOT,
    };

    use crate::autocommit::AutoCommit;

    use crate::automerge::Automerge;

    #[derive(Clone, Copy, Debug, PartialEq)]
    enum Via {
        Local,
        Apply,
        Batch,
        Load,
    }

    fn audit_doc() -> Automerge<Full> {
        Automerge::new().enable_audit_mode().unwrap()
    }

    fn dirty_ranges<H: HashRetention>(doc: &Automerge<H>) -> Vec<Range<usize>> {
        doc.ops().dirty_runs().collect()
    }

    fn ranges_contain(ranges: &[Range<usize>], needle: Range<usize>) -> bool {
        ranges
            .iter()
            .any(|range| range.start <= needle.start && needle.end <= range.end)
    }

    fn assert_dirty_diff_matches_full<H: HashRetention>(
        doc: &Automerge<H>,
        before: &[ChangeId],
        after: &[ChangeId],
    ) {
        let full = doc.diff(before, after).unwrap();
        assert_eq!(after, doc.get_heads().as_slice());
        let dirty = doc.dirty_diff_patches(&doc.viewpoint_at(before.to_vec()));
        assert_patches_have_same_effect(
            doc,
            before,
            after,
            "dirty diff",
            &dirty,
            "full diff",
            &full,
        );
    }

    fn assert_incremental_effect_matches_full<H: HashRetention>(
        doc: &mut Automerge<H>,
        before: &[ChangeId],
        after: &[ChangeId],
    ) {
        let full = doc.diff(before, after).unwrap();
        let incremental = doc.diff_incremental();
        assert_patches_have_same_effect(
            doc,
            before,
            after,
            "incremental diff",
            &incremental,
            "full diff",
            &full,
        );
    }

    fn assert_autocommit_incremental_effect_matches_full<H: HashRetention>(
        doc: &mut AutoCommit<H>,
        before: &[ChangeId],
        after: &[ChangeId],
    ) {
        let full = doc.document().diff(before, after).unwrap();
        let incremental = doc.diff_incremental();
        assert_patches_have_same_effect(
            doc.document(),
            before,
            after,
            "incremental diff",
            &incremental,
            "full diff",
            &full,
        );
    }

    fn assert_local_edit_matches_full<T>(
        setup: impl FnOnce(&mut Transaction<'_, Full>) -> T,
        edit: impl FnOnce(&mut Transaction<'_, Full>, &T),
    ) -> (Automerge<Full>, T) {
        let mut doc = audit_doc();
        let mut tx = doc.transaction();
        let obj = setup(&mut tx);
        tx.commit();

        doc.ops_mut().clear_dirty();
        let before = doc.get_heads();
        let mut tx = doc.transaction();
        edit(&mut tx, &obj);
        tx.commit();
        let after = doc.get_heads();

        assert_dirty_diff_matches_full(&doc, &before, &after);
        (doc, obj)
    }

    fn assert_remote_edit_matches_full<H: HashRetention, T>(
        mut doc1: Automerge<H>,
        remote_actor: Option<ActorId>,
        setup: impl FnOnce(&mut Transaction<'_, H>) -> T,
        local: impl FnOnce(&mut Transaction<'_, H>, &T),
        remote: impl FnOnce(&mut Transaction<'_, H>, &T),
        via: Via,
    ) -> (Automerge<H>, T) {
        let mut tx = doc1.transaction();
        let obj = setup(&mut tx);
        tx.commit();
        let mut doc2 = doc1.fork();
        if let Some(actor) = remote_actor {
            doc2.set_actor(actor);
        }

        let mut tx = doc1.transaction();
        local(&mut tx, &obj);
        tx.commit();
        let mut tx = doc2.transaction();
        remote(&mut tx, &obj);
        tx.commit();

        doc1.ops_mut().clear_dirty();
        let before = doc1.get_heads();
        match via {
            Via::Local => unreachable!("a local edit has nothing to deliver"),
            Via::Apply => doc1
                .apply_changes(doc2.get_changes(&before).unwrap())
                .unwrap(),
            Via::Batch => doc1
                .apply_changes_batch(doc2.get_changes(&before).unwrap())
                .unwrap(),
            // what receiving a v2 sync message does
            Via::Load => {
                doc1.load_incremental(&doc2.save()).unwrap();
            }
        }
        let after = doc1.get_heads();

        assert_dirty_diff_matches_full(&doc1, &before, &after);
        (doc1, obj)
    }

    fn put_list<H: HashRetention>(tx: &mut Transaction<'_, H>, items: &[&str]) -> ExId {
        let list = tx.put_object(ROOT, "list", ObjType::List).unwrap();
        for (index, item) in items.iter().enumerate() {
            tx.insert(&list, index, *item).unwrap();
        }
        list
    }

    fn put_text<H: HashRetention>(
        tx: &mut Transaction<'_, H>,
        content: &str,
        marks: Vec<Mark>,
    ) -> ExId {
        let text = tx.put_object(ROOT, "text", ObjType::Text).unwrap();
        tx.splice_text(&text, 0, 0, content).unwrap();
        for mark in marks {
            tx.mark(&text, mark, ExpandMark::Both).unwrap();
        }
        text
    }

    fn bold(start: usize, end: usize) -> Mark {
        Mark::new("bold".to_string(), true, start, end)
    }

    fn put_text_with_one_mark<H: HashRetention>(tx: &mut Transaction<'_, H>) -> ExId {
        put_text(tx, "abcdef", vec![bold(2, 4)])
    }

    fn put_text_with_nested_marks<H: HashRetention>(tx: &mut Transaction<'_, H>) -> ExId {
        put_text(
            tx,
            "abcdefghij",
            vec![
                bold(2, 8),
                Mark::new("italic".to_string(), true, 4, 6),
                Mark::new("color".to_string(), "red", 5, 9),
            ],
        )
    }

    fn assert_list_dirt_on_register_boundaries<H: HashRetention>(
        doc: &Automerge<H>,
        list: &ExId,
    ) -> Vec<Range<usize>> {
        let ranges = dirty_ranges(doc);
        let list_obj = doc.exid_to_obj(list).unwrap().id;
        let list_range = doc.ops().scope_to_obj(&list_obj);
        assert!(ranges.iter().all(|range| doc
            .ops()
            .list_range_is_on_register_boundaries(range, list_range.clone())));
        ranges
    }

    #[test]
    fn dirty_diff_matches_full_diff_for_map_put() {
        assert_local_edit_matches_full(|_| (), |tx, _| tx.put(ROOT, "key", 1).unwrap());
    }

    #[test]
    fn automerge_diff_incremental_clears_dirty_and_advances_cursor() {
        let mut doc = audit_doc();
        let mut tx = doc.transaction();
        tx.put(ROOT, "key", 1).unwrap();
        tx.commit();
        let first_heads = doc.get_heads();

        assert_incremental_effect_matches_full(&mut doc, &[], &first_heads);
        assert!(doc.ops().dirty_runs().next().is_none());

        let mut tx = doc.transaction();
        tx.put(ROOT, "key", 2).unwrap();
        tx.commit();
        let second_heads = doc.get_heads();

        assert_incremental_effect_matches_full(&mut doc, &first_heads, &second_heads);
        assert!(doc.ops().dirty_runs().next().is_none());
    }

    #[test]
    fn dirty_diff_expands_partial_register_marks() {
        let mut doc = audit_doc();
        let mut tx = doc.transaction();
        let list = put_list(&mut tx, &["a"]);
        tx.commit();
        let before = doc.get_heads();

        let mut tx = doc.transaction();
        tx.put(&list, 0, "A").unwrap();
        tx.commit();
        let after = doc.get_heads();

        // one dirty row inside a multi-row register must widen to the register
        doc.ops_mut().clear_dirty();
        doc.ops_mut().mark_dirty(1);
        let patches = doc.dirty_diff_patches(&doc.viewpoint_at(before.clone()));
        doc.clear_dirty();
        let expected = doc.diff(&before, &after).unwrap();
        assert_eq!(patches, expected);
        assert!(doc.ops().dirty_runs().next().is_none());
    }

    #[test]
    fn automerge_diff_incremental_empty_doc_and_repeated_calls_are_empty() {
        let mut doc = audit_doc();

        assert!(doc.diff_incremental().is_empty());
        assert!(doc.ops().dirty_runs().next().is_none());
        assert!(doc.diff_incremental().is_empty());

        let mut tx = doc.transaction();
        tx.put(ROOT, "key", 1).unwrap();
        tx.commit();
        assert!(!doc.diff_incremental().is_empty());
        assert!(doc.ops().dirty_runs().next().is_none());
        assert!(doc.diff_incremental().is_empty());
    }

    #[test]
    fn automerge_diff_incremental_materializes_loaded_document() {
        let mut source = audit_doc();
        let mut tx = source.transaction();
        let list = tx.put_object(ROOT, "todos", ObjType::List).unwrap();
        tx.insert(&list, 0, "a").unwrap();
        tx.insert(&list, 1, "b").unwrap();
        tx.commit();
        let data = source.save();

        let mut doc = Automerge::load(&data).unwrap();
        let heads = doc.get_heads();

        assert_incremental_effect_matches_full(&mut doc, &[], &heads);
        assert!(doc.ops().dirty_runs().next().is_none());
        assert!(doc.diff_incremental().is_empty());
    }

    #[test]
    fn automerge_diff_incremental_after_load_incremental_uses_saved_cursor() {
        let mut source = audit_doc();
        let mut tx = source.transaction();
        tx.put(ROOT, "base", 1).unwrap();
        tx.commit();
        let base_heads = source.get_heads();
        let base_data = source.save();

        let mut doc = audit_doc();
        doc.load_incremental(&base_data).unwrap();
        assert_incremental_effect_matches_full(&mut doc, &[], &base_heads);

        let mut tx = source.transaction();
        tx.put(ROOT, "later", 2).unwrap();
        tx.commit();
        let data = source.save_after(&base_heads).unwrap();

        let before = doc.get_heads();
        doc.load_incremental(&data).unwrap();
        let after = doc.get_heads();
        assert_incremental_effect_matches_full(&mut doc, &before, &after);
        assert!(doc.ops().dirty_runs().next().is_none());
    }

    #[test]
    fn automerge_diff_incremental_after_apply_merge_and_sync_receive() {
        let mut source = audit_doc();
        let mut tx = source.transaction();
        tx.put(ROOT, "key", 1).unwrap();
        tx.commit();

        let mut doc = audit_doc();
        let before = doc.get_heads();
        doc.apply_changes(source.get_changes(&[]).unwrap()).unwrap();
        let after = doc.get_heads();
        assert_incremental_effect_matches_full(&mut doc, &before, &after);
        assert!(doc.ops().dirty_runs().next().is_none());

        let mut source = audit_doc();
        let mut tx = source.transaction();
        tx.put(ROOT, "merged", 2).unwrap();
        tx.commit();

        let mut doc = audit_doc();
        let before = doc.get_heads();
        doc.merge(&mut source).unwrap();
        let after = doc.get_heads();
        assert_incremental_effect_matches_full(&mut doc, &before, &after);
        assert!(doc.ops().dirty_runs().next().is_none());

        let mut source = audit_doc();
        let mut tx = source.transaction();
        tx.put(ROOT, "synced", 3).unwrap();
        tx.commit();
        // what receiving a v2 sync message does
        let mut doc = audit_doc();
        let before = doc.get_heads();
        doc.load_incremental(&source.save()).unwrap();
        let after = doc.get_heads();
        assert_incremental_effect_matches_full(&mut doc, &before, &after);
        assert!(doc.ops().dirty_runs().next().is_none());
    }

    #[test]
    fn automerge_diff_incremental_fork_inherits_cursor() {
        let mut doc = audit_doc();
        let mut tx = doc.transaction();
        tx.put(ROOT, "base", 1).unwrap();
        tx.commit();
        let base_heads = doc.get_heads();
        doc.diff_incremental();

        let mut fork = doc.fork();
        let mut tx = fork.transaction();
        tx.put(ROOT, "fork", 2).unwrap();
        tx.commit();
        let fork_heads = fork.get_heads();

        assert_incremental_effect_matches_full(&mut fork, &base_heads, &fork_heads);
        assert!(fork.ops().dirty_runs().next().is_none());
    }

    #[test]
    fn autocommit_diff_incremental_repeated_empty_and_rollback_lifecycle() {
        let mut doc = AutoCommit::new().enable_audit_mode().unwrap();

        assert!(doc.diff_incremental().is_empty());

        doc.put(ROOT, "key", 1).unwrap();
        assert!(!doc.diff_incremental().is_empty());
        assert!(doc.document().ops().dirty_runs().next().is_none());
        assert!(doc.diff_incremental().is_empty());

        let heads = doc.get_heads();
        doc.reset_diff_cursor();
        assert_autocommit_incremental_effect_matches_full(&mut doc, &[], &heads);
        assert!(doc.document().ops().dirty_runs().next().is_none());
        assert!(doc.diff_incremental().is_empty());

        doc.put(ROOT, "key", 2).unwrap();
        assert_eq!(doc.rollback(), 1);
        assert!(doc.diff_incremental().is_empty());
        assert!(doc.document().ops().dirty_runs().next().is_none());

        doc.put(ROOT, "key", 3).unwrap();
        assert!(!doc.diff_incremental().is_empty());
        assert!(doc.document().ops().dirty_runs().next().is_none());
    }

    #[test]
    fn dirty_diff_matches_full_diff_for_map_update() {
        assert_local_edit_matches_full(
            |tx| tx.put(ROOT, "key", 1).unwrap(),
            |tx, _| tx.put(ROOT, "key", 2).unwrap(),
        );
    }

    #[test]
    fn adjacent_map_updates_dirty_contiguous_key_ranges() {
        let (doc, _) = assert_local_edit_matches_full(
            |tx| {
                tx.put(ROOT, "a", 1).unwrap();
                tx.put(ROOT, "b", 2).unwrap();
                tx.put(ROOT, "c", 3).unwrap();
            },
            |tx, _| {
                tx.put(ROOT, "a", 10).unwrap();
                tx.put(ROOT, "b", 20).unwrap();
            },
        );

        let a = doc.ops().prop_range(&ObjId::root(), "a");
        let b = doc.ops().prop_range(&ObjId::root(), "b");
        assert_eq!(dirty_ranges(&doc), vec![a.start..b.end]);
    }

    #[test]
    fn remote_map_update_and_adjacent_insert_dirty_contiguous_key_ranges() {
        let (doc1, _) = assert_remote_edit_matches_full(
            audit_doc(),
            None,
            |tx| {
                tx.put(ROOT, "a", 1).unwrap();
                tx.put(ROOT, "c", 3).unwrap();
            },
            |_, _| (),
            |tx, _| {
                tx.put(ROOT, "a", 10).unwrap();
                tx.put(ROOT, "b", 2).unwrap();
            },
            Via::Apply,
        );

        let a = doc1.ops().prop_range(&ObjId::root(), "a");
        let b = doc1.ops().prop_range(&ObjId::root(), "b");
        assert_eq!(a.end, b.start);
        assert_eq!(dirty_ranges(&doc1), vec![a.start..b.end]);
    }

    #[test]
    fn dirty_diff_matches_full_diff_for_map_delete() {
        assert_local_edit_matches_full(
            |tx| tx.put(ROOT, "key", 1).unwrap(),
            |tx, _| tx.delete(ROOT, "key").unwrap(),
        );
    }

    #[test]
    fn dirty_diff_matches_full_diff_for_map_increment() {
        assert_local_edit_matches_full(
            |tx| tx.put(ROOT, "counter", ScalarValue::counter(1)).unwrap(),
            |tx, _| tx.increment(ROOT, "counter", 2).unwrap(),
        );
    }

    #[test]
    fn dirty_diff_matches_full_diff_for_list_insert() {
        assert_local_edit_matches_full(
            |tx| put_list(tx, &[]),
            |tx, list| tx.insert(list, 0, "a").unwrap(),
        );
    }

    #[test]
    fn dirty_diff_matches_full_diff_for_list_update() {
        assert_local_edit_matches_full(
            |tx| put_list(tx, &["a", "b", "c"]),
            |tx, list| tx.put(list, 1, "B").unwrap(),
        );
    }

    #[test]
    fn adjacent_list_updates_dirty_contiguous_register_ranges() {
        let (doc, list) = assert_local_edit_matches_full(
            |tx| put_list(tx, &["a", "b", "c"]),
            |tx, list| {
                tx.put(list, 1, "B").unwrap();
                tx.put(list, 2, "C").unwrap();
            },
        );
        assert_eq!(
            assert_list_dirt_on_register_boundaries(&doc, &list),
            vec![2..6]
        );
    }

    #[test]
    fn remote_adjacent_list_updates_dirty_contiguous_register_ranges() {
        let (doc1, list) = assert_remote_edit_matches_full(
            audit_doc(),
            None,
            |tx| put_list(tx, &["a", "b", "c"]),
            |_, _| (),
            |tx, list| {
                tx.put(list, 1, "B").unwrap();
                tx.put(list, 2, "C").unwrap();
            },
            Via::Apply,
        );
        assert_eq!(
            assert_list_dirt_on_register_boundaries(&doc1, &list),
            vec![2..6]
        );
    }

    #[test]
    fn batch_remote_adjacent_list_update_and_conflict_dirty_register_ranges() {
        let (doc1, list) = assert_remote_edit_matches_full(
            Automerge::new().with_actor(ActorId::from([1])),
            Some(ActorId::from([2])),
            |tx| put_list(tx, &["a", "b", "c"]),
            |tx, list| tx.put(list, 1, "local-b").unwrap(),
            |tx, list| {
                tx.put(list, 1, "remote-b").unwrap();
                tx.put(list, 2, "remote-c").unwrap();
            },
            Via::Batch,
        );
        assert_eq!(
            assert_list_dirt_on_register_boundaries(&doc1, &list).len(),
            1
        );
    }

    #[test]
    fn batch_remote_list_update_plus_nearby_insert_dirty_register_ranges() {
        let (doc1, list) = assert_remote_edit_matches_full(
            audit_doc(),
            None,
            |tx| put_list(tx, &["a", "b", "c"]),
            |_, _| (),
            |tx, list| {
                tx.put(list, 1, "B").unwrap();
                tx.insert(list, 2, "X").unwrap();
            },
            Via::Batch,
        );
        assert_list_dirt_on_register_boundaries(&doc1, &list);
    }

    #[test]
    fn batch_remote_insert_before_updated_list_element_matches_full_diff() {
        let (doc1, list) = assert_remote_edit_matches_full(
            audit_doc(),
            None,
            |tx| put_list(tx, &["a", "b", "c", "d"]),
            |_, _| (),
            |tx, list| {
                tx.insert(list, 1, "X").unwrap();
                tx.put(list, 2, "B").unwrap();
                tx.put(list, 3, "C").unwrap();
            },
            Via::Batch,
        );
        assert_list_dirt_on_register_boundaries(&doc1, &list);
    }

    #[test]
    fn batch_remote_insert_before_conflicting_list_element_matches_full_diff() {
        let (doc1, list) = assert_remote_edit_matches_full(
            Automerge::new().with_actor(ActorId::from([1])),
            Some(ActorId::from([2])),
            |tx| put_list(tx, &["a", "b", "c"]),
            |tx, list| tx.put(list, 1, "local-b").unwrap(),
            |tx, list| {
                tx.insert(list, 1, "X").unwrap();
                tx.put(list, 2, "remote-b").unwrap();
            },
            Via::Batch,
        );
        assert_list_dirt_on_register_boundaries(&doc1, &list);
    }

    #[test]
    fn batch_remote_dependent_insert_and_update_list_changes_match_full_diff() {
        let mut doc1 = audit_doc();
        let mut tx = doc1.transaction();
        let list = put_list(&mut tx, &["a", "b", "c", "d"]);
        tx.commit();
        let mut doc2 = doc1.fork();

        let mut tx = doc2.transaction();
        tx.insert(&list, 1, "X").unwrap();
        tx.commit();
        let mut tx = doc2.transaction();
        tx.put(&list, 3, "C").unwrap();
        tx.insert(&list, 4, "Y").unwrap();
        tx.commit();
        let changes = doc2.get_changes(&doc1.get_heads()).unwrap();

        doc1.ops_mut().clear_dirty();
        let before = doc1.get_heads();
        doc1.apply_changes_batch(changes).unwrap();
        let after = doc1.get_heads();

        assert_list_dirt_on_register_boundaries(&doc1, &list);
        assert_dirty_diff_matches_full(&doc1, &before, &after);
    }

    fn next_dirty_diff_test_rand(seed: &mut u64) -> usize {
        *seed = seed.wrapping_mul(6364136223846793005).wrapping_add(1);
        (*seed >> 32) as usize
    }

    fn assert_batch_random_list_changes_match_full_diff(mut seed: u64, split_changes: bool) {
        let mut doc1 = audit_doc();
        let mut tx = doc1.transaction();
        let list = tx.put_object(ROOT, "list", ObjType::List).unwrap();
        for index in 0..8 {
            tx.insert(&list, index, format!("v{index}")).unwrap();
        }
        tx.commit();
        let mut doc2 = doc1.fork();
        let mut model_len = 8;
        let mut value_counter = 0;

        let change_count = if split_changes { 3 } else { 1 };
        let ops_per_change = if split_changes { 5 } else { 15 };
        for _ in 0..change_count {
            let mut tx = doc2.transaction();
            for _ in 0..ops_per_change {
                value_counter += 1;
                match next_dirty_diff_test_rand(&mut seed) % 4 {
                    // shifts the ranges of later updates in the same batch
                    0 if model_len > 0 => {
                        let index = next_dirty_diff_test_rand(&mut seed) % model_len;
                        tx.insert(&list, index, format!("i{value_counter}"))
                            .unwrap();
                        model_len += 1;
                    }
                    // identity resolves only after the batch's splices
                    1 if model_len > 0 => {
                        let index = next_dirty_diff_test_rand(&mut seed) % model_len;
                        tx.put(&list, index, format!("u{value_counter}")).unwrap();
                    }
                    2 => {
                        let index = next_dirty_diff_test_rand(&mut seed) % (model_len + 1);
                        tx.insert(&list, index, format!("j{value_counter}"))
                            .unwrap();
                        model_len += 1;
                    }
                    // moves later dirty registers left
                    _ if model_len > 1 => {
                        let index = next_dirty_diff_test_rand(&mut seed) % model_len;
                        tx.delete(&list, index).unwrap();
                        model_len -= 1;
                    }
                    _ => {
                        tx.insert(&list, 0, format!("k{value_counter}")).unwrap();
                        model_len += 1;
                    }
                }
            }
            tx.commit();
        }
        let changes = doc2.get_changes(&doc1.get_heads()).unwrap();

        doc1.ops_mut().clear_dirty();
        let before = doc1.get_heads();
        doc1.apply_changes_batch(changes).unwrap();
        let after = doc1.get_heads();

        assert!(!assert_list_dirt_on_register_boundaries(&doc1, &list).is_empty());
        assert_dirty_diff_matches_full(&doc1, &before, &after);
    }

    #[test]
    fn batch_randomized_list_splices_and_existing_updates_match_full_diff() {
        for seed in [1, 2, 3, 5, 8, 13, 21, 34] {
            assert_batch_random_list_changes_match_full_diff(seed, false);
            assert_batch_random_list_changes_match_full_diff(seed, true);
        }
    }

    #[test]
    fn dirty_diff_matches_full_diff_for_list_delete() {
        assert_local_edit_matches_full(
            |tx| put_list(tx, &["a", "b", "c"]),
            |tx, list| tx.delete(list, 1).unwrap(),
        );
    }

    #[test]
    fn dirty_diff_matches_full_diff_for_object_creation_with_child_mutations() {
        assert_local_edit_matches_full(
            |_| (),
            |tx, _| {
                let list = tx.put_object(ROOT, "todos", ObjType::List).unwrap();
                tx.insert(&list, 0, "a").unwrap();
                tx.insert(&list, 1, "b").unwrap();
            },
        );
    }

    #[test]
    fn remote_object_creation_with_child_mutations_dirties_parent_and_child_ranges() {
        let mut doc1 = audit_doc();
        let mut doc2 = doc1.fork();

        let mut tx = doc2.transaction();
        let list = tx.put_object(ROOT, "todos", ObjType::List).unwrap();
        tx.insert(&list, 0, "a").unwrap();
        tx.insert(&list, 1, "b").unwrap();
        tx.commit();
        let changes = doc2.get_changes(&doc1.get_heads()).unwrap();

        doc1.ops_mut().clear_dirty();
        let before = doc1.get_heads();
        doc1.apply_changes(changes).unwrap();
        let after = doc1.get_heads();

        let parent_range = doc1.ops().prop_range(&ObjId::root(), "todos");
        let list_obj = doc1.exid_to_obj(&list).unwrap().id;
        let child_range = doc1.ops().scope_to_obj(&list_obj);
        assert_eq!(parent_range.end, child_range.start);
        assert_eq!(
            dirty_ranges(&doc1),
            vec![parent_range.start..child_range.end]
        );
        assert_dirty_diff_matches_full(&doc1, &before, &after);
    }

    #[test]
    fn remote_nested_object_creation_in_complex_layout_dirties_subtree_ranges() {
        let mut doc1 = audit_doc();
        let mut tx = doc1.transaction();
        tx.put(ROOT, "a", 1).unwrap();
        tx.put(ROOT, "z", 26).unwrap();
        tx.commit();
        let mut doc2 = doc1.fork();

        let mut tx = doc2.transaction();
        let map = tx.put_object(ROOT, "m", ObjType::Map).unwrap();
        tx.put(&map, "scalar", 10).unwrap();
        let list = tx.put_object(&map, "list", ObjType::List).unwrap();
        tx.insert(&list, 0, "a").unwrap();
        tx.insert(&list, 1, "b").unwrap();
        let text = tx.put_object(&map, "text", ObjType::Text).unwrap();
        tx.splice_text(&text, 0, 0, "hello").unwrap();
        tx.commit();
        let changes = doc2.get_changes(&doc1.get_heads()).unwrap();

        doc1.ops_mut().clear_dirty();
        let before = doc1.get_heads();
        doc1.apply_changes_batch(changes).unwrap();
        let after = doc1.get_heads();

        let ranges = dirty_ranges(&doc1);
        let parent_range = doc1.ops().prop_range(&ObjId::root(), "m");
        assert!(ranges_contain(&ranges, parent_range));
        for obj in [&map, &list, &text] {
            let range = doc1.ops().scope_to_obj(&doc1.exid_to_obj(obj).unwrap().id);
            assert!(ranges_contain(&ranges, range));
        }
        assert_dirty_diff_matches_full(&doc1, &before, &after);
    }

    #[test]
    fn dirty_diff_matches_full_diff_for_child_mutation_followed_by_parent_delete() {
        assert_local_edit_matches_full(
            |tx| {
                let map = tx.put_object(ROOT, "map", ObjType::Map).unwrap();
                tx.put(&map, "key", 1).unwrap();
                map
            },
            |tx, map| {
                tx.put(map, "key", 2).unwrap();
                tx.delete(ROOT, "map").unwrap();
            },
        );
    }

    #[test]
    fn concurrent_child_mutation_and_parent_delete_matches_full_diff() {
        // pinned time: a random hash could land on a fragment head and free
        // hashes this test needs
        let mut doc1 = Automerge::new().with_actor(ActorId::from([1]));
        let mut tx = doc1.transaction();
        let map = tx.put_object(ROOT, "map", ObjType::Map).unwrap();
        tx.put(&map, "key", 1).unwrap();
        tx.commit_with(crate::tx::CommitOptions::default().with_time(0));
        let before = doc1.get_heads();
        let mut doc2 = doc1.fork().with_actor(ActorId::from([2]));

        doc1.ops_mut().clear_dirty();
        let mut tx = doc1.transaction();
        tx.put(&map, "key", 2).unwrap();
        tx.put(&map, "other", 3).unwrap();
        tx.commit_with(crate::tx::CommitOptions::default().with_time(0));

        let mut tx = doc2.transaction();
        tx.delete(ROOT, "map").unwrap();
        tx.commit_with(crate::tx::CommitOptions::default().with_time(0));
        let changes = doc2.get_changes(&before).unwrap();
        doc1.apply_changes_batch(changes).unwrap();
        let after = doc1.get_heads();

        let ranges = dirty_ranges(&doc1);
        let parent_range = doc1.ops().prop_range(&ObjId::root(), "map");
        let child_range = doc1.ops().scope_to_obj(&doc1.exid_to_obj(&map).unwrap().id);
        assert!(ranges_contain(&ranges, parent_range));
        assert!(ranges_contain(&ranges, child_range));
        assert_dirty_diff_matches_full(&doc1, &before, &after);
    }

    #[test]
    fn dirty_diff_matches_full_diff_for_remote_map_conflict() {
        assert_remote_edit_matches_full(
            audit_doc(),
            None,
            |tx| tx.put(ROOT, "key", 1).unwrap(),
            |tx, _| tx.put(ROOT, "key", 2).unwrap(),
            |tx, _| tx.put(ROOT, "key", 3).unwrap(),
            Via::Apply,
        );
    }

    /// The map twin of [`remote_list_conflict_dirties_changed_rows`],
    /// run with the batch op winning once and losing once.
    #[test]
    fn remote_map_conflict_dirties_changed_rows() {
        for local in ["0b", "fb"] {
            let (doc1, _) = assert_remote_edit_matches_full(
                Automerge::new()
                    .with_actor(local.try_into().unwrap())
                    .enable_audit_mode()
                    .unwrap(),
                Some("aa".try_into().unwrap()),
                |tx| tx.put(ROOT, "key", 1).unwrap(),
                |tx, _| tx.put(ROOT, "key", 2).unwrap(),
                |tx, _| tx.put(ROOT, "key", 3).unwrap(),
                Via::Apply,
            );

            let key_range = doc1.ops().prop_range(&ObjId::root(), "key");
            let ranges = dirty_ranges(&doc1);
            // the overwritten put plus rows whose `top` moved, not the whole register
            assert_eq!(ranges.len(), 1, "local {local}");
            assert!(
                ranges_contain(std::slice::from_ref(&key_range), ranges[0].clone()),
                "local {local}: {:?} outside {:?}",
                ranges[0],
                key_range
            );
            assert_eq!(ranges[0].start, key_range.start, "local {local}");
        }
    }

    #[test]
    fn batched_remote_map_conflict_dirties_whole_new_key_register() {
        let mut doc1 = audit_doc();
        let mut doc2 = doc1.fork().with_actor(ActorId::from([2]));
        let mut doc3 = doc1.fork().with_actor(ActorId::from([3]));

        let mut tx = doc2.transaction();
        tx.put(ROOT, "key", "a").unwrap();
        tx.commit();

        let mut tx = doc3.transaction();
        tx.put(ROOT, "key", "b").unwrap();
        tx.commit();

        let mut changes = doc2.get_changes(&doc1.get_heads()).unwrap();
        changes.extend(doc3.get_changes(&doc1.get_heads()).unwrap());
        doc1.ops_mut().clear_dirty();
        let before = doc1.get_heads();
        doc1.apply_changes(changes).unwrap();
        let after = doc1.get_heads();

        let key_range = doc1.ops().prop_range(&ObjId::root(), "key");
        assert_eq!(dirty_ranges(&doc1), vec![key_range]);
        assert_dirty_diff_matches_full(&doc1, &before, &after);
    }

    #[test]
    fn dirty_diff_matches_full_diff_for_remote_list_conflict() {
        assert_remote_edit_matches_full(
            audit_doc(),
            None,
            |tx| put_list(tx, &["a"]),
            |tx, list| tx.put(list, 0, "A").unwrap(),
            |tx, list| tx.put(list, 0, "B").unwrap(),
            Via::Apply,
        );
    }

    /// The local actor sorts on each side of the remote, so the batch op
    /// wins once and loses once; either way only the changed rows are dirty.
    #[test]
    fn remote_list_conflict_dirties_changed_rows() {
        for local in ["0b", "fb"] {
            let (doc1, list) = assert_remote_edit_matches_full(
                Automerge::new()
                    .with_actor(local.try_into().unwrap())
                    .enable_audit_mode()
                    .unwrap(),
                Some("aa".try_into().unwrap()),
                |tx| put_list(tx, &["a"]),
                |tx, list| tx.put(list, 0, "A").unwrap(),
                |tx, list| tx.put(list, 0, "B").unwrap(),
                Via::Apply,
            );

            let list_obj = doc1.exid_to_obj(&list).unwrap().id;
            let list_range = doc1.ops().scope_to_obj(&list_obj);
            let ranges = dirty_ranges(&doc1);
            // one run, inside the register, and always covering the
            // element row the batch's put deleted
            assert_eq!(ranges.len(), 1, "local {local}");
            assert!(
                ranges_contain(std::slice::from_ref(&list_range), ranges[0].clone()),
                "local {local}: {:?} outside {:?}",
                ranges[0],
                list_range
            );
            assert_eq!(ranges[0].start, list_range.start, "local {local}");
        }
    }

    #[test]
    fn remote_insert_then_update_same_list_element_dirties_new_register() {
        let (doc1, list) = assert_remote_edit_matches_full(
            audit_doc(),
            None,
            |tx| put_list(tx, &[]),
            |_, _| (),
            |tx, list| {
                tx.insert(list, 0, "a").unwrap();
                tx.put(list, 0, "A").unwrap();
            },
            Via::Apply,
        );

        let list_obj = doc1.exid_to_obj(&list).unwrap().id;
        let list_range = doc1.ops().scope_to_obj(&list_obj);
        assert_eq!(
            assert_list_dirt_on_register_boundaries(&doc1, &list),
            vec![list_range]
        );
    }

    #[test]
    fn dirty_diff_matches_full_diff_for_remote_map_conflict_resolution_exposes_value() {
        // `get_changes` needs every hash it names
        let mut doc1 = audit_doc().with_actor(ActorId::from([1]));
        let mut tx = doc1.transaction();
        tx.put(ROOT, "key", "base").unwrap();
        tx.commit_with(crate::tx::CommitOptions::default().with_time(0));
        let mut doc2 = doc1.fork().with_actor(ActorId::from([2]));

        let mut tx = doc1.transaction();
        tx.put(ROOT, "key", "a").unwrap();
        tx.commit_with(crate::tx::CommitOptions::default().with_time(0));

        let mut tx = doc2.transaction();
        tx.put(ROOT, "key", "b").unwrap();
        tx.commit_with(crate::tx::CommitOptions::default().with_time(0));
        doc1.apply_changes(doc2.get_changes(&doc1.get_heads()).unwrap())
            .unwrap();

        let mut tx = doc2.transaction();
        tx.delete(ROOT, "key").unwrap();
        tx.commit_with(crate::tx::CommitOptions::default().with_time(0));
        let changes = doc2.get_changes(&doc1.get_heads()).unwrap();

        doc1.ops_mut().clear_dirty();
        let before = doc1.get_heads();
        doc1.apply_changes(changes).unwrap();
        let after = doc1.get_heads();

        assert_dirty_diff_matches_full(&doc1, &before, &after);
    }

    #[test]
    fn dirty_diff_matches_full_diff_for_remote_list_conflict_resolution_exposes_value() {
        let mut doc1 = Automerge::new().with_actor(ActorId::from([1]));
        let mut tx = doc1.transaction();
        let list = put_list(&mut tx, &["base"]);
        tx.commit();
        let mut doc2 = doc1.fork().with_actor(ActorId::from([2]));

        let mut tx = doc1.transaction();
        tx.put(&list, 0, "a").unwrap();
        tx.commit();

        let mut tx = doc2.transaction();
        tx.put(&list, 0, "b").unwrap();
        tx.commit();
        doc1.apply_changes(doc2.get_changes(&doc1.get_heads()).unwrap())
            .unwrap();

        let mut tx = doc2.transaction();
        tx.delete(&list, 0).unwrap();
        tx.commit();
        let changes = doc2.get_changes(&doc1.get_heads()).unwrap();

        doc1.ops_mut().clear_dirty();
        let before = doc1.get_heads();
        doc1.apply_changes(changes).unwrap();
        let after = doc1.get_heads();

        assert_dirty_diff_matches_full(&doc1, &before, &after);
    }

    #[test]
    fn dirty_diff_matches_full_diff_for_remote_counter_increment() {
        assert_remote_edit_matches_full(
            audit_doc(),
            None,
            |tx| tx.put(ROOT, "counter", ScalarValue::counter(1)).unwrap(),
            |_, _| (),
            |tx, _| tx.increment(ROOT, "counter", 2).unwrap(),
            Via::Apply,
        );
    }

    #[test]
    fn dirty_diff_matches_full_diff_for_remote_text_insert() {
        assert_remote_edit_matches_full(
            audit_doc(),
            None,
            |tx| put_text(tx, "abc", vec![]),
            |_, _| (),
            |tx, text| tx.splice_text(text, 1, 0, "X").unwrap(),
            Via::Apply,
        );
    }

    #[test]
    fn dirty_diff_matches_full_diff_for_remote_mark() {
        assert_remote_edit_matches_full(
            audit_doc(),
            None,
            |tx| put_text(tx, "abc", vec![]),
            |_, _| (),
            |tx, text| tx.mark(text, bold(0, 3), ExpandMark::Both).unwrap(),
            Via::Apply,
        );
    }

    #[test]
    fn dirty_diff_matches_full_diff_for_remote_middle_mark() {
        let (doc1, _) = assert_remote_edit_matches_full(
            audit_doc(),
            None,
            |tx| put_text(tx, "abcdef", vec![]),
            |_, _| (),
            |tx, text| tx.mark(text, bold(2, 4), ExpandMark::Both).unwrap(),
            Via::Apply,
        );
        // only the mark rows are dirty; the diff must widen to the marked span
        assert!(doc1.ops().dirty_runs().next().is_some());
    }

    #[test]
    fn dirty_diff_matches_full_diff_for_text_insert_without_marks() {
        assert_local_edit_matches_full(
            |tx| put_text(tx, "abc", vec![]),
            |tx, text| tx.splice_text(text, 1, 0, "X").unwrap(),
        );
    }

    #[test]
    fn dirty_diff_matches_full_diff_for_text_delete() {
        assert_local_edit_matches_full(
            |tx| put_text(tx, "abc", vec![]),
            |tx, text| tx.splice_text(text, 1, 1, "").unwrap(),
        );
    }

    #[test]
    fn dirty_diff_matches_full_diff_for_text_insert_inside_mark() {
        assert_local_edit_matches_full(
            |tx| put_text(tx, "abc", vec![bold(0, 3)]),
            |tx, text| tx.splice_text(text, 1, 0, "X").unwrap(),
        );
    }

    #[test]
    fn text_insert_at_mark_boundaries_stays_localized() {
        for index in [1, 3] {
            let (doc, text) = assert_local_edit_matches_full(
                |tx| put_text(tx, "abcd", vec![bold(1, 3)]),
                |tx, text| tx.splice_text(text, index, 0, "X").unwrap(),
            );

            let text_obj = doc.exid_to_obj(&text).unwrap().id;
            let text_range = doc.ops().scope_to_obj(&text_obj);
            let ranges = dirty_ranges(&doc);
            assert_eq!(ranges.len(), 1);
            assert!(text_range.start <= ranges[0].start && ranges[0].end <= text_range.end);
            assert_ne!(ranges[0], text_range);
            assert!(doc
                .ops()
                .mark_index_entries(ranges[0].clone())
                .next()
                .is_none());
        }
    }

    const VIAS: [Via; 4] = [Via::Local, Via::Apply, Via::Batch, Via::Load];

    const SPLICES_AROUND_ONE_MARK: &[(usize, isize, &str)] = &[
        (1, 1, ""),   // delete immediately before mark begin
        (2, 1, ""),   // delete at mark begin
        (3, 1, ""),   // delete inside marked span
        (4, 1, ""),   // delete immediately after mark end
        (1, 4, ""),   // delete through both mark anchors
        (2, 1, "X"),  // replace at mark begin
        (3, 1, "X"),  // replace inside marked span
        (2, 2, "XY"), // replace whole marked span
        (1, 4, "XY"), // replace across both mark anchors
    ];

    const SPLICES_AROUND_NESTED_MARKS: &[(usize, isize, &str)] = &[
        (4, 0, "X"),  // at nested mark begin
        (5, 1, ""),   // at overlapping mark begin
        (6, 2, "XY"), // through nested mark end
        (8, 1, ""),   // at outer mark end
    ];

    fn assert_splice_matches_full(
        setup: fn(&mut Transaction<'_, Full>) -> ExId,
        via: Via,
        (index, del, value): (usize, isize, &str),
    ) {
        let edit = |tx: &mut Transaction<'_, Full>, text: &ExId| {
            tx.splice_text(text, index, del, value).unwrap()
        };
        if via == Via::Local {
            assert_local_edit_matches_full(setup, edit);
        } else {
            assert_remote_edit_matches_full(audit_doc(), None, setup, |_, _| (), edit, via);
        }
    }

    #[test]
    fn text_splices_around_a_mark_match_full_diff() {
        for via in VIAS {
            for &splice in SPLICES_AROUND_ONE_MARK {
                assert_splice_matches_full(put_text_with_one_mark, via, splice);
            }
        }
    }

    #[test]
    fn text_splices_around_nested_overlapping_marks_match_full_diff() {
        for via in VIAS {
            for &splice in SPLICES_AROUND_NESTED_MARKS {
                assert_splice_matches_full(put_text_with_nested_marks, via, splice);
            }
        }
    }

    #[test]
    fn batch_text_edit_plus_mark_in_same_change_matches_full_diff() {
        let (doc1, _) = assert_remote_edit_matches_full(
            audit_doc(),
            None,
            put_text_with_one_mark,
            |_, _| (),
            |tx, text| {
                tx.splice_text(text, 2, 1, "X").unwrap();
                tx.mark(
                    text,
                    Mark::new("italic".to_string(), true, 1, 5),
                    ExpandMark::Both,
                )
                .unwrap();
            },
            Via::Batch,
        );
        // only the touched rows are dirty; the diff must widen to the marked span
        assert!(doc1.ops().dirty_runs().next().is_some());
    }

    #[test]
    fn batch_multiple_text_edits_around_same_mark_match_full_diff() {
        assert_remote_edit_matches_full(
            audit_doc(),
            None,
            put_text_with_one_mark,
            |_, _| (),
            |tx, text| {
                tx.splice_text(text, 4, 1, "Y").unwrap();
                tx.splice_text(text, 2, 1, "X").unwrap();
            },
            Via::Batch,
        );
    }

    #[test]
    fn dirty_diff_matches_full_diff_for_mark() {
        assert_local_edit_matches_full(
            |tx| put_text(tx, "abc", vec![]),
            |tx, text| tx.mark(text, bold(0, 3), ExpandMark::Both).unwrap(),
        );
    }

    /// The dirty diff relies on this to pair a mark's begin and end; a
    /// half-dirty mark would silently drop its patch.
    #[test]
    fn mark_dirties_both_of_its_ops() {
        let mut doc = audit_doc();
        let mut tx = doc.transaction();
        let text = put_text(&mut tx, "abc", vec![]);
        tx.commit();
        let before = doc.get_heads();
        doc.ops_mut().clear_dirty();

        let mut tx = doc.transaction();
        tx.mark(&text, bold(1, 2), ExpandMark::Both).unwrap();
        tx.commit();
        let after = doc.get_heads();

        let text_obj = doc.exid_to_obj(&text).unwrap().id;
        let text_range = doc.ops().scope_to_obj(&text_obj);
        let mark_rows: Vec<usize> = doc
            .ops()
            .iter_range(&text_range)
            .filter(|op| op.action == Action::Mark)
            .map(|op| op.pos)
            .collect();
        assert_eq!(mark_rows.len(), 2, "a mark writes a begin and an end");

        let dirty: std::collections::BTreeSet<usize> = doc.ops().dirty_positions().collect();
        for row in &mark_rows {
            assert!(dirty.contains(row), "mark op at {row} was not dirtied");
        }

        assert_dirty_diff_matches_full(&doc, &before, &after);
        doc.dirty_diff_patches(&doc.viewpoint_at(before.clone()));
        doc.clear_dirty();
        assert!(doc.ops().dirty_runs().next().is_none());
    }
}

/// The actor table must hold exactly the actors the change graph names:
/// actors are added speculatively, and a stray one shifts the indexes a
/// save writes.
#[cfg(test)]
mod actor_hygiene_tests {
    use super::*;
    use crate::autocommit::AutoCommit;
    use crate::tx::Transactable;
    use crate::ROOT;

    fn actor(n: u8) -> ActorId {
        ActorId::from(&[n, n, n, n][..])
    }

    fn assert_clean<H: crate::hash_retention::HashRetention>(doc: &Automerge<H>, what: &str) {
        let unused: Vec<_> = doc.change_graph.unused_actors().collect();
        assert!(
            unused.is_empty(),
            "{what}: {} actors in the table, {} named by the graph, unused {:?}",
            doc.ops.actors.len(),
            doc.change_graph.actor_ids().count(),
            unused,
        );
    }

    struct Snapshot {
        save: Vec<u8>,
        heads: Vec<ChangeId>,
        actors: crate::actor::ActorTable,
        ops: usize,
    }

    fn snapshot<H: crate::hash_retention::HashRetention>(doc: &mut AutoCommit<H>) -> Snapshot {
        Snapshot {
            save: doc.save(),
            heads: doc.get_heads(),
            actors: doc.doc.ops.actors.clone(),
            ops: doc.doc.ops.len(),
        }
    }

    fn assert_unchanged<H: crate::hash_retention::HashRetention>(
        doc: &mut AutoCommit<H>,
        before: &Snapshot,
        what: &str,
    ) {
        assert_eq!(doc.doc.ops.actors, before.actors, "{what}: actor table");
        assert_eq!(doc.doc.ops.len(), before.ops, "{what}: op count");
        assert_eq!(doc.get_heads(), before.heads, "{what}: heads");
        assert_eq!(doc.save(), before.save, "{what}: saved bytes");
        assert_clean(&doc.doc, what);
    }

    #[test]
    fn rolled_back_transaction_leaves_no_actor() {
        let mut doc = Automerge::new();
        doc.set_actor(actor(1));
        let mut tx = doc.transaction();
        tx.put(ROOT, "k", 1).unwrap();
        tx.rollback();
        assert_clean(&doc, "rollback of a new actor's first transaction");

        doc.transact::<_, _, AutomergeError>(|tx| tx.put(ROOT, "k", 1))
            .unwrap();
        let mut tx = doc.transaction();
        tx.put(ROOT, "k", 2).unwrap();
        tx.rollback();
        assert_clean(&doc, "rollback of an established actor's transaction");
    }

    #[test]
    fn empty_transaction_leaves_no_actor() {
        let mut doc = Automerge::new();
        doc.set_actor(actor(1));
        let tx = doc.transaction();
        assert!(tx.commit().is_none());
        assert_clean(&doc, "empty commit by a new actor");
    }

    #[test]
    fn failed_bundle_apply_changes_nothing() {
        let mut src = AutoCommit::new()
            .with_actor(actor(7))
            .enable_audit_mode()
            .unwrap();
        src.put(ROOT, "a", 1).unwrap();
        src.commit();
        src.put(ROOT, "b", 2).unwrap();
        src.commit();
        let changes = src.get_changes(&[]).unwrap();

        let head = changes[1].hash();
        let orphan = crate::change_graph::Fragment {
            head,
            level: head.fragment_level(),
            boundary: vec![changes[0].hash()],
            checkpoints: vec![],
            members: vec![changes[1].id()],
        };
        let whole = crate::change_graph::Fragment {
            head,
            level: head.fragment_level(),
            boundary: vec![],
            checkpoints: vec![],
            members: vec![changes[1].id(), changes[0].id()],
        };

        let mut dst = AutoCommit::new().with_actor(actor(1));
        dst.put(ROOT, "seed", 0).unwrap();
        dst.commit();
        let before = snapshot(&mut dst);
        let r = dst
            .doc
            .apply_bundle(src.doc.bundle_for_fragment(&orphan).unwrap());
        assert!(matches!(r, Err(AutomergeError::MissingDeps)));
        assert_unchanged(&mut dst, &before, "missing boundary");

        dst.doc
            .apply_bundle(src.doc.bundle_for_fragment(&whole).unwrap())
            .unwrap();
    }

    #[test]
    fn only_the_legacy_chunk_carries_a_stray_actor() {
        let mut doc = Automerge::new();
        doc.set_actor(actor(1));
        doc.transact::<_, _, AutomergeError>(|tx| tx.put(ROOT, "k", 1))
            .unwrap();
        let clean = doc.ops.actors.len();

        doc.put_actor(actor(9));
        assert_eq!(doc.change_graph.unused_actors().count(), 1);

        // below the save entry points, which assert against this state
        let bundle = doc.bundle_document_with(NameHashes::All).unwrap();
        assert_eq!(
            bundle.actors().len(),
            clean,
            "a bundle names only the actors its ops use"
        );

        let legacy = crate::storage::Document::new(
            &doc.ops,
            &doc.change_graph,
            crate::storage::document::CompressConfig::None,
        );
        assert_eq!(
            legacy.actors().len(),
            clean + 1,
            "the legacy chunk writes the actor table verbatim"
        );
    }

    #[test]
    fn failed_change_batch_changes_nothing() {
        let mut src = AutoCommit::new()
            .with_actor(actor(7))
            .enable_audit_mode()
            .unwrap();
        src.put(ROOT, "a", 1).unwrap();
        src.commit();
        src.put(ROOT, "b", 2).unwrap();
        src.commit();
        let good = src.get_changes(&[]).unwrap();

        // searched for rather than hand-built, so it survives encoding changes
        let base = good[1].raw_bytes().to_vec();
        let broken = (0..base.len())
            .flat_map(|i| (0..8).map(move |b| (i, b)))
            .find_map(|(i, bit)| {
                let mut raw = base.clone();
                raw[i] ^= 1 << bit;
                let c = crate::Change::try_from(&raw[..]).ok()?;
                let mut probe = Automerge::new();
                probe
                    .apply_changes(vec![good[0].clone(), c.clone()])
                    .is_err()
                    .then_some(c)
            })
            .expect("some corruption of a change fails during apply");

        let mut dst = AutoCommit::new()
            .with_actor(actor(1))
            .enable_audit_mode()
            .unwrap();
        dst.put(ROOT, "seed", 0).unwrap();
        dst.commit();
        let before = snapshot(&mut dst);
        assert!(dst
            .doc
            .apply_changes(vec![good[0].clone(), broken])
            .is_err());
        assert_unchanged(&mut dst, &before, "after a rejected batch");
    }

    #[test]
    fn failed_change_apply_leaves_no_actor() {
        let mut a = AutoCommit::new()
            .with_actor(actor(3))
            .enable_audit_mode()
            .unwrap();
        let mut b = a.fork().with_actor(actor(3));
        a.put(ROOT, "x", 1).unwrap();
        a.commit();
        b.put(ROOT, "x", 2).unwrap();
        b.commit();

        let mut dst = Automerge::new().enable_audit_mode().unwrap();
        dst.apply_changes(a.get_changes(&[]).unwrap()).unwrap();
        let err = dst.apply_changes(b.get_changes(&[]).unwrap());
        assert!(err.is_err(), "expected the equivocating change to fail");
        assert_clean(&dst, "change apply that failed on an equivocation");
    }
}

#[cfg(test)]
mod retained_hash_tests {
    use super::*;
    use crate::autocommit::AutoCommit;
    use crate::tx::{CommitOptions, Transactable};
    use crate::ROOT;
    use std::collections::BTreeSet;

    /// Otherwise the receiver must rehash the whole document.
    #[test]
    fn bundles_name_every_retained_hash() {
        let mut doc = AutoCommit::new().with_actor(ActorId::from(&b"aaaa"[..]));
        for i in 0..2000 {
            doc.put(ROOT, "k", i as i64).unwrap();
            doc.commit_with(CommitOptions::default().with_time(0));
        }
        let doc = doc.document();
        assert!(
            !doc.fragments(1..).is_empty(),
            "fixture needs both fragment bands"
        );

        let cs = doc.bundle_document_with(NameHashes::All).unwrap();
        let named: BTreeSet<ChangeHash> = cs
            .heads()
            .chain(cs.checkpoints.iter().map(|(_, h)| *h))
            .chain(cs.retained.iter().map(|(_, h)| *h))
            .collect();

        let nodes = doc.change_graph.all_nodes();
        let expected = doc
            .change_graph
            .hashes_to_retain(&doc.change_graph.bundle_members(&nodes), NameHashes::All)
            .unwrap()
            .retained;
        assert!(!expected.is_empty());
        let unnamed: Vec<_> = expected
            .iter()
            .filter(|(_, h)| !named.contains(h))
            .collect();
        assert!(
            unnamed.is_empty(),
            "{} of {} retained hashes unnamed",
            unnamed.len(),
            expected.len(),
        );
    }
}
