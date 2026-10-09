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
pub(crate) use crate::read::ReadDoc;

use crate::change_graph::{ChangeGraph, ChangeSetDep, ChangeSetMember};
use crate::change_queue::ChangeQueue;
use crate::cursor::{CursorPosition, MoveCursor, OpCursor};
use crate::exid::ExId;
use crate::hash_retention::{Full, HashRetention, Retained};
use crate::iter::{DiffIter, DocIter, Keys, ListRange, MapRange, Spans, Values};
use crate::marks::{Mark, MarkAccumulator, MarkSet};
use crate::op_set2::change::change_set::ChangeSetApply;
use crate::patches::{Patch, PatchAccumulator};
use crate::storage::{self, change, load, ChangeSet, CompressConfig, Document, VerificationMode};
use crate::tx::{
    self, CommitOptions, Failure, OwnedTransaction, Success, Transactable, Transaction,
    TransactionArgs,
};

use crate::change_graph::Fragment;
use crate::clock::{Clock, ClockRange};
use crate::hydrate;
use crate::types::{ActorId, ChangeHash, ObjId, ObjMeta, OpId, SequenceType, TextEncoding, Value};
use crate::{AutomergeError, Change, ChangeId, Cursor, ObjType, Prop};
use std::borrow::Cow;

pub(crate) mod current_state;
mod dirty_diff;
#[cfg(test)]
mod save_format_tests;

#[derive(Debug, Clone, PartialEq)]
pub(crate) enum Actor {
    Unused(ActorId),
    Cached(usize),
}

impl Actor {
    fn remove_actor(&mut self, index: usize, actors: &[ActorId]) {
        if let Actor::Cached(idx) = self {
            match (*idx).cmp(&index) {
                Ordering::Equal => *self = Actor::Unused(actors[index].clone()),
                Ordering::Greater => *idx -= 1,
                Ordering::Less => (),
            }
        }
    }

    fn rewrite_with_new_actor(&mut self, index: usize) {
        if let Actor::Cached(idx) = self {
            if *idx >= index {
                *idx += 1;
            }
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
    /// Current dependencies of this document (heads hashes).
    /// The set of operations that form this document.
    pub(crate) ops: OpSet,
    /// The current actor.
    actor: Actor,
    /// Cursor for dirty-bit incremental diffs.
    diff_cursor: Vec<ChangeId>,
    /// The current author.
    author: Option<Author<'static>>,
}

impl<H: HashRetention> Automerge<H> {
    pub(crate) fn empty(encoding: TextEncoding) -> Self {
        Automerge {
            queue: ChangeQueue::new(),
            change_graph: ChangeGraph::new(0),
            authors: Authors::with_actors(0),
            ops: OpSet::new(encoding),
            actor: Actor::Unused(ActorId::random()),
            diff_cursor: Vec::new(),
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

    pub(crate) fn from_parts(ops: OpSet, change_graph: ChangeGraph<H>, authors: Authors) -> Self {
        let mut doc = Automerge {
            queue: ChangeQueue::new(),
            change_graph,
            authors,
            ops,
            actor: Actor::Unused(ActorId::random()),
            diff_cursor: Vec::new(),
            author: None,
        };
        doc.remove_unused_actors(false);
        doc
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
        match self.ops.actors.binary_search(&actor) {
            Ok(idx) => {
                self.ensure_actor_tip_hash(idx);
                self.actor = Actor::Cached(idx)
            }
            Err(_) => self.actor = Actor::Unused(actor),
        }
        self
    }

    fn ensure_actor_tip_hash(&mut self, actor_idx: usize) {
        let seq = self.change_graph.seq_for_actor(actor_idx);
        if seq == 0 {
            return;
        }
        let id = self.change_id_at(actor_idx, seq);
        let Some(node) = self.change_graph.node_for_change_id(&id, &self.ops.actors) else {
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
        let actor_index = self.ops.actors.binary_search(actor).ok()?;
        self.authors.get_author_for_actor(actor_index)
    }

    /// Get the current actor id of this document.
    pub fn get_actor(&self) -> &ActorId {
        match &self.actor {
            Actor::Unused(actor) => actor,
            Actor::Cached(index) => self.ops.get_actor(*index),
        }
    }

    pub(crate) fn remove_actor(&mut self, actor: usize) {
        self.actor.remove_actor(actor, &self.ops.actors);
        self.ops.remove_actor(actor);
        self.change_graph.remove_actor(actor);
        self.authors.remove_actor(actor);
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
        self.resolve_heads(heads)?;
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
                    .resolve_heads(heads)
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

        // SAFETY: this unwrap is safe as we always add 1
        let start_op = NonZeroU64::new(self.change_graph.max_op() + 1).unwrap();
        let author = if seq == 1 { self.author.clone() } else { None };
        TransactionArgs {
            actor_index,
            seq,
            start_op,
            deps,
            scope,
            author,
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

    /// Run a transaction on this document in a closure, collecting patches, automatically handling commit or rollback
    /// afterwards.
    ///
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
    /// This will create a new actor ID for the forked document
    ///
    /// Returns an error if any of `heads` is not a change in this document.
    pub fn fork_at(&self, heads: &[ChangeId]) -> Result<Self, AutomergeError> {
        let heads = self.resolve_heads(heads)?;
        let mut seen = HashSet::new();
        let mut heads = heads
            .iter()
            .filter(|head| seen.insert(**head))
            .copied()
            .collect::<Vec<_>>();
        let mut hashes = vec![];
        while let Some(hash) = heads.pop() {
            if !self.change_graph.has_change(&hash)? {
                return Err(AutomergeError::InvalidHash(hash));
            }
            for dep in self.change_graph.deps_for_hash(&hash) {
                let dep = dep?;
                if seen.insert(dep) {
                    heads.push(dep);
                }
            }
            hashes.push(hash);
        }
        let mut f = Self::empty(self.text_encoding());
        let changes = self.get_changes_by_hashes(hashes.into_iter().rev())?;
        f.apply_changes(changes)?;
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
            return Ok(Self::empty(options.text_encoding).with_author(options.author));
        }
        tracing::trace!("loading first chunk");
        let (remaining, first_chunk) = storage::Chunk::parse(storage::parse::Input::new(data))
            .map_err(|e| load::Error::Parse(Box::new(e)))?;
        if !first_chunk.checksum_valid() {
            return Err(load::Error::BadChecksum.into());
        }

        let mut changes: Vec<load::LoadedChunk> = vec![];
        let mut first_chunk_was_doc = false;
        let mut am = match first_chunk {
            storage::Chunk::Document(d) => {
                tracing::trace!("first chunk is document chunk, inflating");
                first_chunk_was_doc = true;
                d.reconstruct(
                    options.verification_mode,
                    options.text_encoding,
                    mark_order.allows_invalid(),
                )
                .map_err(|e| load::Error::InflateDocument(Box::new(e)))?
            }
            storage::Chunk::Change(stored_change) => {
                tracing::trace!("first chunk is change chunk");
                changes.push(load::LoadedChunk::Change(Box::new(
                    Change::new_from_unverified(stored_change.into_owned(), None)
                        .map_err(|e| load::Error::InvalidChangeColumns(Box::new(e)))?,
                )));
                Self::empty(options.text_encoding)
            }
            storage::Chunk::BundleV0(change_set) => {
                tracing::trace!("first chunk is a 3.3.x change_set chunk");
                let storage = change_set
                    .into_owned()
                    .verify()
                    .map_err(|e| load::Error::InvalidChangeSetColumn(Box::new(e)))?;
                let change_set_changes = storage
                    .to_changes()
                    .map_err(|e| load::Error::InvalidChangeSetChange(Box::new(e)))?;
                changes.extend(
                    change_set_changes
                        .into_iter()
                        .map(|c| load::LoadedChunk::Change(Box::new(c))),
                );
                Self::empty(options.text_encoding)
            }
            storage::Chunk::ChangeSetColumns(change_set) => {
                tracing::trace!("first chunk is a change_set-columns chunk");
                // a leading change set is a document save, trailing orphans and all
                first_chunk_was_doc = true;
                changes.push(load::LoadedChunk::ChangeSet(change_set));
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
        let (rest, partial) = match load::load_changes(
            remaining.reset(),
            options.text_encoding,
            &am.change_graph,
            mark_order,
        ) {
            load::LoadedChanges::Complete(c) => (c, None),
            load::LoadedChanges::Partial { loaded, error, .. } => (loaded, Some(error)),
        };
        if let Some(error) = partial {
            if options.on_partial_load == OnPartialLoad::Error {
                return Err(error.into());
            }
            let all = changes.into_iter().chain(rest).collect();
            am.apply_loaded(all)?;
        } else {
            let all = changes.into_iter().chain(rest).collect();
            am.apply_loaded(all)?;
            // Only allow missing deps if the first chunk was a document chunk
            // See https://github.com/automerge/automerge/pull/599#issuecomment-1549667472
            if !am.queue.is_empty()
                && !first_chunk_was_doc
                && options.on_partial_load == OnPartialLoad::Error
            {
                return Err(AutomergeError::MissingDeps);
            }
        }
        if let StringMigration::ConvertToText = options.string_migration {
            am.convert_scalar_strings_to_text()?;
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
            doc = doc.with_actor(self.actor_id().clone());
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
        let clock = ClockRange::default();
        let path_map = DiffIter::log(self, obj, clock, patch_accumulator, recursive);
        patch_accumulator.path_hint(path_map);
    }

    fn apply_loaded(&mut self, chunks: Vec<load::LoadedChunk>) -> Result<(), AutomergeError> {
        let mut pending: Vec<Change> = vec![];
        for chunk in chunks {
            match chunk {
                load::LoadedChunk::Change(c) => pending.push(*c),
                load::LoadedChunk::ChangeSet(b) => {
                    if !pending.is_empty() {
                        self.apply_changes(std::mem::take(&mut pending))?;
                    }
                    self.apply_change_set(*b)?;
                }
            }
        }
        if !pending.is_empty() {
            self.apply_changes(pending)?;
        }
        Ok(())
    }

    /// Apply changes to this document.
    ///
    /// This is idempotent in the sense that if a change has already been applied it will be
    /// ignored.
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
        if let Some(change_set) = self.get_changes_added(other)? {
            tracing::trace!(heads=?change_set.heads().collect::<Vec<_>>(), "merging new changes");
            self.apply_change_set(change_set)?;
        }
        Ok(self.get_heads())
    }

    /// Save the entirety of this document in a compact form.
    ///
    /// A whole-document change set by default; see [`SaveFormat`].
    pub fn save_with_options(&self, options: SaveOptions) -> Vec<u8> {
        self.save_with(options, NameHashes::Anchors)
    }

    pub(crate) fn save_with(&self, options: SaveOptions, names: NameHashes) -> Vec<u8> {
        let mut bytes = if options.format == SaveFormat::Legacy {
            // the actor table is written verbatim, so unused actors would be saved
            self.assert_no_unused_actors(cfg!(debug_assertions));
            Document::new(&self.ops, &self.change_graph, options.compress()).into_bytes()
        } else if self.change_graph.is_empty() {
            // a change set needs a head; empty bytes load back as an empty document
            Vec::new()
        } else {
            let change_set = self
                .change_set_document_with(names)
                .expect("a document's own changes can always be made into a change set");
            if options.deflate {
                change_set.bytes()
            } else {
                change_set.bytes_uncompressed()
            }
        };

        // a change set must be causally closed, so orphans follow as change chunks
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

    /// The whole document as a single change set.
    pub fn change_set_document(&self) -> Result<ChangeSet, AutomergeError> {
        self.change_set_document_with(NameHashes::Anchors)
    }

    pub(crate) fn change_set_document_with(
        &self,
        names: NameHashes,
    ) -> Result<ChangeSet, AutomergeError> {
        let nodes = self.change_graph.all_nodes();
        let storage = ChangeSet::storage_for_document(&self.ops, &self.change_graph, &nodes)?;
        let heads = self.get_head_hashes();
        let external_deps: [ChangeHash; 0] = [];
        self.assemble_change_set(&heads, &external_deps, &nodes, storage, names)
    }

    /// The changes `heads` does not already cover, as one change set;
    /// `None` when there are none. Ids this document does not know are
    /// ignored.
    pub fn change_set_after(
        &self,
        heads: &[ChangeId],
    ) -> Result<Option<ChangeSet>, AutomergeError> {
        let nodes: Vec<_> = heads
            .iter()
            .filter_map(|id| self.node_for_change_id(id))
            .collect();
        let clock = self.change_graph.seq_clock_for_nodes(nodes);
        let fresh = self.change_graph.get_build_indexes(clock);
        let boundary = self.change_ids_to_hashes_lossy(heads);
        self.change_set_nodes(fresh, &boundary, NameHashes::Anchors)
    }

    fn change_set_nodes(
        &self,
        nodes: Vec<crate::change_graph::NodeIdx>,
        boundary: &[ChangeHash],
        names: NameHashes,
    ) -> Result<Option<ChangeSet>, AutomergeError> {
        if nodes.is_empty() {
            return Ok(None);
        }
        let storage = ChangeSet::storage_for_nodes(&self.ops, &self.change_graph, nodes.clone())?;
        let heads: Vec<ChangeHash> = self
            .get_head_hashes()
            .into_iter()
            .filter(|h| {
                self.change_graph
                    .node_by_hash(h)
                    .is_some_and(|n| nodes.binary_search(&n).is_ok())
            })
            .collect();
        let boundary: Vec<ChangeHash> = boundary
            .iter()
            .copied()
            .filter(|h| {
                self.change_graph
                    .node_by_hash(h)
                    .is_some_and(|n| nodes.binary_search(&n).is_err())
            })
            .collect();
        Ok(Some(self.assemble_change_set(
            &heads, &boundary, &nodes, storage, names,
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
            .change_set_after(heads)?
            .map(|b| {
                if options.deflate {
                    b.bytes()
                } else {
                    b.bytes_uncompressed()
                }
            })
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
        let Some(actor) = self.get_actor_index() else {
            return Ok(None);
        };
        let seq = self.change_graph.seq_for_actor(actor);
        if seq == 0 {
            return Ok(None);
        }
        let id = self.change_id_at(actor, seq);
        let hash = self
            .change_graph
            .get_hash_for_change_id(&id, &self.ops.actors)?;
        self.get_change_by_hash(&hash)
    }

    /// The last change this actor made, as a change set.
    ///
    /// `None` when this actor has not committed anything.
    pub fn get_last_local_change(&self) -> Result<Option<ChangeSet>, AutomergeError> {
        let Some(actor) = self.get_actor_index() else {
            return Ok(None);
        };
        let seq = self.change_graph.seq_for_actor(actor);
        if seq == 0 {
            return Ok(None);
        }
        let id = self.change_id_at(actor, seq);
        let Some(node) = self.change_graph.node_for_change_id(&id, &self.ops.actors) else {
            return Ok(None);
        };
        let deps = self.change_graph.parent_hashes(node);
        self.change_set_nodes(vec![node], &deps, NameHashes::All)
    }

    pub(crate) fn clock_range(
        &self,
        before: &[ChangeId],
        after: &[ChangeId],
    ) -> Result<ClockRange, AutomergeError> {
        let before = self.nodes_for_change_ids(before)?;
        let after = self.nodes_for_change_ids(after)?;
        Ok(ClockRange::Diff(
            self.change_graph.clock_for_nodes(before),
            Some(self.change_graph.clock_for_nodes(after)),
        ))
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
            .nodes_for_change_ids(heads)
            .map(|n| self.change_graph.clock_for_nodes(n))
            .expect("validated ids always have clocks");

        for i in 1.. {
            let max_op = self.change_graph.max_op_for_actor(actor_index);
            if max_op == 0 || clock.covers(&OpId::new(max_op, actor_index)) {
                clock.isolate(actor_index);
                break;
            }
            actor_index = self.get_isolated_actor_index(i);
            // need to recompute the clock b/c the actor indexes may have changed
            clock = self
                .nodes_for_change_ids(heads)
                .map(|n| self.change_graph.clock_for_nodes(n))
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
        self.change_graph
            .add_changes(
                changes
                    .iter()
                    .map(|c| (c, self.ops.actors.binary_search(c.actor_id()).unwrap())),
                &mut self.authors,
            )
            .unwrap();
    }

    pub(crate) fn update_history(&mut self, change: &Change) {
        let actor_index = self
            .ops
            .actors
            .binary_search(change.actor_id())
            .expect("Change's actor not already in the document");

        self.change_graph
            .add_change(change, actor_index, &mut self.authors)
            .expect("Change's deps should already be in the document");
    }

    fn insert_actor(&mut self, index: usize, actor: ActorId) -> usize {
        self.ops.insert_actor(index, actor);
        self.change_graph.insert_actor(index);
        self.actor.rewrite_with_new_actor(index);
        self.authors.insert_actor(index);
        index
    }

    /// Insert every actor in `actors` the document lacks, returning the ones
    /// inserted for [`Self::undo_actor_refs`].
    pub(crate) fn put_actor_refs(&mut self, actors: &[ActorId]) -> Vec<ActorId> {
        let mut new: Vec<ActorId> = actors
            .iter()
            .filter(|a| self.ops.actors.binary_search(a).is_err())
            .cloned()
            .collect();
        if new.is_empty() {
            return new;
        }
        new.sort_unstable();
        new.dedup();
        let mut old_to_new_index: Vec<u32> = Vec::with_capacity(self.ops.actors.len());
        let mut j = 0;
        for a in &self.ops.actors {
            while j < new.len() && new[j] < *a {
                j += 1;
            }
            old_to_new_index.push((old_to_new_index.len() + j) as u32);
        }
        let identity = old_to_new_index
            .iter()
            .enumerate()
            .all(|(i, &m)| m as usize == i);
        if !identity {
            self.ops.remap_actor_indexes(&old_to_new_index);
        }
        let mut amap = self.ops.actor_map();
        for a in &new {
            let idx = self.ops.actors.binary_search(a).unwrap_err();
            amap = amap.insert(idx, self.ops.actors.len());
            self.ops.actors.insert(idx, a.clone());
            self.change_graph.insert_actor(idx);
            self.authors.insert_actor(idx);
            self.actor.rewrite_with_new_actor(idx);
        }
        self.ops.set_actor_map(amap);
        new
    }

    /// Undo a [`Self::put_actor_refs`].
    pub(crate) fn undo_actor_refs(&mut self, added: &[ActorId]) {
        let mut idxs: Vec<usize> = added
            .iter()
            .filter_map(|a| self.ops.actors.binary_search(a).ok())
            .collect();
        idxs.sort_unstable();
        for idx in idxs.into_iter().rev() {
            self.remove_actor(idx);
        }
    }

    fn put_actor(&mut self, actor: ActorId) -> usize {
        match self.ops.actors.binary_search(&actor) {
            Ok(idx) => idx,
            Err(idx) => self.insert_actor(idx, actor),
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

    /// Create patches representing the change in the current state of the document between the
    /// `before` and `after` heads.  If the arguments are reverse it will observe the same changes
    /// in the opposite order.
    pub fn diff(
        &self,
        before_heads: &[ChangeId],
        after_heads: &[ChangeId],
    ) -> Result<Vec<Patch>, AutomergeError> {
        let clock = self.clock_range(before_heads, after_heads)?;
        let after_clock = clock.after().cloned();
        let mut patch_accumulator = PatchAccumulator::event_log();
        DiffIter::log(self, ObjMeta::root(), clock, &mut patch_accumulator, true);
        patch_accumulator.heads_clock = after_clock;
        Ok(patch_accumulator.make_patches(self))
    }

    /// The patches since the previous call (or since the empty document).
    pub fn diff_incremental(&mut self) -> Vec<Patch> {
        let before = self.diff_cursor.clone();
        let after = self.get_heads();
        let patches = self
            .dirty_diff_patches_and_clear(&before, &after)
            .expect("dirty diff should support Automerge incremental intervals");
        self.diff_cursor = after;
        patches
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
        before_heads: &[ChangeId],
        after_heads: &[ChangeId],
        recursive: bool,
    ) -> Result<Vec<Patch>, AutomergeError> {
        let obj = self.exid_to_obj(obj.as_ref())?;
        let clock = self.clock_range(before_heads, after_heads)?;
        let after_clock = clock.after().cloned();
        let mut patch_accumulator = PatchAccumulator::event_log();
        DiffIter::log(self, obj, clock, &mut patch_accumulator, recursive);
        patch_accumulator.heads_clock = after_clock;
        Ok(patch_accumulator.make_patches(self))
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
            .map(|id| self.change_graph.node_for_change_id(id, &self.ops.actors))
            .collect::<Option<Vec<_>>>()
            .ok_or(AutomergeError::InvalidFragment(
                "fragment references an unknown change",
            ))?;
        nodes.sort_unstable();
        nodes.dedup();
        Ok(nodes)
    }

    /// Build a change set from the changes it delivers (`heads`) and the
    /// set covering everything before it (`boundary`).
    pub fn make_change_set(
        &self,
        heads: &[ChangeId],
        boundary: &[ChangeId],
    ) -> Result<ChangeSet, AutomergeError> {
        let unknown = || AutomergeError::InvalidFragment("change set names an unknown change");
        let node_of = |id: &ChangeId| {
            self.change_graph
                .node_for_change_id(id, &self.ops.actors)
                .ok_or_else(unknown)
        };
        let head_nodes = heads.iter().map(node_of).collect::<Result<Vec<_>, _>>()?;
        let boundary_nodes = boundary
            .iter()
            .map(node_of)
            .collect::<Result<Vec<_>, _>>()?;

        let clock = self
            .change_graph
            .seq_clock_for_nodes(boundary_nodes.clone());
        let members = self
            .change_graph
            .members_between(head_nodes.clone(), &clock);

        let hash_of = |n: &crate::change_graph::NodeIdx| {
            self.change_graph.hash_for_node(*n).ok_or_else(unknown)
        };
        let head_hashes = head_nodes
            .iter()
            .map(hash_of)
            .collect::<Result<Vec<_>, _>>()?;
        let boundary_hashes = boundary_nodes
            .iter()
            .map(hash_of)
            .collect::<Result<Vec<_>, _>>()?;

        let storage = ChangeSet::storage_for_nodes(&self.ops, &self.change_graph, members.clone())?;
        self.assemble_change_set(
            &head_hashes,
            &boundary_hashes,
            &members,
            storage,
            NameHashes::All,
        )
    }

    /// [`Self::make_change_set`] for a [`Fragment`].
    pub fn make_change_set_from_fragment(&self, f: &Fragment) -> Result<ChangeSet, AutomergeError> {
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

        let change_set = self.make_change_set(&heads, &boundary)?;

        #[cfg(debug_assertions)]
        {
            let sorted = |mut v: Vec<ChangeHash>| {
                v.sort_unstable();
                v
            };
            debug_assert_eq!(
                sorted(change_set.checkpoints.iter().map(|(_, h)| *h).collect()),
                sorted(f.checkpoints.clone()),
                "derived checkpoints disagree with the fragment's",
            );
            debug_assert_eq!(
                sorted(change_set.boundary.iter().map(|(h, _)| *h).collect()),
                sorted(f.boundary.clone()),
                "derived boundary disagrees with the fragment's",
            );
            let mut derived: Vec<ChangeId> = change_set
                .member_actors
                .iter()
                .zip(change_set.member_seqs.iter())
                .map(|(a, s)| ChangeId::new(*s, change_set.actors()[usize::from(*a)].clone(), 0))
                .collect();
            let mut declared = f.members.clone();
            derived.sort();
            declared.sort();
            debug_assert_eq!(
                derived, declared,
                "derived members disagree with the fragment's"
            );
        }

        Ok(change_set)
    }

    #[doc(hidden)]
    pub fn change_set_for_fragment(&self, f: &Fragment) -> Result<ChangeSet, AutomergeError> {
        let nodes = self.fragment_nodes(f)?;
        let storage = ChangeSet::storage_for_nodes(&self.ops, &self.change_graph, nodes.clone())?;
        self.assemble_change_set(&[f.head], &f.boundary, &nodes, storage, NameHashes::All)
    }

    fn assemble_change_set(
        &self,
        heads: &[ChangeHash],
        boundary: &[ChangeHash],
        nodes: &[crate::change_graph::NodeIdx],
        storage: crate::storage::ChangeSetStorage<'static, crate::storage::change::Verified>,
        names: NameHashes,
    ) -> Result<ChangeSet, AutomergeError> {
        let members = crate::change_graph::Members::new(nodes);
        let retained: Vec<(usize, ChangeHash)> = self
            .change_graph
            .hashes_to_retain(nodes, names)
            .into_iter()
            .filter(|(_, h)| h.fragment_level() == 0 && !heads.contains(h))
            .collect();
        let unknown = || AutomergeError::InvalidFragment("fragment references an unknown change");
        // a change set lists its members in node order
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
            .leveled_members(&members)
            .filter(|(_, h)| !heads.iter().any(|(head, _)| head == h))
            .map(|(n, h)| members.position(n).map(|i| (i, h)))
            .collect::<Option<Vec<_>>>()
            .ok_or_else(unknown)?;
        checkpoints.sort_unstable_by_key(|(_, h)| *h);
        let change_id = |h: &ChangeHash| -> Option<ChangeId> {
            let n = self.change_graph.node_by_hash(h)?;
            Some(self.change_graph.change_id(n, &self.ops.actors))
        };
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
        let (member_actors, member_seqs) = storage.member_ids().map_err(|_| unknown())?;

        Ok(ChangeSet {
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

    pub fn change_sets_for_fragments<I: IntoIterator<Item = Fragment>>(
        &self,
        fragments: I,
    ) -> Result<Vec<Vec<u8>>, AutomergeError> {
        let fragments: Vec<Fragment> = fragments.into_iter().collect();
        let nodes = fragments
            .iter()
            .map(|f| self.fragment_nodes(f))
            .collect::<Result<Vec<_>, _>>()?;
        // one shared pass rather than a document walk per change set
        let storages =
            ChangeSet::storage_for_node_sets(&self.ops, &self.change_graph, nodes.clone())?;
        fragments
            .iter()
            .zip(nodes.iter())
            .zip(storages)
            .map(|((f, n), storage)| {
                Ok(self
                    .assemble_change_set(&[f.head], &f.boundary, n, storage, NameHashes::All)?
                    .bytes())
            })
            .collect()
    }

    /// Apply a change set's changes. To apply one change set to several
    /// documents, clone it.
    pub fn apply_change_set(&mut self, change_set: ChangeSet) -> Result<(), AutomergeError> {
        // a failed apply can leave unused actors; the legacy save sweeps them
        let result = self.apply_change_set_inner(change_set);
        if result.is_ok() {
            self.assert_no_unused_actors(cfg!(debug_assertions));
        }
        result
    }

    fn apply_change_set_inner(&mut self, change_set: ChangeSet) -> Result<(), AutomergeError> {
        H::apply_change_set(self, change_set)
    }

    /// Rebuilds and hashes every member instead of trusting the change
    /// set's metadata.
    pub(crate) fn apply_change_set_verified(
        &mut self,
        change_set: ChangeSet,
    ) -> Result<(), AutomergeError> {
        {
            let changes = change_set.to_changes()?;
            // checked before applying, so a rejected change set leaves no trace
            let carried: std::collections::HashSet<ChangeHash> =
                changes.iter().map(|c| c.hash()).collect();
            for c in &changes {
                for dep in c.deps() {
                    if !carried.contains(dep) && !self.change_graph.has_change(dep)? {
                        return Err(AutomergeError::MissingDeps);
                    }
                }
            }
            for (hash, index) in &change_set.heads {
                if changes.get(*index).map(|c| c.hash()) != Some(*hash) {
                    return Err(AutomergeError::MalformedChangeSet(
                        "change set head does not hash to the member it names",
                    ));
                }
            }
            self.apply_changes(changes)?;
            Ok(())
        }
    }
}

impl Automerge<Retained> {
    pub(crate) fn apply_change_set_trusted(
        &mut self,
        mut change_set: ChangeSet,
    ) -> Result<(), AutomergeError> {
        let change_set_ops = change_set.take_change_set_ops()?;
        let change_set = &change_set;

        let member_actors = &change_set.member_actors;
        let member_seqs = &change_set.member_seqs;
        let num_members = member_seqs.len();

        // parsing validated the change set's shape, so its indexes are in bounds

        // ── resolve: nothing in this phase writes to the document ──────
        let member_id = |i: usize| -> ChangeId {
            ChangeId::new(
                member_seqs[i],
                change_set.actors()[usize::from(member_actors[i])].clone(),
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
        let mut next_seq: Vec<Option<u64>> = vec![None; change_set.actors().len()];
        for i in 0..num_members {
            let actor = usize::from(member_actors[i]);
            let seq = member_seqs[i].get();
            let have = self
                .ops
                .lookup_actor(&change_set.actors()[actor])
                .and_then(|idx| seq_clock.get_for_actor(&idx))
                .map(|s| s.get() as u64)
                .unwrap_or(0);
            let next = next_seq[actor].unwrap_or(have + 1);
            match seq.cmp(&next) {
                Ordering::Less => continue, // already have this change
                Ordering::Greater => return Err(AutomergeError::MissingDeps),
                Ordering::Equal => {}
            }
            next_seq[actor] = Some(next + 1);
            keep[i] = true;
            kept_index[i] = num_kept;
            num_kept += 1;
        }

        if num_kept == 0 {
            return Ok(());
        }
        let overlap = num_kept < num_members;

        // only the commit below appends to the graph, so the kept members'
        // nodes are known before they exist
        let base = self.change_graph.len() as u32;
        let member_node = |i: usize| -> Option<crate::change_graph::NodeIdx> {
            if keep[i] {
                Some(crate::change_graph::NodeIdx(base + kept_index[i] as u32))
            } else {
                self.change_graph
                    .node_for_change_id(&member_id(i), &self.ops.actors)
            }
        };

        // recorded after the commit
        let mut hash_pairs: Vec<(crate::change_graph::NodeIdx, ChangeHash)> =
            Vec::with_capacity(change_set.boundary.len() + change_set.dep_ids.len());
        for (hash, id) in &change_set.boundary {
            let node = self
                .change_graph
                .node_for_change_id(id, &self.ops.actors)
                .ok_or(AutomergeError::MissingDeps)?;
            hash_pairs.push((node, *hash));
        }

        // `add_change_set_members*` and `record_fragment_head` maintain the graph's heads
        let mut graph_members: Vec<ChangeSetMember<'_>> = Vec::new();
        let mut ext_nodes: Vec<crate::change_graph::NodeIdx> = Vec::new();
        if overlap {
            let members = change_set.changes()?;
            graph_members.reserve(num_kept);
            for (i, m) in members.iter().enumerate() {
                if !keep[i] {
                    continue;
                }
                let mut deps = Vec::with_capacity(m.deps.len());
                for d in &m.deps {
                    let d = *d as usize;
                    if d < num_members {
                        if keep[d] {
                            deps.push(ChangeSetDep::Member(kept_index[d]));
                        } else {
                            let node = self
                                .change_graph
                                .node_for_change_id(&member_id(d), &self.ops.actors);
                            deps.push(ChangeSetDep::Node(node.ok_or(AutomergeError::MissingDeps)?));
                        }
                    } else {
                        let dep_id = change_set
                            .dep_ids
                            .get(d - num_members)
                            .ok_or(AutomergeError::MalformedChangeSet("bad dep index"))?;
                        let node = self
                            .change_graph
                            .node_for_change_id(dep_id, &self.ops.actors)
                            .ok_or(AutomergeError::MissingDeps)?;
                        hash_pairs.push((node, change_set.deps()[d - num_members]));
                        deps.push(ChangeSetDep::Node(node));
                    }
                }
                graph_members.push(ChangeSetMember {
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
        } else {
            // a declared dep the document lacks is `MissingDeps` even if no
            // member references it
            ext_nodes.reserve(change_set.dep_ids.len());
            for (i, dep_id) in change_set.dep_ids.iter().enumerate() {
                let node = self
                    .change_graph
                    .node_for_change_id(dep_id, &self.ops.actors)
                    .ok_or(AutomergeError::MissingDeps)?;
                hash_pairs.push((node, change_set.deps()[i]));
                ext_nodes.push(node);
            }
        }

        let mut head_nodes = Vec::with_capacity(change_set.heads.len());
        for (hash, index) in &change_set.heads {
            head_nodes.push((
                member_node(*index).ok_or(AutomergeError::MalformedChangeSet(
                    "change set head is not a member of the change set",
                ))?,
                *hash,
            ));
        }
        let checkpoint_nodes: Vec<_> = change_set
            .checkpoints
            .iter()
            .filter_map(|(i, hash)| member_node(*i).map(|n| (n, *hash)))
            .collect();
        let retained_nodes: Vec<_> = change_set
            .retained
            .iter()
            .filter_map(|(i, hash)| member_node(*i).map(|n| (n, *hash)))
            .collect();

        // ── commit: failures below must undo the actor table; the graph is
        // only touched once nothing can fail ──
        let added = self.put_actor_refs(change_set.actors());
        let actor_map: Vec<usize> = change_set
            .actors()
            .iter()
            .map(|a| self.ops.lookup_actor(a).expect("actor was just inserted"))
            .collect();
        let clock = self.change_graph.current_clock();

        // ops the clock covers belong to skipped members and are dropped
        let ops = match ChangeSetApply::new(
            change_set,
            actor_map.clone(),
            &clock,
            overlap,
            &self.ops,
            change_set_ops,
        ) {
            Ok(ops) => ops,
            Err(e) => {
                self.undo_actor_refs(&added);
                return Err(e);
            }
        };
        let resolved = match ops.resolve(self) {
            Ok(r) => r,
            Err(e) => {
                self.undo_actor_refs(&added);
                return Err(e);
            }
        };

        if overlap {
            // change set actor indexes to document actor indexes
            for m in &mut graph_members {
                m.actor = actor_map[m.actor];
            }
        }
        let added_members = if overlap {
            self.change_graph.add_change_set_members(
                graph_members,
                &mut self.authors,
                &self.ops.actors,
            )
        } else {
            self.change_graph.add_change_set_members_cols(
                &change_set.change_cols(),
                member_actors,
                member_seqs,
                &actor_map,
                &ext_nodes,
                &mut self.authors,
                &self.ops.actors,
            )
        };
        if let Err(e) = added_members {
            self.undo_actor_refs(&added);
            return Err(e);
        }

        // the GC is O(graph), so it runs once per apply, not per hash
        let mut owes_gc = false;
        for (node, hash) in hash_pairs {
            owes_gc |= self.change_graph.record_node_hash(node, hash);
        }
        for (node, hash) in head_nodes {
            owes_gc |= self.change_graph.record_fragment_head(node, hash);
        }
        for (node, hash) in checkpoint_nodes {
            owes_gc |= self.change_graph.record_node_hash(node, hash);
        }
        for (node, hash) in retained_nodes {
            owes_gc |= self.change_graph.record_node_hash(node, hash);
        }
        ops.commit(self, resolved);
        // reads the delivered ops, so it waits for the commit
        self.rebuild_missing_hashes(base..self.change_graph.len() as u32);
        if owes_gc {
            self.change_graph.gc_after_batch();
        }
        Ok(())
    }
}

impl<H: HashRetention> Automerge<H> {
    /// Rehash the nodes in `delivered` that must be retained but whose
    /// hashes the change set did not name.
    fn rebuild_missing_hashes(&mut self, delivered: std::ops::Range<u32>) {
        let missing = self.change_graph.unhashed_retained_nodes(delivered);
        if missing.is_empty() {
            return;
        }
        let nodes = self
            .change_graph
            .nodes_back_to_retained(missing.iter().copied());
        // without the delivered anchors this walk would rehash most of the document
        debug_assert_eq!(
            nodes
                .iter()
                .filter(|n| self.change_graph.is_below_fragment_top(**n))
                .count(),
            0,
            "rebuild walked below fragment_top ({} nodes)",
            nodes.len(),
        );
        let Some(hashes) = self.rehash(nodes.clone()) else {
            return;
        };
        let mut owes_gc = false;
        for (node, hash) in nodes.iter().zip(hashes) {
            if missing.binary_search(node).is_ok() {
                owes_gc |= self.change_graph.record_node_hash(*node, hash);
            }
        }
        if owes_gc {
            self.change_graph.gc_after_batch();
        }
    }

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
            .map_err(|e| inflate(Box::new(e)))?;
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
            .collect(&self.ops)
            .map_err(|e| inflate(Box::new(e)))?;
        Ok(collected.changes.iter().map(|c| c.hash()).collect())
    }

    fn map_graph<H2: HashRetention>(
        self,
        f: impl FnOnce(ChangeGraph<H>) -> ChangeGraph<H2>,
    ) -> Automerge<H2> {
        let Automerge {
            queue,
            change_graph,
            authors,
            ops,
            actor,
            diff_cursor,
            author,
        } = self;
        Automerge {
            queue,
            change_graph: f(change_graph),
            authors,
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
        let storage = ChangeSet::storage_for_nodes(&self.ops, &self.change_graph, nodes).ok()?;
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

    /// `Ok(None)` (read the present, via the fast paths) when `heads` are
    /// exactly the current heads. Only sound while no transaction is open:
    /// callers reading around one must resolve a concrete clock instead.
    pub(crate) fn clock_for_ids(
        &self,
        heads: &[ChangeId],
    ) -> Result<Option<Clock>, AutomergeError> {
        let nodes = self.nodes_for_change_ids(heads)?;
        if self.change_graph.nodes_are_heads(&nodes) {
            Ok(None)
        } else {
            Ok(Some(self.change_graph.clock_for_nodes(nodes)))
        }
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

    /// Get the [`ChangeId`] of the change that contains the given `opid`.
    ///
    /// Returns [`None`] if the `opid` is the root object id or does not
    /// exist in this document.
    pub fn change_id_for_opid(&self, exid: &ExId) -> Option<ChangeId> {
        match exid {
            ExId::Root => None,
            ExId::Id(..) => {
                let opid = self.exid_to_opid(exid).ok()?;
                self.change_graph.opid_to_change_id(opid, &self.ops.actors)
            }
        }
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

    pub(crate) fn resolve_heads(
        &self,
        heads: &[ChangeId],
    ) -> Result<Vec<ChangeHash>, AutomergeError> {
        self.change_ids_to_hashes(heads)
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
        self.change_graph.seq_clock_for_nodes(nodes)
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
            Some(change_set) => change_set.to_changes(),
            None => Ok(vec![]),
        }
    }

    /// Get changes in `other` that are not in `self`
    pub fn get_changes_added<H2: HashRetention>(
        &self,
        other: &Automerge<H2>,
    ) -> Result<Option<ChangeSet>, AutomergeError> {
        // per-actor sequences are linear, so a change in `other` is new
        // exactly when our seq clock does not cover it
        let ours = self.change_graph.current_seq_clock();
        let theirs = other.change_graph.current_seq_clock();
        let mut exclude = crate::clock::SeqClock::new(other.change_graph.num_actors());
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
                            .hash_for_change_id(&id, &self.ops.actors)?,
                        other
                            .change_graph
                            .hash_for_change_id(&id, &other.ops.actors)?,
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
        let nodes = other.change_graph.get_build_indexes(exclude.clone());
        // a change set names its external deps by hash; if the GC freed one,
        // widen back to the fragment that covers it
        let nodes = if other.change_graph.boundary_is_nameable(&nodes) {
            nodes
        } else {
            match other.change_graph.widen_boundary_to_fragment(&exclude) {
                Some(widened) => other.change_graph.get_build_indexes(widened),
                // assembling reports the unnameable dep
                None => nodes,
            }
        };
        let boundary = self.get_head_hashes();
        other.change_set_nodes(nodes, &boundary, NameHashes::All)
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

    fn calculate_marks(
        &self,
        obj: &ExId,
        clock: Option<Clock>,
    ) -> Result<Vec<Mark>, AutomergeError> {
        let obj = self.exid_to_obj(obj.as_ref())?;

        let Some(seq_type) = obj.typ.as_sequence_type() else {
            // Really we should return an error here but we don't in order to stay
            // compatibile with older implementations
            return Ok(Vec::new());
        };

        // present-time text marks come straight from the mark and text
        // indexes — no op materialization (the text index carries text
        // widths, so lists still take the walk below)
        if clock.is_none() && seq_type == SequenceType::Text {
            let fast = self.ops().calculate_marks_fast(&obj.id);
            #[cfg(feature = "slow_path_assertions")]
            {
                let slow = self.calculate_marks_slow(&obj, None, seq_type);
                assert_eq!(fast, slow, "indexed marks != walked marks");
            }
            return Ok(fast);
        }

        Ok(self.calculate_marks_slow(&obj, clock, seq_type))
    }

    fn calculate_marks_slow(
        &self,
        obj: &crate::types::ObjMeta,
        clock: Option<Clock>,
        seq_type: SequenceType,
    ) -> Vec<Mark> {
        let mut top_ops = self.ops().top_ops(&obj.id, clock).marks();

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
        let clock = heads.map_or(Ok(None), |heads| self.clock_for_ids(heads))?;
        Ok(self.hydrate_map(&ObjId::root(), clock.as_ref()))
    }

    pub(crate) fn hydrate_obj(
        &self,
        obj: &crate::ObjId,
        heads: Option<&[ChangeId]>,
    ) -> Result<hydrate::Value, AutomergeError> {
        let obj = self.exid_to_obj(obj)?;
        let clock = heads.map_or(Ok(None), |heads| self.clock_for_ids(heads))?;
        Ok(match obj.typ {
            ObjType::Map | ObjType::Table => self.hydrate_map(&obj.id, clock.as_ref()),
            ObjType::List => self.hydrate_list(&obj.id, clock.as_ref()),
            ObjType::Text => self.hydrate_text(&obj.id, clock.as_ref()),
        })
    }

    pub(crate) fn parents_for(
        &self,
        obj: &ExId,
        clock: Option<Clock>,
    ) -> Result<Parents<'_>, AutomergeError> {
        let obj = self.exid_to_obj(obj)?;
        // FIXME - now that we have blocks a correct text_rep is relevent
        Ok(self.ops.parents(obj.id, clock))
    }

    pub(crate) fn keys_for(&self, obj: &ExId, clock: Option<Clock>) -> Keys<'_> {
        self.exid_to_obj(obj)
            .ok()
            .map(|obj| self.ops.keys(&obj.id, clock))
            .unwrap_or_default()
    }

    pub(crate) fn iter_for(&self, obj: &ExId, clock: Option<Clock>) -> DocIter<'_> {
        self.exid_to_obj(obj)
            .ok()
            .map(|obj| DocIter::new(self, obj, clock))
            .unwrap_or_else(|| DocIter::empty(self.text_encoding()))
    }

    pub(crate) fn map_range_for<'a, R: RangeBounds<String> + 'a>(
        &'a self,
        obj: &ExId,
        range: R,
        clock: Option<Clock>,
    ) -> MapRange<'a> {
        self.exid_to_obj(obj)
            .ok()
            .map(|obj| self.ops.map_range(&obj.id, range, clock))
            .unwrap_or_default()
    }

    pub(crate) fn list_range_for<R: RangeBounds<usize>>(
        &self,
        obj: &ExId,
        range: R,
        clock: Option<Clock>,
    ) -> ListRange<'_> {
        self.exid_to_obj(obj)
            .ok()
            .map(|obj| self.ops.list_range(&obj.id, range, clock))
            .unwrap_or_default()
    }

    pub(crate) fn values_for(&self, obj: &ExId, clock: Option<Clock>) -> Values<'_> {
        self.exid_to_obj(obj)
            .ok()
            .map(|obj| Values::new(&self.ops, self.ops.top_ops(&obj.id, clock.clone()), clock))
            .unwrap_or_default()
    }

    pub(crate) fn length_for(&self, obj: &ExId, clock: Option<Clock>) -> usize {
        // FIXME - is doc.length() for a text always the string length?
        self.exid_to_obj(obj)
            .map(|obj| self.ops.seq_length(&obj.id, self.text_encoding(), clock))
            .unwrap_or(0)
    }

    pub(crate) fn text_for(
        &self,
        obj: &ExId,
        clock: Option<Clock>,
    ) -> Result<String, AutomergeError> {
        let obj = self.exid_to_obj(obj)?;
        Ok(self.ops.text(&obj.id, clock))
    }

    pub(crate) fn spans_for(
        &self,
        obj: &ExId,
        clock: Option<Clock>,
    ) -> Result<Spans<'_>, AutomergeError> {
        let obj = self.exid_to_obj(obj)?;
        Ok(Spans::new(self.ops.spans(&obj.id, clock)))
    }

    pub(crate) fn get_cursor_for(
        &self,
        obj: &ExId,
        position: CursorPosition,
        clock: Option<Clock>,
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
                let found = self
                    .ops
                    .seek_ops_by_index(&obj.id, i, seq_type, clock.as_ref());

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
        clock: Option<Clock>,
    ) -> Result<usize, AutomergeError> {
        match cursor {
            Cursor::Start => Ok(0),
            Cursor::End => Ok(self.length_for(obj, clock)),
            Cursor::Op(op) => {
                let obj_meta = self.exid_to_obj(obj)?;

                let Some(seq_type) = obj_meta.typ.as_sequence_type() else {
                    return Err(AutomergeError::InvalidCursor(cursor.clone()));
                };

                let opid = self.op_cursor_to_opid(op, clock.as_ref())?;

                let found = self
                    .ops
                    .seek_list_opid(&obj_meta.id, opid, seq_type, clock.as_ref())
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
                                let f = self.ops.seek_list_opid(
                                    &obj_meta.id,
                                    key,
                                    seq_type,
                                    clock.as_ref(),
                                );

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
        clock: Option<Clock>,
    ) -> Result<Vec<Mark>, AutomergeError> {
        self.calculate_marks(obj, clock)
    }

    pub(crate) fn get_for(
        &self,
        obj: &ExId,
        prop: Prop,
        clock: Option<Clock>,
    ) -> Result<Option<(Value<'_>, ExId)>, AutomergeError> {
        let obj = self.exid_to_obj(obj)?;
        let op = match (obj.typ, prop) {
            (ObjType::Map | ObjType::Table, Prop::Map(key)) => self
                .ops
                .seek_ops_by_map_key(&obj.id, &key, clock.as_ref())
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
                    .seek_ops_by_index(&obj.id, i, seq_type, clock.as_ref())
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
        clock: Option<Clock>,
    ) -> Result<Vec<(Value<'_>, ExId)>, AutomergeError> {
        let prop = prop.into();
        let obj = self.exid_to_obj(obj.as_ref())?;
        let values = match (obj.typ, prop) {
            (ObjType::Map | ObjType::Table, Prop::Map(key)) => self
                .ops
                .seek_ops_by_map_key(&obj.id, &key, clock.as_ref())
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
                    .seek_ops_by_index(&obj.id, i, seq_type, clock.as_ref())
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
        clock: Option<Clock>,
    ) -> Result<MarkSet, AutomergeError> {
        let obj = self.exid_to_obj(obj.as_ref())?;

        if clock.is_none() && obj.typ == ObjType::Text {
            let fast = self.ops().get_marks_fast(&obj.id, index);
            #[cfg(feature = "slow_path_assertions")]
            {
                let slow = self.get_marks_slow(&obj, index, None);
                assert_eq!(fast, slow, "indexed marks != walked marks");
            }
            return Ok(fast);
        }

        Ok(self.get_marks_slow(&obj, index, clock))
    }

    fn get_marks_slow(
        &self,
        obj: &crate::types::ObjMeta,
        index: usize,
        clock: Option<Clock>,
    ) -> MarkSet {
        let Some(seq_type) = obj.typ.as_sequence_type() else {
            return MarkSet::default();
        };
        let mut iter = self.ops.top_ops(&obj.id, clock).marks();
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
                                        None,
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

    // ── Replication: hash-keyed, since a peer names changes this document lacks ──

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
    pub fn disable_audit_mode(self) -> Automerge<Retained> {
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
                stack.extend(other.change_graph.deps_for_hash(&hash).flatten());
            }
        }
        added.reverse();
        other
            .get_changes_by_hashes(added)
            .expect("every hash came from other's graph")
    }

    pub(crate) fn hash_of_id(&self, id: &ChangeId) -> Option<ChangeHash> {
        let node = self.node_for_change_id(id)?;
        Some(self.change_graph.hash_of(node))
    }
}

impl<H: HashRetention> ReadDoc for Automerge<H> {
    fn parents<O: AsRef<ExId>>(&self, obj: O) -> Result<Parents<'_>, AutomergeError> {
        self.parents_for(obj.as_ref(), None)
    }

    fn parents_at<O: AsRef<ExId>>(
        &self,
        obj: O,
        heads: &[ChangeId],
    ) -> Result<Parents<'_>, AutomergeError> {
        let clock = self.clock_for_ids(heads)?;
        self.parents_for(obj.as_ref(), clock)
    }

    fn keys<O: AsRef<ExId>>(&self, obj: O) -> Keys<'_> {
        self.keys_for(obj.as_ref(), None)
    }

    fn keys_at<O: AsRef<ExId>>(
        &self,
        obj: O,
        heads: &[ChangeId],
    ) -> Result<Keys<'_>, AutomergeError> {
        let clock = self.clock_for_ids(heads)?;
        Ok(self.keys_for(obj.as_ref(), clock))
    }

    fn iter_at<O: AsRef<ExId>>(
        &self,
        obj: O,
        heads: Option<&[ChangeId]>,
    ) -> Result<DocIter<'_>, AutomergeError> {
        let clock = heads.map_or(Ok(None), |heads| self.clock_for_ids(heads))?;
        Ok(self.iter_for(obj.as_ref(), clock))
    }

    fn map_range<'a, O: AsRef<ExId>, R: RangeBounds<String> + 'a>(
        &'a self,
        obj: O,
        range: R,
    ) -> MapRange<'a> {
        self.map_range_for(obj.as_ref(), range, None)
    }

    fn map_range_at<'a, O: AsRef<ExId>, R: RangeBounds<String> + 'a>(
        &'a self,
        obj: O,
        range: R,
        heads: &[ChangeId],
    ) -> Result<MapRange<'a>, AutomergeError> {
        let clock = self.clock_for_ids(heads)?;
        Ok(self.map_range_for(obj.as_ref(), range, clock))
    }

    fn list_range<O: AsRef<ExId>, R: RangeBounds<usize>>(&self, obj: O, range: R) -> ListRange<'_> {
        self.list_range_for(obj.as_ref(), range, None)
    }

    fn list_range_at<O: AsRef<ExId>, R: RangeBounds<usize>>(
        &self,
        obj: O,
        range: R,
        heads: &[ChangeId],
    ) -> Result<ListRange<'_>, AutomergeError> {
        let clock = self.clock_for_ids(heads)?;
        Ok(self.list_range_for(obj.as_ref(), range, clock))
    }

    fn values<O: AsRef<ExId>>(&self, obj: O) -> Values<'_> {
        self.values_for(obj.as_ref(), None)
    }

    fn values_at<O: AsRef<ExId>>(
        &self,
        obj: O,
        heads: &[ChangeId],
    ) -> Result<Values<'_>, AutomergeError> {
        let clock = self.clock_for_ids(heads)?;
        Ok(self.values_for(obj.as_ref(), clock))
    }

    fn length<O: AsRef<ExId>>(&self, obj: O) -> usize {
        self.length_for(obj.as_ref(), None)
    }

    fn length_at<O: AsRef<ExId>>(
        &self,
        obj: O,
        heads: &[ChangeId],
    ) -> Result<usize, AutomergeError> {
        let clock = self.clock_for_ids(heads)?;
        Ok(self.length_for(obj.as_ref(), clock))
    }

    fn text<O: AsRef<ExId>>(&self, obj: O) -> Result<String, AutomergeError> {
        self.text_for(obj.as_ref(), None)
    }

    fn spans<O: AsRef<ExId>>(&self, obj: O) -> Result<Spans<'_>, AutomergeError> {
        self.spans_for(obj.as_ref(), None)
    }

    fn spans_at<O: AsRef<ExId>>(
        &self,
        obj: O,
        heads: &[ChangeId],
    ) -> Result<Spans<'_>, AutomergeError> {
        let clock = self.clock_for_ids(heads)?;
        self.spans_for(obj.as_ref(), clock)
    }

    fn get_cursor<O: AsRef<ExId>, I: Into<CursorPosition>>(
        &self,
        obj: O,
        position: I,
        at: Option<&[ChangeId]>,
    ) -> Result<Cursor, AutomergeError> {
        let clock = at.map_or(Ok(None), |heads| self.clock_for_ids(heads))?;
        self.get_cursor_for(obj.as_ref(), position.into(), clock, MoveCursor::After)
    }

    fn get_cursor_moving<O: AsRef<ExId>, I: Into<CursorPosition>>(
        &self,
        obj: O,
        position: I,
        at: Option<&[ChangeId]>,
        move_cursor: MoveCursor,
    ) -> Result<Cursor, AutomergeError> {
        let clock = at.map_or(Ok(None), |heads| self.clock_for_ids(heads))?;
        self.get_cursor_for(obj.as_ref(), position.into(), clock, move_cursor)
    }

    fn get_cursor_position<O: AsRef<ExId>>(
        &self,
        obj: O,
        cursor: &Cursor,
        at: Option<&[ChangeId]>,
    ) -> Result<usize, AutomergeError> {
        let clock = at.map_or(Ok(None), |heads| self.clock_for_ids(heads))?;
        self.get_cursor_position_for(obj.as_ref(), cursor, clock)
    }

    fn text_at<O: AsRef<ExId>>(
        &self,
        obj: O,
        heads: &[ChangeId],
    ) -> Result<String, AutomergeError> {
        let clock = self.clock_for_ids(heads)?;
        self.text_for(obj.as_ref(), clock)
    }

    fn marks<O: AsRef<ExId>>(&self, obj: O) -> Result<Vec<Mark>, AutomergeError> {
        self.marks_for(obj.as_ref(), None)
    }

    fn marks_at<O: AsRef<ExId>>(
        &self,
        obj: O,
        heads: &[ChangeId],
    ) -> Result<Vec<Mark>, AutomergeError> {
        let clock = self.clock_for_ids(heads)?;
        self.marks_for(obj.as_ref(), clock)
    }

    fn hydrate<O: AsRef<ExId>>(
        &self,
        obj: O,
        heads: Option<&[ChangeId]>,
    ) -> Result<hydrate::Value, AutomergeError> {
        let obj = self.exid_to_obj(obj.as_ref())?;
        let clock = heads.map_or(Ok(None), |h| self.clock_for_ids(h))?;
        Ok(match obj.typ {
            ObjType::List => self.hydrate_list(&obj.id, clock.as_ref()),
            ObjType::Text => self.hydrate_text(&obj.id, clock.as_ref()),
            _ => self.hydrate_map(&obj.id, clock.as_ref()),
        })
    }

    fn get_marks<O: AsRef<ExId>>(
        &self,
        obj: O,
        index: usize,
        heads: Option<&[ChangeId]>,
    ) -> Result<MarkSet, AutomergeError> {
        let clock = heads.map_or(Ok(None), |h| self.clock_for_ids(h))?;
        self.get_marks_for(obj.as_ref(), index, clock)
    }

    fn get<O: AsRef<ExId>, P: Into<Prop>>(
        &self,
        obj: O,
        prop: P,
    ) -> Result<Option<(Value<'_>, ExId)>, AutomergeError> {
        self.get_for(obj.as_ref(), prop.into(), None)
    }

    fn get_at<O: AsRef<ExId>, P: Into<Prop>>(
        &self,
        obj: O,
        prop: P,
        heads: &[ChangeId],
    ) -> Result<Option<(Value<'_>, ExId)>, AutomergeError> {
        let clock = self.clock_for_ids(heads)?;
        self.get_for(obj.as_ref(), prop.into(), clock)
    }

    fn get_all<O: AsRef<ExId>, P: Into<Prop>>(
        &self,
        obj: O,
        prop: P,
    ) -> Result<Vec<(Value<'_>, ExId)>, AutomergeError> {
        self.get_all_for(obj.as_ref(), prop.into(), None)
    }

    fn get_all_at<O: AsRef<ExId>, P: Into<Prop>>(
        &self,
        obj: O,
        prop: P,
        heads: &[ChangeId],
    ) -> Result<Vec<(Value<'_>, ExId)>, AutomergeError> {
        let clock = self.clock_for_ids(heads)?;
        self.get_all_for(obj.as_ref(), prop.into(), clock)
    }

    fn object_type<O: AsRef<ExId>>(&self, obj: O) -> Result<ObjType, AutomergeError> {
        let obj = obj.as_ref();
        let opid = self.exid_to_opid(obj)?;
        let typ = self.ops.object_type(&ObjId(opid));
        typ.ok_or_else(|| AutomergeError::InvalidObjId(obj.to_string()))
    }

    fn get_missing_deps(&self, heads: &[ChangeId]) -> Result<Vec<ChangeHash>, AutomergeError> {
        self.missing_deps_with_queued(&self.resolve_heads(heads)?)
    }

    fn get_change_by_hash(&self, hash: &ChangeHash) -> Result<Option<Change>, AutomergeError> {
        match ChangeCollector::for_hashes(&self.ops, &self.change_graph, [*hash]) {
            Ok(mut changes) => Ok(changes.pop()),
            Err(AutomergeError::AuditModeRequired) => Err(AutomergeError::AuditModeRequired),
            Err(_) => Ok(None),
        }
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

impl Default for Automerge<Retained> {
    fn default() -> Self {
        Self::new()
    }
}

/// The format a save writes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum SaveFormat {
    /// A change set, which keeps the document's fragment structure.
    /// Readers which predate change sets cannot load it.
    #[default]
    Default,
    /// A document chunk, for readers that predate change sets. Does not
    /// keep the fragment structure.
    Legacy,
}

/// Which loose-commit hashes a change set names.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum NameHashes {
    /// Only those a load cannot cheaply recompute.
    Anchors,
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
    clock: Clock,
}

#[cfg(test)]
mod dirty_diff_tests {
    use std::ops::Range;

    use crate::{
        marks::{ExpandMark, Mark},
        op_set2::types::Action,
        tx::Transactable,
        types::ObjId,
        ActorId, ScalarValue, ROOT,
    };

    use crate::autocommit::AutoCommit;

    use crate::automerge::Automerge;

    fn dirty_ranges<H: crate::hash_retention::HashRetention>(
        doc: &Automerge<H>,
    ) -> Vec<Range<usize>> {
        doc.ops().dirty_runs().collect()
    }

    fn ranges_contain(ranges: &[Range<usize>], needle: Range<usize>) -> bool {
        ranges
            .iter()
            .any(|range| range.start <= needle.start && needle.end <= range.end)
    }

    fn assert_patch_effects_match<H: crate::hash_retention::HashRetention>(
        doc: &Automerge<H>,
        before: &[crate::ChangeId],
        after: &[crate::ChangeId],
        left_label: &str,
        left: &[crate::Patch],
        right_label: &str,
        right: &[crate::Patch],
    ) {
        crate::patches::effect::assert_patches_have_same_effect(
            doc,
            before,
            after,
            left_label,
            left,
            right_label,
            right,
        );
    }

    fn assert_dirty_diff_matches_full<H: crate::hash_retention::HashRetention>(
        doc: &Automerge<H>,
        before: &[crate::ChangeId],
        after: &[crate::ChangeId],
    ) {
        let full = doc.diff(before, after).unwrap();
        let dirty = doc.dirty_diff_patches(before, after).unwrap();
        assert_patch_effects_match(doc, before, after, "dirty diff", &dirty, "full diff", &full);
    }

    fn assert_incremental_effect_matches_full<H: crate::hash_retention::HashRetention>(
        doc: &mut Automerge<H>,
        before: &[crate::ChangeId],
        after: &[crate::ChangeId],
    ) {
        let full = doc.diff(before, after).unwrap();
        let incremental = doc.diff_incremental();
        assert_patch_effects_match(
            doc,
            before,
            after,
            "incremental diff",
            &incremental,
            "full diff",
            &full,
        );
    }

    fn assert_autocommit_incremental_effect_matches_full<
        H: crate::hash_retention::HashRetention,
    >(
        doc: &mut AutoCommit<H>,
        before: &[crate::ChangeId],
        after: &[crate::ChangeId],
    ) {
        let full = doc.document().diff(before, after).unwrap();
        let incremental = doc.diff_incremental();
        assert_patch_effects_match(
            doc.document(),
            before,
            after,
            "incremental diff",
            &incremental,
            "full diff",
            &full,
        );
    }

    #[test]
    fn dirty_diff_matches_full_diff_for_map_put() {
        let mut doc = Automerge::new().enable_audit_mode().unwrap();
        doc.ops_mut().clear_dirty();
        let before = doc.get_heads();

        let mut tx = doc.transaction();
        tx.put(ROOT, "key", 1).unwrap();
        tx.commit();
        let after = doc.get_heads();

        assert_dirty_diff_matches_full(&doc, &before, &after);
    }

    #[test]
    fn automerge_diff_incremental_clears_dirty_and_advances_cursor() {
        let mut doc = Automerge::new().enable_audit_mode().unwrap();
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
        let mut doc = Automerge::new().enable_audit_mode().unwrap();
        let mut tx = doc.transaction();
        let list = tx.put_object(ROOT, "list", crate::ObjType::List).unwrap();
        tx.insert(&list, 0, "a").unwrap();
        tx.commit();
        let before = doc.get_heads();

        let mut tx = doc.transaction();
        tx.put(&list, 0, "A").unwrap();
        tx.commit();
        let after = doc.get_heads();

        // one dirty row inside a multi-row register must widen to the register
        doc.ops_mut().clear_dirty();
        doc.ops_mut().mark_dirty(1);
        let patches = doc.dirty_diff_patches_and_clear(&before, &after).unwrap();
        let expected = doc.diff(&before, &after).unwrap();
        assert_eq!(patches, expected);
        assert!(doc.ops().dirty_runs().next().is_none());
    }

    #[test]
    fn automerge_diff_incremental_empty_doc_and_repeated_calls_are_empty() {
        let mut doc = Automerge::new().enable_audit_mode().unwrap();

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
        let mut source = Automerge::new().enable_audit_mode().unwrap();
        let mut tx = source.transaction();
        let list = tx.put_object(ROOT, "todos", crate::ObjType::List).unwrap();
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
        let mut source = Automerge::new().enable_audit_mode().unwrap();
        let mut tx = source.transaction();
        tx.put(ROOT, "base", 1).unwrap();
        tx.commit();
        let base_heads = source.get_heads();
        let base_data = source.save();

        let mut doc = Automerge::new().enable_audit_mode().unwrap();
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
        let mut source = Automerge::new().enable_audit_mode().unwrap();
        let mut tx = source.transaction();
        tx.put(ROOT, "key", 1).unwrap();
        tx.commit();

        let mut doc = Automerge::new().enable_audit_mode().unwrap();
        let before = doc.get_heads();
        doc.apply_changes(source.get_changes(&[]).unwrap()).unwrap();
        let after = doc.get_heads();
        assert_incremental_effect_matches_full(&mut doc, &before, &after);
        assert!(doc.ops().dirty_runs().next().is_none());

        let mut source = Automerge::new().enable_audit_mode().unwrap();
        let mut tx = source.transaction();
        tx.put(ROOT, "merged", 2).unwrap();
        tx.commit();

        let mut doc = Automerge::new().enable_audit_mode().unwrap();
        let before = doc.get_heads();
        doc.merge(&mut source).unwrap();
        let after = doc.get_heads();
        assert_incremental_effect_matches_full(&mut doc, &before, &after);
        assert!(doc.ops().dirty_runs().next().is_none());

        let mut source = Automerge::new().enable_audit_mode().unwrap();
        let mut tx = source.transaction();
        tx.put(ROOT, "synced", 3).unwrap();
        tx.commit();
        // what receiving a v2 sync message does
        let mut doc = Automerge::new().enable_audit_mode().unwrap();
        let before = doc.get_heads();
        doc.load_incremental(&source.save()).unwrap();
        let after = doc.get_heads();
        assert_incremental_effect_matches_full(&mut doc, &before, &after);
        assert!(doc.ops().dirty_runs().next().is_none());
    }

    #[test]
    fn automerge_diff_incremental_fork_inherits_cursor() {
        let mut doc = Automerge::new().enable_audit_mode().unwrap();
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
        let mut doc = Automerge::new().enable_audit_mode().unwrap();
        let mut tx = doc.transaction();
        tx.put(ROOT, "key", 1).unwrap();
        tx.commit();

        doc.ops_mut().clear_dirty();
        let before = doc.get_heads();
        let mut tx = doc.transaction();
        tx.put(ROOT, "key", 2).unwrap();
        tx.commit();
        let after = doc.get_heads();

        assert_dirty_diff_matches_full(&doc, &before, &after);
    }

    #[test]
    fn adjacent_map_updates_dirty_contiguous_key_ranges() {
        let mut doc = Automerge::new().enable_audit_mode().unwrap();
        let mut tx = doc.transaction();
        tx.put(ROOT, "a", 1).unwrap();
        tx.put(ROOT, "b", 2).unwrap();
        tx.put(ROOT, "c", 3).unwrap();
        tx.commit();

        doc.ops_mut().clear_dirty();
        let before = doc.get_heads();
        let mut tx = doc.transaction();
        tx.put(ROOT, "a", 10).unwrap();
        tx.put(ROOT, "b", 20).unwrap();
        tx.commit();
        let after = doc.get_heads();

        let a = doc.ops().prop_range(&ObjId::root(), "a");
        let b = doc.ops().prop_range(&ObjId::root(), "b");
        assert_eq!(dirty_ranges(&doc), vec![a.start..b.end]);
        assert_dirty_diff_matches_full(&doc, &before, &after);
    }

    #[test]
    fn remote_map_update_and_adjacent_insert_dirty_contiguous_key_ranges() {
        let mut doc1 = Automerge::new().enable_audit_mode().unwrap();
        let mut tx = doc1.transaction();
        tx.put(ROOT, "a", 1).unwrap();
        tx.put(ROOT, "c", 3).unwrap();
        tx.commit();
        let mut doc2 = doc1.fork();

        let mut tx = doc2.transaction();
        tx.put(ROOT, "a", 10).unwrap();
        tx.put(ROOT, "b", 2).unwrap();
        tx.commit();
        let changes = doc2.get_changes(&doc1.get_heads()).unwrap();

        doc1.ops_mut().clear_dirty();
        let before = doc1.get_heads();
        doc1.apply_changes(changes).unwrap();
        let after = doc1.get_heads();

        let a = doc1.ops().prop_range(&ObjId::root(), "a");
        let b = doc1.ops().prop_range(&ObjId::root(), "b");
        assert_eq!(a.end, b.start);
        assert_eq!(dirty_ranges(&doc1), vec![a.start..b.end]);
        assert_dirty_diff_matches_full(&doc1, &before, &after);
    }

    #[test]
    fn dirty_diff_matches_full_diff_for_map_delete() {
        let mut doc = Automerge::new().enable_audit_mode().unwrap();
        let mut tx = doc.transaction();
        tx.put(ROOT, "key", 1).unwrap();
        tx.commit();

        doc.ops_mut().clear_dirty();
        let before = doc.get_heads();
        let mut tx = doc.transaction();
        tx.delete(ROOT, "key").unwrap();
        tx.commit();
        let after = doc.get_heads();

        assert_dirty_diff_matches_full(&doc, &before, &after);
    }

    #[test]
    fn dirty_diff_matches_full_diff_for_map_increment() {
        let mut doc = Automerge::new().enable_audit_mode().unwrap();
        let mut tx = doc.transaction();
        tx.put(ROOT, "counter", ScalarValue::counter(1)).unwrap();
        tx.commit();

        doc.ops_mut().clear_dirty();
        let before = doc.get_heads();
        let mut tx = doc.transaction();
        tx.increment(ROOT, "counter", 2).unwrap();
        tx.commit();
        let after = doc.get_heads();

        assert_dirty_diff_matches_full(&doc, &before, &after);
    }

    #[test]
    fn dirty_diff_matches_full_diff_for_list_insert() {
        let mut doc = Automerge::new().enable_audit_mode().unwrap();
        let mut tx = doc.transaction();
        let list = tx.put_object(ROOT, "list", crate::ObjType::List).unwrap();
        tx.commit();

        doc.ops_mut().clear_dirty();
        let before = doc.get_heads();
        let mut tx = doc.transaction();
        tx.insert(&list, 0, "a").unwrap();
        tx.commit();
        let after = doc.get_heads();

        assert_dirty_diff_matches_full(&doc, &before, &after);
    }

    #[test]
    fn dirty_diff_matches_full_diff_for_list_update() {
        let mut doc = Automerge::new().enable_audit_mode().unwrap();
        let mut tx = doc.transaction();
        let list = tx.put_object(ROOT, "list", crate::ObjType::List).unwrap();
        tx.insert(&list, 0, "a").unwrap();
        tx.insert(&list, 1, "b").unwrap();
        tx.insert(&list, 2, "c").unwrap();
        tx.commit();

        doc.ops_mut().clear_dirty();
        let before = doc.get_heads();
        let mut tx = doc.transaction();
        tx.put(&list, 1, "B").unwrap();
        tx.commit();
        let after = doc.get_heads();

        assert_dirty_diff_matches_full(&doc, &before, &after);
    }

    #[test]
    fn adjacent_list_updates_dirty_contiguous_register_ranges() {
        let mut doc = Automerge::new().enable_audit_mode().unwrap();
        let mut tx = doc.transaction();
        let list = tx.put_object(ROOT, "list", crate::ObjType::List).unwrap();
        tx.insert(&list, 0, "a").unwrap();
        tx.insert(&list, 1, "b").unwrap();
        tx.insert(&list, 2, "c").unwrap();
        tx.commit();

        doc.ops_mut().clear_dirty();
        let before = doc.get_heads();
        let mut tx = doc.transaction();
        tx.put(&list, 1, "B").unwrap();
        tx.put(&list, 2, "C").unwrap();
        tx.commit();
        let after = doc.get_heads();

        let ranges = dirty_ranges(&doc);
        let list_obj = doc.exid_to_obj(&list).unwrap().id;
        let list_range = doc.ops().scope_to_obj(&list_obj);
        assert_eq!(ranges, vec![2..6]);
        assert!(doc
            .ops()
            .list_range_is_on_register_boundaries(&ranges[0], list_range));
        assert_dirty_diff_matches_full(&doc, &before, &after);
    }

    #[test]
    fn remote_adjacent_list_updates_dirty_contiguous_register_ranges() {
        let mut doc1 = Automerge::new().enable_audit_mode().unwrap();
        let mut tx = doc1.transaction();
        let list = tx.put_object(ROOT, "list", crate::ObjType::List).unwrap();
        tx.insert(&list, 0, "a").unwrap();
        tx.insert(&list, 1, "b").unwrap();
        tx.insert(&list, 2, "c").unwrap();
        tx.commit();
        let mut doc2 = doc1.fork();

        let mut tx = doc2.transaction();
        tx.put(&list, 1, "B").unwrap();
        tx.put(&list, 2, "C").unwrap();
        tx.commit();
        let changes = doc2.get_changes(&doc1.get_heads()).unwrap();

        doc1.ops_mut().clear_dirty();
        let before = doc1.get_heads();
        doc1.apply_changes(changes).unwrap();
        let after = doc1.get_heads();

        let ranges = dirty_ranges(&doc1);
        let list_obj = doc1.exid_to_obj(&list).unwrap().id;
        let list_range = doc1.ops().scope_to_obj(&list_obj);
        assert_eq!(ranges, vec![2..6]);
        assert!(doc1
            .ops()
            .list_range_is_on_register_boundaries(&ranges[0], list_range));
        assert_dirty_diff_matches_full(&doc1, &before, &after);
    }

    #[test]
    fn batch_remote_adjacent_list_update_and_conflict_dirty_register_ranges() {
        let mut doc1 = Automerge::new().with_actor(ActorId::from([1]));
        let mut tx = doc1.transaction();
        let list = tx.put_object(ROOT, "list", crate::ObjType::List).unwrap();
        tx.insert(&list, 0, "a").unwrap();
        tx.insert(&list, 1, "b").unwrap();
        tx.insert(&list, 2, "c").unwrap();
        tx.commit();
        let mut doc2 = doc1.fork().with_actor(ActorId::from([2]));

        let mut tx = doc1.transaction();
        tx.put(&list, 1, "local-b").unwrap();
        tx.commit();

        let mut tx = doc2.transaction();
        tx.put(&list, 1, "remote-b").unwrap();
        tx.put(&list, 2, "remote-c").unwrap();
        tx.commit();
        let changes = doc2.get_changes(&doc1.get_heads()).unwrap();

        doc1.ops_mut().clear_dirty();
        let before = doc1.get_heads();
        doc1.apply_changes_batch(changes).unwrap();
        let after = doc1.get_heads();

        let ranges = dirty_ranges(&doc1);
        let list_obj = doc1.exid_to_obj(&list).unwrap().id;
        let list_range = doc1.ops().scope_to_obj(&list_obj);
        assert_eq!(ranges.len(), 1);
        assert!(doc1
            .ops()
            .list_range_is_on_register_boundaries(&ranges[0], list_range));
        assert_dirty_diff_matches_full(&doc1, &before, &after);
    }

    #[test]
    fn batch_remote_list_update_plus_nearby_insert_dirty_register_ranges() {
        let mut doc1 = Automerge::new().enable_audit_mode().unwrap();
        let mut tx = doc1.transaction();
        let list = tx.put_object(ROOT, "list", crate::ObjType::List).unwrap();
        tx.insert(&list, 0, "a").unwrap();
        tx.insert(&list, 1, "b").unwrap();
        tx.insert(&list, 2, "c").unwrap();
        tx.commit();
        let mut doc2 = doc1.fork();

        let mut tx = doc2.transaction();
        tx.put(&list, 1, "B").unwrap();
        tx.insert(&list, 2, "X").unwrap();
        tx.commit();
        let changes = doc2.get_changes(&doc1.get_heads()).unwrap();

        doc1.ops_mut().clear_dirty();
        let before = doc1.get_heads();
        doc1.apply_changes_batch(changes).unwrap();
        let after = doc1.get_heads();

        let ranges = dirty_ranges(&doc1);
        let list_obj = doc1.exid_to_obj(&list).unwrap().id;
        let list_range = doc1.ops().scope_to_obj(&list_obj);
        assert!(ranges.iter().all(|range| doc1
            .ops()
            .list_range_is_on_register_boundaries(range, list_range.clone())));
        assert_dirty_diff_matches_full(&doc1, &before, &after);
    }

    #[test]
    fn batch_remote_insert_before_updated_list_element_matches_full_diff() {
        let mut doc1 = Automerge::new().enable_audit_mode().unwrap();
        let mut tx = doc1.transaction();
        let list = tx.put_object(ROOT, "list", crate::ObjType::List).unwrap();
        tx.insert(&list, 0, "a").unwrap();
        tx.insert(&list, 1, "b").unwrap();
        tx.insert(&list, 2, "c").unwrap();
        tx.insert(&list, 3, "d").unwrap();
        tx.commit();
        let mut doc2 = doc1.fork();

        let mut tx = doc2.transaction();
        tx.insert(&list, 1, "X").unwrap();
        tx.put(&list, 2, "B").unwrap();
        tx.put(&list, 3, "C").unwrap();
        tx.commit();
        let changes = doc2.get_changes(&doc1.get_heads()).unwrap();

        doc1.ops_mut().clear_dirty();
        let before = doc1.get_heads();
        doc1.apply_changes_batch(changes).unwrap();
        let after = doc1.get_heads();

        let ranges = dirty_ranges(&doc1);
        let list_obj = doc1.exid_to_obj(&list).unwrap().id;
        let list_range = doc1.ops().scope_to_obj(&list_obj);
        assert!(ranges.iter().all(|range| doc1
            .ops()
            .list_range_is_on_register_boundaries(range, list_range.clone())));
        assert_dirty_diff_matches_full(&doc1, &before, &after);
    }

    #[test]
    fn batch_remote_insert_before_conflicting_list_element_matches_full_diff() {
        let mut doc1 = Automerge::new().with_actor(ActorId::from([1]));
        let mut tx = doc1.transaction();
        let list = tx.put_object(ROOT, "list", crate::ObjType::List).unwrap();
        tx.insert(&list, 0, "a").unwrap();
        tx.insert(&list, 1, "b").unwrap();
        tx.insert(&list, 2, "c").unwrap();
        tx.commit();
        let mut doc2 = doc1.fork().with_actor(ActorId::from([2]));

        let mut tx = doc1.transaction();
        tx.put(&list, 1, "local-b").unwrap();
        tx.commit();

        let mut tx = doc2.transaction();
        tx.insert(&list, 1, "X").unwrap();
        tx.put(&list, 2, "remote-b").unwrap();
        tx.commit();
        let changes = doc2.get_changes(&doc1.get_heads()).unwrap();

        doc1.ops_mut().clear_dirty();
        let before = doc1.get_heads();
        doc1.apply_changes_batch(changes).unwrap();
        let after = doc1.get_heads();

        let ranges = dirty_ranges(&doc1);
        let list_obj = doc1.exid_to_obj(&list).unwrap().id;
        let list_range = doc1.ops().scope_to_obj(&list_obj);
        assert!(ranges.iter().all(|range| doc1
            .ops()
            .list_range_is_on_register_boundaries(range, list_range.clone())));
        assert_dirty_diff_matches_full(&doc1, &before, &after);
    }

    #[test]
    fn batch_remote_dependent_insert_and_update_list_changes_match_full_diff() {
        let mut doc1 = Automerge::new().enable_audit_mode().unwrap();
        let mut tx = doc1.transaction();
        let list = tx.put_object(ROOT, "list", crate::ObjType::List).unwrap();
        tx.insert(&list, 0, "a").unwrap();
        tx.insert(&list, 1, "b").unwrap();
        tx.insert(&list, 2, "c").unwrap();
        tx.insert(&list, 3, "d").unwrap();
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

        let ranges = dirty_ranges(&doc1);
        let list_obj = doc1.exid_to_obj(&list).unwrap().id;
        let list_range = doc1.ops().scope_to_obj(&list_obj);
        assert!(ranges.iter().all(|range| doc1
            .ops()
            .list_range_is_on_register_boundaries(range, list_range.clone())));
        assert_dirty_diff_matches_full(&doc1, &before, &after);
    }

    fn next_dirty_diff_test_rand(seed: &mut u64) -> usize {
        *seed = seed.wrapping_mul(6364136223846793005).wrapping_add(1);
        (*seed >> 32) as usize
    }

    fn assert_batch_random_list_changes_match_full_diff(mut seed: u64, split_changes: bool) {
        let mut doc1 = Automerge::new().enable_audit_mode().unwrap();
        let mut tx = doc1.transaction();
        let list = tx.put_object(ROOT, "list", crate::ObjType::List).unwrap();
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

        let ranges = dirty_ranges(&doc1);
        let list_obj = doc1.exid_to_obj(&list).unwrap().id;
        let list_range = doc1.ops().scope_to_obj(&list_obj);
        assert!(!ranges.is_empty());
        assert!(ranges.iter().all(|range| doc1
            .ops()
            .list_range_is_on_register_boundaries(range, list_range.clone())));
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
        let mut doc = Automerge::new().enable_audit_mode().unwrap();
        let mut tx = doc.transaction();
        let list = tx.put_object(ROOT, "list", crate::ObjType::List).unwrap();
        tx.insert(&list, 0, "a").unwrap();
        tx.insert(&list, 1, "b").unwrap();
        tx.insert(&list, 2, "c").unwrap();
        tx.commit();

        doc.ops_mut().clear_dirty();
        let before = doc.get_heads();
        let mut tx = doc.transaction();
        tx.delete(&list, 1).unwrap();
        tx.commit();
        let after = doc.get_heads();

        assert_dirty_diff_matches_full(&doc, &before, &after);
    }

    #[test]
    fn dirty_diff_matches_full_diff_for_object_creation_with_child_mutations() {
        let mut doc = Automerge::new().enable_audit_mode().unwrap();
        doc.ops_mut().clear_dirty();
        let before = doc.get_heads();

        let mut tx = doc.transaction();
        let list = tx.put_object(ROOT, "todos", crate::ObjType::List).unwrap();
        tx.insert(&list, 0, "a").unwrap();
        tx.insert(&list, 1, "b").unwrap();
        tx.commit();
        let after = doc.get_heads();

        assert_dirty_diff_matches_full(&doc, &before, &after);
    }

    #[test]
    fn remote_object_creation_with_child_mutations_dirties_parent_and_child_ranges() {
        let mut doc1 = Automerge::new().enable_audit_mode().unwrap();
        let mut doc2 = doc1.fork();

        let mut tx = doc2.transaction();
        let list = tx.put_object(ROOT, "todos", crate::ObjType::List).unwrap();
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
        let mut doc1 = Automerge::new().enable_audit_mode().unwrap();
        let mut tx = doc1.transaction();
        tx.put(ROOT, "a", 1).unwrap();
        tx.put(ROOT, "z", 26).unwrap();
        tx.commit();
        let mut doc2 = doc1.fork();

        let mut tx = doc2.transaction();
        let map = tx.put_object(ROOT, "m", crate::ObjType::Map).unwrap();
        tx.put(&map, "scalar", 10).unwrap();
        let list = tx.put_object(&map, "list", crate::ObjType::List).unwrap();
        tx.insert(&list, 0, "a").unwrap();
        tx.insert(&list, 1, "b").unwrap();
        let text = tx.put_object(&map, "text", crate::ObjType::Text).unwrap();
        tx.splice_text(&text, 0, 0, "hello").unwrap();
        tx.commit();
        let changes = doc2.get_changes(&doc1.get_heads()).unwrap();

        doc1.ops_mut().clear_dirty();
        let before = doc1.get_heads();
        doc1.apply_changes_batch(changes).unwrap();
        let after = doc1.get_heads();

        let ranges = dirty_ranges(&doc1);
        let parent_range = doc1.ops().prop_range(&ObjId::root(), "m");
        let map_range = doc1.ops().scope_to_obj(&doc1.exid_to_obj(&map).unwrap().id);
        let list_range = doc1
            .ops()
            .scope_to_obj(&doc1.exid_to_obj(&list).unwrap().id);
        let text_range = doc1
            .ops()
            .scope_to_obj(&doc1.exid_to_obj(&text).unwrap().id);
        assert!(ranges_contain(&ranges, parent_range));
        assert!(ranges_contain(&ranges, map_range));
        assert!(ranges_contain(&ranges, list_range));
        assert!(ranges_contain(&ranges, text_range));
        assert_dirty_diff_matches_full(&doc1, &before, &after);
    }

    #[test]
    fn dirty_diff_matches_full_diff_for_child_mutation_followed_by_parent_delete() {
        let mut doc = Automerge::new().enable_audit_mode().unwrap();
        let mut tx = doc.transaction();
        let map = tx.put_object(ROOT, "map", crate::ObjType::Map).unwrap();
        tx.put(&map, "key", 1).unwrap();
        tx.commit();

        doc.ops_mut().clear_dirty();
        let before = doc.get_heads();
        let mut tx = doc.transaction();
        tx.put(&map, "key", 2).unwrap();
        tx.delete(ROOT, "map").unwrap();
        tx.commit();
        let after = doc.get_heads();

        assert_dirty_diff_matches_full(&doc, &before, &after);
    }

    #[test]
    fn concurrent_child_mutation_and_parent_delete_matches_full_diff() {
        // pinned time: a random hash could land on a fragment head and free
        // hashes this test needs
        let mut doc1 = Automerge::new().with_actor(ActorId::from([1]));
        let mut tx = doc1.transaction();
        let map = tx.put_object(ROOT, "map", crate::ObjType::Map).unwrap();
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
        let mut doc1 = Automerge::new().enable_audit_mode().unwrap();
        let mut tx = doc1.transaction();
        tx.put(ROOT, "key", 1).unwrap();
        tx.commit();
        let mut doc2 = doc1.fork();

        let mut tx = doc1.transaction();
        tx.put(ROOT, "key", 2).unwrap();
        tx.commit();

        let mut tx = doc2.transaction();
        tx.put(ROOT, "key", 3).unwrap();
        tx.commit();
        let changes = doc2.get_changes(&doc1.get_heads()).unwrap();

        doc1.ops_mut().clear_dirty();
        let before = doc1.get_heads();
        doc1.apply_changes(changes).unwrap();
        let after = doc1.get_heads();

        assert_dirty_diff_matches_full(&doc1, &before, &after);
    }

    /// The map twin of [`remote_list_conflict_dirties_changed_rows`],
    /// run with the batch op winning once and losing once.
    #[test]
    fn remote_map_conflict_dirties_changed_rows() {
        for local in ["0b", "fb"] {
            let mut doc1 = Automerge::new();
            doc1.set_actor(local.try_into().unwrap());
            let mut doc1 = doc1.enable_audit_mode().unwrap();
            let mut tx = doc1.transaction();
            tx.put(ROOT, "key", 1).unwrap();
            tx.commit();
            let mut doc2 = doc1.fork();
            doc2.set_actor("aa".try_into().unwrap());

            let mut tx = doc1.transaction();
            tx.put(ROOT, "key", 2).unwrap();
            tx.commit();

            let mut tx = doc2.transaction();
            tx.put(ROOT, "key", 3).unwrap();
            tx.commit();
            let changes = doc2.get_changes(&doc1.get_heads()).unwrap();

            doc1.ops_mut().clear_dirty();
            let before = doc1.get_heads();
            doc1.apply_changes(changes).unwrap();
            let after = doc1.get_heads();

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
            assert_dirty_diff_matches_full(&doc1, &before, &after);
        }
    }

    #[test]
    fn batched_remote_map_conflict_dirties_whole_new_key_register() {
        let mut doc1 = Automerge::new().enable_audit_mode().unwrap();
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
        let mut doc1 = Automerge::new().enable_audit_mode().unwrap();
        let mut tx = doc1.transaction();
        let list = tx.put_object(ROOT, "list", crate::ObjType::List).unwrap();
        tx.insert(&list, 0, "a").unwrap();
        tx.commit();
        let mut doc2 = doc1.fork();

        let mut tx = doc1.transaction();
        tx.put(&list, 0, "A").unwrap();
        tx.commit();

        let mut tx = doc2.transaction();
        tx.put(&list, 0, "B").unwrap();
        tx.commit();
        let changes = doc2.get_changes(&doc1.get_heads()).unwrap();

        doc1.ops_mut().clear_dirty();
        let before = doc1.get_heads();
        doc1.apply_changes(changes).unwrap();
        let after = doc1.get_heads();

        assert_dirty_diff_matches_full(&doc1, &before, &after);
    }

    /// The local actor sorts on each side of the remote, so the batch op
    /// wins once and loses once; either way only the changed rows are dirty.
    #[test]
    fn remote_list_conflict_dirties_changed_rows() {
        for local in ["0b", "fb"] {
            let mut doc1 = Automerge::new();
            doc1.set_actor(local.try_into().unwrap());
            let mut doc1 = doc1.enable_audit_mode().unwrap();
            let mut tx = doc1.transaction();
            let list = tx.put_object(ROOT, "list", crate::ObjType::List).unwrap();
            tx.insert(&list, 0, "a").unwrap();
            tx.commit();
            let mut doc2 = doc1.fork();
            doc2.set_actor("aa".try_into().unwrap());

            let mut tx = doc1.transaction();
            tx.put(&list, 0, "A").unwrap();
            tx.commit();

            let mut tx = doc2.transaction();
            tx.put(&list, 0, "B").unwrap();
            tx.commit();
            let changes = doc2.get_changes(&doc1.get_heads()).unwrap();

            doc1.ops_mut().clear_dirty();
            let before = doc1.get_heads();
            doc1.apply_changes(changes).unwrap();
            let after = doc1.get_heads();

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
            assert_dirty_diff_matches_full(&doc1, &before, &after);
        }
    }

    #[test]
    fn remote_insert_then_update_same_list_element_dirties_new_register() {
        let mut doc1 = Automerge::new().enable_audit_mode().unwrap();
        let mut tx = doc1.transaction();
        let list = tx.put_object(ROOT, "list", crate::ObjType::List).unwrap();
        tx.commit();
        let mut doc2 = doc1.fork();

        let mut tx = doc2.transaction();
        tx.insert(&list, 0, "a").unwrap();
        tx.put(&list, 0, "A").unwrap();
        tx.commit();
        let changes = doc2.get_changes(&doc1.get_heads()).unwrap();

        doc1.ops_mut().clear_dirty();
        let before = doc1.get_heads();
        doc1.apply_changes(changes).unwrap();
        let after = doc1.get_heads();

        let list_obj = doc1.exid_to_obj(&list).unwrap().id;
        let list_range = doc1.ops().scope_to_obj(&list_obj);
        let ranges = dirty_ranges(&doc1);
        assert_eq!(ranges, vec![list_range.clone()]);
        assert!(doc1
            .ops()
            .list_range_is_on_register_boundaries(&ranges[0], list_range));
        assert_dirty_diff_matches_full(&doc1, &before, &after);
    }

    #[test]
    fn dirty_diff_matches_full_diff_for_remote_map_conflict_resolution_exposes_value() {
        // pinned time: a random hash could land on a fragment head and free
        // hashes this test needs
        let mut doc1 = Automerge::new().with_actor(ActorId::from([1]));
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
        let list = tx.put_object(ROOT, "list", crate::ObjType::List).unwrap();
        tx.insert(&list, 0, "base").unwrap();
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
        let mut doc1 = Automerge::new().enable_audit_mode().unwrap();
        let mut tx = doc1.transaction();
        tx.put(ROOT, "counter", ScalarValue::counter(1)).unwrap();
        tx.commit();
        let mut doc2 = doc1.fork();

        let mut tx = doc2.transaction();
        tx.increment(ROOT, "counter", 2).unwrap();
        tx.commit();
        let changes = doc2.get_changes(&doc1.get_heads()).unwrap();

        doc1.ops_mut().clear_dirty();
        let before = doc1.get_heads();
        doc1.apply_changes(changes).unwrap();
        let after = doc1.get_heads();

        assert_dirty_diff_matches_full(&doc1, &before, &after);
    }

    #[test]
    fn dirty_diff_matches_full_diff_for_remote_text_insert() {
        let mut doc1 = Automerge::new().enable_audit_mode().unwrap();
        let mut tx = doc1.transaction();
        let text = tx.put_object(ROOT, "text", crate::ObjType::Text).unwrap();
        tx.splice_text(&text, 0, 0, "abc").unwrap();
        tx.commit();
        let mut doc2 = doc1.fork();

        let mut tx = doc2.transaction();
        tx.splice_text(&text, 1, 0, "X").unwrap();
        tx.commit();
        let changes = doc2.get_changes(&doc1.get_heads()).unwrap();

        doc1.ops_mut().clear_dirty();
        let before = doc1.get_heads();
        doc1.apply_changes(changes).unwrap();
        let after = doc1.get_heads();

        assert_dirty_diff_matches_full(&doc1, &before, &after);
    }

    #[test]
    fn dirty_diff_matches_full_diff_for_remote_mark() {
        let mut doc1 = Automerge::new().enable_audit_mode().unwrap();
        let mut tx = doc1.transaction();
        let text = tx.put_object(ROOT, "text", crate::ObjType::Text).unwrap();
        tx.splice_text(&text, 0, 0, "abc").unwrap();
        tx.commit();
        let mut doc2 = doc1.fork();

        let mut tx = doc2.transaction();
        tx.mark(
            &text,
            Mark::new("bold".to_string(), true, 0, 3),
            ExpandMark::Both,
        )
        .unwrap();
        tx.commit();
        let changes = doc2.get_changes(&doc1.get_heads()).unwrap();

        doc1.ops_mut().clear_dirty();
        let before = doc1.get_heads();
        doc1.apply_changes(changes).unwrap();
        let after = doc1.get_heads();

        assert_dirty_diff_matches_full(&doc1, &before, &after);
    }

    #[test]
    fn dirty_diff_matches_full_diff_for_remote_middle_mark() {
        let mut doc1 = Automerge::new().enable_audit_mode().unwrap();
        let mut tx = doc1.transaction();
        let text = tx.put_object(ROOT, "text", crate::ObjType::Text).unwrap();
        tx.splice_text(&text, 0, 0, "abcdef").unwrap();
        tx.commit();
        let mut doc2 = doc1.fork();

        let mut tx = doc2.transaction();
        tx.mark(
            &text,
            Mark::new("bold".to_string(), true, 2, 4),
            ExpandMark::Both,
        )
        .unwrap();
        tx.commit();
        let changes = doc2.get_changes(&doc1.get_heads()).unwrap();

        doc1.ops_mut().clear_dirty();
        let before = doc1.get_heads();
        doc1.apply_changes(changes).unwrap();
        let after = doc1.get_heads();
        // only the mark rows are dirty; the diff must widen to the marked span
        assert!(doc1.ops().dirty_runs().next().is_some());

        assert_dirty_diff_matches_full(&doc1, &before, &after);
    }

    #[test]
    fn dirty_diff_matches_full_diff_for_text_insert_without_marks() {
        let mut doc = Automerge::new().enable_audit_mode().unwrap();
        let mut tx = doc.transaction();
        let text = tx.put_object(ROOT, "text", crate::ObjType::Text).unwrap();
        tx.splice_text(&text, 0, 0, "abc").unwrap();
        tx.commit();

        doc.ops_mut().clear_dirty();
        let before = doc.get_heads();
        let mut tx = doc.transaction();
        tx.splice_text(&text, 1, 0, "X").unwrap();
        tx.commit();
        let after = doc.get_heads();

        assert_dirty_diff_matches_full(&doc, &before, &after);
    }

    #[test]
    fn dirty_diff_matches_full_diff_for_text_delete() {
        let mut doc = Automerge::new().enable_audit_mode().unwrap();
        let mut tx = doc.transaction();
        let text = tx.put_object(ROOT, "text", crate::ObjType::Text).unwrap();
        tx.splice_text(&text, 0, 0, "abc").unwrap();
        tx.commit();

        doc.ops_mut().clear_dirty();
        let before = doc.get_heads();
        let mut tx = doc.transaction();
        tx.splice_text(&text, 1, 1, "").unwrap();
        tx.commit();
        let after = doc.get_heads();

        assert_dirty_diff_matches_full(&doc, &before, &after);
    }

    #[test]
    fn dirty_diff_matches_full_diff_for_text_insert_inside_mark() {
        let mut doc = Automerge::new().enable_audit_mode().unwrap();
        let mut tx = doc.transaction();
        let text = tx.put_object(ROOT, "text", crate::ObjType::Text).unwrap();
        tx.splice_text(&text, 0, 0, "abc").unwrap();
        tx.mark(
            &text,
            Mark::new("bold".to_string(), true, 0, 3),
            ExpandMark::Both,
        )
        .unwrap();
        tx.commit();

        doc.ops_mut().clear_dirty();
        let before = doc.get_heads();
        let mut tx = doc.transaction();
        tx.splice_text(&text, 1, 0, "X").unwrap();
        tx.commit();
        let after = doc.get_heads();

        assert_dirty_diff_matches_full(&doc, &before, &after);
    }

    #[test]
    fn text_insert_at_mark_boundaries_stays_localized() {
        for index in [1, 3] {
            let mut doc = Automerge::new().enable_audit_mode().unwrap();
            let mut tx = doc.transaction();
            let text = tx.put_object(ROOT, "text", crate::ObjType::Text).unwrap();
            tx.splice_text(&text, 0, 0, "abcd").unwrap();
            tx.mark(
                &text,
                Mark::new("bold".to_string(), true, 1, 3),
                ExpandMark::Both,
            )
            .unwrap();
            tx.commit();

            doc.ops_mut().clear_dirty();
            let before = doc.get_heads();
            let mut tx = doc.transaction();
            tx.splice_text(&text, index, 0, "X").unwrap();
            tx.commit();
            let after = doc.get_heads();

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
            assert_dirty_diff_matches_full(&doc, &before, &after);
        }
    }

    fn assert_text_splice_around_mark_matches_full(index: usize, del: isize, value: &str) {
        let mut doc = Automerge::new().enable_audit_mode().unwrap();
        let mut tx = doc.transaction();
        let text = tx.put_object(ROOT, "text", crate::ObjType::Text).unwrap();
        tx.splice_text(&text, 0, 0, "abcdef").unwrap();
        tx.mark(
            &text,
            Mark::new("bold".to_string(), true, 2, 4),
            ExpandMark::Both,
        )
        .unwrap();
        tx.commit();

        doc.ops_mut().clear_dirty();
        let before = doc.get_heads();
        let mut tx = doc.transaction();
        tx.splice_text(&text, index, del, value).unwrap();
        tx.commit();
        let after = doc.get_heads();

        assert_dirty_diff_matches_full(&doc, &before, &after);
    }

    #[test]
    fn text_deletion_around_mark_anchors_matches_full_diff() {
        for (index, del) in [
            (1, 1), // immediately before mark begin
            (2, 1), // at mark begin
            (3, 1), // inside marked span
            (4, 1), // immediately after mark end
            (1, 4), // through both mark anchors
        ] {
            assert_text_splice_around_mark_matches_full(index, del, "");
        }
    }

    #[test]
    fn text_replacement_around_mark_anchors_matches_full_diff() {
        for (index, del, value) in [
            (2, 1, "X"),  // replace at mark begin
            (3, 1, "X"),  // replace inside marked span
            (2, 2, "XY"), // replace whole marked span
            (1, 4, "XY"), // replace across both mark anchors
        ] {
            assert_text_splice_around_mark_matches_full(index, del, value);
        }
    }

    fn assert_text_splice_around_nested_marks_matches_full(index: usize, del: isize, value: &str) {
        let mut doc = Automerge::new().enable_audit_mode().unwrap();
        let mut tx = doc.transaction();
        let text = tx.put_object(ROOT, "text", crate::ObjType::Text).unwrap();
        tx.splice_text(&text, 0, 0, "abcdefghij").unwrap();
        tx.mark(
            &text,
            Mark::new("bold".to_string(), true, 2, 8),
            ExpandMark::Both,
        )
        .unwrap();
        tx.mark(
            &text,
            Mark::new("italic".to_string(), true, 4, 6),
            ExpandMark::Both,
        )
        .unwrap();
        tx.mark(
            &text,
            Mark::new("color".to_string(), "red", 5, 9),
            ExpandMark::Both,
        )
        .unwrap();
        tx.commit();

        doc.ops_mut().clear_dirty();
        let before = doc.get_heads();
        let mut tx = doc.transaction();
        tx.splice_text(&text, index, del, value).unwrap();
        tx.commit();
        let after = doc.get_heads();

        assert_dirty_diff_matches_full(&doc, &before, &after);
    }

    #[test]
    fn text_edits_around_nested_overlapping_mark_boundaries_match_full_diff() {
        for (index, del, value) in [
            (4, 0, "X"),  // at nested mark begin
            (5, 1, ""),   // at overlapping mark begin
            (6, 2, "XY"), // through nested mark end
            (8, 1, ""),   // at outer mark end
        ] {
            assert_text_splice_around_nested_marks_matches_full(index, del, value);
        }
    }

    fn assert_remote_text_splice_around_mark_matches_full(index: usize, del: isize, value: &str) {
        let mut doc1 = Automerge::new().enable_audit_mode().unwrap();
        let mut tx = doc1.transaction();
        let text = tx.put_object(ROOT, "text", crate::ObjType::Text).unwrap();
        tx.splice_text(&text, 0, 0, "abcdef").unwrap();
        tx.mark(
            &text,
            Mark::new("bold".to_string(), true, 2, 4),
            ExpandMark::Both,
        )
        .unwrap();
        tx.commit();
        let mut doc2 = doc1.fork();

        let mut tx = doc2.transaction();
        tx.splice_text(&text, index, del, value).unwrap();
        tx.commit();
        let changes = doc2.get_changes(&doc1.get_heads()).unwrap();

        doc1.ops_mut().clear_dirty();
        let before = doc1.get_heads();
        doc1.apply_changes(changes).unwrap();
        let after = doc1.get_heads();

        assert_dirty_diff_matches_full(&doc1, &before, &after);
    }

    #[test]
    fn remote_text_deletion_around_mark_anchors_matches_full_diff() {
        for (index, del) in [
            (1, 1), // immediately before mark begin
            (2, 1), // at mark begin
            (3, 1), // inside marked span
            (4, 1), // immediately after mark end
            (1, 4), // through both mark anchors
        ] {
            assert_remote_text_splice_around_mark_matches_full(index, del, "");
        }
    }

    #[test]
    fn remote_text_replacement_around_mark_anchors_matches_full_diff() {
        for (index, del, value) in [
            (2, 1, "X"),  // replace at mark begin
            (3, 1, "X"),  // replace inside marked span
            (2, 2, "XY"), // replace whole marked span
            (1, 4, "XY"), // replace across both mark anchors
        ] {
            assert_remote_text_splice_around_mark_matches_full(index, del, value);
        }
    }

    fn assert_batch_text_splice_around_mark_matches_full(index: usize, del: isize, value: &str) {
        let mut doc1 = Automerge::new().enable_audit_mode().unwrap();
        let mut tx = doc1.transaction();
        let text = tx.put_object(ROOT, "text", crate::ObjType::Text).unwrap();
        tx.splice_text(&text, 0, 0, "abcdef").unwrap();
        tx.mark(
            &text,
            Mark::new("bold".to_string(), true, 2, 4),
            ExpandMark::Both,
        )
        .unwrap();
        tx.commit();
        let mut doc2 = doc1.fork();

        let mut tx = doc2.transaction();
        tx.splice_text(&text, index, del, value).unwrap();
        tx.commit();
        let changes = doc2.get_changes(&doc1.get_heads()).unwrap();

        doc1.ops_mut().clear_dirty();
        let before = doc1.get_heads();
        doc1.apply_changes_batch(changes).unwrap();
        let after = doc1.get_heads();

        assert_dirty_diff_matches_full(&doc1, &before, &after);
    }

    #[test]
    fn batch_text_deletion_around_mark_anchors_matches_full_diff() {
        for (index, del) in [
            (1, 1), // immediately before mark begin
            (2, 1), // at mark begin
            (3, 1), // inside marked span
            (4, 1), // immediately after mark end
            (1, 4), // through both mark anchors
        ] {
            assert_batch_text_splice_around_mark_matches_full(index, del, "");
        }
    }

    #[test]
    fn batch_text_replacement_around_mark_anchors_matches_full_diff() {
        for (index, del, value) in [
            (2, 1, "X"),  // replace at mark begin
            (3, 1, "X"),  // replace inside marked span
            (2, 2, "XY"), // replace whole marked span
            (1, 4, "XY"), // replace across both mark anchors
        ] {
            assert_batch_text_splice_around_mark_matches_full(index, del, value);
        }
    }

    fn assert_batch_text_splice_around_nested_marks_matches_full(
        index: usize,
        del: isize,
        value: &str,
    ) {
        let mut doc1 = Automerge::new().enable_audit_mode().unwrap();
        let mut tx = doc1.transaction();
        let text = tx.put_object(ROOT, "text", crate::ObjType::Text).unwrap();
        tx.splice_text(&text, 0, 0, "abcdefghij").unwrap();
        tx.mark(
            &text,
            Mark::new("bold".to_string(), true, 2, 8),
            ExpandMark::Both,
        )
        .unwrap();
        tx.mark(
            &text,
            Mark::new("italic".to_string(), true, 4, 6),
            ExpandMark::Both,
        )
        .unwrap();
        tx.mark(
            &text,
            Mark::new("color".to_string(), "red", 5, 9),
            ExpandMark::Both,
        )
        .unwrap();
        tx.commit();
        let mut doc2 = doc1.fork();

        let mut tx = doc2.transaction();
        tx.splice_text(&text, index, del, value).unwrap();
        tx.commit();
        let changes = doc2.get_changes(&doc1.get_heads()).unwrap();

        doc1.ops_mut().clear_dirty();
        let before = doc1.get_heads();
        doc1.apply_changes_batch(changes).unwrap();
        let after = doc1.get_heads();

        assert_dirty_diff_matches_full(&doc1, &before, &after);
    }

    #[test]
    fn batch_text_edits_around_nested_overlapping_mark_boundaries_match_full_diff() {
        for (index, del, value) in [
            (4, 0, "X"),  // at nested mark begin
            (5, 1, ""),   // at overlapping mark begin
            (6, 2, "XY"), // through nested mark end
            (8, 1, ""),   // at outer mark end
        ] {
            assert_batch_text_splice_around_nested_marks_matches_full(index, del, value);
        }
    }

    #[test]
    fn batch_text_edit_plus_mark_in_same_change_matches_full_diff() {
        let mut doc1 = Automerge::new().enable_audit_mode().unwrap();
        let mut tx = doc1.transaction();
        let text = tx.put_object(ROOT, "text", crate::ObjType::Text).unwrap();
        tx.splice_text(&text, 0, 0, "abcdef").unwrap();
        tx.mark(
            &text,
            Mark::new("bold".to_string(), true, 2, 4),
            ExpandMark::Both,
        )
        .unwrap();
        tx.commit();
        let mut doc2 = doc1.fork();

        let mut tx = doc2.transaction();
        tx.splice_text(&text, 2, 1, "X").unwrap();
        tx.mark(
            &text,
            Mark::new("italic".to_string(), true, 1, 5),
            ExpandMark::Both,
        )
        .unwrap();
        tx.commit();
        let changes = doc2.get_changes(&doc1.get_heads()).unwrap();

        doc1.ops_mut().clear_dirty();
        let before = doc1.get_heads();
        doc1.apply_changes_batch(changes).unwrap();
        let after = doc1.get_heads();
        // only the touched rows are dirty; the diff must widen to the marked span
        assert!(doc1.ops().dirty_runs().next().is_some());

        assert_dirty_diff_matches_full(&doc1, &before, &after);
    }

    #[test]
    fn batch_multiple_text_edits_around_same_mark_match_full_diff() {
        let mut doc1 = Automerge::new().enable_audit_mode().unwrap();
        let mut tx = doc1.transaction();
        let text = tx.put_object(ROOT, "text", crate::ObjType::Text).unwrap();
        tx.splice_text(&text, 0, 0, "abcdef").unwrap();
        tx.mark(
            &text,
            Mark::new("bold".to_string(), true, 2, 4),
            ExpandMark::Both,
        )
        .unwrap();
        tx.commit();
        let mut doc2 = doc1.fork();

        let mut tx = doc2.transaction();
        tx.splice_text(&text, 4, 1, "Y").unwrap();
        tx.splice_text(&text, 2, 1, "X").unwrap();
        tx.commit();
        let changes = doc2.get_changes(&doc1.get_heads()).unwrap();

        doc1.ops_mut().clear_dirty();
        let before = doc1.get_heads();
        doc1.apply_changes_batch(changes).unwrap();
        let after = doc1.get_heads();

        assert_dirty_diff_matches_full(&doc1, &before, &after);
    }

    fn assert_sync_text_splice_around_mark_matches_full(index: usize, del: isize, value: &str) {
        let mut doc1 = Automerge::new().enable_audit_mode().unwrap();
        let mut tx = doc1.transaction();
        let text = tx.put_object(ROOT, "text", crate::ObjType::Text).unwrap();
        tx.splice_text(&text, 0, 0, "abcdef").unwrap();
        tx.mark(
            &text,
            Mark::new("bold".to_string(), true, 2, 4),
            ExpandMark::Both,
        )
        .unwrap();
        tx.commit();
        let mut doc2 = doc1.fork();

        let mut tx = doc2.transaction();
        tx.splice_text(&text, index, del, value).unwrap();
        tx.commit();

        // what receiving a v2 sync message does
        doc1.ops_mut().clear_dirty();
        let before = doc1.get_heads();
        doc1.load_incremental(&doc2.save()).unwrap();
        let after = doc1.get_heads();

        assert_dirty_diff_matches_full(&doc1, &before, &after);
    }

    #[test]
    fn sync_text_edits_around_mark_anchors_match_full_diff() {
        for (index, del, value) in [
            (2, 1, ""),   // delete at mark begin
            (1, 4, ""),   // delete through both mark anchors
            (2, 2, "XY"), // replace whole marked span
            (1, 4, "XY"), // replace across both mark anchors
        ] {
            assert_sync_text_splice_around_mark_matches_full(index, del, value);
        }
    }

    fn assert_sync_text_splice_around_nested_marks_matches_full(
        index: usize,
        del: isize,
        value: &str,
    ) {
        let mut doc1 = Automerge::new().enable_audit_mode().unwrap();
        let mut tx = doc1.transaction();
        let text = tx.put_object(ROOT, "text", crate::ObjType::Text).unwrap();
        tx.splice_text(&text, 0, 0, "abcdefghij").unwrap();
        tx.mark(
            &text,
            Mark::new("bold".to_string(), true, 2, 8),
            ExpandMark::Both,
        )
        .unwrap();
        tx.mark(
            &text,
            Mark::new("italic".to_string(), true, 4, 6),
            ExpandMark::Both,
        )
        .unwrap();
        tx.mark(
            &text,
            Mark::new("color".to_string(), "red", 5, 9),
            ExpandMark::Both,
        )
        .unwrap();
        tx.commit();
        let mut doc2 = doc1.fork();

        let mut tx = doc2.transaction();
        tx.splice_text(&text, index, del, value).unwrap();
        tx.commit();

        // what receiving a v2 sync message does
        doc1.ops_mut().clear_dirty();
        let before = doc1.get_heads();
        doc1.load_incremental(&doc2.save()).unwrap();
        let after = doc1.get_heads();

        assert_dirty_diff_matches_full(&doc1, &before, &after);
    }

    #[test]
    fn sync_text_edits_around_nested_overlapping_mark_boundaries_match_full_diff() {
        for (index, del, value) in [
            (4, 0, "X"),  // at nested mark begin
            (5, 1, ""),   // at overlapping mark begin
            (6, 2, "XY"), // through nested mark end
            (8, 1, ""),   // at outer mark end
        ] {
            assert_sync_text_splice_around_nested_marks_matches_full(index, del, value);
        }
    }

    #[test]
    fn dirty_diff_matches_full_diff_for_mark() {
        let mut doc = Automerge::new().enable_audit_mode().unwrap();
        let mut tx = doc.transaction();
        let text = tx.put_object(ROOT, "text", crate::ObjType::Text).unwrap();
        tx.splice_text(&text, 0, 0, "abc").unwrap();
        tx.commit();

        doc.ops_mut().clear_dirty();
        let before = doc.get_heads();
        let mut tx = doc.transaction();
        tx.mark(
            &text,
            Mark::new("bold".to_string(), true, 0, 3),
            ExpandMark::Both,
        )
        .unwrap();
        tx.commit();
        let after = doc.get_heads();

        assert_dirty_diff_matches_full(&doc, &before, &after);
    }

    /// The dirty diff relies on this to pair a mark's begin and end; a
    /// half-dirty mark would silently drop its patch.
    #[test]
    fn mark_dirties_both_of_its_ops() {
        let mut doc = Automerge::new().enable_audit_mode().unwrap();
        let mut tx = doc.transaction();
        let text = tx.put_object(ROOT, "text", crate::ObjType::Text).unwrap();
        tx.splice_text(&text, 0, 0, "abc").unwrap();
        tx.commit();
        let before = doc.get_heads();
        doc.ops_mut().clear_dirty();

        let mut tx = doc.transaction();
        tx.mark(
            &text,
            Mark::new("bold".to_string(), true, 1, 2),
            ExpandMark::Both,
        )
        .unwrap();
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
        doc.dirty_diff_patches_and_clear(&before, &after).unwrap();
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
    fn failed_change_set_apply_changes_nothing() {
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
        let head0 = changes[0].hash();
        let whole = crate::change_graph::Fragment {
            head,
            level: head.fragment_level(),
            boundary: vec![],
            checkpoints: vec![],
            members: vec![changes[1].id(), changes[0].id()],
        };
        let _ = head0;

        for (what, frag) in [("missing boundary", orphan), ("whole", whole)] {
            let change_set = src.doc.change_set_for_fragment(&frag).unwrap();

            let mut dst = AutoCommit::new().with_actor(actor(1));
            dst.put(ROOT, "seed", 0).unwrap();
            dst.commit();
            let before_save = dst.save();
            let before_heads = dst.get_heads();
            let before_actors = dst.doc.ops.actors.clone();
            let before_ops = dst.doc.ops.len();

            let r = dst.doc.apply_change_set(change_set);
            if what == "whole" {
                assert!(r.is_ok());
                continue;
            }
            assert!(matches!(r, Err(AutomergeError::MissingDeps)), "{what}");

            assert_eq!(dst.doc.ops.actors, before_actors, "{what}: actor table");
            assert_eq!(dst.doc.ops.len(), before_ops, "{what}: op count");
            assert_eq!(dst.get_heads(), before_heads, "{what}: heads");
            assert_eq!(dst.save(), before_save, "{what}: saved bytes");
            assert_clean(&dst.doc, what);
        }
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
        let change_set = doc.change_set_document_with(NameHashes::All).unwrap();
        assert_eq!(
            change_set.actors().len(),
            clean,
            "a change_set names only the actors its ops use"
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
        let before_save = dst.save();
        let before_heads = dst.get_heads();
        let before_actors = dst.doc.ops.actors.clone();
        let before_ops = dst.doc.ops.len();

        assert!(dst
            .doc
            .apply_changes(vec![good[0].clone(), broken])
            .is_err());

        assert_eq!(dst.doc.ops.actors, before_actors, "actor table");
        assert_eq!(dst.doc.ops.len(), before_ops, "op count");
        assert_eq!(dst.get_heads(), before_heads, "heads");
        assert_eq!(dst.save(), before_save, "saved bytes");
        assert_clean(&dst.doc, "after a rejected batch");
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

    /// Otherwise the receiver must fall back to
    /// [`Automerge::rebuild_missing_hashes`].
    #[test]
    fn change_sets_name_every_retained_hash() {
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

        let cs = doc.change_set_document_with(NameHashes::All).unwrap();
        let named: BTreeSet<ChangeHash> = cs
            .heads()
            .chain(cs.checkpoints.iter().map(|(_, h)| *h))
            .chain(cs.retained.iter().map(|(_, h)| *h))
            .collect();

        let nodes = doc.change_graph.all_nodes();
        let expected = doc.change_graph.hashes_to_retain(&nodes, NameHashes::All);
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
