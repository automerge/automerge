use std::cmp::Ordering;
use std::collections::{BTreeSet, HashMap, HashSet};
use std::env;
use std::fmt::Debug;
use std::num::NonZeroU64;
use std::ops::RangeBounds;

use itertools::Itertools;

pub(crate) use crate::op_set2::change::ChangeCollector;
pub(crate) use crate::op_set2::types::ScalarValue;
pub(crate) use crate::op_set2::{
    ChangeMetadata, KeyRef, OpQuery, OpQueryTerm, OpSet, OpType, Parents,
};
pub(crate) use crate::read::ReadDoc;

use crate::change_graph::{ChangeGraph, FragmentDep, FragmentMember};
use crate::change_queue::ChangeQueue;
use crate::cursor::{CursorPosition, MoveCursor, OpCursor};
use crate::exid::ExId;
use crate::iter::{DiffIter, DocIter, Keys, ListRange, MapRange, Spans, Values};
use crate::marks::{Mark, MarkAccumulator, MarkSet};
use crate::op_set2::change::fragment::FragmentApply;
use crate::patches::{Patch, PatchAccumulator};
use crate::storage::document::ReconstructError;
use crate::storage::{
    self, change, load, Bundle, BundleV2, CompressConfig, Document, VerificationMode,
};
use crate::transaction::{
    self, CommitOptions, Failure, OwnedTransaction, Success, Transactable, Transaction,
    TransactionArgs,
};

use crate::clock::{Clock, ClockRange};
use crate::hydrate;
use crate::types::{ActorId, ChangeHash, ObjId, ObjMeta, OpId, SequenceType, TextEncoding, Value};
use crate::{AutomergeError, Change, ChangeId, Cursor, Fragment, ObjType, Prop};
use std::borrow::Cow;

pub(crate) mod current_state;
mod dirty_diff;

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
/// In [`AuditMode::Disabled`] (the default) the change graph retains
/// only the hashes it needs — the heads, loose commits, fragment heads
/// and checkpoints, and their deps — and frees the rest as fragments
/// cover them. Everything id-based works (reads, transactions, forks,
/// diffs, fragment/bundle exchange); operations that need arbitrary
/// interior hashes (the hash-based sync protocol, exporting the full
/// change history) return [`AutomergeError::AuditModeRequired`].
///
/// In [`AuditMode::Enabled`] every change hash is computed, verified
/// and kept. Loading in audit mode does a full hash-graph rebuild, and
/// fragments apply by converting to changes so every hash is verified.
///
/// Switch modes at runtime with [`Automerge::enable_audit_mode`] and
/// [`Automerge::disable_audit_mode`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum AuditMode {
    /// Every change hash is computed, verified and kept.
    Enabled,
    /// Only the retained hash set is kept. The default.
    #[default]
    Disabled,
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
    audit: AuditMode,
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

    /// The [`AuditMode`] to load the document in.
    ///
    /// The default, [`AuditMode::Disabled`], is the fast path: when the
    /// document carries its stored hash columns the load skips the
    /// hash-graph rebuild entirely and imports the retained set. (A
    /// document without stored hash columns computes hashes once during
    /// load and then retains only the set.)
    ///
    /// [`AuditMode::Enabled`] does a full hash-graph rebuild, verifying
    /// every change hash and the document's recorded heads, and keeps
    /// them all.
    pub fn audit(self, audit: AuditMode) -> Self {
        Self { audit, ..self }
    }

    /// Load in [`AuditMode::Enabled`] — see [`Self::audit`].
    pub fn with_audit_mode(self) -> Self {
        self.audit(AuditMode::Enabled)
    }
}

impl std::default::Default for LoadOptions {
    fn default() -> Self {
        Self {
            on_partial_load: OnPartialLoad::Error,
            verification_mode: VerificationMode::Check,
            string_migration: StringMigration::NoMigration,
            text_encoding: TextEncoding::platform_default(),
            audit: AuditMode::default(),
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
/// This type implements [`crate::sync::SyncDoc`]
///
#[derive(Debug, Clone)]
pub struct Automerge {
    /// The list of unapplied changes that are not causally ready.
    pub(crate) queue: ChangeQueue,
    /// Graph of changes
    pub(crate) change_graph: ChangeGraph,
    /// Current dependencies of this document (heads hashes).
    deps: HashSet<ChangeHash>,
    /// The set of operations that form this document.
    pub(crate) ops: OpSet,
    /// The current actor.
    actor: Actor,
    /// Cursor for dirty-bit incremental diffs.
    diff_cursor: Vec<ChangeId>,
}

impl Automerge {
    /// Create a new document with a random actor id.
    pub fn new() -> Self {
        Automerge {
            queue: ChangeQueue::new(),
            change_graph: ChangeGraph::new(0),
            ops: OpSet::new(TextEncoding::platform_default()),
            deps: Default::default(),
            actor: Actor::Unused(ActorId::random()),
            diff_cursor: Vec::new(),
        }
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

    pub fn new_with_encoding(encoding: TextEncoding) -> Self {
        Automerge {
            queue: ChangeQueue::new(),
            change_graph: ChangeGraph::new(0),
            ops: OpSet::new(encoding),
            deps: Default::default(),
            actor: Actor::Unused(ActorId::random()),
            diff_cursor: Vec::new(),
        }
    }

    pub(crate) fn from_parts(ops: OpSet, change_graph: ChangeGraph) -> Self {
        let deps = change_graph.heads().collect();
        let mut doc = Automerge {
            queue: ChangeQueue::new(),
            change_graph,
            ops,
            deps,
            actor: Actor::Unused(ActorId::random()),
            diff_cursor: Vec::new(),
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

    pub(crate) fn changes(&self) -> &ChangeGraph {
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
    ///
    /// Returns [`AutomergeError::AuditModeRequired`] if the actor has made
    /// changes to this document, the hash of its latest change is unknown
    /// (because the hash graph has not been built) and that change is not one
    /// of the current heads — committing as this actor would require the
    /// missing hash.
    pub fn with_actor(mut self, actor: ActorId) -> Result<Self, AutomergeError> {
        self.set_actor(actor)?;
        Ok(self)
    }

    /// Set the actor id for this document.
    ///
    /// See [`Self::with_actor`] for the error contract.
    pub fn set_actor(&mut self, actor: ActorId) -> Result<&mut Self, AutomergeError> {
        match self.ops.actors.binary_search(&actor) {
            Ok(idx) => {
                self.check_actor_tip_hash(idx)?;
                self.actor = Actor::Cached(idx)
            }
            Err(_) => self.actor = Actor::Unused(actor),
        }
        Ok(self)
    }

    /// Committing as an actor with prior history needs the hash of the
    /// actor's latest change (to record the sequential dependency). Refuse
    /// actors for which that hash is missing.
    fn check_actor_tip_hash(&self, actor_idx: usize) -> Result<(), AutomergeError> {
        let seq = self.change_graph.seq_for_actor(actor_idx);
        if seq == 0 {
            return Ok(());
        }
        let id = self.change_id_at(actor_idx, seq);
        self.change_graph
            .get_hash_for_change_id(&id, &self.ops.actors)?;
        Ok(())
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
        let args = self.transaction_args(None);
        Transaction::new(self, args)
    }

    /// Start a transaction isolated at the given heads.
    pub fn transaction_at(
        &mut self,
        heads: &[ChangeId],
    ) -> Result<Transaction<'_>, AutomergeError> {
        // fail fast: an isolated transaction commits with these heads as
        // its deps, which the wire format records as hashes
        self.resolve_heads(heads)?;
        let args = self.transaction_args(Some(heads));
        Ok(Transaction::new(self, args))
    }

    /// Start a transaction that owns the document, consuming `self`.
    pub fn into_transaction(
        self,
        heads: Option<&[ChangeId]>,
    ) -> Result<OwnedTransaction, AutomergeError> {
        OwnedTransaction::new(self, heads)
    }

    pub(crate) fn transaction_args(&mut self, heads: Option<&[ChangeId]>) -> TransactionArgs {
        let actor_index;
        let seq;
        let mut deps;
        let scope;
        match heads {
            Some(heads) => {
                // the isolation heads become the change's deps, which the
                // wire format records as hashes; callers validated
                // resolvability when the isolation was created
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
                    // set_actor refuses actors whose latest change hash is
                    // missing, so the hash is always available here
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

        TransactionArgs {
            actor_index,
            seq,
            start_op,
            deps,
            scope,
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
        // the change was just added, so it always resolves
        self.hash_to_change_id(&hash)
            .expect("hash of a newly created change is always known")
            .expect("newly created change must be in the document")
    }

    /// Fork this document at the current point for use by a different actor.
    ///
    /// This will create a new actor ID for the forked document
    pub fn fork(&self) -> Self {
        let mut f = self.clone();
        f.set_actor(ActorId::random())
            .expect("a random actor is always acceptable");
        f
    }

    /// Fork this document at the given heads
    ///
    /// This will create a new actor ID for the forked document
    ///
    /// Unlike the `*_at` query methods (which silently skip unknown hashes),
    /// this returns [`AutomergeError::InvalidHash`] if any of `heads` is not
    /// a change in this document.
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
        let mut f = Self::new_with_encoding(self.text_encoding());
        f.set_actor(ActorId::random())
            .expect("a random actor is always acceptable");
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

    fn load_with_options_and_mark_validation(
        data: &[u8],
        options: LoadOptions,
        mark_order: load::MarkOrderValidation,
    ) -> Result<Self, AutomergeError> {
        if data.is_empty() {
            tracing::trace!("no data, initializing empty document");
            return Ok(Self::new());
        }
        tracing::trace!("loading first chunk");
        let (remaining, first_chunk) = storage::Chunk::parse(storage::parse::Input::new(data))
            .map_err(|e| load::Error::Parse(Box::new(e)))?;
        if !first_chunk.checksum_valid() {
            return Err(load::Error::BadChecksum.into());
        }

        let mut changes = vec![];
        let mut first_chunk_was_doc = false;
        let mut am = match first_chunk {
            storage::Chunk::Document(d) => {
                tracing::trace!("first chunk is document chunk, inflating");
                first_chunk_was_doc = true;
                match d.reconstruct(
                    options.verification_mode,
                    options.text_encoding,
                    options.audit,
                ) {
                    Ok(doc) => doc,
                    Err(ReconstructError::InvalidMarkOrderDoc {
                        doc,
                        error_message: _,
                    }) if mark_order.allows_invalid() => *doc,
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
        tracing::trace!("loading change chunks");
        match load::load_changes(
            remaining.reset(),
            options.text_encoding,
            &am.change_graph,
            mark_order,
        ) {
            load::LoadedChanges::Complete(c) => {
                am.apply_changes(changes.into_iter().chain(c))?;
                // Only allow missing deps if the first chunk was a document chunk
                // See https://github.com/automerge/automerge/pull/599#issuecomment-1549667472
                if !am.queue.is_empty()
                    && !first_chunk_was_doc
                    && options.on_partial_load == OnPartialLoad::Error
                {
                    return Err(AutomergeError::MissingDeps);
                }
            }
            load::LoadedChanges::Partial { error, .. } => {
                if options.on_partial_load == OnPartialLoad::Error {
                    return Err(error.into());
                }
            }
        }
        if let StringMigration::ConvertToText = options.string_migration {
            am.convert_scalar_strings_to_text()?;
        }
        Ok(am)
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
            let mut doc = Self::load_with_options(
                data,
                LoadOptions::new()
                    .text_encoding(self.text_encoding())
                    .on_partial_load(OnPartialLoad::Ignore)
                    .verification_mode(VerificationMode::Check)
                    // replacing self must not change the audit mode
                    .audit(self.audit_mode()),
            )?;
            doc = doc.with_actor(self.actor_id().clone())?;
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
        self.apply_changes(changes)?;
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

    /// Apply changes to this document.
    ///
    /// This is idempotent in the sense that if a change has already been applied it will be
    /// ignored.
    pub fn apply_changes(
        &mut self,
        changes: impl IntoIterator<Item = Change> + Clone,
    ) -> Result<(), AutomergeError> {
        self.apply_changes_batch(changes)
    }

    /// Takes all the changes in `other` which are not in `self` and applies them
    pub fn merge(&mut self, other: &mut Self) -> Result<Vec<ChangeId>, AutomergeError> {
        let changes = self.get_changes_added(other)?;
        tracing::trace!(changes=?changes.iter().map(|c| c.hash()).collect::<Vec<_>>(), "merging new changes");
        self.apply_changes(changes)?;
        Ok(self.get_heads())
    }

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
            for orphaned in self.queue.iter() {
                bytes.extend(orphaned.raw_bytes());
            }
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
    pub fn save_after(&self, heads: &[ChangeId]) -> Result<Vec<u8>, AutomergeError> {
        let changes = self.get_changes(heads)?;
        let mut bytes = vec![];
        for c in changes {
            bytes.extend(c.raw_bytes());
        }
        Ok(bytes)
    }

    /// Hash-keyed version of [`Self::save_after`], for the save cursor
    /// (which stores hashes — the currency of storage).
    pub(crate) fn save_after_hashes(
        &self,
        heads: &[ChangeHash],
    ) -> Result<Vec<u8>, AutomergeError> {
        let clock = self.change_graph.seq_clock_for_heads_lossy(heads);
        let changes = ChangeCollector::exclude_seq_clock(&self.ops, &self.change_graph, clock)?;
        let mut bytes = vec![];
        for c in changes {
            bytes.extend(c.raw_bytes());
        }
        Ok(bytes)
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

    /// Get the last change this actor made to the document.
    pub fn get_last_local_change(&self) -> Result<Option<Change>, AutomergeError> {
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

    /// Clock range for diffing between two head sets, resolved with the
    /// lossy (`*_at`-read) semantics.
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
        // callers validate heads before isolating, so the clock is always
        // computable
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
            )
            .unwrap();
        self.deps = self.change_graph.heads().collect();
    }

    pub(crate) fn update_history(&mut self, change: &Change) {
        self.update_deps(change);

        let actor_index = self
            .ops
            .actors
            .binary_search(change.actor_id())
            .expect("Change's actor not already in the document");

        self.change_graph
            .add_change(change, actor_index)
            .expect("Change's deps should already be in the document");
    }

    fn insert_actor(&mut self, index: usize, actor: ActorId) -> usize {
        self.ops.insert_actor(index, actor);
        self.change_graph.insert_actor(index);
        self.actor.rewrite_with_new_actor(index);
        index
    }
    pub(crate) fn put_actor_ref(&mut self, actor: &ActorId) -> usize {
        match self.ops.actors.binary_search(actor) {
            Ok(idx) => idx,
            Err(idx) => self.insert_actor(idx, actor.clone()),
        }
    }

    /// Insert every actor in `actors` the document lacks, remapping
    /// the op columns ONCE for the whole batch instead of once per
    /// actor. Pure appends (every new actor sorting after the existing
    /// ones) skip the remap entirely.
    pub(crate) fn put_actor_refs(&mut self, actors: &[ActorId]) {
        let mut new: Vec<ActorId> = actors
            .iter()
            .filter(|a| self.ops.actors.binary_search(a).is_err())
            .cloned()
            .collect();
        if new.is_empty() {
            return;
        }
        new.sort_unstable();
        new.dedup();
        // old index -> final index: old actors shift right past the
        // new ones sorting before them
        let mut map: Vec<u32> = Vec::with_capacity(self.ops.actors.len());
        let mut j = 0;
        for a in &self.ops.actors {
            while j < new.len() && new[j] < *a {
                j += 1;
            }
            map.push((map.len() + j) as u32);
        }
        let identity = map.iter().enumerate().all(|(i, &m)| m as usize == i);
        if !identity {
            self.ops.remap_actor_indexes(&map);
        }
        // the cheap per-actor state; the op columns defer their
        // renumbering through the actor map
        let mut amap = self.ops.actor_map();
        for a in new {
            let idx = self.ops.actors.binary_search(&a).unwrap_err();
            amap = amap.insert(idx, self.ops.actors.len());
            self.ops.actors.insert(idx, a);
            self.change_graph.insert_actor(idx);
            self.actor.rewrite_with_new_actor(idx);
        }
        self.ops.set_actor_map(amap);
    }

    pub(crate) fn put_actor(&mut self, actor: ActorId) -> usize {
        match self.ops.actors.binary_search(&actor) {
            Ok(idx) => idx,
            Err(idx) => self.insert_actor(idx, actor),
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
        let after_clock = clock.after_clock();
        let mut patch_accumulator = PatchAccumulator::event_log();
        DiffIter::log(self, ObjMeta::root(), clock, &mut patch_accumulator, true);
        patch_accumulator.heads_clock = after_clock;
        Ok(patch_accumulator.make_patches(self))
    }

    /// Generate an incremental diff from the last incremental cursor to the current heads.
    ///
    /// This uses the internal dirty-range diff path, clears dirty bits after successful patch
    /// generation, and advances the incremental cursor to the current heads.
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
        let after_clock = clock.after_clock();
        let mut patch_accumulator = PatchAccumulator::event_log();
        DiffIter::log(self, obj, clock, &mut patch_accumulator, recursive);
        patch_accumulator.heads_clock = after_clock;
        Ok(patch_accumulator.make_patches(self))
    }

    /// This document's [`AuditMode`].
    pub fn audit_mode(&self) -> AuditMode {
        self.change_graph.audit_mode()
    }

    /// EXPERIMENTAL: Return the fragments covering the document history at
    /// the given levels, ordered oldest to newest.
    ///
    /// This is an experimental API, it may change or be removed without
    /// warning.
    #[doc(hidden)]
    pub fn fragments<R: RangeBounds<usize>>(
        &self,
        levels: R,
    ) -> Result<Vec<Fragment>, AutomergeError> {
        Ok(self
            .change_graph
            .fragments(&self.get_head_hashes(), levels, &self.ops.actors))
    }

    /// EXPERIMENTAL: Return the fragment with the given head hash, if any.
    ///
    /// This is an experimental API, it may change or be removed without
    /// warning.
    #[doc(hidden)]
    pub fn get_fragment(&self, head: ChangeHash) -> Result<Option<Fragment>, AutomergeError> {
        Ok(self.change_graph.get_fragment(head, &self.ops.actors))
    }

    /// EXPERIMENTAL: Encode each fragment as a bundle's bytes.
    ///
    /// This is an experimental API, it may change or be removed without
    /// warning.
    #[doc(hidden)]
    pub fn bundle_fragments<I: IntoIterator<Item = Fragment>>(
        &self,
        fragments: I,
    ) -> Result<Vec<Vec<u8>>, AutomergeError> {
        Ok(fragments
            .into_iter()
            .filter_map(|f| {
                if f.head.fragment_level() == 0 && f.members.len() == 1 {
                    let change = self.get_change_by_hash(&f.head).ok()??;
                    return Some(change.raw_bytes().to_vec());
                }
                // members are (actor, seq) ids; bundles are built from
                // nodes so only boundary hashes are required
                let mut nodes = f
                    .members
                    .iter()
                    .map(|id| self.change_graph.node_for_change_id(id, &self.ops.actors))
                    .collect::<Option<Vec<_>>>()?;
                nodes.sort_unstable();
                let bundle =
                    crate::storage::Bundle::for_nodes(&self.ops, &self.change_graph, nodes).ok()?;
                Some(bundle.bytes().to_vec())
            })
            .collect())
    }

    /// EXPERIMENTAL: Encode a fragment as a [`BundleV2`]: a v1 bundle
    /// plus the metadata a fragments-mode document needs to apply it —
    /// the head, checkpoint and boundary hashes paired with their
    /// change ids, and the `(actor, seq)` id of every external dep.
    ///
    /// This is an experimental API, it may change or be removed without
    /// warning.
    #[doc(hidden)]
    pub fn bundle_fragment_v2(&self, f: &Fragment) -> Result<BundleV2, AutomergeError> {
        let unknown = || AutomergeError::InvalidFragment("fragment references an unknown change");
        let mut nodes = f
            .members
            .iter()
            .map(|id| self.change_graph.node_for_change_id(id, &self.ops.actors))
            .collect::<Option<Vec<_>>>()
            .ok_or_else(unknown)?;
        nodes.sort_unstable();
        // fragments can share members (a loose commit covered by more
        // than one fragment clock) — a member must appear once
        nodes.dedup();
        let bundle = storage::Bundle::for_nodes(&self.ops, &self.change_graph, nodes.clone())?;

        // member indexes are positions in the bundle's (topologically
        // ordered) change list, which is node order
        let member_index = |h: &ChangeHash| -> Option<usize> {
            let n = self.change_graph.node_by_hash(h)?;
            nodes.binary_search(&n).ok()
        };
        let head_index = member_index(&f.head).ok_or_else(unknown)?;
        let checkpoints = f
            .checkpoints
            .iter()
            .filter(|h| **h != f.head)
            .map(|h| member_index(h).map(|i| (i, *h)))
            .collect::<Option<Vec<_>>>()
            .ok_or_else(unknown)?;
        let change_id = |h: &ChangeHash| -> Option<ChangeId> {
            let n = self.change_graph.node_by_hash(h)?;
            Some(self.change_graph.change_id(n, &self.ops.actors))
        };
        let boundary = f
            .boundary
            .iter()
            .map(|h| change_id(h).map(|id| (*h, id.actor().clone(), id.seq_nonzero())))
            .collect::<Option<Vec<_>>>()
            .ok_or_else(unknown)?;
        let dep_ids = bundle
            .deps()
            .iter()
            .map(|h| change_id(h).map(|id| (id.actor().clone(), id.seq_nonzero())))
            .collect::<Option<Vec<_>>>()
            .ok_or_else(unknown)?;
        // mirrors what `BundleV2::try_from` validates on the way back in
        let member_seqs = bundle
            .iter_changes()
            .map(|c| std::num::NonZeroU64::new(c.seq))
            .collect::<Option<Vec<_>>>()
            .ok_or_else(unknown)?;

        Ok(BundleV2::new(
            f.head,
            head_index,
            checkpoints,
            boundary,
            dep_ids,
            member_seqs,
            bundle,
        ))
    }

    /// EXPERIMENTAL: [`Self::bundle_fragment_v2`] for several fragments,
    /// returning each one's encoded bytes.
    ///
    /// This is an experimental API, it may change or be removed without
    /// warning.
    #[doc(hidden)]
    pub fn bundle_fragments_v2<I: IntoIterator<Item = Fragment>>(
        &self,
        fragments: I,
    ) -> Result<Vec<Vec<u8>>, AutomergeError> {
        fragments
            .into_iter()
            .map(|f| Ok(self.bundle_fragment_v2(&f)?.bytes()))
            .collect()
    }

    /// EXPERIMENTAL: Apply a fragment's bundle directly, without
    /// converting it into [`Change`]s.
    ///
    /// This is the fast path for ingesting the output of
    /// [`Self::fragments`]/[`Self::bundle_fragments_v2`]: a bundle's ops
    /// are already in document order, so they merge into the op set in a
    /// single pass — no per-change reconstruction and no hashing. The
    /// bundle's metadata prefix supplies the hashes worth knowing (head,
    /// checkpoints, boundary, deps), each paired with its change, and the
    /// document records them as it applies — so its heads stay exact and
    /// later fragments' deps keep resolving.
    ///
    /// Unlike [`Self::load_incremental`] nothing is queued — the bundle
    /// must be immediately applicable, and errors with
    /// [`AutomergeError::MissingDeps`] otherwise (a dependency is not in
    /// this document, or a member change's seq leaves a gap in its
    /// actor's change sequence). Member changes the document already has
    /// are skipped, along with their ops — applying a fully present
    /// fragment is a no-op.
    ///
    /// Outside audit mode the interior member changes are never
    /// reconstructed, so their hashes stay unknown. In audit mode the
    /// bundle is converted back to changes and applied through
    /// [`Self::apply_changes`], computing and keeping every hash.
    ///
    /// This is an experimental API, it may change or be removed without
    /// warning.
    #[doc(hidden)]
    pub fn apply_fragment(&mut self, v2: &BundleV2) -> Result<(), AutomergeError> {
        if self.audit_mode() == AuditMode::Enabled {
            // audit mode keeps and verifies every hash: reconstruct the
            // member changes and apply them individually, computing each
            // hash instead of taking the bundle's metadata on trust
            self.apply_changes(v2.bundle().to_changes()?)?;
            // the members applied (rather than queued) exactly when the
            // computed head hash names a change — this both enforces the
            // manifold path's no-missing-deps contract and verifies the
            // bundle's claimed head against the computed hashes
            if self.change_graph.hash_to_index(&v2.head()).is_none() {
                return Err(AutomergeError::MissingDeps);
            }
            return Ok(());
        }

        let bundle = v2.bundle();

        // member changes are in topological order
        let members: Vec<storage::BundleChange<'_>> = bundle.iter_changes().collect();
        let num_members = members.len();

        // BundleV2 parsing catches all shape errors — a bundle that
        // exists is well formed, so indexes below need no bounds checks

        // insert any new actors, then map bundle actor indexes to the
        // (possibly shifted) document indexes
        self.put_actor_refs(bundle.actors());
        let actor_map: Vec<usize> = bundle
            .actors()
            .iter()
            .map(|a| self.ops.lookup_actor(a).expect("actor was just inserted"))
            .collect();

        // everything the document already has, as clocks: a member (or
        // one of its ops) is already here exactly when the clock covers
        // it, since changes arrive in per-actor order
        let clock = self.change_graph.current_clock();
        let seq_clock = self.change_graph.current_seq_clock();

        // Split the members into ones we already have (skipped — applying
        // them twice would be an error) and new ones, which must extend
        // their actor's change sequence without gaps.
        let mut keep = vec![false; num_members];
        // a kept member's position among the kept (its graph-member index)
        let mut kept_index = vec![usize::MAX; num_members];
        let mut num_kept = 0;
        let mut next_seq: Vec<Option<u64>> = vec![None; bundle.actors().len()];
        for (i, m) in members.iter().enumerate() {
            let have = seq_clock
                .get_for_actor(&actor_map[m.actor])
                .map(|s| s.get() as u64)
                .unwrap_or(0);
            let next = next_seq[m.actor].unwrap_or(have + 1);
            match m.seq.cmp(&next) {
                Ordering::Less => continue, // already have this change
                // a gap in this actor's chain: the fragment is not
                // applicable to this document yet
                Ordering::Greater => return Err(AutomergeError::MissingDeps),
                Ordering::Equal => {}
            }
            next_seq[m.actor] = Some(next + 1);
            keep[i] = true;
            kept_index[i] = num_kept;
            num_kept += 1;
        }

        if num_kept == 0 {
            // everything is already in the document
            return Ok(());
        }

        // load the ops before touching the graph, so a malformed bundle
        // fails without altering history. Ops the clock covers belong to
        // skipped members and are dropped.
        let overlap = num_kept < num_members;
        let ops = match FragmentApply::new(bundle, actor_map.clone(), &clock, overlap, &self.ops) {
            Ok(f) => f,
            Err(e) => {
                self.remove_unused_actors(false);
                return Err(e);
            }
        };

        // record the boundary pairings — every boundary head is an
        // ancestor of the members, so it must already be a node here
        //
        // FIXME: this loop, and the dep loop below, record hashes into
        // the change graph *before* a later entry can return
        // `MissingDeps` — so a rejected bundle can still leave hash ->
        // node pairings (and the fragment-index entries `cache_fragment`
        // derives from them) behind, contradicting this method's "nothing
        // is applied on error" contract. The pairings are unverified
        // bundle metadata, and `has_change` consults them, so a bad
        // bundle can teach the document a hash it does not have. Resolve
        // and drain the pairings first, then commit them only once every
        // member has resolved.
        for (hash, actor, seq) in &v2.boundary {
            let id = ChangeId::new(*seq, actor.clone(), 0);
            let node = self
                .change_graph
                .node_for_change_id(&id, &self.ops.actors)
                .ok_or(AutomergeError::MissingDeps)?;
            self.change_graph.record_node_hash(node, *hash);
        }

        // A kept member's deps resolve to other kept members (by their
        // kept position) or to existing nodes: skipped members, and
        // external deps via their (actor, seq) ids from the metadata
        // prefix — whose hash pairings we record for later fragments.
        // Skipped members' deps are not consulted at all.
        let member_ids: Vec<ChangeId> = members
            .iter()
            .zip(&v2.member_seqs)
            .map(|(m, seq)| {
                let doc_actor = actor_map[m.actor];
                ChangeId::new(*seq, self.ops.actors[doc_actor].clone(), doc_actor)
            })
            .collect();
        let mut graph_members = Vec::with_capacity(num_kept);
        for (i, m) in members.into_iter().enumerate() {
            if !keep[i] {
                continue;
            }
            let mut deps = Vec::with_capacity(m.deps.len());
            for d in &m.deps {
                let d = *d as usize;
                if d < num_members {
                    if keep[d] {
                        deps.push(FragmentDep::Member(kept_index[d]));
                    } else {
                        let node = self
                            .change_graph
                            .node_for_change_id(&member_ids[d], &self.ops.actors);
                        deps.push(FragmentDep::Node(node.ok_or(AutomergeError::MissingDeps)?));
                    }
                } else {
                    let (dep_actor, dep_seq) = v2
                        .dep_ids
                        .get(d - num_members)
                        .ok_or(AutomergeError::InvalidFragment("bad dep index"))?;
                    let dep_id = ChangeId::new(*dep_seq, dep_actor.clone(), 0);
                    let node = self
                        .change_graph
                        .node_for_change_id(&dep_id, &self.ops.actors)
                        .ok_or(AutomergeError::MissingDeps)?;
                    // learn the dep's hash pairing — an anchor for
                    // later fragments that reference it by hash
                    self.change_graph
                        .record_node_hash(node, bundle.deps()[d - num_members]);
                    deps.push(FragmentDep::Node(node));
                }
            }
            graph_members.push(FragmentMember {
                actor: actor_map[m.actor],
                seq: m.seq,
                max_op: m.max_op,
                num_ops: 1 + m.max_op - m.start_op,
                timestamp: m.timestamp,
                message: m.message.map(|s| s.into_owned()),
                extra: Cow::Owned(m.extra.into_owned()),
                deps,
            });
        }
        // the covered heads move on to the fragment head. The covered
        // parents are the resolved dep nodes — with skipped members in
        // play a covered head can be an internal dep, so bundle.deps()
        // alone is not enough
        for m in &graph_members {
            for d in &m.deps {
                if let FragmentDep::Node(n) = d {
                    if let Some(h) = self.change_graph.hash_for_node(*n) {
                        self.deps.remove(&h);
                    }
                }
            }
        }

        self.change_graph.add_fragment_members(graph_members);

        // record the head and checkpoint hashes on their nodes: the head
        // so it can serve as a head of the document and an anchor for
        // the next fragment, the checkpoints so nested fragments stay
        // exportable
        let head_node = self
            .change_graph
            .node_for_change_id(&member_ids[v2.head_index], &self.ops.actors)
            .ok_or(AutomergeError::InvalidFragment(
                "fragment head is not a member of the bundle",
            ))?;
        self.change_graph.record_fragment_head(head_node, v2.head);
        for (i, hash) in &v2.checkpoints {
            if let Some(node) = self
                .change_graph
                .node_for_change_id(&member_ids[*i], &self.ops.actors)
            {
                self.change_graph.record_node_hash(node, *hash);
            }
        }

        self.deps.insert(v2.head);

        self.remove_unused_actors(true);

        ops.apply(self)
    }

    /// Test-support deep validation of the document.
    ///
    /// 1. Op columns must be in document order.
    /// 2. The incrementally-maintained indexes (top/visible/text/inc/
    ///    mark/obj-info) must match a from-scratch rebuild by the load
    ///    path's index builder.
    /// 3. The op columns must reproduce the document's history: every
    ///    change is re-encoded from the columns and its hash
    ///    recomputed — replaying those changes into a fresh document
    ///    can only reach the same heads if every column value is
    ///    exactly right, because any miswritten value changes a
    ///    reconstructed change's bytes and breaks the hash chain.
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

    /// Switch this document to [`AuditMode::Enabled`].
    ///
    /// Every change is reconstructed and hashed; the hashes retained so
    /// far (the head pairing from load time, the stored hash columns
    /// and everything added since) are verified against the recomputed
    /// ones, erroring with [`AutomergeError::InvalidHash`] on any
    /// mismatch. Afterwards all hash-based APIs (including sync) work.
    ///
    /// This is a no-op on a document already in audit mode.
    pub fn enable_audit_mode(&mut self) -> Result<(), AutomergeError> {
        if self.change_graph.is_audit_enabled() {
            return Ok(());
        }

        let inflate = |e: Box<dyn std::error::Error + Send + Sync + 'static>| {
            AutomergeError::Load(load::Error::InflateDocument(e))
        };

        // reconstruct and hash every change directly from our own op set
        // and change graph; changes are emitted in node (topological) order
        // so each change's deps are hashed before it is
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

        // this also verifies the hashes we already knew: the claimed head
        // pairing from load time and everything added since
        self.change_graph
            .install_checked_hashes(collected.changes.iter().map(|c| c.hash()).collect())
            .map_err(AutomergeError::InvalidHash)?;

        // regenerate the fragment index now that every hash is known
        self.change_graph.cache_fragments();
        Ok(())
    }

    /// Switch this document to [`AuditMode::Disabled`], freeing every
    /// hash outside the retained set (heads, loose commits, fragment
    /// heads and checkpoints, and their deps).
    ///
    /// This is a no-op on a document already outside audit mode (beyond
    /// a garbage-collection pass over the retained set).
    pub fn disable_audit_mode(&mut self) {
        self.change_graph.retain_hashes_only();
    }

    /// Get the heads of this document.
    ///
    /// The heads are the [`ChangeId`]s of the changes which have no
    /// successors in this document — collectively they identify the
    /// current state. Pass them to the `*_at` methods of
    /// [`crate::ReadDoc`] to read historical values, or convert them to
    /// hashes with [`Self::change_ids_to_hashes`].
    pub fn get_heads(&self) -> Vec<ChangeId> {
        self.change_graph.head_change_ids(&self.ops.actors)
    }

    /// The heads of this document as [`ChangeHash`]es.
    ///
    /// The head hashes are always known, whatever the audit mode. Hashes
    /// are the currency of the sync protocol and storage; for everything
    /// else prefer the [`ChangeId`]s from [`Self::get_heads`].
    pub fn get_head_hashes(&self) -> Vec<ChangeHash> {
        let mut deps: Vec<_> = self.deps.iter().copied().collect();
        deps.sort_unstable();
        deps
    }

    /// Returns `Ok(None)` — an unscoped read of the present document —
    /// when `heads` is exactly the current heads, so
    /// `*_at(doc.get_heads())` takes the same indexed fast paths as the
    /// un-suffixed methods.
    ///
    /// The shortcut is sound here because pending transaction ops enter
    /// the op set before the graph's heads advance, and an `Automerge`
    /// cannot be read through `&self` while a transaction holds it
    /// mutably. Anything reading *around* an in-flight transaction
    /// (`AutoCommit`, the transaction types) — or needing a concrete
    /// clock — must resolve nodes and use the [`ChangeGraph`] resolvers
    /// instead.
    ///
    /// This never needs hashes so it works in any audit mode.
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

    /// Resolve a [`ChangeId`] to its node, verifying the id's actor index
    /// hint. Hash-free.
    pub(crate) fn node_for_change_id(&self, id: &ChangeId) -> Option<crate::change_graph::NodeIdx> {
        self.change_graph.node_for_change_id(id, &self.ops.actors)
    }

    /// The [`ChangeId`] naming the change at (actor index, seq) — the
    /// index is stamped as the id's hint.
    pub(crate) fn change_id_at(&self, actor_idx: usize, seq: u64) -> ChangeId {
        ChangeId::from_doc_seq(seq, self.ops.actors[actor_idx].clone(), actor_idx)
    }

    /// Resolve each id to its node, erroring on ids not in this document.
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
    /// exist in this document. Never needs hashes.
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

    /// Whether this document contains the change identified by `id`.
    ///
    /// This never needs hashes so it works in any audit mode.
    pub fn has_change_id(&self, id: &ChangeId) -> bool {
        self.node_for_change_id(id).is_some()
    }

    /// Resolve heads to hashes, erroring on ids not in this document.
    pub(crate) fn resolve_heads(
        &self,
        heads: &[ChangeId],
    ) -> Result<Vec<ChangeHash>, AutomergeError> {
        self.change_ids_to_hashes(heads)
    }

    pub fn get_changes(&self, have_deps: &[ChangeId]) -> Result<Vec<Change>, AutomergeError> {
        // `have_deps` describes what the caller already has — ids this
        // document doesn't know contribute nothing to the exclusion set
        // and are skipped (a peer may know changes we don't). Building
        // the emitted changes is still fallible if their deps' hashes
        // are unknown.
        let clock = self.seq_clock_for_ids_lossy(have_deps);
        ChangeCollector::exclude_seq_clock(&self.ops, &self.change_graph, clock)
    }

    pub fn get_changes_meta(
        &self,
        have_deps: &[ChangeId],
    ) -> Result<Vec<ChangeMetadata<'_>>, AutomergeError> {
        // like `get_changes`, unknown ids in the exclusion set are skipped
        let have_deps: Vec<ChangeHash> = have_deps
            .iter()
            .filter_map(|id| self.change_id_to_hash(id).ok().flatten())
            .collect();
        ChangeCollector::exclude_hashes_meta(&self.ops, &self.change_graph, &have_deps)
    }

    /// The seq clock for a set of [`ChangeId`]s, silently skipping ids
    /// not in this document (they contribute nothing to the exclusion
    /// set). Hash-free.
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
        match ChangeCollector::meta_for_hashes(&self.ops, &self.change_graph, [*hash]) {
            Ok(mut metas) => Ok(metas.pop()),
            Err(AutomergeError::AuditModeRequired) => Err(AutomergeError::AuditModeRequired),
            Err(_) => Ok(None),
        }
    }

    /// Get changes in `other` that are not in `self`
    pub fn get_changes_added(&self, other: &Self) -> Result<Vec<Change>, AutomergeError> {
        // hash-free: per-actor change sequences are linear, so a change
        // in `other` is new to us exactly when our seq clock does not
        // cover its (actor, seq) — the same identify-by-(actor, seq)
        // rule the apply paths use. Building the returned changes is
        // still fallible if their deps' hashes were freed in `other`.
        let ours = self.change_graph.current_seq_clock();
        let theirs = other.change_graph.current_seq_clock();
        let mut exclude = crate::clock::SeqClock::new(other.change_graph.num_actors());
        for (actor_idx, seq) in ours.iter() {
            let Some(seq) = seq else { continue };
            if let Some(other_idx) = other.ops.lookup_actor(&self.ops.actors[actor_idx]) {
                // we may be ahead of `other` for this actor — cap at
                // their chain so nothing is sliced out of range
                let Some(cap) = theirs.get_for_actor(&other_idx) else {
                    continue;
                };
                let shared = seq.get().min(cap.get());
                // (actor, seq) identity assumes the two documents agree
                // on this actor's chain. Where both sides still retain
                // the hashes, verify it: a divergent hash at a shared
                // seq is an equivocation, exactly what the hash-based
                // traversal used to surface as DuplicateSeqNumber.
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
                        // find the first divergent shared seq (only on
                        // the error path)
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
        ChangeCollector::exclude_seq_clock(&other.ops, &other.change_graph, exclude)
    }

    /// Get the hash of the change that contains the given `opid`.
    ///
    /// Returns `Ok(None)` if the `opid`:
    /// - is the root object id
    /// - does not exist in this document
    ///
    /// Returns [`AutomergeError::AuditModeRequired`] if the change is in
    /// this document but the hash graph has not been built.
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
        let mut iter = self.ops.top_ops(&obj.id, clock).marks();
        iter.nth(index);
        match iter.get_marks() {
            Some(arc) => Ok(arc.as_ref().clone().without_unmarks()),
            None => Ok(MarkSet::default()),
        }
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

    /// Whether the peer represented by `other` has all the changes we have
    pub fn has_our_changes(&self, other: &crate::sync::State) -> bool {
        other.shared_heads == self.get_head_hashes()
    }

    pub(crate) fn has_change(&self, head: &ChangeHash) -> Result<bool, AutomergeError> {
        Ok(self.change_graph.has_change(head)?)
    }

    /// Hash-based version of [`ReadDoc::get_missing_deps`], for callers (like the
    /// sync protocol) which hold hashes for changes this document may not have.
    pub(crate) fn get_missing_deps_hashes(
        &self,
        heads: &[ChangeHash],
    ) -> Result<Vec<ChangeHash>, AutomergeError> {
        let queued = self.queue.iter().map(|change| change.hash());
        self.missing_deps_from(queued.chain(heads.iter().copied()))
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

impl ReadDoc for Automerge {
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
        self.get_missing_deps_hashes(&self.resolve_heads(heads)?)
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
    /// Whether to save changes which we do not have the dependencies for
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
        sync::{State as SyncState, SyncDoc},
        transaction::Transactable,
        types::ObjId,
        ActorId, AutoCommit, Automerge, ScalarValue, ROOT,
    };

    fn dirty_ranges(doc: &Automerge) -> Vec<Range<usize>> {
        doc.ops().dirty_runs().collect()
    }

    fn ranges_contain(ranges: &[Range<usize>], needle: Range<usize>) -> bool {
        ranges
            .iter()
            .any(|range| range.start <= needle.start && needle.end <= range.end)
    }

    fn assert_patch_effects_match(
        doc: &Automerge,
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

    fn assert_dirty_diff_matches_full(
        doc: &Automerge,
        before: &[crate::ChangeId],
        after: &[crate::ChangeId],
    ) {
        let full = doc.diff(before, after).unwrap();
        let dirty = doc.dirty_diff_patches(before, after).unwrap();
        assert_patch_effects_match(doc, before, after, "dirty diff", &dirty, "full diff", &full);
    }

    fn assert_incremental_effect_matches_full(
        doc: &mut Automerge,
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

    fn assert_autocommit_incremental_effect_matches_full(
        doc: &mut AutoCommit,
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
        let mut doc = Automerge::new();
        doc.enable_audit_mode().unwrap();
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
        let mut doc = Automerge::new();
        doc.enable_audit_mode().unwrap();
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
        let mut doc = Automerge::new();
        doc.enable_audit_mode().unwrap();
        let mut tx = doc.transaction();
        let list = tx.put_object(ROOT, "list", crate::ObjType::List).unwrap();
        tx.insert(&list, 0, "a").unwrap();
        tx.commit();
        let before = doc.get_heads();

        let mut tx = doc.transaction();
        tx.put(&list, 0, "A").unwrap();
        tx.commit();
        let after = doc.get_heads();

        // a single-row mark inside a multi-row register is widened to
        // the register, so the diff still sees the whole election
        doc.ops_mut().clear_dirty();
        doc.ops_mut().mark_dirty(1);
        let patches = doc.dirty_diff_patches_and_clear(&before, &after).unwrap();
        let expected = doc.diff(&before, &after).unwrap();
        assert_eq!(patches, expected);
        assert!(doc.ops().dirty_runs().next().is_none());
    }

    #[test]
    fn automerge_diff_incremental_empty_doc_and_repeated_calls_are_empty() {
        let mut doc = Automerge::new();
        doc.enable_audit_mode().unwrap();

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
        let mut source = Automerge::new();
        source.enable_audit_mode().unwrap();
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
        let mut source = Automerge::new();
        source.enable_audit_mode().unwrap();
        let mut tx = source.transaction();
        tx.put(ROOT, "base", 1).unwrap();
        tx.commit();
        let base_heads = source.get_heads();
        let base_data = source.save();

        let mut doc = Automerge::new();
        doc.enable_audit_mode().unwrap();
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
        let mut source = Automerge::new();
        source.enable_audit_mode().unwrap();
        let mut tx = source.transaction();
        tx.put(ROOT, "key", 1).unwrap();
        tx.commit();

        let mut doc = Automerge::new();
        doc.enable_audit_mode().unwrap();
        let before = doc.get_heads();
        doc.apply_changes(source.get_changes(&[]).unwrap()).unwrap();
        let after = doc.get_heads();
        assert_incremental_effect_matches_full(&mut doc, &before, &after);
        assert!(doc.ops().dirty_runs().next().is_none());

        let mut source = Automerge::new();
        source.enable_audit_mode().unwrap();
        let mut tx = source.transaction();
        tx.put(ROOT, "merged", 2).unwrap();
        tx.commit();

        let mut doc = Automerge::new();
        doc.enable_audit_mode().unwrap();
        let before = doc.get_heads();
        doc.merge(&mut source).unwrap();
        let after = doc.get_heads();
        assert_incremental_effect_matches_full(&mut doc, &before, &after);
        assert!(doc.ops().dirty_runs().next().is_none());

        let mut source = Automerge::new();
        source.enable_audit_mode().unwrap();
        let mut tx = source.transaction();
        tx.put(ROOT, "synced", 3).unwrap();
        tx.commit();
        let mut sync_state = SyncState::new();
        let message = source.generate_sync_message(&mut sync_state).unwrap();

        let mut doc = Automerge::new();
        doc.enable_audit_mode().unwrap();
        let before = doc.get_heads();
        doc.receive_sync_message(&mut SyncState::new(), message.unwrap())
            .unwrap();
        let after = doc.get_heads();
        assert_incremental_effect_matches_full(&mut doc, &before, &after);
        assert!(doc.ops().dirty_runs().next().is_none());
    }

    #[test]
    fn automerge_diff_incremental_fork_inherits_cursor() {
        let mut doc = Automerge::new();
        doc.enable_audit_mode().unwrap();
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
        let mut doc = AutoCommit::new();
        doc.enable_audit_mode().unwrap();

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
        let mut doc = Automerge::new();
        doc.enable_audit_mode().unwrap();
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
        let mut doc = Automerge::new();
        doc.enable_audit_mode().unwrap();
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
        let mut doc1 = Automerge::new();
        doc1.enable_audit_mode().unwrap();
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
        let mut doc = Automerge::new();
        doc.enable_audit_mode().unwrap();
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
        let mut doc = Automerge::new();
        doc.enable_audit_mode().unwrap();
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
        let mut doc = Automerge::new();
        doc.enable_audit_mode().unwrap();
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
        let mut doc = Automerge::new();
        doc.enable_audit_mode().unwrap();
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
        let mut doc = Automerge::new();
        doc.enable_audit_mode().unwrap();
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
        let mut doc1 = Automerge::new();
        doc1.enable_audit_mode().unwrap();
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
        let mut doc1 = Automerge::new().with_actor(ActorId::from([1])).unwrap();
        let mut tx = doc1.transaction();
        let list = tx.put_object(ROOT, "list", crate::ObjType::List).unwrap();
        tx.insert(&list, 0, "a").unwrap();
        tx.insert(&list, 1, "b").unwrap();
        tx.insert(&list, 2, "c").unwrap();
        tx.commit();
        let mut doc2 = doc1.fork().with_actor(ActorId::from([2])).unwrap();

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
        let mut doc1 = Automerge::new();
        doc1.enable_audit_mode().unwrap();
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
        let mut doc1 = Automerge::new();
        doc1.enable_audit_mode().unwrap();
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
        let mut doc1 = Automerge::new().with_actor(ActorId::from([1])).unwrap();
        let mut tx = doc1.transaction();
        let list = tx.put_object(ROOT, "list", crate::ObjType::List).unwrap();
        tx.insert(&list, 0, "a").unwrap();
        tx.insert(&list, 1, "b").unwrap();
        tx.insert(&list, 2, "c").unwrap();
        tx.commit();
        let mut doc2 = doc1.fork().with_actor(ActorId::from([2])).unwrap();

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
        let mut doc1 = Automerge::new();
        doc1.enable_audit_mode().unwrap();
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
        let mut doc1 = Automerge::new();
        doc1.enable_audit_mode().unwrap();
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
                    // Insert before an existing element, shifting the final ranges for later
                    // existing-register updates in the same batch.
                    0 if model_len > 0 => {
                        let index = next_dirty_diff_test_rand(&mut seed) % model_len;
                        tx.insert(&list, index, format!("i{value_counter}"))
                            .unwrap();
                        model_len += 1;
                    }
                    // Update an existing register whose identity must be resolved after all
                    // batch splices have been applied.
                    1 if model_len > 0 => {
                        let index = next_dirty_diff_test_rand(&mut seed) % model_len;
                        tx.put(&list, index, format!("u{value_counter}")).unwrap();
                    }
                    // Insert at any legal sequence position, including after the last element.
                    2 => {
                        let index = next_dirty_diff_test_rand(&mut seed) % (model_len + 1);
                        tx.insert(&list, index, format!("j{value_counter}"))
                            .unwrap();
                        model_len += 1;
                    }
                    // Delete an element so later dirty existing-register identities may move left.
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
        let mut doc = Automerge::new();
        doc.enable_audit_mode().unwrap();
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
        let mut doc = Automerge::new();
        doc.enable_audit_mode().unwrap();
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
        let mut doc1 = Automerge::new();
        doc1.enable_audit_mode().unwrap();
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
        let mut doc1 = Automerge::new();
        doc1.enable_audit_mode().unwrap();
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
        let mut doc = Automerge::new();
        doc.enable_audit_mode().unwrap();
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
        let mut doc1 = Automerge::new().with_actor(ActorId::from([1])).unwrap();
        let mut tx = doc1.transaction();
        let map = tx.put_object(ROOT, "map", crate::ObjType::Map).unwrap();
        tx.put(&map, "key", 1).unwrap();
        tx.commit();
        let before = doc1.get_heads();
        let mut doc2 = doc1.fork().with_actor(ActorId::from([2])).unwrap();

        doc1.ops_mut().clear_dirty();
        let mut tx = doc1.transaction();
        tx.put(&map, "key", 2).unwrap();
        tx.put(&map, "other", 3).unwrap();
        tx.commit();

        let mut tx = doc2.transaction();
        tx.delete(ROOT, "map").unwrap();
        tx.commit();
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
        let mut doc1 = Automerge::new();
        doc1.enable_audit_mode().unwrap();
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

    #[test]
    fn remote_map_conflict_dirties_whole_key_register() {
        let mut doc1 = Automerge::new();
        doc1.enable_audit_mode().unwrap();
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

        let key_range = doc1.ops().prop_range(&ObjId::root(), "key");
        assert_eq!(dirty_ranges(&doc1), vec![key_range]);
        assert_dirty_diff_matches_full(&doc1, &before, &after);
    }

    #[test]
    fn batched_remote_map_conflict_dirties_whole_new_key_register() {
        let mut doc1 = Automerge::new();
        doc1.enable_audit_mode().unwrap();
        let mut doc2 = doc1.fork().with_actor(ActorId::from([2])).unwrap();
        let mut doc3 = doc1.fork().with_actor(ActorId::from([3])).unwrap();

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
        let mut doc1 = Automerge::new();
        doc1.enable_audit_mode().unwrap();
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

    #[test]
    fn remote_list_conflict_dirties_whole_element_register() {
        let mut doc1 = Automerge::new();
        doc1.enable_audit_mode().unwrap();
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

        let list_obj = doc1.exid_to_obj(&list).unwrap().id;
        let list_range = doc1.ops().scope_to_obj(&list_obj);
        let ranges = dirty_ranges(&doc1);
        assert_eq!(ranges.len(), 1);
        assert!(doc1
            .ops()
            .list_range_is_on_register_boundaries(&ranges[0], list_range.clone()));
        assert_eq!(ranges[0], list_range);
        assert_dirty_diff_matches_full(&doc1, &before, &after);
    }

    #[test]
    fn remote_insert_then_update_same_list_element_dirties_new_register() {
        let mut doc1 = Automerge::new();
        doc1.enable_audit_mode().unwrap();
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
        let mut doc1 = Automerge::new().with_actor(ActorId::from([1])).unwrap();
        let mut tx = doc1.transaction();
        tx.put(ROOT, "key", "base").unwrap();
        tx.commit();
        let mut doc2 = doc1.fork().with_actor(ActorId::from([2])).unwrap();

        let mut tx = doc1.transaction();
        tx.put(ROOT, "key", "a").unwrap();
        tx.commit();

        let mut tx = doc2.transaction();
        tx.put(ROOT, "key", "b").unwrap();
        tx.commit();
        doc1.apply_changes(doc2.get_changes(&doc1.get_heads()).unwrap())
            .unwrap();

        let mut tx = doc2.transaction();
        tx.delete(ROOT, "key").unwrap();
        tx.commit();
        let changes = doc2.get_changes(&doc1.get_heads()).unwrap();

        doc1.ops_mut().clear_dirty();
        let before = doc1.get_heads();
        doc1.apply_changes(changes).unwrap();
        let after = doc1.get_heads();

        assert_dirty_diff_matches_full(&doc1, &before, &after);
    }

    #[test]
    fn dirty_diff_matches_full_diff_for_remote_list_conflict_resolution_exposes_value() {
        let mut doc1 = Automerge::new().with_actor(ActorId::from([1])).unwrap();
        let mut tx = doc1.transaction();
        let list = tx.put_object(ROOT, "list", crate::ObjType::List).unwrap();
        tx.insert(&list, 0, "base").unwrap();
        tx.commit();
        let mut doc2 = doc1.fork().with_actor(ActorId::from([2])).unwrap();

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
        let mut doc1 = Automerge::new();
        doc1.enable_audit_mode().unwrap();
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
        let mut doc1 = Automerge::new();
        doc1.enable_audit_mode().unwrap();
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
        let mut doc1 = Automerge::new();
        doc1.enable_audit_mode().unwrap();
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
        let mut doc1 = Automerge::new();
        doc1.enable_audit_mode().unwrap();
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
        // only the inserted mark rows are dirty; the diff widens
        // mark-bearing text ranges to the whole object itself
        assert!(doc1.ops().dirty_runs().next().is_some());

        assert_dirty_diff_matches_full(&doc1, &before, &after);
    }

    #[test]
    fn dirty_diff_matches_full_diff_for_text_insert_without_marks() {
        let mut doc = Automerge::new();
        doc.enable_audit_mode().unwrap();
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
        let mut doc = Automerge::new();
        doc.enable_audit_mode().unwrap();
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
        let mut doc = Automerge::new();
        doc.enable_audit_mode().unwrap();
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
            let mut doc = Automerge::new();
            doc.enable_audit_mode().unwrap();
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
            assert!(!doc.ops().range_has_mark(ranges[0].clone()));
            assert_dirty_diff_matches_full(&doc, &before, &after);
        }
    }

    fn assert_text_splice_around_mark_matches_full(index: usize, del: isize, value: &str) {
        let mut doc = Automerge::new();
        doc.enable_audit_mode().unwrap();
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
        let mut doc = Automerge::new();
        doc.enable_audit_mode().unwrap();
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
        let mut doc1 = Automerge::new();
        doc1.enable_audit_mode().unwrap();
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
        let mut doc1 = Automerge::new();
        doc1.enable_audit_mode().unwrap();
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
        let mut doc1 = Automerge::new();
        doc1.enable_audit_mode().unwrap();
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
        let mut doc1 = Automerge::new();
        doc1.enable_audit_mode().unwrap();
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
        // only the touched rows are dirty; the diff widens mark-bearing
        // text ranges to the whole object itself
        assert!(doc1.ops().dirty_runs().next().is_some());

        assert_dirty_diff_matches_full(&doc1, &before, &after);
    }

    #[test]
    fn batch_multiple_text_edits_around_same_mark_match_full_diff() {
        let mut doc1 = Automerge::new();
        doc1.enable_audit_mode().unwrap();
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
        let mut doc1 = Automerge::new();
        doc1.enable_audit_mode().unwrap();
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

        let mut sync_state = SyncState::new();
        let message = doc2.generate_sync_message(&mut sync_state).unwrap();

        doc1.ops_mut().clear_dirty();
        let before = doc1.get_heads();
        doc1.receive_sync_message(&mut SyncState::new(), message.unwrap())
            .unwrap();
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
        let mut doc1 = Automerge::new();
        doc1.enable_audit_mode().unwrap();
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

        let mut sync_state = SyncState::new();
        let message = doc2.generate_sync_message(&mut sync_state).unwrap();

        doc1.ops_mut().clear_dirty();
        let before = doc1.get_heads();
        doc1.receive_sync_message(&mut SyncState::new(), message.unwrap())
            .unwrap();
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
        let mut doc = Automerge::new();
        doc.enable_audit_mode().unwrap();
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

    #[test]
    fn partial_text_mark_dirty_range_expands_to_whole_object() {
        let mut doc = Automerge::new();
        doc.enable_audit_mode().unwrap();
        let mut tx = doc.transaction();
        let text = tx.put_object(ROOT, "text", crate::ObjType::Text).unwrap();
        tx.splice_text(&text, 0, 0, "abc").unwrap();
        tx.commit();
        let before = doc.get_heads();

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
        let mark_pos = doc
            .ops()
            .iter_range(&text_range)
            .find(|op| op.action == Action::Mark)
            .unwrap()
            .pos;

        doc.ops_mut().clear_dirty();
        doc.ops_mut().mark_dirty(mark_pos);

        let full = doc.diff(&before, &after).unwrap();
        let dirty = doc.dirty_diff_patches_and_clear(&before, &after).unwrap();
        assert_patch_effects_match(
            &doc,
            &before,
            &after,
            "dirty diff",
            &dirty,
            "full diff",
            &full,
        );
        assert!(doc.ops().dirty_runs().next().is_none());
    }
}
