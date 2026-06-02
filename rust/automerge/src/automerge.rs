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

use crate::change_graph::ChangeGraph;
use crate::change_queue::ChangeQueue;
use crate::cursor::{CursorPosition, MoveCursor, OpCursor};
use crate::exid::ExId;
use crate::iter::{DiffIter, DocIter, Keys, ListRange, MapRange, Spans, Values};
use crate::marks::{Mark, MarkAccumulator, MarkSet};
use crate::patches::{Patch, PatchAccumulator};
use crate::storage::document::ReconstructError;
use crate::storage::{self, change, load, Bundle, CompressConfig, Document, VerificationMode};
use crate::transaction::{
    self, CommitOptions, Failure, OwnedTransaction, Success, Transactable, Transaction,
    TransactionArgs,
};

use crate::clock::{Clock, ClockRange};
use crate::hydrate;
use crate::types::{ActorId, ChangeHash, ObjId, ObjMeta, OpId, SequenceType, TextEncoding, Value};
use crate::{AutomergeError, Change, Cursor, ObjType, Prop};

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
}

impl std::default::Default for LoadOptions {
    fn default() -> Self {
        Self {
            on_partial_load: OnPartialLoad::Error,
            verification_mode: VerificationMode::Check,
            string_migration: StringMigration::NoMigration,
            text_encoding: TextEncoding::platform_default(),
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
    /// Cursor for dirty-range incremental diffs.
    diff_cursor: Vec<ChangeHash>,
    /// Heads corresponding to `ops.index.baseline_visible`.
    ///
    /// When dirty diffing from these heads to the current heads, map/list diff
    /// can use the baseline visibility bitmap instead of reconstructing
    /// historical visibility with clock and successor scans.
    dirty_diff_base: Vec<ChangeHash>,
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
            dirty_diff_base: Vec::new(),
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
            dirty_diff_base: Vec::new(),
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
            dirty_diff_base: Vec::new(),
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

    pub(crate) fn clear_dirty_and_reset_diff_baseline(&mut self, heads: Vec<ChangeHash>) {
        // `clear_dirty` also snapshots current visibility into baseline_visible;
        // keep the recorded heads in lockstep with that snapshot.
        self.ops.clear_dirty();
        self.dirty_diff_base = heads;
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
            Ok(idx) => self.actor = Actor::Cached(idx),
            Err(_) => self.actor = Actor::Unused(actor),
        }
        self
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
    pub fn transaction_at(&mut self, heads: &[ChangeHash]) -> Transaction<'_> {
        let args = self.transaction_args(Some(heads));
        Transaction::new(self, args)
    }

    /// Start a transaction that owns the document, consuming `self`.
    pub fn into_transaction(self, heads: Option<&[ChangeHash]>) -> OwnedTransaction {
        OwnedTransaction::new(self, heads)
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

        // SAFETY: this unwrap is safe as we always add 1
        let start_op = NonZeroU64::new(self.change_graph.max_op() + 1).unwrap();

        TransactionArgs {
            actor_index,
            seq,
            checkpoint: self.ops.save_checkpoint(),
            start_op,
            deps,
            scope,
        }
    }

    #[cfg(test)]
    pub(crate) fn save_checkpoint(&self) -> std::collections::HashMap<&'static str, Vec<u8>> {
        self.ops.save_column_checkpoint()
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
                let hash = if let Some(c) = c {
                    let commit_options = c(&result);
                    tx.commit_with(commit_options)
                } else {
                    tx.commit()
                };
                Ok(Success { result, hash })
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
    pub fn empty_commit(&mut self, opts: CommitOptions) -> ChangeHash {
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
    /// This will create a new actor ID for the forked document
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
        Ok(Self::load_with_options_and_mark_validation(
            data,
            Default::default(),
            load::MarkOrderValidation::AllowInvalid,
        )?
        .hydrate(None))
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
                match d.reconstruct(options.verification_mode, options.text_encoding) {
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
                Self::new()
            }
            storage::Chunk::Bundle(bundle) => {
                tracing::trace!("first chunk is change chunk");
                let bundle = Bundle::new_from_unverified(bundle.into_owned())
                    .map_err(|e| load::Error::InvalidBundleColumn(Box::new(e)))?;
                let bundle_changes = bundle
                    .to_changes()
                    .map_err(|e| load::Error::InvalidBundleChange(Box::new(e)))?;
                changes.extend(bundle_changes);
                Self::new()
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
                Self::new()
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
                    .verification_mode(VerificationMode::Check),
            )?;
            doc = doc.with_actor(self.actor_id().clone());
            let len = doc.ops().len();
            doc.ops_mut().mark_dirty_range(0..len);
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

    fn seq_for_actor(&self, actor: &ActorId) -> u64 {
        self.ops
            .lookup_actor(actor)
            .map(|idx| self.change_graph.seq_for_actor(idx))
            .unwrap_or(0)
    }

    pub(crate) fn has_actor_seq(&self, change: &Change) -> bool {
        self.seq_for_actor(change.actor_id()) >= change.seq()
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
    pub fn merge(&mut self, other: &mut Self) -> Result<Vec<ChangeHash>, AutomergeError> {
        let changes = self.get_changes_added(other);
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
    pub fn save_after(&self, heads: &[ChangeHash]) -> Vec<u8> {
        let changes = self.get_changes(heads);
        let mut bytes = vec![];
        for c in changes {
            bytes.extend(c.raw_bytes());
        }
        bytes
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

    pub(crate) fn clock_range(&self, before: &[ChangeHash], after: &[ChangeHash]) -> ClockRange {
        let before = self.clock_at(before);
        let after = self.clock_at(after);
        ClockRange::Diff(before, Some(after))
    }

    pub(crate) fn clock_at(&self, heads: &[ChangeHash]) -> Clock {
        self.change_graph.clock_for_heads(heads)
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
        let mut clock = self.clock_at(heads);

        for i in 1.. {
            let max_op = self.change_graph.max_op_for_actor(actor_index);
            if max_op == 0 || clock.covers(&OpId::new(max_op, actor_index)) {
                clock.isolate(actor_index);
                break;
            }
            actor_index = self.get_isolated_actor_index(i);
            clock = self.clock_at(heads); // need to recompute the clock b/c the actor indexes may have changed
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
    pub fn diff(&self, before_heads: &[ChangeHash], after_heads: &[ChangeHash]) -> Vec<Patch> {
        let clock = self.clock_range(before_heads, after_heads);
        let mut patch_accumulator = PatchAccumulator::event_log();
        DiffIter::log(self, ObjMeta::root(), clock, &mut patch_accumulator, true);
        patch_accumulator.heads = Some(after_heads.to_vec());
        patch_accumulator.make_patches(self)
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
        before_heads: &[ChangeHash],
        after_heads: &[ChangeHash],
        recursive: bool,
    ) -> Result<Vec<Patch>, AutomergeError> {
        let obj = self.exid_to_obj(obj.as_ref())?;
        let clock = self.clock_range(before_heads, after_heads);
        let mut patch_accumulator = PatchAccumulator::event_log();
        DiffIter::log(self, obj, clock, &mut patch_accumulator, recursive);
        patch_accumulator.heads = Some(after_heads.to_vec());
        Ok(patch_accumulator.make_patches(self))
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
        ChangeCollector::exclude_hashes_meta(&self.ops, &self.change_graph, have_deps)
    }

    pub fn get_change_meta_by_hash(&self, hash: &ChangeHash) -> Option<ChangeMetadata<'_>> {
        ChangeCollector::meta_for_hashes(&self.ops, &self.change_graph, [*hash])
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

    fn calculate_marks(
        &self,
        obj: &ExId,
        clock: Option<Clock>,
    ) -> Result<Vec<Mark>, AutomergeError> {
        let obj = self.exid_to_obj(obj.as_ref())?;
        let mut top_ops = self.ops().top_ops(&obj.id, clock).marks();

        let Some(seq_type) = obj.typ.as_sequence_type() else {
            // Really we should return an error here but we don't in order to stay
            // compatibile with older implementations
            return Ok(Vec::new());
        };

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
        Ok(acc.into_iter_no_unmark().collect())
    }

    pub fn hydrate(&self, heads: Option<&[ChangeHash]>) -> hydrate::Value {
        let clock = heads.map(|heads| self.clock_at(heads));
        self.hydrate_map(&ObjId::root(), clock.as_ref())
    }

    pub(crate) fn hydrate_obj(
        &self,
        obj: &crate::ObjId,
        heads: Option<&[ChangeHash]>,
    ) -> Result<hydrate::Value, AutomergeError> {
        let obj = self.exid_to_obj(obj)?;
        let clock = heads.map(|heads| self.clock_at(heads));
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
            .iter()
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
        self.parents_for(obj.as_ref(), None)
    }

    fn parents_at<O: AsRef<ExId>>(
        &self,
        obj: O,
        heads: &[ChangeHash],
    ) -> Result<Parents<'_>, AutomergeError> {
        let clock = self.clock_at(heads);
        self.parents_for(obj.as_ref(), Some(clock))
    }

    fn keys<O: AsRef<ExId>>(&self, obj: O) -> Keys<'_> {
        self.keys_for(obj.as_ref(), None)
    }

    fn keys_at<O: AsRef<ExId>>(&self, obj: O, heads: &[ChangeHash]) -> Keys<'_> {
        let clock = self.clock_at(heads);
        self.keys_for(obj.as_ref(), Some(clock))
    }

    fn iter_at<O: AsRef<ExId>>(&self, obj: O, heads: Option<&[ChangeHash]>) -> DocIter<'_> {
        //let obj = self.exid_to_obj(obj.as_ref()).unwrap();
        let clock = heads.map(|heads| self.clock_at(heads));
        self.iter_for(obj.as_ref(), clock)
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
        heads: &[ChangeHash],
    ) -> MapRange<'a> {
        let clock = self.clock_at(heads);
        self.map_range_for(obj.as_ref(), range, Some(clock))
    }

    fn list_range<O: AsRef<ExId>, R: RangeBounds<usize>>(&self, obj: O, range: R) -> ListRange<'_> {
        self.list_range_for(obj.as_ref(), range, None)
    }

    fn list_range_at<O: AsRef<ExId>, R: RangeBounds<usize>>(
        &self,
        obj: O,
        range: R,
        heads: &[ChangeHash],
    ) -> ListRange<'_> {
        let clock = self.clock_at(heads);
        self.list_range_for(obj.as_ref(), range, Some(clock))
    }

    fn values<O: AsRef<ExId>>(&self, obj: O) -> Values<'_> {
        self.values_for(obj.as_ref(), None)
    }

    fn values_at<O: AsRef<ExId>>(&self, obj: O, heads: &[ChangeHash]) -> Values<'_> {
        let clock = self.clock_at(heads);
        self.values_for(obj.as_ref(), Some(clock))
    }

    fn length<O: AsRef<ExId>>(&self, obj: O) -> usize {
        self.length_for(obj.as_ref(), None)
    }

    fn length_at<O: AsRef<ExId>>(&self, obj: O, heads: &[ChangeHash]) -> usize {
        let clock = self.clock_at(heads);
        self.length_for(obj.as_ref(), Some(clock))
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
        heads: &[ChangeHash],
    ) -> Result<Spans<'_>, AutomergeError> {
        let clock = self.clock_at(heads);
        self.spans_for(obj.as_ref(), Some(clock))
    }

    fn get_cursor<O: AsRef<ExId>, I: Into<CursorPosition>>(
        &self,
        obj: O,
        position: I,
        at: Option<&[ChangeHash]>,
    ) -> Result<Cursor, AutomergeError> {
        let clock = at.map(|heads| self.clock_at(heads));
        self.get_cursor_for(obj.as_ref(), position.into(), clock, MoveCursor::After)
    }

    fn get_cursor_moving<O: AsRef<ExId>, I: Into<CursorPosition>>(
        &self,
        obj: O,
        position: I,
        at: Option<&[ChangeHash]>,
        move_cursor: MoveCursor,
    ) -> Result<Cursor, AutomergeError> {
        let clock = at.map(|heads| self.clock_at(heads));
        self.get_cursor_for(obj.as_ref(), position.into(), clock, move_cursor)
    }

    fn get_cursor_position<O: AsRef<ExId>>(
        &self,
        obj: O,
        cursor: &Cursor,
        at: Option<&[ChangeHash]>,
    ) -> Result<usize, AutomergeError> {
        let clock = at.map(|heads| self.clock_at(heads));
        self.get_cursor_position_for(obj.as_ref(), cursor, clock)
    }

    fn text_at<O: AsRef<ExId>>(
        &self,
        obj: O,
        heads: &[ChangeHash],
    ) -> Result<String, AutomergeError> {
        let clock = self.clock_at(heads);
        self.text_for(obj.as_ref(), Some(clock))
    }

    fn marks<O: AsRef<ExId>>(&self, obj: O) -> Result<Vec<Mark>, AutomergeError> {
        self.marks_for(obj.as_ref(), None)
    }

    fn marks_at<O: AsRef<ExId>>(
        &self,
        obj: O,
        heads: &[ChangeHash],
    ) -> Result<Vec<Mark>, AutomergeError> {
        let clock = self.clock_at(heads);
        self.marks_for(obj.as_ref(), Some(clock))
    }

    fn hydrate<O: AsRef<ExId>>(
        &self,
        obj: O,
        heads: Option<&[ChangeHash]>,
    ) -> Result<hydrate::Value, AutomergeError> {
        let obj = self.exid_to_obj(obj.as_ref())?;
        let clock = heads.map(|h| self.clock_at(h));
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
        heads: Option<&[ChangeHash]>,
    ) -> Result<MarkSet, AutomergeError> {
        let clock = heads.map(|h| self.clock_at(h));
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
        heads: &[ChangeHash],
    ) -> Result<Option<(Value<'_>, ExId)>, AutomergeError> {
        let clock = Some(self.clock_at(heads));
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
        heads: &[ChangeHash],
    ) -> Result<Vec<(Value<'_>, ExId)>, AutomergeError> {
        let clock = Some(self.clock_at(heads));
        self.get_all_for(obj.as_ref(), prop.into(), clock)
    }

    fn object_type<O: AsRef<ExId>>(&self, obj: O) -> Result<ObjType, AutomergeError> {
        let obj = obj.as_ref();
        let opid = self.exid_to_opid(obj)?;
        let typ = self.ops.object_type(&ObjId(opid));
        typ.ok_or_else(|| AutomergeError::InvalidObjId(obj.to_string()))
    }

    fn get_missing_deps(&self, heads: &[ChangeHash]) -> Vec<ChangeHash> {
        let queued = self.queue.iter().map(|change| change.hash());
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
        doc.ops().dirty_runs().map(|run| run.range).collect()
    }

    fn ranges_contain(ranges: &[Range<usize>], needle: Range<usize>) -> bool {
        ranges
            .iter()
            .any(|range| range.start <= needle.start && needle.end <= range.end)
    }

    fn assert_patch_effects_match(
        doc: &Automerge,
        before: &[crate::ChangeHash],
        after: &[crate::ChangeHash],
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
        before: &[crate::ChangeHash],
        after: &[crate::ChangeHash],
    ) {
        let full = doc.diff(before, after);
        let dirty = doc.dirty_diff_patches(before, after).unwrap();
        assert_patch_effects_match(doc, before, after, "dirty diff", &dirty, "full diff", &full);
    }

    fn assert_incremental_effect_matches_full(
        doc: &mut Automerge,
        before: &[crate::ChangeHash],
        after: &[crate::ChangeHash],
    ) {
        let full = doc.diff(before, after);
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
        before: &[crate::ChangeHash],
        after: &[crate::ChangeHash],
    ) {
        let full = doc.document().diff(before, after);
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
    fn dirty_diff_patches_and_clear_keeps_dirty_bits_on_error() {
        let mut doc = Automerge::new();
        let mut tx = doc.transaction();
        let list = tx.put_object(ROOT, "list", crate::ObjType::List).unwrap();
        tx.insert(&list, 0, "a").unwrap();
        tx.commit();
        let before = doc.get_heads();

        let mut tx = doc.transaction();
        tx.put(&list, 0, "A").unwrap();
        tx.commit();
        let after = doc.get_heads();

        doc.ops_mut().clear_dirty();
        doc.ops_mut().mark_dirty(1);
        assert!(doc.dirty_diff_patches_and_clear(&before, &after).is_err());
        assert_eq!(dirty_ranges(&doc), vec![1..2]);
    }

    #[test]
    fn automerge_diff_incremental_empty_doc_and_repeated_calls_are_empty() {
        let mut doc = Automerge::new();

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
        let mut tx = source.transaction();
        tx.put(ROOT, "base", 1).unwrap();
        tx.commit();
        let base_heads = source.get_heads();
        let base_data = source.save();

        let mut doc = Automerge::new();
        doc.load_incremental(&base_data).unwrap();
        assert_incremental_effect_matches_full(&mut doc, &[], &base_heads);

        let mut tx = source.transaction();
        tx.put(ROOT, "later", 2).unwrap();
        tx.commit();
        let data = source.save_after(&base_heads);

        let before = doc.get_heads();
        doc.load_incremental(&data).unwrap();
        let after = doc.get_heads();
        assert_incremental_effect_matches_full(&mut doc, &before, &after);
        assert!(doc.ops().dirty_runs().next().is_none());
    }

    #[test]
    fn automerge_diff_incremental_after_apply_merge_and_sync_receive() {
        let mut source = Automerge::new();
        let mut tx = source.transaction();
        tx.put(ROOT, "key", 1).unwrap();
        tx.commit();

        let mut doc = Automerge::new();
        let before = doc.get_heads();
        doc.apply_changes(source.get_changes(&[])).unwrap();
        let after = doc.get_heads();
        assert_incremental_effect_matches_full(&mut doc, &before, &after);
        assert!(doc.ops().dirty_runs().next().is_none());

        let mut source = Automerge::new();
        let mut tx = source.transaction();
        tx.put(ROOT, "merged", 2).unwrap();
        tx.commit();

        let mut doc = Automerge::new();
        let before = doc.get_heads();
        doc.merge(&mut source).unwrap();
        let after = doc.get_heads();
        assert_incremental_effect_matches_full(&mut doc, &before, &after);
        assert!(doc.ops().dirty_runs().next().is_none());

        let mut source = Automerge::new();
        let mut tx = source.transaction();
        tx.put(ROOT, "synced", 3).unwrap();
        tx.commit();
        let mut sync_state = SyncState::new();
        let message = source.generate_sync_message(&mut sync_state).unwrap();

        let mut doc = Automerge::new();
        let before = doc.get_heads();
        doc.receive_sync_message(&mut SyncState::new(), message)
            .unwrap();
        let after = doc.get_heads();
        assert_incremental_effect_matches_full(&mut doc, &before, &after);
        assert!(doc.ops().dirty_runs().next().is_none());
    }

    #[test]
    fn automerge_diff_incremental_fork_inherits_cursor() {
        let mut doc = Automerge::new();
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
        let mut tx = doc1.transaction();
        tx.put(ROOT, "a", 1).unwrap();
        tx.put(ROOT, "c", 3).unwrap();
        tx.commit();
        let mut doc2 = doc1.fork();

        let mut tx = doc2.transaction();
        tx.put(ROOT, "a", 10).unwrap();
        tx.put(ROOT, "b", 2).unwrap();
        tx.commit();
        let changes = doc2.get_changes(&doc1.get_heads());

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
        let changes = doc2.get_changes(&doc1.get_heads());

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
        let changes = doc2.get_changes(&doc1.get_heads());

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
        let changes = doc2.get_changes(&doc1.get_heads());

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
        let changes = doc2.get_changes(&doc1.get_heads());

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
        let changes = doc2.get_changes(&doc1.get_heads());

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
        let changes = doc2.get_changes(&doc1.get_heads());

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
        let changes = doc2.get_changes(&doc1.get_heads());

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
        let mut doc2 = doc1.fork();

        let mut tx = doc2.transaction();
        let list = tx.put_object(ROOT, "todos", crate::ObjType::List).unwrap();
        tx.insert(&list, 0, "a").unwrap();
        tx.insert(&list, 1, "b").unwrap();
        tx.commit();
        let changes = doc2.get_changes(&doc1.get_heads());

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
        let changes = doc2.get_changes(&doc1.get_heads());

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
        let mut doc1 = Automerge::new().with_actor(ActorId::from([1]));
        let mut tx = doc1.transaction();
        let map = tx.put_object(ROOT, "map", crate::ObjType::Map).unwrap();
        tx.put(&map, "key", 1).unwrap();
        tx.commit();
        let before = doc1.get_heads();
        let mut doc2 = doc1.fork().with_actor(ActorId::from([2]));

        doc1.ops_mut().clear_dirty();
        let mut tx = doc1.transaction();
        tx.put(&map, "key", 2).unwrap();
        tx.put(&map, "other", 3).unwrap();
        tx.commit();

        let mut tx = doc2.transaction();
        tx.delete(ROOT, "map").unwrap();
        tx.commit();
        let changes = doc2.get_changes(&before);
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
        let changes = doc2.get_changes(&doc1.get_heads());

        doc1.ops_mut().clear_dirty();
        let before = doc1.get_heads();
        doc1.apply_changes(changes).unwrap();
        let after = doc1.get_heads();

        assert_dirty_diff_matches_full(&doc1, &before, &after);
    }

    #[test]
    fn remote_map_conflict_dirties_whole_key_register() {
        let mut doc1 = Automerge::new();
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
        let changes = doc2.get_changes(&doc1.get_heads());

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
        let mut doc2 = doc1.fork().with_actor(ActorId::from([2]));
        let mut doc3 = doc1.fork().with_actor(ActorId::from([3]));

        let mut tx = doc2.transaction();
        tx.put(ROOT, "key", "a").unwrap();
        tx.commit();

        let mut tx = doc3.transaction();
        tx.put(ROOT, "key", "b").unwrap();
        tx.commit();

        let mut changes = doc2.get_changes(&doc1.get_heads());
        changes.extend(doc3.get_changes(&doc1.get_heads()));
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
        let changes = doc2.get_changes(&doc1.get_heads());

        doc1.ops_mut().clear_dirty();
        let before = doc1.get_heads();
        doc1.apply_changes(changes).unwrap();
        let after = doc1.get_heads();

        assert_dirty_diff_matches_full(&doc1, &before, &after);
    }

    #[test]
    fn remote_list_conflict_dirties_whole_element_register() {
        let mut doc1 = Automerge::new();
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
        let changes = doc2.get_changes(&doc1.get_heads());

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
        let mut tx = doc1.transaction();
        let list = tx.put_object(ROOT, "list", crate::ObjType::List).unwrap();
        tx.commit();
        let mut doc2 = doc1.fork();

        let mut tx = doc2.transaction();
        tx.insert(&list, 0, "a").unwrap();
        tx.put(&list, 0, "A").unwrap();
        tx.commit();
        let changes = doc2.get_changes(&doc1.get_heads());

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
        let mut doc1 = Automerge::new().with_actor(ActorId::from([1]));
        let mut tx = doc1.transaction();
        tx.put(ROOT, "key", "base").unwrap();
        tx.commit();
        let mut doc2 = doc1.fork().with_actor(ActorId::from([2]));

        let mut tx = doc1.transaction();
        tx.put(ROOT, "key", "a").unwrap();
        tx.commit();

        let mut tx = doc2.transaction();
        tx.put(ROOT, "key", "b").unwrap();
        tx.commit();
        doc1.apply_changes(doc2.get_changes(&doc1.get_heads()))
            .unwrap();

        let mut tx = doc2.transaction();
        tx.delete(ROOT, "key").unwrap();
        tx.commit();
        let changes = doc2.get_changes(&doc1.get_heads());

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
        doc1.apply_changes(doc2.get_changes(&doc1.get_heads()))
            .unwrap();

        let mut tx = doc2.transaction();
        tx.delete(&list, 0).unwrap();
        tx.commit();
        let changes = doc2.get_changes(&doc1.get_heads());

        doc1.ops_mut().clear_dirty();
        let before = doc1.get_heads();
        doc1.apply_changes(changes).unwrap();
        let after = doc1.get_heads();

        assert_dirty_diff_matches_full(&doc1, &before, &after);
    }

    #[test]
    fn dirty_diff_matches_full_diff_for_remote_counter_increment() {
        let mut doc1 = Automerge::new();
        let mut tx = doc1.transaction();
        tx.put(ROOT, "counter", ScalarValue::counter(1)).unwrap();
        tx.commit();
        let mut doc2 = doc1.fork();

        let mut tx = doc2.transaction();
        tx.increment(ROOT, "counter", 2).unwrap();
        tx.commit();
        let changes = doc2.get_changes(&doc1.get_heads());

        doc1.ops_mut().clear_dirty();
        let before = doc1.get_heads();
        doc1.apply_changes(changes).unwrap();
        let after = doc1.get_heads();

        assert_dirty_diff_matches_full(&doc1, &before, &after);
    }

    #[test]
    fn dirty_diff_matches_full_diff_for_remote_text_insert() {
        let mut doc1 = Automerge::new();
        let mut tx = doc1.transaction();
        let text = tx.put_object(ROOT, "text", crate::ObjType::Text).unwrap();
        tx.splice_text(&text, 0, 0, "abc").unwrap();
        tx.commit();
        let mut doc2 = doc1.fork();

        let mut tx = doc2.transaction();
        tx.splice_text(&text, 1, 0, "X").unwrap();
        tx.commit();
        let changes = doc2.get_changes(&doc1.get_heads());

        doc1.ops_mut().clear_dirty();
        let before = doc1.get_heads();
        doc1.apply_changes(changes).unwrap();
        let after = doc1.get_heads();

        assert_dirty_diff_matches_full(&doc1, &before, &after);
    }

    #[test]
    fn dirty_diff_matches_full_diff_for_remote_mark() {
        let mut doc1 = Automerge::new();
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
        let changes = doc2.get_changes(&doc1.get_heads());

        doc1.ops_mut().clear_dirty();
        let before = doc1.get_heads();
        doc1.apply_changes(changes).unwrap();
        let after = doc1.get_heads();

        assert_dirty_diff_matches_full(&doc1, &before, &after);
    }

    #[test]
    fn dirty_diff_matches_full_diff_for_remote_middle_mark() {
        let mut doc1 = Automerge::new();
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
        let changes = doc2.get_changes(&doc1.get_heads());

        doc1.ops_mut().clear_dirty();
        let before = doc1.get_heads();
        doc1.apply_changes(changes).unwrap();
        let after = doc1.get_heads();
        let text_obj = doc1.exid_to_obj(&text).unwrap().id;
        let text_range = doc1.ops().scope_to_obj(&text_obj);
        assert!(doc1.ops().dirty_runs().any(|run| run.range == text_range));

        assert_dirty_diff_matches_full(&doc1, &before, &after);
    }

    #[test]
    fn dirty_diff_matches_full_diff_for_text_insert_without_marks() {
        let mut doc = Automerge::new();
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
        let changes = doc2.get_changes(&doc1.get_heads());

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
        let changes = doc2.get_changes(&doc1.get_heads());

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
        let changes = doc2.get_changes(&doc1.get_heads());

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
        let changes = doc2.get_changes(&doc1.get_heads());

        doc1.ops_mut().clear_dirty();
        let before = doc1.get_heads();
        doc1.apply_changes_batch(changes).unwrap();
        let after = doc1.get_heads();
        let text_obj = doc1.exid_to_obj(&text).unwrap().id;
        let text_range = doc1.ops().scope_to_obj(&text_obj);
        assert!(doc1.ops().dirty_runs().any(|run| run.range == text_range));

        assert_dirty_diff_matches_full(&doc1, &before, &after);
    }

    #[test]
    fn batch_multiple_text_edits_around_same_mark_match_full_diff() {
        let mut doc1 = Automerge::new();
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
        let changes = doc2.get_changes(&doc1.get_heads());

        doc1.ops_mut().clear_dirty();
        let before = doc1.get_heads();
        doc1.apply_changes_batch(changes).unwrap();
        let after = doc1.get_heads();

        assert_dirty_diff_matches_full(&doc1, &before, &after);
    }

    fn assert_sync_text_splice_around_mark_matches_full(index: usize, del: isize, value: &str) {
        let mut doc1 = Automerge::new();
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
        doc1.receive_sync_message(&mut SyncState::new(), message)
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
        doc1.receive_sync_message(&mut SyncState::new(), message)
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

        let full = doc.diff(&before, &after);
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
