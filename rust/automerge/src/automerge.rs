use std::cmp::Ordering;
use std::collections::{BTreeSet, HashMap, HashSet};
use std::fmt::Debug;
use std::num::NonZeroU64;
use std::ops::RangeBounds;

use itertools::Itertools;

use crate::change_graph::ChangeGraph;
use crate::columnar::Key as EncodedKey;
use crate::exid::ExId;
use crate::iter::{Keys, ListRange, MapRange, Spans, Values};
use crate::marks::{Mark, MarkAccumulator, MarkSet, MarkStateMachine};
use crate::op_set::{OpSet, OpSetData};
use crate::parents::Parents;
use crate::patches::{Patch, PatchLog, TextRepresentation};
use crate::query;
use crate::read::ReadDocInternal;
use crate::storage::{self, load, CompressConfig, VerificationMode};
use crate::transaction::{
    self, CommitOptions, Failure, Success, Transactable, Transaction, TransactionArgs,
};
use crate::types::{
    ActorId, ChangeHash, Clock, ElemId, Export, Exportable, Key, ListEncoding, MarkData, ObjId,
    ObjMeta, OpBuilder, OpId, OpIds, OpType, Value,
};
use crate::{hydrate, ScalarValue};
use crate::{AutomergeError, Change, Cursor, ObjType, Prop, ReadDoc};

pub(crate) mod current_state;
pub(crate) mod diff;

#[cfg(test)]
mod tests;

#[derive(Debug, Clone, PartialEq)]
pub(crate) enum Actor {
    Unused(ActorId),
    Cached(usize),
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
}

impl std::default::Default for LoadOptions<'static> {
    fn default() -> Self {
        Self {
            on_partial_load: OnPartialLoad::Error,
            verification_mode: VerificationMode::Check,
            patch_log: None,
            string_migration: StringMigration::NoMigration,
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
#[derive(Debug, Clone)]
pub struct Automerge {
    /// The list of unapplied changes that are not causally ready.
    queue: Vec<Change>,
    /// The history of changes that form this document, topologically sorted too.
    history: Vec<Change>,
    /// Mapping from change hash to index into the history list.
    history_index: HashMap<ChangeHash, usize>,
    /// Graph of changes
    change_graph: ChangeGraph,
    /// Mapping from actor index to list of seqs seen for them.
    states: HashMap<usize, Vec<usize>>,
    /// Current dependencies of this document (heads hashes).
    deps: HashSet<ChangeHash>,
    /// The set of operations that form this document.
    ops: OpSet,
    /// The current actor.
    actor: Actor,
    /// The maximum operation counter this document has seen.
    max_op: u64,
}

impl Automerge {
    /// Create a new document with a random actor id.
    pub fn new() -> Self {
        Automerge {
            queue: vec![],
            history: vec![],
            history_index: HashMap::new(),
            change_graph: ChangeGraph::new(),
            states: HashMap::new(),
            ops: Default::default(),
            deps: Default::default(),
            actor: Actor::Unused(ActorId::random()),
            max_op: 0,
        }
    }

    pub(crate) fn ops_mut(&mut self) -> &mut OpSet {
        &mut self.ops
    }

    pub(crate) fn ops(&self) -> &OpSet {
        &self.ops
    }

    pub(crate) fn osd(&self) -> &OpSetData {
        &self.ops.osd
    }

    /// Whether this document has any operations
    pub fn is_empty(&self) -> bool {
        self.history.is_empty() && self.queue.is_empty()
    }

    pub(crate) fn actor_id(&self) -> ActorId {
        match &self.actor {
            Actor::Unused(id) => id.clone(),
            Actor::Cached(idx) => self.ops.osd.actors[*idx].clone(),
        }
    }

    /// Set the actor id for this document.
    pub fn with_actor(mut self, actor: ActorId) -> Self {
        self.actor = Actor::Unused(actor);
        self
    }

    /// Set the actor id for this document.
    pub fn set_actor(&mut self, actor: ActorId) -> &mut Self {
        self.actor = Actor::Unused(actor);
        self
    }

    /// Get the current actor id of this document.
    pub fn get_actor(&self) -> &ActorId {
        match &self.actor {
            Actor::Unused(actor) => actor,
            Actor::Cached(index) => self.ops.osd.actors.get(*index),
        }
    }

    pub(crate) fn get_actor_index(&mut self) -> usize {
        match &mut self.actor {
            Actor::Unused(actor) => {
                let index = self
                    .ops
                    .osd
                    .actors
                    .cache(std::mem::replace(actor, ActorId::from(&[][..])));
                self.actor = Actor::Cached(index);
                index
            }
            Actor::Cached(index) => *index,
        }
    }

    /// Start a transaction.
    pub fn transaction(&mut self) -> Transaction<'_> {
        let args = self.transaction_args(None);
        Transaction::new(
            self,
            args,
            PatchLog::inactive(TextRepresentation::default()),
        )
    }

    /// Start a transaction which records changes in a [`PatchLog`]
    pub fn transaction_log_patches(&mut self, patch_log: PatchLog) -> Transaction<'_> {
        let args = self.transaction_args(None);
        Transaction::new(self, args, patch_log)
    }

    /// Start a transaction isolated at a given heads
    pub fn transaction_at(&mut self, patch_log: PatchLog, heads: &[ChangeHash]) -> Transaction<'_> {
        let args = self.transaction_args(Some(heads));
        Transaction::new(self, args, patch_log)
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
                actor_index = self.get_actor_index();
                seq = self.states.get(&actor_index).map_or(0, |v| v.len()) as u64 + 1;
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
        let start_op = NonZeroU64::new(self.max_op + 1).unwrap();
        let idx_range = self.osd().start_range();
        TransactionArgs {
            actor_index,
            seq,
            start_op,
            idx_range,
            deps,
            scope,
        }
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
    pub fn transact_and_log_patches<F, O, E>(
        &mut self,
        text_rep: TextRepresentation,
        f: F,
    ) -> transaction::Result<O, E>
    where
        F: FnOnce(&mut Transaction<'_>) -> Result<O, E>,
    {
        self.transact_and_log_patches_with_impl(text_rep, None::<&dyn Fn(&O) -> CommitOptions>, f)
    }

    /// Like [`Self::transact_and_log_patches()`] but with a function for generating the commit options
    pub fn transact_and_log_patches_with<F, O, E, C>(
        &mut self,
        text_rep: TextRepresentation,
        c: C,
        f: F,
    ) -> transaction::Result<O, E>
    where
        F: FnOnce(&mut Transaction<'_>) -> Result<O, E>,
        C: FnOnce(&O) -> CommitOptions,
    {
        self.transact_and_log_patches_with_impl(text_rep, Some(c), f)
    }

    fn transact_and_log_patches_with_impl<F, O, E, C>(
        &mut self,
        text_rep: TextRepresentation,
        c: Option<C>,
        f: F,
    ) -> transaction::Result<O, E>
    where
        F: FnOnce(&mut Transaction<'_>) -> Result<O, E>,
        C: FnOnce(&O) -> CommitOptions,
    {
        let mut tx = self.transaction_log_patches(PatchLog::active(text_rep));
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
        let mut seen = heads.iter().cloned().collect::<HashSet<_>>();
        let mut heads = heads.to_vec();
        let mut changes = vec![];
        while let Some(hash) = heads.pop() {
            if let Some(idx) = self.history_index.get(&hash) {
                let change = &self.history[*idx];
                for dep in change.deps() {
                    if !seen.contains(dep) {
                        heads.push(*dep);
                    }
                }
                changes.push(change);
                seen.insert(hash);
            } else {
                return Err(AutomergeError::InvalidHash(hash));
            }
        }
        let mut f = Self::new();
        f.set_actor(ActorId::random());
        f.apply_changes(changes.into_iter().rev().cloned())?;
        Ok(f)
    }

    pub(crate) fn exid_to_opid(&self, id: &ExId) -> Result<OpId, AutomergeError> {
        match id {
            ExId::Root => Ok(OpId::new(0, 0)),
            ExId::Id(ctr, actor, idx) => {
                let opid = if self.ops.osd.actors.cache.get(*idx) == Some(actor) {
                    OpId::new(*ctr, *idx)
                } else if let Some(backup_idx) = self.ops.osd.actors.lookup(actor) {
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
        } else if let Some(typ) = self.ops.obj_type(&id) {
            Ok(ObjMeta { id, typ })
        } else {
            Err(AutomergeError::NotAnObject)
        }
    }

    pub(crate) fn cursor_to_opid(
        &self,
        cursor: &Cursor,
        clock: Option<&Clock>,
    ) -> Result<OpId, AutomergeError> {
        if let Some(idx) = self.ops.osd.actors.lookup(cursor.actor()) {
            let opid = OpId::new(cursor.ctr(), idx);
            match clock {
                Some(clock) if !clock.covers(&opid) => {
                    Err(AutomergeError::InvalidCursor(cursor.clone()))
                }
                _ => Ok(opid),
            }
        } else {
            Err(AutomergeError::InvalidCursor(cursor.clone()))
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
    pub fn load_with_options<'a, 'b>(
        data: &'a [u8],
        options: LoadOptions<'b>,
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

        let mut change: Option<Change> = None;
        let mut first_chunk_was_doc = false;
        let mut am = match first_chunk {
            storage::Chunk::Document(d) => {
                tracing::trace!("first chunk is document chunk, inflating");
                first_chunk_was_doc = true;
                reconstruct_document(&d, options.verification_mode)?
            }
            storage::Chunk::Change(stored_change) => {
                tracing::trace!("first chunk is change chunk");
                change = Some(
                    Change::new_from_unverified(stored_change.into_owned(), None)
                        .map_err(|e| load::Error::InvalidChangeColumns(Box::new(e)))?,
                );
                Self::new()
            }
            storage::Chunk::CompressedChange(stored_change, compressed) => {
                tracing::trace!("first chunk is compressed change");
                change = Some(
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
        match load::load_changes(remaining.reset()) {
            load::LoadedChanges::Complete(c) => {
                am.apply_changes(change.into_iter().chain(c))?;
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
        if let Some(patch_log) = options.patch_log {
            if patch_log.is_active() {
                current_state::log_current_state_patches(&am, patch_log);
            }
        }
        Ok(am)
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
    pub fn current_state(&self, text_rep: TextRepresentation) -> Vec<Patch> {
        let mut patch_log = PatchLog::active(text_rep);
        current_state::log_current_state_patches(self, &mut patch_log);
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
        self.load_incremental_log_patches(
            data,
            &mut PatchLog::inactive(TextRepresentation::default()),
        )
    }

    /// Like [`Self::load_incremental()`] but log the changes to the current state of the document
    /// to [`PatchLog`]
    pub fn load_incremental_log_patches(
        &mut self,
        data: &[u8],
        patch_log: &mut PatchLog,
    ) -> Result<usize, AutomergeError> {
        if self.is_empty() {
            let mut doc = Self::load_with_options(
                data,
                LoadOptions::new()
                    .on_partial_load(OnPartialLoad::Ignore)
                    .verification_mode(VerificationMode::Check),
            )?;
            doc = doc.with_actor(self.actor_id());
            if patch_log.is_active() {
                current_state::log_current_state_patches(&doc, patch_log);
            }
            *self = doc;
            return Ok(self.ops.len());
        }
        let changes = match load::load_changes(storage::parse::Input::new(data)) {
            load::LoadedChanges::Complete(c) => c,
            load::LoadedChanges::Partial { error, loaded, .. } => {
                tracing::warn!(successful_chunks=loaded.len(), err=?error, "partial load");
                loaded
            }
        };
        let start = self.ops.len();
        self.apply_changes_log_patches(changes, patch_log)?;
        let delta = self.ops.len() - start;
        Ok(delta)
    }

    fn duplicate_seq(&self, change: &Change) -> bool {
        let mut dup = false;
        if let Some(actor_index) = self.ops.osd.actors.lookup(change.actor_id()) {
            if let Some(s) = self.states.get(&actor_index) {
                dup = s.len() >= change.seq() as usize;
            }
        }
        dup
    }

    /// Apply changes to this document.
    ///
    /// This is idempotent in the sense that if a change has already been applied it will be
    /// ignored.
    pub fn apply_changes(
        &mut self,
        changes: impl IntoIterator<Item = Change>,
    ) -> Result<(), AutomergeError> {
        self.apply_changes_log_patches(
            changes,
            &mut PatchLog::inactive(TextRepresentation::default()),
        )
    }

    /// Like [`Self::apply_changes()`] but log the resulting changes to the current state of the
    /// document to `patch_log`
    pub fn apply_changes_log_patches<I: IntoIterator<Item = Change>>(
        &mut self,
        changes: I,
        patch_log: &mut PatchLog,
    ) -> Result<(), AutomergeError> {
        // Record this so we can avoid observing each individual change and instead just observe
        // the final state after all the changes have been applied. We can only do this for an
        // empty document right now, once we have logic to produce the diffs between arbitrary
        // states of the OpSet we can make this cleaner.
        for c in changes {
            if !self.history_index.contains_key(&c.hash()) {
                if self.duplicate_seq(&c) {
                    return Err(AutomergeError::DuplicateSeqNumber(
                        c.seq(),
                        c.actor_id().clone(),
                    ));
                }
                if self.is_causally_ready(&c) {
                    self.apply_change(c, patch_log)?;
                } else {
                    self.queue.push(c);
                }
            }
        }
        while let Some(c) = self.pop_next_causally_ready_change() {
            if !self.history_index.contains_key(&c.hash()) {
                self.apply_change(c, patch_log)?;
            }
        }
        Ok(())
    }

    fn apply_change(
        &mut self,
        change: Change,
        patch_log: &mut PatchLog,
    ) -> Result<(), AutomergeError> {
        let ops = self.import_ops(&change);
        self.update_history(change, ops.len());
        for (obj, op, pred) in ops {
            self.insert_op(&obj, op, &pred, patch_log)?;
        }
        Ok(())
    }

    fn is_causally_ready(&self, change: &Change) -> bool {
        change
            .deps()
            .iter()
            .all(|d| self.history_index.contains_key(d))
    }

    fn pop_next_causally_ready_change(&mut self) -> Option<Change> {
        let mut index = 0;
        while index < self.queue.len() {
            if self.is_causally_ready(&self.queue[index]) {
                return Some(self.queue.swap_remove(index));
            }
            index += 1;
        }
        None
    }

    fn import_ops(&mut self, change: &Change) -> Vec<(ObjId, OpBuilder, OpIds)> {
        let actor = self.ops.osd.actors.cache(change.actor_id().clone());
        let mut actors = Vec::with_capacity(change.other_actor_ids().len() + 1);
        actors.push(actor);
        actors.extend(
            change
                .other_actor_ids()
                .iter()
                .map(|a| self.ops.osd.actors.cache(a.clone()))
                .collect::<Vec<_>>(),
        );
        change
            .iter_ops()
            .enumerate()
            .map(|(i, c)| {
                let id = OpId::new(change.start_op().get() + i as u64, actor);
                let key = match &c.key {
                    EncodedKey::Prop(n) => Key::Map(self.ops.osd.props.cache(n.to_string())),
                    EncodedKey::Elem(e) if e.is_head() => Key::Seq(ElemId::head()),
                    EncodedKey::Elem(ElemId(o)) => {
                        Key::Seq(ElemId(OpId::new(o.counter(), actors[o.actor()])))
                    }
                };
                let obj = if c.obj.is_root() {
                    ObjId::root()
                } else {
                    ObjId(OpId::new(
                        c.obj.opid().counter(),
                        actors[c.obj.opid().actor()],
                    ))
                };
                let pred = c
                    .pred
                    .iter()
                    .map(|p| OpId::new(p.counter(), actors[p.actor()]));
                let pred = self.ops.osd.sorted_opids(pred);
                (
                    obj,
                    OpBuilder {
                        id,
                        action: OpType::from_action_and_value(
                            c.action,
                            c.val,
                            c.mark_name,
                            c.expand,
                        ),
                        key,
                        insert: c.insert,
                    },
                    pred,
                )
            })
            .collect()
    }

    /// Takes all the changes in `other` which are not in `self` and applies them
    pub fn merge(&mut self, other: &mut Self) -> Result<Vec<ChangeHash>, AutomergeError> {
        self.merge_and_log_patches(
            other,
            &mut PatchLog::inactive(TextRepresentation::default()),
        )
    }

    /// Takes all the changes in `other` which are not in `self` and applies them whilst logging
    /// the resulting changes to the current state of the document to `patch_log`
    pub fn merge_and_log_patches(
        &mut self,
        other: &mut Self,
        patch_log: &mut PatchLog,
    ) -> Result<Vec<ChangeHash>, AutomergeError> {
        // TODO: Make this fallible and figure out how to do this transactionally
        let changes = self
            .get_changes_added(other)
            .into_iter()
            .cloned()
            .collect::<Vec<_>>();
        tracing::trace!(changes=?changes.iter().map(|c| c.hash()).collect::<Vec<_>>(), "merging new changes");
        self.apply_changes_log_patches(changes, patch_log)?;
        Ok(self.get_heads())
    }

    /// Save the entirety of this document in a compact form.
    pub fn save_with_options(&self, options: SaveOptions) -> Vec<u8> {
        let heads = self.get_heads();
        let c = self.history.iter();
        let compress = if options.deflate {
            None
        } else {
            Some(CompressConfig::None)
        };
        let mut bytes = crate::storage::save::save_document(
            c,
            self.ops.iter().map(|(objid, _, op)| (objid, op)),
            &self.ops.osd.actors,
            &self.ops.osd.props,
            &heads,
            compress,
        );
        if options.retain_orphans {
            for orphaned in self.queue.iter() {
                bytes.extend(orphaned.raw_bytes());
            }
        }
        bytes
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
            .filter(|hash| self.history_index.contains_key(hash))
            .copied()
            .collect::<Vec<_>>();

        self.change_graph.remove_ancestors(changes, &heads);

        Ok(())
    }

    /// Get the changes since `have_deps` in this document using a clock internally.
    fn get_changes_clock(&self, have_deps: &[ChangeHash]) -> Vec<&Change> {
        // get the clock for the given deps
        let clock = self.clock_at(have_deps);

        // get the documents current clock

        let mut change_indexes: Vec<usize> = Vec::new();
        // walk the state from the given deps clock and add them into the vec
        for (actor_index, actor_changes) in &self.states {
            if let Some(clock_data) = clock.get_for_actor(actor_index) {
                // find the change in this actors sequence of changes that corresponds to the max_op
                // recorded for them in the clock
                change_indexes.extend(&actor_changes[clock_data.seq as usize..]);
            } else {
                change_indexes.extend(&actor_changes[..]);
            }
        }

        // ensure the changes are still in sorted order
        change_indexes.sort_unstable();

        change_indexes
            .into_iter()
            .map(|i| &self.history[i])
            .collect()
    }

    /// Get the last change this actor made to the document.
    pub fn get_last_local_change(&self) -> Option<&Change> {
        return self
            .history
            .iter()
            .rev()
            .find(|c| c.actor_id() == self.get_actor());
    }

    pub(crate) fn clock_at(&self, heads: &[ChangeHash]) -> Clock {
        self.change_graph.clock_for_heads(heads)
    }

    fn get_isolated_actor_index(&mut self, level: usize) -> usize {
        if level == 0 {
            self.get_actor_index()
        } else {
            let base_actor = self.get_actor();
            let new_actor = base_actor.with_concurrency(level);
            self.ops.osd.actors.cache(new_actor)
        }
    }

    pub(crate) fn isolate_actor(&mut self, heads: &[ChangeHash]) -> Isolation {
        let mut clock = self.clock_at(heads);
        let mut actor_index = self.get_isolated_actor_index(0);

        for i in 1.. {
            let max_op = self.max_op_for_actor(actor_index);
            if max_op == 0 || clock.covers(&OpId::new(max_op, actor_index)) {
                clock.isolate(actor_index);
                break;
            }
            actor_index = self.get_isolated_actor_index(i);
        }

        let seq = self.states.get(&actor_index).map_or(0, |v| v.len()) as u64 + 1;

        Isolation {
            actor_index,
            seq,
            clock,
        }
    }

    fn get_hash(&self, actor: usize, seq: u64) -> Result<ChangeHash, AutomergeError> {
        self.states
            .get(&actor)
            .and_then(|v| v.get(seq as usize - 1))
            .and_then(|&i| self.history.get(i))
            .map(|c| c.hash())
            .ok_or(AutomergeError::InvalidSeq(seq))
    }

    fn max_op_for_actor(&mut self, actor_index: usize) -> u64 {
        self.states
            .get(&actor_index)
            .and_then(|s| s.last())
            .and_then(|index| self.history.get(*index))
            .map(|change| change.max_op())
            .unwrap_or(0)
    }

    pub(crate) fn update_history(&mut self, change: Change, num_ops: usize) -> usize {
        self.max_op = std::cmp::max(self.max_op, change.start_op().get() + num_ops as u64 - 1);

        self.update_deps(&change);

        let history_index = self.history.len();

        let actor_index = self.ops.osd.actors.cache(change.actor_id().clone());
        self.states
            .entry(actor_index)
            .or_default()
            .push(history_index);

        self.history_index.insert(change.hash(), history_index);
        self.change_graph
            .add_change(&change, actor_index)
            .expect("Change's deps should already be in the document");

        self.history.push(change);

        history_index
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
                .osd
                .actors
                .lookup(&actor)
                .ok_or_else(|| AutomergeError::InvalidObjId(s.to_owned()))?;
            let obj = ExId::Id(counter, self.ops.osd.actors.cache[actor].clone(), actor);
            Ok(obj)
        }
    }

    pub(crate) fn to_short_string<E: Exportable>(&self, id: E) -> String {
        match id.export() {
            Export::Id(id) => {
                let mut actor = self.ops.osd.actors[id.actor()].to_string();
                actor.truncate(6);
                format!("{}@{}", id.counter(), actor)
            }
            Export::Prop(index) => self.ops.osd.props[index].clone(),
            Export::Special(s) => s,
        }
    }

    pub fn dump(&self) {
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
        for (obj, _, op) in self.ops.iter() {
            let id = self.to_short_string(*op.id());
            let obj = self.to_short_string(obj);
            let key = match *op.key() {
                Key::Map(n) => self.ops.osd.props[n].clone(),
                Key::Seq(n) => self.to_short_string(n),
            };
            let value: String = match op.action() {
                OpType::Put(value) => format!("{}", value),
                OpType::Make(obj) => format!("make({})", obj),
                OpType::Increment(obj) => format!("inc({})", obj),
                OpType::Delete => format!("del{}", 0),
                OpType::MarkBegin(_, MarkData { name, value }) => {
                    format!("mark({},{})", name, value)
                }
                OpType::MarkEnd(_) => "/mark".to_string(),
            };
            let pred: Vec<_> = op.pred().map(|op| self.to_short_string(*op.id())).collect();
            let succ: Vec<_> = op.succ().map(|op| self.to_short_string(*op.id())).collect();
            let insert = match op.insert() {
                true => "t",
                false => "f",
            };
            log!(
                "  {:12} {:3} {:12} {:12} {:12} {:12?} {:12?}",
                id,
                insert,
                obj,
                key,
                value,
                pred,
                succ
            );
        }
    }

    /// Return a graphviz representation of the opset.
    ///
    /// # Arguments
    ///
    /// * objects: An optional list of object IDs to display, if not specified all objects are
    ///            visualised
    #[cfg(feature = "optree-visualisation")]
    pub fn visualise_optree(&self, objects: Option<Vec<ExId>>) -> String {
        let objects = objects.map(|os| {
            os.iter()
                .filter_map(|o| self.exid_to_obj(o).ok())
                .map(|o| o.id)
                .collect()
        });
        self.ops.visualise(objects)
    }

    pub(crate) fn insert_op(
        &mut self,
        obj: &ObjId,
        op: OpBuilder,
        pred: &OpIds,
        patch_log: &mut PatchLog,
    ) -> Result<(), AutomergeError> {
        let is_delete = op.is_delete();
        let idx = self.ops.load(*obj, op);
        let op = idx.as_op(&self.ops.osd);

        let (pos, succ) = if patch_log.is_active() {
            let obj = self.get_obj_meta(*obj)?;
            let found = self.ops.find_op_with_patch_log(
                &obj,
                patch_log.text_rep().encoding(obj.typ),
                op,
                pred,
            );
            found.log_patches(&obj, op, pred, self, patch_log);
            (found.pos, found.succ)
        } else {
            let found = self.ops.find_op_without_patch_log(obj, op, pred);
            (found.pos, found.succ)
        };

        self.ops.add_succ(obj, &succ, idx);

        if !is_delete {
            self.ops.insert(pos, obj, idx);
        }
        Ok(())
    }

    /// Create patches representing the change in the current state of the document between the
    /// `before` and `after` heads.  If the arguments are reverse it will observe the same changes
    /// in the opposite order.
    pub fn diff(
        &self,
        before_heads: &[ChangeHash],
        after_heads: &[ChangeHash],
        text_rep: TextRepresentation,
    ) -> Vec<Patch> {
        let before = self.clock_at(before_heads);
        let after = self.clock_at(after_heads);
        let mut patch_log = PatchLog::active(text_rep);
        diff::log_diff(self, &before, &after, &mut patch_log);
        patch_log.heads = Some(after_heads.to_vec());
        patch_log.make_patches(self)
    }

    /// Get the heads of this document.
    pub fn get_heads(&self) -> Vec<ChangeHash> {
        let mut deps: Vec<_> = self.deps.iter().copied().collect();
        deps.sort_unstable();
        deps
    }

    pub fn get_changes(&self, have_deps: &[ChangeHash]) -> Vec<&Change> {
        self.get_changes_clock(have_deps)
    }

    /// Get changes in `other` that are not in `self`
    pub fn get_changes_added<'a>(&self, other: &'a Self) -> Vec<&'a Change> {
        // Depth-first traversal from the heads through the dependency graph,
        // until we reach a change that is already present in other
        let mut stack: Vec<_> = other.get_heads();
        tracing::trace!(their_heads=?stack, "finding changes to merge");
        let mut seen_hashes = HashSet::new();
        let mut added_change_hashes = Vec::new();
        while let Some(hash) = stack.pop() {
            if !seen_hashes.contains(&hash) && self.get_change_by_hash(&hash).is_none() {
                seen_hashes.insert(hash);
                added_change_hashes.push(hash);
                if let Some(change) = other.get_change_by_hash(&hash) {
                    stack.extend(change.deps());
                }
            }
        }
        // Return those changes in the reverse of the order in which the depth-first search
        // found them. This is not necessarily a topological sort, but should usually be close.
        added_change_hashes.reverse();
        added_change_hashes
            .into_iter()
            .filter_map(|h| other.get_change_by_hash(&h))
            .collect()
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
                let actor_indices = self.states.get(&opid.actor())?;
                let change_index_index = actor_indices
                    .binary_search_by(|change_index| {
                        let change = self
                            .history
                            .get(*change_index)
                            .expect("State index should refer to a valid change");
                        let start = change.start_op().get();
                        let len = change.len() as u64;
                        if opid.counter() < start {
                            Ordering::Greater
                        } else if start + len <= opid.counter() {
                            Ordering::Less
                        } else {
                            Ordering::Equal
                        }
                    })
                    .ok()?;
                let change_index = actor_indices.get(change_index_index).unwrap();
                Some(self.history.get(*change_index).unwrap().hash())
            }
        }
    }

    fn calculate_marks(
        &self,
        obj: &ExId,
        clock: Option<Clock>,
    ) -> Result<Vec<Mark<'_>>, AutomergeError> {
        let obj = self.exid_to_obj(obj.as_ref())?;
        let ops_by_key = self.ops().iter_ops(&obj.id).chunk_by(|o| o.elemid_or_key());
        let mut index = 0;
        let mut marks = MarkStateMachine::default();
        let mut acc = MarkAccumulator::default();
        let mut last_marks = None;
        let mut mark_len = 0;
        let mut mark_index = 0;
        for (_key, key_ops) in ops_by_key.into_iter() {
            if let Some(o) = key_ops.filter(|o| o.visible_or_mark(clock.as_ref())).last() {
                match o.action() {
                    OpType::Make(_) | OpType::Put(_) => {
                        let len = o.width(TextRepresentation::String.encoding(obj.typ));
                        if last_marks.as_ref() != marks.current() {
                            match last_marks.as_ref() {
                                Some(m) if mark_len > 0 => acc.add(mark_index, mark_len, m),
                                _ => (),
                            }
                            last_marks = marks.current().cloned();
                            mark_index = index;
                            mark_len = 0;
                        }
                        mark_len += len;
                        index += len;
                    }
                    OpType::MarkBegin(_, data) => {
                        marks.mark_begin(*o.id(), data, &self.ops.osd);
                    }
                    OpType::MarkEnd(_) => {
                        marks.mark_end(*o.id(), &self.ops.osd);
                    }
                    OpType::Increment(_) | OpType::Delete => {}
                }
            }
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
        Ok(self
            .ops
            .parents(obj.id, TextRepresentation::default(), clock))
    }

    pub(crate) fn keys_for(&self, obj: &ExId, clock: Option<Clock>) -> Keys<'_> {
        self.exid_to_obj(obj)
            .ok()
            .map(|obj| self.ops.keys(&obj.id, clock))
            .unwrap_or_default()
    }

    pub(crate) fn map_range_for<'a, R: RangeBounds<String> + 'a>(
        &'a self,
        obj: &ExId,
        range: R,
        clock: Option<Clock>,
    ) -> MapRange<'a, R> {
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
    ) -> ListRange<'_, R> {
        self.exid_to_obj(obj)
            .ok()
            .map(|obj| {
                self.ops.list_range(
                    &obj.id,
                    range,
                    TextRepresentation::Array.encoding(obj.typ),
                    clock,
                )
            })
            .unwrap_or_default()
    }

    pub(crate) fn values_for(&self, obj: &ExId, clock: Option<Clock>) -> Values<'_> {
        self.exid_to_obj(obj)
            .ok()
            .map(|obj| Values::new(self.ops.top_ops(&obj.id, clock.clone()), clock))
            .unwrap_or_default()
    }

    pub(crate) fn length_for(&self, obj: &ExId, clock: Option<Clock>) -> usize {
        // FIXME - is doc.length() for a text always the string length?
        self.exid_to_obj(obj)
            .map(|obj| {
                self.ops
                    .length(&obj.id, TextRepresentation::String.encoding(obj.typ), clock)
            })
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
        let iter = self.ops.iter_obj(&obj.id);
        Ok(Spans::new(iter, self, clock))
    }

    pub(crate) fn get_cursor_for(
        &self,
        obj: &ExId,
        position: usize,
        clock: Option<Clock>,
    ) -> Result<Cursor, AutomergeError> {
        let obj = self.exid_to_obj(obj)?;
        if !obj.typ.is_sequence() {
            Err(AutomergeError::InvalidOp(obj.typ))
        } else {
            let found = self.ops.seek_ops_by_prop(
                &obj.id,
                position.into(),
                TextRepresentation::String.encoding(obj.typ),
                clock.as_ref(),
            );
            if let Some(op) = found.ops.last() {
                Ok(Cursor::new(*op.id(), &self.ops.osd))
            } else {
                Err(AutomergeError::InvalidIndex(position))
            }
        }
    }

    pub(crate) fn get_cursor_position_for(
        &self,
        obj: &ExId,
        cursor: &Cursor,
        clock: Option<Clock>,
    ) -> Result<usize, AutomergeError> {
        let obj = self.exid_to_obj(obj)?;
        let opid = self.cursor_to_opid(cursor, clock.as_ref())?;
        let found = self
            .ops
            .seek_list_opid(
                &obj.id,
                opid,
                TextRepresentation::String.encoding(obj.typ),
                clock.as_ref(),
            )
            .ok_or_else(|| AutomergeError::InvalidCursor(cursor.clone()))?;
        Ok(found.index)
    }

    pub(crate) fn marks_for(
        &self,
        obj: &ExId,
        clock: Option<Clock>,
    ) -> Result<Vec<Mark<'_>>, AutomergeError> {
        self.calculate_marks(obj, clock)
    }

    pub(crate) fn get_for(
        &self,
        obj: &ExId,
        prop: Prop,
        clock: Option<Clock>,
    ) -> Result<Option<(Value<'_>, ExId)>, AutomergeError> {
        let obj = self.exid_to_obj(obj)?;
        Ok(self
            .ops
            .seek_ops_by_prop(
                &obj.id,
                prop,
                TextRepresentation::String.encoding(obj.typ),
                clock.as_ref(),
            )
            .ops
            .into_iter()
            .last()
            .map(|op| op.tagged_value(clock.as_ref())))
    }

    pub(crate) fn get_all_for<O: AsRef<ExId>, P: Into<Prop>>(
        &self,
        obj: O,
        prop: P,
        clock: Option<Clock>,
    ) -> Result<Vec<(Value<'_>, ExId)>, AutomergeError> {
        let prop = prop.into();
        let obj = self.exid_to_obj(obj.as_ref())?;
        let values = self
            .ops
            .seek_ops_by_prop(
                &obj.id,
                prop,
                TextRepresentation::String.encoding(obj.typ),
                clock.as_ref(),
            )
            .ops
            .into_iter()
            .map(|op| op.tagged_value(clock.as_ref()))
            .collect::<Vec<_>>();
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
        let result = self
            .ops
            .search(
                &obj.id,
                query::Nth::new(
                    index,
                    TextRepresentation::String.encoding(obj.typ),
                    clock,
                    &self.ops.osd,
                )
                .with_marks(),
            )
            .marks()
            .as_deref()
            .cloned()
            .unwrap_or_default();
        Ok(result)
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
                    for op in ops {
                        let op = op.as_op(self.osd());
                        if !op.visible() {
                            continue;
                        }
                        if let OpType::Put(ScalarValue::Str(s)) = op.action() {
                            let prop = match *op.key() {
                                Key::Map(prop) => Prop::Map(self.ops.osd.props.get(prop).clone()),
                                Key::Seq(_) => {
                                    let Some(found) = self.ops.seek_list_opid(
                                        &obj.id,
                                        *op.id(),
                                        ListEncoding::List,
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
                                text: s.clone(),
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

    pub(crate) fn visible_obj_paths(
        &self,
        at: Option<&[ChangeHash]>,
    ) -> HashMap<ExId, Vec<(ExId, Prop)>> {
        let at = at.map(|heads| self.clock_at(heads));
        let mut paths = HashMap::<ExId, Vec<(ExId, Prop)>>::new();
        let mut visible_objs = HashSet::<crate::types::ObjId>::new();
        visible_objs.insert(crate::types::ObjId::root());
        paths.insert(ExId::Root, vec![]);

        for (obj, ops) in self.ops.iter_objs() {
            // Note that this works because the OpSet iterates in causal order,
            // which means that we have already seen the operation which
            // creates the object and added it to the visible_objs set if it
            // is visible.
            if !visible_objs.contains(&obj.id) {
                continue;
            }
            for op_idx in ops {
                let op = op_idx.as_op(self.osd());
                if op.visible_at(at.as_ref()) {
                    if let OpType::Make(_) = op.action() {
                        visible_objs.insert(op.id().into());
                        let (mut path, parent_obj_id) = if obj.id.is_root() {
                            (vec![], ExId::Root)
                        } else {
                            let parent_obj_id = self.ops.id_to_exid(obj.id.into());
                            (paths.get(&parent_obj_id).cloned().unwrap(), parent_obj_id)
                        };
                        let prop = match op.key() {
                            Key::Map(prop) => Prop::Map(self.ops.osd.props.get(*prop).clone()),
                            Key::Seq(_) => {
                                let encoding = match obj.typ {
                                    ObjType::Text => ListEncoding::Text,
                                    _ => ListEncoding::List,
                                };
                                let found = self
                                    .ops
                                    .seek_list_opid(&obj.id, *op.id(), encoding, at.as_ref())
                                    .unwrap();
                                Prop::Seq(found.index)
                            }
                        };
                        path.push((parent_obj_id.clone(), prop));
                        let obj_id = self.ops.id_to_exid(*op.id());
                        paths.insert(obj_id, path);
                    }
                }
            }
        }
        paths
    }

    /// Whether the peer represented by `other` has all the changes we have
    pub fn has_our_changes(&self, other: &crate::sync::State) -> bool {
        other.shared_heads == self.get_heads()
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

    fn map_range<'a, O: AsRef<ExId>, R: RangeBounds<String> + 'a>(
        &'a self,
        obj: O,
        range: R,
    ) -> MapRange<'a, R> {
        self.map_range_for(obj.as_ref(), range, None)
    }

    fn map_range_at<'a, O: AsRef<ExId>, R: RangeBounds<String> + 'a>(
        &'a self,
        obj: O,
        range: R,
        heads: &[ChangeHash],
    ) -> MapRange<'a, R> {
        let clock = self.clock_at(heads);
        self.map_range_for(obj.as_ref(), range, Some(clock))
    }

    fn list_range<O: AsRef<ExId>, R: RangeBounds<usize>>(
        &self,
        obj: O,
        range: R,
    ) -> ListRange<'_, R> {
        self.list_range_for(obj.as_ref(), range, None)
    }

    fn list_range_at<O: AsRef<ExId>, R: RangeBounds<usize>>(
        &self,
        obj: O,
        range: R,
        heads: &[ChangeHash],
    ) -> ListRange<'_, R> {
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

    fn get_cursor<O: AsRef<ExId>>(
        &self,
        obj: O,
        position: usize,
        at: Option<&[ChangeHash]>,
    ) -> Result<Cursor, AutomergeError> {
        let clock = at.map(|heads| self.clock_at(heads));
        self.get_cursor_for(obj.as_ref(), position, clock)
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

    fn marks<O: AsRef<ExId>>(&self, obj: O) -> Result<Vec<Mark<'_>>, AutomergeError> {
        self.marks_for(obj.as_ref(), None)
    }

    fn marks_at<O: AsRef<ExId>>(
        &self,
        obj: O,
        heads: &[ChangeHash],
    ) -> Result<Vec<Mark<'_>>, AutomergeError> {
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
        let in_queue: HashSet<_> = self.queue.iter().map(|change| change.hash()).collect();
        let mut missing = HashSet::new();

        for head in self.queue.iter().flat_map(|change| change.deps()) {
            if !self.history_index.contains_key(head) {
                missing.insert(head);
            }
        }

        for head in heads {
            if !self.history_index.contains_key(head) {
                missing.insert(head);
            }
        }

        let mut missing = missing
            .into_iter()
            .filter(|hash| !in_queue.contains(hash))
            .copied()
            .collect::<Vec<_>>();
        missing.sort();
        missing
    }

    fn get_change_by_hash(&self, hash: &ChangeHash) -> Option<&Change> {
        self.history_index
            .get(hash)
            .and_then(|index| self.history.get(*index))
    }

    fn stats(&self) -> crate::read::Stats {
        crate::read::Stats {
            num_changes: self.history.len() as u64,
            num_ops: self.ops.len() as u64,
        }
    }
}

impl ReadDocInternal for Automerge {
    fn live_obj_paths(&self) -> HashMap<ExId, Vec<(ExId, Prop)>> {
        self.visible_obj_paths(None)
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

pub(crate) fn reconstruct_document<'a>(
    doc: &'a storage::Document<'a>,
    mode: VerificationMode,
) -> Result<Automerge, AutomergeError> {
    let storage::load::ReconOpSet {
        changes,
        op_set,
        heads,
        max_op,
    } = storage::load::reconstruct_opset(doc, mode)
        .map_err(|e| load::Error::InflateDocument(Box::new(e)))?;

    let mut hashes_by_index = HashMap::new();
    let mut actor_to_history: HashMap<usize, Vec<usize>> = HashMap::new();
    let mut change_graph = ChangeGraph::new();
    for (index, change) in changes.iter().enumerate() {
        // SAFETY: This should be fine because we just constructed an opset containing
        // all the changes
        let actor_index = op_set.osd.actors.lookup(change.actor_id()).unwrap();
        actor_to_history.entry(actor_index).or_default().push(index);
        hashes_by_index.insert(index, change.hash());
        change_graph.add_change(change, actor_index)?;
    }
    let history_index = hashes_by_index.into_iter().map(|(k, v)| (v, k)).collect();
    Ok(Automerge {
        queue: vec![],
        history: changes,
        history_index,
        states: actor_to_history,
        change_graph,
        ops: op_set,
        deps: heads.into_iter().collect(),
        actor: Actor::Unused(ActorId::random()),
        max_op,
    })
}
