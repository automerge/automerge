use std::cmp::Ordering;
use std::collections::{BTreeSet, HashMap, HashSet};
use std::fmt::Debug;
use std::num::NonZeroU64;
use std::ops::RangeBounds;

use itertools::Itertools;

use crate::change_graph::ChangeGraph;
use crate::columnar::Key as EncodedKey;
use crate::exid::ExId;
use crate::iter::{Keys, ListRange, MapRange, Values};
use crate::marks::{Mark, MarkStateMachine};
use crate::op_observer::{BranchableObserver, OpObserver};
use crate::op_set::OpSet;
use crate::parents::Parents;
use crate::storage::{self, load, CompressConfig, VerificationMode};
use crate::transaction::{
    self, CommitOptions, Failure, Observed, Success, Transaction, TransactionArgs, UnObserved,
};
use crate::types::{
    ActorId, ChangeHash, Clock, ElemId, Export, Exportable, Key, MarkData, ObjId, ObjMeta, Op,
    OpId, OpType, Value,
};
use crate::{AutomergeError, Change, Cursor, ObjType, Prop, ReadDoc};

mod current_state;
mod diff;

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

/// An automerge document which does not manage transactions for you.
///
/// ## Creating, loading, merging and forking documents
///
/// A new document can be created with [`Self::new`], which will create a document with a random
/// [`ActorId`]. Existing documents can be loaded with [`Self::load`], or [`Self::load_with`].
///
/// If you have two documents and you want to merge the changes from one into the other you can use
/// [`Self::merge`] or [`Self::merge_with`].
///
/// If you have a document you want to split into two concurrent threads of execution you can use
/// [`Self::fork`]. If you want to split a document from ealier in its history you can use
/// [`Self::fork_at`].
///
/// ## Reading values
///
/// [`Self`] implements [`ReadDoc`], which provides methods for reading values from the document.
///
/// ## Modifying a document (Transactions)
///
/// [`Automerge`] provides an interface for viewing and modifying automerge documents which does
/// not manage transactions for you. To create changes you use either [`Automerge::transaction`] or
/// [`Automerge::transact`] (or the `_with` variants).
///
/// ## Sync
///
/// This type implements [`crate::sync::SyncDoc`]
///
/// ## Observers
///
/// Many of the methods on this type have an `_with` or `_observed` variant
/// which allow you to pass in an [`OpObserver`] to observe any changes which
/// occur.
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
    /// Heads at the last save.
    saved: Vec<ChangeHash>,
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
            saved: Default::default(),
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

    /// Whether this document has any operations
    pub fn is_empty(&self) -> bool {
        self.history.is_empty() && self.queue.is_empty()
    }

    pub(crate) fn actor_id(&self) -> ActorId {
        match &self.actor {
            Actor::Unused(id) => id.clone(),
            Actor::Cached(idx) => self.ops.m.actors[*idx].clone(),
        }
    }

    /// Remove the current actor from the opset if it has no ops
    ///
    /// If the current actor ID has no ops in the opset then remove it from the cache of actor IDs.
    /// This us used when rolling back a transaction. If the rolled back ops are the only ops for
    /// the current actor then we want to remove that actor from the opset so it doesn't end up in
    /// any saved version of the document.
    ///
    /// # Panics
    ///
    /// If the last actor in the OpSet is not the actor ID of this document
    pub(crate) fn rollback_last_actor(&mut self) {
        if let Actor::Cached(actor_idx) = self.actor {
            if self.states.get(&actor_idx).is_none() && self.ops.m.actors.len() > 0 {
                assert!(self.ops.m.actors.len() == actor_idx + 1);
                let actor = self.ops.m.actors.remove_last();
                self.actor = Actor::Unused(actor);
            }
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
            Actor::Cached(index) => self.ops.m.actors.get(*index),
        }
    }

    pub(crate) fn get_actor_index(&mut self) -> usize {
        match &mut self.actor {
            Actor::Unused(actor) => {
                let index = self
                    .ops
                    .m
                    .actors
                    .cache(std::mem::replace(actor, ActorId::from(&[][..])));
                self.actor = Actor::Cached(index);
                index
            }
            Actor::Cached(index) => *index,
        }
    }

    /// Start a transaction.
    pub fn transaction(&mut self) -> Transaction<'_, UnObserved> {
        let args = self.transaction_args();
        Transaction::new(self, args, UnObserved)
    }

    /// Start a transaction with an observer
    pub fn transaction_with_observer<Obs: OpObserver + BranchableObserver>(
        &mut self,
        op_observer: Obs,
    ) -> Transaction<'_, Observed<Obs>> {
        let args = self.transaction_args();
        Transaction::new(self, args, Observed::new(op_observer))
    }

    pub(crate) fn transaction_args(&mut self) -> TransactionArgs {
        let actor = self.get_actor_index();
        let seq = self.states.get(&actor).map_or(0, |v| v.len()) as u64 + 1;
        let mut deps = self.get_heads();
        if seq > 1 {
            let last_hash = self.get_hash(actor, seq - 1).unwrap();
            if !deps.contains(&last_hash) {
                deps.push(last_hash);
            }
        }
        // SAFETY: this unwrap is safe as we always add 1
        let start_op = NonZeroU64::new(self.max_op + 1).unwrap();

        TransactionArgs {
            actor_index: actor,
            seq,
            start_op,
            deps,
        }
    }

    /// Run a transaction on this document in a closure, automatically handling commit or rollback
    /// afterwards.
    pub fn transact<F, O, E>(&mut self, f: F) -> transaction::Result<O, (), E>
    where
        F: FnOnce(&mut Transaction<'_, UnObserved>) -> Result<O, E>,
    {
        self.transact_with_impl(None::<&dyn Fn(&O) -> CommitOptions>, f)
    }

    /// Like [`Self::transact`] but with a function for generating the commit options.
    pub fn transact_with<F, O, E, C>(&mut self, c: C, f: F) -> transaction::Result<O, (), E>
    where
        F: FnOnce(&mut Transaction<'_, UnObserved>) -> Result<O, E>,
        C: FnOnce(&O) -> CommitOptions,
    {
        self.transact_with_impl(Some(c), f)
    }

    fn transact_with_impl<F, O, E, C>(
        &mut self,
        c: Option<C>,
        f: F,
    ) -> transaction::Result<O, (), E>
    where
        F: FnOnce(&mut Transaction<'_, UnObserved>) -> Result<O, E>,
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
                Ok(Success {
                    result,
                    hash,
                    op_observer: (),
                })
            }
            Err(error) => Err(Failure {
                error,
                cancelled: tx.rollback(),
            }),
        }
    }

    /// Run a transaction on this document in a closure, observing ops with `Obs`, automatically handling commit or rollback
    /// afterwards.
    pub fn transact_observed<F, O, E, Obs>(&mut self, f: F) -> transaction::Result<O, Obs, E>
    where
        F: FnOnce(&mut Transaction<'_, Observed<Obs>>) -> Result<O, E>,
        Obs: OpObserver + BranchableObserver + Default,
    {
        self.transact_observed_with_impl(None::<&dyn Fn(&O) -> CommitOptions>, f)
    }

    /// Like [`Self::transact_observed`] but with a function for generating the commit options
    pub fn transact_observed_with<F, O, E, C, Obs>(
        &mut self,
        c: C,
        f: F,
    ) -> transaction::Result<O, Obs, E>
    where
        F: FnOnce(&mut Transaction<'_, Observed<Obs>>) -> Result<O, E>,
        C: FnOnce(&O) -> CommitOptions,
        Obs: OpObserver + BranchableObserver + Default,
    {
        self.transact_observed_with_impl(Some(c), f)
    }

    fn transact_observed_with_impl<F, O, Obs, E, C>(
        &mut self,
        c: Option<C>,
        f: F,
    ) -> transaction::Result<O, Obs, E>
    where
        F: FnOnce(&mut Transaction<'_, Observed<Obs>>) -> Result<O, E>,
        C: FnOnce(&O) -> CommitOptions,
        Obs: OpObserver + BranchableObserver + Default,
    {
        let observer = Obs::default();
        let mut tx = self.transaction_with_observer(observer);
        let result = f(&mut tx);
        match result {
            Ok(result) => {
                let (obs, hash) = if let Some(c) = c {
                    let commit_options = c(&result);
                    tx.commit_with(commit_options)
                } else {
                    tx.commit()
                };
                Ok(Success {
                    result,
                    hash,
                    op_observer: obs,
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
        let args = self.transaction_args();
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
                let opid = if self.ops.m.actors.cache.get(*idx) == Some(actor) {
                    OpId::new(*ctr, *idx)
                } else if let Some(backup_idx) = self.ops.m.actors.lookup(actor) {
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
        } else if let Some((typ, encoding)) = self.ops.type_and_encoding(&id) {
            Ok(ObjMeta { id, typ, encoding })
        } else {
            Err(AutomergeError::NotAnObject)
        }
    }

    pub(crate) fn cursor_to_opid(
        &self,
        cursor: &Cursor,
        clock: Option<&Clock>,
    ) -> Result<OpId, AutomergeError> {
        if let Some(idx) = self.ops.m.actors.lookup(cursor.actor()) {
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

    pub(crate) fn export_value<'a>(&self, op: &'a Op, clock: Option<&Clock>) -> (Value<'a>, ExId) {
        (op.value_at(clock), self.id_to_exid(op.id))
    }

    pub(crate) fn id_to_exid(&self, id: OpId) -> ExId {
        self.ops.id_to_exid(id)
    }

    /// Load a document.
    pub fn load(data: &[u8]) -> Result<Self, AutomergeError> {
        Self::load_with::<()>(data, OnPartialLoad::Error, VerificationMode::Check, None)
    }

    /// Load a document without verifying the head hashes
    ///
    /// This is useful for debugging as it allows you to examine a corrupted document.
    pub fn load_unverified_heads(data: &[u8]) -> Result<Self, AutomergeError> {
        Self::load_with::<()>(
            data,
            OnPartialLoad::Error,
            VerificationMode::DontCheck,
            None,
        )
    }

    /// Load a document with an observer
    #[tracing::instrument(skip(data, observer), err)]
    pub fn load_with<Obs: OpObserver>(
        data: &[u8],
        on_error: OnPartialLoad,
        mode: VerificationMode,
        mut observer: Option<&mut Obs>,
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
        let mut am = match first_chunk {
            storage::Chunk::Document(d) => {
                tracing::trace!("first chunk is document chunk, inflating");
                let storage::load::Reconstructed {
                    max_op,
                    result: op_set,
                    changes,
                    heads,
                } = storage::load::reconstruct_document(&d, mode, OpSet::builder())
                    .map_err(|e| load::Error::InflateDocument(Box::new(e)))?;
                let mut hashes_by_index = HashMap::new();
                let mut actor_to_history: HashMap<usize, Vec<usize>> = HashMap::new();
                let mut change_graph = ChangeGraph::new();
                for (index, change) in changes.iter().enumerate() {
                    // SAFETY: This should be fine because we just constructed an opset containing
                    // all the changes
                    let actor_index = op_set.m.actors.lookup(change.actor_id()).unwrap();
                    actor_to_history.entry(actor_index).or_default().push(index);
                    hashes_by_index.insert(index, change.hash());
                    change_graph.add_change(change, actor_index)?;
                }
                let history_index = hashes_by_index.into_iter().map(|(k, v)| (v, k)).collect();
                Self {
                    queue: vec![],
                    history: changes,
                    history_index,
                    states: actor_to_history,
                    change_graph,
                    ops: op_set,
                    deps: heads.into_iter().collect(),
                    saved: Default::default(),
                    actor: Actor::Unused(ActorId::random()),
                    max_op,
                }
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
                if !am.queue.is_empty() {
                    return Err(AutomergeError::MissingDeps);
                }
            }
            load::LoadedChanges::Partial { error, .. } => {
                if on_error == OnPartialLoad::Error {
                    return Err(error.into());
                }
            }
        }
        if let Some(observer) = &mut observer {
            current_state::observe_current_state(&am, *observer);
        }
        Ok(am)
    }

    /// Load an incremental save of a document.
    ///
    /// Unlike `load` this imports changes into an existing document. It will work with both the
    /// output of [`Self::save`] and [`Self::save_incremental`]
    ///
    /// The return value is the number of ops which were applied, this is not useful and will
    /// change in future.
    pub fn load_incremental(&mut self, data: &[u8]) -> Result<usize, AutomergeError> {
        self.load_incremental_with::<()>(data, None)
    }

    /// Like [`Self::load_incremental`] but with an observer
    pub fn load_incremental_with<Obs: OpObserver>(
        &mut self,
        data: &[u8],
        op_observer: Option<&mut Obs>,
    ) -> Result<usize, AutomergeError> {
        if self.is_empty() {
            let mut doc =
                Self::load_with::<()>(data, OnPartialLoad::Ignore, VerificationMode::Check, None)?;
            doc = doc.with_actor(self.actor_id());
            if let Some(obs) = op_observer {
                current_state::observe_current_state(&doc, obs);
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
        self.apply_changes_with(changes, op_observer)?;
        let delta = self.ops.len() - start;
        Ok(delta)
    }

    fn duplicate_seq(&self, change: &Change) -> bool {
        let mut dup = false;
        if let Some(actor_index) = self.ops.m.actors.lookup(change.actor_id()) {
            if let Some(s) = self.states.get(&actor_index) {
                dup = s.len() >= change.seq() as usize;
            }
        }
        dup
    }

    /// Apply changes to this document.
    ///
    /// This is idemptotent in the sense that if a change has already been applied it will be
    /// ignored.
    pub fn apply_changes(
        &mut self,
        changes: impl IntoIterator<Item = Change>,
    ) -> Result<(), AutomergeError> {
        self.apply_changes_with::<_, ()>(changes, None)
    }

    /// Like [`Self::apply_changes`] but with an observer
    pub fn apply_changes_with<I: IntoIterator<Item = Change>, Obs: OpObserver>(
        &mut self,
        changes: I,
        mut op_observer: Option<&mut Obs>,
    ) -> Result<(), AutomergeError> {
        // Record this so we can avoid observing each individual change and instead just observe
        // the final state after all the changes have been applied. We can only do this for an
        // empty document right now, once we have logic to produce the diffs between arbitrary
        // states of the OpSet we can make this cleaner.
        let empty_at_start = self.is_empty();
        for c in changes {
            if !self.history_index.contains_key(&c.hash()) {
                if self.duplicate_seq(&c) {
                    return Err(AutomergeError::DuplicateSeqNumber(
                        c.seq(),
                        c.actor_id().clone(),
                    ));
                }
                if self.is_causally_ready(&c) {
                    if empty_at_start {
                        self.apply_change::<()>(c, &mut None)?;
                    } else {
                        self.apply_change(c, &mut op_observer)?;
                    }
                } else {
                    self.queue.push(c);
                }
            }
        }
        while let Some(c) = self.pop_next_causally_ready_change() {
            if !self.history_index.contains_key(&c.hash()) {
                if empty_at_start {
                    self.apply_change::<()>(c, &mut None)?;
                } else {
                    self.apply_change(c, &mut op_observer)?;
                }
            }
        }
        if empty_at_start {
            if let Some(observer) = &mut op_observer {
                current_state::observe_current_state(self, *observer);
            }
        }
        Ok(())
    }

    fn apply_change<Obs: OpObserver>(
        &mut self,
        change: Change,
        observer: &mut Option<&mut Obs>,
    ) -> Result<(), AutomergeError> {
        let ops = self.import_ops(&change);
        self.update_history(change, ops.len());
        for (obj, op) in ops {
            self.insert_op(&obj, op, observer)?;
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

    fn import_ops(&mut self, change: &Change) -> Vec<(ObjId, Op)> {
        let actor = self.ops.m.actors.cache(change.actor_id().clone());
        let mut actors = Vec::with_capacity(change.other_actor_ids().len() + 1);
        actors.push(actor);
        actors.extend(
            change
                .other_actor_ids()
                .iter()
                .map(|a| self.ops.m.actors.cache(a.clone()))
                .collect::<Vec<_>>(),
        );
        change
            .iter_ops()
            .enumerate()
            .map(|(i, c)| {
                let id = OpId::new(change.start_op().get() + i as u64, actor);
                let key = match &c.key {
                    EncodedKey::Prop(n) => Key::Map(self.ops.m.props.cache(n.to_string())),
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
                let pred = self.ops.m.sorted_opids(pred);
                (
                    obj,
                    Op {
                        id,
                        action: OpType::from_action_and_value(
                            c.action,
                            c.val,
                            c.mark_name,
                            c.expand,
                        ),
                        key,
                        succ: Default::default(),
                        pred,
                        insert: c.insert,
                    },
                )
            })
            .collect()
    }

    /// Takes all the changes in `other` which are not in `self` and applies them
    pub fn merge(&mut self, other: &mut Self) -> Result<Vec<ChangeHash>, AutomergeError> {
        self.merge_with::<()>(other, None)
    }

    /// Takes all the changes in `other` which are not in `self` and applies them
    pub fn merge_with<Obs: OpObserver>(
        &mut self,
        other: &mut Self,
        op_observer: Option<&mut Obs>,
    ) -> Result<Vec<ChangeHash>, AutomergeError> {
        // TODO: Make this fallible and figure out how to do this transactionally
        let changes = self
            .get_changes_added(other)
            .into_iter()
            .cloned()
            .collect::<Vec<_>>();
        tracing::trace!(changes=?changes.iter().map(|c| c.hash()).collect::<Vec<_>>(), "merging new changes");
        self.apply_changes_with(changes, op_observer)?;
        Ok(self.get_heads())
    }

    /// Save the entirety of this document in a compact form.
    ///
    /// This takes a mutable reference to self because it saves the heads of the last save so that
    /// `save_incremental` can be used to produce only the changes since the last `save`. This API
    /// will be changing in future.
    pub fn save(&mut self) -> Vec<u8> {
        let heads = self.get_heads();
        let c = self.history.iter();
        let bytes = crate::storage::save::save_document(
            c,
            self.ops.iter().map(|(objid, _, op)| (objid, op)),
            &self.ops.m.actors,
            &self.ops.m.props,
            &heads,
            None,
        );
        self.saved = self.get_heads();
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
        let heads = self.get_heads();
        let c = self.history.iter();
        let bytes = crate::storage::save::save_document(
            c,
            self.ops.iter().map(|(objid, _, op)| (objid, op)),
            &self.ops.m.actors,
            &self.ops.m.props,
            &heads,
            Some(CompressConfig::None),
        );
        self.saved = self.get_heads();
        bytes
    }

    /// Save the changes since the last call to [Self::save`]
    ///
    /// The output of this will not be a compressed document format, but a series of individual
    /// changes. This is useful if you know you have only made a small change since the last `save`
    /// and you want to immediately send it somewhere (e.g. you've inserted a single character in a
    /// text object).
    pub fn save_incremental(&mut self) -> Vec<u8> {
        let changes = self
            .get_changes(self.saved.as_slice())
            .expect("Should only be getting changes using previously saved heads");
        let mut bytes = vec![];
        for c in changes {
            bytes.extend(c.raw_bytes());
        }
        if !bytes.is_empty() {
            self.saved = self.get_heads()
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
    fn get_changes_clock(&self, have_deps: &[ChangeHash]) -> Result<Vec<&Change>, AutomergeError> {
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

        Ok(change_indexes
            .into_iter()
            .map(|i| &self.history[i])
            .collect())
    }

    /// Get the last change this actor made to the document.
    pub fn get_last_local_change(&self) -> Option<&Change> {
        return self
            .history
            .iter()
            .rev()
            .find(|c| c.actor_id() == self.get_actor());
    }

    fn clock_at(&self, heads: &[ChangeHash]) -> Clock {
        self.change_graph.clock_for_heads(heads)
    }

    fn get_hash(&self, actor: usize, seq: u64) -> Result<ChangeHash, AutomergeError> {
        self.states
            .get(&actor)
            .and_then(|v| v.get(seq as usize - 1))
            .and_then(|&i| self.history.get(i))
            .map(|c| c.hash())
            .ok_or(AutomergeError::InvalidSeq(seq))
    }

    pub(crate) fn update_history(&mut self, change: Change, num_ops: usize) -> usize {
        self.max_op = std::cmp::max(self.max_op, change.start_op().get() + num_ops as u64 - 1);

        self.update_deps(&change);

        let history_index = self.history.len();

        let actor_index = self.ops.m.actors.cache(change.actor_id().clone());
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
                .m
                .actors
                .lookup(&actor)
                .ok_or_else(|| AutomergeError::InvalidObjId(s.to_owned()))?;
            let obj = ExId::Id(counter, self.ops.m.actors.cache[actor].clone(), actor);
            Ok(obj)
        }
    }

    pub(crate) fn to_short_string<E: Exportable>(&self, id: E) -> String {
        match id.export() {
            Export::Id(id) => {
                let mut actor = self.ops.m.actors[id.actor()].to_string();
                actor.truncate(6);
                format!("{}@{}", id.counter(), actor)
            }
            Export::Prop(index) => self.ops.m.props[index].clone(),
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
            let id = self.to_short_string(op.id);
            let obj = self.to_short_string(obj);
            let key = match op.key {
                Key::Map(n) => self.ops.m.props[n].clone(),
                Key::Seq(n) => self.to_short_string(n),
            };
            let value: String = match &op.action {
                OpType::Put(value) => format!("{}", value),
                OpType::Make(obj) => format!("make({})", obj),
                OpType::Increment(obj) => format!("inc({})", obj),
                OpType::Delete => format!("del{}", 0),
                OpType::MarkBegin(_, MarkData { name, value }) => {
                    format!("mark({},{})", name, value)
                }
                OpType::MarkEnd(_) => "/mark".to_string(),
            };
            let pred: Vec<_> = op.pred.iter().map(|id| self.to_short_string(*id)).collect();
            let succ: Vec<_> = op
                .succ
                .into_iter()
                .map(|id| self.to_short_string(*id))
                .collect();
            let insert = match op.insert {
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

    pub(crate) fn insert_op<Obs: OpObserver>(
        &mut self,
        obj: &ObjId,
        op: Op,
        observer: &mut Option<&mut Obs>,
    ) -> Result<(), AutomergeError> {
        let (pos, succ) = if let Some(observer) = observer {
            let obj = self.get_obj_meta(*obj)?;
            let found = self.ops.find_op_with_observer(&obj, &op);
            found.observe(&obj, &op, self, *observer);
            (found.pos, found.succ)
        } else {
            let found = self.ops.find_op_without_observer(obj, &op);
            (found.pos, found.succ)
        };

        self.ops.add_succ(obj, &succ, &op);

        if !op.is_delete() {
            self.ops.insert(pos, obj, op);
        }
        Ok(())
    }

    /// Observe changes in the document between the 'before'
    /// and 'after' heads.  If the arguments are reverse it will
    /// observe the same changes in the opposite order.
    pub fn observe_diff<Obs: OpObserver>(
        &self,
        before: &[ChangeHash],
        after: &[ChangeHash],
        observer: &mut Obs,
    ) -> Result<(), AutomergeError> {
        diff::observe_diff(self, before, after, observer);
        Ok(())
    }

    /// Get the heads of this document.
    pub fn get_heads(&self) -> Vec<ChangeHash> {
        let mut deps: Vec<_> = self.deps.iter().copied().collect();
        deps.sort_unstable();
        deps
    }

    pub fn get_changes(&self, have_deps: &[ChangeHash]) -> Result<Vec<&Change>, AutomergeError> {
        self.get_changes_clock(have_deps)
    }

    /// Get changes in `other` that are not in `self
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

    /// Get the hash of the change that contains the given opid.
    ///
    /// Returns none if the opid:
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

    fn calculate_marks<O: AsRef<ExId>>(
        &self,
        obj: O,
        heads: Option<&[ChangeHash]>,
    ) -> Result<Vec<Mark<'_>>, AutomergeError> {
        let obj = self.exid_to_obj(obj.as_ref())?;
        let clock = heads.map(|heads| self.clock_at(heads));
        let ops_by_key = self.ops().iter_ops(&obj.id).group_by(|o| o.elemid_or_key());
        let mut index = 0;
        let mut marks = MarkStateMachine::default();

        Ok(ops_by_key
            .into_iter()
            .filter_map(|(_key, key_ops)| {
                key_ops
                    .filter(|o| o.visible_or_mark(clock.as_ref()))
                    .last()
                    .and_then(|o| match &o.action {
                        OpType::Make(_) | OpType::Put(_) => {
                            index += o.width(obj.encoding);
                            None
                        }
                        OpType::MarkBegin(_, data) => marks.mark_begin(o.id, index, data, self),
                        OpType::MarkEnd(_) => marks.mark_end(o.id, index, self),
                        OpType::Increment(_) | OpType::Delete => None,
                    })
            })
            .collect())
    }
}

impl ReadDoc for Automerge {
    fn parents<O: AsRef<ExId>>(&self, obj: O) -> Result<Parents<'_>, AutomergeError> {
        let obj = self.exid_to_obj(obj.as_ref())?;
        Ok(self.ops.parents(obj.id, None))
    }

    fn parents_at<O: AsRef<ExId>>(
        &self,
        obj: O,
        heads: &[ChangeHash],
    ) -> Result<Parents<'_>, AutomergeError> {
        let obj = self.exid_to_obj(obj.as_ref())?;
        let clock = self.clock_at(heads);
        Ok(self.ops.parents(obj.id, Some(clock)))
    }

    fn keys<O: AsRef<ExId>>(&self, obj: O) -> Keys<'_> {
        self.exid_to_obj(obj.as_ref())
            .ok()
            .map(|obj| self.ops.keys(&obj.id, None))
            .unwrap_or_default()
    }

    fn keys_at<O: AsRef<ExId>>(&self, obj: O, heads: &[ChangeHash]) -> Keys<'_> {
        let clock = self.clock_at(heads);
        self.exid_to_obj(obj.as_ref())
            .ok()
            .map(|obj| self.ops.keys(&obj.id, Some(clock)))
            .unwrap_or_default()
    }

    fn map_range<'a, O: AsRef<ExId>, R: RangeBounds<String> + 'a>(
        &'a self,
        obj: O,
        range: R,
    ) -> MapRange<'a, R> {
        self.exid_to_obj(obj.as_ref())
            .ok()
            .map(|obj| self.ops.map_range(&obj.id, range, None))
            .unwrap_or_default()
    }

    fn map_range_at<'a, O: AsRef<ExId>, R: RangeBounds<String> + 'a>(
        &'a self,
        obj: O,
        range: R,
        heads: &[ChangeHash],
    ) -> MapRange<'a, R> {
        let clock = self.clock_at(heads);
        self.exid_to_obj(obj.as_ref())
            .ok()
            .map(|obj| self.ops.map_range(&obj.id, range, Some(clock)))
            .unwrap_or_default()
    }

    fn list_range<O: AsRef<ExId>, R: RangeBounds<usize>>(
        &self,
        obj: O,
        range: R,
    ) -> ListRange<'_, R> {
        self.exid_to_obj(obj.as_ref())
            .ok()
            .map(|obj| self.ops.list_range(&obj.id, range, obj.encoding, None))
            .unwrap_or_default()
    }

    fn list_range_at<O: AsRef<ExId>, R: RangeBounds<usize>>(
        &self,
        obj: O,
        range: R,
        heads: &[ChangeHash],
    ) -> ListRange<'_, R> {
        let clock = self.clock_at(heads);
        self.exid_to_obj(obj.as_ref())
            .ok()
            .map(|obj| {
                self.ops
                    .list_range(&obj.id, range, obj.encoding, Some(clock))
            })
            .unwrap_or_default()
    }

    fn values<O: AsRef<ExId>>(&self, obj: O) -> Values<'_> {
        self.exid_to_obj(obj.as_ref())
            .ok()
            .map(|obj| Values::new(self.ops.top_ops(&obj.id, None), self, None))
            .unwrap_or_default()
    }

    fn values_at<O: AsRef<ExId>>(&self, obj: O, heads: &[ChangeHash]) -> Values<'_> {
        let clock = self.clock_at(heads);
        self.exid_to_obj(obj.as_ref())
            .ok()
            .map(|obj| {
                Values::new(
                    self.ops.top_ops(&obj.id, Some(clock.clone())),
                    self,
                    Some(clock),
                )
            })
            .unwrap_or_default()
    }

    fn length<O: AsRef<ExId>>(&self, obj: O) -> usize {
        self.exid_to_obj(obj.as_ref())
            .map(|obj| self.ops.length(&obj.id, obj.encoding, None))
            .unwrap_or(0)
    }

    fn length_at<O: AsRef<ExId>>(&self, obj: O, heads: &[ChangeHash]) -> usize {
        let clock = self.clock_at(heads);
        self.exid_to_obj(obj.as_ref())
            .map(|obj| self.ops.length(&obj.id, obj.encoding, Some(clock)))
            .unwrap_or(0)
    }

    fn object_type<O: AsRef<ExId>>(&self, obj: O) -> Result<ObjType, AutomergeError> {
        self.exid_to_obj(obj.as_ref()).map(|obj| obj.typ)
    }

    fn text<O: AsRef<ExId>>(&self, obj: O) -> Result<String, AutomergeError> {
        let obj = self.exid_to_obj(obj.as_ref())?;
        Ok(self.ops.text(&obj.id, None))
    }

    fn get_cursor<O: AsRef<ExId>>(
        &self,
        obj: O,
        position: usize,
        at: Option<&[ChangeHash]>,
    ) -> Result<Cursor, AutomergeError> {
        let obj = self.exid_to_obj(obj.as_ref())?;
        let clock = at.map(|heads| self.clock_at(heads));
        if !obj.typ.is_sequence() {
            Err(AutomergeError::InvalidOp(obj.typ))
        } else {
            let found =
                self.ops
                    .seek_ops_by_prop(&obj.id, position.into(), obj.encoding, clock.as_ref());
            if let Some(op) = found.ops.last() {
                Ok(Cursor::new(op.id, &self.ops.m))
            } else {
                Err(AutomergeError::InvalidIndex(position))
            }
        }
    }

    fn get_cursor_position<O: AsRef<ExId>>(
        &self,
        obj: O,
        cursor: &Cursor,
        at: Option<&[ChangeHash]>,
    ) -> Result<usize, AutomergeError> {
        let obj = self.exid_to_obj(obj.as_ref())?;
        let clock = at.map(|heads| self.clock_at(heads));
        let opid = self.cursor_to_opid(cursor, clock.as_ref())?;
        let found = self
            .ops
            .seek_opid(&obj.id, opid, clock.as_ref())
            .ok_or_else(|| AutomergeError::InvalidCursor(cursor.clone()))?;
        Ok(found.index)
    }

    fn text_at<O: AsRef<ExId>>(
        &self,
        obj: O,
        heads: &[ChangeHash],
    ) -> Result<String, AutomergeError> {
        let obj = self.exid_to_obj(obj.as_ref())?;
        let clock = self.clock_at(heads);
        Ok(self.ops.text(&obj.id, Some(clock)))
    }

    fn marks<O: AsRef<ExId>>(&self, obj: O) -> Result<Vec<Mark<'_>>, AutomergeError> {
        self.calculate_marks(obj, None)
    }

    fn marks_at<O: AsRef<ExId>>(
        &self,
        obj: O,
        heads: &[ChangeHash],
    ) -> Result<Vec<Mark<'_>>, AutomergeError> {
        self.calculate_marks(obj, Some(heads))
    }

    fn get<O: AsRef<ExId>, P: Into<Prop>>(
        &self,
        obj: O,
        prop: P,
    ) -> Result<Option<(Value<'_>, ExId)>, AutomergeError> {
        let obj = self.exid_to_obj(obj.as_ref())?;
        let clock = None;
        Ok(self
            .ops
            .seek_ops_by_prop(&obj.id, prop.into(), obj.encoding, clock)
            .ops
            .into_iter()
            .last()
            .map(|op| self.export_value(op, clock)))
    }

    fn get_at<O: AsRef<ExId>, P: Into<Prop>>(
        &self,
        obj: O,
        prop: P,
        heads: &[ChangeHash],
    ) -> Result<Option<(Value<'_>, ExId)>, AutomergeError> {
        let obj = self.exid_to_obj(obj.as_ref())?;
        let clock = Some(self.clock_at(heads));
        Ok(self
            .ops
            .seek_ops_by_prop(&obj.id, prop.into(), obj.encoding, clock.as_ref())
            .ops
            .into_iter()
            .last()
            .map(|op| self.export_value(op, clock.as_ref())))
    }

    fn get_all<O: AsRef<ExId>, P: Into<Prop>>(
        &self,
        obj: O,
        prop: P,
    ) -> Result<Vec<(Value<'_>, ExId)>, AutomergeError> {
        let obj = self.exid_to_obj(obj.as_ref())?;
        let clock = None;
        let values = self
            .ops
            .seek_ops_by_prop(&obj.id, prop.into(), obj.encoding, clock)
            .ops
            .into_iter()
            .map(|op| self.export_value(op, clock))
            .collect::<Vec<_>>();
        // this is a test to make sure opid and exid are always sorting the same way
        assert_eq!(
            values.iter().map(|v| &v.1).collect::<Vec<_>>(),
            values.iter().map(|v| &v.1).sorted().collect::<Vec<_>>()
        );
        Ok(values)
    }

    fn get_all_at<O: AsRef<ExId>, P: Into<Prop>>(
        &self,
        obj: O,
        prop: P,
        heads: &[ChangeHash],
    ) -> Result<Vec<(Value<'_>, ExId)>, AutomergeError> {
        let prop = prop.into();
        let obj = self.exid_to_obj(obj.as_ref())?;
        let clock = Some(self.clock_at(heads));
        let values = self
            .ops
            .seek_ops_by_prop(&obj.id, prop, obj.encoding, clock.as_ref())
            .ops
            .into_iter()
            .map(|op| self.export_value(op, clock.as_ref()))
            .collect::<Vec<_>>();
        // this is a test to make sure opid and exid are always sorting the same way
        assert_eq!(
            values.iter().map(|v| &v.1).collect::<Vec<_>>(),
            values.iter().map(|v| &v.1).sorted().collect::<Vec<_>>()
        );
        Ok(values)
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
}

impl Default for Automerge {
    fn default() -> Self {
        Self::new()
    }
}
