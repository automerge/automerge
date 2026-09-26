use crate::actor::{ActorInsert, ActorRefs, ActorRemoval, ActorShift, ActorTable, HasActorIndices};
use crate::automerge::view::{ClockRange, ReadAt, VisibleClock};
use crate::automerge::Automerge;
use crate::exid::ExId;
use crate::hydrate::Value;
use crate::iter::{DiffIter, SpanInternal};
use crate::marks::{MarkAccumulator, MarkSet};
use crate::op_set2::PropRef;
use crate::transaction::TransactionArgs;
use crate::types::{ActorId, ObjId, ObjMeta, ObjType, OpId, Prop, SequenceType, TextEncoding};
use crate::Patch;
use std::collections::{BTreeMap, BTreeSet, HashSet};
use std::sync::Arc;

use super::PatchBuilder;

/// A record of changes made to a document
///
/// It is often necessary to maintain a materialized view of the current state of a document. E.g.
/// in a text editor you may be rendering the current state a text field in the UI. In order to
/// efficiently update the state of the materialized view any method which adds operations to the
/// document has a variant which takes a [`PatchLog`] as an argument. This allows the caller to
/// record the changes made and then use either [`crate::Automerge::make_patches()`] or
/// [`crate::AutoCommit::make_patches()`] to generate a [`Vec<Patch>`] which can be used to update the
/// materialized view.
///
/// A [`PatchLog`] is a set of _relative_ changes. It represents the changes required to go from the
/// state at one point in history to another. What those two points are depends on how you use the
/// log. A typical reason to create a [`PatchLog`] is to record the changes made by remote peers.
/// Consider this example:
///
/// ```no_run
/// # use automerge::{AutoCommit, Change, Patch, PatchLog, Value, sync::{Message, State as
/// SyncState, SyncDoc}, TextEncoding};
/// let doc = AutoCommit::new();
/// let sync_message: Message = unimplemented!();
/// let mut sync_state = SyncState::new();
/// let mut patch_log = PatchLog::active();
/// doc.sync().receive_sync_message_log_patches(&mut sync_state, sync_message, &mut patch_log);
///
/// // These patches represent the changes needed to go from the state of the document before the
/// // sync message was received, to the state after.
/// let patches = doc.make_patches(&mut patch_log);
/// ```
#[derive(Clone, Debug)]
pub struct PatchLog {
    events: ActorRefs<Vec<(ObjId, Event)>>,
    expose: ActorRefs<HashSet<OpId>>,
    completed_patches: Vec<Patch>,
    active: bool,
    path_map: BTreeMap<ObjId, (Prop, ObjId)>,
    path_hint: usize,
    /// How this log is bound to its document.
    ///
    /// The *observed view* is the visible clock the pending events have
    /// been recorded up to. It is not the same as the diff cursor: the
    /// cursor is the last view a caller drained patches for; the observed
    /// view is where the log currently is. Paths and exposed contents are
    /// resolved against the observed view when patches are made.
    binding: Binding,
    /// Actors which were speculatively added to `actors` when a transaction was opened. If the
    /// transaction produces no ops the actor is removed from the document again on commit/rollback,
    /// so these must be removed from the patch log too (see [`PatchLog::finish_transaction`]).
    speculative_actor: Option<ActorId>,
}

/// How a [`PatchLog`] is bound to its document.
///
/// An observed view is only meaningful together with the actors behind its
/// columns: storing one without the other would let a later actor
/// insertion misattribute the clock's entries. `Bound` therefore always
/// carries both. A `Bound` log over an empty document is legitimate (no
/// actors, a zero-length view); growth from there uses the ordinary actor
/// migration path. Length or identity mismatches against a document are
/// still checked at runtime ([`PatchLog::validate`]).
#[derive(Clone, Debug)]
enum Binding {
    /// A fresh log: no actors, no observed view.
    Unbound,
    /// The log knows its document's actors but has no observed view. This
    /// is the state of an inactive log, or an active one after `truncate`.
    Actors(ActorTable),
    /// The observed view and the actors behind its columns, always together.
    Bound {
        view: VisibleClock,
        actors: ActorTable,
    },
}

impl Binding {
    fn actors(&self) -> Option<&ActorTable> {
        match self {
            Binding::Unbound => None,
            Binding::Actors(actors) | Binding::Bound { actors, .. } => Some(actors),
        }
    }

    fn view(&self) -> Option<&VisibleClock> {
        match self {
            Binding::Bound { view, .. } => Some(view),
            Binding::Unbound | Binding::Actors(_) => None,
        }
    }

    fn take_actors(&mut self) -> ActorTable {
        match std::mem::replace(self, Binding::Unbound) {
            Binding::Unbound => ActorTable::new(),
            Binding::Actors(actors) | Binding::Bound { actors, .. } => actors,
        }
    }
}

#[derive(Clone, PartialEq, Debug)]
pub(crate) enum Event {
    PutMap {
        key: String,
        value: Value,
        id: OpId,
        conflict: bool,
    },
    PutSeq {
        index: usize,
        value: Value,
        id: OpId,
        conflict: bool,
    },
    DeleteSeq {
        index: usize,
        num: usize,
    },
    DeleteMap {
        key: String,
    },
    Splice {
        index: usize,
        text: String,
        marks: Option<Arc<MarkSet>>,
    },
    Insert {
        index: usize,
        value: Value,
        id: OpId,
        conflict: bool,
    },
    IncrementMap {
        key: String,
        n: i64,
        id: OpId,
    },
    IncrementSeq {
        index: usize,
        n: i64,
        id: OpId,
    },
    FlagConflictMap {
        key: String,
    },
    FlagConflictSeq {
        index: usize,
    },
    Mark {
        marks: MarkAccumulator,
    },
}

impl Event {
    /// Re-index the op id this event refers to, if it has one. Events which
    /// carry no op id are returned unchanged.
    ///
    /// If the callback `f` returns `None`, then the return value is also `None`.
    fn map_id(self, f: impl FnOnce(OpId) -> Option<OpId>) -> Option<Self> {
        Some(match self {
            Self::PutMap {
                key,
                value,
                id,
                conflict,
            } => Self::PutMap {
                key,
                value,
                id: f(id)?,
                conflict,
            },
            Self::PutSeq {
                index,
                value,
                id,
                conflict,
            } => Self::PutSeq {
                index,
                value,
                id: f(id)?,
                conflict,
            },
            Self::Insert {
                index,
                value,
                id,
                conflict,
            } => Self::Insert {
                index,
                value,
                id: f(id)?,
                conflict,
            },
            Self::IncrementMap { key, n, id } => Self::IncrementMap { key, n, id: f(id)? },
            Self::IncrementSeq { index, n, id } => Self::IncrementSeq {
                index,
                n,
                id: f(id)?,
            },
            event => event,
        })
    }
}

impl HasActorIndices for Event {
    fn shifted(self, shift: &ActorShift) -> Self {
        self.map_id(|id| Some(id.shifted(shift)))
            .expect("shifting never drops an event")
    }

    // This may only be called for actors which are not referenced by the event (e.g. an actor
    // which was speculatively added when opening a transaction but ended up making no ops). If the
    // event _does_ reference the removed actor this returns `None`.
    fn removed(self, removal: &ActorRemoval) -> Option<Self> {
        self.map_id(|id| id.removed(removal))
    }
}

impl PatchLog {
    /// Create a new [`PatchLog`]
    ///
    /// # Arguments
    ///
    /// * `active`   - If `true` the log will record all changes made to the document. If [`false`] then no changes will be recorded.
    ///
    /// Why, you ask, would you create a [`PatchLog`] which doesn't record any changes? Operations
    /// which record patches are more expensive, so sometimes you may wish to turn off patch
    /// logging for parts of the application, but not others; but you don't want to complicate your
    /// code with an [`Option<PatchLog>`]. In that case you can use an inactive [`PatchLog`].
    pub fn new(active: bool) -> Self {
        PatchLog {
            active,
            events: ActorRefs::default(),
            expose: ActorRefs::default(),
            completed_patches: Vec::new(),
            binding: Binding::Unbound,
            path_map: Default::default(),
            path_hint: 0,
            speculative_actor: None,
        }
    }

    /// Create a new [`PatchLog`] which doesn't record any changes.
    ///
    /// See also: [`PatchLog::new()`] for a more detailed explanation.
    pub fn inactive() -> Self {
        Self::new(false)
    }

    pub fn null() -> Self {
        Self::new(false)
    }

    /// Create a new [`PatchLog`] which does record changes.
    ///
    /// See also: [`PatchLog::new()`] for a more detailed explanation.
    pub fn active() -> Self {
        Self::new(true)
    }

    pub(crate) fn is_active(&self) -> bool {
        self.active
    }

    fn push_event(&mut self, obj: ObjId, event: Event) {
        self.events.push((obj, event));
    }

    fn events_len(&self) -> usize {
        self.events.len()
    }

    /// Move the observed view forward after recording ordinary events.
    ///
    /// This does not log a diff: the events for the move have already been
    /// recorded. Actor migration runs even on an inactive log, mirroring
    /// [`Self::transition_to`]; only the view itself is skipped, and it is
    /// computed lazily so an inactive log pays nothing for it.
    fn advance_view_with(&mut self, doc: &Automerge, view: impl FnOnce() -> VisibleClock) {
        self.migrate_actors(doc.actors())
            .expect("patch log actors must match the document when binding a view");
        if self.active {
            self.adopt_view(view());
        }
    }

    fn actors(&self) -> impl Iterator<Item = &ActorId> {
        self.binding.actors().into_iter().flat_map(ActorTable::iter)
    }

    /// Move the observed view to `after`, recording the difference.
    ///
    /// Finishes the pending segment at the current observed view, logs the
    /// diff from that view to `after`, adopts `after` as the new observed
    /// view and finishes again so the transition is resolved in isolation.
    /// A log with no observed view simply adopts `after` without recording
    /// anything.
    ///
    /// `after` is a closure so the clock is only computed when the log is
    /// active; actor migration still runs unconditionally.
    pub(crate) fn transition_to(
        &mut self,
        doc: &Automerge,
        after: impl FnOnce(&Automerge) -> VisibleClock,
    ) -> Result<(), crate::PatchLogMismatch> {
        self.migrate_actors(doc.actors())?;
        if !self.active {
            return Ok(());
        }
        let after = after(doc);
        match self.binding.view().cloned() {
            Some(before) if before == after => Ok(()),
            Some(before) => {
                self.finish_view(doc);
                DiffIter::log(
                    doc,
                    ObjMeta::root(),
                    ClockRange::diff(before, after.clone()),
                    &mut self.events(),
                    true,
                );
                self.adopt_view(after);
                // Do not sort across view transitions. Resolve exposed
                // subtrees and paths at the new observed view before later
                // events move them.
                self.finish_view(doc);
                Ok(())
            }
            None => {
                self.adopt_view(after);
                Ok(())
            }
        }
    }

    /// Adopt `view` as the observed view, keeping the actors already bound.
    ///
    /// Callers must have migrated the actors against the document `view`
    /// was computed from, so the clock's columns and `actors` line up.
    fn adopt_view(&mut self, view: VisibleClock) {
        let actors = self.binding.take_actors();
        debug_assert_eq!(
            view.len(),
            actors.len(),
            "an observed view must have one column per bound actor"
        );
        self.binding = Binding::Bound { view, actors };
    }

    /// An event-only handle over this log, for recording paths that manage
    /// the log's lifecycle themselves (transactions between
    /// [`Self::begin_transaction`] and [`Self::finish_transaction`]).
    pub(crate) fn events(&mut self) -> Events<'_> {
        Events(self)
    }

    /// Record forward progress under `doc`.
    ///
    /// Validates and migrates the actors, runs `record` with an event-only
    /// handle, then adopts `after` as the new observed view. `after` is
    /// computed lazily, so an inactive log skips the clock computation.
    ///
    /// Forward progress does not finalize the pending segment: events stay
    /// pending and are resolved together at the observed view they end up
    /// at, where an event addressed to a since-superseded object is
    /// dropped. Only a visibility transition ([`Self::transition_to`] with
    /// a different clock) finalizes a segment.
    pub(crate) fn record<T>(
        &mut self,
        doc: &Automerge,
        record: impl FnOnce(&mut Events<'_>) -> T,
        after: impl FnOnce(&Automerge) -> VisibleClock,
    ) -> Result<T, crate::PatchLogMismatch> {
        self.migrate_actors(&doc.ops.actors)?;
        if !self.active {
            return Ok(record(&mut self.events()));
        }
        let after = after(doc);
        let result = record(&mut self.events());
        self.adopt_view(after);
        Ok(result)
    }

    /// Finalize one segment of events under the log's saved view, even if the
    /// document's heads have changed since those events were recorded.
    ///
    /// Patch log events normally move forward through history, which makes it
    /// safe for `make_current_patches` to sort them by object. This method is
    /// only needed when the next view may be at heads that happen before the
    /// current heads, as when isolating a document to an earlier state. In that
    /// case, sorting events from both sides of the transition together would
    /// reorder changes that must remain chronological.
    ///
    /// Paths must also be resolved while this view is still current: list
    /// indexes may identify different objects after the transition. Finalizing
    /// concrete patches here preserves both their ordering and their paths, and
    /// lets them be safely concatenated with patches from subsequent views.
    fn finish_view(&mut self, doc: &Automerge) {
        if !self.events.is_empty() || !self.expose.is_empty() {
            self.migrate_actors(doc.actors())
                .expect("patch log actors must be validated before finalizing a view");
            let patches = self.make_current_patches(doc);
            self.completed_patches.extend(patches);
            self.events.clear();
            self.expose.clear();
            self.path_hint = 0;
            self.path_map.clear();
        }
    }

    fn delete_seq(&mut self, obj: ObjId, index: usize, num: usize) {
        self.push_event(obj, Event::DeleteSeq { index, num })
    }

    fn delete_map(&mut self, obj: ObjId, key: &str) {
        self.push_event(obj, Event::DeleteMap { key: key.into() })
    }

    fn increment(&mut self, obj: ObjId, prop: PropRef<'_>, value: i64, id: OpId) {
        match prop {
            PropRef::Map(key) => self.increment_map(obj, &key, value, id),
            PropRef::Seq(index) => self.increment_seq(obj, index, value, id),
        }
    }

    fn increment_map(&mut self, obj: ObjId, key: &str, n: i64, id: OpId) {
        self.events.push((
            obj,
            Event::IncrementMap {
                key: key.into(),
                n,
                id,
            },
        ))
    }

    pub(crate) fn increment_seq(&mut self, obj: ObjId, index: usize, n: i64, id: OpId) {
        self.push_event(obj, Event::IncrementSeq { index, n, id })
    }

    fn flag_conflict(&mut self, obj: ObjId, prop: &Prop) {
        match prop {
            Prop::Map(key) => self.flag_conflict_map(obj, key),
            Prop::Seq(index) => self.flag_conflict_seq(obj, *index),
        }
    }

    fn flag_conflict_map(&mut self, obj: ObjId, key: &str) {
        self.push_event(obj, Event::FlagConflictMap { key: key.into() })
    }

    fn flag_conflict_seq(&mut self, obj: ObjId, index: usize) {
        self.push_event(obj, Event::FlagConflictSeq { index })
    }

    fn put(
        &mut self,
        obj: ObjId,
        prop: PropRef<'_>,
        value: Value,
        id: OpId,
        conflict: bool,
        expose: bool,
    ) {
        match prop {
            PropRef::Map(key) => self.put_map(obj, &key, value, id, conflict, expose),
            PropRef::Seq(index) => self.put_seq(obj, index, value, id, conflict, expose),
        }
    }

    fn put_map(
        &mut self,
        obj: ObjId,
        key: &str,
        value: Value,
        id: OpId,
        conflict: bool,
        expose: bool,
    ) {
        if expose && value.is_object() {
            self.expose.insert(id);
        }
        self.events.push((
            obj,
            Event::PutMap {
                key: key.into(),
                value,
                id,
                conflict,
            },
        ))
    }

    fn put_seq(
        &mut self,
        obj: ObjId,
        index: usize,
        value: Value,
        id: OpId,
        conflict: bool,
        expose: bool,
    ) {
        if expose && value.is_object() {
            self.expose.insert(id);
        }
        self.events.push((
            obj,
            Event::PutSeq {
                index,
                value,
                id,
                conflict,
            },
        ))
    }

    #[allow(clippy::too_many_arguments)]
    fn replace_seq(
        &mut self,
        obj: ObjId,
        index: usize,
        old_value: &Value,
        value: Value,
        id: OpId,
        conflict: bool,
        expose: bool,
        seq_type: SequenceType,
        text_encoding: TextEncoding,
        marks: Option<Arc<MarkSet>>,
    ) {
        if seq_type == SequenceType::List {
            self.put_seq(obj, index, value, id, conflict, expose);
            return;
        }

        self.delete_seq(obj, index, old_value.width(seq_type, text_encoding));
        if value.is_object() {
            self.insert_and_maybe_expose(obj, index, value, id, conflict, expose);
        } else {
            self.splice(obj, index, value.as_str(), marks);
        }
    }

    fn splice(&mut self, obj: ObjId, index: usize, text: &str, marks: Option<Arc<MarkSet>>) {
        self.events.push((
            obj,
            Event::Splice {
                index,
                text: text.to_string(),
                marks,
            },
        ))
    }

    fn mark(&mut self, obj: ObjId, index: usize, len: usize, marks: &Arc<MarkSet>) {
        if let Some((_, Event::Mark { marks: tail_marks })) = self.events.last_mut() {
            tail_marks.add(index, len, marks);
            return;
        }
        let mut acc = MarkAccumulator::default();
        acc.add(index, len, marks);
        self.push_event(obj, Event::Mark { marks: acc })
    }

    fn insert_and_maybe_expose(
        &mut self,
        obj: ObjId,
        index: usize,
        value: Value,
        id: OpId,
        conflict: bool,
        expose: bool,
    ) {
        if expose && value.is_object() {
            self.expose.insert(id);
        }
        self.insert(obj, index, value, id, conflict)
    }

    pub(crate) fn insert(
        &mut self,
        obj: ObjId,
        index: usize,
        value: Value,
        id: OpId,
        conflict: bool,
    ) {
        let event = Event::Insert {
            index,
            value,
            id,
            conflict,
        };
        self.push_event(obj, event)
    }

    fn get_path_map(&mut self) -> BTreeMap<ObjId, (Prop, ObjId)> {
        if self.path_hint != self.events_len() {
            self.path_hint = 0;
            self.path_map = BTreeMap::default();
        }
        std::mem::take(&mut self.path_map)
    }

    pub(crate) fn make_patches(&mut self, doc: &Automerge) -> Vec<Patch> {
        let mut patches = self.completed_patches.clone();
        patches.extend(self.make_current_patches(doc));
        patches
    }

    fn make_current_patches(&mut self, doc: &Automerge) -> Vec<Patch> {
        // The document may have gained actors since the log last saw it
        // (e.g. through `apply_changes` with another, inactive log). The
        // stored view and event ids index into the actor list, so re-align
        // them before resolving anything against `doc`. Migration only
        // fails for a log from a different document lineage.
        self.migrate_actors(doc.actors())
            .expect("patch log actors must match the document when making patches");
        let view = self.binding.view().cloned();
        let read = match view.as_ref() {
            Some(v) => doc.read_visible(v),
            None => doc.read_current(),
        };
        let path_map = self.get_path_map();
        let text_encoding = doc.text_encoding();
        self.events
            .sort_by(|(obj_a, _), (obj_b, _)| obj_a.cmp(obj_b));
        let mut expose = ExposeQueue(self.expose.iter().map(|id| doc.id_to_exid(*id)).collect());
        let mut patch_builder = PatchBuilder::new(doc, path_map, read.clone(), text_encoding);
        for (obj, event) in self.events.iter() {
            let key = doc.id_to_exid(obj.0);
            expose.pump_queue(&key, &mut patch_builder, doc, &read);
            if expose.should_skip(&key) {
                continue;
            }
            patch_builder.log_event(doc, key, event);
        }
        expose.flush_queue(&mut patch_builder, doc, &read);
        patch_builder.take_patches()
    }

    pub(crate) fn truncate(&mut self) {
        self.active = true;
        self.events.clear();
        self.expose.clear();
        self.completed_patches.clear();
        self.binding = Binding::Actors(self.binding.take_actors());
        self.path_hint = 0;
        self.path_map = Default::default();
    }

    pub(crate) fn branch(&mut self) -> Self {
        Self {
            active: self.active,
            events: ActorRefs::default(),
            expose: ActorRefs::default(),
            completed_patches: Vec::new(),
            path_map: Default::default(),
            path_hint: 0,
            binding: self.binding.clone(),
            speculative_actor: None,
        }
    }

    /// Insert `actor` into the binding, growing the stored view alongside it.
    /// A trailing append grows the view but shifts no existing event indices.
    fn insert_actor(&mut self, actor: ActorId) {
        if matches!(self.binding, Binding::Unbound) {
            self.binding = Binding::Actors(ActorTable::new());
        }
        let (actors, view) = match &mut self.binding {
            Binding::Unbound => unreachable!("binding was initialized above"),
            Binding::Actors(actors) => (actors, None),
            Binding::Bound { actors, view } => (actors, Some(view)),
        };
        if let ActorInsert::Inserted(shift) = actors.insert(actor) {
            if let Some(view) = view {
                view.insert_actor(&shift);
            }
            if shift.index() + 1 != actors.len() {
                self.events.shift_actors(&shift);
                self.expose.shift_actors(&shift);
            }
        }
    }

    /// Forget the actor at `index`, which must not be referenced by any
    /// logged event (it was speculatively added for a transaction which
    /// produced no ops).
    fn remove_actor(&mut self, index: usize) {
        let removal = match &mut self.binding {
            Binding::Unbound => return,
            Binding::Actors(actors) => actors.remove(index).1,
            Binding::Bound { view, actors } => {
                let (_, removal) = actors.remove(index);
                view.remove_actor(&removal);
                removal
            }
        };
        self.events
            .remove_actor(&removal)
            .expect("removed actor is still referenced by a logged event");
        self.expose
            .remove_actor(&removal)
            .expect("removed actor is still referenced by an exposed op");
    }

    /// Notify the patch log that we are beginning a new transaction
    ///
    /// This is necessary because the transaction may be creating a new actor,
    /// which needs to be tracked as speculative until the transaction is
    /// committed or rolled back so that if the transaction produces no ops the
    /// actor can be removed from the document in [`Self::finish_transaction`]
    pub(crate) fn begin_transaction(
        &mut self,
        doc: &Automerge,
        args: &TransactionArgs,
    ) -> Result<(), crate::PatchLogMismatch> {
        self.transition_to(doc, |d| d.visible(&args.deps))?;
        // If this is the actor's first change then the actor was (potentially)
        // just added to the document. It should be removed again on
        // commit/rollback if the transaction produces no ops, so flag it as
        // speculative.
        if let Some(speculative_actor) =
            (args.seq == 1).then(|| doc.actors()[args.actor_index].clone())
        {
            assert!(
                self.speculative_actor.is_none(),
                "beginning a transaction when a speculative actor is already present"
            );
            self.speculative_actor = Some(speculative_actor);
        }
        Ok(())
    }

    /// Notify the patch log that the transaction has been committed.
    ///
    /// Removes any speculative actor added when the transaction began, then
    /// adopts `after` as the observed view for the events the transaction
    /// recorded. `after` is computed lazily, so an inactive log skips it.
    pub(crate) fn finish_transaction(
        &mut self,
        doc: &Automerge,
        after: impl FnOnce(&Automerge) -> VisibleClock,
    ) {
        self.remove_speculative_actor(&doc.ops.actors);
        self.advance_view_with(doc, || after(doc));
    }

    /// Notify the patch log that the transaction was rolled back.
    ///
    /// Removes any speculative actor without changing the observed view: a
    /// rollback reverts the document, so the view recorded before the
    /// transaction began is still the state the log has seen.
    pub(crate) fn abandon_transaction(&mut self, doc_actors: &ActorTable) {
        self.remove_speculative_actor(doc_actors);
    }

    fn remove_speculative_actor(&mut self, doc_actors: &ActorTable) {
        let Some(speculative_actor) = self.speculative_actor.take() else {
            return;
        };
        if doc_actors.lookup(&speculative_actor).is_none() {
            if let Some(index) = self
                .binding
                .actors()
                .and_then(|actors| actors.lookup(&speculative_actor))
            {
                self.remove_actor(index);
            }
        }
        debug_assert!(self.actors().eq(doc_actors.iter()));
    }

    /// Read-only compatibility check: every actor this log knows must still
    /// exist in `doc_actors`. Both lists are sorted, so this is a
    /// subsequence test. The importer calls this before draining the change
    /// queue, so a rejected log leaves previously queued changes available
    /// for a retry with a compatible log.
    pub(crate) fn validate(&self, doc_actors: &ActorTable) -> Result<(), crate::PatchLogMismatch> {
        let mut remaining = doc_actors.iter();
        if self
            .actors()
            .all(|actor| remaining.any(|other| other == actor))
        {
            Ok(())
        } else {
            Err(crate::PatchLogMismatch)
        }
    }

    // Re-align this patch log's actor list (and the event indices into it) with the document's
    // actor list (`others`).
    //
    // The document's actor list can grow between uses of a patch log (e.g. applying changes adds
    // new actors). Because actor lists are sorted, inserting a new actor shifts the indices of the
    // actors after it, so the event ids stored in the patch log have to be re-indexed to match.
    fn migrate_actors(&mut self, others: &ActorTable) -> Result<(), crate::PatchLogMismatch> {
        if self.actors().eq(others.iter()) {
            return Ok(());
        }
        // Validate before mutating so a mismatch leaves the original binding
        // and its recorded events intact.
        self.validate(others)?;
        if self.binding.view().is_none() && self.binding.actors().is_none_or(ActorTable::is_empty) {
            self.binding = Binding::Actors(others.clone());
            return Ok(());
        }
        for actor in others.iter() {
            self.insert_actor(actor.clone());
        }
        if self.actors().eq(others.iter()) {
            Ok(())
        } else {
            Err(crate::PatchLogMismatch)
        }
    }

    pub(crate) fn merge(&mut self, other: Self) {
        debug_assert!(self.actors().eq(other.actors()));
        if matches!(other.binding, Binding::Bound { .. }) {
            self.binding = other.binding;
        }
        self.completed_patches.extend(other.completed_patches);
        self.events.extend(other.events.0);
        self.expose.extend(other.expose.0);
    }

    pub(crate) fn path_hint(&mut self, hint: BTreeMap<ObjId, (Prop, ObjId)>) {
        self.path_map = hint;
        self.path_hint = self.events_len();
    }
}

/// A handle for recording events into a [`PatchLog`].
///
/// Exposes only the emission methods, so code that records events cannot
/// also bind the log, move its observed view, or drain its patches. Obtain a
/// handle to [`Events`] from [`PatchLog::record`] or [`PatchLog::events`].
#[derive(Debug)]
pub(crate) struct Events<'a>(&'a mut PatchLog);

impl Events<'_> {
    /// Whether the underlying log records events.
    pub(crate) fn is_active(&self) -> bool {
        self.0.is_active()
    }

    pub(crate) fn delete_seq(&mut self, obj: ObjId, index: usize, num: usize) {
        self.0.delete_seq(obj, index, num)
    }

    pub(crate) fn delete_map(&mut self, obj: ObjId, key: &str) {
        self.0.delete_map(obj, key)
    }

    pub(crate) fn increment(&mut self, obj: ObjId, prop: PropRef<'_>, value: i64, id: OpId) {
        self.0.increment(obj, prop, value, id)
    }

    pub(crate) fn increment_map(&mut self, obj: ObjId, key: &str, n: i64, id: OpId) {
        self.0.increment_map(obj, key, n, id)
    }

    pub(crate) fn increment_seq(&mut self, obj: ObjId, index: usize, n: i64, id: OpId) {
        self.0.increment_seq(obj, index, n, id)
    }

    pub(crate) fn flag_conflict(&mut self, obj: ObjId, prop: &Prop) {
        self.0.flag_conflict(obj, prop)
    }

    pub(crate) fn flag_conflict_map(&mut self, obj: ObjId, key: &str) {
        self.0.flag_conflict_map(obj, key)
    }

    pub(crate) fn flag_conflict_seq(&mut self, obj: ObjId, index: usize) {
        self.0.flag_conflict_seq(obj, index)
    }

    pub(crate) fn put(
        &mut self,
        obj: ObjId,
        prop: PropRef<'_>,
        value: Value,
        id: OpId,
        conflict: bool,
        expose: bool,
    ) {
        self.0.put(obj, prop, value, id, conflict, expose)
    }

    pub(crate) fn put_map(
        &mut self,
        obj: ObjId,
        key: &str,
        value: Value,
        id: OpId,
        conflict: bool,
        expose: bool,
    ) {
        self.0.put_map(obj, key, value, id, conflict, expose)
    }

    pub(crate) fn put_seq(
        &mut self,
        obj: ObjId,
        index: usize,
        value: Value,
        id: OpId,
        conflict: bool,
        expose: bool,
    ) {
        self.0.put_seq(obj, index, value, id, conflict, expose)
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn replace_seq(
        &mut self,
        obj: ObjId,
        index: usize,
        old_value: &Value,
        value: Value,
        id: OpId,
        conflict: bool,
        expose: bool,
        seq_type: SequenceType,
        text_encoding: TextEncoding,
        marks: Option<Arc<MarkSet>>,
    ) {
        self.0.replace_seq(
            obj,
            index,
            old_value,
            value,
            id,
            conflict,
            expose,
            seq_type,
            text_encoding,
            marks,
        )
    }

    pub(crate) fn splice(
        &mut self,
        obj: ObjId,
        index: usize,
        text: &str,
        marks: Option<Arc<MarkSet>>,
    ) {
        self.0.splice(obj, index, text, marks)
    }

    pub(crate) fn mark(&mut self, obj: ObjId, index: usize, len: usize, marks: &Arc<MarkSet>) {
        self.0.mark(obj, index, len, marks)
    }

    pub(crate) fn insert_and_maybe_expose(
        &mut self,
        obj: ObjId,
        index: usize,
        value: Value,
        id: OpId,
        conflict: bool,
        expose: bool,
    ) {
        self.0
            .insert_and_maybe_expose(obj, index, value, id, conflict, expose)
    }

    pub(crate) fn insert(
        &mut self,
        obj: ObjId,
        index: usize,
        value: Value,
        id: OpId,
        conflict: bool,
    ) {
        self.0.insert(obj, index, value, id, conflict)
    }
}

impl AsRef<OpId> for &(ObjId, Event) {
    fn as_ref(&self) -> &OpId {
        &self.0 .0
    }
}

#[derive(Clone, Default, PartialEq, Debug)]
struct ExposeQueue(BTreeSet<ExId>);

impl ExposeQueue {
    fn should_skip(&self, obj: &ExId) -> bool {
        if let Some(exposed) = self.0.first() {
            exposed == obj
        } else {
            false
        }
    }

    fn pump_queue(
        &mut self,
        obj: &ExId,
        patch_builder: &mut PatchBuilder<'_>,
        doc: &Automerge,
        read: &ReadAt<'_>,
    ) {
        while let Some(exposed) = self.0.first() {
            if exposed >= obj {
                break;
            }
            self.flush_obj(exposed.clone(), patch_builder, doc, read);
        }
    }

    fn flush_queue(
        &mut self,
        patch_builder: &mut PatchBuilder<'_>,
        doc: &Automerge,
        read: &ReadAt<'_>,
    ) {
        while let Some(exposed) = self.0.first() {
            self.flush_obj(exposed.clone(), patch_builder, doc, read);
        }
    }

    fn insert(&mut self, obj: ExId) -> bool {
        self.0.insert(obj)
    }

    fn remove(&mut self, obj: &ExId) -> bool {
        self.0.remove(obj)
    }

    fn flush_obj(
        &mut self,
        exid: ExId,
        patch_builder: &mut PatchBuilder<'_>,
        doc: &Automerge,
        read: &ReadAt<'_>,
    ) -> Option<()> {
        let id = exid.to_internal_obj();
        self.remove(&exid);
        match doc.ops().object_type(&id)? {
            ObjType::Text => {
                for span in doc.ops().spans(&id, read.borrow()) {
                    match span {
                        SpanInternal::Text(text, index, marks) => {
                            patch_builder.splice_text(exid.clone(), index, &text, marks.export());
                        }
                        SpanInternal::Obj(id, index, _) => {
                            let child = doc.id_to_exid(id);
                            self.insert(child.clone());
                            patch_builder.insert(
                                exid.clone(),
                                index,
                                (crate::Value::Object(ObjType::Map), child),
                                false,
                            );
                        }
                    }
                }
            }
            ObjType::List => {
                for item in doc.list_range_for(&exid, .., read.borrow()) {
                    let value = item.value.to_value();
                    let id = item.id();
                    let conflict = item.conflict;
                    let index = item.index;
                    if value.is_object() {
                        self.insert(id.clone());
                    }
                    patch_builder.insert(exid.clone(), index, (value, id), conflict);
                }
            }
            ObjType::Map | ObjType::Table => {
                for m in doc.map_range_for(&exid, .., read.borrow()) {
                    let value = m.value.to_value();
                    let id = m.id();
                    if value.is_object() {
                        self.insert(id.clone());
                    }
                    patch_builder.put(exid.clone(), m.key.into(), (value, id), m.conflict);
                }
            }
        }
        Some(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::clock::Clock;
    use crate::transaction::Transactable;
    use crate::ROOT;

    fn document(changes: &[(u8, usize)]) -> Automerge {
        let mut doc = Automerge::new();
        for &(actor, ops) in changes {
            doc.set_actor(ActorId::from(vec![actor]));
            let mut tx = doc.transaction();
            for value in 0..ops {
                tx.put(ROOT, "key", value as i64).unwrap();
            }
            tx.commit();
        }
        doc
    }

    fn actors(ids: &[u8]) -> ActorTable {
        ActorTable::from_actors(ids.iter().map(|id| ActorId::from(vec![*id])))
    }

    #[test]
    fn actor_migration_keeps_view_events_and_expose_aligned() {
        let doc = document(&[(2, 7)]);
        let mut log = PatchLog::active();
        log.record(&doc, |_| {}, Automerge::visible_current)
            .unwrap();
        let id = OpId::new(7, 0);
        log.increment_seq(ObjId(id), 0, 1, id);
        log.expose.insert(id);

        // Both an earlier insertion and a trailing append add clock slots.
        log.migrate_actors(&actors(&[1, 2, 3])).unwrap();
        assert_eq!(
            log.binding.view().unwrap().clock(),
            &Clock::from_counters([None, Some(7), None])
        );
        let shifted = OpId::new(7, 1);
        assert_eq!(
            log.events[0],
            (
                ObjId(shifted),
                Event::IncrementSeq {
                    index: 0,
                    n: 1,
                    id: shifted
                }
            )
        );
        assert!(log.expose.contains(&shifted));

        // Removing an unused speculative actor shifts all three back.
        log.remove_actor(0);
        log.remove_actor(1);
        assert!(log.actors().eq(actors(&[2]).iter()));
        assert_eq!(
            log.binding.view().unwrap().clock(),
            &Clock::from_counters([Some(7)])
        );
        assert_eq!(
            log.events[0],
            (ObjId(id), Event::IncrementSeq { index: 0, n: 1, id })
        );
        assert!(log.expose.contains(&id));
    }

    #[test]
    fn actor_migration_grows_a_view_bound_to_an_empty_document() {
        let doc = document(&[]);
        let mut log = PatchLog::active();
        log.record(&doc, |_| {}, Automerge::visible_current)
            .unwrap();
        let table = actors(&[1, 2]);

        log.migrate_actors(&table).unwrap();

        assert!(log.actors().eq(table.iter()));
        assert_eq!(
            log.binding.view().unwrap().clock(),
            &Clock::from_counters([None, None])
        );
        let view = log.binding.view().cloned();
        log.migrate_actors(&table).unwrap();
        assert_eq!(log.binding.view(), view.as_ref());
    }

    #[test]
    fn actor_migration_mismatch_leaves_the_log_unchanged() {
        let doc = document(&[(2, 5), (4, 2)]);
        let mut log = PatchLog::active();
        log.record(&doc, |_| {}, Automerge::visible_current)
            .unwrap();
        let id = OpId::new(7, 1);
        log.increment_seq(ObjId(id), 0, 1, id);
        log.expose.insert(id);
        let before = log.clone();

        // A new earlier actor must not be inserted before noticing the missing
        // trailing actor, otherwise the log is corrupted despite the error.
        assert!(log.migrate_actors(&actors(&[1, 2, 3])).is_err());

        assert!(log.actors().eq(before.actors()));
        assert_eq!(log.binding.view(), before.binding.view());
        assert_eq!(log.events, before.events);
        assert_eq!(log.expose, before.expose);
    }
}
