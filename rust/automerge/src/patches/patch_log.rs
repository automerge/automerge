use crate::automerge::Automerge;
use crate::exid::ExId;
use crate::hydrate::Value;
use crate::marks::{MarkAccumulator, MarkSet};
use crate::op_set2::PropRef;
use crate::transaction::TransactionArgs;
use crate::types::{ActorId, Clock, ObjId, ObjType, OpId, Prop, SequenceType, TextEncoding};
use crate::{ChangeHash, Patch};
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
    events: Vec<(ObjId, Event)>,
    expose: HashSet<OpId>,
    completed_patches: Vec<Patch>,
    active: bool,
    path_map: BTreeMap<ObjId, (Prop, ObjId)>,
    path_hint: usize,
    pub(crate) heads_clock: Option<Clock>,
    pub(crate) actors: Vec<ActorId>,
    /// Actors which were speculatively added to `actors` when a transaction was opened. If the
    /// transaction produces no ops the actor is removed from the document again on commit/rollback,
    /// so these must be removed from the patch log too (see [`PatchLog::finish_transaction`]).
    speculative_actor: Option<ActorId>,
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
    pub(crate) fn with_new_actor(self, idx: usize) -> Self {
        match self {
            Self::PutMap {
                key,
                value,
                id,
                conflict,
            } => Self::PutMap {
                key,
                value,
                id: id.with_new_actor(idx),
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
                id: id.with_new_actor(idx),
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
                id: id.with_new_actor(idx),
                conflict,
            },
            Self::IncrementMap { key, n, id } => Self::IncrementMap {
                key,
                n,
                id: id.with_new_actor(idx),
            },
            Self::IncrementSeq { index, n, id } => Self::IncrementSeq {
                index,
                n,
                id: id.with_new_actor(idx),
            },
            event => event,
        }
    }

    // Re-index this event after the actor at `idx` has been removed from the actor list.
    //
    // This may only be called for actors which are not referenced by the event (e.g. an actor
    // which was speculatively added when opening a transaction but ended up making no ops). If the
    // event _does_ reference the removed actor this returns `None`.
    pub(crate) fn without_actor(self, idx: usize) -> Option<Self> {
        Some(match self {
            Self::PutMap {
                key,
                value,
                id,
                conflict,
            } => Self::PutMap {
                key,
                value,
                id: id.without_actor(idx)?,
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
                id: id.without_actor(idx)?,
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
                id: id.without_actor(idx)?,
                conflict,
            },
            Self::IncrementMap { key, n, id } => Self::IncrementMap {
                key,
                n,
                id: id.without_actor(idx)?,
            },
            Self::IncrementSeq { index, n, id } => Self::IncrementSeq {
                index,
                n,
                id: id.without_actor(idx)?,
            },
            event => event,
        })
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
            events: Vec::new(),
            expose: HashSet::new(),
            completed_patches: Vec::new(),
            heads_clock: None,
            path_map: Default::default(),
            path_hint: 0,
            actors: vec![],
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

    pub(crate) fn set_active(&mut self, setting: bool) {
        self.active = setting
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

    /// Finalizes the events recorded for the current view before moving the
    /// document to another point in history.
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
    pub(crate) fn finish_current_view(&mut self, doc: &Automerge, heads: &[ChangeHash]) {
        if !self.events.is_empty() || !self.expose.is_empty() {
            self.migrate_actors(&doc.ops.actors)
                .expect("AutoCommit's patch log always belongs to its document");
            let clock = doc.change_graph.clock_for_heads_lossy(heads);
            let previous_heads = self.heads_clock.replace(clock);
            let patches = self.make_current_patches(doc);
            self.heads_clock = previous_heads;
            self.completed_patches.extend(patches);
            self.events.clear();
            self.expose.clear();
            self.path_hint = 0;
            self.path_map.clear();
        }
    }

    pub(crate) fn delete_seq(&mut self, obj: ObjId, index: usize, num: usize) {
        self.push_event(obj, Event::DeleteSeq { index, num })
    }

    pub(crate) fn delete_map(&mut self, obj: ObjId, key: &str) {
        self.push_event(obj, Event::DeleteMap { key: key.into() })
    }

    pub(crate) fn increment(&mut self, obj: ObjId, prop: PropRef<'_>, value: i64, id: OpId) {
        match prop {
            PropRef::Map(key) => self.increment_map(obj, &key, value, id),
            PropRef::Seq(index) => self.increment_seq(obj, index, value, id),
        }
    }

    pub(crate) fn increment_map(&mut self, obj: ObjId, key: &str, n: i64, id: OpId) {
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

    pub(crate) fn flag_conflict_map(&mut self, obj: ObjId, key: &str) {
        self.push_event(obj, Event::FlagConflictMap { key: key.into() })
    }

    pub(crate) fn flag_conflict_seq(&mut self, obj: ObjId, index: usize) {
        self.push_event(obj, Event::FlagConflictSeq { index })
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
        match prop {
            PropRef::Map(key) => self.put_map(obj, &key, value, id, conflict, expose),
            PropRef::Seq(index) => self.put_seq(obj, index, value, id, conflict, expose),
        }
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

    pub(crate) fn put_seq(
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

    pub(crate) fn splice(
        &mut self,
        obj: ObjId,
        index: usize,
        text: &str,
        marks: Option<Arc<MarkSet>>,
    ) {
        self.events.push((
            obj,
            Event::Splice {
                index,
                text: text.to_string(),
                marks,
            },
        ))
    }

    pub(crate) fn mark(&mut self, obj: ObjId, index: usize, len: usize, marks: &Arc<MarkSet>) {
        if let Some((_, Event::Mark { marks: tail_marks })) = self.events.last_mut() {
            tail_marks.add(index, len, marks);
            return;
        }
        let mut acc = MarkAccumulator::default();
        acc.add(index, len, marks);
        self.push_event(obj, Event::Mark { marks: acc })
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
        let clock = self.heads_clock.clone();
        let path_map = self.get_path_map();
        let text_encoding = doc.text_encoding();
        self.events
            .sort_by(|(obj_a, _), (obj_b, _)| obj_a.cmp(obj_b));
        let mut expose = ExposeQueue(self.expose.iter().map(|id| doc.id_to_exid(*id)).collect());
        let mut patch_builder = PatchBuilder::new(doc, path_map, clock.clone(), text_encoding);
        for (obj, event) in &self.events {
            let key = doc.id_to_exid(obj.0);
            expose.pump_queue(&key, &mut patch_builder, doc, clock.as_ref());
            if expose.should_skip(&key) {
                continue;
            }
            patch_builder.log_event(doc, key, event);
        }
        expose.flush_queue(&mut patch_builder, doc, clock.as_ref());
        patch_builder.take_patches()
    }

    pub(crate) fn truncate(&mut self) {
        self.active = true;
        self.events.clear();
        self.expose.clear();
        self.completed_patches.clear();
        self.path_hint = 0;
        self.path_map = Default::default();
    }

    pub(crate) fn branch(&mut self) -> Self {
        Self {
            active: self.active,
            events: Vec::new(),
            expose: HashSet::new(),
            completed_patches: Vec::new(),
            path_map: Default::default(),
            path_hint: 0,
            heads_clock: None,
            actors: self.actors.clone(),
            speculative_actor: None,
        }
    }

    pub(crate) fn migrate_actor(&mut self, index: usize) {
        let dirty = std::mem::take(&mut self.events);
        self.events = dirty
            .into_iter()
            .map(|(obj, event)| (obj.with_new_actor(index), event.with_new_actor(index)))
            .collect();
        let dirty = std::mem::take(&mut self.expose);
        self.expose = dirty
            .into_iter()
            .map(|id| id.with_new_actor(index))
            .collect();
    }

    fn remove_actor(&mut self, index: usize) {
        self.actors.remove(index);
        let dirty = std::mem::take(&mut self.events);
        self.events = dirty
            .into_iter()
            .filter_map(|(obj, event)| {
                Some((obj.without_actor(index)?, event.without_actor(index)?))
            })
            .collect();
        let dirty = std::mem::take(&mut self.expose);
        self.expose = dirty
            .into_iter()
            .filter_map(|id| id.without_actor(index))
            .collect();
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
        self.migrate_actors(&doc.ops.actors)?;
        // If this is the actor's first change then the actor was (potentially)
        // just added to the document. It should be removed again on
        // commit/rollback if the transaction produces no ops, so flag it as
        // speculative.
        if let Some(speculative_actor) =
            (args.seq == 1).then(|| doc.ops.actors[args.actor_index].clone())
        {
            assert!(
                self.speculative_actor.is_none(),
                "beginning a transaction when a speculative actor is already present"
            );
            self.speculative_actor = Some(speculative_actor);
        }
        Ok(())
    }

    /// Notify the patch log that the transaction has finished
    ///
    /// This allows the patch log to clean up any speculative actors that were added
    /// when the transaction began. This method should be called after the transaction
    /// has been committed or rolled back.
    pub(crate) fn finish_transaction(&mut self, doc_actors: &[ActorId]) {
        let Some(speculative_actor) = self.speculative_actor.take() else {
            return;
        };
        if !doc_actors.contains(&speculative_actor) {
            if let Ok(index) = self.actors.binary_search(&speculative_actor) {
                self.remove_actor(index);
            }
        }
        debug_assert_eq!(self.actors.as_slice(), doc_actors);
    }

    // Re-align this patch log's actor list (and the event indices into it) with the document's
    // actor list (`others`).
    //
    // The document's actor list can grow between uses of a patch log (e.g. applying changes adds
    // new actors). Because actor lists are sorted, inserting a new actor shifts the indices of the
    // actors after it, so the event ids stored in the patch log have to be re-indexed to match.
    pub(crate) fn migrate_actors(
        &mut self,
        others: &[ActorId],
    ) -> Result<(), crate::PatchLogMismatch> {
        if self.actors.as_slice() == others {
            return Ok(());
        }
        if self.actors.is_empty() {
            self.actors = others.to_vec();
            return Ok(());
        }
        for i in 0..others.len() {
            match (self.actors.get(i), others.get(i)) {
                (Some(a), Some(b)) if a == b => {}
                (Some(a), Some(b)) if b < a => {
                    self.actors.insert(i, b.clone());
                    self.migrate_actor(i);
                }
                (None, Some(b)) => {
                    self.actors.insert(i, b.clone());
                }
                _ => return Err(crate::PatchLogMismatch),
            }
        }
        Ok(())
    }

    pub(crate) fn merge(&mut self, other: Self) {
        self.completed_patches.extend(other.completed_patches);
        self.events.extend(other.events);
        self.expose.extend(other.expose);
    }

    pub(crate) fn path_hint(&mut self, hint: BTreeMap<ObjId, (Prop, ObjId)>) {
        self.path_map = hint;
        self.path_hint = self.events_len();
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
        clock: Option<&Clock>,
    ) {
        while let Some(exposed) = self.0.first() {
            if exposed >= obj {
                break;
            }
            self.flush_obj(exposed.clone(), patch_builder, doc, clock);
        }
    }

    fn flush_queue(
        &mut self,
        patch_builder: &mut PatchBuilder<'_>,
        doc: &Automerge,
        clock: Option<&Clock>,
    ) {
        while let Some(exposed) = self.0.first() {
            self.flush_obj(exposed.clone(), patch_builder, doc, clock);
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
        clock: Option<&Clock>,
    ) -> Option<()> {
        let id = exid.to_internal_obj();
        self.remove(&exid);
        match doc.ops().object_type(&id)? {
            ObjType::Text => {
                let text = doc.text_for(&exid, clock.cloned()).ok()?;
                // TODO - need doc, text_spans()
                patch_builder.splice_text(exid, 0, &text, None);
            }
            ObjType::List => {
                for item in doc.list_range_for(&exid, .., clock.cloned()) {
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
                for m in doc.map_range_for(&exid, .., clock.cloned()) {
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
