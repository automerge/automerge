use crate::automerge::Automerge;
use crate::error::AutomergeError;
use crate::exid::ExId;
use crate::hydrate::Value;
use crate::marks::{MarkAccumulator, MarkSet};
use crate::op_set2::PropRef;
use crate::types::{ActorId, Clock, ObjId, ObjType, OpId, Prop, TextEncoding};
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
    pub(crate) events: Vec<(ObjId, Event)>,
    expose: HashSet<OpId>,
    active: bool,
    path_map: BTreeMap<ObjId, (Prop, ObjId)>,
    path_hint: usize,
    pub(crate) heads: Option<Vec<ChangeHash>>,
    pub(crate) actors: Vec<ActorId>,
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
            expose: HashSet::default(),
            events: vec![],
            heads: None,
            path_map: Default::default(),
            path_hint: 0,
            actors: vec![],
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

    pub(crate) fn delete_seq(&mut self, obj: ObjId, index: usize, num: usize) {
        self.events.push((obj, Event::DeleteSeq { index, num }))
    }

    pub(crate) fn delete_map(&mut self, obj: ObjId, key: &str) {
        self.events
            .push((obj, Event::DeleteMap { key: key.into() }))
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
        self.events
            .push((obj, Event::IncrementSeq { index, n, id }))
    }

    pub(crate) fn flag_conflict(&mut self, obj: ObjId, prop: &Prop) {
        match prop {
            Prop::Map(key) => self.flag_conflict_map(obj, key),
            Prop::Seq(index) => self.flag_conflict_seq(obj, *index),
        }
    }

    pub(crate) fn flag_conflict_map(&mut self, obj: ObjId, key: &str) {
        self.events
            .push((obj, Event::FlagConflictMap { key: key.into() }))
    }

    pub(crate) fn flag_conflict_seq(&mut self, obj: ObjId, index: usize) {
        self.events.push((obj, Event::FlagConflictSeq { index }))
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
        self.events.push((obj, Event::Mark { marks: acc }))
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
        self.events.push((obj, event))
    }

    fn get_path_map(&mut self) -> BTreeMap<ObjId, (Prop, ObjId)> {
        if self.path_hint != self.events.len() {
            self.path_hint = 0;
            self.path_map = BTreeMap::default();
        }
        std::mem::take(&mut self.path_map)
    }

    pub(crate) fn make_patches(&mut self, doc: &Automerge) -> Vec<Patch> {
        self.events.sort_by(|(a, _), (b, _)| a.cmp(b));
        let expose = ExposeQueue(self.expose.iter().map(|id| doc.id_to_exid(*id)).collect());
        let clock = self.heads.as_ref().map(|h| doc.clock_at(h));
        let path_map = self.get_path_map();
        let text_encoding = doc.text_encoding();
        Self::make_patches_inner(&self.events, expose, path_map, doc, clock, text_encoding)
    }

    fn make_patches_inner(
        events: &[(ObjId, Event)],
        mut expose_queue: ExposeQueue,
        path_map: BTreeMap<ObjId, (Prop, ObjId)>,
        doc: &Automerge,
        clock: Option<Clock>,
        text_encoding: TextEncoding,
    ) -> Vec<Patch> {
        let mut patch_builder = PatchBuilder::new(doc, path_map, clock.clone(), text_encoding);
        for (obj, event) in events {
            let exid = doc.id_to_exid(obj.0);
            // ignore events on objects in the expose queue
            // incremental updates are ignored and a observation
            // of the final state is used b/c observers did not see
            // past state changes
            if expose_queue.should_skip(&exid) {
                continue;
            }
            // any objects exposed BEFORE exid get observed here
            expose_queue.pump_queue(&exid, &mut patch_builder, doc, clock.as_ref());

            patch_builder.log_event(doc, exid, event);
        }
        // any objects exposed AFTER all other events get exposed here
        expose_queue.flush_queue(&mut patch_builder, doc, clock.as_ref());

        patch_builder.take_patches()
    }

    pub(crate) fn truncate(&mut self) {
        self.active = true;
        self.events.truncate(0);
        self.path_hint = 0;
        self.path_map = Default::default();
        self.expose.clear();
    }

    pub(crate) fn branch(&mut self) -> Self {
        Self {
            active: self.active,
            expose: HashSet::new(),
            events: Default::default(),
            path_map: Default::default(),
            path_hint: 0,
            heads: None,
            actors: self.actors.clone(),
        }
    }

    pub(crate) fn migrate_actor(&mut self, index: usize) {
        let dirty = std::mem::take(&mut self.events);
        self.events = dirty
            .into_iter()
            .map(|(o, e)| (o.with_new_actor(index), e.with_new_actor(index)))
            .collect();

        let dirty = std::mem::take(&mut self.expose);
        self.expose = dirty
            .into_iter()
            .map(|id| id.with_new_actor(index))
            .collect();
    }

    // if a new actor is added to an opset, the id's inside the patch log need to be re-ordered
    // this is an uncommon operation so this seems preferable to storing ExId's in place
    // for every objid and opid
    pub(crate) fn migrate_actors(&mut self, others: &Vec<ActorId>) -> Result<(), AutomergeError> {
        if &self.actors != others {
            if self.actors.is_empty() {
                self.actors = others.clone();
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
                    _ => return Err(AutomergeError::PatchLogMismatch),
                }
            }
        }
        Ok(())
    }

    pub(crate) fn merge(&mut self, other: Self) {
        self.events.extend(other.events);
    }

    pub(crate) fn path_hint(&mut self, hint: BTreeMap<ObjId, (Prop, ObjId)>) {
        self.path_map = hint;
        self.path_hint = self.events.len();
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
