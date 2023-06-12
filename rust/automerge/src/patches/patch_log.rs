use crate::exid::ExId;
use crate::hydrate::Value;
use crate::iter::{ListRangeItem, MapRangeItem};
use crate::marks::{MarkAccumulator, RichText};
use crate::types::{Clock, ObjId, ObjMeta, ObjType, OpId, Prop};
use crate::{Automerge, ChangeHash, Patch};
use std::collections::BTreeSet;
use std::collections::HashSet;
use std::sync::Arc;

use super::{PatchBuilder, TextRepresentation};

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
/// SyncState, SyncDoc}, patches::TextRepresentation};
/// let doc = AutoCommit::new();
/// let sync_message: Message = unimplemented!();
/// let mut sync_state = SyncState::new();
/// let mut patch_log = PatchLog::active(TextRepresentation::String);
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
    text_rep: TextRepresentation,
    pub(crate) heads: Option<Vec<ChangeHash>>,
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
        marks: Option<Arc<RichText>>,
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

impl PatchLog {
    /// Create a new [`PatchLog`]
    ///
    /// # Arguments
    ///
    /// * `active`   - If `true` the log will record all changes made to the document. If [`false`]
    ///                then no changes will be recorded.
    /// * `text_rep` - How text will be represented in the generated patches
    ///
    /// Why, you ask, would you create a [`PatchLog`] which doesn't record any changes? Operations
    /// which record patches are more expensive, so sometimes you may wish to turn off patch
    /// logging for parts of the application, but not others; but you don't want to complicate your
    /// code with an [`Option<PatchLog>`]. In that case you can use an inactive [`PatchLog`].
    pub fn new(active: bool, text_rep: TextRepresentation) -> Self {
        PatchLog {
            active,
            expose: HashSet::default(),
            events: vec![],
            heads: None,
            text_rep,
        }
    }

    /// Create a new [`PatchLog`] which doesn't record any changes.
    ///
    /// See also: [`PatchLog::new()`] for a more detailed explanation.
    pub fn inactive(text_rep: TextRepresentation) -> Self {
        Self::new(false, text_rep)
    }

    pub fn null() -> Self {
        Self::new(false, TextRepresentation::default())
    }

    /// Create a new [`PatchLog`] which does record changes.
    ///
    /// See also: [`PatchLog::new()`] for a more detailed explanation.
    pub fn active(text_rep: TextRepresentation) -> Self {
        Self::new(true, text_rep)
    }

    pub(crate) fn set_active(&mut self, setting: bool) {
        self.active = setting
    }

    pub(crate) fn is_active(&self) -> bool {
        self.active
    }

    pub(crate) fn delete_seq(&mut self, obj: &ObjMeta, index: usize, num: usize) {
        self.events.push((obj.id, Event::DeleteSeq { index, num }))
    }

    pub(crate) fn delete_map(&mut self, obj: &ObjMeta, key: &str) {
        self.events
            .push((obj.id, Event::DeleteMap { key: key.into() }))
    }

    pub(crate) fn increment(&mut self, obj: &ObjMeta, prop: &Prop, value: i64, id: OpId) {
        match prop {
            Prop::Map(key) => self.increment_map(obj, key, value, id),
            Prop::Seq(index) => self.increment_seq(obj, *index, value, id),
        }
    }

    pub(crate) fn increment_map(&mut self, obj: &ObjMeta, key: &str, n: i64, id: OpId) {
        self.events.push((
            obj.id,
            Event::IncrementMap {
                key: key.into(),
                n,
                id,
            },
        ))
    }

    pub(crate) fn increment_seq(&mut self, obj: &ObjMeta, index: usize, n: i64, id: OpId) {
        self.events
            .push((obj.id, Event::IncrementSeq { index, n, id }))
    }

    pub(crate) fn flag_conflict(&mut self, obj: &ObjMeta, prop: &Prop) {
        match prop {
            Prop::Map(key) => self.flag_conflict_map(obj, key),
            Prop::Seq(index) => self.flag_conflict_seq(obj, *index),
        }
    }

    pub(crate) fn flag_conflict_map(&mut self, obj: &ObjMeta, key: &str) {
        self.events
            .push((obj.id, Event::FlagConflictMap { key: key.into() }))
    }

    pub(crate) fn flag_conflict_seq(&mut self, obj: &ObjMeta, index: usize) {
        self.events.push((obj.id, Event::FlagConflictSeq { index }))
    }

    pub(crate) fn put(
        &mut self,
        obj: &ObjMeta,
        prop: &Prop,
        value: Value,
        id: OpId,
        conflict: bool,
        expose: bool,
    ) {
        match prop {
            Prop::Map(key) => self.put_map(obj, key, value, id, conflict, expose),
            Prop::Seq(index) => self.put_seq(obj, *index, value, id, conflict, expose),
        }
    }

    pub(crate) fn put_map(
        &mut self,
        obj: &ObjMeta,
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
            obj.id,
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
        obj: &ObjMeta,
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
            obj.id,
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
        obj: &ObjMeta,
        index: usize,
        text: &str,
        marks: Option<Arc<RichText>>,
    ) {
        self.events.push((
            obj.id,
            Event::Splice {
                index,
                text: text.to_string(),
                marks,
            },
        ))
    }

    pub(crate) fn mark(&mut self, obj: ObjId, index: usize, len: usize, marks: &Arc<RichText>) {
        if let Some((_, Event::Mark { marks: tail_marks })) = self.events.last_mut() {
            tail_marks.add(index, len, marks);
            return;
        }
        let mut acc = MarkAccumulator::default();
        acc.add(index, len, marks);
        self.events.push((obj, Event::Mark { marks: acc }))
    }

    pub(crate) fn insert(
        &mut self,
        obj: &ObjMeta,
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
        self.events.push((obj.id, event))
    }

    pub(crate) fn make_patches(&mut self, doc: &Automerge) -> Vec<Patch> {
        self.events.sort_by(|a, b| doc.ops().osd.lamport_cmp(a, b));
        let expose = ExposeQueue(self.expose.iter().map(|id| doc.id_to_exid(*id)).collect());
        let clock = self.heads.as_ref().map(|heads| doc.clock_at(heads));
        Self::make_patches_inner(&self.events, expose, doc, clock.as_ref(), self.text_rep)
    }

    fn make_patches_inner(
        events: &[(ObjId, Event)],
        mut expose_queue: ExposeQueue,
        doc: &Automerge,
        clock: Option<&Clock>,
        text_rep: TextRepresentation,
    ) -> Vec<Patch> {
        let mut patch_builder = PatchBuilder::new();
        for (obj, event) in events {
            // FIXME - adding a Cow::* to parents would let us not clone here
            let parents = doc.ops().parents(*obj, text_rep, clock.cloned());
            let exid = doc.id_to_exid(obj.0);
            // ignore events on objects in the expose queue
            // incremental updates are ignored and a observation
            // of the final state is used b/c observers did not see
            // past state changes
            if expose_queue.should_skip(&exid) {
                continue;
            }
            // any objects exposed BEFORE exid get observed here
            expose_queue.pump_queue(&exid, &mut patch_builder, doc, clock, text_rep);
            match event {
                Event::PutMap {
                    key,
                    value,
                    id,
                    conflict,
                } => {
                    let opid = doc.id_to_exid(*id);
                    patch_builder.put(parents, exid, key.into(), (value.into(), opid), *conflict);
                }
                Event::DeleteMap { key } => {
                    patch_builder.delete_map(parents, exid, key);
                }
                Event::IncrementMap { key, n, id } => {
                    let opid = doc.id_to_exid(*id);
                    patch_builder.increment(parents, exid, key.into(), (*n, opid));
                }
                Event::FlagConflictMap { key } => {
                    patch_builder.flag_conflict(parents, exid, key.into());
                }
                Event::PutSeq {
                    index,
                    value,
                    id,
                    conflict,
                } => {
                    let opid = doc.id_to_exid(*id);
                    patch_builder.put(parents, exid, index.into(), (value.into(), opid), *conflict);
                }
                Event::Insert {
                    index,
                    value,
                    id,
                    conflict,
                    //marks,
                } => {
                    let opid = doc.id_to_exid(*id);
                    patch_builder.insert(
                        parents,
                        exid,
                        *index,
                        (value.into(), opid),
                        *conflict,
                        //marks.clone(),
                    );
                }
                Event::DeleteSeq { index, num } => {
                    patch_builder.delete_seq(parents, exid, *index, *num);
                }
                Event::IncrementSeq { index, n, id } => {
                    let opid = doc.id_to_exid(*id);
                    patch_builder.increment(parents, exid, index.into(), (*n, opid));
                }
                Event::FlagConflictSeq { index } => {
                    patch_builder.flag_conflict(parents, exid, index.into());
                }
                Event::Splice { index, text, marks } => {
                    patch_builder.splice_text(parents, exid, *index, text, marks.clone());
                }
                Event::Mark { marks } => {
                    patch_builder.mark(parents, exid, marks.clone().into_iter())
                }
            }
        }
        // any objects exposed AFTER all other events get exposed here
        expose_queue.flush_queue(&mut patch_builder, doc, clock, text_rep);

        patch_builder.take_patches()
    }

    pub(crate) fn truncate(&mut self) {
        self.active = true;
        self.events.truncate(0);
        self.expose.clear();
    }

    pub(crate) fn branch(&mut self) -> Self {
        Self {
            active: self.active,
            expose: HashSet::new(),
            events: Default::default(),
            text_rep: self.text_rep,
            heads: None,
        }
    }

    pub(crate) fn merge(&mut self, other: Self) {
        self.events.extend(other.events);
    }

    pub(crate) fn text_rep(&self) -> TextRepresentation {
        self.text_rep
    }

    pub(crate) fn set_text_rep(&mut self, rep: TextRepresentation) {
        self.text_rep = rep;
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
        patch_builder: &mut PatchBuilder,
        doc: &Automerge,
        clock: Option<&Clock>,
        text_rep: TextRepresentation,
    ) {
        while let Some(exposed) = self.0.first() {
            if exposed >= obj {
                break;
            }
            self.flush_obj(exposed.clone(), patch_builder, doc, clock, text_rep);
        }
    }

    fn flush_queue(
        &mut self,
        patch_builder: &mut PatchBuilder,
        doc: &Automerge,
        clock: Option<&Clock>,
        text_rep: TextRepresentation,
    ) {
        while let Some(exposed) = self.0.first() {
            self.flush_obj(exposed.clone(), patch_builder, doc, clock, text_rep);
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
        patch_builder: &mut PatchBuilder,
        doc: &Automerge,
        clock: Option<&Clock>,
        text_rep: TextRepresentation,
    ) -> Option<()> {
        let id = exid.to_internal_obj();
        let parents = doc.ops().parents(id, text_rep, clock.cloned());
        self.remove(&exid);
        match doc.ops().object_type(&id)? {
            ObjType::Text if matches!(text_rep, TextRepresentation::String) => {
                let text = doc.ops().text(&id, clock.cloned());
                // TODO - need text_spans()
                patch_builder.splice_text(parents, exid, 0, &text, None);
            }
            ObjType::List | ObjType::Text => {
                for ListRangeItem {
                    index,
                    value,
                    id,
                    conflict,
                    ..
                } in doc.list_range_for(&exid, .., clock.cloned())
                {
                    if value.is_object() {
                        self.insert(id.clone());
                    }
                    patch_builder.insert(
                        parents.clone(),
                        exid.clone(),
                        index,
                        (value, id),
                        conflict,
                    );
                }
            }
            ObjType::Map | ObjType::Table => {
                for MapRangeItem {
                    key,
                    value,
                    id,
                    conflict,
                } in doc.map_range_for(&exid, .., clock.cloned())
                {
                    if value.is_object() {
                        self.insert(id.clone());
                    }
                    patch_builder.put(
                        parents.clone(),
                        exid.clone(),
                        key.into(),
                        (value, id),
                        conflict,
                    );
                }
            }
        }
        Some(())
    }
}
