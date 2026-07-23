use crate::automerge::Automerge;
use crate::exid::ExId;
use crate::hydrate::Value;
use crate::marks::{MarkAccumulator, MarkSet};
use crate::types::{Clock, ObjId, ObjType, OpId, Prop, TextEncoding};
use crate::{ChangeHash, Patch};
use std::collections::{BTreeMap, BTreeSet, HashSet};
use std::sync::Arc;

use super::PatchBuilder;

/// Internal accumulator used while converting diffs to patches.
#[derive(Clone, Debug)]
pub(crate) struct PatchAccumulator {
    pub(crate) events: Vec<(ObjId, Event)>,
    expose: HashSet<OpId>,
    record_events: bool,
    path_map: BTreeMap<ObjId, (Prop, ObjId)>,
    path_hint: usize,
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

impl PatchAccumulator {
    pub(crate) fn inactive() -> Self {
        Self {
            record_events: false,
            expose: HashSet::default(),
            events: vec![],
            heads: None,
            path_map: Default::default(),
            path_hint: 0,
        }
    }

    pub(crate) fn event_log() -> Self {
        Self {
            record_events: true,
            ..Self::inactive()
        }
    }

    pub(crate) fn should_record_events(&self) -> bool {
        self.record_events
    }

    pub(crate) fn delete_seq(&mut self, obj: ObjId, index: usize, num: usize) {
        if self.should_record_events() {
            self.events.push((obj, Event::DeleteSeq { index, num }))
        }
    }

    pub(crate) fn delete_map(&mut self, obj: ObjId, key: &str) {
        if self.should_record_events() {
            self.events
                .push((obj, Event::DeleteMap { key: key.into() }))
        }
    }

    pub(crate) fn increment_map(&mut self, obj: ObjId, key: &str, n: i64, id: OpId) {
        if self.should_record_events() {
            self.events.push((
                obj,
                Event::IncrementMap {
                    key: key.into(),
                    n,
                    id,
                },
            ))
        }
    }

    pub(crate) fn increment_seq(&mut self, obj: ObjId, index: usize, n: i64, id: OpId) {
        if self.should_record_events() {
            self.events
                .push((obj, Event::IncrementSeq { index, n, id }))
        }
    }

    pub(crate) fn flag_conflict_map(&mut self, obj: ObjId, key: &str) {
        if self.should_record_events() {
            self.events
                .push((obj, Event::FlagConflictMap { key: key.into() }))
        }
    }

    pub(crate) fn flag_conflict_seq(&mut self, obj: ObjId, index: usize) {
        if self.should_record_events() {
            self.events.push((obj, Event::FlagConflictSeq { index }))
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
        if !self.should_record_events() {
            return;
        }
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
        if !self.should_record_events() {
            return;
        }
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
        if self.should_record_events() {
            self.events.push((
                obj,
                Event::Splice {
                    index,
                    text: text.to_string(),
                    marks,
                },
            ))
        }
    }

    pub(crate) fn mark(&mut self, obj: ObjId, index: usize, len: usize, marks: &Arc<MarkSet>) {
        if !self.should_record_events() {
            return;
        }
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
        if !self.should_record_events() {
            return;
        }
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
        if self.should_record_events() {
            let event = Event::Insert {
                index,
                value,
                id,
                conflict,
            };
            self.events.push((obj, event))
        }
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
        let clock = self.heads.as_ref().and_then(|h| doc.clock_at(h));
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
            // any objects exposed BEFORE exid get observed here — pump
            // before the skip check so the expose observation lands in
            // event order across history transitions
            expose_queue.pump_queue(&exid, &mut patch_builder, doc, clock.as_ref());
            // ignore events on objects in the expose queue
            // incremental updates are ignored and a observation
            // of the final state is used b/c observers did not see
            // past state changes
            if expose_queue.should_skip(&exid) {
                continue;
            }
            patch_builder.log_event(doc, exid, event);
        }
        // any objects exposed AFTER all other events get exposed here
        expose_queue.flush_queue(&mut patch_builder, doc, clock.as_ref());

        patch_builder.take_patches()
    }

    pub(crate) fn path_hint(&mut self, hint: BTreeMap<ObjId, (Prop, ObjId)>) {
        if self.should_record_events() {
            self.path_map = hint;
            self.path_hint = self.events.len();
        }
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
