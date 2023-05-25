use crate::automerge::diff::ReadDocAt;
use crate::exid::ExId;
use crate::hydrate::Value;
use crate::marks::Mark;
use crate::types::{ObjId, ObjType, OpId, Prop};
use crate::{Automerge, ChangeHash, OpObserver, ReadDoc};
use std::collections::BTreeSet;
use std::collections::HashSet;

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct History {
    events: Vec<(ObjId, Event)>,
    expose: HashSet<OpId>,
    active: bool,
}

#[derive(Clone, Debug, PartialEq)]
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
        mark: Vec<Mark<'static>>,
    },
}

impl History {
    pub(crate) fn new(active: bool) -> Self {
        History {
            active,
            expose: HashSet::default(),
            events: vec![],
        }
    }

    pub(crate) fn innactive() -> Self {
        History {
            active: false,
            expose: HashSet::default(),
            events: vec![],
        }
    }

    pub(crate) fn active() -> Self {
        History {
            active: true,
            expose: HashSet::default(),
            events: vec![],
        }
    }

    pub(crate) fn set_active(&mut self, setting: bool) {
        self.active = setting
    }

    pub(crate) fn is_active(&self) -> bool {
        self.active
    }

    pub(crate) fn delete(&mut self, obj: ObjId, prop: &Prop) {
        match prop {
            Prop::Map(key) => self.delete_map(obj, key),
            Prop::Seq(index) => self.delete_seq(obj, *index, 1),
        }
    }

    pub(crate) fn delete_seq(&mut self, obj: ObjId, index: usize, num: usize) {
        self.events.push((obj, Event::DeleteSeq { index, num }))
    }

    pub(crate) fn delete_map(&mut self, obj: ObjId, key: &str) {
        self.events
            .push((obj, Event::DeleteMap { key: key.into() }))
    }

    pub(crate) fn increment(&mut self, obj: ObjId, prop: &Prop, value: i64, id: OpId) {
        match prop {
            Prop::Map(key) => self.increment_map(obj, key, value, id),
            Prop::Seq(index) => self.increment_seq(obj, *index, value, id),
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

    pub(crate) fn splice(&mut self, obj: ObjId, index: usize, text: &str) {
        self.events.push((
            obj,
            Event::Splice {
                index,
                text: text.to_string(),
            },
        ))
    }

    pub(crate) fn mark(&mut self, obj: ObjId, marks: &[Mark<'_>]) {
        self.events.push((
            obj,
            Event::Mark {
                mark: marks.iter().map(|m| m.clone().into_owned()).collect(),
            },
        ))
    }

    pub(crate) fn insert(
        &mut self,
        obj: ObjId,
        index: usize,
        value: Value,
        id: OpId,
        conflict: bool,
    ) {
        self.events.push((
            obj,
            Event::Insert {
                index,
                value,
                id,
                conflict,
            },
        ))
    }

    pub(crate) fn observe<'a, O: OpObserver + 'a, T: Into<Option<&'a mut O>>>(
        &mut self,
        observer: T,
        doc: &Automerge,
        heads: Option<&[ChangeHash]>,
    ) {
        if let Some(observer) = observer.into() {
            self.events.sort_by(|a, b| doc.ops().m.lamport_cmp(a, b));
            if let Some(heads) = heads {
                let read_doc = ReadDocAt { doc, heads };
                self.observe_inner(observer, doc, &read_doc);
            } else {
                self.observe_inner(observer, doc, doc);
            }
        }
    }

    fn take_exposed(&mut self, doc: &Automerge) -> ExposeQueue {
        let mut alt_set = HashSet::new();
        std::mem::swap(&mut alt_set, &mut self.expose);
        ExposeQueue(alt_set.into_iter().map(|id| doc.id_to_exid(id)).collect())
    }

    fn observe_inner<O: OpObserver, R: ReadDoc>(
        &mut self,
        observer: &mut O,
        doc: &Automerge,
        read_doc: &R,
    ) {
        let mut expose_queue = self.take_exposed(doc);
        //let mut expose_queue = ExposeQueue::default();
        for (obj, event) in self.events.drain(..) {
            let exid = doc.id_to_exid(obj.0);
            // ignore events on objects in the expose queue
            // incremental updates are ignored and a observation
            // of the final state is used b/c observers did not see
            // past state changes
            if expose_queue.should_skip(&exid) {
                continue;
            }
            // any objects exposed BEFORE exid get observed here
            expose_queue.pump_queue(&exid, observer, doc, read_doc);
            match event {
                Event::PutMap {
                    key,
                    value,
                    id,
                    conflict,
                } => {
                    let opid = doc.id_to_exid(id);
                    observer.put(read_doc, exid, key.into(), (value.into(), opid), conflict);
                }
                Event::DeleteMap { key } => {
                    observer.delete_map(read_doc, exid, &key);
                }
                Event::IncrementMap { key, n, id } => {
                    let opid = doc.id_to_exid(id);
                    observer.increment(read_doc, exid, key.into(), (n, opid));
                }
                Event::FlagConflictMap { key } => {
                    observer.flag_conflict(read_doc, exid, key.into());
                }
                Event::PutSeq {
                    index,
                    value,
                    id,
                    conflict,
                } => {
                    let opid = doc.id_to_exid(id);
                    observer.put(read_doc, exid, index.into(), (value.into(), opid), conflict);
                }
                Event::Insert {
                    index,
                    value,
                    id,
                    conflict,
                } => {
                    let opid = doc.id_to_exid(id);
                    observer.insert(read_doc, exid, index, (value.into(), opid), conflict);
                }
                Event::DeleteSeq { index, num } => {
                    observer.delete_seq(read_doc, exid, index, num);
                }
                Event::IncrementSeq { index, n, id } => {
                    let opid = doc.id_to_exid(id);
                    observer.increment(read_doc, exid, index.into(), (n, opid));
                }
                Event::FlagConflictSeq { index } => {
                    observer.flag_conflict(read_doc, exid, index.into());
                }
                Event::Splice { index, text } => {
                    observer.splice_text(read_doc, exid, index, &text);
                }
                Event::Mark { mark } => observer.mark(read_doc, exid, mark.into_iter()),
            }
        }
        // any objects exposed AFTER all other events get exposed here
        expose_queue.flush_queue(observer, doc, read_doc);
    }

    pub(crate) fn truncate(&mut self) {
        self.active = true;
        self.events.truncate(0);
    }

    pub(crate) fn branch(&mut self) -> Self {
        Self {
            active: self.active,
            expose: HashSet::new(),
            events: Default::default(),
        }
    }

    pub(crate) fn merge(&mut self, other: Self) {
        self.events.extend(other.events);
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

    fn pump_queue<O: OpObserver, R: ReadDoc>(
        &mut self,
        obj: &ExId,
        observer: &mut O,
        doc: &Automerge,
        read_doc: &R,
    ) {
        while let Some(exposed) = self.0.first() {
            if exposed >= obj {
                break;
            }
            self.flush_obj(exposed.clone(), observer, doc, read_doc);
        }
    }

    fn flush_queue<O: OpObserver, R: ReadDoc>(
        &mut self,
        observer: &mut O,
        doc: &Automerge,
        read_doc: &R,
    ) {
        while let Some(exposed) = self.0.first() {
            self.flush_obj(exposed.clone(), observer, doc, read_doc);
        }
    }

    fn insert(&mut self, obj: ExId) -> bool {
        self.0.insert(obj)
    }

    fn remove(&mut self, obj: &ExId) -> bool {
        self.0.remove(obj)
    }

    fn flush_obj<O: OpObserver, R: ReadDoc>(
        &mut self,
        exid: ExId,
        observer: &mut O,
        doc: &Automerge,
        read_doc: &R,
    ) -> Option<()> {
        let id = exid.to_internal_obj();
        self.remove(&exid);
        match doc.ops().object_type(&id)? {
            ObjType::Text if doc.text_as_seq() => {
                let text = read_doc.text(&exid).ok()?;
                observer.splice_text(read_doc, exid, 0, &text);
            }
            ObjType::List | ObjType::Text => {
                for (index, value, id, conflict) in read_doc.list_range(&exid, ..) {
                    if value.is_object() {
                        self.insert(id.clone());
                    }
                    observer.insert(read_doc, exid.clone(), index, (value, id), conflict);
                }
            }
            ObjType::Map | ObjType::Table => {
                for (key, value, id, conflict) in read_doc.map_range(&exid, ..) {
                    if value.is_object() {
                        self.insert(id.clone());
                    }
                    observer.put(read_doc, exid.clone(), key.into(), (value, id), conflict);
                }
            }
        }
        Some(())
    }
}
