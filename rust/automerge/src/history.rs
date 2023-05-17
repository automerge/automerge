//use crate::clock::Clock;
use crate::hydrate::Value;
use crate::marks::Mark;
use crate::types::{ObjId, OpId, Prop};
use crate::{Automerge, ChangeHash, OpObserver, ReadDoc};

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct History {
    events: Vec<(ObjId, Event)>,
    active: bool,
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) enum Event {
    PutMap {
        key: String,
        value: Value,
        id: OpId,
        conflict: bool,
        expose: bool,
    },
    PutSeq {
        index: usize,
        value: Value,
        id: OpId,
        conflict: bool,
        expose: bool,
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
            events: vec![],
        }
    }

    pub(crate) fn innactive() -> Self {
        History {
            active: false,
            events: vec![],
        }
    }

    pub(crate) fn active() -> Self {
        History {
            active: true,
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
        self.events.push((
            obj,
            Event::PutMap {
                key: key.into(),
                value,
                id,
                conflict,
                expose,
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
        self.events.push((
            obj,
            Event::PutSeq {
                index,
                value,
                id,
                conflict,
                expose,
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
                let read_doc = crate::automerge::diff::ReadDocAt { doc, heads };
                self.observe_inner(observer, doc, &read_doc);
            } else {
                self.observe_inner(observer, doc, doc);
            }
        }
    }

    fn observe_inner<O: OpObserver, R: ReadDoc>(
        &mut self,
        observer: &mut O,
        doc: &Automerge,
        read_doc: &R,
        //_clock: Option<&Clock>,
        //heads: Option<&[ChangeHash]>,
    ) {
        for (obj, event) in self.events.drain(..) {
            let exid = doc.id_to_exid(obj.0);
            match event {
                Event::PutMap {
                    key,
                    value,
                    id,
                    conflict,
                    expose,
                } => {
                    let opid = doc.id_to_exid(id);
                    if expose {
                        observer.expose(read_doc, exid, key.into(), (value.into(), opid), conflict);
                    } else {
                        observer.put(read_doc, exid, key.into(), (value.into(), opid), conflict);
                    }
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
                    expose,
                } => {
                    let opid = doc.id_to_exid(id);
                    if expose {
                        observer.expose(
                            read_doc,
                            exid,
                            index.into(),
                            (value.into(), opid),
                            conflict,
                        );
                    } else {
                        observer.put(read_doc, exid, index.into(), (value.into(), opid), conflict);
                    }
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
    }

    pub(crate) fn truncate(&mut self) {
        self.active = true;
        self.events.truncate(0);
    }

    pub(crate) fn branch(&mut self) -> Self {
        Self {
            active: self.active,
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
