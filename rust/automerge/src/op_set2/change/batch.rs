use crate::hydrate::Value;
use crate::iter::RichTextDiff;
use crate::op_set2::types::{Action, KeyRef, MarkData, PropRef};
use crate::op_set2::SuccInsert;
use crate::types::{
    ActorId, ElemId, ObjId, ObjType, OpId, Prop, ScalarValue, SequenceType, SmallHashMap,
};
use crate::{Automerge, Change, ChangeHash, PatchLog};
use crate::{AutomergeError, TextEncoding};

use super::super::op::{ChangeOp, Op, OpBuilder};
use super::super::op_set::{ObjIdIter, ObjIndex, OpIter, OpSet};

use std::borrow::Cow;
use std::cmp::Ordering;
use std::collections::{HashMap, HashSet};
use std::ops::Range;

type PredCache = SmallHashMap<OpId, Vec<(OpId, Option<i64>)>>;

#[derive(Debug, Clone, Default)]
struct BatchApply {
    ops: Vec<ChangeOp>,
    changes: Vec<Change>,
    actor_seq: HashMap<ActorId, HashSet<u64>>,
    actor_author: HashSet<ActorId>,
    hashes: HashSet<ChangeHash>,
    pred: PredCache,
    obj_spans: Vec<ObjSpan>,
}

struct Untangler<'a> {
    // the tangle of change ops we need to navigate
    change_ops: &'a mut [ChangeOp],
    // these are entry points into the change_op tangle
    // when we see a doc_op.id equal to key, put this vec onto the stack
    // ops inserted after HEAD are put onto the stack immidately
    entry: SmallHashMap<OpId, Vec<usize>>,
    // same concept as entry but internal to the change_ops array
    gosub: SmallHashMap<usize, Vec<usize>>,
    // stack of change ops ready to be processed
    stack: Vec<usize>,
    // these are change ops updating a pre-existing doc op elemid's
    // and are handled differently than inserts
    updates: SmallHashMap<ElemId, Vec<usize>>,
    updates_stack: Vec<usize>,
    pred: &'a mut PredCache,
    // Top and Conflicts keep track of changes that need to
    // be made to the index.top and index.visible columns
    top: Top,
    conflicts: &'a mut Vec<Adjust>,
    value: ValueState<'a>,
    seq_type: SequenceType,
    text_encoding: TextEncoding,
    count: usize,
    index: usize,
    max: usize,
    width: usize,
}

impl<'a> Untangler<'a> {
    fn flush(&mut self, log: &mut PatchLog) {
        self.value.list_flush(self.index, log);
        self.top.reset(self.conflicts);
        self.index += self.width;
        self.width = 0;
    }

    fn handle_doc_op(&mut self, doc_op: &Op<'a>, succ: &mut Vec<SuccInsert>, log: &mut PatchLog) {
        let mut deleted = false;
        if let Some(v) = self.pred.remove(&doc_op.id) {
            for (id, inc) in v {
                deleted |= inc.is_none();
                succ.push(doc_op.add_succ(id, inc));
            }
        }

        if doc_op.insert {
            self.flush(log);
            self.value.key = Some(PropRef::Seq(self.index));
        }

        if doc_op.visible() && !deleted {
            self.width = doc_op.width(self.seq_type, self.text_encoding);
        }
        self.value.process_doc_op(doc_op, deleted);
        self.top.process_doc_op(self.change_ops, doc_op, deleted);
    }

    fn element_update(&mut self, doc_op: &Op<'_>) {
        while let Some(last) = self.updates_stack.last().copied() {
            if doc_op.insert || doc_op.id > self.change_ops[last].id() {
                self.updates_stack.pop();
                self.change_ops[last].pos = Some(doc_op.pos);
                self.change_ops[last].subsort = self.count;
                if self.change_ops[last].visible() {
                    self.width = self.change_ops[last].width(self.seq_type, self.text_encoding);
                }
                self.value.process_change_op(&self.change_ops[last]);
                self.count += 1;
                self.top
                    .process_change_op(self.conflicts, self.change_ops, last);
            } else {
                break;
            }
        }
    }

    fn finish_updates(&mut self) {
        for i in self.updates_stack.iter().rev() {
            self.change_ops[*i].pos = Some(self.max);
            self.change_ops[*i].subsort = self.count;
            self.width = 0;
            if self.change_ops[*i].visible() {
                self.width = self.change_ops[*i].width(self.seq_type, self.text_encoding);
            }
            self.value.process_change_op(&self.change_ops[*i]);

            self.top
                .process_change_op(self.conflicts, self.change_ops, *i);

            self.count += 1;
        }
    }

    fn finish_inserts(&mut self, log: &mut PatchLog) {
        while !self.stack.is_empty() {
            self.untangle_inner(self.max, log);
        }
    }

    fn finish(mut self, log: &mut PatchLog) {
        self.finish_updates();

        self.flush(log);

        assert!(self.entry.is_empty());

        self.finish_inserts(log);

        self.flush(log);

        assert_eq!(self.top, Top::Nothing);

        self.change_ops.sort_by(|a, b| {
            a.pos
                .unwrap()
                .cmp(&b.pos.unwrap())
                .then_with(|| a.subsort.cmp(&b.subsort))
        });
    }

    fn untangle_inserts(&mut self, id: OpId, insert_pos: usize, log: &mut PatchLog) {
        self.flush(log);

        if let Err(n) = self
            .stack
            .binary_search_by(|n| self.change_ops[*n].id().cmp(&id))
        {
            while self.stack.len() > n {
                self.untangle_inner(insert_pos, log);
            }
        }
        if let Some(v) = self.entry.remove(&id) {
            self.stack.extend(v);
        }
        if let Some(u) = self.updates.get(&ElemId(id)) {
            self.updates_stack.extend(u.iter().rev());
        }
    }

    fn untangle_inner(&mut self, insert_pos: usize, log: &mut PatchLog) -> Option<()> {
        let mut pos = self.stack.pop()?;
        let op = self.change_ops.get_mut(pos)?;

        let mut conflict = false;
        let mut vis = None;

        let key = KeyRef::Seq(ElemId(op.id()));

        assert!(op.pos.is_none());
        op.pos = Some(insert_pos);
        op.subsort = self.count;
        self.count += 1;

        if op.is_set_or_make() && !op.has_succ() {
            vis = Some(pos);
        } else if op.action() == Action::Mark {
            self.value.process_mark(op.id(), op.mark_data());
        }

        if let Some(v) = self.gosub.get(&pos) {
            self.stack.extend(v);
        }

        for i in (pos + 1)..self.change_ops.len() {
            let next_vis = {
                let next_op = &mut self.change_ops[i];

                pos += 1;

                if next_op.insert() || next_op.key() != &key {
                    break;
                }

                next_op.pos = Some(insert_pos);
                next_op.subsort = self.count;
                self.count += 1;

                next_op.is_set_or_make() && !next_op.has_succ()
            };

            if next_vis {
                // borrow checker stuff
                if let Some(conflict_pos) = vis {
                    self.change_ops[conflict_pos].conflicted = true;
                    conflict = true;
                }
                vis = Some(pos);
            }
        }

        if let Some(p) = vis {
            let op = &mut self.change_ops[p];
            if self.seq_type == SequenceType::List {
                let value = op.hydrate_value_and_fix_counters(self.text_encoding);
                log.insert(op.bld.obj, self.index, value, op.id(), conflict);
                self.index += 1;
            } else {
                let marks = self.value.marks.after.current().cloned();
                match op.bld.action {
                    Action::MakeMap => {
                        // Block markers
                        log.insert(
                            op.bld.obj,
                            self.index,
                            Value::map(),
                            op.bld.id,
                            op.conflicted,
                        );
                    }
                    _ => {
                        log.splice(op.bld.obj, self.index, op.bld.as_str(), marks);
                    }
                }
                self.index += op.width(self.seq_type, self.text_encoding);
            }
        }

        Some(())
    }

    fn new(
        obj: ObjId,
        encoding: SequenceType,
        text_encoding: TextEncoding,
        conflicts: &'a mut Vec<Adjust>,
        change_ops: &'a mut [ChangeOp],
        pred: &'a mut PredCache,
        max: usize,
    ) -> Self {
        let mut e_to_i = SmallHashMap::default();
        let mut gosub: SmallHashMap<usize, Vec<usize>> = HashMap::default();
        let mut entry: SmallHashMap<OpId, Vec<usize>> = HashMap::default();
        let mut stack: Vec<usize> = Vec::with_capacity(change_ops.len());
        let mut updates: SmallHashMap<ElemId, Vec<usize>> = HashMap::default();
        let mut last_e = None;
        let value = ValueState::new(obj, encoding, text_encoding);
        for (i, op) in change_ops.iter_mut().enumerate() {
            if let Some(v) = pred.remove(&op.id()) {
                op.succ = v;
            }
            if let KeyRef::Seq(e) = op.key() {
                if op.insert() {
                    if let Some(j) = e_to_i.get(e) {
                        gosub.entry(*j).or_default().push(i);
                    } else if e.is_head() {
                        stack.push(i);
                    } else {
                        entry.entry(e.0).or_default().push(i);
                    }
                } else if last_e != Some(*e) {
                    updates.entry(*e).or_default().push(i);
                }
                if op.insert() {
                    let this_e = ElemId(op.id());
                    e_to_i.insert(this_e, i);
                    last_e = Some(this_e);
                }
            }
        }
        let updates_stack = Vec::with_capacity(change_ops.len());
        Self {
            gosub,
            entry,
            stack,
            pred,
            change_ops,
            updates,
            seq_type: encoding,
            text_encoding,
            conflicts,
            updates_stack,
            top: Top::Nothing,
            count: 0,
            index: 0,
            width: 0,
            value,
            max,
        }
    }
}

fn walk_list<'a>(
    mut ut: Untangler<'a>,
    doc_ops: OpIter<'a>,
    succ: &mut Vec<SuccInsert>,
    log: &mut PatchLog,
) {
    for op in doc_ops {
        ut.element_update(&op);

        if op.insert {
            ut.untangle_inserts(op.id, op.pos, log);
        }

        ut.handle_doc_op(&op, succ, log);
    }

    ut.finish(log);
}

struct MapWalker<'a, 'b> {
    ops: OpIter<'a>,
    log: &'b mut PatchLog,
    pred: &'b mut PredCache,
    succ: &'b mut Vec<SuccInsert>,
    value: ValueState<'a>,
    pos: usize,
    doc_op: Option<Op<'a>>,
    conflicts: &'b mut Vec<Adjust>,
    top: Top,
}

#[derive(Debug, Clone)]
enum Adjust {
    Conflict(usize),
    Expose(usize),
}

#[derive(PartialEq, Debug)]
enum Top {
    Nothing,
    ChangeIndex(usize),
    Doc(usize),
    Expose(usize),
}

impl Top {
    fn reset(&mut self, conflicts: &mut Vec<Adjust>) {
        if let Top::Expose(i) = self {
            conflicts.push(Adjust::Expose(*i));
        }
        *self = Top::Nothing;
    }

    fn process_doc_op(&mut self, ops: &mut [ChangeOp], d: &Op<'_>, deleted: bool) {
        if d.visible() {
            if deleted {
                if let Top::Doc(i) = self {
                    *self = Top::Expose(*i)
                }
            } else {
                if let Top::ChangeIndex(i) = self {
                    ops[*i].conflicted = true
                }
                *self = Top::Doc(d.pos);
            }
        }
    }

    fn process_change_op(&mut self, conflicts: &mut Vec<Adjust>, ops: &mut [ChangeOp], pos: usize) {
        if ops[pos].visible() {
            match self {
                Top::ChangeIndex(i) => ops[*i].conflicted = true,
                Top::Doc(i) => conflicts.push(Adjust::Conflict(*i)),
                _ => {}
            }
            *self = Top::ChangeIndex(pos);
        }
    }
}

impl<'a, 'b> MapWalker<'a, 'b> {
    fn new(
        obj: ObjId,
        mut ops: OpIter<'a>,
        text_encoding: TextEncoding,
        pred: &'b mut PredCache,
        succ: &'b mut Vec<SuccInsert>,
        log: &'b mut PatchLog,
        conflicts: &'b mut Vec<Adjust>,
    ) -> Self {
        let pos = ops.pos();
        let doc_op = ops.next();
        let value = ValueState::new(obj, SequenceType::List, text_encoding);
        let top = Top::Nothing;
        MapWalker {
            ops,
            log,
            pos,
            doc_op,
            pred,
            succ,
            value,
            conflicts,
            top,
        }
    }

    fn next_doc_op(&mut self) {
        self.pos = self.ops.pos();
        self.doc_op = self.ops.next();
    }

    fn change_op(&mut self, ops: &mut [ChangeOp], pos: usize) {
        if let Some(v) = self.pred.remove(&ops[pos].id()) {
            ops[pos].succ = v
        }

        self.advance_doc_op(pos, ops);

        if ops[pos].prop() != self.value.key {
            self.value.map_flush(self.log);
            self.value.key = ops[pos].prop_static();
            self.top.reset(self.conflicts);
        }
        self.value.process_change_op(&ops[pos]);

        ops[pos].pos = Some(self.pos);

        self.top.process_change_op(self.conflicts, ops, pos);
    }

    fn advance_doc_op(&mut self, pos: usize, ops: &mut [ChangeOp]) {
        while let Some(d) = self.doc_op.as_ref() {
            // TODO - sometimes we can fast forward to the next property
            match d.key.partial_cmp(ops[pos].key()) {
                Some(Ordering::Greater) => break,
                Some(Ordering::Equal) if d.id > ops[pos].id() => break,
                _ => {
                    let deleted = process_pred(self.doc_op.as_ref(), self.pred, self.succ);
                    if d.prop() != self.value.key {
                        self.value.map_flush(self.log);
                        self.value.key = d.prop();
                        self.top.reset(self.conflicts);
                    }
                    self.value.process_doc_op(d, deleted);
                    self.top.process_doc_op(ops, d, deleted);
                }
            }
            self.next_doc_op();
        }
    }

    fn finish(&mut self, ops: &mut [ChangeOp]) {
        while let Some(d) = self.doc_op.as_ref() {
            let deleted = process_pred(self.doc_op.as_ref(), self.pred, self.succ);
            if d.prop() == self.value.key {
                self.top.process_doc_op(ops, d, deleted);
                self.value.process_doc_op(d, deleted);
                self.next_doc_op();
            } else {
                break;
            }
        }
        self.value.map_flush(self.log);
        self.top.reset(self.conflicts);
    }
}

fn process_pred(doc_op: Option<&Op<'_>>, pred: &mut PredCache, succ: &mut Vec<SuccInsert>) -> bool {
    if let Some(d) = doc_op {
        let mut deleted = false;
        if let Some(v) = pred.remove(&d.id) {
            for (id, inc) in v {
                deleted |= inc.is_none();
                succ.push(d.add_succ(id, inc));
            }
        }
        deleted
    } else {
        false
    }
}

#[derive(Debug, Clone)]
struct ValueState<'a> {
    obj: ObjId,
    seq_type: SequenceType,
    text_encoding: TextEncoding,
    key: Option<PropRef<'a>>,
    doc: OpValueOption,
    change: OpValueOption,
    marks: RichTextDiff<'a>,
}

#[derive(Debug, Clone)]
struct OpValue {
    id: OpId,
    value: Value,
    deleted: bool,
    conflict: bool,
    expose: bool,
}

#[derive(Debug, Default, Clone)]
struct OpValueOption(Option<OpValue>);

impl OpValueOption {
    fn id(&self) -> Option<OpId> {
        self.value().map(|o| o.id)
    }

    fn increment(&mut self, n: i64) {
        if let Self(Some(ov)) = self {
            if let Value::Scalar(ScalarValue::Counter(c)) = &mut ov.value {
                c.increment(n);
            }
        }
    }

    fn expose(&mut self) {
        if let Self(Some(ov)) = self {
            ov.expose = true;
        }
    }

    fn set(&mut self, value: Value, id: OpId, deleted: bool) {
        if deleted && self.is_visible() {
            self.expose();
        } else {
            let conflict = self.is_visible();
            *self = Self(Some(OpValue {
                value,
                id,
                conflict,
                deleted,
                expose: false,
            }));
        }
    }

    fn is_none(&self) -> bool {
        self.value().is_none()
    }

    fn value(&self) -> Option<&OpValue> {
        self.0.as_ref()
    }

    fn is_visible(&self) -> bool {
        self.value().map(|o| !o.deleted).unwrap_or(false)
    }

    fn is_deleted(&self) -> bool {
        self.value().map(|o| o.deleted).unwrap_or(false)
    }

    fn take(&mut self) -> Self {
        Self(self.0.take())
    }

    fn into_value(self) -> Option<OpValue> {
        self.0
    }
}

impl<'a> ValueState<'a> {
    fn new(obj: ObjId, encoding: SequenceType, text_encoding: TextEncoding) -> Self {
        Self {
            obj,
            seq_type: encoding,
            text_encoding,
            key: None,
            doc: OpValueOption(None),
            change: OpValueOption(None),
            marks: RichTextDiff::default(),
        }
    }

    fn process_doc_op(&mut self, doc_op: &Op<'a>, deleted: bool) {
        match doc_op.action {
            Action::Increment => {}
            Action::Mark => {
                self.marks.before.process(doc_op.id, doc_op.action());
                self.marks.after.process(doc_op.id, doc_op.action());
            }
            _ => {
                if doc_op.visible() {
                    self.doc
                        .set(doc_op.hydrate_value(self.text_encoding), doc_op.id, deleted);
                }
            }
        }
    }

    fn do_increment(&mut self, op: &ChangeOp) {
        if self.change.is_none() {
            if let Some(id) = self.doc.id() {
                if op.pred().contains(&id) && !self.doc.is_deleted() {
                    self.change = self.doc.clone();
                }
            }
        }
        if let Some(id) = self.change.id() {
            if op.pred().contains(&id) {
                self.change.increment(op.value().as_i64());
            }
        }
    }

    fn process_mark(&mut self, id: OpId, data: Option<MarkData<'static>>) {
        if let Some(data) = data {
            self.marks.after.mark_begin(id, data);
        } else {
            self.marks.after.mark_end(id);
        }
    }

    fn process_change_op(&mut self, op: &ChangeOp) {
        match op.action() {
            Action::Delete => {}
            Action::Increment => self.do_increment(op),
            Action::Mark => self.process_mark(op.id(), op.mark_data()),
            _ => {
                if op.visible() {
                    self.change
                        .set(op.hydrate_value(self.text_encoding), op.id(), false);
                }
            }
        }
    }

    fn map_flush(&mut self, log: &mut PatchLog) {
        let obj = self.obj;
        let change = self.change.take();
        let doc = self.doc.take();
        if let Some(PropRef::Map(key)) = self.key.take() {
            Self::map_process(obj, &key, doc, change, log);
        }
    }

    fn list_flush(&mut self, index: usize, log: &mut PatchLog) {
        if self.key.take().is_none() {
            return;
        }
        let obj = self.obj;
        let encoding = self.seq_type;
        if encoding == SequenceType::List {
            match (self.doc.0.take(), self.change.0.take()) {
                (None, Some(c)) => log.insert(obj, index, c.value, c.id, c.conflict),
                (Some(d), Some(c)) if d.id == c.id => {
                    let n = c.value.as_i64() - d.value.as_i64();
                    if n != 0 {
                        log.increment_seq(obj, index, n, c.id);
                    }
                }
                (Some(d), Some(c)) if c.id < d.id => {
                    log.flag_conflict(obj, &Prop::from(index));
                }
                (Some(d), Some(c)) => {
                    let conflict = !d.deleted || c.conflict;
                    log.put_seq(obj, index, c.value, c.id, conflict, false)
                }
                (Some(d), None) => {
                    if d.expose {
                        log.put_seq(obj, index, d.value, d.id, d.conflict, true);
                    } else if d.deleted {
                        log.delete_seq(obj, index, 1);
                    }
                }
                _ => {}
            }
        } else {
            match (self.doc.0.take(), self.change.0.take()) {
                (None, Some(c)) => {
                    match c.value {
                        Value::Scalar(_) => {
                            // I don't think this branch can ever actually happen in practice. If we
                            // reach here it's because there is a non-inserting operation (i.e. an
                            // update) to the operation at `index`, but we only allow insertions
                            // into text objects. Regardless, we handle this is a splice just in
                            // case
                            log.splice(obj, index, c.value.as_str(), self.marks.current().export());
                        }
                        _ => log.insert(obj, index, c.value, c.id, c.conflict),
                    }
                }
                (Some(d), None) if d.deleted => {
                    let w = d.value.width(self.seq_type, self.text_encoding);
                    log.delete_seq(obj, index, w);
                }
                (Some(d), None) => {
                    if let Some(m) = self.marks.current().export() {
                        log.mark(
                            obj,
                            index,
                            d.value.width(self.seq_type, self.text_encoding),
                            &m,
                        );
                    }
                }
                _ => {}
            }
        }
    }

    fn map_process(
        obj: ObjId,
        key: &str,
        doc: OpValueOption,
        change: OpValueOption,
        log: &mut PatchLog,
    ) {
        match (doc.into_value(), change.into_value()) {
            (None, Some(c)) => {
                log.put_map(obj, key, c.value, c.id, c.conflict, false);
            }
            (Some(d), None) => {
                if d.expose {
                    log.put_map(obj, key, d.value, d.id, d.conflict, true);
                } else if d.deleted {
                    log.delete_map(obj, key);
                }
            }
            (Some(d), Some(c)) if c.id > d.id => {
                let conflict = (c.conflict && !d.conflict) || !d.deleted;
                log.put_map(obj, key, c.value, c.id, conflict, false);
            }
            (Some(d), Some(c)) if c.id < d.id => {
                if !d.conflict {
                    log.flag_conflict(obj, &Prop::from(key));
                }
            }
            (Some(d), Some(c)) if d.id == c.id => {
                let n = c.value.as_i64() - d.value.as_i64();
                if n != 0 {
                    log.increment_map(obj, key, n, c.id);
                }
            }
            _ => {}
        }
    }
}

fn walk_map(mw: &mut MapWalker<'_, '_>, change_ops: &mut [ChangeOp]) {
    for pos in 0..change_ops.len() {
        mw.change_op(change_ops, pos);
    }
    mw.finish(change_ops);
}

impl BatchApply {
    fn has_change(&self, doc: &Automerge, hash: ChangeHash) -> bool {
        doc.change_graph.has_change(&hash)
            || self.hashes.contains(&hash)
            || doc.ready_q_has_hash(&hash)
    }

    fn push(&mut self, c: Change) {
        assert!(!self.has_actor_seq(&c));
        assert!(!self.has_actor_author(&c));
        self.record_actor_author_seq(&c);

        assert!(!self.hashes.contains(&c.hash()));
        self.hashes.insert(c.hash());

        self.changes.push(c);
    }

    fn record_actor_author_seq(&mut self, c: &Change) {
        if let Some(set) = self.actor_seq.get_mut(c.actor_id()) {
            set.insert(c.seq());
        } else {
            self.actor_seq
                .insert(c.actor_id().clone(), HashSet::from([c.seq()]));
        }
        if c.author().is_some() {
            self.actor_author.insert(c.actor_id().clone());
        }
    }

    fn has_actor_seq(&self, c: &Change) -> bool {
        self.actor_seq
            .get(c.actor_id())
            .map(|set| set.contains(&c.seq()))
            .unwrap_or(false)
    }

    fn has_actor_author(&self, c: &Change) -> bool {
        self.actor_author.contains(c.actor_id())
    }

    fn duplicate_seq(&self, doc: &Automerge, c: &Change) -> bool {
        doc.has_actor_seq(c) || self.has_actor_seq(c) || doc.ready_q_has_dupe_seq(c)
    }

    fn duplicate_author(&self, doc: &Automerge, c: &Change) -> bool {
        doc.has_actor_author(c) || self.has_actor_author(c) || doc.ready_q_has_dupe_author(c)
    }

    fn insert_new_actors(&mut self, doc: &mut Automerge) {
        for c in self.changes.iter().filter(|c| c.seq() == 1) {
            doc.put_actor_ref(c.actor_id());
        }
    }

    fn import_ops(&mut self, doc: &mut Automerge) {
        for c in &self.changes {
            doc.import_ops_to(c, &mut self.ops).unwrap();
            doc.update_history(c);
        }
        doc.remove_unused_actors(true);
    }

    pub(crate) fn apply(&mut self, doc: &mut Automerge, log: &mut PatchLog) {
        self.insert_new_actors(doc);

        log.migrate_actors(&doc.ops().actors).unwrap();

        self.import_ops(doc);

        let mut obj_info = doc.ops().obj_info.clone();

        self.order_ops_for_doc(&mut obj_info);

        let mut succ = vec![];

        let mut walker = ObjWalker::new(doc.ops());

        let mut conflicts = vec![];

        for os in &self.obj_spans {
            let obj_range = walker.seek_to_obj(os.obj);
            let doc_ops = doc.ops().iter_range(&obj_range);
            match obj_info.object_type(&os.obj) {
                Some(ObjType::Map) => {
                    let mut walker = MapWalker::new(
                        os.obj,
                        doc_ops,
                        doc.text_encoding(),
                        &mut self.pred,
                        &mut succ,
                        log,
                        &mut conflicts,
                    );
                    let change_ops = &mut self.ops[os.span.clone()];
                    walk_map(&mut walker, change_ops);
                }
                Some(otype) if otype.is_sequence() => {
                    let sequence_type = match otype {
                        ObjType::Text => SequenceType::Text,
                        ObjType::List => SequenceType::List,
                        _ => unreachable!(),
                    };
                    let ut = Untangler::new(
                        os.obj,
                        sequence_type,
                        doc.text_encoding(),
                        &mut conflicts,
                        &mut self.ops[os.span.clone()],
                        &mut self.pred,
                        doc_ops.end_pos(),
                    );
                    walk_list(ut, doc_ops, &mut succ, log);
                }
                _ => panic!("Obj {:?} Missing from Index", os.obj),
            }
        }

        #[cfg(debug_assertions)]
        {
            // should always be ordered correctly - just double checking
            let mut tmp_succ = succ.clone();
            tmp_succ.sort_by(|a, b| {
                a.pos
                    .cmp(&b.pos)
                    .then_with(|| a.sub_pos.cmp(&b.sub_pos))
                    .then_with(|| a.id.counter().cmp(&b.id.counter()))
                    .then_with(|| a.id.actor().cmp(&b.id.actor()))
            });
            debug_assert_eq!(succ, tmp_succ);
        }

        for a in conflicts {
            match a {
                Adjust::Conflict(index) => doc.ops.conflict(index),
                Adjust::Expose(index) => doc.ops.expose(index),
            }
        }

        doc.ops.add_succ(&succ);

        self.insert_runs_of_ops(doc);

        debug_assert!(doc.ops.validate_op_order());
    }

    fn insert_runs_of_ops(&mut self, doc: &mut Automerge) {
        let mut last_pos = None;
        let mut start = 0;
        let mut shift = 0;
        for (i, op) in self.ops.iter().enumerate() {
            if op.pos != last_pos {
                if let Some(pos) = last_pos {
                    let end = i;
                    shift += self.insert_ops(doc, pos + shift, start..end);
                    start = end;
                }
                last_pos = op.pos;
            }
        }
        if let Some(pos) = last_pos {
            self.insert_ops(doc, pos + shift, start..self.ops.len());
        }
    }

    pub(crate) fn insert_ops(&self, doc: &mut Automerge, pos: usize, range: Range<usize>) -> usize {
        let batch = &self.ops[range];
        let start = doc.ops().len();
        doc.ops_mut().splice(pos, batch);
        doc.ops().len() - start
    }

    pub(crate) fn order_ops_for_doc(&mut self, obj_info: &mut ObjIndex) {
        self.ops.sort_by(|a, b| {
            a.bld.obj.cmp(&b.bld.obj).then_with(|| {
                match a.elemid_or_key().partial_cmp(&b.elemid_or_key()) {
                    Some(Ordering::Equal) | None => a.bld.id.cmp(&b.bld.id),
                    Some(order) => order,
                }
            })
        });
        let mut start = 0;
        let mut last_obj = None;
        for (i, o) in self.ops.iter().enumerate() {
            for p in o.pred().iter() {
                self.pred
                    .entry(*p)
                    .or_default()
                    .push((o.id(), o.get_increment_value()));
            }
            if let Some(info) = o.obj_info() {
                obj_info.insert(o.id(), info)
            }
            if Some(o.bld.obj) != last_obj {
                if let Some(obj) = last_obj {
                    self.obj_spans.push(ObjSpan {
                        obj,
                        span: start..i,
                    });
                }
                start = i;
                last_obj = Some(o.bld.obj);
            }
        }
        if let Some(obj) = last_obj {
            let span = start..self.ops.len();
            self.obj_spans.push(ObjSpan { obj, span });
        }
    }
}

#[derive(Debug, Clone)]
struct ObjWalker<'a> {
    iter: ObjIdIter<'a>,
}

impl<'a> ObjWalker<'a> {
    fn new(ops: &'a OpSet) -> Self {
        let iter = ops.obj_id_iter();
        Self { iter }
    }

    fn seek_to_obj(&mut self, obj: ObjId) -> Range<usize> {
        self.iter.seek_to_value(obj)
    }
}

#[derive(Debug, Clone, Default)]
struct ObjSpan {
    obj: ObjId,
    span: Range<usize>,
}

impl Automerge {
    pub fn apply_changes_batch(
        &mut self,
        changes: impl IntoIterator<Item = Change> + Clone,
    ) -> Result<(), AutomergeError> {
        self.apply_changes_batch_log_patches(changes, &mut PatchLog::inactive())
    }

    pub fn apply_changes_batch_log_patches<I: IntoIterator<Item = Change>>(
        &mut self,
        changes: I,
        log: &mut PatchLog,
    ) -> Result<(), AutomergeError> {
        let mut chap = BatchApply::default();
        let mut result = Ok(());
        for c in changes {
            if !chap.has_change(self, c.hash()) {
                if chap.duplicate_seq(self, &c) {
                    result = Err(AutomergeError::DuplicateSeqNumber(
                        c.seq(),
                        c.actor_id().clone(),
                    ));
                    break;
                }
                if chap.duplicate_author(self, &c) {
                    result = Err(AutomergeError::DuplicateAuthor(
                        c.author().unwrap_or_default().into(),
                        c.actor_id().clone(),
                        c.seq(),
                    ));
                    break;
                }
                if self.is_causally_ready(&c, &chap.hashes) {
                    chap.push(c);
                } else {
                    self.queue.push(c);
                }
            }
        }
        if result.is_ok() {
            while let Some(c) = self.pop_next_causally_ready_change(&chap.hashes) {
                chap.push(c);
            }
        }
        chap.apply(self, log);

        result
    }

    fn ready_q_has_hash(&self, hash: &ChangeHash) -> bool {
        // if the queue gets huge this could be slow - maybe add an index
        self.queue.iter().any(|c| &c.hash() == hash)
    }

    fn ready_q_has_dupe_seq(&self, change: &Change) -> bool {
        // if the queue gets huge this could be slow - maybe add an index
        self.queue.iter().any(|c| {
            c.seq() == change.seq()
                && c.actor_id() == change.actor_id()
                && c.hash() != change.hash()
        })
    }

    fn ready_q_has_dupe_author(&self, change: &Change) -> bool {
        // if the queue gets huge this could be slow - maybe add an index
        if change.author().is_none() {
            false
        } else {
            self.queue.iter().any(|c| {
                c.author() == change.author()
                    && c.actor_id() == change.actor_id()
                    && c.hash() != change.hash()
            })
        }
    }

    fn is_causally_ready(&self, change: &Change, ready: &HashSet<ChangeHash>) -> bool {
        change
            .deps()
            .iter()
            .all(|d| self.change_graph.has_change(d) || ready.contains(d))
    }

    fn pop_next_causally_ready_change(&mut self, ready: &HashSet<ChangeHash>) -> Option<Change> {
        let mut index = 0;
        while index < self.queue.len() {
            if self.is_causally_ready(&self.queue[index], ready) {
                return Some(self.queue.swap_remove(index));
            }
            index += 1;
        }
        None
    }

    fn import_ops_to(
        &mut self,
        change: &Change,
        ops: &mut Vec<ChangeOp>,
    ) -> Result<(), AutomergeError> {
        let new_ops = self.import_ops(change)?;
        ops.extend(new_ops);
        Ok(())
    }

    fn import_ops(&mut self, change: &Change) -> Result<Vec<ChangeOp>, AutomergeError> {
        let actors: Vec<_> = change
            .actors()
            .map(|a| self.ops.lookup_actor(a).unwrap())
            .collect();

        change
            .iter_ops()
            .enumerate()
            .map(|(i, c)| {
                let id = OpId::new(change.start_op().get() + i as u64, 0).map(&actors)?;
                let key = c.key.map(&actors)?;
                let obj = c.obj.map(&actors)?;
                let pred = c
                    .pred
                    .into_iter()
                    .map(|id| id.map(&actors))
                    .collect::<Result<Vec<_>, _>>()?;
                let bld = OpBuilder {
                    id,
                    obj,
                    key,
                    action: c.action.try_into()?,
                    value: c.val.into_ref(),
                    mark_name: c.mark_name.map(String::from).map(Cow::Owned),
                    expand: c.expand,
                    insert: c.insert,
                    pred,
                };
                let change = ChangeOp {
                    pos: None,
                    subsort: 0,
                    conflicted: false,
                    succ: vec![],
                    bld,
                };
                Ok(change)
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::marks::{ExpandMark, Mark};
    use crate::read::ReadDoc;
    use crate::transaction::Transactable;
    use crate::types;
    use crate::{ActorId, AutoCommit, ROOT};
    use rand::prelude::*;

    impl AutoCommit {
        fn apply_changes_iter(
            &mut self,
            changes: impl IntoIterator<Item = Change> + Clone,
        ) -> Result<(), AutomergeError> {
            let changes = changes.into_iter().collect::<Vec<_>>();
            for c in changes {
                self.apply_changes([c])?;
            }
            self.validate_top_index();
            Ok(())
        }
    }

    #[test]
    fn map_batch_apply() {
        let actor3 = ActorId::try_from("aaaaaa").unwrap();
        let actor2 = ActorId::try_from("bbbbbb").unwrap();
        let actor1 = ActorId::try_from("cccccc").unwrap();

        let mut doc1 = AutoCommit::new().with_actor(actor1);
        let map1 = doc1.put_object(&ROOT, "map", ObjType::Map).unwrap();
        doc1.put(&map1, "key1", "val1").unwrap();
        doc1.put(&map1, "key2", "val2").unwrap();

        let heads1 = doc1.get_heads();

        let mut doc2 = doc1.fork().with_actor(actor2);
        doc2.put(&map1, "key1", "val3a").unwrap();
        doc2.put(&map1, "key1", "val3a.1").unwrap();
        doc2.put(&map1, "key1", "val3a.2").unwrap();
        doc2.delete(&map1, "key2").unwrap();
        doc2.put(&map1, "key3", "val4a").unwrap();
        let map2 = doc2.put_object(&map1, "map2", ObjType::Map).unwrap();
        doc2.put(&map2, "key1", "val5a").unwrap();

        let map3 = doc1.put_object(&map1, "map3", ObjType::Map).unwrap();
        doc1.put(&map1, "key1", "val6a").unwrap();
        doc1.put(&map3, "key1", "val7a").unwrap();

        let mut doc3 = doc1.fork().with_actor(actor3);
        doc3.put(&map1, "key1", "val3b").unwrap();
        doc3.put(&map1, "key3", "val4b").unwrap();

        let mut doc1_test = doc1.fork();
        let mut changes2 = doc2.get_changes(&heads1);

        let changes3 = doc3.get_changes(&heads1);
        changes2.extend(changes3);

        doc1.apply_changes_iter(changes2.clone()).unwrap();
        doc1_test.doc.apply_changes_batch(changes2.clone()).unwrap();
        doc1_test.validate_top_index();

        doc1.dump();
        doc1_test.dump();
        doc1.doc.debug_cmp(&doc1_test.doc);
    }

    #[test]
    fn list_batch_apply() {
        let actor3 = ActorId::try_from("aaaaaa").unwrap();
        let actor2 = ActorId::try_from("bbbbbb").unwrap();
        let actor1 = ActorId::try_from("cccccc").unwrap();

        let mut doc1 = AutoCommit::new().with_actor(actor1);
        let list = doc1.put_object(&ROOT, "list", ObjType::List).unwrap();
        doc1.insert(&list, 0, "val1").unwrap();
        doc1.insert(&list, 1, "val2").unwrap();
        doc1.insert(&list, 2, "val3").unwrap();

        let heads1 = doc1.get_heads();

        let mut doc2 = doc1.fork().with_actor(actor2);
        doc2.insert(&list, 1, "val4a").unwrap();
        doc2.insert(&list, 1, "val4b").unwrap();
        doc2.insert(&list, 2, "val4c").unwrap();
        doc2.insert(&list, 0, "val4d").unwrap();
        doc2.insert(&list, 0, "val4e").unwrap();
        doc2.insert(&list, 0, "val4f").unwrap();

        let mut doc3 = doc1.fork().with_actor(actor3);
        doc3.insert(&list, 1, "val5a").unwrap();
        doc3.insert(&list, 1, "val5b").unwrap();
        doc3.insert(&list, 2, "val5c").unwrap();
        doc3.insert(&list, 3, "val5d").unwrap();
        doc3.insert(&list, 1, "val5e").unwrap();
        doc3.insert(&list, 1, "val5f").unwrap();
        doc3.insert(&list, 0, "val5g").unwrap();
        doc3.insert(&list, 0, "val5h").unwrap();

        let mut doc1_test = doc1.fork();
        let mut changes2 = doc2.get_changes(&heads1);
        let changes3 = doc3.get_changes(&heads1);
        changes2.extend(changes3);

        doc1.apply_changes_iter(changes2.clone()).unwrap();
        doc1_test.doc.apply_changes_batch(changes2.clone()).unwrap();
        doc1_test.validate_top_index();

        doc1.dump();
        doc1_test.dump();

        doc1.doc.debug_cmp(&doc1_test.doc);
    }

    #[test]
    fn text_batch_apply() {
        let actor3 = ActorId::try_from("aaaaaa").unwrap();
        let actor2 = ActorId::try_from("bbbbbb").unwrap();
        let actor1 = ActorId::try_from("cccccc").unwrap();

        let mut doc1 = AutoCommit::new().with_actor(actor1);
        let text = doc1.put_object(&ROOT, "text", ObjType::Text).unwrap();
        doc1.splice_text(&text, 0, 0, "the quick fox jumped over the lazy dog")
            .unwrap();

        let heads1 = doc1.get_heads();

        let mut doc2 = doc1.fork().with_actor(actor2);
        doc2.splice_text(&text, 0, 0, "abc").unwrap();

        let mut doc3 = doc1.fork().with_actor(actor3);
        doc3.splice_text(&text, 3, 1, "aalks").unwrap();

        let mut doc1_test = doc1.fork();
        let mut changes2 = doc2.get_changes(&heads1);
        let changes3 = doc3.get_changes(&heads1);
        changes2.extend(changes3);

        doc1.apply_changes_iter(changes2.clone()).unwrap();
        doc1_test.apply_changes_batch(changes2.clone()).unwrap();
        doc1_test.validate_top_index();

        doc1.dump();
        doc1_test.dump();

        doc1.doc.debug_cmp(&doc1_test.doc);
        //assert_eq!(doc1.save(), doc1_test.save());
    }

    #[test]
    fn multi_put_batch_apply() {
        let mut rng = make_rng();
        let mut doc1 = AutoCommit::new().with_actor(rng.random());
        let list = doc1.put_object(&ROOT, "list", ObjType::List).unwrap();
        doc1.insert(&list, 0, "a").unwrap();
        doc1.insert(&list, 1, "b").unwrap();
        doc1.insert(&list, 2, "c").unwrap();
        let heads = doc1.get_heads();

        let mut doc2 = doc1.fork().with_actor(rng.random());
        for i in 0..10 {
            let mut tmp = doc1.fork().with_actor(rng.random());
            tmp.put(&list, 0, i).unwrap();
            doc2.merge(&mut tmp).unwrap();
        }
        let changes = doc2.get_changes(&heads);
        doc1.apply_changes_batch(changes).unwrap();
        doc1.validate_top_index();
        assert_eq!(doc1.save(), doc2.save());
    }

    #[test]
    fn multi_insert_batch_apply() {
        let mut rng = make_rng();
        let mut doc1 = AutoCommit::new().with_actor(rng.random());
        let list = doc1.put_object(&ROOT, "list", ObjType::List).unwrap();
        doc1.insert(&list, 0, "a").unwrap();
        doc1.insert(&list, 1, "b").unwrap();
        doc1.insert(&list, 2, "c").unwrap();
        let heads = doc1.get_heads();

        let mut doc2 = doc1.fork().with_actor(rng.random());

        for i in 0..10 {
            let mut tmp = doc1.fork().with_actor(rng.random());
            tmp.insert(&list, 1, i).unwrap();
            //let change = tmp.get_last_local_change().unwrap();
            doc2.merge(&mut tmp).unwrap();
        }

        let changes = doc2.get_changes(&heads);
        doc1.apply_changes_batch(changes).unwrap();
        doc1.validate_top_index();
        assert_eq!(doc1.save(), doc2.save());
    }

    #[test]
    fn multi_update_batch_apply() {
        let mut rng = make_rng();
        let mut doc1 = AutoCommit::new().with_actor(rng.random());
        let list = doc1.put_object(&ROOT, "list", ObjType::List).unwrap();
        doc1.insert(&list, 0, "a").unwrap();
        doc1.insert(&list, 1, "b").unwrap();
        doc1.insert(&list, 2, "c").unwrap();
        let heads = doc1.get_heads();

        let mut doc2 = doc1.fork().with_actor(rng.random());

        for i in 0..3 {
            let mut tmp = doc1.fork().with_actor(rng.random());
            tmp.put(&list, 2, i).unwrap();
            doc2.merge(&mut tmp).unwrap();
        }

        let changes = doc2.get_changes(&heads);
        doc1.apply_changes_batch(changes).unwrap();
        doc1.validate_top_index();
        assert_eq!(doc1.save(), doc2.save());
    }

    fn make_rng() -> SmallRng {
        let seed = std::env::var("AUTOMERGE_TEST_SEED")
            .ok()
            .and_then(|s| s.parse::<u64>().ok())
            .unwrap_or_else(rand::random::<u64>);
        log!("SEED: {}", seed);
        SmallRng::seed_from_u64(seed)
    }

    #[test]
    fn fuzz_batch_list_apply() {
        let mut rng = make_rng();
        let mut doc1 = AutoCommit::new().with_actor(rng.random());
        let list = doc1.put_object(&ROOT, "list", ObjType::List).unwrap();
        doc1.insert(&list, 0, "a").unwrap();
        doc1.insert(&list, 1, "b").unwrap();
        doc1.insert(&list, 2, "c").unwrap();
        let mut value = 0;
        let mut val = move || {
            value += 1;
            value
        };
        let heads = doc1.get_heads();

        let mut doc1_tmp = doc1.fork().with_actor(rng.random());
        let mut doc2 = doc1.fork().with_actor(rng.random());

        for _ in 0..3 {
            for _ in 0..30 {
                let mut tmp = doc1_tmp.fork().with_actor(rng.random());
                let num_inserts = rng.random::<u32>() % 10 + 1;
                let num_updates = rng.random::<u32>() % 10 + 1;
                let num_deletes = rng.random::<u32>() % 2;
                for _ in 0..num_inserts {
                    let len = tmp.length(&list) as u32;
                    let pos = rng.random::<u32>() % len;
                    tmp.insert(&list, pos as usize, val()).unwrap();
                }
                for _ in 0..num_updates {
                    let len = tmp.length(&list) as u32;
                    let pos = rng.random::<u32>() % len;
                    tmp.put(&list, pos as usize, val()).unwrap();
                }
                for _ in 0..num_deletes {
                    let len = tmp.length(&list) as u32;
                    let pos = rng.random::<u32>() % len;
                    tmp.delete(&list, pos as usize).unwrap();
                }
                doc2.merge(&mut tmp).unwrap();
            }
            doc1_tmp.merge(&mut doc2).unwrap();
        }

        let changes = doc2.get_changes(&heads);
        doc1.apply_changes_batch(changes).unwrap();
        doc1.validate_top_index();
        assert_eq!(doc1.save(), doc2.save());
    }

    #[test]
    fn fuzz_batch_map1_apply() {
        let mut rng = make_rng();
        let mut doc1 = AutoCommit::new().with_actor(rng.random());
        let map1 = doc1.put_object(&ROOT, "map1", ObjType::Map).unwrap();
        let map2 = doc1.put_object(&map1, "map2", ObjType::Map).unwrap();
        let map3 = doc1.put_object(&map2, "map3", ObjType::Map).unwrap();
        let maps = [map1, map2, map3];
        let mut value = 0;
        let mut val = move || {
            value += 1;
            value
        };
        let heads = doc1.get_heads();

        let mut doc1_tmp = doc1.fork().with_actor(rng.random());
        let mut doc2 = doc1.fork().with_actor(rng.random());

        for _ in 0..3 {
            for _ in 0..30 {
                let mut tmp = doc1_tmp.fork().with_actor(rng.random());
                let num_updates = rng.random::<u32>() % 10 + 1;
                let num_deletes = rng.random::<u32>() % 2;
                for _ in 0..num_updates {
                    let key = format!("key{}", rng.random::<u32>() % 20);
                    let map = rng.random::<u32>() % (maps.len() as u32);
                    tmp.put(&maps[map as usize], key, val()).unwrap();
                }
                for _ in 0..num_deletes {
                    let key = format!("key{}", rng.random::<u32>() % 20);
                    let map = rng.random::<u32>() % (maps.len() as u32);
                    tmp.delete(&maps[map as usize], key).unwrap();
                }
                doc2.merge(&mut tmp).unwrap();
            }
            doc1_tmp.merge(&mut doc2).unwrap();
        }

        let changes = doc2.get_changes(&heads);
        doc1.apply_changes_batch(changes).unwrap();
        doc1.validate_top_index();
        assert_eq!(doc1.save(), doc2.save());
    }

    #[test]
    fn fuzz_batch_map2_apply() {
        let mut rng = make_rng();
        let mut doc1 = AutoCommit::new().with_actor(rng.random());
        let map1 = doc1.put_object(&ROOT, "map1", ObjType::Map).unwrap();
        let map2 = doc1.put_object(&map1, "map2", ObjType::Map).unwrap();
        let map3 = doc1.put_object(&map2, "map3", ObjType::Map).unwrap();
        let maps = [map1, map2, map3];
        let mut value = 0;
        let mut val = move || {
            value += 1;
            value
        };
        let heads = doc1.get_heads();

        let mut doc1_tmp = doc1.fork().with_actor(rng.random());
        let mut doc2 = doc1.fork().with_actor(rng.random());

        for _ in 0..3 {
            for _ in 0..30 {
                let mut tmp = doc1_tmp.fork().with_actor(rng.random());
                let num_updates = rng.random::<u32>() % 10 + 1;
                let num_deletes = rng.random::<u32>() % 2;
                for _ in 0..num_updates {
                    let key = format!("key{}", rng.random::<u32>() % 1000);
                    let map = rng.random::<u32>() % (maps.len() as u32);
                    tmp.put(&maps[map as usize], key, val()).unwrap();
                }
                for _ in 0..num_deletes {
                    let key = format!("key{}", rng.random::<u32>() % 1000);
                    let map = rng.random::<u32>() % (maps.len() as u32);
                    tmp.delete(&maps[map as usize], key).unwrap();
                }
                doc2.merge(&mut tmp).unwrap();
            }
            doc1_tmp.merge(&mut doc2).unwrap();
        }

        let changes = doc2.get_changes(&heads);

        let mut doc_a = doc1;
        let mut doc_b = doc_a.clone();

        doc_a.update_diff_cursor();
        doc_a.apply_changes_batch(changes.clone()).unwrap();
        doc_a.validate_top_index();
        doc_b.apply_changes_iter(changes).unwrap();

        let final_heads = doc_a.get_heads();

        assert_eq!(doc_a.save(), doc_b.save());

        let pa = doc_a.diff_incremental();
        let pb = doc_b.diff(&heads, &final_heads);

        let len = std::cmp::max(pa.len(), pb.len());

        for i in 0..len {
            if pa.get(i) != pb.get(i) {
                log!(" i={} ", i);
                log!(" pa={:?}", pa.get(i));
                log!(" pb={:?}", pb.get(i));
            }
        }

        if pa != pb {
            panic!()
        }
    }

    #[test]
    fn fuzz_batch_map_counter_apply() {
        let mut rng = make_rng();
        let mut doc1 = AutoCommit::new().with_actor(rng.random());
        let map1 = doc1.put_object(&ROOT, "map1", ObjType::Map).unwrap();
        doc1.put(&map1, "key1", ScalarValue::counter(10)).unwrap();
        doc1.increment(&map1, "key1", 15).unwrap();
        doc1.increment(&map1, "key1", 10).unwrap();
        let map2 = doc1.put_object(&map1, "map2", ObjType::Map).unwrap();
        doc1.put(&map2, "key1", ScalarValue::counter(100)).unwrap();
        doc1.increment(&map2, "key1", 20).unwrap();
        doc1.increment(&map2, "key1", 1).unwrap();
        doc1.delete(&map2, "key1").unwrap();
        doc1.put(&map2, "key1", ScalarValue::counter(101)).unwrap();
        doc1.increment(&map2, "key1", 1).unwrap();
        let map3 = doc1.put_object(&map2, "map3", ObjType::Map).unwrap();
        doc1.put(&map3, "key1", ScalarValue::counter(1000)).unwrap();
        doc1.increment(&map3, "key1", 30).unwrap();
        let maps = [map1, map2, map3];
        let mut value = 0;
        let mut val = move || {
            value += 1;
            value
        };
        let heads = doc1.get_heads();

        let mut doc1_tmp = doc1.fork().with_actor(rng.random());
        let mut doc2 = doc1.fork().with_actor(rng.random());

        for _ in 0..4 {
            for _ in 0..30 {
                let mut tmp = doc1_tmp.fork().with_actor(rng.random());
                let num_updates = rng.random::<u32>() % 10 + 1;
                let num_deletes = rng.random::<u32>() % 2;
                for _ in 0..num_updates {
                    let key = format!("key{}", rng.random::<u32>() % 30);
                    let map = rng.random::<u32>() % (maps.len() as u32);
                    if let Ok(Some((
                        types::Value::Scalar(Cow::Owned(ScalarValue::Counter(_))),
                        _,
                    ))) = tmp.get(&maps[map as usize], &key)
                    {
                        tmp.increment(&maps[map as usize], key, val()).unwrap();
                    } else {
                        tmp.put(&maps[map as usize], key, ScalarValue::counter(val()))
                            .unwrap();
                    }
                }
                for _ in 0..num_deletes {
                    let key = format!("key{}", rng.random::<u32>() % 30);
                    let map = rng.random::<u32>() % (maps.len() as u32);
                    tmp.delete(&maps[map as usize], key).unwrap();
                }
                doc2.merge(&mut tmp).unwrap();
            }
            doc1_tmp.merge(&mut doc2).unwrap();
        }

        let changes = doc2.get_changes(&heads);

        let mut doc_a = doc1;
        let mut doc_b = doc_a.clone();

        doc_a.update_diff_cursor();
        doc_a.apply_changes_batch(changes.clone()).unwrap();
        doc_a.validate_top_index();

        doc_b.apply_changes_iter(changes).unwrap();

        let final_heads = doc_a.get_heads();

        assert_eq!(doc_a.save(), doc_b.save());

        let pa = doc_a.diff_incremental();
        let pb = doc_b.diff(&heads, &final_heads);

        let len = std::cmp::max(pa.len(), pb.len());

        for i in 0..len {
            if pa.get(i) != pb.get(i) {
                log!(" i={} ", i);
                log!(" pa={:?}", pa.get(i));
                log!(" pb={:?}", pb.get(i));
            }
        }

        if pa != pb {
            panic!()
        }
    }

    #[test]
    fn batch_counter_list_patch() {
        let mut rng = make_rng();
        let mut value = 0;
        let mut val = move || {
            value += 1;
            value
        };
        let mut doc1 = AutoCommit::new().with_actor(rng.random());
        let list1 = doc1.put_object(&ROOT, "list1", ObjType::List).unwrap();
        doc1.insert(&list1, 0, ScalarValue::counter(val())).unwrap();
        doc1.insert(&list1, 1, ScalarValue::counter(val())).unwrap();
        doc1.insert(&list1, 2, ScalarValue::counter(val())).unwrap();

        let mut doc1_copy = doc1.fork().with_actor(rng.random());
        let mut doc2 = doc1.fork().with_actor(rng.random());
        let mut doc2_copy = doc1.fork().with_actor(rng.random());

        let mut changes = vec![];
        //for _ in 0..3 {
        for _ in 0..2 {
            //for _ in 0..10 {
            for _ in 0..2 {
                let mut tmp = doc2.fork().with_actor(rng.random());
                //let num_updates = rng.gen::<usize>() % 10 + 1;
                let num_updates = 2;
                //let num_inserts = rng.gen::<usize>() % 10 + 1;
                let num_inserts = 2;
                //let num_deletes = rng.gen::<usize>() % 2;
                let num_deletes = 1;
                for _ in 0..num_updates {
                    let len = tmp.length(&list1);
                    let index = rng.random::<u32>() % (len as u32);
                    tmp.increment(&list1, index as usize, val()).unwrap();
                }
                for _ in 0..num_inserts {
                    let len = tmp.length(&list1);
                    let index = rng.random::<u32>() % (len as u32);
                    tmp.insert(&list1, index as usize, ScalarValue::counter(val()))
                        .unwrap();
                }
                for _ in 0..num_deletes {
                    let len = tmp.length(&list1);
                    let index = rng.random::<u32>() % (len as u32);
                    tmp.delete(&list1, index as usize).unwrap();
                }
                let change = tmp.get_last_local_change().unwrap();
                changes.push(change);
            }
            merge_and_diff(&mut doc2, &mut doc2_copy, &changes);
        }
        merge_and_diff(&mut doc1, &mut doc1_copy, &changes);
    }

    #[test]
    fn batch_list_patch() {
        let mut rng = make_rng();
        let mut value = 0;
        let mut val = move || {
            value += 1;
            value
        };
        let mut doc1 = AutoCommit::new().with_actor(rng.random());
        let list1 = doc1.put_object(&ROOT, "list1", ObjType::List).unwrap();
        doc1.insert(&list1, 0, val()).unwrap();
        doc1.insert(&list1, 1, val()).unwrap();
        doc1.insert(&list1, 2, val()).unwrap();

        let mut doc1_copy = doc1.fork().with_actor(rng.random());
        let mut doc2 = doc1.fork().with_actor(rng.random());
        let mut doc2_copy = doc1.fork().with_actor(rng.random());

        let mut changes = vec![];
        for _ in 0..3 {
            for _ in 0..30 {
                let mut tmp = doc2.fork().with_actor(rng.random());
                let num_updates = rng.random::<u32>() % 10 + 1;
                let num_inserts = rng.random::<u32>() % 10 + 1;
                let num_deletes = rng.random::<u32>() % 2;
                for _ in 0..num_updates {
                    let len = tmp.length(&list1);
                    let index = rng.random::<u32>() % (len as u32);
                    tmp.put(&list1, index as usize, val()).unwrap();
                }
                for _ in 0..num_inserts {
                    let len = tmp.length(&list1);
                    let index = rng.random::<u32>() % (len as u32);
                    tmp.insert(&list1, index as usize, val()).unwrap();
                }
                for _ in 0..num_deletes {
                    let len = tmp.length(&list1);
                    let index = rng.random::<u32>() % (len as u32);
                    tmp.delete(&list1, index as usize).unwrap();
                }
                let change = tmp.get_last_local_change().unwrap();
                changes.push(change);
            }
            merge_and_diff(&mut doc2, &mut doc2_copy, &changes);
        }
        merge_and_diff(&mut doc1, &mut doc1_copy, &changes);
    }

    #[test]
    fn batch_text_patch() {
        let mut rng = make_rng();
        let mut value = 0;
        let mut val = move || {
            value += 1;
            value
        };
        let mut doc1 = AutoCommit::new().with_actor(rng.random());
        let text1 = doc1.put_object(&ROOT, "text1", ObjType::Text).unwrap();
        doc1.splice_text(&text1, 0, 0, "--------").unwrap();

        let mut doc1_copy = doc1.fork().with_actor(rng.random());
        let mut doc2 = doc1.fork().with_actor(rng.random());
        let mut doc2_copy = doc1.fork().with_actor(rng.random());

        let mut changes = vec![];
        for _ in 0..10 {
            for _ in 0..5 {
                let mut tmp = doc2.fork().with_actor(rng.random());
                let num_splices = rng.random::<u32>() % 10 + 1;
                for _ in 0..num_splices {
                    let len = tmp.length(&text1) as u32;
                    let index = rng.random::<u32>() % len;
                    let del = std::cmp::min(rng.random::<u32>() % 2, len - index);
                    tmp.splice_text(
                        &text1,
                        index as usize,
                        del as isize,
                        &format!("[{}]", val()),
                    )
                    .unwrap();
                }
                let change = tmp.get_last_local_change().unwrap();
                changes.push(change);
            }
            merge_and_diff(&mut doc2, &mut doc2_copy, &changes);
        }
        merge_and_diff(&mut doc1, &mut doc1_copy, &changes);
    }

    #[test]
    fn batch_marks_patch() {
        let mut rng = make_rng();
        let mut value = 0;
        let mut val = move || {
            value += 1;
            value
        };
        let mut doc1 = AutoCommit::new().with_actor(rng.random());
        let text1 = doc1.put_object(&ROOT, "text1", ObjType::Text).unwrap();
        doc1.splice_text(&text1, 0, 0, "---------------------")
            .unwrap();

        let mut doc1_copy = doc1.fork().with_actor(rng.random());
        let mut doc2 = doc1.fork().with_actor(rng.random());
        let mut doc2_copy = doc1.fork().with_actor(rng.random());

        let mut changes = vec![];
        for _ in 0..5 {
            for _ in 0..10 {
                let mut tmp = doc2.fork().with_actor(rng.random());
                let num_splices = rng.random::<u32>() % 10 + 1;
                for _ in 0..num_splices {
                    let len = tmp.length(&text1) as u32;
                    let index = rng.random::<u32>() % len;
                    let del = std::cmp::min(rng.random::<u32>() % 2, len - index);
                    tmp.splice_text(
                        &text1,
                        index as usize,
                        del as isize,
                        &format!("[{}]", val()),
                    )
                    .unwrap();
                }
                let num_marks = rng.random::<u32>() % 3 + 1;
                for _ in 0..num_marks {
                    let len = tmp.length(&text1) as u32;
                    let a = rng.random::<u32>() % len;
                    let b = rng.random::<u32>() % len;
                    if a == b {
                        continue;
                    }
                    let start = std::cmp::min(a, b);
                    let end = std::cmp::max(a, b);
                    let name = "bold".into();
                    let value = ScalarValue::from(val());
                    let mark = Mark {
                        start: start as usize,
                        end: end as usize,
                        name,
                        value,
                    };
                    tmp.mark(&text1, mark, ExpandMark::After).unwrap();
                }
                let change = tmp.get_last_local_change().unwrap();
                changes.push(change);
            }
            merge_and_diff(&mut doc2, &mut doc2_copy, &changes);
        }
        merge_and_diff(&mut doc1, &mut doc1_copy, &changes);
    }

    fn merge_and_diff(a: &mut AutoCommit, a_copy: &mut AutoCommit, changes: &[Change]) {
        let heads = a.get_heads();

        a.update_diff_cursor();
        a.apply_changes_batch(changes.to_owned()).unwrap();
        a.validate_top_index();
        let pa = a.diff_incremental();
        let final_heads = a.get_heads();

        a_copy.apply_changes_iter(changes.to_owned()).unwrap();
        let pb = a_copy.diff(&heads, &final_heads);

        let len = std::cmp::max(pa.len(), pb.len());

        assert_eq!(a.get_heads(), a_copy.get_heads());

        for i in 0..len {
            if pa.get(i) != pb.get(i) {
                log!(" i={} ", i);
                log!(" pa={:?}", pa.get(i));
                log!(" pb={:?}", pb.get(i));
            }
        }

        if pa != pb {
            panic!()
        }
    }

    #[test]
    fn map_key_conflict() {
        let mut rng = make_rng();
        let mut doc = AutoCommit::new().with_actor(rng.random());

        doc.put(&ROOT, "key1", "value1").unwrap();

        const CYCLES: u32 = 10;
        const DOCS: u32 = 5;
        const KEYS: u32 = 4;

        let mut docs = vec![];

        for _ in 0..DOCS {
            docs.push(doc.fork().with_actor(rng.random()));
        }

        for _ in 0..CYCLES {
            for d in &mut docs {
                for _ in 0..10 {
                    let k = rng.random::<u32>() % KEYS;
                    let val = rng.random::<u32>();
                    d.put(&ROOT, format!("key{}", k), format!("value{}", val))
                        .unwrap();
                }
                let k = rng.random::<u32>() % KEYS;
                let _ = d.delete(&ROOT, format!("key{}", k));
            }

            let changes: Vec<_> = docs
                .iter_mut()
                .map(|d| d.get_last_local_change().unwrap())
                .collect();

            doc.apply_changes(changes).unwrap();

            doc.validate_top_index();
        }
    }

    #[test]
    fn list_element_conflict() {
        let mut rng = make_rng();
        let mut doc = AutoCommit::new().with_actor(rng.random());

        let list = doc.put_object(&ROOT, "list", ObjType::List).unwrap();

        const CYCLES: u32 = 5;
        const DOCS: u32 = 6;
        const KEYS: u32 = 3;

        for i in 0..KEYS {
            doc.insert(&list, i as usize, "_").unwrap();
        }

        let mut docs = vec![];

        for _ in 0..DOCS {
            docs.push(doc.fork().with_actor(rng.random()));
        }

        for _ in 0..CYCLES {
            for d in &mut docs {
                for _ in 0..3 {
                    let k = rng.random::<u32>() % KEYS;
                    let val = rng.random::<u32>();
                    d.put(&list, k as usize, format!("value{}", val)).unwrap();
                }
            }

            let changes: Vec<_> = docs
                .iter_mut()
                .map(|d| d.get_last_local_change().unwrap())
                .collect();

            doc.apply_changes(changes).unwrap();
            doc.validate_top_index();
        }
    }

    #[test]
    fn conflicts_with_isolate() {
        let mut rng = make_rng();
        let mut doc = AutoCommit::new().with_actor(rng.random());

        let list = doc.put_object(&ROOT, "list", ObjType::List).unwrap();
        let map = doc.put_object(&ROOT, "map", ObjType::Map).unwrap();
        doc.insert(&list, 0, "_").unwrap();
        doc.put(&map, "key", "_").unwrap();

        const CYCLES: u32 = 5;
        const DOCS: u32 = 6;

        let mut docs = vec![];
        let mut heads = vec![doc.get_heads()];

        for _ in 0..DOCS {
            docs.push(doc.fork().with_actor(rng.random()));
        }

        for _ in 0..CYCLES {
            for d in &mut docs {
                let head = rng.random::<u32>() % (heads.len() as u32);
                d.isolate(&heads[head as usize]);
                for _ in 0..3 {
                    let del = rng.random::<u32>() % 5;
                    let val = rng.random::<u32>();
                    let len = d.length(&list);
                    let val = format!("value{}", val);
                    if del == 0 {
                        if len > 0 {
                            d.delete(&list, 0).unwrap();
                        }
                        d.delete(&map, "key").unwrap();
                    } else {
                        if len > 0 {
                            d.put(&list, 0, &val).unwrap();
                        } else {
                            d.insert(&list, 0, &val).unwrap();
                        }
                        d.put(&map, "key", &val).unwrap();
                    }
                }
                d.integrate();
                d.validate_top_index();
            }

            let changes: Vec<_> = docs
                .iter_mut()
                .map(|d| d.get_last_local_change().unwrap())
                .collect();

            doc.apply_changes(changes).unwrap();

            heads.push(doc.get_heads());

            doc.validate_top_index();
        }
    }
}
