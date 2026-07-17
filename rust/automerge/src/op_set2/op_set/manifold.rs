//! Seek-mode batch apply: resolve insert positions and successor
//! updates for a document-ordered stream of change ops without
//! walking the whole document.
//!
//! [`ApplyManifold`] holds every piece of streaming state — one
//! forward-only iterator per column plus the object/key/element
//! scoping — so a batch can be fed op by op ([`apply_op`]) and
//! resolved at the end ([`finish`]).
//!
//! [`apply_op`]: ApplyManifold::apply_op
//! [`finish`]: ApplyManifold::finish

use crate::op_set2::change::batch::Adjust;
use crate::op_set2::op::ChangeOp;
use crate::op_set2::types::ScalarValue as OpScalarValue;
use crate::op_set2::SuccInsert;
use crate::types::{Clock, ElemId, ObjId, ObjType, OpId};

use super::index::ObjIndex;
use super::op_iter::{ObjIdIter, OpIdIter, SuccIterIter};
use super::{OpSet, ValueIter};

use hexane::Shiftable;

use std::borrow::Cow;
use std::collections::HashMap;
use std::ops::Range;

/// What a batch resolves to.
pub(crate) struct ManifoldResult {
    /// succ entries to write into existing document rows
    pub(crate) doc_succ: Vec<SuccInsert>,
    /// succ for targets pending in the batch itself, deferred to the
    /// consumer's change ops (unnormalized)
    pub(crate) change_succ: HashMap<OpId, Vec<(OpId, Option<i64>)>>,
    /// one insert position per non-delete op, in arrival order
    pub(crate) insert_pos: Vec<usize>,
    /// top/text index adjustments for existing rows — apply BEFORE
    /// add_succ/splice, exactly like v1's conflict/expose pass
    pub(crate) adjusts: Vec<Adjust>,
    /// batch ops that lost their group to a sibling: the consumer sets
    /// `ChangeOp::conflicted` on these before splicing, which is what
    /// `OpLike::top` reads to write the new row's top bit
    pub(crate) conflicted: Vec<OpId>,
}

impl OpSet {
    /// Create an [`ApplyManifold`] over this op set.
    ///
    /// `clock` is the document clock *before* the batch: it decides
    /// which predecessors live in the document (succ written in place)
    /// versus in the batch itself (deferred to the change ops).
    pub(crate) fn apply_manifold(&self, clock: Clock) -> ApplyManifold<'_> {
        ApplyManifold::new(self, clock)
    }
}

/// All the state of a streaming batch apply.
///
/// CONTRACT: ops arrive in final *document* order (the order they
/// will occupy in the op set), with actor indexes already mapped to
/// this doc. Everything below leans on that: slots are monotone,
/// iterators only move forward, pending parents precede their
/// descendants, and same-anchor siblings arrive by descending id.
/// NOTE: `order_ops_for_doc` alone does NOT produce this for sequence
/// inserts (it sorts them by their own elemid) — the caller must
/// provide untangled order.
pub(crate) struct ApplyManifold<'a> {
    op_set: &'a OpSet,
    clock: Clock,
    obj_info: ObjIndex,

    obj_id_iter: ObjIdIter<'a>,
    id_iter: OpIdIter<'a>,
    key_iter: hexane::Iter<'a, Option<String>>,
    pred_iter: OpIdIter<'a>,
    succ_iter: SuccIterIter<'a>,
    value_iter: ValueIter<'a>,
    // two insert-column cursors, both forward-only: `ins` hops the
    // insert arm past update rows, `ins_upd` sizes update groups. They
    // cannot share one iterator — a group-end scan consumes the very
    // row the next child insert's hop must land on (and vice versa)
    ins: hexane::Iter<'a, bool>,
    ins_upd: hexane::Iter<'a, bool>,

    obj: ObjId,
    obj_type: ObjType,
    obj_scope: Range<usize>,
    key: Option<String>,

    insert_pos: Vec<usize>,
    succ_cache: SuccCache,
    // the last row a slot scan stopped on: (row id, slot). Reusable by
    // any op that beats the row — except one anchored at the row
    // itself, which belongs after it (document order guarantees no
    // other anchor can sit between a memo slot and its row)
    lesser: Option<(OpId, usize)>,
    // the current update scope: an element and its group in the doc
    elem: Option<ElemId>,
    last_insert: Option<OpId>,
    elem_scope: Range<usize>,

    // top/text delta tracking: one scope at a time, finalized at the
    // same boundaries the succ cache flushes
    top: TopCalc,
    // lags the stream; shifted to each finalized scope (scopes ascend
    // globally, so strictly forward)
    vis_iter: hexane::Iter<'a, bool>,
}

impl<'a> ApplyManifold<'a> {
    pub(crate) fn new(op_set: &'a OpSet, clock: Clock) -> Self {
        let obj_info = op_set.obj_info.clone();

        let mut obj_id_iter = op_set.obj_id_iter();
        let mut id_iter = op_set.id_iter();
        let mut key_iter = op_set.key_str_iter();
        let mut pred_iter = op_set.id_iter();
        let succ_iter = op_set.succ_iter();
        let value_iter = op_set.value_iter();
        let ins = op_set.insert().values().iter();
        let ins_upd = op_set.insert().values().iter();
        let vis_iter = op_set.cols.index.visible.iter();

        let obj = ObjId::root();
        let obj_scope = obj_id_iter.seek_to_value(obj);

        key_iter.shift(obj_scope.clone());
        id_iter.shift(obj_scope.clone());
        pred_iter.shift(obj_scope.clone());

        let succ_cache = SuccCache::new(clock.clone());

        ApplyManifold {
            op_set,
            clock,
            obj_info,
            obj_id_iter,
            id_iter,
            key_iter,
            pred_iter,
            succ_iter,
            value_iter,
            ins,
            ins_upd,
            obj,
            obj_type: ObjType::Map,
            obj_scope,
            key: None,
            insert_pos: vec![],
            succ_cache,
            lesser: None,
            elem: None,
            last_insert: None,
            elem_scope: 0..0,
            top: TopCalc::default(),
            vis_iter,
        }
    }

    /// Feed the manifold all of `ops` and resolve.
    pub(crate) fn apply<I: Iterator<Item = ChangeOp>>(mut self, ops: I) -> ManifoldResult {
        for op in ops {
            self.apply_op(op);
        }
        self.finish()
    }

    /// Close the scope that is ending: resolve its cached preds while
    /// the iterators still cover it, then decide its top winner and
    /// emit any adjustments. The two always travel together — the
    /// flush notes which rows the batch deletes, and the winner cannot
    /// be decided before that
    fn flush(&mut self) {
        self.succ_cache.flush(
            &mut self.pred_iter,
            &mut self.succ_iter,
            &mut self.value_iter,
            &mut self.top,
        );
        self.top.finalize(&mut self.vis_iter);
    }

    /// Process one change op. Ops must arrive in document order (see
    /// the type-level contract).
    pub(crate) fn apply_op(&mut self, op: ChangeOp) {
        if let Some(info) = op.obj_info() {
            self.obj_info.insert(op.id(), info);
        }
        if op.bld.obj != self.obj {
            self.flush();

            self.obj = op.bld.obj;
            self.obj_scope = self.obj_id_iter.seek_to_value(self.obj);
            self.obj_type = self.obj_info.object_type(&self.obj).unwrap();

            self.id_iter.shift(self.obj_scope.clone());
            self.pred_iter.shift(self.obj_scope.clone());
            self.elem = None;
            self.lesser = None;

            if self.obj_type == ObjType::Map {
                self.key_iter.shift(self.obj_scope.clone());
                self.key = None;
            }
        }
        if self.obj_type == ObjType::Map {
            if self.key.as_deref() != op.bld.key.key_str().as_deref() {
                self.key = op.bld.key.key_str().map(Cow::into_owned);
                let key_scope = self.key_iter.seek_to_value(self.key.as_deref(), ..);

                self.flush();
                self.top.open(key_scope.clone());

                self.id_iter.shift(key_scope.clone());
                self.pred_iter.shift(key_scope.clone());
                self.succ_iter.shift(key_scope);
            }
            let inc = op.get_increment_value();
            for pred_id in op.pred() {
                self.succ_cache.push(*pred_id, op.id(), inc);
                if !self.clock.covers(pred_id) {
                    self.top.kill(pred_id, inc);
                }
            }
            if !op.bld.is_delete() {
                let r = self.id_iter.seek_to_value(&op.id());
                assert!(r.is_empty());
                self.insert_pos.push(r.start);
                self.top.candidate(&op, r.start);
            }
        } else if op.insert() {
            let e = op.key().elemid().unwrap();
            self.last_insert = Some(op.id());

            // every insert starts a new element group: close the
            // previous scope (its updates all precede this op in
            // document order) and open one with no doc rows
            self.flush();
            // slot is irrelevant: an insert's scope never has doc rows
            self.top.candidate(&op, 0);
            self.elem = None;

            if !e.is_head() && self.clock.covers(&e.0) {
                // an update group may have narrowed the window; the
                // anchor can live anywhere ahead in the object
                self.id_iter.set_max(self.obj_scope.end);
                let start = self.id_iter.pos();
                // optimisticly scan forward - most of the time itll be ahead of us
                if self.id_iter.scan_to_value(&e.0).is_some() {
                    // the anchor sits past the memo slot, so the memo
                    // cannot apply any more
                    self.lesser = None;
                } else {
                    // whoops - anchor was behind us
                    self.id_iter = self.op_set.id_iter_range(&(start..self.obj_scope.end));
                }
            }
            if matches!(self.lesser, Some((row_id, _)) if row_id < op.id() && row_id != e.0) {
                // memo hit: same slot, and crucially no iterator moves —
                // a hop here would drag id_iter/ins past rows the next
                // update group or sibling still needs
                let (_, found) = self.lesser.unwrap();
                self.insert_pos.push(found);
            } else {
                // an insert only ever lands immediately before another
                // insert row (or the object end): hop past any update
                // rows before the slot scan. Bounded to the object: an
                // unbounded miss parks `ins` at the column end and a hit
                // can land in (and consume) the next object's first
                // insert row
                let cur = self.id_iter.pos();
                self.ins.shift(cur..self.obj_scope.end);
                let epos = self.ins.scan_to_value(true).unwrap_or(self.obj_scope.end);
                self.id_iter.advance_to(epos);
                if let Some((found, row_id)) = self.id_iter.scan_to_lesser(op.id()) {
                    self.insert_pos.push(found);
                    self.lesser = Some((row_id, found));
                } else {
                    self.insert_pos.push(self.obj_scope.end);
                    self.lesser = None;
                }
            }
        } else {
            // non-insert seq ops (set/delete/increment): the group
            // analog of the map path — scope to the element's run of
            // updates, sized via the insert column
            let e = op.key().elemid().unwrap();
            if self.elem != Some(e) {
                if self.last_insert == Some(e.0) {
                    // a pending element's updates belong to the scope
                    // its insert opened — and that insert just
                    // flushed, so there is nothing cached to resolve
                    debug_assert!(self.succ_cache.preds.is_empty());
                } else {
                    self.flush();
                }

                self.elem = Some(e);

                if self.last_insert == Some(e.0) {
                    // a pending element: its updates ride at the slot
                    // its insert was just given (id_iter.pos() would be
                    // slot + 1 here — scan_to_lesser consumes the hit).
                    // No iterator needs to move: an empty scope has
                    // nothing to seek and no covered preds. The memo is
                    // deliberately left alone — this element's children
                    // will collapse onto the same slot through it
                    let p = *self.insert_pos.last().unwrap();
                    self.elem_scope = p..p;
                } else {
                    // the element row may already be consumed: when an
                    // insert's slot scan stopped exactly on it, the memo
                    // holds it (id_iter stands one past). Otherwise it
                    // is ahead — the previous group narrowed the window,
                    // so re-widen and scan
                    let epos = match self.lesser {
                        Some((row_id, s)) if row_id == e.0 => s,
                        _ => {
                            self.id_iter.set_max(self.obj_scope.end);
                            self.id_iter.scan_to_value(&e.0).unwrap()
                        }
                    };
                    // this group's element row is an insert row between
                    // any older memo slot and the cursor: the memo's
                    // reuse invariant is gone
                    self.lesser = None;
                    // group-end scan on its own cursor (`ins_upd`),
                    // bounded to the object so a miss parks at the
                    // object end, not the column end
                    self.ins_upd.shift((epos + 1)..self.obj_scope.end);
                    let group_end = self
                        .ins_upd
                        .scan_to_value(true)
                        .unwrap_or(self.obj_scope.end);
                    self.elem_scope = epos..group_end;

                    self.top.open(self.elem_scope.clone());

                    self.id_iter.shift(self.elem_scope.clone());
                    self.pred_iter.shift(self.elem_scope.clone());
                    self.succ_iter.shift(self.elem_scope.clone());
                }
            }
            let inc = op.get_increment_value();
            for pred_id in op.pred() {
                self.succ_cache.push(*pred_id, op.id(), inc);
                if !self.clock.covers(pred_id) {
                    self.top.kill(pred_id, inc);
                }
            }
            if !op.bld.is_delete() {
                if self.elem_scope.is_empty() {
                    // pending target: rides along at its insert's slot
                    self.insert_pos.push(self.elem_scope.start);
                    self.top.candidate(&op, self.elem_scope.start);
                } else {
                    let r = self.id_iter.seek_to_value(&op.id());
                    assert!(r.is_empty());
                    self.insert_pos.push(r.start);
                    self.top.candidate(&op, r.start);
                }
            }
        }
    }

    /// Flush the trailing scope and hand back the batch resolution:
    /// document succ inserts, deferred change-op succ, and one insert
    /// position per non-delete op (in arrival order).
    pub(crate) fn finish(mut self) -> ManifoldResult {
        self.flush();

        ManifoldResult {
            doc_succ: self.succ_cache.doc_succ,
            change_succ: self.succ_cache.change_succ,
            insert_pos: self.insert_pos,
            adjusts: self.top.adjusts,
            conflicted: self.top.conflicted,
        }
    }
}

/// Per-scope top/text delta tracking.
///
/// The load-bearing invariant: within a key/element group the doc's
/// `top` bit is true exactly on the max-id **visible** row. So the
/// current top (`vmax`), the post-batch doc candidate (`dmax` — max-id
/// visible row not deleted this batch) and the batch candidate
/// (`cmax`) decide everything. Neither the `top` column nor any id is
/// ever read: candidate slots are id-ordered insertion points, so
/// document order alone ranks batch ops against doc rows:
///
/// * batch wins, `vmax` undeleted → `Conflict(vmax)` (old top demoted,
///   stays visible)
/// * batch wins, `vmax` deleted → nothing (`add_succ` clears its bits;
///   any `dmax` was already shadowed)
/// * doc wins and `vmax` was deleted → `Expose(dmax)` (shadowed row
///   promoted; its text width is recomputed by `expose()`)
/// * every other visible batch op in the scope → `conflicted`
///
/// Text needs no separate pass: `conflict()`/`expose()` maintain the
/// text index alongside the top bit, and `Columns::splice` derives new
/// rows' entries from `OpLike::top`.
#[derive(Default)]
struct TopCalc {
    // current scope: doc row range (empty for insert/pending scopes)
    doc_scope: Range<usize>,
    // visible batch ops in arrival (= ascending id) order
    batch: Vec<BatchTop>,
    // doc positions given an inc=None succ this scope (from flush,
    // post-normalization, in ascending position order)
    deleted: Vec<usize>,

    adjusts: Vec<Adjust>,
    conflicted: Vec<OpId>,
}

struct BatchTop {
    id: OpId,
    // the op's insertion point within the scope. Rows in a group are
    // id-sorted and the slot came from an id-ordered seek, so slots
    // compare against doc row positions exactly as ids compare —
    // `slot > pos` ⇔ this op's id beats the row's id
    slot: usize,
    is_counter: bool,
    alive: bool,
}

impl TopCalc {
    fn open(&mut self, doc_scope: Range<usize>) {
        debug_assert!(self.batch.is_empty() && self.deleted.is_empty());
        self.doc_scope = doc_scope;
    }

    /// Kill the in-scope batch target `pred` overwrites, if any.
    /// Callers pre-filter with `clock.covers`: a covered pred is a doc
    /// row and can never be in the batch list, so the common case
    /// never walks it.
    fn kill(&mut self, pred: &OpId, inc: Option<i64>) {
        if let Some(b) = self.batch.iter_mut().find(|b| b.id == *pred) {
            // same normalization flush applies to doc targets: an
            // increment only preserves a counter
            if inc.is_none() || !b.is_counter {
                b.alive = false;
            }
        }
    }

    /// Register a batch op as a top candidate if it is the kind of op
    /// that can hold top (matches v1's `Top` visibility test). `slot`
    /// is its already-computed insertion point within the scope.
    fn candidate(&mut self, op: &ChangeOp, slot: usize) {
        if !op.bld.is_inc() {
            self.batch.push(BatchTop {
                id: op.id(),
                slot,
                is_counter: matches!(op.bld.value, OpScalarValue::Counter(_)),
                alive: true,
            });
        }
    }

    /// Called by the succ flush for every covered pred resolved with
    /// `inc = None` — the rows `add_succ` will clear.
    fn note_deleted(&mut self, pos: usize) {
        self.deleted.push(pos);
    }

    /// Close the scope: decide the group winner and emit adjustments.
    fn finalize(&mut self, vis_iter: &mut hexane::Iter<'_, bool>) {
        let cmax = self.batch.iter().rev().find(|b| b.alive);
        if cmax.is_none() && self.deleted.is_empty() {
            // increments-only, deletes-of-nothing, or empty — the hot
            // insert path exits here with zero column reads
            self.batch.clear();
            self.doc_scope = 0..0;
            return;
        }

        // scan the scope's visible runs once; the top column is never
        // read (top == max-id visible row by invariant)
        let mut vmax: Option<usize> = None;
        let mut dmax: Option<usize> = None;
        if !self.doc_scope.is_empty() {
            vis_iter.shift(self.doc_scope.clone());
            let mut runs: Vec<Range<usize>> = vec![];
            let mut pos = self.doc_scope.start;
            while let Some(run) = vis_iter.next_run() {
                if run.value {
                    runs.push(pos..pos + run.count);
                }
                pos += run.count;
            }
            vmax = runs.last().map(|r| r.end - 1);
            'outer: for r in runs.iter().rev() {
                for p in r.clone().rev() {
                    // deleted is tiny: this loop exits within
                    // deleted.len() + 1 steps
                    if !self.deleted.contains(&p) {
                        dmax = Some(p);
                        break 'outer;
                    }
                }
            }
        }

        // no id reads at all: candidate slots are id-ordered insertion
        // points within the scope, so document order decides —
        // `slot > pos` ⇔ the batch op's id beats the row's id
        let batch_wins = match (cmax, dmax) {
            (Some(c), Some(d)) => c.slot > d,
            (Some(_), None) => true,
            (None, _) => false,
        };

        if batch_wins {
            if let Some(v) = vmax {
                if dmax == Some(v) {
                    // the standing top stays visible but is demoted
                    self.adjusts.push(Adjust::Conflict(v));
                }
            }
            let winner = cmax.map(|b| b.id);
            for b in self.batch.iter().filter(|b| b.alive) {
                if Some(b.id) != winner {
                    self.conflicted.push(b.id);
                }
            }
        } else {
            if let (Some(v), Some(d)) = (vmax, dmax) {
                if v != d {
                    // the old top was deleted: promote the shadowed row
                    self.adjusts.push(Adjust::Expose(d));
                }
            }
            for b in self.batch.iter().filter(|b| b.alive) {
                self.conflicted.push(b.id);
            }
        }

        self.batch.clear();
        self.deleted.clear();
        self.doc_scope = 0..0;
    }
}

struct SuccCache {
    clock: Clock,
    preds: Vec<(OpId, OpId, Option<i64>)>,
    change_succ: HashMap<OpId, Vec<(OpId, Option<i64>)>>,
    doc_succ: Vec<SuccInsert>,
}

impl SuccCache {
    fn new(clock: Clock) -> Self {
        SuccCache {
            clock,
            preds: vec![],
            doc_succ: vec![],
            change_succ: HashMap::default(),
        }
    }

    fn push(&mut self, pred: OpId, succ: OpId, inc: Option<i64>) {
        self.preds.push((pred, succ, inc));
    }

    fn flush(
        &mut self,
        pred_iter: &mut OpIdIter<'_>,
        succ_iter: &mut SuccIterIter<'_>,
        value_iter: &mut ValueIter<'_>,
        top: &mut TopCalc,
    ) {
        // sorted preds arrive in position order, so the iterators only
        // move forward; shared targets are adjacent and add_succ_at
        // re-reads the row it is parked on, so each successor's
        // sub_pos comes from the same pre-insert cursors
        self.preds.sort_unstable();
        // shared targets are adjacent after the sort; the value read is
        // memoized so a second increment on the same pred does not
        // re-read (value_iter only moves forward)
        let mut counter_memo: Option<(OpId, bool)> = None;
        for (pred, succ, mut inc) in self.preds.drain(..) {
            if self.clock.covers(&pred) {
                let r = pred_iter.seek_to_value(&pred);
                assert!(r.len() == 1, "covered pred must be present");
                if inc.is_some() {
                    let is_counter = match counter_memo {
                        Some((p, ic)) if p == pred => ic,
                        _ => {
                            let ic = value_iter.shift_next(r.clone()).unwrap().is_counter();
                            counter_memo = Some((pred, ic));
                            ic
                        }
                    };
                    if !is_counter {
                        // increments overwrite non-counter targets
                        inc = None;
                    }
                }
                if inc.is_none() {
                    // this row's top/visible bits get cleared by
                    // add_succ — the scope's winner must be recomputed
                    top.note_deleted(r.start);
                }
                let s = succ_iter.add_succ_at(r.start, succ, inc).unwrap();
                self.doc_succ.push(s);
            } else {
                self.change_succ.entry(pred).or_default().push((succ, inc));
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::op_set2::change::batch::{
        normalize_increment_successors, walk_list, walk_map, BatchApply, MapWalker, ObjWalker,
        Untangler,
    };
    use crate::op_set2::types::ScalarValue as OpScalarValue;
    use crate::read::ReadDoc;
    use crate::transaction::Transactable;
    use crate::types::SequenceType;
    use crate::{make_rng, AutoCommit, Change, ObjType, PatchLog, ScalarValue, ROOT};
    use rand::prelude::*;

    /// Run the manifold and the v1 walk over the same batch against the
    /// same document (neither mutates it) and compare insert
    /// positions, doc succ and change-op succ.
    fn assert_manifold_matches_v1(doc: &mut AutoCommit, changes: Vec<Change>) {
        let doc = &mut doc.doc;

        let mut chap = BatchApply::default();
        for c in changes {
            chap.push(c);
        }
        chap.insert_new_actors(doc);
        for c in &chap.changes {
            doc.import_ops_to(c, &mut chap.ops).unwrap();
        }

        let mut obj_info = doc.ops().obj_info.clone();
        chap.order_ops_for_doc(&mut obj_info);

        // v1 reference first: the walkers assign pos/subsort and
        // re-sort each span into final *document* order — exactly the
        // order the manifold's contract requires as input
        let mut log = PatchLog::inactive();
        let mut succ = vec![];
        let mut conflicts = vec![];
        let mut walker = ObjWalker::new(doc.ops());
        for os in &chap.obj_spans {
            let obj_range = walker.seek_to_obj(os.obj);
            let doc_ops = doc.ops().iter_range(&obj_range);
            match obj_info.object_type(&os.obj) {
                Some(ObjType::Map) => {
                    let mut mw = MapWalker::new(
                        os.obj,
                        doc_ops,
                        doc.text_encoding(),
                        &mut chap.pred,
                        &mut succ,
                        &mut log,
                        &mut conflicts,
                    );
                    walk_map(&mut mw, &mut chap.ops[os.span.clone()]);
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
                        &mut chap.ops[os.span.clone()],
                        &mut chap.pred,
                        doc_ops.end_pos(),
                    );
                    walk_list(ut, doc_ops, &mut succ, &mut log);
                }
                _ => panic!("obj missing from index"),
            }
        }

        // manifold: read-only pass over the untangled (document-ordered) ops
        let ordered: Vec<ChangeOp> = chap.ops.clone();
        let r = doc
            .ops()
            .apply_manifold(doc.change_graph.current_clock())
            .apply(ordered.iter().cloned());
        let (v2_succ, v2_change_succ, v2_pos) = (r.doc_succ, r.change_succ, r.insert_pos);

        // insert positions, paired by op id
        let v1_pos: HashMap<OpId, usize> =
            chap.ops.iter().map(|o| (o.id(), o.pos.unwrap())).collect();
        if std::env::var("MANIFOLD_DEBUG").is_ok() {
            let mut k = 0;
            for o in &ordered {
                let qualifies = !o.bld.is_delete();
                if qualifies {
                    eprintln!(
                        "op {:?} key {:?} insert {} v1 {} v2 {}",
                        o.id(),
                        o.key(),
                        o.insert(),
                        v1_pos[&o.id()],
                        v2_pos[k]
                    );
                    k += 1;
                }
            }
        }
        let mut k = 0;
        for o in &ordered {
            let qualifies = !o.bld.is_delete();
            if qualifies {
                assert_eq!(v2_pos[k], v1_pos[&o.id()], "pos of {:?}", o.id());
                k += 1;
            }
        }
        assert_eq!(k, v2_pos.len(), "v2 produced extra positions");

        // doc succ: same set under the canonical order
        let key = |s: &SuccInsert| (s.pos, s.sub_pos, s.id);
        let mut a = v2_succ;
        a.sort_by_key(key);
        let mut b = succ;
        b.sort_by_key(key);
        assert_eq!(a, b, "doc succ");

        // change-op succ: v1 stores them on the target op, normalized;
        // v2 defers normalization of batch targets to the consumer
        for o in &chap.ops {
            let mut v1 = o.succ.clone();
            v1.sort_unstable();
            let mut v2 = v2_change_succ.get(&o.id()).cloned().unwrap_or_default();
            let is_counter = matches!(o.bld.value, OpScalarValue::Counter(_));
            normalize_increment_successors(is_counter, &mut v2);
            v2.sort_unstable();
            assert_eq!(v1, v2, "change succ of {:?}", o.id());
        }

        // top/text adjustments: compare by net effect. v1 can emit
        // redundant entries (Conflict on an already-shadowed row), so
        // replay each list into pos -> bit and drop no-ops against the
        // pre-batch top column
        let canon = |list: &[Adjust]| -> HashMap<usize, bool> {
            let mut m = HashMap::default();
            for a in list {
                match a {
                    Adjust::Conflict(p) => m.insert(*p, false),
                    Adjust::Expose(p) => m.insert(*p, true),
                };
            }
            m.retain(|p, v| doc.ops().cols.index.top.get(*p).map(|pv| pv.value) != Some(*v));
            m
        };
        if std::env::var("MANIFOLD_DEBUG").is_ok() {
            eprintln!(
                "adjusts: v1 {} v2 {} (canon {}) conflicted {}",
                conflicts.len(),
                r.adjusts.len(),
                canon(&r.adjusts).len(),
                r.conflicted.len()
            );
        }
        assert_eq!(canon(&conflicts), canon(&r.adjusts), "top adjustments");

        // conflicted flags: v1 marks the op in place, v2 returns ids
        for o in &chap.ops {
            assert_eq!(
                o.conflicted,
                r.conflicted.contains(&o.id()),
                "conflicted flag of {:?}",
                o.id()
            );
        }
    }

    fn changes_since(src: &mut AutoCommit, heads: &[crate::ChangeHash]) -> Vec<Change> {
        src.get_changes(heads).unwrap()
    }

    #[test]
    fn manifold_double_increment_same_pred() {
        let mut rng = make_rng();
        let mut doc = AutoCommit::new().with_actor(rng.random()).unwrap();
        doc.put(&ROOT, "c", ScalarValue::counter(10)).unwrap();
        let heads = doc.get_heads();

        let mut f1 = doc.fork().with_actor(rng.random()).unwrap();
        let mut f2 = doc.fork().with_actor(rng.random()).unwrap();
        f1.increment(&ROOT, "c", 5).unwrap();
        f2.increment(&ROOT, "c", 7).unwrap();
        let mut src = doc.fork().with_actor(rng.random()).unwrap();
        src.merge(&mut f1).unwrap();
        src.merge(&mut f2).unwrap();

        let changes = changes_since(&mut src, &heads);
        assert_manifold_matches_v1(&mut doc, changes);
    }

    #[test]
    fn manifold_inserts_around_updates() {
        // the doc holds update rows inside element groups; new sibling
        // inserts must not split them
        let mut rng = make_rng();
        let mut doc = AutoCommit::new().with_actor(rng.random()).unwrap();
        let list = doc.put_object(&ROOT, "list", ObjType::List).unwrap();
        doc.insert(&list, 0, "a").unwrap();
        doc.insert(&list, 1, "b").unwrap();
        doc.put(&list, 0, "a2").unwrap(); // update row in a's group
        doc.put(&list, 0, "a3").unwrap();
        let heads = doc.get_heads();

        let mut f1 = doc.fork().with_actor(rng.random()).unwrap();
        let mut f2 = doc.fork().with_actor(rng.random()).unwrap();
        f1.insert(&list, 1, "x").unwrap(); // after a, competing with b
        f2.insert(&list, 1, "y").unwrap();
        f2.insert(&list, 2, "z").unwrap(); // chained after y
        let mut src = doc.fork().with_actor(rng.random()).unwrap();
        src.merge(&mut f1).unwrap();
        src.merge(&mut f2).unwrap();

        let changes = changes_since(&mut src, &heads);
        assert_manifold_matches_v1(&mut doc, changes);
    }

    #[test]
    fn manifold_memo_resets_across_objects() {
        let mut rng = make_rng();
        let mut doc = AutoCommit::new().with_actor(rng.random()).unwrap();
        let l1 = doc.put_object(&ROOT, "l1", ObjType::List).unwrap();
        let l2 = doc.put_object(&ROOT, "l2", ObjType::List).unwrap();
        doc.insert(&l1, 0, 1).unwrap();
        doc.insert(&l2, 0, 2).unwrap();
        let heads = doc.get_heads();

        let mut f = doc.fork().with_actor(rng.random()).unwrap();
        f.insert(&l1, 0, 10).unwrap();
        f.insert(&l2, 0, 20).unwrap();
        let mut src = doc.fork().with_actor(rng.random()).unwrap();
        src.merge(&mut f).unwrap();

        let changes = changes_since(&mut src, &heads);
        assert_manifold_matches_v1(&mut doc, changes);
    }

    #[test]
    fn manifold_pending_chains() {
        // typing runs: every insert anchors at the previous pending one
        let mut rng = make_rng();
        let mut doc = AutoCommit::new().with_actor(rng.random()).unwrap();
        let text = doc.put_object(&ROOT, "text", ObjType::Text).unwrap();
        doc.splice_text(&text, 0, 0, "base").unwrap();
        let heads = doc.get_heads();

        let mut f1 = doc.fork().with_actor(rng.random()).unwrap();
        let mut f2 = doc.fork().with_actor(rng.random()).unwrap();
        f1.splice_text(&text, 2, 0, "hello world").unwrap();
        f2.splice_text(&text, 4, 0, "concurrent!").unwrap();
        let mut src = doc.fork().with_actor(rng.random()).unwrap();
        src.merge(&mut f1).unwrap();
        src.merge(&mut f2).unwrap();

        let changes = changes_since(&mut src, &heads);
        assert_manifold_matches_v1(&mut doc, changes);
    }

    #[test]
    fn manifold_fuzz() {
        let mut rng = make_rng();
        for _ in 0..20 {
            let mut doc = AutoCommit::new().with_actor(rng.random()).unwrap();
            let map = doc.put_object(&ROOT, "map", ObjType::Map).unwrap();
            let list = doc.put_object(&ROOT, "list", ObjType::List).unwrap();
            doc.put(&map, "c", ScalarValue::counter(0)).unwrap();
            for i in 0..5 {
                doc.insert(&list, i, i as i64).unwrap();
            }
            // update rows in the doc for the scans to step around
            doc.put(&list, 1, "u1").unwrap();
            doc.put(&list, 3, "u2").unwrap();
            let heads = doc.get_heads();

            let mut src = doc.fork().with_actor(rng.random()).unwrap();
            for _ in 0..6 {
                let mut f = doc.fork().with_actor(rng.random()).unwrap();
                for _ in 0..rng.random_range(1..8u32) {
                    match rng.random_range(0..7u32) {
                        0 => {
                            let len = f.length(&list);
                            let at = rng.random_range(0..=len as u32) as usize;
                            f.insert(&list, at, rng.random_range(0..100i64)).unwrap();
                        }
                        1 => {
                            let k = format!("k{}", rng.random_range(0..6u32));
                            f.put(&map, k, rng.random_range(0..100i64)).unwrap();
                        }
                        2 => {
                            let k = format!("k{}", rng.random_range(0..6u32));
                            let _ = f.delete(&map, k);
                        }
                        3 => {
                            f.increment(&map, "c", rng.random_range(1..10i64)).unwrap();
                        }
                        4 => {
                            let len = f.length(&list);
                            let at = rng.random_range(0..len as u32) as usize;
                            f.put(&list, at, rng.random_range(0..100i64)).unwrap();
                        }
                        5 => {
                            let len = f.length(&list);
                            if len > 1 {
                                let at = rng.random_range(0..len as u32) as usize;
                                f.delete(&list, at).unwrap();
                            }
                        }
                        _ => {
                            // a second commit in the fork chains ids
                            f.commit();
                            let len = f.length(&list);
                            let at = rng.random_range(0..=len as u32) as usize;
                            f.insert(&list, at, rng.random_range(0..100i64)).unwrap();
                        }
                    }
                }
                src.merge(&mut f).unwrap();
            }

            let changes = changes_since(&mut src, &heads);
            assert_manifold_matches_v1(&mut doc, changes);
        }
    }

    #[test]
    fn manifold_two_lists() {
        // `ins`/`ins_upd` must stay bounded per object: an unbounded
        // miss in the first list parks them at the column end and every
        // later object then sizes groups/hops against nothing
        let mut rng = make_rng();
        let mut doc = AutoCommit::new().with_actor(rng.random()).unwrap();
        let l1 = doc.put_object(&ROOT, "a", ObjType::List).unwrap();
        let l2 = doc.put_object(&ROOT, "b", ObjType::List).unwrap();
        for i in 0..4 {
            doc.insert(&l1, i, i as i64).unwrap();
            doc.insert(&l2, i, (i + 10) as i64).unwrap();
        }
        let heads = doc.get_heads();

        let mut f = doc.fork().with_actor(rng.random()).unwrap();
        // update the LAST element of l1: its group-end scan misses
        // inside l1 (no later insert row in the object)
        f.put(&l1, 3, "tail").unwrap();
        // then work in l2: inserts and updates that need working scans
        f.insert(&l2, 2, 99i64).unwrap();
        f.put(&l2, 1, "mid").unwrap();
        f.insert(&l2, 4, 98i64).unwrap();

        let changes = changes_since(&mut f, &heads);
        assert_manifold_matches_v1(&mut doc, changes);
    }

    #[test]
    fn manifold_update_after_slot_scan_consumed_elem() {
        // an insert whose slot scan stops exactly on the next element's
        // row consumes it; that element's updates arrive right after
        // (doc order) and must resolve it through the memo
        let mut rng = make_rng();
        let mut doc = AutoCommit::new().with_actor(rng.random()).unwrap();
        let list = doc.put_object(&ROOT, "list", ObjType::List).unwrap();
        for i in 0..5 {
            doc.insert(&list, i, i as i64).unwrap();
        }
        let heads = doc.get_heads();

        let mut f1 = doc.fork().with_actor(rng.random()).unwrap();
        // insert after elem 1: slot scan stops on elem 2's row
        f1.insert(&list, 2, 50i64).unwrap();
        let mut f2 = doc.fork().with_actor(rng.random()).unwrap();
        // update elem 2: arrives after the insert in document order
        f2.put(&list, 2, "upd").unwrap();
        let mut src = doc.fork().with_actor(rng.random()).unwrap();
        src.merge(&mut f1).unwrap();
        src.merge(&mut f2).unwrap();

        let changes = changes_since(&mut src, &heads);
        assert_manifold_matches_v1(&mut doc, changes);
    }

    #[test]
    fn manifold_conflict_on_stale_batch_put() {
        // the doc evolves past the fork point: the batch's put wins by
        // id but the doc's own concurrent put stays visible -> the
        // standing top is demoted with Adjust::Conflict
        let mut doc = AutoCommit::new()
            .with_actor("aa".try_into().unwrap())
            .unwrap();
        doc.put(&ROOT, "k", "old").unwrap();
        doc.commit();
        let mut f = doc.fork().with_actor("bb".try_into().unwrap()).unwrap();
        let heads = doc.get_heads();
        f.put(&ROOT, "k", "fork1").unwrap();
        f.commit();
        f.put(&ROOT, "k", "fork2").unwrap(); // ctr 4: beats the local put
        doc.put(&ROOT, "k", "local").unwrap(); // ctr 3, concurrent, stays visible
        let changes = changes_since(&mut f, &heads);
        assert_manifold_matches_v1(&mut doc, changes);
    }

    #[test]
    fn manifold_batch_put_loses_to_local() {
        // mirror image: the local put has the higher ctr, the batch op
        // becomes the conflicted loser, no doc adjustment
        let mut doc = AutoCommit::new()
            .with_actor("aa".try_into().unwrap())
            .unwrap();
        doc.put(&ROOT, "k", "old").unwrap();
        doc.commit();
        let mut f = doc.fork().with_actor("bb".try_into().unwrap()).unwrap();
        let heads = doc.get_heads();
        f.put(&ROOT, "k", "fork").unwrap(); // ctr 3
        doc.put(&ROOT, "k", "l1").unwrap();
        doc.commit();
        doc.put(&ROOT, "k", "l2").unwrap(); // ctr 4: local wins
        let changes = changes_since(&mut f, &heads);
        assert_manifold_matches_v1(&mut doc, changes);
    }

    #[test]
    fn manifold_expose_on_delete_of_winner() {
        // the doc holds a concurrent visible pair; the batch deletes
        // only one side. When it deletes the winner, the shadowed row
        // must be re-promoted (Adjust::Expose). Both actor orders run
        // so the winner is on each side once
        for other in ["0b", "fb"] {
            let mut doc = AutoCommit::new()
                .with_actor("aa".try_into().unwrap())
                .unwrap();
            doc.put(&ROOT, "k", "old").unwrap();
            doc.commit();
            let mut fa = doc.fork().with_actor(other.try_into().unwrap()).unwrap();
            fa.put(&ROOT, "k", "a").unwrap();
            fa.commit();
            doc.put(&ROOT, "k", "b").unwrap();
            doc.commit();
            let pre = fa.get_heads();
            doc.merge(&mut fa).unwrap(); // doc: "a" and "b" both visible
            fa.delete(&ROOT, "k").unwrap(); // fa only sees "a": deletes one side
            let changes = changes_since(&mut fa, &pre);
            assert_manifold_matches_v1(&mut doc, changes);
        }
    }

    #[test]
    fn manifold_conflicted_among_pending_updates() {
        // two actors concurrently update an element that is itself
        // pending in the batch: one update wins, the other is flagged
        let mut rng = make_rng();
        let mut doc = AutoCommit::new().with_actor(rng.random()).unwrap();
        let list = doc.put_object(&ROOT, "list", ObjType::List).unwrap();
        doc.insert(&list, 0, 1i64).unwrap();
        let heads = doc.get_heads();

        let mut f1 = doc.fork().with_actor(rng.random()).unwrap();
        f1.insert(&list, 1, 100i64).unwrap();
        f1.commit();
        let mut f2 = f1.fork().with_actor(rng.random()).unwrap();
        f2.put(&list, 1, "x").unwrap();
        let mut f3 = f1.fork().with_actor(rng.random()).unwrap();
        f3.put(&list, 1, "y").unwrap();
        f1.merge(&mut f2).unwrap();
        f1.merge(&mut f3).unwrap();
        let changes = changes_since(&mut f1, &heads);
        assert_manifold_matches_v1(&mut doc, changes);
    }

    #[test]
    fn manifold_increment_only_scope_no_adjusts() {
        // increments keep the counter visible and on top: the scope
        // takes the early-out and emits nothing
        let mut doc = AutoCommit::new()
            .with_actor("aa".try_into().unwrap())
            .unwrap();
        doc.put(&ROOT, "c", ScalarValue::counter(5)).unwrap();
        let heads = doc.get_heads();
        let mut f = doc.fork().with_actor("bb".try_into().unwrap()).unwrap();
        f.increment(&ROOT, "c", 2).unwrap();
        f.commit();
        f.increment(&ROOT, "c", 3).unwrap();
        let changes = changes_since(&mut f, &heads);
        assert_manifold_matches_v1(&mut doc, changes);
    }

    #[test]
    fn manifold_pending_targets_stress() {
        // fork-of-fork: a second actor updates/deletes/increments
        // elements (and writes into an object) that are themselves
        // pending in the batch
        let mut rng = make_rng();
        for _ in 0..50 {
            let mut doc = AutoCommit::new().with_actor(rng.random()).unwrap();
            let list = doc.put_object(&ROOT, "list", ObjType::List).unwrap();
            for i in 0..4 {
                doc.insert(&list, i, i as i64).unwrap();
            }
            doc.put(&list, 1, "u1").unwrap();
            let heads = doc.get_heads();

            let mut src = doc.fork().with_actor(rng.random()).unwrap();
            for _ in 0..4 {
                let mut f1 = doc.fork().with_actor(rng.random()).unwrap();
                let at = rng.random_range(0..=f1.length(&list) as u32) as usize;
                f1.insert(&list, at, 100i64).unwrap();
                let at2 = rng.random_range(0..=f1.length(&list) as u32) as usize;
                f1.insert(&list, at2, ScalarValue::counter(5)).unwrap();
                let at3 = rng.random_range(0..=f1.length(&list) as u32) as usize;
                let nested = f1.insert_object(&list, at3, ObjType::Map).unwrap();
                f1.commit();
                let mut f2 = f1.fork().with_actor(rng.random()).unwrap();
                for _ in 0..rng.random_range(1..8u32) {
                    let len = f2.length(&list) as u32;
                    match rng.random_range(0..5u32) {
                        0 => {
                            let at = rng.random_range(0..len) as usize;
                            f2.put(&list, at, "x").unwrap();
                        }
                        1 => {
                            let at = rng.random_range(0..len) as usize;
                            let _ = f2.delete(&list, at);
                        }
                        2 => {
                            let at = rng.random_range(0..=len) as usize;
                            f2.insert(&list, at, 7i64).unwrap();
                        }
                        3 => {
                            let at = rng.random_range(0..len) as usize;
                            let _ = f2.increment(&list, at, 3);
                        }
                        _ => {
                            let k = format!("n{}", rng.random_range(0..3u32));
                            f2.put(&nested, k, 1i64).unwrap();
                        }
                    }
                }
                f1.merge(&mut f2).unwrap();
                src.merge(&mut f1).unwrap();
            }
            let changes = changes_since(&mut src, &heads);
            assert_manifold_matches_v1(&mut doc, changes);
        }
    }

    /// Time the position/succ-resolution phase only: the v1 walkers
    /// (order_ops_for_doc ordering assumed done, walk assigns
    /// pos/subsort + succ) vs the manifold (same inputs, doc-ordered).
    /// Per-iteration state clones happen outside the timed sections.
    /// Run with:
    ///   cargo test -p automerge --release --lib bench_manifold_vs_v1 -- --ignored --nocapture
    #[test]
    #[ignore]
    fn bench_manifold_vs_v1() {
        use std::time::{Duration, Instant};

        fn report(label: &str, mut v1: Vec<Duration>, mut v2: Vec<Duration>) {
            v1.sort();
            v2.sort();
            let med1 = v1[v1.len() / 2];
            let med2 = v2[v2.len() / 2];
            println!(
                "{label:40} v1 min {:>10.2?} med {:>10.2?} | v2 min {:>10.2?} med {:>10.2?} | med speedup {:>6.1}x",
                v1[0],
                med1,
                v2[0],
                med2,
                med1.as_nanos() as f64 / med2.as_nanos().max(1) as f64,
            );
        }

        fn run(label: &str, doc: &mut AutoCommit, changes: Vec<Change>, iters: usize) {
            let doc = &mut doc.doc;

            let mut chap = BatchApply::default();
            for c in changes {
                chap.push(c);
            }
            chap.insert_new_actors(doc);
            for c in &chap.changes {
                doc.import_ops_to(c, &mut chap.ops).unwrap();
            }
            let mut obj_info = doc.ops().obj_info.clone();
            chap.order_ops_for_doc(&mut obj_info);

            let base_ops = chap.ops.clone();
            let base_pred = chap.pred.clone();

            let mut v1_times = Vec::with_capacity(iters);
            for _ in 0..iters {
                chap.ops = base_ops.clone();
                chap.pred = base_pred.clone();
                let mut log = PatchLog::inactive();
                let mut succ = vec![];
                let mut conflicts = vec![];
                let t = Instant::now();
                let mut walker = ObjWalker::new(doc.ops());
                for os in &chap.obj_spans {
                    let obj_range = walker.seek_to_obj(os.obj);
                    let doc_ops = doc.ops().iter_range(&obj_range);
                    match obj_info.object_type(&os.obj) {
                        Some(ObjType::Map) => {
                            let mut mw = MapWalker::new(
                                os.obj,
                                doc_ops,
                                doc.text_encoding(),
                                &mut chap.pred,
                                &mut succ,
                                &mut log,
                                &mut conflicts,
                            );
                            walk_map(&mut mw, &mut chap.ops[os.span.clone()]);
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
                                &mut chap.ops[os.span.clone()],
                                &mut chap.pred,
                                doc_ops.end_pos(),
                            );
                            walk_list(ut, doc_ops, &mut succ, &mut log);
                        }
                        _ => panic!("obj missing from index"),
                    }
                }
                v1_times.push(t.elapsed());
                std::hint::black_box(&succ);
            }

            // chap.ops is now in final document order — v2's input
            let ordered: Vec<ChangeOp> = chap.ops.clone();
            let mut v2_times = Vec::with_capacity(iters);
            for _ in 0..iters {
                let ops = ordered.clone();
                let t = Instant::now();
                let out = doc
                    .ops()
                    .apply_manifold(doc.change_graph.current_clock())
                    .apply(ops.into_iter());
                v2_times.push(t.elapsed());
                std::hint::black_box(&out);
            }

            report(label, v1_times, v2_times);
        }

        let mut rng = make_rng();

        // ── scenario 1: large batch into a large text document ──────
        let mut doc = AutoCommit::new().with_actor(rng.random()).unwrap();
        let text = doc.put_object(&ROOT, "text", ObjType::Text).unwrap();
        let body: String = (0..20_000)
            .map(|i| (b'a' + (i % 26) as u8) as char)
            .collect();
        doc.splice_text(&text, 0, 0, &body).unwrap();
        let heads = doc.get_heads();
        let mut f = doc.fork().with_actor(rng.random()).unwrap();
        for _ in 0..4 {
            for _ in 0..500 {
                let len = f.length(&text);
                if rng.random_range(0..4u32) == 0 && len > 1 {
                    let at = rng.random_range(0..len as u32) as usize;
                    f.splice_text(&text, at, 1, "").unwrap();
                } else {
                    let at = rng.random_range(0..=len as u32) as usize;
                    f.splice_text(&text, at, 0, "x").unwrap();
                }
            }
            f.commit();
        }
        let changes = changes_since(&mut f, &heads);
        run("2000 ops -> 20k-char text doc", &mut doc, changes, 15);

        // ── scenario 2: single insert into a large text document ────
        let mut doc = AutoCommit::new().with_actor(rng.random()).unwrap();
        let text = doc.put_object(&ROOT, "text", ObjType::Text).unwrap();
        let body: String = (0..100_000)
            .map(|i| (b'a' + (i % 26) as u8) as char)
            .collect();
        doc.splice_text(&text, 0, 0, &body).unwrap();
        let heads = doc.get_heads();
        let mut f = doc.fork().with_actor(rng.random()).unwrap();
        f.splice_text(&text, 50_000, 0, "x").unwrap();
        let changes = changes_since(&mut f, &heads);
        run("1 insert -> 100k-char text doc", &mut doc, changes, 400);

        // ── scenario 2b: single insert near the END of a large doc ──
        let mut doc = AutoCommit::new().with_actor(rng.random()).unwrap();
        let text = doc.put_object(&ROOT, "text", ObjType::Text).unwrap();
        doc.splice_text(&text, 0, 0, &body).unwrap();
        let heads = doc.get_heads();
        let mut f = doc.fork().with_actor(rng.random()).unwrap();
        f.splice_text(&text, 99_999, 0, "x").unwrap();
        let changes = changes_since(&mut f, &heads);
        run("1 insert -> end of 100k-char text", &mut doc, changes, 400);
    }
}
