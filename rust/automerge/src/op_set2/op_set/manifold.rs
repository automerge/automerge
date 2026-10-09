//! Seek-mode batch apply: resolves where a document-ordered stream of
//! change ops lands, and which document rows it succeeds, without
//! walking the whole document.

use crate::op_set2::op::DocSucc;
use crate::op_set2::types::Action;
use crate::storage::change_set::{ManifoldOp, ManifoldOps};
use crate::types::{Clock, ElemId, ObjId, ObjType, OpId};
use crate::AutomergeError;

use super::index::{ObjIndex, ObjInfo};
use super::op_iter::{ObjIdIter, OpIdIter, SuccIterIter};
use super::{OpSet, ValueIter};

use hexane::Shiftable;

use std::ops::Range;

pub(crate) struct ManifoldResult {
    pub(crate) doc_succ: DocSucc,
    /// Ascending batch-op ranges, each with the pre-merge document
    /// position it lands at. Delete ops never land, so their indexes
    /// fall in the gaps between runs.
    pub(crate) insert_runs: Vec<CopyRange>,
    /// `top` bits to rewrite on document rows, by pre-merge position.
    /// Ascending, as [`OpSet::write_tops`] requires.
    pub(crate) doc_tops: Vec<(usize, bool)>,
    /// The same for batch rows, indexed into the fragment's own columns.
    pub(crate) batch_tops: Vec<(usize, bool)>,
}

/// Fragment rows `range` land in front of document row `pos`
/// (pre-merge); `sub_range` and `val_range` are their succ entries and
/// value bytes in the fragment.
#[derive(Debug, Clone)]
pub(crate) struct CopyRange {
    pub(crate) pos: usize,
    pub(crate) range: Range<usize>,
    pub(crate) sub_range: Range<usize>,
    pub(crate) val_range: Range<usize>,
}

impl OpSet {
    /// `clock` is the document clock before the batch: preds it covers
    /// are document rows.
    pub(crate) fn apply_manifold(&self, clock: Clock) -> ApplyManifold<'_> {
        ApplyManifold::new(self, clock)
    }
}

/// Streaming state of a batch apply.
///
/// Ops must arrive in final document order, with actor indexes mapped
/// to this doc: every cursor here is forward-only. `order_ops_for_doc`
/// alone does not produce this order for sequence inserts.
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
    // Cannot share one iterator: a group-end scan consumes the row the
    // next insert's hop must land on, and vice versa.
    ins_hop: hexane::Iter<'a, bool>,
    ins_group_end: hexane::Iter<'a, bool>,

    obj: ObjId,
    obj_type: ObjType,
    obj_scope: Range<usize>,
    key: Option<String>,

    insert_runs: Vec<CopyRange>,
    op_index: usize,
    sub_consumed: usize,
    val_consumed: usize,
    succ_cache: SuccCache,
    // (row, slot) of the last row a slot scan stopped on. Any later
    // insert that beats the row lands at the same slot, except one
    // anchored at the row itself.
    memo: Option<(OpId, usize)>,
    // the last slot was the object end with no memo, so every later op
    // in this object appends there too
    appending: bool,
    elem: Option<ElemId>,
    last_insert: Option<OpId>,
    // Doc elem rows the cursor stopped on in this object. A covered
    // anchor behind the cursor is always one of these: document order
    // forbids later ops from targeting any other row it passed.
    consumed: rustc_hash::FxHashSet<OpId>,
    elem_scope: Range<usize>,

    top: TopCalc,
    hint_overshot: bool,
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
        let ins_hop = op_set.insert().values().iter();
        let ins_group_end = op_set.insert().values().iter();

        let obj = ObjId::root();
        let obj_scope = obj_id_iter.seek_to_value(obj);

        key_iter.shift(obj_scope.clone());
        id_iter.shift(obj_scope.clone());
        pred_iter.shift(obj_scope.clone());

        let succ_cache = SuccCache::new();

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
            ins_hop,
            ins_group_end,
            obj,
            obj_type: ObjType::Map,
            obj_scope,
            key: None,
            insert_runs: vec![],
            op_index: 0,
            sub_consumed: 0,
            val_consumed: 0,
            succ_cache,
            memo: None,
            appending: false,
            elem: None,
            last_insert: None,
            consumed: rustc_hash::FxHashSet::default(),
            elem_scope: 0..0,
            top: TopCalc::new(),
            hint_overshot: false,
        }
    }

    /// Record that the `n` ops from `op_index` land at `pos`, carrying
    /// `sub` succ entries and `val` value bytes between them.
    fn push_run(&mut self, pos: usize, n: usize, sub: usize, val: usize) {
        let i = self.op_index;
        let (sub0, val0) = (self.sub_consumed, self.val_consumed);
        self.sub_consumed += sub;
        self.val_consumed += val;
        match self.insert_runs.last_mut() {
            Some(cr) if cr.pos == pos && cr.range.end == i => {
                cr.range.end = i + n;
                cr.sub_range.end = self.sub_consumed;
                cr.val_range.end = self.val_consumed;
            }
            _ => self.insert_runs.push(CopyRange {
                pos,
                range: i..i + n,
                sub_range: sub0..self.sub_consumed,
                val_range: val0..self.val_consumed,
            }),
        }
    }

    fn push_pos(&mut self, pos: usize, sub: usize, val: usize) {
        self.push_run(pos, 1, sub, val);
    }

    fn close_scope(&mut self) {
        self.succ_cache.flush(
            &mut self.pred_iter,
            &mut self.succ_iter,
            &mut self.value_iter,
        );
        self.top
            .finalize(self.op_set, &self.succ_cache.doc_succ.clears);
    }

    /// Resolve the fragment against the document without writing to it.
    ///
    /// Fails with [`AutomergeError::MalformedChangeSet`] if an op's
    /// position hint overshoots its target row: hints are floors in any
    /// document holding the change set's deps, so the sender computed
    /// it wrong.
    pub(crate) fn apply_change_set_ops(
        mut self,
        src: &mut ManifoldOps<'_>,
    ) -> Result<ManifoldResult, AutomergeError> {
        while src.pos < src.len {
            if self.tail_ready(src) {
                self.consume_tail(src);
                continue;
            }
            let op = src.next_op();
            self.apply_op(&op);
            if self.hint_overshot {
                return Err(AutomergeError::MalformedChangeSet(
                    "op position hint overshot its target row",
                ));
            }
        }
        Ok(self.finish())
    }

    fn push_slot(&mut self, pos: usize, sub: usize, val: usize) {
        self.push_pos(pos, sub, val);
        self.appending = pos == self.obj_scope.end && self.memo.is_none();
    }

    fn land(&mut self, op: &ManifoldOp<'_>, slot: usize) {
        self.push_slot(slot, op.sub_len, op.val_len);
        self.top.candidate(op, slot, self.op_index);
    }

    fn doc_spent(&self) -> bool {
        self.obj_scope.end >= self.op_set.len()
    }

    fn tail_ready(&self, src: &ManifoldOps<'_>) -> bool {
        if !self.appending || src.pos >= src.len {
            return false;
        }
        let rem = src.same_obj_run(src.len - src.pos);
        rem > 0 && src.pred_free_run() >= rem
    }

    fn record_if_object(&mut self, op: &ManifoldOp<'_>) -> bool {
        let Ok(obj_type) = ObjType::try_from(op.action) else {
            return false;
        };
        self.obj_info.insert(
            op.id,
            ObjInfo {
                parent: op.obj,
                obj_type,
            },
        );
        true
    }

    /// Consume the rest of the current object, all of it landing at the
    /// object end. The scope open at the transition may still hold doc
    /// rows, so ops resolve one at a time until it closes.
    fn consume_tail(&mut self, src: &mut ManifoldOps<'_>) {
        let end = self.obj_scope.end;
        let mut rem = src.same_obj_run(src.len - src.pos);
        let mut straddling = true;
        while rem > 0 && straddling {
            let n = src.clean_insert_run().min(rem);
            if n > 0 {
                straddling = false;
                self.close_scope();
                // each clean insert is a single-candidate scope, so it
                // registers no top candidate
                let run = src.skip_clean(n);
                self.push_run(end, n, 0, run.val_bytes);
                self.op_index += n;
                self.last_insert = Some(run.last_id);
                self.elem = None;
                rem -= n;
                continue;
            }

            let op = src.next_op();
            debug_assert!(op.preds.is_empty(), "tail op carries doc preds");
            debug_assert!(op.action != Action::Delete, "tail delete has no row");
            self.record_if_object(&op);
            let mut pending_after_run = false;
            let boundary = if let Some(k) = op.key.key_str() {
                if self.key.as_deref() != Some(k) {
                    self.key = Some(k.to_owned());
                    true
                } else {
                    false
                }
            } else if op.insert {
                self.elem = None;
                true
            } else {
                let e = op.key.elemid().unwrap();
                let fresh = self.elem != Some(e) && self.last_insert != Some(e.0);
                // the clean run's last insert skipped candidate
                // registration, so an update to it re-adds it
                pending_after_run = self.elem.is_none() && self.last_insert == Some(e.0);
                self.elem = Some(e);
                fresh
            };
            if boundary {
                straddling = false;
                self.close_scope();
            }
            if pending_after_run {
                self.top.candidate_raw(end, self.op_index - 1, true);
            }
            if op.insert {
                self.last_insert = Some(op.id);
            }
            self.push_pos(end, op.sub_len, op.val_len);
            self.top.candidate(&op, end, self.op_index);
            self.op_index += 1;
            rem -= 1;
        }
        if rem > 0 {
            self.consume_blank(src, end, rem);
        }
        if straddling {
            self.close_scope();
        } else {
            self.top
                .finalize_blank(self.succ_cache.doc_succ.clears.len());
        }
        self.top.open(end..end);
        self.elem = None;
    }

    /// Take the object's remaining `rem` ops, all landing at `end`. Every
    /// scope left is fragment-only, so only `Make*` rows are decoded.
    fn consume_blank(&mut self, src: &mut ManifoldOps<'_>, end: usize, mut rem: usize) {
        // Past the last document row nothing is left to decide for any
        // object, and the fragment's own index build supplies `obj_info`:
        // the rest of the fragment is one copy range.
        if self.doc_spent() {
            rem = src.len - src.pos;
            if rem == 0 {
                return;
            }
            let (sub_end, val_end) = src.consume_rest();
            let (sub, val) = (sub_end - self.sub_consumed, val_end - self.val_consumed);
            self.push_run(end, rem, sub, val);
            self.op_index += rem;
            return;
        }
        while rem > 0 {
            let n = src.make_free_run(rem);
            if n > 0 {
                let run = src.skip_tail(n);
                self.push_run(end, n, run.sub, run.val);
                self.op_index += n;
                if let Some((_, id)) = run.last_insert {
                    self.last_insert = Some(id);
                }
                rem -= n;
                continue;
            }

            let op = src.next_op();
            debug_assert!(op.preds.is_empty(), "tail op carries doc preds");
            let is_object = self.record_if_object(&op);
            assert!(is_object, "make-bounded run");
            if op.insert {
                self.last_insert = Some(op.id);
            }
            self.push_pos(end, op.sub_len, op.val_len);
            self.op_index += 1;
            rem -= 1;
        }
    }

    fn apply_op(&mut self, op: &ManifoldOp<'_>) {
        self.record_if_object(op);
        if op.obj != self.obj {
            self.enter_object(op.obj);
        }
        if self.obj_type == ObjType::Map {
            self.apply_map_op(op);
        } else if op.insert {
            self.apply_insert(op);
        } else {
            self.apply_elem_update(op);
        }
        self.op_index += 1;
    }

    fn enter_object(&mut self, obj: ObjId) {
        self.close_scope();

        self.obj = obj;
        self.obj_scope = self.obj_id_iter.seek_to_value(self.obj);
        self.obj_type = self.obj_info.object_type(&self.obj).unwrap();

        self.id_iter.shift(self.obj_scope.clone());
        self.pred_iter.shift(self.obj_scope.clone());
        self.elem = None;
        self.memo = None;
        self.consumed.clear();
        self.appending = false;

        if self.obj_type == ObjType::Map {
            self.key_iter.shift(self.obj_scope.clone());
            self.key = None;
        }
    }

    fn apply_map_op(&mut self, op: &ManifoldOp<'_>) {
        if self.key.as_deref() != op.key.key_str() {
            self.key = op.key.key_str().map(str::to_owned);
            let key_scope = self.key_iter.seek_to_value(self.key.as_deref(), ..);
            self.close_scope();
            self.top.open(key_scope.clone());

            self.id_iter.shift(key_scope.clone());
            self.pred_iter.shift(key_scope.clone());
            self.succ_iter.shift(key_scope);
        }
        self.cache_preds(op);
        if op.action != Action::Delete {
            let r = self.id_iter.seek_to_value(&op.id);
            assert!(r.is_empty());
            self.land(op, r.start);
        }
    }

    fn apply_insert(&mut self, op: &ManifoldOp<'_>) {
        let e = op.key.elemid().unwrap();
        self.last_insert = Some(op.id);

        self.close_scope();
        // an insert's scope has no doc rows, so the slot is unused
        self.top.candidate(op, 0, self.op_index);
        self.elem = None;

        if !e.is_head() && self.clock.covers(&e.0) {
            self.widen_id_iter();
            // Only a row in `consumed` is known to be behind the cursor
            // (where the slot search just continues); treating any other
            // anchor as behind would corrupt the slot.
            if !self.consumed.contains(&e.0) {
                let start = self.id_iter.pos();
                if self.scan_for_elem(&e.0, op.hint).is_some() {
                    self.consumed.insert(e.0);
                    // the memo slot precedes the anchor
                    self.memo = None;
                } else {
                    // behind the cursor after all: rewind and let the
                    // memo or hop resolve the slot
                    self.id_iter = self.op_set.id_iter_range(&(start..self.obj_scope.end));
                }
            }
        }
        let memo_hit = self
            .memo
            .filter(|&(row_id, _)| row_id < op.id && row_id != e.0);
        if let Some((_, slot)) = memo_hit {
            // no iterator may move: the next update group or sibling
            // still needs the rows ahead
            self.push_slot(slot, op.sub_len, op.val_len);
        } else {
            // Inserts land only in front of an insert row or the object
            // end, so hop past update rows first. Head- and
            // pending-anchored inserts skip the re-widening above, and a
            // narrowed window would yield a bogus end-of-object slot.
            self.widen_id_iter();
            let cur = self.id_iter.pos();
            self.ins_hop.shift(cur..self.obj_scope.end);
            let epos = self
                .ins_hop
                .scan_to_value(true)
                .unwrap_or(self.obj_scope.end);
            self.id_iter.advance_to(epos);
            if let Some((found, row_id)) = self.id_iter.scan_to_lesser(op.id) {
                self.push_slot(found, op.sub_len, op.val_len);
                self.memo = Some((row_id, found));
                self.consumed.insert(row_id);
            } else {
                self.push_slot(self.obj_scope.end, op.sub_len, op.val_len);
                self.memo = None;
            }
        }
    }

    fn apply_elem_update(&mut self, op: &ManifoldOp<'_>) {
        let e = op.key.elemid().unwrap();
        if self.elem != Some(e) {
            if self.last_insert == Some(e.0) {
                debug_assert!(self.succ_cache.preds.is_empty());
                self.elem = Some(e);
                // A pending element has no doc rows: its updates land at
                // its insert's slot. The memo stays so its children land
                // there too.
                let p = self.insert_runs.last().unwrap().pos;
                self.elem_scope = p..p;
            } else {
                self.close_scope();
                self.elem = Some(e);
                self.open_elem_group(e, op);
            }
        }
        self.cache_preds(op);
        if op.action != Action::Delete {
            let slot = if self.elem_scope.is_empty() {
                self.elem_scope.start
            } else {
                let r = self.id_iter.seek_to_value(&op.id);
                assert!(r.is_empty());
                r.start
            };
            self.land(op, slot);
        }
    }

    fn open_elem_group(&mut self, e: ElemId, op: &ManifoldOp<'_>) {
        let epos = match self.memo {
            Some((row_id, slot)) if row_id == e.0 => slot,
            _ => {
                self.widen_id_iter();
                match self.scan_for_elem(&e.0, op.hint) {
                    Some(p) => p,
                    None => panic!(
                        "update target {:?} not found: op {:?} action {:?} pred {:?} id_iter pos {} obj_scope {:?} elem_scope {:?} memo {:?} last_insert {:?}",
                        e, op.id, op.action, op.preds, self.id_iter.pos(), self.obj_scope, self.elem_scope, self.memo, self.last_insert
                    ),
                }
            }
        };
        // the element row now sits between any memo slot and the cursor
        self.memo = None;
        self.consumed.insert(e.0);
        self.ins_group_end.shift((epos + 1)..self.obj_scope.end);
        let group_end = self
            .ins_group_end
            .scan_to_value(true)
            .unwrap_or(self.obj_scope.end);
        self.elem_scope = epos..group_end;

        self.top.open(self.elem_scope.clone());

        self.id_iter.shift(self.elem_scope.clone());
        self.pred_iter.shift(self.elem_scope.clone());
        self.succ_iter.shift(self.elem_scope.clone());
    }

    fn widen_id_iter(&mut self) {
        self.id_iter.set_max(self.obj_scope.end);
    }

    /// Scan forward for the elem row `target`, jumping to `hint` first.
    ///
    /// A hint is a floor on the row's rank within the object in any
    /// document holding the fragment's deps. A miss after a jump rescans
    /// the skipped region: finding the row there means the hint overshot.
    fn scan_for_elem(&mut self, target: &OpId, hint: Option<u64>) -> Option<usize> {
        let start = self.id_iter.pos();
        let mut jumped = false;
        if let Some(h) = hint {
            let h = self.obj_scope.start + h as usize;
            if h > start && h < self.obj_scope.end {
                self.id_iter.advance_to(h);
                jumped = true;
            }
        }
        let mut found = self.id_iter.scan_to_value(target);
        if found.is_none() && jumped {
            self.id_iter = self.op_set.id_iter_range(&(start..self.obj_scope.end));
            found = self.id_iter.scan_to_value(target);
            self.hint_overshot |= found.is_some();
        }
        found
    }

    fn cache_preds(&mut self, op: &ManifoldOp<'_>) {
        for pred_id in &op.preds {
            debug_assert!(self.clock.covers(pred_id), "fragment op had a batch pred");
            self.succ_cache.push(*pred_id, op.id, op.inc);
        }
    }

    pub(crate) fn finish(mut self) -> ManifoldResult {
        self.close_scope();

        ManifoldResult {
            doc_succ: self.succ_cache.doc_succ,
            insert_runs: self.insert_runs,
            doc_tops: self.top.doc_tops,
            batch_tops: self.top.batch_tops,
        }
    }
}

/// Decides each touched register's `top` bit while the batch walks. The
/// winner is the later of the last document row still visible after
/// this batch's deletes and the last visible batch op.
///
/// Registers with no document rows are not decided here: the fragment's
/// own index build already elected them over exactly their own rows.
struct TopCalc {
    doc_scope: Range<usize>,
    batch: Vec<BatchTop>,
    /// start of this scope's entries in `DocSucc::clears`, the rows
    /// this batch deletes
    deleted_from: usize,
    doc_tops: Vec<(usize, bool)>,
    batch_tops: Vec<(usize, bool)>,
}

struct BatchTop {
    /// the document row this op lands in front of
    slot: usize,
    batch_row: usize,
    alive: bool,
}

impl TopCalc {
    fn new() -> Self {
        TopCalc {
            doc_scope: 0..0,
            batch: vec![],
            deleted_from: 0,
            doc_tops: vec![],
            batch_tops: vec![],
        }
    }

    fn open(&mut self, doc_scope: Range<usize>) {
        debug_assert!(self.batch.is_empty());
        self.doc_scope = doc_scope;
    }

    /// Increments change a counter's value, not which op the register
    /// shows, so they never compete.
    fn candidate(&mut self, op: &ManifoldOp<'_>, slot: usize, batch_row: usize) {
        if op.action == Action::Increment {
            return;
        }
        self.candidate_raw(slot, batch_row, op.alive);
    }

    fn candidate_raw(&mut self, slot: usize, batch_row: usize, alive: bool) {
        self.batch.push(BatchTop {
            slot,
            batch_row,
            alive,
        });
    }

    fn finalize_blank(&mut self, clears: usize) {
        self.clear_scope(clears);
    }

    fn clear_scope(&mut self, clears: usize) {
        self.batch.clear();
        self.deleted_from = clears;
        self.doc_scope = 0..0;
    }

    fn finalize(&mut self, op_set: &OpSet, clears: &[usize]) {
        if self.doc_scope.is_empty() {
            self.finalize_blank(clears.len());
            return;
        }
        // slots ascend (document order), so the last alive op is the latest
        let winner = self.batch.iter().rev().find(|b| b.alive);
        let deleted = &clears[self.deleted_from..];
        if winner.is_none() && deleted.is_empty() {
            self.clear_scope(clears.len());
            return;
        }

        // `top` is on the register's last visible row, so it falls out
        // of the same pass rather than reading the column
        let scope = self.doc_scope.clone();
        let mut old_top = None;
        let mut last_doc = None;
        for (i, visible) in op_set
            .cols
            .index
            .visible
            .iter_range(scope.clone())
            .enumerate()
        {
            if visible {
                let pos = scope.start + i;
                old_top = Some(pos);
                if !deleted.contains(&pos) {
                    last_doc = Some(pos);
                }
            }
        }

        // a slot equal to a document row lands in front of it, so the
        // row wins ties
        let batch_wins = match (winner, last_doc) {
            (Some(w), Some(d)) => w.slot > d,
            (Some(_), None) => true,
            (None, _) => false,
        };

        // the fragment already gave `winner` its bit; it only steps down
        // when the document beats it
        let doc_top = if batch_wins {
            None
        } else {
            if let Some(w) = winner {
                self.batch_tops.push((w.batch_row, false));
            }
            last_doc
        };
        // `add_succ` already cleared `top` on every row in `deleted`
        if let Some(old) = old_top {
            if doc_top != Some(old) && !deleted.contains(&old) {
                self.doc_tops.push((old, false));
            }
        }
        if let Some(new) = doc_top {
            if old_top != Some(new) {
                self.doc_tops.push((new, true));
            }
        }

        self.clear_scope(clears.len());
    }
}

struct SuccCache {
    preds: Vec<(OpId, OpId, Option<i64>)>,
    doc_succ: DocSucc,
}

impl SuccCache {
    fn new() -> Self {
        SuccCache {
            preds: vec![],
            doc_succ: DocSucc::default(),
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
    ) {
        // sorted preds are in row order, keeping the iterators forward-only
        self.preds.sort_unstable();
        // value_iter cannot re-read a row, so a second increment of the
        // same pred needs the memo
        let mut counter_memo: Option<(OpId, bool)> = None;
        for (pred, succ, mut inc) in self.preds.drain(..) {
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
            let s = succ_iter.add_succ_at(r.start, succ, inc).unwrap();
            self.doc_succ.push(s);
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::autocommit::AutoCommit;
    use crate::read::ReadDoc;
    use crate::tx::Transactable;
    use crate::{make_rng, Change, ObjType, ScalarValue, ROOT};
    use rand::prelude::*;

    fn assert_batch_validates<H: crate::hash_retention::HashRetention>(
        doc: &mut AutoCommit<H>,
        changes: Vec<Change>,
    ) {
        let mut d = doc.fork();
        d.doc.apply_changes_batch(changes).unwrap();
        d.doc.validate_document();
    }

    /// A fragment-created object holds no delete rows: deletes of its
    /// elements ride the succ column.
    #[test]
    fn manifold_own_object_put_and_delete() {
        let actor = |b: u8| crate::ActorId::from(vec![b]);
        let mut doc = AutoCommit::new()
            .with_actor(actor(1))
            .enable_audit_mode()
            .unwrap();
        doc.put(&ROOT, "base", "x").unwrap();
        doc.commit();
        let heads = doc.get_heads();

        // the list and its element are created inside the fragment
        let mut src = doc.fork().with_actor(actor(3));
        let list = src.put_object(&ROOT, "list", ObjType::List).unwrap();
        src.insert(&list, 0, "a").unwrap();
        src.commit();

        // concurrent put and delete of that element, also in the fragment
        let mut fa = src.fork().with_actor(actor(2));
        fa.put(&list, 0, "put").unwrap();
        let mut fb = src.fork().with_actor(actor(9));
        fb.delete(&list, 0).unwrap();
        src.merge(&mut fa).unwrap();
        src.merge(&mut fb).unwrap();

        let changes = changes_since(&mut src, &heads);
        assert_batch_validates(&mut doc, changes);
    }

    /// The fragment elects the delete row, which never merges, so the
    /// batch election must put `top` back on the put.
    #[test]
    fn manifold_concurrent_put_and_delete_same_key() {
        let actor = |b: u8| crate::ActorId::from(vec![b]);
        let mut doc = AutoCommit::new()
            .with_actor(actor(1))
            .enable_audit_mode()
            .unwrap();
        doc.put(&ROOT, "k", "v").unwrap();
        doc.commit();
        let heads = doc.get_heads();

        let mut fa = doc.fork().with_actor(actor(2));
        fa.put(&ROOT, "k", "a").unwrap();
        let mut fb = doc.fork().with_actor(actor(9));
        fb.delete(&ROOT, "k").unwrap();

        let mut src = doc.fork().with_actor(actor(3));
        src.merge(&mut fa).unwrap();
        src.merge(&mut fb).unwrap();

        let changes = changes_since(&mut src, &heads);
        assert_batch_validates(&mut doc, changes);
    }

    /// In the fragment's index, an update to a document element fuses
    /// into the insert before it; the merge must split their tops again.
    #[test]
    fn manifold_insert_then_update_of_doc_element() {
        let actor = |b: u8| crate::ActorId::from(vec![b]);
        let mut doc = AutoCommit::new()
            .with_actor(actor(1))
            .enable_audit_mode()
            .unwrap();
        let list = doc.put_object(&ROOT, "list", ObjType::List).unwrap();
        doc.insert(&list, 0, "a").unwrap();
        doc.insert(&list, 1, "b").unwrap();
        let heads = doc.get_heads();

        let mut f = doc.fork().with_actor(actor(2));
        f.insert(&list, 0, "x").unwrap();
        f.put(&list, 1, "a2").unwrap();

        let changes = changes_since(&mut f, &heads);
        assert_batch_validates(&mut doc, changes);
    }

    fn changes_since<H: crate::hash_retention::HashRetention>(
        src: &mut AutoCommit<H>,
        heads: &[crate::ChangeId],
    ) -> Vec<Change> {
        src.get_changes(heads).unwrap()
    }

    #[test]
    fn manifold_double_increment_same_pred() {
        let mut rng = make_rng();
        let mut doc = AutoCommit::new()
            .with_actor(rng.random())
            .enable_audit_mode()
            .unwrap();
        doc.put(&ROOT, "c", ScalarValue::counter(10)).unwrap();
        let heads = doc.get_heads();

        let mut f1 = doc.fork().with_actor(rng.random());
        let mut f2 = doc.fork().with_actor(rng.random());
        f1.increment(&ROOT, "c", 5).unwrap();
        f2.increment(&ROOT, "c", 7).unwrap();
        let mut src = doc.fork().with_actor(rng.random());
        src.merge(&mut f1).unwrap();
        src.merge(&mut f2).unwrap();

        let changes = changes_since(&mut src, &heads);
        assert_batch_validates(&mut doc, changes);
    }

    #[test]
    fn manifold_inserts_around_updates() {
        // the doc holds update rows inside element groups; new sibling
        // inserts must not split them
        let mut rng = make_rng();
        let mut doc = AutoCommit::new()
            .with_actor(rng.random())
            .enable_audit_mode()
            .unwrap();
        let list = doc.put_object(&ROOT, "list", ObjType::List).unwrap();
        doc.insert(&list, 0, "a").unwrap();
        doc.insert(&list, 1, "b").unwrap();
        doc.put(&list, 0, "a2").unwrap(); // update row in a's group
        doc.put(&list, 0, "a3").unwrap();
        let heads = doc.get_heads();

        let mut f1 = doc.fork().with_actor(rng.random());
        let mut f2 = doc.fork().with_actor(rng.random());
        f1.insert(&list, 1, "x").unwrap(); // after a, competing with b
        f2.insert(&list, 1, "y").unwrap();
        f2.insert(&list, 2, "z").unwrap(); // chained after y
        let mut src = doc.fork().with_actor(rng.random());
        src.merge(&mut f1).unwrap();
        src.merge(&mut f2).unwrap();

        let changes = changes_since(&mut src, &heads);
        assert_batch_validates(&mut doc, changes);
    }

    #[test]
    fn manifold_memo_resets_across_objects() {
        let mut rng = make_rng();
        let mut doc = AutoCommit::new()
            .with_actor(rng.random())
            .enable_audit_mode()
            .unwrap();
        let l1 = doc.put_object(&ROOT, "l1", ObjType::List).unwrap();
        let l2 = doc.put_object(&ROOT, "l2", ObjType::List).unwrap();
        doc.insert(&l1, 0, 1).unwrap();
        doc.insert(&l2, 0, 2).unwrap();
        let heads = doc.get_heads();

        let mut f = doc.fork().with_actor(rng.random());
        f.insert(&l1, 0, 10).unwrap();
        f.insert(&l2, 0, 20).unwrap();
        let mut src = doc.fork().with_actor(rng.random());
        src.merge(&mut f).unwrap();

        let changes = changes_since(&mut src, &heads);
        assert_batch_validates(&mut doc, changes);
    }

    #[test]
    fn manifold_pending_chains() {
        // typing runs: every insert anchors at the previous pending one
        let mut rng = make_rng();
        let mut doc = AutoCommit::new()
            .with_actor(rng.random())
            .enable_audit_mode()
            .unwrap();
        let text = doc.put_object(&ROOT, "text", ObjType::Text).unwrap();
        doc.splice_text(&text, 0, 0, "base").unwrap();
        let heads = doc.get_heads();

        let mut f1 = doc.fork().with_actor(rng.random());
        let mut f2 = doc.fork().with_actor(rng.random());
        f1.splice_text(&text, 2, 0, "hello world").unwrap();
        f2.splice_text(&text, 4, 0, "concurrent!").unwrap();
        let mut src = doc.fork().with_actor(rng.random());
        src.merge(&mut f1).unwrap();
        src.merge(&mut f2).unwrap();

        let changes = changes_since(&mut src, &heads);
        assert_batch_validates(&mut doc, changes);
    }

    /// Update groups narrow `id_iter`, then a head-anchored insert chain
    /// must still see the whole object.
    #[test]
    fn manifold_head_insert_after_update_groups() {
        let actor = |b: u8| crate::ActorId::from(vec![b]);
        let mut doc = AutoCommit::new()
            .with_actor(actor(1))
            .enable_audit_mode()
            .unwrap();
        let text = doc.put_object(&ROOT, "text", ObjType::Text).unwrap();
        doc.splice_text(&text, 0, 0, "ab").unwrap();

        // concurrent head typing by the LARGER actor: its subtree
        // leads the list once merged
        let mut f_c = doc.fork().with_actor(actor(3));
        f_c.splice_text(&text, 0, 0, "cd").unwrap();

        // concurrent head typing by the SMALLER actor (lands after
        // actor 3's subtree), plus an insert after 'a' and a delete of
        // 'b' — the covered-anchor ops downstream of the head chain
        let mut f_b = doc.fork().with_actor(actor(2));
        f_b.splice_text(&text, 0, 0, "hij").unwrap();
        f_b.splice_text(&text, 4, 0, "k").unwrap();
        f_b.splice_text(&text, 5, 1, "").unwrap();

        // deletes of c,d are causally after f_c: in the batch they are
        // update groups on doc rows, resolved first in document order
        let mut src = doc.fork().with_actor(actor(4));
        src.merge(&mut f_c).unwrap();
        src.splice_text(&text, 0, 2, "").unwrap();
        src.merge(&mut f_b).unwrap();

        // the receiver already has f_c — the batch is the deletes plus
        // f_b's concurrent changes
        doc.merge(&mut f_c).unwrap();
        let heads = doc.get_heads();
        let changes = changes_since(&mut src, &heads);
        assert_batch_validates(&mut doc, changes);
    }

    #[test]
    fn manifold_fuzz() {
        let mut rng = make_rng();
        for _ in 0..20 {
            let mut doc = AutoCommit::new()
                .with_actor(rng.random())
                .enable_audit_mode()
                .unwrap();
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

            let mut src = doc.fork().with_actor(rng.random());
            for _ in 0..6 {
                let mut f = doc.fork().with_actor(rng.random());
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
            assert_batch_validates(&mut doc, changes);
        }
    }

    #[test]
    fn manifold_two_lists() {
        // insert-column scans must stay bounded per object
        let mut rng = make_rng();
        let mut doc = AutoCommit::new()
            .with_actor(rng.random())
            .enable_audit_mode()
            .unwrap();
        let l1 = doc.put_object(&ROOT, "a", ObjType::List).unwrap();
        let l2 = doc.put_object(&ROOT, "b", ObjType::List).unwrap();
        for i in 0..4 {
            doc.insert(&l1, i, i as i64).unwrap();
            doc.insert(&l2, i, (i + 10) as i64).unwrap();
        }
        let heads = doc.get_heads();

        let mut f = doc.fork().with_actor(rng.random());
        // update the LAST element of l1: its group-end scan misses
        // inside l1 (no later insert row in the object)
        f.put(&l1, 3, "tail").unwrap();
        // then work in l2
        f.insert(&l2, 2, 99i64).unwrap();
        f.put(&l2, 1, "mid").unwrap();
        f.insert(&l2, 4, 98i64).unwrap();

        let changes = changes_since(&mut f, &heads);
        assert_batch_validates(&mut doc, changes);
    }

    #[test]
    fn manifold_update_after_slot_scan_consumed_elem() {
        // an insert whose slot scan stops exactly on the next element's
        // row consumes it; that element's updates arrive right after
        // (doc order) and must resolve it through the memo
        let mut rng = make_rng();
        let mut doc = AutoCommit::new()
            .with_actor(rng.random())
            .enable_audit_mode()
            .unwrap();
        let list = doc.put_object(&ROOT, "list", ObjType::List).unwrap();
        for i in 0..5 {
            doc.insert(&list, i, i as i64).unwrap();
        }
        let heads = doc.get_heads();

        let mut f1 = doc.fork().with_actor(rng.random());
        // insert after elem 1: slot scan stops on elem 2's row
        f1.insert(&list, 2, 50i64).unwrap();
        let mut f2 = doc.fork().with_actor(rng.random());
        // update elem 2: arrives after the insert in document order
        f2.put(&list, 2, "upd").unwrap();
        let mut src = doc.fork().with_actor(rng.random());
        src.merge(&mut f1).unwrap();
        src.merge(&mut f2).unwrap();

        let changes = changes_since(&mut src, &heads);
        assert_batch_validates(&mut doc, changes);
    }

    #[test]
    fn manifold_conflict_on_stale_batch_put() {
        // the batch's put wins by id while the doc's concurrent put
        // stays visible
        let mut doc = AutoCommit::new()
            .with_actor("aa".try_into().unwrap())
            .enable_audit_mode()
            .unwrap();
        doc.put(&ROOT, "k", "old").unwrap();
        doc.commit();
        let mut f = doc.fork().with_actor("bb".try_into().unwrap());
        let heads = doc.get_heads();
        f.put(&ROOT, "k", "fork1").unwrap();
        f.commit();
        f.put(&ROOT, "k", "fork2").unwrap(); // ctr 4: beats the local put
        doc.put(&ROOT, "k", "local").unwrap(); // ctr 3, concurrent, stays visible
        let changes = changes_since(&mut f, &heads);
        assert_batch_validates(&mut doc, changes);
    }

    #[test]
    fn manifold_batch_put_loses_to_local() {
        // mirror image: the local put has the higher ctr
        let mut doc = AutoCommit::new()
            .with_actor("aa".try_into().unwrap())
            .enable_audit_mode()
            .unwrap();
        doc.put(&ROOT, "k", "old").unwrap();
        doc.commit();
        let mut f = doc.fork().with_actor("bb".try_into().unwrap());
        let heads = doc.get_heads();
        f.put(&ROOT, "k", "fork").unwrap(); // ctr 3
        doc.put(&ROOT, "k", "l1").unwrap();
        doc.commit();
        doc.put(&ROOT, "k", "l2").unwrap(); // ctr 4: local wins
        let changes = changes_since(&mut f, &heads);
        assert_batch_validates(&mut doc, changes);
    }

    #[test]
    fn manifold_expose_on_delete_of_winner() {
        // the batch deletes one of a visible pair; both actor orders
        // run so it deletes the winner once
        for other in ["0b", "fb"] {
            let mut doc = AutoCommit::new()
                .with_actor("aa".try_into().unwrap())
                .enable_audit_mode()
                .unwrap();
            doc.put(&ROOT, "k", "old").unwrap();
            doc.commit();
            let mut fa = doc.fork().with_actor(other.try_into().unwrap());
            fa.put(&ROOT, "k", "a").unwrap();
            fa.commit();
            doc.put(&ROOT, "k", "b").unwrap();
            doc.commit();
            let pre = fa.get_heads();
            doc.merge(&mut fa).unwrap(); // doc: "a" and "b" both visible
            fa.delete(&ROOT, "k").unwrap(); // fa only sees "a": deletes one side
            let changes = changes_since(&mut fa, &pre);
            assert_batch_validates(&mut doc, changes);
        }
    }

    #[test]
    fn manifold_conflicted_among_pending_updates() {
        // two actors concurrently update an element that is itself
        // pending in the batch: one update wins, the other is flagged
        let mut rng = make_rng();
        let mut doc = AutoCommit::new()
            .with_actor(rng.random())
            .enable_audit_mode()
            .unwrap();
        let list = doc.put_object(&ROOT, "list", ObjType::List).unwrap();
        doc.insert(&list, 0, 1i64).unwrap();
        let heads = doc.get_heads();

        let mut f1 = doc.fork().with_actor(rng.random());
        f1.insert(&list, 1, 100i64).unwrap();
        f1.commit();
        let mut f2 = f1.fork().with_actor(rng.random());
        f2.put(&list, 1, "x").unwrap();
        let mut f3 = f1.fork().with_actor(rng.random());
        f3.put(&list, 1, "y").unwrap();
        f1.merge(&mut f2).unwrap();
        f1.merge(&mut f3).unwrap();
        let changes = changes_since(&mut f1, &heads);
        assert_batch_validates(&mut doc, changes);
    }

    #[test]
    fn manifold_increment_only_scope_no_adjusts() {
        // increments keep the counter visible and on top
        let mut doc = AutoCommit::new()
            .with_actor("aa".try_into().unwrap())
            .enable_audit_mode()
            .unwrap();
        doc.put(&ROOT, "c", ScalarValue::counter(5)).unwrap();
        let heads = doc.get_heads();
        let mut f = doc.fork().with_actor("bb".try_into().unwrap());
        f.increment(&ROOT, "c", 2).unwrap();
        f.commit();
        f.increment(&ROOT, "c", 3).unwrap();
        let changes = changes_since(&mut f, &heads);
        assert_batch_validates(&mut doc, changes);
    }

    #[test]
    fn manifold_concurrent_counters_incremented() {
        // increments that see both concurrent counters carry two preds
        let mut rng = make_rng();
        let mut doc = AutoCommit::new()
            .with_actor(rng.random())
            .enable_audit_mode()
            .unwrap();
        doc.put(&ROOT, "k", "seed").unwrap();
        let heads = doc.get_heads();

        let mut fa = doc.fork().with_actor(rng.random());
        fa.put(&ROOT, "k", ScalarValue::counter(0)).unwrap();
        fa.commit();
        for i in 0..4 {
            fa.increment(&ROOT, "k", i + 1).unwrap();
            fa.commit();
        }
        let mut fb = doc.fork().with_actor(rng.random());
        fb.put(&ROOT, "k", ScalarValue::counter(10)).unwrap();
        fb.commit();
        for i in 0..3 {
            fb.increment(&ROOT, "k", 10 * (i + 1)).unwrap();
            fb.commit();
        }
        let mut fc = fa.fork().with_actor(rng.random());
        fc.merge(&mut fb).unwrap();
        for i in 0..3 {
            fc.increment(&ROOT, "k", 100 * (i + 1)).unwrap();
            fc.commit();
        }

        let mut src = doc.fork().with_actor(rng.random());
        src.merge(&mut fa).unwrap();
        src.merge(&mut fb).unwrap();
        src.merge(&mut fc).unwrap();
        let changes = changes_since(&mut src, &heads);
        assert_batch_validates(&mut doc, changes);

        // same, with the counters already in the doc
        let mut doc2 = AutoCommit::new()
            .with_actor(rng.random())
            .enable_audit_mode()
            .unwrap();
        doc2.put(&ROOT, "k", "seed").unwrap();
        let mut ga = doc2.fork().with_actor(rng.random());
        ga.put(&ROOT, "k", ScalarValue::counter(0)).unwrap();
        let mut gb = doc2.fork().with_actor(rng.random());
        gb.put(&ROOT, "k", ScalarValue::counter(10)).unwrap();
        doc2.merge(&mut ga).unwrap();
        doc2.merge(&mut gb).unwrap(); // doc2: conflicted counter pair
        let heads2 = doc2.get_heads();
        let mut gc = doc2.fork().with_actor(rng.random());
        for i in 0..4 {
            gc.increment(&ROOT, "k", i + 1).unwrap();
            gc.commit();
        }
        let changes2 = changes_since(&mut gc, &heads2);
        assert_batch_validates(&mut doc2, changes2);
    }

    #[test]
    #[cfg_attr(
        not(feature = "deep_fuzz"),
        ignore = "deep fuzz: run with --features deep_fuzz"
    )]
    fn manifold_pending_targets_stress() {
        // fork-of-fork: a second actor updates/deletes/increments
        // elements (and writes into an object) that are themselves
        // pending in the batch
        let mut rng = make_rng();
        for _ in 0..50 {
            let mut doc = AutoCommit::new()
                .with_actor(rng.random())
                .enable_audit_mode()
                .unwrap();
            let list = doc.put_object(&ROOT, "list", ObjType::List).unwrap();
            for i in 0..4 {
                doc.insert(&list, i, i as i64).unwrap();
            }
            doc.put(&list, 1, "u1").unwrap();
            let heads = doc.get_heads();

            let mut src = doc.fork().with_actor(rng.random());
            for _ in 0..4 {
                let mut f1 = doc.fork().with_actor(rng.random());
                let at = rng.random_range(0..=f1.length(&list) as u32) as usize;
                f1.insert(&list, at, 100i64).unwrap();
                let at2 = rng.random_range(0..=f1.length(&list) as u32) as usize;
                f1.insert(&list, at2, ScalarValue::counter(5)).unwrap();
                let at3 = rng.random_range(0..=f1.length(&list) as u32) as usize;
                let nested = f1.insert_object(&list, at3, ObjType::Map).unwrap();
                f1.commit();
                let mut f2 = f1.fork().with_actor(rng.random());
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
            assert_batch_validates(&mut doc, changes);
        }
    }
}
