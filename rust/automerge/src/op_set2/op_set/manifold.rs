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

use crate::op_set2::op::DocSucc;
use crate::op_set2::types::Action;
use crate::storage::bundle::{FragMeta, FragOp, FragOps};
use crate::types::{Clock, ElemId, ObjId, ObjType, OpId};

use super::index::ObjIndex;
use super::op_iter::{ObjIdIter, OpIdIter, SuccIterIter};
use super::{OpSet, ValueIter};

use hexane::Shiftable;

use std::collections::HashMap;
use std::ops::Range;
use std::sync::atomic::{AtomicU64, Ordering::Relaxed};

/// MANIFOLD_STATS diagnostics: (calls, rows scanned) per scan site.
pub(crate) static STAT_UPD_SCAN: [AtomicU64; 2] = [AtomicU64::new(0), AtomicU64::new(0)];
pub(crate) static STAT_SLOT_WALK: [AtomicU64; 2] = [AtomicU64::new(0), AtomicU64::new(0)];
pub(crate) static STAT_SLOT_EDGE: [AtomicU64; 2] = [AtomicU64::new(0), AtomicU64::new(0)];
pub(crate) static STAT_INS_LESSER: [AtomicU64; 2] = [AtomicU64::new(0), AtomicU64::new(0)];
pub(crate) static STAT_MAP_SEEK: [AtomicU64; 2] = [AtomicU64::new(0), AtomicU64::new(0)];
pub(crate) static STAT_ADD_SUCC: [AtomicU64; 2] = [AtomicU64::new(0), AtomicU64::new(0)];
pub(crate) static STAT_RESET: [AtomicU64; 2] = [AtomicU64::new(0), AtomicU64::new(0)];
pub(crate) static STAT_JUMP: [AtomicU64; 2] = [AtomicU64::new(0), AtomicU64::new(0)];

pub fn dump_manifold_stats() {
    let d = |s: &[AtomicU64; 2]| (s[0].swap(0, Relaxed), s[1].swap(0, Relaxed));
    let (c1, r1) = d(&STAT_UPD_SCAN);
    let (c2, r2) = d(&STAT_SLOT_WALK);
    let (c3, r3) = d(&STAT_SLOT_EDGE);
    let (c4, r4) = d(&STAT_INS_LESSER);
    let (c5, r5) = d(&STAT_MAP_SEEK);
    let (c6, r6) = d(&STAT_ADD_SUCC);
    let (c7, r7) = d(&STAT_RESET);
    let (c8, r8) = d(&STAT_JUMP);
    eprintln!("MSTATS upd-elem-scan   calls {:>9} rows {:>12}", c1, r1);
    eprintln!("MSTATS ins-anchor-scan calls {:>9} rows {:>12}", c2, r2);
    eprintln!("MSTATS ins-anchor-MISS calls {:>9} rows {:>12}", c3, r3);
    eprintln!("MSTATS ins-lesser-scan calls {:>9} rows {:>12}", c4, r4);
    eprintln!("MSTATS map-key-seek    calls {:>9} rows {:>12}", c5, r5);
    eprintln!("MSTATS add-succ      applies {:>9} entries {:>9}", c6, r6);
    eprintln!("MSTATS reset-mixed    groups {:>9} rows {:>12}", c7, r7);
    eprintln!("MSTATS hint-jump       jumps {:>9} dist {:>12}", c8, r8);
}

/// What a batch resolves to.
pub(crate) struct ManifoldResult {
    /// succ entries to write into existing document rows, batched
    pub(crate) doc_succ: DocSucc,
    /// succ for targets pending in the batch itself, deferred to the
    /// consumer's change ops (unnormalized)
    pub(crate) change_succ: HashMap<OpId, Vec<(OpId, Option<i64>)>>,
    /// insert runs, in arrival order: `(pos, start..end)` means the
    /// batch ops at indexes `start..end` all land at document position
    /// `pos`. Delete ops never land, so their indexes fall in the gaps
    /// between runs — a delete-free batch applied to an empty doc is
    /// the single run `(0, 0..ops.len())`
    pub(crate) insert_runs: Vec<CopyRange>,
    /// groups whose top/text election may have changed: they contain
    /// visible rows (or fresh deletions) from BOTH the document and the
    /// batch. Each entry is the group's doc row range (pre-merge) plus
    /// the min/max batch row indexes merged into it (`None` when the
    /// batch only deleted). After the merge, re-running the last-visible
    /// election over each merged range (`OpSet::reset_top`) settles
    /// top/text — every other group's bits are straight copies
    pub(crate) mixed_groups: Vec<MixedGroup>,
}

/// One group flagged for post-merge re-election. See
/// [`ManifoldResult::mixed_groups`].
#[derive(Debug)]
pub(crate) struct MixedGroup {
    /// the group's doc rows, pre-merge
    pub(crate) doc: Range<usize>,
    /// min/max batch row merged into the group (None: deletes only)
    pub(crate) rows: Option<(usize, usize)>,
    /// the batch row of the object's last insert before this group's
    /// first row (deletes included): the head of the standalone
    /// register the group's rows fused with, needing re-election
    pub(crate) head_hint: Option<usize>,
}

/// One insert run with everything the column merge needs: the doc row
/// position (pre-merge), the fragment row range, and the fragment's
/// succ-entry and value-byte ranges for those rows — stamped while the
/// manifold streams, so the merge never queries the fragment's
/// prefix columns.
#[derive(Debug, Clone)]
pub(crate) struct CopyRange {
    pub(crate) pos: usize,
    pub(crate) range: Range<usize>,
    pub(crate) sub_range: Range<usize>,
    pub(crate) val_range: Range<usize>,
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

    insert_runs: Vec<CopyRange>,
    // index of the op the current `apply_op` call is processing —
    // the run ranges are in these units
    op_index: usize,
    // fragment succ entries / value bytes consumed through the pushed
    // ops so far — the source of each CopyRange's sub/val ranges
    frag_sub: usize,
    frag_val: usize,
    succ_cache: SuccCache,
    // the last row a slot scan stopped on: (row id, slot). Reusable by
    // any op that beats the row — except one anchored at the row
    // itself, which belongs after it (document order guarantees no
    // other anchor can sit between a memo slot and its row)
    lesser: Option<(OpId, usize)>,
    // the last resolved slot was the object end with no memo — every
    // later same-object op appends there too (tail fast path trigger)
    appending: bool,
    // the current update scope: an element and its group in the doc
    elem: Option<ElemId>,
    last_insert: Option<OpId>,
    // every doc elem row the cursor stopped at in this object
    // (update-group targets, covered insert anchors, lesser-scan
    // hits). A later covered anchor can only be *behind* the cursor
    // if it is one of these — every other row the cursor passed lies
    // in a region document order forbids later ops from targeting
    consumed: rustc_hash::FxHashSet<OpId>,
    // batch row index of `last_insert` — the head of any standalone
    // register a following headless run fused with
    last_ins_row: Option<usize>,
    elem_scope: Range<usize>,

    // mixed-group detection: one scope at a time, finalized at the
    // same boundaries the succ cache flushes
    top: TopCalc,
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
            insert_runs: vec![],
            op_index: 0,
            frag_sub: 0,
            frag_val: 0,
            succ_cache,
            lesser: None,
            appending: false,
            elem: None,
            last_insert: None,
            consumed: rustc_hash::FxHashSet::default(),
            last_ins_row: None,
            elem_scope: 0..0,
            top: TopCalc::new(),
        }
    }

    /// Record that the current op lands at document position `pos`,
    /// carrying `sub` succ entries and `val` value bytes: extend the
    /// last run when it is contiguous (same position, no delete gap),
    /// otherwise open a new one. Delete ops never push and carry
    /// neither, so consecutive runs' sub/val ranges stay contiguous.
    fn push_pos(&mut self, pos: usize, sub: usize, val: usize) {
        let i = self.op_index;
        let (sub0, val0) = (self.frag_sub, self.frag_val);
        self.frag_sub += sub;
        self.frag_val += val;
        match self.insert_runs.last_mut() {
            Some(cr) if cr.pos == pos && cr.range.end == i => {
                cr.range.end = i + 1;
                cr.sub_range.end = self.frag_sub;
                cr.val_range.end = self.frag_val;
            }
            _ => self.insert_runs.push(CopyRange {
                pos,
                range: i..i + 1,
                sub_range: sub0..self.frag_sub,
                val_range: val0..self.frag_val,
            }),
        }
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
        self.top.finalize();
    }

    /// Whether MANIFOLD_TRACE=<start>-<end> covers the current op index.
    fn traced(&self) -> bool {
        match std::env::var("MANIFOLD_TRACE") {
            Ok(v) => {
                let mut it = v.split('-').filter_map(|s| s.parse::<usize>().ok());
                match (it.next(), it.next()) {
                    (Some(a), Some(b)) => (a..=b).contains(&self.op_index),
                    _ => true,
                }
            }
            Err(_) => false,
        }
    }

    /// Drive the manifold from a fragment's columns: ops stream out of
    /// [`FragOps`] minimally decoded and in document order. When every
    /// remaining op of the current object is fragment-internal (no doc
    /// preds — visible as a zero run on the pred-count column) and the
    /// doc cursor has passed the object's last row, the tail fast path
    /// takes over: clean insert runs are consumed wholesale and the
    /// rest run in blank mode with no doc iterator work.
    pub(crate) fn apply_frag(mut self, src: &mut FragOps<'_>, meta: &FragMeta) -> ManifoldResult {
        while src.pos < src.len {
            if self.tail_ready(src) {
                self.consume_tail(src, meta);
                continue;
            }
            let op = src.next_op(meta);
            self.apply_frag_op(&op);
        }
        self.finish()
    }

    /// Push a resolved slot and note whether appends have begun: once
    /// an op lands at the object end with no memo, every later
    /// same-object op lands there too (document order), which is what
    /// arms the tail fast path.
    fn push_slot(&mut self, pos: usize, sub: usize, val: usize) {
        self.push_pos(pos, sub, val);
        self.appending = pos == self.obj_scope.end && self.lesser.is_none();
    }

    /// The tail fast path is ready when appends have begun in the
    /// current object and the remaining same-object ops carry no doc
    /// preds (document order guarantees the latter once appends begin —
    /// the pred-run peek is the cheap proof).
    fn tail_ready(&self, src: &FragOps<'_>) -> bool {
        if !self.appending || src.pos >= src.len {
            return false;
        }
        let rem = src.same_obj_run(src.len - src.pos);
        rem > 0 && src.pred_free_run() >= rem
    }

    /// Consume the rest of the current object in blank mode: every op
    /// lands at the object end, in-fragment relationships ride the succ
    /// column, and scope logic needs only the op fields. Clean insert
    /// runs (set/insert, no preds, no succ) skip per-op work entirely.
    ///
    /// The scope open at the transition may straddle into the tail:
    /// its ops keep joining it and its close is a full doc-mode flush.
    /// Every scope after that is fragment-only (blank finalize), and
    /// the trailing scope is closed here so the next object's flush
    /// starts clean.
    fn consume_tail(&mut self, src: &mut FragOps<'_>, meta: &FragMeta) {
        let end = self.obj_scope.end;
        let mut rem = src.same_obj_run(src.len - src.pos);
        let mut straddling = true;
        while rem > 0 {
            let n = src.clean_insert_run().min(rem);
            if n > 0 {
                if straddling {
                    straddling = false;
                    self.flush();
                } else {
                    self.top.finalize_blank();
                }
                // each insert is its own single-candidate scope —
                // alive and unconflicted — so only the positions
                // matter. If an update on the run's last insert
                // follows, its candidate is reconstructed there.
                let i = self.op_index;
                let (last_id, vbytes) = src.skip_clean(n);
                let (sub0, val0) = (self.frag_sub, self.frag_val);
                self.frag_val += vbytes;
                match self.insert_runs.last_mut() {
                    Some(cr) if cr.pos == end && cr.range.end == i => {
                        cr.range.end = i + n;
                        cr.sub_range.end = self.frag_sub;
                        cr.val_range.end = self.frag_val;
                    }
                    _ => self.insert_runs.push(CopyRange {
                        pos: end,
                        range: i..i + n,
                        sub_range: sub0..self.frag_sub,
                        val_range: val0..self.frag_val,
                    }),
                }
                self.op_index += n;
                self.last_insert = Some(last_id);
                self.last_ins_row = Some(self.op_index - 1);
                self.elem = None;
                rem -= n;
                continue;
            }

            let op = src.next_op(meta);
            debug_assert!(op.preds.is_empty(), "tail op carries doc preds");
            debug_assert!(op.action != Action::Delete, "tail delete has no row");
            if let Ok(obj_type) = ObjType::try_from(op.action) {
                self.obj_info.insert(
                    op.id,
                    super::index::ObjInfo {
                        parent: op.obj,
                        obj_type,
                    },
                );
            }
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
                // an update following a clean run targets the run's
                // last insert (updates follow their element directly
                // in document order) — its scope skipped candidate
                // registration, so reconstruct the insert's entry
                pending_after_run = self.elem.is_none() && self.last_insert == Some(e.0);
                self.elem = Some(e);
                fresh
            };
            if boundary {
                if straddling {
                    straddling = false;
                    self.flush();
                } else {
                    self.top.finalize_blank();
                }
            }
            if pending_after_run {
                let id = self.last_insert.unwrap();
                // the run's last insert is the previous stream index
                let head = self.last_ins_row;
                self.top.candidate_raw(
                    id,
                    self.op_index - 1,
                    head,
                    meta.counters.contains(&id),
                    true,
                );
            }
            if op.insert {
                self.last_insert = Some(op.id);
                self.last_ins_row = Some(self.op_index);
            }
            self.push_pos(end, op.sub_len, op.val_len);
            self.top.candidate(&op, self.op_index, self.last_ins_row);
            self.op_index += 1;
            rem -= 1;
        }
        // close the trailing scope and leave the top scope empty so
        // the next doc-mode flush is a no-op
        if straddling {
            self.flush();
        } else {
            self.top.finalize_blank();
        }
        self.top.open(end..end);
        self.elem = None;
    }

    /// Process one fragment op. Ops must arrive in document order (see
    /// the type-level contract).
    fn apply_frag_op(&mut self, op: &FragOp<'_>) {
        if std::env::var("BATCH_DEBUG").is_ok() {
            eprintln!(
                "mop {:?} key {:?} ins {} del {} obj {:?}",
                op.id,
                op.key,
                op.insert,
                (op.action == Action::Delete),
                op.obj
            );
        }
        if self.traced() {
            eprintln!(
                "TRACE[{}] IN  op {:?} key {:?} ins {} del {} | pos {} lesser {:?} elem {:?} elem_scope {:?} last_insert {:?}",
                self.op_index,
                op.id,
                op.key,
                op.insert,
                (op.action == Action::Delete),
                self.id_iter.pos(),
                self.lesser,
                self.elem,
                self.elem_scope,
                self.last_insert,
            );
        }
        if let Ok(obj_type) = ObjType::try_from(op.action) {
            self.obj_info.insert(
                op.id,
                super::index::ObjInfo {
                    parent: op.obj,
                    obj_type,
                },
            );
        }
        if op.obj != self.obj {
            self.flush();

            self.obj = op.obj;
            self.obj_scope = self.obj_id_iter.seek_to_value(self.obj);
            self.obj_type = self.obj_info.object_type(&self.obj).unwrap();

            self.id_iter.shift(self.obj_scope.clone());
            self.pred_iter.shift(self.obj_scope.clone());
            self.elem = None;
            self.lesser = None;
            self.consumed.clear();
            self.last_ins_row = None;
            self.appending = false;

            if self.obj_type == ObjType::Map {
                self.key_iter.shift(self.obj_scope.clone());
                self.key = None;
            }
        }
        if self.obj_type == ObjType::Map {
            if self.key.as_deref() != op.key.key_str() {
                self.key = op.key.key_str().map(str::to_owned);
                let key_scope = self.key_iter.seek_to_value(self.key.as_deref(), ..);
                if self.traced() {
                    eprintln!(
                        "TRACE[{}]   key {:?} scope {:?} id_iter (a_pos, a_max, c_pos, c_max) {:?}",
                        self.op_index,
                        self.key,
                        key_scope,
                        self.id_iter.debug_state()
                    );
                }

                self.flush();
                self.top.open(key_scope.clone());

                self.id_iter.shift(key_scope.clone());
                self.pred_iter.shift(key_scope.clone());
                self.succ_iter.shift(key_scope);
            }
            let inc = op.inc;
            for pred_id in &op.preds {
                self.succ_cache.push(*pred_id, op.id, inc);
                if !self.clock.covers(pred_id) {
                    self.top.kill(pred_id, inc);
                }
            }
            if !(op.action == Action::Delete) {
                if std::env::var("BATCH_DEBUG").is_ok() {
                    eprintln!("  map-seek {:?} id_iter pos {}", op.id, self.id_iter.pos());
                }
                let before = self.id_iter.pos();
                let r = self.id_iter.seek_to_value(&op.id);
                STAT_MAP_SEEK[0].fetch_add(1, Relaxed);
                STAT_MAP_SEEK[1].fetch_add((r.start.saturating_sub(before)) as u64, Relaxed);
                assert!(r.is_empty());
                self.push_slot(r.start, op.sub_len, op.val_len);
                self.top.candidate(op, self.op_index, self.last_ins_row);
            } else {
                self.top.note_del(self.op_index, self.last_ins_row);
            }
        } else if op.insert {
            let e = op.key.elemid().unwrap();
            self.last_insert = Some(op.id);
            self.last_ins_row = Some(self.op_index);

            // every insert starts a new element group: close the
            // previous scope (its updates all precede this op in
            // document order) and open one with no doc rows
            self.flush();
            // slot is irrelevant: an insert's scope never has doc rows
            self.top.candidate(op, self.op_index, self.last_ins_row);
            self.elem = None;

            if !e.is_head() && self.clock.covers(&e.0) {
                // an update group may have narrowed the window; the
                // anchor can live anywhere ahead in the object
                self.id_iter.set_max(self.obj_scope.end);
                // BEHIND CHECK: the anchor row is behind the cursor
                // exactly when the cursor already consumed it, and
                // every row the cursor stopped at is in `consumed`.
                // For those, the slot search continues from the
                // cursor (doc order: slots never go backward) — no
                // scan. Claiming "behind" from anything but a known
                // consumed row would corrupt the slot, so anything
                // else scans forward — cheap when the anchor is ahead
                // (slab min/max pruning jumps the scan), a full-window
                // walk only for an anchor consumed by a scan that
                // never stopped on it
                if !self.consumed.contains(&e.0) {
                    let start = self.id_iter.pos();
                    // HINT JUMP: the fragment's covered-rank floor for
                    // the anchor row — sound in any doc holding the
                    // fragment's deps, exact on a chain. A miss after
                    // a jump is re-checked from the pre-jump cursor
                    // before it may mean anything
                    let mut jumped = false;
                    if let Some(h) = op.hint {
                        let h = h as usize;
                        if h > start && h < self.obj_scope.end {
                            self.id_iter.advance_to(h);
                            STAT_JUMP[0].fetch_add(1, Relaxed);
                            STAT_JUMP[1].fetch_add((h - start) as u64, Relaxed);
                            jumped = true;
                        }
                    }
                    let mut found = self.id_iter.scan_to_value(&e.0).is_some();

                    if !found && jumped {
                        // suspect hint (receiver diverged from the
                        // fragment's causal past): rescan the skipped
                        // region
                        self.id_iter = self.op_set.id_iter_range(&(start..self.obj_scope.end));
                        found = self.id_iter.scan_to_value(&e.0).is_some();
                    }
                    if found {
                        STAT_SLOT_WALK[0].fetch_add(1, Relaxed);
                        STAT_SLOT_WALK[1].fetch_add((self.id_iter.pos() - start) as u64, Relaxed);
                        self.consumed.insert(e.0);
                        // the anchor sits past the memo slot, so the
                        // memo cannot apply any more
                        self.lesser = None;
                    } else {
                        // a consumed anchor the set never saw (its row
                        // streamed past mid-scan): the memo/hop below
                        // still resolves the slot, this was just the
                        // expensive way to learn "behind"
                        STAT_SLOT_EDGE[0].fetch_add(1, Relaxed);
                        STAT_SLOT_EDGE[1].fetch_add((self.obj_scope.end - start) as u64, Relaxed);
                        self.id_iter = self.op_set.id_iter_range(&(start..self.obj_scope.end));
                    }
                }
            }
            if matches!(self.lesser, Some((row_id, _)) if row_id < op.id && row_id != e.0) {
                // memo hit: same slot, and crucially no iterator moves —
                // a hop here would drag id_iter/ins past rows the next
                // update group or sibling still needs
                let (_, found) = self.lesser.unwrap();
                self.push_slot(found, op.sub_len, op.val_len);
            } else {
                // an insert only ever lands immediately before another
                // insert row (or the object end): hop past any update
                // rows before the slot scan. Bounded to the object: an
                // unbounded miss parks `ins` at the column end and a hit
                // can land in (and consume) the next object's first
                // insert row.
                // A previous update group may have narrowed the window
                // (the covered-anchor branch re-widens, but head- and
                // pending-anchored inserts arrive here directly): the
                // slot scan must see the whole object, or an empty
                // window yields a bogus end-of-object slot while the
                // hop below drags `ins` ahead of the pinned cursor
                self.id_iter.set_max(self.obj_scope.end);
                let cur = self.id_iter.pos();
                self.ins.shift(cur..self.obj_scope.end);
                let epos = self.ins.scan_to_value(true).unwrap_or(self.obj_scope.end);
                self.id_iter.advance_to(epos);
                STAT_INS_LESSER[0].fetch_add(1, Relaxed);
                let lesser_from = self.id_iter.pos();
                let hit = self.id_iter.scan_to_lesser(op.id);
                STAT_INS_LESSER[1].fetch_add(
                    (self.id_iter.pos().saturating_sub(lesser_from)) as u64,
                    Relaxed,
                );
                if let Some((found, row_id)) = hit {
                    self.push_slot(found, op.sub_len, op.val_len);
                    self.lesser = Some((row_id, found));
                    // the scan stopped on (and consumed) this row — a
                    // later op may anchor at it
                    self.consumed.insert(row_id);
                } else {
                    self.push_slot(self.obj_scope.end, op.sub_len, op.val_len);
                    self.lesser = None;
                }
            }
        } else {
            // non-insert seq ops (set/delete/increment): the group
            // analog of the map path — scope to the element's run of
            // updates, sized via the insert column
            let e = op.key.elemid().unwrap();
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
                    let p = self.insert_runs.last().unwrap().pos;
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
                            let start = self.id_iter.pos();
                            // HINT JUMP (see the insert arm)
                            let mut jumped = false;
                            if let Some(h) = op.hint {
                                let h = h as usize;
                                if h > start && h < self.obj_scope.end {
                                    self.id_iter.advance_to(h);
                                    STAT_JUMP[0].fetch_add(1, Relaxed);
                                    STAT_JUMP[1].fetch_add((h - start) as u64, Relaxed);
                                    jumped = true;
                                }
                            }
                            let mut found = self.id_iter.scan_to_value(&e.0);

                            if found.is_none() && jumped {
                                self.id_iter =
                                    self.op_set.id_iter_range(&(start..self.obj_scope.end));
                                found = self.id_iter.scan_to_value(&e.0);
                            }
                            STAT_UPD_SCAN[0].fetch_add(1, Relaxed);
                            STAT_UPD_SCAN[1].fetch_add(
                                (self.id_iter.pos().saturating_sub(start)) as u64,
                                Relaxed,
                            );
                            match found {
                                Some(p) => p,
                                None => panic!(
                                    "update target {:?} not found: op {:?} action {:?} pred {:?} id_iter pos {} obj_scope {:?} elem_scope {:?} lesser {:?} last_insert {:?}",
                                    e, op.id, op.action, op.preds, self.id_iter.pos(), self.obj_scope, self.elem_scope, self.lesser, self.last_insert
                                ),
                            }
                        }
                    };
                    // this group's element row is an insert row between
                    // any older memo slot and the cursor: the memo's
                    // reuse invariant is gone
                    self.lesser = None;
                    self.consumed.insert(e.0);
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
            let inc = op.inc;
            for pred_id in &op.preds {
                self.succ_cache.push(*pred_id, op.id, inc);
                if !self.clock.covers(pred_id) {
                    self.top.kill(pred_id, inc);
                }
            }
            if !(op.action == Action::Delete) {
                if self.elem_scope.is_empty() {
                    // pending target: rides along at its insert's slot
                    self.push_slot(self.elem_scope.start, op.sub_len, op.val_len);
                    self.top.candidate(op, self.op_index, self.last_ins_row);
                } else {
                    let r = self.id_iter.seek_to_value(&op.id);
                    assert!(r.is_empty());
                    self.push_slot(r.start, op.sub_len, op.val_len);
                    self.top.candidate(op, self.op_index, self.last_ins_row);
                }
            } else {
                self.top.note_del(self.op_index, self.last_ins_row);
            }
        }
        if self.traced() {
            eprintln!(
                "TRACE[{}] OUT slot {:?} | pos {} lesser {:?} elem_scope {:?}",
                self.op_index,
                self.insert_runs.last(),
                self.id_iter.pos(),
                self.lesser,
                self.elem_scope,
            );
        }
        self.op_index += 1;
    }

    /// Flush the trailing scope and hand back the batch resolution:
    /// document succ inserts, deferred change-op succ, and the insert
    /// runs (in arrival order).
    pub(crate) fn finish(mut self) -> ManifoldResult {
        self.flush();

        ManifoldResult {
            doc_succ: self.succ_cache.doc_succ,
            change_succ: self.succ_cache.change_succ,
            insert_runs: self.insert_runs,
            mixed_groups: self.top.mixed_groups,
        }
    }
}

/// Per-scope mixed-group detection.
///
/// The load-bearing invariant: within a key/element group the `top`
/// bit is true exactly on the max-id **visible** row, and the merged
/// `visible` column is already correct everywhere (batch rows carry
/// their final succ; `add_succ` maintains the doc rows). So top/text
/// need no decisions here at all — they are a pure function of
/// `visible` within the group. This tracker only *detects* the groups
/// whose election may have changed: ones with doc rows AND either an
/// alive batch row landing in them or a doc row deleted by the batch.
/// The consumer re-runs the last-visible election over each such
/// group's merged range (`OpSet::reset_top`); every other group's
/// index bits are straight copies (fragment-standalone or untouched
/// doc).
struct TopCalc {
    enabled: bool,
    // current scope: doc row range (empty for insert/pending scopes)
    doc_scope: Range<usize>,
    // visible batch ops in arrival (= ascending id) order
    batch: Vec<BatchTop>,
    // min/max batch row index merged into this scope (all non-delete
    // ops, including increments — they extend the merged range)
    rows: Option<(usize, usize)>,
    // first batch row of any kind in this scope — delete rows count
    // (they exist in the fragment columns even though they never merge)
    first_row: Option<usize>,
    // the last insert row before `first_row` (same object): the head
    // of the standalone register this scope's rows fused with
    head_hint: Option<usize>,
    // the succ flush gave a doc row in this scope an inc=None succ —
    // its visibility flipped, so the election may have moved
    saw_delete: bool,

    mixed_groups: Vec<MixedGroup>,
}

struct BatchTop {
    id: OpId,
    is_counter: bool,
    alive: bool,
}

impl TopCalc {
    fn new() -> Self {
        TopCalc {
            enabled: std::env::var("MANIFOLD_NO_TOP").is_err(),
            doc_scope: 0..0,
            batch: vec![],
            rows: None,
            first_row: None,
            head_hint: None,
            saw_delete: false,
            mixed_groups: vec![],
        }
    }

    fn open(&mut self, doc_scope: Range<usize>) {
        debug_assert!(self.batch.is_empty() && !self.saw_delete);
        self.doc_scope = doc_scope;
    }

    /// Kill the in-scope batch target `pred` overwrites, if any.
    /// Callers pre-filter with `clock.covers`: a covered pred is a doc
    /// row and can never be in the batch list, so the common case
    /// never walks it.
    fn kill(&mut self, pred: &OpId, inc: Option<i64>) {
        if !self.enabled {
            return;
        }

        if let Some(b) = self.batch.iter_mut().find(|b| b.id == *pred) {
            // same normalization flush applies to doc targets: an
            // increment only preserves a counter
            if inc.is_none() || !b.is_counter {
                b.alive = false;
            }
        }
    }

    /// [`Self::candidate`] from explicit fields, for candidates whose
    /// op was consumed by a run skip.
    /// Extend the scope's merged-row span with a batch row index.
    /// `head` is the batch row of the object's most recent insert — the
    /// standalone register a headless run fused with.
    fn note_row(&mut self, index: usize, head: Option<usize>) {
        self.rows = Some(match self.rows {
            Some((lo, hi)) => (lo.min(index), hi.max(index)),
            None => (index, index),
        });
        self.note_del(index, head);
    }

    /// Note a batch row that joins the scope without merging (a delete
    /// row): it still occupies a fragment row, and the fragment's
    /// standalone index can fuse it into the preceding register.
    fn note_del(&mut self, index: usize, head: Option<usize>) {
        if !self.enabled {
            return;
        }
        if self.first_row.is_none() {
            self.head_hint = head.filter(|&h| h < index);
        }
        self.first_row = Some(match self.first_row {
            Some(f) => f.min(index),
            None => index,
        });
    }

    /// [`Self::candidate`] from explicit fields, for candidates whose
    /// op was consumed by a run skip.
    fn candidate_raw(
        &mut self,
        id: OpId,
        index: usize,
        head: Option<usize>,
        is_counter: bool,
        alive: bool,
    ) {
        if !self.enabled {
            return;
        }
        self.note_row(index, head);
        self.batch.push(BatchTop {
            id,
            is_counter,
            alive,
        });
    }

    /// Register a batch op in the current scope: its row extends the
    /// merged range, and — unless it is an increment — it competes for
    /// top with aliveness from its own (normalized) in-fragment succ.
    fn candidate(&mut self, op: &FragOp<'_>, index: usize, head: Option<usize>) {
        if !self.enabled {
            return;
        }

        self.note_row(index, head);
        if op.action != Action::Increment {
            self.batch.push(BatchTop {
                id: op.id,
                is_counter: op.is_counter,
                alive: op.alive,
            });
        }
    }

    /// Called by the succ flush for every covered pred resolved with
    /// `inc = None` — the rows `add_succ` will clear.
    fn note_deleted(&mut self, _pos: usize) {
        if !self.enabled {
            return;
        }

        self.saw_delete = true;
    }

    /// [`finalize`](Self::finalize) for scopes living entirely in the
    /// fragment (the tail's blank mode): nothing to emit — the merged-in
    /// index bits were built standalone over exactly this group, and
    /// no delete row can be present (delete rows carry doc preds, which
    /// keep their object off the tail path), so they are already final.
    fn finalize_blank(&mut self) {
        self.batch.clear();
        self.rows = None;
        self.first_row = None;
        self.head_hint = None;
    }

    /// Close the scope: decide the group winner and emit adjustments.
    /// Close the scope: record it as a mixed group when its top/text
    /// election may have changed — it has doc rows AND either an alive
    /// batch row joined it or the batch deleted one of its rows.
    /// Groups with no doc rows keep their fragment-standalone bits (a
    /// poisoning delete row would need a doc pred in the group, and
    /// there are no doc rows to target); untouched doc groups keep
    /// theirs.
    fn finalize(&mut self) {
        if !self.enabled {
            return;
        }

        let alive = self.batch.iter().any(|b| b.alive);
        if !self.doc_scope.is_empty() && (alive || self.saw_delete) {
            self.mixed_groups.push(MixedGroup {
                doc: self.doc_scope.clone(),
                rows: self.rows,
                head_hint: self.head_hint,
            });
        }

        self.batch.clear();
        self.rows = None;
        self.first_row = None;
        self.head_hint = None;
        self.saw_delete = false;
        self.doc_scope = 0..0;
    }
}

struct SuccCache {
    clock: Clock,
    preds: Vec<(OpId, OpId, Option<i64>)>,
    change_succ: HashMap<OpId, Vec<(OpId, Option<i64>)>>,
    doc_succ: DocSucc,
}

impl SuccCache {
    fn new(clock: Clock) -> Self {
        SuccCache {
            clock,
            preds: vec![],
            doc_succ: DocSucc::default(),
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
                if std::env::var("BATCH_DEBUG").is_ok() {
                    eprintln!("  flush-seek {:?} pred_iter pos {}", pred, pred_iter.pos());
                }
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
    use crate::read::ReadDoc;
    use crate::transaction::Transactable;
    use crate::{make_rng, AutoCommit, Change, ObjType, ScalarValue, ROOT};
    use rand::prelude::*;

    /// Apply the changes through the (single) batch pipeline on a fork
    /// and deep-validate the result: the incrementally-maintained
    /// indexes must match a from-scratch rebuild, and the op columns
    /// must reproduce the history hash-for-hash.
    fn assert_batch_validates(doc: &mut AutoCommit, changes: Vec<Change>) {
        let mut d = doc.fork();
        d.doc.apply_changes_batch(changes).unwrap();
        d.doc.validate_document();
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
        assert_batch_validates(&mut doc, changes);
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
        assert_batch_validates(&mut doc, changes);
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
        assert_batch_validates(&mut doc, changes);
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
        assert_batch_validates(&mut doc, changes);
    }

    /// Regression for the A2 shape: update groups narrow `id_iter` to
    /// their elem scope, then a head-anchored insert chain arrives —
    /// head (and pending) anchors skip the covered branch that
    /// re-widens the window, so the miss path must re-widen itself or
    /// the slot scan sees an empty window, emits a bogus end-of-object
    /// slot, and every degenerate hop drags the shared `ins` cursor
    /// ahead of the pinned `id_iter` — corrupting the next
    /// covered-anchor insert's slot and stranding the delete after it.
    #[test]
    fn manifold_head_insert_after_update_groups() {
        let actor = |b: u8| crate::ActorId::from(vec![b]);
        let mut doc = AutoCommit::new().with_actor(actor(1)).unwrap();
        let text = doc.put_object(&ROOT, "text", ObjType::Text).unwrap();
        doc.splice_text(&text, 0, 0, "ab").unwrap();

        // concurrent head typing by the LARGER actor: its subtree
        // leads the list once merged
        let mut f_c = doc.fork().with_actor(actor(3)).unwrap();
        f_c.splice_text(&text, 0, 0, "cd").unwrap();

        // concurrent head typing by the SMALLER actor (lands after
        // actor 3's subtree), plus an insert after 'a' and a delete of
        // 'b' — the covered-anchor ops downstream of the head chain
        let mut f_b = doc.fork().with_actor(actor(2)).unwrap();
        f_b.splice_text(&text, 0, 0, "hij").unwrap();
        f_b.splice_text(&text, 4, 0, "k").unwrap();
        f_b.splice_text(&text, 5, 1, "").unwrap();

        // deletes of c,d are causally after f_c: in the batch they are
        // update groups on doc rows, resolved first in document order
        let mut src = doc.fork().with_actor(actor(4)).unwrap();
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
            assert_batch_validates(&mut doc, changes);
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
        assert_batch_validates(&mut doc, changes);
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
        assert_batch_validates(&mut doc, changes);
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
        assert_batch_validates(&mut doc, changes);
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
        assert_batch_validates(&mut doc, changes);
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
            assert_batch_validates(&mut doc, changes);
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
        assert_batch_validates(&mut doc, changes);
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
        assert_batch_validates(&mut doc, changes);
    }

    #[test]
    fn manifold_concurrent_counters_incremented() {
        // two forks each put a counter at the same key, increment
        // several times, then everything merges: increments seeing
        // both counters carry multiple preds, exercising the flush
        // memo's (pred, succ) ordering
        let mut rng = make_rng();
        let mut doc = AutoCommit::new().with_actor(rng.random()).unwrap();
        doc.put(&ROOT, "k", "seed").unwrap();
        let heads = doc.get_heads();

        let mut fa = doc.fork().with_actor(rng.random()).unwrap();
        fa.put(&ROOT, "k", ScalarValue::counter(0)).unwrap();
        fa.commit();
        for i in 0..4 {
            fa.increment(&ROOT, "k", i + 1).unwrap();
            fa.commit();
        }
        let mut fb = doc.fork().with_actor(rng.random()).unwrap();
        fb.put(&ROOT, "k", ScalarValue::counter(10)).unwrap();
        fb.commit();
        for i in 0..3 {
            fb.increment(&ROOT, "k", 10 * (i + 1)).unwrap();
            fb.commit();
        }
        // fc sees BOTH counters: its increments list both as preds
        let mut fc = fa.fork().with_actor(rng.random()).unwrap();
        fc.merge(&mut fb).unwrap();
        for i in 0..3 {
            fc.increment(&ROOT, "k", 100 * (i + 1)).unwrap();
            fc.commit();
        }

        let mut src = doc.fork().with_actor(rng.random()).unwrap();
        src.merge(&mut fa).unwrap();
        src.merge(&mut fb).unwrap();
        src.merge(&mut fc).unwrap();
        let changes = changes_since(&mut src, &heads);
        assert_batch_validates(&mut doc, changes);

        // same shape but the counters are already IN the doc when the
        // multi-pred increments arrive (covered preds -> flush memo)
        let mut doc2 = AutoCommit::new().with_actor(rng.random()).unwrap();
        doc2.put(&ROOT, "k", "seed").unwrap();
        let mut ga = doc2.fork().with_actor(rng.random()).unwrap();
        ga.put(&ROOT, "k", ScalarValue::counter(0)).unwrap();
        let mut gb = doc2.fork().with_actor(rng.random()).unwrap();
        gb.put(&ROOT, "k", ScalarValue::counter(10)).unwrap();
        doc2.merge(&mut ga).unwrap();
        doc2.merge(&mut gb).unwrap(); // doc2: conflicted counter pair
        let heads2 = doc2.get_heads();
        let mut gc = doc2.fork().with_actor(rng.random()).unwrap();
        for i in 0..4 {
            gc.increment(&ROOT, "k", i + 1).unwrap();
            gc.commit();
        }
        let changes2 = changes_since(&mut gc, &heads2);
        assert_batch_validates(&mut doc2, changes2);
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
            assert_batch_validates(&mut doc, changes);
        }
    }
}
