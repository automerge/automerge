use crate::change_queue::ChangeBatch;
use crate::op_set2::types::{KeyRef, ScalarValue as OpScalarValue};
use crate::types::{ActorId, Clock, ElemId, ObjId, OpId, SmallHashMap};
use crate::AutomergeError;
use crate::{Automerge, Change, ChangeHash, PatchLog, PatchLogMismatch};

use super::super::op::{ChangeOp, OpBuilder};
use super::super::op_set::{ObjIdIter, OpSet};

use std::borrow::Cow;
use std::cmp::Ordering;
use std::collections::{HashMap, HashSet};
use std::ops::Range;

pub(crate) type PredCache = SmallHashMap<OpId, Vec<(OpId, Option<i64>)>>;

#[derive(Debug, Clone, Default)]
pub(crate) struct BatchApply {
    pub(crate) ops: Vec<ChangeOp>,
    pub(crate) changes: Vec<Change>,
    actor_seq: HashMap<ActorId, HashSet<u64>>,
    hashes: HashSet<ChangeHash>,
}

/// The batch sort order: objects ascending, keys/elemids within an
/// object, ids within a group — document order for maps; sequences
/// still need untangling afterward.
pub(super) fn doc_order_cmp(a: &ChangeOp, b: &ChangeOp) -> Ordering {
    a.bld.obj.cmp(&b.bld.obj).then_with(|| {
        match a.elemid_or_key().partial_cmp(&b.elemid_or_key()) {
            Some(Ordering::Equal) | None => a.bld.id.cmp(&b.bld.id),
            Some(order) => order,
        }
    })
}

/// [`untangle_order`] over a slice of indexes into `ops`: the ops stay
/// put and the `u32` indexes are permuted instead.
pub(super) fn untangle_order_idx(
    ops: &[ChangeOp],
    idxs: &mut [u32],
    mut ids: crate::op_set2::op_set::OpIdIter<'_>,
    mut inserts: hexane::Iter<'_, bool>,
) {
    let mut ut = UntangleLiteIdx::new(ops, idxs);
    for id in ids.by_ref() {
        let insert = inserts.next().expect("insert column length matches ids");
        ut.element_update(insert, id);
        if insert {
            ut.untangle_inserts(id);
        }
    }
    ut.finish();
}

/// [`UntangleLite`] operating on span positions of an index slice: all
/// op lookups indirect through `idxs`, and `finish` permutes the
/// indexes rather than the ops.
struct UntangleLiteIdx<'a> {
    ops: &'a [ChangeOp],
    idxs: &'a mut [u32],
    order: Vec<u32>,
    count: u32,
    entry: SmallHashMap<OpId, Vec<usize>>,
    stack: Vec<usize>,
    gosub: SmallHashMap<usize, Vec<usize>>,
    updates: SmallHashMap<ElemId, Vec<usize>>,
    updates_stack: Vec<usize>,
}

impl<'a> UntangleLiteIdx<'a> {
    fn op(&self, pos: usize) -> &ChangeOp {
        &self.ops[self.idxs[pos] as usize]
    }

    fn new(ops: &'a [ChangeOp], idxs: &'a mut [u32]) -> Self {
        let mut e_to_i = SmallHashMap::default();
        let mut gosub: SmallHashMap<usize, Vec<usize>> = HashMap::default();
        let mut entry: SmallHashMap<OpId, Vec<usize>> = HashMap::default();
        let mut stack: Vec<usize> = Vec::with_capacity(idxs.len());
        let mut updates: SmallHashMap<ElemId, Vec<usize>> = HashMap::default();
        let mut last_e = None;
        for (i, &ix) in idxs.iter().enumerate() {
            let op = &ops[ix as usize];
            if let KeyRef::Seq(e) = op.key() {
                if op.insert() {
                    if let Some(j) = e_to_i.get(e) {
                        gosub.entry(*j).or_default().push(i);
                    } else if e.is_head() {
                        stack.push(i);
                    } else {
                        entry.entry(e.0).or_default().push(i);
                    }
                    let this_e = ElemId(op.id());
                    e_to_i.insert(this_e, i);
                    last_e = Some(this_e);
                } else if last_e != Some(*e) {
                    updates.entry(*e).or_default().push(i);
                }
            }
        }
        let order = vec![u32::MAX; idxs.len()];
        Self {
            ops,
            idxs,
            order,
            count: 0,
            entry,
            stack,
            gosub,
            updates,
            updates_stack: vec![],
        }
    }

    fn emit(&mut self, i: usize) {
        debug_assert_eq!(self.order[i], u32::MAX);
        self.order[i] = self.count;
        self.count += 1;
    }

    fn element_update(&mut self, doc_insert: bool, doc_id: OpId) {
        while let Some(last) = self.updates_stack.last().copied() {
            if doc_insert || doc_id > self.op(last).id() {
                self.updates_stack.pop();
                self.emit(last);
            } else {
                break;
            }
        }
    }

    fn untangle_inserts(&mut self, id: OpId) {
        if let Err(n) = self.stack.binary_search_by(|n| self.op(*n).id().cmp(&id)) {
            while self.stack.len() > n {
                self.untangle_inner();
            }
        }
        if let Some(v) = self.entry.remove(&id) {
            self.stack.extend(v);
        }
        if let Some(u) = self.updates.get(&ElemId(id)) {
            self.updates_stack.extend(u.iter().rev());
        }
    }

    fn untangle_inner(&mut self) -> Option<()> {
        let pos = self.stack.pop()?;
        let key = KeyRef::Seq(ElemId(self.op(pos).id()));
        self.emit(pos);
        if let Some(v) = self.gosub.remove(&pos) {
            self.stack.extend(v);
        }
        // the op's own updates follow it contiguously in sorted order
        for i in (pos + 1)..self.idxs.len() {
            if self.op(i).insert() || self.op(i).key() != &key {
                break;
            }
            self.emit(i);
        }
        Some(())
    }

    fn finish(mut self) {
        for i in std::mem::take(&mut self.updates_stack).into_iter().rev() {
            self.emit(i);
        }
        while !self.stack.is_empty() {
            self.untangle_inner();
        }
        debug_assert!(self.order.iter().all(|&o| o != u32::MAX));
        // apply the permutation to the index slice by cycles
        let mut order = std::mem::take(&mut self.order);
        for i in 0..order.len() {
            while order[i] as usize != i {
                let j = order[i] as usize;
                self.idxs.swap(i, j);
                order.swap(i, j);
            }
        }
    }
}

#[derive(Debug, Clone)]
pub(crate) enum Adjust {
    Conflict(usize),
    Expose(usize),
}

/// Increment operations preserve and update counter predecessors, but
/// act as ordinary overwrites for non-counter predecessors.
pub(crate) fn normalize_increment_successors(
    is_counter: bool,
    successors: &mut [(OpId, Option<i64>)],
) {
    if !is_counter {
        for (_, increment) in successors {
            if increment.is_some() {
                *increment = None;
            }
        }
    }
}

impl BatchApply {
    pub(crate) fn push(&mut self, c: Change) {
        assert!(!self.has_actor_seq(&c));
        self.record_actor_seq(&c);

        assert!(!self.hashes.contains(&c.hash()));
        self.hashes.insert(c.hash());

        self.changes.push(c);
    }

    fn record_actor_seq(&mut self, c: &Change) {
        if let Some(set) = self.actor_seq.get_mut(c.actor_id()) {
            set.insert(c.seq());
        } else {
            self.actor_seq
                .insert(c.actor_id().clone(), HashSet::from([c.seq()]));
        }
    }

    fn has_actor_seq(&self, c: &Change) -> bool {
        self.actor_seq
            .get(c.actor_id())
            .map(|set| set.contains(&c.seq()))
            .unwrap_or(false)
    }

    pub(crate) fn insert_new_actors(&mut self, doc: &mut Automerge) {
        for c in self.changes.iter().filter(|c| c.seq() == 1) {
            doc.put_actor_ref(c.actor_id());
        }
    }

    /// Apply the batch: convert the v1 changes into the v2 succ-format
    /// columns and run the v2 pipeline. Patches for an active log are
    /// produced by diffing the document across the apply.
    pub(crate) fn apply(
        &mut self,
        doc: &mut Automerge,
        log: &mut PatchLog,
    ) -> Result<(), PatchLogMismatch> {
        let before = log.is_active().then(|| doc.get_heads());
        self.apply_v2(doc, log)?;
        if let Some(before) = before {
            doc.log_diff(&before, log);
        }
        Ok(())
    }

    /// Normalize to the succ-carrying shape fragment bundles arrive in:
    /// preds targeting ops in this batch become succ entries on their
    /// targets (sorted, increments normalized) and ops keep only
    /// doc-row preds. Deletes left with no preds carry nothing beyond
    /// the succ already stamped — callers drop them from the stream.
    fn stamp_succ(&mut self, clock: &Clock) {
        let mut succ_map = PredCache::default();
        for op in self.ops.iter_mut() {
            let id = op.id();
            let inc = op.get_increment_value();
            op.bld.pred.retain(|p| {
                if clock.covers(p) {
                    true
                } else {
                    succ_map.entry(*p).or_default().push((id, inc));
                    false
                }
            });
        }
        for op in self.ops.iter_mut() {
            if let Some(mut succ) = succ_map.remove(&op.id()) {
                succ.sort_unstable_by_key(|(id, _)| *id);
                let is_counter = matches!(op.bld.value, OpScalarValue::Counter(_));
                normalize_increment_successors(is_counter, &mut succ);
                op.succ = succ;
            }
        }
        debug_assert!(succ_map.is_empty(), "succ target missing from batch");
    }

    /// EXPERIMENT (`BATCH_V2=1`): convert the v1 batch into the v2
    /// succ-format columns as early as possible and continue on the v2
    /// code path. The ops vec is never sorted or compacted — a `u32`
    /// index vec is filtered, sorted and untangled instead, then the
    /// ops are encoded into bundle columns in index order and handed to
    /// [`FragmentApply`], which decodes and applies them exactly like a
    /// received fragment. Measures the conversion tax of making the
    /// compressed columns canonical.
    fn apply_v2(
        &mut self,
        doc: &mut Automerge,
        log: &mut PatchLog,
    ) -> Result<(), PatchLogMismatch> {
        let timing = std::env::var("BATCH_TIMING").is_ok();
        let mut t = std::time::Instant::now();
        let mut lap = |label: &str| {
            if timing {
                eprintln!(
                    "V2CONV {:<22} {:>9.3}ms",
                    label,
                    t.elapsed().as_secs_f64() * 1e3
                );
                t = std::time::Instant::now();
            }
        };
        self.insert_new_actors(doc);
        log.migrate_actors(&doc.ops().actors)?;
        for c in &self.changes {
            doc.import_ops_to(c, &mut self.ops).unwrap();
        }
        lap("import_ops");
        let clock = doc.change_graph.current_clock();
        doc.update_history_batch(&self.changes);
        lap("update_history");

        self.stamp_succ(&clock);
        lap("normalize succ");

        // sort and filter u32 indexes, not 200-byte ChangeOps
        let mut idxs: Vec<u32> = (0..self.ops.len() as u32)
            .filter(|&i| {
                let op = &self.ops[i as usize];
                !(op.bld.is_delete() && op.bld.pred.is_empty())
            })
            .collect();
        idxs.sort_unstable_by(|&a, &b| doc_order_cmp(&self.ops[a as usize], &self.ops[b as usize]));
        lap("index sort");

        let mut obj_info = doc.ops().obj_info.clone();
        for &i in &idxs {
            let op = &self.ops[i as usize];
            if let Some(info) = op.obj_info() {
                obj_info.insert(op.id(), info);
            }
        }
        // untangle each sequence object's span of the index vec
        let mut walker = ObjWalker::new(doc.ops());
        let mut start = 0;
        while start < idxs.len() {
            let obj = self.ops[idxs[start] as usize].bld.obj;
            let mut end = start + 1;
            while end < idxs.len() && self.ops[idxs[end] as usize].bld.obj == obj {
                end += 1;
            }
            if matches!(obj_info.object_type(&obj), Some(t) if t.is_sequence()) {
                let obj_range = walker.seek_to_obj(obj);
                untangle_order_idx(
                    &self.ops,
                    &mut idxs[start..end],
                    doc.ops().id_iter_range(&obj_range),
                    doc.ops().insert().values().iter_range(obj_range.clone()),
                );
            }
            start = end;
        }
        lap("untangle idx");

        // encode the v2 op columns in index (= document) order. This is
        // an in-process handoff: actor indexes stay the document's, so
        // the mapping is identity
        let actors_len = doc.ops().actors.len();
        let mut members: Vec<(usize, u64, u64, u64)> = self
            .changes
            .iter()
            .map(|c| {
                let actor = doc.ops().actors.binary_search(c.actor_id()).unwrap();
                (actor, c.seq(), c.start_op().get(), c.max_op())
            })
            .collect();
        members.sort_unstable_by_key(|(a, s, _, _)| (*a, *s));
        let members: Vec<(usize, u64, u64)> =
            members.into_iter().map(|(a, _, s, m)| (a, s, m)).collect();

        let mut data = Vec::new();
        let (raw, id_ctr) = {
            let mut mapper = crate::op_set2::change::ActorMapper::new(&doc.ops().actors);
            let mut writer = crate::storage::bundle::BundleOpWriter::default();
            for &i in &idxs {
                let op = &self.ops[i as usize];
                let succ_ids: Vec<OpId> = op.succ.iter().map(|s| s.0).collect();
                writer.add(&op.bld, &succ_ids, 0, &mut mapper);
            }
            mapper.mapping = (0..actors_len)
                .map(|i| Some(crate::op_set2::types::ActorIdx::from(i)))
                .collect();
            let (cols, id_ctr) = writer.finish(&mapper, &mut data, &members);
            (cols.raw_columns(), id_ctr)
        };
        lap("encode v2");

        // from here on this IS the v2 path: decode + manifold + splice
        let actor_map: Vec<usize> = (0..actors_len).collect();
        let mut frag = super::fragment::FragmentApply::from_ops(
            || crate::storage::bundle::OpIter::new(&raw, &data, &id_ctr),
            &actor_map,
            &clock,
        )
        .unwrap();
        lap("decode v2");
        frag.apply_manifold(doc, log).unwrap();
        lap("fragment apply");
        Ok(())
    }
}

#[derive(Debug, Clone)]
pub(crate) struct ObjWalker<'a> {
    iter: ObjIdIter<'a>,
}

impl<'a> ObjWalker<'a> {
    pub(crate) fn new(ops: &'a OpSet) -> Self {
        let iter = ops.obj_id_iter();
        Self { iter }
    }

    pub(crate) fn seek_to_obj(&mut self, obj: ObjId) -> Range<usize> {
        self.iter.seek_to_value(obj)
    }
}

/// Splice the manifold's insert runs into the document's columns: each
/// run is `(pos, start..end)` — the batch ops at indexes `start..end`
/// land at document position `pos`. Adjacent runs share a position only
/// when delete ops (which never splice, and are filtered by the column
/// splice) split the index range, so such runs merge into one call.
pub(super) fn splice_insert_runs(
    ops: &[ChangeOp],
    runs: &[(usize, Range<usize>)],
    doc: &mut Automerge,
) {
    let mut shift = 0;
    let mut i = 0;
    let mut calls = 0;
    while i < runs.len() {
        let (pos, range) = &runs[i];
        let mut end = range.end;
        // merging [(4, 0..1), (4, 100..101)] into one splice of
        // ops[0..101] at pos 4 is safe because indexes 1..100 can only
        // be DELETE ops: every non-delete op pushes a position, so if
        // any op in the gap were a non-delete its run would sit
        // between these two in the vec and they would not be adjacent.
        // The column splice filters `Action::Delete` from the slice,
        // so the merged call inserts exactly ops 0 and 100 — the same
        // rows two separate calls at pos 4 (with the shift in between)
        // would have produced, in the same order
        while matches!(runs.get(i + 1), Some((p, _)) if p == pos) {
            i += 1;
            end = runs[i].1.end;
        }
        shift += insert_ops(ops, doc, pos + shift, range.start..end);
        calls += 1;
        i += 1;
    }
    if std::env::var("FRAG_TIMING").is_ok() || std::env::var("BATCH_TIMING").is_ok() {
        eprintln!("SPLICE   {} runs -> {} splice calls", runs.len(), calls);
    }
}

fn insert_ops(ops: &[ChangeOp], doc: &mut Automerge, pos: usize, range: Range<usize>) -> usize {
    let batch = &ops[range];
    let start = doc.ops().len();
    doc.ops_mut().splice(pos, batch);
    doc.ops().len() - start
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
        // Add new changes, deduplicating and checking for duplicate seq numbers.
        let mut batch = ChangeBatch::new();
        for c in changes {
            let hash = c.hash();
            if self.change_graph.has_change(&hash)? {
                continue;
            }
            if self.queue.has_hash(&c.hash()) {
                continue;
            }
            if self.has_actor_seq(&c) {
                self.queue
                    .remove_actor_branch_from(c.actor_id(), c.seq().saturating_add(1));
                return Err(AutomergeError::DuplicateSeqNumber(
                    c.seq(),
                    c.actor_id().clone(),
                ));
            }
            if self.queue.has_actor_seq(&c) {
                return Err(AutomergeError::DuplicateSeqNumber(
                    c.seq(),
                    c.actor_id().clone(),
                ));
            }
            batch.push(c)?;
        }

        self.queue.extend(batch);

        if self.queue.is_empty() {
            return Ok(());
        }

        let mut chap = BatchApply::default();
        for c in self.queue.pop_topo_sorted_ready(&self.change_graph) {
            chap.push(c);
        }

        Ok(chap.apply(self, log)?)
    }

    pub(crate) fn import_ops_to(
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
    use crate::types::{ObjType, ScalarValue};
    use crate::{make_rng, ActorId, AutoCommit, ROOT};
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
    fn v1_increment_plus_child_insert_layout() {
        // v1 batch apply must keep an element's updates ahead of its
        // child inserts; a same-batch increment + insert-after on the
        // same element must not interleave
        let mut doc = AutoCommit::new()
            .with_actor("aa".try_into().unwrap())
            .unwrap();
        let list = doc.put_object(&ROOT, "list", ObjType::List).unwrap();
        doc.insert(&list, 0, ScalarValue::counter(5)).unwrap();
        let heads = doc.get_heads();

        let mut f = doc.fork().with_actor("bb".try_into().unwrap()).unwrap();
        f.increment(&list, 0, 1).unwrap();
        f.insert(&list, 1, "x").unwrap();

        doc.merge(&mut f).unwrap();
        // walking the merged doc's changes re-encounters the layout
        let _ = doc.doc.get_changes(&heads).unwrap();
    }

    #[test]
    fn batch_apply_per_merge_validates() {
        let mut rng = make_rng();
        for _ in 0..10 {
            let mut base = AutoCommit::new().with_actor(rng.random()).unwrap();
            let list = base.put_object(&ROOT, "list", ObjType::List).unwrap();
            let map = base.put_object(&ROOT, "map", ObjType::Map).unwrap();
            base.put(&map, "c", ScalarValue::counter(0)).unwrap();
            for i in 0..5 {
                base.insert(&list, i, i as i64).unwrap();
            }
            base.put(&list, 1, "u1").unwrap();

            let heads = base.get_heads();
            let mut src = base.fork().with_actor(rng.random()).unwrap();
            for _ in 0..6 {
                // concurrent: every fork branches from base, so its
                // changes land on a src that has moved on
                let mut f = base.fork().with_actor(rng.random()).unwrap();
                for _ in 0..rng.random_range(1..8u32) {
                    let len = f.length(&list);
                    match rng.random_range(0..7u32) {
                        0 => {
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
                        4 if len > 0 => {
                            let at = rng.random_range(0..len as u32) as usize;
                            f.put(&list, at, "x").unwrap();
                        }
                        5 if len > 1 => {
                            let at = rng.random_range(0..len as u32) as usize;
                            f.delete(&list, at).unwrap();
                        }
                        _ => {
                            f.commit();
                        }
                    }
                }
                let changes = changes_since(&mut f, &heads);

                src.doc.apply_changes_batch(changes).unwrap();
                src.doc.validate_document();
            }
        }
    }

    #[test]
    fn batch_apply_fuzz_validates() {
        // random concurrent batches through the one pipeline; the doc
        // must deep-validate (index rebuild + hash round-trip) after
        let mut rng = make_rng();
        for _ in 0..20 {
            let mut base = AutoCommit::new().with_actor(rng.random()).unwrap();
            let list = base.put_object(&ROOT, "list", ObjType::List).unwrap();
            let map = base.put_object(&ROOT, "map", ObjType::Map).unwrap();
            base.put(&map, "c", ScalarValue::counter(0)).unwrap();
            for i in 0..5 {
                base.insert(&list, i, i as i64).unwrap();
            }
            base.put(&list, 1, "u1").unwrap();
            let heads = base.get_heads();

            let mut src = base.fork().with_actor(rng.random()).unwrap();
            for _ in 0..6 {
                let mut f = base.fork().with_actor(rng.random()).unwrap();
                for _ in 0..rng.random_range(1..8u32) {
                    let len = f.length(&list);
                    match rng.random_range(0..7u32) {
                        0 => {
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
                        4 if len > 0 => {
                            let at = rng.random_range(0..len as u32) as usize;
                            f.put(&list, at, "x").unwrap();
                        }
                        5 if len > 1 => {
                            let at = rng.random_range(0..len as u32) as usize;
                            f.delete(&list, at).unwrap();
                        }
                        _ => {
                            f.commit();
                            let at = rng.random_range(0..=len as u32) as usize;
                            f.insert(&list, at, rng.random_range(0..100i64)).unwrap();
                        }
                    }
                }
                src.merge(&mut f).unwrap();
            }
            let changes = changes_since(&mut src, &heads);

            let mut doc = base.fork();
            doc.doc.apply_changes_batch(changes).unwrap();
            doc.doc.validate_document();
        }
    }

    fn changes_since(src: &mut AutoCommit, heads: &[crate::ChangeHash]) -> Vec<Change> {
        src.get_changes(heads).unwrap()
    }

    #[test]
    fn map_batch_apply() {
        let actor3 = ActorId::try_from("aaaaaa").unwrap();
        let actor2 = ActorId::try_from("bbbbbb").unwrap();
        let actor1 = ActorId::try_from("cccccc").unwrap();

        let mut doc1 = AutoCommit::new().with_actor(actor1).unwrap();
        let map1 = doc1.put_object(&ROOT, "map", ObjType::Map).unwrap();
        doc1.put(&map1, "key1", "val1").unwrap();
        doc1.put(&map1, "key2", "val2").unwrap();

        let heads1 = doc1.get_heads();

        let mut doc2 = doc1.fork().with_actor(actor2).unwrap();
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

        let mut doc3 = doc1.fork().with_actor(actor3).unwrap();
        doc3.put(&map1, "key1", "val3b").unwrap();
        doc3.put(&map1, "key3", "val4b").unwrap();

        let mut doc1_test = doc1.fork();
        let mut changes2 = doc2.get_changes(&heads1).unwrap();

        let changes3 = doc3.get_changes(&heads1).unwrap();
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

        let mut doc1 = AutoCommit::new().with_actor(actor1).unwrap();
        let list = doc1.put_object(&ROOT, "list", ObjType::List).unwrap();
        doc1.insert(&list, 0, "val1").unwrap();
        doc1.insert(&list, 1, "val2").unwrap();
        doc1.insert(&list, 2, "val3").unwrap();

        let heads1 = doc1.get_heads();

        let mut doc2 = doc1.fork().with_actor(actor2).unwrap();
        doc2.insert(&list, 1, "val4a").unwrap();
        doc2.insert(&list, 1, "val4b").unwrap();
        doc2.insert(&list, 2, "val4c").unwrap();
        doc2.insert(&list, 0, "val4d").unwrap();
        doc2.insert(&list, 0, "val4e").unwrap();
        doc2.insert(&list, 0, "val4f").unwrap();

        let mut doc3 = doc1.fork().with_actor(actor3).unwrap();
        doc3.insert(&list, 1, "val5a").unwrap();
        doc3.insert(&list, 1, "val5b").unwrap();
        doc3.insert(&list, 2, "val5c").unwrap();
        doc3.insert(&list, 3, "val5d").unwrap();
        doc3.insert(&list, 1, "val5e").unwrap();
        doc3.insert(&list, 1, "val5f").unwrap();
        doc3.insert(&list, 0, "val5g").unwrap();
        doc3.insert(&list, 0, "val5h").unwrap();

        let mut doc1_test = doc1.fork();
        let mut changes2 = doc2.get_changes(&heads1).unwrap();
        let changes3 = doc3.get_changes(&heads1).unwrap();
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

        let mut doc1 = AutoCommit::new().with_actor(actor1).unwrap();
        let text = doc1.put_object(&ROOT, "text", ObjType::Text).unwrap();
        doc1.splice_text(&text, 0, 0, "the quick fox jumped over the lazy dog")
            .unwrap();

        let heads1 = doc1.get_heads();

        let mut doc2 = doc1.fork().with_actor(actor2).unwrap();
        doc2.splice_text(&text, 0, 0, "abc").unwrap();

        let mut doc3 = doc1.fork().with_actor(actor3).unwrap();
        doc3.splice_text(&text, 3, 1, "aalks").unwrap();

        let mut doc1_test = doc1.fork();
        let mut changes2 = doc2.get_changes(&heads1).unwrap();
        let changes3 = doc3.get_changes(&heads1).unwrap();
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
        let mut doc1 = AutoCommit::new().with_actor(rng.random()).unwrap();
        let list = doc1.put_object(&ROOT, "list", ObjType::List).unwrap();
        doc1.insert(&list, 0, "a").unwrap();
        doc1.insert(&list, 1, "b").unwrap();
        doc1.insert(&list, 2, "c").unwrap();
        let heads = doc1.get_heads();

        let mut doc2 = doc1.fork().with_actor(rng.random()).unwrap();
        for i in 0..10 {
            let mut tmp = doc1.fork().with_actor(rng.random()).unwrap();
            tmp.put(&list, 0, i).unwrap();
            doc2.merge(&mut tmp).unwrap();
        }
        let changes = doc2.get_changes(&heads).unwrap();
        doc1.apply_changes_batch(changes).unwrap();
        doc1.validate_top_index();
        assert_eq!(doc1.save(), doc2.save());
    }

    #[test]
    fn multi_insert_batch_apply() {
        let mut rng = make_rng();
        let mut doc1 = AutoCommit::new().with_actor(rng.random()).unwrap();
        let list = doc1.put_object(&ROOT, "list", ObjType::List).unwrap();
        doc1.insert(&list, 0, "a").unwrap();
        doc1.insert(&list, 1, "b").unwrap();
        doc1.insert(&list, 2, "c").unwrap();
        let heads = doc1.get_heads();

        let mut doc2 = doc1.fork().with_actor(rng.random()).unwrap();

        for i in 0..10 {
            let mut tmp = doc1.fork().with_actor(rng.random()).unwrap();
            tmp.insert(&list, 1, i).unwrap();
            //let change = tmp.get_last_local_change().unwrap().unwrap();
            doc2.merge(&mut tmp).unwrap();
        }

        let changes = doc2.get_changes(&heads).unwrap();
        doc1.apply_changes_batch(changes).unwrap();
        doc1.validate_top_index();
        assert_eq!(doc1.save(), doc2.save());
    }

    #[test]
    fn multi_update_batch_apply() {
        let mut rng = make_rng();
        let mut doc1 = AutoCommit::new().with_actor(rng.random()).unwrap();
        let list = doc1.put_object(&ROOT, "list", ObjType::List).unwrap();
        doc1.insert(&list, 0, "a").unwrap();
        doc1.insert(&list, 1, "b").unwrap();
        doc1.insert(&list, 2, "c").unwrap();
        let heads = doc1.get_heads();

        let mut doc2 = doc1.fork().with_actor(rng.random()).unwrap();

        for i in 0..3 {
            let mut tmp = doc1.fork().with_actor(rng.random()).unwrap();
            tmp.put(&list, 2, i).unwrap();
            doc2.merge(&mut tmp).unwrap();
        }

        let changes = doc2.get_changes(&heads).unwrap();
        doc1.apply_changes_batch(changes).unwrap();
        doc1.validate_top_index();
        assert_eq!(doc1.save(), doc2.save());
    }

    #[test]
    fn fuzz_batch_list_apply() {
        let mut rng = make_rng();
        let mut doc1 = AutoCommit::new().with_actor(rng.random()).unwrap();
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

        let mut doc1_tmp = doc1.fork().with_actor(rng.random()).unwrap();
        let mut doc2 = doc1.fork().with_actor(rng.random()).unwrap();

        for _ in 0..3 {
            for _ in 0..30 {
                let mut tmp = doc1_tmp.fork().with_actor(rng.random()).unwrap();
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

        let changes = doc2.get_changes(&heads).unwrap();
        doc1.apply_changes_batch(changes).unwrap();
        doc1.validate_top_index();
        assert_eq!(doc1.save(), doc2.save());
    }

    #[test]
    fn fuzz_batch_map1_apply() {
        let mut rng = make_rng();
        let mut doc1 = AutoCommit::new().with_actor(rng.random()).unwrap();
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

        let mut doc1_tmp = doc1.fork().with_actor(rng.random()).unwrap();
        let mut doc2 = doc1.fork().with_actor(rng.random()).unwrap();

        for _ in 0..3 {
            for _ in 0..30 {
                let mut tmp = doc1_tmp.fork().with_actor(rng.random()).unwrap();
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

        let changes = doc2.get_changes(&heads).unwrap();
        doc1.apply_changes_batch(changes).unwrap();
        doc1.validate_top_index();
        assert_eq!(doc1.save(), doc2.save());
    }

    #[test]
    fn fuzz_batch_map2_apply() {
        let mut rng = make_rng();
        let mut doc1 = AutoCommit::new().with_actor(rng.random()).unwrap();
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

        let mut doc1_tmp = doc1.fork().with_actor(rng.random()).unwrap();
        let mut doc2 = doc1.fork().with_actor(rng.random()).unwrap();

        for _ in 0..3 {
            for _ in 0..30 {
                let mut tmp = doc1_tmp.fork().with_actor(rng.random()).unwrap();
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

        let changes = doc2.get_changes(&heads).unwrap();

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
        let mut doc1 = AutoCommit::new().with_actor(rng.random()).unwrap();
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

        let mut doc1_tmp = doc1.fork().with_actor(rng.random()).unwrap();
        let mut doc2 = doc1.fork().with_actor(rng.random()).unwrap();

        for _ in 0..4 {
            for _ in 0..30 {
                let mut tmp = doc1_tmp.fork().with_actor(rng.random()).unwrap();
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

        let changes = doc2.get_changes(&heads).unwrap();

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
        let mut doc1 = AutoCommit::new().with_actor(rng.random()).unwrap();
        let list1 = doc1.put_object(&ROOT, "list1", ObjType::List).unwrap();
        doc1.insert(&list1, 0, ScalarValue::counter(val())).unwrap();
        doc1.insert(&list1, 1, ScalarValue::counter(val())).unwrap();
        doc1.insert(&list1, 2, ScalarValue::counter(val())).unwrap();

        let mut doc1_copy = doc1.fork().with_actor(rng.random()).unwrap();
        let mut doc2 = doc1.fork().with_actor(rng.random()).unwrap();
        let mut doc2_copy = doc1.fork().with_actor(rng.random()).unwrap();

        let mut changes = vec![];
        //for _ in 0..3 {
        for _ in 0..2 {
            //for _ in 0..10 {
            for _ in 0..2 {
                let mut tmp = doc2.fork().with_actor(rng.random()).unwrap();
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
                let change = tmp.get_last_local_change().unwrap().unwrap();
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
        let mut doc1 = AutoCommit::new().with_actor(rng.random()).unwrap();
        let list1 = doc1.put_object(&ROOT, "list1", ObjType::List).unwrap();
        doc1.insert(&list1, 0, val()).unwrap();
        doc1.insert(&list1, 1, val()).unwrap();
        doc1.insert(&list1, 2, val()).unwrap();

        let mut doc1_copy = doc1.fork().with_actor(rng.random()).unwrap();
        let mut doc2 = doc1.fork().with_actor(rng.random()).unwrap();
        let mut doc2_copy = doc1.fork().with_actor(rng.random()).unwrap();

        let mut changes = vec![];
        for _ in 0..3 {
            for _ in 0..30 {
                let mut tmp = doc2.fork().with_actor(rng.random()).unwrap();
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
                let change = tmp.get_last_local_change().unwrap().unwrap();
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
        let mut doc1 = AutoCommit::new().with_actor(rng.random()).unwrap();
        let text1 = doc1.put_object(&ROOT, "text1", ObjType::Text).unwrap();
        doc1.splice_text(&text1, 0, 0, "--------").unwrap();

        let mut doc1_copy = doc1.fork().with_actor(rng.random()).unwrap();
        let mut doc2 = doc1.fork().with_actor(rng.random()).unwrap();
        let mut doc2_copy = doc1.fork().with_actor(rng.random()).unwrap();

        let mut changes = vec![];
        for _ in 0..10 {
            for _ in 0..5 {
                let mut tmp = doc2.fork().with_actor(rng.random()).unwrap();
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
                let change = tmp.get_last_local_change().unwrap().unwrap();
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
        let mut doc1 = AutoCommit::new().with_actor(rng.random()).unwrap();
        let text1 = doc1.put_object(&ROOT, "text1", ObjType::Text).unwrap();
        doc1.splice_text(&text1, 0, 0, "---------------------")
            .unwrap();

        let mut doc1_copy = doc1.fork().with_actor(rng.random()).unwrap();
        let mut doc2 = doc1.fork().with_actor(rng.random()).unwrap();
        let mut doc2_copy = doc1.fork().with_actor(rng.random()).unwrap();

        let mut changes = vec![];
        for _ in 0..5 {
            for _ in 0..10 {
                let mut tmp = doc2.fork().with_actor(rng.random()).unwrap();
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
                let change = tmp.get_last_local_change().unwrap().unwrap();
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
        let mut doc = AutoCommit::new().with_actor(rng.random()).unwrap();

        doc.put(&ROOT, "key1", "value1").unwrap();

        const CYCLES: u32 = 10;
        const DOCS: u32 = 5;
        const KEYS: u32 = 4;

        let mut docs = vec![];

        for _ in 0..DOCS {
            docs.push(doc.fork().with_actor(rng.random()).unwrap());
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
                .map(|d| d.get_last_local_change().unwrap().unwrap())
                .collect();

            doc.apply_changes(changes).unwrap();

            doc.validate_top_index();
        }
    }

    #[test]
    fn list_element_conflict() {
        let mut rng = make_rng();
        let mut doc = AutoCommit::new().with_actor(rng.random()).unwrap();

        let list = doc.put_object(&ROOT, "list", ObjType::List).unwrap();

        const CYCLES: u32 = 5;
        const DOCS: u32 = 6;
        const KEYS: u32 = 3;

        for i in 0..KEYS {
            doc.insert(&list, i as usize, "_").unwrap();
        }

        let mut docs = vec![];

        for _ in 0..DOCS {
            docs.push(doc.fork().with_actor(rng.random()).unwrap());
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
                .map(|d| d.get_last_local_change().unwrap().unwrap())
                .collect();

            doc.apply_changes(changes).unwrap();
            doc.validate_top_index();
        }
    }

    #[test]
    fn conflicts_with_isolate() {
        let mut rng = make_rng();
        let mut doc = AutoCommit::new().with_actor(rng.random()).unwrap();

        let list = doc.put_object(&ROOT, "list", ObjType::List).unwrap();
        let map = doc.put_object(&ROOT, "map", ObjType::Map).unwrap();
        doc.insert(&list, 0, "_").unwrap();
        doc.put(&map, "key", "_").unwrap();

        const CYCLES: u32 = 5;
        const DOCS: u32 = 6;

        let mut docs = vec![];
        let mut heads = vec![doc.get_heads()];

        for _ in 0..DOCS {
            docs.push(doc.fork().with_actor(rng.random()).unwrap());
        }

        for _ in 0..CYCLES {
            for d in &mut docs {
                let head = rng.random::<u32>() % (heads.len() as u32);
                d.isolate(&heads[head as usize]).unwrap();
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
                .map(|d| d.get_last_local_change().unwrap().unwrap())
                .collect();

            doc.apply_changes(changes).unwrap();

            heads.push(doc.get_heads());

            doc.validate_top_index();
        }
    }
}
