use crate::clock::Clock;
use crate::op_set2::types::{KeyRef, ScalarValue as OpScalarValue};
use crate::storage::bundle::Bundle;
use crate::types::OpId;
use crate::{Automerge, AutomergeError, PatchLog};

use super::super::op::{ChangeOp, OpBuilder};
use super::batch::normalize_increment_successors;

use std::borrow::Cow;

/// Applies the ops of a bundle directly, without converting the bundle
/// into [`crate::Change`]s first.
///
/// This is the fragment twin of `BatchApply`: the same walk over the
/// document's op set, but exploiting the bundle's invariants — its ops
/// are already in document order and never causally precede anything in
/// the receiving document — so no sorting or untangling is needed.
/// Where the streaming manifold reads the fragment's op columns from.
#[derive(Debug)]
pub(crate) enum FragSrc<'a> {
    /// a received bundle's columns, borrowed
    Bundle(&'a Bundle),
    /// columns encoded in-process (the batch path, and re-encoded
    /// overlap fragments)
    Owned {
        raw: crate::storage::RawColumns<crate::storage::columns::compression::Uncompressed>,
        data: Vec<u8>,
        id_ctr: Vec<i64>,
    },
}

impl FragSrc<'_> {
    fn parts(
        &self,
    ) -> (
        &crate::storage::RawColumns<crate::storage::columns::compression::Uncompressed>,
        &[u8],
        &[i64],
    ) {
        match self {
            FragSrc::Bundle(b) => (
                &b.storage.ops_meta,
                &b.storage.bytes[b.storage.ops_data.clone()],
                &b.storage.id_ctr,
            ),
            FragSrc::Owned { raw, data, id_ctr } => (raw, data, id_ctr),
        }
    }
}

#[derive(Debug)]
pub(crate) struct FragmentApply<'a> {
    /// the document clock *before* this fragment — the manifold needs
    /// it to split doc preds from in-bundle preds
    clock: Clock,
    /// bundle-actor -> doc-actor translation for the column stream
    actor_map: Vec<usize>,
    src: FragSrc<'a>,
    /// the fragment's op columns loaded through the document load path
    /// — actor indexes remapped, indexes built — ready to merge. The
    /// pred columns stay outside (they describe rows *before* the
    /// fragment and feed the manifold, not the op set).
    frag: crate::op_set2::op_set::OpSet,
}

impl<'a> FragmentApply<'a> {
    /// Wrap already doc-ordered, succ-stamped columns (the batch path,
    /// identity actor map).
    pub(crate) fn from_parts(
        clock: Clock,
        actor_map: Vec<usize>,
        src: FragSrc<'a>,
        doc_ops: &crate::op_set2::op_set::OpSet,
    ) -> Result<Self, AutomergeError> {
        let frag = load_frag_set(&src, &actor_map, doc_ops)?;
        Ok(Self {
            clock,
            actor_map,
            src,
            frag,
        })
    }

    /// Prepare a received bundle for application: load its op columns
    /// as an op set (which also validates them — a malformed bundle
    /// fails here, before any history is touched), remapping bundle
    /// actor indexes to the document's via `actor_map` (every bundle
    /// actor must already be in the document).
    ///
    /// `overlap` marks a bundle whose members are partially present:
    /// the covered rows must not apply again, so the kept ops are
    /// decoded, filtered against `clock` and re-encoded (rare).
    pub(crate) fn new(
        bundle: &'a Bundle,
        actor_map: Vec<usize>,
        clock: &Clock,
        overlap: bool,
        doc_ops: &crate::op_set2::op_set::OpSet,
    ) -> Result<Self, AutomergeError> {
        let (src, actor_map) = if overlap {
            let (ops, _) = decode_ops(|| bundle.storage.iter_ops(), &actor_map, clock)?;
            let (raw, data, id_ctr) = super::batch::encode_frag_ops(ops.iter(), doc_ops);
            let identity: Vec<usize> = (0..doc_ops.actors.len()).collect();
            (FragSrc::Owned { raw, data, id_ctr }, identity)
        } else {
            (FragSrc::Bundle(bundle), actor_map)
        };
        let frag = load_frag_set(&src, &actor_map, doc_ops)?;
        Ok(Self {
            clock: clock.clone(),
            actor_map,
            src,
            frag,
        })
    }
}

/// Load a fragment source's op columns as a fully indexed op set in
/// document actor space (see [`OpSet::load_frag`]).
fn load_frag_set(
    src: &FragSrc<'_>,
    actor_map: &[usize],
    doc_ops: &crate::op_set2::op_set::OpSet,
) -> Result<crate::op_set2::op_set::OpSet, AutomergeError> {
    let (raw, data, id_ctr) = src.parts();
    crate::op_set2::op_set::OpSet::load_frag(
        raw,
        data,
        id_ctr,
        actor_map,
        doc_ops.actors.clone(),
        &doc_ops.obj_info,
        doc_ops.text_encoding,
    )
    .map_err(|e| {
        if std::env::var("FRAG_DEBUG").is_ok() {
            eprintln!("fragment column load failed: {}", e);
        }
        AutomergeError::InvalidFragment("invalid fragment op columns")
    })
}

/// Decode a doc-ordered succ-format op stream into splice-ready
/// [`ChangeOp`]s. `iter` is called twice: an increment-value prepass,
/// then the main pass. Returns the ops and whether any clock-covered
/// rows were skipped (overlap).
fn decode_ops<'a, F, I>(
    iter: F,
    actor_map: &[usize],
    clock: &Clock,
) -> Result<(Vec<ChangeOp>, bool), AutomergeError>
where
    F: Fn() -> I,
    I: Iterator<Item = crate::storage::bundle::BundleOp<'a>>,
{
    // increment successors need their value; increments are rows
    let mut inc_values: std::collections::HashMap<OpId, i64> = Default::default();
    for bop in iter() {
        if let Some(v) = bop.op.get_increment_value() {
            inc_values.insert(bop.op.id, v);
        }
    }

    let mut ops = Vec::new();
    // overlap: a skipped (already-present) row's succ entries that
    // name *kept* ops must still land as doc succ. The kept
    // successor is in the same group with a larger id, so document
    // order puts it after the skipped row — record the pairing and
    // hand it to the successor as an extra (external) pred, which
    // the manifold resolves like any other doc-row pred
    let mut pending: std::collections::HashMap<OpId, Vec<OpId>> = Default::default();
    let mut overlap = false;
    for bop in iter() {
        let op = bop.op;
        let id = op.id.map(actor_map)?;
        if clock.covers(&id) {
            overlap = true;
            for s in &bop.succ {
                let sm = s.map(actor_map)?;
                if !clock.covers(&sm) {
                    pending.entry(sm).or_default().push(id);
                }
            }
            continue;
        }
        let obj = op.obj.map(actor_map)?;
        let key = match op.key {
            KeyRef::Map(s) => KeyRef::Map(Cow::Owned(s.into_owned())),
            KeyRef::Seq(e) => KeyRef::Seq(e.map(actor_map)?),
        };
        let mut op_pred = op
            .pred
            .iter()
            .map(|p| p.map(actor_map))
            .collect::<Result<Vec<_>, _>>()?;
        if let Some(extra) = pending.remove(&id) {
            op_pred.extend(extra);
        }
        let mut succ = bop
            .succ
            .iter()
            .map(|s| Ok((s.map(actor_map)?, inc_values.get(s).copied())))
            .collect::<Result<Vec<_>, AutomergeError>>()?;
        let is_counter = matches!(op.value, OpScalarValue::Counter(_));
        normalize_increment_successors(is_counter, &mut succ);
        let bld = OpBuilder {
            id,
            obj,
            key,
            action: op.action,
            value: op.value.into_owned(),
            mark_name: op.mark_name.map(|s| Cow::Owned(s.into_owned())),
            expand: op.expand,
            insert: op.insert,
            pred: op_pred,
        };
        ops.push(ChangeOp {
            conflicted: false,
            succ,
            bld,
        });
    }
    debug_assert!(pending.is_empty(), "skipped-row successor never arrived");
    Ok((ops, overlap))
}

impl<'a> FragmentApply<'a> {
    /// Apply the bundle's ops. Patches for an active log are produced
    /// by diffing the document across the apply.
    pub(crate) fn apply(
        self,
        doc: &mut Automerge,
        log: &mut PatchLog,
    ) -> Result<(), AutomergeError> {
        let before = log.is_active().then(|| doc.get_heads());
        self.apply_manifold(doc, log)?;
        if let Some(before) = before {
            doc.log_diff(&before, log);
        }
        Ok(())
    }

    /// Resolve the bundle with [`crate::op_set2::op_set::ApplyManifold`]:
    /// the bundle's ops are already in document order — the manifold's
    /// exact contract — so positions, succ and top/text adjustments come
    /// from seeks over the touched scopes only.
    ///
    /// `pub(super)` so the batch path can join this pipeline after
    /// converting a v1 batch into the succ-format columns.
    pub(super) fn apply_manifold(
        self,
        doc: &mut Automerge,
        log: &mut PatchLog,
    ) -> Result<(), AutomergeError> {
        let timing = std::env::var("FRAG_TIMING").is_ok();
        let mut t = std::time::Instant::now();
        let lap = |label: &str, t: &mut std::time::Instant| {
            if timing {
                eprintln!(
                    "TIMING   {:<26} {:>10.3}ms",
                    label,
                    t.elapsed().as_secs_f64() * 1e3
                );
                *t = std::time::Instant::now();
            }
        };
        log.migrate_actors(&doc.ops().actors)?;

        let mut r = {
            let (raw, data, id_ctr) = self.src.parts();
            let meta = crate::storage::bundle::frag_prepass(raw, data, id_ctr, &self.actor_map);
            let mut fs = crate::storage::bundle::FragOps::new(raw, data, id_ctr, &self.actor_map);
            lap("manifold: prepass", &mut t);
            let m = doc.ops().apply_manifold(self.clock.clone());
            lap("manifold: setup", &mut t);
            m.apply_frag(&mut fs, &meta)
        };
        lap("manifold: stream", &mut t);

        // in the succ format every pred names a doc row — nothing can
        // defer to the batch side
        debug_assert!(r.change_succ.is_empty(), "fragment op had a batch pred");

        if std::env::var("MERGE_DEBUG").is_ok() {
            eprintln!("== merge: runs {:?}", r.insert_runs);
            eprintln!("   mixed groups {:?}", r.mixed_groups);
            eprintln!("== frag opset ==");
            self.frag.dump();
        }

        // write the doc succ while positions are still pre-merge —
        // add_succ also clears vis/top/text on rows it deletes, so the
        // visible column is final before the elections below read it
        crate::op_set2::op_set::manifold::STAT_ADD_SUCC[0]
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        crate::op_set2::op_set::manifold::STAT_ADD_SUCC[1].fetch_add(
            r.doc_succ.len() as u64,
            std::sync::atomic::Ordering::Relaxed,
        );
        doc.ops.add_succ(std::mem::take(&mut r.doc_succ));
        lap("add_succ", &mut t);

        // the merge: copy the fragment's columns and indexes in at the
        // insert runs
        doc.ops.merge(self.frag, &r.insert_runs);
        lap("merge columns", &mut t);

        // top/text are the only index bits that aren't a straight copy,
        // and only in groups shared between the doc and the fragment —
        // re-run the last-visible election over each such group.
        // INDEX_REBUILD=1 rebuilds everything from scratch instead (A/B
        // debugging fallback)
        if std::env::var("INDEX_REBUILD").is_ok() {
            doc.ops
                .rebuild_indexes()
                .map_err(|_| AutomergeError::InvalidFragment("index rebuild failed"))?;
            lap("index rebuild", &mut t);
        } else {
            reset_mixed_groups(doc, &r);
            lap("reset mixed groups", &mut t);
            if std::env::var("MERGE_VALIDATE").is_ok() {
                let diffs = doc.ops.index_diff_positions();
                if !diffs.is_empty() {
                    let runs = &r.insert_runs;
                    let mut shifts = Vec::with_capacity(runs.len());
                    let mut acc = 0usize;
                    for cr in runs.iter() {
                        shifts.push(acc);
                        acc += cr.range.len();
                    }
                    for (col, pos) in &diffs {
                        // provenance: which frag run (or doc row) owns pos
                        let mut src = "past all runs".to_string();
                        for (k, cr) in runs.iter().enumerate() {
                            let base = cr.pos + shifts[k];
                            if *pos >= base && *pos < base + cr.range.len() {
                                src = format!("frag row {}", cr.range.start + (pos - base));
                                break;
                            } else if *pos < base {
                                src = format!("doc row {}", pos - shifts[k]);
                                break;
                            }
                        }
                        eprintln!("IDXDIFF {} at {} ({})", col, pos, src);
                    }
                    eprintln!("  runs {:?}", r.insert_runs);
                    eprintln!("  mixed {:?}", r.mixed_groups);
                    panic!("index diverges after mixed-group reset");
                }
            }
        }

        #[cfg(debug_assertions)]
        if !doc.ops.validate_op_order() {
            eprintln!("== insert runs {:?} ==", r.insert_runs);
            eprintln!("== doc rows ==");
            for op in doc.ops().iter() {
                eprintln!(
                    "  row {:>3} id {:?} obj {:?} key {:?} ins {}",
                    op.pos,
                    op.id,
                    op.obj,
                    op.elemid_or_key(),
                    op.insert
                );
            }
            panic!("op order violated");
        }
        Ok(())
    }
}

/// Re-run the top/text election in every group the manifold flagged as
/// mixed. A group's merged range is its doc rows — shifted by the
/// fragment rows inserted at or before each end — widened to cover the
/// fragment rows that landed inside it (mapped through the insert
/// runs). `OpSet::reset_top` then settles the group from its (already
/// final) visible bits.
fn reset_mixed_groups(doc: &mut Automerge, r: &crate::op_set2::op_set::manifold::ManifoldResult) {
    if r.mixed_groups.is_empty() {
        return;
    }
    let runs = &r.insert_runs;
    // prefix sums: shifts[i] = rows merged before run i
    let mut shifts = Vec::with_capacity(runs.len());
    let mut acc = 0usize;
    for cr in runs.iter() {
        shifts.push(acc);
        acc += cr.range.len();
    }
    let total = acc;
    // fragment row -> merged position (rows in mixed groups always merge)
    let row_pos = |row: usize| -> usize {
        let i = runs.partition_point(|cr| cr.range.start <= row) - 1;
        let cr = &runs[i];
        debug_assert!(cr.range.contains(&row), "mixed-group row not merged");
        cr.pos + shifts[i] + (row - cr.range.start)
    };
    // pre-merge doc position -> merged position: displaced by every
    // fragment row inserted at or before it
    let doc_pos = |p: usize| -> usize {
        // runs 0..i sit at positions <= p, so exactly their rows
        // displace this doc row
        let i = runs.partition_point(|cr| cr.pos <= p);
        p + if i == runs.len() { total } else { shifts[i] }
    };
    // consecutive headless runs share one fused register: re-elect each
    // head once
    let mut done_head = usize::MAX;
    for g in &r.mixed_groups {
        // the fragment's standalone build fused the register holding
        // this group's first fragment row (deletes included — they
        // occupy rows too) with the fragment register before it: a
        // headless run has no insert of its own. That register's
        // election is poisoned — redo it at its own merged position
        // (it can sit far from this group)
        if let Some(head) = g.head_hint {
            if head != done_head {
                doc.ops.reset_register_at(row_pos(head));
                done_head = head;
            }
        }
        let mut start = doc_pos(g.doc.start);
        let mut end = doc_pos(g.doc.end - 1) + 1;
        if let Some((lo, hi)) = g.rows {
            start = start.min(row_pos(lo));
            end = end.max(row_pos(hi) + 1);
        }
        crate::op_set2::op_set::manifold::STAT_RESET[0]
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        crate::op_set2::op_set::manifold::STAT_RESET[1]
            .fetch_add((end - start) as u64, std::sync::atomic::Ordering::Relaxed);
        doc.ops.reset_top_range(start..end);
    }
}

#[cfg(test)]
mod tests {
    use crate::marks::{ExpandMark, Mark};
    use crate::read::ReadDoc;
    use crate::transaction::Transactable;
    use crate::types::ChangeHash;
    use crate::{
        make_rng, AutoCommit, Automerge, AutomergeError, BundleV2, Change, ChangeId, Fragment,
        HashGraphState, ObjType, ScalarValue, ROOT,
    };
    use rand::prelude::*;
    use std::collections::HashSet;

    /// Build the fragment metadata describing `changes` (a causally
    /// closed set with a single head, in topological order).
    fn fragment_for(changes: &[Change]) -> Fragment {
        let in_set: HashSet<ChangeHash> = changes.iter().map(|c| c.hash()).collect();
        let mut has_child: HashSet<ChangeHash> = HashSet::new();
        for c in changes {
            for d in c.deps() {
                has_child.insert(*d);
            }
        }
        let heads: Vec<&Change> = changes
            .iter()
            .filter(|c| !has_child.contains(&c.hash()))
            .collect();
        assert_eq!(heads.len(), 1, "test fragments must have a single head");
        let head = heads[0];
        let id = |c: &Change| ChangeId {
            actor: c.actor_id().clone(),
            seq: c.seq(),
        };
        // members lead with the head, matching Fragment::export
        let mut members = vec![id(head)];
        members.extend(changes.iter().filter(|c| c.hash() != head.hash()).map(&id));
        let boundary = changes
            .iter()
            .flat_map(|c| c.deps().iter())
            .filter(|d| !in_set.contains(*d))
            .copied()
            .collect();
        Fragment {
            head: head.hash(),
            level: head.hash().fragment_level(),
            boundary,
            checkpoints: vec![],
            members,
        }
    }

    /// Bundle everything in `src` after `heads` and apply it to `dst`
    /// with `apply_fragment`; apply the same changes to a fork of `dst`
    /// with the batch path; the results must agree — including the
    /// heads, both before and after rebuilding the hash graph.
    fn apply_and_compare(src: &mut AutoCommit, dst: &mut AutoCommit, heads: &[ChangeHash]) {
        let changes = src.get_changes(heads).unwrap();
        let frag = fragment_for(&changes);
        let v2 = src.doc.bundle_fragment_v2(&frag).unwrap();

        let mut dst_ref = dst.fork();
        dst_ref.doc.apply_changes_batch(changes).unwrap();
        dst_ref.validate_top_index();

        dst.doc.apply_fragment(&v2).unwrap();
        dst.validate_top_index();

        assert_eq!(dst.doc.hash_graph_state(), HashGraphState::FragmentHashes);
        assert_eq!(dst.get_heads(), dst_ref.get_heads());

        dst.doc.debug_cmp(&dst_ref.doc);

        // hashing every member verifies the head hash taken on trust
        dst.doc.rebuild_hash_graph().unwrap();
        assert_eq!(dst.doc.hash_graph_state(), HashGraphState::Checked);
        assert_eq!(dst.doc.save(), dst_ref.doc.save());
    }

    #[test]
    fn fragment_map_apply() {
        let mut rng = make_rng();
        let mut doc1 = AutoCommit::new().with_actor(rng.random()).unwrap();
        let map1 = doc1.put_object(&ROOT, "map", ObjType::Map).unwrap();
        doc1.put(&map1, "key1", "val1").unwrap();
        doc1.put(&map1, "key2", "val2").unwrap();
        let heads = doc1.get_heads();

        let mut src = doc1.fork().with_actor(rng.random()).unwrap();
        for i in 0..5 {
            let mut tmp = doc1.fork().with_actor(rng.random()).unwrap();
            tmp.put(&map1, "key1", format!("conflict{}", i)).unwrap();
            tmp.delete(&map1, "key2").unwrap();
            let m = tmp
                .put_object(&map1, format!("map{}", i), ObjType::Map)
                .unwrap();
            tmp.put(&m, "inner", i).unwrap();
            src.merge(&mut tmp).unwrap();
        }
        // unify the concurrent branches under a single head
        src.put(&map1, "done", true).unwrap();

        apply_and_compare(&mut src, &mut doc1, &heads);
    }

    #[test]
    fn fragment_fuzz_list_apply() {
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

        let mut src = doc1.fork().with_actor(rng.random()).unwrap();

        for _ in 0..3 {
            for _ in 0..20 {
                let mut tmp = src.fork().with_actor(rng.random()).unwrap();
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
                src.merge(&mut tmp).unwrap();
            }
        }
        // unify the concurrent branches under a single head
        src.put(&ROOT, "done", true).unwrap();

        apply_and_compare(&mut src, &mut doc1, &heads);
    }

    #[test]
    fn fragment_fuzz_text_marks_apply() {
        let mut rng = make_rng();
        let mut doc1 = AutoCommit::new().with_actor(rng.random()).unwrap();
        let text1 = doc1.put_object(&ROOT, "text1", ObjType::Text).unwrap();
        doc1.splice_text(&text1, 0, 0, "---------------------")
            .unwrap();
        let mut value = 0;
        let mut val = move || {
            value += 1;
            value
        };
        let heads = doc1.get_heads();

        let mut src = doc1.fork().with_actor(rng.random()).unwrap();

        for _ in 0..5 {
            for _ in 0..10 {
                let mut tmp = src.fork().with_actor(rng.random()).unwrap();
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
                let num_marks = rng.random::<u32>() % 3;
                for _ in 0..num_marks {
                    let len = tmp.length(&text1) as u32;
                    let a = rng.random::<u32>() % len;
                    let b = rng.random::<u32>() % len;
                    if a == b {
                        continue;
                    }
                    let mark = Mark {
                        start: std::cmp::min(a, b) as usize,
                        end: std::cmp::max(a, b) as usize,
                        name: "bold".into(),
                        value: ScalarValue::from(val()),
                    };
                    tmp.mark(&text1, mark, ExpandMark::After).unwrap();
                }
                src.merge(&mut tmp).unwrap();
            }
        }
        // unify the concurrent branches under a single head
        src.splice_text(&text1, 0, 0, "!").unwrap();

        apply_and_compare(&mut src, &mut doc1, &heads);
    }

    #[test]
    fn fragment_fuzz_map_counter_apply() {
        let mut rng = make_rng();
        let mut doc1 = AutoCommit::new().with_actor(rng.random()).unwrap();
        let map1 = doc1.put_object(&ROOT, "map1", ObjType::Map).unwrap();
        doc1.put(&map1, "key1", ScalarValue::counter(10)).unwrap();
        doc1.increment(&map1, "key1", 15).unwrap();
        let mut value = 0;
        let mut val = move || {
            value += 1;
            value
        };
        let heads = doc1.get_heads();

        let mut src = doc1.fork().with_actor(rng.random()).unwrap();

        for _ in 0..30 {
            let mut tmp = src.fork().with_actor(rng.random()).unwrap();
            let num_updates = rng.random::<u32>() % 5 + 1;
            for _ in 0..num_updates {
                let key = format!("key{}", rng.random::<u32>() % 8);
                match rng.random::<u32>() % 3 {
                    0 => {
                        tmp.put(&map1, key, ScalarValue::counter(val())).unwrap();
                    }
                    1 => {
                        if tmp.get(&map1, &key).unwrap().is_some() {
                            let _ = tmp.increment(&map1, key, val());
                        }
                    }
                    _ => {
                        let _ = tmp.delete(&map1, key);
                    }
                }
            }
            src.merge(&mut tmp).unwrap();
        }
        // unify the concurrent branches under a single head
        src.put(&map1, "done", true).unwrap();

        apply_and_compare(&mut src, &mut doc1, &heads);
    }

    #[test]
    fn fragment_sequential_bundles() {
        let mut rng = make_rng();
        let mut src = AutoCommit::new().with_actor(rng.random()).unwrap();
        let text = src.put_object(&ROOT, "text", ObjType::Text).unwrap();
        for i in 0..40 {
            let len = src.length(&text);
            let pos = if len == 0 {
                0
            } else {
                rng.random::<u32>() as usize % len
            };
            src.splice_text(&text, pos, 0, &format!("{}!", i)).unwrap();
            src.commit();
        }

        // feed the history to an empty document as a chain of bundles —
        // each bundle's boundary dep is the previous fragment's head,
        // whose hash apply_fragment learned from the fragment metadata
        let changes = src.get_changes(&[]).unwrap();
        let mut dst = Automerge::new();
        for chunk in changes.chunks(7) {
            // round trip through the encoded chunk
            let frag = fragment_for(chunk);
            let bytes = src.doc.bundle_fragment_v2(&frag).unwrap().bytes();
            let v2 = BundleV2::try_from(&bytes[..]).unwrap();
            dst.apply_fragment(&v2).unwrap();
        }

        assert_eq!(dst.get_heads(), src.get_heads());
        dst.debug_cmp(&src.doc);

        dst.rebuild_hash_graph().unwrap();
        assert_eq!(dst.hash_graph_state(), HashGraphState::Checked);
        assert_eq!(dst.save(), src.doc.save());
    }

    #[test]
    fn fragment_apply_errors() {
        let mut rng = make_rng();
        let mut src = AutoCommit::new().with_actor(rng.random()).unwrap();
        for i in 0..9 {
            src.put(&ROOT, "key", i).unwrap();
            src.commit();
        }

        let changes = src.get_changes(&[]).unwrap();
        let chunks: Vec<_> = changes.chunks(3).collect();
        let bundles: Vec<_> = chunks
            .iter()
            .map(|c| src.doc.bundle_fragment_v2(&fragment_for(c)).unwrap())
            .collect();

        let mut dst = Automerge::new();

        // out of order: the middle chunk's boundary dep is missing
        assert!(matches!(
            dst.apply_fragment(&bundles[1]),
            Err(AutomergeError::MissingDeps)
        ));

        dst.apply_fragment(&bundles[0]).unwrap();

        // duplicate application is a no-op
        let heads = dst.get_heads();
        dst.apply_fragment(&bundles[0]).unwrap();
        assert_eq!(dst.get_heads(), heads);

        dst.apply_fragment(&bundles[1]).unwrap();
        dst.apply_fragment(&bundles[2]).unwrap();

        assert_eq!(dst.get_heads(), src.get_heads());
        dst.rebuild_hash_graph().unwrap();
        assert_eq!(dst.save(), src.doc.save());
    }

    #[test]
    fn fragment_apply_overlap() {
        // fragments can contain a mixture of changes the document does
        // and does not have — the present ones (and their ops) are
        // skipped
        let mut rng = make_rng();
        let mut src = AutoCommit::new().with_actor(rng.random()).unwrap();
        let text = src.put_object(&ROOT, "text", ObjType::Text).unwrap();
        for i in 0..9 {
            src.splice_text(&text, 0, 0, &format!("{}", i)).unwrap();
            src.commit();
        }

        let changes = src.get_changes(&[]).unwrap();
        let make = |cs: &[Change]| src.doc.bundle_fragment_v2(&fragment_for(cs)).unwrap();
        let first = make(&changes[..6]);
        let overlapping = make(&changes[3..]); // 3 present, 3 new

        let mut dst = Automerge::new();
        dst.apply_fragment(&first).unwrap();
        dst.apply_fragment(&overlapping).unwrap();

        assert_eq!(dst.get_heads(), src.get_heads());
        dst.debug_cmp(&src.doc);
        dst.rebuild_hash_graph().unwrap();
        assert_eq!(dst.save(), src.doc.save());
    }
}
