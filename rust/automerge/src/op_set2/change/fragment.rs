use crate::clock::Clock;
use crate::op_set2::types::{KeyRef, PropRef, ScalarValue as OpScalarValue};
use crate::op_set2::SuccInsert;
use crate::storage::bundle::Bundle;
use crate::types::{ObjId, ObjType, OpId, SequenceType};
use crate::{Automerge, AutomergeError, PatchLog, TextEncoding};

use super::super::op::{ChangeOp, Op, OpBuilder};
use super::super::op_set::{ObjIndex, OpIter};
use super::batch::{
    insert_runs_of_ops, normalize_increment_successors, walk_map, Adjust, MapWalker, ObjSpan,
    ObjWalker, PredCache, Top, ValueState,
};

use std::borrow::Cow;
use std::collections::HashSet;

/// Applies the ops of a bundle directly, without converting the bundle
/// into [`crate::Change`]s first.
///
/// This is the fragment twin of `BatchApply`: the same walk over the
/// document's op set, but exploiting the bundle's invariants — its ops
/// are already in document order and never causally precede anything in
/// the receiving document — so no sorting or untangling is needed.
#[derive(Debug, Clone, Default)]
pub(crate) struct FragmentApply {
    ops: Vec<ChangeOp>,
    pred: PredCache,
    obj_spans: Vec<ObjSpan>,
}

impl FragmentApply {
    /// Load the bundle's ops, remapping its actor indexes to the
    /// document's via `actor_map` (every bundle actor must already be
    /// in the document). `clock` is the document's current clock — ops
    /// it covers belong to member changes the document already has and
    /// are dropped.
    pub(crate) fn new(
        bundle: &Bundle,
        actor_map: &[usize],
        clock: &Clock,
    ) -> Result<Self, AutomergeError> {
        let mut ops = Vec::new();
        for op in bundle.storage.iter_ops() {
            let id = op.id.map(actor_map)?;
            if clock.covers(&id) {
                continue;
            }
            let obj = op.obj.map(actor_map)?;
            let key = match op.key {
                KeyRef::Map(s) => KeyRef::Map(Cow::Owned(s.into_owned())),
                KeyRef::Seq(e) => KeyRef::Seq(e.map(actor_map)?),
            };
            let pred = op
                .pred
                .iter()
                .map(|p| p.map(actor_map))
                .collect::<Result<Vec<_>, _>>()?;
            let bld = OpBuilder {
                id,
                obj,
                key,
                action: op.action,
                value: op.value.into_owned(),
                mark_name: op.mark_name.map(|s| Cow::Owned(s.into_owned())),
                expand: op.expand,
                insert: op.insert,
                pred,
            };
            ops.push(ChangeOp {
                pos: None,
                subsort: 0,
                conflicted: false,
                succ: vec![],
                bld,
            });
        }
        Ok(Self {
            ops,
            pred: PredCache::default(),
            obj_spans: Vec::new(),
        })
    }

    /// The linear stand-in for `BatchApply::order_ops_for_doc`: the ops
    /// are already in document order, so one pass builds the pred
    /// cache, the object spans and the new object infos.
    fn scan_ops(&mut self, obj_info: &mut ObjIndex) -> Result<(), AutomergeError> {
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
                    if o.bld.obj < obj {
                        // document order means each object appears once,
                        // in ascending order
                        return Err(AutomergeError::InvalidFragment(
                            "bundle ops are not in document order",
                        ));
                    }
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
        // a bundle emits the delete ops of a group in arbitrary order, so
        // concurrent successors of one op can arrive unsorted; the succ
        // columns want them in id order
        for successors in self.pred.values_mut() {
            successors.sort_unstable_by_key(|(id, _)| *id);
        }
        Ok(())
    }

    pub(crate) fn apply(
        &mut self,
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

        let mut obj_info = doc.ops().obj_info.clone();
        lap("obj_info.clone", &mut t);

        self.scan_ops(&mut obj_info)?;
        lap("scan_ops", &mut t);

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
                    let sw = SeqWalker::new(
                        os.obj,
                        sequence_type,
                        doc.text_encoding(),
                        &mut conflicts,
                        &mut self.ops[os.span.clone()],
                        &mut self.pred,
                        doc_ops.end_pos(),
                    );
                    walk_seq(sw, doc_ops, &mut succ, log);
                }
                _ => panic!("Obj {:?} Missing from Index", os.obj),
            }
        }

        lap("walks (map+seq)", &mut t);

        for a in conflicts {
            match a {
                Adjust::Conflict(index) => doc.ops.conflict(index),
                Adjust::Expose(index) => doc.ops.expose(index),
            }
        }
        lap("conflict adjusts", &mut t);

        doc.ops.add_succ(&succ);
        lap("add_succ", &mut t);

        insert_runs_of_ops(&self.ops, doc);
        lap("insert_runs_of_ops (splice)", &mut t);

        debug_assert!(doc.ops.validate_op_order());
        Ok(())
    }
}

/// Walks a sequence object, merging the (already document-ordered)
/// change ops into the object's existing ops in a single pass.
///
/// Both streams are subsequences of the final document order, so the
/// next op in the merged order is always either the current doc op or
/// the head of the change-op stream:
///
/// * a change op is *ready* once the element it keys at has been
///   emitted (elements created by the fragment itself are always ready,
///   because the stream order puts a parent before its dependents)
/// * a ready insert emits when the next doc insert has a smaller id —
///   siblings sort in descending id order, and every descendant has a
///   larger id than its ancestor, so this comparison also keeps pending
///   inserts behind a larger sibling's whole subtree
/// * a ready non-insert (update/delete/increment) belongs to the group
///   the doc walk is currently inside, so it emits before any insert
///   (its group is ending) or before a same-group doc op with a larger
///   id
struct SeqWalker<'a, 'b> {
    change_ops: &'a mut [ChangeOp],
    // head of the change-op stream — ops before `next` have been emitted
    next: usize,
    // insert ids belonging to the change ops themselves
    internal: HashSet<OpId>,
    // doc elements the change ops attach to...
    attach: HashSet<OpId>,
    // ...and those of them the doc walk has passed
    emitted: HashSet<OpId>,
    pred: &'b mut PredCache,
    conflicts: &'b mut Vec<Adjust>,
    top: Top,
    value: ValueState<'a>,
    seq_type: SequenceType,
    text_encoding: TextEncoding,
    count: usize,
    index: usize,
    width: usize,
    max: usize,
}

impl<'a, 'b> SeqWalker<'a, 'b> {
    fn new(
        obj: ObjId,
        seq_type: SequenceType,
        text_encoding: TextEncoding,
        conflicts: &'b mut Vec<Adjust>,
        change_ops: &'a mut [ChangeOp],
        pred: &'b mut PredCache,
        max: usize,
    ) -> Self {
        let mut internal = HashSet::with_capacity(change_ops.len());
        let mut attach = HashSet::new();
        let value = ValueState::new(obj, seq_type, text_encoding);
        for op in change_ops.iter_mut() {
            if let Some(mut successors) = pred.remove(&op.id()) {
                let is_counter = matches!(op.bld.value, OpScalarValue::Counter(_));
                normalize_increment_successors(is_counter, &mut successors);
                op.succ = successors;
            }
            if let KeyRef::Seq(e) = op.key() {
                if !e.is_head() && !internal.contains(&e.0) {
                    attach.insert(e.0);
                }
            }
            if op.insert() {
                internal.insert(op.id());
            }
        }
        Self {
            change_ops,
            next: 0,
            internal,
            attach,
            emitted: HashSet::new(),
            pred,
            conflicts,
            top: Top::Nothing,
            value,
            seq_type,
            text_encoding,
            count: 0,
            index: 0,
            width: 0,
            max,
        }
    }

    fn ready(&self, op: &ChangeOp) -> bool {
        match op.key() {
            KeyRef::Seq(e) => {
                e.is_head() || self.internal.contains(&e.0) || self.emitted.contains(&e.0)
            }
            KeyRef::Map(_) => false,
        }
    }

    /// Emit every change op that precedes doc op `d` in the merged order.
    fn emit_due(&mut self, d: &Op<'_>, log: &mut PatchLog) {
        while let Some(op) = self.change_ops.get(self.next) {
            if !self.ready(op) {
                break;
            }
            let due = if op.insert() {
                d.insert && op.id() > d.id
            } else {
                d.insert || d.id > op.id()
            };
            if !due {
                break;
            }
            self.emit(d.pos, log);
        }
    }

    fn emit(&mut self, pos: usize, log: &mut PatchLog) {
        let i = self.next;
        self.next += 1;
        if self.change_ops[i].insert() {
            self.flush(log);
            self.value.key = Some(PropRef::Seq(self.index));
        }
        let op = &mut self.change_ops[i];
        debug_assert!(op.pos.is_none());
        op.pos = Some(pos);
        op.subsort = self.count;
        self.count += 1;
        if op.visible() {
            self.width = op.width(self.seq_type, self.text_encoding);
        }
        self.value.process_change_op(op);
        self.top
            .process_change_op(self.conflicts, self.change_ops, i);
    }

    fn flush(&mut self, log: &mut PatchLog) {
        self.value.list_flush(self.index, log);
        self.top.reset(self.conflicts);
        self.index += self.width;
        self.width = 0;
    }

    fn handle_doc_op(&mut self, doc_op: &Op<'a>, succ: &mut Vec<SuccInsert>, log: &mut PatchLog) {
        let mut deleted = false;
        if let Some(mut successors) = self.pred.remove(&doc_op.id) {
            normalize_increment_successors(doc_op.is_counter(), &mut successors);
            for (id, inc) in successors {
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

    fn finish(mut self, log: &mut PatchLog) {
        while self.next < self.change_ops.len() {
            assert!(
                self.ready(&self.change_ops[self.next]),
                "fragment op references an element missing from the document"
            );
            self.emit(self.max, log);
        }
        self.flush(log);
        assert_eq!(self.top, Top::Nothing);
    }
}

fn walk_seq<'a>(
    mut sw: SeqWalker<'a, '_>,
    doc_ops: OpIter<'a>,
    succ: &mut Vec<SuccInsert>,
    log: &mut PatchLog,
) {
    for op in doc_ops {
        sw.emit_due(&op, log);

        if op.insert && sw.attach.contains(&op.id) {
            sw.emitted.insert(op.id);
        }

        sw.handle_doc_op(&op, succ, log);
    }

    sw.finish(log);
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
