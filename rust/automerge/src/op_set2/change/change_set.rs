use crate::clock::Clock;
use crate::op_set2::op_set::manifold::ManifoldResult;
use crate::storage::ChangeSet;
use crate::{Automerge, AutomergeError};

/// Applies the ops of a change set directly, without converting the change set
/// into [`crate::Change`]s first.
///
/// This is the change set twin of `BatchApply`: the same walk over the
/// document's op set, but exploiting the change set's invariants — its ops
/// are already in document order and never causally precede anything in
/// the receiving document — so no sorting or untangling is needed.
/// Where the streaming manifold reads the change set's op columns from.
#[derive(Debug)]
pub(crate) enum ChangeSetSrc<'a> {
    /// a received change set's columns, borrowed
    ChangeSet(&'a ChangeSet),
    /// columns encoded in-process (the batch path, and re-encoded
    /// overlap change sets)
    Owned {
        raw: crate::storage::RawColumns<crate::storage::columns::compression::Uncompressed>,
        data: Vec<u8>,
    },
}

impl ChangeSetSrc<'_> {
    fn parts(
        &self,
    ) -> (
        &crate::storage::RawColumns<crate::storage::columns::compression::Uncompressed>,
        &[u8],
    ) {
        match self {
            ChangeSetSrc::ChangeSet(b) => (
                &b.storage.ops_meta,
                &b.storage.bytes[b.storage.ops_data.clone()],
            ),
            ChangeSetSrc::Owned { raw, data } => (raw, data),
        }
    }
}

#[derive(Debug)]
pub(crate) struct ChangeSetApply<'a> {
    /// the document clock *before* this change set — the manifold needs
    /// it to split doc preds from in-change set preds
    clock: Clock,
    /// change set-actor -> doc-actor translation for the column stream
    actor_map: Vec<usize>,
    src: ChangeSetSrc<'a>,
    /// the change set's op columns loaded through the document load path
    /// — actor indexes remapped, indexes built — ready to merge. The
    /// pred columns stay outside (they describe rows *before* the
    /// change set and feed the manifold, not the op set).
    frag: crate::op_set2::op_set::OpSet,
}

impl<'a> ChangeSetApply<'a> {
    /// Wrap already doc-ordered, succ-stamped columns (the batch path,
    /// identity actor map).
    pub(crate) fn from_parts(
        clock: Clock,
        actor_map: Vec<usize>,
        src: ChangeSetSrc<'a>,
        doc_ops: &crate::op_set2::op_set::OpSet,
    ) -> Result<Self, AutomergeError> {
        let frag = load_change_set(&src, &actor_map, doc_ops)?;
        Ok(Self {
            clock,
            actor_map,
            src,
            frag,
        })
    }

    /// Prepare a received change set for application: load its op columns
    /// as an op set (which also validates them — a malformed change set
    /// fails here, before any history is touched), remapping change set
    /// actor indexes to the document's via `actor_map` (every change set
    /// actor must already be in the document).
    ///
    /// `overlap` marks a change set whose members are partially present:
    /// the covered rows must not apply again, so the kept ops are
    /// decoded, filtered against `clock` and re-encoded (rare).
    /// `change_set_ops` is the change set's op columns as the parse left them —
    /// decoded, validated, still in change set actor space. All that remains
    /// is the document-dependent half ([`OpSet::index_change_set`]): rebase the
    /// actors and build the indexes against `doc_ops`.
    pub(crate) fn new(
        change_set: &'a ChangeSet,
        actor_map: Vec<usize>,
        clock: &Clock,
        overlap: bool,
        doc_ops: &crate::op_set2::op_set::OpSet,
        change_set_ops: crate::op_set2::op_set::OpSet,
    ) -> Result<Self, AutomergeError> {
        let (src, frag) = if overlap {
            // the covered rows must not apply again, so they are cut out
            // — which makes the parse's columns (every row, unfiltered)
            // the wrong ones for the manifold's read, and the filtered
            // op set is written back out for it
            let mut ops = change_set_ops;
            let preds = ops.drop_covered(
                &change_set.storage.ops_meta,
                &change_set.storage.bytes[change_set.storage.ops_data.clone()],
                clock,
                &actor_map,
            );
            let (raw, data) = ops.export_change_set(preds);
            // a filtered change set names elements it no longer contains
            // whatever its deps say, so it always takes the safe reading
            let frag = ops
                .index_change_set(&actor_map, doc_ops, true)
                .map_err(|_| AutomergeError::MalformedChangeSet("invalid change set op columns"))?;
            (ChangeSetSrc::Owned { raw, data }, frag)
        } else {
            let frag = change_set_ops
                .index_change_set(&actor_map, doc_ops, !change_set.storage.deps.is_empty())
                .map_err(|_| AutomergeError::MalformedChangeSet("invalid change set op columns"))?;
            (ChangeSetSrc::ChangeSet(change_set), frag)
        };
        Ok(Self {
            clock: clock.clone(),
            actor_map,
            src,
            frag,
        })
    }
}

/// Load a change set source's op columns as a fully indexed op set in
/// document actor space (see [`OpSet::load_change_set`]).
fn load_change_set(
    src: &ChangeSetSrc<'_>,
    actor_map: &[usize],
    doc_ops: &crate::op_set2::op_set::OpSet,
) -> Result<crate::op_set2::op_set::OpSet, AutomergeError> {
    let (raw, data) = src.parts();
    crate::op_set2::op_set::OpSet::load_change_set(raw, data, actor_map, doc_ops)
        .map_err(|_| AutomergeError::MalformedChangeSet("invalid change set op columns"))
}

impl<'a> ChangeSetApply<'a> {
    /// Resolve the change set with [`crate::op_set2::op_set::ApplyManifold`]:
    /// the change set's ops are already in document order — the manifold's
    /// exact contract — so positions, succ and top/text adjustments come
    /// from seeks over the touched scopes only.
    ///
    /// Reads only. Everything that can reject a fragment happens here,
    /// so [`Self::commit`] cannot fail and a rejected fragment leaves
    /// the document exactly as it was.
    ///
    /// `pub(super)` so the batch path can join this pipeline after
    /// converting a v1 batch into the succ-format columns.
    pub(crate) fn resolve(&self, doc: &Automerge) -> Result<ManifoldResult, AutomergeError> {
        let (raw, data) = self.src.parts();
        let len = self.frag.len();
        let mut fs = crate::storage::change_set::ManifoldOps::new(
            raw,
            data,
            len,
            &self.actor_map,
            self.frag.succ_entries(),
            self.frag.value_bytes(),
            self.frag.inc_index(),
        );
        let m = doc.ops().apply_manifold(self.clock.clone());
        m.apply_change_set_ops(&mut fs)
    }

    /// Write what [`Self::resolve`] decided. Infallible by construction.
    pub(crate) fn commit(self, doc: &mut Automerge, mut r: ManifoldResult) {
        // write the doc succ while positions are still pre-merge —
        // add_succ also clears vis/top/text on rows it deletes, so the
        // visible column is final before the elections below read it
        doc.ops.add_succ(std::mem::take(&mut r.doc_succ));

        // top/text are the only index bits that aren't a straight copy.
        // Each side is written in its own coordinates, before the merge
        // mixes them: the merge carries the bits into place along with
        // the columns holding them
        let mut frag = self.frag;
        doc.ops.write_tops(&r.doc_tops, true);
        // every merged row is marked dirty by the merge itself
        frag.write_tops(&r.batch_tops, false);

        // the merge: copy the fragment's columns and indexes in at the
        // insert runs
        doc.ops.merge(frag, &r.insert_runs);

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
    }
}

#[cfg(test)]
mod tests {
    use crate::marks::{ExpandMark, Mark};
    use crate::read::ReadDoc;
    use crate::transaction::Transactable;
    use crate::types::ChangeHash;
    use crate::{
        make_rng, AuditMode, AutoCommit, Automerge, AutomergeError, Change, ChangeId, ChangeSet,
        Fragment, ObjType, ScalarValue, ROOT,
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
        let id = |c: &Change| ChangeId::from_doc_seq(c.seq(), c.actor_id().clone(), 0);
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

    /// Change set everything in `src` after `heads` and apply it to `dst`
    /// with `apply_change_set_opsment`; apply the same changes to a fork of `dst`
    /// with the batch path; the results must agree — including the
    /// heads, both before and after rebuilding the hash graph.
    fn apply_and_compare(src: &mut AutoCommit, dst: &mut AutoCommit, heads: &[crate::ChangeId]) {
        let changes = src.get_changes(heads).unwrap();
        let frag = fragment_for(&changes);
        let change_set = src.doc.change_set_for_fragment(&frag).unwrap();

        let mut dst_ref = dst.fork();
        dst_ref.doc.apply_changes_batch(changes).unwrap();
        dst_ref.validate_top_index();

        dst.doc.apply_change_set(change_set.clone()).unwrap();
        dst.validate_top_index();

        assert_eq!(dst.doc.audit_mode(), AuditMode::Disabled);
        assert_eq!(dst.get_heads(), dst_ref.get_heads());

        dst.doc.debug_cmp(&dst_ref.doc);

        // hashing every member verifies the head hash taken on trust
        dst.doc.enable_audit_mode().unwrap();
        assert_eq!(dst.doc.audit_mode(), AuditMode::Enabled);
        assert_eq!(dst.doc.save(), dst_ref.doc.save());
    }

    #[test]
    fn change_set_map_apply() {
        let mut rng = make_rng();
        let mut doc1 = AutoCommit::new().with_actor(rng.random());
        let map1 = doc1.put_object(&ROOT, "map", ObjType::Map).unwrap();
        doc1.put(&map1, "key1", "val1").unwrap();
        doc1.put(&map1, "key2", "val2").unwrap();
        let heads = doc1.get_heads();

        let mut src = doc1.fork().with_actor(rng.random());
        src.enable_audit_mode().unwrap();
        for i in 0..5 {
            let mut tmp = doc1.fork().with_actor(rng.random());
            // merging FROM tmp enumerates its changes, which needs its
            // hashes — kept only in audit mode
            tmp.enable_audit_mode().unwrap();
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
    fn change_set_fuzz_list_apply() {
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

        let mut src = doc1.fork().with_actor(rng.random());
        src.enable_audit_mode().unwrap();

        for _ in 0..3 {
            for _ in 0..20 {
                let mut tmp = src.fork().with_actor(rng.random());
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
    fn change_set_fuzz_text_marks_apply() {
        let mut rng = make_rng();
        let mut doc1 = AutoCommit::new().with_actor(rng.random());
        let text1 = doc1.put_object(&ROOT, "text1", ObjType::Text).unwrap();
        doc1.splice_text(&text1, 0, 0, "---------------------")
            .unwrap();
        let mut value = 0;
        let mut val = move || {
            value += 1;
            value
        };
        let heads = doc1.get_heads();

        let mut src = doc1.fork().with_actor(rng.random());
        src.enable_audit_mode().unwrap();

        for _ in 0..5 {
            for _ in 0..10 {
                let mut tmp = src.fork().with_actor(rng.random());
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
    fn change_set_fuzz_map_counter_apply() {
        let mut rng = make_rng();
        let mut doc1 = AutoCommit::new().with_actor(rng.random());
        let map1 = doc1.put_object(&ROOT, "map1", ObjType::Map).unwrap();
        doc1.put(&map1, "key1", ScalarValue::counter(10)).unwrap();
        doc1.increment(&map1, "key1", 15).unwrap();
        let mut value = 0;
        let mut val = move || {
            value += 1;
            value
        };
        let heads = doc1.get_heads();

        let mut src = doc1.fork().with_actor(rng.random());
        src.enable_audit_mode().unwrap();

        for _ in 0..30 {
            let mut tmp = src.fork().with_actor(rng.random());
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
    fn change_set_sequential() {
        let mut rng = make_rng();
        let mut src = AutoCommit::new().with_actor(rng.random());
        src.enable_audit_mode().unwrap();
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

        // feed the history to an empty document as a chain of change sets —
        // each change set's boundary dep is the previous fragment's head,
        // whose hash apply_change_set_opsment learned from the fragment metadata
        let changes = src.get_changes(&[]).unwrap();
        let mut dst = Automerge::new();
        for chunk in changes.chunks(7) {
            // round trip through the encoded chunk
            let frag = fragment_for(chunk);
            let bytes = src.doc.change_set_for_fragment(&frag).unwrap().bytes();
            let change_set = ChangeSet::try_from(&bytes[..]).unwrap();
            dst.apply_change_set(change_set.clone()).unwrap();
        }

        assert_eq!(dst.get_heads(), src.get_heads());
        dst.debug_cmp(&src.doc);

        dst.enable_audit_mode().unwrap();
        assert_eq!(dst.audit_mode(), AuditMode::Enabled);
        assert_eq!(dst.save(), src.doc.save());
    }

    #[test]
    fn change_set_apply_errors() {
        let mut rng = make_rng();
        let mut src = AutoCommit::new().with_actor(rng.random());
        src.enable_audit_mode().unwrap();
        for i in 0..9 {
            src.put(&ROOT, "key", i).unwrap();
            src.commit();
        }

        let changes = src.get_changes(&[]).unwrap();
        let chunks: Vec<_> = changes.chunks(3).collect();
        let change_sets: Vec<_> = chunks
            .iter()
            .map(|c| src.doc.change_set_for_fragment(&fragment_for(c)).unwrap())
            .collect();

        let mut dst = Automerge::new();

        // out of order: the middle chunk's boundary dep is missing
        assert!(matches!(
            dst.apply_change_set(change_sets[1].clone()),
            Err(AutomergeError::MissingDeps)
        ));

        dst.apply_change_set(change_sets[0].clone()).unwrap();

        // duplicate application is a no-op
        let heads = dst.get_heads();
        dst.apply_change_set(change_sets[0].clone()).unwrap();
        assert_eq!(dst.get_heads(), heads);

        dst.apply_change_set(change_sets[1].clone()).unwrap();
        dst.apply_change_set(change_sets[2].clone()).unwrap();

        assert_eq!(dst.get_heads(), src.get_heads());
        dst.enable_audit_mode().unwrap();
        assert_eq!(dst.save(), src.doc.save());
    }

    #[test]
    fn change_set_apply_overlap() {
        // fragments can contain a mixture of changes the document does
        // and does not have — the present ones (and their ops) are
        // skipped
        let mut rng = make_rng();
        let mut src = AutoCommit::new().with_actor(rng.random());
        src.enable_audit_mode().unwrap();
        let text = src.put_object(&ROOT, "text", ObjType::Text).unwrap();
        for i in 0..9 {
            src.splice_text(&text, 0, 0, &format!("{}", i)).unwrap();
            src.commit();
        }

        let changes = src.get_changes(&[]).unwrap();
        let make = |cs: &[Change]| src.doc.change_set_for_fragment(&fragment_for(cs)).unwrap();
        let first = make(&changes[..6]);
        let overlapping = make(&changes[3..]); // 3 present, 3 new

        let mut dst = Automerge::new();
        dst.apply_change_set(first.clone()).unwrap();
        dst.apply_change_set(overlapping.clone()).unwrap();

        assert_eq!(dst.get_heads(), src.get_heads());
        dst.debug_cmp(&src.doc);
        dst.enable_audit_mode().unwrap();
        assert_eq!(dst.save(), src.doc.save());
    }

    /// Every mixture of present and new members the history allows:
    /// change set a prefix, then a suffix that reaches back into it. The
    /// cut is taken at a unifying commit so both halves have one head.
    #[test]
    fn change_set_fuzz_overlap_apply() {
        let mut rng = make_rng();
        let mut src = AutoCommit::new().with_actor(rng.random());
        src.enable_audit_mode().unwrap();
        let list = src.put_object(&ROOT, "list", ObjType::List).unwrap();
        let text = src.put_object(&ROOT, "text", ObjType::Text).unwrap();
        let map = src.put_object(&ROOT, "map", ObjType::Map).unwrap();
        src.insert(&list, 0, "seed").unwrap();
        src.splice_text(&text, 0, 0, "seed").unwrap();
        src.put(&map, "counter", ScalarValue::counter(1)).unwrap();
        src.commit();

        let mut value = 0;
        let mut val = move || {
            value += 1;
            value
        };
        // a cut is a change count with a single head — the only place a
        // fragment can start or end
        let mut cuts = vec![src.get_changes(&[]).unwrap().len()];
        for round in 0..8 {
            for _ in 0..3 {
                let mut tmp = src.fork().with_actor(rng.random());
                tmp.enable_audit_mode().unwrap();
                for _ in 0..(rng.random::<u32>() % 6 + 1) {
                    let key = format!("key{}", rng.random::<u32>() % 5);
                    match rng.random::<u32>() % 8 {
                        0 => {
                            let len = tmp.length(&list);
                            tmp.insert(&list, rng.random::<u32>() as usize % len, val())
                                .unwrap();
                        }
                        1 => {
                            let len = tmp.length(&list);
                            tmp.put(&list, rng.random::<u32>() as usize % len, val())
                                .unwrap();
                        }
                        2 => {
                            let len = tmp.length(&list);
                            if len > 1 {
                                tmp.delete(&list, rng.random::<u32>() as usize % len)
                                    .unwrap();
                            }
                        }
                        3 => {
                            let len = tmp.length(&text);
                            let at = rng.random::<u32>() as usize % len;
                            tmp.splice_text(&text, at, 0, &format!("[{}]", val()))
                                .unwrap();
                        }
                        4 => {
                            let len = tmp.length(&text);
                            let at = rng.random::<u32>() as usize % len;
                            let end = std::cmp::min(at + 3, len);
                            if at < end {
                                let mark = Mark {
                                    start: at,
                                    end,
                                    name: "bold".into(),
                                    value: ScalarValue::from(val()),
                                };
                                tmp.mark(&text, mark, ExpandMark::After).unwrap();
                            }
                        }
                        5 => {
                            tmp.put(&map, key, ScalarValue::counter(val())).unwrap();
                        }
                        6 => {
                            if tmp.get(&map, &key).unwrap().is_some() {
                                let _ = tmp.increment(&map, key, val());
                            }
                        }
                        _ => {
                            let _ = tmp.delete(&map, key);
                        }
                    }
                }
                src.merge(&mut tmp).unwrap();
            }
            // unify the concurrent branches, opening a cut
            src.put(&ROOT, "round", round).unwrap();
            src.commit();
            cuts.push(src.get_changes(&[]).unwrap().len());
        }

        let changes = src.get_changes(&[]).unwrap();
        let heads = src.get_heads();
        let saved = src.doc.save();
        let make = |cs: &[Change]| src.doc.change_set_for_fragment(&fragment_for(cs)).unwrap();
        // every (present, new) split: the second change set re-delivers
        // everything from `start` on, of which `first` rows are present
        for (i, &first) in cuts.iter().enumerate() {
            for &start in &cuts[..i] {
                let mut dst = Automerge::new();
                dst.apply_change_set(make(&changes[..first])).unwrap();
                dst.apply_change_set(make(&changes[start..])).unwrap();

                assert_eq!(dst.get_heads(), heads, "cut {}..{}", start, first);
                dst.debug_cmp(&src.doc);
                dst.enable_audit_mode().unwrap();
                assert_eq!(dst.save(), saved, "cut {}..{}", start, first);
            }
        }
    }

    /// One new op succeeding *both* values of a conflicted register
    /// whose members are all present: whether it has a row of its own
    /// (a put) or not (a delete), it ends up carrying two preds.
    #[test]
    fn change_set_apply_overlap_succeeds_conflict() {
        for delete in [true, false] {
            let mut rng = make_rng();
            let mut src = AutoCommit::new().with_actor(rng.random());
            src.enable_audit_mode().unwrap();
            src.put(&ROOT, "seed", 0).unwrap();
            src.commit();

            // concurrent writers: each forks before the other's put
            let mut tmp = src.fork().with_actor(rng.random());
            tmp.enable_audit_mode().unwrap();
            tmp.put(&ROOT, "x", 2).unwrap();
            tmp.commit();
            src.put(&ROOT, "x", 1).unwrap();
            src.commit();
            src.merge(&mut tmp).unwrap();
            // unify the two writers, leaving x conflicted
            src.put(&ROOT, "unify", true).unwrap();
            src.commit();

            let present = src.get_changes(&[]).unwrap().len();
            if delete {
                src.delete(&ROOT, "x").unwrap();
            } else {
                src.put(&ROOT, "x", 3).unwrap();
            }
            src.commit();

            let changes = src.get_changes(&[]).unwrap();
            let make = |cs: &[Change]| src.doc.change_set_for_fragment(&fragment_for(cs)).unwrap();
            let first = make(&changes[..present]);
            let overlapping = make(&changes);

            let mut dst = Automerge::new();
            dst.apply_change_set(first).unwrap();
            dst.apply_change_set(overlapping).unwrap();

            assert_eq!(dst.get(&ROOT, "x").unwrap().is_none(), delete);
            assert_eq!(dst.get_heads(), src.get_heads());
            dst.debug_cmp(&src.doc);
            dst.enable_audit_mode().unwrap();
            assert_eq!(dst.save(), src.doc.save());
        }
    }

    #[test]
    fn change_set_apply_overlap_delete_of_skipped_op() {
        // a kept member deletes an op belonging to a skipped member:
        // the deletion rides the skipped row's succ column and has no
        // row of its own, so dropping that row must not drop the delete
        let mut rng = make_rng();
        let mut src = AutoCommit::new().with_actor(rng.random());
        src.enable_audit_mode().unwrap();
        src.put(&ROOT, "x", 1).unwrap();
        src.commit();
        src.put(&ROOT, "y", 2).unwrap();
        src.commit();
        src.delete(&ROOT, "x").unwrap();
        src.commit();

        let changes = src.get_changes(&[]).unwrap();
        let make = |cs: &[Change]| src.doc.change_set_for_fragment(&fragment_for(cs)).unwrap();
        let first = make(&changes[..2]);
        let overlapping = make(&changes); // 2 present, 1 new

        let mut dst = Automerge::new();
        dst.apply_change_set(first).unwrap();
        dst.apply_change_set(overlapping).unwrap();

        assert_eq!(dst.get_heads(), src.get_heads());
        dst.debug_cmp(&src.doc);
        dst.enable_audit_mode().unwrap();
        assert_eq!(dst.save(), src.doc.save());
    }
}
