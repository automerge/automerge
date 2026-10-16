//! Loading a document from many bundles at once: one manifold pass over
//! all of their ops instead of one per bundle.

use super::{Automerge, BundleRehash};
use crate::hash_retention::HashRetention;
use crate::op_set2::op_set::intake::Intake;
use crate::op_set2::op_set::{ObjIndex, ObjInfo, ObjSeed, OpSet};
use crate::storage::bundle::ManifoldOps;
use crate::storage::Bundle;
use crate::types::{ChangeHash, ObjType};
use crate::AutomergeError;

struct Staged {
    bundle: Bundle,
    actor_map: Vec<usize>,
    frag: OpSet,
    rehash: Option<BundleRehash>,
}

impl<H: HashRetention> Automerge<H> {
    /// Apply `bundles` to this empty document. `hashes[i]`, when given,
    /// are bundle `i`'s verified member hashes.
    pub(crate) fn apply_bundle_feeds(
        &mut self,
        bundles: Vec<(Bundle, Option<Vec<ChangeHash>>)>,
    ) -> Result<(), AutomergeError> {
        debug_assert!(self.ops.len() == 0, "feeds apply to an empty document");
        let mut owes_gc = false;

        // a bundle waits until the bundles it depends on are in the graph
        let mut staged: Vec<Staged> = Vec::with_capacity(bundles.len());
        let mut pending = bundles;
        while !pending.is_empty() {
            let mut waiting = Vec::new();
            let before = pending.len();
            for (mut bundle, hashes) in pending {
                let plan = match self.plan_bundle(&bundle, hashes.clone()) {
                    Err(AutomergeError::MissingDeps) => {
                        waiting.push((bundle, hashes));
                        continue;
                    }
                    Err(e) => return Err(e),
                    Ok(plan) => plan,
                };
                let (_, actor_map) = self.add_bundle_actors(&bundle)?;
                let rehash = match plan {
                    Some(plan) => {
                        let (gc, rehash) = self.commit_bundle_graph(&bundle, plan, &actor_map)?;
                        owes_gc |= gc;
                        Some(rehash)
                    }
                    None => None,
                };
                let mask = self.bundle_mask(&bundle, &actor_map)?;
                debug_assert!(mask.is_none(), "a fresh document has no write-frontier");
                let frag = bundle.take_bundle_ops()?;
                staged.push(Staged {
                    bundle,
                    actor_map,
                    frag,
                    rehash,
                });
            }
            if waiting.len() == before {
                return Err(AutomergeError::MissingDeps);
            }
            pending = waiting;
        }

        // later bundles' actors shift earlier ones' indexes
        for s in &mut staged {
            s.actor_map = s
                .bundle
                .actors()
                .iter()
                .map(|a| {
                    self.ops
                        .lookup_actor(a)
                        .expect("staged actors are in the document")
                })
                .collect();
        }

        // the document was empty before the feeds: an empty clock over the
        // actors they brought
        let clock = self.change_graph.current_clock().empty_like();

        // every fragment's index needs the types of objects the others create
        let mut objs = self.ops.obj_info.clone();
        for s in &staged {
            created_objects(&s.bundle, &s.actor_map, s.frag.len(), &mut objs);
        }
        let shared = std::sync::Arc::new(objs);
        let mut frags = Vec::with_capacity(staged.len());
        for s in &mut staged {
            let frag = std::mem::replace(&mut s.frag, OpSet::new(self.text_encoding()));
            frags.push(
                frag.index_bundle_with(
                    &s.actor_map,
                    &self.ops,
                    ObjSeed::Shared(shared.clone()),
                    true,
                    None,
                )
                .map_err(|_| crate::MalformedBundle::Invalid("invalid bundle op columns"))?,
            );
        }

        let (resolved, fixes) = {
            let readers = staged
                .iter()
                .zip(&frags)
                .map(|(s, frag)| (reader(&s.bundle, &s.actor_map, frag), frag))
                .collect();
            let mut intake = Intake::new(readers, &shared);
            let resolved = self
                .ops
                .apply_manifold(clock, None)
                .with_objects((*shared).clone())
                .apply_bundle_ops(&mut intake)?;
            (resolved, intake.finish()?)
        };

        let mut r = resolved;
        self.ops.add_succ(std::mem::take(&mut r.doc_succ));
        self.ops.write_tops(&r.doc_tops, true);
        let mut batch_tops: Vec<Vec<(usize, bool)>> = vec![Vec::new(); frags.len()];
        for (b, t) in &r.batch_tops {
            batch_tops[b.src as usize].push((b.row, *t));
        }
        for (frag, tops) in frags.iter_mut().zip(batch_tops) {
            frag.write_tops(&tops, false);
        }
        self.ops.merge_many(frags, &r.insert_runs, fixes);
        // the fragments' indexes leave their objects to `shared`
        self.ops.obj_info = std::sync::Arc::try_unwrap(shared).unwrap_or_else(|s| (*s).clone());
        debug_assert!(self.ops.validate_op_order(), "op order violated");

        self.debug_assert_mask_indexes();
        self.republish_if_pending();
        for s in staged {
            if let Some(rehash) = s.rehash {
                owes_gc |= self.rehash_bundle(&s.bundle, rehash)?;
            }
        }
        if owes_gc {
            self.change_graph.gc_after_batch();
        }
        Ok(())
    }
}

fn reader<'a>(bundle: &'a Bundle, actor_map: &'a [usize], frag: &'a OpSet) -> ManifoldOps<'a> {
    ManifoldOps::new(
        &bundle.storage.ops_meta,
        &bundle.storage.bytes[bundle.storage.ops_data.clone()],
        frag.len(),
        actor_map,
        frag.succ_entries(),
        frag.value_bytes(),
        frag.inc_index(),
    )
}

/// Record the objects `bundle`'s ops create, decoding only their rows.
fn created_objects(bundle: &Bundle, actor_map: &[usize], rows: usize, objs: &mut ObjIndex) {
    let empty = hexane::Column::<Option<i64>>::new();
    let mut ops = ManifoldOps::new(
        &bundle.storage.ops_meta,
        &bundle.storage.bytes[bundle.storage.ops_data.clone()],
        rows,
        actor_map,
        0,
        0,
        &empty,
    );
    while ops.pos < ops.len {
        let n = ops.make_free_run(ops.len - ops.pos);
        if n > 0 {
            ops.skip_tail(n);
            continue;
        }
        let op = ops.next_op();
        if let Ok(obj_type) = ObjType::try_from(op.action) {
            objs.insert(
                op.id,
                ObjInfo {
                    parent: op.obj,
                    obj_type,
                },
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::autocommit::AutoCommit;
    use crate::automerge::Automerge;
    use crate::hash_retention::{Full, HashRetention, Retained};
    use crate::marks::{ExpandMark, Mark};
    use crate::read::ReadDoc;
    use crate::storage::Bundle;
    use crate::tx::Transactable;
    use crate::{make_rng, ChangeHash, ObjType, ScalarValue, ROOT};
    use rand::prelude::*;

    /// Splits `doc` the way subduction stores it: level-1+ fragments plus
    /// loose level-0 commits.
    pub(crate) fn decompose(doc: &Automerge<Full>) -> Vec<Bundle> {
        let fragments = doc.fragments(1..).into_iter().chain(doc.fragments(0..=0));
        doc.bundles_for_fragments(fragments)
            .unwrap()
            .iter()
            .map(|b| Bundle::try_from(&b[..]).unwrap())
            .collect()
    }

    fn feeds<H: HashRetention>(bundles: Vec<Bundle>) -> Automerge<H> {
        let bundles = bundles
            .into_iter()
            .map(|b| {
                let hashes = (H::AUDIT == crate::automerge::AuditMode::Enabled)
                    .then(|| b.audit::<ChangeHash>().unwrap());
                (b, hashes)
            })
            .collect();
        let mut doc = Automerge::<H>::empty(crate::TextEncoding::platform_default());
        doc.apply_bundle_feeds(bundles).unwrap();
        doc
    }

    pub(crate) fn check(doc: &Automerge<Full>) {
        let bundles = decompose(doc);
        let audited = feeds::<Full>(bundles.clone());
        assert_eq!(audited.get_heads(), doc.get_heads());
        audited.debug_cmp(doc);
        audited.validate_document();
        let hashes = |d: &Automerge<Full>| {
            let mut h: Vec<_> = d
                .get_changes(&[])
                .unwrap()
                .iter()
                .map(|c| c.hash())
                .collect();
            h.sort();
            h
        };
        assert_eq!(hashes(&audited), hashes(doc));
        // the feeds add changes in their own topological order, so compare
        // what a reload makes of its save rather than the bytes
        let reloaded = Automerge::load(&audited.save()).unwrap();
        assert_eq!(reloaded.get_heads(), doc.get_heads());
        reloaded.debug_cmp(doc);

        let next = feeds::<Retained>(bundles);
        assert_eq!(next.get_heads(), doc.get_heads());
        next.debug_cmp(doc);
        assert!(next.ops.validate_op_order());
        next.ops.validate_indexes();
    }

    fn concurrent_edits(
        rounds: usize,
        edit: impl Fn(&mut AutoCommit<Full>, &mut rand::rngs::SmallRng, u64),
    ) -> Automerge<Full> {
        let mut rng = make_rng();
        let mut src = AutoCommit::new()
            .with_actor(rng.random())
            .enable_audit_mode()
            .unwrap();
        let mut n = 0;
        for _ in 0..rounds {
            let mut forks: Vec<_> = (0..3)
                .map(|_| src.fork().with_actor(rng.random()))
                .collect();
            for f in &mut forks {
                for _ in 0..rng.random_range(1..6) {
                    n += 1;
                    edit(f, &mut rng, n);
                    f.commit();
                }
            }
            for mut f in forks {
                src.merge(&mut f).unwrap();
            }
        }
        src.put(&ROOT, "done", true).unwrap();
        src.commit();
        src.doc.clone()
    }

    #[test]
    fn feeds_of_list_edits_load_as_the_document() {
        let doc = concurrent_edits(40, |d, rng, n| {
            let list = match d.get(&ROOT, "list").unwrap() {
                Some((_, id)) => id,
                None => d.put_object(&ROOT, "list", ObjType::List).unwrap(),
            };
            let len = d.length(&list);
            match rng.random_range(0..4) {
                0 | 1 => d
                    .insert(&list, rng.random_range(0..=len), n as i64)
                    .unwrap(),
                2 if len > 0 => d.put(&list, rng.random_range(0..len), n as i64).unwrap(),
                3 if len > 0 => d.delete(&list, rng.random_range(0..len)).unwrap(),
                _ => d.insert(&list, 0, n as i64).unwrap(),
            }
        });
        check(&doc);
    }

    #[test]
    fn feeds_of_text_and_mark_edits_load_as_the_document() {
        let doc = concurrent_edits(40, |d, rng, n| {
            let text = match d.get(&ROOT, "text").unwrap() {
                Some((_, id)) => id,
                None => d.put_object(&ROOT, "text", ObjType::Text).unwrap(),
            };
            let len = d.length(&text);
            if len > 2 && rng.random_range(0..5) == 0 {
                let a = rng.random_range(0..len);
                let b = rng.random_range(0..len);
                if a != b {
                    let mark = Mark {
                        start: a.min(b),
                        end: a.max(b),
                        name: "bold".into(),
                        value: ScalarValue::from(n as i64),
                    };
                    d.mark(&text, mark, ExpandMark::After).unwrap();
                }
                return;
            }
            let at = rng.random_range(0..=len);
            let del = if at < len {
                rng.random_range(0..2).min(len - at)
            } else {
                0
            };
            d.splice_text(&text, at, del as isize, &format!("[{n}]"))
                .unwrap();
        });
        check(&doc);
    }

    #[test]
    fn feeds_of_map_and_counter_edits_load_as_the_document() {
        let doc = concurrent_edits(40, |d, rng, n| {
            let key = format!("k{}", rng.random_range(0..6));
            match rng.random_range(0..4) {
                0 => d.put(&ROOT, key, ScalarValue::counter(n as i64)).unwrap(),
                1 => {
                    let _ = d.increment(&ROOT, key, 1);
                }
                2 => {
                    let _ = d.delete(&ROOT, key);
                }
                _ => {
                    let m = d.put_object(&ROOT, key, ObjType::Map).unwrap();
                    d.put(&m, "x", n as i64).unwrap();
                }
            }
        });
        check(&doc);
    }

    #[test]
    fn feeds_of_edits_to_separate_objects_load_as_the_document() {
        let doc = concurrent_edits(30, |d, rng, n| {
            let key = format!("t{}", d.get_actor().to_hex_string());
            let text = match d.get(&ROOT, &key).unwrap() {
                Some((_, id)) => id,
                None => d.put_object(&ROOT, key, ObjType::Text).unwrap(),
            };
            let len = d.length(&text);
            let at = rng.random_range(0..=len);
            d.splice_text(&text, at, 0, &format!("<{n}>")).unwrap();
            if len > 4 && rng.random_range(0..3) == 0 {
                d.delete(&text, rng.random_range(0..len)).unwrap();
            }
        });
        check(&doc);
    }

    #[test]
    fn small_edits_scattered_through_a_long_text_load_as_the_document() {
        let mut rng = make_rng();
        let mut src = AutoCommit::new()
            .with_actor(rng.random())
            .enable_audit_mode()
            .unwrap();
        let text = src.put_object(&ROOT, "text", ObjType::Text).unwrap();
        src.splice_text(&text, 0, 0, &"abcdefghij".repeat(200))
            .unwrap();
        src.commit();
        for round in 0..20 {
            let mut forks: Vec<_> = (0..3)
                .map(|_| src.fork().with_actor(rng.random()))
                .collect();
            for f in &mut forks {
                let len = f.length(&text);
                let at = rng.random_range(0..len);
                f.splice_text(
                    &text,
                    at,
                    rng.random_range(0..2i32) as isize,
                    &format!("<{round}>"),
                )
                .unwrap();
                f.commit();
            }
            for mut f in forks {
                src.merge(&mut f).unwrap();
            }
        }
        src.put(&ROOT, "done", true).unwrap();
        src.commit();
        check(&src.doc);
    }
}
