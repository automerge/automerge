use super::core::{record_control, Authority, Eligibility, Input, Session, Transition};
use crate::{
    transaction::Transactable, ActorId, Automerge, Change, ChangeHash, ObjId, ObjType, ReadDoc,
    ROOT,
};
use itertools::Itertools;

fn actor(n: u8) -> ActorId {
    ActorId::from(vec![n])
}
fn put(doc: &mut Automerge, key: &str, value: &str) -> ChangeHash {
    let mut tx = doc.transaction();
    tx.put(ROOT, key, value).unwrap();
    tx.commit().0.unwrap()
}
fn content(doc: &Automerge, hash: ChangeHash) -> Input {
    let c = doc.get_change_by_hash(&hash).unwrap();
    let author = if c.actor_id() == &actor(20) {
        b"alice".to_vec()
    } else {
        b"other".to_vec()
    };
    Input::Content(c, author)
}
fn check_replay(s: &Session, t: &Transition) {
    let mut observer = s.observe(&t.before).unwrap();
    t.apply(&mut observer).unwrap();
    assert_eq!(
        observer.value,
        s.hydrate(&t.after),
        "patches: {:?}",
        t.content
    );
}
fn deliver(s: &mut Session, inputs: Vec<Input>) -> Transition {
    let t = s.deliver(inputs).unwrap();
    check_replay(s, &t);
    t
}
struct Ex01 {
    doc: Automerge,
    h: ChangeHash,
    a: ChangeHash,
    r: ChangeHash,
    c: ChangeHash,
    original: ObjId,
}
fn ex01() -> Ex01 {
    let mut doc = Automerge::new().with_actor(actor(10));
    let h = put(&mut doc, "title", "Trip plan");
    doc.set_actor(actor(20));
    let a = put(&mut doc, "suggestion", "Camping");
    let original = doc.get(ROOT, "suggestion").unwrap().unwrap().1;
    let r = record_control(&mut doc, actor(30), b"alice", vec![h]).unwrap();
    let c = put(&mut doc, "title", "Weekend plan");
    Ex01 {
        doc,
        h,
        a,
        r,
        c,
        original,
    }
}

#[test]
fn ex01_all_1920_schedules_and_duplicate_idempotence() {
    let f = ex01();
    let events = [
        content(&f.doc, f.a),
        content(&f.doc, f.r),
        content(&f.doc, f.c),
        Input::Authorize(f.r),
        Input::Invalidate(f.r),
    ];
    let mut schedules = 0;
    for order in (0..5).permutations(5) {
        for partition in 0..16 {
            schedules += 1;
            let mut s = Session::new();
            deliver(&mut s, vec![content(&f.doc, f.h)]);
            let mut seen = [false; 5];
            let mut group = Vec::new();
            let mut captures = Vec::new();
            for (index, event) in order.iter().copied().enumerate() {
                group.push(events[event].clone());
                seen[event] = true;
                if index != 4 && partition & (1 << index) == 0 {
                    continue;
                }
                let t = deliver(&mut s, std::mem::take(&mut group));
                let capture = s.capture();
                // Independent finite fixture oracle: graph chain A -> R -> C and E2 -> E1.
                let a_ready = seen[0];
                let r_ready = a_ready && seen[1];
                let c_ready = r_ready && seen[2];
                let excludes = seen[1] && seen[3] && !seen[4];
                assert_eq!(
                    s.scalar(&capture, "title").as_deref(),
                    Some(if c_ready { "Weekend plan" } else { "Trip plan" })
                );
                assert_eq!(
                    s.scalar(&capture, "suggestion").as_deref(),
                    if a_ready && !excludes {
                        Some("Camping")
                    } else {
                        None
                    }
                );
                if seen[0] {
                    assert_eq!(
                        capture.eligibility[&f.a],
                        if excludes {
                            Eligibility::Excluded
                        } else {
                            Eligibility::Eligible
                        }
                    );
                }
                if seen[1] {
                    assert_eq!(
                        capture.authority[&f.r],
                        if !seen[3] {
                            Authority::Pending
                        } else if seen[4] {
                            Authority::Invalidated
                        } else {
                            Authority::Authorized
                        }
                    );
                    assert_eq!(capture.integrated.contains(&f.r), r_ready);
                }
                if seen[2] {
                    assert_eq!(capture.eligibility[&f.c], Eligibility::Eligible);
                    assert_eq!(capture.integrated.contains(&f.c), c_ready);
                }
                assert_eq!(capture.missing_evidence.contains(&f.r), seen[4] && !seen[3]);
                captures.push((capture.clone(), s.hydrate(&capture)));
                assert_eq!(t.after, capture);
            }
            let final_capture = s.capture();
            assert_eq!(
                s.candidates(&final_capture, &ROOT, "suggestion")[0].1,
                f.original
            );
            for event in &events {
                let t = deliver(&mut s, vec![event.clone()]);
                assert!(t.content.is_empty());
                assert!(t.status.is_empty());
                assert_eq!(t.before, t.after);
            }
            for (capture, expected) in captures {
                assert_eq!(s.hydrate(&capture), expected);
            }
        }
    }
    assert_eq!(schedules, 1920);
}

#[test]
fn ex04_adopt_excluded_scalar_as_bobs_new_change_without_reinstating_alice() {
    let f = ex01();
    let mut s = Session::new();
    deliver(
        &mut s,
        vec![
            content(&f.doc, f.h),
            content(&f.doc, f.a),
            content(&f.doc, f.r),
            content(&f.doc, f.c),
            Input::Authorize(f.r),
        ],
    );
    let excluded = s.capture();
    assert_eq!(excluded.eligibility[&f.a], Eligibility::Excluded);
    assert_eq!(s.scalar(&excluded, "suggestion"), None);
    assert_eq!(
        s.scalar(&excluded, "title").as_deref(),
        Some("Weekend plan")
    );

    // Inspect original retained work, not the root's eligible projection.
    let source = s.doc.get_change_by_hash(&f.a).unwrap();
    assert_eq!(source.actor_id(), &actor(20));
    let proposed = source.decode().operations[0].primitive_value().unwrap();
    assert_eq!(proposed.as_str(), Some("Camping"));
    let reviewed_source = source.hash();
    let bob = actor(40);
    let (d, t) = s
        .edit(&excluded, bob.clone(), b"bob", |tx| {
            tx.put(ROOT, "suggestion", proposed.clone())
        })
        .unwrap();
    check_replay(&s, &t);
    assert_ne!(d, reviewed_source);
    let adoption = s.doc.get_change_by_hash(&d).unwrap();
    assert_eq!(adoption.actor_id(), &bob);
    assert_eq!(adoption.deps(), excluded.heads.as_slice());
    assert!(adoption.decode().operations[0].pred.is_empty());
    // The mock author binding is an explicit captured input, distinct from actor ID.
    let package = s.export();
    let (_, author) = package
        .content
        .iter()
        .find(|(bytes, _)| Change::from_bytes(bytes.clone()).unwrap().hash() == d)
        .unwrap();
    assert_eq!(author, b"bob");
    let adopted = s.capture();
    assert_eq!(adopted.eligibility[&f.a], Eligibility::Excluded);
    assert_eq!(adopted.eligibility[&d], Eligibility::Eligible);
    assert_eq!(s.scalar(&adopted, "suggestion").as_deref(), Some("Camping"));
    assert_eq!(s.scalar(&adopted, "title").as_deref(), Some("Weekend plan"));
    let adoption_id = s.candidates(&adopted, &ROOT, "suggestion")[0].1.clone();
    assert_ne!(adoption_id, f.original);
    assert_eq!(s.doc.hash_for_opid(&adoption_id), Some(d));
    assert_eq!(
        s.doc
            .get_change_by_hash(&reviewed_source)
            .unwrap()
            .raw_bytes(),
        source.raw_bytes()
    );

    // Restoring A does not give D a new predecessor: equal scalars still conflict by identity.
    deliver(&mut s, vec![Input::Invalidate(f.r)]);
    let restored = s.capture();
    assert_eq!(restored.eligibility[&f.a], Eligibility::Eligible);
    let candidates = s.candidates(&restored, &ROOT, "suggestion");
    assert_eq!(
        candidates
            .iter()
            .map(|(_, id)| id.clone())
            .collect::<Vec<_>>(),
        vec![f.original, adoption_id]
    );
    assert!(candidates
        .iter()
        .all(|(value, _)| value.as_str() == Some("Camping")));
    assert_eq!(s.scalar(&excluded, "suggestion"), None);
    assert_eq!(s.candidates(&adopted, &ROOT, "suggestion").len(), 1);
}

#[test]
fn native_action_diagnostic_includes_enabled_experimental_range() {
    let error = crate::op_set2::types::Action::try_from(9)
        .unwrap_err()
        .to_string();
    assert!(error.contains("between 0 and 8"), "{error}");
}

#[test]
fn ex01_status_only_and_earlier_actor_live_reread() {
    let mut f = ex01();
    let mut tx = f.doc.transaction();
    tx.delete(ROOT, "suggestion").unwrap();
    let d = tx.commit().0.unwrap();
    let mut s = Session::new();
    deliver(
        &mut s,
        vec![
            content(&f.doc, f.h),
            content(&f.doc, f.a),
            content(&f.doc, f.r),
            content(&f.doc, f.c),
            content(&f.doc, d),
        ],
    );
    let frozen = s.capture();
    let value = s.hydrate(&frozen);
    let t = deliver(&mut s, vec![Input::Authorize(f.r)]);
    assert!(t.content.is_empty());
    assert!(t.status.contains(&f.a));
    assert!(t.status.contains(&f.r));
    let t = deliver(&mut s, vec![Input::Invalidate(f.r)]);
    assert!(t.content.is_empty());
    assert!(t.status.contains(&f.a));
    let mut early = Automerge::new().with_actor(actor(1));
    let x = put(&mut early, "new", "actor");
    deliver(&mut s, vec![content(&early, x)]);
    assert_eq!(s.hydrate(&frozen), value);
    let foreign = Session::new();
    assert!(foreign.scope(&frozen).is_err());
}

fn queued_control_known_frontier(valid_dependency: bool) {
    let f = ex01();
    let mut dependency = if valid_dependency {
        f.doc.fork_at(&[f.h]).unwrap()
    } else {
        Automerge::new()
    };
    dependency.set_actor(actor(50));
    let x = dependency.empty_commit(Default::default());
    let mut expanded = f.doc.get_change_by_hash(&f.r).unwrap().decode();
    expanded.deps = vec![x];
    let control: Change = expanded.into();
    let r = control.hash();
    let mut s = Session::new();
    deliver(&mut s, vec![content(&f.doc, f.h), content(&f.doc, f.a)]);
    // Receipt alone is not credible authority and cannot hide Alice's work.
    deliver(&mut s, vec![Input::Content(control, b"other".to_vec())]);
    let unverified = s.capture();
    assert_eq!(unverified.authority[&r], Authority::Pending);
    assert_eq!(unverified.eligibility[&f.a], Eligibility::Eligible);
    assert_eq!(
        s.scalar(&unverified, "suggestion").as_deref(),
        Some("Camping")
    );

    let t = deliver(&mut s, vec![Input::Authorize(r)]);
    let queued = s.capture();
    assert_eq!(queued.authority[&r], Authority::Authorized);
    assert!(!queued.integrated.contains(&r));
    assert!(queued.integrated.contains(&f.h));
    assert_eq!(
        queued.missing_dependencies[&r],
        std::collections::BTreeSet::from([x])
    );
    assert!(!queued.unresolved_frontiers.contains_key(&r)); // H is known, not missing.
    assert_eq!(queued.eligibility[&f.a], Eligibility::Pending);
    assert_eq!(
        queued.reasons[&f.a],
        super::policy::Reason::AwaitingControlValidation(r)
    );
    assert_eq!(s.scalar(&queued, "suggestion"), None); // Pending effects are hidden.
    assert!(t.status.contains(&f.a));
    let restored = s.reconstruct().unwrap();
    assert_eq!(restored.capture(), queued);

    if valid_dependency {
        let t = deliver(&mut s, vec![content(&dependency, x)]);
        let integrated = s.capture();
        assert!(integrated.integrated.contains(&r));
        assert_eq!(integrated.authority[&r], Authority::Authorized);
        assert_eq!(integrated.eligibility[&f.a], Eligibility::Excluded);
        assert_eq!(
            integrated.reasons[&f.a],
            super::policy::Reason::OutsideFrontier(r)
        );
        assert_eq!(s.scalar(&integrated, "suggestion"), None);
        assert!(t.content.is_empty()); // Pending -> excluded is status-only.
        assert!(t.status.contains(&f.a));
    } else {
        let mut independent = Automerge::new().with_actor(actor(60));
        let d = put(&mut independent, "independent", "must not publish");
        for _ in 0..2 {
            let error = s
                .deliver(vec![content(&independent, d), content(&dependency, x)])
                .err()
                .unwrap();
            assert_eq!(error, "frontier is not dependency ancestry");
            assert_eq!(s.capture(), queued);
            assert!(s.doc.get_change_by_hash(&x).is_none());
            assert!(s.doc.get_change_by_hash(&d).is_none());
            assert_eq!(s.scalar(&s.capture(), "suggestion"), None);
        }
        // No invented quarantine: the invalid queued R remains pending, not excluded.
        assert_eq!(s.capture().eligibility[&f.a], Eligibility::Pending);
    }
    assert_eq!(
        s.scalar(&unverified, "suggestion").as_deref(),
        Some("Camping")
    );
    assert_eq!(s.scalar(&queued, "suggestion"), None);
    assert_eq!(queued.eligibility[&f.a], Eligibility::Pending);
}

#[test]
fn native_queued_control_waits_for_valid_dependency_before_exclusion() {
    queued_control_known_frontier(true);
}

#[test]
fn native_queued_control_invalid_dependency_rejection_preserves_pending_capture() {
    queued_control_known_frontier(false);
}

#[test]
fn native_gate_strict_group_rejection_and_ancestry() {
    let f = ex01();
    let mut s = Session::new();
    deliver(&mut s, vec![content(&f.doc, f.h)]);
    let before = s.capture();
    let value = s.hydrate(&before);
    let bytes = f.doc.get_change_by_hash(&f.a).unwrap().raw_bytes().to_vec();
    assert!(s
        .receive(0, vec![(bytes.clone(), b"alice".to_vec())])
        .is_err());
    assert_eq!(s.capture(), before);
    assert!(s
        .receive(
            1,
            vec![
                (bytes.clone(), b"alice".to_vec()),
                (vec![0], b"other".to_vec())
            ]
        )
        .is_err());
    assert_eq!(s.capture(), before);
    assert_eq!(s.hydrate(&before), value);
    let mut bad = f.doc.get_change_by_hash(&f.r).unwrap().decode();
    bad.deps = vec![];
    let bad: Change = bad.into();
    assert!(s
        .deliver(vec![
            content(&f.doc, f.a),
            Input::Content(bad, b"other".to_vec()),
            Input::Authorize(f.r)
        ])
        .is_err());
    assert_eq!(s.capture(), before); // ancestry validation did not publish earlier staged A
    let mut mixed = f.doc.get_change_by_hash(&f.r).unwrap().decode();
    mixed
        .operations
        .push(f.doc.get_change_by_hash(&f.a).unwrap().decode().operations[0].clone());
    assert!(s
        .deliver(vec![Input::Content(mixed.into(), b"other".to_vec())])
        .is_err());
    assert_eq!(s.capture(), before);
    let t = s.receive(1, vec![(bytes, b"alice".to_vec())]).unwrap();
    check_replay(&s, &t);
    assert_eq!(
        s.scalar(&s.capture(), "suggestion").as_deref(),
        Some("Camping")
    );
}

#[test]
fn native_control_batched_integration_and_incremental_bytes_preserve_identity() {
    let f = ex01();
    let mut batched = Automerge::new();
    let mut log = crate::PatchLog::active();
    batched
        .apply_changes_log_patches(f.doc.get_changes(&[]), &mut log)
        .unwrap();
    let mut value = Automerge::new().hydrate(None);
    value
        .apply_patches(batched.text_encoding(), batched.make_patches(&mut log))
        .unwrap();
    assert_eq!(value, f.doc.hydrate(None));
    let mut incremental = Automerge::new();
    let bytes: Vec<_> = f
        .doc
        .get_changes(&[])
        .iter()
        .flat_map(|c| c.raw_bytes().to_vec())
        .collect();
    incremental.load_incremental(&bytes).unwrap();
    assert_eq!(incremental.get_heads(), f.doc.get_heads());
    assert_eq!(
        incremental.get_change_by_hash(&f.r).unwrap().raw_bytes(),
        f.doc.get_change_by_hash(&f.r).unwrap().raw_bytes()
    );
    assert_eq!(incremental.hydrate(None), f.doc.hydrate(None));
    // These ordinary paths preserve statements, NOT captured external authority.
    let mut s = Session::new();
    let inputs = f
        .doc
        .get_changes(&[])
        .into_iter()
        .map(|c| content(&f.doc, c.hash()))
        .collect();
    deliver(&mut s, inputs);
    deliver(&mut s, vec![Input::Authorize(f.r)]);
    assert_eq!(s.scalar(&s.capture(), "suggestion"), None);
    let content_only = Automerge::load(&s.doc.save()).unwrap();
    assert_eq!(
        content_only
            .get(ROOT, "suggestion")
            .unwrap()
            .unwrap()
            .0
            .as_str(),
        Some("Camping")
    );
}

#[test]
fn native_control_is_not_an_empty_key_candidate_or_predecessor() {
    let mut doc = Automerge::new().with_actor(actor(10));
    let h = put(&mut doc, "", "base");
    let mut concurrent = doc.fork();
    concurrent.set_actor(actor(40));
    let x = put(&mut concurrent, "", "concurrent");
    let r = record_control(&mut doc, actor(30), b"unknown", vec![h]).unwrap();
    doc.merge(&mut concurrent).unwrap();
    assert_eq!(doc.get_all(ROOT, "").unwrap().len(), 1);
    let n = put(&mut doc, "", "after-control");
    let op = doc.get_change_by_hash(&n).unwrap().decode().operations[0].clone();
    assert_eq!(op.pred.len(), 1);
    assert_eq!(op.pred.get(0).unwrap().actor(), &actor(40));
    assert_eq!(doc.get_all(ROOT, "").unwrap().len(), 1);
    let mut s = Session::new();
    deliver(
        &mut s,
        vec![
            content(&doc, h),
            content(&doc, r),
            content(&doc, x),
            content(&doc, n),
            Input::Authorize(r),
        ],
    );
    assert_eq!(s.scalar(&s.capture(), "").as_deref(), Some("after-control"));
}

#[test]
fn unknown_target_is_reclassified_when_its_author_arrives() {
    let mut base = Automerge::new().with_actor(actor(10));
    let h = base.empty_commit(Default::default());
    let mut alice = base.fork();
    alice.set_actor(actor(20));
    let a = put(&mut alice, "late", "work");
    let r = record_control(&mut base, actor(30), b"alice", vec![h]).unwrap();
    let mut s = Session::new();
    deliver(
        &mut s,
        vec![content(&base, h), content(&base, r), Input::Authorize(r)],
    );
    deliver(&mut s, vec![content(&alice, a)]);
    assert_eq!(s.capture().eligibility[&a], Eligibility::Excluded);
    assert_eq!(s.scalar(&s.capture(), "late"), None);
    deliver(&mut s, vec![Input::Invalidate(r)]);
    assert_eq!(s.scalar(&s.capture(), "late").as_deref(), Some("work"));
}

#[test]
fn evidence_only_receipt_is_a_status_transition_even_before_control() {
    let f = ex01();
    let mut s = Session::new();
    let before = s.capture();
    let t = deliver(&mut s, vec![Input::Invalidate(f.r)]);
    assert!(t.content.is_empty());
    assert!(!t.status.is_empty());
    assert_ne!(t.after, before);
    let t = deliver(&mut s, vec![Input::Authorize(f.r)]);
    assert!(t.content.is_empty());
    assert!(!t.status.is_empty());
    let t = deliver(&mut s, vec![Input::Grant(9)]);
    assert!(t.content.is_empty());
    assert!(!t.status.is_empty());
}

#[test]
fn native_control_roundtrip_empty_key_and_control_only_actor() {
    let mut doc = Automerge::new().with_actor(actor(10));
    let h = put(&mut doc, "", "ordinary");
    let before = doc.hydrate(None);
    let r = record_control(&mut doc, actor(30), b"unknown-author", vec![h]).unwrap();
    assert_eq!(doc.hydrate(None), before);
    assert_eq!(doc.get_all(ROOT, "").unwrap().len(), 1);
    assert!(doc.diff(&[h], &[r]).is_empty());
    let change = doc.get_change_by_hash(&r).unwrap();
    assert_eq!(change.len(), 1);
    assert_eq!(change.iter_ops().next().unwrap().action, 8);
    let decoded = Change::from_bytes(change.raw_bytes().to_vec()).unwrap();
    assert_eq!(decoded.hash(), r);
    assert!(matches!(
        decoded.decode().operations[0].action,
        crate::legacy::OpType::Revoke(_)
    ));
    let loaded = Automerge::load(&doc.save()).unwrap();
    assert_eq!(loaded.hydrate(None), before);
    assert_eq!(
        loaded.get_change_by_hash(&r).unwrap().raw_bytes(),
        change.raw_bytes()
    );
    assert!(crate::anonymize::anonymize(&loaded).is_err());
    let mut only = Automerge::new();
    let r = record_control(&mut only, actor(30), b"nobody", vec![]).unwrap();
    let loaded = Automerge::load(&only.save()).unwrap();
    assert_eq!(loaded.get_change_by_hash(&r).unwrap().len(), 1);
    assert_eq!(loaded.hydrate(None), Automerge::new().hydrate(None));
    // Gate-disabled regression consumes these actual native bytes.
    assert_eq!(hex::encode(only.get_change_by_hash(&r).unwrap().raw_bytes()), "856f4a834cacdedd012a00011e0101000000061502340142025603570b70027f00017f087fb7010106006e6f626f647900007f00");
}

#[test]
fn ex02_all_192_schedules() {
    let mut doc = Automerge::new().with_actor(actor(10));
    let h0 = doc.empty_commit(Default::default());
    doc.set_actor(actor(20));
    let a1 = put(&mut doc, "x", "1");
    let mut branch = doc.fork();
    branch.set_actor(actor(30));
    let h = branch.empty_commit(Default::default());
    let r = record_control(&mut branch, actor(40), b"alice", vec![h]).unwrap();
    let a2 = put(&mut doc, "y", "2");
    let events = [
        vec![content(&doc, a1)],
        vec![content(&doc, a2)],
        vec![content(&branch, h)],
        vec![content(&branch, r), Input::Authorize(r)],
    ];
    let mut schedules = 0;
    for order in (0..4).permutations(4) {
        for partition in 0..8 {
            schedules += 1;
            let mut s = Session::new();
            deliver(&mut s, vec![content(&doc, h0)]);
            let mut seen = [false; 4];
            let mut group = Vec::new();
            let mut frozen = Vec::new();
            for (i, event) in order.iter().copied().enumerate() {
                seen[event] = true;
                group.extend(events[event].clone());
                if i != 3 && partition & (1 << i) == 0 {
                    continue;
                }
                deliver(&mut s, std::mem::take(&mut group));
                let c = s.capture();
                let boundary = seen[0] && seen[2];
                let a1_status = if !seen[3] {
                    Eligibility::Eligible
                } else if boundary {
                    Eligibility::Eligible
                } else {
                    Eligibility::Pending
                };
                let a2_status = if !seen[3] {
                    Eligibility::Eligible
                } else if boundary {
                    Eligibility::Excluded
                } else {
                    Eligibility::Pending
                };
                if seen[0] {
                    assert_eq!(c.eligibility[&a1], a1_status);
                }
                if seen[1] {
                    assert_eq!(c.eligibility[&a2], a2_status);
                }
                assert_eq!(
                    s.scalar(&c, "x").as_deref(),
                    if seen[0] && a1_status == Eligibility::Eligible {
                        Some("1")
                    } else {
                        None
                    }
                );
                assert_eq!(
                    s.scalar(&c, "y").as_deref(),
                    if seen[0] && seen[1] && a2_status == Eligibility::Eligible {
                        Some("2")
                    } else {
                        None
                    }
                );
                frozen.push((c.clone(), s.hydrate(&c)));
            }
            for (c, v) in frozen {
                assert_eq!(s.hydrate(&c), v);
            }
        }
    }
    assert_eq!(schedules, 192);
}

#[test]
fn ex03_all_1920_schedules_preserve_context_gap() {
    let mut doc = Automerge::new().with_actor(actor(20));
    let a1 = put(&mut doc, "before", "yes");
    let mut branch = doc.fork();
    let r = record_control(&mut branch, actor(30), b"alice", vec![a1]).unwrap();
    let a2 = put(&mut doc, "during", "no");
    let a3 = put(&mut doc, "after", "yes");
    let events = [
        vec![content(&doc, a1)],
        vec![content(&doc, a2)],
        vec![content(&doc, a3), Input::Context(a3, 7)],
        vec![content(&branch, r), Input::Authorize(r)],
        vec![Input::Grant(7)],
    ];
    let mut schedules = 0;
    for order in (0..5).permutations(5) {
        for partition in 0..16 {
            schedules += 1;
            let mut s = Session::new();
            let mut seen = [false; 5];
            let mut group = Vec::new();
            let mut frozen = Vec::new();
            for (i, event) in order.iter().copied().enumerate() {
                seen[event] = true;
                group.extend(events[event].clone());
                if i != 4 && partition & (1 << i) == 0 {
                    continue;
                }
                deliver(&mut s, std::mem::take(&mut group));
                let c = s.capture();
                let a1_status = if seen[3] && !seen[0] {
                    Eligibility::Pending
                } else {
                    Eligibility::Eligible
                };
                let a2_status = if !seen[3] {
                    Eligibility::Eligible
                } else if !seen[0] {
                    Eligibility::Pending
                } else {
                    Eligibility::Excluded
                };
                let a3_status = if seen[4] {
                    Eligibility::Eligible
                } else {
                    Eligibility::Pending
                };
                if seen[0] {
                    assert_eq!(c.eligibility[&a1], a1_status);
                }
                if seen[1] {
                    assert_eq!(c.eligibility[&a2], a2_status);
                }
                if seen[2] {
                    assert_eq!(c.eligibility[&a3], a3_status);
                }
                assert_eq!(
                    s.scalar(&c, "before").as_deref(),
                    if seen[0] { Some("yes") } else { None }
                );
                assert_eq!(
                    s.scalar(&c, "during").as_deref(),
                    if seen[0] && seen[1] && !seen[3] {
                        Some("no")
                    } else {
                        None
                    }
                );
                assert_eq!(
                    s.scalar(&c, "after").as_deref(),
                    if seen[0] && seen[1] && seen[2] && seen[4] {
                        Some("yes")
                    } else {
                        None
                    }
                );
                frozen.push((c.clone(), s.hydrate(&c)));
            }
            for (c, v) in frozen {
                assert_eq!(s.hydrate(&c), v);
            }
        }
    }
    assert_eq!(schedules, 1920);
}

#[test]
fn ex02_queued_boundary_freezes_pending_and_isolated_content() {
    let mut doc = Automerge::new().with_actor(actor(10));
    let h0 = doc.empty_commit(Default::default());
    doc.set_actor(actor(20));
    let a1 = put(&mut doc, "x", "1");
    let mut branch = doc.fork();
    branch.set_actor(actor(30));
    let h = branch.empty_commit(Default::default());
    let r = record_control(&mut branch, actor(40), b"alice", vec![h]).unwrap();
    let a2 = put(&mut doc, "y", "2");
    let mut s = Session::new();
    deliver(
        &mut s,
        vec![content(&doc, h0), content(&doc, a1), content(&doc, a2)],
    );
    let isolated_heads = s.capture().heads;
    let t = deliver(&mut s, vec![content(&branch, r), Input::Authorize(r)]);
    let pending = s.capture();
    assert!(!pending.integrated.contains(&r));
    assert!(pending.integrated.contains(&a1));
    assert_eq!(pending.eligibility[&a1], Eligibility::Pending);
    assert_eq!(pending.eligibility[&a2], Eligibility::Pending);
    assert!(s.candidates(&pending, &ROOT, "x").is_empty());
    assert!(t.status.contains(&a1));
    assert!(t.status.contains(&a2));
    let t = deliver(&mut s, vec![content(&branch, h)]);
    let resolved = s.capture();
    assert_eq!(resolved.eligibility[&a1], Eligibility::Eligible);
    assert_eq!(resolved.eligibility[&a2], Eligibility::Excluded);
    assert_eq!(s.scalar(&resolved, "x").as_deref(), Some("1"));
    assert_eq!(s.scalar(&resolved, "y"), None);
    assert!(t.status.contains(&a1));
    assert!(t.status.contains(&a2));
    assert_eq!(s.scalar(&pending, "x"), None);
    let mut isolated = resolved.clone();
    isolated.heads = isolated_heads;
    assert_eq!(s.scalar(&isolated, "x").as_deref(), Some("1"));
    assert_eq!(s.scalar(&isolated, "y"), None);
}

#[test]
fn capture_export_restores_queued_content_and_frozen_policy() {
    let f = ex01();
    let mut s = Session::new();
    deliver(
        &mut s,
        vec![
            content(&f.doc, f.h),
            content(&f.doc, f.c),
            Input::Invalidate(f.r),
        ],
    );
    let frozen = s.capture();
    let package = s.export();
    deliver(
        &mut s,
        vec![
            content(&f.doc, f.a),
            content(&f.doc, f.r),
            Input::Authorize(f.r),
        ],
    );
    let mut restored = Session::restore(package.clone()).unwrap();
    assert_eq!(restored.capture(), frozen);
    assert_eq!(restored.hydrate(&frozen), s.hydrate(&frozen));
    assert!(!restored.capture().integrated.contains(&f.c));
    deliver(
        &mut restored,
        vec![
            content(&f.doc, f.a),
            content(&f.doc, f.r),
            Input::Authorize(f.r),
        ],
    );
    assert_eq!(restored.capture(), s.capture());
    assert_eq!(restored.scalar(&frozen, "suggestion"), None);
    let mut damaged = package;
    damaged.content.pop();
    assert!(Session::restore(damaged).is_err());
}

#[test]
fn ex02_restored_object_boundary_edit_is_not_duplicated() {
    let mut doc = Automerge::new().with_actor(actor(10));
    let h0 = doc.empty_commit(Default::default());
    doc.set_actor(actor(20));
    let mut tx = doc.transaction();
    let m = tx.put_object(ROOT, "section", ObjType::Map).unwrap();
    tx.put(&m, "x", "1").unwrap();
    let a1 = tx.commit().0.unwrap();
    let mut branch = doc.fork();
    branch.set_actor(actor(30));
    let mut tx = branch.transaction();
    tx.put(&m, "boundary", "yes").unwrap();
    let h = tx.commit().0.unwrap();
    let r = record_control(&mut branch, actor(40), b"alice", vec![h]).unwrap();
    let a2 = put(&mut doc, "y", "2");
    let mut s = Session::new();
    deliver(
        &mut s,
        vec![
            content(&doc, h0),
            content(&doc, a1),
            content(&doc, a2),
            content(&branch, r),
            Input::Authorize(r),
        ],
    );
    assert!(s.candidates(&s.capture(), &ROOT, "section").is_empty());
    let t = deliver(&mut s, vec![content(&branch, h)]);
    let child_puts = t.content.iter().filter(|p| p.obj == m).count();
    assert_eq!(child_puts, 2);
    assert_eq!(
        s.candidates(&s.capture(), &m, "boundary")[0].0.as_str(),
        Some("yes")
    );
}

#[test]
fn ex06_excluded_delete_restores_original_then_invalidation_hides_it() {
    let mut doc = Automerge::new().with_actor(actor(10));
    let a = put(&mut doc, "title", "Original");
    doc.set_actor(actor(20));
    let mut tx = doc.transaction();
    tx.delete(ROOT, "title").unwrap();
    let b = tx.commit().0.unwrap();
    let r = record_control(&mut doc, actor(30), b"alice", vec![a]).unwrap();
    let mut s = Session::new();
    deliver(
        &mut s,
        vec![
            content(&doc, a),
            content(&doc, b),
            content(&doc, r),
            Input::Authorize(r),
        ],
    );
    assert_eq!(s.scalar(&s.capture(), "title").as_deref(), Some("Original"));
    deliver(&mut s, vec![Input::Invalidate(r)]);
    assert_eq!(s.scalar(&s.capture(), "title"), None);
}

#[test]
fn ex03_fresh_context_is_pending_until_its_grant_resolves() {
    let mut doc = Automerge::new().with_actor(actor(20));
    let a1 = put(&mut doc, "before", "yes");
    let mut branch = doc.fork();
    let r = record_control(&mut branch, actor(30), b"alice", vec![a1]).unwrap();
    let a2 = put(&mut doc, "during", "no");
    let a3 = put(&mut doc, "after", "yes");
    let mut s = Session::new();
    deliver(
        &mut s,
        vec![
            content(&doc, a1),
            content(&doc, a2),
            content(&doc, a3),
            Input::Context(a3, 7),
            content(&branch, r),
            Input::Authorize(r),
        ],
    );
    let pending = s.capture();
    assert_eq!(pending.eligibility[&a3], Eligibility::Pending);
    assert_eq!(s.scalar(&pending, "after"), None);
    deliver(&mut s, vec![Input::Grant(7)]);
    assert_eq!(s.scalar(&s.capture(), "after").as_deref(), Some("yes"));
    assert_eq!(s.scalar(&pending, "after"), None);
}

#[test]
fn ex03_fresh_context_keeps_same_actor_hole_with_late_a2() {
    let mut doc = Automerge::new().with_actor(actor(20));
    let a1 = put(&mut doc, "before", "yes");
    let mut control_doc = doc.fork();
    let r = record_control(&mut control_doc, actor(30), b"alice", vec![a1]).unwrap();
    let a2 = put(&mut doc, "during", "no");
    let a3 = put(&mut doc, "after", "yes");
    let mut s = Session::new();
    deliver(
        &mut s,
        vec![
            content(&doc, a1),
            content(&control_doc, r),
            Input::Authorize(r),
        ],
    );
    deliver(
        &mut s,
        vec![content(&doc, a3), Input::Context(a3, 7), Input::Grant(7)],
    );
    assert!(!s.capture().integrated.contains(&a3));
    deliver(&mut s, vec![content(&doc, a2)]);
    let capture = s.capture();
    assert_eq!(capture.eligibility[&a1], Eligibility::Eligible);
    assert_eq!(capture.eligibility[&a2], Eligibility::Excluded);
    assert_eq!(capture.eligibility[&a3], Eligibility::Eligible);
    assert_eq!(s.scalar(&capture, "before").as_deref(), Some("yes"));
    assert_eq!(s.scalar(&capture, "during"), None);
    assert_eq!(s.scalar(&capture, "after").as_deref(), Some("yes"));
    let restored = s.reconstruct().unwrap();
    assert_eq!(restored.hydrate(&capture), s.hydrate(&capture));
}

fn direct_chain(delete: bool) -> (Session, ChangeHash, ObjId, Option<ObjId>) {
    let mut doc = Automerge::new().with_actor(actor(10));
    let a = put(&mut doc, "title", "Original");
    let original = doc.get(ROOT, "title").unwrap().unwrap().1;
    doc.set_actor(actor(20));
    let b = put(&mut doc, "title", "Spam");
    doc.set_actor(actor(40));
    let c = if delete {
        let mut tx = doc.transaction();
        tx.delete(ROOT, "title").unwrap();
        tx.commit().0.unwrap()
    } else {
        put(&mut doc, "title", "Revised")
    };
    let revised = doc.get(ROOT, "title").unwrap().map(|v| v.1);
    let r = record_control(&mut doc, actor(30), b"alice", vec![a]).unwrap();
    let mut s = Session::new();
    deliver(
        &mut s,
        vec![
            content(&doc, a),
            content(&doc, b),
            content(&doc, c),
            content(&doc, r),
            Input::Authorize(r),
        ],
    );
    (s, r, original, revised)
}
#[test]
fn ex05_direct_target_conflict_not_transitive_suppression() {
    let (mut s, r, a, c) = direct_chain(false);
    assert_eq!(
        s.candidates(&s.capture(), &ROOT, "title")
            .iter()
            .map(|v| v.1.clone())
            .collect::<Vec<_>>(),
        vec![a, c.clone().unwrap()]
    );
    deliver(&mut s, vec![Input::Invalidate(r)]);
    assert_eq!(
        s.candidates(&s.capture(), &ROOT, "title")
            .iter()
            .map(|v| v.1.clone())
            .collect::<Vec<_>>(),
        vec![c.unwrap()]
    );
}
#[test]
fn ex06_eligible_delete_targets_only_excluded_replacement() {
    let (s, _, a, _) = direct_chain(true);
    assert_eq!(s.candidates(&s.capture(), &ROOT, "title")[0].1, a);
    assert_eq!(s.scalar(&s.capture(), "title").as_deref(), Some("Original"));
}

proptest::proptest! {
    #![proptest_config(proptest::test_runner::Config::with_cases(48))]
    #[test]
    fn model_nonprefix_register_candidates_are_suppressed_only_by_direct_eligible_successors(mask in proptest::collection::vec(proptest::bool::ANY,1..18)) {
        let mut doc=Automerge::new().with_actor(actor(10));let base=put(&mut doc,"title","base");
        let mut hashes=vec![base];let mut ids=vec![doc.get(ROOT,"title").unwrap().unwrap().1];
        doc.set_actor(actor(20));
        for i in 0..mask.len() { hashes.push(put(&mut doc,"title",&format!("v{i}")));ids.push(doc.get(ROOT,"title").unwrap().unwrap().1); }
        let r=record_control(&mut doc,actor(30),b"alice",vec![base]).unwrap();
        let mut inputs:Vec<_>=hashes.iter().copied().map(|h|content(&doc,h)).collect();inputs.push(content(&doc,r));inputs.push(Input::Authorize(r));inputs.push(Input::Grant(7));
        for (i,eligible) in mask.iter().enumerate() { if *eligible {inputs.push(Input::Context(hashes[i+1],7));} }
        let mut s=Session::new();deliver(&mut s,inputs);
        let eligible:Vec<_>=std::iter::once(true).chain(mask.iter().copied()).collect();
        let expected:Vec<_>=ids.iter().enumerate().filter(|(i,_)|eligible[*i]&&!eligible.get(i+1).copied().unwrap_or(false)).map(|(_,id)|id.clone()).collect();
        let actual:Vec<_>=s.candidates(&s.capture(),&ROOT,"title").into_iter().map(|(_,id)|id).collect();
        proptest::prop_assert_eq!(actual,expected);
        let frozen=s.capture();let restored=s.reconstruct().unwrap();proptest::prop_assert_eq!(s.hydrate(&frozen),restored.hydrate(&frozen));
        deliver(&mut s,vec![Input::Invalidate(r)]);
        proptest::prop_assert_eq!(s.candidates(&s.capture(),&ROOT,"title")[0].1.clone(),ids.last().unwrap().clone());
    }
}

#[test]
fn capture_scope_rejects_unknown_content_heads() {
    let mut s = Session::new();
    let mut foreign = Automerge::new().with_actor(actor(10));
    let h = put(&mut foreign, "foreign", "yes");
    let mut capture = s.capture();
    capture.heads = vec![h];
    assert!(s.scope(&capture).is_err());
    // An invalid captured authoring context must not mutate the graph.
    let before = s.capture();
    assert!(s
        .edit(&capture, actor(50), b"bob", |tx| tx.put(ROOT, "x", 1))
        .is_err());
    assert_eq!(s.capture(), before);
}

#[test]
fn patch_observer_rejects_foreign_and_stale_endpoints_atomically() {
    let f = ex01();
    let mut s = Session::new();
    deliver(&mut s, vec![content(&f.doc, f.h)]);
    let mut observer = s.observe(&s.capture()).unwrap();
    let before = observer.clone();
    let t = deliver(&mut s, vec![content(&f.doc, f.a)]);
    t.apply(&mut observer).unwrap();
    assert_eq!(observer.value, s.hydrate(&s.capture()));
    let after = observer.clone();
    assert!(t.apply(&mut observer).is_err());
    assert_eq!(observer, after);
    let foreign = Session::new();
    let mut other = foreign.observe(&foreign.capture()).unwrap();
    let unchanged = other.clone();
    assert!(t.apply(&mut other).is_err());
    assert_eq!(other, unchanged);
    assert_ne!(before, observer);
}

#[test]
fn unsupported_structural_content_and_competing_controls_fail_at_session_boundary() {
    let mut doc = Automerge::new().with_actor(actor(10));
    let mut tx = doc.transaction();
    tx.put_object(ROOT, "list", ObjType::List).unwrap();
    let a = tx.commit().0.unwrap();
    let mut s = Session::new();
    let before = s.capture();
    assert!(s.deliver(vec![content(&doc, a)]).is_err());
    assert_eq!(s.capture(), before);
    let f = ex01();
    deliver(
        &mut s,
        vec![
            content(&f.doc, f.h),
            content(&f.doc, f.a),
            content(&f.doc, f.r),
        ],
    );
    let before = s.capture();
    let mut other = f.doc.clone();
    let r2 = record_control(&mut other, actor(90), b"alice", vec![f.h]).unwrap();
    assert!(s
        .deliver(vec![content(&f.doc, f.c), content(&other, r2)])
        .is_err());
    assert_eq!(s.capture(), before);
}

#[test]
fn inspection_explains_missing_frontier_and_rejects_changed_provenance_on_restore() {
    let f = ex01();
    let mut s = Session::new();
    deliver(
        &mut s,
        vec![
            content(&f.doc, f.h),
            content(&f.doc, f.c),
            Input::Invalidate(f.r),
        ],
    );
    let capture = s.capture();
    assert_eq!(
        capture.missing_dependencies[&f.c],
        std::collections::BTreeSet::from([f.r])
    );
    let mut package = s.export();
    package.content[0].1 = b"forged-other-author".to_vec();
    assert!(Session::restore(package).is_err());
}

#[test]
fn selected_authoring_targets_observed_candidates_and_allows_unreachable_objects() {
    let (mut s, _, original, _) = direct_chain(true);
    let capture = s.capture();
    let (hash, t) = s
        .edit(&capture, actor(5), b"bob", |tx| {
            tx.delete(ROOT, "title")?;
            tx.put(ROOT, "local", "first")?;
            assert_eq!(tx.get(ROOT, "local")?.unwrap().0.as_str(), Some("first"));
            tx.put(ROOT, "local", "second")?;
            Ok(())
        })
        .unwrap();
    check_replay(&s, &t);
    assert_eq!(s.scalar(&s.capture(), "title"), None);
    let change = s.doc.get_change_by_hash(&hash).unwrap();
    assert_eq!(change.deps(), capture.heads.as_slice());
    assert_eq!(
        change.decode().operations[0]
            .pred
            .iter()
            .map(|p| p.to_string())
            .collect::<Vec<_>>(),
        vec![original.to_string()]
    );
    assert_eq!(change.decode().operations[2].pred.len(), 1);

    let mut doc = Automerge::new().with_actor(actor(20));
    let mut tx = doc.transaction();
    let m = tx.put_object(ROOT, "section", ObjType::Map).unwrap();
    let a = tx.commit().0.unwrap();
    let r = record_control(&mut doc, actor(30), b"alice", vec![]).unwrap();
    let mut s = Session::new();
    deliver(
        &mut s,
        vec![content(&doc, a), content(&doc, r), Input::Authorize(r)],
    );
    let cap = s.capture();
    let (_, t) = s
        .edit(&cap, actor(5), b"bob", |tx| {
            tx.put(&m, "title", "unreachable")
        })
        .unwrap();
    check_replay(&s, &t);
    assert!(t.content.is_empty());
    assert_eq!(
        s.candidates(&s.capture(), &m, "title")[0].0.as_str(),
        Some("unreachable")
    );
    assert!(s.candidates(&s.capture(), &ROOT, "section").is_empty());
}

#[test]
fn selected_authoring_does_not_admit_unobserved_actor_history() {
    let mut doc = Automerge::new().with_actor(actor(40));
    let h = put(&mut doc, "base", "yes");
    let mut s = Session::new();
    deliver(&mut s, vec![content(&doc, h)]);
    let before = s.capture();
    let late = put(&mut doc, "unobserved", "no");
    deliver(&mut s, vec![content(&doc, late)]);
    let (d, t) = s
        .edit(&before, actor(40), b"other", |tx| {
            assert!(tx.get(ROOT, "unobserved")?.is_none());
            tx.put(ROOT, "observed", "yes")
        })
        .unwrap();
    check_replay(&s, &t);
    let change = s.doc.get_change_by_hash(&d).unwrap();
    assert_eq!(change.deps(), &[h]);
    assert_ne!(change.actor_id(), &actor(40));
}

#[test]
fn ex07_restored_container_exposes_preexisting_eligible_child_once() {
    let mut doc = Automerge::new().with_actor(actor(10));
    let h = doc.empty_commit(Default::default());
    doc.set_actor(actor(20));
    let mut tx = doc.transaction();
    let m = tx.put_object(ROOT, "section", ObjType::Map).unwrap();
    let a = tx.commit().0.unwrap();
    doc.set_actor(actor(40));
    let mut tx = doc.transaction();
    tx.put(&m, "title", "Plans").unwrap();
    tx.put(ROOT, "title", "Independent").unwrap();
    let b = tx.commit().0.unwrap();
    let r = record_control(&mut doc, actor(30), b"alice", vec![h]).unwrap();
    let mut s = Session::new();
    deliver(
        &mut s,
        vec![
            content(&doc, h),
            content(&doc, a),
            content(&doc, b),
            content(&doc, r),
            Input::Authorize(r),
        ],
    );
    let hidden = s.capture();
    assert_eq!(hidden.eligibility[&b], Eligibility::Eligible);
    assert!(s.candidates(&hidden, &ROOT, "section").is_empty());
    assert_eq!(
        s.candidates(&hidden, &m, "title")[0].0.as_str(),
        Some("Plans")
    );
    assert_eq!(s.scalar(&hidden, "title").as_deref(), Some("Independent"));
    let t = deliver(&mut s, vec![Input::Invalidate(r)]);
    assert!(!t.content.is_empty());
    assert_eq!(s.candidates(&s.capture(), &ROOT, "section")[0].1, m);
    assert!(s.candidates(&hidden, &ROOT, "section").is_empty());
}
