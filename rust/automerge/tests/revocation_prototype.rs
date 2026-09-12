//! Prototype A — external interpretation. Disposable experiment; see
//! `docs/prototype/`. Every fixture builds real Automerge changes, feeds them
//! through the experimental `eligibility::Session`, and checks against
//! independent expected states (never the production evaluator).

use std::collections::BTreeMap;

use automerge::eligibility::{
    Authority, AuthorizationContextId, Decision, Eligibility, EventId, Evidence, Input, Reason,
    Session, Transition,
};
use automerge::hydrate::{self, Value};
use automerge::transaction::Transactable;
use automerge::{
    ActorId, Author, Automerge, Change, ChangeHash, ReadDoc, ScalarValue, TextEncoding, ROOT,
};

fn actor(n: u8) -> ActorId {
    ActorId::from(vec![n; 8])
}

fn author(name: &str) -> Author<'static> {
    Author::from(name.as_bytes().to_vec())
}

fn expect_map(pairs: &[(&str, &str)]) -> Value {
    let mut m: std::collections::HashMap<&str, Value> = Default::default();
    for (k, v) in pairs {
        m.insert(k, Value::Scalar(ScalarValue::Str((*v).into())));
    }
    Value::Map(hydrate::Map::from(m))
}

/// Canonical (ordered) form of a hydrated map, ignoring conflict flags.
fn flat(v: &Value) -> BTreeMap<String, String> {
    match v {
        Value::Map(m) => m
            .iter()
            .map(|(k, mv)| (k.clone(), format!("{:?}", mv.value)))
            .collect(),
        other => panic!("not a map: {other:?}"),
    }
}

fn hydrated_eq(a: &Value, b: &Value) -> bool {
    flat(a) == flat(b)
}

/// EX-01 fixture. H preloaded; A, C are real changes; R/E1/E2 are evidence.
struct Ex01 {
    base: Automerge,
    a: Change,
    c: Change,
    r: Evidence,
    e1: Evidence,
    e2: Evidence,
}

const R_ID: EventId = EventId(1);
const E1_ID: EventId = EventId(2);
const E2_ID: EventId = EventId(3);

fn ex01() -> Ex01 {
    let mut base = Automerge::new()
        .with_author(Some(author("carol")))
        .with_actor(actor(3));
    base.transact::<_, _, automerge::AutomergeError>(|tx| {
        tx.put(ROOT, "title", "Trip plan")?;
        Ok(())
    })
    .unwrap();
    let h = base.get_heads()[0];

    let mut alice = base
        .fork()
        .with_author(Some(author("alice")))
        .with_actor(actor(1));
    alice
        .transact::<_, _, automerge::AutomergeError>(|tx| {
            tx.put(ROOT, "suggestion", "Camping")?;
            Ok(())
        })
        .unwrap();
    let a = alice.get_last_local_change().unwrap();

    let mut carol = alice
        .fork()
        .with_author(Some(author("carol")))
        .with_actor(actor(3));
    carol
        .transact::<_, _, automerge::AutomergeError>(|tx| {
            tx.put(ROOT, "title", "Weekend plan")?;
            Ok(())
        })
        .unwrap();
    let c = carol.get_last_local_change().unwrap();

    let r = Evidence::Revocation {
        id: R_ID,
        target: author("alice"),
        frontier: vec![h],
    };
    let e1 = Evidence::Authorizes {
        id: E1_ID,
        event: R_ID,
    };
    let e2 = Evidence::Invalidates {
        id: E2_ID,
        event: R_ID,
        after: vec![E1_ID],
    };
    Ex01 {
        base,
        a,
        c,
        r,
        e1,
        e2,
    }
}

fn deliver(session: &mut Session, inputs: Vec<Input>) -> Transition {
    session.deliver(inputs).expect("group accepted")
}

#[test]
fn ex01_three_checkpoints() {
    let fx = ex01();
    let a_hash = fx.a.hash();
    let c_hash = fx.c.hash();
    let mut session = Session::new(fx.base.clone());

    // Checkpoint 1: A, C, R present; no evidence.
    let t1 = deliver(
        &mut session,
        vec![
            Input::Change(fx.a.clone()),
            Input::Change(fx.c.clone()),
            Input::Evidence(fx.r.clone()),
        ],
    );
    let cp1 = session.current();
    assert!(hydrated_eq(
        &session.hydrate(cp1),
        &expect_map(&[("title", "Weekend plan"), ("suggestion", "Camping")])
    ));
    assert_eq!(session.authority(cp1, R_ID), Authority::Pending(vec![]));
    assert_eq!(
        session.decision(cp1, &a_hash).eligibility,
        Eligibility::Eligible
    );
    assert_eq!(
        session.decision(cp1, &c_hash).eligibility,
        Eligibility::Eligible
    );
    assert!(t1.status.authority_changes.contains_key(&R_ID));

    // Checkpoint 2: E1 -> R authorized, A excluded.
    let t2 = deliver(&mut session, vec![Input::Evidence(fx.e1.clone())]);
    let cp2 = session.current();
    assert!(hydrated_eq(
        &session.hydrate(cp2),
        &expect_map(&[("title", "Weekend plan")])
    ));
    assert_eq!(session.authority(cp2, R_ID), Authority::Authorized);
    let d: Decision = session.decision(cp2, &a_hash);
    assert_eq!(d.eligibility, Eligibility::Excluded);
    assert!(d
        .reasons
        .iter()
        .any(|r| matches!(r, Reason::OutsideFrontier { event, .. } if *event == R_ID)));
    assert_eq!(
        session.decision(cp2, &c_hash).eligibility,
        Eligibility::Eligible
    );
    // Patch replay: before + patches == after.
    let mut replay = session.hydrate(cp1);
    replay
        .apply_patches(TextEncoding::platform_default(), t2.patches.clone())
        .unwrap();
    assert_eq!(replay, session.hydrate(cp2));
    assert_eq!(
        t2.status.eligibility_changes.get(&a_hash),
        Some(&(Eligibility::Eligible, Eligibility::Excluded))
    );

    // Checkpoint 3: E2 -> R invalidated, A eligible again (same identity).
    let t3 = deliver(&mut session, vec![Input::Evidence(fx.e2.clone())]);
    let cp3 = session.current();
    assert!(hydrated_eq(
        &session.hydrate(cp3),
        &expect_map(&[("title", "Weekend plan"), ("suggestion", "Camping")])
    ));
    assert_eq!(session.authority(cp3, R_ID), Authority::Invalidated);
    assert_eq!(
        session.decision(cp3, &a_hash).eligibility,
        Eligibility::Eligible
    );
    assert_eq!(
        session.decision(cp3, &c_hash).eligibility,
        Eligibility::Eligible
    );
    let mut replay = session.hydrate(cp2);
    replay
        .apply_patches(TextEncoding::platform_default(), t3.patches.clone())
        .unwrap();
    assert_eq!(replay, session.hydrate(cp3));
    // Original identity: the restored suggestion value is A's original op.
    let vals = session.get_all(cp3, &ROOT, "suggestion");
    assert_eq!(vals.len(), 1);
    assert_eq!(vals[0].1.to_string(), format!("2@{}", actor(1)));
    // The document itself never lost A.
    assert!(session.doc().get_change_by_hash(&a_hash).is_some());

    // Frozen captures: rereading cp1/cp2 after E2 yields the same views.
    assert!(hydrated_eq(
        &session.hydrate(cp2),
        &expect_map(&[("title", "Weekend plan")])
    ));
    assert_eq!(
        session.decision(cp2, &a_hash).eligibility,
        Eligibility::Excluded
    );
    assert_eq!(session.authority(cp1, R_ID), Authority::Pending(vec![]));
}

// ---------------------------------------------------------------------------
// EX-01 schedules: independent expectation model
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
enum Ev {
    A,
    R,
    C,
    E1,
    E2,
}

/// Independent fixture expectation for EX-01 given the *set* of delivered
/// events (order-free by construction). Never consults the session.
struct Ex01Expect {
    a_integrated: bool,
    c_integrated: bool,
    r_authority: Option<Authority>,
    a_eligibility: Eligibility,
    view: Value,
}

fn ex01_expect(delivered: &std::collections::BTreeSet<Ev>) -> Ex01Expect {
    let has = |e| delivered.contains(&e);
    let a_integrated = has(Ev::A);
    let c_integrated = has(Ev::A) && has(Ev::C);
    let r_authority = if !has(Ev::R) {
        None
    } else if has(Ev::E2) && has(Ev::E1) {
        Some(Authority::Invalidated)
    } else if has(Ev::E2) {
        Some(Authority::Pending(vec![E1_ID]))
    } else if has(Ev::E1) {
        Some(Authority::Authorized)
    } else {
        Some(Authority::Pending(vec![]))
    };
    let a_eligibility = if r_authority == Some(Authority::Authorized) {
        Eligibility::Excluded
    } else {
        Eligibility::Eligible
    };
    let title = if c_integrated {
        "Weekend plan"
    } else {
        "Trip plan"
    };
    let mut pairs = vec![("title", title)];
    if a_integrated && a_eligibility == Eligibility::Eligible {
        pairs.push(("suggestion", "Camping"));
    }
    Ex01Expect {
        a_integrated,
        c_integrated,
        r_authority,
        a_eligibility,
        view: expect_map(&pairs),
    }
}

fn permutations<T: Clone>(items: &[T]) -> Vec<Vec<T>> {
    if items.len() <= 1 {
        return vec![items.to_vec()];
    }
    let mut out = Vec::new();
    for i in 0..items.len() {
        let mut rest = items.to_vec();
        let head = rest.remove(i);
        for mut p in permutations(&rest) {
            p.insert(0, head.clone());
            out.push(p);
        }
    }
    out
}

/// All contiguous partitions of `order` into groups (2^(n-1) of them).
fn partitions<T: Clone>(order: &[T]) -> Vec<Vec<Vec<T>>> {
    let n = order.len();
    let mut out = Vec::new();
    for cuts in 0..(1u32 << (n - 1)) {
        let mut groups = Vec::new();
        let mut current = vec![order[0].clone()];
        for i in 1..n {
            if cuts & (1 << (i - 1)) != 0 {
                groups.push(std::mem::take(&mut current));
            }
            current.push(order[i].clone());
        }
        groups.push(current);
        out.push(groups);
    }
    out
}

fn ex01_input(fx: &Ex01, ev: Ev) -> Input {
    match ev {
        Ev::A => Input::Change(fx.a.clone()),
        Ev::C => Input::Change(fx.c.clone()),
        Ev::R => Input::Evidence(fx.r.clone()),
        Ev::E1 => Input::Evidence(fx.e1.clone()),
        Ev::E2 => Input::Evidence(fx.e2.clone()),
    }
}

fn check_ex01(session: &Session, fx: &Ex01, delivered: &std::collections::BTreeSet<Ev>) {
    let exp = ex01_expect(delivered);
    let cp = session.current();
    let got = session.hydrate(cp);
    assert!(
        hydrated_eq(&got, &exp.view),
        "delivered {delivered:?}: got {got:?}, expected {:?}",
        exp.view
    );
    assert_eq!(session.is_integrated(cp, &fx.a.hash()), exp.a_integrated);
    assert_eq!(session.is_integrated(cp, &fx.c.hash()), exp.c_integrated);
    if exp.a_integrated {
        assert_eq!(
            session.decision(cp, &fx.a.hash()).eligibility,
            exp.a_eligibility,
            "delivered {delivered:?}"
        );
    }
    if exp.c_integrated {
        assert_eq!(
            session.decision(cp, &fx.c.hash()).eligibility,
            Eligibility::Eligible
        );
    } else if delivered.contains(&Ev::C) {
        // C received but waiting for A.
        assert_eq!(
            session.waiting(cp, &fx.c.hash()),
            Some([fx.a.hash()].into_iter().collect())
        );
    }
    if let Some(auth) = exp.r_authority {
        assert_eq!(session.authority(cp, R_ID), auth, "delivered {delivered:?}");
    }
}

fn replay_ok(session: &Session, t: &Transition) {
    let mut replay = session.hydrate(t.before);
    replay
        .apply_patches(TextEncoding::platform_default(), t.patches.clone())
        .unwrap();
    assert_eq!(replay, session.hydrate(t.after), "patch replay mismatch");
}

#[test]
fn ex01_all_1920_schedules() {
    let fx = ex01();
    let events = [Ev::A, Ev::R, Ev::C, Ev::E1, Ev::E2];
    let mut schedules = 0;
    let mut final_views = Vec::new();
    for order in permutations(&events) {
        for groups in partitions(&order) {
            schedules += 1;
            let mut session = Session::new(fx.base.clone());
            let mut delivered = std::collections::BTreeSet::new();
            for group in &groups {
                let inputs = group.iter().map(|e| ex01_input(&fx, *e)).collect();
                let t = deliver(&mut session, inputs);
                delivered.extend(group.iter().copied());
                check_ex01(&session, &fx, &delivered);
                replay_ok(&session, &t);
            }
            let cp = session.current();
            final_views.push((
                flat(&session.hydrate(cp)),
                session.decision(cp, &fx.a.hash()),
                session.decision(cp, &fx.c.hash()),
                session.authority(cp, R_ID),
            ));
        }
    }
    assert_eq!(schedules, 1920);
    // Convergence: every schedule reaches the same resolved endpoint.
    assert!(final_views.windows(2).all(|w| w[0] == w[1]));
    assert_eq!(final_views[0].3, Authority::Invalidated);
}

#[test]
fn ex01_duplicates_are_idempotent() {
    let fx = ex01();
    let events = [Ev::A, Ev::R, Ev::C, Ev::E1, Ev::E2];
    let mut session = Session::new(fx.base.clone());
    for e in events {
        deliver(&mut session, vec![ex01_input(&fx, e)]);
    }
    let before = session.current();
    for e in events {
        let t = deliver(&mut session, vec![ex01_input(&fx, e)]);
        assert!(t.patches.is_empty(), "duplicate {e:?} produced patches");
        assert!(t.status.is_empty(), "duplicate {e:?} produced status delta");
        assert_eq!(session.hydrate(before), session.hydrate(session.current()));
    }
    let t = deliver(
        &mut session,
        events.iter().map(|e| ex01_input(&fx, *e)).collect(),
    );
    assert!(t.patches.is_empty() && t.status.is_empty());
}

#[test]
fn ex01_e2_before_e1_exposes_unresolved_then_resolves() {
    let fx = ex01();
    let mut session = Session::new(fx.base.clone());
    deliver(
        &mut session,
        vec![
            Input::Change(fx.a.clone()),
            Input::Change(fx.c.clone()),
            Input::Evidence(fx.r.clone()),
            Input::Evidence(fx.e2.clone()),
        ],
    );
    let cp = session.current();
    assert_eq!(session.authority(cp, R_ID), Authority::Pending(vec![E1_ID]));
    assert_eq!(
        session.authority(cp, E2_ID),
        Authority::Pending(vec![E1_ID])
    );
    assert_eq!(
        session.decision(cp, &fx.a.hash()).eligibility,
        Eligibility::Eligible
    );
    // Late E1 must not overwrite E2's conclusion.
    let t = deliver(&mut session, vec![Input::Evidence(fx.e1.clone())]);
    let cp = session.current();
    assert_eq!(session.authority(cp, R_ID), Authority::Invalidated);
    assert_eq!(
        session.decision(cp, &fx.a.hash()).eligibility,
        Eligibility::Eligible
    );
    assert!(t.patches.is_empty());
    assert_eq!(
        t.status.authority_changes.get(&R_ID),
        Some(&(
            Some(Authority::Pending(vec![E1_ID])),
            Authority::Invalidated
        ))
    );
}

#[test]
fn ex01_status_only_variant_independent_delete_hides_suggestion() {
    let fx = ex01();
    // Carol independently deletes `suggestion` after observing A.
    let mut carol = fx
        .base
        .fork()
        .with_author(Some(author("carol")))
        .with_actor(actor(3));
    carol.apply_changes([fx.a.clone()]).unwrap();
    carol
        .transact::<_, _, automerge::AutomergeError>(|tx| {
            tx.delete(ROOT, "suggestion")?;
            Ok(())
        })
        .unwrap();
    let del = carol.get_last_local_change().unwrap();

    let mut session = Session::new(fx.base.clone());
    deliver(
        &mut session,
        vec![
            Input::Change(fx.a.clone()),
            Input::Change(del.clone()),
            Input::Evidence(fx.r.clone()),
        ],
    );
    let cp1 = session.current();
    assert!(hydrated_eq(
        &session.hydrate(cp1),
        &expect_map(&[("title", "Trip plan")])
    ));

    let t2 = deliver(&mut session, vec![Input::Evidence(fx.e1.clone())]);
    assert!(
        t2.patches.is_empty(),
        "no content change, got {:?}",
        t2.patches
    );
    assert_eq!(
        t2.status.eligibility_changes.get(&fx.a.hash()),
        Some(&(Eligibility::Eligible, Eligibility::Excluded))
    );
    assert_eq!(
        t2.status.authority_changes.get(&R_ID),
        Some(&(Some(Authority::Pending(vec![])), Authority::Authorized))
    );

    let t3 = deliver(&mut session, vec![Input::Evidence(fx.e2.clone())]);
    assert!(t3.patches.is_empty());
    assert_eq!(
        t3.status.eligibility_changes.get(&fx.a.hash()),
        Some(&(Eligibility::Excluded, Eligibility::Eligible))
    );
    assert_eq!(
        t3.status.authority_changes.get(&R_ID),
        Some(&(Some(Authority::Authorized), Authority::Invalidated))
    );
    assert!(hydrated_eq(
        &session.hydrate(session.current()),
        &expect_map(&[("title", "Trip plan")])
    ));
}

#[test]
fn ex01_status_stream_without_alice_content() {
    let fx = ex01();
    let mut session = Session::new(fx.base.clone());
    deliver(&mut session, vec![Input::Evidence(fx.r.clone())]);
    let t1 = deliver(&mut session, vec![Input::Evidence(fx.e1.clone())]);
    let t2 = deliver(&mut session, vec![Input::Evidence(fx.e2.clone())]);
    assert!(t1.patches.is_empty() && t2.patches.is_empty());
    assert!(!t1.status.is_empty());
    assert!(!t2.status.is_empty());
    assert_eq!(
        session.authority(session.current(), R_ID),
        Authority::Invalidated
    );
}

// ---------------------------------------------------------------------------
// EX-02: credible revocation with unknown content frontier
// ---------------------------------------------------------------------------

fn tx_put(doc: &mut Automerge, key: &str, value: i64) -> Change {
    doc.transact::<_, _, automerge::AutomergeError>(|tx| {
        tx.put(ROOT, key, value)?;
        Ok(())
    })
    .unwrap();
    doc.get_last_local_change().unwrap()
}

fn expect_ints(pairs: &[(&str, i64)]) -> Value {
    let mut m: std::collections::HashMap<&str, Value> = Default::default();
    for (k, v) in pairs {
        m.insert(k, Value::Scalar(ScalarValue::Int(*v)));
    }
    Value::Map(hydrate::Map::from(m))
}

#[test]
fn ex02_missing_boundary_then_resolution() {
    let h0 = Automerge::new()
        .with_author(Some(author("carol")))
        .with_actor(actor(3));
    let mut alice = h0
        .fork()
        .with_author(Some(author("alice")))
        .with_actor(actor(1));
    let a1 = tx_put(&mut alice, "x", 1);
    let a2 = tx_put(&mut alice, "y", 2);
    // Carol's boundary H: empty change acknowledging A1 only.
    let mut carol = h0
        .fork()
        .with_author(Some(author("carol")))
        .with_actor(actor(3));
    carol.apply_changes([a1.clone()]).unwrap();
    let h = carol.empty_commit(Default::default());
    let h_change = carol.get_change_by_hash(&h).unwrap();
    assert_eq!(h_change.len(), 0);
    let r = Evidence::Revocation {
        id: R_ID,
        target: author("alice"),
        frontier: vec![h],
    };
    let e1 = Evidence::Authorizes {
        id: E1_ID,
        event: R_ID,
    };

    let mut bob = Session::new(h0.clone());
    deliver(
        &mut bob,
        vec![Input::Change(a1.clone()), Input::Change(a2.clone())],
    );
    let cp0 = bob.current();
    assert!(hydrated_eq(
        &bob.hydrate(cp0),
        &expect_ints(&[("x", 1), ("y", 2)])
    ));

    // Authoritative R known, H missing.
    let t1 = deliver(
        &mut bob,
        vec![Input::Evidence(r.clone()), Input::Evidence(e1.clone())],
    );
    let cp1 = bob.current();
    assert!(hydrated_eq(&bob.hydrate(cp1), &expect_ints(&[])));
    for c in [&a1, &a2] {
        let d = bob.decision(cp1, &c.hash());
        assert_eq!(d.eligibility, Eligibility::Pending);
        assert!(d.reasons.iter().any(
            |r| matches!(r, Reason::MissingBoundary { event, missing } if *event == R_ID && missing == &vec![h])
        ));
        // Pending eligibility is distinct from missing content dependencies.
        assert!(bob.is_integrated(cp1, &c.hash()));
        assert_eq!(bob.waiting(cp1, &c.hash()), None);
    }
    replay_ok(&bob, &t1);
    // Heads unchanged: same content heads, different view.
    assert_eq!(bob.capture(cp0).spec.heads, bob.capture(cp1).spec.heads);

    // H arrives: A1 eligible, A2 excluded.
    let t2 = deliver(&mut bob, vec![Input::Change(h_change.clone())]);
    let cp2 = bob.current();
    assert!(hydrated_eq(&bob.hydrate(cp2), &expect_ints(&[("x", 1)])));
    assert_eq!(
        bob.decision(cp2, &a1.hash()).eligibility,
        Eligibility::Eligible
    );
    assert_eq!(
        bob.decision(cp2, &a2.hash()).eligibility,
        Eligibility::Excluded
    );
    assert_eq!(
        t2.status.eligibility_changes.get(&a1.hash()),
        Some(&(Eligibility::Pending, Eligibility::Eligible))
    );
    assert_eq!(
        t2.status.eligibility_changes.get(&a2.hash()),
        Some(&(Eligibility::Pending, Eligibility::Excluded))
    );
    replay_ok(&bob, &t2);
    // Only A1 gains a content effect.
    assert_eq!(t2.patches.len(), 1);

    // Frozen captures do not reinterpret after H arrives.
    assert!(hydrated_eq(&bob.hydrate(cp1), &expect_ints(&[])));
    assert_eq!(
        bob.decision(cp1, &a1.hash()).eligibility,
        Eligibility::Pending
    );
    assert!(hydrated_eq(
        &bob.hydrate(cp0),
        &expect_ints(&[("x", 1), ("y", 2)])
    ));
}

#[test]
fn ex02_variant_boundary_also_edits_restored_object() {
    let h0 = Automerge::new()
        .with_author(Some(author("carol")))
        .with_actor(actor(3));
    let mut alice = h0
        .fork()
        .with_author(Some(author("alice")))
        .with_actor(actor(1));
    let a1 = tx_put(&mut alice, "x", 1);
    let a2 = tx_put(&mut alice, "y", 2);
    let mut carol = h0
        .fork()
        .with_author(Some(author("carol")))
        .with_actor(actor(3));
    carol.apply_changes([a1.clone()]).unwrap();
    // H edits `x` too (root map is the restored object here).
    let h_change = tx_put(&mut carol, "z", 3);
    let h = h_change.hash();
    let r = Evidence::Revocation {
        id: R_ID,
        target: author("alice"),
        frontier: vec![h],
    };
    let e1 = Evidence::Authorizes {
        id: E1_ID,
        event: R_ID,
    };
    let mut bob = Session::new(h0.clone());
    deliver(
        &mut bob,
        vec![Input::Change(a1.clone()), Input::Change(a2.clone())],
    );
    deliver(&mut bob, vec![Input::Evidence(r), Input::Evidence(e1)]);
    let t = deliver(&mut bob, vec![Input::Change(h_change)]);
    replay_ok(&bob, &t);
    assert!(hydrated_eq(
        &bob.hydrate(bob.current()),
        &expect_ints(&[("x", 1), ("z", 3)])
    ));
    // Exactly one put per newly visible key; no duplicate exposure.
    assert_eq!(t.patches.len(), 2, "{:?}", t.patches);
}

// ---------------------------------------------------------------------------
// EX-03: fresh grant does not admit the excluded period
// ---------------------------------------------------------------------------

const G_ID: EventId = EventId(4);
const CTX0: AuthorizationContextId = AuthorizationContextId(0);
const CTXG: AuthorizationContextId = AuthorizationContextId(7);

struct Ex03 {
    base: Automerge,
    a1: Change,
    a2: Change,
    a3: Change,
    r: Evidence,
    e1: Evidence,
    g: Evidence,
}

fn ex03() -> Ex03 {
    let base = Automerge::new()
        .with_author(Some(author("carol")))
        .with_actor(actor(3));
    let mut alice = base
        .fork()
        .with_author(Some(author("alice")))
        .with_actor(actor(1));
    let a1 = tx_put(&mut alice, "before", 1);
    let a2 = tx_put(&mut alice, "during", 2);
    let a3 = tx_put(&mut alice, "after", 3);
    let r = Evidence::Revocation {
        id: R_ID,
        target: author("alice"),
        frontier: vec![a1.hash()],
    };
    let e1 = Evidence::Authorizes {
        id: E1_ID,
        event: R_ID,
    };
    let g = Evidence::Grant {
        id: G_ID,
        context: CTXG,
    };
    Ex03 {
        base,
        a1,
        a2,
        a3,
        r,
        e1,
        g,
    }
}

fn ex03_check_final(session: &Session, fx: &Ex03) {
    let cp = session.current();
    assert!(hydrated_eq(
        &session.hydrate(cp),
        &expect_ints(&[("before", 1), ("after", 3)])
    ));
    assert_eq!(
        session.decision(cp, &fx.a1.hash()).eligibility,
        Eligibility::Eligible
    );
    assert_eq!(
        session.decision(cp, &fx.a2.hash()).eligibility,
        Eligibility::Excluded
    );
    let d3 = session.decision(cp, &fx.a3.hash());
    assert_eq!(d3.eligibility, Eligibility::Eligible);
    assert!(d3.reasons.contains(&Reason::AdmittedByContext(CTXG)));
}

#[test]
fn ex03_fresh_grant_non_prefix_selection() {
    let fx = ex03();
    let bindings = vec![
        Input::Binding(fx.a1.hash(), CTX0),
        Input::Binding(fx.a2.hash(), CTX0),
        Input::Binding(fx.a3.hash(), CTXG),
    ];
    // Order 1: everything at once.
    let mut s = Session::new(fx.base.clone());
    let mut all = bindings.clone();
    all.extend([
        Input::Change(fx.a1.clone()),
        Input::Change(fx.a2.clone()),
        Input::Change(fx.a3.clone()),
        Input::Evidence(fx.r.clone()),
        Input::Evidence(fx.e1.clone()),
        Input::Evidence(fx.g.clone()),
    ]);
    let t = deliver(&mut s, all);
    replay_ok(&s, &t);
    ex03_check_final(&s, &fx);

    // Order 2: A2 delivered late, after G and after receipt of A3.
    let mut s = Session::new(fx.base.clone());
    let mut first = bindings.clone();
    first.extend([
        Input::Change(fx.a1.clone()),
        Input::Evidence(fx.r.clone()),
        Input::Evidence(fx.e1.clone()),
        Input::Evidence(fx.g.clone()),
    ]);
    deliver(&mut s, first);
    let t = deliver(&mut s, vec![Input::Change(fx.a3.clone())]);
    let cp = s.current();
    // A3 waits structurally for A2; that is not an eligibility state.
    assert!(!s.is_integrated(cp, &fx.a3.hash()));
    assert_eq!(
        s.waiting(cp, &fx.a3.hash()),
        Some([fx.a2.hash()].into_iter().collect())
    );
    assert!(t.patches.is_empty());
    let t = deliver(&mut s, vec![Input::Change(fx.a2.clone())]);
    replay_ok(&s, &t);
    ex03_check_final(&s, &fx);
    // Exactly one put (after), never `during`.
    assert_eq!(t.patches.len(), 1, "{:?}", t.patches);

    // The selection has a hole for one continuing actor: capture and reread.
    let cap = s.capture(s.current()).spec.clone();
    let sel: Vec<(ChangeHash, Eligibility)> = [&fx.a1, &fx.a2, &fx.a3]
        .iter()
        .map(|c| (c.hash(), cap.selection.get(&c.hash()).unwrap()))
        .collect();
    assert_eq!(
        sel.iter().map(|s| s.1).collect::<Vec<_>>(),
        vec![
            Eligibility::Eligible,
            Eligibility::Excluded,
            Eligibility::Eligible
        ]
    );
    let reread = s.doc().hydrate_view(&cap).unwrap();
    assert!(hydrated_eq(
        &reread,
        &expect_ints(&[("before", 1), ("after", 3)])
    ));
}

#[test]
fn ex03_removing_restrictions_is_not_the_grant() {
    // Contrast: invalidating R restores A2 too (EX-01 style), unlike G.
    let fx = ex03();
    let mut s = Session::new(fx.base.clone());
    deliver(
        &mut s,
        vec![
            Input::Binding(fx.a2.hash(), CTX0),
            Input::Binding(fx.a3.hash(), CTXG),
            Input::Change(fx.a1.clone()),
            Input::Change(fx.a2.clone()),
            Input::Change(fx.a3.clone()),
            Input::Evidence(fx.r.clone()),
            Input::Evidence(fx.e1.clone()),
        ],
    );
    let cp = s.current();
    assert!(hydrated_eq(&s.hydrate(cp), &expect_ints(&[("before", 1)])));
    deliver(
        &mut s,
        vec![Input::Evidence(Evidence::Invalidates {
            id: E2_ID,
            event: R_ID,
            after: vec![E1_ID],
        })],
    );
    assert!(hydrated_eq(
        &s.hydrate(s.current()),
        &expect_ints(&[("before", 1), ("during", 2), ("after", 3)])
    ));
}

// ---------------------------------------------------------------------------
// EX-05/06: direct targets through excluded predecessors
// ---------------------------------------------------------------------------

/// Explicit whole-change selection helper: exclude the listed hashes.
fn exclude_evidence(target: &str, excluded_frontier: &[ChangeHash]) -> Vec<Input> {
    vec![
        Input::Evidence(Evidence::Revocation {
            id: R_ID,
            target: author(target),
            frontier: excluded_frontier.to_vec(),
        }),
        Input::Evidence(Evidence::Authorizes {
            id: E1_ID,
            event: R_ID,
        }),
    ]
}

struct Chain {
    base: Automerge,
    a: Change,
    b: Change,
    c: Change,
}

/// A: title=Original (Alice); B: title=Spam (Bob, excluded); C by Carol
/// authored while seeing B. `c_action` builds C.
fn chain(c_action: impl Fn(&mut Automerge)) -> Chain {
    let base = Automerge::new()
        .with_author(Some(author("carol")))
        .with_actor(actor(3));
    let mut alice = base
        .fork()
        .with_author(Some(author("alice")))
        .with_actor(actor(1));
    alice
        .transact::<_, _, automerge::AutomergeError>(|tx| {
            tx.put(ROOT, "title", "Original")?;
            Ok(())
        })
        .unwrap();
    let a = alice.get_last_local_change().unwrap();
    let mut bob = alice
        .fork()
        .with_author(Some(author("bob")))
        .with_actor(actor(2));
    bob.transact::<_, _, automerge::AutomergeError>(|tx| {
        tx.put(ROOT, "title", "Spam")?;
        Ok(())
    })
    .unwrap();
    let b = bob.get_last_local_change().unwrap();
    let mut carol = bob
        .fork()
        .with_author(Some(author("carol")))
        .with_actor(actor(3));
    c_action(&mut carol);
    let c = carol.get_last_local_change().unwrap();
    Chain { base, a, b, c }
}

#[test]
fn ex05_eligible_overwrite_through_excluded_predecessor() {
    let fx = chain(|doc| {
        doc.transact::<_, _, automerge::AutomergeError>(|tx| {
            tx.put(ROOT, "title", "Revised")?;
            Ok(())
        })
        .unwrap();
    });
    let mut s = Session::new(fx.base.clone());
    deliver(
        &mut s,
        vec![
            Input::Change(fx.a.clone()),
            Input::Change(fx.b.clone()),
            Input::Change(fx.c.clone()),
        ],
    );
    let all = s.current();
    let vals = s.get_all(all, &ROOT, "title");
    assert_eq!(vals.len(), 1);
    assert_eq!(vals[0].0.to_string(), "\"Revised\"");
    // Exclude Bob's B (frontier = A; B is outside).
    let t = deliver(&mut s, exclude_evidence("bob", &[fx.a.hash()]));
    let cp = s.current();
    assert_eq!(
        s.decision(cp, &fx.b.hash()).eligibility,
        Eligibility::Excluded
    );
    let vals = s.get_all(cp, &ROOT, "title");
    let ids: Vec<String> = vals.iter().map(|v| v.1.to_string()).collect();
    assert_eq!(
        ids,
        vec![format!("1@{}", actor(1)), format!("3@{}", actor(3))],
        "candidates must be A and C, got {vals:?}"
    );
    replay_ok(&s, &t);
    // Conflict is exposed in the hydrated view.
    match s.hydrate(cp) {
        Value::Map(m) => assert!(m.iter().find(|(k, _)| *k == "title").unwrap().1.conflict),
        other => panic!("{other:?}"),
    }
}

#[test]
fn ex06_eligible_delete_of_excluded_replacement() {
    let fx = chain(|doc| {
        doc.transact::<_, _, automerge::AutomergeError>(|tx| {
            tx.delete(ROOT, "title")?;
            Ok(())
        })
        .unwrap();
    });
    let mut s = Session::new(fx.base.clone());
    deliver(
        &mut s,
        vec![
            Input::Change(fx.a.clone()),
            Input::Change(fx.b.clone()),
            Input::Change(fx.c.clone()),
        ],
    );
    assert!(hydrated_eq(&s.hydrate(s.current()), &expect_map(&[])));
    let t = deliver(&mut s, exclude_evidence("bob", &[fx.a.hash()]));
    let cp = s.current();
    // C targeted B only; A reappears.
    assert!(hydrated_eq(
        &s.hydrate(cp),
        &expect_map(&[("title", "Original")])
    ));
    let vals = s.get_all(cp, &ROOT, "title");
    assert_eq!(vals.len(), 1);
    assert_eq!(vals[0].1.to_string(), format!("1@{}", actor(1)));
    replay_ok(&s, &t);
}

#[test]
fn ex06_simple_excluded_delete_targeting_eligible_value() {
    // A eligible; B (Bob) deletes title; exclude B -> A reappears; restore B -> hidden again.
    let base = Automerge::new()
        .with_author(Some(author("carol")))
        .with_actor(actor(3));
    let mut alice = base
        .fork()
        .with_author(Some(author("alice")))
        .with_actor(actor(1));
    alice
        .transact::<_, _, automerge::AutomergeError>(|tx| {
            tx.put(ROOT, "title", "Original")?;
            Ok(())
        })
        .unwrap();
    let a = alice.get_last_local_change().unwrap();
    let mut bob = alice
        .fork()
        .with_author(Some(author("bob")))
        .with_actor(actor(2));
    bob.transact::<_, _, automerge::AutomergeError>(|tx| {
        tx.delete(ROOT, "title")?;
        Ok(())
    })
    .unwrap();
    let b = bob.get_last_local_change().unwrap();
    let mut s = Session::new(base.clone());
    deliver(
        &mut s,
        vec![Input::Change(a.clone()), Input::Change(b.clone())],
    );
    assert!(hydrated_eq(&s.hydrate(s.current()), &expect_map(&[])));
    let t = deliver(&mut s, exclude_evidence("bob", &[a.hash()]));
    assert!(hydrated_eq(
        &s.hydrate(s.current()),
        &expect_map(&[("title", "Original")])
    ));
    replay_ok(&s, &t);
    let t = deliver(
        &mut s,
        vec![Input::Evidence(Evidence::Invalidates {
            id: E2_ID,
            event: R_ID,
            after: vec![E1_ID],
        })],
    );
    assert!(hydrated_eq(&s.hydrate(s.current()), &expect_map(&[])));
    replay_ok(&s, &t);
}

// ---------------------------------------------------------------------------
// EX-07: excluded object creation, eligible child edit; restoration replay
// ---------------------------------------------------------------------------

#[test]
fn ex07_excluded_container_eligible_child_and_restore_replay() {
    let base = Automerge::new()
        .with_author(Some(author("carol")))
        .with_actor(actor(3));
    let mut alice = base
        .fork()
        .with_author(Some(author("alice")))
        .with_actor(actor(1));
    let section = alice
        .transact::<_, _, automerge::AutomergeError>(|tx| {
            let m = tx.put_object(ROOT, "section", automerge::ObjType::Map)?;
            Ok(m)
        })
        .unwrap()
        .result;
    let mk = alice.get_last_local_change().unwrap();
    let mut bob = alice
        .fork()
        .with_author(Some(author("bob")))
        .with_actor(actor(2));
    bob.transact::<_, _, automerge::AutomergeError>(|tx| {
        tx.put(&section, "title", "Plans")?;
        Ok(())
    })
    .unwrap();
    let child = bob.get_last_local_change().unwrap();
    // Counterexample to causal taint: Bob's independent root edit after observing Alice.
    bob.transact::<_, _, automerge::AutomergeError>(|tx| {
        tx.put(ROOT, "title", "Root")?;
        Ok(())
    })
    .unwrap();
    let root_edit = bob.get_last_local_change().unwrap();

    let mut s = Session::new(base.clone());
    deliver(
        &mut s,
        vec![
            Input::Change(mk.clone()),
            Input::Change(child.clone()),
            Input::Change(root_edit.clone()),
        ],
    );
    // Exclude Alice entirely (frontier = empty base heads).
    let t = deliver(&mut s, exclude_evidence("alice", &[]));
    let cp = s.current();
    assert_eq!(
        s.decision(cp, &mk.hash()).eligibility,
        Eligibility::Excluded
    );
    assert_eq!(
        s.decision(cp, &child.hash()).eligibility,
        Eligibility::Eligible
    );
    let view = s.hydrate(cp);
    assert!(
        hydrated_eq(&view, &expect_map(&[("title", "Root")])),
        "{view:?}"
    );
    replay_ok(&s, &t);
    // The child is inspectable by object id without restoring M.
    let inside = s.get_all(cp, &section, "title");
    assert_eq!(inside.len(), 1);
    assert_eq!(inside[0].0.to_string(), "\"Plans\"");

    // Restore: invalidate R. Replay must reproduce section + child exactly once.
    let t = deliver(
        &mut s,
        vec![Input::Evidence(Evidence::Invalidates {
            id: E2_ID,
            event: R_ID,
            after: vec![E1_ID],
        })],
    );
    let cp2 = s.current();
    let after = s.hydrate(cp2);
    match &after {
        Value::Map(m) => {
            let sec = m.get("section").expect("section restored");
            match sec {
                Value::Map(sm) => assert_eq!(
                    sm.get("title"),
                    Some(&Value::Scalar(ScalarValue::Str("Plans".into())))
                ),
                other => panic!("{other:?}"),
            }
        }
        other => panic!("{other:?}"),
    }
    replay_ok(&s, &t);
    // Exactly one patch attaches the section (with its contents exposed);
    // the child's put is not emitted a second time as a separate delta.
    let attaches = t
        .patches
        .iter()
        .filter(|p| p.obj == ROOT && matches!(&p.action, automerge::PatchAction::PutMap { key, .. } if key == "section"))
        .count();
    assert_eq!(attaches, 1, "{:?}", t.patches);
}

// ---------------------------------------------------------------------------
// Failure atomicity and actor-table recompilation
// ---------------------------------------------------------------------------

#[test]
fn failed_group_publishes_nothing() {
    let fx = ex01();
    let mut s = Session::new(fx.base.clone());
    deliver(
        &mut s,
        vec![
            Input::Change(fx.a.clone()),
            Input::Evidence(fx.r.clone()),
            Input::Evidence(fx.e1.clone()),
        ],
    );
    let before = s.current();
    let before_view = s.hydrate(before);
    // Group with valid content C plus conflicting reuse of evidence id E1.
    let bad = Evidence::Authorizes {
        id: E1_ID,
        event: EventId(99),
    };
    let err = s
        .deliver(vec![Input::Change(fx.c.clone()), Input::Evidence(bad)])
        .err()
        .expect("group rejected");
    assert!(matches!(
        err,
        automerge::eligibility::SessionError::Evidence(_)
    ));
    assert_eq!(s.current(), before);
    assert_eq!(s.hydrate(before), before_view);
    assert!(!s.is_integrated(before, &fx.c.hash()));
    assert!(s.doc().get_change_by_hash(&fx.c.hash()).is_none());
    assert_eq!(
        s.decision(before, &fx.a.hash()).eligibility,
        Eligibility::Excluded
    );
}

#[test]
fn captured_scope_recompiles_after_earlier_sorting_actor_arrives() {
    // EX-03 selection with a hole; then a new actor sorting before Alice's
    // (actor 0x00) integrates. Rereading the capture must not leak `during`.
    let fx = ex03();
    let mut s = Session::new(fx.base.clone());
    deliver(
        &mut s,
        vec![
            Input::Binding(fx.a2.hash(), CTX0),
            Input::Binding(fx.a3.hash(), CTXG),
            Input::Change(fx.a1.clone()),
            Input::Change(fx.a2.clone()),
            Input::Change(fx.a3.clone()),
            Input::Evidence(fx.r.clone()),
            Input::Evidence(fx.e1.clone()),
            Input::Evidence(fx.g.clone()),
        ],
    );
    let cp = s.current();
    ex03_check_final(&s, &fx);
    let mut zed = fx
        .base
        .fork()
        .with_author(Some(author("zed")))
        .with_actor(actor(0));
    let z = tx_put(&mut zed, "zed", 9);
    let t = deliver(&mut s, vec![Input::Change(z.clone())]);
    replay_ok(&s, &t);
    // Old capture rereads identically with the new actor table.
    assert!(hydrated_eq(
        &s.hydrate(cp),
        &expect_ints(&[("before", 1), ("after", 3)])
    ));
    assert!(hydrated_eq(
        &s.hydrate(s.current()),
        &expect_ints(&[("before", 1), ("after", 3), ("zed", 9)])
    ));
    assert_eq!(
        s.decision(s.current(), &fx.a2.hash()).eligibility,
        Eligibility::Excluded
    );
}
