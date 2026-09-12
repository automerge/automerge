//! Prototype A — external interpretation. Disposable experiment; see
//! `docs/prototype/`. Every fixture builds real Automerge changes, feeds them
//! through the experimental `eligibility::Session`, and checks against
//! independent expected states (never the production evaluator).

use std::collections::BTreeMap;

use automerge::eligibility::{
    Authority, AuthorizationContextId, ContextBinding, ContextKind, Decision, Eligibility, EventId,
    Evidence, Input, Reason, Session, Transition,
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
        &session.hydrate(cp1).unwrap(),
        &expect_map(&[("title", "Weekend plan"), ("suggestion", "Camping")])
    ));
    assert_eq!(
        session.authority(cp1, R_ID).unwrap(),
        Authority::Pending(vec![])
    );
    assert_eq!(
        session.decision(cp1, &a_hash).unwrap().eligibility,
        Eligibility::Eligible
    );
    assert_eq!(
        session.decision(cp1, &c_hash).unwrap().eligibility,
        Eligibility::Eligible
    );
    assert!(t1.status.authority_changes.contains_key(&R_ID));

    // Checkpoint 2: E1 -> R authorized, A excluded.
    let t2 = deliver(&mut session, vec![Input::Evidence(fx.e1.clone())]);
    let cp2 = session.current();
    assert!(hydrated_eq(
        &session.hydrate(cp2).unwrap(),
        &expect_map(&[("title", "Weekend plan")])
    ));
    assert_eq!(session.authority(cp2, R_ID).unwrap(), Authority::Authorized);
    let d: Decision = session.decision(cp2, &a_hash).unwrap();
    assert_eq!(d.eligibility, Eligibility::Excluded);
    assert!(d
        .reasons
        .iter()
        .any(|r| matches!(r, Reason::OutsideFrontier { event, .. } if *event == R_ID)));
    assert_eq!(
        session.decision(cp2, &c_hash).unwrap().eligibility,
        Eligibility::Eligible
    );
    // Patch replay: before + patches == after.
    let mut replay = session.hydrate(cp1).unwrap();
    replay
        .apply_patches(TextEncoding::platform_default(), t2.patches.clone())
        .unwrap();
    assert_eq!(replay, session.hydrate(cp2).unwrap());
    assert_eq!(
        t2.status.eligibility_changes.get(&a_hash),
        Some(&(Eligibility::Eligible, Eligibility::Excluded))
    );

    // Checkpoint 3: E2 -> R invalidated, A eligible again (same identity).
    let t3 = deliver(&mut session, vec![Input::Evidence(fx.e2.clone())]);
    let cp3 = session.current();
    assert!(hydrated_eq(
        &session.hydrate(cp3).unwrap(),
        &expect_map(&[("title", "Weekend plan"), ("suggestion", "Camping")])
    ));
    assert_eq!(
        session.authority(cp3, R_ID).unwrap(),
        Authority::Invalidated
    );
    assert_eq!(
        session.decision(cp3, &a_hash).unwrap().eligibility,
        Eligibility::Eligible
    );
    assert_eq!(
        session.decision(cp3, &c_hash).unwrap().eligibility,
        Eligibility::Eligible
    );
    let mut replay = session.hydrate(cp2).unwrap();
    replay
        .apply_patches(TextEncoding::platform_default(), t3.patches.clone())
        .unwrap();
    assert_eq!(replay, session.hydrate(cp3).unwrap());
    // Original identity: the restored suggestion value is A's original op.
    let vals = session.get_all(cp3, &ROOT, "suggestion").unwrap();
    assert_eq!(vals.len(), 1);
    assert_eq!(vals[0].1.to_string(), format!("2@{}", actor(1)));
    // The document itself never lost A.
    assert!(session.doc().get_change_by_hash(&a_hash).is_some());

    // Frozen captures: rereading cp1/cp2 after E2 yields the same views.
    assert!(hydrated_eq(
        &session.hydrate(cp2).unwrap(),
        &expect_map(&[("title", "Weekend plan")])
    ));
    assert_eq!(
        session.decision(cp2, &a_hash).unwrap().eligibility,
        Eligibility::Excluded
    );
    assert_eq!(
        session.authority(cp1, R_ID).unwrap(),
        Authority::Pending(vec![])
    );
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
    let got = session.hydrate(cp).unwrap();
    assert!(
        hydrated_eq(&got, &exp.view),
        "delivered {delivered:?}: got {got:?}, expected {:?}",
        exp.view
    );
    assert_eq!(
        session.is_integrated(cp, &fx.a.hash()).unwrap(),
        exp.a_integrated
    );
    assert_eq!(
        session.is_integrated(cp, &fx.c.hash()).unwrap(),
        exp.c_integrated
    );
    if exp.a_integrated {
        assert_eq!(
            session.decision(cp, &fx.a.hash()).unwrap().eligibility,
            exp.a_eligibility,
            "delivered {delivered:?}"
        );
    }
    if exp.c_integrated {
        assert_eq!(
            session.decision(cp, &fx.c.hash()).unwrap().eligibility,
            Eligibility::Eligible
        );
    } else if delivered.contains(&Ev::C) {
        // C received but waiting for A.
        assert_eq!(
            session.waiting(cp, &fx.c.hash()).unwrap(),
            Some([fx.a.hash()].into_iter().collect())
        );
    }
    if let Some(auth) = exp.r_authority {
        assert_eq!(
            session.authority(cp, R_ID).unwrap(),
            auth,
            "delivered {delivered:?}"
        );
    }
}

fn replay_ok(session: &Session, t: &Transition) {
    let mut replay = session.hydrate(t.before).unwrap();
    replay
        .apply_patches(TextEncoding::platform_default(), t.patches.clone())
        .unwrap();
    assert_eq!(
        replay,
        session.hydrate(t.after).unwrap(),
        "patch replay mismatch"
    );
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
                flat(&session.hydrate(cp).unwrap()),
                session.decision(cp, &fx.a.hash()).unwrap(),
                session.decision(cp, &fx.c.hash()).unwrap(),
                session.authority(cp, R_ID).unwrap(),
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
        assert_eq!(
            session.hydrate(before).unwrap(),
            session.hydrate(session.current()).unwrap()
        );
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
    assert_eq!(
        session.authority(cp, R_ID).unwrap(),
        Authority::Pending(vec![E1_ID])
    );
    assert_eq!(
        session.authority(cp, E2_ID).unwrap(),
        Authority::Pending(vec![E1_ID])
    );
    assert_eq!(
        session.decision(cp, &fx.a.hash()).unwrap().eligibility,
        Eligibility::Eligible
    );
    // Late E1 must not overwrite E2's conclusion.
    let t = deliver(&mut session, vec![Input::Evidence(fx.e1.clone())]);
    let cp = session.current();
    assert_eq!(session.authority(cp, R_ID).unwrap(), Authority::Invalidated);
    assert_eq!(
        session.decision(cp, &fx.a.hash()).unwrap().eligibility,
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
        &session.hydrate(cp1).unwrap(),
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
        &session.hydrate(session.current()).unwrap(),
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
        session.authority(session.current(), R_ID).unwrap(),
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
        &bob.hydrate(cp0).unwrap(),
        &expect_ints(&[("x", 1), ("y", 2)])
    ));

    // Authoritative R known, H missing.
    let t1 = deliver(
        &mut bob,
        vec![Input::Evidence(r.clone()), Input::Evidence(e1.clone())],
    );
    let cp1 = bob.current();
    assert!(hydrated_eq(&bob.hydrate(cp1).unwrap(), &expect_ints(&[])));
    for c in [&a1, &a2] {
        let d = bob.decision(cp1, &c.hash()).unwrap();
        assert_eq!(d.eligibility, Eligibility::Pending);
        assert!(d.reasons.iter().any(
            |r| matches!(r, Reason::MissingBoundary { event, missing } if *event == R_ID && missing == &vec![h])
        ));
        // Pending eligibility is distinct from missing content dependencies.
        assert!(bob.is_integrated(cp1, &c.hash()).unwrap());
        assert_eq!(bob.waiting(cp1, &c.hash()).unwrap(), None);
    }
    replay_ok(&bob, &t1);
    // Heads unchanged: same content heads, different view.
    assert_eq!(
        bob.capture(cp0).unwrap().spec.heads,
        bob.capture(cp1).unwrap().spec.heads
    );

    // H arrives: A1 eligible, A2 excluded.
    let t2 = deliver(&mut bob, vec![Input::Change(h_change.clone())]);
    let cp2 = bob.current();
    assert!(hydrated_eq(
        &bob.hydrate(cp2).unwrap(),
        &expect_ints(&[("x", 1)])
    ));
    assert_eq!(
        bob.decision(cp2, &a1.hash()).unwrap().eligibility,
        Eligibility::Eligible
    );
    assert_eq!(
        bob.decision(cp2, &a2.hash()).unwrap().eligibility,
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
    assert!(hydrated_eq(&bob.hydrate(cp1).unwrap(), &expect_ints(&[])));
    assert_eq!(
        bob.decision(cp1, &a1.hash()).unwrap().eligibility,
        Eligibility::Pending
    );
    assert!(hydrated_eq(
        &bob.hydrate(cp0).unwrap(),
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
        &bob.hydrate(bob.current()).unwrap(),
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
/// Alice's original (established) context.
const B0: ContextBinding = ContextBinding {
    context: CTX0,
    kind: ContextKind::Established,
};
/// The context linked to fresh grant G: unresolved until G is known.
const BG: ContextBinding = ContextBinding {
    context: CTXG,
    kind: ContextKind::FreshGrant,
};

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
        &session.hydrate(cp).unwrap(),
        &expect_ints(&[("before", 1), ("after", 3)])
    ));
    assert_eq!(
        session.decision(cp, &fx.a1.hash()).unwrap().eligibility,
        Eligibility::Eligible
    );
    assert_eq!(
        session.decision(cp, &fx.a2.hash()).unwrap().eligibility,
        Eligibility::Excluded
    );
    let d3 = session.decision(cp, &fx.a3.hash()).unwrap();
    assert_eq!(d3.eligibility, Eligibility::Eligible);
    assert!(d3.reasons.contains(&Reason::AdmittedByContext(CTXG)));
}

#[test]
fn ex03_fresh_grant_non_prefix_selection() {
    let fx = ex03();
    let bindings = vec![
        Input::Binding(fx.a1.hash(), B0),
        Input::Binding(fx.a2.hash(), B0),
        Input::Binding(fx.a3.hash(), BG),
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
    assert!(!s.is_integrated(cp, &fx.a3.hash()).unwrap());
    assert_eq!(
        s.waiting(cp, &fx.a3.hash()).unwrap(),
        Some([fx.a2.hash()].into_iter().collect())
    );
    assert!(t.patches.is_empty());
    let t = deliver(&mut s, vec![Input::Change(fx.a2.clone())]);
    replay_ok(&s, &t);
    ex03_check_final(&s, &fx);
    // Exactly one put (after), never `during`.
    assert_eq!(t.patches.len(), 1, "{:?}", t.patches);

    // The selection has a hole for one continuing actor: capture and reread.
    let cap = s.capture(s.current()).unwrap().spec.clone();
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
            Input::Binding(fx.a2.hash(), B0),
            Input::Binding(fx.a3.hash(), BG),
            Input::Change(fx.a1.clone()),
            Input::Change(fx.a2.clone()),
            Input::Change(fx.a3.clone()),
            Input::Evidence(fx.r.clone()),
            Input::Evidence(fx.e1.clone()),
            Input::Evidence(fx.g.clone()),
        ],
    );
    let cp = s.current();
    assert!(hydrated_eq(
        &s.hydrate(cp).unwrap(),
        &expect_ints(&[("before", 1), ("after", 3)])
    ));
    // Invalidating R (not G) is what restores `during`.
    deliver(
        &mut s,
        vec![Input::Evidence(Evidence::Invalidates {
            id: E2_ID,
            event: R_ID,
            after: vec![E1_ID],
        })],
    );
    assert!(hydrated_eq(
        &s.hydrate(s.current()).unwrap(),
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
    let vals = s.get_all(all, &ROOT, "title").unwrap();
    assert_eq!(vals.len(), 1);
    assert_eq!(vals[0].0.to_string(), "\"Revised\"");
    // Exclude Bob's B (frontier = A; B is outside).
    let t = deliver(&mut s, exclude_evidence("bob", &[fx.a.hash()]));
    let cp = s.current();
    assert_eq!(
        s.decision(cp, &fx.b.hash()).unwrap().eligibility,
        Eligibility::Excluded
    );
    let vals = s.get_all(cp, &ROOT, "title").unwrap();
    let ids: Vec<String> = vals.iter().map(|v| v.1.to_string()).collect();
    assert_eq!(
        ids,
        vec![format!("1@{}", actor(1)), format!("3@{}", actor(3))],
        "candidates must be A and C, got {vals:?}"
    );
    replay_ok(&s, &t);
    // Conflict is exposed in the hydrated view.
    match s.hydrate(cp).unwrap() {
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
    assert!(hydrated_eq(
        &s.hydrate(s.current()).unwrap(),
        &expect_map(&[])
    ));
    let t = deliver(&mut s, exclude_evidence("bob", &[fx.a.hash()]));
    let cp = s.current();
    // C targeted B only; A reappears.
    assert!(hydrated_eq(
        &s.hydrate(cp).unwrap(),
        &expect_map(&[("title", "Original")])
    ));
    let vals = s.get_all(cp, &ROOT, "title").unwrap();
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
    assert!(hydrated_eq(
        &s.hydrate(s.current()).unwrap(),
        &expect_map(&[])
    ));
    let t = deliver(&mut s, exclude_evidence("bob", &[a.hash()]));
    assert!(hydrated_eq(
        &s.hydrate(s.current()).unwrap(),
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
    assert!(hydrated_eq(
        &s.hydrate(s.current()).unwrap(),
        &expect_map(&[])
    ));
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
        s.decision(cp, &mk.hash()).unwrap().eligibility,
        Eligibility::Excluded
    );
    assert_eq!(
        s.decision(cp, &child.hash()).unwrap().eligibility,
        Eligibility::Eligible
    );
    let view = s.hydrate(cp).unwrap();
    assert!(
        hydrated_eq(&view, &expect_map(&[("title", "Root")])),
        "{view:?}"
    );
    replay_ok(&s, &t);
    // The child is inspectable by object id without restoring M.
    let inside = s.get_all(cp, &section, "title").unwrap();
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
    let after = s.hydrate(cp2).unwrap();
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
    let before_view = s.hydrate(before).unwrap();
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
    assert_eq!(s.hydrate(before).unwrap(), before_view);
    assert!(!s.is_integrated(before, &fx.c.hash()).unwrap());
    assert!(s.doc().get_change_by_hash(&fx.c.hash()).is_none());
    assert_eq!(
        s.decision(before, &fx.a.hash()).unwrap().eligibility,
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
            Input::Binding(fx.a2.hash(), B0),
            Input::Binding(fx.a3.hash(), BG),
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
        &s.hydrate(cp).unwrap(),
        &expect_ints(&[("before", 1), ("after", 3)])
    ));
    assert!(hydrated_eq(
        &s.hydrate(s.current()).unwrap(),
        &expect_ints(&[("before", 1), ("after", 3), ("zed", 9)])
    ));
    assert_eq!(
        s.decision(s.current(), &fx.a2.hash()).unwrap().eligibility,
        Eligibility::Excluded
    );
}

// ---------------------------------------------------------------------------
// Fix wave 1 — review findings
// ---------------------------------------------------------------------------

fn invalidate_r() -> Input {
    Input::Evidence(Evidence::Invalidates {
        id: E2_ID,
        event: R_ID,
        after: vec![E1_ID],
    })
}

/// Finding 4 / EX-10: eligible base 10, excluded +100 (Bob), eligible +5.
struct Ex10 {
    base: Automerge,
    mk: Change,
    inc100: Change,
    inc5: Change,
}

fn ex10() -> Ex10 {
    let base = Automerge::new()
        .with_author(Some(author("carol")))
        .with_actor(actor(3));
    let mut alice = base
        .fork()
        .with_author(Some(author("alice")))
        .with_actor(actor(1));
    alice
        .transact::<_, _, automerge::AutomergeError>(|tx| {
            tx.put(ROOT, "n", ScalarValue::Counter(10.into()))?;
            Ok(())
        })
        .unwrap();
    let mk = alice.get_last_local_change().unwrap();
    let mut bob = alice
        .fork()
        .with_author(Some(author("bob")))
        .with_actor(actor(2));
    bob.transact::<_, _, automerge::AutomergeError>(|tx| {
        tx.increment(ROOT, "n", 100)?;
        Ok(())
    })
    .unwrap();
    let inc100 = bob.get_last_local_change().unwrap();
    let mut carol = bob
        .fork()
        .with_author(Some(author("carol")))
        .with_actor(actor(3));
    carol
        .transact::<_, _, automerge::AutomergeError>(|tx| {
            tx.increment(ROOT, "n", 5)?;
            Ok(())
        })
        .unwrap();
    let inc5 = carol.get_last_local_change().unwrap();
    Ex10 {
        base,
        mk,
        inc100,
        inc5,
    }
}

fn counter_value(v: &Value) -> i64 {
    match v {
        Value::Map(m) => match m.get("n") {
            Some(Value::Scalar(ScalarValue::Counter(c))) => i64::from(c),
            other => panic!("no counter: {other:?}"),
        },
        other => panic!("{other:?}"),
    }
}

#[test]
fn ex10_excluded_increment_self_diff_duplicate_and_restore_replay() {
    let fx = ex10();
    let mut s = Session::new(fx.base.clone());
    deliver(
        &mut s,
        vec![
            Input::Change(fx.mk.clone()),
            Input::Change(fx.inc100.clone()),
            Input::Change(fx.inc5.clone()),
        ],
    );
    assert_eq!(counter_value(&s.hydrate(s.current()).unwrap()), 115);
    let t = deliver(&mut s, exclude_evidence("bob", &[fx.mk.hash()]));
    let cp = s.current();
    assert_eq!(counter_value(&s.hydrate(cp).unwrap()), 15);
    replay_ok(&s, &t);
    // Same-capture diff must be empty.
    let spec = &s.capture(cp).unwrap().spec;
    let self_diff = s.doc().diff_view(spec, spec).unwrap();
    assert!(self_diff.is_empty(), "self diff: {self_diff:?}");
    // Empty delivery group and duplicate delivery are no-ops with replay.
    let t = deliver(&mut s, vec![]);
    assert!(t.patches.is_empty(), "{:?}", t.patches);
    replay_ok(&s, &t);
    let t = deliver(&mut s, vec![Input::Change(fx.inc100.clone())]);
    assert!(t.patches.is_empty(), "{:?}", t.patches);
    assert!(t.status.is_empty());
    assert_eq!(counter_value(&s.hydrate(s.current()).unwrap()), 15);
    // Restore +100: replay must yield 115.
    let t = deliver(&mut s, vec![invalidate_r()]);
    assert_eq!(counter_value(&s.hydrate(s.current()).unwrap()), 115);
    replay_ok(&s, &t);
}

/// Finding 1: bindings are immutable fixture inputs.
#[test]
fn conflicting_context_binding_is_rejected_atomically_in_both_orders() {
    let fx = ex03();
    for order in [[B0, BG], [BG, B0]] {
        let mut s = Session::new(fx.base.clone());
        deliver(
            &mut s,
            vec![
                Input::Binding(fx.a1.hash(), B0),
                Input::Binding(fx.a2.hash(), order[0]),
                Input::Binding(fx.a3.hash(), BG),
                Input::Change(fx.a1.clone()),
                Input::Change(fx.a2.clone()),
                Input::Evidence(fx.r.clone()),
                Input::Evidence(fx.e1.clone()),
                Input::Evidence(fx.g.clone()),
            ],
        );
        // Identical binding: idempotent, no status delta.
        let t = deliver(&mut s, vec![Input::Binding(fx.a2.hash(), order[0])]);
        assert!(t.patches.is_empty() && t.status.is_empty());
        let before = s.current();
        let count_before = s.checkpoint_count();
        let before_view = s.hydrate(before).unwrap();
        let inspection_before = s.capture(before).unwrap().inspection.clone();
        // Conflicting binding in a group with otherwise-valid content A3.
        let err = s
            .deliver(vec![
                Input::Change(fx.a3.clone()),
                Input::Binding(fx.a2.hash(), order[1]),
            ])
            .err()
            .expect("conflicting binding rejected");
        assert!(
            matches!(
                err,
                automerge::eligibility::SessionError::ConflictingBinding { .. }
            ),
            "{err:?}"
        );
        assert_eq!(s.current(), before);
        assert_eq!(s.checkpoint_count(), count_before);
        assert_eq!(s.hydrate(before).unwrap(), before_view);
        assert_eq!(
            *s.capture(before).unwrap().inspection,
            *inspection_before,
            "frozen inspection must be unchanged"
        );
        assert!(!s.is_integrated(s.current(), &fx.a3.hash()).unwrap());
        assert!(s.doc().get_change_by_hash(&fx.a3.hash()).is_none());
        assert_eq!(
            s.capture(s.current()).unwrap().inspection.bindings[&fx.a2.hash()],
            order[0]
        );
    }
    // Rebinding A2 to CTXG after the fact cannot admit it.
    let mut s = Session::new(fx.base.clone());
    deliver(
        &mut s,
        vec![
            Input::Binding(fx.a2.hash(), B0),
            Input::Binding(fx.a3.hash(), BG),
            Input::Change(fx.a1.clone()),
            Input::Change(fx.a2.clone()),
            Input::Change(fx.a3.clone()),
            Input::Evidence(fx.r.clone()),
            Input::Evidence(fx.e1.clone()),
            Input::Evidence(fx.g.clone()),
        ],
    );
    ex03_check_final(&s, &fx);
    assert!(s.deliver(vec![Input::Binding(fx.a2.hash(), BG)]).is_err());
    ex03_check_final(&s, &fx);
}

/// Finding 2: ViewIds are session-namespaced and checked.
#[test]
fn foreign_view_id_is_rejected_not_misread() {
    let fx = ex01();
    let mut s1 = Session::new(fx.base.clone());
    let mut s2 = Session::new(fx.base.clone());
    deliver(
        &mut s1,
        vec![
            Input::Change(fx.a.clone()),
            Input::Change(fx.c.clone()),
            Input::Evidence(fx.r.clone()),
        ],
    );
    deliver(
        &mut s2,
        vec![
            Input::Change(fx.a.clone()),
            Input::Change(fx.c.clone()),
            Input::Evidence(fx.r.clone()),
            Input::Evidence(fx.e1.clone()),
        ],
    );
    let id1 = s1.current();
    let id2 = s2.current();
    assert_ne!(id1, id2);
    assert!(matches!(
        s2.hydrate(id1),
        Err(automerge::eligibility::SessionError::ForeignView(_))
    ));
    assert!(matches!(
        s2.capture(id1),
        Err(automerge::eligibility::SessionError::ForeignView(_))
    ));
    assert!(s2.decision(id1, &fx.a.hash()).is_err());
    assert!(s2.authority(id1, R_ID).is_err());
    assert!(s2.get_all(id1, &ROOT, "title").is_err());
    // Own ids still work and differ in content.
    assert!(hydrated_eq(
        &s1.hydrate(id1).unwrap(),
        &expect_map(&[("title", "Weekend plan"), ("suggestion", "Camping")])
    ));
    assert!(hydrated_eq(
        &s2.hydrate(id2).unwrap(),
        &expect_map(&[("title", "Weekend plan")])
    ));
}

/// Finding 3: queued-only receipt is an observable inspection transition;
/// duplicates are not.
#[test]
fn queued_only_receipt_is_an_inspection_transition() {
    let fx = ex01();
    let mut s = Session::new(fx.base.clone());
    let t = deliver(&mut s, vec![Input::Change(fx.c.clone())]);
    assert!(t.patches.is_empty());
    assert!(!t.status.is_empty(), "queued receipt must be signalled");
    assert_eq!(
        t.status.waiting_changes.get(&fx.c.hash()),
        Some(&(None, Some([fx.a.hash()].into_iter().collect())))
    );
    // Duplicate queued receipt: no-op.
    let t = deliver(&mut s, vec![Input::Change(fx.c.clone())]);
    assert!(
        t.patches.is_empty() && t.status.is_empty(),
        "{:?}",
        t.status
    );
    // A arrives: C leaves the waiting inventory and integrates.
    let t = deliver(&mut s, vec![Input::Change(fx.a.clone())]);
    assert_eq!(
        t.status.waiting_changes.get(&fx.c.hash()),
        Some(&(Some([fx.a.hash()].into_iter().collect()), None))
    );
    assert!(t.status.newly_integrated.contains(&fx.c.hash()));
    // Binding receipt and reason changes are signalled too.
    let t = deliver(&mut s, vec![Input::Binding(fx.a.hash(), B0)]);
    assert!(!t.status.is_empty());
    assert_eq!(
        t.status.binding_changes.get(&fx.a.hash()),
        Some(&(None, B0))
    );
    // Reason-only change: A is already excluded by R; a second authorized
    // revocation R2 adds an `OutsideFrontier` reason without changing
    // eligibility.
    deliver(
        &mut s,
        vec![
            Input::Evidence(fx.r.clone()),
            Input::Evidence(fx.e1.clone()),
        ],
    );
    assert_eq!(
        s.decision(s.current(), &fx.a.hash()).unwrap().eligibility,
        Eligibility::Excluded
    );
    let r2 = EventId(20);
    let t = deliver(
        &mut s,
        vec![
            Input::Evidence(Evidence::Revocation {
                id: r2,
                target: author("alice"),
                frontier: vec![],
            }),
            Input::Evidence(Evidence::Authorizes {
                id: EventId(21),
                event: r2,
            }),
        ],
    );
    assert!(t.status.eligibility_changes.is_empty());
    let (before_reasons, after_reasons) = t.status.reason_changes.get(&fx.a.hash()).unwrap();
    assert_eq!(before_reasons.len(), 1);
    assert_eq!(after_reasons.len(), 2);
    assert!(after_reasons
        .iter()
        .any(|r| matches!(r, Reason::OutsideFrontier { event, .. } if *event == r2)));
}

// ---------------------------------------------------------------------------
// Fix wave 1 — items 5 and 6
// ---------------------------------------------------------------------------

/// Item 5 / EX-02: an isolated view at fixed content heads. H integrates in
/// the session (heads advance), but a view pinned at the *old* heads with
/// the *new* classification shows A1 while the old capture stays pending.
#[test]
fn ex02_fixed_content_heads_view_resolves_without_moving_heads() {
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
    let h = carol.empty_commit(Default::default());
    let h_change = carol.get_change_by_hash(&h).unwrap();
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
        vec![
            Input::Change(a1.clone()),
            Input::Change(a2.clone()),
            Input::Evidence(r),
            Input::Evidence(e1),
        ],
    );
    let pending = bob.current();
    let pending_heads = bob.capture(pending).unwrap().spec.heads.clone();
    assert_eq!(pending_heads, vec![a2.hash()]);
    assert!(hydrated_eq(
        &bob.hydrate(pending).unwrap(),
        &expect_ints(&[])
    ));

    // H integrates; session heads move on.
    deliver(&mut bob, vec![Input::Change(h_change)]);
    let resolved = bob.current();
    assert_ne!(bob.capture(resolved).unwrap().spec.heads, pending_heads);

    // New view: resolved classification at the *unchanged* content heads.
    let fixed = bob.view_at(resolved, &pending_heads).unwrap();
    assert_eq!(fixed.heads, pending_heads);
    let fixed_view = bob.doc().hydrate_view(&fixed).unwrap();
    assert!(
        hydrated_eq(&fixed_view, &expect_ints(&[("x", 1)])),
        "{fixed_view:?}"
    );
    assert_eq!(fixed.selection.get(&a1.hash()), Some(Eligibility::Eligible));
    assert_eq!(fixed.selection.get(&a2.hash()), Some(Eligibility::Excluded));
    // H is outside the fixed heads: it is not part of this view's content.
    assert_eq!(fixed.selection.get(&h), None);

    // Old snapshot stays pending.
    assert!(hydrated_eq(
        &bob.hydrate(pending).unwrap(),
        &expect_ints(&[])
    ));
    assert_eq!(
        bob.decision(pending, &a1.hash()).unwrap().eligibility,
        Eligibility::Pending
    );

    // Patch transition between the pending capture and the fixed-heads view.
    let old_spec = bob.capture(pending).unwrap().spec.clone();
    let patches = bob.doc().diff_view(&old_spec, &fixed).unwrap();
    assert_eq!(patches.len(), 1, "{patches:?}");
    let mut replay = bob.hydrate(pending).unwrap();
    replay
        .apply_patches(TextEncoding::platform_default(), patches)
        .unwrap();
    assert_eq!(replay, fixed_view);
}

/// Item 6 / EX-03: complete capture round-trip through a disposable
/// envelope. Reconstruction replays frozen per-checkpoint inputs; it must not
/// use later evidence to reinterpret an earlier pending checkpoint.
#[test]
fn ex03_envelope_round_trip_preserves_gap_and_pending_checkpoint() {
    let fx = ex03();
    let mut s = Session::new(fx.base.clone());
    // Checkpoint 1: R authoritative but its frontier A1 is not yet known ->
    // A2/A3 not present either; deliver A2, A3 only (they wait for A1).
    deliver(
        &mut s,
        vec![
            Input::Binding(fx.a2.hash(), B0),
            Input::Binding(fx.a3.hash(), BG),
            Input::Change(fx.a2.clone()),
            Input::Change(fx.a3.clone()),
            Input::Evidence(fx.r.clone()),
            Input::Evidence(fx.e1.clone()),
        ],
    );
    let waiting_cp = s.current();
    assert_eq!(
        s.waiting(waiting_cp, &fx.a2.hash()).unwrap(),
        Some([fx.a1.hash()].into_iter().collect())
    );
    // Checkpoint 2: A1 arrives; G not yet known -> A3 *pending* (its fresh
    // context is unresolved), A2 excluded.
    deliver(&mut s, vec![Input::Change(fx.a1.clone())]);
    let pre_grant_cp = s.current();
    assert_eq!(
        s.decision(pre_grant_cp, &fx.a3.hash()).unwrap().eligibility,
        Eligibility::Pending
    );
    assert_eq!(
        s.decision(pre_grant_cp, &fx.a2.hash()).unwrap().eligibility,
        Eligibility::Excluded
    );
    assert!(hydrated_eq(
        &s.hydrate(pre_grant_cp).unwrap(),
        &expect_ints(&[("before", 1)])
    ));
    // Checkpoint 3: G -> gap selection.
    deliver(&mut s, vec![Input::Evidence(fx.g.clone())]);
    let final_cp = s.current();
    ex03_check_final(&s, &fx);

    let envelope = s.export();
    // Content-only export does not carry interpretation.
    let content_only = Automerge::load(&s.doc().save()).unwrap();
    assert_eq!(content_only.get_heads(), s.doc().get_heads());
    assert!(hydrated_eq(
        &content_only.hydrate(None),
        &expect_ints(&[("before", 1), ("during", 2), ("after", 3)])
    ));

    let restored = Session::restore(envelope).expect("envelope restores");
    assert_eq!(restored.checkpoint_count(), s.checkpoint_count());
    let r_final = restored.checkpoint(final_cp.index()).unwrap();
    ex03_check_final(&restored, &fx);
    assert!(hydrated_eq(
        &restored.hydrate(r_final).unwrap(),
        &expect_ints(&[("before", 1), ("after", 3)])
    ));
    // Pending decision restored although G is known in later checkpoints.
    let r_pre = restored.checkpoint(pre_grant_cp.index()).unwrap();
    let d3 = restored.decision(r_pre, &fx.a3.hash()).unwrap();
    assert_eq!(d3.eligibility, Eligibility::Pending);
    assert!(d3.reasons.contains(&Reason::UnresolvedGrant(CTXG)));
    assert!(hydrated_eq(
        &restored.hydrate(r_pre).unwrap(),
        &expect_ints(&[("before", 1)])
    ));
    let r_wait = restored.checkpoint(waiting_cp.index()).unwrap();
    assert_eq!(
        restored.waiting(r_wait, &fx.a2.hash()).unwrap(),
        Some([fx.a1.hash()].into_iter().collect())
    );
    assert!(!restored.is_integrated(r_wait, &fx.a2.hash()).unwrap());
    // Restored ids are namespaced to the restored session.
    assert!(restored.hydrate(final_cp).is_err());
}

/// A tampered envelope (frozen table disagrees with re-evaluation of its own
/// frozen inputs) is rejected, not silently reinterpreted.
#[test]
fn envelope_with_inconsistent_frozen_table_is_rejected() {
    let fx = ex03();
    let mut s = Session::new(fx.base.clone());
    deliver(
        &mut s,
        vec![
            Input::Binding(fx.a2.hash(), B0),
            Input::Change(fx.a1.clone()),
            Input::Change(fx.a2.clone()),
            Input::Evidence(fx.r.clone()),
            Input::Evidence(fx.e1.clone()),
        ],
    );
    let mut envelope = s.export();
    envelope.tamper_last_checkpoint_selection(&fx.a2.hash(), Eligibility::Eligible);
    assert!(matches!(
        Session::restore(envelope),
        Err(automerge::eligibility::SessionError::InconsistentEnvelope { .. })
    ));
}

// ---------------------------------------------------------------------------
// Fix wave 2 — re-review findings A/B and assertion repairs
// ---------------------------------------------------------------------------

/// Finding A: a change in a fresh-grant-linked context is *pending* until the
/// grant is known; an original-context change stays excluded; no allow-all.
#[test]
fn ex03_fresh_context_is_pending_until_grant_arrives() {
    let fx = ex03();
    let mut s = Session::new(fx.base.clone());
    let t0 = deliver(
        &mut s,
        vec![
            Input::Binding(fx.a1.hash(), B0),
            Input::Binding(fx.a2.hash(), B0),
            Input::Binding(fx.a3.hash(), BG),
            Input::Change(fx.a1.clone()),
            Input::Change(fx.a2.clone()),
            Input::Change(fx.a3.clone()),
            Input::Evidence(fx.r.clone()),
            Input::Evidence(fx.e1.clone()),
        ],
    );
    replay_ok(&s, &t0);
    let pending_cp = s.current();
    let d3 = s.decision(pending_cp, &fx.a3.hash()).unwrap();
    assert_eq!(d3.eligibility, Eligibility::Pending);
    assert!(
        d3.reasons.contains(&Reason::UnresolvedGrant(CTXG)),
        "{d3:?}"
    );
    assert_eq!(
        s.decision(pending_cp, &fx.a2.hash()).unwrap().eligibility,
        Eligibility::Excluded
    );
    assert_eq!(
        s.decision(pending_cp, &fx.a1.hash()).unwrap().eligibility,
        Eligibility::Eligible
    );
    // Default materialization omits pending work.
    assert!(hydrated_eq(
        &s.hydrate(pending_cp).unwrap(),
        &expect_ints(&[("before", 1)])
    ));

    // G arrives: A3 Pending -> Eligible, A2 unchanged, one patch, replay.
    let t = deliver(&mut s, vec![Input::Evidence(fx.g.clone())]);
    assert_eq!(
        t.status.eligibility_changes.get(&fx.a3.hash()),
        Some(&(Eligibility::Pending, Eligibility::Eligible))
    );
    assert!(!t.status.eligibility_changes.contains_key(&fx.a2.hash()));
    assert_eq!(t.patches.len(), 1, "{:?}", t.patches);
    replay_ok(&s, &t);
    ex03_check_final(&s, &fx);
    // Frozen pending checkpoint unchanged.
    assert_eq!(
        s.decision(pending_cp, &fx.a3.hash()).unwrap().eligibility,
        Eligibility::Pending
    );
}

/// Finding A (negative): a fresh-context change with *no* revocation in play
/// is still pending without its grant — missing authorization evidence is not
/// allow-all — while an established-context change is eligible.
#[test]
fn fresh_context_without_grant_is_not_allow_all() {
    let fx = ex03();
    let mut s = Session::new(fx.base.clone());
    deliver(
        &mut s,
        vec![
            Input::Binding(fx.a1.hash(), B0),
            Input::Binding(fx.a2.hash(), BG),
            Input::Change(fx.a1.clone()),
            Input::Change(fx.a2.clone()),
        ],
    );
    let cp = s.current();
    assert_eq!(
        s.decision(cp, &fx.a1.hash()).unwrap().eligibility,
        Eligibility::Eligible
    );
    assert_eq!(
        s.decision(cp, &fx.a2.hash()).unwrap().eligibility,
        Eligibility::Pending
    );
    assert!(hydrated_eq(
        &s.hydrate(cp).unwrap(),
        &expect_ints(&[("before", 1)])
    ));
}

/// Finding B: the envelope preserves the document's text encoding.
#[test]
fn envelope_preserves_non_default_text_encoding() {
    let encoding = TextEncoding::Utf16CodeUnit;
    assert_ne!(encoding, TextEncoding::platform_default());
    let base = Automerge::new_with_encoding(encoding)
        .with_author(Some(author("carol")))
        .with_actor(actor(3));
    let mut alice = base
        .fork()
        .with_author(Some(author("alice")))
        .with_actor(actor(1));
    let a1 = tx_put(&mut alice, "x", 1);
    let mut s = Session::new(base);
    deliver(&mut s, vec![Input::Change(a1.clone())]);
    assert_eq!(s.doc().text_encoding(), encoding);
    let restored = Session::restore(s.export()).unwrap();
    assert_eq!(restored.doc().text_encoding(), encoding);
    assert_eq!(restored.doc().get_heads(), s.doc().get_heads());
    assert!(hydrated_eq(
        &restored.hydrate(restored.current()).unwrap(),
        &expect_ints(&[("x", 1)])
    ));
}

/// Out-of-range checkpoint index is rejected explicitly.
#[test]
fn out_of_range_checkpoint_is_rejected() {
    let fx = ex01();
    let s = Session::new(fx.base.clone());
    assert_eq!(s.checkpoint_count(), 1);
    assert!(s.checkpoint(0).is_ok());
    assert!(matches!(
        s.checkpoint(1),
        Err(automerge::eligibility::SessionError::ForeignView(_))
    ));
}

// ---------------------------------------------------------------------------
// A3 — EX-04: inspect excluded work, adopt as a new eligible change
// ---------------------------------------------------------------------------

const CTX_BOB: ContextBinding = ContextBinding {
    context: AuthorizationContextId(42),
    kind: ContextKind::Established,
};

#[test]
fn ex04_inspect_then_adopt_scalar_without_reinstating_alice() {
    let fx = ex01();
    let mut s = Session::new(fx.base.clone());
    deliver(
        &mut s,
        vec![
            Input::Change(fx.a.clone()),
            Input::Change(fx.c.clone()),
            Input::Evidence(fx.r.clone()),
            Input::Evidence(fx.e1.clone()),
        ],
    );
    let view = s.current();
    assert!(hydrated_eq(
        &s.hydrate(view).unwrap(),
        &expect_map(&[("title", "Weekend plan")])
    ));
    // Inspection: A's excluded suggestion is visible under an allow-all
    // inspection view of the same heads, and A is Excluded.
    let all = s.inspect_all(view).unwrap();
    let sugg = s.doc().get_all_view(&all, &ROOT, "suggestion").unwrap();
    assert_eq!(sugg.len(), 1);
    assert_eq!(sugg[0].0.to_string(), "\"Camping\"");
    assert_eq!(sugg[0].1.to_string(), format!("2@{}", actor(1)));
    assert_eq!(
        s.decision(view, &fx.a.hash()).unwrap().eligibility,
        Eligibility::Excluded
    );

    // Bob adopts: authors D against his eligible view.
    let bob_actor = actor(2);
    let (t, d_hash) = s
        .author(view, author("bob"), bob_actor.clone(), CTX_BOB, |tx| {
            // Bob sees no suggestion in his view.
            assert!(tx.get(ROOT, "suggestion").unwrap().is_none());
            tx.put(ROOT, "suggestion", "Camping")?;
            Ok(())
        })
        .unwrap();
    let d_hash = d_hash.expect("D committed");
    replay_ok(&s, &t);
    let after = s.current();
    assert!(hydrated_eq(
        &s.hydrate(after).unwrap(),
        &expect_map(&[("title", "Weekend plan"), ("suggestion", "Camping")])
    ));
    // A stays excluded; D is a new eligible change with Bob's identity.
    assert_eq!(
        s.decision(after, &fx.a.hash()).unwrap().eligibility,
        Eligibility::Excluded
    );
    assert_eq!(
        s.decision(after, &d_hash).unwrap().eligibility,
        Eligibility::Eligible
    );
    assert_ne!(d_hash, fx.a.hash());
    let d = s.doc().get_change_by_hash(&d_hash).unwrap();
    assert_eq!(d.author().unwrap(), author("bob"));
    assert_eq!(d.actor_id(), &bob_actor);
    // D's deps are the captured heads; its op does not target hidden A.
    let mut deps = d.deps().to_vec();
    deps.sort();
    let mut heads = s.capture(view).unwrap().spec.heads.clone();
    heads.sort();
    assert_eq!(deps, heads);
    let expanded = d.decode();
    assert_eq!(expanded.operations.len(), 1);
    assert!(
        expanded.operations[0].pred.is_empty(),
        "D must not target hidden A: {:?}",
        expanded.operations[0].pred
    );
    // Visible candidate is D's op, not A's.
    let vals = s.get_all(after, &ROOT, "suggestion").unwrap();
    assert_eq!(vals.len(), 1);
    assert_eq!(vals[0].1.to_string(), format!("4@{}", bob_actor));
    // Context bound to D.
    assert_eq!(
        s.capture(after).unwrap().inspection.bindings.get(&d_hash),
        Some(&CTX_BOB)
    );

    // If A later returns, ordinary CRDT rules decide: A and D are concurrent
    // candidates for `suggestion`.
    let t = deliver(&mut s, vec![invalidate_r()]);
    replay_ok(&s, &t);
    let vals = s.get_all(s.current(), &ROOT, "suggestion").unwrap();
    let mut ids: Vec<String> = vals.iter().map(|v| v.1.to_string()).collect();
    ids.sort();
    assert_eq!(
        ids,
        vec![format!("2@{}", actor(1)), format!("4@{}", bob_actor)]
    );
}

/// Authoring into a structurally present but hidden container by object id
/// remains allowed (ruling 2); the edit is eligible but unreachable.
#[test]
fn ex04_variant_object_id_edit_into_hidden_container_allowed() {
    let base = Automerge::new()
        .with_author(Some(author("carol")))
        .with_actor(actor(3));
    let mut alice = base
        .fork()
        .with_author(Some(author("alice")))
        .with_actor(actor(1));
    let section = alice
        .transact::<_, _, automerge::AutomergeError>(|tx| {
            Ok(tx.put_object(ROOT, "section", automerge::ObjType::Map)?)
        })
        .unwrap()
        .result;
    let mk = alice.get_last_local_change().unwrap();
    let mut s = Session::new(base);
    deliver(&mut s, vec![Input::Change(mk.clone())]);
    deliver(&mut s, exclude_evidence("alice", &[]));
    let view = s.current();
    let (t, d) = s
        .author(view, author("bob"), actor(2), CTX_BOB, |tx| {
            tx.put(&section, "title", "Plans")?;
            Ok(())
        })
        .unwrap();
    assert!(d.is_some());
    assert!(t.patches.is_empty(), "unreachable edit: {:?}", t.patches);
    assert!(hydrated_eq(
        &s.hydrate(s.current()).unwrap(),
        &expect_map(&[])
    ));
    let inside = s.get_all(s.current(), &section, "title").unwrap();
    assert_eq!(inside.len(), 1);
}

// ---------------------------------------------------------------------------
// A3 — EX-08/09 lists
// ---------------------------------------------------------------------------

fn list_strs(
    s: &Session,
    id: automerge::eligibility::ViewId,
    list: &automerge::ObjId,
) -> Vec<String> {
    s.doc()
        .list_view(&s.capture(id).unwrap().spec, list)
        .unwrap()
        .into_iter()
        .map(|(_, v, _)| v.to_string())
        .collect()
}

fn list_ids(
    s: &Session,
    id: automerge::eligibility::ViewId,
    list: &automerge::ObjId,
) -> Vec<String> {
    s.doc()
        .list_view(&s.capture(id).unwrap().spec, list)
        .unwrap()
        .into_iter()
        .map(|(_, _, e)| e.to_string())
        .collect()
}

fn hydrated_list_strs(v: &Value, key: &str) -> Vec<String> {
    match v {
        Value::Map(m) => match m.get(key) {
            Some(Value::List(l)) => l.iter().map(|lv| format!("{:?}", lv.value)).collect(),
            other => panic!("{other:?}"),
        },
        other => panic!("{other:?}"),
    }
}

struct ListFx {
    base: Automerge,
    list: automerge::ObjId,
    x: Change,
}

/// Carol creates [L, R]; Alice inserts X after L.
fn list_fx() -> ListFx {
    let mut base = Automerge::new()
        .with_author(Some(author("carol")))
        .with_actor(actor(3));
    let list = base
        .transact::<_, _, automerge::AutomergeError>(|tx| {
            let l = tx.put_object(ROOT, "items", automerge::ObjType::List)?;
            tx.insert(&l, 0, "L")?;
            tx.insert(&l, 1, "R")?;
            Ok(l)
        })
        .unwrap()
        .result;
    let mut alice = base
        .fork()
        .with_author(Some(author("alice")))
        .with_actor(actor(1));
    alice
        .transact::<_, _, automerge::AutomergeError>(|tx| {
            tx.insert(&list, 1, "X")?;
            Ok(())
        })
        .unwrap();
    let x = alice.get_last_local_change().unwrap();
    ListFx { base, list, x }
}

#[test]
fn ex08_insertion_after_excluded_anchor_survives_with_stable_order() {
    let fx = list_fx();
    // Bob inserts Y after X; Carol concurrently inserts Z after L (near X).
    let mut bob = fx
        .base
        .fork()
        .with_author(Some(author("bob")))
        .with_actor(actor(2));
    bob.apply_changes([fx.x.clone()]).unwrap();
    bob.transact::<_, _, automerge::AutomergeError>(|tx| {
        tx.insert(&fx.list, 2, "Y")?;
        Ok(())
    })
    .unwrap();
    let y = bob.get_last_local_change().unwrap();
    let mut carol = fx
        .base
        .fork()
        .with_author(Some(author("carol")))
        .with_actor(actor(3));
    carol
        .transact::<_, _, automerge::AutomergeError>(|tx| {
            tx.insert(&fx.list, 1, "Z")?;
            Ok(())
        })
        .unwrap();
    let z = carol.get_last_local_change().unwrap();

    let mut s = Session::new(fx.base.clone());
    deliver(
        &mut s,
        vec![
            Input::Change(fx.x.clone()),
            Input::Change(y.clone()),
            Input::Change(z.clone()),
        ],
    );
    let all = s.current();
    let full = list_strs(&s, all, &fx.list);
    let full_ids = list_ids(&s, all, &fx.list);
    assert_eq!(full.len(), 5);
    assert_eq!(full[0], "\"L\"");
    assert_eq!(full[4], "\"R\"");
    // Exclude Alice's X.
    let t = deliver(&mut s, exclude_evidence("alice", &[]));
    let cp = s.current();
    let vis = list_strs(&s, cp, &fx.list);
    let vis_ids = list_ids(&s, cp, &fx.list);
    assert_eq!(vis.len(), 4);
    assert!(!vis.contains(&"\"X\"".to_string()));
    assert!(vis.contains(&"\"Y\"".to_string()));
    // Surviving identities keep their relative order from the full view.
    let expected_ids: Vec<String> = full_ids
        .iter()
        .filter(|id| vis_ids.contains(id))
        .cloned()
        .collect();
    assert_eq!(vis_ids, expected_ids);
    // Patches index the *visible* view and replay through hydrate.
    replay_ok(&s, &t);
    assert_eq!(
        hydrated_list_strs(&s.hydrate(cp).unwrap(), "items"),
        vis.iter()
            .map(|v| format!("Scalar(Str({v}))"))
            .collect::<Vec<_>>()
    );
    // Restore X: original value reappears in the retained order; Y unchanged.
    let t = deliver(&mut s, vec![invalidate_r()]);
    replay_ok(&s, &t);
    assert_eq!(list_strs(&s, s.current(), &fx.list), full);
    assert_eq!(list_ids(&s, s.current(), &fx.list), full_ids);
}

#[test]
fn ex08_capture_reread_after_earlier_sorting_actor_arrives() {
    let fx = list_fx();
    let mut bob = fx
        .base
        .fork()
        .with_author(Some(author("bob")))
        .with_actor(actor(2));
    bob.apply_changes([fx.x.clone()]).unwrap();
    bob.transact::<_, _, automerge::AutomergeError>(|tx| {
        tx.insert(&fx.list, 2, "Y")?;
        Ok(())
    })
    .unwrap();
    let y = bob.get_last_local_change().unwrap();
    let mut s = Session::new(fx.base.clone());
    deliver(&mut s, vec![Input::Change(fx.x.clone()), Input::Change(y)]);
    deliver(&mut s, exclude_evidence("alice", &[]));
    let cp = s.current();
    let before = list_strs(&s, cp, &fx.list);
    assert_eq!(before, vec!["\"L\"", "\"Y\"", "\"R\""]);
    // Actor 0x00 sorts before everyone; its arrival shifts actor indices.
    let mut zed = fx
        .base
        .fork()
        .with_author(Some(author("zed")))
        .with_actor(actor(0));
    zed.transact::<_, _, automerge::AutomergeError>(|tx| {
        tx.insert(&fx.list, 2, "W")?;
        Ok(())
    })
    .unwrap();
    let w = zed.get_last_local_change().unwrap();
    let t = deliver(&mut s, vec![Input::Change(w)]);
    replay_ok(&s, &t);
    assert_eq!(
        list_strs(&s, cp, &fx.list),
        before,
        "old capture must reread identically"
    );
    let now = list_strs(&s, s.current(), &fx.list);
    assert_eq!(now.len(), 4);
    assert!(!now.contains(&"\"X\"".to_string()));
}

#[test]
fn ex09_eligible_replacement_at_excluded_insertion_identity() {
    let fx = list_fx();
    // Bob assigns Y to X's element identity (put at index 1, not insert).
    let mut bob = fx
        .base
        .fork()
        .with_author(Some(author("bob")))
        .with_actor(actor(2));
    bob.apply_changes([fx.x.clone()]).unwrap();
    bob.transact::<_, _, automerge::AutomergeError>(|tx| {
        tx.put(&fx.list, 1, "Y")?;
        Ok(())
    })
    .unwrap();
    let y = bob.get_last_local_change().unwrap();
    // Y's op targets X's value op as predecessor (same element identity).
    let yexp = y.decode();
    assert_eq!(yexp.operations.len(), 1);
    assert!(!yexp.operations[0].insert);
    assert_eq!(yexp.operations[0].pred.len(), 1);

    let mut s = Session::new(fx.base.clone());
    deliver(
        &mut s,
        vec![Input::Change(fx.x.clone()), Input::Change(y.clone())],
    );
    assert_eq!(
        list_strs(&s, s.current(), &fx.list),
        vec!["\"L\"", "\"Y\"", "\"R\""]
    );
    let t = deliver(&mut s, exclude_evidence("alice", &[]));
    let cp = s.current();
    assert_eq!(
        list_strs(&s, cp, &fx.list),
        vec!["\"L\"", "\"Y\"", "\"R\""],
        "replacement value survives on retained element identity"
    );
    assert_eq!(list_ids(&s, cp, &fx.list)[1], format!("5@{}", actor(2)));
    replay_ok(&s, &t);
    // Restore X: Y's put still supersedes X's value (direct predecessor).
    let t = deliver(&mut s, vec![invalidate_r()]);
    replay_ok(&s, &t);
    assert_eq!(
        list_strs(&s, s.current(), &fx.list),
        vec!["\"L\"", "\"Y\"", "\"R\""]
    );
}

#[test]
fn ex09_variant_replace_excluded_map_element_with_eligible_map() {
    let mut base = Automerge::new()
        .with_author(Some(author("carol")))
        .with_actor(actor(3));
    let list = base
        .transact::<_, _, automerge::AutomergeError>(|tx| {
            let l = tx.put_object(ROOT, "items", automerge::ObjType::List)?;
            tx.insert(&l, 0, "L")?;
            Ok(l)
        })
        .unwrap()
        .result;
    let mut alice = base
        .fork()
        .with_author(Some(author("alice")))
        .with_actor(actor(1));
    let old_map = alice
        .transact::<_, _, automerge::AutomergeError>(|tx| {
            let m = tx.insert_object(&list, 1, automerge::ObjType::Map)?;
            tx.put(&m, "k", "old")?;
            Ok(m)
        })
        .unwrap()
        .result;
    let x = alice.get_last_local_change().unwrap();
    let mut bob = base
        .fork()
        .with_author(Some(author("bob")))
        .with_actor(actor(2));
    bob.apply_changes([x.clone()]).unwrap();
    let new_map = bob
        .transact::<_, _, automerge::AutomergeError>(|tx| {
            let m = tx.put_object(&list, 1, automerge::ObjType::Map)?;
            tx.put(&m, "k", "new")?;
            Ok(m)
        })
        .unwrap()
        .result;
    let y = bob.get_last_local_change().unwrap();
    let mut s = Session::new(base);
    deliver(&mut s, vec![Input::Change(x), Input::Change(y)]);
    let t = deliver(&mut s, exclude_evidence("alice", &[]));
    replay_ok(&s, &t);
    let cp = s.current();
    let items = s
        .doc()
        .list_view(&s.capture(cp).unwrap().spec, &list)
        .unwrap();
    assert_eq!(items.len(), 2);
    assert_eq!(items[1].2, new_map);
    let hv = s.hydrate(cp).unwrap();
    match &hv {
        Value::Map(m) => match m.get("items") {
            Some(Value::List(l)) => match &l.iter().nth(1).unwrap().value {
                Value::Map(inner) => assert_eq!(
                    inner.get("k"),
                    Some(&Value::Scalar(ScalarValue::Str("new".into())))
                ),
                other => panic!("{other:?}"),
            },
            other => panic!("{other:?}"),
        },
        other => panic!("{other:?}"),
    }
    // Old map is not adopted: its child is not visible through the new map.
    let old_children = s.get_all(cp, &old_map, "k").unwrap();
    assert_eq!(old_children.len(), 0, "excluded child must not be visible");
}

// ---------------------------------------------------------------------------
// A3 — EX-11 counters
// ---------------------------------------------------------------------------

fn counter_opt(v: &Value) -> Option<i64> {
    match v {
        Value::Map(m) => match m.get("n") {
            Some(Value::Scalar(ScalarValue::Counter(c))) => Some(i64::from(c)),
            None => None,
            other => panic!("unexpected: {other:?}"),
        },
        other => panic!("{other:?}"),
    }
}

#[test]
fn ex11_excluded_counter_base_supplies_nothing_to_eligible_increment() {
    let base = Automerge::new()
        .with_author(Some(author("carol")))
        .with_actor(actor(3));
    let mut bob = base
        .fork()
        .with_author(Some(author("bob")))
        .with_actor(actor(2));
    bob.transact::<_, _, automerge::AutomergeError>(|tx| {
        tx.put(ROOT, "n", ScalarValue::Counter(100.into()))?;
        Ok(())
    })
    .unwrap();
    let b = bob.get_last_local_change().unwrap();
    let mut carol = bob
        .fork()
        .with_author(Some(author("carol")))
        .with_actor(actor(3));
    carol
        .transact::<_, _, automerge::AutomergeError>(|tx| {
            tx.increment(ROOT, "n", 5)?;
            Ok(())
        })
        .unwrap();
    let c = carol.get_last_local_change().unwrap();
    let mut s = Session::new(base);
    deliver(
        &mut s,
        vec![Input::Change(b.clone()), Input::Change(c.clone())],
    );
    assert_eq!(counter_opt(&s.hydrate(s.current()).unwrap()), Some(105));
    let t = deliver(&mut s, exclude_evidence("bob", &[]));
    let cp = s.current();
    assert_eq!(
        counter_opt(&s.hydrate(cp).unwrap()),
        None,
        "no base, no counter"
    );
    assert!(s.get_all(cp, &ROOT, "n").unwrap().is_empty());
    assert_eq!(
        s.decision(cp, &c.hash()).unwrap().eligibility,
        Eligibility::Eligible
    );
    replay_ok(&s, &t);
    let spec = &s.capture(cp).unwrap().spec;
    assert!(s.doc().diff_view(spec, spec).unwrap().is_empty());
}

#[test]
fn ex11_eligible_increment_on_excluded_replacement_base() {
    let base = Automerge::new()
        .with_author(Some(author("carol")))
        .with_actor(actor(3));
    let mut alice = base
        .fork()
        .with_author(Some(author("alice")))
        .with_actor(actor(1));
    alice
        .transact::<_, _, automerge::AutomergeError>(|tx| {
            tx.put(ROOT, "n", ScalarValue::Counter(10.into()))?;
            Ok(())
        })
        .unwrap();
    let a = alice.get_last_local_change().unwrap();
    let mut bob = alice
        .fork()
        .with_author(Some(author("bob")))
        .with_actor(actor(2));
    bob.transact::<_, _, automerge::AutomergeError>(|tx| {
        tx.put(ROOT, "n", ScalarValue::Counter(100.into()))?;
        Ok(())
    })
    .unwrap();
    let b = bob.get_last_local_change().unwrap();
    let mut carol = bob
        .fork()
        .with_author(Some(author("carol")))
        .with_actor(actor(3));
    carol
        .transact::<_, _, automerge::AutomergeError>(|tx| {
            tx.increment(ROOT, "n", 5)?;
            Ok(())
        })
        .unwrap();
    let c = carol.get_last_local_change().unwrap();
    let mut s = Session::new(base);
    deliver(
        &mut s,
        vec![
            Input::Change(a.clone()),
            Input::Change(b.clone()),
            Input::Change(c.clone()),
        ],
    );
    assert_eq!(counter_opt(&s.hydrate(s.current()).unwrap()), Some(105));
    let t = deliver(&mut s, exclude_evidence("bob", &[a.hash()]));
    let cp = s.current();
    // 10, not 15: the +5 is not migrated onto A's base.
    assert_eq!(counter_opt(&s.hydrate(cp).unwrap()), Some(10));
    let vals = s.get_all(cp, &ROOT, "n").unwrap();
    assert_eq!(vals.len(), 1);
    assert_eq!(vals[0].1.to_string(), format!("1@{}", actor(1)));
    replay_ok(&s, &t);
    let t = deliver(&mut s, vec![]);
    assert!(t.patches.is_empty() && t.status.is_empty());
    let t = deliver(&mut s, vec![Input::Change(c.clone())]);
    assert!(t.patches.is_empty() && t.status.is_empty());
    // Restore B: 105 and A suppressed.
    let t = deliver(&mut s, vec![invalidate_r()]);
    replay_ok(&s, &t);
    assert_eq!(counter_opt(&s.hydrate(s.current()).unwrap()), Some(105));
    let vals = s.get_all(s.current(), &ROOT, "n").unwrap();
    assert_eq!(vals.len(), 1);
    assert_eq!(vals[0].1.to_string(), format!("2@{}", actor(2)));
}

/// Compatibility variant: an increment also suppresses a targeted non-counter
/// conflict candidate; excluding the increment re-exposes the candidate.
#[test]
fn ex11_compat_increment_suppresses_targeted_scalar_candidate() {
    let base = Automerge::new()
        .with_author(Some(author("carol")))
        .with_actor(actor(3));
    // Alice: counter 10. Bob concurrently: n = "text". Then Carol (seeing
    // both) increments by 5: the increment targets both candidates.
    let mut alice = base
        .fork()
        .with_author(Some(author("alice")))
        .with_actor(actor(1));
    alice
        .transact::<_, _, automerge::AutomergeError>(|tx| {
            tx.put(ROOT, "n", ScalarValue::Counter(10.into()))?;
            Ok(())
        })
        .unwrap();
    let a = alice.get_last_local_change().unwrap();
    let mut bob = base
        .fork()
        .with_author(Some(author("bob")))
        .with_actor(actor(2));
    bob.transact::<_, _, automerge::AutomergeError>(|tx| {
        tx.put(ROOT, "n", "text")?;
        Ok(())
    })
    .unwrap();
    let b = bob.get_last_local_change().unwrap();
    let mut carol = base
        .fork()
        .with_author(Some(author("carol")))
        .with_actor(actor(3));
    carol.apply_changes([a.clone(), b.clone()]).unwrap();
    let both = carol.get_all(ROOT, "n").unwrap();
    assert_eq!(both.len(), 2);
    carol
        .transact::<_, _, automerge::AutomergeError>(|tx| {
            tx.increment(ROOT, "n", 5)?;
            Ok(())
        })
        .unwrap();
    let c = carol.get_last_local_change().unwrap();
    let cexp = c.decode();
    assert_eq!(
        cexp.operations[0].pred.len(),
        2,
        "increment targets both candidates"
    );
    // Baseline (allow-all) behaviour: recorded here as characterization.
    let baseline: Vec<String> = carol
        .get_all(ROOT, "n")
        .unwrap()
        .iter()
        .map(|v| v.0.to_string())
        .collect();

    let mut s = Session::new(base);
    deliver(
        &mut s,
        vec![
            Input::Change(a.clone()),
            Input::Change(b.clone()),
            Input::Change(c.clone()),
        ],
    );
    let all: Vec<String> = s
        .get_all(s.current(), &ROOT, "n")
        .unwrap()
        .iter()
        .map(|v| v.0.to_string())
        .collect();
    assert_eq!(all, baseline, "allow-all view equals ordinary Automerge");
    // Exclude Carol's increment: both original candidates are exposed.
    let t = deliver(&mut s, exclude_evidence("carol", &[]));
    replay_ok(&s, &t);
    let vals = s.get_all(s.current(), &ROOT, "n").unwrap();
    let mut got: Vec<String> = vals.iter().map(|v| v.0.to_string()).collect();
    got.sort();
    assert_eq!(got, vec!["\"text\"", "Counter: 10"]);
}
