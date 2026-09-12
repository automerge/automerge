//! Prototype A — external interpretation. Disposable experiment; see
//! `docs/prototype/`. Every fixture builds real Automerge changes, feeds them
//! through the experimental `eligibility::Session`, and checks against
//! independent expected states (never the production evaluator).

use std::collections::BTreeMap;

use automerge::eligibility::{
    Authority, Decision, Eligibility, Evidence, EventId, Input, Reason, Session, Transition,
};
use automerge::hydrate::{self, Value};
use automerge::transaction::Transactable;
use automerge::{
    ActorId, Author, Automerge, Change, ReadDoc, ScalarValue, TextEncoding,
    ROOT,
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

fn hydrated_eq(a: &Value, b: &Value) -> bool {
    // Compare ignoring conflict flags: build plain maps of key -> scalar.
    fn flat(v: &Value) -> BTreeMap<String, String> {
        match v {
            Value::Map(m) => m
                .iter()
                .map(|(k, mv)| (k.clone(), format!("{:?}", mv.value)))
                .collect(),
            other => panic!("not a map: {other:?}"),
        }
    }
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
    let e1 = Evidence::Authorizes { id: E1_ID, event: R_ID };
    let e2 = Evidence::Invalidates {
        id: E2_ID,
        event: R_ID,
        after: vec![E1_ID],
    };
    Ex01 { base, a, c, r, e1, e2 }
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
    assert_eq!(session.decision(cp1, &a_hash).eligibility, Eligibility::Eligible);
    assert_eq!(session.decision(cp1, &c_hash).eligibility, Eligibility::Eligible);
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
    assert_eq!(session.decision(cp2, &c_hash).eligibility, Eligibility::Eligible);
    // Patch replay: before + patches == after.
    let mut replay = session.hydrate(cp1);
    replay
        .apply_patches(TextEncoding::platform_default(), t2.patches.clone())
        .unwrap();
    assert_eq!(replay, session.hydrate(cp2));
    assert_eq!(t2.status.eligibility_changes.get(&a_hash), Some(&(Eligibility::Eligible, Eligibility::Excluded)));

    // Checkpoint 3: E2 -> R invalidated, A eligible again (same identity).
    let t3 = deliver(&mut session, vec![Input::Evidence(fx.e2.clone())]);
    let cp3 = session.current();
    assert!(hydrated_eq(
        &session.hydrate(cp3),
        &expect_map(&[("title", "Weekend plan"), ("suggestion", "Camping")])
    ));
    assert_eq!(session.authority(cp3, R_ID), Authority::Invalidated);
    assert_eq!(session.decision(cp3, &a_hash).eligibility, Eligibility::Eligible);
    assert_eq!(session.decision(cp3, &c_hash).eligibility, Eligibility::Eligible);
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
    assert_eq!(session.decision(cp2, &a_hash).eligibility, Eligibility::Excluded);
    assert_eq!(session.authority(cp1, R_ID), Authority::Pending(vec![]));
}
