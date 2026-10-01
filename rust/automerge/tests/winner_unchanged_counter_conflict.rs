//! Unchanged-winner patches when a counter delta and a conflict change land
//! in the same transition.
//!
//! A register (map key or list element) keeps the same winning op while two
//! things change at once: the winner's visible counter total moves, and the
//! number of visible candidates crosses one (a conflict appears or clears).
//! The patches for that transition must carry both facts, whichever path
//! materialises them. Every path routes through `patches::winner_unchanged`, and these
//! tests pin that from the outside.
//!
//! Each test replays the patches for a transition into a `hydrate::Value`
//! model and compares it with `hydrate()` at the target view. Scenarios
//! form a matrix:
//!
//! - trigger: plain `diff(h1, h2)`, write-frontier mask/reveal,
//!   isolate/integrate, and a batch `merge` as the reference encoder
//! - container: map key vs list element
//! - direction: conflict *appears* (with a delta on the winner) vs
//!   conflict *clears* (the put must carry the final total)
//!
//! One further case holds the delta on a *losing* candidate instead of the
//! winner, to isolate "delta and conflict change on the same winner" as the
//! condition under test.

use automerge::{
    hydrate, transaction::Transactable, ActorId, Author, AutoCommit, ChangeHash, ObjType, Patch,
    ReadDoc, ScalarValue, TextEncoding, ROOT,
};

const ENCODING: TextEncoding = TextEncoding::UnicodeCodePoint;

/// Which container the conflicted counter lives in.
#[derive(Clone, Copy, Debug)]
enum Container {
    Map,
    List,
}

/// The shared topology.
///
/// - `main` (carol) creates the container in an init commit; alice and bob
///   fork from it.
/// - bob puts `counter(0)` at the slot.
/// - alice puts an unrelated value first (so her counter put has the higher
///   opid and wins the conflict), then `counter(0)` at the same slot.
/// - bob merges alice (now sees both counters), then increments by -1. His
///   increment's pred is BOTH counter puts.
/// - `main` merges alice only → heads `h_alice` (alice's counter alone).
/// - `main` merges bob → heads `h_all` (both counters + increment).
struct Scenario {
    main: AutoCommit,
    bob: Author<'static>,
    /// Heads after the init commit; masking bob here hides all his ops.
    h_init: Vec<ChangeHash>,
    /// Heads with alice's counter alone (no conflict, no increment).
    h_alice: Vec<ChangeHash>,
    /// Heads with both counters and bob's increment.
    h_all: Vec<ChangeHash>,
}

fn scenario(container: Container) -> Scenario {
    let alice = Author::try_from("aaaa").unwrap();
    let bob = Author::try_from("bbbb").unwrap();
    let mut main = AutoCommit::new_with_encoding(ENCODING).with_actor(ActorId::from(vec![0x01]));
    let obj = match container {
        Container::Map => {
            main.put(ROOT, "init", 0).unwrap();
            ROOT
        }
        Container::List => {
            let list = main.put_object(ROOT, "list", ObjType::List).unwrap();
            // A seed element so both peers can `put` at index 0 concurrently.
            main.insert(&list, 0, 0).unwrap();
            list
        }
    };
    main.commit();
    let h_init = main.get_heads();
    let mut a = main
        .fork()
        .with_author(Some(alice))
        .with_actor(ActorId::from(vec![0xA0]));
    let mut b = main
        .fork()
        .with_author(Some(bob.clone()))
        .with_actor(ActorId::from(vec![0xB0]));

    let put_counter = |doc: &mut AutoCommit| match container {
        Container::Map => doc.put(&obj, "k1", ScalarValue::counter(0)).unwrap(),
        Container::List => doc.put(&obj, 0, ScalarValue::counter(0)).unwrap(),
    };
    put_counter(&mut b);
    b.commit();
    // Bump alice's opid past bob's so her counter wins.
    a.put(ROOT, "k0", 0).unwrap();
    a.commit();
    put_counter(&mut a);
    a.commit();
    b.merge(&mut a).unwrap();
    match container {
        Container::Map => b.increment(&obj, "k1", -1).unwrap(),
        Container::List => b.increment(&obj, 0, -1).unwrap(),
    }
    b.commit();

    main.merge(&mut a).unwrap();
    let h_alice = main.get_heads();
    main.merge(&mut b).unwrap();
    let h_all = main.get_heads();
    Scenario {
        main,
        bob,
        h_init,
        h_alice,
        h_all,
    }
}

fn replay(mut model: hydrate::Value, patches: &[Patch]) -> hydrate::Value {
    model
        .apply_patches(ENCODING, patches.to_vec())
        .unwrap_or_else(|e| panic!("model rejected patches: {e:?}\n{patches:#?}"));
    model
}

/// alice alone → alice + bob(losing) + increment. Unchanged winner, delta -1,
/// conflict appears.
fn plain_diff_conflict_appears(container: Container) {
    let mut s = scenario(container);
    let before = s.main.hydrate(ROOT, Some(&s.h_alice)).unwrap();
    let after = s.main.hydrate(ROOT, Some(&s.h_all)).unwrap();
    let patches = s.main.diff(&s.h_alice, &s.h_all);
    let model = replay(before, &patches);
    assert_eq!(model, after, "{container:?}: patches: {patches:#?}");
}

#[test]
fn plain_diff_map_conflict_appears() {
    plain_diff_conflict_appears(Container::Map);
}

#[test]
fn plain_diff_list_conflict_appears() {
    plain_diff_conflict_appears(Container::List);
}

/// The reverse direction. alice + bob + increment → alice alone. The
/// conflict clears and the delta is +1; the transition must be a single
/// put carrying the final total, not an increment.
fn plain_diff_conflict_clears(container: Container) {
    let mut s = scenario(container);
    let before = s.main.hydrate(ROOT, Some(&s.h_all)).unwrap();
    let after = s.main.hydrate(ROOT, Some(&s.h_alice)).unwrap();
    let patches = s.main.diff(&s.h_all, &s.h_alice);
    let model = replay(before, &patches);
    assert_eq!(model, after, "{container:?}: patches: {patches:#?}");
}

#[test]
fn plain_diff_map_conflict_clears() {
    plain_diff_conflict_clears(Container::Map);
}

#[test]
fn plain_diff_list_conflict_clears() {
    plain_diff_conflict_clears(Container::List);
}

fn reveal_conflict_appears(container: Container) {
    let mut s = scenario(container);
    s.main.mask_author(s.bob.clone(), &s.h_init);
    let before = s.main.hydrate(ROOT, None).unwrap();
    s.main.update_diff_cursor();
    s.main.reveal_author(&s.bob);
    let patches = s.main.diff_incremental();
    let after = s.main.hydrate(ROOT, None).unwrap();
    let model = replay(before, &patches);
    assert_eq!(model, after, "{container:?}: patches: {patches:#?}");
}

#[test]
fn reveal_map_conflict_appears() {
    reveal_conflict_appears(Container::Map);
}

#[test]
fn reveal_list_conflict_appears() {
    reveal_conflict_appears(Container::List);
}

/// Masking bob clears the conflict; the delta is +1 back to 0.
fn mask_conflict_clears(container: Container) {
    let mut s = scenario(container);
    let before = s.main.hydrate(ROOT, None).unwrap();
    s.main.update_diff_cursor();
    s.main.mask_author(s.bob.clone(), &s.h_init);
    let patches = s.main.diff_incremental();
    let after = s.main.hydrate(ROOT, None).unwrap();
    let model = replay(before, &patches);
    assert_eq!(model, after, "{container:?}: patches: {patches:#?}");
}

#[test]
fn mask_map_conflict_clears() {
    mask_conflict_clears(Container::Map);
}

#[test]
fn mask_list_conflict_clears() {
    mask_conflict_clears(Container::List);
}

#[test]
fn plain_diff_map_conflict_appears_without_delta_on_winner() {
    let alice = Author::try_from("aaaa").unwrap();
    let bob = Author::try_from("bbbb").unwrap();
    let mut main = AutoCommit::new_with_encoding(ENCODING).with_actor(ActorId::from(vec![0x01]));
    main.put(ROOT, "init", 0).unwrap();
    main.commit();
    let mut a = main
        .fork()
        .with_author(Some(alice))
        .with_actor(ActorId::from(vec![0xA0]));
    let mut b = main
        .fork()
        .with_author(Some(bob))
        .with_actor(ActorId::from(vec![0xB0]));
    b.put(ROOT, "k1", ScalarValue::counter(0)).unwrap();
    b.commit();
    b.increment(ROOT, "k1", -1).unwrap(); // pred: bob's own put only
    b.commit();
    a.put(ROOT, "k0", 0).unwrap();
    a.commit();
    a.put(ROOT, "k1", ScalarValue::counter(0)).unwrap();
    a.commit();
    main.merge(&mut a).unwrap();
    let h_alice = main.get_heads();
    main.merge(&mut b).unwrap();
    let h_all = main.get_heads();
    let before = main.hydrate(ROOT, Some(&h_alice)).unwrap();
    let after = main.hydrate(ROOT, Some(&h_all)).unwrap();
    let patches = main.diff(&h_alice, &h_all);
    let model = replay(before, &patches);
    assert_eq!(model, after, "patches: {patches:#?}");
}

fn integrate_conflict_appears(container: Container) {
    let mut s = scenario(container);
    s.main.isolate(&s.h_alice);
    s.main.update_diff_cursor();
    let before = s.main.hydrate(ROOT, Some(&s.h_alice)).unwrap();
    s.main.integrate();
    let patches = s.main.diff_incremental();
    let after = s.main.hydrate(ROOT, None).unwrap();
    let model = replay(before, &patches);
    assert_eq!(model, after, "{container:?}: patches: {patches:#?}");
}

#[test]
fn integrate_map_conflict_appears() {
    integrate_conflict_appears(Container::Map);
}

#[test]
fn integrate_list_conflict_appears() {
    integrate_conflict_appears(Container::List);
}

fn batch_merge_conflict_appears(container: Container) {
    // Rebuild the topology by hand: main merges alice, drains, then merges
    // bob as one batch.
    let alice = Author::try_from("aaaa").unwrap();
    let bob = Author::try_from("bbbb").unwrap();
    let mut main = AutoCommit::new_with_encoding(ENCODING).with_actor(ActorId::from(vec![0x01]));
    let obj = match container {
        Container::Map => {
            main.put(ROOT, "init", 0).unwrap();
            ROOT
        }
        Container::List => {
            let list = main.put_object(ROOT, "list", ObjType::List).unwrap();
            main.insert(&list, 0, 0).unwrap();
            list
        }
    };
    main.commit();
    let mut a = main
        .fork()
        .with_author(Some(alice))
        .with_actor(ActorId::from(vec![0xA0]));
    let mut b = main
        .fork()
        .with_author(Some(bob))
        .with_actor(ActorId::from(vec![0xB0]));
    let put_counter = |doc: &mut AutoCommit| match container {
        Container::Map => doc.put(&obj, "k1", ScalarValue::counter(0)).unwrap(),
        Container::List => doc.put(&obj, 0, ScalarValue::counter(0)).unwrap(),
    };
    put_counter(&mut b);
    b.commit();
    a.put(ROOT, "k0", 0).unwrap();
    a.commit();
    put_counter(&mut a);
    a.commit();
    b.merge(&mut a).unwrap();
    match container {
        Container::Map => b.increment(&obj, "k1", -1).unwrap(),
        Container::List => b.increment(&obj, 0, -1).unwrap(),
    }
    b.commit();

    main.merge(&mut a).unwrap();
    let before = main.hydrate(ROOT, None).unwrap();
    main.update_diff_cursor();
    main.merge(&mut b).unwrap();
    let patches = main.diff_incremental();
    let after = main.hydrate(ROOT, None).unwrap();
    let model = replay(before, &patches);
    assert_eq!(model, after, "{container:?}: patches: {patches:#?}");
}

#[test]
fn batch_merge_map_conflict_appears() {
    batch_merge_conflict_appears(Container::Map);
}

#[test]
fn batch_merge_list_conflict_appears() {
    batch_merge_conflict_appears(Container::List);
}
