use super::{boundary_cutoff, boundary_summary::Summary, BoundaryPastMemo};
use crate::transaction::Transactable;
use crate::{Author, Automerge, ChangeHash, Config, ReadDoc, ROOT};
use ed25519_dalek::SigningKey;
use std::collections::HashMap;

fn append(doc: &mut Automerge, name: &str) -> ChangeHash {
    let mut tx = doc.transaction();
    tx.put(ROOT, name, true).unwrap();
    tx.commit().0.unwrap()
}

#[test]
fn warm_boundary_past_matches_a_cold_walk_on_both_sides_of_application() {
    let key = SigningKey::from_bytes(&[8; 32]);
    let author = Author::from(key.verifying_key().to_bytes().to_vec());
    let mut receiver = Automerge::from_config(Config::default().signing(author).unwrap());
    let applied = append(&mut receiver, "applied");
    let mut sender = receiver.clone();
    let middle = append(&mut sender, "middle");
    let head = append(&mut sender, "head");
    let middle_change = sender.get_change_by_hash(&middle).unwrap();
    let head_change = sender.get_change_by_hash(&head).unwrap();
    let candidates = HashMap::from([(middle, &middle_change), (head, &head_change)]);

    receiver
        .try_with_signing(|signing| {
            let cold = BoundaryPastMemo::new(signing.graph, &candidates).summary(&[head]);
            let mut memo = BoundaryPastMemo::new(signing.graph, &candidates);
            let prefix = memo.summary(&[middle]);
            assert_eq!(prefix.maxima.get(middle_change.actor_id()), Some(&2));
            assert_eq!(prefix.seeds.iter().copied().collect::<Vec<_>>(), [applied]);
            let warm = memo.summary(&[head]);
            assert_eq!(warm.maxima.get(head_change.actor_id()), Some(&3));
            assert_eq!(warm.maxima, cold.maxima);
            assert_eq!(warm.seeds, cold.seeds);
            let hit = memo.summary(&[head]);
            assert_eq!(hit.maxima, cold.maxima);
            assert_eq!(hit.seeds, cold.seeds);
            let union = memo.summary(&[middle, applied, head, middle]);
            assert_eq!(union.maxima, cold.maxima);
            assert_eq!(union.seeds, cold.seeds);
        })
        .unwrap();
}

#[test]
fn each_side_of_the_cut_uses_its_own_maximum() {
    let mut summary = Summary::<u8, u8>::new();
    summary.candidate(42, 9);
    let position = |hash| (hash == 1).then_some((0, 6));
    let clock = |_| Some(5);
    let candidate = |hash| match hash {
        2 => (&42, 9),
        3 => (&42, 10),
        _ => panic!("applied change queried as a candidate"),
    };
    // The candidate maximum of 9 must not cover the applied sequence 6.
    assert!(!boundary_cutoff::covers(
        &summary, 1, position, clock, candidate
    ));
    assert!(boundary_cutoff::covers(
        &summary, 2, position, clock, candidate
    ));
    assert!(!boundary_cutoff::covers(
        &summary,
        3,
        position,
        |_| Some(20),
        candidate
    ));
}
