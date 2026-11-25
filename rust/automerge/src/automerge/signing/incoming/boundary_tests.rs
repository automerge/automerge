use super::{BoundaryPastMemo, Incoming};
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
            let incoming = Incoming {
                graph: signing.graph,
                ops: signing.ops,
                authors: signing.authors,
                state: signing.state,
            };
            let cold =
                incoming.boundary_past_for(head, &candidates, &mut BoundaryPastMemo::default());
            let mut memo = BoundaryPastMemo::default();
            let prefix = incoming.boundary_past_for(middle, &candidates, &mut memo);
            assert_eq!(prefix.maxima.get(middle_change.actor_id()), Some(&2));
            assert_eq!(prefix.seeds.iter().copied().collect::<Vec<_>>(), [applied]);
            let warm = incoming.boundary_past_for(head, &candidates, &mut memo);
            assert_eq!(warm.maxima.get(head_change.actor_id()), Some(&3));
            assert_eq!(warm.maxima, cold.maxima);
            assert_eq!(warm.seeds, cold.seeds);
            let hit = incoming.boundary_past_for(head, &candidates, &mut memo);
            assert_eq!(hit.maxima, cold.maxima);
            assert_eq!(hit.seeds, cold.seeds);
        })
        .unwrap();
}
