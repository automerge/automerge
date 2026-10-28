//! Exercise the witness-only store interface used by signature receipt.

use std::collections::BTreeMap;

use proptest::prelude::*;

use super::*;

#[test]
fn retention_keeps_evidence_for_selected_hashes_and_removes_whole_records() {
    let (changes, key) = fixture([7; 32]);
    let verifier = verifying_author(&key);
    let mut store = SignatureStore::default();
    for change in &changes {
        let signature = sign(&key, change);
        store
            .insert_ed25519(verifier.verify_hash(&change.hash(), &signature).unwrap())
            .unwrap();
    }
    let kept = changes[1].hash();
    let before = evidence_entries(&store, &kept);
    store.retain_hashes(|hash| *hash == kept);
    assert_eq!(store.verified_hashes().copied().collect::<Vec<_>>(), [kept]);
    assert_eq!(evidence_entries(&store, &kept), before);
    assert!(evidence_entries(&store, &changes[0].hash()).is_empty());
    store.retain_hashes(|_| false);
    assert!(store.verified_hashes().next().is_none());
    assert!(evidence_entries(&store, &kept).is_empty());
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(32))]

    #[test]
    fn insertion_and_retention_match_an_author_bound_map(
        operations in prop::collection::vec((0u8..3, 0u8..4, any::<bool>()), 0..24)
    ) {
        let keys = [SigningKey::from_bytes(&[7; 32]), SigningKey::from_bytes(&[8; 32])];
        let authors = keys.each_ref().map(|key| Author::from(key.verifying_key().to_bytes().to_vec()));
        let signatures: Vec<_> = keys.iter().map(|key| (0u8..4).map(|hash| {
            ed25519::Signature::from_bytes(key.sign(&signing_message(&ChangeHash([hash; 32]))).to_bytes())
        }).collect::<Vec<_>>()).collect();
        let mut model = BTreeMap::<u8, usize>::new();
        let mut store = SignatureStore::default();
        for (operation, hash, second_author) in operations {
            let author = usize::from(second_author);
            let change = ChangeHash([hash; 32]);
            if operation == 2 {
                model.retain(|h, _| *h != hash);
                store.retain_hashes(|h| *h != change);
            } else {
                let signature = signatures[author][hash as usize];
                let witness = verifying_author(&keys[author]).verify_hash(&change, &signature).unwrap();
                let conflict = model.get(&hash).is_some_and(|old| *old != author);
                let duplicate = model.contains_key(&hash);
                let result = store.insert_ed25519(witness);
                if conflict {
                    prop_assert!(matches!(result, Err(InsertError::AuthorConflict { change: h }) if h == change), "conflicting author must reject the exact hash");
                } else {
                    prop_assert_eq!(result.unwrap(), if duplicate { Insert::Duplicate } else { Insert::Stored });
                    model.insert(hash, author);
                }
            }
            prop_assert_eq!(store.verified_hashes().copied().collect::<Vec<_>>(), model.keys().map(|hash| ChangeHash([*hash; 32])).collect::<Vec<_>>());
            for hash in 0u8..4 {
                let expected = model.get(&hash).map(|author| vec![(authors[*author].clone(), signatures[*author][hash as usize])]).unwrap_or_default();
                prop_assert_eq!(evidence_entries(&store, &ChangeHash([hash; 32])), expected);
            }
        }
    }
}
