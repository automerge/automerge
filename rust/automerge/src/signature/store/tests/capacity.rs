//! Test the production collection insertion algorithm with synthetic bytes.
//! These are not verification witnesses. Store identity checks use real evidence
//! in the sibling tests; no test-side copy of the store's author check exists.

use std::collections::BTreeSet;

use proptest::prelude::*;

use crate::signature::ed25519;
use crate::signature::store::{Ed25519Evidence, Insert};

fn signature(byte: u8) -> ed25519::Signature {
    ed25519::Signature::from_bytes([byte; 64])
}

#[test]
fn full_collection_rejects_new_entries_but_accepts_duplicates_without_evicting_evidence() {
    let mut evidence = Ed25519Evidence::new(signature(0));
    for value in 1..16 {
        assert_eq!(evidence.insert(signature(value)), Insert::Stored);
    }
    let expected: Vec<_> = (0..16).map(signature).collect();
    assert_eq!(evidence.iter().copied().collect::<Vec<_>>(), expected);
    for value in 16..=u8::MAX {
        assert_eq!(evidence.insert(signature(value)), Insert::DroppedOverflow);
    }
    for value in 0..16 {
        assert_eq!(evidence.insert(signature(value)), Insert::Duplicate);
    }
    assert_eq!(evidence.iter().copied().collect::<Vec<_>>(), expected);
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(100))]

    #[test]
    fn bounded_collection_preserves_the_first_sixteen_distinct_signatures(
        initial in any::<u8>(),
        inputs in prop::collection::vec(any::<u8>(), 0..128),
    ) {
        let mut evidence = Ed25519Evidence::new(signature(initial));
        let mut order = vec![initial];
        let mut seen = BTreeSet::from([initial]);
        for value in inputs {
            // Independent model: the first 16 distinct inputs, not private fields.
            if seen.insert(value) { order.push(value); }
            let expected: Vec<_> = order.iter().take(16).copied().map(signature).collect();
            let before: Vec<_> = evidence.iter().copied().collect();
            let outcome = evidence.insert(signature(value));
            prop_assert_eq!(evidence.iter().copied().collect::<Vec<_>>(), expected);
            if before.contains(&signature(value)) {
                prop_assert_eq!(outcome, Insert::Duplicate);
            } else if before.len() == 16 {
                prop_assert_eq!(outcome, Insert::DroppedOverflow);
            } else {
                prop_assert_eq!(outcome, Insert::Stored);
            }
        }
    }
}
