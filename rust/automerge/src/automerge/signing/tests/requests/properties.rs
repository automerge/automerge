//! Independent semantic checks of request planning over small random
//! single-author histories (chains with occasional branches and merges).
//!
//! The oracle does not re-run the planner: it checks publication and
//! eligibility conditions directly on the document.

use proptest::prelude::*;

use super::*;

/// Build a fully local required-mode history from a shape script. Each step
/// either extends the newest head or branches from an older head, with a
/// merge every few steps so hard fragments cover several members.
fn build(steps: &[u8]) -> (Automerge, SigningKey, Vec<ChangeHash>) {
    let k = key(5);
    let mut doc = Automerge::from_config(crate::Config::default().signing(author(&k)).unwrap())
        .with_actor(ActorId::from([1]))
        .unwrap();
    let mut hashes: Vec<ChangeHash> = Vec::new();
    for (index, &step) in steps.iter().enumerate() {
        let level = usize::from(step % 3 == 0 && index > 0);
        let heads: Vec<ChangeHash> = if hashes.len() >= 2 && step % 2 == 1 {
            let older = hashes[hashes.len() - 2];
            vec![older]
        } else {
            doc.get_heads()
        };
        let value = i64::from(step) * 100 + index as i64 + 1;
        let hash = append_at_level(&mut doc, Some(&heads), value, level);
        hashes.push(hash);
    }
    (doc, k, hashes)
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(12))]

    #[test]
    fn planner_requests_only_unsigned_local_hashes_and_drains_local_history(
        steps in proptest::collection::vec(0u8..12, 1..6)
    ) {
        let (mut doc, k, hashes) = build(&steps);
        prop_assert!(doc.has_unpublished_changes());

        let mut rounds = 0;
        loop {
            let requests = doc.signing_requests().unwrap();
            let requested: Vec<_> = requests.iter().map(|r| r.hash()).collect();
            // Determinism and no mutation.
            prop_assert_eq!(&requested, &doc.signing_requests().unwrap().iter().map(|r| r.hash()).collect::<Vec<_>>());
            let mut sorted = requested.clone();
            sorted.sort_unstable();
            sorted.dedup();
            prop_assert_eq!(&requested, &sorted, "hash ordered and deduplicated");
            for request in &requests {
                prop_assert!(hashes.contains(&request.hash()), "only locally committed hashes");
                prop_assert!(!doc.try_signing().unwrap().is_published(&request.hash()));
                prop_assert_eq!(request.message(), signing_message(&request.hash()));
            }
            if requests.is_empty() {
                break;
            }
            sign_requests(&mut doc, &requests, &k);
            // Every requested hash became published in this batch.
            for hash in &requested {
                prop_assert!(doc.try_signing().unwrap().is_published(hash));
            }
            rounds += 1;
            prop_assert!(rounds <= hashes.len(), "no unneeded repeats");
        }
        // A fully local history drains completely.
        prop_assert!(!doc.has_unpublished_changes());
        prop_assert!(drafts(&mut doc).is_empty());
        for hash in &hashes {
            prop_assert!(doc.try_signing().unwrap().is_published(hash));
        }
    }
}
