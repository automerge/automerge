//! Local candidate assembly owns its result and does not perform acceptance.

use std::collections::HashSet;

use crate::admission::CandidateHistory;
use crate::{Automerge, Change, Fragment, ReadDoc};

use super::{append_at_level, fixture, retain_signature};

fn assert_contents(contents: &[(Fragment, Vec<Change>)], expected: &[(Fragment, Vec<Change>)]) {
    assert_eq!(
        contents, expected,
        "preserve descriptor, content and group order"
    );
    for (fragment, members) in contents {
        assert_eq!(members.len(), fragment.members.len());
        assert_eq!(
            members.iter().map(Change::hash).collect::<HashSet<_>>(),
            fragment.members.iter().copied().collect()
        );
    }
}

#[test]
fn empty_local_contents_are_empty_without_mutating_the_document() {
    let mut document = Automerge::signing();
    let before = document.save_nocompress();
    let state = format!("{:?}", document.signing);
    let contents = document
        .try_signing()
        .unwrap()
        .local_candidate_contents()
        .unwrap();
    assert!(contents.is_empty());
    assert_eq!(document.save_nocompress(), before);
    assert_eq!(format!("{:?}", document.signing), state);
}

#[test]
fn fully_signed_local_contents_remain_unaccepted_until_explicit_completion() {
    let (mut document, changes, key) = fixture(&[0, 0, 1, 0]);
    // Outgoing discovery is evidence-gated; assembly uses the structural cover.
    assert!(document.fragments(..).is_empty());
    let expected: Vec<_> = document
        .change_graph
        .fragments(..)
        .map(|fragment| {
            let members = fragment
                .members
                .iter()
                .map(|hash| document.get_change_by_hash(hash).unwrap())
                .collect();
            (fragment, members)
        })
        .collect();
    let before = document.save_nocompress();
    let mut signing = document.try_signing().unwrap();
    retain_signature(&mut signing, &changes[2], &key);
    retain_signature(&mut signing, &changes[3], &key);
    let accepted_before = format!("{:?}", signing.state.accepted);
    let signatures_before = format!("{:?}", signing.state.signatures);

    let contents = signing.local_candidate_contents().unwrap();
    assert_contents(&contents, &expected);
    assert_contents(&signing.local_candidate_contents().unwrap(), &expected);
    assert_eq!(format!("{:?}", signing.state.accepted), accepted_before);
    assert_eq!(format!("{:?}", signing.state.signatures), signatures_before);
    assert!(changes
        .iter()
        .all(|change| !signing.is_accepted(&change.hash())));

    // The returned data does not borrow the signing handle: it can be borrowed
    // as candidates while that same handle completes their acceptance.
    let candidates: Vec<_> = contents
        .iter()
        .map(|(fragment, members)| {
            CandidateHistory::from_fragment(fragment, members.iter().collect())
        })
        .collect();
    signing.accept_applied(&candidates).unwrap();
    assert!(changes
        .iter()
        .all(|change| signing.is_accepted(&change.hash())));
    assert!(signing.local_candidate_contents().unwrap().is_empty());
    assert_eq!(format!("{:?}", signing.state.signatures), signatures_before);
    assert_eq!(document.save_nocompress(), before);
}

#[test]
fn local_contents_skip_accepted_groups_but_keep_partly_accepted_groups_whole() {
    let (mut document, changes, key) = fixture(&[1, 0]);
    {
        let mut signing = document.try_signing().unwrap();
        for change in &changes {
            retain_signature(&mut signing, change, &key);
        }
        signing.accept_local().unwrap();
        assert!(changes
            .iter()
            .all(|change| signing.is_accepted(&change.hash())));
    }
    let middle = append_at_level(&mut document, 3, 0);
    let tip = append_at_level(&mut document, 4, 1);
    let suffix = append_at_level(&mut document, 5, 0);
    assert!(document.get_fragment(tip).is_none());
    assert!(document.get_fragment(suffix).is_none());
    let fragment = document.change_graph.get_fragment(tip).unwrap();
    let loose = document.change_graph.get_fragment(suffix).unwrap();
    assert_eq!(fragment.boundary, [changes[0].hash()]);
    assert!(fragment.members.contains(&changes[1].hash()));
    let expected: Vec<_> = [loose, fragment]
        .into_iter()
        .map(|fragment| {
            let members = fragment
                .members
                .iter()
                .map(|hash| document.get_change_by_hash(hash).unwrap())
                .collect();
            (fragment, members)
        })
        .collect();
    let before = document.save_nocompress();
    let state = format!("{:?}", document.signing);
    {
        let signing = document.try_signing().unwrap();
        let contents = signing.local_candidate_contents().unwrap();
        assert_contents(&contents, &expected);
        assert!(contents
            .iter()
            .all(|(fragment, _)| fragment.head != changes[0].hash()));
        assert!(signing.is_accepted(&changes[1].hash()));
        for hash in [middle, tip, suffix] {
            assert!(!signing.is_accepted(&hash));
        }
    }
    assert_eq!(document.save_nocompress(), before);
    assert_eq!(format!("{:?}", document.signing), state);
}
