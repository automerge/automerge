//! Candidate views share member bytes even when owners are not contiguous.

use crate::admission::{select, CandidateHistory};

use super::{append_at_level, overlapping_fragments, retain_signature, AcceptedHistoryArray};
use crate::{ActorId, AutoCommit, Change};

#[test]
fn overlapping_candidates_borrow_separate_owners_and_preserve_selection() {
    let mut fixture = overlapping_fragments();
    // Box the unique changes separately, as applied reconstruction and staged
    // storage need not be adjacent. Both candidates borrow the exact same a.
    let mut members = fixture.g_changes.into_iter().map(Box::new);
    let a = members.next().unwrap();
    let x = members.next().unwrap();
    let c = members.next().unwrap();
    assert!(members.next().is_none());
    let f = Box::new(fixture.f_changes.pop().unwrap());
    let candidates = [
        CandidateHistory::CanonicalFragment {
            fragment: &fixture.f,
            members: vec![f.as_ref(), a.as_ref()],
        },
        CandidateHistory::from_fragment(&fixture.g, vec![c.as_ref(), a.as_ref(), x.as_ref()]),
    ];
    let f_members: Vec<_> = candidates[0].changes().collect();
    let g_members: Vec<_> = candidates[1].changes().collect();
    for (actual, expected) in f_members.iter().zip([f.as_ref(), a.as_ref()]) {
        assert!(std::ptr::eq(*actual, expected));
    }
    for (actual, expected) in g_members.iter().zip([c.as_ref(), a.as_ref(), x.as_ref()]) {
        assert!(std::ptr::eq(*actual, expected));
    }
    assert_eq!(f_members.len(), 2);
    assert_eq!(g_members.len(), 3);
    assert!(std::ptr::eq(f_members[1], g_members[1]));
    assert_eq!(
        candidates[1]
            .changes()
            .map(Change::hash)
            .collect::<Vec<_>>(),
        [c.hash(), a.hash(), x.hash()],
        "repeated iteration preserves supplied member order"
    );

    let accepted = AcceptedHistoryArray::default();
    // Existing f/c evidence cannot make G pass without x. Once G is excluded,
    // c is unavailable to attest a, so F cannot pass either.
    let selection = select(&accepted, &candidates, &fixture.signatures);
    assert!(selection.selected().is_empty());
    assert_eq!(selection.pending().len(), 2);

    retain_signature(&mut fixture.signatures, &a, &fixture.a_key);
    let selection = select(&accepted, &candidates, &fixture.signatures);
    assert_eq!(selection.selected().len(), 1);
    assert!(std::ptr::eq(selection.selected()[0], &candidates[0]));
    assert_eq!(selection.pending().len(), 1);
    assert!(std::ptr::eq(selection.pending()[0], &candidates[1]));

    retain_signature(&mut fixture.signatures, &x, &fixture.b_key);
    let selection = select(&accepted, &candidates, &fixture.signatures);
    assert_eq!(selection.selected().len(), 2);
    assert!(selection.pending().is_empty());
    assert!(std::ptr::eq(
        candidates[0].changes().nth(1).unwrap(),
        a.as_ref()
    ));
    assert!(std::ptr::eq(
        candidates[1].changes().nth(1).unwrap(),
        a.as_ref()
    ));
}

#[test]
fn reference_adapter_preserves_loose_and_singleton_fragment_classification() {
    for level in [0, 1] {
        let mut doc = AutoCommit::new().with_actor(ActorId::from([1]));
        let head = append_at_level(&mut doc, 1, level);
        let change = doc.get_changes(&[]).pop().unwrap();
        let descriptor = doc.get_fragment(head).unwrap();
        let candidate = CandidateHistory::from_fragment(&descriptor, vec![&change]);
        match (&candidate, level) {
            (CandidateHistory::Loose(member), 0) => assert!(std::ptr::eq(*member, &change)),
            (CandidateHistory::CanonicalFragment { fragment, members }, 1) => {
                assert!(std::ptr::eq(*fragment, &descriptor));
                assert_eq!(members.len(), 1);
                assert!(std::ptr::eq(members[0], &change));
            }
            _ => panic!("classification must come from the descriptor, not member count"),
        }
        let mut changes = candidate.changes();
        assert!(std::ptr::eq(changes.next().unwrap(), &change));
        assert!(changes.next().is_none());
    }
}
