use std::collections::BTreeMap;

use ed25519_dalek::{Signer, SigningKey};

use crate::signature::{ed25519, signing_message, SignatureStore};
use crate::transaction::{CommitOptions, Transactable};
use crate::{ActorId, Author, AutoCommit, Change, ChangeHash, Fragment, ROOT};

use super::{select, AcceptedHistory, CandidateHistory};

mod lifecycle;
mod oracle;

// Vary commit metadata rather than fabricating hashes. Levels 0, 0, 1 give
// this linear history a three-member fragment with no earlier boundary.
fn append_at_level(doc: &mut AutoCommit, value: i64, level: usize) -> ChangeHash {
    for time in 0..10_000 {
        let mut next = doc.clone();
        next.put(ROOT, "value", value).unwrap();
        let hash = next
            .commit_with(CommitOptions::default().with_time(time))
            .unwrap();
        if hash.fragment_level() == level {
            *doc = next;
            return hash;
        }
    }
    panic!("fixture search exhausted for fragment level {level}");
}

#[test]
fn signed_descendant_attests_other_members_of_the_same_fragment() {
    let key = SigningKey::from_bytes(&[7; 32]);
    let author = Author::from(key.verifying_key().to_bytes().to_vec());
    let actor = ActorId::from([1]);
    let mut doc = AutoCommit::new()
        .with_author(Some(author.clone()))
        .with_actor(actor.clone());
    let a1 = append_at_level(&mut doc, 1, 0);
    let a2 = append_at_level(&mut doc, 2, 0);
    let a3 = append_at_level(&mut doc, 3, 1);
    let changes = doc.get_changes(&[]);
    assert_eq!(
        changes.iter().map(|c| c.hash()).collect::<Vec<_>>(),
        [a1, a2, a3]
    );
    assert_eq!(changes[1].deps(), &[a1]);
    assert_eq!(changes[2].deps(), &[a2]);

    let fragment = doc.get_fragment(a3).unwrap();
    assert!(fragment.boundary.is_empty());
    assert_eq!(fragment.members.len(), 3);
    assert!([a1, a2, a3]
        .iter()
        .all(|hash| fragment.members.contains(hash)));

    let authors = BTreeMap::from([(actor, ed25519::VerifyingAuthor::new(author).unwrap())]);
    let verifier = ed25519::AuthorVerifier::new(&authors);
    let signature = ed25519::Signature::from_bytes(key.sign(&signing_message(&a3)).to_bytes());
    let mut signatures = SignatureStore::default();
    signatures
        .insert_ed25519(verifier.verify_change(&changes[2], &signature).unwrap())
        .unwrap();
    assert_eq!(signatures.ed25519_entries(&a1).count(), 0);
    assert_eq!(signatures.ed25519_entries(&a2).count(), 0);
    assert_eq!(signatures.ed25519_entries(&a3).count(), 1);

    let accepted = AcceptedHistory::default();
    let candidates = [CandidateHistory::CanonicalFragment {
        fragment: &fragment,
        members: &changes,
    }];

    let selection = select(&accepted, &candidates, &signatures);

    assert_eq!(selection.selected().len(), 1);
    assert!(std::ptr::eq(selection.selected()[0], &candidates[0]));
    assert!(selection.pending().is_empty());
}

#[test]
fn signed_loose_change_waits_when_its_dependency_cannot_be_admitted() {
    let key = SigningKey::from_bytes(&[7; 32]);
    let author = Author::from(key.verifying_key().to_bytes().to_vec());
    let actor = ActorId::from([1]);
    let mut doc = AutoCommit::new()
        .with_author(Some(author.clone()))
        .with_actor(actor.clone());
    let a1 = append_at_level(&mut doc, 1, 0);
    let a2 = append_at_level(&mut doc, 2, 0);
    let changes = doc.get_changes(&[]);
    assert_eq!(
        changes.iter().map(|c| c.hash()).collect::<Vec<_>>(),
        [a1, a2]
    );
    assert_eq!(changes[0].deps(), &[]);
    assert_eq!(changes[1].deps(), &[a1]);

    let authors = BTreeMap::from([(actor, ed25519::VerifyingAuthor::new(author).unwrap())]);
    let verifier = ed25519::AuthorVerifier::new(&authors);
    let signature = ed25519::Signature::from_bytes(key.sign(&signing_message(&a2)).to_bytes());
    let mut signatures = SignatureStore::default();
    signatures
        .insert_ed25519(verifier.verify_change(&changes[1], &signature).unwrap())
        .unwrap();
    // a2 has its own evidence, but its loose predecessor a1 does not.
    assert_eq!(signatures.ed25519_entries(&a1).count(), 0);
    assert_eq!(signatures.ed25519_entries(&a2).count(), 1);

    let accepted = AcceptedHistory::default();
    let candidates = [
        CandidateHistory::Loose(&changes[0]),
        CandidateHistory::Loose(&changes[1]),
    ];

    let selection = select(&accepted, &candidates, &signatures);

    assert!(selection.selected().is_empty());
    assert_eq!(selection.pending().len(), 2);
}

fn retain_signature(store: &mut SignatureStore, change: &Change, key: &SigningKey) {
    let author = Author::from(key.verifying_key().to_bytes().to_vec());
    let authors = BTreeMap::from([(
        change.actor_id().clone(),
        ed25519::VerifyingAuthor::new(author).unwrap(),
    )]);
    let verifier = ed25519::AuthorVerifier::new(&authors);
    let signature =
        ed25519::Signature::from_bytes(key.sign(&signing_message(&change.hash())).to_bytes());
    store
        .insert_ed25519(verifier.verify_change(change, &signature).unwrap())
        .unwrap();
}

#[test]
fn signature_on_earlier_member_does_not_attest_later_member() {
    let key = SigningKey::from_bytes(&[7; 32]);
    let author = Author::from(key.verifying_key().to_bytes().to_vec());
    let mut doc = AutoCommit::new()
        .with_author(Some(author))
        .with_actor(ActorId::from([1]));
    let a1 = append_at_level(&mut doc, 1, 0);
    let a2 = append_at_level(&mut doc, 2, 0);
    let a3 = append_at_level(&mut doc, 3, 1);
    let changes = doc.get_changes(&[]);
    assert_eq!(changes[1].hash(), a2);
    assert_eq!(changes[1].deps(), &[a1]);
    assert_eq!(changes[2].hash(), a3);
    assert_eq!(changes[2].deps(), &[a2]);
    let fragment = doc.get_fragment(a3).unwrap();
    assert!(fragment.boundary.is_empty());
    assert_eq!(fragment.members.len(), 3);
    assert!([a1, a2, a3]
        .iter()
        .all(|hash| fragment.members.contains(hash)));

    let mut signatures = SignatureStore::default();
    retain_signature(&mut signatures, &changes[1], &key);
    assert_eq!(signatures.ed25519_entries(&a2).count(), 1);
    assert_eq!(signatures.ed25519_entries(&a3).count(), 0);
    let accepted = AcceptedHistory::default();
    let candidates = [CandidateHistory::CanonicalFragment {
        fragment: &fragment,
        members: &changes,
    }];

    let selection = select(&accepted, &candidates, &signatures);

    assert!(selection.selected().is_empty());
    assert_eq!(selection.pending().len(), 1);
    assert!(std::ptr::eq(selection.pending()[0], &candidates[0]));

    // Control: adding evidence on the tip makes the same fragment admissible.
    retain_signature(&mut signatures, &changes[2], &key);
    let selection = select(&accepted, &candidates, &signatures);
    assert_eq!(selection.selected().len(), 1);
    assert!(std::ptr::eq(selection.selected()[0], &candidates[0]));
    assert!(selection.pending().is_empty());
}

// F = {a, f}, G = {a, x, c}. Both heads have level 1, and their shared
// root a has level 0. Actor A writes a,c. Actor B writes x. The branch
// actor A' writes f with A's author but cannot attest A's changes.
// Initially only c and f are signed, leaving G blocked on x.
struct OverlappingFragments {
    f: Fragment,
    f_changes: Vec<Change>,
    g: Fragment,
    g_changes: Vec<Change>,
    signatures: SignatureStore,
    a_key: SigningKey,
    b_key: SigningKey,
}

fn overlapping_fragments() -> OverlappingFragments {
    let a_key = SigningKey::from_bytes(&[7; 32]);
    let b_key = SigningKey::from_bytes(&[8; 32]);
    let a_author = Author::from(a_key.verifying_key().to_bytes().to_vec());
    let b_author = Author::from(b_key.verifying_key().to_bytes().to_vec());
    let a_actor = ActorId::from([1]);
    let mut original = AutoCommit::new()
        .with_author(Some(a_author.clone()))
        .with_actor(a_actor.clone());
    let a = append_at_level(&mut original, 1, 0);

    // Explicit actors keep fixture hashes deterministic despite fork_at's
    // randomly generated initial actor.
    let mut branch = original
        .fork_at(&[a])
        .unwrap()
        .with_author(Some(a_author.clone()))
        .with_actor(ActorId::from([3]));
    let f = append_at_level(&mut branch, 4, 1);
    let f_changes = branch.get_changes(&[]);
    let f_fragment = branch.get_fragment(f).unwrap();

    // Changing author generates a fresh actor. Override it afterward only
    // for this deterministic fixture, preserving the intended actor chains.
    original.set_author(Some(b_author));
    original.set_actor(ActorId::from([2]));
    let x = append_at_level(&mut original, 2, 0);
    original.set_author(Some(a_author));
    original.set_actor(a_actor);
    let c = append_at_level(&mut original, 3, 1);
    let g_changes = original.get_changes(&[]);
    let g_fragment = original.get_fragment(c).unwrap();

    assert_eq!(
        f_changes.iter().map(Change::hash).collect::<Vec<_>>(),
        [a, f]
    );
    assert_eq!(
        g_changes.iter().map(Change::hash).collect::<Vec<_>>(),
        [a, x, c]
    );
    assert!(f_fragment.boundary.is_empty());
    assert!(g_fragment.boundary.is_empty());
    assert_eq!(f_fragment.members.len(), 2);
    assert_eq!(g_fragment.members.len(), 3);
    assert!([a, f].iter().all(|hash| f_fragment.members.contains(hash)));
    assert!([a, x, c]
        .iter()
        .all(|hash| g_fragment.members.contains(hash)));
    assert_eq!(f_changes[1].deps(), &[a]);
    assert_eq!(g_changes[1].deps(), &[a]);
    assert_eq!(g_changes[0].actor_id(), g_changes[2].actor_id());
    assert_ne!(g_changes[0].actor_id(), f_changes[1].actor_id());
    assert!(g_changes[2].deps().contains(&a));
    assert!(g_changes[2].deps().contains(&x));

    let mut signatures = SignatureStore::default();
    retain_signature(&mut signatures, &f_changes[1], &a_key);
    retain_signature(&mut signatures, &g_changes[2], &a_key);
    assert_eq!(signatures.ed25519_entries(&a).count(), 0);
    assert_eq!(signatures.ed25519_entries(&x).count(), 0);

    OverlappingFragments {
        f: f_fragment,
        f_changes,
        g: g_fragment,
        g_changes,
        signatures,
        a_key,
        b_key,
    }
}

#[test]
fn shared_signature_remains_usable_when_another_fragment_stays_pending() {
    let mut fixture = overlapping_fragments();
    // Model sig(a) arriving with G. The store associates it with a, not G.
    retain_signature(
        &mut fixture.signatures,
        &fixture.g_changes[0],
        &fixture.a_key,
    );
    let a = fixture.f_changes[0].hash();
    assert_eq!(fixture.signatures.ed25519_entries(&a).count(), 1);
    let accepted = AcceptedHistory::default();

    for reverse in [false, true] {
        let mut candidates = [
            CandidateHistory::CanonicalFragment {
                fragment: &fixture.f,
                members: &fixture.f_changes,
            },
            CandidateHistory::CanonicalFragment {
                fragment: &fixture.g,
                members: &fixture.g_changes,
            },
        ];
        if reverse {
            candidates.reverse();
        }
        let f_index = usize::from(reverse);
        let selection = select(&accepted, &candidates, &fixture.signatures);

        assert_eq!(selection.selected().len(), 1);
        assert!(std::ptr::eq(selection.selected()[0], &candidates[f_index]));
        assert_eq!(selection.pending().len(), 1);
        assert!(std::ptr::eq(
            selection.pending()[0],
            &candidates[1 - f_index]
        ));
    }

    // G need not even be a candidate: sig(a) is reusable for F's copy of a.
    let candidates = [CandidateHistory::CanonicalFragment {
        fragment: &fixture.f,
        members: &fixture.f_changes,
    }];
    let selection = select(&accepted, &candidates, &fixture.signatures);
    assert_eq!(selection.selected().len(), 1);
    assert!(std::ptr::eq(selection.selected()[0], &candidates[0]));
    assert!(selection.pending().is_empty());
    assert_eq!(fixture.signatures.ed25519_entries(&a).count(), 1);
}

#[test]
fn excluded_signed_history_cannot_attest_another_fragment() {
    let mut fixture = overlapping_fragments();
    let accepted = AcceptedHistory::default();
    let candidates = [
        CandidateHistory::CanonicalFragment {
            fragment: &fixture.f,
            members: &fixture.f_changes,
        },
        CandidateHistory::CanonicalFragment {
            fragment: &fixture.g,
            members: &fixture.g_changes,
        },
    ];

    // In the first pass c attests a, making F appear admissible. G fails
    // because x has no evidence. After excluding G, c is no longer available,
    // so F must also be excluded even though sig(c) remains in the store.
    let selection = select(&accepted, &candidates, &fixture.signatures);
    assert!(selection.selected().is_empty());
    assert_eq!(selection.pending().len(), 2);
    for candidate in &candidates {
        assert!(selection
            .pending()
            .iter()
            .any(|pending| std::ptr::eq(*pending, candidate)));
    }
    assert_eq!(
        fixture.signatures.ed25519_entries(&fixture.g.head).count(),
        1
    );

    // Without G's history at all, its stored signature is still insufficient.
    let selection = select(&accepted, &candidates[..1], &fixture.signatures);
    assert!(selection.selected().is_empty());
    assert_eq!(selection.pending().len(), 1);

    // Control: signing x admits G and makes its signed descendant c available
    // alongside F. Both fragments can now be selected without sig(a).
    retain_signature(
        &mut fixture.signatures,
        &fixture.g_changes[1],
        &fixture.b_key,
    );
    let selection = select(&accepted, &candidates, &fixture.signatures);
    assert_eq!(selection.selected().len(), 2);
    assert!(selection.pending().is_empty());
    assert_eq!(
        fixture
            .signatures
            .ed25519_entries(&fixture.f_changes[0].hash())
            .count(),
        0
    );
}
