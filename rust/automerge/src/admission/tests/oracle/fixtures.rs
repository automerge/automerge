use std::collections::BTreeMap;

use ed25519_dalek::SigningKey;

use crate::admission::CandidateHistory;
use crate::{ActorId, Author, AutoCommit, Change, Fragment};

use super::super::append_at_level;

pub(super) struct Fixture {
    pub(super) changes: Vec<Change>,
    pub(super) keys: BTreeMap<ActorId, SigningKey>,
    groups: Vec<Group>,
    pub(super) accepted_len: usize,
    pub(super) required_signed: usize,
}

enum Group {
    Loose(Box<Change>),
    Fragment {
        descriptor: Fragment,
        members: Vec<Change>,
    },
}

impl Fixture {
    pub(super) fn candidates(&self) -> Vec<CandidateHistory<'_>> {
        self.groups
            .iter()
            .map(|group| match group {
                Group::Loose(change) => CandidateHistory::Loose(change),
                Group::Fragment {
                    descriptor,
                    members,
                } => CandidateHistory::CanonicalFragment {
                    fragment: descriptor,
                    members,
                },
            })
            .collect()
    }
}

fn author(key: &SigningKey) -> Author<'static> {
    Author::from(key.verifying_key().to_bytes().to_vec())
}

fn fragment(descriptor: Fragment, changes: &[Change]) -> Group {
    let members = descriptor
        .members
        .iter()
        .map(|hash| {
            changes
                .iter()
                .find(|change| change.hash() == *hash)
                .unwrap()
                .clone()
        })
        .collect();
    Group::Fragment {
        descriptor,
        members,
    }
}

fn linear(levels: [usize; 3]) -> (AutoCommit, Vec<Change>, BTreeMap<ActorId, SigningKey>) {
    let key = SigningKey::from_bytes(&[7; 32]);
    let actor = ActorId::from([1]);
    let mut doc = AutoCommit::new()
        .with_author(Some(author(&key)))
        .with_actor(actor.clone());
    for (index, level) in levels.into_iter().enumerate() {
        append_at_level(&mut doc, index as i64 + 1, level);
    }
    let changes = doc.get_changes(&[]);
    let keys = BTreeMap::from([(actor, key)]);
    (doc, changes, keys)
}

pub(super) fn loose_chain() -> Fixture {
    let (_, changes, keys) = linear([0, 0, 0]);
    let groups = changes
        .iter()
        .cloned()
        .map(|change| Group::Loose(Box::new(change)))
        .collect();
    Fixture {
        changes,
        keys,
        groups,
        accepted_len: 0,
        required_signed: 0,
    }
}

pub(super) fn internal_chain() -> Fixture {
    let (doc, changes, keys) = linear([0, 0, 1]);
    let descriptor = doc.get_fragment(changes[2].hash()).unwrap();
    assert_eq!(descriptor.members.len(), 3);
    assert!(descriptor.boundary.is_empty());
    let groups = vec![fragment(descriptor, &changes)];
    Fixture {
        changes,
        keys,
        groups,
        accepted_len: 0,
        required_signed: 0,
    }
}

pub(super) fn accepted_prefix() -> Fixture {
    let (doc, changes, keys) = linear([0, 1, 0]);
    let prefix = doc.get_fragment(changes[1].hash()).unwrap();
    assert_eq!(prefix.members.len(), 2);
    assert!(prefix.boundary.is_empty());
    // The prefix's frontier proof is mandatory in every evidence variation.
    // No production selector is used to certify this starting history.
    let groups = vec![
        Group::Loose(Box::new(changes[0].clone())),
        Group::Loose(Box::new(changes[2].clone())),
    ];
    Fixture {
        changes,
        keys,
        groups,
        accepted_len: 2,
        required_signed: 1 << 1,
    }
}

pub(super) fn overlapping() -> Fixture {
    let existing = super::super::overlapping_fragments();
    // Unique history: a, x, c, f. A and A' share a key, not an actor identity.
    let mut changes = existing.g_changes;
    changes.push(existing.f_changes[1].clone());
    let keys = BTreeMap::from([
        (changes[0].actor_id().clone(), existing.a_key.clone()),
        (changes[1].actor_id().clone(), existing.b_key),
        (changes[3].actor_id().clone(), existing.a_key),
    ]);
    let groups = vec![
        fragment(existing.f, &changes),
        fragment(existing.g, &changes),
    ];
    Fixture {
        changes,
        keys,
        groups,
        accepted_len: 0,
        required_signed: 0,
    }
}

pub(super) fn mutual_support() -> Fixture {
    let a_key = SigningKey::from_bytes(&[7; 32]);
    let b_key = SigningKey::from_bytes(&[8; 32]);
    let a_actor = ActorId::from([1]);
    let b_actor = ActorId::from([2]);
    let mut doc = AutoCommit::new()
        .with_author(Some(author(&a_key)))
        .with_actor(a_actor.clone());
    let a = append_at_level(&mut doc, 1, 0);
    doc.set_author(Some(author(&b_key)));
    doc.set_actor(b_actor.clone());
    let b = append_at_level(&mut doc, 2, 1);
    let f = doc.get_fragment(b).unwrap();
    doc.set_author(Some(author(&a_key)));
    doc.set_actor(a_actor.clone());
    let c = append_at_level(&mut doc, 3, 1);
    let g = doc.get_fragment(c).unwrap();
    let changes = doc.get_changes(&[]);
    assert_eq!(
        changes.iter().map(Change::hash).collect::<Vec<_>>(),
        [a, b, c]
    );
    assert_eq!(f.members.len(), 2);
    assert!(f.members.contains(&a) && f.members.contains(&b));
    assert!(f.boundary.is_empty());
    assert_eq!(g.boundary, [b]);
    assert_eq!(g.members, [c]);
    let keys = BTreeMap::from([(a_actor, a_key), (b_actor, b_key)]);
    let groups = vec![fragment(f, &changes), fragment(g, &changes)];
    Fixture {
        changes,
        keys,
        groups,
        accepted_len: 0,
        required_signed: 0,
    }
}
