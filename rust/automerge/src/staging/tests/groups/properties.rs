//! Group lifecycle against a small index/set model over real hashed histories.
//!
//! Fixtures are immutable and shared to avoid repeating fragment-level searches.
//! Generated cases vary delivery and operation schedules, not invented hashes.

use std::collections::{BTreeSet, HashSet};
use std::sync::OnceLock;

use proptest::prelude::*;

use crate::staging::StagedGroup;
use crate::{ActorId, Automerge, Change, ChangeHash, Fragment};

use super::{assert_contents, assert_unchanged, at_level, batch, history, stage, StagedChanges};

struct Fixture {
    changes: Vec<Change>,
    parents: Vec<Vec<usize>>,
    descriptors: Vec<Fragment>,
}

impl Fixture {
    fn check(&self) {
        // Pin the independently specified topology against the actual commits.
        for (index, change) in self.changes.iter().enumerate() {
            assert!(self.parents[index].iter().all(|parent| *parent < index));
            assert_eq!(
                change.deps().iter().copied().collect::<BTreeSet<_>>(),
                self.parents[index]
                    .iter()
                    .map(|parent| self.changes[*parent].hash())
                    .collect()
            );
        }
    }

    fn index(&self, hash: &ChangeHash) -> usize {
        self.changes
            .iter()
            .position(|change| change.hash() == *hash)
            .unwrap()
    }
}

fn chain() -> &'static Fixture {
    static FIXTURE: OnceLock<Fixture> = OnceLock::new();
    FIXTURE.get_or_init(|| {
        let (source, changes) = history(&[0, 0, 1, 0, 0, 0]);
        let descriptors = (2..changes.len())
            .map(|index| source.get_fragment(changes[index].hash()).unwrap())
            .collect();
        let fixture = Fixture {
            changes,
            parents: vec![vec![], vec![0], vec![1], vec![2], vec![3], vec![4]],
            descriptors,
        };
        fixture.check();
        assert_eq!(
            fixture.descriptors[0]
                .members
                .iter()
                .map(|hash| fixture.index(hash))
                .collect::<BTreeSet<_>>(),
            BTreeSet::from([0, 1, 2])
        );
        fixture
    })
}

fn overlap() -> &'static Fixture {
    static FIXTURE: OnceLock<Fixture> = OnceLock::new();
    FIXTURE.get_or_init(|| {
        // z -> a -> b -> c, with a -> d -> e on another actor, plus root x.
        // z is external to both fragments and can be withheld independently.
        let (base, prefix) = history(&[1, 0]);
        let mut left = base.clone().with_actor(ActorId::from([2]));
        let b = at_level(&mut left, 3, 0);
        let c = at_level(&mut left, 4, 1);
        let mut right = base.clone().with_actor(ActorId::from([3]));
        let d = at_level(&mut right, 5, 0);
        let e = at_level(&mut right, 6, 1);
        let mut independent = Automerge::new().with_actor(ActorId::from([4]));
        let x = at_level(&mut independent, 7, 0);
        let descriptors = vec![
            left.get_fragment(c.hash()).unwrap(),
            right.get_fragment(e.hash()).unwrap(),
            base.get_fragment(prefix[1].hash()).unwrap(),
            independent.get_fragment(x.hash()).unwrap(),
        ];
        let fixture = Fixture {
            changes: vec![prefix[0].clone(), prefix[1].clone(), b, c, d, e, x],
            parents: vec![vec![], vec![0], vec![1], vec![2], vec![1], vec![4], vec![]],
            descriptors,
        };
        fixture.check();
        for (fragment, members) in fixture.descriptors.iter().zip([
            BTreeSet::from([1, 2, 3]),
            BTreeSet::from([1, 4, 5]),
            BTreeSet::from([1]),
            BTreeSet::from([6]),
        ]) {
            assert_eq!(
                fragment
                    .members
                    .iter()
                    .map(|hash| fixture.index(hash))
                    .collect::<BTreeSet<_>>(),
                members
            );
        }
        fixture
    })
}

// The model stores recipe indices and currently known descriptors in delivery
// order. Discard removes knowledge, not a permanent tombstone: restoring bytes
// alone must not restore a descriptor. No expectation uses production group helpers.
struct Model<'a> {
    fixture: &'a Fixture,
    held: Vec<usize>,
    applied: BTreeSet<usize>,
    groups: Vec<(Fragment, BTreeSet<usize>)>,
}

impl Model<'_> {
    fn ready(&self) -> Vec<usize> {
        let mut available = vec![false; self.fixture.changes.len()];
        // One forward pass over the recipe, not the production Kahn work list.
        for index in 0..available.len() {
            available[index] = self.applied.contains(&index)
                || (self.held.contains(&index)
                    && self.fixture.parents[index]
                        .iter()
                        .all(|parent| available[*parent]));
        }
        (0..available.len())
            .filter(|index| available[*index] && self.held.contains(index))
            .collect()
    }

    fn check(&self, staged: &StagedChanges, applied: &Automerge) {
        let expected: Vec<_> = self
            .held
            .iter()
            .map(|index| &self.fixture.changes[*index])
            .collect();
        assert_contents(staged, &expected);
        for (index, change) in self.fixture.changes.iter().enumerate() {
            assert_eq!(
                applied.change_graph.has_change(&change.hash()),
                self.applied.contains(&index)
            );
        }
        assert_eq!(staged.groups().len(), self.groups.len());
        for (actual, (descriptor, members)) in staged.groups().iter().zip(&self.groups) {
            assert!(!members.is_subset(&self.applied));
            assert!(members
                .iter()
                .all(|index| self.applied.contains(index) || self.held.contains(index)));
            match actual {
                StagedGroup::Loose(hash) => {
                    assert_eq!(descriptor.level, 0);
                    assert_eq!(*hash, descriptor.head);
                }
                StagedGroup::Fragment(fragment) => {
                    assert!(descriptor.level > 0);
                    assert_eq!(fragment, descriptor, "preserve the entire first descriptor");
                }
            }
        }
        let prepared = staged.prepare(&applied.change_graph);
        let hashes: Vec<_> = prepared
            .changes()
            .iter()
            .map(|change| change.hash())
            .collect();
        let expected_ready: BTreeSet<_> = self
            .ready()
            .iter()
            .map(|i| self.fixture.changes[*i].hash())
            .collect();
        assert_eq!(
            hashes.iter().copied().collect::<BTreeSet<_>>(),
            expected_ready
        );
        assert_eq!(hashes.len(), expected_ready.len(), "no duplicate emission");
        let raw = stage(&expected);
        assert_eq!(
            hashes,
            raw.prepare(&applied.change_graph)
                .changes()
                .iter()
                .map(|change| change.hash())
                .collect::<Vec<_>>(),
            "group annotations must not alter FIFO order"
        );
        for (position, change) in prepared.changes().iter().enumerate() {
            for dep in change.deps() {
                assert!(
                    self.applied.contains(&self.fixture.index(dep))
                        || hashes[..position].contains(dep)
                );
            }
            let held = staged
                .iter()
                .find(|held| held.hash() == change.hash())
                .unwrap();
            assert!(std::ptr::eq(*change, held));
        }
    }

    fn complete(&mut self, count: usize, staged: &mut StagedChanges, applied: &mut Automerge) {
        // Choosing a prefix of the oracle's topological ready set yields a valid
        // partial application without using production preparation as the oracle.
        let completed: Vec<_> = self.ready().into_iter().take(count).collect();
        applied
            .apply_changes(
                completed
                    .iter()
                    .map(|i| self.fixture.changes[*i].clone())
                    .collect::<Vec<_>>(),
            )
            .unwrap();
        self.applied.extend(completed.iter().copied());
        self.held.retain(|index| !self.applied.contains(index));
        self.groups
            .retain(|(_, members)| !members.is_subset(&self.applied));
        let hashes: HashSet<_> = completed
            .iter()
            .map(|i| self.fixture.changes[*i].hash())
            .collect();
        for _ in 0..2 {
            staged.remove_completed(&hashes, &applied.change_graph);
            self.check(staged, applied);
        }
        staged.remove_completed(&HashSet::new(), &applied.change_graph);
        self.check(staged, applied);
    }

    fn discard(
        &mut self,
        actor: &ActorId,
        seq: u64,
        staged: &mut StagedChanges,
        applied: &Automerge,
    ) {
        let mut removed = BTreeSet::new();
        // A forward topological scan finds descendants. No reverse index, queue,
        // or removed-set inspection from StagedChanges is used by this oracle.
        for (index, change) in self.fixture.changes.iter().enumerate() {
            if self.held.contains(&index)
                && ((change.actor_id() == actor && change.seq() >= seq)
                    || self.fixture.parents[index]
                        .iter()
                        .any(|parent| removed.contains(parent)))
            {
                removed.insert(index);
            }
        }
        self.held.retain(|index| !removed.contains(index));
        self.groups
            .retain(|(_, members)| members.is_disjoint(&removed));
        for _ in 0..2 {
            staged.remove_actor_branch_from(actor, seq);
            self.check(staged, applied);
        }
    }

    fn redeliver(
        &mut self,
        group: usize,
        grouped: bool,
        reverse: bool,
        staged: &mut StagedChanges,
        applied: &Automerge,
    ) {
        self.check(staged, applied);
        let mut descriptor = self.fixture.descriptors[group].clone();
        let members: BTreeSet<_> = descriptor
            .members
            .iter()
            .map(|hash| self.fixture.index(hash))
            .collect();
        let mut order: Vec<_> = members.iter().copied().collect();
        if reverse {
            order.reverse();
            descriptor.members.reverse();
            descriptor.boundary.reverse();
            descriptor.checkpoints.reverse();
        }
        // Staging takes only new unapplied contents. Respect that importer
        // precondition using actual storage/graph membership, not oracle state.
        let incoming: Vec<_> = order
            .iter()
            .map(|index| &self.fixture.changes[*index])
            .filter(|change| {
                !applied.change_graph.has_change(&change.hash())
                    && !staged.hashes.contains(&change.hash())
            })
            .cloned()
            .collect();
        let pointers: Vec<_> = staged.iter().map(std::ptr::from_ref).collect();
        for index in order {
            if !self.applied.contains(&index) && !self.held.contains(&index) {
                self.held.push(index);
            }
        }
        if grouped
            && !members.is_subset(&self.applied)
            && !self
                .groups
                .iter()
                .any(|(known, _)| known.head == descriptor.head)
        {
            self.groups.push((descriptor.clone(), members));
        }
        if grouped {
            staged.extend_with_fragments(batch(&incoming), [descriptor], &applied.change_graph);
        } else {
            staged.extend(batch(&incoming));
        }
        if incoming.is_empty() {
            assert_eq!(
                staged.iter().map(std::ptr::from_ref).collect::<Vec<_>>(),
                pointers
            );
        }
        self.check(staged, applied);
    }
}

#[derive(Clone, Debug)]
struct Delivery {
    /// Sort key per change's recipe index; smaller keys are delivered first.
    contents: [u8; 7],
    /// Sort key per fixture descriptor; independent of content delivery order.
    groups: [u8; 4],
    /// Which fixture descriptors are supplied at setup (others remain unknown).
    known: [bool; 4],
    /// Number of initial contents staged without descriptors, clamped to length.
    raw_prefix: usize,
    /// Entries in the ordered descriptor delivery to repeat, modulo its length.
    repeats: Vec<usize>,
}

fn deliveries() -> impl Strategy<Value = Delivery> {
    (
        any::<[u8; 7]>(),
        any::<[u8; 4]>(),
        any::<[bool; 4]>(),
        0usize..8,
        prop::collection::vec(0usize..4, 0..9),
    )
        .prop_map(|(contents, groups, known, raw_prefix, repeats)| Delivery {
            contents,
            groups,
            known,
            raw_prefix,
            repeats,
        })
}

fn setup<'a>(
    fixture: &'a Fixture,
    length: usize,
    applied_len: usize,
    omit_boundary: bool,
    delivery: &Delivery,
) -> (Model<'a>, StagedChanges, Automerge) {
    let mut held: Vec<_> = (applied_len..length)
        .filter(|index| !omit_boundary || *index != 0)
        .collect();
    held.sort_by_key(|index| (delivery.contents[*index], *index));
    let mut order: Vec<_> = (0..fixture.descriptors.len())
        .filter(|index| {
            delivery.known[*index] && fixture.index(&fixture.descriptors[*index].head) < length
        })
        .collect();
    order.sort_by_key(|index| (delivery.groups[*index], *index));
    let mut descriptors: Vec<_> = order
        .into_iter()
        .map(|index| fixture.descriptors[index].clone())
        .collect();
    let applied_indices: BTreeSet<_> = (0..applied_len).collect();
    let groups = descriptors
        .iter()
        .map(|descriptor| {
            (
                descriptor.clone(),
                descriptor
                    .members
                    .iter()
                    .map(|hash| fixture.index(hash))
                    .collect(),
            )
        })
        .filter(|(_, members): &(_, BTreeSet<usize>)| !members.is_subset(&applied_indices))
        .collect();
    let model = Model {
        fixture,
        held,
        applied: applied_indices,
        groups,
    };
    let mut applied = Automerge::new();
    applied
        .apply_changes(fixture.changes[..applied_len].to_vec())
        .unwrap();
    let contents: Vec<_> = model
        .held
        .iter()
        .map(|i| fixture.changes[*i].clone())
        .collect();
    let split = delivery.raw_prefix.min(contents.len());
    let mut staged = StagedChanges::new();
    staged.extend(batch(&contents[..split]));
    let mut incoming = descriptors.clone();
    if !descriptors.is_empty() {
        incoming.extend(
            delivery
                .repeats
                .iter()
                .map(|index| descriptors[*index % descriptors.len()].clone()),
        );
    }
    staged.extend_with_fragments(batch(&contents[split..]), incoming, &applied.change_graph);
    model.check(&staged, &applied);
    let before = staged.clone();
    let pointers: Vec<_> = staged.iter().map(std::ptr::from_ref).collect();
    // Redelivery changes vector order, not descriptor meaning or first-seen order.
    for descriptor in &mut descriptors {
        descriptor.members.reverse();
        descriptor.boundary.reverse();
        descriptor.checkpoints.reverse();
    }
    descriptors.reverse();
    staged.extend_with_fragments(batch(&[]), descriptors, &applied.change_graph);
    assert_unchanged(&staged, &before);
    assert_eq!(
        staged.iter().map(std::ptr::from_ref).collect::<Vec<_>>(),
        pointers
    );
    model.check(&staged, &applied);
    (model, staged, applied)
}

fn exercise_chain(length: usize, prefix: usize, delivery: &Delivery, completions: &[usize]) {
    let (mut model, mut staged, mut applied) = setup(chain(), length, prefix, false, delivery);
    let original = staged.clone();
    let before = original.clone();
    for count in completions.iter().copied().chain([length]) {
        model.complete(count, &mut staged, &mut applied);
        assert_unchanged(&original, &before);
    }
    assert!(staged.is_empty());
    assert!(staged.groups().is_empty());
}

#[derive(Clone, Debug)]
enum Action {
    Complete(usize),
    Discard(usize),
    Redeliver {
        group: usize,
        grouped: bool,
        reverse: bool,
    },
}

fn actions() -> impl Strategy<Value = Action> {
    prop_oneof![
        (0usize..8).prop_map(Action::Complete),
        (0usize..8).prop_map(Action::Discard),
        (0usize..4, any::<bool>(), any::<bool>()).prop_map(|(group, grouped, reverse)| {
            Action::Redeliver {
                group,
                grouped,
                reverse,
            }
        }),
    ]
}

fn exercise_overlap(prefix: usize, omit_boundary: bool, delivery: &Delivery, actions: &[Action]) {
    let fixture = overlap();
    let (mut model, mut staged, mut applied) = setup(fixture, 7, prefix, omit_boundary, delivery);
    let original = staged.clone();
    let before = original.clone();
    for action in actions {
        match *action {
            Action::Complete(count) => model.complete(count, &mut staged, &mut applied),
            Action::Discard(value) => {
                // Includes heads, shared/external ancestry, loose history and absent actors.
                let (actor, seq) = match fixture.changes.get(value) {
                    Some(change) => (change.actor_id().clone(), change.seq()),
                    None => (ActorId::from([99]), 1),
                };
                model.discard(&actor, seq, &mut staged, &applied);
            }
            Action::Redeliver {
                group,
                grouped,
                reverse,
            } => {
                model.redeliver(group, grouped, reverse, &mut staged, &applied);
            }
        }
        assert_unchanged(&original, &before);
    }
    model.complete(7, &mut staged, &mut applied);
    // If the external boundary was withheld (rather than discarded), deliver it
    // through the applied graph and then finish the newly ready staged history.
    if omit_boundary && prefix == 0 {
        applied.apply_changes([fixture.changes[0].clone()]).unwrap();
        model.applied.insert(0);
        staged.remove_completed(
            &HashSet::from([fixture.changes[0].hash()]),
            &applied.change_graph,
        );
        model.check(&staged, &applied);
        model.complete(7, &mut staged, &mut applied);
    }
    assert!(model.ready().is_empty());
    model.check(&staged, &applied);
    assert_unchanged(&original, &before);
}

#[test]
fn lifecycle_edges_always_exercise_partial_overlap_and_empty_states() {
    let delivery = Delivery {
        contents: [6, 5, 4, 3, 2, 1, 0],
        groups: [3, 2, 1, 0],
        known: [true; 4],
        raw_prefix: 7,
        repeats: vec![3, 0, 1, 0, 2],
    };
    for length in 3..=6 {
        for prefix in 0..=length {
            exercise_chain(length, prefix, &delivery, &[0, 1, 1, 1, 1, 1, 1]);
        }
    }
    for prefix in 0..7 {
        // With an applied prefix, the external boundary is already present;
        // omitting it from delivery would repeat the exact same initial state.
        let boundary_options: &[bool] = if prefix == 0 {
            &[false, true]
        } else {
            &[false]
        };
        for &omit_boundary in boundary_options {
            for actions in [
                vec![],
                vec![Action::Complete(1), Action::Discard(2)], // partial completion, then one branch
                vec![Action::Discard(0)],                      // external root: transitive discard
                vec![Action::Discard(1)],                      // shared member: both fragments
                vec![Action::Discard(3)], // head only: prefix survives unclassified
                vec![Action::Complete(4), Action::Discard(4)], // one complete fragment, discard the other
                vec![Action::Discard(6), Action::Discard(7)],  // loose root, then absent actor
            ] {
                exercise_overlap(prefix, omit_boundary, &delivery, &actions);
            }
        }
    }
}

#[test]
fn lifecycle_edges_redeliver_after_partial_completion_and_invalidation() {
    let delivery = Delivery {
        contents: [6, 5, 4, 3, 2, 1, 0],
        groups: [0, 1, 2, 3],
        known: [true; 4],
        raw_prefix: 3,
        repeats: vec![0, 1, 0],
    };
    let grouped = Action::Redeliver {
        group: 0,
        grouped: true,
        reverse: true,
    };
    let raw = Action::Redeliver {
        group: 0,
        grouped: false,
        reverse: true,
    };
    for prefix in 0..7 {
        let boundary_options: &[bool] = if prefix == 0 {
            &[false, true]
        } else {
            &[false]
        };
        for &omit_boundary in boundary_options {
            for actions in [
                vec![Action::Complete(2), grouped.clone(), grouped.clone()],
                vec![Action::Discard(2), grouped.clone(), grouped.clone()],
                vec![Action::Discard(2), raw.clone(), grouped.clone()],
                // Restoring shared bytes must not revive the other invalidated
                // descriptor (or the old loose claim about that shared member).
                vec![Action::Discard(1), raw.clone(), grouped.clone()],
                vec![
                    Action::Discard(2),
                    grouped.clone(),
                    Action::Discard(2),
                    raw.clone(),
                ],
                vec![Action::Complete(7), grouped.clone()],
                vec![
                    Action::Discard(6),
                    Action::Redeliver {
                        group: 3,
                        grouped: true,
                        reverse: false,
                    },
                ],
            ] {
                exercise_overlap(prefix, omit_boundary, &delivery, &actions);
                // Also exercise first descriptor delivery after lifecycle changes.
                let unclassified = Delivery {
                    known: [false; 4],
                    ..delivery.clone()
                };
                exercise_overlap(prefix, omit_boundary, &unclassified, &actions);
            }
        }
    }
    // One explicit fully-applied schedule pins all no-op kinds, rather than
    // replaying every schedule against empty staging for each boundary flag.
    exercise_overlap(
        7,
        false,
        &delivery,
        &[
            Action::Complete(0),
            Action::Discard(0),
            grouped,
            raw,
            Action::Redeliver {
                group: 3,
                grouped: true,
                reverse: false,
            },
            Action::Redeliver {
                group: 3,
                grouped: false,
                reverse: false,
            },
            Action::Complete(7),
        ],
    );
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(128))]

    #[test]
    fn delivery_and_completion_match_full_membership_sets(
        suffix in 0usize..4,
        prefix in 0usize..7,
        delivery in deliveries(),
        completions in prop::collection::vec(0usize..7, 0..7),
    ) {
        let length = 3 + suffix;
        exercise_chain(length, prefix.min(length), &delivery, &completions);
    }

    #[test]
    fn overlapping_groups_follow_completion_discard_and_redelivery(
        prefix in 0usize..8,
        omit_boundary in any::<bool>(),
        delivery in deliveries(),
        actions in prop::collection::vec(actions(), 0..13),
    ) {
        exercise_overlap(prefix, omit_boundary, &delivery, &actions);
    }
}
