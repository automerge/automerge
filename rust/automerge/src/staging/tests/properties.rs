//! Check readiness and completion against an independent availability model
//! over real content-addressed DAGs.

use std::collections::{BTreeSet, HashSet};
use std::num::NonZeroU64;

use proptest::prelude::*;

use crate::{ActorId, Automerge, Change, ExpandedChange};

use super::{hashes, stage};

#[derive(Clone, Debug)]
struct Step {
    actor: u8,
    dependencies: u16,
    staged: bool,
    order: u8,
}

#[derive(Clone, Debug)]
struct Recipe {
    steps: Vec<Step>,
    applied_len: usize,
}

fn recipes() -> impl Strategy<Value = Recipe> {
    (
        prop::collection::vec((0u8..3, any::<u16>(), any::<bool>(), any::<u8>()), 0..11),
        0usize..12,
    )
        .prop_map(|(steps, applied_len)| Recipe {
            steps: steps
                .into_iter()
                .map(|(actor, dependencies, staged, order)| Step {
                    actor,
                    dependencies,
                    staged,
                    order,
                })
                .collect(),
            applied_len,
        })
}

// Dependencies only reference earlier nodes. Add each actor's predecessor so
// generated actor chains are valid without filtering away inconvenient recipes.
fn materialize(recipe: &Recipe) -> (Vec<Change>, Vec<Vec<usize>>) {
    let mut changes: Vec<Change> = Vec::new();
    let mut dependencies = Vec::new();
    let mut previous = [None; 3];
    let mut sequences = [0; 3];
    for (index, step) in recipe.steps.iter().enumerate() {
        let actor = usize::from(step.actor);
        let mut deps: BTreeSet<usize> = (0..index)
            .filter(|dep| step.dependencies & (1u16 << dep) != 0)
            .collect();
        if let Some(predecessor) = previous[actor] {
            deps.insert(predecessor);
        }
        let deps: Vec<_> = deps.into_iter().collect();
        let mut dep_hashes: Vec<_> = deps.iter().map(|dep| changes[*dep].hash()).collect();
        dep_hashes.sort();
        sequences[actor] += 1;
        let change = Change::from(ExpandedChange {
            operations: Vec::new(),
            actor_id: ActorId::from([step.actor + 1]),
            hash: None,
            seq: sequences[actor],
            start_op: NonZeroU64::MIN,
            time: index as i64,
            message: None,
            deps: dep_hashes,
            extra_bytes: Vec::new(),
            author: None,
        });
        assert_eq!(change.deps().len(), deps.len());
        for dep in &deps {
            assert!(change.deps().contains(&changes[*dep].hash()));
        }
        previous[actor] = Some(index);
        changes.push(change);
        dependencies.push(deps);
    }
    // Fixture guard: compatible real changes, not fabricated hashes.
    let mut document = Automerge::new();
    document.apply_changes(changes.clone()).unwrap();
    (changes, dependencies)
}

fn check(
    changes: &[Change],
    dependencies: &[Vec<usize>],
    applied_len: usize,
    staged_indices: &[usize],
) {
    let staged_set: BTreeSet<_> = staged_indices.iter().copied().collect();
    // This oracle follows the recipe's already-topological indices. It has no
    // waiting counts, reverse edges, or ready queue. Missing nodes block all
    // their descendants, even when those descendants' direct parents are staged.
    let mut available = vec![false; changes.len()];
    for index in 0..changes.len() {
        available[index] = index < applied_len
            || (staged_set.contains(&index)
                && dependencies[index].iter().all(|dep| available[*dep]));
    }
    let expected: BTreeSet<_> = staged_indices
        .iter()
        .filter(|index| available[**index])
        .map(|index| changes[*index].hash())
        .collect();

    let mut staged = stage(
        &staged_indices
            .iter()
            .map(|index| &changes[*index])
            .collect::<Vec<_>>(),
    );
    let before: Vec<_> = staged.iter().cloned().collect();
    let mut applied = Automerge::new();
    applied
        .apply_changes(changes[..applied_len].to_vec())
        .unwrap();
    let heads_before = applied.get_heads();
    let bytes_before = applied.save();
    let prepared = staged.prepare(&applied.change_graph);
    let actual = hashes(prepared.changes());

    assert_eq!(actual.iter().copied().collect::<BTreeSet<_>>(), expected);
    assert_eq!(actual.len(), expected.len(), "duplicate emission");
    for (position, change) in prepared.changes().iter().enumerate() {
        for dep in change.deps() {
            assert!(
                applied.change_graph.has_change(dep) || actual[..position].contains(dep),
                "dependency must be available before its dependent"
            );
        }
        let stored = staged
            .iter()
            .find(|stored| stored.hash() == change.hash())
            .unwrap();
        assert!(
            std::ptr::eq(*change, stored),
            "borrow the original staged contents"
        );
    }
    assert_eq!(
        hashes(staged.prepare(&applied.change_graph).changes()),
        actual
    );

    let blocked: Vec<_> = before
        .iter()
        .filter(|change| !expected.contains(&change.hash()))
        .cloned()
        .collect();

    assert_eq!(staged.iter().cloned().collect::<Vec<_>>(), before);
    assert_eq!(applied.get_heads(), heads_before);
    assert_eq!(applied.save(), bytes_before);

    // Exercise completion after successful fixture application. The expected
    // survivors come from the independent readiness oracle above, not removal.
    applied
        .apply_changes(
            prepared
                .changes()
                .iter()
                .map(|c| (*c).clone())
                .collect::<Vec<_>>(),
        )
        .unwrap();
    let completed: HashSet<_> = actual.into_iter().collect();
    for _ in 0..2 {
        staged.remove_completed(&completed, &applied.change_graph);
        assert_eq!(staged.iter().cloned().collect::<Vec<_>>(), blocked);
        assert_eq!(staged.is_empty(), blocked.is_empty());
        assert_eq!(
            staged.hashes,
            blocked.iter().map(Change::hash).collect::<HashSet<_>>()
        );
        assert_eq!(
            staged.incoming_actor_seqs,
            blocked
                .iter()
                .map(|change| (change.actor_id().clone(), change.seq()))
                .collect::<HashSet<_>>()
        );
        assert!(staged.prepare(&applied.change_graph).changes().is_empty());
    }
}

fn exercise(recipe: Recipe) {
    let (changes, dependencies) = materialize(&recipe);
    let applied_len = recipe.applied_len.min(changes.len());
    let mut suffix: Vec<_> = (applied_len..changes.len()).collect();
    suffix.sort_by_key(|index| (recipe.steps[*index].order, *index));
    let mut partial: Vec<_> = suffix
        .iter()
        .copied()
        .filter(|index| recipe.steps[*index].staged)
        .collect();

    check(&changes, &dependencies, applied_len, &partial);
    partial.reverse();
    check(&changes, &dependencies, applied_len, &partial);
    // Always exercise a fully ready suffix as well, not just blocked subsets.
    check(&changes, &dependencies, applied_len, &suffix);
    suffix.reverse();
    check(&changes, &dependencies, applied_len, &suffix);
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(128))]

    #[test]
    fn preparation_and_completion_match_ready_history(recipe in recipes()) {
        exercise(recipe);
    }
}
