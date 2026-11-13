//! Head selection preserves the existing cover, including descriptor order.

use std::collections::HashSet;
use std::ops::Bound::{Excluded, Included, Unbounded};
use std::ops::RangeBounds;

use crate::transaction::{CommitOptions, Transactable};
use crate::{ActorId, Automerge, ChangeHash, Fragment, ROOT};

use super::ChangeGraph;

fn at_level(doc: &mut Automerge, value: i64, level: usize) -> ChangeHash {
    for time in 0..10_000 {
        let mut next = doc.clone();
        let mut tx = next.transaction();
        tx.put(ROOT, "value", value).unwrap();
        let (hash, _) = tx.commit_with(CommitOptions::default().with_time(time));
        let hash = hash.unwrap();
        if hash.fragment_level() == level {
            *doc = next;
            return hash;
        }
    }
    panic!("fixture level search exhausted");
}

fn check_selections<R: RangeBounds<usize> + Clone>(
    graph: &ChangeGraph,
    levels: R,
    selected_hashes: &[ChangeHash],
) {
    // Materialize the unselected cover before invoking the filtered query.
    // Exact Vec equality checks order and every descriptor field, not just heads.
    let cover: Vec<_> = graph.fragments(levels.clone()).collect();
    for mask in 0..(1usize << selected_hashes.len()) {
        let selected: HashSet<_> = selected_hashes
            .iter()
            .enumerate()
            .filter(|(index, _)| mask & (1 << index) != 0)
            .map(|(_, hash)| *hash)
            .collect();
        let expected: Vec<_> = cover
            .iter()
            .filter(|fragment| selected.contains(&fragment.head))
            .cloned()
            .collect();
        let actual: Vec<_> = graph
            .fragments_filtered(levels.clone(), |head| selected.contains(head))
            .collect();
        assert_eq!(actual, expected, "selection mask {mask:#b}");
    }
    assert_eq!(
        graph
            .fragments_filtered(levels, |_| true)
            .collect::<Vec<_>>(),
        cover
    );
}

#[test]
fn filtered_fragments_match_post_export_selection_for_every_subset_and_level_range() {
    // a -> b -> c -> d -> e. Nonzero b/d delimit two fragments; a/c
    // are covered interior nodes. e/f remain loose, with f on another actor.
    let mut doc = Automerge::new().with_actor(ActorId::from([1]));
    let a = at_level(&mut doc, 1, 0);
    let b = at_level(&mut doc, 2, 1);
    let c = at_level(&mut doc, 3, 0);
    let d = at_level(&mut doc, 4, 1);
    let e = at_level(&mut doc, 5, 0);
    let mut other = Automerge::new().with_actor(ActorId::from([2]));
    let f = at_level(&mut other, 6, 0);
    doc.apply_changes(other.get_changes(&[])).unwrap();
    let graph = &doc.change_graph;
    // The graph enumerates its current heads in hash order.
    assert_eq!(graph.heads().collect::<Vec<_>>(), [e, f]);
    let cover: Vec<_> = graph.fragments(..).collect();
    // A literal cover guards the baseline: fragments() itself delegates to the
    // filtered implementation, so comparing the two routes alone isn't enough.
    assert_eq!(
        cover,
        vec![
            Fragment {
                head: e,
                level: 0,
                boundary: vec![d],
                checkpoints: vec![],
                members: vec![e]
            },
            Fragment {
                head: f,
                level: 0,
                boundary: vec![],
                checkpoints: vec![],
                members: vec![f]
            },
            Fragment {
                head: d,
                level: 1,
                boundary: vec![b],
                checkpoints: vec![],
                members: vec![d, c]
            },
            Fragment {
                head: b,
                level: 1,
                boundary: vec![],
                checkpoints: vec![],
                members: vec![b, a]
            },
        ]
    );

    let unknown = ChangeHash([0xff; 32]);
    assert!(!graph.has_change(&unknown));
    let selected_hashes = [a, b, c, d, e, f, unknown];
    check_selections(graph, .., &selected_hashes);
    check_selections(graph, 0..=0, &selected_hashes);
    check_selections(graph, 1.., &selected_hashes);
    check_selections(graph, 1..=1, &selected_hashes);
    check_selections(graph, 0..0, &selected_hashes);
    check_selections(graph, 2.., &selected_hashes);
    check_selections(graph, (Excluded(0), Included(1)), &selected_hashes);
    check_selections(graph, (Unbounded, Excluded(1)), &selected_hashes);
}

#[test]
fn selecting_a_loose_ancestor_does_not_prune_traversal_at_a_rejected_tip() {
    let mut doc = Automerge::new().with_actor(ActorId::from([1]));
    let a = at_level(&mut doc, 1, 0);
    let b = at_level(&mut doc, 2, 0);
    let c = at_level(&mut doc, 3, 0);
    let graph = &doc.change_graph;
    let cover: Vec<_> = graph.fragments(..).collect();
    assert_eq!(
        cover
            .iter()
            .map(|fragment| fragment.head)
            .collect::<Vec<_>>(),
        [c, b, a]
    );
    check_selections(graph, .., &[a, b, c]);
    let selected: Vec<_> = graph.fragments_filtered(.., |head| *head == a).collect();
    assert_eq!(
        selected,
        vec![Fragment {
            head: a,
            level: 0,
            boundary: vec![],
            checkpoints: vec![],
            members: vec![a],
        }]
    );
}

#[test]
fn head_filter_is_not_invoked_for_candidates_outside_the_level_range() {
    let mut doc = Automerge::new().with_actor(ActorId::from([1]));
    at_level(&mut doc, 1, 1);
    at_level(&mut doc, 2, 0);
    let graph = &doc.change_graph;
    let loose: Vec<_> = graph
        .fragments_filtered(..1, |head| {
            assert_eq!(head.fragment_level(), 0);
            true
        })
        .collect();
    assert_eq!(loose.len(), 1);
    let cached: Vec<_> = graph
        .fragments_filtered(1.., |head| {
            assert_eq!(head.fragment_level(), 1);
            true
        })
        .collect();
    assert_eq!(cached.len(), 1);
    assert_eq!(
        graph
            .fragments_filtered(0..0, |_| panic!("empty range invoked filter"))
            .count(),
        0
    );
}

#[test]
fn readiness_filters_cached_fragments_but_preserves_loose_changes() {
    let mut doc = Automerge::new().with_actor(ActorId::from([1]));
    let cached = at_level(&mut doc, 1, 1);
    at_level(&mut doc, 2, 0);
    at_level(&mut doc, 3, 0);
    let graph = &doc.change_graph;
    let mut readiness = crate::change_graph::FragmentReadiness::new(graph, |_| false);
    let loose: Vec<_> = graph.fragments(..1).collect();
    let mut signed: HashSet<_> = loose.iter().map(|fragment| fragment.head).collect();

    let selected = crate::change_graph::OutgoingSelection::new(graph, &readiness)
        .collect(|hash| signed.contains(hash));
    assert_eq!(selected, loose);
    assert!(selected.iter().all(|fragment| fragment.level == 0));

    signed.insert(cached);
    readiness.signature_added(&cached);
    assert_eq!(
        crate::change_graph::OutgoingSelection::new(graph, &readiness)
            .collect(|hash| signed.contains(hash)),
        graph.fragments(..).collect::<Vec<_>>()
    );
}

#[test]
fn filtering_an_empty_graph_does_not_invoke_the_predicate() {
    let doc = Automerge::new();
    assert_eq!(
        doc.change_graph
            .fragments_filtered(.., |_| panic!("empty graph invoked filter"))
            .count(),
        0
    );
}
