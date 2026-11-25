use proptest::prelude::*;

use super::resolve;

// Each row lists the dependencies of one vertex. IDs outside the slice
// stand for missing dependencies; already applied dependencies are omitted.
fn resolve_dependencies(dependencies: &[Vec<usize>]) -> Vec<usize> {
    let mut dependents = vec![Vec::new(); dependencies.len()];
    for (child, parents) in dependencies.iter().enumerate() {
        for &parent in parents {
            if let Some(children) = dependents.get_mut(parent) {
                children.push(child);
            }
        }
    }
    resolve(&dependents, dependencies.iter().map(Vec::len).collect())
}

#[test]
fn resolves_diamond_with_repeated_edges_but_not_missing_or_cyclic_ancestry() {
    let order = resolve_dependencies(&[
        vec![],     // root
        vec![0, 0], // repeated edge
        vec![0],    // other branch
        vec![1, 2], // both branches are required
        vec![10],   // missing dependency
        vec![3, 4], // transitively blocked by 10
        vec![7],    // cycle
        vec![6],    // cycle
        vec![7, 3], // depends on the cycle
    ]);
    assert_eq!(order, [0, 1, 2, 3]);
}

proptest! {
    #[test]
    fn worklist_matches_repeated_scan(
        deps in prop::collection::vec(prop::collection::vec(0usize..7, 0..5), 0..7),
    ) {
        let order = resolve_dependencies(&deps);
        let mut seen = vec![false; deps.len()];
        for v in order {
            prop_assert!(!seen[v]);
            prop_assert!(deps[v].iter().all(|&p| seen.get(p) == Some(&true)));
            seen[v] = true;
        }

        // Slow oracle: repeatedly scan for unblocked vertices rather than
        // maintaining reverse edges and counters.
        let mut expected = vec![false; deps.len()];
        loop {
            let Some(v) = (0..deps.len()).find(|&v| {
                !expected[v] && deps[v].iter().all(|&p| expected.get(p) == Some(&true))
            }) else {
                break;
            };
            expected[v] = true;
        }
        prop_assert_eq!(seen, expected);
    }
}
