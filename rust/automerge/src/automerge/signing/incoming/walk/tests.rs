use super::{walk, Visit};

#[test]
fn keeps_only_the_reached_frontier_without_opening_stopped_nodes() {
    let frontier = walk(
        vec![0, 0],
        |node| match node {
            0 => vec![1, 2, 3],
            2 => vec![0, 1, 4],
            _ => panic!("opened a stopped node"),
        },
        |node| match node {
            0 | 2 => Visit::Expand,
            1 | 4 => Visit::Keep,
            _ => Visit::Ignore,
        },
    );
    assert_eq!(frontier, [1, 4]);
}
