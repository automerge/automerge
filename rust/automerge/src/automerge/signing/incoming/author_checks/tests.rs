use super::{cache_matches, find_predecessor};

#[test]
fn predecessor_requires_both_actor_and_sequence_and_can_be_authorless() {
    let describe = |dep| match dep {
        1 => Some((10, 3, Some(99))),
        2 => Some((20, 2, Some(99))),
        3 => Some((10, 2, None)),
        4 => Some((10, 2, Some(88))),
        _ => None,
    };
    assert_eq!(find_predecessor(&[0, 1, 2], 10, 2, describe), None);
    assert_eq!(find_predecessor(&[1, 2, 3, 4], 10, 2, describe), Some(None));
}

#[test]
fn cache_requires_the_exact_author_for_every_frontier() {
    let expected = [Some(10), Some(20), None];
    let recorded = [Some(10), Some(10), None];
    let expected = |hash: usize| expected[hash];
    let recorded = |hash: usize| recorded[hash].as_ref();
    assert!(cache_matches(&[0], expected, recorded));
    assert!(!cache_matches(&[0, 1], expected, recorded));
    assert!(!cache_matches(&[0, 2], expected, recorded));
}
