use super::cache_matches;

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
