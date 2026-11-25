use super::find_conflict;

#[test]
fn finds_conflicts_with_applied_slots_and_with_distinct_candidate_hashes() {
    let view = |id| match id {
        10 | 11 => ((1, 3), false),
        20 => ((2, 3), false),
        30 => ((3, 9), true),
        _ => unreachable!(),
    };
    assert_eq!(find_conflict(&[10, 20], view), None);
    assert_eq!(find_conflict(&[10, 20, 11, 30], view), Some(11));
    assert_eq!(find_conflict(&[20, 30], view), Some(30));
    // In a different ancestry, only one arm of the candidate fork is present.
    assert_eq!(find_conflict(&[11, 20], view), None);
}
