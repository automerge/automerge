use super::*;

#[test]
fn first_candidate_becomes_unconflicted_winner() {
    let summary = existing(&[(3, Value::scalar(true))]);
    let present = present(&summary);
    assert_eq!(present.winner.id, id(3));
    assert_eq!(present.winner.origin, CandidateOrigin::Existing);
    assert_eq!(present.other_candidates, 0);
    assert!(!present.conflicted());
}

#[test]
fn incoming_candidate_records_its_origin() {
    let mut summary = CandidateSummary::default();
    summary.add_incoming(id(7), Value::scalar(1));
    assert_eq!(present(&summary).winner.origin, CandidateOrigin::Incoming);
}

#[test]
fn ascending_insertion_selects_greatest_id() {
    let summary = existing(&[
        (1, Value::scalar(1)),
        (2, Value::scalar(2)),
        (3, Value::scalar(3)),
    ]);
    let present = present(&summary);
    assert_eq!(present.winner.id, id(3));
    assert_eq!(present.winner.value.as_i64(), 3);
    assert_eq!(present.other_candidates, 2);
    assert!(present.conflicted());
}

#[test]
fn descending_insertion_selects_greatest_id() {
    let summary = existing(&[
        (3, Value::scalar(3)),
        (2, Value::scalar(2)),
        (1, Value::scalar(1)),
    ]);
    let present = present(&summary);
    assert_eq!(present.winner.id, id(3));
    assert_eq!(present.winner.value.as_i64(), 3);
    assert_eq!(present.other_candidates, 2);
}

#[test]
fn equal_payloads_with_distinct_ids_still_count_separately() {
    let summary = existing(&[(1, Value::scalar(false)), (4, Value::scalar(false))]);
    let present = present(&summary);
    assert_eq!(present.winner.id, id(4));
    assert_eq!(present.other_candidates, 1);
    assert!(present.conflicted());
}
