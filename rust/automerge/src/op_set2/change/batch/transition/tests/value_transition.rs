use super::*;

#[test]
fn empty_to_empty_is_absent() {
    let ValueTransition(transition) =
        ValueTransition::new(CandidateSummary::default(), CandidateSummary::default());
    assert!(matches!(transition, Transition::Absent));
}

#[test]
fn empty_to_present_is_appeared_with_after_winner() {
    let after = existing(&[(2, Value::scalar(true))]);
    let ValueTransition(Transition::Appeared { after }) =
        ValueTransition::new(CandidateSummary::default(), after)
    else {
        panic!("expected Appeared");
    };
    assert_eq!(after.winner.id, id(2));
    assert!(!after.conflicted());
}

#[test]
fn present_to_empty_is_disappeared_with_before_winner() {
    let before = existing(&[(2, Value::scalar(true)), (5, Value::scalar(false))]);
    let ValueTransition(Transition::Disappeared { before }) =
        ValueTransition::new(before, CandidateSummary::default())
    else {
        panic!("expected Disappeared");
    };
    assert_eq!(before.winner.id, id(5));
    assert!(before.conflicted());
}

#[test]
fn different_ids_in_either_direction_are_replaced() {
    let low = existing(&[(1, Value::scalar(true))]);
    let high = existing(&[(4, Value::scalar(false))]);

    let ValueTransition(Transition::WinnerReplaced { before, after }) =
        ValueTransition::new(low.clone(), high.clone())
    else {
        panic!("expected Replaced");
    };
    assert_eq!(before.winner.id, id(1));
    assert_eq!(after.winner.id, id(4));

    // A deleted higher winner can be replaced by a surviving lower ID.
    let ValueTransition(Transition::WinnerReplaced { before, after }) =
        ValueTransition::new(high, low)
    else {
        panic!("expected Replaced");
    };
    assert_eq!(before.winner.id, id(4));
    assert_eq!(after.winner.id, id(1));
}

#[test]
fn equal_payloads_with_different_ids_are_replaced_not_retained() {
    let before = existing(&[(4, Value::scalar(false))]);
    let after = existing(&[(5, Value::scalar(false))]);
    let ValueTransition(transition) = ValueTransition::new(before, after);
    assert!(matches!(transition, Transition::WinnerReplaced { .. }));
}

#[test]
fn same_id_is_retained_for_every_conflict_combination() {
    for before_conflicted in [false, true] {
        for after_conflicted in [false, true] {
            let mut before = existing(&[(4, Value::scalar(true))]);
            if before_conflicted {
                before.add_existing(id(1), Value::scalar(false));
            }
            let mut after = existing(&[(4, Value::scalar(true))]);
            if after_conflicted {
                after.add_incoming(id(2), Value::scalar(false));
            }
            let ValueTransition(Transition::WinnerUnchanged { before, after }) =
                ValueTransition::new(before, after)
            else {
                panic!(
                    "same identity must remain Retained ({before_conflicted}, {after_conflicted})"
                );
            };
            assert_eq!(before.winner.id, id(4));
            assert_eq!(after.winner.id, id(4));
            assert_eq!(before.conflicted(), before_conflicted);
            assert_eq!(after.conflicted(), after_conflicted);
        }
    }
}

#[test]
fn conflict_clearing_is_semantically_retained() {
    let mut before = CandidateSummary::default();
    before.add_existing(id(1), Value::scalar(false));
    before.add_existing(id(4), counter(10));
    let mut after = CandidateSummary::default();
    after.add_existing(id(4), counter(15));

    let ValueTransition(Transition::WinnerUnchanged { before, after }) =
        ValueTransition::new(before, after)
    else {
        panic!("same identity must remain Retained");
    };
    assert_eq!(before.winner.id, id(4));
    assert_eq!(after.winner.id, id(4));
    assert!(before.conflicted());
    assert!(!after.conflicted());
    assert_eq!(before.winner.value.as_i64(), 10);
    assert_eq!(after.winner.value.as_i64(), 15);
}

#[test]
fn classification_ignores_candidate_origin() {
    let before = existing(&[(4, Value::scalar(true))]);
    let mut after = CandidateSummary::default();
    after.add_incoming(id(4), Value::scalar(true));
    let ValueTransition(transition) = ValueTransition::new(before, after);
    assert!(matches!(transition, Transition::WinnerUnchanged { .. }));
}
