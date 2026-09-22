use super::*;

#[test]
fn list_absent_emits_nothing() {
    let fx = fixture();
    assert!(list_patches(
        &fx,
        CandidateSummary::default(),
        CandidateSummary::default()
    )
    .is_empty());
}

#[test]
fn list_appeared_inserts_after_value_with_final_conflict() {
    let fx = fixture();
    let mut after = summary(fx.winner, Value::scalar(true));
    after.add_incoming(fx.lower, Value::scalar(false));
    let action = one(list_patches(&fx, CandidateSummary::default(), after));
    let (index, values) = insert_of(&action);
    assert_eq!(index, 0);
    assert_eq!(values.len(), 1);
    assert_eq!(values[0].0.as_bool(), Some(true));
    assert!(values[0].1);
}

#[test]
fn list_disappeared_deletes_one_element() {
    let fx = fixture();
    let before = summary(fx.winner, Value::scalar(true));
    let action = one(list_patches(&fx, before, CandidateSummary::default()));
    assert!(matches!(
        action,
        PatchAction::DeleteSeq {
            index: 0,
            length: 1
        }
    ));
}

#[test]
fn list_replaced_by_lower_id_puts_survivor() {
    let fx = fixture();
    let before = summary(fx.winner, Value::scalar(false));
    let mut after = CandidateSummary::default();
    after.add_incoming(fx.lower, Value::scalar(true));
    let action = one(list_patches(&fx, before, after));
    let (index, value, conflict) = put_seq_of(&action);
    assert_eq!(index, 0);
    assert_eq!(value.as_bool(), Some(true));
    assert!(!conflict);
}

#[test]
fn list_retained_counter_conflict_clear_puts_full_value_once() {
    let fx = fixture();
    let mut before = summary(fx.winner, counter(10));
    before.add_existing(fx.lower, Value::scalar(false));
    let after = summary(fx.winner, counter(15));
    let action = one(list_patches(&fx, before, after));
    let (index, value, conflict) = put_seq_of(&action);
    assert_eq!(index, 0);
    assert_eq!(value.as_i64(), Some(15));
    assert!(!conflict);
}

#[test]
fn list_retained_counter_increments_and_flags_new_conflict() {
    let fx = fixture();
    let before = summary(fx.winner, counter(10));
    let mut after = summary(fx.winner, counter(15));
    after.add_incoming(fx.lower, Value::scalar(false));
    let actions = list_patches(&fx, before, after);
    assert_eq!(actions.len(), 2, "{actions:?}");
    assert!(matches!(
        &actions[0],
        PatchAction::Increment {
            prop: Prop::Seq(0),
            value: 5
        }
    ));
    assert!(matches!(
        &actions[1],
        PatchAction::Conflict { prop: Prop::Seq(0) }
    ));
}

#[test]
fn list_retained_counter_changes_without_new_conflict_emit_only_increment() {
    let fx = fixture();
    for delta in [-2, 2] {
        for conflicted in [false, true] {
            let mut before = summary(fx.winner, counter(0));
            let mut after = summary(fx.winner, counter(delta));
            if conflicted {
                before.add_existing(fx.lower, Value::scalar(false));
                after.add_existing(fx.lower, Value::scalar(false));
            }
            let action = one(list_patches(&fx, before, after));
            assert!(matches!(action,
                PatchAction::Increment { prop: Prop::Seq(0), value } if value == delta));
        }
    }
}

#[test]
fn list_retained_unchanged_value_only_flags_new_conflict() {
    let fx = fixture();
    for value in [
        counter(0),
        Value::scalar(false),
        Value::scalar(7),
        Value::scalar("same"),
    ] {
        let before = summary(fx.winner, value);
        let mut after = before.clone();
        after.add_incoming(fx.lower, Value::scalar(false));
        assert!(matches!(
            one(list_patches(&fx, before, after)),
            PatchAction::Conflict { prop: Prop::Seq(0) }
        ));
    }
}

#[test]
fn list_retained_unchanged_values_emit_nothing() {
    let fx = fixture();
    for value in [
        counter(0),
        Value::scalar(false),
        Value::scalar(7),
        Value::scalar("same"),
    ] {
        // Different loser counts still represent the same conflicted value.
        for (before_count, after_count) in [(1, 1), (2, 2), (2, 3), (3, 2), (3, 3)] {
            let (before, after) =
                retained_endpoints(&fx, &value, &value, before_count, after_count);
            assert!(list_patches(&fx, before, after).is_empty());
        }
    }
}
