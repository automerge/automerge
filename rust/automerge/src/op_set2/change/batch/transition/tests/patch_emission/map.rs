use super::*;

#[test]
fn map_absent_emits_nothing() {
    let fx = fixture();
    assert!(map_patches(
        &fx,
        CandidateSummary::default(),
        CandidateSummary::default()
    )
    .is_empty());
}

#[test]
fn map_appeared_puts_after_value_with_final_conflict() {
    let fx = fixture();
    let mut after = summary(fx.winner, Value::scalar(true));
    after.add_incoming(fx.lower, Value::scalar(false));
    let action = one(map_patches(&fx, CandidateSummary::default(), after));
    let (key, value, conflict) = put_map_of(&action);
    assert_eq!(key, "n");
    assert_eq!(value.as_bool(), Some(true));
    assert!(conflict);
}

#[test]
fn map_disappeared_deletes_key() {
    let fx = fixture();
    let before = summary(fx.winner, Value::scalar(true));
    let action = one(map_patches(&fx, before, CandidateSummary::default()));
    assert!(matches!(action, PatchAction::DeleteMap { key } if key == "n"));
}

#[test]
fn map_replaced_by_lower_id_puts_survivor() {
    let fx = fixture();
    let before = summary(fx.winner, Value::scalar(false));
    let mut after = CandidateSummary::default();
    after.add_incoming(fx.lower, Value::scalar(true));
    let action = one(map_patches(&fx, before, after));
    let (key, value, conflict) = put_map_of(&action);
    assert_eq!(key, "n");
    assert_eq!(value.as_bool(), Some(true));
    assert!(!conflict);
}

#[test]
fn retained_counter_conflict_clear_encodes_full_value_once() {
    let fx = fixture();
    let mut before = summary(fx.winner, counter(10));
    before.add_existing(fx.lower, Value::scalar(false));
    let after = summary(fx.winner, counter(15));
    let action = one(map_patches(&fx, before, after));
    let (key, value, conflict) = put_map_of(&action);
    assert_eq!(key, "n");
    assert_eq!(value.as_i64(), Some(15));
    assert!(!conflict);
}

#[test]
fn map_retained_counter_increments_without_conflict_change() {
    let fx = fixture();
    let before = summary(fx.winner, counter(10));
    let after = summary(fx.winner, counter(15));
    let action = one(map_patches(&fx, before, after));
    assert!(matches!(
        action,
        PatchAction::Increment { prop: Prop::Map(key), value: 5 } if key == "n"
    ));
}

#[test]
fn map_retained_counter_increments_and_flags_new_conflict() {
    let fx = fixture();
    let before = summary(fx.winner, counter(10));
    let mut after = summary(fx.winner, counter(15));
    after.add_incoming(fx.lower, Value::scalar(false));
    let actions = map_patches(&fx, before, after);
    assert_eq!(actions.len(), 2, "{actions:?}");
    assert!(matches!(
        &actions[0],
        PatchAction::Increment { prop: Prop::Map(key), value: 5 } if key == "n"
    ));
    assert!(matches!(&actions[1], PatchAction::Conflict { prop: Prop::Map(key) } if key == "n"));
}

#[test]
fn map_retained_counter_changes_without_new_conflict_emit_only_increment() {
    let fx = fixture();
    for delta in [-2, 2] {
        for conflicted in [false, true] {
            let mut before = summary(fx.winner, counter(0));
            let mut after = summary(fx.winner, counter(delta));
            if conflicted {
                before.add_existing(fx.lower, Value::scalar(false));
                after.add_existing(fx.lower, Value::scalar(false));
            }
            let action = one(map_patches(&fx, before, after));
            assert!(matches!(action,
                PatchAction::Increment { prop: Prop::Map(key), value }
                    if key == "n" && value == delta));
        }
    }
}

#[test]
fn map_retained_unchanged_value_only_flags_new_conflict() {
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
            one(map_patches(&fx, before, after)),
            PatchAction::Conflict { prop: Prop::Map(key) } if key == "n"
        ));
    }
}

#[test]
fn map_retained_unchanged_values_emit_nothing() {
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
            assert!(map_patches(&fx, before, after).is_empty());
        }
    }
}

#[test]
fn map_retained_appearing_conflict_only_flags() {
    let fx = fixture();
    let before = summary(fx.winner, Value::scalar(false));
    let mut after = summary(fx.winner, Value::scalar(false));
    after.add_incoming(fx.lower, Value::scalar(true));
    let action = one(map_patches(&fx, before, after));
    assert!(matches!(action, PatchAction::Conflict { prop: Prop::Map(key) } if key == "n"));
}

#[test]
fn map_replaced_by_existing_lower_counter_puts_final_total() {
    let fx = fixture();
    let mut before = summary(fx.lower, counter(10));
    before.add_existing(fx.winner, Value::scalar(false));
    let after = summary(fx.lower, counter(15));
    let action = one(map_patches(&fx, before, after));
    let (key, value, conflict) = put_map_of(&action);
    assert_eq!(key, "n");
    assert_eq!(value.as_i64(), Some(15));
    assert!(!conflict);
}
