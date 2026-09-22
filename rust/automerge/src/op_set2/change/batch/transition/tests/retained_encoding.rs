use super::*;

/// Build a retained transition with the same winner ID in both endpoints and
/// return its two payloads.
fn retained(
    before_winner: Value,
    before_conflicted: bool,
    after_winner: Value,
    after_conflicted: bool,
) -> (PresentCandidates, PresentCandidates) {
    let mut before = existing(&[(4, before_winner)]);
    if before_conflicted {
        before.add_existing(id(1), Value::scalar(false));
    }
    let mut after = existing(&[(4, after_winner)]);
    if after_conflicted {
        after.add_incoming(id(2), Value::scalar(false));
    }
    let ValueTransition(Transition::WinnerUnchanged { before, after }) =
        ValueTransition::new(before, after)
    else {
        panic!("same identity must remain Retained");
    };
    (before, after)
}

#[test]
fn retained_counter_delta_without_conflict_change_increments() {
    let (before, after) = retained(counter(10), false, counter(15), false);
    assert_eq!(
        WinnerUnchangedPatch::new(&before, &after),
        WinnerUnchangedPatch::Increment {
            delta: 5,
            conflict_appeared: false
        }
    );
}

#[test]
fn retained_counter_delta_with_appearing_conflict_increments_and_flags() {
    let (before, after) = retained(counter(10), false, counter(15), true);
    assert_eq!(
        WinnerUnchangedPatch::new(&before, &after),
        WinnerUnchangedPatch::Increment {
            delta: 5,
            conflict_appeared: true
        }
    );
}

#[test]
fn retained_counter_delta_with_clearing_conflict_is_put_not_increment() {
    let (before, after) = retained(counter(10), true, counter(15), false);
    assert_eq!(
        WinnerUnchangedPatch::new(&before, &after),
        WinnerUnchangedPatch::Put
    );
}

#[test]
fn retained_counter_delta_with_persisting_conflict_increments_without_flag() {
    let (before, after) = retained(counter(10), true, counter(15), true);
    assert_eq!(
        WinnerUnchangedPatch::new(&before, &after),
        WinnerUnchangedPatch::Increment {
            delta: 5,
            conflict_appeared: false
        }
    );
}

#[test]
fn zero_counter_delta_never_increments() {
    let (before, after) = retained(counter(10), false, counter(10), true);
    assert_eq!(
        WinnerUnchangedPatch::new(&before, &after),
        WinnerUnchangedPatch::ConflictAppeared
    );
    let (before, after) = retained(counter(10), true, counter(10), false);
    assert_eq!(
        WinnerUnchangedPatch::new(&before, &after),
        WinnerUnchangedPatch::Put
    );
    let (before, after) = retained(counter(10), false, counter(10), false);
    assert_eq!(
        WinnerUnchangedPatch::new(&before, &after),
        WinnerUnchangedPatch::Unchanged
    );
    let (before, after) = retained(counter(10), true, counter(10), true);
    assert_eq!(
        WinnerUnchangedPatch::new(&before, &after),
        WinnerUnchangedPatch::Unchanged
    );
}

#[test]
fn unchanged_scalar_with_persisting_conflict_is_unchanged() {
    let (before, after) = retained(Value::scalar(false), true, Value::scalar(false), true);
    assert_eq!(
        WinnerUnchangedPatch::new(&before, &after),
        WinnerUnchangedPatch::Unchanged
    );
}

#[test]
fn unchanged_scalar_with_appearing_conflict_only_flags() {
    let (before, after) = retained(Value::scalar(false), false, Value::scalar(false), true);
    assert_eq!(
        WinnerUnchangedPatch::new(&before, &after),
        WinnerUnchangedPatch::ConflictAppeared
    );
}

#[test]
fn unchanged_scalar_with_clearing_conflict_is_put() {
    let (before, after) = retained(Value::scalar(false), true, Value::scalar(false), false);
    assert_eq!(
        WinnerUnchangedPatch::new(&before, &after),
        WinnerUnchangedPatch::Put
    );
}

#[test]
fn stable_object_with_clearing_conflict_is_put() {
    let (before, after) = retained(Value::map(), true, Value::map(), false);
    assert_eq!(
        WinnerUnchangedPatch::new(&before, &after),
        WinnerUnchangedPatch::Put
    );
}
