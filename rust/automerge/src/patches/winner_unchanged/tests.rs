//! The decision table behind [`Encoding::new`]. Emission is covered
//! by `transition::tests::patch_emission` and the
//! `winner_unchanged_counter_conflict` integration tests.

use super::*;

/// Resolve the encoding for an unchanged winner from its two endpoints.
fn encoding(before_conflicted: bool, after_conflicted: bool, delta: i64) -> Encoding {
    let conflict = Conflict::new(before_conflicted, after_conflicted);
    Encoding::new(Facts {
        conflict,
        counter_delta: CounterDelta::new(delta),
    })
}

#[test]
fn counter_delta_without_conflict_change_increments() {
    assert_eq!(
        encoding(false, false, 5),
        Encoding::Increment {
            delta: 5,
            conflict_appeared: false
        }
    );
}

#[test]
fn counter_delta_with_appearing_conflict_increments_and_flags() {
    assert_eq!(
        encoding(false, true, 5),
        Encoding::Increment {
            delta: 5,
            conflict_appeared: true
        }
    );
}

#[test]
fn counter_delta_with_clearing_conflict_is_put_not_increment() {
    assert_eq!(encoding(true, false, 5), Encoding::Put);
}

#[test]
fn counter_delta_with_persisting_conflict_increments_without_flag() {
    assert_eq!(
        encoding(true, true, 5),
        Encoding::Increment {
            delta: 5,
            conflict_appeared: false
        }
    );
}

#[test]
fn zero_counter_delta_never_increments() {
    assert_eq!(encoding(false, true, 0), Encoding::ConflictAppeared);
    assert_eq!(encoding(true, false, 0), Encoding::Put);
    assert_eq!(encoding(false, false, 0), Encoding::Unchanged);
    assert_eq!(encoding(true, true, 0), Encoding::Unchanged);
}

#[test]
fn empty_counter_delta_is_zero() {
    assert_eq!(CounterDelta::empty(), CounterDelta::new(0));
}

#[test]
fn unchanged_scalar_with_persisting_conflict_is_unchanged() {
    assert_eq!(encoding(true, true, 0), Encoding::Unchanged);
}

#[test]
fn unchanged_scalar_with_appearing_conflict_only_flags() {
    assert_eq!(encoding(false, true, 0), Encoding::ConflictAppeared);
}

#[test]
fn unchanged_scalar_with_clearing_conflict_is_put() {
    assert_eq!(encoding(true, false, 0), Encoding::Put);
}
