//! A repeat run's count is an `i64` on the wire, so a column can't hold a
//! run of more than `i64::MAX` equal values. Making one is a panic, as
//! for `Column::fill`, rather than a column that saves to bytes that
//! don't load.

use hexane::Column;

#[test]
#[should_panic(expected = "is not encodable")]
fn growing_a_run_past_i64_max_panics() {
    let mut col = Column::<u64>::fill(i64::MAX as usize, 5);
    col.insert(0, 5);
}

#[test]
#[should_panic(expected = "is not encodable")]
fn growing_a_run_past_i64_max_panics_at_the_end() {
    let mut col = Column::<u64>::fill(i64::MAX as usize, 5);
    col.push(5);
}

#[cfg(target_pointer_width = "64")]
#[test]
fn a_longer_column_of_shorter_runs_still_round_trips() {
    let mut col = Column::<u64>::fill(i64::MAX as usize, 5);
    col.insert(0, 6);
    let bytes = col.save();
    let loaded = Column::<u64>::load(&bytes).unwrap();
    assert_eq!(loaded.len(), i64::MAX as usize + 1);
    assert_eq!(loaded.get(0), Some(6));
    assert_eq!(loaded.get(1), Some(5));
    assert_eq!(loaded.save(), bytes);
}
