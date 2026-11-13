//! A column counts its writes, and an iterator state suspended from it
//! resumes only while the count is the same (`try_resume`). But `remap`
//! rebuilt the column from scratch, starting the count again at 0, so a
//! state suspended before earlier writes could match it again and resume
//! over values it never described.

use hexane::Column;

#[test]
fn a_state_from_before_a_remap_does_not_resume() {
    let mut col = Column::<u64>::from_values((0..10).collect());
    let mut it = col.iter();
    it.nth(2);
    let state = it.suspend();
    col.push(99u64);
    assert!(state.try_resume(&col).is_err());
    col.remap(|x| x + 1000);
    col.truncate(10);
    assert!(state.try_resume(&col).is_err());
}

#[test]
fn a_remap_alone_ends_suspended_states() {
    let mut col = Column::<u64>::from_values((0..10).collect());
    let mut it = col.iter();
    it.nth(2);
    let state = it.suspend();
    col.remap(|x| x + 1);
    assert!(state.try_resume(&col).is_err());
}

#[test]
fn a_state_suspended_after_the_remap_resumes() {
    let mut col = Column::<u64>::from_values((0..10).collect());
    col.remap(|x| x * 2);
    let mut it = col.iter();
    it.nth(2);
    let state = it.suspend();
    let mut resumed = state.try_resume(&col).unwrap();
    assert_eq!(resumed.next(), Some(6));
}
