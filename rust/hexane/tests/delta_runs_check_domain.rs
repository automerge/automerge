//! `DeltaColumn::splice_runs` and `DeltaEdit::insert_runs` take
//! `DeltaRun`s, whose values must lie in the column's domain. But a run
//! carries no type, so runs from an `i64` column went into a `u32`
//! column unchecked: `[-5, 7]` spliced into `[1, 2, 3]` read back as
//! `[1, 4294967291, 7, 2, 3]`, which `save` then wrote and `load`
//! rejected. The fix checked each run's values against the domain.
//!
//! Both now require that the runs' values be ones the column can keep
//! (`delta_runs_fit`), and `DeltaColumn::run_fits` is the check of a
//! run that they made. Checked: `run_fits` refuses the runs that went in
//! unchecked, and runs in the domain pass it and still copy.

use hexane::{DeltaColumn, DeltaRun};

/// Whether every run of `runs` passes the check of a column of `u32`.
fn all_fit_u32(runs: &[DeltaRun]) -> bool {
    runs.iter().all(DeltaColumn::<u32>::run_fits)
}

#[test]
fn runs_below_the_domain() {
    let src = DeltaColumn::<i64>::from_values(&[-5, 7]);
    let runs: Vec<DeltaRun> = src.iter().runs().collect();
    assert!(!all_fit_u32(&runs));
}

#[test]
fn runs_above_the_domain() {
    let src = DeltaColumn::<u64>::from_values(&[1 << 40]);
    let runs: Vec<DeltaRun> = src.iter().runs().collect();
    assert!(!all_fit_u32(&runs));
}

#[test]
fn runs_out_of_a_nullable_domain() {
    let src = DeltaColumn::<Option<i64>>::from_values(&[Some(3), None, Some(-1)]);
    let runs: Vec<DeltaRun> = src.iter().runs().collect();
    assert!(!runs.iter().all(DeltaColumn::<Option<u64>>::run_fits));
}

#[test]
fn runs_in_the_domain_still_copy() {
    let src = DeltaColumn::<i64>::from_values(&[5, 6, 7, 7, 100]);
    let runs: Vec<DeltaRun> = src.iter().runs().collect();
    assert!(all_fit_u32(&runs));
    let mut dst = DeltaColumn::<u32>::from_values(&[1, 2, 3]);
    dst.splice_runs(1, 1, &runs);
    assert_eq!(dst.to_vec(), vec![1, 5, 6, 7, 7, 100, 3]);
    let src = DeltaColumn::<u32>::from_values(&[u32::MAX, 0, 9]);
    let runs: Vec<DeltaRun> = src.iter().runs().collect();
    assert!(all_fit_u32(&runs));
    let mut dst = DeltaColumn::<u32>::from_values(&[4]);
    let mut e = dst.edit();
    e.insert_runs(&runs);
    e.finish();
    assert_eq!(dst.to_vec(), vec![u32::MAX, 0, 9, 4]);
    let bytes = dst.save();
    assert_eq!(
        DeltaColumn::<u32>::load(&bytes).unwrap().to_vec(),
        dst.to_vec()
    );
}
