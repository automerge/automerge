//! `DeltaColumn::splice_runs` and `DeltaEdit::insert_runs` take
//! `DeltaRun`s, whose values must lie in the column's domain: a writer's
//! contract, like `DeltaValue::to_i64`, and a write outside the domain
//! panics. But a run carries no type, so runs from an `i64` column went
//! into a `u32` column unchecked: `[-5, 7]` spliced into `[1, 2, 3]` read
//! back as `[1, 4294967291, 7, 2, 3]`, which `save` then wrote and `load`
//! rejected. Now each run's values are checked against the domain.

use hexane::DeltaColumn;

#[test]
#[should_panic(expected = "delta run out of domain")]
fn spliced_runs_below_the_domain() {
    let src = DeltaColumn::<i64>::from_values(vec![-5, 7]);
    let mut dst = DeltaColumn::<u32>::from_values(vec![1, 2, 3]);
    dst.splice_runs(1, 0, src.iter().runs());
}

#[test]
#[should_panic(expected = "delta run out of domain")]
fn spliced_runs_above_the_domain() {
    let src = DeltaColumn::<u64>::from_values(vec![1 << 40]);
    let mut dst = DeltaColumn::<u32>::from_values(vec![1, 2, 3]);
    dst.splice_runs(0, 1, src.iter().runs());
}

#[test]
#[should_panic(expected = "delta run out of domain")]
fn cursor_runs_out_of_the_domain() {
    let src = DeltaColumn::<Option<i64>>::from_values(vec![Some(3), None, Some(-1)]);
    let mut dst = DeltaColumn::<Option<u64>>::from_values(vec![Some(1)]);
    let mut e = dst.edit();
    e.seek(1).insert_runs(src.iter().runs());
}

#[test]
fn runs_in_the_domain_still_copy() {
    let src = DeltaColumn::<i64>::from_values(vec![5, 6, 7, 7, 100]);
    let mut dst = DeltaColumn::<u32>::from_values(vec![1, 2, 3]);
    dst.splice_runs(1, 1, src.iter().runs());
    assert_eq!(dst.to_vec(), vec![1, 5, 6, 7, 7, 100, 3]);
    let src = DeltaColumn::<u32>::from_values(vec![u32::MAX, 0, 9]);
    let mut dst = DeltaColumn::<u32>::from_values(vec![4]);
    {
        let mut e = dst.edit();
        e.insert_runs(src.iter().runs());
    }
    assert_eq!(dst.to_vec(), vec![u32::MAX, 0, 9, 4]);
    let bytes = dst.save();
    assert_eq!(
        DeltaColumn::<u32>::load(&bytes).unwrap().to_vec(),
        dst.to_vec()
    );
}
