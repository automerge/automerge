//! A delete from a delta column that leaves a slab's tail empty closes
//! the slab, and closing it can merge it with the next slab before the
//! delta that bridges the deleted items is written: until it is, the next
//! slab's first delta is relative to an item that's gone. Re-weighing the
//! merged slab then met a running sum outside `i64` and panicked ("delta
//! run overflows i64"), though every value, before and after, is in the
//! domain. The aggregate is now taken with wrapping arithmetic, exact
//! wherever the sums fit, as the index's merges are; the slab is weighed
//! again once the bridging delta is written, and an edit that leaves the
//! values out of the domain still panics (`check_span`).

use hexane::DeltaColumn;

const M: u64 = i64::MAX as u64;

#[test]
fn a_delete_across_slabs() {
    let vals: Vec<u64> = (0..65).map(|i| if i % 2 == 0 { 0 } else { M }).collect();
    let mut col = DeltaColumn::<u64>::from_values(vals);
    col.splice(3, 60, []);
    assert_eq!(col.to_vec(), vec![0, M, 0, M, 0]);
    assert_eq!(
        DeltaColumn::<u64>::load(&col.save()).unwrap().to_vec(),
        vec![0, M, 0, M, 0]
    );
}

#[test]
fn a_small_delete_across_slabs() {
    let mut col = DeltaColumn::<u64>::with_max_segments(8);
    col.splice(0, 0, [0, M, 0, 0, 1, 0, 0, M, 0]);
    col.splice(3, 4, []);
    assert_eq!(col.to_vec(), vec![0, M, 0, M, 0]);
}

#[test]
fn a_cursor_delete_across_slabs() {
    let vals: Vec<i64> = (0..65)
        .map(|i| if i % 2 == 0 { 0 } else { i64::MAX })
        .collect();
    let mut col = DeltaColumn::<i64>::from_values(vals);
    {
        let mut e = col.edit();
        e.seek(3);
        e.delete(61);
        e.finish();
    }
    assert_eq!(col.to_vec(), vec![0, i64::MAX, 0, 0]);
}

#[test]
#[should_panic(expected = "span 2^63 or more")]
fn values_out_of_the_domain_still_panic() {
    let mut col = DeltaColumn::<i64>::from_values(vec![0, 1, 2]);
    col.insert(3, i64::MAX);
    col.insert(0, i64::MIN + 1);
}

/// After any delete the index's aggregates are the slabs' again: the
/// value search, which prunes slabs by them, agrees with the values.
#[test]
fn the_index_is_right_after_deletes() {
    let mut seed = 7u64;
    let mut next = move || {
        seed ^= seed << 13;
        seed ^= seed >> 7;
        seed ^= seed << 17;
        seed
    };
    for round in 0..300 {
        let n = 20 + (next() % 200) as usize;
        let mut model: Vec<u64> = (0..n)
            .map(|i| match next() % 4 {
                0 => 0,
                1 => M,
                2 => (i as u64) % 5,
                _ => M - (i as u64) % 5,
            })
            .collect();
        let mut col = DeltaColumn::<u64>::with_max_segments(2 + (next() % 8) as usize);
        col.splice(0, 0, model.iter().copied());
        for _ in 0..5 {
            if model.is_empty() {
                break;
            }
            let at = (next() % model.len() as u64) as usize;
            let del = (next() % (model.len() - at) as u64) as usize + 1;
            col.splice(at, del, []);
            model.drain(at..at + del);
            col.check_invariants();
            assert_eq!(col.to_vec(), model, "round {round}");
            // (not `M` itself: a search for `i64::MAX` is fixed separately,
            // on fix/delta-find-at-i64-limits)
            for t in [0, 1, 4, M - 4, M - 1] {
                let want: Vec<usize> = (0..model.len()).filter(|&i| model[i] == t).collect();
                assert_eq!(
                    col.find_by_value(t).collect::<Vec<_>>(),
                    want,
                    "round {round} t {t}"
                );
            }
            if !model.is_empty() {
                let i = (next() % model.len() as u64) as usize;
                assert_eq!(col.get(i), Some(model[i]), "round {round}");
            }
        }
    }
}
