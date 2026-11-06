//! `copy_ranges` normalizes its splices, merging one into the one before
//! where they touch. A splice whose source range was reversed (`5..3`,
//! which as a `Range` is empty) wasn't treated as empty: merged into the
//! one before, its end replaced that one's, which lost source items. With
//! `[0..5 at 0, then 5..3 at 0 deleting 1]`, `copy_ranges` gave
//! `[0, 1, 2, 11, 12]` where the splices one by one give
//! `[0, 1, 2, 3, 4, 11, 12]`. And kept where it was (for its delete), it
//! could sit past the next splice's range, which the copy then skipped
//! (or, inverted, deleted out of bounds). Now an empty range, reversed or
//! not, copies nothing and stays where the last range kept ended.

use hexane::{Column, DeltaColumn, Splice};

/// `5..3`, built at run time (a literal reversed range is a lint)
fn reversed() -> std::ops::Range<usize> {
    std::hint::black_box(5)..3
}

#[test]
fn a_reversed_range_copies_nothing() {
    let dst = Column::<u64>::from_values(vec![10, 11, 12]);
    let src = Column::<u64>::from_values(vec![0, 1, 2, 3, 4, 5, 6]);
    let splices = vec![
        Splice {
            pos: 0,
            delete: 0,
            range: 0..5,
        },
        Splice {
            pos: 0,
            delete: 1,
            range: reversed(),
        },
    ];
    let mut c = dst.clone();
    c.copy_ranges(src.clone(), splices.clone());
    assert_eq!(c.to_vec(), vec![0, 1, 2, 3, 4, 11, 12]);

    let dst = DeltaColumn::<i64>::from_values(vec![10, 11, 12]);
    let src = DeltaColumn::<i64>::from_values(vec![0, 1, 2, 3, 4, 5, 6]);
    let mut c = dst.clone();
    c.copy_ranges(src, splices);
    assert_eq!(c.to_vec(), vec![0, 1, 2, 3, 4, 11, 12]);
}

#[test]
fn alone_it_still_deletes() {
    let mut c = Column::<u64>::from_values(vec![10, 11, 12]);
    let src = Column::<u64>::from_values(vec![0, 1, 2, 3, 4, 5, 6]);
    c.copy_ranges(
        src,
        vec![Splice {
            pos: 1,
            delete: 1,
            range: reversed(),
        }],
    );
    assert_eq!(c.to_vec(), vec![10, 12]);
}

#[test]
fn a_reversed_range_before_an_earlier_one() {
    let dst = Column::<u64>::from_values(vec![10, 11, 12]);
    let src = Column::<u64>::from_values((0..7).collect());
    let splices = vec![
        Splice {
            pos: 0,
            delete: 1,
            range: reversed(),
        },
        Splice {
            pos: 1,
            delete: 0,
            range: 3..4,
        },
    ];
    let mut c = dst.clone();
    c.copy_ranges(src, splices);
    assert_eq!(c.to_vec(), vec![3, 11, 12]);

    // big enough for the inverted strategy
    let src = Column::<u64>::from_values_with_max_segments((0..2000).collect(), 4);
    let mut c = Column::<u64>::from_values_with_max_segments(vec![10, 11, 12], 4);
    c.copy_ranges(
        src,
        vec![
            Splice {
                pos: 0,
                delete: 0,
                range: 0..2,
            },
            Splice {
                pos: 0,
                delete: 1,
                range: std::hint::black_box(1500)..3,
            },
            Splice {
                pos: 1,
                delete: 0,
                range: 3..1400,
            },
        ],
    );
    let want: Vec<u64> = (0..2).chain(3..1400).chain([11, 12]).collect();
    assert_eq!(c.to_vec(), want);
}
