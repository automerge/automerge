//! Known bugs with no fix yet.
//!
//! Each test here fails, and each names one defect. They are `#[ignore]`d
//! so CI stays green; run them with
//!
//! ```text
//! cargo test -p hexane --test pending -- --ignored
//! ```
//!
//! Drop the attribute as each fix lands. A test that starts passing means
//! the bug is gone — or that the test stopped reaching it, which is worth
//! checking before deleting.

use hexane::{Column, DeltaColumn, DeltaRun, Iter, LoadOpts};

/// `try_resume` accepts any column, and the write counter alone cannot
/// tell two of them apart — every fresh column starts at zero. So an
/// iterator suspended over one column resumes into another and silently
/// reads its data.
///
/// The surviving slab-count check masks this whenever the two columns
/// have different layouts, so the columns here are deliberately the same
/// shape.
#[test]
#[ignore = "known bug: resume does not check column identity"]
fn resume_rejects_a_foreign_column() {
    let a = Column::<u64>::from_values_with_max_segments((0..200u64).collect(), 2);
    let b = Column::<u64>::from_values_with_max_segments((1000..1200u64).collect(), 2);
    assert_eq!(
        a.slab_count(),
        b.slab_count(),
        "same shape, or nothing is proved"
    );

    let mut it = a.iter();
    it.nth(2);
    let st = it.suspend();
    match st.try_resume(&b) {
        Err(_) => {}
        Ok(r) => panic!(
            "resumed into a foreign column and read {:?}",
            r.take(3).collect::<Vec<_>>()
        ),
    }
}

/// Nulls carry no value, so the value-range index cannot see them and
/// `find_by_value(None)` answers "no matches" for a column full of them
/// — disagreeing with `scan_to_value`, which finds them.
#[test]
#[ignore = "known bug: find_by_value cannot see nulls"]
fn find_by_value_finds_nulls() {
    let col = DeltaColumn::<Option<u32>>::from_values(vec![None, None, Some(1), Some(3)]);
    assert_eq!(col.iter().scan_to_value(None), Some(0), "scan finds them");
    assert_eq!(col.find_by_value(None).collect::<Vec<_>>(), vec![0, 1]);
}

/// Both public run-insert paths must enforce the value domain, or one
/// silently writes a column the other's `load` refuses. `insert_runs`
/// panics on this run; `splice_runs` accepts it.
#[test]
#[ignore = "known bug: splice_runs skips the domain check insert_runs makes"]
fn splice_runs_enforces_the_domain_like_insert_runs() {
    let run = DeltaRun {
        prefix: 0,
        delta: Some(1 << 40),
        count: 2,
    };
    let mut c = DeltaColumn::<u32>::new();
    let wrote = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        c.splice_runs(0, 0, [run]);
    }));
    if wrote.is_ok() {
        assert!(
            DeltaColumn::<u32>::load(&c.save()).is_ok(),
            "splice_runs wrote a column its own loader refuses"
        );
    }
}

/// Run counts come off the wire, so a running total that wraps describes
/// bytes that are not there. `load` must reject rather than panic (debug)
/// or hand back a nonsense length (release).
#[test]
#[ignore = "known bug: the RLE loader sums untrusted counts unchecked"]
fn rle_load_rejects_a_wrapped_length() {
    fn sleb(mut v: i64, out: &mut Vec<u8>) {
        loop {
            let byte = (v as u8) & 0x7f;
            v >>= 7;
            let done = (v == 0 && byte & 0x40 == 0) || (v == -1 && byte & 0x40 != 0);
            out.push(if done { byte } else { byte | 0x80 });
            if done {
                return;
            }
        }
    }
    // three repeat runs of i64::MAX items, distinct values so the
    // canonical-form check does not reject them first: the total wraps
    let mut bytes = Vec::new();
    for i in 0..3u8 {
        sleb(i64::MAX, &mut bytes);
        bytes.push(1 + i); // the repeated value
    }
    let r = std::panic::catch_unwind(|| Column::<u64>::load(&bytes));
    match r {
        Err(_) => panic!("load panicked on untrusted counts"),
        Ok(Err(_)) => {}
        Ok(Ok(c)) => panic!("load accepted a wrapped length: len = {}", c.len()),
    }
}

/// `Iter::default()` exists so it can be a placeholder field — automerge
/// derives `Default` around one and calls `shift_next` on it — so
/// repositioning a column-less iterator must yield nothing, not panic.
#[test]
#[ignore = "known bug: a default Iter panics on any repositioning call"]
fn a_default_iter_repositions_without_panicking() {
    let r = std::panic::catch_unwind(|| {
        let mut it = Iter::<u64>::default();
        it.shift(2..5);
        it.next()
    });
    assert_eq!(r.ok(), Some(None), "shift on a default Iter must be empty");

    let r = std::panic::catch_unwind(|| Iter::<u64>::default().shift_next(2..5));
    assert_eq!(
        r.ok(),
        Some(None),
        "shift_next on a default Iter must be empty"
    );
}

/// A fill is caller-supplied but arrives through `load`, so one outside
/// the value domain must be an error — not a column that cannot be read
/// back.
#[test]
#[ignore = "known bug: load_with's fill escapes domain validation"]
fn load_with_rejects_an_out_of_domain_fill() {
    let c = DeltaColumn::<u32>::load_with(&[], LoadOpts::new().with_length(3).with_fill(-5i64));
    match c {
        Err(_) => {}
        Ok(c) => panic!("accepted an out-of-domain fill -> {:?}", c.to_vec()),
    }
}
