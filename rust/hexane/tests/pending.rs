//! Known bugs with no fix yet.
//!
//! Each test here names one defect. Those that fail are `#[ignore]`d so
//! CI stays green; run them with
//!
//! ```text
//! cargo test -p hexane --test pending -- --ignored
//! ```
//!
//! Drop the attribute as each fix lands: the tests without one pass. A
//! test that starts passing means the bug is gone — or that the test
//! stopped reaching it, which is worth checking before deleting.

use hexane::{Column, DeltaColumn, Iter, LoadOpts};

/// Run counts come off the wire, so a running total that wraps describes
/// bytes that are not there. `load` must reject rather than panic (debug)
/// or hand back a nonsense length (release).
#[test]
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
fn load_with_rejects_an_out_of_domain_fill() {
    let c = DeltaColumn::<u32>::load_with(&[], LoadOpts::new().with_length(3).with_fill(-5i64));
    match c {
        Err(_) => {}
        Ok(c) => panic!("accepted an out-of-domain fill -> {:?}", c.to_vec()),
    }
}
