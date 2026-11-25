//! A prefix column whose sum type has room for fewer values than it
//! holds (`Amount::room_len`) checks after each edit that its sums still
//! fit. It used to read all its values to see; now it reads only what the
//! edit put in, and adds their sum to what the column added up to: where
//! that fits, every sum of the column does. Where it does not, the
//! values the edit took out may have made room, and the column is read
//! whole as before.
//!
//! `Big` is a type of the client's whose values add up to themselves in a
//! `u64`, which is sure to have room for one of them.

use hexane::{Amount, Newtype, PackError, PrefixColumn};

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
struct Big(u64);

impl Newtype for Big {
    type Base = u64;

    fn to_base(self) -> u64 {
        self.0
    }

    fn from_base(b: u64) -> Result<Big, PackError> {
        Ok(Big(b))
    }
}

impl Amount for Big {
    type Sum = u64;

    fn room_len() -> usize {
        1
    }

    fn part_of(v: Big) -> u64 {
        v.0
    }
}

const HALF: u64 = u64::MAX / 2;

fn sums(col: &PrefixColumn<Big>) -> Vec<u64> {
    (0..col.len()).map(|i| col.get_total(i)).collect()
}

/// Edits whose values fit beside the column's sum: the quick check.
#[test]
fn edits_that_fit() {
    let mut col = PrefixColumn::<Big>::new();
    for i in 0..100 {
        col.insert(i / 2, Big(i as u64));
    }
    col.splice(10, 5, [Big(7), Big(8)]);
    col.remove(3);
    let want: u64 = col.iter().map(|b| b.0).sum();
    assert_eq!(col.get_total(col.len() - 1), want);
}

/// Taking a value out makes room for one that, beside the column's sum
/// before the edit, would not fit: the whole check takes it.
#[test]
fn room_an_edit_makes_is_seen() {
    let mut col = PrefixColumn::<Big>::new();
    col.splice(0, 0, [Big(HALF), Big(HALF)]);
    // `HALF + HALF + (HALF + 1)` is past `u64::MAX`, but the splice takes
    // the second `HALF` out
    col.splice(1, 1, [Big(HALF + 1)]);
    assert_eq!(sums(&col), vec![HALF, u64::MAX]);
}

/// An edit whose sums do not fit still panics.
#[test]
#[should_panic(expected = "prefix sum overflows")]
fn an_edit_that_overflows_panics() {
    let mut col = PrefixColumn::<Big>::new();
    col.splice(0, 0, [Big(HALF), Big(HALF)]);
    col.push(Big(2));
}

/// The same past a column's middle, with values on both sides.
#[test]
#[should_panic(expected = "prefix sum overflows")]
fn an_edit_in_the_middle_that_overflows_panics() {
    let mut col = PrefixColumn::<Big>::new();
    col.splice(0, 0, [Big(1); 40]);
    col.splice(20, 0, [Big(HALF), Big(HALF)]);
    col.insert(5, Big(HALF));
}
