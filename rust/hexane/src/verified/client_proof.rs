//! Clients of the verified public API, checked by Verus: the API's
//! acceptance test.
//!
//! They know of a column only what the API's specifications say, and
//! hold no assumption of their own (no `assume`, no `admit`, no
//! `external_body`). From that alone Verus proves that a column gives
//! back the values put in, and that it loads from its saved bytes as the
//! same values, which save to the same bytes: for every value type and
//! codec ([`round_trip`]), and so for numbers, for values that may be
//! null, for `bool`, and for a type of the client's own that is stored
//! as a number and is not every number ([`Action`]). And that a column
//! loaded from any bytes at all saves back to those bytes
//! ([`load_then_save`]).
//!
//! This module exists only while Verus checks the crate.

use super::{refused, VCodec, VNewtype, VValue, VerifiedColumn};
use crate::{Leb128, PackError};
use vstd::prelude::*;

verus! {

/// A column of the values `input`: it gives them back, and loads from
/// its saved bytes as the same values, which save to the same bytes.
/// The values are few enough for `load` to be sure to take what they
/// save to.
#[allow(dead_code)]
fn round_trip<V: VValue<C>, C: VCodec>(input: &Vec<V>)
    requires
        V::size_bound(input@) <= 0xffff_fff5,
        input@.len() <= usize::MAX - 3,
{
    let mut col = VerifiedColumn::<V, C>::new();
    col.splice(0, 0, input.as_slice());
    let out = col.to_vec();
    assert(out@ == input@);

    let bytes = col.save();
    let back = VerifiedColumn::<V, C>::load(bytes.as_slice());
    assert(back is Ok);
    assert(back->Ok_0@ == input@);
    // `save` runs, so it is called rather than named in an assertion
    let again = back.unwrap().save();
    assert(again@ == bytes@);
}

/// Numbers.
#[allow(dead_code)]
fn round_trip_u64(input: &Vec<u64>)
    requires
        input@.len() < 1_000_000,
{
    round_trip::<u64, Leb128>(input);
}

/// Values that may be null.
#[allow(dead_code)]
fn round_trip_nullable(input: &Vec<Option<i64>>)
    requires
        input@.len() < 1_000_000,
{
    round_trip::<Option<i64>, Leb128>(input);
}

/// `bool`, whose bound is another.
#[allow(dead_code)]
fn round_trip_bool(input: &Vec<bool>)
    requires
        input@.len() < 1_000_000,
{
    round_trip::<bool, Leb128>(input);
}

/// A value type of the client's own: one of four actions, each stored as
/// a number. Any other number is no action, and bytes that hold one do
/// not load as a column of actions.
#[derive(Clone, Copy)]
#[allow(dead_code)]
pub enum Action {
    Make,
    Set,
    Delete,
    Increment,
}

impl VNewtype for Action {
    type Base = u64;

    open spec fn base(v: Action) -> u64 {
        match v {
            Action::Make => 0,
            Action::Set => 1,
            Action::Delete => 3,
            Action::Increment => 4,
        }
    }

    proof fn lemma_base(v: Action, w: Action) {
    }

    fn to_base(self) -> u64 {
        match self {
            Action::Make => 0,
            Action::Set => 1,
            Action::Delete => 3,
            Action::Increment => 4,
        }
    }

    fn from_base(b: u64) -> Result<Action, PackError> {
        match b {
            0 => Ok(Action::Make),
            1 => Ok(Action::Set),
            3 => Ok(Action::Delete),
            4 => Ok(Action::Increment),
            _ => Err(refused("not an action")),
        }
    }
}

/// The client's own type, and nulls of it.
#[allow(dead_code)]
fn round_trip_action(input: &Vec<Action>, nullable: &Vec<Option<Action>>)
    requires
        input@.len() < 1_000_000,
        nullable@.len() < 1_000_000,
{
    round_trip::<Action, Leb128>(input);
    round_trip::<Option<Action>, Leb128>(nullable);
}

/// What a client reads is what it wrote: an edit, then a read of each
/// value.
#[allow(dead_code)]
fn edit_and_read(a: Action, b: Action) {
    let mut col = VerifiedColumn::<Option<Action>>::new();
    col.push(Some(a));
    col.push(Some(b));
    col.insert(1, None);
    assert(col@ == seq![Some(a), None, Some(b)]);
    let x = col.get(0);
    let y = col.get(1);
    let z = col.get(3);
    assert(x == Some(Some(a)));
    assert(y == Some(None::<Action>));
    assert(z is None);
    col.remove(0);
    assert(col@ == seq![None, Some(b)]);
    let mut it = col.iter();
    let first = it.next();
    let second = it.next();
    let third = it.next();
    assert(first == Some(None::<Action>));
    assert(second == Some(Some(b)));
    assert(third is None);
}

/// What loads, saves back to the bytes it was loaded from: no two byte
/// strings load as the same column. For any bytes, of any provenance.
#[allow(dead_code)]
fn load_then_save<V: VValue<C>, C: VCodec>(bytes: &[u8]) {
    if let Ok(col) = VerifiedColumn::<V, C>::load(bytes) {
        proof {
            VerifiedColumn::<V, C>::lemma_encoding(bytes@);
        }
        let again = col.save();
        assert(again@ == bytes@);
    }
}

/// Counting and cutting: a column's length, a range of it, and what is
/// left of it cut short.
#[allow(dead_code)]
fn count_and_cut(input: &Vec<u32>)
    requires
        input@.len() >= 3,
{
    let mut col = VerifiedColumn::<u32>::default();
    let none = col.is_empty();
    assert(none);
    col.splice(0, 0, input.as_slice());
    assert(col@ == input@);
    let n = col.len();
    let none = col.is_empty();
    assert(n == input@.len());
    assert(!none);
    let mut it = col.iter_range(1, 3);
    let x = it.next();
    let y = it.next();
    let z = it.next();
    assert(x == Some(input@[1]));
    assert(y == Some(input@[2]));
    assert(z is None);
    col.truncate(2);
    assert(col@ == input@.take(2));
    col.truncate(5);
    assert(col@ == input@.take(2));
    let mut small = VerifiedColumn::<bool>::with_max_segments(2);
    small.push(true);
    assert(small@ == seq![true]);
}

} // verus!
