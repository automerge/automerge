//! A client of the verified public API, checked by Verus: the API's
//! acceptance test.
//!
//! It knows of a column only what the API's specifications say, and
//! holds no assumption of its own (no `assume`, no `admit`, no
//! `external_body`). From that alone Verus proves that a column gives
//! back the values put in, and that it loads from its saved bytes as the
//! same values, which save to the same bytes.
//!
//! This module exists only while Verus checks the crate.

use super::VerifiedColumn;
use vstd::prelude::*;

verus! {

#[allow(dead_code)]
fn round_trip(input: &Vec<u64>)
    requires
        input@.len() < 1_000_000,
{
    let mut col = VerifiedColumn::<u64>::new();
    col.splice(0, 0, input.as_slice());
    let out = col.to_vec();
    assert(out@ == input@);

    let bytes = col.save();
    let back = VerifiedColumn::<u64>::load(bytes.as_slice());
    assert(back is Ok);
    assert(back->Ok_0@ == input@);
    // `save` runs, so it is called rather than named in an assertion
    let again = back.unwrap().save();
    assert(again@ == bytes@);
}

} // verus!
