//! Byte strings as values of the verified public API, verified with
//! Verus.
//!
//! A column of `Vec<u8>` holds byte strings. On the wire each is its
//! length, as an unsigned number of the column's codec, and then its
//! bytes: what [`Column<Vec<u8>>`](crate::Column) writes. A column gives
//! one as a slice borrowed from its slab and takes one as a slice
//! (`&[u8]`); a specification has it as its bytes (`Seq<u8>`). A column
//! of `Option<Vec<u8>>` holds byte strings and nulls.
//!
//! Three types are in play for one value, where a number has one: the
//! slice read from a slab, which lives as long as the borrow of the
//! column; the slice a column's model holds, which is the same bytes for
//! as long as the column is (`&'static [u8]`, in specifications only: no
//! such slice exists at run time); and the bytes. A slice is its bytes
//! and nothing more (vstd's `axiom_slice_ext_equal`), so the first two
//! are one value to a proof.
//!
//! What is trusted here, besides vstd's specifications of slices and of
//! `Vec`: one function that makes the error for malformed bytes
//! (`bad_format`), of which nothing is assumed.
//!
//! This file is a specification, to be read as the guide to reviewing
//! says ([`verified`](crate::verified#reviewing-the-specification)): each
//! function's body is a call of one of `imp` (`bytes/imp.rs`), which has
//! its contract word for word and is proved there.

// Verus proofs read more plainly with `x = x + n`.
#![allow(clippy::len_zero, clippy::assign_op_pattern)]

use super::codec::Codec;
#[cfg(verus_keep_ghost)]
use super::model::Model;
use super::model::Plain;
use super::{seal, ColumnValueRef, Owned, VForm, VGet, VSpec};
#[cfg(verus_keep_ghost)]
use crate::rle::lift::Lift;
#[cfg(verus_keep_ghost)]
use crate::rle::model::{RleOps, WriteOps};

use std::cmp::Ordering;
#[allow(unused_imports)]
use vstd::prelude::*;
#[allow(unused_imports)]
use vstd::slice::*;

use crate::trusted::bytes_bad_format as bad_format;
mod imp;
#[allow(unused_imports)]
use imp::*;
#[allow(unused_imports)]
pub(crate) use imp::{
    bytes_len, compare_bytes, put_bytes, read_bytes, try_read_bytes, BytesCow, VBytesOps,
    VBytesWrite, VOptBytesOps, VOptBytesWrite,
};
#[cfg(verus_keep_ghost)]
#[allow(unused_imports)]
pub(crate) use imp::{
    lemma_bytes_below, lemma_bytes_sizes, lemma_enc_bytes, lemma_opt_bytes_sizes, lemma_slice_len,
    lemma_slice_same,
};

verus! {

// ── A byte string on the wire ───────────────────────────────────────────────

/// The bytes of the byte string `b` under codec `C`: its length, then
/// itself.
pub open spec fn enc_bytes<C: Codec>(b: Seq<u8>) -> Seq<u8> {
    C::enc_u(b.len() as u64) + b
}

// ── The order of byte strings ───────────────────────────────────────────────

/// The byte string `a` comes before `b` by what is at `i`: up to there
/// they are the same bytes, and there `a` ends and `b` does not, or
/// `a` has the lower byte.
pub open spec fn below_at(a: Seq<u8>, b: Seq<u8>, i: int) -> bool {
    &&& 0 <= i <= a.len()
    &&& i < b.len()
    &&& a.take(i) == b.take(i)
    &&& (i == a.len() || a[i] < b[i])
}

/// Whether the byte string `a` comes before `b`, in the order of slices
/// (`[u8]`): at the first place where they are not the same, `a` has
/// the lower byte, or ends where `b` goes on.
pub open spec fn bytes_below(a: Seq<u8>, b: Seq<u8>) -> bool {
    exists|i: int| below_at(a, b, i)
}

/// The slice of the bytes `s`.
pub open spec fn slice_of(s: Seq<u8>) -> &'static [u8] {
    choose|v: &'static [u8]| v@ == s
}

/// The byte string a [`BytesCow`] stands for.
pub open spec fn cow_val<'a>(c: BytesCow<'a>) -> &'a [u8] {
    match c {
        BytesCow::Ref(v) => v,
        BytesCow::Own(o) => slice_of(o@),
    }
}

/// The value an `Option` of a [`BytesCow`] stands for.
pub open spec fn opt_cow_val<'a>(c: Option<BytesCow<'a>>) -> Option<&'a [u8]> {
    match c {
        Some(x) => Some(cow_val(x)),
        None => None,
    }
}

impl VGet for &[u8] {
    type S = Seq<u8>;

    open spec fn sv(self) -> Seq<u8> {
        self@
    }

    open spec fn svs(vs: Seq<Self>) -> Seq<Seq<u8>> {
        vs.map_values(|v: Self| v@)
    }

    proof fn lemma_svs(vs: Seq<Self>) {
    }

    proof fn lemma_sv_inj(a: Self, b: Self) {
        lemma_slice_same(a, b);
    }
}

/// The bytes of a byte string that may be null.
pub open spec fn opt_view<'a>(v: Option<&'a [u8]>) -> Option<Seq<u8>> {
    match v {
        Some(x) => Some(x@),
        None => None,
    }
}

impl VGet for Option<&[u8]> {
    type S = Option<Seq<u8>>;

    open spec fn sv(self) -> Option<Seq<u8>> {
        opt_view(self)
    }

    open spec fn svs(vs: Seq<Self>) -> Seq<Option<Seq<u8>>> {
        vs.map_values(|v: Self| opt_view(v))
    }

    proof fn lemma_svs(vs: Seq<Self>) {
    }

    proof fn lemma_sv_inj(a: Self, b: Self) {
        imp::vget_lemma_sv_inj_option_u8(a, b)
    }
}

impl VSpec for Vec<u8> {
    type S = Seq<u8>;

    open spec fn sv_of(v: Vec<u8>) -> Seq<u8> {
        v@
    }

    open spec fn below(a: Seq<u8>, b: Seq<u8>) -> bool {
        bytes_below(a, b)
    }

    proof fn lemma_below(a: Seq<u8>, b: Seq<u8>, c: Seq<u8>) {
        lemma_bytes_below(a, b, c);
    }
}

impl<'a> VForm<'a> for Vec<u8> {
    type Get = &'a [u8];
}

impl VSpec for Option<Vec<u8>> {
    type S = Option<Seq<u8>>;

    open spec fn sv_of(v: Option<Vec<u8>>) -> Option<Seq<u8>> {
        match v {
            Some(b) => Some(b@),
            None => None,
        }
    }

    open spec fn below(a: Option<Seq<u8>>, b: Option<Seq<u8>>) -> bool {
        match (a, b) {
            (None, Some(_)) => true,
            (Some(x), Some(y)) => bytes_below(x, y),
            _ => false,
        }
    }

    proof fn lemma_below(a: Option<Seq<u8>>, b: Option<Seq<u8>>, c: Option<Seq<u8>>) {
        imp::vspec_lemma_below_option_vec_u8(a, b, c)
    }
}

impl<'a> VForm<'a> for Option<Vec<u8>> {
    type Get = Option<&'a [u8]>;
}

// ── Sizes ───────────────────────────────────────────────────────────────────

/// How many bytes the byte strings `vs` are, all together.
pub open spec fn total_len(vs: Seq<Seq<u8>>) -> nat
    decreases vs.len(),
{
    if vs.len() == 0 {
        0
    } else {
        total_len(vs.drop_last()) + vs.last().len()
    }
}

/// How many bytes the byte strings of `vs` are, all together: a null is
/// none.
pub open spec fn total_len_opt(vs: Seq<Option<Seq<u8>>>) -> nat
    decreases vs.len(),
{
    if vs.len() == 0 {
        0
    } else {
        total_len_opt(vs.drop_last()) + match vs.last() {
            Some(b) => b.len(),
            None => 0,
        }
    }
}

impl<C: Codec> ColumnValueRef<C> for Vec<u8> {
    /// The byte strings' own bytes, and for each a length, a header and
    /// a count of ten bytes each.
    open spec fn size_bound(vs: Seq<Seq<u8>>) -> nat {
        30 * vs.len() + total_len(vs)
    }

    proof fn lemma_size(d: Seq<u8>) {
        imp::vvalue_lemma_size_vec_u8::<C>(d)
    }

    proof fn lemma_own<'a>(v: &'a [u8]) {
    }

    #[inline(always)]
    fn same(a: &[u8], b: &[u8]) -> (r: bool) {
        imp::vvalue_same_vec_u8(a, b)
    }

    #[inline(always)]
    fn compare(a: &[u8], b: &[u8]) -> (r: Ordering) {
        imp::vvalue_compare_vec_u8(a, b)
    }
}

impl<C: Codec> ColumnValueRef<C> for Option<Vec<u8>> {
    /// The byte strings' own bytes, and for each value a length, a
    /// header and a count of ten bytes each.
    open spec fn size_bound(vs: Seq<Option<Seq<u8>>>) -> nat {
        30 * vs.len() + total_len_opt(vs)
    }

    proof fn lemma_size(d: Seq<u8>) {
        imp::vvalue_lemma_size_option_vec_u8::<C>(d)
    }

    proof fn lemma_own<'a>(v: Option<&'a [u8]>) {
    }

    #[inline(always)]
    fn same(a: Option<&[u8]>, b: Option<&[u8]>) -> (r: bool) {
        imp::vvalue_same_option_vec_u8(a, b)
    }

    #[inline(always)]
    fn compare(a: Option<&[u8]>, b: Option<&[u8]>) -> (r: Ordering) {
        imp::vvalue_compare_option_vec_u8(a, b)
    }
}

} // verus!
