//! Strings as values of the verified public API, verified with Verus.
//!
//! A column of `String` holds strings, and one of `Option<String>`
//! strings and nulls (`None`), as a column of `Option<Vec<u8>>` holds
//! byte strings and nulls. On the wire each string is a byte string
//! (`bytes`): the length of its UTF-8 bytes, as an unsigned number of the
//! column's codec, and then those bytes. That is what
//! [`Column<String>`](crate::Column) writes. A column gives one as a
//! `&str` borrowed from its slab and takes one as a `&str` (as an
//! `Option<&str>` where it may be null); a specification has it as its
//! characters (`Seq<char>`, Verus's view of a `str`).
//!
//! A load refuses bytes that are not UTF-8 where a string should be, with
//! [`PackError::InvalidUtf8`] as `Column<String>` does. What is read from
//! a column after that was checked by the load, and is checked again as
//! it is read: this module has no `unsafe`, so it asks the standard
//! library for the `str` of the bytes (`core::str::from_utf8`), which
//! looks at each byte. That it finds them UTF-8 is proved, and the branch
//! where it would not is proved not to be reached.
//!
//! # What is trusted here
//!
//! Besides vstd's specifications of `str`, `String` and UTF-8:
//!
//! * the specification of `core::str::from_utf8` given here, which vstd
//!   has none of: the `str` it gives has the bytes it was given, it
//!   gives one for all bytes that are valid UTF-8 (vstd's `valid_utf8`),
//!   and it does not panic;
//! * `axiom_str_same`: two `str`s of the same characters are one `str`.
//!   vstd says so of slices (`axiom_slice_ext_equal`) and not of `str`;
//! * one function that makes the error for bytes that are not UTF-8
//!   (`bad_utf8`), of which nothing is assumed.
//!
//! # A string too long to be one
//!
//! A `str` is bytes in memory, so it has no more of them than a `usize`
//! counts. vstd says that of a slice and not of a `str`: to a proof, a
//! `str` may be of any length. So this module does not say that every
//! `str` can be held in a column. It says which can: those of at most
//! `usize::MAX` bytes. A longer one is, in the words of the model under
//! a column, a "null" (`is_null`): a value that no column holds and no
//! bytes are read as. In a column of `String`, which has no nulls, that
//! is all a null is. In one of `Option<String>`, `None` is a null too,
//! and the only one a column holds: what the model asks of the value
//! type is only that its null value be a null (`axiom_null`), and a
//! writer is told, of each value it is handed, that one that is a null
//! is `None` (`WriteOps::cow_is_null`). Every `str` a program has is
//! short enough, which is proved where one is taken, by looking at its
//! bytes as a slice.
//!
//! That every `str` is so short cannot be assumed instead. vstd says
//! that string literals with different ids have different characters
//! (`axiom_new_strlit_view_id`), for every integer id, so that to a
//! proof there are infinitely many `str`s; there are only finitely many
//! of at most `usize::MAX` bytes.
//!
//! This file is a specification, to be read as the guide to reviewing
//! says ([`verified`](crate::verified#reviewing-the-specification)): each
//! function's body is a call of one of `imp` (`text/imp.rs`), which has
//! its contract word for word and is proved there.

// Verus proofs read more plainly with `x = x + n`.
#![allow(clippy::len_zero, clippy::assign_op_pattern)]

#[cfg(verus_keep_ghost)]
use super::bytes::bytes_below;
use super::bytes::{bytes_len, compare_bytes, put_bytes, read_bytes, try_read_bytes};
use super::codec::VCodec;
#[cfg(verus_keep_ghost)]
use super::model::Model;
use super::model::Plain;
use super::{seal, Owned, VForm, VGet, VSpec, VValue};
#[cfg(verus_keep_ghost)]
use crate::rle::lift::Lift;
#[cfg(verus_keep_ghost)]
use crate::rle::model::{RleOps, WriteOps};
use crate::PackError;
use std::cmp::Ordering;
#[allow(unused_imports)]
use vstd::prelude::*;
#[allow(unused_imports)]
use vstd::string::*;
#[cfg(verus_keep_ghost)]
use vstd::utf8::{encode_utf8, valid_utf8};

mod imp;
#[allow(unused_imports)]
use imp::*;
#[cfg(verus_keep_ghost)]
#[allow(unused_imports)]
pub(crate) use imp::{lemma_opt_text_sizes, lemma_text_sizes};
#[allow(unused_imports)]
pub(crate) use imp::{TextCow, VOptTextOps, VOptTextWrite, VTextOps, VTextWrite};

verus! {

// ── What is trusted ─────────────────────────────────────────────────────────

#[verifier::external_type_specification]
#[verifier::external_body]
#[allow(dead_code)]
pub struct ExUtf8Error(core::str::Utf8Error);

/// Trusted: what the standard library's `from_utf8` does. The `str` it
/// gives is of the bytes it was given, it gives one wherever those are
/// valid UTF-8, and it does not panic. (That it gives one only where
/// they are valid UTF-8 is not assumed: the bytes of a `str` are, which
/// vstd proves.)
pub assume_specification<'a>[ core::str::from_utf8 ](v: &'a [u8]) -> (r: Result<
    &'a str,
    core::str::Utf8Error,
>)
    ensures
        match r {
            Ok(s) => s.spec_bytes() == v@,
            Err(_) => !valid_utf8(v@),
        },
    no_unwind
;

/// Trusted: two `str`s of the same characters are one `str`. A `str` is
/// its characters and nothing more, as a slice is its elements (which
/// vstd says, in `axiom_slice_ext_equal`; of `str` it does not).
#[verifier::external_body]
proof fn axiom_str_same(a: &str, b: &str)
    requires
        a@ == b@,
    ensures
        a == b,
{
}

/// The error for bytes that are not UTF-8 where a string should be.
#[verifier::external_body]
#[cold]
fn bad_utf8() -> PackError {
    PackError::InvalidUtf8
}

/// The `str` of the characters `s`.
pub open spec fn str_of(s: Seq<char>) -> &'static str {
    choose|v: &'static str| v@ == s
}

/// The string a [`TextCow`] stands for.
pub open spec fn cow_val<'a>(c: TextCow<'a>) -> &'a str {
    match c {
        TextCow::Ref(v) => v,
        TextCow::Own(o) => str_of(o@),
    }
}

impl VGet for &str {
    type S = Seq<char>;

    open spec fn sv(self) -> Seq<char> {
        self@
    }

    open spec fn svs(vs: Seq<Self>) -> Seq<Seq<char>> {
        vs.map_values(|v: Self| v@)
    }

    proof fn lemma_svs(vs: Seq<Self>) {
    }

    proof fn lemma_sv_inj(a: Self, b: Self) {
        axiom_str_same(a, b);
    }
}

impl VSpec for String {
    type S = Seq<char>;

    open spec fn sv_of(v: String) -> Seq<char> {
        v@
    }

    /// The order of the strings' UTF-8 bytes, which is the order Rust
    /// has for `str`.
    open spec fn below(a: Seq<char>, b: Seq<char>) -> bool {
        bytes_below(encode_utf8(a), encode_utf8(b))
    }

    proof fn lemma_below(a: Seq<char>, b: Seq<char>, c: Seq<char>) {
        imp::vspec_lemma_below_string(a, b, c)
    }
}

impl<'a> VForm<'a> for String {
    type Get = &'a str;
}

// ── Sizes ───────────────────────────────────────────────────────────────────

/// How many bytes the strings `vs` are in UTF-8, all together.
pub open spec fn total_str_len(vs: Seq<Seq<char>>) -> nat
    decreases vs.len(),
{
    if vs.len() == 0 {
        0
    } else {
        total_str_len(vs.drop_last()) + encode_utf8(vs.last()).len()
    }
}

impl<C: VCodec> VValue<C> for String {
    /// The strings' own bytes, and for each a length, a header and a
    /// count of ten bytes each.
    open spec fn size_bound(vs: Seq<Seq<char>>) -> nat {
        30 * vs.len() + total_str_len(vs)
    }

    proof fn lemma_size(d: Seq<u8>) {
        imp::vvalue_lemma_size_string::<C>(d)
    }

    proof fn lemma_own<'a>(v: &'a str) {
    }

    #[inline(always)]
    fn same(a: &str, b: &str) -> (r: bool) {
        same_text(&a, &b)
    }

    #[inline(always)]
    fn compare(a: &str, b: &str) -> (r: Ordering) {
        imp::vvalue_compare_string(a, b)
    }
}

/// The value an `Option` of a [`TextCow`] stands for.
pub open spec fn opt_cow_val<'a>(c: Option<TextCow<'a>>) -> Option<&'a str> {
    match c {
        Some(x) => Some(cow_val(x)),
        None => None,
    }
}

/// The characters of a string that may be null.
pub open spec fn opt_chars<'a>(v: Option<&'a str>) -> Option<Seq<char>> {
    match v {
        Some(x) => Some(x@),
        None => None,
    }
}

impl VGet for Option<&str> {
    type S = Option<Seq<char>>;

    open spec fn sv(self) -> Option<Seq<char>> {
        opt_chars(self)
    }

    open spec fn svs(vs: Seq<Self>) -> Seq<Option<Seq<char>>> {
        vs.map_values(|v: Self| opt_chars(v))
    }

    proof fn lemma_svs(vs: Seq<Self>) {
    }

    proof fn lemma_sv_inj(a: Self, b: Self) {
        imp::vget_lemma_sv_inj_option_str(a, b)
    }
}

impl VSpec for Option<String> {
    type S = Option<Seq<char>>;

    open spec fn sv_of(v: Option<String>) -> Option<Seq<char>> {
        match v {
            Some(s) => Some(s@),
            None => None,
        }
    }

    /// The null before every string, and strings in the order of their
    /// UTF-8 bytes, as for `String`: the order Rust has for
    /// `Option<&str>`.
    open spec fn below(a: Option<Seq<char>>, b: Option<Seq<char>>) -> bool {
        match (a, b) {
            (None, Some(_)) => true,
            (Some(x), Some(y)) => bytes_below(encode_utf8(x), encode_utf8(y)),
            _ => false,
        }
    }

    proof fn lemma_below(a: Option<Seq<char>>, b: Option<Seq<char>>, c: Option<Seq<char>>) {
        imp::vspec_lemma_below_option_string(a, b, c)
    }
}

impl<'a> VForm<'a> for Option<String> {
    type Get = Option<&'a str>;
}

// ── Sizes of strings that may be null ───────────────────────────────────────

/// How many bytes the strings of `vs` are in UTF-8, all together: a null
/// is none.
pub open spec fn total_str_len_opt(vs: Seq<Option<Seq<char>>>) -> nat
    decreases vs.len(),
{
    if vs.len() == 0 {
        0
    } else {
        total_str_len_opt(vs.drop_last()) + match vs.last() {
            Some(s) => encode_utf8(s).len(),
            None => 0,
        }
    }
}

impl<C: VCodec> VValue<C> for Option<String> {
    /// The strings' own bytes, and for each value a length, a header and
    /// a count of ten bytes each.
    open spec fn size_bound(vs: Seq<Option<Seq<char>>>) -> nat {
        30 * vs.len() + total_str_len_opt(vs)
    }

    proof fn lemma_size(d: Seq<u8>) {
        imp::vvalue_lemma_size_option_string::<C>(d)
    }

    proof fn lemma_own<'a>(v: Option<&'a str>) {
    }

    #[inline(always)]
    fn same(a: Option<&str>, b: Option<&str>) -> (r: bool) {
        imp::vvalue_same_option_string(a, b)
    }

    #[inline(always)]
    fn compare(a: Option<&str>, b: Option<&str>) -> (r: Ordering) {
        imp::vvalue_compare_option_string(a, b)
    }
}

} // verus!
