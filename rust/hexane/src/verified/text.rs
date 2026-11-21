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

// Verus proofs read more plainly with `x = x + n`.
#![allow(clippy::len_zero, clippy::assign_op_pattern)]

#[cfg(verus_keep_ghost)]
use super::bytes::{
    bytes_below, enc_bytes, lemma_bytes_below, lemma_enc_bytes, lemma_slice_len, lemma_slice_same,
};
use super::bytes::{bytes_len, compare_bytes, put_bytes, read_bytes, try_read_bytes};
use super::codec::VCodec;
#[cfg(verus_keep_ghost)]
use super::model::Model;
use super::model::Plain;
use super::{seal, Owned, VForm, VGet, VSpec, VValue};
#[cfg(verus_keep_ghost)]
use crate::rle::dec::slab_values;
use crate::rle::dec::RleReads;
use crate::rle::edit::RleModel;
use crate::rle::enc::REncoder;
use crate::rle::lift::Lift;
#[cfg(verus_keep_ghost)]
use crate::rle::model::encs;
use crate::rle::model::{RleOps, WriteOps};
#[cfg(verus_keep_ghost)]
use crate::rle::size::heads_bound;
use crate::PackError;
use std::cmp::Ordering;
use std::marker::PhantomData;
#[allow(unused_imports)]
use vstd::prelude::*;
#[allow(unused_imports)]
use vstd::string::*;
#[cfg(verus_keep_ghost)]
use vstd::utf8::{encode_utf8, encode_utf8_decode_utf8, encode_utf8_valid_utf8, valid_utf8};

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

// ── A string and its bytes ──────────────────────────────────────────────────

/// Characters with the same UTF-8 bytes are the same characters.
proof fn lemma_utf8_inj(a: Seq<char>, b: Seq<char>)
    requires
        encode_utf8(a) == encode_utf8(b),
    ensures
        a == b,
{
    encode_utf8_decode_utf8(a);
    encode_utf8_decode_utf8(b);
}

/// The UTF-8 bytes of a string. They are a slice, so there are no more
/// of them than a `usize` counts.
#[inline(always)]
fn str_bytes(s: &str) -> (b: &[u8])
    ensures
        b@ == s.spec_bytes(),
        s.spec_bytes().len() <= usize::MAX,
    no_unwind
{
    let b = s.as_bytes();
    proof {
        lemma_slice_len(b);
    }
    b
}

/// The string at the start of `data` and how many bytes it takes there,
/// or an error where no string's bytes start `data`: there is no byte
/// string there, or one that is not UTF-8.
#[inline(always)]
fn try_read_text<C: VCodec>(data: &[u8]) -> (r: Result<(usize, &str), PackError>)
    ensures
        match r {
            Ok((n, v)) => n <= data.len() && data@.take(n as int) == enc_bytes::<C>(v.spec_bytes())
                && v.spec_bytes().len() <= usize::MAX,
            Err(_) => forall|s: Seq<char>|
                encode_utf8(s).len() <= usize::MAX ==> !(#[trigger] enc_bytes::<C>(
                    encode_utf8(s),
                )).is_prefix_of(data@),
        },
{
    let (n, b) = match try_read_bytes::<C>(data) {
        Ok(x) => x,
        Err(e) => {
            return Err(e);
        },
    };
    match core::str::from_utf8(b) {
        Ok(v) => {
            proof {
                lemma_slice_len(b);
            }
            Ok((n, v))
        },
        Err(_) => {
            proof {
                lemma_slice_len(b);
                assert forall|s: Seq<char>| encode_utf8(s).len() <= usize::MAX implies !(#[trigger] enc_bytes::<C>(
                    encode_utf8(s),
                )).is_prefix_of(data@) by {
                    if enc_bytes::<C>(encode_utf8(s)).is_prefix_of(data@) {
                        lemma_text_at::<C>(data@, n as int, b@, s);
                    }
                }
            }
            Err(bad_utf8())
        },
    }
}

/// Where the byte string `b` is the first `n` bytes of `d` and a
/// string's bytes start `d` too, `b` is that string's bytes: so it is
/// UTF-8.
proof fn lemma_text_at<C: VCodec>(d: Seq<u8>, n: int, b: Seq<u8>, s: Seq<char>)
    requires
        0 <= n <= d.len(),
        d.take(n) == enc_bytes::<C>(b),
        b.len() <= usize::MAX,
        encode_utf8(s).len() <= usize::MAX,
        enc_bytes::<C>(encode_utf8(s)).is_prefix_of(d),
    ensures
        b == encode_utf8(s),
        valid_utf8(b),
{
    let e = encode_utf8(s);
    let x = enc_bytes::<C>(e);
    assert(x + d.skip(x.len() as int) =~= d);
    assert(d.take(n) + d.skip(n) =~= d);
    lemma_enc_bytes::<C>(e, b, d.skip(x.len() as int), d.skip(n));
    encode_utf8_valid_utf8(s);
}

/// The string known to start `data`, and how many bytes it takes there.
#[inline(always)]
fn read_text<C: VCodec>(data: &[u8]) -> (r: (usize, &str))
    requires
        exists|s: Seq<char>|
            encode_utf8(s).len() <= usize::MAX && (#[trigger] enc_bytes::<C>(encode_utf8(s))).is_prefix_of(
                data@,
            ),
    ensures
        r.0 <= data.len(),
        data@.take(r.0 as int) == enc_bytes::<C>(r.1.spec_bytes()),
        r.1.spec_bytes().len() <= usize::MAX,
    no_unwind
{
    let ghost s = choose|s: Seq<char>|
        encode_utf8(s).len() <= usize::MAX && (#[trigger] enc_bytes::<C>(encode_utf8(s))).is_prefix_of(data@);
    let (n, b) = read_bytes::<C>(data);
    proof {
        lemma_slice_len(b);
        lemma_text_at::<C>(data@, n as int, b@, s);
    }
    match core::str::from_utf8(b) {
        Ok(v) => (n, v),
        // the bytes are a string's, which are UTF-8: this is not reached
        Err(_) => {
            proof {
                assert(false);
            }
            (0, "")
        },
    }
}

/// Whether two strings are the same string.
#[inline(always)]
fn same_text(a: &&str, b: &&str) -> (r: bool)
    ensures
        r == (*a == *b),
        r == (a@ == b@),
{
    let x = str_bytes(a);
    let y = str_bytes(b);
    let r = x == y;
    proof {
        broadcast use vstd::slice::group_slice_axioms;

        if r {
            assert(x =~= y);
            lemma_utf8_inj(a@, b@);
            axiom_str_same(*a, *b);
        }
        if a@ == b@ {
            lemma_slice_same(x, y);
        }
    }
    r
}

// ── A string a writer is handed ─────────────────────────────────────────────

/// A string a writer is handed: borrowed, from a slab or from a client,
/// or owned.
pub enum TextCow<'a> {
    Ref(&'a str),
    Own(String),
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

/// The string of a cow.
#[inline(always)]
fn cow_str<'c, 'a>(c: &'c TextCow<'a>) -> (r: &'c str)
    ensures
        r == cow_val(*c),
{
    match c {
        TextCow::Ref(v) => v,
        TextCow::Own(o) => {
            let r = o.as_str();
            proof {
                // there is a `str` of these characters: `r`, and no other
                assert(r@ == o@);
                axiom_str_same(str_of(o@), r);
            }
            r
        },
    }
}

/// The cow, owned: a borrowed string is copied.
#[inline(always)]
fn cow_own<'a>(c: TextCow<'a>) -> (r: TextCow<'static>)
    ensures
        cow_val(r) == cow_val(c),
{
    match c {
        TextCow::Ref(v) => {
            let o = v.to_owned();
            proof {
                assert(v@ == o@);
                axiom_str_same(str_of(o@), v);
            }
            TextCow::Own(o)
        },
        TextCow::Own(o) => TextCow::Own(o),
    }
}

/// An owned string as a cow: it is moved, not copied.
#[inline(always)]
fn cow_of(o: String) -> (r: TextCow<'static>)
    ensures
        cow_val(r)@ == o@,
{
    // there is a `str` of these characters: the string's own
    let _v = o.as_str();
    assert(_v@ == o@);
    assert(str_of(o@)@ == o@);
    TextCow::Own(o)
}

// ── Strings to the RLE layer ────────────────────────────────────────────────

/// Strings under codec `C`, none of them null: what they are on the
/// wire.
#[verifier::reject_recursive_types(C)]
pub struct VTextOps<C>(PhantomData<C>);

/// Strings under codec `C`, none of them null: how they are written.
#[verifier::reject_recursive_types(C)]
pub struct VTextWrite<C>(PhantomData<C>);

impl<'a, C: VCodec> RleOps<'a> for VTextOps<C> {
    type V = &'a str;

    open spec fn nullable() -> bool {
        false
    }

    /// A string of more bytes than a `usize` counts, which no column
    /// holds (see the module's documentation). There is none in a
    /// program.
    open spec fn is_null(v: &'a str) -> bool {
        v.spec_bytes().len() > usize::MAX
    }

    open spec fn enc(v: &'a str) -> Seq<u8> {
        enc_bytes::<C>(v.spec_bytes())
    }

    open spec fn null() -> &'a str {
        arbitrary()
    }

    open spec fn enc_signed(n: i64) -> Seq<u8> {
        C::enc_i(n)
    }

    open spec fn enc_unsigned(n: u64) -> Seq<u8> {
        C::enc_u(n)
    }

    proof fn axiom_enc(v: &'a str, w: &'a str, s: Seq<u8>, t: Seq<u8>) {
        C::lemma_u(v.spec_bytes().len() as u64, 0, Seq::empty(), Seq::empty());
        if !Self::is_null(v) && !Self::is_null(w) {
            lemma_enc_bytes::<C>(v.spec_bytes(), w.spec_bytes(), s, t);
            if v.spec_bytes() == w.spec_bytes() {
                lemma_utf8_inj(v@, w@);
                axiom_str_same(v, w);
            }
        }
    }

    proof fn axiom_null() {
    }

    proof fn axiom_enc_signed(a: i64, b: i64, s: Seq<u8>, t: Seq<u8>) {
        C::lemma_i(a, b, s, t);
    }

    proof fn axiom_enc_unsigned(a: u64, b: u64, s: Seq<u8>, t: Seq<u8>) {
        C::lemma_u(a, b, s, t);
    }

    #[inline(always)]
    fn nullable_exec() -> bool {
        false
    }

    #[inline(always)]
    fn same(a: &&'a str, b: &&'a str) -> bool {
        same_text(a, b)
    }

    #[inline(always)]
    fn try_unpack(data: &'a [u8]) -> (r: Result<(usize, &'a str), PackError>) {
        let r = try_read_text::<C>(data);
        proof {
            if r is Err {
                assert forall|v: &'a str| !Self::is_null(v) implies !Self::enc(v).is_prefix_of(data@) by {
                    assert(!enc_bytes::<C>(encode_utf8(v@)).is_prefix_of(data@));
                }
            }
        }
        r
    }

    #[inline(always)]
    fn unpack(data: &'a [u8]) -> (usize, &'a str) {
        proof {
            let v = choose|v: &'a str| !Self::is_null(v) && Self::enc(v).is_prefix_of(data@);
            assert(enc_bytes::<C>(encode_utf8(v@)).is_prefix_of(data@));
        }
        read_text::<C>(data)
    }

    #[inline(always)]
    fn value_len(data: &[u8]) -> (r: Option<usize>) {
        let r = bytes_len::<C>(data);
        proof {
            assert forall|v: &'a str| !Self::is_null(v) && Self::enc(v).is_prefix_of(data@) implies r == Some(
                Self::enc(v).len() as usize,
            ) by {
                assert(enc_bytes::<C>(v.spec_bytes()).is_prefix_of(data@));
            }
        }
        r
    }

    #[inline(always)]
    fn get_null() -> &'a str {
        // no column of these has a null
        vstd::pervasive::unreached()
    }

    #[inline(always)]
    fn read_signed_opt(data: &[u8]) -> Option<(usize, i64)> {
        C::read_i_opt(data)
    }

    #[inline(always)]
    fn read_unsigned_opt(data: &[u8]) -> Option<(usize, u64)> {
        C::read_u_opt(data)
    }

    #[inline(always)]
    fn read_signed(data: &[u8]) -> Result<(usize, i64), PackError> {
        C::read_i(data)
    }

    #[inline(always)]
    fn read_unsigned(data: &[u8]) -> Result<(usize, u64), PackError> {
        C::read_u(data)
    }
}

impl<'a, C: VCodec> WriteOps<'a> for VTextWrite<C> {
    type R = VTextOps<C>;

    type Cow = TextCow<'a>;

    open spec fn val(c: TextCow<'a>) -> &'a str {
        cow_val(c)
    }

    #[inline(always)]
    fn cow_ref(v: &'a str) -> TextCow<'a> {
        TextCow::Ref(v)
    }

    #[inline(always)]
    fn cow_eq(a: &TextCow<'a>, b: &TextCow<'a>) -> bool {
        let x = cow_str(a);
        let y = cow_str(b);
        same_text(&x, &y)
    }

    #[inline(always)]
    fn cow_is_null(c: &TextCow<'a>) -> bool {
        // none is: a string a program has is bytes in a slice, which
        // are no more than a `usize` counts
        let _b = str_bytes(cow_str(c));
        false
    }

    #[inline(always)]
    fn cow_pack(c: &TextCow<'a>, buf: &mut Vec<u8>) -> usize {
        put_bytes::<C>(str_bytes(cow_str(c)), buf)
    }

    #[inline(always)]
    fn put_signed(n: i64, buf: &mut Vec<u8>) {
        C::put_i(n, buf)
    }

    #[inline(always)]
    fn put_unsigned(n: u64, buf: &mut Vec<u8>) {
        C::put_u(n, buf)
    }

    #[inline(always)]
    fn rewrite_lit_header(buf: &mut Vec<u8>, pos: usize, total: usize) -> i64 {
        C::rewrite_header(buf, pos, total)
    }
}

impl<'s, C: VCodec> Lift<'s> for VTextWrite<C> {
    type W = VTextWrite<C>;

    /// The same string: a `str` is its characters, whatever it borrows
    /// from.
    open spec fn lift(v: &'s str) -> &'static str {
        v
    }

    proof fn axiom_lift(v: &'s str) {
    }

    proof fn axiom_lift_codec(n: i64, m: u64) {
    }

    proof fn axiom_unlift(w: &'static str) {
        assert(<VTextWrite<C> as Lift<'s>>::lift(w) == w);
    }

    #[inline(always)]
    fn own(c: TextCow<'s>) -> TextCow<'static> {
        cow_own(c)
    }
}

impl<C: VCodec> Plain for VTextWrite<C> {
    proof fn lemma_plain(v: &'static str) {
    }

    proof fn lemma_heads() {
        lemma_text_sizes::<C>(Seq::empty());
    }

    proof fn lemma_lift_inj<'s>(v: &'s str, w: &'s str) {
    }
}

// ── What a string is to a client ────────────────────────────────────────────

impl seal::GetSeal for &str {

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

impl seal::SpecSeal for String {

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
        lemma_bytes_below(encode_utf8(a), encode_utf8(b), encode_utf8(c));
        if encode_utf8(a) == encode_utf8(b) {
            lemma_utf8_inj(a, b);
        }
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

/// No header and no count of a column of strings is longer than ten
/// bytes, and its values are at most ten bytes each more than they are
/// long.
pub(crate) proof fn lemma_text_sizes<C: VCodec>(vs: Seq<&'static str>)
    ensures
        heads_bound::<VTextOps<C>>(10),
        encs::<VTextOps<C>>(vs).len() <= 10 * vs.len() + total_str_len(<&'static str as VGet>::svs(vs)),
    decreases vs.len(),
{
    assert forall|n: i64| (#[trigger] VTextOps::<C>::enc_signed(n)).len() <= 10 by {
        C::lemma_i(n, n, Seq::empty(), Seq::empty());
    }
    assert forall|n: u64| (#[trigger] VTextOps::<C>::enc_unsigned(n)).len() <= 10 by {
        C::lemma_u(n, n, Seq::empty(), Seq::empty());
    }
    let x = <&'static str as VGet>::svs(vs);
    if vs.len() > 0 {
        lemma_text_sizes::<C>(vs.drop_last());
        // whatever its length, the number before a string is at most
        // ten bytes
        C::lemma_u(vs.last().spec_bytes().len() as u64, 0, Seq::empty(), Seq::empty());
        assert(x.drop_last() =~= <&'static str as VGet>::svs(vs.drop_last()));
        assert(x.last() == vs.last()@);
    }
}

// ── Columns of strings ──────────────────────────────────────────────────────

impl<C: VCodec> seal::Sealed<C> for String {
    type X = RleReads<VTextOps<C>>;

    type E = RleModel<VTextWrite<C>>;
}

impl<C: VCodec> Owned<C> for String {
    type N = REncoder<'static, VTextWrite<C>>;

    #[inline(always)]
    fn to_owned(g: &str) -> (v: String) {
        g.to_owned()
    }

    #[inline(always)]
    fn owned_in(w: String) -> (x: TextCow<'static>) {
        cow_of(w)
    }

    #[inline(always)]
    fn owned_for<'a>(w: String) -> (x: TextCow<'a>) {
        cow_of(w)
    }
}

impl<C: VCodec> VValue<C> for String {
    /// The strings' own bytes, and for each a length, a header and a
    /// count of ten bytes each.
    open spec fn size_bound(vs: Seq<Seq<char>>) -> nat {
        30 * vs.len() + total_str_len(vs)
    }

    proof fn lemma_size(d: Seq<u8>) {
        <RleModel<VTextWrite<C>> as Model<RleReads<VTextOps<C>>>>::lemma_size(d);
        lemma_text_sizes::<C>(slab_values::<VTextOps<C>>(d));
    }

    proof fn lemma_own<'a>(v: &'a str) {
    }

    #[inline(always)]
    fn same(a: &str, b: &str) -> (r: bool) {
        same_text(&a, &b)
    }

    #[inline(always)]
    fn compare(a: &str, b: &str) -> (r: Ordering) {
        proof {
            lemma_bytes_below(a.spec_bytes(), b.spec_bytes(), a.spec_bytes());
            if a.spec_bytes() == b.spec_bytes() {
                lemma_utf8_inj(a@, b@);
            }
        }
        compare_bytes(str_bytes(a), str_bytes(b))
    }
}


// ── Strings that may be null ────────────────────────────────────────────────

/// `Option`s of strings under codec `C`, `None` the null: what they are
/// on the wire.
#[verifier::reject_recursive_types(C)]
pub struct VOptTextOps<C>(PhantomData<C>);

/// `Option`s of strings under codec `C`, `None` the null: how they are
/// written.
#[verifier::reject_recursive_types(C)]
pub struct VOptTextWrite<C>(PhantomData<C>);

/// The value an `Option` of a [`TextCow`] stands for.
pub open spec fn opt_cow_val<'a>(c: Option<TextCow<'a>>) -> Option<&'a str> {
    match c {
        Some(x) => Some(cow_val(x)),
        None => None,
    }
}

impl<'a, C: VCodec> RleOps<'a> for VOptTextOps<C> {
    type V = Option<&'a str>;

    open spec fn nullable() -> bool {
        true
    }

    /// `None`, the null; and a `Some` of a string of more bytes than a
    /// `usize` counts, which no column holds (see the module's
    /// documentation). There is no such string in a program.
    open spec fn is_null(v: Option<&'a str>) -> bool {
        match v {
            Some(x) => x.spec_bytes().len() > usize::MAX,
            None => true,
        }
    }

    /// A null has no bytes of its own: a column counts its nulls. The
    /// one here is to make every value's bytes some bytes.
    open spec fn enc(v: Option<&'a str>) -> Seq<u8> {
        match v {
            Some(x) => enc_bytes::<C>(x.spec_bytes()),
            None => seq![0u8],
        }
    }

    open spec fn null() -> Option<&'a str> {
        None
    }

    open spec fn enc_signed(n: i64) -> Seq<u8> {
        C::enc_i(n)
    }

    open spec fn enc_unsigned(n: u64) -> Seq<u8> {
        C::enc_u(n)
    }

    proof fn axiom_enc(v: Option<&'a str>, w: Option<&'a str>, s: Seq<u8>, t: Seq<u8>) {
        if let Some(x) = v {
            C::lemma_u(x.spec_bytes().len() as u64, 0, Seq::empty(), Seq::empty());
        }
        if let (Some(x), Some(y)) = (v, w) {
            if !Self::is_null(v) && !Self::is_null(w) {
                lemma_enc_bytes::<C>(x.spec_bytes(), y.spec_bytes(), s, t);
                if x.spec_bytes() == y.spec_bytes() {
                    lemma_utf8_inj(x@, y@);
                    axiom_str_same(x, y);
                }
            }
        }
    }

    proof fn axiom_null() {
    }

    proof fn axiom_enc_signed(a: i64, b: i64, s: Seq<u8>, t: Seq<u8>) {
        C::lemma_i(a, b, s, t);
    }

    proof fn axiom_enc_unsigned(a: u64, b: u64, s: Seq<u8>, t: Seq<u8>) {
        C::lemma_u(a, b, s, t);
    }

    #[inline(always)]
    fn nullable_exec() -> bool {
        true
    }

    #[inline(always)]
    fn same(a: &Option<&'a str>, b: &Option<&'a str>) -> bool {
        match (a, b) {
            (Some(x), Some(y)) => same_text(x, y),
            (None, None) => true,
            _ => false,
        }
    }

    #[inline(always)]
    fn try_unpack(data: &'a [u8]) -> (r: Result<(usize, Option<&'a str>), PackError>) {
        match try_read_text::<C>(data) {
            Ok((n, v)) => Ok((n, Some(v))),
            Err(e) => {
                proof {
                    assert forall|v: Option<&'a str>| !Self::is_null(v) implies !Self::enc(v).is_prefix_of(data@) by {
                        assert(!enc_bytes::<C>(encode_utf8(v->Some_0@)).is_prefix_of(data@));
                    }
                }
                Err(e)
            },
        }
    }

    #[inline(always)]
    fn unpack(data: &'a [u8]) -> (usize, Option<&'a str>) {
        proof {
            let v = choose|v: Option<&'a str>| !Self::is_null(v) && Self::enc(v).is_prefix_of(data@);
            assert(enc_bytes::<C>(encode_utf8(v->Some_0@)).is_prefix_of(data@));
        }
        let (n, v) = read_text::<C>(data);
        (n, Some(v))
    }

    #[inline(always)]
    fn value_len(data: &[u8]) -> (r: Option<usize>) {
        let r = bytes_len::<C>(data);
        proof {
            assert forall|v: Option<&'a str>| !Self::is_null(v) && Self::enc(v).is_prefix_of(data@) implies r == Some(
                Self::enc(v).len() as usize,
            ) by {
                assert(enc_bytes::<C>(v->Some_0.spec_bytes()).is_prefix_of(data@));
            }
        }
        r
    }

    #[inline(always)]
    fn get_null() -> Option<&'a str> {
        None
    }

    #[inline(always)]
    fn read_signed_opt(data: &[u8]) -> Option<(usize, i64)> {
        C::read_i_opt(data)
    }

    #[inline(always)]
    fn read_unsigned_opt(data: &[u8]) -> Option<(usize, u64)> {
        C::read_u_opt(data)
    }

    #[inline(always)]
    fn read_signed(data: &[u8]) -> Result<(usize, i64), PackError> {
        C::read_i(data)
    }

    #[inline(always)]
    fn read_unsigned(data: &[u8]) -> Result<(usize, u64), PackError> {
        C::read_u(data)
    }
}

impl<'a, C: VCodec> WriteOps<'a> for VOptTextWrite<C> {
    type R = VOptTextOps<C>;

    type Cow = Option<TextCow<'a>>;

    open spec fn val(c: Option<TextCow<'a>>) -> Option<&'a str> {
        opt_cow_val(c)
    }

    #[inline(always)]
    #[allow(clippy::manual_map)]  // Verus has no closure to read here
    fn cow_ref(v: Option<&'a str>) -> Option<TextCow<'a>> {
        match v {
            Some(x) => Some(TextCow::Ref(x)),
            None => None,
        }
    }

    #[inline(always)]
    fn cow_eq(a: &Option<TextCow<'a>>, b: &Option<TextCow<'a>>) -> bool {
        match (a, b) {
            (Some(x), Some(y)) => {
                let x = cow_str(x);
                let y = cow_str(y);
                same_text(&x, &y)
            },
            (None, None) => true,
            _ => false,
        }
    }

    #[inline(always)]
    fn cow_is_null(c: &Option<TextCow<'a>>) -> bool {
        match c {
            // no `Some` is: a string a program has is bytes in a slice,
            // which are no more than a `usize` counts
            Some(x) => {
                let _b = str_bytes(cow_str(x));
                false
            },
            None => true,
        }
    }

    #[inline(always)]
    fn cow_pack(c: &Option<TextCow<'a>>, buf: &mut Vec<u8>) -> usize {
        match c {
            Some(v) => put_bytes::<C>(str_bytes(cow_str(v)), buf),
            // a null is not packed: this is not reached
            None => {
                proof {
                    assert(false);
                }
                0
            },
        }
    }

    #[inline(always)]
    fn put_signed(n: i64, buf: &mut Vec<u8>) {
        C::put_i(n, buf)
    }

    #[inline(always)]
    fn put_unsigned(n: u64, buf: &mut Vec<u8>) {
        C::put_u(n, buf)
    }

    #[inline(always)]
    fn rewrite_lit_header(buf: &mut Vec<u8>, pos: usize, total: usize) -> i64 {
        C::rewrite_header(buf, pos, total)
    }
}

impl<'s, C: VCodec> Lift<'s> for VOptTextWrite<C> {
    type W = VOptTextWrite<C>;

    /// The same string, or the null.
    open spec fn lift(v: Option<&'s str>) -> Option<&'static str> {
        v
    }

    proof fn axiom_lift(v: Option<&'s str>) {
    }

    proof fn axiom_lift_codec(n: i64, m: u64) {
    }

    proof fn axiom_unlift(w: Option<&'static str>) {
        assert(<VOptTextWrite<C> as Lift<'s>>::lift(w) == w);
    }

    #[inline(always)]
    #[allow(clippy::manual_map)]  // Verus has no closure to read here
    fn own(c: Option<TextCow<'s>>) -> Option<TextCow<'static>> {
        match c {
            Some(x) => Some(cow_own(x)),
            None => None,
        }
    }
}

impl<C: VCodec> Plain for VOptTextWrite<C> {
    proof fn lemma_plain(v: Option<&'static str>) {
    }

    proof fn lemma_heads() {
        lemma_opt_text_sizes::<C>(Seq::empty());
    }

    proof fn lemma_lift_inj<'s>(v: Option<&'s str>, w: Option<&'s str>) {
    }
}

// ── What a string that may be null is to a client ───────────────────────────

impl seal::GetSeal for Option<&str> {

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
        if let (Some(x), Some(y)) = (a, b) {
            axiom_str_same(x, y);
        }
    }
}

impl seal::SpecSeal for Option<String> {

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
        match (a, b, c) {
            (Some(x), Some(y), Some(z)) => {
                lemma_bytes_below(encode_utf8(x), encode_utf8(y), encode_utf8(z));
                if encode_utf8(x) == encode_utf8(y) {
                    lemma_utf8_inj(x, y);
                }
            },
            (Some(x), Some(y), None) => {
                lemma_bytes_below(encode_utf8(x), encode_utf8(y), encode_utf8(y));
                if encode_utf8(x) == encode_utf8(y) {
                    lemma_utf8_inj(x, y);
                }
            },
            (Some(x), None, _) => lemma_bytes_below(encode_utf8(x), encode_utf8(x), encode_utf8(x)),
            _ => {},
        }
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

/// The same as `lemma_text_sizes`, of a column of strings that may be
/// null (a null counts as one byte here, though it is none in the
/// column).
pub(crate) proof fn lemma_opt_text_sizes<C: VCodec>(vs: Seq<Option<&'static str>>)
    ensures
        heads_bound::<VOptTextOps<C>>(10),
        encs::<VOptTextOps<C>>(vs).len() <= 10 * vs.len() + total_str_len_opt(
            <Option<&'static str> as VGet>::svs(vs),
        ),
    decreases vs.len(),
{
    assert forall|n: i64| (#[trigger] VOptTextOps::<C>::enc_signed(n)).len() <= 10 by {
        C::lemma_i(n, n, Seq::empty(), Seq::empty());
    }
    assert forall|n: u64| (#[trigger] VOptTextOps::<C>::enc_unsigned(n)).len() <= 10 by {
        C::lemma_u(n, n, Seq::empty(), Seq::empty());
    }
    let x = <Option<&'static str> as VGet>::svs(vs);
    if vs.len() > 0 {
        lemma_opt_text_sizes::<C>(vs.drop_last());
        if let Some(b) = vs.last() {
            // whatever its length, the number before a string is at most
            // ten bytes
            C::lemma_u(b.spec_bytes().len() as u64, 0, Seq::empty(), Seq::empty());
        }
        assert(x.drop_last() =~= <Option<&'static str> as VGet>::svs(vs.drop_last()));
        assert(x.last() == opt_chars(vs.last()));
    }
}

// ── Columns of strings that may be null ─────────────────────────────────────

impl<C: VCodec> seal::Sealed<C> for Option<String> {
    type X = RleReads<VOptTextOps<C>>;

    type E = RleModel<VOptTextWrite<C>>;
}

impl<C: VCodec> Owned<C> for Option<String> {
    type N = REncoder<'static, VOptTextWrite<C>>;

    #[inline(always)]
    #[allow(clippy::manual_map)]  // Verus has no closure to read here
    fn to_owned(g: Option<&str>) -> (v: Option<String>) {
        match g {
            Some(s) => Some(s.to_owned()),
            None => None,
        }
    }

    #[inline(always)]
    #[allow(clippy::manual_map)]  // Verus has no closure to read here
    fn owned_in(w: Option<String>) -> (x: Option<TextCow<'static>>) {
        match w {
            Some(s) => Some(cow_of(s)),
            None => None,
        }
    }

    #[inline(always)]
    #[allow(clippy::manual_map)]  // Verus has no closure to read here
    fn owned_for<'a>(w: Option<String>) -> (x: Option<TextCow<'a>>) {
        match w {
            Some(s) => Some(cow_of(s)),
            None => None,
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
        <RleModel<VOptTextWrite<C>> as Model<RleReads<VOptTextOps<C>>>>::lemma_size(d);
        lemma_opt_text_sizes::<C>(slab_values::<VOptTextOps<C>>(d));
    }

    proof fn lemma_own<'a>(v: Option<&'a str>) {
    }

    #[inline(always)]
    fn same(a: Option<&str>, b: Option<&str>) -> (r: bool) {
        match (a, b) {
            (Some(x), Some(y)) => same_text(&x, &y),
            (None, None) => true,
            _ => false,
        }
    }

    #[inline(always)]
    fn compare(a: Option<&str>, b: Option<&str>) -> (r: Ordering) {
        match (a, b) {
            (Some(x), Some(y)) => {
                proof {
                    lemma_bytes_below(x.spec_bytes(), y.spec_bytes(), x.spec_bytes());
                    if x.spec_bytes() == y.spec_bytes() {
                        lemma_utf8_inj(x@, y@);
                    }
                }
                compare_bytes(str_bytes(x), str_bytes(y))
            },
            (None, None) => Ordering::Equal,
            (None, Some(_)) => Ordering::Less,
            (Some(_), None) => Ordering::Greater,
        }
    }
}

} // verus!
