//! The implementation of `verified::text` and its proofs: what its
//! specification (`text.rs`) says. Each function named `..._impl` here has
//! the contract of the function there that calls it, word for word; a
//! function named after a trait, a method and a type (`vpack_pack_u64`)
//! is the body of that method in that type's impl of the trait, with the
//! trait's contract said of the type.

#![allow(unused_imports)]

#[cfg(verus_keep_ghost)]
use super::super::bytes::{
    enc_bytes, lemma_bytes_below, lemma_enc_bytes, lemma_slice_len, lemma_slice_same,
};
use super::*;

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
#[cfg(verus_keep_ghost)]
use crate::verified::{saved, saved_values};
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

// ── A string and its bytes ──────────────────────────────────────────────────

/// Characters with the same UTF-8 bytes are the same characters.
pub(super) proof fn lemma_utf8_inj(a: Seq<char>, b: Seq<char>)
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
pub(super) fn str_bytes(s: &str) -> (b: &[u8])
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
pub(super) fn try_read_text<C: Codec>(data: &[u8]) -> (r: Result<(usize, &str), PackError>)
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
pub(super) proof fn lemma_text_at<C: Codec>(d: Seq<u8>, n: int, b: Seq<u8>, s: Seq<char>)
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
pub(super) fn read_text<C: Codec>(data: &[u8]) -> (r: (usize, &str))
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
pub(super) fn same_text(a: &&str, b: &&str) -> (r: bool)
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

/// The string of a cow.
#[inline(always)]
pub(super) fn cow_str<'c, 'a>(c: &'c TextCow<'a>) -> (r: &'c str)
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
pub(super) fn cow_own<'a>(c: TextCow<'a>) -> (r: TextCow<'static>)
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
pub(super) fn cow_of(o: String) -> (r: TextCow<'static>)
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
pub struct TextOps<C>(PhantomData<C>);

/// Strings under codec `C`, none of them null: how they are written.
#[verifier::reject_recursive_types(C)]
pub struct TextWrite<C>(PhantomData<C>);

impl<'a, C: Codec> RleOps<'a> for TextOps<C> {
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

impl<'a, C: Codec> WriteOps<'a> for TextWrite<C> {
    type R = TextOps<C>;

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

impl<'s, C: Codec> Lift<'s> for TextWrite<C> {
    type W = TextWrite<C>;

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
        assert(<TextWrite<C> as Lift<'s>>::lift(w) == w);
    }

    #[inline(always)]
    fn own(c: TextCow<'s>) -> TextCow<'static> {
        cow_own(c)
    }
}

impl<C: Codec> Plain for TextWrite<C> {
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

impl seal::SpecSeal for String {

}

/// No header and no count of a column of strings is longer than ten
/// bytes, and its values are at most ten bytes each more than they are
/// long.
pub(crate) proof fn lemma_text_sizes<C: Codec>(vs: Seq<&'static str>)
    ensures
        heads_bound::<TextOps<C>>(10),
        encs::<TextOps<C>>(vs).len() <= 10 * vs.len() + total_str_len(<&'static str as VGet>::svs(vs)),
    decreases vs.len(),
{
    assert forall|n: i64| (#[trigger] TextOps::<C>::enc_signed(n)).len() <= 10 by {
        C::lemma_i(n, n, Seq::empty(), Seq::empty());
    }
    assert forall|n: u64| (#[trigger] TextOps::<C>::enc_unsigned(n)).len() <= 10 by {
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

impl<C: Codec> seal::Sealed<C> for String {
    type X = RleReads<TextOps<C>>;

    type E = RleModel<TextWrite<C>>;
}

impl<C: Codec> Owned<C> for String {
    type N = REncoder<'static, TextWrite<C>>;

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

// ── Strings that may be null ────────────────────────────────────────────────

/// `Option`s of strings under codec `C`, `None` the null: what they are
/// on the wire.
#[verifier::reject_recursive_types(C)]
pub struct OptTextOps<C>(PhantomData<C>);

/// `Option`s of strings under codec `C`, `None` the null: how they are
/// written.
#[verifier::reject_recursive_types(C)]
pub struct OptTextWrite<C>(PhantomData<C>);

impl<'a, C: Codec> RleOps<'a> for OptTextOps<C> {
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

impl<'a, C: Codec> WriteOps<'a> for OptTextWrite<C> {
    type R = OptTextOps<C>;

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

impl<'s, C: Codec> Lift<'s> for OptTextWrite<C> {
    type W = OptTextWrite<C>;

    /// The same string, or the null.
    open spec fn lift(v: Option<&'s str>) -> Option<&'static str> {
        v
    }

    proof fn axiom_lift(v: Option<&'s str>) {
    }

    proof fn axiom_lift_codec(n: i64, m: u64) {
    }

    proof fn axiom_unlift(w: Option<&'static str>) {
        assert(<OptTextWrite<C> as Lift<'s>>::lift(w) == w);
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

impl<C: Codec> Plain for OptTextWrite<C> {
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

impl seal::SpecSeal for Option<String> {

}

/// The same as `lemma_text_sizes`, of a column of strings that may be
/// null (a null counts as one byte here, though it is none in the
/// column).
pub(crate) proof fn lemma_opt_text_sizes<C: Codec>(vs: Seq<Option<&'static str>>)
    ensures
        heads_bound::<OptTextOps<C>>(10),
        encs::<OptTextOps<C>>(vs).len() <= 10 * vs.len() + total_str_len_opt(
            <Option<&'static str> as VGet>::svs(vs),
        ),
    decreases vs.len(),
{
    assert forall|n: i64| (#[trigger] OptTextOps::<C>::enc_signed(n)).len() <= 10 by {
        C::lemma_i(n, n, Seq::empty(), Seq::empty());
    }
    assert forall|n: u64| (#[trigger] OptTextOps::<C>::enc_unsigned(n)).len() <= 10 by {
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

impl<C: Codec> seal::Sealed<C> for Option<String> {
    type X = RleReads<OptTextOps<C>>;

    type E = RleModel<OptTextWrite<C>>;
}

impl<C: Codec> Owned<C> for Option<String> {
    type N = REncoder<'static, OptTextWrite<C>>;

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

pub(super) proof fn vspec_lemma_below_string(a: Seq<char>, b: Seq<char>, c: Seq<char>)
    ensures
        !<String as VSpec>::below(a, a),
        <String as VSpec>::below(a, b) && <String as VSpec>::below(b, c) ==> <String as VSpec>::below(a, c),
        a == b || <String as VSpec>::below(a, b) || <String as VSpec>::below(b, a),
{
    lemma_bytes_below(encode_utf8(a), encode_utf8(b), encode_utf8(c));
    if encode_utf8(a) == encode_utf8(b) {
        lemma_utf8_inj(a, b);
    }
}

pub(super) proof fn vvalue_lemma_size_string<C: Codec>(d: Seq<u8>)
    requires
        saved::<String, C>(d),
    ensures
        d.len() <= <String as ColumnValueRef<C>>::size_bound(saved_values::<String, C>(d)),
{
    <RleModel<TextWrite<C>> as Model<RleReads<TextOps<C>>>>::lemma_size(d);
    lemma_text_sizes::<C>(slab_values::<TextOps<C>>(d));
}

#[inline(always)]
pub(super) fn vvalue_compare_string(a: &str, b: &str) -> (r: Ordering)
    ensures
        (r is Less) == <String as VSpec>::below(a.sv(), b.sv()),
        (r is Greater) == <String as VSpec>::below(b.sv(), a.sv()),
        (r is Equal) == (a.sv() == b.sv()),
{
    proof {
        lemma_bytes_below(a.spec_bytes(), b.spec_bytes(), a.spec_bytes());
        if a.spec_bytes() == b.spec_bytes() {
            lemma_utf8_inj(a@, b@);
        }
    }
    compare_bytes(str_bytes(a), str_bytes(b))
}

pub(super) proof fn vget_lemma_sv_inj_option_str(a: Option<&str>, b: Option<&str>)
    requires
        a.sv() == b.sv(),
    ensures
        a == b,
{
    if let (Some(x), Some(y)) = (a, b) {
        axiom_str_same(x, y);
    }
}

pub(super) proof fn vspec_lemma_below_option_string(a: Option<Seq<char>>, b: Option<Seq<char>>, c: Option<Seq<char>>)
    ensures
        !<Option<String> as VSpec>::below(a, a),
        <Option<String> as VSpec>::below(a, b) && <Option<String> as VSpec>::below(b, c) ==> <Option<String> as VSpec>::below(a, c),
        a == b || <Option<String> as VSpec>::below(a, b) || <Option<String> as VSpec>::below(b, a),
{
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

pub(super) proof fn vvalue_lemma_size_option_string<C: Codec>(d: Seq<u8>)
    requires
        saved::<Option<String>, C>(d),
    ensures
        d.len() <= <Option<String> as ColumnValueRef<C>>::size_bound(saved_values::<Option<String>, C>(d)),
{
    <RleModel<OptTextWrite<C>> as Model<RleReads<OptTextOps<C>>>>::lemma_size(d);
    lemma_opt_text_sizes::<C>(slab_values::<OptTextOps<C>>(d));
}

#[inline(always)]
pub(super) fn vvalue_same_option_string(a: Option<&str>, b: Option<&str>) -> (r: bool)
    ensures
        r == (a.sv() == b.sv()),
{
    match (a, b) {
        (Some(x), Some(y)) => same_text(&x, &y),
        (None, None) => true,
        _ => false,
    }
}

#[inline(always)]
pub(super) fn vvalue_compare_option_string(a: Option<&str>, b: Option<&str>) -> (r: Ordering)
    ensures
        (r is Less) == <Option<String> as VSpec>::below(a.sv(), b.sv()),
        (r is Greater) == <Option<String> as VSpec>::below(b.sv(), a.sv()),
        (r is Equal) == (a.sv() == b.sv()),
{
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

} // verus!
