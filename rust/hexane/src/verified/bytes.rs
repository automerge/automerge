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

// Verus proofs read more plainly with `x = x + n`.
#![allow(clippy::len_zero, clippy::assign_op_pattern)]

use super::codec::VCodec;
#[cfg(verus_keep_ghost)]
use super::model::Model;
use super::model::Plain;
use super::{seal, VForm, VGet, VSpec, VValue};
#[cfg(verus_keep_ghost)]
use crate::rle::dec::slab_values;
use crate::rle::dec::RleReads;
use crate::rle::edit::RleModel;
use crate::rle::lift::Lift;
#[cfg(verus_keep_ghost)]
use crate::rle::model::encs;
use crate::rle::model::{RleOps, WriteOps};
#[cfg(verus_keep_ghost)]
use crate::rle::size::heads_bound;
use crate::PackError;
use std::marker::PhantomData;
#[allow(unused_imports)]
use vstd::prelude::*;
#[allow(unused_imports)]
use vstd::slice::*;

verus! {

// ── A byte string on the wire ───────────────────────────────────────────────

/// The bytes of the byte string `b` under codec `C`: its length, then
/// itself.
pub open spec fn enc_bytes<C: VCodec>(b: Seq<u8>) -> Seq<u8> {
    C::enc_u(b.len() as u64) + b
}

/// A byte string is one to ten bytes longer on the wire than it is, and
/// no two byte strings' bytes start the same string.
pub(crate) proof fn lemma_enc_bytes<C: VCodec>(a: Seq<u8>, b: Seq<u8>, s: Seq<u8>, t: Seq<u8>)
    requires
        a.len() <= u64::MAX,
        b.len() <= u64::MAX,
    ensures
        a.len() < enc_bytes::<C>(a).len() <= 10 + a.len(),
        enc_bytes::<C>(a) + s == enc_bytes::<C>(b) + t ==> a == b,
{
    let ha = C::enc_u(a.len() as u64);
    let hb = C::enc_u(b.len() as u64);
    C::lemma_u(a.len() as u64, b.len() as u64, a + s, b + t);
    if enc_bytes::<C>(a) + s == enc_bytes::<C>(b) + t {
        let x = enc_bytes::<C>(a) + s;
        assert(x =~= ha + (a + s));
        assert(x =~= hb + (b + t));
        // the lengths are one length, so the two start at one place
        assert(a.len() == b.len());
        assert(a =~= x.subrange(ha.len() as int, (ha.len() + a.len()) as int));
        assert(b =~= x.subrange(hb.len() as int, (hb.len() + b.len()) as int));
    }
}

/// Where no number starts `d`, no byte string does.
proof fn lemma_no_length<C: VCodec>(d: Seq<u8>)
    requires
        forall|v: u64| !(#[trigger] C::enc_u(v)).is_prefix_of(d),
    ensures
        forall|b: Seq<u8>| !(#[trigger] enc_bytes::<C>(b)).is_prefix_of(d),
{
    assert forall|b: Seq<u8>| !(#[trigger] enc_bytes::<C>(b)).is_prefix_of(d) by {
        let h = C::enc_u(b.len() as u64);
        if enc_bytes::<C>(b).is_prefix_of(d) {
            assert(h =~= d.subrange(0, h.len() as int));
            assert(h.is_prefix_of(d));
        }
    }
}

/// A byte string at the front of `d`, where the number `n` is in `h`
/// bytes: it is the `n` bytes after the number.
proof fn lemma_front_bytes<C: VCodec>(d: Seq<u8>, h: int, n: u64, b: Seq<u8>)
    requires
        0 <= h <= d.len(),
        d.take(h) == C::enc_u(n),
        b.len() <= u64::MAX,
        enc_bytes::<C>(b).is_prefix_of(d),
    ensures
        b.len() == n,
        h + n <= d.len(),
        enc_bytes::<C>(b).len() == h + n,
        b == d.subrange(h, h + n),
{
    let e = enc_bytes::<C>(b);
    let k = C::enc_u(b.len() as u64);
    let r = d.skip(e.len() as int);
    assert(e == d.subrange(0, e.len() as int));
    assert(d =~= e + r);
    assert(e + r =~= k + (b + r));
    assert(d =~= C::enc_u(n) + d.skip(h));
    C::lemma_u(b.len() as u64, n, b + r, d.skip(h));
    assert(k.len() == h);
    assert(b =~= d.subrange(h, h + n));
}

/// The number `n` in `h` bytes at the front of `d` and the `n` bytes
/// after it are those bytes' byte string.
proof fn lemma_bytes_at<C: VCodec>(d: Seq<u8>, h: int, n: u64)
    requires
        0 <= h,
        h + n <= d.len(),
        d.take(h) == C::enc_u(n),
    ensures
        d.take(h + n) == enc_bytes::<C>(d.subrange(h, h + n)),
{
    assert(d.take(h + n) =~= d.take(h) + d.subrange(h, h + n));
}

/// A slice is no longer than a `usize` can count.
proof fn lemma_slice_len(v: &[u8])
    ensures
        v@.len() <= usize::MAX,
{
    broadcast use vstd::slice::group_slice_axioms;

    assert(spec_slice_len(v) == v@.len());
}

/// Two slices of the same bytes are one slice.
proof fn lemma_slice_same(v: &[u8], w: &[u8])
    requires
        v@ == w@,
    ensures
        v == w,
{
    broadcast use vstd::slice::group_slice_axioms;

    assert(v =~= w);
}

/// The error for bytes that do not start with a byte string.
#[verifier::external_body]
#[cold]
fn bad_format() -> PackError {
    PackError::BadFormat
}

/// The byte string at the start of `data` and how many bytes it takes
/// there, or an error where no byte string's bytes start `data`.
#[inline(always)]
fn try_read_bytes<C: VCodec>(data: &[u8]) -> (r: Result<(usize, &[u8]), PackError>)
    ensures
        match r {
            Ok((n, v)) => n <= data.len() && data@.take(n as int) == enc_bytes::<C>(v@),
            Err(_) => forall|b: Seq<u8>|
                b.len() <= usize::MAX ==> !(#[trigger] enc_bytes::<C>(b)).is_prefix_of(data@),
        },
{
    broadcast use vstd::slice::group_slice_axioms;

    let (hdr, len) = match C::read_u(data) {
        Ok(x) => x,
        Err(e) => {
            proof {
                lemma_no_length::<C>(data@);
            }
            return Err(e);
        },
    };
    // a length past `usize::MAX` (on 32-bit targets) is past the data's
    // end too: it is refused, not truncated
    let n = match usize::try_from(len) {
        Ok(n) => n,
        Err(_) => {
            proof {
                assert forall|b: Seq<u8>| b.len() <= usize::MAX implies !(#[trigger] enc_bytes::<C>(b)).is_prefix_of(
                    data@,
                ) by {
                    if enc_bytes::<C>(b).is_prefix_of(data@) {
                        lemma_front_bytes::<C>(data@, hdr as int, len, b);
                    }
                }
            }
            return Err(bad_format());
        },
    };
    let rest = &data[hdr..];
    if rest.len() < n {
        proof {
            assert forall|b: Seq<u8>| b.len() <= usize::MAX implies !(#[trigger] enc_bytes::<C>(b)).is_prefix_of(
                data@,
            ) by {
                if enc_bytes::<C>(b).is_prefix_of(data@) {
                    lemma_front_bytes::<C>(data@, hdr as int, len, b);
                }
            }
        }
        return Err(bad_format());
    }
    let v = &rest[..n];
    proof {
        assert(v@ =~= data@.subrange(hdr as int, hdr + n));
        lemma_bytes_at::<C>(data@, hdr as int, len);
    }
    Ok((hdr + n, v))
}

/// The byte string known to start `data`, and how many bytes it takes
/// there.
#[inline(always)]
fn read_bytes<C: VCodec>(data: &[u8]) -> (r: (usize, &[u8]))
    requires
        exists|b: Seq<u8>| b.len() <= usize::MAX && (#[trigger] enc_bytes::<C>(b)).is_prefix_of(data@),
    ensures
        r.0 <= data.len(),
        data@.take(r.0 as int) == enc_bytes::<C>(r.1@),
{
    broadcast use vstd::slice::group_slice_axioms;

    let ghost b = choose|b: Seq<u8>| b.len() <= usize::MAX && (#[trigger] enc_bytes::<C>(b)).is_prefix_of(data@);
    match C::read_u_opt(data) {
        Some((hdr, len)) => {
            proof {
                lemma_front_bytes::<C>(data@, hdr as int, len, b);
            }
            let n = len as usize;
            let rest = &data[hdr..];
            let v = &rest[..n];
            proof {
                assert(v@ =~= data@.subrange(hdr as int, hdr + n));
                lemma_bytes_at::<C>(data@, hdr as int, len);
            }
            (hdr + n, v)
        },
        // a byte string starts with its length: this is not reached
        None => {
            proof {
                lemma_no_length::<C>(data@);
                assert(false);
            }
            (0, data)
        },
    }
}

/// How many bytes the byte string that starts `data` takes there,
/// without reading it.
#[inline(always)]
fn bytes_len<C: VCodec>(data: &[u8]) -> (r: Option<usize>)
    ensures
        forall|b: Seq<u8>|
            b.len() <= usize::MAX && (#[trigger] enc_bytes::<C>(b)).is_prefix_of(data@) ==> r == Some(
                enc_bytes::<C>(b).len() as usize,
            ),
{
    let (hdr, len) = match C::read_u_opt(data) {
        Some(x) => x,
        None => {
            proof {
                lemma_no_length::<C>(data@);
            }
            return None;
        },
    };
    proof {
        assert forall|b: Seq<u8>|
            b.len() <= usize::MAX && (#[trigger] enc_bytes::<C>(b)).is_prefix_of(data@) implies b.len() == len && hdr
            + len <= data@.len() && enc_bytes::<C>(b).len() == hdr + len by {
            lemma_front_bytes::<C>(data@, hdr as int, len, b);
        }
    }
    let n = match usize::try_from(len) {
        Ok(n) => n,
        Err(_) => {
            return None;
        },
    };
    if data.len() - hdr < n {
        return None;
    }
    Some(hdr + n)
}

/// Append the bytes of the byte string `v`; how many they are.
#[inline(always)]
fn put_bytes<C: VCodec>(v: &[u8], buf: &mut Vec<u8>) -> (n: usize)
    ensures
        final(buf)@ == old(buf)@ + enc_bytes::<C>(v@),
        n == enc_bytes::<C>(v@).len(),
{
    broadcast use vstd::slice::group_slice_axioms;

    let pos = buf.len();
    C::put_u(v.len() as u64, buf);
    buf.extend_from_slice(v);
    proof {
        assert(buf@ =~= old(buf)@ + enc_bytes::<C>(v@));
    }
    buf.len() - pos
}

/// Whether two slices are the same bytes.
#[inline(always)]
fn same_bytes(a: &&[u8], b: &&[u8]) -> (r: bool)
    ensures
        r == (*a == *b),
{
    broadcast use vstd::slice::group_slice_axioms;

    let r = *a == *b;
    proof {
        if r {
            assert(*a =~= *b);
        }
    }
    r
}

// ── A byte string a writer is handed ────────────────────────────────────────

/// A byte string a writer is handed: borrowed, from a slab or from a
/// client, or owned.
pub enum BytesCow<'a> {
    Ref(&'a [u8]),
    Own(Vec<u8>),
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

/// The byte string of a cow.
#[inline(always)]
fn cow_bytes<'c, 'a>(c: &'c BytesCow<'a>) -> (r: &'c [u8])
    ensures
        r == cow_val(*c),
{
    match c {
        BytesCow::Ref(v) => v,
        BytesCow::Own(o) => {
            let r = o.as_slice();
            proof {
                // there is a slice of these bytes: `r`, and no other
                assert(r@ == o@);
                lemma_slice_same(slice_of(o@), r);
            }
            r
        },
    }
}

/// The cow, owned: a borrowed byte string is copied.
#[inline(always)]
fn cow_own<'a>(c: BytesCow<'a>) -> (r: BytesCow<'static>)
    ensures
        cow_val(r) == cow_val(c),
{
    match c {
        BytesCow::Ref(v) => {
            let o = slice_to_vec(v);
            proof {
                assert(v@ == o@);
                lemma_slice_same(slice_of(o@), v);
            }
            BytesCow::Own(o)
        },
        BytesCow::Own(o) => BytesCow::Own(o),
    }
}

// ── Byte strings to the RLE layer ───────────────────────────────────────────

/// Byte strings under codec `C`, none of them null: what they are on the
/// wire.
#[verifier::reject_recursive_types(C)]
pub struct VBytesOps<C>(PhantomData<C>);

/// Byte strings under codec `C`, none of them null: how they are
/// written.
#[verifier::reject_recursive_types(C)]
pub struct VBytesWrite<C>(PhantomData<C>);

impl<'a, C: VCodec> RleOps<'a> for VBytesOps<C> {
    type V = &'a [u8];

    open spec fn nullable() -> bool {
        false
    }

    open spec fn is_null(v: &'a [u8]) -> bool {
        false
    }

    open spec fn enc(v: &'a [u8]) -> Seq<u8> {
        enc_bytes::<C>(v@)
    }

    open spec fn null() -> &'a [u8] {
        arbitrary()
    }

    open spec fn enc_signed(n: i64) -> Seq<u8> {
        C::enc_i(n)
    }

    open spec fn enc_unsigned(n: u64) -> Seq<u8> {
        C::enc_u(n)
    }

    proof fn axiom_enc(v: &'a [u8], w: &'a [u8], s: Seq<u8>, t: Seq<u8>) {
        lemma_slice_len(v);
        lemma_slice_len(w);
        lemma_enc_bytes::<C>(v@, w@, s, t);
        if v@ == w@ {
            lemma_slice_same(v, w);
        }
    }

    proof fn axiom_null(v: &'a [u8]) {
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
    fn same(a: &&'a [u8], b: &&'a [u8]) -> bool {
        same_bytes(a, b)
    }

    #[inline(always)]
    fn try_unpack(data: &'a [u8]) -> (r: Result<(usize, &'a [u8]), PackError>) {
        let r = try_read_bytes::<C>(data);
        proof {
            if r is Err {
                assert forall|v: &'a [u8]| !Self::is_null(v) implies !Self::enc(v).is_prefix_of(data@) by {
                    lemma_slice_len(v);
                    assert(!enc_bytes::<C>(v@).is_prefix_of(data@));
                }
            }
        }
        r
    }

    #[inline(always)]
    fn unpack(data: &'a [u8]) -> (usize, &'a [u8]) {
        proof {
            let v = choose|v: &'a [u8]| !Self::is_null(v) && Self::enc(v).is_prefix_of(data@);
            lemma_slice_len(v);
            assert(enc_bytes::<C>(v@).is_prefix_of(data@));
        }
        read_bytes::<C>(data)
    }

    #[inline(always)]
    fn value_len(data: &[u8]) -> (r: Option<usize>) {
        let r = bytes_len::<C>(data);
        proof {
            assert forall|v: &'a [u8]| !Self::is_null(v) && Self::enc(v).is_prefix_of(data@) implies r == Some(
                Self::enc(v).len() as usize,
            ) by {
                lemma_slice_len(v);
                assert(enc_bytes::<C>(v@).is_prefix_of(data@));
            }
        }
        r
    }

    #[inline(always)]
    fn get_null() -> &'a [u8] {
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

impl<'a, C: VCodec> WriteOps<'a> for VBytesWrite<C> {
    type R = VBytesOps<C>;

    type Cow = BytesCow<'a>;

    open spec fn val(c: BytesCow<'a>) -> &'a [u8] {
        cow_val(c)
    }

    #[inline(always)]
    fn cow_ref(v: &'a [u8]) -> BytesCow<'a> {
        BytesCow::Ref(v)
    }

    #[inline(always)]
    fn cow_eq(a: &BytesCow<'a>, b: &BytesCow<'a>) -> bool {
        let x = cow_bytes(a);
        let y = cow_bytes(b);
        same_bytes(&x, &y)
    }

    #[inline(always)]
    #[allow(unused_variables)]  // none is null
    fn cow_is_null(c: &BytesCow<'a>) -> bool {
        false
    }

    #[inline(always)]
    fn cow_pack(c: &BytesCow<'a>, buf: &mut Vec<u8>) -> usize {
        put_bytes::<C>(cow_bytes(c), buf)
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

impl<'s, C: VCodec> Lift<'s> for VBytesWrite<C> {
    type W = VBytesWrite<C>;

    /// The same bytes: a slice is its bytes, whatever it borrows from.
    open spec fn lift(v: &'s [u8]) -> &'static [u8] {
        v
    }

    proof fn axiom_lift(v: &'s [u8]) {
    }

    proof fn axiom_lift_codec(n: i64, m: u64) {
    }

    proof fn axiom_unlift(w: &'static [u8]) {
        assert(<VBytesWrite<C> as Lift<'s>>::lift(w) == w);
    }

    #[inline(always)]
    fn own(c: BytesCow<'s>) -> BytesCow<'static> {
        cow_own(c)
    }
}

impl<C: VCodec> Plain for VBytesWrite<C> {
    proof fn lemma_plain(v: &'static [u8]) {
    }

    proof fn lemma_heads() {
        lemma_bytes_sizes::<C>(Seq::empty());
    }
}

// ── Byte strings that may be null ───────────────────────────────────────────

/// `Option`s of byte strings under codec `C`, `None` the null: what they
/// are on the wire.
#[verifier::reject_recursive_types(C)]
pub struct VOptBytesOps<C>(PhantomData<C>);

/// `Option`s of byte strings under codec `C`, `None` the null: how they
/// are written.
#[verifier::reject_recursive_types(C)]
pub struct VOptBytesWrite<C>(PhantomData<C>);

/// The value an `Option` of a [`BytesCow`] stands for.
pub open spec fn opt_cow_val<'a>(c: Option<BytesCow<'a>>) -> Option<&'a [u8]> {
    match c {
        Some(x) => Some(cow_val(x)),
        None => None,
    }
}

impl<'a, C: VCodec> RleOps<'a> for VOptBytesOps<C> {
    type V = Option<&'a [u8]>;

    open spec fn nullable() -> bool {
        true
    }

    open spec fn is_null(v: Option<&'a [u8]>) -> bool {
        v is None
    }

    /// A null has no bytes of its own: a column counts its nulls. The
    /// one here is to make every value's bytes some bytes.
    open spec fn enc(v: Option<&'a [u8]>) -> Seq<u8> {
        match v {
            Some(x) => enc_bytes::<C>(x@),
            None => seq![0u8],
        }
    }

    open spec fn null() -> Option<&'a [u8]> {
        None
    }

    open spec fn enc_signed(n: i64) -> Seq<u8> {
        C::enc_i(n)
    }

    open spec fn enc_unsigned(n: u64) -> Seq<u8> {
        C::enc_u(n)
    }

    proof fn axiom_enc(v: Option<&'a [u8]>, w: Option<&'a [u8]>, s: Seq<u8>, t: Seq<u8>) {
        match (v, w) {
            (Some(x), Some(y)) => {
                lemma_slice_len(x);
                lemma_slice_len(y);
                lemma_enc_bytes::<C>(x@, y@, s, t);
                if x@ == y@ {
                    lemma_slice_same(x, y);
                }
            },
            (Some(x), None) => {
                lemma_slice_len(x);
                lemma_enc_bytes::<C>(x@, x@, s, s);
            },
            _ => {},
        }
    }

    proof fn axiom_null(v: Option<&'a [u8]>) {
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
    fn same(a: &Option<&'a [u8]>, b: &Option<&'a [u8]>) -> bool {
        match (a, b) {
            (Some(x), Some(y)) => same_bytes(x, y),
            (None, None) => true,
            _ => false,
        }
    }

    #[inline(always)]
    fn try_unpack(data: &'a [u8]) -> (r: Result<(usize, Option<&'a [u8]>), PackError>) {
        match try_read_bytes::<C>(data) {
            Ok((n, v)) => Ok((n, Some(v))),
            Err(e) => {
                proof {
                    assert forall|v: Option<&'a [u8]>| !Self::is_null(v) implies !Self::enc(v).is_prefix_of(data@) by {
                        lemma_slice_len(v->Some_0);
                        assert(!enc_bytes::<C>(v->Some_0@).is_prefix_of(data@));
                    }
                }
                Err(e)
            },
        }
    }

    #[inline(always)]
    fn unpack(data: &'a [u8]) -> (usize, Option<&'a [u8]>) {
        proof {
            let v = choose|v: Option<&'a [u8]>| !Self::is_null(v) && Self::enc(v).is_prefix_of(data@);
            lemma_slice_len(v->Some_0);
            assert(enc_bytes::<C>(v->Some_0@).is_prefix_of(data@));
        }
        let (n, v) = read_bytes::<C>(data);
        (n, Some(v))
    }

    #[inline(always)]
    fn value_len(data: &[u8]) -> (r: Option<usize>) {
        let r = bytes_len::<C>(data);
        proof {
            assert forall|v: Option<&'a [u8]>| !Self::is_null(v) && Self::enc(v).is_prefix_of(data@) implies r == Some(
                Self::enc(v).len() as usize,
            ) by {
                lemma_slice_len(v->Some_0);
                assert(enc_bytes::<C>(v->Some_0@).is_prefix_of(data@));
            }
        }
        r
    }

    #[inline(always)]
    fn get_null() -> Option<&'a [u8]> {
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

impl<'a, C: VCodec> WriteOps<'a> for VOptBytesWrite<C> {
    type R = VOptBytesOps<C>;

    type Cow = Option<BytesCow<'a>>;

    open spec fn val(c: Option<BytesCow<'a>>) -> Option<&'a [u8]> {
        opt_cow_val(c)
    }

    #[inline(always)]
    #[allow(clippy::manual_map)]  // Verus has no closure to read here
    fn cow_ref(v: Option<&'a [u8]>) -> Option<BytesCow<'a>> {
        match v {
            Some(x) => Some(BytesCow::Ref(x)),
            None => None,
        }
    }

    #[inline(always)]
    fn cow_eq(a: &Option<BytesCow<'a>>, b: &Option<BytesCow<'a>>) -> bool {
        match (a, b) {
            (Some(x), Some(y)) => {
                let x = cow_bytes(x);
                let y = cow_bytes(y);
                same_bytes(&x, &y)
            },
            (None, None) => true,
            _ => false,
        }
    }

    #[inline(always)]
    fn cow_is_null(c: &Option<BytesCow<'a>>) -> bool {
        c.is_none()
    }

    #[inline(always)]
    fn cow_pack(c: &Option<BytesCow<'a>>, buf: &mut Vec<u8>) -> usize {
        match c {
            Some(v) => put_bytes::<C>(cow_bytes(v), buf),
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

impl<'s, C: VCodec> Lift<'s> for VOptBytesWrite<C> {
    type W = VOptBytesWrite<C>;

    /// The same bytes, or the null.
    open spec fn lift(v: Option<&'s [u8]>) -> Option<&'static [u8]> {
        v
    }

    proof fn axiom_lift(v: Option<&'s [u8]>) {
    }

    proof fn axiom_lift_codec(n: i64, m: u64) {
    }

    proof fn axiom_unlift(w: Option<&'static [u8]>) {
        assert(<VOptBytesWrite<C> as Lift<'s>>::lift(w) == w);
    }

    #[inline(always)]
    #[allow(clippy::manual_map)]  // Verus has no closure to read here
    fn own(c: Option<BytesCow<'s>>) -> Option<BytesCow<'static>> {
        match c {
            Some(x) => Some(cow_own(x)),
            None => None,
        }
    }
}

impl<C: VCodec> Plain for VOptBytesWrite<C> {
    proof fn lemma_plain(v: Option<&'static [u8]>) {
    }

    proof fn lemma_heads() {
        lemma_opt_bytes_sizes::<C>(Seq::empty());
    }
}

// ── What a byte string is to a client ───────────────────────────────────────

impl seal::GetSeal for &[u8] {

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

impl seal::GetSeal for Option<&[u8]> {

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
        if let (Some(x), Some(y)) = (a, b) {
            lemma_slice_same(x, y);
        }
    }
}

impl seal::SpecSeal for Vec<u8> {

}

impl VSpec for Vec<u8> {
    type S = Seq<u8>;
}

impl<'a> VForm<'a> for Vec<u8> {
    type Get = &'a [u8];
}

impl seal::SpecSeal for Option<Vec<u8>> {

}

impl VSpec for Option<Vec<u8>> {
    type S = Option<Seq<u8>>;
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

/// No header and no count of a column of byte strings is longer than ten
/// bytes, and its values are at most ten bytes each more than they are
/// long.
pub(crate) proof fn lemma_bytes_sizes<C: VCodec>(vs: Seq<&'static [u8]>)
    ensures
        heads_bound::<VBytesOps<C>>(10),
        encs::<VBytesOps<C>>(vs).len() <= 10 * vs.len() + total_len(<&'static [u8] as VGet>::svs(vs)),
    decreases vs.len(),
{
    assert forall|n: i64| (#[trigger] VBytesOps::<C>::enc_signed(n)).len() <= 10 by {
        C::lemma_i(n, n, Seq::empty(), Seq::empty());
    }
    assert forall|n: u64| (#[trigger] VBytesOps::<C>::enc_unsigned(n)).len() <= 10 by {
        C::lemma_u(n, n, Seq::empty(), Seq::empty());
    }
    let x = <&'static [u8] as VGet>::svs(vs);
    if vs.len() > 0 {
        lemma_bytes_sizes::<C>(vs.drop_last());
        lemma_slice_len(vs.last());
        lemma_enc_bytes::<C>(vs.last()@, vs.last()@, Seq::empty(), Seq::empty());
        assert(x.drop_last() =~= <&'static [u8] as VGet>::svs(vs.drop_last()));
        assert(x.last() == vs.last()@);
    }
}

/// The same, of a column of byte strings that may be null (a null counts
/// as one byte here, though it is none in the column).
pub(crate) proof fn lemma_opt_bytes_sizes<C: VCodec>(vs: Seq<Option<&'static [u8]>>)
    ensures
        heads_bound::<VOptBytesOps<C>>(10),
        encs::<VOptBytesOps<C>>(vs).len() <= 10 * vs.len() + total_len_opt(
            <Option<&'static [u8]> as VGet>::svs(vs),
        ),
    decreases vs.len(),
{
    assert forall|n: i64| (#[trigger] VOptBytesOps::<C>::enc_signed(n)).len() <= 10 by {
        C::lemma_i(n, n, Seq::empty(), Seq::empty());
    }
    assert forall|n: u64| (#[trigger] VOptBytesOps::<C>::enc_unsigned(n)).len() <= 10 by {
        C::lemma_u(n, n, Seq::empty(), Seq::empty());
    }
    let x = <Option<&'static [u8]> as VGet>::svs(vs);
    if vs.len() > 0 {
        lemma_opt_bytes_sizes::<C>(vs.drop_last());
        if let Some(b) = vs.last() {
            lemma_slice_len(b);
            lemma_enc_bytes::<C>(b@, b@, Seq::empty(), Seq::empty());
        }
        assert(x.drop_last() =~= <Option<&'static [u8]> as VGet>::svs(vs.drop_last()));
        assert(x.last() == opt_view(vs.last()));
    }
}

// ── Columns of byte strings ─────────────────────────────────────────────────

impl<C: VCodec> seal::Sealed<C> for Vec<u8> {
    type X = RleReads<VBytesOps<C>>;

    type E = RleModel<VBytesWrite<C>>;
}

impl<C: VCodec> VValue<C> for Vec<u8> {
    /// The byte strings' own bytes, and for each a length, a header and
    /// a count of ten bytes each.
    open spec fn size_bound(vs: Seq<Seq<u8>>) -> nat {
        30 * vs.len() + total_len(vs)
    }

    proof fn lemma_size(d: Seq<u8>) {
        <RleModel<VBytesWrite<C>> as Model<RleReads<VBytesOps<C>>>>::lemma_size(d);
        lemma_bytes_sizes::<C>(slab_values::<VBytesOps<C>>(d));
    }

    proof fn lemma_own<'a>(v: &'a [u8]) {
    }
}

impl<C: VCodec> seal::Sealed<C> for Option<Vec<u8>> {
    type X = RleReads<VOptBytesOps<C>>;

    type E = RleModel<VOptBytesWrite<C>>;
}

impl<C: VCodec> VValue<C> for Option<Vec<u8>> {
    /// The byte strings' own bytes, and for each value a length, a
    /// header and a count of ten bytes each.
    open spec fn size_bound(vs: Seq<Option<Seq<u8>>>) -> nat {
        30 * vs.len() + total_len_opt(vs)
    }

    proof fn lemma_size(d: Seq<u8>) {
        <RleModel<VOptBytesWrite<C>> as Model<RleReads<VOptBytesOps<C>>>>::lemma_size(d);
        lemma_opt_bytes_sizes::<C>(slab_values::<VOptBytesOps<C>>(d));
    }

    proof fn lemma_own<'a>(v: Option<&'a [u8]>) {
    }
}

} // verus!
