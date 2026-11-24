//! The number codecs, verified with Verus.
//!
//! A column writes its numbers (run headers, null counts, and the
//! integers it holds) with a codec. [`Codec`] says what a codec is: what
//! the bytes of a number are, that no two numbers' bytes start the same
//! string, and that the reading and writing functions read and write
//! those bytes.
//! It is *proved* for [`Leb128`](crate::Leb128), from the verified LEB128
//! codec in `leb128_codec`.
//!
//! This file is a specification, to be read as the guide to reviewing
//! says ([Reviewing the specification](crate#reviewing-the-specification)): each
//! function's body is a call of one of `imp` (`codec/imp.rs`), which has
//! its contract word for word and is proved there.

// Verus proofs read more plainly with `x = x + n`.
#![allow(clippy::len_zero, clippy::assign_op_pattern)]

// spec functions and lemmas exist only when Verus compiles the crate

#[cfg(verus_keep_ghost)]
use crate::Leb128;
use crate::PackError;
#[allow(unused_imports)]
use vstd::prelude::*;

use crate::trusted::bad_number;
mod imp;
#[allow(unused_imports)]
use imp::*;

verus! {

/// A codec for the numbers of a [`Column`](super::Column),
/// with what it does specified and proved. The codecs are this crate's
/// ([`Leb128`](crate::Leb128), and `Bijou64` with the `bijou64`
/// feature) and a client's own.
///
/// The proofs about a column rest on what this trait's functions ensure,
/// its laws. Verus checks them for a client it is run on. A client
/// compiled without Verus writes only the functions, and it is then that
/// client's duty that they keep them: that a number is one to ten bytes,
/// and no number's bytes start another's; that a read reads the number
/// whose bytes start `data`, and fails where none do; that a length is
/// that of the number a read would read; and that `rewrite_header`
/// replaces a header with another as it says.
pub trait Codec {
    /// The bytes of an unsigned number.
    spec fn enc_u(n: u64) -> Seq<u8>;

    /// The bytes of a signed number.
    spec fn enc_i(n: i64) -> Seq<u8>;

    /// An unsigned number is one to ten bytes, and no two numbers' bytes
    /// start the same string.
    proof fn lemma_u(a: u64, b: u64, s: Seq<u8>, t: Seq<u8>)
        ensures
            0 < Self::enc_u(a).len() <= 10,
            Self::enc_u(a) + s == Self::enc_u(b) + t ==> a == b,
    ;

    /// The same, for signed numbers.
    proof fn lemma_i(a: i64, b: i64, s: Seq<u8>, t: Seq<u8>)
        ensures
            0 < Self::enc_i(a).len() <= 10,
            Self::enc_i(a) + s == Self::enc_i(b) + t ==> a == b,
    ;

    /// Append the bytes of `n`.
    fn put_u(n: u64, buf: &mut Vec<u8>)
        ensures
            final(buf)@ == old(buf)@ + Self::enc_u(n),
    ;

    /// Append the bytes of `n`.
    fn put_i(n: i64, buf: &mut Vec<u8>)
        ensures
            final(buf)@ == old(buf)@ + Self::enc_i(n),
    ;

    /// The unsigned number at the start of `data` and its length, or an
    /// error where no number's bytes start `data`.
    fn read_u(data: &[u8]) -> (r: Result<(usize, u64), PackError>)
        ensures
            match r {
                Ok((n, v)) => n <= data.len() && data@.take(n as int) == Self::enc_u(v),
                Err(_) => forall|v: u64| !(#[trigger] Self::enc_u(v)).is_prefix_of(data@),
            },
    ;

    /// [`read_u`](Self::read_u), with no word on what is wrong.
    fn read_u_opt(data: &[u8]) -> (r: Option<(usize, u64)>)
        ensures
            match r {
                Some((n, v)) => n <= data.len() && data@.take(n as int) == Self::enc_u(v),
                None => forall|v: u64| !(#[trigger] Self::enc_u(v)).is_prefix_of(data@),
            },
        no_unwind
    ;

    /// The signed number at the start of `data` and its length, or an
    /// error where no number's bytes start `data`.
    fn read_i(data: &[u8]) -> (r: Result<(usize, i64), PackError>)
        ensures
            match r {
                Ok((n, v)) => n <= data.len() && data@.take(n as int) == Self::enc_i(v),
                Err(_) => forall|v: i64| !(#[trigger] Self::enc_i(v)).is_prefix_of(data@),
            },
    ;

    /// [`read_i`](Self::read_i), with no word on what is wrong.
    fn read_i_opt(data: &[u8]) -> (r: Option<(usize, i64)>)
        ensures
            match r {
                Some((n, v)) => n <= data.len() && data@.take(n as int) == Self::enc_i(v),
                None => forall|v: i64| !(#[trigger] Self::enc_i(v)).is_prefix_of(data@),
            },
        no_unwind
    ;

    /// The length of the unsigned number that starts `data`, without
    /// reading it.
    fn len_u(data: &[u8]) -> (r: Option<usize>)
        ensures
            forall|v: u64| (#[trigger] Self::enc_u(v)).is_prefix_of(data@) ==> r == Some(Self::enc_u(v).len() as usize),
        no_unwind
    ;

    /// The length of the signed number that starts `data`, without
    /// reading it.
    fn len_i(data: &[u8]) -> (r: Option<usize>)
        ensures
            forall|v: i64| (#[trigger] Self::enc_i(v)).is_prefix_of(data@) ==> r == Some(Self::enc_i(v).len() as usize),
        no_unwind
    ;

    /// Replace the signed number at `pos`, a literal run's header, with
    /// the header for `total` values; the change in length.
    #[doc(hidden)]
    fn rewrite_header(buf: &mut Vec<u8>, pos: usize, total: usize) -> (d: i64)
        requires
            pos <= old(buf).len(),
            1 <= total <= i64::MAX,
            exists|h: i64| (#[trigger] Self::enc_i(h)).is_prefix_of(old(buf)@.skip(pos as int)),
        ensures
            final(buf).len() == old(buf).len() + d,
            forall|h: i64| (#[trigger] Self::enc_i(h)).is_prefix_of(old(buf)@.skip(pos as int))
                ==> final(buf)@ == old(buf)@.take(pos as int)
                    + Self::enc_i(-(total as int) as i64)
                    + old(buf)@.skip(pos + Self::enc_i(h).len()),
    ;
}

/// The number of bytes of `n` as an unsigned LEB128 number: as many as
/// [`Leb128`](crate::Leb128) writes of it.
#[inline(always)]
pub fn ulebsize(n: u64) -> (r: u64)
    ensures
        r == <Leb128 as Codec>::enc_u(n).len(),
{
    imp::ulebsize_impl(n)
}

/// The number of bytes of `n` as a signed LEB128 number: as many as
/// [`Leb128`](crate::Leb128) writes of it.
#[inline(always)]
pub fn lebsize(n: i64) -> (r: u64)
    ensures
        r == <Leb128 as Codec>::enc_i(n).len(),
{
    imp::lebsize_impl(n)
}

} // verus!
