//! The RLE wire format as mathematics, for verification with Verus.
//!
//! A slab stands for a sequence of values (`Seq<V>`, nulls included).
//! Its bytes are a sequence of [`Group`]s — repeat runs, literal runs and
//! null runs — each written as a signed header and its payload:
//!
//! ```text
//!   Run(n, v)   : signed(n)  enc(v)            n >= 2
//!   Lit(vs)     : signed(-k) enc(v0) … enc(vk-1)   k = |vs| >= 1
//!   Null(n)     : signed(0)  unsigned(n)       n >= 1
//! ```
//!
//! [`canonical`] picks out the one grouping of each value sequence a
//! writer may produce: no two adjacent literals or null runs, no equal
//! values side by side across a group boundary or inside a literal, and
//! no repeat run shorter than two.  Load accepts exactly the canonical
//! encodings, so a column that loads saves back to the same bytes.
//!
//! The value type and the varint codec come in through [`RleOps`].  Its
//! implementation, [`Ops`], is trusted: it forwards to the column type's
//! [`RleValue`](crate::RleValue) impl and the codec, and its contract is
//! what those must satisfy — encodings are prefix-free and nonempty, and
//! `==` on values is equality.

#![allow(clippy::len_zero)]

use crate::{Codec, PackError, RleValue};
use std::marker::PhantomData;
#[allow(unused_imports)]
use vstd::prelude::*;

verus! {

#[verifier::external_type_specification]
#[verifier::external_body]
#[allow(dead_code)]
pub struct ExPackError(PackError);

#[verifier::external_type_specification]
#[allow(dead_code)]
pub struct ExRun<V>(crate::Run<V>);

/// What the RLE layer needs of a column's value type `V` and its varint
/// codec, for slabs borrowed for `'a`.
pub trait RleOps<'a> {
    type V: Copy;

    /// Whether the column type allows nulls.
    spec fn nullable() -> bool;

    spec fn is_null(v: Self::V) -> bool;

    /// The encoding of a non-null value.
    spec fn enc(v: Self::V) -> Seq<u8>;

    /// The column type's null value (meaningful if it is nullable).
    spec fn null() -> Self::V;

    /// The codec's encoding of a signed integer.
    spec fn enc_signed(n: i64) -> Seq<u8>;

    /// The codec's encoding of an unsigned integer.
    spec fn enc_unsigned(n: u64) -> Seq<u8>;

    /// Value encodings are nonempty, and no two values' encodings start
    /// the same string (so they are injective and prefix-free).
    proof fn axiom_enc(v: Self::V, w: Self::V, s: Seq<u8>, t: Seq<u8>)
        ensures
            Self::enc(v).len() > 0,
            !Self::is_null(v) && !Self::is_null(w) && Self::enc(v) + s == Self::enc(w) + t ==> v == w;

    /// The same, for the codec's signed integers.
    proof fn axiom_enc_signed(a: i64, b: i64, s: Seq<u8>, t: Seq<u8>)
        ensures
            Self::enc_signed(a).len() > 0,
            Self::enc_signed(a) + s == Self::enc_signed(b) + t ==> a == b;

    /// The same, for the codec's unsigned integers.
    proof fn axiom_enc_unsigned(a: u64, b: u64, s: Seq<u8>, t: Seq<u8>)
        ensures
            Self::enc_unsigned(a).len() > 0,
            Self::enc_unsigned(a) + s == Self::enc_unsigned(b) + t ==> a == b;

    fn nullable_exec() -> (r: bool)
        ensures r == Self::nullable();

    /// `a == b`.
    fn same(a: &Self::V, b: &Self::V) -> (r: bool)
        ensures r == (*a == *b);

    /// Decode the non-null value at the start of `data`.
    fn try_unpack(data: &'a [u8]) -> (r: Result<(usize, Self::V), PackError>)
        ensures
            match r {
                Ok((n, v)) => !Self::is_null(v) && n <= data.len() && data@.take(n as int) == Self::enc(v),
                Err(_) => forall|v: Self::V| !Self::is_null(v) ==> !Self::enc(v).is_prefix_of(data@),
            };

    /// Decode the non-null value known to start `data`.
    fn unpack(data: &'a [u8]) -> (r: (usize, Self::V))
        requires exists|v: Self::V| !Self::is_null(v) && Self::enc(v).is_prefix_of(data@),
        ensures !Self::is_null(r.1), r.0 <= data.len(), data@.take(r.0 as int) == Self::enc(r.1);

    /// The length of the non-null value starting `data`, without
    /// decoding it.
    fn value_len(data: &[u8]) -> (r: Option<usize>)
        ensures
            forall|v: Self::V| !Self::is_null(v) && Self::enc(v).is_prefix_of(data@)
                ==> r == Some(Self::enc(v).len() as usize);

    /// The null value.
    fn get_null() -> (r: Self::V)
        requires Self::nullable(),
        ensures r == Self::null(), Self::is_null(r);

    /// Decode the signed integer at the start of `data`, if any.
    fn read_signed_opt(data: &[u8]) -> (r: Option<(usize, i64)>)
        ensures
            match r {
                Some((n, x)) => n <= data.len() && data@.take(n as int) == Self::enc_signed(x),
                None => forall|x: i64| !Self::enc_signed(x).is_prefix_of(data@),
            };

    /// Decode the unsigned integer at the start of `data`, if any.
    fn read_unsigned_opt(data: &[u8]) -> (r: Option<(usize, u64)>)
        ensures
            match r {
                Some((n, x)) => n <= data.len() && data@.take(n as int) == Self::enc_unsigned(x),
                None => forall|x: u64| !Self::enc_unsigned(x).is_prefix_of(data@),
            };

    /// Decode the signed integer at the start of `data`.
    fn read_signed(data: &[u8]) -> (r: Result<(usize, i64), PackError>)
        ensures
            match r {
                Ok((n, x)) => n <= data.len() && data@.take(n as int) == Self::enc_signed(x),
                Err(_) => forall|x: i64| !Self::enc_signed(x).is_prefix_of(data@),
            };

    /// Decode the unsigned integer at the start of `data`.
    fn read_unsigned(data: &[u8]) -> (r: Result<(usize, u64), PackError>)
        ensures
            match r {
                Ok((n, x)) => n <= data.len() && data@.take(n as int) == Self::enc_unsigned(x),
                Err(_) => forall|x: u64| !Self::enc_unsigned(x).is_prefix_of(data@),
            };
}

} // verus!

/// [`RleOps`] for column type `T` with codec `C`: the trusted bridge from
/// the verified RLE code to the value type's and codec's own code.
pub(crate) struct Ops<'a, T, C>(PhantomData<(&'a T, C)>);

// Outside `verus!`, so Verus takes it on trust; its spec items exist only
// when Verus compiles the crate.
impl<'a, T: RleValue, C: Codec> RleOps<'a> for Ops<'a, T, C> {
    type V = <T as crate::ColumnValueRef>::Get<'a>;

    #[cfg(verus_keep_ghost)]
    fn nullable() -> bool {
        unimplemented!()
    }

    #[cfg(verus_keep_ghost)]
    fn is_null(_v: Self::V) -> bool {
        unimplemented!()
    }

    #[cfg(verus_keep_ghost)]
    fn enc(_v: Self::V) -> Seq<u8> {
        unimplemented!()
    }

    #[cfg(verus_keep_ghost)]
    fn null() -> Self::V {
        unimplemented!()
    }

    #[cfg(verus_keep_ghost)]
    fn enc_signed(_n: i64) -> Seq<u8> {
        unimplemented!()
    }

    #[cfg(verus_keep_ghost)]
    fn enc_unsigned(_n: u64) -> Seq<u8> {
        unimplemented!()
    }

    #[cfg(verus_keep_ghost)]
    fn axiom_enc(_v: Self::V, _w: Self::V, _s: Seq<u8>, _t: Seq<u8>) {}

    #[cfg(verus_keep_ghost)]
    fn axiom_enc_signed(_a: i64, _b: i64, _s: Seq<u8>, _t: Seq<u8>) {}

    #[cfg(verus_keep_ghost)]
    fn axiom_enc_unsigned(_a: u64, _b: u64, _s: Seq<u8>, _t: Seq<u8>) {}

    #[inline(always)]
    fn nullable_exec() -> bool {
        T::NULLABLE
    }

    #[inline(always)]
    fn same(a: &Self::V, b: &Self::V) -> bool {
        *a == *b
    }

    #[inline(always)]
    fn try_unpack(data: &'a [u8]) -> Result<(usize, Self::V), PackError> {
        T::try_unpack::<C>(data)
    }

    #[inline(always)]
    fn unpack(data: &'a [u8]) -> (usize, Self::V) {
        T::unpack::<C>(data)
    }

    #[inline(always)]
    fn value_len(data: &[u8]) -> Option<usize> {
        T::value_len::<C>(data)
    }

    #[inline(always)]
    fn get_null() -> Self::V {
        T::get_null()
    }

    #[inline(always)]
    fn read_signed_opt(data: &[u8]) -> Option<(usize, i64)> {
        C::read_signed(data)
    }

    #[inline(always)]
    fn read_unsigned_opt(data: &[u8]) -> Option<(usize, u64)> {
        C::read_unsigned(data)
    }

    #[inline(always)]
    fn read_signed(data: &[u8]) -> Result<(usize, i64), PackError> {
        C::try_read_signed(data)
    }

    #[inline(always)]
    fn read_unsigned(data: &[u8]) -> Result<(usize, u64), PackError> {
        C::try_read_unsigned(data)
    }
}

verus! {

// ── Groups ──────────────────────────────────────────────────────────────────

/// One run of a slab's encoding (for specifications only).
#[allow(dead_code)]
pub enum Group<V> {
    /// `count` copies of a non-null value.
    Run { count: nat, value: V },
    /// Values written out one by one.
    Lit { values: Seq<V> },
    /// `count` nulls.
    Null { count: nat },
}

/// The values a group stands for; `null` is the column type's null.
pub open spec fn group_values<V>(g: Group<V>, null: V) -> Seq<V> {
    match g {
        Group::Run { count, value } => Seq::new(count, |_i| value),
        Group::Lit { values } => values,
        Group::Null { count } => Seq::new(count, |_i| null),
    }
}

/// The number of values a group stands for.
pub open spec fn group_len<V>(g: Group<V>) -> nat {
    match g {
        Group::Run { count, .. } => count,
        Group::Lit { values } => values.len(),
        Group::Null { count } => count,
    }
}

/// The number of values a sequence of groups stands for.
pub open spec fn groups_len<V>(gs: Seq<Group<V>>) -> nat
    decreases gs.len(),
{
    if gs.len() == 0 { 0 } else { groups_len(gs.drop_last()) + group_len(gs.last()) }
}

/// A slab's segment count: one per repeat or null run, one per literal
/// value.
pub open spec fn group_segments<V>(g: Group<V>) -> nat {
    match g {
        Group::Lit { values } => values.len(),
        _ => 1,
    }
}

pub open spec fn groups_segments<V>(gs: Seq<Group<V>>) -> nat
    decreases gs.len(),
{
    if gs.len() == 0 { 0 } else { groups_segments(gs.drop_last()) + group_segments(gs.last()) }
}

/// The values of a sequence of groups.
pub open spec fn groups_values<V>(gs: Seq<Group<V>>, null: V) -> Seq<V>
    decreases gs.len(),
{
    if gs.len() == 0 { Seq::empty() } else { groups_values(gs.drop_last(), null) + group_values(gs.last(), null) }
}

/// The concatenated encodings of `vs`.
pub open spec fn encs<'a, O: RleOps<'a>>(vs: Seq<O::V>) -> Seq<u8>
    decreases vs.len(),
{
    if vs.len() == 0 { Seq::empty() } else { encs::<O>(vs.drop_last()) + O::enc(vs.last()) }
}

/// A group's bytes.
pub open spec fn group_bytes<'a, O: RleOps<'a>>(g: Group<O::V>) -> Seq<u8> {
    match g {
        Group::Run { count, value } => O::enc_signed(count as i64) + O::enc(value),
        Group::Lit { values } => O::enc_signed(-(values.len() as int) as i64) + encs::<O>(values),
        Group::Null { count } => O::enc_signed(0) + O::enc_unsigned(count as u64),
    }
}

/// A slab's bytes.
pub open spec fn groups_bytes<'a, O: RleOps<'a>>(gs: Seq<Group<O::V>>) -> Seq<u8>
    decreases gs.len(),
{
    if gs.len() == 0 { Seq::empty() } else { groups_bytes::<O>(gs.drop_last()) + group_bytes::<O>(gs.last()) }
}

/// A group a writer may emit, on its own: a repeat run of at least two
/// non-null values, a nonempty literal of non-null values with no two
/// equal neighbours, or a nonempty null run in a nullable column — each
/// with a count its header can hold.
pub open spec fn group_ok<'a, O: RleOps<'a>>(g: Group<O::V>) -> bool {
    match g {
        Group::Run { count, value } => 2 <= count <= i64::MAX && !O::is_null(value),
        Group::Lit { values } => {
            &&& 1 <= values.len() <= i64::MAX
            &&& forall|i: int| 0 <= i < values.len() ==> !O::is_null(#[trigger] values[i])
            &&& forall|i: int| 0 < i < values.len() ==> values[i - 1] != #[trigger] values[i]
        },
        Group::Null { count } => 1 <= count <= u64::MAX && O::nullable(),
    }
}

/// `b` may follow `a`: the two could not be one group, or regrouped.
pub open spec fn adjacent_ok<V>(a: Group<V>, b: Group<V>) -> bool {
    match (a, b) {
        (Group::Lit { .. }, Group::Lit { .. }) => false,
        (Group::Null { .. }, Group::Null { .. }) => false,
        (Group::Run { value: x, .. }, Group::Run { value: y, .. }) => x != y,
        (Group::Run { value: x, .. }, Group::Lit { values }) => x != values[0],
        (Group::Lit { values }, Group::Run { value: y, .. }) => values.last() != y,
        _ => true,
    }
}

/// The groups are the canonical encoding of their values.
pub open spec fn canonical<'a, O: RleOps<'a>>(gs: Seq<Group<O::V>>) -> bool {
    &&& forall|i: int| 0 <= i < gs.len() ==> group_ok::<O>(#[trigger] gs[i])
    &&& forall|i: int| 0 < i < gs.len() ==> adjacent_ok(gs[i - 1], #[trigger] gs[i])
}

/// A repeat run's value.
pub open spec fn run_value<V>(g: Group<V>) -> V {
    match g {
        Group::Run { value, .. } => value,
        _ => arbitrary(),
    }
}

/// A literal run's header, for `k` values.
pub open spec fn lit_head<'a, O: RleOps<'a>>(k: nat) -> Seq<u8> {
    O::enc_signed(-(k as int) as i64)
}

/// A group's header value.
pub open spec fn header_of<V>(g: Group<V>) -> int {
    match g {
        Group::Run { count, .. } => count as int,
        Group::Lit { values } => -(values.len() as int),
        Group::Null { .. } => 0,
    }
}

/// A group's bytes after its header.
pub open spec fn payload<'a, O: RleOps<'a>>(g: Group<O::V>) -> Seq<u8> {
    match g {
        Group::Run { value, .. } => O::enc(value),
        Group::Lit { values } => encs::<O>(values),
        Group::Null { count } => O::enc_unsigned(count as u64),
    }
}

/// A canonical encoding of `data` whose groups fit the slab metadata: at
/// most `usize::MAX` values, and no group over `u32::MAX` bytes.
#[verifier::opaque]
pub open spec fn parses<'a, O: RleOps<'a>>(gs: Seq<Group<O::V>>, data: Seq<u8>) -> bool {
    &&& canonical::<O>(gs)
    &&& groups_len(gs) <= usize::MAX
    &&& forall|i: int| 0 <= i < gs.len() ==> (#[trigger] group_bytes::<O>(gs[i])).len() <= u32::MAX
    &&& groups_bytes::<O>(gs) == data
}

// ── Lemmas ──────────────────────────────────────────────────────────────────

pub proof fn lemma_groups_bytes_concat<'a, O: RleOps<'a>>(a: Seq<Group<O::V>>, b: Seq<Group<O::V>>)
    ensures groups_bytes::<O>(a + b) == groups_bytes::<O>(a) + groups_bytes::<O>(b),
    decreases b.len(),
{
    if b.len() == 0 {
        assert(a + b =~= a);
    } else {
        assert((a + b).drop_last() =~= a + b.drop_last());
        lemma_groups_bytes_concat::<O>(a, b.drop_last());
    }
}

pub proof fn lemma_groups_len_concat<V>(a: Seq<Group<V>>, b: Seq<Group<V>>)
    ensures
        groups_len(a + b) == groups_len(a) + groups_len(b),
        groups_segments(a + b) == groups_segments(a) + groups_segments(b),
    decreases b.len(),
{
    if b.len() == 0 {
        assert(a + b =~= a);
    } else {
        assert((a + b).drop_last() =~= a + b.drop_last());
        lemma_groups_len_concat(a, b.drop_last());
    }
}

pub proof fn lemma_encs_concat<'a, O: RleOps<'a>>(a: Seq<O::V>, b: Seq<O::V>)
    ensures encs::<O>(a + b) == encs::<O>(a) + encs::<O>(b),
    decreases b.len(),
{
    if b.len() == 0 {
        assert(a + b =~= a);
    } else {
        assert((a + b).drop_last() =~= a + b.drop_last());
        lemma_encs_concat::<O>(a, b.drop_last());
    }
}

/// The bytes of the groups from `n` on, split off their first.
pub proof fn lemma_groups_bytes_at<'a, O: RleOps<'a>>(gs: Seq<Group<O::V>>, n: int)
    requires 0 <= n < gs.len(),
    ensures
        groups_bytes::<O>(gs) == groups_bytes::<O>(gs.take(n)) + group_bytes::<O>(gs[n])
            + groups_bytes::<O>(gs.skip(n + 1)),
{
    assert(gs =~= gs.take(n) + seq![gs[n]] + gs.skip(n + 1));
    lemma_groups_bytes_concat::<O>(gs.take(n) + seq![gs[n]], gs.skip(n + 1));
    lemma_groups_bytes_concat::<O>(gs.take(n), seq![gs[n]]);
    assert(seq![gs[n]].drop_last() =~= Seq::<Group<O::V>>::empty());
    reveal_with_fuel(groups_bytes, 2);
    assert(groups_bytes::<O>(seq![gs[n]]) =~= group_bytes::<O>(gs[n]));
}

/// The bytes of values from `n` on, split off their first.
pub proof fn lemma_encs_at<'a, O: RleOps<'a>>(vs: Seq<O::V>, n: int)
    requires 0 <= n < vs.len(),
    ensures encs::<O>(vs) == encs::<O>(vs.take(n)) + O::enc(vs[n]) + encs::<O>(vs.skip(n + 1)),
{
    assert(vs =~= vs.take(n) + seq![vs[n]] + vs.skip(n + 1));
    lemma_encs_concat::<O>(vs.take(n) + seq![vs[n]], vs.skip(n + 1));
    lemma_encs_concat::<O>(vs.take(n), seq![vs[n]]);
    assert(seq![vs[n]].drop_last() =~= Seq::<O::V>::empty());
    reveal_with_fuel(encs, 2);
    assert(encs::<O>(seq![vs[n]]) =~= O::enc(vs[n]));
}

/// Two strings that start with encodings of `a` and `b` and are equal
/// start with the same one.
pub proof fn lemma_same_start(x: Seq<u8>, s: Seq<u8>, y: Seq<u8>, t: Seq<u8>, rest: Seq<u8>)
    requires x + s == y + t, x.len() == y.len(),
    ensures x == y, s == t,
{
    assert(x =~= (x + s).take(x.len() as int));
    assert(y =~= (y + t).take(y.len() as int));
    assert(s =~= (x + s).skip(x.len() as int));
    assert(t =~= (y + t).skip(y.len() as int));
}

} // verus!
