//! A verified public API for columns.
//!
//! [`VerifiedColumn`] is a column whose every operation carries a
//! specification that [Verus](https://github.com/verus-lang/verus) has
//! checked, stated in terms of the column's values as a sequence
//! (`col@`). A client that is itself checked by Verus can so prove what
//! its own code does with a column: that what it reads is what it wrote,
//! and that a column it saved loads again as the same values and saves
//! back to the same bytes (`client_proof` is such a client).
//!
//! It runs the same verified code as [`Column`](crate::Column): the same
//! slabs, index, cursor and loader. What differs is what stands around
//! that code. `Column` reaches a value type and a codec through its
//! packing traits, and takes its closures and iterators through small
//! adapters; Verus sees neither, and takes on trust that they do what the
//! specifications say. Here the codec (`codec`), the value types (`value`,
//! `counts`) and all that lies between this API and the core are verified
//! too.
//!
//! # Value types
//!
//! A `VerifiedColumn<V>` holds values of a type `V` that is a
//! [`VValue`]:
//!
//! * a number: `u64`, `i64`, `u32`, `i32` or `usize`;
//! * `bool`;
//! * a client's own type that is stored as one of those numbers
//!   ([`VNewtype`]). The client says which number each value is, and a
//!   load refuses bytes that hold a number that is none of its values;
//! * `Option<T>` of a number or a client's type `T`: `None` is a column's
//!   null.
//!
//! The second parameter of `VerifiedColumn<V, C>` is the codec the
//! column's numbers are written with, a [`VCodec`]. It is [`Leb128`]
//! where none is named, which is for now the only one.
//!
//! # What is proved
//!
//! Of every method, its `ensures`, for every input that meets its
//! `requires`, for every value type and codec. In particular:
//!
//! * [`splice`](VerifiedColumn::splice) leaves the values before `at`,
//!   the values given, then the values from `at + del`;
//! * [`get`](VerifiedColumn::get), [`to_vec`](VerifiedColumn::to_vec) and
//!   the [iterator](VerifiedIter) give the column's values;
//! * [`save`](VerifiedColumn::save) writes the one encoding of the
//!   column's values (`encoding`): bytes that are `valid` and hold those
//!   `values`, and no more of them than the values' `size_bound`. Two
//!   columns of the same values so save to the same bytes
//!   (`lemma_save_unique`);
//! * [`load`](VerifiedColumn::load) gives a column only for valid bytes,
//!   and then the column of their values; and it does give one for all
//!   valid bytes short of 4 GiB that hold values a `usize` can count
//!   (`loadable`).
//!
//! `valid`, `values`, `encoding` and `loadable` are specification
//! functions of a column type (`VerifiedColumn::<V>::valid(bytes)`),
//! `size_bound` is one of a value type, and `lemma_save_unique` and
//! `lemma_encoding` are lemmas: they exist only for Verus, and a normal
//! build compiles them away.
//!
//! The specifications say what a saved column is in their own terms.
//! That those are the bytes of [`Column`](crate::Column) for the same
//! values is not proved but tested (`tests/verified_api.rs`): for each
//! value type the two save the same bytes, each loads what the other
//! saved, and they refuse the same bytes with the same error.
//!
//! # What is trusted
//!
//! Verus and its solver Z3, and Verus's specifications of the standard
//! library (`Vec`, slices). Of this crate, the functions Verus is told to
//! take as they are, which are few and small, and are those the verified
//! core of [`Column`](crate::Column) has too:
//!
//! * the helpers that panic, which say why and do nothing else. A method
//!   panics where a column would outgrow what its arithmetic can count
//!   (some `usize::MAX` values, or a slab of 4 GiB), and, like every Rust
//!   function, a method of this API may run out of memory;
//! * the constructors of [`PackError`], of which nothing is assumed
//!   ([`refused`] is the one for a client's type);
//! * two specifications of standard-library calls that Verus has none
//!   for: replacing a range of a `Vec` of bytes (`rle::writer::splice_bytes`)
//!   and of a `Vec` of slabs (`index::splice_vec`);
//! * three helpers of which nothing is assumed: an addition of counts
//!   whose sum the proofs take as unknown (`rle::writer::add`), the reuse
//!   of a buffer (`recycle`), and a constant (`carry_budget`).
//!
//! No value type's encoding is trusted, nor the codec's, and there is no
//! unverified code between this API and the core: every other function on
//! a path through this API is checked.
//!
//! A client's own value type is checked with the client: Verus proves
//! what [`VNewtype`] asks of it when it is run on the client's crate. A
//! client that is compiled without Verus has that to keep by itself, as
//! it has every `requires` of this API.
//!
//! That a [`splice`](VerifiedColumn::splice) returns is not proved: it
//! runs the core's loop over the values to insert, which Verus is not
//! asked to show ends. (It ends when the slice does.) Every other loop
//! is proved to end.

// Verus proofs read more plainly with `x = x + n`.
#![allow(clippy::len_zero, clippy::assign_op_pattern)]

#[cfg(verus_keep_ghost)]
mod client_proof;
pub(crate) mod codec;
pub(crate) mod counts;
pub(crate) mod model;
pub(crate) mod value;

pub use codec::VCodec;
pub use value::{refused, VNewtype, VPack};

use crate::bool_edit::BoolModel;
use crate::bool_wire::BoolReads;
use crate::column::Slab;
use crate::copy::NoRuns;
use crate::cursor::{ReadsAgree, RunFeed, Slabs};
use crate::iter_core::{Decoders, ICore};
use crate::rle::dec::RleReads;
use crate::rle::edit::RleModel;
use crate::{Leb128, PackError};
use counts::VB;
use model::{Core, Model};
use std::marker::PhantomData;
use value::{VOps, VOptOps, VOptWrite, VWrite};
#[allow(unused_imports)]
use vstd::prelude::*;

mod seal {
    use super::model::Model;
    use crate::cursor::Slabs;
    use crate::iter_core::{Decode, Decoders};

    vstd::prelude::verus! {

    /// A codec of the verified public API is one of this crate's: no
    /// other can name this trait.
    pub trait CodecSeal {

    }

    /// A type that packs as one number of a codec is one of this
    /// crate's, or says which of this crate's it is: no other can name
    /// this trait.
    pub trait PackSeal {

    }

    /// What a value type is to the verified core, under codec `C`. A
    /// value type of the verified public API is one of this crate's, or
    /// one that says which of this crate's it is stored as: no other
    /// can name this trait.
    #[allow(private_bounds)]
    pub trait Sealed<C>: Copy + Sized {
        /// How the values are read.
        type X: for <'a>Decoders<'a, Tail = <Self::E as Slabs>::Tail, D: Decode<Item = Self>>;

        /// The slabs that hold them.
        type E: Slabs<V = Self> + Model<Self::X>;
    }

    } // verus!
}

verus! {

/// A slab's segment budget, where none is given: that of
/// [`Column`](crate::Column).
const MAX_SEG: usize = 64;

/// `d` is a saved column of `V` under codec `C`
/// ([`VerifiedColumn::valid`]).
#[doc(hidden)]
pub closed spec fn saved<V: seal::Sealed<C>, C>(d: Seq<u8>) -> bool {
    <V::E as Model<V::X>>::valid(d)
}

/// The values of the saved column `d` ([`VerifiedColumn::values`]).
#[doc(hidden)]
pub closed spec fn saved_values<V: seal::Sealed<C>, C>(d: Seq<u8>) -> Seq<V> {
    <V::E as Model<V::X>>::values(d)
}

/// A value type a [`VerifiedColumn`] can hold, its numbers written with
/// codec `C`.
///
/// These are `u64`, `i64`, `u32`, `i32`, `usize` and every type that
/// says which of them it is stored as ([`VNewtype`]); an `Option` of any
/// of those, whose `None` is a column's null; and `bool`.
pub trait VValue<C: VCodec = Leb128>: seal::Sealed<C> {
    /// No column of the values `vs` saves to more bytes than this.
    spec fn size_bound(vs: Seq<Self>) -> nat;

    /// A saved column is within the bound of its values.
    #[doc(hidden)]
    proof fn lemma_size(d: Seq<u8>)
        requires
            saved::<Self, C>(d),
        ensures
            d.len() <= Self::size_bound(saved_values::<Self, C>(d)),
    ;
}

impl<T: VPack, C: VCodec> seal::Sealed<C> for T {
    type X = RleReads<VOps<T, C>>;

    type E = RleModel<VWrite<T, C>>;
}

impl<T: VPack, C: VCodec> VValue<C> for T {
    /// Ten bytes a value, and a header and a count of ten bytes each.
    open spec fn size_bound(vs: Seq<T>) -> nat {
        30 * vs.len()
    }

    proof fn lemma_size(d: Seq<u8>) {
        <RleModel<VWrite<T, C>> as Model<RleReads<VOps<T, C>>>>::lemma_size(d);
    }
}

impl<T: VPack, C: VCodec> seal::Sealed<C> for Option<T> {
    type X = RleReads<VOptOps<T, C>>;

    type E = RleModel<VOptWrite<T, C>>;
}

impl<T: VPack, C: VCodec> VValue<C> for Option<T> {
    /// Ten bytes a value, and a header and a count of ten bytes each.
    open spec fn size_bound(vs: Seq<Option<T>>) -> nat {
        30 * vs.len()
    }

    proof fn lemma_size(d: Seq<u8>) {
        <RleModel<VOptWrite<T, C>> as Model<RleReads<VOptOps<T, C>>>>::lemma_size(d);
    }
}

impl<C: VCodec> seal::Sealed<C> for bool {
    type X = BoolReads<VB<C>>;

    type E = BoolModel<VB<C>>;
}

impl<C: VCodec> VValue<C> for bool {
    /// Ten bytes a run, which is at least one value but for the first.
    open spec fn size_bound(vs: Seq<bool>) -> nat {
        10 * vs.len() + 10
    }

    proof fn lemma_size(d: Seq<u8>) {
        <BoolModel<VB<C>> as Model<BoolReads<VB<C>>>>::lemma_size(d);
    }
}

// ── What a splice is given ──────────────────────────────────────────────────

/// The values a splice inserts, from a slice: each a run of one.
#[verifier::reject_recursive_types(C)]
struct SliceFeed<'a, V: VValue<C>, C: VCodec> {
    values: &'a [V],
    /// how many have been given
    pos: usize,
    _c: PhantomData<C>,
}

impl<'a, V: VValue<C>, C: VCodec> RunFeed<V::E> for SliceFeed<'a, V, C> {
    type X = <V::E as ReadsAgree<'a, V::X>>::Ins;

    /// The values to give: a feed keeps its slice.
    type K = Seq<V>;

    closed spec fn inv(&self) -> bool {
        self.pos <= self.values@.len()
    }

    closed spec fn key(&self) -> Seq<V> {
        self.values@
    }

    closed spec fn given(&self) -> Seq<V> {
        self.values@.take(self.pos as int)
    }

    closed spec fn ended(&self) -> bool {
        self.pos == self.values@.len()
    }

    #[inline(always)]
    fn next_run(&mut self) -> (r: Option<(<V::E as ReadsAgree<'a, V::X>>::Ins, usize)>) {
        if self.pos < self.values.len() {
            let v = self.values[self.pos];
            proof {
                <V::E as Model<V::X>>::lemma_own(v);
                assert(self.values@.take(self.pos + 1) =~= self.values@.take(self.pos as int) + Seq::new(
                    1,
                    |_i: int| v,
                ));
            }
            self.pos = self.pos + 1;
            Some((<V::E as ReadsAgree<'a, V::X>>::ins(v), 1))
        } else {
            None
        }
    }
}

// ── The column ──────────────────────────────────────────────────────────────

/// A column of values of type `V`: the sequence `self@`. Its numbers are
/// written with the codec `C`.
pub struct VerifiedColumn<V: VValue<C>, C: VCodec = Leb128> {
    core: Core<V::E>,
}

impl<V: VValue<C>, C: VCodec> View for VerifiedColumn<V, C> {
    type V = Seq<V>;

    closed spec fn view(&self) -> Seq<V> {
        self.core@
    }
}

impl<V: VValue<C>, C: VCodec> Default for VerifiedColumn<V, C> {
    /// An empty column.
    fn default() -> (r: Self)
        ensures
            r@ == Seq::<V>::empty(),
    {
        Self::new()
    }
}

// The saved form
impl<V: VValue<C>, C: VCodec> VerifiedColumn<V, C> {
    /// `d` is a saved column: the canonical encoding of some values.
    pub closed spec fn valid(d: Seq<u8>) -> bool {
        saved::<V, C>(d)
    }

    /// The values of the saved column `d`.
    pub closed spec fn values(d: Seq<u8>) -> Seq<V> {
        saved_values::<V, C>(d)
    }

    /// The bytes a column of the values `vs` saves to.
    pub closed spec fn encoding(vs: Seq<V>) -> Seq<u8> {
        choose|d: Seq<u8>| Self::valid(d) && Self::values(d) == vs
    }

    /// A saved column that [`load`](VerifiedColumn::load) is sure to
    /// take: its bytes are short of 4 GiB by a header's length, and a
    /// `usize` can count its values with three to spare.
    pub open spec fn loadable(d: Seq<u8>) -> bool {
        &&& Self::valid(d)
        &&& d.len() <= 0xffff_fff5
        &&& Self::values(d).len() <= usize::MAX - 3
    }

    /// Two saved columns of the same values are the same bytes.
    pub proof fn lemma_save_unique(x: Seq<u8>, y: Seq<u8>)
        requires
            Self::valid(x),
            Self::valid(y),
            Self::values(x) == Self::values(y),
        ensures
            x == y,
    {
        <V::E as Model<V::X>>::lemma_unique(x, y);
    }

    /// A saved column is the encoding of its values.
    pub proof fn lemma_encoding(d: Seq<u8>)
        requires
            Self::valid(d),
        ensures
            Self::encoding(Self::values(d)) == d,
    {
        let e = Self::encoding(Self::values(d));
        assert(Self::valid(e) && Self::values(e) == Self::values(d));
        Self::lemma_save_unique(e, d);
    }
}

impl<V: VValue<C>, C: VCodec> VerifiedColumn<V, C> {
    /// An empty column.
    pub fn new() -> (r: Self)
        ensures
            r@ == Seq::<V>::empty(),
    {
        Self::with_max_segments(MAX_SEG)
    }

    /// An empty column whose slabs hold at most `max_segments` runs
    /// each.
    pub fn with_max_segments(max_segments: usize) -> (r: Self)
        requires
            max_segments >= 2,
        ensures
            r@ == Seq::<V>::empty(),
    {
        let core = Core::<V::E>::with_budget(max_segments);
        proof {
            assert(core@ =~= Seq::empty());
        }
        VerifiedColumn { core }
    }

    /// The number of values.
    #[inline(always)]
    pub fn len(&self) -> (r: usize)
        ensures
            r == self@.len(),
    {
        self.core.len()
    }

    /// Whether the column holds no value.
    #[inline(always)]
    pub fn is_empty(&self) -> (r: bool)
        ensures
            r == (self@.len() == 0),
    {
        self.core.len() == 0
    }

    /// Delete `del` values at `at` and insert `values` there.
    pub fn splice(&mut self, at: usize, del: usize, values: &[V])
        requires
            at <= old(self)@.len(),
            del <= old(self)@.len() - at,
        ensures
            final(self)@ == old(self)@.take(at as int) + values@ + old(self)@.skip(at + del),
    {
        let mut f = <V::E as Model<V::X>>::blind();
        let mut feed = SliceFeed::<V, C> { values, pos: 0, _c: PhantomData };
        let _ins = self.core.splice(at, del, &mut f, &mut feed);
        proof {
            assert(values@.take(0) =~= Seq::<V>::empty());
            assert(values@.take(values@.len() as int) =~= values@);
            assert(_ins@ =~= values@);
        }
    }

    /// Delete `del` values at `at`.
    fn delete(&mut self, at: usize, del: usize)
        requires
            at <= old(self)@.len(),
            del <= old(self)@.len() - at,
        ensures
            final(self)@ == old(self)@.take(at as int) + old(self)@.skip(at + del),
    {
        let mut f = <V::E as Model<V::X>>::blind();
        let mut feed = NoRuns::<<V::E as ReadsAgree<'static, V::X>>::Ins> { none: None };
        let _ins = self.core.splice(at, del, &mut f, &mut feed);
        proof {
            assert(_ins@ =~= Seq::<V>::empty());
            assert(old(self)@.take(at as int) + _ins@ =~= old(self)@.take(at as int));
        }
    }

    /// Insert `value` before the value at `at`.
    #[inline(always)]
    pub fn insert(&mut self, at: usize, value: V)
        requires
            at <= old(self)@.len(),
        ensures
            final(self)@ == old(self)@.insert(at as int, value),
    {
        // Which of the two it is, is a constant of the column's type: the
        // other is not compiled.
        if <V::E as Model<V::X>>::ONE_APART {
            self.insert_apart(at, value);
        } else {
            let one = [value];
            self.splice(at, 0, one.as_slice());
            proof {
                assert(one@ =~= seq![value]);
                assert(self@ =~= old(self)@.insert(at as int, value));
            }
        }
    }

    /// `insert`, out of line: a splice of one value that deletes none,
    /// compiled for that alone, apart from `splice`.
    fn insert_apart(&mut self, at: usize, value: V)
        requires
            at <= old(self)@.len(),
        ensures
            final(self)@ == old(self)@.insert(at as int, value),
    {
        let one = [value];
        let values = one.as_slice();
        let mut f = <V::E as Model<V::X>>::blind();
        let mut feed = SliceFeed::<V, C> { values, pos: 0, _c: PhantomData };
        let _ins = self.core.splice(at, 0, &mut f, &mut feed);
        proof {
            assert(values@.take(0) =~= Seq::<V>::empty());
            assert(values@.take(values@.len() as int) =~= values@);
            assert(_ins@ =~= values@);
            assert(one@ =~= seq![value]);
            assert(self@ =~= old(self)@.insert(at as int, value));
        }
    }

    /// Add `value` at the end.
    #[inline(always)]
    pub fn push(&mut self, value: V)
        ensures
            final(self)@ == old(self)@.push(value),
    {
        let at = self.core.len();
        self.insert(at, value);
        proof {
            assert(self@ =~= old(self)@.push(value));
        }
    }

    /// Delete the value at `at`.
    #[inline(always)]
    pub fn remove(&mut self, at: usize)
        requires
            at < old(self)@.len(),
        ensures
            final(self)@ == old(self)@.remove(at as int),
    {
        self.delete(at, 1);
        proof {
            assert(self@ =~= old(self)@.remove(at as int));
        }
    }

    /// Keep the first `len` values, where there are more.
    #[inline(always)]
    pub fn truncate(&mut self, len: usize)
        ensures
            final(self)@ == if len < old(self)@.len() {
                old(self)@.take(len as int)
            } else {
                old(self)@
            },
    {
        let n = self.core.len();
        if len < n {
            self.delete(len, n - len);
            proof {
                assert(self@ =~= old(self)@.take(len as int));
            }
        }
    }

    /// A reader of the values `start..end`.
    #[inline(always)]
    fn reader(&self, start: usize, end: usize) -> (it: ICore<<V::X as Decoders<'_>>::D>)
        requires
            start <= end <= self@.len(),
        ensures
            it.wf::<V::X>(self.core.slabs_spec()),
            it.out::<V::X>(self.core.slabs_spec()) == self@.subrange(start as int, end as int),
    {
        let it = self.core.reader::<V::X>(start, end);
        proof {
            let o = it.out::<V::X>(self.core.slabs_spec());
            let s = self@.subrange(start as int, end as int);
            assert(o.len() == s.len());
            assert forall|i: int| 0 <= i < o.len() implies #[trigger] o[i] == s[i] by {
                <V::E as Model<V::X>>::lemma_own(o[i]);
            }
            assert(o =~= s);
        }
        it
    }

    /// The value at `i`, if the column has one there.
    pub fn get(&self, i: usize) -> (r: Option<V>)
        ensures
            i < self@.len() ==> r == Some(self@[i as int]),
            i >= self@.len() ==> r is None,
    {
        if i >= self.core.len() {
            return None;
        }
        // the column's length is short of `usize::MAX`
        self.core.lemma_column();
        let slabs = self.core.slabs();
        let mut it = self.reader(i, i + 1);
        it.next::<V::X>(slabs)
    }

    /// The values.
    pub fn to_vec(&self) -> (r: Vec<V>)
        ensures
            r@ == self@,
    {
        let n = self.core.len();
        let slabs = self.core.slabs();
        let mut it = self.reader(0, n);
        let mut out: Vec<V> = Vec::with_capacity(n);
        proof {
            assert(self@.subrange(0, n as int) =~= self@);
        }
        loop
            invariant
                it.wf::<V::X>(slabs@),
                out@ + it.out::<V::X>(slabs@) == self@,
            ensures
                out@ == self@,
            decreases it.out::<V::X>(slabs@).len(),
        {
            let ghost o = it.out::<V::X>(slabs@);
            match it.next_inline::<V::X>(slabs) {
                Some(v) => {
                    proof {
                        assert(out@.push(v) + o.skip(1) =~= out@ + o);
                    }
                    out.push(v);
                },
                None => {
                    proof {
                        assert(out@ + o =~= out@);
                    }
                    break ;
                },
            }
        }
        out
    }

    /// An iterator over the values.
    #[inline(always)]
    pub fn iter(&self) -> (it: VerifiedIter<'_, V, C>)
        ensures
            it.wf(),
            it.rest() == self@,
    {
        let n = self.core.len();
        proof {
            assert(self@.subrange(0, n as int) =~= self@);
        }
        VerifiedIter { it: self.reader(0, n), slabs: self.core.slabs() }
    }

    /// An iterator over the values `start..end`.
    #[inline(always)]
    pub fn iter_range(&self, start: usize, end: usize) -> (it: VerifiedIter<'_, V, C>)
        requires
            start <= end <= self@.len(),
        ensures
            it.wf(),
            it.rest() == self@.subrange(start as int, end as int),
    {
        VerifiedIter { it: self.reader(start, end), slabs: self.core.slabs() }
    }

    /// The column, saved: the encoding of its values.
    pub fn save(&self) -> (r: Vec<u8>)
        ensures
            Self::valid(r@),
            Self::values(r@) == self@,
            r@ == Self::encoding(self@),
            r@.len() <= V::size_bound(self@),
    {
        self.core.lemma_column();
        let mut out: Vec<u8> = Vec::new();
        <V::E as Model<V::X>>::save(self.core.slabs(), self.core.len(), &mut out);
        proof {
            assert(out@.skip(0) =~= out@);
            Self::lemma_encoding(out@);
            V::lemma_size(out@);
        }
        out
    }

    /// The column saved as `bytes`. An error unless `bytes` is a saved
    /// column; and no error for one that is `loadable`.
    pub fn load(bytes: &[u8]) -> (r: Result<Self, PackError>)
        ensures
            Self::loadable(bytes@) ==> r is Ok,
            r matches Ok(c) ==> Self::valid(bytes@) && c@ == Self::values(bytes@),
    {
        match <V::E as Model<V::X>>::load(bytes, MAX_SEG) {
            Ok(core) => Ok(VerifiedColumn { core }),
            Err(e) => Err(e),
        }
    }
}

// ── The iterator ────────────────────────────────────────────────────────────

/// An iterator over a column's values.
pub struct VerifiedIter<'a, V: VValue<C>, C: VCodec = Leb128> {
    it: ICore<<V::X as Decoders<'a>>::D>,
    slabs: &'a [Slab<<V::E as Slabs>::Tail>],
}

impl<'a, V: VValue<C>, C: VCodec> VerifiedIter<'a, V, C> {
    /// The values the iterator has left to give.
    pub closed spec fn rest(&self) -> Seq<V> {
        self.it.out::<V::X>(self.slabs@)
    }

    /// The iterator stands in its column: as every iterator a column
    /// gives does, and keeps doing.
    pub closed spec fn wf(&self) -> bool {
        self.it.wf::<V::X>(self.slabs@)
    }

    /// The next value, if one is left.
    #[inline(always)]
    #[allow(clippy::should_implement_trait)]  // `Iterator::next` has no `requires`
    pub fn next(&mut self) -> (r: Option<V>)
        requires
            old(self).wf(),
        ensures
            final(self).wf(),
            match r {
                Some(v) => {
                    &&& old(self).rest().len() > 0
                    &&& v == old(self).rest()[0]
                    &&& final(self).rest() == old(self).rest().skip(1)
                },
                None => old(self).rest().len() == 0 && final(self).rest().len() == 0,
            },
    {
        self.it.next_inline::<V::X>(self.slabs)
    }
}

} // verus!

// one budget for both APIs
const _: () = assert!(MAX_SEG == crate::column::DEFAULT_MAX_SEG);
