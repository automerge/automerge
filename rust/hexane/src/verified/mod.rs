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
//! that code. `Column` reaches a value type through its packing traits,
//! and takes its closures and iterators through small adapters; Verus
//! sees neither, and takes on trust that they do what the specifications
//! say. Here the value types are verified too (`leb`), and so is all that
//! lies between this API and the core.
//!
//! # What is proved
//!
//! Of every method, its `ensures`, for every input that meets its
//! `requires`. In particular:
//!
//! * [`splice`](VerifiedColumn::splice) leaves the values before `at`,
//!   the values given, then the values from `at + del`;
//! * [`get`](VerifiedColumn::get), [`to_vec`](VerifiedColumn::to_vec) and
//!   the [iterator](VerifiedIter) give the column's values;
//! * [`save`](VerifiedColumn::save) writes the one encoding of the
//!   column's values (`encoding`): bytes that are `valid` and hold those
//!   `values`. Two columns of the same values so save to the same bytes
//!   (`lemma_save_unique`);
//! * [`load`](VerifiedColumn::load) gives a column only for valid bytes,
//!   and then the column of their values; and it does give one for all
//!   valid bytes short of 4 GiB that hold values a `usize` can count
//!   (`loadable`).
//!
//! `valid`, `values`, `encoding` and `loadable` are specification
//! functions, and `lemma_save_unique` is a lemma: they exist only for
//! Verus, and a normal build compiles them away.
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
//! * the constructors of [`PackError`], of which nothing is assumed;
//! * two specifications of standard-library calls that Verus has none
//!   for: replacing a range of a `Vec` of bytes (`rle::writer::splice_bytes`)
//!   and of a `Vec` of slabs (`index::splice_vec`);
//! * three helpers of which nothing is assumed: an addition of counts
//!   whose sum the proofs take as unknown (`rle::writer::add`), the reuse
//!   of a buffer (`recycle`), and a constant (`carry_budget`).
//!
//! No value type's encoding is trusted, and there is no unverified code
//! between this API and the core: every other function on a path through
//! this API is checked.
//!
//! That a [`splice`](VerifiedColumn::splice) returns is not proved: it
//! runs the core's loop over the values to insert, which Verus is not
//! asked to show ends. (It ends when the slice does.) Every other loop
//! is proved to end.

// Verus proofs read more plainly with `x = x + n`.
#![allow(clippy::len_zero, clippy::assign_op_pattern)]

#[cfg(verus_keep_ghost)]
mod client_proof;
pub(crate) mod leb;

use crate::btree::SlabBTree;
use crate::column::Slab;
use crate::column_core::ColumnCore;
use crate::copy::NoRuns;
#[cfg(verus_keep_ghost)]
use crate::cursor::Slabs;
use crate::cursor::{RunFeed, Visitor};
use crate::iter_core::ICore;
use crate::load_core::LoadCore;
use crate::rle::cut::Load;
#[cfg(verus_keep_ghost)]
use crate::rle::cut::{head_bound, small};
#[cfg(verus_keep_ghost)]
use crate::rle::dec::{slab_values, valid_slab};
use crate::rle::dec::{RDecoder, RleReads};
use crate::rle::edit::{discard, pass, save_slabs, ReadIns, RleModel, RleRebuild, Visit};
use crate::rle::lift::Lift;
#[cfg(verus_keep_ghost)]
use crate::rle::model::encs;
#[cfg(verus_keep_ghost)]
use crate::rle::size::{heads_bound, lemma_slab_size};
#[cfg(verus_keep_ghost)]
use crate::rle::state::copies;
use crate::rle::state::VOf;
#[cfg(verus_keep_ghost)]
use crate::rle::unique::lemma_slab_unique;
use crate::rle::RleTail;
use crate::weights::LenModel;
use crate::PackError;
#[allow(unused_imports)]
use vstd::prelude::*;

mod seal {
    use crate::rle::lift::Lift;
    use crate::rle::model::{RleOps, WriteOps};

    vstd::prelude::verus! {

    /// What a value type is to the verified core. A value type of the
    /// verified public API is one of this crate's: no other can name
    /// this trait.
    pub trait Sealed: Copy + Sized {
        /// How the values are written.
        type L: for<'s> Lift<'s, W = Self::L> + for<'s> WriteOps<'s, R = Self::O>;

        /// What the values are on the wire.
        type O: for<'b> RleOps<'b, V = Self>;
    }

    } // verus!
}

verus! {

/// A slab's segment budget, where none is given: that of
/// [`Column`](crate::Column).
const MAX_SEG: usize = 64;

/// No header and no count of a column of `V` is longer than ten bytes.
#[doc(hidden)]
pub closed spec fn heads_fit<V: seal::Sealed>() -> bool {
    heads_bound::<V::O>(10)
}

/// The length of the encodings of the values `vs`, one after another.
#[doc(hidden)]
pub closed spec fn encs_len<V: seal::Sealed>(vs: Seq<V>) -> nat {
    encs::<V::O>(vs).len()
}

/// A value type a [`VerifiedColumn`] can hold.
pub trait VValue: seal::Sealed {
    /// No column of the values `vs` saves to more bytes than this.
    spec fn size_bound(vs: Seq<Self>) -> nat;

    /// A value read is the value written.
    #[doc(hidden)]
    proof fn lemma_own(v: Self)
        ensures
            <Self::L as Lift<'static>>::lift(v) == v,
    ;

    /// No header and no count is longer than ten bytes.
    #[doc(hidden)]
    proof fn lemma_heads()
        ensures
            heads_fit::<Self>(),
    ;

    /// The values' encodings, and twenty bytes of headers for each, are
    /// within the bound.
    #[doc(hidden)]
    proof fn lemma_size(vs: Seq<Self>)
        ensures
            encs_len::<Self>(vs) + 20 * vs.len() <= Self::size_bound(vs),
    ;
}

impl seal::Sealed for u64 {
    type L = leb::VU64Write;

    type O = leb::VU64Ops;
}

impl VValue for u64 {
    /// Ten bytes a value, and a header and a count of ten bytes each.
    open spec fn size_bound(vs: Seq<u64>) -> nat {
        30 * vs.len()
    }

    proof fn lemma_own(v: u64) {
    }

    proof fn lemma_heads() {
        leb::lemma_u64_heads();
    }

    proof fn lemma_size(vs: Seq<u64>) {
        leb::lemma_u64_encs(vs);
    }
}

// ── The saved form ──────────────────────────────────────────────────────────

/// `d` is a saved column of `V`: the canonical encoding of some values.
pub closed spec fn valid<V: VValue>(d: Seq<u8>) -> bool {
    valid_slab::<V::O>(d)
}

/// The values of the saved column `d`.
pub closed spec fn values<V: VValue>(d: Seq<u8>) -> Seq<V> {
    slab_values::<V::O>(d)
}

/// The bytes a column of the values `vs` saves to.
pub closed spec fn encoding<V: VValue>(vs: Seq<V>) -> Seq<u8> {
    choose|d: Seq<u8>| valid::<V>(d) && values::<V>(d) == vs
}

/// A saved column that [`load`](VerifiedColumn::load) is sure to take:
/// its bytes are short of 4 GiB by a header's length, and a `usize` can
/// count its values with three to spare.
pub open spec fn loadable<V: VValue>(d: Seq<u8>) -> bool {
    &&& valid::<V>(d)
    &&& d.len() <= 0xffff_fff5
    &&& values::<V>(d).len() <= usize::MAX - 3
}

/// Two saved columns of the same values are the same bytes.
pub proof fn lemma_save_unique<V: VValue>(x: Seq<u8>, y: Seq<u8>)
    requires
        valid::<V>(x),
        valid::<V>(y),
        values::<V>(x) == values::<V>(y),
    ensures
        x == y,
{
    lemma_slab_unique::<V::O>(x, y);
}

/// A saved column is the encoding of its values.
pub proof fn lemma_encoding<V: VValue>(d: Seq<u8>)
    requires
        valid::<V>(d),
    ensures
        encoding::<V>(values::<V>(d)) == d,
{
    let e = encoding::<V>(values::<V>(d));
    assert(valid::<V>(e) && values::<V>(e) == values::<V>(d));
    lemma_save_unique::<V>(e, d);
}

// ── What a splice is given ──────────────────────────────────────────────────

/// A walk's visitor that looks at nothing: it only keeps count, for the
/// proof.
struct Blind<T> {
    #[allow(dead_code)]
    seen: Ghost<Seq<T>>,
}

impl<T> Visit<T> for Blind<T> {
    closed spec fn seen(&self) -> Seq<T> {
        self.seen@
    }

    #[inline(always)]
    #[allow(unused_variables)]  // `v` and `n` are for the spec
    fn visit(&mut self, v: T, n: usize) {
        *self = Blind { seen: Ghost(self.seen@ + copies(n as nat, v)) };
    }
}

/// A cursor's visitor that looks at nothing: a splice here has no use
/// for the values it moves over or deletes.
#[verifier::reject_recursive_types(L)]
struct NoVisit<L: for<'t> Lift<'t, W = L>> {
    #[allow(dead_code)]
    seen: Ghost<Seq<VOf<'static, L>>>,
}

impl<L: for<'t> Lift<'t, W = L>> Visitor<RleModel<L>> for NoVisit<L> {
    closed spec fn seen(&self) -> Seq<VOf<'static, L>> {
        self.seen@
    }

    #[inline(always)]
    fn pass<'s>(&mut self, e: &mut RleRebuild<L>, s: &'s Slab<RleTail>, n: usize, runs: usize) -> (left: usize) {
        let ghost a = RleModel::<L>::rb_read(*e) as int;
        let mut d = Blind::<VOf<'s, L>> { seen: Ghost(Seq::empty()) };
        let left = pass::<L, Blind<VOf<'s, L>>>(e, s, n, runs, &mut d);
        *self = NoVisit { seen: Ghost(self.seen@ + RleModel::<L>::vals(*s).subrange(a, a + (n - left))) };
        left
    }

    #[inline(always)]
    fn discard<'s>(&mut self, e: &mut RleRebuild<L>, s: &'s Slab<RleTail>, n: usize) {
        let ghost a = RleModel::<L>::rb_read(*e) as int;
        let mut d = Blind::<VOf<'s, L>> { seen: Ghost(Seq::empty()) };
        discard::<L, Blind<VOf<'s, L>>>(e, s, n, &mut d);
        *self = NoVisit { seen: Ghost(self.seen@ + RleModel::<L>::vals(*s).subrange(a, a + n)) };
    }

    #[inline(always)]
    #[allow(unused_variables)]  // `s` is for the spec
    fn dropped(&mut self, s: &Slab<RleTail>) {
        *self = NoVisit { seen: Ghost(self.seen@ + RleModel::<L>::vals(*s)) };
    }
}

/// The values a splice inserts, from a slice: each a run of one.
struct SliceFeed<'a, V: VValue> {
    values: &'a [V],
    /// how many have been given
    pos: usize,
}

impl<'a, V: VValue> RunFeed<RleModel<V::L>> for SliceFeed<'a, V> {
    type X = ReadIns<'a, V::L>;

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
    fn next_run(&mut self) -> (r: Option<(ReadIns<'a, V::L>, usize)>) {
        if self.pos < self.values.len() {
            let v = self.values[self.pos];
            proof {
                V::lemma_own(v);
                assert(self.values@.take(self.pos + 1) =~= self.values@.take(self.pos as int) + Seq::new(
                    1,
                    |_i: int| v,
                ));
            }
            self.pos = self.pos + 1;
            Some((ReadIns { v }, 1))
        } else {
            None
        }
    }
}

// ── The column ──────────────────────────────────────────────────────────────

/// A column of values of type `V`: the sequence `self@`.
pub struct VerifiedColumn<V: VValue> {
    core: ColumnCore<RleModel<V::L>, usize, SlabBTree<usize>, LenModel>,
}

impl<V: VValue> View for VerifiedColumn<V> {
    type V = Seq<V>;

    closed spec fn view(&self) -> Seq<V> {
        self.core@
    }
}

impl<V: VValue> Default for VerifiedColumn<V> {
    /// An empty column.
    fn default() -> (r: Self)
        ensures
            r@ == Seq::<V>::empty(),
    {
        Self::new()
    }
}

/// A load of a column of `V` in progress.
type Loading<'a, V> = LoadCore<
    'a,
    RleModel<<V as seal::Sealed>::L>,
    RleReads<<V as seal::Sealed>::O>,
    usize,
    LenModel,
    Load<'a, <V as seal::Sealed>::L>,
>;

impl<V: VValue> VerifiedColumn<V> {
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
        let core = ColumnCore::new(max_segments);
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
        let mut f = NoVisit::<V::L> { seen: Ghost(Seq::empty()) };
        let mut feed = SliceFeed::<V> { values, pos: 0 };
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
        let mut f = NoVisit::<V::L> { seen: Ghost(Seq::empty()) };
        let mut feed = NoRuns::<ReadIns<'static, V::L>> { none: None };
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
        let one = [value];
        self.splice(at, 0, one.as_slice());
        proof {
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
        let one = [value];
        self.splice(at, 0, one.as_slice());
        proof {
            assert(one@ =~= seq![value]);
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
    fn reader(&self, start: usize, end: usize) -> (it: ICore<RDecoder<'_, V::O>>)
        requires
            start <= end <= self@.len(),
        ensures
            it.wf::<RleReads<V::O>>(self.core.slabs_spec()),
            it.out::<RleReads<V::O>>(self.core.slabs_spec()) == self@.subrange(start as int, end as int),
    {
        let it = self.core.reader::<RleReads<V::O>>(start, end);
        proof {
            let o = it.out::<RleReads<V::O>>(self.core.slabs_spec());
            let s = self@.subrange(start as int, end as int);
            assert(o.len() == s.len());
            assert forall|i: int| 0 <= i < o.len() implies o[i] == s[i] by {
                V::lemma_own(o[i]);
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
        it.next::<RleReads<V::O>>(slabs)
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
                it.wf::<RleReads<V::O>>(slabs@),
                out@ + it.out::<RleReads<V::O>>(slabs@) == self@,
            ensures
                out@ == self@,
            decreases it.out::<RleReads<V::O>>(slabs@).len(),
        {
            let ghost o = it.out::<RleReads<V::O>>(slabs@);
            match it.next_inline::<RleReads<V::O>>(slabs) {
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
    pub fn iter(&self) -> (it: VerifiedIter<'_, V>)
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
    pub fn iter_range(&self, start: usize, end: usize) -> (it: VerifiedIter<'_, V>)
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
            valid::<V>(r@),
            values::<V>(r@) == self@,
            r@ == encoding::<V>(self@),
            r@.len() <= V::size_bound(self@),
    {
        self.core.lemma_column();
        let mut out: Vec<u8> = Vec::new();
        save_slabs::<V::L>(self.core.slabs(), self.core.len(), &mut out);
        proof {
            assert(out@.skip(0) =~= out@);
            lemma_encoding::<V>(out@);
            V::lemma_heads();
            lemma_slab_size::<V::O>(out@, 10);
            V::lemma_size(self@);
        }
        out
    }

    /// The column saved as `bytes`. An error unless `bytes` is a saved
    /// column; and no error for one that is `loadable`.
    pub fn load(bytes: &[u8]) -> (r: Result<Self, PackError>)
        ensures
            loadable::<V>(bytes@) ==> r is Ok,
            r matches Ok(c) ==> valid::<V>(bytes@) && c@ == values::<V>(bytes@),
    {
        let ld: Loading<'_, V> = LoadCore::start(bytes, MAX_SEG);
        match ld.finish::<SlabBTree<usize>>(MAX_SEG, None, i64::MIN, i64::MAX) {
            Ok(core) => {
                proof {
                    let o = slab_values::<V::O>(bytes@);
                    assert(core@.len() == o.len());
                    assert forall|i: int| 0 <= i < o.len() implies core@[i] == o[i] by {
                        V::lemma_own(o[i]);
                    }
                    assert(core@ =~= o);
                }
                Ok(VerifiedColumn { core })
            },
            Err(e) => {
                proof {
                    if loadable::<V>(bytes@) {
                        V::lemma_heads();
                        assert(head_bound::<V::O>(10));
                        assert(small::<V::O>(bytes@));
                    }
                }
                Err(e)
            },
        }
    }
}

// ── The iterator ────────────────────────────────────────────────────────────

/// An iterator over a column's values.
pub struct VerifiedIter<'a, V: VValue> {
    it: ICore<RDecoder<'a, V::O>>,
    slabs: &'a [Slab<RleTail>],
}

impl<'a, V: VValue> VerifiedIter<'a, V> {
    /// The values the iterator has left to give.
    pub closed spec fn rest(&self) -> Seq<V> {
        self.it.out::<RleReads<V::O>>(self.slabs@)
    }

    /// The iterator stands in its column: as every iterator a column
    /// gives does, and keeps doing.
    pub closed spec fn wf(&self) -> bool {
        self.it.wf::<RleReads<V::O>>(self.slabs@)
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
        self.it.next_inline::<RleReads<V::O>>(self.slabs)
    }
}

} // verus!

// one budget for both APIs
const _: () = assert!(MAX_SEG == crate::column::DEFAULT_MAX_SEG);
