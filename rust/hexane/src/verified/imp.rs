//! The implementation of `verified` and its proofs: what its
//! specification (`mod.rs`) says. Each function named `..._impl` here has
//! the contract of the function there that calls it, word for word; a
//! function named after a trait, a method and a type (`vpack_pack_u64`)
//! is the body of that method in that type's impl of the trait, with the
//! trait's contract said of the type.

#![allow(unused_imports)]

use super::*;

use crate::bool_edit::{BoolModel, BoolRecoder};
use crate::bool_wire::BoolReads;
use crate::btree::SlabAggregate;
use crate::column::Slab;
use crate::copy::NoRuns;
#[cfg(verus_keep_ghost)]
use crate::cursor::{lemma_lens_ok, lemma_vals_own, lemma_vals_unit, slab_lens, Weigh};
use crate::cursor::{BoundedFeed, ReadsAgree, RunFeed, Slabs};
#[cfg(verus_keep_ghost)]
use crate::fenwick::{sum, total};
#[cfg(verus_keep_ghost)]
use crate::index::wlens;
use crate::index::IndexOps;
#[cfg(verus_keep_ghost)]
use crate::iter_core::{far_ok, find_ok, found, lemma_from, lens_ok, monotone, vals_of};
use crate::iter_core::{Decode, Decoders, ICore, Probe, Side};
use crate::remap::{Encodes, Mapping};
#[cfg(verus_keep_ghost)]
use crate::rle::dec::slab_values;
use crate::rle::dec::RleReads;
use crate::rle::edit::RleModel;
use crate::rle::enc::REncoder;
#[cfg(verus_keep_ghost)]
use crate::verified::{saved, saved_values};
use crate::{Leb128, LoadOpts, MaybeFill, PackError, Run};
use counts::VB;
use kind::KCore;
use model::{Model, Streams};
use std::cmp::Ordering;
use std::marker::PhantomData;
use std::ops::{Bound, Range, RangeBounds};
#[cfg(verus_keep_ghost)]
use value::{lemma_opt_sizes, lemma_plain_sizes};
use value::{VOps, VOptOps, VOptWrite, VWrite};
#[allow(unused_imports)]
use vstd::prelude::*;
#[cfg(verus_keep_ghost)]
use vstd::std_specs::iter::{FromIteratorSpecImpl, IteratorSpec};
#[cfg(verus_keep_ghost)]
use vstd::std_specs::range::RangeBoundsSpec;

verus! {

/// A slab's segment budget, where none is given: that of
/// [`Column`](crate::Column).
pub(super) const MAX_SEG: usize = 64;

/// `range_in`: `range` clamped to a column of `len` values.
pub(crate) fn range_in_exec(range: Range<usize>, len: usize) -> (r: (usize, usize))
    ensures
        (r.0 as int, r.1 as int) == range_in(range, len as nat),
{
    let start = if range.start <= len {
        range.start
    } else {
        len
    };
    let end = if range.end <= len {
        range.end
    } else {
        len
    };
    (start, if end < start {
        start
    } else {
        end
    })
}

/// `bound_start` and `bound_end` of `range`, where `max` is the
/// place after the last there is: the old API's `normalize_range_max`.
pub(crate) fn bounds_exec<R: RangeBounds<usize>>(range: &R, max: usize) -> (r: (usize, usize))
    ensures
        r.0 as int == bound_start(range.spec_start_bound()),
        r.1 as int == bound_end(range.spec_start_bound(), range.spec_end_bound(), max as int),
{
    let start = match range.start_bound() {
        Bound::Included(n) => *n,
        Bound::Excluded(n) => if *n == usize::MAX {
            usize::MAX
        } else {
            *n + 1
        },
        Bound::Unbounded => 0,
    };
    let end = match range.end_bound() {
        Bound::Included(n) => if *n == usize::MAX {
            usize::MAX
        } else {
            *n + 1
        },
        Bound::Excluded(n) => *n,
        Bound::Unbounded => max,
    };
    (start, if end < start {
        start
    } else {
        end
    })
}

/// `bounds_in`: `range` clamped to a column of `len` values.
pub(crate) fn bounds_in_exec<R: RangeBounds<usize>>(range: &R, len: usize) -> (r: (usize, usize))
    ensures
        (r.0 as int, r.1 as int) == bounds_in(range.spec_start_bound(), range.spec_end_bound(), len as nat),
{
    let (start, end) = bounds_exec(range, len);
    (if start <= len {
        start
    } else {
        len
    }, if end <= len {
        end
    } else {
        len
    })
}

impl<T: RleValue> seal::GetSeal for T {

}

impl<T: RleValue> seal::GetSeal for Option<T> {

}

impl seal::GetSeal for bool {

}

impl<T: RleValue> seal::SpecSeal for T {

}

impl<T: RleValue> seal::SpecSeal for Option<T> {

}

impl seal::SpecSeal for bool {

}

/// What decoders give, as a type of theirs: a bound on it can then
/// name the lifetime `'a`, which one on `Decode::Item` cannot. Every
/// `Decoders` has it.
pub(crate) trait Items<'a>: Decoders<'a, D: Decode<Item = <Self as Items<'a>>::Item>> {
    type Item: Copy;
}

impl<'a, X: Decoders<'a>> Items<'a> for X {
    type Item = <X::D as Decode>::Item;
}

/// A value as a column of `V` holds it.
#[cfg(verus_keep_ghost)]
pub(super) type Held<V, C> = <<V as seal::Sealed<C>>::E as Slabs>::V;

/// `svs` goes through a concatenation.
pub(super) proof fn lemma_svs_add<T: VGet>(a: Seq<T>, b: Seq<T>)
    ensures
        T::svs(a + b) == T::svs(a) + T::svs(b),
{
    T::lemma_svs(a);
    T::lemma_svs(b);
    T::lemma_svs(a + b);
    assert(T::svs(a + b) =~= T::svs(a) + T::svs(b));
}

/// `svs` goes through a range.
pub(super) proof fn lemma_svs_sub<T: VGet>(a: Seq<T>, i: int, j: int)
    requires
        0 <= i <= j <= a.len(),
    ensures
        T::svs(a.subrange(i, j)) == T::svs(a).subrange(i, j),
{
    T::lemma_svs(a);
    T::lemma_svs(a.subrange(i, j));
    assert(T::svs(a.subrange(i, j)) =~= T::svs(a).subrange(i, j));
}

/// `svs` goes through a window.
pub(super) proof fn lemma_svs_window<T: VGet>(v: Seq<T>, a: int, b: int)
    requires
        0 <= a,
    ensures
        T::svs(window(v, a, b)) == window(T::svs(v), a, b),
{
    T::lemma_svs(v);
    if a >= v.len() || b <= a {
        lemma_svs_none::<T>();
    } else if b <= v.len() {
        lemma_svs_sub(v, a, b);
    } else {
        lemma_svs_sub(v, a, v.len() as int);
    }
}

/// No values are no values.
pub(super) proof fn lemma_svs_none<T: VGet>()
    ensures
        T::svs(Seq::<T>::empty()) == Seq::<T::S>::empty(),
{
    T::lemma_svs(Seq::<T>::empty());
    assert(T::svs(Seq::<T>::empty()) =~= Seq::<T::S>::empty());
}

/// One value is that value.
pub(super) proof fn lemma_svs_one<T: VGet>(v: T)
    ensures
        T::svs(seq![v]) == seq![v.sv()],
{
    T::lemma_svs(seq![v]);
    assert(T::svs(seq![v]) =~= seq![v.sv()]);
}

/// Values that are the same in a specification are the same.
pub(super) proof fn lemma_svs_inj<T: VGet>(a: Seq<T>, b: Seq<T>)
    requires
        T::svs(a) == T::svs(b),
    ensures
        a == b,
{
    T::lemma_svs(a);
    T::lemma_svs(b);
    let x = T::svs(a);
    let y = T::svs(b);
    assert forall|i: int| 0 <= i < a.len() implies a[i] == b[i] by {
        assert(x[i] == a[i].sv());
        assert(y[i] == b[i].sv());
        T::lemma_sv_inj(a[i], b[i]);
    }
    assert(a =~= b);
}

/// A splice of values is that splice of what they are in a
/// specification.
pub(super) proof fn lemma_svs_spliced<T: VGet>(a: Seq<T>, at: int, del: int, ins: Seq<T>)
    requires
        0 <= at <= a.len(),
        0 <= del <= a.len() - at,
    ensures
        T::svs(a.take(at) + ins + a.skip(at + del)) == T::svs(a).take(at) + T::svs(ins) + T::svs(a).skip(at + del),
{
    T::lemma_svs(a);
    lemma_svs_add(a.take(at), ins);
    lemma_svs_add(a.take(at) + ins, a.skip(at + del));
    lemma_svs_sub(a, 0, at);
    lemma_svs_sub(a, at + del, a.len() as int);
}

/// The values `vs`, given to a column of `V`, as it holds them.
pub(super) open spec fn held<'v, V: seal::Sealed<C>, C>(vs: Seq<Get<'v, V>>) -> Seq<Held<V, C>> {
    Seq::new(vs.len(), |i: int| <V::E as ReadsAgree<'v, V::X>>::own(vs[i]))
}

/// What a column holds for the values given are, in a specification,
/// those values.
pub(super) proof fn lemma_held<'v, V: ColumnValueRef<C>, C: Codec>(vs: Seq<Get<'v, V>>)
    ensures
        <Held<V, C> as VGet>::svs(held::<V, C>(vs)) == <Get<'v, V> as VGet>::svs(vs),
{
    let h = held::<V, C>(vs);
    let a = <Held<V, C> as VGet>::svs(h);
    let b = <Get<'v, V> as VGet>::svs(vs);
    <Held<V, C> as VGet>::lemma_svs(h);
    <Get<'v, V> as VGet>::lemma_svs(vs);
    assert forall|i: int| 0 <= i < vs.len() implies a[i] == b[i] by {
        V::lemma_own(vs[i]);
    }
    assert(a =~= b);
}

/// What the encoder of values of type `V` takes: a value in the
/// encoder's own form.
pub(super) type Given<'a, V, C> = <<<V as seal::Sealed<C>>::E as Streams<
    'a,
    <V as seal::Sealed<C>>::X,
>>::Enc as Encodes<<V as seal::Sealed<C>>::E>>::In;

/// How the values of a type go through a remap: read from a column,
/// handed to the client's function owned, and what it gives appended.
pub(crate) trait Owned<C>: seal::Sealed<C> {
    /// The encoder that a remap of a column appends to.
    type N: Encodes<Self::E>;

    /// A value read from a column, owned.
    fn to_owned<'a>(g: <Self as VForm<'a>>::Get) -> (v: Self)
        ensures
            Self::sv_of(v) == g.sv(),
    ;

    /// An owned value, as the encoder appends it.
    fn owned_in(w: Self) -> (x: <Self::N as Encodes<Self::E>>::In)
        ensures
            <Self::N as Encodes<Self::E>>::val(x).sv() == Self::sv_of(w),
    ;

    /// An owned value, as a [`Encoder`] of the lifetime `'a`
    /// appends it.
    fn owned_for<'a>(w: Self) -> (x: Given<'a, Self, C>)
        ensures
            <<Self::E as Streams<'a, Self::X>>::Enc as Encodes<Self::E>>::val(x).sv() == Self::sv_of(w),
    ;
}

impl<T: RleValue, C: Codec> seal::Sealed<C> for T {
    type X = RleReads<VOps<T, C>>;

    type E = RleModel<VWrite<T, C>>;
}

impl<T: RleValue, C: Codec> Owned<C> for T {
    type N = REncoder<'static, VWrite<T, C>>;

    #[inline(always)]
    fn to_owned<'a>(g: T) -> (v: T) {
        g
    }

    #[inline(always)]
    fn owned_in(w: T) -> (x: T) {
        w
    }

    #[inline(always)]
    fn owned_for<'a>(w: T) -> (x: Given<'a, T, C>) {
        w
    }
}

impl<T: RleValue, C: Codec> seal::Sealed<C> for Option<T> {
    type X = RleReads<VOptOps<T, C>>;

    type E = RleModel<VOptWrite<T, C>>;
}

impl<T: RleValue, C: Codec> Owned<C> for Option<T> {
    type N = REncoder<'static, VOptWrite<T, C>>;

    #[inline(always)]
    fn to_owned<'a>(g: Option<T>) -> (v: Option<T>) {
        g
    }

    #[inline(always)]
    fn owned_in(w: Option<T>) -> (x: Option<T>) {
        w
    }

    #[inline(always)]
    fn owned_for<'a>(w: Option<T>) -> (x: Given<'a, Option<T>, C>) {
        w
    }
}

impl<C: Codec> seal::Sealed<C> for bool {
    type X = BoolReads<VB<C>>;

    type E = BoolModel<VB<C>>;
}

impl<C: Codec> Owned<C> for bool {
    type N = BoolRecoder<VB<C>>;

    #[inline(always)]
    fn to_owned<'a>(g: bool) -> (v: bool) {
        g
    }

    #[inline(always)]
    fn owned_in(w: bool) -> (x: bool) {
        w
    }

    #[inline(always)]
    fn owned_for<'a>(w: bool) -> (x: Given<'a, bool, C>) {
        w
    }
}

// ── Searching ───────────────────────────────────────────────────────────────

/// A search target in a column of `V`: where a value stands to it in
/// the order of the values. Nothing here is trusted: the comparison is
/// the value type's own, which is verified.
#[verifier::reject_recursive_types(C)]
pub(super) struct VProbe<'t, V: ColumnValueRef<C>, C: Codec> {
    pub(super) target: Get<'t, V>,
    pub(super) _c: PhantomData<C>,
}

impl<'a, 't, V: ColumnValueRef<C>, C: Codec> Probe<Get<'a, V>> for VProbe<'t, V, C> {
    open(in crate::verified) spec fn side(&self, v: Get<'a, V>) -> int {
        if V::below(v.sv(), self.target.sv()) {
            -1
        } else if V::below(self.target.sv(), v.sv()) {
            1
        } else {
            0
        }
    }

    proof fn axiom_at(&self, a: Get<'a, V>, b: Get<'a, V>) {
        let t = self.target.sv();
        // neither before the target nor after it: the target's value
        V::lemma_below(a.sv(), t, t);
        V::lemma_below(b.sv(), t, t);
        VGet::lemma_sv_inj(a, b);
    }

    #[inline(always)]
    fn side_of(&self, v: &Get<'a, V>) -> Side {
        proof {
            // not both before the target and after it
            V::lemma_below(v.sv(), self.target.sv(), v.sv());
        }
        match V::compare(*v, self.target) {
            Ordering::Less => Side::Below,
            Ordering::Equal => Side::At,
            Ordering::Greater => Side::Above,
        }
    }
}

/// The search of [`Column::scope_to_value`] and of
/// [`Iter::seek_to_value`], over what the cursor `it` has left
/// in `slabs`: the range of the places that hold `target`, or the empty
/// range where it would stand, if the values are in order as far as
/// `target` can tell. Where they are not, some range of the places the
/// cursor had left.
#[inline(always)]
pub(super) fn seek_sorted<'a, 't, V: ColumnValueRef<C>, C: Codec, W: SlabAggregate, I: IndexOps<W>>(
    it: ICore<<V::X as Decoders<'a>>::D>,
    slabs: &'a [Slab<<V::E as Slabs>::Tail>],
    index: &I,
    target: Get<'t, V>,
) -> (r: (usize, usize))
    requires
        it.wf::<V::X>(slabs@),
        index.ok(),
        wlens::<W, I>(index.ws()) == slab_lens(slabs@),
    ensures
        ({
            let w = <Get<'a, V> as VGet>::svs(it.out::<V::X>(slabs@));
            let t = target.sv();
            &&& it.pos <= r.0 <= r.1 <= it.pos + w.len()
            &&& sorted_for::<V>(w, t) ==> forall|k: int|
                0 <= k < w.len() ==> {
                    &&& (it.pos + k < r.0) == V::below(#[trigger] w[k], t)
                    &&& (it.pos + k < r.1) == !V::below(t, w[k])
                    &&& (r.0 <= it.pos + k < r.1) == (w[k] == t)
                }
        }),
{
    let ghost w = it.out::<V::X>(slabs@);
    let ghost p0 = it.pos;
    let mut it = it;
    let probe = VProbe::<V, C> { target, _c: PhantomData };
    let find = |t: usize| -> (r: (usize, usize))
        requires
            index.ok(),
            wlens::<W, I>(index.ws()) == slab_lens(slabs@),
        ensures
            find_ok(slabs@, t, r),
        {
            let r = index.ix_find(t);
            proof {
                assert(index.ws().map_values(|w: W| I::wlen(w))
                    =~= wlens::<W, I>(index.ws()));
            }
            // past the slabs, the whole position lies beyond them
            if r.0 >= slabs.len() {
                (slabs.len(), 0)
            } else {
                r
            }
        };
    let start_of = |i: usize| -> (n: usize)
        requires
            index.ok(),
            wlens::<W, I>(index.ws()) == slab_lens(slabs@),
            i <= slabs@.len(),
        ensures
            n == sum(slab_lens(slabs@), 0, i as int),
        {
            let n = index.ix_before(i);
            proof {
                assert(index.ws().map_values(|w: W| I::wlen(w))
                    =~= wlens::<W, I>(index.ws()));
            }
            n
        };
    let r = it.seek_value::<V::X, _, _, _>(slabs, &probe, &find, &start_of);
    proof {
        let t = target.sv();
        let sub = <Get<'a, V> as VGet>::svs(w);
        <Get<'a, V> as VGet>::lemma_svs(w);
        if sorted_for::<V>(sub, t) {
            assert forall|i: int, j: int| 0 <= i <= j < w.len() implies #[trigger] probe.side(w[i])
                <= #[trigger] probe.side(w[j]) by {
                assert(sub[i] == w[i].sv());
                assert(sub[j] == w[j].sv());
            }
            assert(monotone(probe, w));
            assert(found(probe, w, r.0 - p0, r.1 - p0));
            assert forall|k: int| 0 <= k < sub.len() implies {
                &&& (p0 + k < r.0) == V::below(#[trigger] sub[k], t)
                &&& (p0 + k < r.1) == !V::below(t, sub[k])
                &&& (r.0 <= p0 + k < r.1) == (sub[k] == t)
            } by {
                let x = sub[k];
                assert(w[k].sv() == x);
                // not both before `t` and after it, and where
                // neither, `t`
                V::lemma_below(x, t, x);
                V::lemma_below(t, t, t);
            }
        }
    }
    r
}

// ── Remapping ───────────────────────────────────────────────────────────────

/// A client's function as a remap applies it (`Mapping`): to a value
/// read from the column, owned, and what it gives in the form the
/// encoder appends. Nothing is assumed of the function: what it asks of
/// a value and what it gives are its own `requires` and `ensures`.
#[verifier::reject_recursive_types(V)]
#[verifier::reject_recursive_types(C)]
#[verifier::reject_recursive_types(F)]
pub(super) struct VMap<'f, V, C, F> {
    pub(super) f: &'f F,
    pub(super) _c: PhantomData<(V, C)>,
}

impl<'a, 'f, V: ColumnValueRef<C>, C: Codec, F: Fn(V) -> V> Mapping<Get<'a, V>, <V::N as Encodes<V::E>>::In> for VMap<
    'f,
    V,
    C,
    F,
> {
    open(in crate::verified) spec fn pre(&self, g: Get<'a, V>) -> bool {
        forall|v: V| V::sv_of(v) == g.sv() ==> #[trigger] call_requires(*self.f, (v,))
    }

    open(in crate::verified) spec fn maps(&self, g: Get<'a, V>, x: <V::N as Encodes<V::E>>::In) -> bool {
        mapped(*self.f, g.sv(), <V::N as Encodes<V::E>>::val(x).sv())
    }

    #[inline(always)]
    fn apply(&self, g: Get<'a, V>) -> (x: <V::N as Encodes<V::E>>::In) {
        let v = V::to_owned(g);
        let w = (self.f)(v);
        V::owned_in(w)
    }
}

// ── What a splice is given ──────────────────────────────────────────────────

/// The values a splice inserts, from a slice: each a run of one.
#[verifier::reject_recursive_types(C)]
pub(super) struct SliceFeed<'a, 'v, V: ColumnValueRef<C>, C: Codec> {
    pub(super) values: &'a [Get<'v, V>],
    /// how many have been given
    pub(super) pos: usize,
    pub(super) _c: PhantomData<C>,
}

impl<'a, 'v, V: ColumnValueRef<C>, C: Codec> RunFeed<V::E> for SliceFeed<'a, 'v, V, C> {
    type X = <V::E as ReadsAgree<'v, V::X>>::Ins;

    /// The values to give: a feed keeps its slice.
    type K = Seq<Get<'v, V>>;

    open(in crate::verified) spec fn inv(&self) -> bool {
        self.pos <= self.values@.len()
    }

    open(in crate::verified) spec fn key(&self) -> Seq<Get<'v, V>> {
        self.values@
    }

    open(in crate::verified) spec fn given(&self) -> Seq<Held<V, C>> {
        held::<V, C>(self.values@.take(self.pos as int))
    }

    open(in crate::verified) spec fn ended(&self) -> bool {
        self.pos == self.values@.len()
    }

    #[inline(always)]
    fn next_run(&mut self) -> (r: Option<(<V::E as ReadsAgree<'v, V::X>>::Ins, usize)>) {
        if self.pos < self.values.len() {
            let v = self.values[self.pos];
            proof {
                let own = <V::E as ReadsAgree<'v, V::X>>::own(v);
                assert(held::<V, C>(self.values@.take(self.pos + 1)) =~= held::<V, C>(
                    self.values@.take(self.pos as int),
                ) + Seq::new(1, |_i: int| own));
            }
            self.pos = self.pos + 1;
            Some((<V::E as ReadsAgree<'v, V::X>>::ins(v), 1))
        } else {
            None
        }
    }
}

impl<'a, 'v, V: ColumnValueRef<C>, C: Codec> BoundedFeed<V::E> for SliceFeed<'a, 'v, V, C> {
    /// The values not given yet: each a run.
    open(in crate::verified) spec fn bound(&self) -> nat {
        (self.values@.len() - self.pos) as nat
    }

    #[inline(always)]
    fn next_bounded(&mut self) -> (r: Option<(<V::E as ReadsAgree<'v, V::X>>::Ins, usize)>) {
        if self.pos < self.values.len() {
            let v = self.values[self.pos];
            self.pos = self.pos + 1;
            Some((<V::E as ReadsAgree<'v, V::X>>::ins(v), 1))
        } else {
            None
        }
    }
}

/// The values a splice inserts, from a caller's iterator: each a run of
/// one.
#[verifier::reject_recursive_types(C)]
pub(super) struct IterFeed<'v, V: ColumnValueRef<C>, C: Codec, I: Iterator<Item = Get<'v, V>>> {
    pub(super) it: I,
    /// the iterator as it was handed over
    #[allow(dead_code)]
    pub(super) init: Ghost<I>,
    /// what it has given since
    #[allow(dead_code)]
    pub(super) given: Ghost<Seq<Get<'v, V>>>,
    /// it has said that it has no more
    #[allow(dead_code)]
    pub(super) ended: Ghost<bool>,
    pub(super) _c: PhantomData<C>,
}

impl<'v, V: ColumnValueRef<C>, C: Codec, I: Iterator<Item = Get<'v, V>>> RunFeed<V::E> for IterFeed<'v, V, C, I> {
    type X = <V::E as ReadsAgree<'v, V::X>>::Ins;

    /// The iterator as it was handed over: a feed keeps that.
    type K = I;

    /// Where the iterator keeps to what Verus asks of one, what it has
    /// given and what it is yet to give are together what it was to give
    /// when it was handed over.
    #[verifier::prophetic]
    open(in crate::verified) spec fn inv(&self) -> bool {
        &&& self.it.obeys_prophetic_iter_laws() == self.init@.obeys_prophetic_iter_laws()
        &&& self.it.obeys_prophetic_iter_laws() ==> {
            &&& self.given@ + self.it.remaining() == self.init@.remaining()
            &&& self.ended@ ==> self.it.remaining().len() == 0
        }
    }

    open(in crate::verified) spec fn key(&self) -> I {
        self.init@
    }

    open(in crate::verified) spec fn given(&self) -> Seq<Held<V, C>> {
        held::<V, C>(self.given@)
    }

    open(in crate::verified) spec fn ended(&self) -> bool {
        self.ended@
    }

    #[inline(always)]
    fn next_run(&mut self) -> (r: Option<(<V::E as ReadsAgree<'v, V::X>>::Ins, usize)>) {
        let ghost before = self.given@;
        let ghost rem = self.it.remaining();
        match self.it.next() {
            Some(v) => {
                proof {
                    let own = <V::E as ReadsAgree<'v, V::X>>::own(v);
                    self.given@ = before.push(v);
                    assert(held::<V, C>(before.push(v)) =~= held::<V, C>(before) + Seq::new(1, |_i: int| own));
                    if self.it.obeys_prophetic_iter_laws() {
                        assert(self.given@ + self.it.remaining() =~= before + rem);
                    }
                }
                Some((<V::E as ReadsAgree<'v, V::X>>::ins(v), 1))
            },
            None => {
                proof {
                    self.ended@ = true;
                }
                None
            },
        }
    }
}

pub(super) proof fn lemma_run_values_push_impl<G: VGet>(rs: Seq<Run<G>>, r: Run<G>)
    ensures
        run_values(rs.push(r)) == run_values(rs) + Seq::new(r.count as nat, |_i: int| r.value.sv()),
{
    assert(rs.push(r).drop_last() =~= rs);
}

pub(super) proof fn lemma_run_values_add_impl<G: VGet>(a: Seq<Run<G>>, b: Seq<Run<G>>)
    ensures
        run_values(a + b) == run_values(a) + run_values(b),
    decreases b.len(),
{
    if b.len() == 0 {
        assert(a + b =~= a);
        assert(run_values(a) + run_values(b) =~= run_values(a));
    } else {
        lemma_run_values_add_impl(a, b.drop_last());
        assert((a + b).drop_last() =~= a + b.drop_last());
        let run = Seq::new(b.last().count as nat, |_i: int| b.last().value.sv());
        assert(run_values(a) + run_values(b.drop_last()) + run =~= run_values(a) + (run_values(b.drop_last()) + run));
    }
}

/// The values of the runs `rs`, given to a column of `V`, as it holds
/// them.
pub(super) open spec fn held_runs<'v, V: seal::Sealed<C>, C>(rs: Seq<Run<Get<'v, V>>>) -> Seq<Held<V, C>>
    decreases rs.len(),
{
    if rs.len() == 0 {
        Seq::empty()
    } else {
        held_runs::<V, C>(rs.drop_last()) + Seq::new(
            rs.last().count as nat,
            |_i: int| <V::E as ReadsAgree<'v, V::X>>::own(rs.last().value),
        )
    }
}

/// What a column holds for the runs given are, in a specification, the
/// runs' values.
pub(super) proof fn lemma_held_runs<'v, V: ColumnValueRef<C>, C: Codec>(rs: Seq<Run<Get<'v, V>>>)
    ensures
        <Held<V, C> as VGet>::svs(held_runs::<V, C>(rs)) == run_values(rs),
    decreases rs.len(),
{
    if rs.len() == 0 {
        lemma_svs_none::<Held<V, C>>();
    } else {
        let r = rs.last();
        let before = held_runs::<V, C>(rs.drop_last());
        let own = <V::E as ReadsAgree<'v, V::X>>::own(r.value);
        let run = Seq::new(r.count as nat, |_i: int| own);
        lemma_held_runs::<V, C>(rs.drop_last());
        lemma_svs_add(before, run);
        <Held<V, C> as VGet>::lemma_svs(run);
        V::lemma_own(r.value);
        assert(<Held<V, C> as VGet>::svs(run) =~= Seq::new(r.count as nat, |_i: int| r.value.sv()));
    }
}

/// The values a splice inserts, from a caller's iterator of runs.
#[verifier::reject_recursive_types(C)]
pub(super) struct RunsFeed<'v, V: ColumnValueRef<C>, C: Codec, I: Iterator<Item = Run<Get<'v, V>>>> {
    pub(super) it: I,
    /// the iterator as it was handed over
    #[allow(dead_code)]
    pub(super) init: Ghost<I>,
    /// what it has given since
    #[allow(dead_code)]
    pub(super) given: Ghost<Seq<Run<Get<'v, V>>>>,
    /// it has said that it has no more
    #[allow(dead_code)]
    pub(super) ended: Ghost<bool>,
    pub(super) _c: PhantomData<C>,
}

impl<'v, V: ColumnValueRef<C>, C: Codec, I: Iterator<Item = Run<Get<'v, V>>>> RunFeed<V::E> for RunsFeed<'v, V, C, I> {
    type X = <V::E as ReadsAgree<'v, V::X>>::Ins;

    /// The iterator as it was handed over: a feed keeps that.
    type K = I;

    /// As [`IterFeed`]'s, of runs.
    #[verifier::prophetic]
    open(in crate::verified) spec fn inv(&self) -> bool {
        &&& self.it.obeys_prophetic_iter_laws() == self.init@.obeys_prophetic_iter_laws()
        &&& self.it.obeys_prophetic_iter_laws() ==> {
            &&& self.given@ + self.it.remaining() == self.init@.remaining()
            &&& self.ended@ ==> self.it.remaining().len() == 0
        }
    }

    open(in crate::verified) spec fn key(&self) -> I {
        self.init@
    }

    open(in crate::verified) spec fn given(&self) -> Seq<Held<V, C>> {
        held_runs::<V, C>(self.given@)
    }

    open(in crate::verified) spec fn ended(&self) -> bool {
        self.ended@
    }

    #[inline(always)]
    fn next_run(&mut self) -> (r: Option<(<V::E as ReadsAgree<'v, V::X>>::Ins, usize)>) {
        let ghost before = self.given@;
        let ghost rem = self.it.remaining();
        match self.it.next() {
            Some(run) => {
                proof {
                    self.given@ = before.push(run);
                    assert(before.push(run).drop_last() =~= before);
                    if self.it.obeys_prophetic_iter_laws() {
                        assert(self.given@ + self.it.remaining() =~= before + rem);
                    }
                }
                Some((<V::E as ReadsAgree<'v, V::X>>::ins(run.value), run.count))
            },
            None => {
                proof {
                    self.ended@ = true;
                }
                None
            },
        }
    }
}

/// What a splice of a slice's values leaves, as a client has it.
pub(super) proof fn lemma_spliced<'v, V: ColumnValueRef<C>, C: Codec>(
    a: Seq<Held<V, C>>,
    at: int,
    del: int,
    vs: Seq<Get<'v, V>>,
    ins: Seq<Held<V, C>>,
)
    requires
        0 <= at <= a.len(),
        0 <= del <= a.len() - at,
        held::<V, C>(vs.take(0)) + ins == held::<V, C>(vs.take(vs.len() as int)),
    ensures
        <Held<V, C> as VGet>::svs(a.take(at) + ins + a.skip(at + del)) == <Held<V, C> as VGet>::svs(a).take(at)
            + <Get<'v, V> as VGet>::svs(vs) + <Held<V, C> as VGet>::svs(a).skip(at + del),
{
    assert(vs.take(vs.len() as int) =~= vs);
    assert(ins =~= held::<V, C>(vs));
    lemma_svs_spliced(a, at, del, ins);
    lemma_held::<V, C>(vs);
}

// ── The column ──────────────────────────────────────────────────────────────

/// Slab `k` holds the values of the column that come after those of
/// the slabs before it.
pub(super) proof fn lemma_slab_vals<'a, X: Decoders<'a>>(s: Seq<Slab<X::Tail>>, k: int)
    requires
        lens_ok::<X>(s),
        0 <= k < s.len(),
    ensures
        sum(slab_lens(s), 0, k + 1) == sum(slab_lens(s), 0, k) + s[k].len,
        sum(slab_lens(s), 0, k + 1) <= vals_of::<X>(s).len(),
        X::vals(s[k]).len() == s[k].len,
        forall|i: int|
            0 <= i < s[k].len ==> #[trigger] X::vals(s[k])[i] == vals_of::<X>(s)[sum(
                slab_lens(s),
                0,
                k,
            ) + i],
{
    lemma_from::<X>(s, k, 0);
    let v = X::vals(s[k]);
    let base = sum(slab_lens(s), 0, k) as int;
    assert(v.skip(0) =~= v);
    assert forall|i: int| 0 <= i < s[k].len implies #[trigger] v[i] == vals_of::<X>(s)[base + i] by {
        assert(vals_of::<X>(s).skip(base)[i] == v[i]);
    }
}

pub(super) proof fn lemma_sorted_impl<V: VSpec>(vs: Seq<V::S>, t: V::S)
    requires
        sorted::<V>(vs),
    ensures
        sorted_for::<V>(vs, t),
{
    assert forall|i: int, j: int| 0 <= i <= j < vs.len() implies {
        &&& V::below(#[trigger] vs[j], t) ==> V::below(#[trigger] vs[i], t)
        &&& V::below(t, vs[i]) ==> V::below(t, vs[j])
    } by {
        // `vs[i]` is `vs[j]` or comes before it
        V::lemma_below(vs[i], vs[j], t);
        V::lemma_below(t, vs[i], vs[j]);
    }
}

/// How many values a slab holds.
pub(super) fn len_of<T: Copy + Clone + std::fmt::Debug + Default>(s: &Slab<T>) -> (n: usize)
    ensures
        n == s.len,
{
    s.len
}

/// How many runs a slab holds.
pub(super) fn segments_in<T: Copy + Clone + std::fmt::Debug + Default>(s: &Slab<T>) -> usize {
    s.segments
}

/// The lengths of slabs, collected as [`Column`](crate::Column) collects
/// them. The functions mapped have names because Verus proves nothing of
/// a closure here whose argument is of a generic type.
#[inline]
pub(super) fn lens_of<T: Copy + Clone + std::fmt::Debug + Default>(slabs: &[Slab<T>]) -> (r: Vec<usize>)
    ensures
        r@.len() == slabs@.len(),
        forall|k: int| 0 <= k < r@.len() ==> r@[k] == (#[trigger] slabs@[k]).len,
{
    let it = slabs.iter();
    let r: Vec<usize> = it.map(len_of::<T>).collect();
    proof {
        assert forall|k: int| 0 <= k < r@.len() implies r@[k] == (#[trigger] slabs@[k]).len by {
            assert(*it.remaining()[k] == slabs@[k]);
        }
    }
    r
}

/// The segment counts of slabs, collected likewise.
#[inline]
pub(super) fn segments_of<T: Copy + Clone + std::fmt::Debug + Default>(slabs: &[Slab<T>]) -> Vec<usize> {
    slabs.iter().map(segments_in::<T>).collect()
}

/// Numbers that are the lengths of slabs add up as the slabs' lengths do.
pub(super) proof fn lemma_sum_of_lens<T: Copy + Clone + std::fmt::Debug + Default>(lens: Seq<usize>, slabs: Seq<Slab<T>>, k: int)
    requires
        lens.len() == slabs.len(),
        forall|i: int| 0 <= i < lens.len() ==> lens[i] == (#[trigger] slabs[i]).len,
        0 <= k <= lens.len(),
    ensures
        sum_of(lens.take(k)) == sum(slab_lens(slabs), 0, k),
    decreases k,
{
    if k > 0 {
        lemma_sum_of_lens(lens, slabs, k - 1);
        assert(lens.take(k).drop_last() =~= lens.take(k - 1));
    }
}

/// What [`Column::dump_slabs`] prints of a slab: the values and
/// the runs it counts, its first run and its last.
pub(super) struct SlabRuns<G> {
    pub(super) len: usize,
    pub(super) segments: usize,
    pub(super) first: Option<Run<G>>,
    pub(super) last: Option<Run<G>>,
}

pub(super) proof fn lemma_after_edits_impl(n: usize, k: nat)
    requires
        k <= usize::MAX,
    ensures
        after_edits(n, k) == if n + k <= usize::MAX {
            n + k
        } else {
            n + k - usize::MAX - 1
        },
        k > 0 ==> after_edits(n, k) != n,
    decreases k,
{
    if k > 0 {
        lemma_after_edits_impl(n, (k - 1) as nat);
    }
}

impl<'v, V: ColumnValueRef<C>, C: Codec, K: VIndex<V, C>> Column<V, C, K> {
    pub(super) fn extend_impl<I: IntoIterator<Item = Get<'v, V>>>(&mut self, iter: I)
        ensures
            gives(iter, |xs: Seq<I::Item>| final(self)@ == old(self)@ + VGet::svs(xs)),
            final(self).edits() == after_call(old(self).edits(), final(self)@.len() != old(self)@.len()),
    {
        let len = self.len();
        self.splice(len, 0, iter);
        proof {
            let it = choose|it: I::IntoIter| #[trigger]
                (I::into_iter).ensures((iter,), it) && (it.obeys_prophetic_iter_laws() ==> self@ == old(self)@.take(
                    len as int,
                ) + VGet::svs(it.remaining()) + old(self)@.skip(len + 0));
            let vs = <Get<'v, V> as VGet>::svs(it.remaining());
            assert(old(self)@.take(len as int) + vs + old(self)@.skip(len + 0) =~= old(self)@ + vs);
        }
    }
}

impl<'v, V: ColumnValueRef<C>, C: Codec, K: VIndex<V, C>> Column<V, C, K> {
    pub(super) fn from_iter_impl<I: IntoIterator<Item = Get<'v, V>>>(iter: I) -> (s: Self)
        ensures
            // what Verus asks of every `FromIterator`
            exists|initial: I::IntoIter| #[trigger] (I::into_iter).ensures((iter,), initial) && (initial.obeys_prophetic_iter_laws() ==> s@ == <Get<'v, V> as VGet>::svs(initial.remaining())),
    {
        let mut s = Self::new();
        s.splice(0, 0, iter);
        proof {
            let it = choose|it: I::IntoIter| #[trigger]
                (I::into_iter).ensures((iter,), it) && (it.obeys_prophetic_iter_laws() ==> s@ == Seq::<
                    V::S,
                >::empty().take(0) + VGet::svs(it.remaining()) + Seq::<V::S>::empty().skip(0));
            let vs = <Get<'v, V> as VGet>::svs(it.remaining());
            assert(Seq::<V::S>::empty().take(0) + vs + Seq::<V::S>::empty().skip(0) =~= vs);
        }
        s
    }
}

impl<V: ColumnValueRef<C>, C: Codec> Column<V, C> {
    pub(super) proof fn lemma_save_unique_impl(x: Seq<u8>, y: Seq<u8>)
        requires
            Self::valid(x),
            Self::valid(y),
            Self::values(x) == Self::values(y),
        ensures
            x == y,
    {
        lemma_svs_inj(<V::E as Model<V::X>>::values(x), <V::E as Model<V::X>>::values(y));
        <V::E as Model<V::X>>::lemma_unique(x, y);
    }

    pub(super) proof fn lemma_encoding_impl(d: Seq<u8>)
        requires
            Self::valid(d),
        ensures
            Self::encoding(Self::values(d)) == d,
    {
        let e = Self::encoding(Self::values(d));
        assert(Self::valid(e) && Self::values(e) == Self::values(d));
        Self::lemma_save_unique(e, d);
    }

    pub(super) proof fn lemma_no_bytes_impl()
        ensures
            Self::valid(Seq::<u8>::empty()),
            Self::values(Seq::<u8>::empty()) == Seq::<V::S>::empty(),
    {
        <V::E as Model<V::X>>::lemma_none();
        lemma_svs_none::<Held<V, C>>();
    }
}

impl<V: ColumnValueRef<C>, C: Codec, K: VIndex<V, C>> Column<V, C, K> {
    pub(super) fn new_impl() -> (r: Self)
        ensures
            r@ == Seq::<V::S>::empty(),
    {
        Self::with_max_segments(MAX_SEG)
    }

    pub(super) fn with_max_segments_impl(max_segments: usize) -> (r: Self)
        requires
            max_segments >= 2,
        ensures
            r@ == Seq::<V::S>::empty(),
    {
        let core = KCore::<V, C, K>::with_budget(max_segments);
        proof {
            assert(core@ =~= Seq::empty());
            lemma_svs_none::<Held<V, C>>();
        }
        Column { core }
    }

    pub(super) fn from_values_impl(values: &[Get<'_, V>]) -> (r: Self)
        ensures
            r@ == VGet::svs(values@),
    {
        Self::from_values_with_max_segments(values, MAX_SEG)
    }

    pub(super) fn from_values_with_max_segments_impl(values: &[Get<'_, V>], max_segments: usize) -> (r: Self)
        requires
            max_segments >= 2,
        ensures
            r@ == VGet::svs(values@),
    {
        let mut r = Self::with_max_segments(max_segments);
        r.splice_slice(0, 0, values);
        proof {
            let vs = VGet::svs(values@);
            assert(Seq::<V::S>::empty().take(0) + vs + Seq::<V::S>::empty().skip(0) =~= vs);
        }
        r
    }

    pub(super) fn fill_impl(len: usize, value: Get<'_, V>) -> (r: Self)
        requires
            len <= i64::MAX,
            len <= usize::MAX - 3,
        ensures
            r@ == Seq::new(len as nat, |_i: int| value.sv()),
    {
        Self::filled(len, value, MAX_SEG)
    }

    /// [`fill`](Self::fill), in slabs of at most `max_segments` runs.
    #[inline(always)]
    pub(super) fn filled(len: usize, value: Get<'_, V>, max_segments: usize) -> (r: Self)
        requires
            len <= i64::MAX,
            len <= usize::MAX - 3,
            max_segments >= 2,
        ensures
            r@ == Seq::new(len as nat, |_i: int| value.sv()),
    {
        if len == 0 {
            let r = Self::with_max_segments(max_segments);
            proof {
                assert(r@ =~= Seq::new(len as nat, |_i: int| value.sv()));
            }
            return r;
        }
        let slab = <V::E as Model<V::X>>::fill(len, value);
        let ghost s = slab;
        let mut slabs = Vec::with_capacity(1);
        slabs.push(slab);
        proof {
            assert(slabs@ =~= seq![s]);
            lemma_vals_unit::<V::E>(s);
        }
        let core = KCore::<V, C, K>::from_cut(slabs, max_segments);
        proof {
            let vs = Seq::new(len as nat, |_i: int| value.sv());
            <Held<V, C> as VGet>::lemma_svs(core@);
            V::lemma_own(value);
            assert(<Held<V, C> as VGet>::svs(core@) =~= vs);
        }
        Column { core }
    }
}

impl<V: ColumnValueRef<C>, C: Codec, K: VIndex<V, C>> Column<V, C, K> {
    #[inline(always)]
    pub(super) fn len_impl(&self) -> (r: usize)
        ensures
            r == self@.len(),
    {
        proof {
            <Held<V, C> as VGet>::lemma_svs(self.core@);
        }
        self.core.len()
    }

    #[inline(always)]
    pub(super) fn is_empty_impl(&self) -> (r: bool)
        ensures
            r == (self@.len() == 0),
    {
        proof {
            <Held<V, C> as VGet>::lemma_svs(self.core@);
        }
        self.core.len() == 0
    }

    /// Set the count of edits after a call: to one more than `before`,
    /// which the column counted before the call, if the call was an edit
    /// (`edit`), and to `before` if it was none. The shared cursor moves
    /// the count as it writes, by a number that depends on the edit;
    /// this makes it one an edit.
    #[inline(always)]
    pub(super) fn count_call(&mut self, before: usize, edit: bool)
        ensures
            final(self)@ == old(self)@,
            final(self).core@ == old(self).core@,
            final(self).edits() == after_call(before, edit),
    {
        self.core.set_counter(
            if edit {
                before.wrapping_add(1)
            } else {
                before
            },
        );
    }

    pub(super) fn splice_slice_impl(&mut self, at: usize, del: usize, values: &[Get<'_, V>])
        requires
            at <= old(self)@.len(),
            del <= old(self)@.len() - at,
        ensures
            final(self)@ == old(self)@.take(at as int) + VGet::svs(values@) + old(self)@.skip(at + del),
            final(self).edits() == after_call(old(self).edits(), del > 0 || values@.len() > 0),
    {
        proof {
            <Held<V, C> as VGet>::lemma_svs(self.core@);
        }
        let counter = self.core.counter();
        let mut f = <V::E as Model<V::X>>::blind();
        let mut feed = SliceFeed::<V, C> { values, pos: 0, _c: PhantomData };
        let _ins = self.core.splice_bounded(at, del, &mut f, &mut feed);
        proof {
            lemma_spliced::<V, C>(old(self).core@, at as int, del as int, values@, _ins@);
        }
        self.count_call(counter, del > 0 || values.len() > 0);
    }

    pub(super) fn splice_impl<'v, I: IntoIterator<Item = Get<'v, V>>>(&mut self, at: usize, del: usize, values: I)
        requires
            at <= old(self)@.len(),
            del <= old(self)@.len() - at,
        ensures
            gives(values, |xs: Seq<I::Item>| final(self)@ == old(self)@.take(at as int) + VGet::svs(xs) + old(self)@.skip(at + del)),
            final(self).edits() == after_call(old(self).edits(), del > 0 || final(self)@.len() != old(self)@.len()),
    {
        proof {
            <Held<V, C> as VGet>::lemma_svs(self.core@);
        }
        let counter = self.core.counter();
        let len = self.core.len();
        let it = values.into_iter();
        let ghost it0 = it;
        let mut f = <V::E as Model<V::X>>::blind();
        let mut feed = IterFeed::<V, C, I::IntoIter> {
            it,
            init: Ghost(it0),
            given: Ghost(Seq::empty()),
            ended: Ghost(false),
            _c: PhantomData,
        };
        proof {
            assert(feed.given@ + feed.it.remaining() =~= it0.remaining());
        }
        let _ins = self.core.splice(at, del, &mut f, &mut feed);
        proof {
            if it0.obeys_prophetic_iter_laws() {
                let vs = it0.remaining();
                assert(feed.given@ =~= feed.given@ + feed.it.remaining());
                assert(vs.take(0) =~= Seq::<Get<'v, V>>::empty());
                assert(vs.take(vs.len() as int) =~= vs);
                lemma_spliced::<V, C>(old(self).core@, at as int, del as int, vs, _ins@);
            }
            <Held<V, C> as VGet>::lemma_svs(self.core@);
        }
        let edit = del > 0 || self.core.len() != len;
        self.count_call(counter, edit);
    }

    pub(super) fn splice_runs_impl<'v, I: IntoIterator<Item = Run<Get<'v, V>>>>(&mut self, at: usize, del: usize, runs: I)
        requires
            at <= old(self)@.len(),
            del <= old(self)@.len() - at,
        ensures
            gives(runs, |xs: Seq<I::Item>| final(self)@ == old(self)@.take(at as int) + run_values(xs) + old(self)@.skip(at + del)),
            final(self).edits() == after_call(old(self).edits(), del > 0 || final(self)@.len() != old(self)@.len()),
    {
        proof {
            <Held<V, C> as VGet>::lemma_svs(self.core@);
        }
        let counter = self.core.counter();
        let len = self.core.len();
        let it = runs.into_iter();
        let ghost it0 = it;
        let mut f = <V::E as Model<V::X>>::blind();
        let mut feed = RunsFeed::<V, C, I::IntoIter> {
            it,
            init: Ghost(it0),
            given: Ghost(Seq::empty()),
            ended: Ghost(false),
            _c: PhantomData,
        };
        proof {
            assert(feed.given@ + feed.it.remaining() =~= it0.remaining());
        }
        let _ins = self.core.splice(at, del, &mut f, &mut feed);
        proof {
            if it0.obeys_prophetic_iter_laws() {
                let rs = it0.remaining();
                assert(feed.given@ =~= feed.given@ + feed.it.remaining());
                assert(_ins@ =~= Seq::<Held<V, C>>::empty() + _ins@);
                lemma_svs_spliced(old(self).core@, at as int, del as int, _ins@);
                lemma_held_runs::<V, C>(rs);
            }
            <Held<V, C> as VGet>::lemma_svs(self.core@);
        }
        let edit = del > 0 || self.core.len() != len;
        self.count_call(counter, edit);
    }

    /// Delete `del` values at `at`, which is an edit unless `del` is 0.
    pub(super) fn delete(&mut self, at: usize, del: usize)
        requires
            at <= old(self)@.len(),
            del <= old(self)@.len() - at,
        ensures
            final(self)@ == old(self)@.take(at as int) + old(self)@.skip(at + del),
            final(self).edits() == after_call(old(self).edits(), del > 0),
    {
        proof {
            <Held<V, C> as VGet>::lemma_svs(self.core@);
        }
        let counter = self.core.counter();
        let mut f = <V::E as Model<V::X>>::blind();
        let mut feed = NoRuns::<<V::E as ReadsAgree<'static, V::X>>::Ins> { none: None };
        let _ins = self.core.splice_bounded(at, del, &mut f, &mut feed);
        proof {
            let a = old(self).core@;
            let none = Seq::<Held<V, C>>::empty();
            assert(_ins@ =~= none);
            lemma_svs_spliced(a, at as int, del as int, none);
            lemma_svs_none::<Held<V, C>>();
            assert(self@ =~= old(self)@.take(at as int) + old(self)@.skip(at + del));
        }
        self.count_call(counter, del > 0);
    }

    #[inline(always)]
    pub(super) fn insert_impl(&mut self, at: usize, value: Get<'_, V>)
        requires
            at <= old(self)@.len(),
        ensures
            final(self)@ == old(self)@.insert(at as int, value.sv()),
            final(self).edits() == after_edit(old(self).edits()),
    {
        // Which of the two it is, is a constant of the column's type: the
        // other is not compiled.
        if <V::E as Model<V::X>>::ONE_APART {
            self.insert_apart(at, value);
        } else {
            let one = [value];
            self.splice_slice(at, 0, one.as_slice());
            proof {
                assert(one@ =~= seq![value]);
                lemma_svs_one(value);
                assert(self@ =~= old(self)@.insert(at as int, value.sv()));
            }
        }
    }

    /// `insert`, out of line: a splice of one value that deletes none,
    /// compiled for that alone, apart from `splice`.
    pub(super) fn insert_apart(&mut self, at: usize, value: Get<'_, V>)
        requires
            at <= old(self)@.len(),
        ensures
            final(self)@ == old(self)@.insert(at as int, value.sv()),
            final(self).edits() == after_edit(old(self).edits()),
    {
        proof {
            <Held<V, C> as VGet>::lemma_svs(self.core@);
        }
        let counter = self.core.counter();
        let one = [value];
        let values = one.as_slice();
        let mut f = <V::E as Model<V::X>>::blind();
        let mut feed = SliceFeed::<V, C> { values, pos: 0, _c: PhantomData };
        let _ins = self.core.splice_bounded(at, 0, &mut f, &mut feed);
        proof {
            lemma_spliced::<V, C>(old(self).core@, at as int, 0, values@, _ins@);
            assert(one@ =~= seq![value]);
            lemma_svs_one(value);
            assert(self@ =~= old(self)@.insert(at as int, value.sv()));
        }
        self.count_call(counter, true);
    }

    #[inline(always)]
    pub(super) fn push_impl(&mut self, value: Get<'_, V>)
        ensures
            final(self)@ == old(self)@.push(value.sv()),
            final(self).edits() == after_edit(old(self).edits()),
    {
        proof {
            <Held<V, C> as VGet>::lemma_svs(self.core@);
        }
        let at = self.core.len();
        self.insert(at, value);
        proof {
            assert(self@ =~= old(self)@.push(value.sv()));
        }
    }

    #[inline(always)]
    pub(super) fn remove_impl(&mut self, at: usize)
        requires
            at < old(self)@.len(),
        ensures
            final(self)@ == old(self)@.remove(at as int),
            final(self).edits() == after_edit(old(self).edits()),
    {
        self.delete(at, 1);
        proof {
            assert(self@ =~= old(self)@.remove(at as int));
        }
    }

    #[inline(always)]
    pub(super) fn truncate_impl(&mut self, len: usize)
        ensures
            final(self)@ == if len < old(self)@.len() {
                old(self)@.take(len as int)
            } else {
                old(self)@
            },
            final(self).edits() == after_call(old(self).edits(), len < old(self)@.len()),
    {
        proof {
            <Held<V, C> as VGet>::lemma_svs(self.core@);
        }
        let n = self.core.len();
        if len < n {
            self.delete(len, n - len);
            proof {
                assert(self@ =~= old(self)@.take(len as int));
            }
        }
    }

    #[inline(always)]
    pub(super) fn remove_n_impl(&mut self, at: usize, n: usize)
        requires
            at <= old(self)@.len(),
            n <= old(self)@.len() - at,
        ensures
            final(self)@ == old(self)@.take(at as int) + old(self)@.skip(at + n),
            final(self).edits() == after_call(old(self).edits(), n > 0),
    {
        if n > 0 {
            self.delete(at, n);
        } else {
            proof {
                assert(self@ =~= self@.take(at as int) + self@.skip(at as int));
            }
        }
    }

    pub(super) fn clear_impl(&mut self)
        ensures
            final(self)@ == Seq::<V::S>::empty(),
            final(self).edits() == after_call(old(self).edits(), old(self)@.len() > 0),
    {
        if self.core.len() == 0 {
            proof {
                <Held<V, C> as VGet>::lemma_svs(self.core@);
                assert(self@ =~= Seq::empty());
            }
            return;
        }
        // a new column, and not a delete of every value, which would
        // leave the last slab to be emptied through a rebuild. The edit
        // counter goes on from where it is, as after any other edit.
        proof {
            // the column has a value
            <Held<V, C> as VGet>::lemma_svs(self.core@);
        }
        self.core.lemma_column();
        let mut core = KCore::<V, C, K>::with_budget(self.core.max_segments());
        core.set_counter(self.core.counter().wrapping_add(1));
        proof {
            assert(core@ =~= Seq::empty());
            lemma_svs_none::<Held<V, C>>();
        }
        self.core = core;
    }

    pub(super) fn slab_count_impl(&self) -> usize
    {
        self.core.slabs().len()
    }

    pub(super) fn slab_lens_impl(&self) -> (r: Vec<usize>)
        ensures
            sum_of(r@) == self@.len(),
    {
        self.core.lemma_column();
        let slabs = self.core.slabs();
        proof {
            <Held<V, C> as VGet>::lemma_svs(self.core@);
        }
        let r = lens_of(slabs);
        proof {
            lemma_sum_of_lens(r@, slabs@, r@.len() as int);
            assert(r@.take(r@.len() as int) =~= r@);
        }
        r
    }

    pub(super) fn slab_segments_impl(&self) -> Vec<usize>
    {
        segments_of(self.core.slabs())
    }

    /// Slab `i` as [`dump_slabs`](Self::dump_slabs) prints it: the
    /// counts it holds, its first run and its last, which are `None` for
    /// a slab of no values. `None` if the column has no slab `i`.
    // a Verus loop with an invariant: `loop` + `match`
    #[allow(clippy::while_let_loop)]
    pub(super) fn slab_runs(&self, i: usize) -> Option<SlabRuns<Get<'_, V>>> {
        self.core.lemma_column();
        let slabs = self.core.slabs();
        if i >= slabs.len() {
            return None;
        }
        proof {
            lemma_lens_ok::<V::E, V::X>(slabs@);
        }
        let s = &slabs[i];
        let mut d = <V::X as Decoders>::open(s);
        let first = d.d_run_max(usize::MAX);
        let mut last = first;
        if first.is_some() {
            loop
                invariant
                    d.inv(),
                decreases d.rest().len(),
            {
                match d.d_run_max(usize::MAX) {
                    Some(run) => {
                        last = Some(run);
                    },
                    None => {
                        break ;
                    },
                }
            }
        }
        Some(SlabRuns { len: s.len, segments: s.segments, first, last })
    }

    /// How many slabs the column's index counts. For a column that
    /// holds values: an emptied one may be without its index (after a
    /// panic in an edit), and this panics then.
    pub(super) fn index_len(&self) -> usize {
        self.core.lemma_column();
        self.core.index().ix_len()
    }

    /// How many values the column's index counts before slab `i`, or
    /// `None` if it counts fewer than `i` slabs. For a column that holds
    /// values, as [`index_len`](Self::index_len).
    pub(super) fn index_before(&self, i: usize) -> Option<usize> {
        self.core.lemma_column();
        let ix = self.core.index();
        if i <= ix.ix_len() {
            Some(ix.ix_before(i))
        } else {
            None
        }
    }

    /// What the slabs' readers give is, as values, the column's.
    #[inline(always)]
    pub(super) fn lemma_read(&self)
        ensures
            VGet::svs(vals_of::<V::X>(self.core.slabs_spec())) == self@,
            self@.len() == total(slab_lens(self.core.slabs_spec())),
    {
        self.core.lemma_column();
        proof {
            let o = vals_of::<V::X>(self.core.slabs_spec());
            lemma_vals_own::<V::E, V::X>(self.core.slabs_spec());
            assert(self.core@ =~= held::<V, C>(o));
            lemma_held::<V, C>(o);
            <Held<V, C> as VGet>::lemma_svs(self.core@);
        }
    }

    pub(super) proof fn lemma_kept_impl(tracked &self)
        ensures
            K::keeps(self@),
    {
        let s = self.core.slabs_spec();
        self.core.lemma_view();
        self.core.lemma_no_parts();
        if !self.core.ready() {
            assert(slab_lens(s) =~= Seq::<nat>::empty());
            <K::G as Weigh<V::E, K::W>>::lemma_dom_empty(s);
        }
        K::lemma_kept(s);
    }

    /// A reader of the values `start..end`.
    #[inline(always)]
    pub(super) fn reader(&self, start: usize, end: usize) -> (it: ICore<<V::X as Decoders<'_>>::D>)
        requires
            start <= end <= self@.len(),
        ensures
            it.wf::<V::X>(self.core.slabs_spec()),
            it.pos == start,
            it.items_left == end - start,
            VGet::svs(it.out::<V::X>(self.core.slabs_spec())) == self@.subrange(start as int, end as int),
    {
        proof {
            <Held<V, C> as VGet>::lemma_svs(self.core@);
        }
        let it = self.core.reader::<V::X>(start, end);
        proof {
            let o = it.out::<V::X>(self.core.slabs_spec());
            let s = self.core@.subrange(start as int, end as int);
            assert(s =~= held::<V, C>(o));
            lemma_held::<V, C>(o);
            lemma_svs_sub(self.core@, start as int, end as int);
        }
        it
    }

    pub(super) fn get_impl(&self, i: usize) -> (r: Option<Get<'_, V>>)
        ensures
            i < self@.len() ==> (r matches Some(v) && v.sv() == self@[i as int]),
            i >= self@.len() ==> r is None,
    {
        proof {
            <Held<V, C> as VGet>::lemma_svs(self.core@);
        }
        if i >= self.core.len() {
            return None;
        }
        // the column's length is short of `usize::MAX`
        self.core.lemma_column();
        let slabs = self.core.slabs();
        let mut it = self.reader(i, i + 1);
        proof {
            let o = it.out::<V::X>(slabs@);
            let s = VGet::svs(o);
            VGet::lemma_svs(o);
            assert(s[0] == self@[i as int]);
            assert(s[0] == o[0].sv());
        }
        it.next::<V::X>(slabs)
    }

    pub(super) fn to_vec_impl(&self) -> (r: Vec<Get<'_, V>>)
        ensures
            VGet::svs(r@) == self@,
    {
        proof {
            <Held<V, C> as VGet>::lemma_svs(self.core@);
        }
        let n = self.core.len();
        let slabs = self.core.slabs();
        let mut it = self.reader(0, n);
        let mut out: Vec<Get<'_, V>> = Vec::with_capacity(n);
        let ghost o = it.out::<V::X>(slabs@);
        it.drain_into::<V::X>(slabs, &mut out);
        proof {
            assert(out@ =~= o);
            assert(self@.subrange(0, n as int) =~= self@);
        }
        out
    }

    pub(super) fn is_only_impl(&self, value: Get<'_, V>) -> (r: bool)
        ensures
            r == only(self@, value.sv()),
    {
        self.core.lemma_column();
        let slabs = self.core.slabs();
        let ghost vs = vals_of::<V::X>(slabs@);
        proof {
            lemma_lens_ok::<V::E, V::X>(slabs@);
            lemma_vals_own::<V::E, V::X>(slabs@);
            assert(self.core@ =~= held::<V, C>(vs));
            lemma_held::<V, C>(vs);
            VGet::lemma_svs(vs);
        }
        // slab by slab: a slab of one value is one run of it
        let mut j: usize = 0;
        while j < slabs.len()
            invariant
                j <= slabs@.len(),
                lens_ok::<V::X>(slabs@),
                vs == vals_of::<V::X>(slabs@),
                vs.len() == self@.len(),
                vs.len() < usize::MAX,
                forall|i: int| 0 <= i < vs.len() ==> self@[i] == (#[trigger] vs[i]).sv(),
                forall|i: int| 0 <= i < sum(slab_lens(slabs@), 0, j as int) ==> self@[i] == value.sv(),
            decreases slabs@.len() - j,
        {
            let s = &slabs[j];
            let ghost base = sum(slab_lens(slabs@), 0, j as int) as int;
            let ghost sv = <V::X as Decoders>::vals(*s);
            proof {
                lemma_slab_vals::<V::X>(slabs@, j as int);
            }
            if !<V::X as Decoders>::maybe_uniform(s) {
                proof {
                    // two of the slab's values differ, so one is not
                    // `value`
                    let i = choose|i: int| 0 < i < sv.len() && sv[i - 1] != #[trigger] sv[i];
                    let a = sv[i - 1];
                    let b = sv[i];
                    assert(a == vs[base + i - 1]);
                    assert(b == vs[base + i]);
                    if a.sv() == b.sv() {
                        VGet::lemma_sv_inj(a, b);
                    }
                    if self@[base + i - 1] == value.sv() {
                        assert(self@[base + i] != value.sv());
                    }
                }
                return false;
            }
            let mut d = <V::X as Decoders>::open(s);
            if let Some(run) = d.d_run_max(usize::MAX) {
                if run.count != s.len || !V::same(run.value, value) {
                    proof {
                        // the slab's first value is another, or the one
                        // at which its first run ends
                        let k: int = if run.value.sv() == value.sv() {
                            run.count as int
                        } else {
                            0
                        };
                        let w = sv[k];
                        assert(sv.take(run.count as int)[0] == run.value);
                        if w.sv() == run.value.sv() {
                            VGet::lemma_sv_inj(w, run.value);
                        }
                        assert(w == vs[base + k]);
                        assert(self@[base + k] != value.sv());
                    }
                    return false;
                }
                proof {
                    assert forall|i: int| base <= i < base + s.len implies self@[i]
                        == value.sv() by {
                        assert(sv.take(run.count as int)[i - base] == run.value);
                        assert(sv[i - base] == vs[i]);
                    }
                }
            }
            j += 1;
        }
        true
    }

    pub(super) fn scope_to_value_impl<R: RangeBounds<usize>>(&self, value: Get<'_, V>, range: R) -> (r: Range<usize>)
        ensures
            ({
                let (start, end) = bounds_in(range.spec_start_bound(), range.spec_end_bound(), self@.len());
                &&& start <= r.start <= r.end <= end
                &&& sorted_for::<V>(self@.subrange(start, end), value.sv()) ==> forall|k: int|
                    start <= k < end ==> {
                        &&& (k < r.start) == V::below(#[trigger] self@[k], value.sv())
                        &&& (r.start <= k < r.end) == (self@[k] == value.sv())
                        &&& (r.end <= k) == V::below(value.sv(), self@[k])
                    }
            }),
    {
        let (start, end) = bounds_in_exec(&range, self.len());
        self.scope_within(value, start, end)
    }

    /// [`scope_to_value`](Column::scope_to_value) over the places
    /// `start..end`.
    fn scope_within(&self, value: Get<'_, V>, start: usize, end: usize) -> (r: Range<usize>)
        requires
            start <= end <= self@.len(),
        ensures
            start <= r.start <= r.end <= end,
            sorted_for::<V>(self@.subrange(start as int, end as int), value.sv()) ==> forall|k: int|
                start <= k < end ==> {
                    &&& (k < r.start) == V::below(#[trigger] self@[k], value.sv())
                    &&& (r.start <= k < r.end) == (self@[k] == value.sv())
                    &&& (r.end <= k) == V::below(value.sv(), self@[k])
                },
    {
        if start == end {
            // nothing to look at (and a column that an edit's panic left
            // without its parts has no index to ask)
            return start..start;
        }
        self.core.lemma_column();
        let slabs = self.core.slabs();
        let index = self.core.index();
        let it = self.reader(start, end);
        let ghost w = it.out::<V::X>(slabs@);
        let ghost sub = self@.subrange(start as int, end as int);
        let r = seek_sorted::<V, C, K::W, K::I>(it, slabs, index, value);
        proof {
            VGet::lemma_svs(w);
            assert(VGet::svs(w) == sub);
            if sorted_for::<V>(sub, value.sv()) {
                assert forall|k: int| start <= k < end implies {
                    &&& (k < r.0) == V::below(#[trigger] self@[k], value.sv())
                    &&& (k < r.1) == !V::below(value.sv(), self@[k])
                    &&& (r.0 <= k < r.1) == (self@[k] == value.sv())
                } by {
                    assert(sub[k - start] == self@[k]);
                }
            }
        }
        r.0..r.1
    }

    pub(super) fn remap_impl<F: Fn(V) -> V>(&mut self, f: F)
        requires
            forall|v: V| old(self)@.contains(V::sv_of(v)) ==> #[trigger] call_requires(f, (v,)),
        ensures
            final(self)@.len() == old(self)@.len(),
            forall|i: int| 0 <= i < old(self)@.len() ==> mapped(f, old(self)@[i], #[trigger] final(self)@[i]),
            final(self).edits() == after_edit(old(self).edits()),
    {
        let counter = self.core.counter();
        let m = VMap::<V, C, F> { f: &f, _c: PhantomData };
        let ghost o = vals_of::<V::X>(self.core.slabs_spec());
        self.core.lemma_column();
        proof {
            // the values read are the column's
            lemma_vals_own::<V::E, V::X>(self.core.slabs_spec());
            assert(self.core@ =~= held::<V, C>(o));
            lemma_held::<V, C>(o);
            VGet::lemma_svs(o);
            assert forall|i: int| 0 <= i < self.core@.len() implies m.pre(#[trigger] o[i]) by {
                assert(self@[i] == o[i].sv());
            }
        }
        let (core, Ghost(ws)) = self.core.remap::<V::X, V::N, VMap<V, C, F>>(&m);
        proof {
            <Held<V, C> as VGet>::lemma_svs(core@);
            assert forall|i: int| 0 <= i < o.len() implies mapped(
                f,
                old(self)@[i],
                #[trigger] <Held<V, C> as VGet>::svs(core@)[i],
            ) by {
                assert(m.maps(o[i], ws[i]));
                assert(old(self)@[i] == o[i].sv());
            }
        }
        self.core = core;
        // a write like any other: the edit counter goes on from where it
        // was
        self.core.set_counter(counter.wrapping_add(1));
    }

    #[inline(always)]
    pub(super) fn iter_impl(&self) -> (it: Iter<'_, V, C, K>)
        ensures
            it.rest() == self@,
            it.column() == self@,
            it.source() == Some(*self),
            it.pos() == 0,
            it.end_pos() == self@.len(),
    {
        proof {
            <Held<V, C> as VGet>::lemma_svs(self.core@);
        }
        self.lemma_read();
        let n = self.core.len();
        proof {
            assert(self@.subrange(0, n as int) =~= self@);
        }
        Iter { it: self.reader(0, n), slabs: self.core.slabs(), col: Some(&self.core) }
    }

    #[inline(always)]
    pub(super) fn iter_range_impl(&self, range: Range<usize>) -> (it: Iter<'_, V, C, K>)
        ensures
            ({
                let (start, end) = range_in(range, self@.len());
                &&& it.rest() == self@.subrange(start, end)
                &&& it.column() == self@
                &&& it.source() == Some(*self)
                &&& it.pos() == start
                &&& it.end_pos() == end
            }),
    {
        let (start, end) = range_in_exec(range, self.len());
        self.iter_within(start, end)
    }

    /// An iterator over the values `start..end`.
    pub(super) fn iter_within(&self, start: usize, end: usize) -> (it: Iter<'_, V, C, K>)
        requires
            start <= end <= self@.len(),
        ensures
            it.rest() == self@.subrange(start as int, end as int),
            it.column() == self@,
            it.source() == Some(*self),
            it.pos() == start,
            it.end_pos() == end,
    {
        self.lemma_read();
        Iter { it: self.reader(start, end), slabs: self.core.slabs(), col: Some(&self.core) }
    }

    pub(super) fn save_impl(&self) -> (r: Vec<u8>)
        ensures
            Column::<V, C>::valid(r@),
            Column::<V, C>::values(r@) == self@,
            r@ == Column::<V, C>::encoding(self@),
            r@.len() <= V::size_bound(self@),
    {
        self.core.lemma_column();
        let mut out: Vec<u8> = Vec::new();
        <V::E as Model<V::X>>::save(self.core.slabs(), self.core.len(), &mut out);
        proof {
            assert(out@.skip(0) =~= out@);
            Column::<V, C>::lemma_encoding(out@);
            V::lemma_size(out@);
        }
        out
    }

    pub(super) fn save_to_impl(&self, out: &mut Vec<u8>) -> (r: Range<usize>)
        ensures
            final(out)@ == old(out)@ + Column::<V, C>::encoding(self@),
            Column::<V, C>::valid(Column::<V, C>::encoding(self@)),
            Column::<V, C>::values(Column::<V, C>::encoding(self@)) == self@,
            Column::<V, C>::encoding(self@).len() <= V::size_bound(self@),
            r.start == old(out)@.len(),
            r.end == final(out)@.len(),
    {
        self.core.lemma_column();
        let start = out.len();
        <V::E as Model<V::X>>::save(self.core.slabs(), self.core.len(), out);
        proof {
            let d = out@.skip(start as int);
            Column::<V, C>::lemma_encoding(d);
            V::lemma_size(d);
            assert(out@ =~= old(out)@ + d);
        }
        start..out.len()
    }

    pub(super) fn save_to_unless_impl(&self, out: &mut Vec<u8>, value: Get<'_, V>) -> (r: Range<usize>)
        ensures
            final(out)@ == if only(self@, value.sv()) {
                old(out)@
            } else {
                old(out)@ + Column::<V, C>::encoding(self@)
            },
            !only(self@, value.sv()) ==> {
                &&& Column::<V, C>::valid(Column::<V, C>::encoding(self@))
                &&& Column::<V, C>::values(Column::<V, C>::encoding(self@)) == self@
                &&& Column::<V, C>::encoding(self@).len() <= V::size_bound(self@)
            },
            r.start == old(out)@.len(),
            r.end == final(out)@.len(),
    {
        if self.is_only(value) {
            out.len()..out.len()
        } else {
            self.save_to(out)
        }
    }

    pub(super) fn validate_encoding_impl(&self) -> (r: Result<(), PackError>)
        ensures
            Column::<V, C>::encoding(self@).len() <= u32::MAX ==> r is Ok,
            V::size_bound(self@) <= u32::MAX ==> r is Ok,
    {
        let bytes = self.save();
        match <V::E as Model<V::X>>::validate(bytes.as_slice()) {
            Ok(_) => Ok(()),
            Err(e) => Err(e),
        }
    }
}

impl<V: ColumnValueRef<C>, C: Codec, K: VIndex<V, C>> Column<V, C, K> {
    pub(super) fn load_impl(bytes: &[u8]) -> (r: Result<Self, PackError>)
        ensures
            Column::<V, C>::loadable(bytes@) && K::keeps(Column::<V, C>::values(bytes@)) ==> r is Ok,
            r matches Ok(c) ==> Column::<V, C>::valid(bytes@) && c@ == Column::<V, C>::values(bytes@),
    {
        proof {
            let vs = <V::E as Model<V::X>>::values(bytes@);
            <Held<V, C> as VGet>::lemma_svs(vs);
            if K::keeps(<Held<V, C> as VGet>::svs(vs)) {
                K::lemma_refuses(vs);
            }
        }
        match <V::E as Model<V::X>>::load::<K::W, K::I, K::G>(bytes, MAX_SEG, None, i64::MIN, i64::MAX) {
            Ok(core) => Ok(Column { core }),
            Err(e) => Err(e),
        }
    }

    pub(super) fn load_with_impl<'a, F: MaybeFill<Get<'a, V>>>(bytes: &[u8], opts: LoadOpts<F>) -> (r: Result<Self, PackError>)
        requires
            opts.max_segments() >= 2,
        ensures
            match fill_of(bytes@, opts.length(), opts.fill().value()) {
                Some((len, v)) => r matches Ok(c) && c@ == Seq::new(len as nat, |_i: int| v.sv()),
                None => {
                    &&& r matches Ok(c) ==> {
                        &&& Column::<V, C>::valid(bytes@)
                        &&& c@ == Column::<V, C>::values(bytes@)
                        &&& opts.length() matches Some(len) ==> c@.len() == len
                    }
                    &&& Column::<V, C>::loadable(bytes@) && K::keeps(Column::<V, C>::values(bytes@))
                        && (opts.length() matches Some(len) ==> Column::<V, C>::values(bytes@).len() == len)
                        ==> r is Ok
                },
            },
    {
        let (length, fill, max_segments) = opts.parts();
        if bytes.len() == 0 {
            if let Some(len) = length {
                if len as u64 <= i64::MAX as u64 && len <= usize::MAX - 3 {
                    if let Some(v) = fill.fill_value() {
                        return Ok(Self::filled(len, v, max_segments));
                    }
                }
            }
        }
        proof {
            let vs = <V::E as Model<V::X>>::values(bytes@);
            <Held<V, C> as VGet>::lemma_svs(vs);
            if K::keeps(<Held<V, C> as VGet>::svs(vs)) {
                K::lemma_refuses(vs);
            }
        }
        match <V::E as Model<V::X>>::load::<K::W, K::I, K::G>(bytes, max_segments, length, i64::MIN, i64::MAX) {
            Ok(core) => Ok(Column { core }),
            Err(e) => Err(e),
        }
    }
}

/// A fold of `vs` is `f` on its first value and a fold of the others.
pub(super) proof fn lemma_folded_step<B, T, F: FnMut(B, T) -> B>(f: F, a: B, vs: Seq<T>, b: B, r: B)
    requires
        vs.len() > 0,
        f.ensures((a, vs[0]), b),
        folded(f, b, vs.drop_first(), r),
    ensures
        folded(f, a, vs, r),
{
}

pub(super) proof fn lemma_runs_first_impl<T>(o: Seq<T>)
    requires
        o.len() <= usize::MAX,
    ensures
        (runs_of(o).len() == 0) == (o.len() == 0),
        o.len() > 0 ==> runs_of(o)[0].value == o[0] && 1 <= runs_of(o)[0].count <= o.len(),
    decreases o.len(),
{
    if o.len() > 0 {
        lemma_runs_first_impl(o.drop_first());
    }
}

pub(super) proof fn lemma_runs_of_impl<T>(o: Seq<T>, v: T, n: usize)
    requires
        head_run(o, v, n as int),
        o.len() <= usize::MAX,
    ensures
        runs_of(o) == seq![Run { count: n, value: v }] + runs_of(o.skip(n as int)),
    decreases n,
{
    let d = o.drop_first();
    let r = runs_of(d);
    lemma_runs_first(d);
    if n == 1 {
        assert(d =~= o.skip(1));
        if d.len() > 0 {
            assert(d[0] == o[1]);
        }
    } else {
        assert forall|i: int| 0 <= i < n - 1 implies #[trigger] d[i] == v by {
            assert(d[i] == o[i + 1]);
        }
        if n - 1 < d.len() {
            assert(d[n - 1] == o[n as int]);
        }
        lemma_runs_of_impl(d, v, (n - 1) as usize);
        assert(d.skip(n - 1) =~= o.skip(n as int));
        assert(r.drop_first() =~= runs_of(o.skip(n as int)));
    }
}

pub(super) proof fn lemma_runs_of_values_impl<G: VGet>(o: Seq<G>)
    requires
        o.len() <= usize::MAX,
    ensures
        run_values(runs_of(o)) == G::svs(o),
    decreases o.len(),
{
    G::lemma_svs(o);
    if o.len() == 0 {
        assert(run_values(runs_of(o)) =~= G::svs(o));
    } else {
        let d = o.drop_first();
        let r = runs_of(d);
        let x = o[0].sv();
        lemma_runs_of_values_impl(d);
        lemma_runs_first(d);
        G::lemma_svs(d);
        let none = Seq::<Run<G>>::empty();
        if r.len() > 0 && r[0].value == o[0] {
            let (a, b) = (Run { count: (r[0].count + 1) as usize, value: o[0] }, r.drop_first());
            lemma_run_values_push(none, a);
            lemma_run_values_push(none, r[0]);
            assert(none.push(a) =~= seq![a]);
            assert(none.push(r[0]) =~= seq![r[0]]);
            lemma_run_values_add(seq![a], b);
            lemma_run_values_add(seq![r[0]], b);
            assert(seq![r[0]] + b =~= r);
            let was = Seq::new(r[0].count as nat, |_i: int| x) + run_values(b);
            let now = Seq::new(a.count as nat, |_i: int| x) + run_values(b);
            assert(G::svs(d) == was);
            assert(run_values(runs_of(o)) == now);
            assert forall|i: int| 0 <= i < o.len() implies now[i] == #[trigger] G::svs(o)[i] by {
                if i > 0 {
                    assert(was[i - 1] == d[i - 1].sv());
                }
            }
            assert(now =~= G::svs(o));
        } else {
            let a = Run { count: 1, value: o[0] };
            lemma_run_values_push(none, a);
            assert(none.push(a) =~= seq![a]);
            lemma_run_values_add(seq![a], r);
            assert(run_values(runs_of(o)) =~= G::svs(o));
        }
    }
}

impl<'a, V: ColumnValueRef<C>, C: Codec, K: VIndex<V, C>> Iter<'a, V, C, K> {
    /// The iterator stands in the slabs of its column, in the column or
    /// at its end.
    #[verifier::type_invariant]
    pub(super) open spec fn inv(&self) -> bool {
        &&& self.it.wf::<V::X>(self.slabs@)
        &&& self.it.pos <= total(slab_lens(self.slabs@))
        &&& match self.col {
            Some(c) => self.slabs@ == c.slabs_spec(),
            None => self.slabs@.len() == 0,
        }
    }

    pub(super) proof fn lemma_items_len_impl(&self)
        ensures
            self.items().len() <= usize::MAX,
            self.rest().len() == self.items().len(),
    {
        <Get<'a, V> as VGet>::lemma_svs(self.items());
    }

    pub(super) proof fn lemma_source_impl(tracked &self)
        ensures
            match self.source() {
                Some(c) => self.column() == c@,
                None => self.column().len() == 0,
            },
    {
        use_type_invariant(self);
        match self.col {
            Some(c) => {
                c.lemma_view();
                let o = vals_of::<V::X>(c.slabs_spec());
                lemma_vals_own::<V::E, V::X>(c.slabs_spec());
                assert(c@ =~= held::<V, C>(o));
                lemma_held::<V, C>(o);
                <Held<V, C> as VGet>::lemma_svs(c@);
            },
            None => {
                assert(vals_of::<V::X>(self.slabs@) =~= Seq::empty());
                <Get<'a, V> as VGet>::lemma_svs(vals_of::<V::X>(self.slabs@));
            },
        }
    }

    /// What is left is the column's values in the window.
    pub(super) proof fn lemma_window(&self)
        requires
            self.it.wf::<V::X>(self.slabs@),
        ensures
            self.spec_pos() == self.it.pos,
            self.spec_end_pos() == self.it.pos + self.it.items_left,
            self.column().len() == total(slab_lens(self.slabs@)),
            self.rest().len() == self.items().len(),
            self.rest() == window(self.column(), self.spec_pos() as int, self.spec_end_pos() as int),
    {
        self.it.lemma_out_len::<V::X>(self.slabs@);
        self.it.lemma_out_window::<V::X>(self.slabs@);
        <Get<'a, V> as VGet>::lemma_svs(vals_of::<V::X>(self.slabs@));
        <Get<'a, V> as VGet>::lemma_svs(self.items());
        lemma_svs_window(vals_of::<V::X>(self.slabs@), self.it.pos as int, self.it.pos + self.it.items_left);
    }

    pub(super) proof fn lemma_rest_impl(tracked &self)
        ensures
            self.pos() <= self.end_pos(),
            self.pos() <= self.column().len(),
            self.rest() == window(self.column(), self.pos() as int, self.end_pos() as int),
    {
        use_type_invariant(self);
        self.lemma_window();
    }

    #[inline]
    pub(super) fn pos_impl(&self) -> (r: usize)
        ensures
            r == self.pos(),
    {
        proof {
            use_type_invariant(self);
            self.lemma_window();
        }
        self.it.pos
    }

    #[inline]
    pub(super) fn end_pos_impl(&self) -> (r: usize)
        ensures
            r == self.end_pos(),
    {
        proof {
            use_type_invariant(self);
            self.lemma_window();
        }
        self.it.pos + self.it.items_left
    }

    #[inline]
    pub(super) fn get_slab_impl(&self) -> usize
    {
        self.it.slab_idx
    }

    pub(super) fn suspend_impl(&self) -> (s: IterState)
        ensures
            s.pos() == self.pos(),
            s.end_pos() == self.end_pos(),
            self.rest() == window(self.column(), s.pos() as int, s.end_pos() as int),
            match self.source() {
                Some(c) => s.fits(&c),
                None => true,
            },
    {
        proof {
            use_type_invariant(self);
            self.lemma_window();
        }
        let (slab_idx, slab_consumed, pos, items_left) = self.it.suspended::<V::X>(self.slabs);
        let counter = match self.col {
            Some(c) => c.counter(),
            None => 0,
        };
        IterState { counter, num_slabs: self.slabs.len(), slab_idx, pos, items_left, slab_consumed }
    }

    pub(super) fn set_max_impl(&mut self, end: usize)
        ensures
            final(self).column() == old(self).column(),
            final(self).source() == old(self).source(),
            final(self).pos() == old(self).pos(),
            final(self).end_pos() == if end >= old(self).pos() {
                end
            } else {
                old(self).pos()
            },
            final(self).rest() == window(old(self).column(), old(self).pos() as int, end as int),
    {
        proof {
            use_type_invariant(&*self);
        }
        self.it.set_max::<V::X>(self.slabs, end);
        proof {
            self.lemma_window();
        }
    }

    pub(super) fn advance_to_impl(&mut self, target: usize)
        ensures
            final(self).column() == old(self).column(),
            final(self).source() == old(self).source(),
            final(self).rest() == past(old(self).rest(), target - old(self).pos()),
            target <= old(self).pos() ==> {
                &&& final(self).pos() == old(self).pos()
                &&& final(self).end_pos() == old(self).end_pos()
            },
            old(self).pos() < target <= old(self).pos() + old(self).rest().len() ==> {
                &&& final(self).pos() == target
                &&& final(self).end_pos() == old(self).end_pos()
            },
            target > old(self).pos() + old(self).rest().len() ==> {
                &&& final(self).pos() == old(self).pos() + old(self).rest().len()
                &&& final(self).end_pos() == final(self).pos()
            },
    {
        proof {
            <Get<'a, V> as VGet>::lemma_svs(self.items());
        }
        if target > self.it.pos {
            self.nth(target - self.it.pos - 1);
            proof {
                <Get<'a, V> as VGet>::lemma_svs(self.items());
            }
        }
    }

    pub(super) fn advance_by_impl(&mut self, amount: usize)
        ensures
            final(self).column() == old(self).column(),
            final(self).source() == old(self).source(),
            final(self).rest() == past(old(self).rest(), amount as int),
            amount <= old(self).rest().len() ==> {
                &&& final(self).pos() == old(self).pos() + amount
                &&& final(self).end_pos() == old(self).end_pos()
            },
            amount > old(self).rest().len() ==> {
                &&& final(self).pos() == old(self).pos() + old(self).rest().len()
                &&& final(self).end_pos() == final(self).pos()
            },
    {
        proof {
            use_type_invariant(&*self);
            // if the sum saturates, both move past everything left
            self.lemma_window();
        }
        self.advance_to(self.it.pos.saturating_add(amount));
    }

    pub(super) fn shift_impl(&mut self, range: Range<usize>)
        ensures
            final(self).column() == old(self).column(),
            final(self).source() == old(self).source(),
            final(self).rest() == window(
                old(self).column(),
                if range.start <= old(self).pos() {
                    old(self).pos() as int
                } else {
                    range.start as int
                },
                range.end as int,
            ),
            range.start <= old(self).pos() ==> {
                &&& final(self).pos() == old(self).pos()
                &&& final(self).end_pos() == if range.end >= old(self).pos() {
                    range.end
                } else {
                    old(self).pos()
                }
            },
            old(self).pos() < range.start <= range.end && range.start <= old(self).column().len() ==> {
                &&& final(self).pos() == range.start
                &&& final(self).end_pos() == range.end
            },
            old(self).pos() < range.start && (range.start > range.end || range.start > old(self).column().len()) ==> {
                &&& final(self).pos() == if range.end <= old(self).pos() {
                    old(self).pos() as int
                } else if range.end <= old(self).column().len() {
                    range.end as int
                } else {
                    old(self).column().len() as int
                }
                &&& final(self).end_pos() == final(self).pos()
            },
    {
        proof {
            use_type_invariant(&*self);
            self.lemma_window();
        }
        self.set_max(range.end);
        self.advance_to(range.start);
    }

    pub(super) fn shift_next_impl(&mut self, range: Range<usize>) -> (r: Option<Get<'a, V>>)
        requires
            range.start >= old(self).pos(),
        ensures
            final(self).column() == old(self).column(),
            final(self).source() == old(self).source(),
            ({
                let w = window(old(self).column(), range.start as int, range.end as int);
                &&& w.len() > 0 ==> {
                    &&& r matches Some(v) && v.sv() == w[0]
                    &&& final(self).rest() == w.skip(1)
                    &&& final(self).pos() == range.start + 1
                    &&& final(self).end_pos() == range.end
                }
                &&& w.len() == 0 ==> {
                    &&& r is None
                    &&& final(self).rest().len() == 0
                    &&& final(self).pos() == if range.end <= old(self).pos() {
                        old(self).pos() as int
                    } else if range.end <= old(self).column().len() {
                        range.end as int
                    } else {
                        old(self).column().len() as int
                    }
                    &&& final(self).end_pos() == final(self).pos()
                }
            }),
    {
        let ghost (c, p) = (self.column(), self.it.pos as int);
        proof {
            use_type_invariant(&*self);
            self.lemma_window();
        }
        self.set_max(range.end);
        let ghost o1 = self.rest();
        proof {
            <Get<'a, V> as VGet>::lemma_svs(self.items());
        }
        let r = self.nth(range.start - self.it.pos);
        proof {
            let (w, n) = (window(c, range.start as int, range.end as int), range.start - p);
            <Get<'a, V> as VGet>::lemma_svs(self.items());
            if n < o1.len() {
                assert(w =~= o1.skip(n));
                assert(w.skip(1) =~= o1.skip(n + 1));
            }
        }
        r
    }

    #[inline(always)]
    pub(super) fn next_run_impl(&mut self) -> (r: Option<Run<Get<'a, V>>>)
        ensures
            final(self).column() == old(self).column(),
            final(self).source() == old(self).source(),
            match r {
                Some(run) => {
                    &&& head_run(old(self).items(), run.value, run.count as int)
                    &&& head_run(old(self).rest(), run.value.sv(), run.count as int)
                    &&& final(self).items() == old(self).items().skip(run.count as int)
                    &&& final(self).rest() == old(self).rest().skip(run.count as int)
                    &&& final(self).pos() == old(self).pos() + run.count
                    &&& final(self).end_pos() == old(self).end_pos()
                },
                None => {
                    &&& old(self).items().len() == 0
                    &&& old(self).rest().len() == 0
                    &&& final(self).items().len() == 0
                    &&& final(self).rest().len() == 0
                    &&& final(self).pos() == old(self).pos()
                    &&& final(self).end_pos() == old(self).pos()
                },
            },
    {
        proof {
            use_type_invariant(&*self);
            self.it.lemma_out_len::<V::X>(self.slabs@);
        }
        let ghost o = self.it.out::<V::X>(self.slabs@);
        proof {
            <Get<'a, V> as VGet>::lemma_svs(o);
        }
        let max = self.it.items_left;
        if max == 0 {
            return None;
        }
        let first = self.it.run_once::<V::X>(self.slabs, max)?;
        let run = if self.it.slab_remaining == 0 && self.it.items_left > 0 {
            // The run reaches its slab's end, and may go on in the next.
            // Whether it does is a comparison of two values, which is not
            // known to return; so it is made on a copy of where the
            // iterator stands, and the iterator stands rightly whatever
            // comes of it.
            let mut it = self.it.dup();
            let count = it.merge_on::<V::X>(self.slabs, first.value, first.count, max - first.count, Ghost(o));
            self.it = it;
            Run { count, value: first.value }
        } else {
            first
        };
        proof {
            let n = run.count as int;
            lemma_svs_sub(o, n, o.len() as int);
            assert forall|i: int| 0 <= i < n implies #[trigger] o[i] == run.value by {
                assert(o.take(n)[i] == run.value);
            }
            if n < o.len() && o[n].sv() == run.value.sv() {
                <Get<'a, V> as VGet>::lemma_sv_inj(o[n], run.value);
            }
        }
        Some(run)
    }

    pub(super) fn runs_impl(self) -> (r: Runs<'a, V, C, K>)
        ensures
            r.iter() == self,
    {
        Runs(self)
    }

    #[allow(clippy::while_let_loop)]
    pub(super) fn scan_to_value_impl(&mut self, target: Get<'_, V>) -> (r: Option<usize>)
        ensures
            final(self).column() == old(self).column(),
            final(self).source() == old(self).source(),
            match r {
                Some(at) => {
                    &&& old(self).pos() <= at < old(self).pos() + old(self).rest().len()
                    &&& old(self).rest()[at - old(self).pos()] == target.sv()
                    &&& forall|i: int| 0 <= i < at - old(self).pos() ==> #[trigger] old(self).rest()[i] != target.sv()
                    &&& final(self).items() == old(self).items().skip(at - old(self).pos() + 1)
                    &&& final(self).rest() == old(self).rest().skip(at - old(self).pos() + 1)
                    &&& final(self).pos() == at + 1
                    &&& final(self).end_pos() == old(self).end_pos()
                },
                None => {
                    &&& forall|i: int| 0 <= i < old(self).rest().len() ==> #[trigger] old(self).rest()[i] != target.sv()
                    &&& final(self).items().len() == 0
                    &&& final(self).rest().len() == 0
                    &&& final(self).pos() == old(self).pos() + old(self).rest().len()
                    &&& final(self).end_pos() == final(self).pos()
                },
            },
    {
        proof {
            self.lemma_rest_impl();
        }
        let end = self.end_pos();
        let mut probe = self.clone();
        proof {
            probe.lemma_rest_impl();
        }
        let mut pos = probe.pos();
        let ghost o = self.items();
        let ghost p0 = self.pos();
        loop
            invariant
                self.items() == o,
                self.pos() == p0,
                self.end_pos() == end,
                self.column() == old(self).column(),
                self.source() == old(self).source(),
                o == old(self).items(),
                p0 == old(self).pos(),
                end == old(self).end_pos(),
                p0 <= pos,
                pos - p0 <= o.len(),
                probe.pos() == pos,
                probe.items() == o.skip(pos - p0),
                forall|i: int| 0 <= i < pos - p0 ==> (#[trigger] o[i]).sv() != target.sv(),
            ensures
                forall|i: int| 0 <= i < o.len() ==> (#[trigger] o[i]).sv() != target.sv(),
            decreases o.len() - (pos - p0),
        {
            let ghost left = probe.items();
            let run = match probe.next_run() {
                Some(run) => run,
                None => {
                    break ;
                },
            };
            proof {
                assert forall|i: int| pos - p0 <= i < pos - p0 + run.count implies #[trigger] o[i] == run.value by {
                    assert(left[i - (pos - p0)] == run.value);
                }
            }
            if V::same(run.value, target) {
                proof {
                    <Get<'a, V> as VGet>::lemma_svs(o);
                }
                if pos == self.it.pos {
                    // the value is where the iterator stands
                    self.next();
                } else {
                    self.nth(pos - self.it.pos);
                }
                return Some(pos);
            }
            pos += run.count;
        }
        self.advance_to(end);
        proof {
            <Get<'a, V> as VGet>::lemma_svs(o);
            <Get<'a, V> as VGet>::lemma_svs(self.items());
        }
        None
    }

    pub(super) fn seek_to_value_impl<R: RangeBounds<usize>>(&mut self, target: Get<'_, V>, range: R) -> (r: Range<usize>)
        ensures
            final(self).column() == old(self).column(),
            final(self).source() == old(self).source(),
            ({
                let (p, o, t) = (old(self).pos(), old(self).rest(), target.sv());
                // where the range starts and ends
                let (rs, re) = (
                    bound_start(range.spec_start_bound()),
                    bound_end(range.spec_start_bound(), range.spec_end_bound(), old(self).end_pos() as int),
                );
                // how far the iterator moves before it searches, and the
                // values searched
                let a = if rs <= p {
                    0
                } else if rs - p <= o.len() {
                    rs - p
                } else {
                    o.len() as int
                };
                let w = window(o, a, re - p);
                &&& p + a <= r.start <= r.end <= p + a + w.len()
                &&& final(self).pos() == r.start
                &&& final(self).rest() == o.skip(r.start - p)
                &&& final(self).end_pos() == if rs <= p + o.len() {
                    old(self).end_pos()
                } else {
                    final(self).pos()
                }
                &&& sorted_for::<V>(w, t) ==> forall|j: int|
                    a <= j < a + w.len() ==> {
                        &&& (p + j < r.start) == V::below(#[trigger] o[j], t)
                        &&& (r.start <= p + j < r.end) == (o[j] == t)
                        &&& (r.end <= p + j) == V::below(t, o[j])
                    }
            }),
    {
        let (start, end) = bounds_exec(&range, self.end_pos());
        self.seek_within(target, start..end)
    }

    /// [`seek_to_value`](Iter::seek_to_value) over the places
    /// `range.start..range.end`.
    fn seek_within(&mut self, target: Get<'_, V>, range: Range<usize>) -> (r: Range<usize>)
        ensures
            final(self).column() == old(self).column(),
            final(self).source() == old(self).source(),
            ({
                let (p, o, t) = (old(self).pos(), old(self).rest(), target.sv());
                // how far the iterator moves before it searches, and the
                // values searched
                let a = if range.start <= p {
                    0
                } else if range.start - p <= o.len() {
                    range.start - p
                } else {
                    o.len() as int
                };
                let w = window(o, a, range.end - p);
                &&& p + a <= r.start <= r.end <= p + a + w.len()
                &&& final(self).pos() == r.start
                &&& final(self).rest() == o.skip(r.start - p)
                &&& final(self).end_pos() == if range.start <= p + o.len() {
                    old(self).end_pos()
                } else {
                    final(self).pos()
                }
                &&& sorted_for::<V>(w, t) ==> forall|j: int|
                    a <= j < a + w.len() ==> {
                        &&& (p + j < r.start) == V::below(#[trigger] o[j], t)
                        &&& (r.start <= p + j < r.end) == (o[j] == t)
                        &&& (r.end <= p + j) == V::below(t, o[j])
                    }
            }),
    {
        proof {
            self.lemma_rest_impl();
        }
        let ghost (p, o, c) = (self.pos() as int, self.rest(), self.column());
        // the iterator cannot leave its window, so neither can the search
        let end = range.end.min(self.end_pos());
        if range.start > self.it.pos {
            self.advance_to(range.start);
        }
        proof {
            use_type_invariant(&*self);
            self.lemma_window();
        }
        let here = self.it.pos;
        let ghost a = here - p;
        let slabs = self.slabs;
        let index = match self.col {
            Some(col) => {
                col.lemma_column();
                match col.index_opt() {
                    Some(index) => index,
                    None => {
                        return here..here;
                    },
                }
            },
            None => {
                return here..here;
            },
        };
        // The search is a comparison of values, which is not known to
        // return; so it is made on a copy of where the iterator stands.
        let mut it = self.it.dup();
        it.set_max::<V::X>(slabs, end);
        let ghost w = <Get<'a, V> as VGet>::svs(it.out::<V::X>(slabs@));
        proof {
            it.lemma_out_window::<V::X>(slabs@);
            lemma_svs_window(vals_of::<V::X>(slabs@), here as int, here + it.items_left);
            assert(w =~= window(o, a, range.end - p));
        }
        let r = seek_sorted::<V, C, K::W, K::I>(it, slabs, index, target);
        self.advance_to(r.0);
        proof {
            assert forall|j: int| a <= j < a + w.len() implies w[j - a] == #[trigger] o[j] by {}
        }
        r.0..r.1
    }

    /// Where item `t` of the column is, as the column's index has it:
    /// what a long move asks before it goes. (A column that an edit's
    /// panic left without its parts has no slabs and no index, and every
    /// item is past its end.)
    #[inline(always)]
    pub(super) fn look(&self, t: usize) -> (far: (usize, usize, usize))
        ensures
            far_ok(self.slabs@, t, far),
    {
        proof {
            use_type_invariant(self);
        }
        let n = self.slabs.len();
        match self.col {
            Some(c) => {
                c.lemma_column();
                match c.index_opt() {
                    Some(ix) => {
                        let r = ix.ix_find(t);
                        proof {
                            assert(ix.ws().map_values(|w: K::W| <K::I as IndexOps<K::W>>::wlen(w))
                                =~= wlens::<K::W, K::I>(ix.ws()));
                        }
                        if r.0 >= n {
                            // past the slabs, the whole position lies
                            // beyond them
                            (n, 0, c.len())
                        } else {
                            (r.0, r.1, t - r.1)
                        }
                    },
                    None => (n, 0, 0),
                }
            },
            None => (n, 0, 0),
        }
    }

    /// `nth`, for a move that may leave the slab the iterator stands in:
    /// a function of its own, so that a caller of `nth` has the short
    /// move inline and calls for the long one.
    #[inline(never)]
    pub(super) fn nth_far(&mut self, n: usize) -> (r: Option<Get<'a, V>>)
        ensures
            n < old(self).items().len() ==> {
                &&& r == Some(old(self).items()[n as int])
                &&& final(self).items() == old(self).items().skip(n + 1)
                &&& r matches Some(v) && v.sv() == old(self).rest()[n as int]
                &&& final(self).rest() == old(self).rest().skip(n + 1)
                &&& final(self).pos() == old(self).pos() + n + 1
                &&& final(self).end_pos() == old(self).end_pos()
            },
            n >= old(self).items().len() ==> {
                &&& r is None
                &&& final(self).items().len() == 0
                &&& final(self).pos() == old(self).pos() + old(self).items().len()
                &&& final(self).end_pos() == final(self).pos()
            },
            final(self).column() == old(self).column(),
            final(self).source() == old(self).source(),
    {
        proof {
            use_type_invariant(&*self);
        }
        let ghost o = self.it.out::<V::X>(self.slabs@);
        let far = match self.it.far_target(n) {
            Some(t) => self.look(t),
            None => (0, 0, 0),
        };
        let r = self.it.nth_at::<V::X>(self.slabs, n, far);
        proof {
            <Get<'a, V> as VGet>::lemma_svs(o);
            <Get<'a, V> as VGet>::lemma_svs(self.it.out::<V::X>(self.slabs@));
            if n < o.len() {
                lemma_svs_sub(o, n + 1, o.len() as int);
            }
        }
        r
    }
}

impl<'a, V: ColumnValueRef<C>, C: Codec, K: VIndex<V, C>> Iter<'a, V, C, K> {
    #[inline(always)]
    pub(super) fn next_impl(&mut self) -> (r: Option<Get<'a, V>>)
        ensures
            match r {
                Some(v) => {
                    &&& old(self).rest().len() > 0
                    &&& v.sv() == old(self).rest()[0]
                    &&& final(self).rest() == old(self).rest().skip(1)
                    &&& final(self).pos() == old(self).pos() + 1
                    &&& final(self).end_pos() == old(self).end_pos()
                },
                None => {
                    &&& old(self).rest().len() == 0
                    &&& final(self).rest().len() == 0
                    &&& final(self).pos() == old(self).pos()
                    &&& final(self).end_pos() == old(self).pos()
                },
            },
            final(self).column() == old(self).column(),
            final(self).source() == old(self).source(),
            // what Verus asks of every iterator's `next`
            old(self).items().len() > 0 ==> final(self).items() == old(self).items().drop_first() && r == Some(old(self).items()[0]),
            old(self).items().len() == 0 ==> final(self).items() == old(self).items() && r is None,
    {
        proof {
            use_type_invariant(&*self);
            self.it.lemma_out_len::<V::X>(self.slabs@);
        }
        let ghost o = self.it.out::<V::X>(self.slabs@);
        let r = self.it.next_inline::<V::X>(self.slabs);
        proof {
            let o1 = self.it.out::<V::X>(self.slabs@);
            <Get<'a, V> as VGet>::lemma_svs(o);
            <Get<'a, V> as VGet>::lemma_svs(o1);
            if r is Some {
                lemma_svs_sub(o, 1, o.len() as int);
                assert(o.skip(1) =~= o.drop_first());
            } else {
                assert(o1 =~= o);
            }
        }
        r
    }

    #[inline]
    pub(super) fn nth_impl(&mut self, n: usize) -> (r: Option<Get<'a, V>>)
        ensures
            n < old(self).items().len() ==> {
                &&& r == Some(old(self).items()[n as int])
                &&& final(self).items() == old(self).items().skip(n + 1)
                &&& r matches Some(v) && v.sv() == old(self).rest()[n as int]
                &&& final(self).rest() == old(self).rest().skip(n + 1)
                &&& final(self).pos() == old(self).pos() + n + 1
                &&& final(self).end_pos() == old(self).end_pos()
            },
            n >= old(self).items().len() ==> {
                &&& r is None
                &&& final(self).items().len() == 0
                &&& final(self).pos() == old(self).pos() + old(self).items().len()
                &&& final(self).end_pos() == final(self).pos()
            },
            final(self).column() == old(self).column(),
            final(self).source() == old(self).source(),
    {
        proof {
            use_type_invariant(&*self);
        }
        if n < self.it.items_left && n < self.it.slab_remaining {
            // within the slab the iterator stands in, and its window:
            // the index is not asked
            let ghost o = self.it.out::<V::X>(self.slabs@);
            let r = self.it.nth_at::<V::X>(self.slabs, n, (0, 0, 0));
            proof {
                <Get<'a, V> as VGet>::lemma_svs(o);
                <Get<'a, V> as VGet>::lemma_svs(self.it.out::<V::X>(self.slabs@));
                if n < o.len() {
                    lemma_svs_sub(o, n + 1, o.len() as int);
                }
            }
            r
        } else {
            self.nth_far(n)
        }
    }

    #[inline]
    pub(super) fn size_hint_impl(&self) -> (r: (usize, Option<usize>))
        ensures
            r.0 == self.items().len(),
            r.0 == self.rest().len(),
            r.1 == Some(r.0),
    {
        proof {
            use_type_invariant(self);
            self.it.lemma_here::<V::X>(self.slabs@);
            <Get<'a, V> as VGet>::lemma_svs(self.items());
        }
        // the window can run past the column's end
        let left = match self.col {
            Some(col) => {
                col.lemma_column();
                self.it.items_left.min(col.len().saturating_sub(self.it.pos))
            },
            None => 0,
        };
        (left, Some(left))
    }

    pub(super) fn fold_impl<B, F>(self, init: B, f: F) -> (r: B) where F: FnMut(B, Get<'a, V>) -> B
        requires
            forall|b: B, v: Get<'a, V>| #[trigger] f.requires((b, v)),
        ensures
            folded(f, init, self.items(), r),
    {
        let ghost all = self.it.out::<V::X>(self.slabs@);
        let ghost f0 = f;
        let mut me = self;
        let mut f = f;
        let mut acc = init;
        loop
            invariant
                f == f0,
                forall|b: B, v: Get<'a, V>| #[trigger] f.requires((b, v)),
                forall|r: B| folded(f, acc, me.it.out::<V::X>(me.slabs@), r) ==> folded(f, init, all, r),
            ensures
                folded(f, init, all, acc),
            decreases me.it.items_left,
        {
            proof {
                use_type_invariant(&me);
            }
            if me.it.items_left == 0 {
                proof {
                    assert(me.it.out::<V::X>(me.slabs@).len() == 0);
                }
                break ;
            }
            let max = me.it.items_left;
            let ghost o = me.it.out::<V::X>(me.slabs@);
            proof {
                me.it.lemma_out_len::<V::X>(me.slabs@);
            }
            match me.it.run_once::<V::X>(me.slabs, max) {
                Some(run) => {
                    proof {
                        assert(o.skip(0) =~= o);
                    }
                    let mut k: usize = 0;
                    while k < run.count
                        invariant
                            k <= run.count <= o.len(),
                            o.take(run.count as int) == Seq::new(run.count as nat, |i: int| run.value),
                            f == f0,
                            forall|b: B, v: Get<'a, V>| #[trigger] f.requires((b, v)),
                            forall|r: B| folded(f, acc, o.skip(k as int), r) ==> folded(f, init, all, r),
                        decreases run.count - k,
                    {
                        let ghost a = acc;
                        acc = f(acc, run.value);
                        proof {
                            let now = o.skip(k as int);
                            assert(now[0] == o.take(run.count as int)[k as int]);
                            assert(now.drop_first() =~= o.skip(k + 1));
                            assert forall|r: B| folded(f, acc, o.skip(k + 1), r) implies folded(f, init, all, r) by {
                                lemma_folded_step(f, a, now, acc, r);
                            }
                        }
                        k += 1;
                    }
                },
                None => {
                    break ;
                },
            }
        }
        acc
    }
}

impl<'a, V: ColumnValueRef<C>, C: Codec, K: VIndex<V, C>> Iter<'a, V, C, K> {
    pub(super) fn clone_impl(&self) -> (r: Self)
        ensures
            r.items() == self.items(),
            r.column() == self.column(),
            r.source() == self.source(),
            r.pos() == self.pos(),
            r.end_pos() == self.end_pos(),
    {
        proof {
            use_type_invariant(self);
        }
        Iter { it: self.it.dup(), slabs: self.slabs, col: self.col }
    }
}

impl<'a, V: ColumnValueRef<C>, C: Codec, K: VIndex<V, C>> Iter<'a, V, C, K> {
    pub(super) fn default_impl() -> (r: Self)
        ensures
            r.items().len() == 0,
            r.rest().len() == 0,
            r.column().len() == 0,
            r.source() is None,
            r.pos() == 0,
            r.end_pos() == 0,
    {
        let r = Iter { it: ICore::empty::<V::X>(), slabs: &[], col: None };
        proof {
            <Get<'a, V> as VGet>::lemma_svs(r.items());
            r.it.lemma_out_len::<V::X>(r.slabs@);
            assert(slab_lens(r.slabs@) =~= Seq::<nat>::empty());
            <Get<'a, V> as VGet>::lemma_svs(vals_of::<V::X>(r.slabs@));
        }
        r
    }
}

impl IterState {
    pub(super) proof fn lemma_fits_impl<V: ColumnValueRef<C>, C: Codec, K: VIndex<V, C>>(&self, column: &Column<V, C, K>)
        requires
            self.fits(column),
        ensures
            self.edits() == column.edits(),
    {
    }

    pub(super) fn try_resume_impl<'a, V: ColumnValueRef<C>, C: Codec, K: VIndex<V, C>>(&self, column: &'a Column<V, C, K>) -> (r: Result<
        Iter<'a, V, C, K>,
        PackError,
    >)
        ensures
            r is Ok <==> self.fits(column),
            r is Ok ==> self.edits() == column.edits(),
            r matches Ok(it) ==> {
                &&& it.source() == Some(*column)
                &&& it.column() == column@
                &&& it.pos() == self.pos()
                &&& it.rest() == window(column@, self.pos() as int, self.end_pos() as int)
                &&& it.end_pos() == self.end_pos() || (self.pos() == column@.len() && it.end_pos() == self.pos())
            },
    {
        if self.counter != column.core.counter() {
            return Err(invalid_resume());
        }
        let slabs = column.core.slabs();
        if self.num_slabs != slabs.len() {
            return Err(invalid_resume());
        }
        column.lemma_read();
        proof {
            <Held<V, C> as VGet>::lemma_svs(column.core@);
        }
        if self.pos > column.core.len() {
            return Err(invalid_resume());
        }
        let it = match column.core.resume::<V::X>(self.slab_idx, self.slab_consumed, self.pos, self.items_left) {
            Some(it) => it,
            None => {
                return Err(invalid_resume());
            },
        };
        let r = Iter { it, slabs, col: Some(&column.core) };
        proof {
            r.lemma_window();
        }
        Ok(r)
    }
}

impl<'a, V: ColumnValueRef<C>, C: Codec, K: VIndex<V, C>> Runs<'a, V, C, K> {
    #[inline(always)]
    pub(super) fn next_impl(&mut self) -> (r: Option<Run<Get<'a, V>>>)
        ensures
            final(self).iter().column() == old(self).iter().column(),
            final(self).iter().source() == old(self).iter().source(),
            match r {
                Some(run) => {
                    &&& head_run(old(self).iter().items(), run.value, run.count as int)
                    &&& head_run(old(self).iter().rest(), run.value.sv(), run.count as int)
                    &&& final(self).iter().items() == old(self).iter().items().skip(run.count as int)
                    &&& final(self).iter().rest() == old(self).iter().rest().skip(run.count as int)
                    &&& final(self).iter().pos() == old(self).iter().pos() + run.count
                    &&& final(self).iter().end_pos() == old(self).iter().end_pos()
                },
                None => {
                    &&& old(self).iter().items().len() == 0
                    &&& old(self).iter().rest().len() == 0
                    &&& final(self).iter().items().len() == 0
                    &&& final(self).iter().rest().len() == 0
                    &&& final(self).iter().pos() == old(self).iter().pos()
                    &&& final(self).iter().end_pos() == old(self).iter().pos()
                },
            },
            // what Verus asks of every iterator's `next`
            runs_of(old(self).iter().items()).len() > 0 ==> runs_of(final(self).iter().items()) == runs_of(old(self).iter().items()).drop_first() && r == Some(runs_of(old(self).iter().items())[0]),
            runs_of(old(self).iter().items()).len() == 0 ==> runs_of(final(self).iter().items()) == runs_of(old(self).iter().items()) && r is None,
    {
        let ghost o = self.0.items();
        let r = self.0.next_run();
        proof {
            if let Some(run) = r {
                lemma_runs_of(o, run.value, run.count);
            }
        }
        r
    }
}

pub(super) proof fn vspec_lemma_below_option_t<T: RleValue>(a: Option<T>, b: Option<T>, c: Option<T>)
    ensures
        !<Option<T> as VSpec>::below(a, a),
        <Option<T> as VSpec>::below(a, b) && <Option<T> as VSpec>::below(b, c) ==> <Option<T> as VSpec>::below(a, c),
        a == b || <Option<T> as VSpec>::below(a, b) || <Option<T> as VSpec>::below(b, a),
{
    if let (Some(x), Some(y)) = (a, b) {
        T::lemma_num(x, y);
    }
}

pub(super) proof fn vvalue_lemma_size_t<T: RleValue, C: Codec>(d: Seq<u8>)
    requires
        saved::<T, C>(d),
    ensures
        d.len() <= <T as ColumnValueRef<C>>::size_bound(saved_values::<T, C>(d)),
{
    <RleModel<VWrite<T, C>> as Model<RleReads<VOps<T, C>>>>::lemma_size(d);
    lemma_plain_sizes::<T, C>(slab_values::<VOps<T, C>>(d));
}

#[inline(always)]
pub(super) fn vvalue_compare_t<T: RleValue>(a: T, b: T) -> (r: Ordering)
    ensures
        (r is Less) == <T as VSpec>::below(a.sv(), b.sv()),
        (r is Greater) == <T as VSpec>::below(b.sv(), a.sv()),
        (r is Equal) == (a.sv() == b.sv()),
{
    proof {
        T::lemma_num(a, b);
    }
    <T as RleValue>::compare(&a, &b)
}

pub(super) proof fn vvalue_lemma_size_option_t<T: RleValue, C: Codec>(d: Seq<u8>)
    requires
        saved::<Option<T>, C>(d),
    ensures
        d.len() <= <Option<T> as ColumnValueRef<C>>::size_bound(saved_values::<Option<T>, C>(d)),
{
    <RleModel<VOptWrite<T, C>> as Model<RleReads<VOptOps<T, C>>>>::lemma_size(d);
    lemma_opt_sizes::<T, C>(slab_values::<VOptOps<T, C>>(d));
}

#[inline(always)]
pub(super) fn vvalue_same_option_t<T: RleValue>(a: Option<T>, b: Option<T>) -> (r: bool)
    ensures
        r == (a.sv() == b.sv()),
{
    match (a, b) {
        (Some(x), Some(y)) => <T as RleValue>::same(&x, &y),
        (None, None) => true,
        _ => false,
    }
}

#[inline(always)]
pub(super) fn vvalue_compare_option_t<T: RleValue>(a: Option<T>, b: Option<T>) -> (r: Ordering)
    ensures
        (r is Less) == <Option<T> as VSpec>::below(a.sv(), b.sv()),
        (r is Greater) == <Option<T> as VSpec>::below(b.sv(), a.sv()),
        (r is Equal) == (a.sv() == b.sv()),
{
    match (a, b) {
        (Some(x), Some(y)) => {
            proof {
                T::lemma_num(x, y);
            }
            <T as RleValue>::compare(&x, &y)
        },
        (None, None) => Ordering::Equal,
        (None, Some(_)) => Ordering::Less,
        (Some(_), None) => Ordering::Greater,
    }
}

#[inline(always)]
pub(super) fn vvalue_compare_bool(a: bool, b: bool) -> (r: Ordering)
    ensures
        (r is Less) == <bool as VSpec>::below(a.sv(), b.sv()),
        (r is Greater) == <bool as VSpec>::below(b.sv(), a.sv()),
        (r is Equal) == (a.sv() == b.sv()),
{
    if a == b {
        Ordering::Equal
    } else if b {
        Ordering::Less
    } else {
        Ordering::Greater
    }
}

} // verus!

impl<V: ColumnValueRef<C>, C: Codec, K: VIndex<V, C>> std::fmt::Debug for Runs<'_, V, C, K> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("Runs").field(&self.0).finish()
    }
}

impl<V: ColumnValueRef<C>, C: Codec, K: VIndex<V, C>> std::fmt::Debug for Iter<'_, V, C, K> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Iter")
            .field("pos", &self.it.pos)
            .field("items_left", &self.it.items_left)
            .finish()
    }
}

// The two functions of this block print and compare, and Verus does not
// check them. They read the column through checked functions that ask
// nothing of a caller.
impl<V: ColumnValueRef<C>, C: Codec, K: VIndex<V, C>> Column<V, C, K> {
    /// Print a line for each slab, as [`Column`](crate::Column) does:
    /// where in the column the slab starts, how many values and runs it
    /// holds, its first run and its last.
    ///
    /// The numbers and the runs are read by checked code; the printing
    /// is not checked. Nothing is specified of what is printed: it tells
    /// how the column lies in memory.
    pub fn dump_slabs<'a>(&'a self)
    where
        Get<'a, V>: std::fmt::Debug,
    {
        let mut offset = 0;
        let mut i = 0;
        while let Some(s) = self.slab_runs(i) {
            log!(
                "  slab[{i}]: offset={offset} len={} segments={} first={:?} last={:?}",
                s.len,
                s.segments,
                s.first,
                s.last
            );
            offset += s.len;
            i += 1;
        }
    }

    /// Check at run time how the column lies in memory, and panic with
    /// what is wrong, as `Column::check_invariants` does: every slab
    /// holds values, and no more runs than the budget (but for one value
    /// too long to share a slab); its bytes read through as the values
    /// and runs it counts; no two neighbours should have been merged;
    /// the lengths add up to the column's; and the index counts the
    /// slabs and their lengths.
    ///
    /// A tool for tests. Verus does not check it, and it promises
    /// nothing. What it compares of the bytes, the lengths and the index
    /// is proved of every column; the budget and the merging are the
    /// layout policy, which is not proved.
    ///
    /// The merging is the policy of edits, and a load does not keep it:
    /// it cuts slabs of half the budget, and the last slab holds what is
    /// left, however little. So this panics for some columns that were
    /// loaded, with nothing wrong with them, as `Column::check_invariants`
    /// does for the same columns.
    #[doc(hidden)]
    pub fn check_invariants(&self) {
        let slabs = self.core.slabs();
        let max = self.core.max_segments();
        let min = (max / 4).max(1);
        if self.core.len() == 0 {
            assert!(slabs.len() <= 1, "empty column holds {} slabs", slabs.len());
            return;
        }
        for (i, slab) in slabs.iter().enumerate() {
            assert_ne!(slab.len, 0, "slab {i} of {} is empty", slabs.len());
            assert!(
                slab.segments <= max || slab.segments == 1,
                "slab {i} has {} segments, over the budget of {max} \
                 (only a single oversize value may exceed it)",
                slab.segments
            );
            let (len, segments) = <V::E as Model<V::X>>::validate(&slab.data)
                .unwrap_or_else(|e| panic!("slab {i} does not parse: {e}"));
            assert_eq!(len, slab.len, "slab {i} cached len");
            assert_eq!(segments, slab.segments, "slab {i} cached segments");
        }
        for i in 1..slabs.len() {
            let (a, b) = (slabs[i - 1].segments, slabs[i].segments);
            assert!(
                !((a < min || b < min) && a + b <= max),
                "slabs {} and {i} ({a} and {b} segments) should have been merged \
                 (min {min}, max {max})",
                i - 1
            );
        }
        assert_eq!(
            self.core.len(),
            slabs.iter().map(|s| s.len).sum::<usize>(),
            "total_len"
        );
        assert_eq!(self.index_len(), slabs.len(), "index length");
        let mut before = 0usize;
        for (i, slab) in slabs.iter().enumerate() {
            assert_eq!(
                self.index_before(i),
                Some(before),
                "index items_before({i})"
            );
            before += slab.len;
        }
    }
}

impl<V: ColumnValueRef<C>, C: Codec, K: VIndex<V, C>> std::fmt::Debug for Column<V, C, K> {
    /// How many values and how many slabs, as `Column` prints itself.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Column")
            .field("len", &self.core.len())
            .field("slabs", &self.core.slabs().len())
            .finish()
    }
}

// one budget for both APIs
const _: () = assert!(MAX_SEG == crate::column::DEFAULT_MAX_SEG);

#[cfg(test)]
mod tests {
    use super::*;

    /// For each window `start..end` of a column of `vals` that lies in
    /// several slabs: `ICore::drain_into` appends the window's values to
    /// what the vector held, and leaves nothing to read.
    macro_rules! windows {
        ($t:ty, $vals:expr) => {{
            let vals = $vals;
            let col = Column::<$t>::from_values_with_max_segments(&vals, 4);
            let slabs = col.core.slabs();
            assert!(slabs.len() > 3, "{} slabs", slabs.len());
            for start in 0..=vals.len() {
                for end in start..=vals.len() {
                    let mut out = Vec::new();
                    let mut it = col.reader(0, start);
                    it.drain_into::<<$t as seal::Sealed<Leb128>>::X>(slabs, &mut out);
                    let mut it = col.reader(start, end);
                    it.drain_into::<<$t as seal::Sealed<Leb128>>::X>(slabs, &mut out);
                    assert_eq!(out[..], vals[..end], "{start}..{end}");
                    assert!(it.next::<<$t as seal::Sealed<Leb128>>::X>(slabs).is_none());
                }
            }
        }};
    }

    /// `to_vec` reads a whole column, and so every slab to its end. Here
    /// the windows end anywhere, inside a slab and inside a run.
    #[test]
    fn drain_into_appends_a_window() {
        let nums: Vec<u64> = (0..90u64)
            .map(|i| if i % 9 < 4 { i / 9 } else { i * 1000 })
            .collect();
        windows!(u64, nums.clone());
        let opts: Vec<Option<u64>> = nums
            .iter()
            .enumerate()
            .map(|(i, v)| (i % 5 != 0 && i % 13 != 6).then_some(*v))
            .collect();
        windows!(Option<u64>, opts);
        let bools: Vec<bool> = (0..90).map(|i| i % 7 < 3 || i % 11 == 0).collect();
        windows!(bool, bools);
        let owned: Vec<Vec<u8>> = nums.iter().map(|v| v.to_string().into_bytes()).collect();
        let lent: Vec<&[u8]> = owned.iter().map(|v| &v[..]).collect();
        windows!(Vec<u8>, lent);
    }
}
