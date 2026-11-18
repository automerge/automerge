//! Accumulator columns of the verified public API: columns kept with
//! what their values add up to in a type of the client's own, verified
//! with Verus.
//!
//! A prefix column ([`VerifiedPrefixColumn`](super::VerifiedPrefixColumn))
//! adds its values up as integers, in one of this crate's integer
//! types. An accumulator column adds them up in a type its client
//! gives, an accumulator ([`VAccum`]): the client says what a value adds
//! ([`VAdds`]), how two of the accumulators are added, and what no
//! values add up to. A set of the marks that are open at a position is
//! one, where a value opens a mark or closes it.
//!
//! [`PrefixColumn`](crate::PrefixColumn) reaches the arithmetic of such
//! a type through a trusted bridge, which states that its `+` is
//! associative and that its `-` undoes it, and leaves Verus to take
//! that on trust. Here the client *proves* those laws of its type
//! ([`VGroup`]), and what the column answers is proved from them.
//!
//! An accumulator column is a [`VerifiedColumn`] kept with an
//! [`AccIndex`]: of each slab the index holds, besides the count of its
//! values, what they add up to (`accumulated`). What a column of the
//! kind says of its values, and its bytes when saved, are a plain
//! column's, and there is one of any values: nothing overflows, so a
//! load refuses no saved column that a plain column takes, and no edit
//! panics for what the values add up to.

// Verus proofs read more plainly with `x = x + n`.
#![allow(clippy::len_zero, clippy::assign_op_pattern)]

use super::codec::VCodec;
use super::kind::{Kind, VIndex, VKeeps};
use super::seal;
use super::value::VPack;
#[cfg(verus_keep_ghost)]
use super::VGet;
#[cfg(verus_keep_ghost)]
use super::{held, lemma_held, lemma_svs_sub, Held};
use super::{Get, VForm, VSpec, VValue, VerifiedColumn};
use crate::attrib::Accumulate;
use crate::btree::{PrefixAggregate, SlabAggregate, SlabBTree};
use crate::column::Slab;
use crate::column_core::too_many_slabs;
#[cfg(verus_keep_ghost)]
use crate::cursor::ReadsAgree;
#[cfg(verus_keep_ghost)]
use crate::cursor::{lemma_lens_ok, slab_lens};
use crate::cursor::{Slabs, Weigh};
#[cfg(verus_keep_ghost)]
use crate::fenwick::total;
#[cfg(verus_keep_ghost)]
use crate::index::lemma_btree_index;
use crate::index::IndexOps;
#[cfg(verus_keep_ghost)]
use crate::iter_core::Decoders;
#[cfg(verus_keep_ghost)]
use crate::iter_core::{lens_ok, vals_of};
#[cfg(verus_keep_ghost)]
use crate::prefix_core::{before, ix_ok, lemma_vsum_cat, lemma_vsum_run, reps, vsum};
use crate::prefix_core::{p_prefix, p_slab_sum, p_sum_range, PrefixOps, PrefixWeight};
use crate::weights::{count_overflows, CopyWeigh, LoadWeigh};
use crate::{Leb128, PackError, Run};
use std::fmt::Debug;
use std::marker::PhantomData;
#[allow(unused_imports)]
use vstd::prelude::*;

verus! {

// ── What the values are added up in ─────────────────────────────────────────

/// The laws of a type an accumulator column adds its values up in: what
/// no values add up to (`zero`), what two of them add up to, one after
/// the other (`plus`), and what is to be added to one to give another
/// (`minus`).
///
/// `plus` is associative, and `zero` added on either side changes
/// nothing (`lemma_plus`); and `minus(a, b)`, added after `b`, gives
/// `a` (`lemma_minus`). That makes the type a group. `plus` need not
/// give the same for `a` and `b` as for `b` and `a`: a column adds its
/// values in the order it holds them.
///
/// The laws are of the type's values as Verus compares them (`==`).
/// For a type of plain data, numbers and structures of them, that is
/// what Rust's `==` compares.
pub trait VGroup: Sized {
    /// What no values add up to.
    spec fn zero() -> Self;

    /// `a` and then `b`, added.
    spec fn plus(a: Self, b: Self) -> Self;

    /// What, added after `b`, gives `a`.
    spec fn minus(a: Self, b: Self) -> Self;

    /// `plus` is associative, and `zero` is its identity.
    proof fn lemma_plus(a: Self, b: Self, c: Self)
        ensures
            Self::plus(Self::plus(a, b), c) == Self::plus(a, Self::plus(b, c)),
            Self::plus(Self::zero(), a) == a,
            Self::plus(a, Self::zero()) == a,
    ;

    /// Adding back what was taken away restores what there was.
    proof fn lemma_minus(a: Self, b: Self)
        ensures
            Self::plus(b, Self::minus(a, b)) == a,
    ;
}

/// `n` copies of `a`, added.
pub open spec fn times<A: VGroup>(n: nat, a: A) -> A
    decreases n,
{
    if n == 0 {
        A::zero()
    } else {
        A::plus(times::<A>((n - 1) as nat, a), a)
    }
}

/// What is added to the same and gives the same is the same: from the
/// laws of a [`VGroup`] alone.
pub proof fn lemma_cancel<A: VGroup>(b: A, t: A, r: A)
    requires
        A::plus(b, t) == A::plus(b, r),
    ensures
        t == r,
{
    // `i` undoes `b` from the right, and `j` undoes `i`: so `i` undoes
    // `b` from the left too
    let i = A::minus(A::zero(), b);
    let j = A::minus(A::zero(), i);
    A::lemma_minus(A::zero(), b);
    A::lemma_minus(A::zero(), i);
    A::lemma_plus(A::plus(i, b), i, j);
    A::lemma_plus(i, b, i);
    A::lemma_plus(i, A::zero(), j);
    A::lemma_plus(i, i, i);
    A::lemma_plus(A::plus(i, b), i, i);
    assert(A::plus(A::plus(i, b), A::zero()) == A::plus(i, b));
    assert(A::plus(A::plus(i, b), A::plus(i, j)) == A::plus(A::plus(A::plus(i, b), i), j));
    assert(A::plus(A::plus(i, b), i) == A::plus(i, A::plus(b, i)));
    assert(A::plus(i, A::zero()) == i);
    assert(A::plus(i, b) == A::zero());
    A::lemma_plus(i, b, t);
    A::lemma_plus(i, b, r);
    A::lemma_plus(t, t, t);
    A::lemma_plus(r, r, r);
}

/// A type an accumulator column adds its values up in: an accumulator.
/// Its laws are a [`VGroup`]'s, and these are the functions that
/// compute what the laws are of.
pub trait VAccum: VGroup + Debug {
    /// What no values add up to.
    fn new_zero() -> (r: Self)
        ensures
            r == Self::zero(),
    ;

    /// Adds `b`, after what there is.
    fn add_acc(&mut self, b: &Self)
        ensures
            *final(self) == Self::plus(*old(self), *b),
    ;

    /// Takes `b` away: leaves what, added after `b`, gives what there
    /// was.
    fn sub_acc(&mut self, b: &Self)
        ensures
            *final(self) == Self::minus(*old(self), *b),
    ;

    /// A copy.
    fn dup(&self) -> (r: Self)
        ensures
            r == *self,
    ;

    /// Adds `b`, `n` times over: what a run of `n` equal values adds.
    /// As it is given here it adds `b` one time after another, which
    /// takes as long as the run is; a type that has a shorter way gives
    /// its own. One whose columns are loaded from bytes that are not
    /// trusted should: a few bytes can be a run of billions of values.
    fn add_times(&mut self, b: &Self, n: usize)
        ensures
            *final(self) == Self::plus(*old(self), times::<Self>(n as nat, *b)),
    {
        let ghost start = *self;
        let mut k: usize = 0;
        proof {
            Self::lemma_plus(start, start, start);
        }
        while k < n
            invariant
                k <= n,
                *self == Self::plus(start, times::<Self>(k as nat, *b)),
            decreases n - k,
        {
            proof {
                Self::lemma_plus(start, times::<Self>(k as nat, *b), *b);
            }
            self.add_acc(b);
            k += 1;
        }
    }
}

// ── A slab's weight ─────────────────────────────────────────────────────────

/// What an accumulator column's index holds of a slab: how many values
/// it has, and what they add up to.
#[doc(hidden)]
#[derive(Debug)]
pub struct AccWeight<A: VAccum> {
    pub len: usize,
    pub acc: A,
}

impl<A: VAccum> Default for AccWeight<A> {
    #[inline(always)]
    fn default() -> Self {
        AccWeight { len: 0, acc: A::new_zero() }
    }
}

impl<A: VAccum> Clone for AccWeight<A> {
    #[inline(always)]
    fn clone(&self) -> (r: Self)
        ensures
            r == *self,
    {
        AccWeight { len: self.len, acc: self.acc.dup() }
    }
}

impl<A: VAccum> SlabAggregate for AccWeight<A> {
    open spec fn spec_len(&self) -> nat {
        self.len as nat
    }

    open spec fn spec_merge(l: Self, r: Self) -> Self {
        AccWeight { len: (l.len + r.len) as usize, acc: A::plus(l.acc, r.acc) }
    }

    open spec fn merge_ok(l: Self, r: Self) -> bool {
        l.len + r.len <= usize::MAX
    }

    open spec fn spec_empty() -> Self {
        AccWeight { len: 0, acc: A::zero() }
    }

    #[inline(always)]
    fn merge(l: &Self, r: &Self) -> Self {
        let mut acc = l.acc.dup();
        acc.add_acc(&r.acc);
        AccWeight { len: l.len + r.len, acc }
    }

    #[inline(always)]
    fn len(&self) -> usize {
        self.len
    }

    #[inline(always)]
    fn empty() -> Self {
        AccWeight { len: 0, acc: A::new_zero() }
    }

    proof fn lemma_empty(a: Self) {
        A::lemma_plus(a.acc, a.acc, a.acc);
    }

    proof fn lemma_merge_len(a: Self, b: Self) {
    }

    proof fn lemma_assoc(a: Self, b: Self, c: Self) {
        A::lemma_plus(a.acc, b.acc, c.acc);
    }

    open spec fn plain() -> bool {
        true
    }

    proof fn lemma_plain(a: Self, b: Self) {
    }
}

impl<A: VAccum> PrefixAggregate for AccWeight<A> {
    type Prefix = A;

    open spec fn spec_prefix(&self) -> A {
        self.acc
    }

    open spec fn prefix_ok(&self) -> bool {
        true
    }

    open spec fn prefix_zero() -> A {
        A::zero()
    }

    open spec fn prefix_plus(a: A, b: A) -> A {
        A::plus(a, b)
    }

    open spec fn prefix_plus_ok(a: A, b: A) -> bool {
        true
    }

    #[inline(always)]
    fn prefix(&self) -> &A {
        &self.acc
    }

    #[inline(always)]
    fn zero_prefix() -> A {
        A::new_zero()
    }

    #[inline(always)]
    fn add_prefix(acc: &mut A, x: &A) {
        acc.add_acc(x);
    }

    proof fn lemma_prefix_empty(x: A) {
    }

    proof fn lemma_prefix_merge(a: Self, b: Self) {
    }
}

/// The laws the shared proofs of running sums rest on, which are the
/// client's: what [`PrefixColumn`](crate::PrefixColumn) takes on trust
/// of a prefix type is here what a [`VGroup`] proves.
impl<A: VAccum> PrefixWeight for AccWeight<A> {
    open spec fn prefix_minus(a: A, b: A) -> A {
        A::minus(a, b)
    }

    proof fn lemma_prefix_add(a: A, b: A, c: A) {
        A::lemma_plus(a, b, c);
    }

    proof fn lemma_prefix_sub(a: A, b: A) {
        A::lemma_minus(a, b);
    }

    proof fn lemma_prefix_total() {
    }
}

/// `n` copies of an accumulator, added, as the shared proofs say it.
proof fn lemma_reps<A: VAccum>(n: nat, c: A)
    ensures
        reps::<AccWeight<A>>(n, c) == times::<A>(n, c),
    decreases n,
{
    if n > 0 {
        lemma_reps::<A>((n - 1) as nat, c);
    }
}

// ── What a value adds ───────────────────────────────────────────────────────

/// A value type whose values an accumulator column can add up: the
/// accumulator they are added up in, and what a value adds.
///
/// It is for a client to give of a type of its own
/// ([`VNewtype`](super::VNewtype)).
pub trait VAdds: VPack {
    /// The accumulator the values are added up in.
    type Acc: VAccum;

    /// What the value `v` adds.
    spec fn adds_of(v: Self) -> Self::Acc;

    /// What `v` adds.
    fn acc_of(v: Self) -> (r: Self::Acc)
        ensures
            r == Self::adds_of(v),
    ;
}

/// A value type an accumulator column can hold: a type that says what
/// its values add ([`VAdds`]), or an `Option` of one, whose `None` adds
/// nothing. No other type can implement this trait.
pub trait VAccumulated: VSpec + for <'a>VForm<'a> + seal::AccSeal {
    /// The accumulator the values are added up in.
    type Acc: VAccum;

    /// What the value `v` adds.
    spec fn adds(v: <Self as VSpec>::S) -> Self::Acc;

    /// What `v` adds.
    #[doc(hidden)]
    fn to_acc<'a>(v: Get<'a, Self>) -> (r: Self::Acc)
        ensures
            r == Self::adds(v.sv()),
    ;
}

impl<T: VAdds> seal::AccSeal for T {

}

impl<T: VAdds> VAccumulated for T {
    type Acc = <T as VAdds>::Acc;

    open spec fn adds(v: T) -> <T as VAdds>::Acc {
        T::adds_of(v)
    }

    #[inline(always)]
    fn to_acc<'a>(v: T) -> <T as VAdds>::Acc {
        T::acc_of(v)
    }
}

impl<T: VAdds> seal::AccSeal for Option<T> {

}

impl<T: VAdds> VAccumulated for Option<T> {
    type Acc = <T as VAdds>::Acc;

    open spec fn adds(v: Option<T>) -> <T as VAdds>::Acc {
        match v {
            Some(x) => T::adds_of(x),
            None => <<T as VAdds>::Acc as VGroup>::zero(),
        }
    }

    #[inline(always)]
    fn to_acc<'a>(v: Option<T>) -> <T as VAdds>::Acc {
        match v {
            Some(x) => T::acc_of(x),
            None => <<T as VAdds>::Acc as VAccum>::new_zero(),
        }
    }
}

// ── What some values add up to ──────────────────────────────────────────────

/// What the values `vs` add up to, in the order they are in.
pub open spec fn accumulated<V: VAccumulated>(vs: Seq<<V as VSpec>::S>) -> V::Acc
    decreases vs.len(),
{
    if vs.len() == 0 {
        <V::Acc as VGroup>::zero()
    } else {
        <V::Acc as VGroup>::plus(accumulated::<V>(vs.drop_last()), V::adds(vs.last()))
    }
}

/// What two sequences add up to, one after the other, is what each
/// adds up to, added.
pub proof fn lemma_acc_cat<V: VAccumulated>(a: Seq<<V as VSpec>::S>, b: Seq<<V as VSpec>::S>)
    ensures
        accumulated::<V>(a + b) == <V::Acc as VGroup>::plus(accumulated::<V>(a), accumulated::<V>(b)),
    decreases b.len(),
{
    if b.len() == 0 {
        assert(a + b =~= a);
        <V::Acc as VGroup>::lemma_plus(accumulated::<V>(a), accumulated::<V>(a), accumulated::<V>(a));
    } else {
        assert((a + b).drop_last() =~= a + b.drop_last());
        lemma_acc_cat::<V>(a, b.drop_last());
        <V::Acc as VGroup>::lemma_plus(accumulated::<V>(a), accumulated::<V>(b.drop_last()), V::adds(b.last()));
    }
}

/// `n` copies of a value add up to what it adds, `n` times over.
pub proof fn lemma_acc_run<V: VAccumulated>(n: nat, x: <V as VSpec>::S)
    ensures
        accumulated::<V>(Seq::new(n, |_i: int| x)) == times::<V::Acc>(n, V::adds(x)),
    decreases n,
{
    let s = Seq::new(n, |_i: int| x);
    if n > 0 {
        assert(s.drop_last() =~= Seq::new((n - 1) as nat, |_i: int| x));
        lemma_acc_run::<V>((n - 1) as nat, x);
    }
}

/// What the values before position `j` add up to is what those before
/// `i` do, and then those from `i` up to `j`.
pub proof fn lemma_acc_range<V: VAccumulated>(vs: Seq<<V as VSpec>::S>, i: int, j: int)
    requires
        0 <= i <= j <= vs.len(),
    ensures
        <V::Acc as VGroup>::plus(accumulated::<V>(vs.take(i)), accumulated::<V>(vs.subrange(i, j))) == accumulated::<V>(
            vs.take(j),
        ),
{
    assert(vs.take(j) =~= vs.take(i) + vs.subrange(i, j));
    lemma_acc_cat::<V>(vs.take(i), vs.subrange(i, j));
}

// ── The accumulators, to the shared proofs ──────────────────────────────────

/// The arithmetic of the accumulators of a column of `V`, as the
/// shared proofs of running sums take it (`PrefixOps`): the client's,
/// and proved to be what they take it to be.
#[doc(hidden)]
#[verifier::reject_recursive_types(V)]
pub struct AccOf<V>(PhantomData<V>);

impl<'a, V: VAccumulated> PrefixOps<Get<'a, V>> for AccOf<V> {
    type P = V::Acc;

    type W = AccWeight<V::Acc>;

    open spec fn contrib(v: Get<'a, V>) -> V::Acc {
        V::adds(v.sv())
    }

    #[inline(always)]
    fn acc(t: &mut V::Acc, v: Get<'a, V>) {
        let p = V::to_acc(v);
        t.add_acc(&p);
    }

    #[inline(always)]
    fn acc_run(t: &mut V::Acc, run: &Run<Get<'a, V>>) {
        let n = run.count;
        let p = V::to_acc(run.value);
        proof {
            lemma_reps::<V::Acc>(n as nat, p);
        }
        t.add_times(&p, n);
    }

    #[inline(always)]
    fn try_acc_run(t: &mut V::Acc, run: &Run<Get<'a, V>>) -> bool {
        let n = run.count;
        let p = V::to_acc(run.value);
        proof {
            lemma_reps::<V::Acc>(n as nat, p);
        }
        t.add_times(&p, n);
        true
    }

    #[inline(always)]
    fn zero() -> V::Acc {
        <V::Acc as VAccum>::new_zero()
    }

    #[inline(always)]
    fn sub(a: V::Acc, b: &V::Acc) -> V::Acc {
        let mut a = a;
        a.sub_acc(b);
        a
    }

    #[inline(always)]
    fn cp(a: &V::Acc) -> V::Acc {
        a.dup()
    }

    #[inline(always)]
    fn add_to(a: &mut V::Acc, b: &V::Acc) {
        a.add_acc(b);
    }

    #[inline(always)]
    fn max_unchecked_len() -> usize {
        usize::MAX
    }

    /// Never asked: an accumulator column has no total to check
    /// (`total_ok`).
    #[inline(always)]
    fn try_add(_t: &mut V::Acc, _b: &V::Acc) -> bool {
        true
    }
}

/// The sum the shared proofs speak of is what the values add up to.
pub(crate) proof fn lemma_vacc<'a, V: VAccumulated>(vs: Seq<Get<'a, V>>)
    ensures
        vsum::<Get<'a, V>, AccOf<V>>(vs) == accumulated::<V>(<Get<'a, V> as VGet>::svs(vs)),
    decreases vs.len(),
{
    let sv = <Get<'a, V> as VGet>::svs(vs);
    <Get<'a, V> as VGet>::lemma_svs(vs);
    if vs.len() > 0 {
        let d = vs.drop_last();
        <Get<'a, V> as VGet>::lemma_svs(d);
        assert(sv.drop_last() =~= <Get<'a, V> as VGet>::svs(d));
        lemma_vacc::<V>(d);
    }
}

// ── An accumulator column's weights ─────────────────────────────────────────

/// An accumulator column's weights: each slab's item count and what its
/// values add up to. They ask nothing of the column.
#[doc(hidden)]
#[verifier::reject_recursive_types(V)]
#[verifier::reject_recursive_types(C)]
pub struct AccModel<V, C>(PhantomData<(V, C)>);

/// The values read of a slab add up to what those read of a copy of it
/// do.
proof fn lemma_same_acc<V: VAccumulated + VValue<C>, C: VCodec>(
    a: Slab<<V::E as Slabs>::Tail>,
    b: Slab<<V::E as Slabs>::Tail>,
)
    requires
        <V::E as Slabs>::ok(a),
        a.same(b),
    ensures
        vsum::<Get<'static, V>, AccOf<V>>(<V::X as Decoders<'static>>::vals(a)) == vsum::<Get<'static, V>, AccOf<V>>(
            <V::X as Decoders<'static>>::vals(b),
        ),
{
    let xa = <V::X as Decoders<'static>>::vals(a);
    let xb = <V::X as Decoders<'static>>::vals(b);
    <V::E as Slabs>::lemma_copy(a, b);
    <V::E as ReadsAgree<'static, V::X>>::lemma_reads(a);
    <V::E as ReadsAgree<'static, V::X>>::lemma_reads(b);
    assert(<V::E as Slabs>::vals(a) =~= held::<V, C>(xa));
    assert(<V::E as Slabs>::vals(b) =~= held::<V, C>(xb));
    lemma_held::<V, C>(xa);
    lemma_held::<V, C>(xb);
    lemma_vacc::<V>(xa);
    lemma_vacc::<V>(xb);
}

impl<V: VAccumulated + VValue<C>, C: VCodec> Weigh<V::E, AccWeight<V::Acc>> for AccModel<V, C> {
    open spec fn wrel(w: AccWeight<V::Acc>, s: Slab<<V::E as Slabs>::Tail>) -> bool {
        w.len == s.len && w.acc == vsum::<Get<'static, V>, AccOf<V>>(<V::X as Decoders<'static>>::vals(s))
    }

    #[inline]
    fn compute<I: IndexOps<AccWeight<V::Acc>>>(s: &Slab<<V::E as Slabs>::Tail>) -> (w: AccWeight<V::Acc>) {
        proof {
            <V::E as ReadsAgree<'static, V::X>>::lemma_reads(*s);
        }
        let acc = p_slab_sum::<V::X, AccOf<V>>(s);
        let w = AccWeight { len: s.len, acc };
        proof {
            I::lemma_wlen(w);
        }
        w
    }

    #[inline(always)]
    fn check_set<I: IndexOps<AccWeight<V::Acc>>>(_index: &I, _i: usize, _w: &AccWeight<V::Acc>, _len: usize) {
    }

    #[inline(always)]
    fn check_splice<I: IndexOps<AccWeight<V::Acc>>>(
        _index: &I,
        _start: usize,
        _end: usize,
        _new: &Vec<AccWeight<V::Acc>>,
        _len: usize,
    ) {
    }

    open spec fn dom_ok(s: Seq<Slab<<V::E as Slabs>::Tail>>) -> bool {
        true
    }

    #[inline(always)]
    fn dom_check(_slabs: &Vec<Slab<<V::E as Slabs>::Tail>>, _len: usize) -> (r: bool) {
        true
    }

    #[inline(always)]
    fn total_ok(_ws: &[AccWeight<V::Acc>], _len: usize) -> bool {
        true
    }

    proof fn lemma_dom_empty(s: Seq<Slab<<V::E as Slabs>::Tail>>) {
    }
}

/// A clone of an accumulator column's slabs and index is an accumulator
/// column of the same values.
impl<V: VAccumulated + VValue<C>, C: VCodec> CopyWeigh<V::E, AccWeight<V::Acc>> for AccModel<V, C> {
    proof fn lemma_copy_weight() {
    }

    proof fn lemma_copy_wrel(
        w: AccWeight<V::Acc>,
        a: Slab<<V::E as Slabs>::Tail>,
        b: Slab<<V::E as Slabs>::Tail>,
    ) {
        lemma_same_acc::<V, C>(a, b);
    }

    proof fn lemma_copy_dom(s: Seq<Slab<<V::E as Slabs>::Tail>>, t: Seq<Slab<<V::E as Slabs>::Tail>>) {
    }
}

impl<'a, V: VAccumulated + VValue<C>, C: VCodec> Accumulate<Get<'a, V>, AccWeight<V::Acc>> for AccModel<V, C> {
    open spec fn wvals(w: AccWeight<V::Acc>, vs: Seq<Get<'a, V>>) -> bool {
        w.len == vs.len() && w.acc == vsum::<Get<'a, V>, AccOf<V>>(vs)
    }

    #[inline(always)]
    fn new_zero() -> AccWeight<V::Acc> {
        AccWeight { len: 0, acc: <V::Acc as VAccum>::new_zero() }
    }

    #[inline]
    fn accumulate_run(
        w: &mut AccWeight<V::Acc>,
        n: usize,
        v: Get<'a, V>,
        Ghost(vs): Ghost<Seq<Get<'a, V>>>,
    ) -> Result<(), PackError> {
        if w.len > usize::MAX - n {
            return Err(count_overflows());
        }
        <AccOf<V> as PrefixOps<Get<'a, V>>>::acc_run(&mut w.acc, &Run { count: n, value: v });
        proof {
            lemma_vsum_cat::<Get<'a, V>, AccOf<V>>(vs, Seq::new(n as nat, |_i: int| v));
            lemma_vsum_run::<Get<'a, V>, AccOf<V>>(n as nat, v);
        }
        w.len = w.len + n;
        Ok(())
    }
}

/// A load computes each slab's weight, and refuses no column for what
/// its values add up to.
impl<'a, V: VAccumulated + VValue<C>, C: VCodec> LoadWeigh<'a, V::E, V::X, AccWeight<V::Acc>> for AccModel<V, C> {
    open spec fn accumulates() -> bool {
        false
    }

    #[inline(always)]
    fn accumulates_exec() -> bool {
        false
    }

    open spec fn may_refuse(vs: Seq<Held<V, C>>) -> bool {
        false
    }

    proof fn lemma_may_refuse(vs: Seq<Held<V, C>>) {
    }

    proof fn lemma_wrel(w: AccWeight<V::Acc>, s: Slab<<V::E as Slabs>::Tail>) {
        <V::E as ReadsAgree<'a, V::X>>::lemma_reads(s);
    }

    #[inline(always)]
    fn load_check(
        _ws: &[AccWeight<V::Acc>],
        _slabs: &[Slab<<V::E as Slabs>::Tail>],
        _len: usize,
        _lo: i64,
        _hi: i64,
    ) -> (r: Result<(), PackError>) {
        Ok(())
    }
}

// ── The kind of index, and the column ───────────────────────────────────────

/// The index of an accumulator column: of each slab it counts the
/// values and holds what they add up to.
pub struct AccIndex;

impl<V: VAccumulated + VValue<C>, C: VCodec> VKeeps<V, C> for AccIndex {
    /// Every column: nothing overflows.
    open spec fn keeps(vs: Seq<<V as VSpec>::S>) -> bool {
        true
    }
}

impl<V: VAccumulated + VValue<C>, C: VCodec> Kind<V, C> for AccIndex {
    type W = AccWeight<V::Acc>;

    type G = AccModel<V, C>;

    proof fn lemma_refuses(vs: Seq<Held<V, C>>) {
    }

    proof fn lemma_kept(s: Seq<Slab<<V::E as Slabs>::Tail>>) {
    }
}

impl<V: VAccumulated + VValue<C>, C: VCodec> VIndex<V, C> for AccIndex {

}

/// A column kept with what its values add up to in a type of the
/// client's own: a [`VerifiedColumn`] with an [`AccIndex`].
pub type VerifiedAccColumn<V, C = Leb128> = VerifiedColumn<V, C, AccIndex>;

/// What an accumulator column answers from its index.
impl<V: VAccumulated + VValue<C>, C: VCodec> VerifiedColumn<V, C, AccIndex> {
    /// The index as the readers of the sums take it, with what they ask
    /// of it; none for a column an edit's panic left without its parts,
    /// which is empty.
    #[inline(always)]
    pub(super) fn acc_index(&self) -> (r: Option<&SlabBTree<AccWeight<V::Acc>>>)
        ensures
            <Get<'static, V> as VGet>::svs(vals_of::<V::X>(self.core.slabs_spec())) == self@,
            vals_of::<V::X>(self.core.slabs_spec()).len() == self@.len(),
            self.core@.len() == self@.len(),
            self@.len() <= usize::MAX,
            match r {
                Some(ix) => {
                    &&& ix.wf()
                    &&& ix_ok::<V::X, AccOf<V>>(ix@, self.core.slabs_spec())
                    &&& lens_ok::<V::X>(self.core.slabs_spec())
                    &&& total(slab_lens(self.core.slabs_spec())) <= usize::MAX
                    &&& self.core.slabs_spec().len() < usize::MAX
                },
                None => self@.len() == 0 && self.core.slabs_spec().len() == 0,
            },
    {
        self.core.lemma_column();
        self.lemma_read();
        let r = self.core.index_opt();
        if self.core.slabs().len() == usize::MAX {
            too_many_slabs();
        }
        proof {
            let s = self.core.slabs_spec();
            <Get<'static, V> as VGet>::lemma_svs(vals_of::<V::X>(s));
            match r {
                Some(ix) => {
                    lemma_lens_ok::<V::E, V::X>(s);
                    lemma_btree_index(*ix);
                    assert forall|i: int| 0 <= i < s.len() implies {
                        &&& #[trigger] ix@[i].spec_len() == s[i].len
                        &&& ix@[i].spec_prefix() == vsum::<Get<'static, V>, AccOf<V>>(
                            <V::X as Decoders<'static>>::vals(s[i]),
                        )
                    } by {
                        assert(<AccModel<V, C> as Weigh<V::E, AccWeight<V::Acc>>>::wrel(ix.ws()[i], s[i]));
                    }
                },
                None => {
                    assert(slab_lens(s) =~= Seq::<nat>::empty());
                },
            }
        }
        r
    }

    /// What the values before position `index` add up to; where `index`
    /// is past the end, what all the values do.
    ///
    /// The index gives what the slabs before the one that holds the
    /// position add up to, and of that slab the runs before it are
    /// read: no value of another slab is.
    pub fn get_prefix(&self, index: usize) -> (r: V::Acc)
        ensures
            index <= self@.len() ==> r == accumulated::<V>(self@.take(index as int)),
            index >= self@.len() ==> r == accumulated::<V>(self@),
    {
        let ghost k = if index <= self@.len() {
            index as int
        } else {
            self@.len() as int
        };
        proof {
            assert(self@.take(self@.len() as int) =~= self@);
        }
        match self.acc_index() {
            Some(ix) => {
                let r = p_prefix::<V::X, AccOf<V>>(self.core.slabs(), ix, index);
                proof {
                    self.lemma_before(index);
                }
                r
            },
            None => {
                proof {
                    assert(self@.take(k) =~= self@);
                }
                <V::Acc as VAccum>::new_zero()
            },
        }
    }

    /// The sum the shared proofs give for the values before a position
    /// is what the first `index` of the column's values add up to (all
    /// of them, past the end).
    proof fn lemma_before(&self, index: usize)
        requires
            <Get<'static, V> as VGet>::svs(vals_of::<V::X>(self.core.slabs_spec())) == self@,
            vals_of::<V::X>(self.core.slabs_spec()).len() == self@.len(),
        ensures
            vsum::<Get<'static, V>, AccOf<V>>(before::<V::X>(self.core.slabs_spec(), index as int)) == accumulated::<V>(
                self@.take(
                    if index <= self@.len() {
                        index as int
                    } else {
                        self@.len() as int
                    },
                ),
            ),
    {
        let k = if index <= self@.len() {
            index as int
        } else {
            self@.len() as int
        };
        let o = vals_of::<V::X>(self.core.slabs_spec());
        let b = before::<V::X>(self.core.slabs_spec(), index as int);
        assert(b =~= o.subrange(0, k));
        lemma_svs_sub(o, 0, k);
        assert(self@.subrange(0, k) =~= self@.take(k));
        lemma_vacc::<V>(b);
    }

    /// What the values through position `index` add up to, the one at
    /// `index` among them; where `index` is at or past the end, what
    /// all the values do. It is `get_prefix(index + 1)`.
    pub fn get_total(&self, index: usize) -> (r: V::Acc)
        ensures
            index < self@.len() ==> r == accumulated::<V>(self@.take(index + 1)),
            index >= self@.len() ==> r == accumulated::<V>(self@),
    {
        // a column is shorter than `usize::MAX`: where the addition
        // saturates, `index` is past the end
        self.core.lemma_column();
        self.lemma_read();
        self.get_prefix(index.saturating_add(1))
    }

    /// What the values at the positions from `start` up to, and not
    /// including, `end` add up to: of those of them that the column
    /// has, and nothing where there are none, as where `end` is not
    /// after `start`.
    ///
    /// It is what, added after what the values before `start` add up
    /// to, gives what those before `end` do, found in one pass.
    pub fn sum_range(&self, start: usize, end: usize) -> (r: V::Acc)
        ensures
            start <= end <= self@.len() ==> r == accumulated::<V>(self@.subrange(start as int, end as int)),
            start <= self@.len() <= end ==> r == accumulated::<V>(self@.subrange(start as int, self@.len() as int)),
            start >= end || start >= self@.len() ==> r == <V::Acc as VGroup>::zero(),
    {
        let ghost n = self@.len() as int;
        let ix = match self.acc_index() {
            Some(ix) => ix,
            None => {
                proof {
                    assert(self@.subrange(0, 0).len() == 0);
                }
                return <V::Acc as VAccum>::new_zero();
            },
        };
        if start >= end {
            proof {
                if start <= n {
                    assert(self@.subrange(start as int, start as int).len() == 0);
                    assert(self@.subrange(start as int, n).len() == 0 || start < n);
                }
            }
            return <V::Acc as VAccum>::new_zero();
        }
        let len = self.core.len();
        let slabs = self.core.slabs();
        let mut core = self.reader(0, len);
        let mut total = <V::Acc as VAccum>::new_zero();
        let mut base = <V::Acc as VAccum>::new_zero();
        proof {
            assert(before::<V::X>(self.core.slabs_spec(), 0) =~= Seq::<Get<'static, V>>::empty());
            <AccWeight<V::Acc> as PrefixWeight>::lemma_prefix_add(base, base, base);
        }
        p_sum_range::<V::X, AccOf<V>>(&mut core, slabs, Some(ix), start, end, &mut total, &mut base);
        proof {
            let s0 = if start <= n {
                start as int
            } else {
                n
            };
            let e0 = if end <= n {
                end as int
            } else {
                n
            };
            self.lemma_before(start);
            self.lemma_before(end);
            lemma_acc_range::<V>(self@, s0, e0);
            lemma_cancel::<V::Acc>(base, total, accumulated::<V>(self@.subrange(s0, e0)));
            if start >= n {
                assert(self@.subrange(n, n).len() == 0);
            }
        }
        total
    }
}

} // verus!
