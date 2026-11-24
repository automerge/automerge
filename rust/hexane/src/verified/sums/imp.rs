//! The implementation of `verified::sums` and its proofs: what its
//! specification (`sums.rs`) says. Each function named `..._impl` here has
//! the contract of the function there that calls it, word for word; a
//! function named after a trait, a method and a type (`vpack_pack_u64`)
//! is the body of that method in that type's impl of the trait, with the
//! trait's contract said of the type.

#![allow(unused_imports)]

#[cfg(verus_keep_ghost)]
use super::super::{held, lemma_held, lemma_svs_add, lemma_svs_none, lemma_svs_sub};
use super::*;

use crate::attrib::Accumulate;
use crate::btree::{OrdPrefix, PrefixAggregate, SlabAggregate};
use crate::column::Slab;
#[cfg(verus_keep_ghost)]
use crate::cursor::ReadsAgree;
#[cfg(verus_keep_ghost)]
use crate::cursor::{all_ok, lemma_lens_ok, lemma_vals_own, lemma_vals_total, slab_lens};
use crate::cursor::{Slabs, Weigh};
#[cfg(verus_keep_ghost)]
use crate::fenwick::{lemma_sum_le, sum, total};
use crate::index::IndexOps;
#[cfg(verus_keep_ghost)]
use crate::iter_core::{each, lemma_upto, lens_ok, vals_of};
use crate::iter_core::{Decode, Decoders};
#[cfg(verus_keep_ghost)]
use crate::prefix_core::{lemma_vsum_cat, lemma_vsum_run, reps, vsum};
use crate::prefix_core::{p_slab_sum, PrefixOps, PrefixWeight};
#[cfg(verus_keep_ghost)]
use crate::prefix_search::nsum;
use crate::prefix_search::PrefixOrd;
use crate::weights::{count_overflows, prefix_sum_overflows, sum_overflows, CopyWeigh, LoadWeigh};
use crate::{PackError, Run};
use std::fmt::Debug;
use std::marker::PhantomData;
use std::num::NonZeroU32;
use std::ops::{Add, AddAssign, SubAssign};
#[cfg(verus_keep_ghost)]
use vstd::arithmetic::div_mod::{
    lemma_add_mod_noop_right, lemma_div_nonincreasing, lemma_fundamental_div_mod_converse,
    lemma_mod_bound, lemma_mod_sub_multiples_vanish,
};
#[cfg(verus_keep_ghost)]
use vstd::arithmetic::mul::lemma_mul_inequality;
#[allow(unused_imports)]
use vstd::prelude::*;

verus! {

/// What a remainder by `m` is, as far as wrapping arithmetic asks.
pub(super) proof fn lemma_wrap(x: int, y: int, m: int)
    requires
        m > 0,
    ensures
        0 <= x % m < m,
        0 <= x < m ==> x % m == x,
        m <= x < 2 * m ==> x % m == x - m,
        -m <= x < 0 ==> x % m == x + m,
        ((x % m) + y) % m == (x + y) % m,
        ((x % m) - m + y) % m == (x + y) % m,
{
    lemma_mod_bound(x, m);
    if 0 <= x < m {
        lemma_fundamental_div_mod_converse(x, m, 0, x);
    }
    if m <= x < 2 * m {
        lemma_fundamental_div_mod_converse(x, m, 1, x - m);
    }
    if -m <= x < 0 {
        lemma_fundamental_div_mod_converse(x, m, -1, x + m);
    }
    lemma_add_mod_noop_right(y, x, m);
    lemma_mod_sub_multiples_vanish((x % m) + y, m);
}

} // verus!

/// [`VSum`] and [`UnsignedPrefix`] for an unsigned type: its integers are those
/// from zero, and it wraps by the remainder.
macro_rules! unsigned_sum {
    ($t:ty) => {
        verus! {

        impl seal::SumSeal for $t {

        }

        impl VSum for $t {
            open spec fn as_int(self) -> int {
                self as int
            }

            open spec fn lo() -> int {
                0
            }

            open spec fn hi() -> int {
                <$t>::MAX as int
            }

            open spec fn of_int(x: int) -> $t {
                (x % (<$t>::MAX + 1)) as $t
            }

            proof fn lemma_as_int(p: $t) {
                lemma_wrap(p as int, 0, <$t>::MAX + 1);
            }

            proof fn lemma_of_int(x: int, y: int) {
                lemma_wrap(x, y, <$t>::MAX + 1);
            }

            #[inline(always)]
            fn zero() -> $t {
                proof {
                    lemma_wrap(0, 0, <$t>::MAX + 1);
                }
                0
            }

            #[inline(always)]
            fn w_add(self, b: $t) -> $t {
                proof {
                    lemma_wrap(self + b, 0, <$t>::MAX + 1);
                }
                self.wrapping_add(b)
            }

            #[inline(always)]
            fn w_sub(self, b: $t) -> $t {
                proof {
                    lemma_wrap(self - b, 0, <$t>::MAX + 1);
                }
                self.wrapping_sub(b)
            }

            #[inline(always)]
            fn w_mul(self, n: usize) -> $t {
                self.wrapping_mul(n as $t)
            }

            #[inline(always)]
            fn c_add(self, b: $t) -> Option<$t> {
                self.checked_add(b)
            }

            #[inline(always)]
            fn c_mul(self, n: usize) -> Option<$t> {
                self.checked_mul(n as $t)
            }

            #[inline(always)]
            fn le(self, b: $t) -> bool {
                self <= b
            }
        }

        impl UnsignedPrefix for $t {
            proof fn lemma_count() {
            }

            #[inline(always)]
            fn div_up(self, b: $t) -> $t {
                if self == 0 || b == 0 {
                    return 0;
                }
                proof {
                    lemma_div_nonincreasing((self - 1) as int, b as int);
                }
                (self - 1) / b + 1
            }

            // one text for the three types: of one of them a cast is to
            // the type itself
            #[allow(clippy::unnecessary_cast)]
            #[inline(always)]
            fn as_len(self) -> Option<usize> {
                if (self as u128) <= (usize::MAX as u128) {
                    Some(self as usize)
                } else {
                    None
                }
            }

            #[inline(always)]
            fn one() -> $t {
                1
            }
        }

        } // verus!
    };
}

unsigned_sum!(u64);

unsigned_sum!(u128);

unsigned_sum!(usize);
verus! {

impl seal::SumSeal for i128 {

}

// ── A slab's weight ─────────────────────────────────────────────────────────

/// What a prefix column's index holds of a slab: how many values it
/// has, and what they add up to, wrapping.
#[doc(hidden)]
#[derive(Debug, Default)]
pub struct SumWeight<P: VSum> {
    pub len: usize,
    pub sum: P,
}

impl<P: VSum> Clone for SumWeight<P> {
    #[inline(always)]
    fn clone(&self) -> (r: Self)
        ensures
            r == *self,
    {
        *self
    }
}

impl<P: VSum> Copy for SumWeight<P> {

}

impl<P: VSum> SlabAggregate for SumWeight<P> {
    open spec fn spec_len(&self) -> nat {
        self.len as nat
    }

    open spec fn spec_merge(l: Self, r: Self) -> Self {
        SumWeight { len: (l.len + r.len) as usize, sum: P::of_int(l.sum.as_int() + r.sum.as_int()) }
    }

    open spec fn merge_ok(l: Self, r: Self) -> bool {
        l.len + r.len <= usize::MAX
    }

    open spec fn spec_empty() -> Self {
        SumWeight { len: 0, sum: P::of_int(0) }
    }

    #[inline(always)]
    fn merge(l: &Self, r: &Self) -> Self {
        SumWeight { len: l.len + r.len, sum: l.sum.w_add(r.sum) }
    }

    #[inline(always)]
    fn len(&self) -> usize {
        self.len
    }

    #[inline(always)]
    fn empty() -> Self {
        SumWeight { len: 0, sum: P::zero() }
    }

    proof fn lemma_empty(a: Self) {
        P::lemma_of_int(0, a.sum.as_int());
        P::lemma_as_int(a.sum);
    }

    proof fn lemma_merge_len(a: Self, b: Self) {
    }

    proof fn lemma_assoc(a: Self, b: Self, c: Self) {
        P::lemma_of_int(a.sum.as_int() + b.sum.as_int(), c.sum.as_int());
        P::lemma_of_int(b.sum.as_int() + c.sum.as_int(), a.sum.as_int());
    }

    open spec fn plain() -> bool {
        true
    }

    proof fn lemma_plain(a: Self, b: Self) {
    }
}

impl<P: VSum> PrefixAggregate for SumWeight<P> {
    type Prefix = P;

    open spec fn spec_prefix(&self) -> P {
        self.sum
    }

    open spec fn prefix_ok(&self) -> bool {
        true
    }

    open spec fn prefix_zero() -> P {
        P::of_int(0)
    }

    open spec fn prefix_plus(a: P, b: P) -> P {
        P::of_int(a.as_int() + b.as_int())
    }

    open spec fn prefix_plus_ok(a: P, b: P) -> bool {
        true
    }

    #[inline(always)]
    fn prefix(&self) -> &P {
        &self.sum
    }

    #[inline(always)]
    fn zero_prefix() -> P {
        P::zero()
    }

    #[inline(always)]
    fn add_prefix(acc: &mut P, x: &P) {
        *acc = (*acc).w_add(*x);
    }

    proof fn lemma_prefix_empty(x: P) {
    }

    proof fn lemma_prefix_merge(a: Self, b: Self) {
    }
}

/// The laws the shared proofs of running sums rest on, which wrapping
/// arithmetic has.
impl<P: VSum> PrefixWeight for SumWeight<P> {
    open spec fn prefix_minus(a: P, b: P) -> P {
        P::of_int(a.as_int() - b.as_int())
    }

    proof fn lemma_prefix_add(a: P, b: P, c: P) {
        P::lemma_of_int(a.as_int() + b.as_int(), c.as_int());
        P::lemma_of_int(b.as_int() + c.as_int(), a.as_int());
        P::lemma_of_int(0, a.as_int());
        P::lemma_as_int(a);
    }

    proof fn lemma_prefix_sub(a: P, b: P) {
        P::lemma_of_int(a.as_int() - b.as_int(), b.as_int());
        P::lemma_as_int(a);
    }

    proof fn lemma_prefix_total() {
    }
}

impl<P: VSum> OrdPrefix for SumWeight<P> {
    open spec fn prefix_le(a: P, b: P) -> bool {
        a.as_int() <= b.as_int()
    }

    #[inline(always)]
    fn le_prefix(a: &P, b: &P) -> bool {
        (*a).le(*b)
    }

    #[inline(always)]
    fn plus_prefix(a: &P, b: &P) -> P {
        (*a).w_add(*b)
    }
}

/// `n` copies of a sum, added: the sum times `n`, wrapping.
pub(super) proof fn lemma_reps<P: VSum>(n: nat, c: P)
    ensures
        reps::<SumWeight<P>>(n, c) == P::of_int(c.as_int() * n),
    decreases n,
{
    if n == 0 {
        assert(c.as_int() * 0 == 0);
    } else {
        lemma_reps::<P>((n - 1) as nat, c);
        P::lemma_of_int(c.as_int() * (n - 1), c.as_int());
        assert(c.as_int() * (n - 1) + c.as_int() == c.as_int() * n) by (nonlinear_arith);
    }
}

/// `u32_room` values of at most `u32::MAX` add up to a `u64`.
pub(super) proof fn lemma_u32_room()
    ensures
        u32_room() >= 1,
        u32_room() * u32::MAX <= u64::MAX,
{
    if usize::MAX - u32::MAX >= 2 {
        assert((u32::MAX + 2) * u32::MAX <= u64::MAX) by (compute);
    } else {
        lemma_mul_inequality(usize::MAX as int, u32::MAX + 2, u32::MAX as int);
        assert((u32::MAX + 2) * u32::MAX <= u64::MAX) by (compute);
    }
}

/// `u32_room`, as a `usize`.
#[inline(always)]
pub(super) fn u32_room_len() -> (n: usize)
    ensures
        n == u32_room(),
{
    (u32::MAX as usize).saturating_add(2)
}

impl<T: VAmount> seal::SummedSeal for T {

}

impl<T: VAmount> seal::SummedSeal for Option<T> {

}

impl seal::SummedSeal for bool {

}

pub(super) proof fn lemma_sum_cat_impl<V: PrefixValue>(a: Seq<<V as VSpec>::S>, b: Seq<<V as VSpec>::S>)
    ensures
        summed::<V>(a + b) == summed::<V>(a) + summed::<V>(b),
    decreases b.len(),
{
    if b.len() == 0 {
        assert(a + b =~= a);
    } else {
        assert((a + b).drop_last() =~= a + b.drop_last());
        lemma_sum_cat_impl::<V>(a, b.drop_last());
    }
}

pub(super) proof fn lemma_sum_run_impl<V: PrefixValue>(n: nat, x: <V as VSpec>::S)
    ensures
        summed::<V>(Seq::new(n, |_i: int| x)) == V::amount(x) * n,
    decreases n,
{
    let s = Seq::new(n, |_i: int| x);
    if n == 0 {
        assert(V::amount(x) * 0 == 0);
    } else {
        assert(s.drop_last() =~= Seq::new((n - 1) as nat, |_i: int| x));
        lemma_sum_run_impl::<V>((n - 1) as nat, x);
        assert(V::amount(x) * (n - 1) + V::amount(x) == V::amount(x) * n) by (nonlinear_arith);
    }
}

pub(super) proof fn lemma_sum_bound_impl<V: PrefixValue>(vs: Seq<<V as VSpec>::S>)
    ensures
        -(vs.len() * V::most()) <= summed::<V>(vs) <= vs.len() * V::most(),
        V::Sum::lo() == 0 ==> summed::<V>(vs) >= 0,
    decreases vs.len(),
{
    let m = V::most() as int;
    if vs.len() == 0 {
        assert(0 * m == 0);
    } else {
        let n = vs.len() as int;
        lemma_sum_bound_impl::<V>(vs.drop_last());
        V::lemma_amount(vs.last());
        assert((n - 1) * m + m == n * m) by (nonlinear_arith);
    }
}

pub(super) proof fn lemma_sum_mono_impl<V: PrefixValue>(vs: Seq<<V as VSpec>::S>, k: int)
    requires
        V::Sum::lo() == 0,
        0 <= k <= vs.len(),
    ensures
        0 <= summed::<V>(vs.take(k)) <= summed::<V>(vs),
{
    assert(vs =~= vs.take(k) + vs.skip(k));
    lemma_sum_cat::<V>(vs.take(k), vs.skip(k));
    lemma_sum_bound::<V>(vs.take(k));
    lemma_sum_bound::<V>(vs.skip(k));
}

pub(super) proof fn lemma_sums_fit_impl<V: PrefixValue>(vs: Seq<<V as VSpec>::S>)
    requires
        vs.len() <= V::safe(),
    ensures
        sums_fit::<V>(vs),
{
    V::lemma_safe();
    let m = V::most() as int;
    let s = V::safe() as int;
    assert forall|k: int| 0 <= k <= vs.len() implies V::Sum::lo() <= #[trigger] summed::<V>(vs.take(k))
        <= V::Sum::hi() by {
        lemma_sum_bound::<V>(vs.take(k));
        assert(k * m <= s * m) by (nonlinear_arith)
            requires
                0 <= k <= s,
                0 <= m,
        ;
    }
}

pub(super) proof fn lemma_sums_fit_unsigned_impl<V: PrefixValue>(vs: Seq<<V as VSpec>::S>)
    requires
        V::Sum::lo() == 0,
    ensures
        sums_fit::<V>(vs) <==> summed::<V>(vs) <= V::Sum::hi(),
{
    assert(vs.take(vs.len() as int) =~= vs);
    if summed::<V>(vs) <= V::Sum::hi() {
        assert forall|k: int| 0 <= k <= vs.len() implies V::Sum::lo() <= #[trigger] summed::<V>(vs.take(k))
            <= V::Sum::hi() by {
            lemma_sum_mono::<V>(vs, k);
        }
    }
    if sums_fit::<V>(vs) {
        assert(summed::<V>(vs.take(vs.len() as int)) <= V::Sum::hi());
    }
}

pub(super) proof fn lemma_sums_fit_take_impl<V: PrefixValue>(vs: Seq<<V as VSpec>::S>, k: int)
    requires
        sums_fit::<V>(vs),
        0 <= k <= vs.len(),
    ensures
        sums_fit::<V>(vs.take(k)),
        V::Sum::lo() <= summed::<V>(vs.take(k)) <= V::Sum::hi(),
{
    assert forall|j: int| 0 <= j <= vs.take(k).len() implies V::Sum::lo() <= #[trigger] summed::<V>(
        vs.take(k).take(j),
    ) <= V::Sum::hi() by {
        assert(vs.take(k).take(j) =~= vs.take(j));
    }
}

pub(super) proof fn lemma_sum_range_impl<V: PrefixValue>(vs: Seq<<V as VSpec>::S>, i: int, j: int)
    requires
        0 <= i <= j <= vs.len(),
    ensures
        summed::<V>(vs.subrange(i, j)) == summed::<V>(vs.take(j)) - summed::<V>(vs.take(i)),
        sums_fit::<V>(vs) && vs.len() <= usize::MAX ==> V::Sum::lo() <= summed::<V>(vs.subrange(i, j)) <= V::Sum::hi(),
{
    let part = vs.subrange(i, j);
    assert(vs.take(j) =~= vs.take(i) + part);
    lemma_sum_cat::<V>(vs.take(i), part);
    if sums_fit::<V>(vs) && vs.len() <= usize::MAX {
        lemma_sum_bound::<V>(part);
        lemma_sum_bound::<V>(vs.take(i));
        if V::Sum::lo() != 0 {
            // a signed sum: so few values cannot leave the type
            V::lemma_safe();
            let m = V::most() as int;
            let s = V::safe() as int;
            let n = part.len() as int;
            assert(n * m <= s * m) by (nonlinear_arith)
                requires
                    0 <= n <= s,
                    0 <= m,
            ;
        }
    }
}

/// What one value adds is a sum of the type.
pub(super) proof fn lemma_part<V: PrefixValue>(x: <V as VSpec>::S)
    ensures
        V::Sum::lo() <= V::amount(x) <= V::Sum::hi(),
        V::Sum::of_int(V::amount(x)).as_int() == V::amount(x),
{
    V::lemma_amount(x);
    V::lemma_safe();
    let m = V::most() as int;
    let s = V::safe() as int;
    assert(m <= s * m) by (nonlinear_arith)
        requires
            s >= 1,
            m >= 0,
    ;
    V::Sum::lemma_of_int(V::amount(x), 0);
}

// ── The sums, to the shared proofs ──────────────────────────────────────────

/// The arithmetic of the sums of a column of `V`, as the shared proofs
/// of running sums take it (`PrefixOps`): wrapping, and proved to be
/// what they take it to be.
#[doc(hidden)]
#[verifier::reject_recursive_types(V)]
pub struct SumOf<V>(PhantomData<V>);

impl<'a, V: PrefixValue> PrefixOps<Get<'a, V>> for SumOf<V> {
    type P = V::Sum;

    type W = SumWeight<V::Sum>;

    open spec fn contrib(v: Get<'a, V>) -> V::Sum {
        V::Sum::of_int(V::amount(v.sv()))
    }

    #[inline(always)]
    fn acc(t: &mut V::Sum, v: Get<'a, V>) {
        let p = V::to_part(v);
        proof {
            V::Sum::lemma_as_int(p);
        }
        *t = (*t).w_add(p);
    }

    #[inline(always)]
    fn acc_run(t: &mut V::Sum, run: &Run<Get<'a, V>>) {
        let n = run.count;
        let p = V::to_part(run.value);
        proof {
            V::Sum::lemma_as_int(p);
            lemma_reps::<V::Sum>(n as nat, p);
        }
        *t = (*t).w_add(p.w_mul(n));
    }

    #[inline(always)]
    fn try_acc_run(t: &mut V::Sum, run: &Run<Get<'a, V>>) -> bool {
        let n = run.count;
        let p = V::to_part(run.value);
        proof {
            V::Sum::lemma_as_int(p);
            lemma_reps::<V::Sum>(n as nat, p);
        }
        *t = (*t).w_add(p.w_mul(n));
        true
    }

    #[inline(always)]
    fn zero() -> V::Sum {
        V::Sum::zero()
    }

    #[inline(always)]
    fn sub(a: V::Sum, b: &V::Sum) -> V::Sum {
        a.w_sub(*b)
    }

    #[inline(always)]
    fn cp(a: &V::Sum) -> V::Sum {
        *a
    }

    #[inline(always)]
    fn add_to(a: &mut V::Sum, b: &V::Sum) {
        *a = (*a).w_add(*b);
    }

    #[inline(always)]
    fn max_unchecked_len() -> usize {
        usize::MAX
    }

    #[inline(always)]
    fn try_add(t: &mut V::Sum, b: &V::Sum) -> bool {
        *t = (*t).w_add(*b);
        true
    }
}

/// The sum the shared proofs speak of is the sum of the values,
/// wrapped.
pub(crate) proof fn lemma_vsum<'a, V: PrefixValue>(vs: Seq<Get<'a, V>>)
    ensures
        vsum::<Get<'a, V>, SumOf<V>>(vs) == V::Sum::of_int(summed::<V>(<Get<'a, V> as VGet>::svs(vs))),
    decreases vs.len(),
{
    let sv = <Get<'a, V> as VGet>::svs(vs);
    <Get<'a, V> as VGet>::lemma_svs(vs);
    if vs.len() > 0 {
        let d = vs.drop_last();
        <Get<'a, V> as VGet>::lemma_svs(d);
        assert(sv.drop_last() =~= <Get<'a, V> as VGet>::svs(d));
        lemma_vsum::<V>(d);
        lemma_part::<V>(vs.last().sv());
        V::Sum::lemma_of_int(summed::<V>(sv.drop_last()), V::amount(vs.last().sv()));
    }
}

/// Where a sum and a second, wrapped, are what the shared proofs give
/// for two integers that fit, and so does their difference, the second
/// is the difference.
pub(crate) proof fn lemma_sum_diff<P: VSum>(base: P, t: P, a: int, b: int)
    requires
        base == P::of_int(a),
        <SumWeight<P> as PrefixAggregate>::prefix_plus(base, t) == P::of_int(b),
        P::lo() <= a <= P::hi(),
        P::lo() <= b <= P::hi(),
        P::lo() <= b - a <= P::hi(),
    ensures
        t.as_int() == b - a,
{
    P::lemma_of_int(a, 0);
    P::lemma_of_int(b, 0);
    P::lemma_of_int(b - a, 0);
    P::lemma_as_int(t);
    // `a + t` wraps to what `b` does: less `a`, `t` to what `b - a` does
    P::lemma_of_int(a + t.as_int(), -a);
    P::lemma_of_int(b, -a);
}

/// The sums of a column of `V` as numbers, where they are never
/// negative: what the shared search by sum takes of them (`PrefixOrd`).
/// That a prefix type's sums and order are its numbers' is trusted of
/// the old API's types; of these it is proved.
impl<'a, V: PrefixValue> PrefixOrd<Get<'a, V>> for SumOf<V> where V::Sum: UnsignedPrefix {
    open spec fn pnum(p: V::Sum) -> nat {
        p.as_int() as nat
    }

    open spec fn pmax() -> nat {
        V::Sum::hi() as nat
    }

    proof fn axiom_pnum(a: V::Sum, b: V::Sum) {
        V::Sum::lemma_count();
        V::Sum::lemma_as_int(a);
        V::Sum::lemma_as_int(b);
        V::Sum::lemma_of_int(0, 0);
        V::Sum::lemma_of_int(a.as_int() + b.as_int(), 0);
        V::Sum::lemma_of_int(a.as_int() - b.as_int(), 0);
    }

    proof fn axiom_ple(a: V::Sum, b: V::Sum) {
        V::Sum::lemma_count();
        V::Sum::lemma_as_int(a);
        V::Sum::lemma_as_int(b);
    }

    #[inline(always)]
    fn le(a: &V::Sum, b: &V::Sum) -> bool {
        proof {
            V::Sum::lemma_count();
            V::Sum::lemma_as_int(*a);
            V::Sum::lemma_as_int(*b);
        }
        (*a).le(*b)
    }

    #[inline(always)]
    fn div_ceil(a: &V::Sum, b: &V::Sum) -> V::Sum {
        let r = (*a).div_up(*b);
        proof {
            V::Sum::lemma_count();
            V::Sum::lemma_as_int(*a);
            V::Sum::lemma_as_int(*b);
            V::Sum::lemma_as_int(r);
        }
        r
    }

    #[inline(always)]
    fn to_usize(p: &V::Sum) -> Option<usize> {
        proof {
            V::Sum::lemma_count();
            V::Sum::lemma_as_int(*p);
        }
        (*p).as_len()
    }

    #[inline(always)]
    fn add(a: &V::Sum, b: &V::Sum) -> V::Sum {
        (*a).w_add(*b)
    }

    #[inline(always)]
    fn checked_add(a: &V::Sum, b: &V::Sum) -> Option<V::Sum> {
        let r = (*a).c_add(*b);
        proof {
            V::Sum::lemma_count();
            V::Sum::lemma_as_int(*a);
            V::Sum::lemma_as_int(*b);
            if r is Some {
                V::Sum::lemma_as_int(r->0);
            }
        }
        r
    }

    #[inline(always)]
    fn one() -> V::Sum {
        V::Sum::one()
    }
}

/// The number the shared search takes for a sum of values is what the
/// values add up to.
pub(crate) proof fn lemma_nsum<'a, V: PrefixValue>(vs: Seq<Get<'a, V>>)
    where V::Sum: UnsignedPrefix
    ensures
        nsum::<Get<'a, V>, SumOf<V>>(vs) == summed::<V>(<Get<'a, V> as VGet>::svs(vs)),
    decreases vs.len(),
{
    let sv = <Get<'a, V> as VGet>::svs(vs);
    <Get<'a, V> as VGet>::lemma_svs(vs);
    if vs.len() > 0 {
        let d = vs.drop_last();
        <Get<'a, V> as VGet>::lemma_svs(d);
        assert(sv.drop_last() =~= <Get<'a, V> as VGet>::svs(d));
        lemma_nsum::<V>(d);
        lemma_part::<V>(vs.last().sv());
        V::lemma_amount(vs.last().sv());
        V::Sum::lemma_count();
    }
}

// ── A prefix column's weights ───────────────────────────────────────────────

/// A prefix column's weights: each slab's item count and the sum of its
/// values, wrapping; and what they ask of the column, that its sums
/// fit ([`sums_fit`]).
#[doc(hidden)]
#[verifier::reject_recursive_types(V)]
#[verifier::reject_recursive_types(C)]
pub struct SumModel<V, C>(PhantomData<(V, C)>);

/// The values of one more slab.
pub(super) proof fn lemma_one_more<'a, X: Decoders<'a>>(s: Seq<Slab<X::Tail>>, k: int)
    requires
        0 <= k < s.len(),
    ensures
        vals_of::<X>(s.take(k + 1)) == vals_of::<X>(s.take(k)) + X::vals(s[k]),
{
    let e = each::<X>(s.take(k + 1));
    assert(e.drop_last() =~= each::<X>(s.take(k)));
    assert(e.last() == X::vals(s[k]));
}

/// `t` and the sum of a slab's values, where that fits; for sums of an
/// unsigned type.
pub(super) fn slab_fits<'a, V: PrefixValue + ColumnValueRef<C>, C: Codec>(slab: &'a Slab<<V::E as Slabs>::Tail>, t: V::Sum) -> (r: Option<
    V::Sum,
>)
    requires
        <V::X as Decoders<'a>>::ok(*slab),
        V::Sum::lo() == 0,
    ensures
        match r {
            Some(u) => u.as_int() == t.as_int() + summed::<V>(
                <Get<'a, V> as VGet>::svs(<V::X as Decoders<'a>>::vals(*slab)),
            ),
            None => t.as_int() + summed::<V>(<Get<'a, V> as VGet>::svs(<V::X as Decoders<'a>>::vals(*slab)))
                > V::Sum::hi(),
        },
{
    let ghost v = <V::X as Decoders<'a>>::vals(*slab);
    let ghost sv = <Get<'a, V> as VGet>::svs(v);
    let mut d = <V::X as Decoders<'a>>::open(slab);
    let mut u = t;
    let ghost mut k: int = 0;
    proof {
        <Get<'a, V> as VGet>::lemma_svs(v);
        assert(sv.take(0) =~= Seq::<<V as VSpec>::S>::empty());
        assert(v.skip(0) =~= v);
        V::Sum::lemma_as_int(t);
    }
    loop
        invariant
            v == <V::X as Decoders<'a>>::vals(*slab),
            sv == <Get<'a, V> as VGet>::svs(v),
            sv.len() == v.len(),
            forall|i: int| 0 <= i < v.len() ==> (#[trigger] sv[i]) == v[i].sv(),
            V::Sum::lo() == 0,
            t.as_int() >= 0,
            d.inv(),
            0 <= k <= v.len(),
            d.rest() == v.skip(k),
            u.as_int() == t.as_int() + summed::<V>(sv.take(k)),
        ensures
            u.as_int() == t.as_int() + summed::<V>(sv),
        decreases d.rest().len(),
    {
        match d.d_run_max(usize::MAX) {
            Some(run) => {
                let p = V::to_part(run.value);
                let ghost c = run.count as int;
                let ghost a = V::amount(run.value.sv());
                proof {
                    let r = Seq::new(run.count as nat, |_i: int| run.value.sv());
                    assert forall|j: int| k <= j < k + c implies #[trigger] sv[j] == run.value.sv() by {
                        assert(v.skip(k).take(c)[j - k] == run.value);
                    }
                    assert(sv.take(k + c) =~= sv.take(k) + r);
                    lemma_sum_cat::<V>(sv.take(k), r);
                    lemma_sum_run::<V>(run.count as nat, run.value.sv());
                    assert(v.skip(k).skip(c) =~= v.skip(k + c));
                    lemma_sum_mono::<V>(sv, k);
                    lemma_sum_mono::<V>(sv, k + c);
                    V::lemma_amount(run.value.sv());
                    assert(a * c >= 0) by (nonlinear_arith)
                        requires
                            a >= 0,
                            c >= 0,
                    ;
                    V::Sum::lemma_as_int(u);
                }
                let q = match p.c_mul(run.count) {
                    Some(q) => q,
                    None => {
                        return None;
                    },
                };
                match u.c_add(q) {
                    Some(w) => {
                        u = w;
                    },
                    None => {
                        return None;
                    },
                }
                proof {
                    k = k + c;
                }
            },
            None => {
                proof {
                    assert(sv.take(k) =~= sv);
                }
                break ;
            },
        }
    }
    Some(u)
}

/// Whether the sums of the values of `slabs`, `len` of them, fit:
/// exactly, reading every value where the column is long enough for
/// them not to.
pub(super) fn fits<V: PrefixValue + ColumnValueRef<C>, C: Codec>(slabs: &[Slab<<V::E as Slabs>::Tail>], len: usize) -> (r: bool)
    requires
        all_ok::<V::E>(slabs@),
        len == total(slab_lens(slabs@)),
    ensures
        r == sums_fit::<V>(<Get<'static, V> as VGet>::svs(vals_of::<V::X>(slabs@))),
        len <= V::safe() ==> r,
{
    let ghost s = slabs@;
    let ghost all = vals_of::<V::X>(s);
    let ghost sv = <Get<'static, V> as VGet>::svs(all);
    proof {
        lemma_vals_total::<V::E>(s);
        lemma_vals_own::<V::E, V::X>(s);
        lemma_lens_ok::<V::E, V::X>(s);
        <Get<'static, V> as VGet>::lemma_svs(all);
        assert(sv.len() == len);
    }
    if len <= V::safe_len() {
        proof {
            lemma_sums_fit::<V>(sv);
        }
        return true;
    }
    let mut t = V::Sum::zero();
    let mut k: usize = 0;
    proof {
        V::lemma_safe();
        V::Sum::lemma_as_int(t);
        V::Sum::lemma_of_int(0, 0);
        assert(s.take(0) =~= Seq::<Slab<<V::E as Slabs>::Tail>>::empty());
        assert(vals_of::<V::X>(s.take(0)) =~= Seq::<Get<'static, V>>::empty());
        lemma_svs_none::<Get<'static, V>>();
    }
    while k < slabs.len()
        invariant
            s == slabs@,
            all == vals_of::<V::X>(s),
            sv == <Get<'static, V> as VGet>::svs(all),
            sv.len() == len,
            all.len() == len,
            len == total(slab_lens(s)),
            lens_ok::<V::X>(s),
            all_ok::<V::E>(s),
            V::Sum::lo() == 0,
            len > V::safe(),
            0 <= k <= s.len(),
            t.as_int() == summed::<V>(<Get<'static, V> as VGet>::svs(vals_of::<V::X>(s.take(k as int)))),
        decreases s.len() - k,
    {
        let ghost before = vals_of::<V::X>(s.take(k as int));
        let ghost here = <V::X as Decoders<'static>>::vals(s[k as int]);
        proof {
            <V::E as ReadsAgree<'static, V::X>>::lemma_reads(s[k as int]);
            lemma_one_more::<V::X>(s, k as int);
            lemma_svs_add::<Get<'static, V>>(before, here);
            lemma_sum_cat::<V>(
                <Get<'static, V> as VGet>::svs(before),
                <Get<'static, V> as VGet>::svs(here),
            );
        }
        match slab_fits::<V, C>(&slabs[k], t) {
            Some(u) => {
                t = u;
            },
            None => {
                proof {
                    // the values through this slab are the column's first
                    // `m`, and their sum does not fit
                    let m = sum(slab_lens(s), 0, k + 1) as int;
                    lemma_upto::<V::X>(s, k + 1);
                    lemma_sum_le(slab_lens(s), 0, k + 1, 0, s.len() as int);
                    lemma_svs_sub::<Get<'static, V>>(all, 0, m);
                    assert(all.take(m) =~= all.subrange(0, m));
                    assert(sv.take(m) =~= sv.subrange(0, m));
                    assert(summed::<V>(sv.take(m)) > V::Sum::hi());
                }
                return false;
            },
        }
        k += 1;
    }
    proof {
        assert(s.take(k as int) =~= s);
        V::Sum::lemma_as_int(t);
        lemma_sums_fit_unsigned::<V>(sv);
    }
    true
}

/// What a column's slabs hold are, in a specification, the values read
/// of them.
pub(crate) proof fn lemma_svs_slabs<V: ColumnValueRef<C>, C: Codec>(s: Seq<Slab<<V::E as Slabs>::Tail>>)
    requires
        all_ok::<V::E>(s),
    ensures
        <Held<V, C> as VGet>::svs(crate::cursor::vals_of::<V::E>(s)) == <Get<'static, V> as VGet>::svs(
            vals_of::<V::X>(s),
        ),
{
    let o = vals_of::<V::X>(s);
    lemma_vals_own::<V::E, V::X>(s);
    assert(crate::cursor::vals_of::<V::E>(s) =~= held::<V, C>(o));
    lemma_held::<V, C>(o);
}

/// The values read of a slab add up to what those read of a copy of it
/// do.
pub(super) proof fn lemma_same_sum<V: PrefixValue + ColumnValueRef<C>, C: Codec>(
    a: Slab<<V::E as Slabs>::Tail>,
    b: Slab<<V::E as Slabs>::Tail>,
)
    requires
        <V::E as Slabs>::ok(a),
        a.same(b),
    ensures
        vsum::<Get<'static, V>, SumOf<V>>(<V::X as Decoders<'static>>::vals(a)) == vsum::<Get<'static, V>, SumOf<V>>(
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
    lemma_vsum::<V>(xa);
    lemma_vsum::<V>(xb);
}

impl<V: PrefixValue + ColumnValueRef<C>, C: Codec> Weigh<V::E, SumWeight<V::Sum>> for SumModel<V, C> {
    open spec fn wrel(w: SumWeight<V::Sum>, s: Slab<<V::E as Slabs>::Tail>) -> bool {
        w.len == s.len && w.sum == vsum::<Get<'static, V>, SumOf<V>>(<V::X as Decoders<'static>>::vals(s))
    }

    #[inline]
    fn compute<I: IndexOps<SumWeight<V::Sum>>>(s: &Slab<<V::E as Slabs>::Tail>) -> (w: SumWeight<V::Sum>) {
        proof {
            <V::E as ReadsAgree<'static, V::X>>::lemma_reads(*s);
        }
        let sum = p_slab_sum::<V::X, SumOf<V>>(s);
        let w = SumWeight { len: s.len, sum };
        proof {
            I::lemma_wlen(w);
        }
        w
    }

    #[inline(always)]
    fn check_set<I: IndexOps<SumWeight<V::Sum>>>(_index: &I, _i: usize, _w: &SumWeight<V::Sum>, _len: usize) {
    }

    #[inline(always)]
    fn check_splice<I: IndexOps<SumWeight<V::Sum>>>(
        _index: &I,
        _start: usize,
        _end: usize,
        _new: &Vec<SumWeight<V::Sum>>,
        _len: usize,
    ) {
    }

    open spec fn dom_ok(s: Seq<Slab<<V::E as Slabs>::Tail>>) -> bool {
        sums_fit::<V>(<Get<'static, V> as VGet>::svs(vals_of::<V::X>(s)))
    }

    /// Panics where a sum does not fit, as [`PrefixColumn`]'s edits
    /// do, and with their message.
    ///
    /// [`PrefixColumn`]: crate::PrefixColumn
    #[inline]
    fn dom_check(slabs: &Vec<Slab<<V::E as Slabs>::Tail>>, len: usize) -> (r: bool) {
        if !fits::<V, C>(slabs.as_slice(), len) {
            sum_overflows();
        }
        true
    }

    #[inline(always)]
    fn total_ok(_ws: &[SumWeight<V::Sum>], _len: usize) -> bool {
        true
    }

    proof fn lemma_dom_empty(s: Seq<Slab<<V::E as Slabs>::Tail>>) {
        let all = vals_of::<V::X>(s);
        let sv = <Get<'static, V> as VGet>::svs(all);
        lemma_vals_own::<V::E, V::X>(s);
        <Get<'static, V> as VGet>::lemma_svs(all);
        V::Sum::lemma_as_int(V::Sum::of_int(0));
        assert forall|k: int| 0 <= k <= sv.len() implies V::Sum::lo() <= #[trigger] summed::<V>(sv.take(k))
            <= V::Sum::hi() by {
            assert(sv.take(k).len() == 0);
        }
    }
}

/// A clone of a prefix column's slabs and index is a prefix column of
/// the same values.
impl<V: PrefixValue + ColumnValueRef<C>, C: Codec> CopyWeigh<V::E, SumWeight<V::Sum>> for SumModel<V, C> {
    proof fn lemma_copy_weight() {
    }

    proof fn lemma_copy_wrel(
        w: SumWeight<V::Sum>,
        a: Slab<<V::E as Slabs>::Tail>,
        b: Slab<<V::E as Slabs>::Tail>,
    ) {
        lemma_same_sum::<V, C>(a, b);
    }

    proof fn lemma_copy_dom(s: Seq<Slab<<V::E as Slabs>::Tail>>, t: Seq<Slab<<V::E as Slabs>::Tail>>) {
        assert forall|j: int| 0 <= j < t.len() implies {
            &&& <V::E as Slabs>::ok(#[trigger] t[j])
            &&& <V::E as Slabs>::vals(t[j]) == <V::E as Slabs>::vals(s[j])
        } by {
            <V::E as Slabs>::lemma_copy(s[j], t[j]);
        }
        let f = |x: Slab<<V::E as Slabs>::Tail>| <V::E as Slabs>::vals(x);
        assert(t.map_values(f) =~= s.map_values(f));
        lemma_svs_slabs::<V, C>(s);
        lemma_svs_slabs::<V, C>(t);
    }
}

impl<'a, V: PrefixValue + ColumnValueRef<C>, C: Codec> Accumulate<Get<'a, V>, SumWeight<V::Sum>> for SumModel<V, C> {
    open spec fn wvals(w: SumWeight<V::Sum>, vs: Seq<Get<'a, V>>) -> bool {
        w.len == vs.len() && w.sum == vsum::<Get<'a, V>, SumOf<V>>(vs)
    }

    #[inline(always)]
    fn new_zero() -> SumWeight<V::Sum> {
        SumWeight { len: 0, sum: V::Sum::zero() }
    }

    #[inline]
    fn accumulate_run(w: &mut SumWeight<V::Sum>, n: usize, v: Get<'a, V>, Ghost(vs): Ghost<Seq<Get<'a, V>>>) -> Result<
        (),
        PackError,
    > {
        if w.len > usize::MAX - n {
            return Err(count_overflows());
        }
        let mut sum = w.sum;
        <SumOf<V> as PrefixOps<Get<'a, V>>>::acc_run(&mut sum, &Run { count: n, value: v });
        proof {
            lemma_vsum_cat::<Get<'a, V>, SumOf<V>>(vs, Seq::new(n as nat, |_i: int| v));
            lemma_vsum_run::<Get<'a, V>, SumOf<V>>(n as nat, v);
        }
        *w = SumWeight { len: w.len + n, sum };
        Ok(())
    }
}

/// A load computes each slab's weight, and then checks that the sums
/// fit: it refuses a column for its sums only where they do not.
impl<'a, V: PrefixValue + ColumnValueRef<C>, C: Codec> LoadWeigh<'a, V::E, V::X, SumWeight<V::Sum>> for SumModel<V, C> {
    open spec fn accumulates() -> bool {
        false
    }

    #[inline(always)]
    fn accumulates_exec() -> bool {
        false
    }

    open spec fn may_refuse(vs: Seq<Held<V, C>>) -> bool {
        !sums_fit::<V>(<Held<V, C> as VGet>::svs(vs))
    }

    open spec fn bounded(s: Seq<Slab<<V::E as Slabs>::Tail>>, lo: i64, hi: i64) -> bool {
        true
    }

    proof fn lemma_may_refuse(vs: Seq<Held<V, C>>) {
    }

    proof fn lemma_wrel(w: SumWeight<V::Sum>, s: Slab<<V::E as Slabs>::Tail>) {
        <V::E as ReadsAgree<'a, V::X>>::lemma_reads(s);
    }

    /// Refuses where a sum does not fit, as [`PrefixColumn`]'s load
    /// does, and with its error.
    ///
    /// [`PrefixColumn`]: crate::PrefixColumn
    #[inline]
    fn load_check(
        _ws: &[SumWeight<V::Sum>],
        slabs: &[Slab<<V::E as Slabs>::Tail>],
        len: usize,
        _lo: i64,
        _hi: i64,
    ) -> (r: Result<(), PackError>) {
        proof {
            lemma_svs_slabs::<V, C>(slabs@);
        }
        if fits::<V, C>(slabs, len) {
            Ok(())
        } else {
            Err(prefix_sum_overflows())
        }
    }
}

#[inline(always)]
pub(super) fn vsum_zero_i128() -> (r: i128)
    ensures
        r == <i128 as VSum>::of_int(0),
{
    proof {
        lemma_wrap(0, 0, u128::MAX + 1);
    }
    0
}

#[inline(always)]
pub(super) fn vsum_w_add_i128(this: i128, b: i128) -> (r: i128)
    ensures
        r == <i128 as VSum>::of_int(<i128 as VSum>::as_int(this) + b.as_int()),
{
    proof {
        lemma_wrap(this + b, 0, u128::MAX + 1);
    }
    this.wrapping_add(b)
}

#[inline(always)]
pub(super) fn vsum_w_sub_i128(this: i128, b: i128) -> (r: i128)
    ensures
        r == <i128 as VSum>::of_int(<i128 as VSum>::as_int(this) - b.as_int()),
{
    proof {
        lemma_wrap(this - b, 0, u128::MAX + 1);
    }
    this.wrapping_sub(b)
}

pub(super) proof fn vamount_lemma_room_u64()
    ensures
        <u64 as VAmount>::room() >= 1,
        <u64 as VAmount>::room() * <u64 as VAmount>::amount_max() <= <u64 as VAmount>::Sum::hi(),
        <u64 as VAmount>::Sum::lo() == 0 || -(<u64 as VAmount>::room() * <u64 as VAmount>::amount_max()) >= <u64 as VAmount>::Sum::lo(),
        <u64 as VAmount>::Sum::lo() == 0 || <u64 as VAmount>::room() >= usize::MAX,
{
    lemma_mul_inequality(usize::MAX as int, u64::MAX as int, u64::MAX as int);
    assert(u64::MAX * u64::MAX <= u128::MAX) by (compute);
}

pub(super) proof fn vamount_lemma_room_i64()
    ensures
        <i64 as VAmount>::room() >= 1,
        <i64 as VAmount>::room() * <i64 as VAmount>::amount_max() <= <i64 as VAmount>::Sum::hi(),
        <i64 as VAmount>::Sum::lo() == 0 || -(<i64 as VAmount>::room() * <i64 as VAmount>::amount_max()) >= <i64 as VAmount>::Sum::lo(),
        <i64 as VAmount>::Sum::lo() == 0 || <i64 as VAmount>::room() >= usize::MAX,
{
    lemma_mul_inequality(usize::MAX as int, u64::MAX as int, 0x8000_0000_0000_0000);
    assert(u64::MAX * 0x8000_0000_0000_0000 <= i128::MAX) by (compute);
}

#[inline(always)]
pub(super) fn vamount_part_of_nonzerou32(v: NonZeroU32) -> (r: u64)
    ensures
        r.as_int() == <NonZeroU32 as VAmount>::amount_of(v),
{
    broadcast use vstd::std_specs::nonzero::group_nonzero_axioms;

    v.get() as u64
}

pub(super) proof fn vsummed_lemma_amount_option_t<T: VAmount>(v: Option<T>)
    ensures
        -(<Option<T> as PrefixValue>::most() as int) <= <Option<T> as PrefixValue>::amount(v) <= <Option<T> as PrefixValue>::most(),
        <Option<T> as PrefixValue>::Sum::lo() == 0 ==> <Option<T> as PrefixValue>::amount(v) >= 0,
{
    match v {
        Some(x) => T::lemma_amount_of(x),
        None => {},
    }
}

#[inline(always)]
pub(super) fn vsummed_to_part_option_t<T: VAmount>(v: Option<T>) -> (r: <T as VAmount>::Sum)
    ensures
        r.as_int() == <Option<T> as PrefixValue>::amount(v.sv()),
{
    match v {
        Some(x) => T::part_of(x),
        None => {
            proof {
                <T as VAmount>::Sum::lemma_as_int(<T as VAmount>::Sum::of_int(0));
                <T as VAmount>::Sum::lemma_of_int(0, 0);
            }
            <T as VAmount>::Sum::zero()
        },
    }
}

#[inline(always)]
pub(super) fn vsummed_to_part_bool(v: bool) -> (r: usize)
    ensures
        r.as_int() == <bool as PrefixValue>::amount(v.sv()),
{
    if v {
        1
    } else {
        0
    }
}

} // verus!
