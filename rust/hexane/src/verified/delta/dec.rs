//! The decoder of a saved delta column ([`VerifiedDeltaDecoder`]),
//! verified with Verus.
//!
//! It reads the differences a saved delta column holds with the slab
//! decoder that [`DeltaDecoder`](crate::delta::DeltaDecoder) reads them
//! with, from the bytes as they are, and adds them up as it goes: no
//! column is built.
//!
//! What it speaks of is the whole saved column (`column()`) and how
//! far into it the decoder stands (`pos()`), so that a run of
//! differences read ([`next_delta_run_max`]) can be said by the
//! differences of the column (`deltas`), as the runs of a column's
//! iterator are.
//!
//! [`next_delta_run_max`]: VerifiedDeltaDecoder::next_delta_run_max

// Verus proofs read more plainly with `x = x + n`.
#![allow(clippy::len_zero, clippy::assign_op_pattern)]

#[cfg(verus_keep_ghost)]
use super::diffs::{lemma_deltas_kept, lemma_vals_fit};
#[cfg(verus_keep_ghost)]
use super::{all_dom, all_fit, deltas, in_dom, vals};
use super::{DIt, DKind, VDeltaValue, DE, DX};
#[cfg(verus_keep_ghost)]
use super::{VDelta, VerifiedDeltaColumn};
#[cfg(verus_keep_ghost)]
use crate::cursor::{ReadsAgree, Slabs};
use crate::delta::agg::DeltaOps;
#[cfg(verus_keep_ghost)]
use crate::delta::agg::{deltas_of, dval, lemma_run_sum, psum, reps};
#[cfg(verus_keep_ghost)]
use crate::delta::ddomain::{rvals, DeltaWrite};
#[cfg(verus_keep_ghost)]
use crate::delta::dedit::{lemma_rz_cat, lemma_rz_len, lemma_rz_nulls, lemma_rz_run, run_vals, rz};
use crate::delta::diter::add_run;
#[cfg(verus_keep_ghost)]
use crate::delta::diter::{crop, lemma_crop, lemma_crop_add, lemma_wrapping_add, realize};
#[cfg(verus_keep_ghost)]
use crate::iter_core::past;
use crate::iter_core::{Decode, Decoders};
use crate::verified::codec::VCodec;
#[cfg(verus_keep_ghost)]
use crate::verified::head_run_max;
use crate::verified::model::Model;
use crate::weights::delta_out_of_domain;
use crate::{Leb128, PackError, Run};
use vstd::prelude::*;

verus! {

/// How the values of the column underneath are differences.
type DB<T, C> = <T as DKind<C>>::B;

/// The differences that the owned values `vs` of a column underneath
/// are.
spec fn dvos<E, X, B>(vs: Seq<E::V>) -> Seq<Option<i64>> where
    E: Slabs + for<'a> ReadsAgree<'a, X>,
    X: for<'a> Decoders<'a, Tail = E::Tail>,
    B: for<'a> DeltaWrite<'a, E, X>,
 {
    vs.map_values(|o: E::V| <B as DeltaWrite<'static, E, X>>::dvo(o))
}

/// The differences that the saved delta column `bytes` holds.
spec fn saved_ds<T: VDeltaValue<C>, C: VCodec>(bytes: Seq<u8>) -> Seq<Option<i64>> {
    dvos::<DE<T, C>, DX<T, C>, DB<T, C>>(<DE<T, C> as Model<DX<T, C>>>::values(bytes))
}

/// What a saved delta column keeps are its differences, each added to
/// those before it.
proof fn lemma_saved<T: VDeltaValue<C>, C: VCodec>(bytes: Seq<u8>)
    ensures
        VerifiedDeltaColumn::<T, C>::saved_kept(bytes) == rz(0, saved_ds::<T, C>(bytes)),
{
    let vs = <DE<T, C> as Model<DX<T, C>>>::values(bytes);
    assert(rvals::<DE<T, C>, DX<T, C>, DB<T, C>>(vs) == rz(0, dvos::<DE<T, C>, DX<T, C>, DB<T, C>>(vs)));
}

/// What a decoder of the column underneath reads (`o`) are, as
/// differences, those of the values it stands for (`vs`).
proof fn lemma_read<'a, T: VDeltaValue<C>, C: VCodec>(o: Seq<DIt<'a, T, C>>, vs: Seq<<DE<T, C> as Slabs>::V>)
    requires
        vs.len() == o.len(),
        forall|i: int| 0 <= i < o.len() ==> #[trigger] vs[i] == <DE<T, C> as ReadsAgree<'a, DX<T, C>>>::own(o[i]),
    ensures
        deltas_of::<DIt<'a, T, C>, DB<T, C>>(o) == dvos::<DE<T, C>, DX<T, C>, DB<T, C>>(vs),
{
    let a = deltas_of::<DIt<'a, T, C>, DB<T, C>>(o);
    let b = dvos::<DE<T, C>, DX<T, C>, DB<T, C>>(vs);
    assert forall|i: int| 0 <= i < o.len() implies a[i] == b[i] by {
        <DB<T, C> as DeltaWrite<'a, DE<T, C>, DX<T, C>>>::axiom_own(o[i]);
        assert(vs[i] == <DE<T, C> as ReadsAgree<'a, DX<T, C>>>::own(o[i]));
    }
    assert(a =~= b);
}

/// The value at `i` of the column whose differences are `ds`: the sum
/// of the differences through `i`, or a null.
proof fn lemma_at<T: VDelta>(ds: Seq<Option<i64>>, i: int)
    requires
        all_dom::<T>(rz(0, ds)),
        0 <= i < ds.len(),
    ensures
        vals::<T>(rz(0, ds)).len() == ds.len(),
        vals::<T>(rz(0, ds))[i] == T::of(realize(psum(ds.take(i)), ds[i])),
        in_dom::<T>(realize(psum(ds.take(i)), ds[i])),
        psum(ds.take(i + 1)) == psum(ds.take(i)) + dval(ds[i]),
{
    assert(rz(0, ds)[i] == realize(0 + psum(ds.take(i)), ds[i]));
    assert(in_dom::<T>(rz(0, ds)[i]));
    assert(ds.take(i + 1).drop_last() =~= ds.take(i));
    assert(ds.take(i + 1).last() == ds[i]);
}

/// A run of `n` differences `d` from `at` on adds `n` times `d`.
proof fn lemma_run(ds: Seq<Option<i64>>, at: int, n: int, d: Option<i64>)
    requires
        0 <= at,
        0 <= n,
        at + n <= ds.len(),
        forall|j: int| at <= j < at + n ==> #[trigger] ds[j] == d,
    ensures
        ds.take(at + n) == ds.take(at) + reps(n as nat, d),
        psum(ds.take(at + n)) == psum(ds.take(at)) + n * dval(d),
{
    assert(ds.take(at + n) =~= ds.take(at) + reps(n as nat, d));
    lemma_run_sum(ds.take(at), n as nat, d);
}

/// The differences of the column whose differences are `ds` are `ds`.
proof fn lemma_deltas<T: VDelta>(ds: Seq<Option<i64>>)
    requires
        all_dom::<T>(rz(0, ds)),
    ensures
        deltas(vals::<T>(rz(0, ds))) == ds,
        vals::<T>(rz(0, ds)).len() == ds.len(),
{
    lemma_rz_len(0, ds);
    let got = deltas(vals::<T>(rz(0, ds)));
    assert forall|i: int| 0 <= i < ds.len() implies got[i] == ds[i] by {
        lemma_deltas_kept::<T>(ds, i);
    }
    assert(got =~= ds);
}

/// A reader that has the differences from `at` on to read has, `n`
/// items on, those from `at + n` on.
proof fn lemma_moved<V: Copy, B: DeltaOps<V>>(o: Seq<V>, ds: Seq<Option<i64>>, at: int, n: int)
    requires
        0 <= at <= ds.len(),
        0 <= n <= o.len(),
        deltas_of::<V, B>(o) == ds.skip(at),
    ensures
        at + n <= ds.len(),
        deltas_of::<V, B>(o.skip(n)) == ds.skip(at + n),
{
    assert(deltas_of::<V, B>(o).len() == o.len());
    assert forall|i: int| 0 <= i < o.len() - n implies deltas_of::<V, B>(o.skip(n))[i] == ds.skip(at + n)[i] by {
        assert(deltas_of::<V, B>(o)[n + i] == ds.skip(at)[n + i]);
    }
    assert(deltas_of::<V, B>(o.skip(n)) =~= ds.skip(at + n));
}

/// Between its first and its last value a run of one difference stays:
/// each value of it lies between the two.
proof fn lemma_between(p: int, x: int, n: int, i: int, lo: int, hi: int)
    requires
        1 <= i <= n,
        lo <= p + x <= hi,
        lo <= p + n * x <= hi,
    ensures
        lo <= p + i * x <= hi,
{
    if x >= 0 {
        assert(x <= i * x <= n * x) by (nonlinear_arith)
            requires
                1 <= i <= n,
                x >= 0,
        ;
    } else {
        assert(n * x <= i * x <= x) by (nonlinear_arith)
            requires
                1 <= i <= n,
                x < 0,
        ;
    }
}

/// A run of nulls after differences whose values are the type's leaves
/// values that are the type's, where the type has nulls, and adds
/// nothing.
proof fn lemma_null_run<T: VDelta>(ds: Seq<Option<i64>>, k: int, n: int)
    requires
        0 <= k,
        0 <= n,
        k + n <= ds.len(),
        forall|j: int| k <= j < k + n ==> #[trigger] ds[j] == None::<i64>,
        all_dom::<T>(rz(0, ds.take(k))),
        T::nullable(),
    ensures
        all_dom::<T>(rz(0, ds.take(k + n))),
        psum(ds.take(k + n)) == psum(ds.take(k)),
{
    let a = rz(0, ds.take(k));
    let r = rz(0, ds.take(k + n));
    lemma_run(ds, k, n, None);
    lemma_rz_cat(0, ds.take(k), reps(n as nat, None));
    lemma_rz_nulls(psum(ds.take(k)), n as nat);
    lemma_rz_len(0, ds.take(k));
    assert(r == a + reps(n as nat, None));
    assert forall|i: int| 0 <= i < r.len() implies in_dom::<T>(#[trigger] r[i]) by {
        if i < a.len() {
            assert(r[i] == a[i]);
        } else {
            assert(r[i] == None::<i64>);
        }
    }
}

/// A run of the difference `x` after differences whose values are the
/// type's, its first and its last value the type's and no sum
/// wrapping, leaves values that are the type's.
proof fn lemma_num_run<T: VDelta>(ds: Seq<Option<i64>>, k: int, n: int, x: i64)
    requires
        0 <= k,
        1 <= n,
        k + n <= ds.len(),
        forall|j: int| k <= j < k + n ==> #[trigger] ds[j] == Some(x),
        all_dom::<T>(rz(0, ds.take(k))),
        T::least() <= psum(ds.take(k)) + x <= T::most(),
        T::least() <= psum(ds.take(k)) + n * x <= T::most(),
    ensures
        all_dom::<T>(rz(0, ds.take(k + n))),
        psum(ds.take(k + n)) == psum(ds.take(k)) + n * x,
{
    let p = psum(ds.take(k));
    let a = rz(0, ds.take(k));
    let r = rz(0, ds.take(k + n));
    lemma_run(ds, k, n, Some(x));
    lemma_rz_cat(0, ds.take(k), reps(n as nat, Some(x)));
    lemma_rz_run(p, x, n as nat);
    lemma_rz_len(0, ds.take(k));
    assert(r == a + run_vals(p, x, n as nat));
    assert forall|i: int| 0 <= i < r.len() implies in_dom::<T>(#[trigger] r[i]) by {
        if i < a.len() {
            assert(r[i] == a[i]);
        } else {
            let m = i - a.len();
            assert(r[i] == run_vals(p, x, n as nat)[m]);
            lemma_between(p, x as int, n, m + 1, T::least() as int, T::most() as int);
            lemma_crop(p + (m + 1) * x);
        }
    }
}

/// What a run the slab decoder gives says of the column's differences:
/// the first `n` of the items left are `v`, so the `n` differences from
/// `at` on are `v`'s; and where the run stopped short of `max` and of the
/// end, the difference after it is another.
proof fn lemma_head<'a, T: VDeltaValue<C>, C: VCodec>(
    o: Seq<DIt<'a, T, C>>,
    ds: Seq<Option<i64>>,
    at: int,
    n: int,
    max: int,
    v: DIt<'a, T, C>,
)
    requires
        0 <= at <= ds.len(),
        all_dom::<T>(rz(0, ds)),
        deltas_of::<DIt<'a, T, C>, DB<T, C>>(o) == ds.skip(at),
        1 <= n <= max,
        n <= o.len(),
        o.take(n) == Seq::new(n as nat, |_i: int| v),
        n < max && n < o.len() ==> o[n] != v,
    ensures
        at + n <= ds.len(),
        head_run_max(
            deltas(vals::<T>(rz(0, ds))).skip(at),
            <DB<T, C> as DeltaOps<DIt<'a, T, C>>>::dv(v),
            n,
            max,
        ),
        deltas_of::<DIt<'a, T, C>, DB<T, C>>(o.skip(n)) == ds.skip(at + n),
        psum(ds.take(at + n)) == psum(ds.take(at)) + match <DB<T, C> as DeltaOps<DIt<'a, T, C>>>::dv(v) {
            Some(x) => n * x,
            None => 0,
        },
        vals::<T>(rz(0, ds)).skip(at + n) == vals::<T>(rz(0, ds)).skip(at).skip(n),
{
    let delta = <DB<T, C> as DeltaOps<DIt<'a, T, C>>>::dv(v);
    let dso = deltas_of::<DIt<'a, T, C>, DB<T, C>>(o);
    let col = vals::<T>(rz(0, ds));
    assert(dso.len() == o.len());
    lemma_deltas::<T>(ds);
    lemma_moved::<DIt<'a, T, C>, DB<T, C>>(o, ds, at, n);
    assert forall|j: int| at <= j < at + n implies #[trigger] ds[j] == delta by {
        assert(o.take(n)[j - at] == v);
        assert(dso[j - at] == ds.skip(at)[j - at]);
    }
    lemma_run(ds, at, n, delta);
    match delta {
        Some(x) => {
            assert(dval(delta) == x);
        },
        None => {
            assert(dval(delta) == 0);
            assert(n * 0 == 0);
        },
    }
    let s = deltas(col).skip(at);
    assert(s =~= ds.skip(at));
    assert forall|i: int| 0 <= i < n implies #[trigger] s[i] == delta by {
        assert(ds[at + i] == delta);
    }
    // the difference after the run is another
    if n < max && n < s.len() {
        assert(dso[n] == ds.skip(at)[n]);
        T::lemma_item(o[n], v);
    }
    assert(col.skip(at + n) =~= col.skip(at).skip(n));
}

/// A decoder of a saved delta column of values of type `T`, its
/// numbers written with the codec `C`: it gives the column's values
/// one after another from the bytes as they are, without a column in
/// between, as [`DeltaDecoder`](crate::delta::DeltaDecoder) does.
///
/// It is a `std::iter::Iterator`. The values of the whole saved column
/// are a specification `column()`, how many of them the decoder has
/// passed is `pos()`, and what it has left to give, `rest()`, are the
/// column's values from there on; Verus's specifications of the
/// standard library's iterator functions speak of `remaining()`, which
/// is `rest()`. A decoder made of bytes has all the values to give that
/// the bytes are the saved column of
/// (`VerifiedDeltaColumn::values`), which are the values of the column
/// or the encoder that saved them.
///
/// It keeps the sum of the differences it has passed, wrapping as an
/// `i64` does; that no value of a saved column is a wrapped one is
/// part of what `valid` says of the bytes.
pub struct VerifiedDeltaDecoder<'a, T: VDeltaValue<C>, C: VCodec = Leb128> {
    d: <DX<T, C> as Decoders<'a>>::D,
    /// The sum of the differences passed, wrapping as an `i64` does.
    running: i64,
    /// The differences of the whole saved column.
    ds: Ghost<Seq<Option<i64>>>,
    /// How many of them are passed.
    at: Ghost<int>,
}

impl<'a, T: VDeltaValue<C>, C: VCodec> VerifiedDeltaDecoder<'a, T, C> {
    /// The decoder stands in a saved delta column's bytes with the
    /// differences from `at` on left to read and the sum of those
    /// before; and the column's values are the type's.
    #[verifier::type_invariant]
    spec fn inv(&self) -> bool {
        &&& self.d.inv()
        &&& 0 <= self.at@ <= self.ds@.len()
        &&& deltas_of::<DIt<'a, T, C>, DB<T, C>>(self.d.rest()) == self.ds@.skip(self.at@)
        &&& self.running == crop(psum(self.ds@.take(self.at@)))
        &&& all_dom::<T>(rz(0, self.ds@))
    }

    /// The values of the whole saved column that the decoder reads.
    pub closed spec fn column(&self) -> Seq<T> {
        vals::<T>(rz(0, self.ds@))
    }

    /// How many values of the column the decoder has passed.
    pub closed spec fn pos(&self) -> nat {
        self.at@ as nat
    }

    /// The values the decoder has left to give.
    pub open spec fn rest(&self) -> Seq<T> {
        self.column().skip(self.pos() as int)
    }

    /// A decoder stands within its column or at its end, and the
    /// column's values fit: they are values a delta column holds.
    pub proof fn lemma_column(tracked &self)
        ensures
            self.pos() <= self.column().len(),
            all_fit(self.column()),
    {
        use_type_invariant(self);
        lemma_rz_len(0, self.ds@);
        lemma_vals_fit::<T>(rz(0, self.ds@));
    }

    /// A decoder of the saved delta column `bytes`, which has all its
    /// values to give.
    ///
    /// The bytes are not looked at first: that they are a saved delta
    /// column is required. A client that is compiled without Verus has
    /// that to keep by itself, and on other bytes the decoder may panic
    /// or give nonsense. For bytes of which it is not known, there is
    /// [`try_new`](Self::try_new).
    #[inline]
    pub fn new(bytes: &'a [u8]) -> (r: Self)
        requires
            VerifiedDeltaColumn::<T, C>::valid(bytes@),
        ensures
            r.column() == VerifiedDeltaColumn::<T, C>::values(bytes@),
            r.pos() == 0,
            r.rest() == VerifiedDeltaColumn::<T, C>::values(bytes@),
    {
        let d = <DE<T, C> as Model<DX<T, C>>>::decoder(bytes);
        let ghost ds = saved_ds::<T, C>(bytes@);
        proof {
            lemma_saved::<T, C>(bytes@);
            lemma_read::<T, C>(d.rest(), <DE<T, C> as Model<DX<T, C>>>::values(bytes@));
            assert(ds.skip(0) =~= ds);
            assert(ds.take(0) =~= Seq::<Option<i64>>::empty());
            lemma_crop(0);
            assert(vals::<T>(rz(0, ds)).skip(0) =~= vals::<T>(rz(0, ds)));
        }
        VerifiedDeltaDecoder { d, running: 0, ds: Ghost(ds), at: Ghost(0) }
    }

    /// A decoder of the saved delta column `bytes`, which are read
    /// through first: an error unless they are a saved column of the
    /// column underneath, and an error unless the values are the
    /// type's, by the check the old API's `try_new` makes.
    ///
    /// The check adds the differences up exactly, run by run: a run is
    /// taken if its first and its last value are the type's and
    /// neither sum leaves `i64`. That is enough for every value to be
    /// the type's, which is what is proved: bytes that are taken are a
    /// saved delta column. It is not said that every saved delta column
    /// is taken, as it is not said of
    /// [`load`](crate::verified::VerifiedDeltaColumn::load).
    pub fn try_new(bytes: &'a [u8]) -> (r: Result<Self, PackError>)
        ensures
            r matches Ok(d) ==> {
                &&& VerifiedDeltaColumn::<T, C>::valid(bytes@)
                &&& d.column() == VerifiedDeltaColumn::<T, C>::values(bytes@)
                &&& d.pos() == 0
                &&& d.rest() == VerifiedDeltaColumn::<T, C>::values(bytes@)
            },
    {
        match <DE<T, C> as Model<DX<T, C>>>::validate(bytes) {
            Ok(_) => {},
            Err(e) => {
                return Err(e);
            },
        }
        let mut d = <DE<T, C> as Model<DX<T, C>>>::decoder(bytes);
        let ghost vs = <DE<T, C> as Model<DX<T, C>>>::values(bytes@);
        let ghost ds = saved_ds::<T, C>(bytes@);
        let ghost mut k: int = 0;
        let mut running: i64 = 0;
        let (lo, hi) = (T::lo(), T::hi());
        proof {
            lemma_read::<T, C>(d.rest(), vs);
            assert(ds.skip(0) =~= ds);
            assert(ds.take(0) =~= Seq::<Option<i64>>::empty());
            assert(rz(0, ds.take(0)) =~= Seq::<Option<i64>>::empty());
        }
        loop
            invariant
                d.inv(),
                0 <= k <= ds.len(),
                ds == saved_ds::<T, C>(bytes@),
                vs == <DE<T, C> as Model<DX<T, C>>>::values(bytes@),
                deltas_of::<DIt<'a, T, C>, DB<T, C>>(d.rest()) == ds.skip(k),
                running as int == psum(ds.take(k)),
                all_dom::<T>(rz(0, ds.take(k))),
                lo == T::least(),
                hi == T::most(),
            ensures
                k == ds.len(),
            decreases ds.len() - k,
        {
            let ghost o = d.rest();
            proof {
                assert(deltas_of::<DIt<'a, T, C>, DB<T, C>>(o).len() == o.len());
            }
            match d.d_run_max(usize::MAX) {
                None => {
                    break;
                },
                Some(run) => {
                    let delta = <DB<T, C> as DeltaOps<DIt<'a, T, C>>>::to_opt(run.value);
                    let ghost n = run.count as int;
                    proof {
                        assert forall|j: int| k <= j < k + n implies #[trigger] ds[j] == delta by {
                            assert(o.take(n)[j - k] == run.value);
                            assert(deltas_of::<DIt<'a, T, C>, DB<T, C>>(o)[j - k] == ds.skip(k)[j - k]);
                        }
                        lemma_moved::<DIt<'a, T, C>, DB<T, C>>(o, ds, k, n);
                    }
                    match delta {
                        None => {
                            proof {
                                // a null among the bytes: the type has
                                // nulls
                                T::lemma_kind(vs[k], vs[k]);
                                assert(ds[k] == <DB<T, C> as DeltaWrite<'static, DE<T, C>, DX<T, C>>>::dvo(vs[k]));
                                lemma_null_run::<T>(ds, k, n);
                            }
                        },
                        Some(x) => {
                            // the run's first value and its last, the
                            // sums exact
                            let first = running.checked_add(x);
                            let c = run.count as u64;
                            let last = if c <= i64::MAX as u64 {
                                match x.checked_mul(c as i64) {
                                    Some(step) => running.checked_add(step),
                                    None => None,
                                }
                            } else {
                                None
                            };
                            match (first, last) {
                                (Some(f), Some(l)) => {
                                    if lo <= f && f <= hi && lo <= l && l <= hi {
                                        proof {
                                            assert(x * n == n * x) by (nonlinear_arith);
                                            lemma_num_run::<T>(ds, k, n, x);
                                        }
                                        running = l;
                                    } else {
                                        return Err(delta_out_of_domain());
                                    }
                                },
                                _ => {
                                    return Err(delta_out_of_domain());
                                },
                            }
                        },
                    }
                    proof {
                        k = k + n;
                    }
                },
            }
        }
        proof {
            assert(ds.take(k) =~= ds);
            lemma_saved::<T, C>(bytes@);
        }
        Ok(Self::new(bytes))
    }

    /// Move `n` values on: past as many as the decoder has left, if it
    /// has fewer.
    pub fn advance_by(&mut self, n: usize)
        ensures
            final(self).column() == old(self).column(),
            final(self).pos() == if old(self).pos() + n <= old(self).column().len() {
                old(self).pos() + n
            } else {
                old(self).column().len() as int
            },
            final(self).rest() == past(old(self).rest(), n as int),
    {
        let ghost me = *self;
        proof {
            use_type_invariant(&*self);
            lemma_rz_len(0, me.ds@);
        }
        if n > 0 {
            let _ = self.nth(n - 1);
        }
        proof {
            let (col, p) = (me.column(), me.pos() as int);
            if n > 0 {
                if n <= me.rest().len() {
                    assert(col.skip(p + n) =~= col.skip(p).skip(n as int));
                } else {
                    assert(col.skip(col.len() as int) =~= Seq::<T>::empty());
                }
            }
        }
    }

    /// The next run of differences, of no more than `max` values: the
    /// difference between the value the decoder gives next and the
    /// number before it (or none, for a null), and how many times that
    /// difference comes before another does, `max` are reached or the
    /// decoder has no more. The decoder moves past those values. With a
    /// `max` of 0 there is no run to give: `None`, and the decoder does
    /// not move.
    ///
    /// The differences are those of the column, `deltas(column())`,
    /// from `pos()` on; the number before the run is
    /// `delta_prefix(column(), pos())`, and `lemma_delta_run_at` says
    /// what values such a run stands for.
    #[inline]
    pub fn next_delta_run_max(&mut self, max: usize) -> (r: Option<Run<Option<i64>>>)
        ensures
            final(self).column() == old(self).column(),
            match r {
                Some(run) => {
                    &&& head_run_max(
                        deltas(old(self).column()).skip(old(self).pos() as int),
                        run.value,
                        run.count as int,
                        max as int,
                    )
                    &&& final(self).pos() == old(self).pos() + run.count
                    &&& final(self).rest() == old(self).rest().skip(run.count as int)
                },
                None => {
                    &&& max == 0 || old(self).rest().len() == 0
                    &&& final(self).pos() == old(self).pos()
                },
            },
    {
        // What `head_run_max` says is given whole by `lemma_head` and
        // wanted whole: opened here, it would have the solver spend half
        // of what it may on the laws of sequences.
        hide(head_run_max);
        let ghost me = *self;
        proof {
            use_type_invariant(&*self);
            lemma_deltas::<T>(me.ds@);
        }
        if max == 0 {
            return None;
        }
        let ghost (ds, at) = (me.ds@, me.at@);
        let ghost o = me.d.rest();
        proof {
            assert(deltas_of::<DIt<'a, T, C>, DB<T, C>>(o).len() == o.len());
        }
        // the reader and the sum move together, and are written back
        // together
        let mut d = self.d.dup();
        match d.d_run_max(max) {
            Some(run) => {
                let delta = <DB<T, C> as DeltaOps<DIt<'a, T, C>>>::to_opt(run.value);
                let ghost n = run.count as int;
                proof {
                    lemma_head::<T, C>(o, ds, at, n, max as int, run.value);
                }
                let running = match delta {
                    Some(x) => add_run(self.running, x, run.count, Ghost(psum(ds.take(at)))),
                    None => self.running,
                };
                *self = VerifiedDeltaDecoder { d, running, ds: Ghost(ds), at: Ghost(at + n) };
                Some(Run { count: run.count, value: delta })
            },
            None => None,
        }
    }
}

#[cfg(verus_keep_ghost)]
impl<'a, T: VDeltaValue<C>, C: VCodec> vstd::std_specs::iter::IteratorSpecImpl for VerifiedDeltaDecoder<'a, T, C> {
    /// The decoder keeps to what Verus asks of an iterator: `next`
    /// gives `remaining()`, one value after another, and then `None`.
    open spec fn obeys_prophetic_iter_laws(&self) -> bool {
        true
    }

    open spec fn remaining(&self) -> Seq<T> {
        self.rest()
    }

    open spec fn will_return_none(&self) -> bool {
        true
    }

    open spec fn decrease(&self) -> Option<nat> {
        Some(self.rest().len())
    }

    open spec fn peek(&self, index: int) -> Option<T> {
        if 0 <= index < self.rest().len() {
            Some(self.rest()[index])
        } else {
            None
        }
    }
}

impl<'a, T: VDeltaValue<C>, C: VCodec> Iterator for VerifiedDeltaDecoder<'a, T, C> {
    type Item = T;

    /// The next value, if one is left.
    #[inline]
    fn next(&mut self) -> (r: Option<T>)
        ensures
            final(self).column() == old(self).column(),
            match r {
                Some(v) => {
                    &&& old(self).rest().len() > 0
                    &&& v == old(self).rest()[0]
                    &&& final(self).rest() == old(self).rest().skip(1)
                    &&& final(self).pos() == old(self).pos() + 1
                },
                None => {
                    &&& old(self).rest().len() == 0
                    &&& final(self).pos() == old(self).pos()
                },
            },
    {
        let ghost me = *self;
        proof {
            use_type_invariant(&*self);
            lemma_rz_len(0, me.ds@);
        }
        let ghost (ds, at) = (me.ds@, me.at@);
        let ghost o = me.d.rest();
        proof {
            assert(deltas_of::<DIt<'a, T, C>, DB<T, C>>(o).len() == o.len());
        }
        // the reader and the sum move together, and are written back
        // together
        let mut d = self.d.dup();
        match d.d_next() {
            Some(item) => {
                let delta = <DB<T, C> as DeltaOps<DIt<'a, T, C>>>::to_opt(item);
                proof {
                    assert(deltas_of::<DIt<'a, T, C>, DB<T, C>>(o)[0] == ds.skip(at)[0]);
                    lemma_moved::<DIt<'a, T, C>, DB<T, C>>(o, ds, at, 1);
                    lemma_at::<T>(ds, at);
                    assert(me.rest().skip(1) =~= me.column().skip(at + 1));
                    assert(me.rest().skip(1) =~= me.rest().drop_first());
                }
                match delta {
                    Some(x) => {
                        let running = self.running.wrapping_add(x);
                        proof {
                            lemma_wrapping_add(self.running, x);
                            lemma_crop_add(psum(ds.take(at)), x as int);
                        }
                        *self = VerifiedDeltaDecoder { d, running, ds: Ghost(ds), at: Ghost(at + 1) };
                        Some(T::from_i64(Some(running)))
                    },
                    None => {
                        *self = VerifiedDeltaDecoder { d, running: self.running, ds: Ghost(ds), at: Ghost(at + 1) };
                        Some(T::from_i64(None))
                    },
                }
            },
            None => None,
        }
    }

    /// The `n`th of the values left, counted from 0, if there are that
    /// many: it and the values before it are then given. If there are
    /// not, none is left.
    ///
    /// The values before it are passed over a run of differences at a
    /// time.
    fn nth(&mut self, n: usize) -> (r: Option<T>)
        ensures
            final(self).column() == old(self).column(),
            n < old(self).rest().len() ==> {
                &&& r == Some(old(self).rest()[n as int])
                &&& final(self).pos() == old(self).pos() + n + 1
            },
            n >= old(self).rest().len() ==> {
                &&& r is None
                &&& final(self).pos() == old(self).column().len()
            },
    {
        let ghost me = *self;
        proof {
            use_type_invariant(&*self);
            lemma_deltas::<T>(me.ds@);
        }
        let mut left = n;
        while left > 0
            invariant
                me == *old(self),
                self.column() == me.column(),
                deltas(me.column()).len() == me.column().len(),
                left <= n,
                self.pos() == me.pos() + (n - left),
                self.pos() <= me.column().len(),
            decreases left,
        {
            match self.next_delta_run_max(left) {
                Some(run) => {
                    left = left - run.count;
                },
                None => {
                    return None;
                },
            }
        }
        self.next()
    }
}

impl<'a, T: VDeltaValue<C>, C: VCodec> Clone for VerifiedDeltaDecoder<'a, T, C> {
    /// A decoder of its own of the same column, at the same place.
    fn clone(&self) -> (r: Self)
        ensures
            r.column() == self.column(),
            r.pos() == self.pos(),
            r.rest() == self.rest(),
    {
        proof {
            use_type_invariant(self);
        }
        VerifiedDeltaDecoder { d: self.d.dup(), running: self.running, ds: self.ds, at: self.at }
    }
}

} // verus!

impl<T: VDeltaValue<C>, C: VCodec> std::fmt::Debug for VerifiedDeltaDecoder<'_, T, C> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("VerifiedDeltaDecoder")
            .field("running", &self.running)
            .finish()
    }
}
