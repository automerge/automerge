//! The delta encoder of the verified API: [`VerifiedDeltaEncoder`] and
//! [`VerifiedDeltaEncoderState`], which write the bytes of a delta
//! column from values given one after another. They mirror
//! [`DeltaEncoder`](crate::DeltaEncoder) and
//! [`DeltaEncoderState`](crate::delta::DeltaEncoderState).
//!
//! A delta column keeps each number as its difference from the number
//! before it, and its saved form is that of the column of the
//! differences. So the encoder here gives the differences to the
//! encoder of that column ([`VerifiedEncoderState`]) and keeps, itself,
//! the last number given. What is proved is said of the bytes: they are
//! the encoding of the values given (`VerifiedDeltaColumn::encoding`),
//! the bytes that a column of those values saves.
//!
//! Differences are taken wrapping and read wrapping, so the bytes are
//! the encoding whatever numbers are given. But a delta column holds no
//! values that span 2^63 or more, and no load takes their bytes: so
//! `finish` panics for such values, as the old encoder does, and what it
//! returns from promises that there are none (`delta_span_ok`).
#[cfg(verus_keep_ghost)]
use super::{all_fit, dvs, lemma_kept, VerifiedDeltaColumn, DE, DX};
use super::{DKind, VDelta, VDeltaValue};
#[cfg(verus_keep_ghost)]
use crate::cursor::Slabs;
#[cfg(verus_keep_ghost)]
use crate::delta::agg::{psum, reps};
#[cfg(verus_keep_ghost)]
use crate::delta::ddomain::DeltaWrite;
use crate::delta::dedit::delta_to;
#[cfg(verus_keep_ghost)]
use crate::delta::dedit::{
    lemma_psum_one, lemma_rz_cat, lemma_rz_nulls, lemma_rz_one, lemma_rz_run, rz,
};
#[cfg(verus_keep_ghost)]
use crate::delta::diter::{crop, lemma_crop, lemma_crop_add, lemma_psum_cat};
use crate::delta::stream::values_too_wide;
#[cfg(verus_keep_ghost)]
use crate::delta::stream::{lemma_vals_run, span_ok, vhigh, vlow, wide};
#[cfg(verus_keep_ghost)]
use crate::rle::state::copies;
use crate::verified::codec::VCodec;
#[cfg(verus_keep_ghost)]
use crate::verified::model::Model;
use crate::verified::VerifiedEncoderState;
#[cfg(verus_keep_ghost)]
use crate::verified::{only, VGet, VSpec, VerifiedColumn};
use crate::Leb128;
use std::ops::Range;
use vstd::prelude::*;
#[cfg(verus_keep_ghost)]
use vstd::std_specs::iter::IteratorSpec;

verus! {

/// Whether `b` is less than 2^63 above `a`; so it is, too, where one of
/// them is null.
pub open spec fn delta_near<T: VDelta>(a: T, b: T) -> bool {
    match (a.dv(), b.dv()) {
        (Some(x), Some(y)) => y - x <= i64::MAX,
        _ => true,
    }
}

/// Whether no two numbers among `vs` are 2^63 or more apart. A delta
/// column holds no values that are, and an encoder panics for them when
/// it is done ([`finish`](VerifiedDeltaEncoderState::finish)).
pub open spec fn delta_span_ok<T: VDelta>(vs: Seq<T>) -> bool {
    forall|i: int, j: int| 0 <= i < vs.len() && 0 <= j < vs.len() ==> delta_near(#[trigger] vs[i], #[trigger] vs[j])
}

/// Whether the values an iterator has to give are known to fit a delta
/// column: every value of their type does (every type's but `u64`'s,
/// `usize`'s and their `Option`s'), or the iterator keeps the laws Verus
/// has for one and what it has to give fits.
#[verifier::prophetic]
pub open spec fn delta_iter_fits<T: VDelta, I: Iterator<Item = T>>(it: I) -> bool {
    (forall|v: T| #[trigger] v.fits()) || (it.obeys_prophetic_iter_laws() && all_fit(it.remaining()))
}

/// A number among `vs` is between the least and the greatest of them.
proof fn lemma_between(vs: Seq<Option<i64>>, i: int)
    requires
        0 <= i < vs.len(),
        vs[i] is Some,
    ensures
        vlow(vs) <= vs[i]->Some_0 <= vhigh(vs),
    decreases vs.len(),
{
    if i < vs.len() - 1 {
        lemma_between(vs.drop_last(), i);
    }
}

/// What an encoder's `uniform` says of the values it keeps: that they
/// are all the one, or that there is none or more than one.
spec fn told(u: Option<Option<i64>>, vs: Seq<Option<i64>>) -> bool {
    match u {
        Some(k) => vs.len() > 0 && only(vs, k),
        None => vs.len() == 0 || !only(vs, vs[0]),
    }
}

/// `uniform` after `n` more of `k`.
proof fn lemma_told(u: Option<Option<i64>>, vs: Seq<Option<i64>>, k: Option<i64>, n: nat)
    requires
        told(u, vs),
        n >= 1,
    ensures
        told(
            if vs.len() == 0 {
                Some(k)
            } else if u == Some(k) {
                u
            } else {
                None
            },
            vs + copies(n, k),
        ),
{
    let ws = vs + copies(n, k);
    if vs.len() == 0 || u == Some(k) {
        assert(only(ws, k));
    } else {
        assert(ws[0] == vs[0]);
        assert(ws[vs.len() as int] == k);
        match u {
            Some(a) => {
                assert(vs[0] == a);
            },
            None => {
                let i = choose|i: int| 0 <= i < vs.len() && (#[trigger] vs[i]) != vs[0];
                assert(ws[i] == vs[i]);
            },
        }
        assert(!only(ws, ws[0]));
    }
}

/// What differences read as, wrapping, once a number `x` is given `n`
/// times after them: by its difference `d` from the last number, and
/// then `n - 1` zeros.
proof fn lemma_given_some(ds0: Seq<Option<i64>>, d: i64, x: i64, n: nat)
    requires
        n >= 1,
        crop(crop(psum(ds0)) + d) == x,
    ensures
        ({
            let ds = ds0 + (seq![Some(d)] + reps((n - 1) as nat, Some(0i64)));
            &&& rz(0, ds) == rz(0, ds0) + copies(n, Some(x))
            &&& crop(psum(ds)) == x
        }),
{
    let p0 = psum(ds0);
    let m = (n - 1) as nat;
    let one = seq![Some(d)];
    let zeros = reps(m, Some(0i64));
    // read wrapping, the difference is the number, and each 0 after
    // it the number again
    lemma_rz_cat(0, ds0, one + zeros);
    lemma_rz_cat(p0, one, zeros);
    lemma_rz_one(p0, d);
    lemma_psum_one(d);
    lemma_crop_add(p0, d as int);
    lemma_rz_run(p0 + d, 0, m);
    lemma_psum_cat(ds0, one + zeros);
    lemma_psum_cat(one, zeros);
    assert(crop(p0 + d) == x);
    assert(rz(p0 + d, zeros) =~= copies(m, Some(x)));
    assert(seq![Some(x)] + copies(m, Some(x)) =~= copies(n, Some(x)));
}

/// What differences read as, wrapping, once `n` nulls are given after
/// them: the nulls, which leave the sum.
proof fn lemma_given_none(ds0: Seq<Option<i64>>, n: nat)
    ensures
        ({
            let ds = ds0 + reps(n, None);
            &&& rz(0, ds) == rz(0, ds0) + copies(n, None::<i64>)
            &&& psum(ds) == psum(ds0)
        }),
{
    let none = reps(n, None::<i64>);
    lemma_rz_cat(0, ds0, none);
    lemma_rz_nulls(psum(ds0), n);
    lemma_psum_cat(ds0, none);
    assert(none =~= copies(n, None::<i64>));
}

/// Whether `u` says that every value is kept as `k`.
#[inline(always)]
fn is_kept(u: Option<Option<i64>>, k: Option<i64>) -> (r: bool)
    ensures
        r == (u == Some(k)),
{
    match (u, k) {
        (Some(Some(a)), Some(b)) => a == b,
        (Some(None), None) => true,
        _ => false,
    }
}

/// Whether the type's numbers can be 2^63 or more apart.
#[inline(always)]
fn is_wide<T: VDelta>() -> (r: bool)
    ensures
        r == wide(T::least(), T::most()),
{
    T::hi() as i128 - T::lo() as i128 > i64::MAX as i128
}

/// A delta encoder that writes into a buffer of its caller's, which is
/// given to every call that may write: it holds no bytes of its own.
/// [`finish`](Self::finish) writes what is left.
///
/// The buffer must be the same one each time, with what this state wrote
/// to it left as it is: that is `on`, which every method that takes the
/// buffer requires and nothing checks as it runs.
pub struct VerifiedDeltaEncoderState<'a, T: VDeltaValue<C>, C: VCodec = Leb128> {
    /// The encoder of the differences.
    s: VerifiedEncoderState<'a, <T as DKind<C>>::I, C>,
    /// The last number given, 0 before the first.
    abs: i64,
    /// The least and the greatest number given, kept only for the types
    /// whose numbers can be 2^63 apart.
    low: i64,
    high: i64,
    /// What every value given is kept as, where all are kept as one and
    /// the same.
    uniform: Option<Option<i64>>,
    /// The values given.
    #[allow(dead_code)]
    vals: Ghost<Seq<T>>,
}

impl<'a, T: VDeltaValue<C>, C: VCodec> View for VerifiedDeltaEncoderState<'a, T, C> {
    type V = Seq<T>;

    /// The values given so far.
    closed spec fn view(&self) -> Seq<T> {
        self.vals@
    }
}

impl<'a, T: VDeltaValue<C>, C: VCodec> Default for VerifiedDeltaEncoderState<'a, T, C> {
    fn default() -> (r: Self)
        ensures
            r@ == Seq::<T>::empty(),
            forall|buf: Seq<u8>| #[trigger] r.on(buf),
            forall|buf: Seq<u8>| #[trigger] r.before(buf) == buf,
    {
        Self::new()
    }
}

impl<'a, T: VDeltaValue<C>, C: VCodec> VerifiedDeltaEncoderState<'a, T, C> {
    /// The differences given to the encoder underneath.
    spec fn given(&self) -> Seq<Option<i64>> {
        self.s@.map_values(|s: <<T as DKind<C>>::I as VSpec>::S| <T as DKind<C>>::d_of(s))
    }

    /// The values given fit, and the differences given to the encoder
    /// underneath are theirs: read wrapping, they are the values, and
    /// their sum is the last number.
    spec fn ok(&self) -> bool {
        let vs = dvs(self.vals@);
        let ds = self.given();
        &&& all_fit(self.vals@)
        &&& rz(0, ds) == vs
        &&& crop(psum(ds)) == self.abs
        &&& wide(T::least(), T::most()) ==> self.low == vlow(vs) && self.high == vhigh(vs)
        &&& told(self.uniform, vs)
    }

    /// `buf` is what the buffer this state writes into holds now.
    pub closed spec fn on(&self, buf: Seq<u8>) -> bool {
        self.s.on(buf) && self.ok()
    }

    /// What the buffer held before this state wrote to it, the buffer
    /// holding `buf` now.
    pub closed spec fn before(&self, buf: Seq<u8>) -> Seq<u8> {
        self.s.before(buf)
    }

    /// A state that was given nothing. It may start on any buffer.
    #[inline(always)]
    pub fn new() -> (r: Self)
        ensures
            r@ == Seq::<T>::empty(),
            forall|buf: Seq<u8>| #[trigger] r.on(buf),
            forall|buf: Seq<u8>| #[trigger] r.before(buf) == buf,
    {
        let r = VerifiedDeltaEncoderState {
            s: VerifiedEncoderState::new(),
            abs: 0,
            low: i64::MAX,
            high: i64::MIN,
            uniform: None,
            vals: Ghost(Seq::empty()),
        };
        proof {
            assert(r.given() =~= Seq::<Option<i64>>::empty());
            assert(dvs(r.vals@) =~= Seq::<Option<i64>>::empty());
            assert(rz(0, r.given()) =~= Seq::<Option<i64>>::empty());
            lemma_crop(0);
            assert(r.ok());
        }
        r
    }

    /// The number of values given.
    #[inline(always)]
    pub fn len(&self) -> (r: usize)
        requires
            exists|buf: Seq<u8>| self.on(buf),
        ensures
            r == self@.len(),
    {
        proof {
            let buf = choose|buf: Seq<u8>| self.on(buf);
            assert(self.s.on(buf));
            assert(rz(0, self.given()).len() == self.s@.len());
        }
        self.s.len()
    }

    /// Whether no value was given.
    #[inline(always)]
    pub fn is_empty(&self) -> (r: bool)
        requires
            exists|buf: Seq<u8>| self.on(buf),
        ensures
            r == (self@.len() == 0),
    {
        self.len() == 0
    }

    /// Give the state `value`, writing to `buf` what no longer needs to
    /// be held.
    ///
    /// The value must be one a delta column holds (`fits`): the old
    /// encoder panics for one that is not. Like every method that gives
    /// the state values, this panics if there would be more of them than
    /// a column can count (some `usize::MAX`).
    #[inline(always)]
    pub fn append(&mut self, buf: &mut Vec<u8>, value: T)
        requires
            old(self).on(old(buf)@),
            value.fits(),
        ensures
            final(self).on(final(buf)@),
            final(self).before(final(buf)@) == old(self).before(old(buf)@),
            final(self)@ == old(self)@.push(value),
    {
        self.append_n(buf, value, 1);
        proof {
            assert(self@ =~= old(self)@.push(value));
        }
    }

    /// Give the state `value`, `n` times over.
    pub fn append_n(&mut self, buf: &mut Vec<u8>, value: T, n: usize)
        requires
            old(self).on(old(buf)@),
            value.fits(),
        ensures
            final(self).on(final(buf)@),
            final(self).before(final(buf)@) == old(self).before(old(buf)@),
            final(self)@ == old(self)@ + Seq::new(n as nat, |_i: int| value),
    {
        let ghost vals0 = self.vals@;
        let ghost vs0 = dvs(vals0);
        let ghost ds0 = self.given();
        let ghost s0 = self.s@;
        let ghost u0 = self.uniform;
        let ghost added = Seq::new(n as nat, |_i: int| value);
        if n == 0 {
            proof {
                assert(vals0 + added =~= vals0);
            }
            return ;
        }
        let k = <T as VDelta>::to_i64(value);
        proof {
            T::lemma_delta(value, k);
            assert(self.s.on(buf@));
            assert(rz(0, ds0).len() == s0.len());
            lemma_told(u0, vs0, k, n as nat);
            assert(dvs(vals0 + added) =~= vs0 + copies(n as nat, k));
            lemma_vals_run(vs0, n as nat, k);
        }
        if self.s.is_empty() {
            self.uniform = Some(k);
        } else if !is_kept(self.uniform, k) {
            self.uniform = None;
        }
        match k {
            Some(x) => {
                if is_wide::<T>() {
                    self.low = if self.low <= x { self.low } else { x };
                    self.high = if self.high >= x { self.high } else { x };
                }
                let d = delta_to(self.abs, x);
                self.abs = x;
                let first = <T as DKind<C>>::item(Some(d));
                self.s.append_owned(buf, first);
                let rest = <T as DKind<C>>::item(Some(0));
                if n > 1 {
                    self.s.append_n_owned(buf, rest, n - 1);
                }
                proof {
                    self.vals = Ghost(vals0 + added);
                    let m = (n - 1) as nat;
                    let (a, b) = (
                        <<T as DKind<C>>::I as VSpec>::sv_of(first),
                        <<T as DKind<C>>::I as VSpec>::sv_of(rest),
                    );
                    assert(self.s@ =~= s0.push(a) + Seq::new(m, |_i: int| b));
                    assert(self.given() =~= ds0 + (seq![Some(d)] + reps(m, Some(0i64))));
                    lemma_given_some(ds0, d, x, n as nat);
                    assert(self.ok());
                }
            },
            None => {
                let nulls = <T as DKind<C>>::item(None);
                self.s.append_n_owned(buf, nulls, n);
                proof {
                    self.vals = Ghost(vals0 + added);
                    let a = <<T as DKind<C>>::I as VSpec>::sv_of(nulls);
                    assert(self.s@ =~= s0 + Seq::new(n as nat, |_i: int| a));
                    assert(self.given() =~= ds0 + reps(n as nat, None));
                    lemma_given_none(ds0, n as nat);
                    assert(self.ok());
                }
            },
        }
    }

    /// What the buffer held before this state wrote to it is still at
    /// its start.
    pub proof fn lemma_before(&self, buf: Seq<u8>)
        requires
            self.on(buf),
        ensures
            self.before(buf).len() <= buf.len(),
            buf.take(self.before(buf).len() as int) == self.before(buf),
    {
        self.s.lemma_before(buf);
    }

    /// Every value given is one a delta column holds.
    pub proof fn lemma_fits(&self, buf: Seq<u8>)
        requires
            self.on(buf),
        ensures
            all_fit(self@),
    {
    }

    /// Give the state every value of `iter`, in order.
    ///
    /// The values must be known to fit (`delta_iter_fits`): that asks
    /// nothing of an iterator over `u32`s, `i32`s or `i64`s, or over
    /// `Option`s of them.
    #[verifier::exec_allows_no_decreases_clause]
    pub fn extend<I: IntoIterator<Item = T>>(&mut self, buf: &mut Vec<u8>, iter: I)
        requires
            old(self).on(old(buf)@),
            forall|it: I::IntoIter| #[trigger] (I::into_iter).ensures((iter,), it) ==> delta_iter_fits(it),
        ensures
            final(self).on(final(buf)@),
            final(self).before(final(buf)@) == old(self).before(old(buf)@),
            exists|it: I::IntoIter| #[trigger]
                (I::into_iter).ensures((iter,), it) && (it.obeys_prophetic_iter_laws() ==> final(self)@ == old(
                    self,
                )@ + it.remaining()),
    {
        let mut it = iter.into_iter();
        let ghost it0 = it;
        let ghost mut given: Seq<T> = Seq::empty();
        proof {
            assert(given + it.remaining() =~= it0.remaining());
            assert(self@ =~= old(self)@ + given);
        }
        #[allow(clippy::while_let_loop)]
        loop
            invariant_except_break
                it.obeys_prophetic_iter_laws() ==> given + it.remaining() == it0.remaining(),
            invariant
                self.on(buf@),
                self.before(buf@) == old(self).before(old(buf)@),
                it.obeys_prophetic_iter_laws() == it0.obeys_prophetic_iter_laws(),
                self@ == old(self)@ + given,
                delta_iter_fits(it0),
            ensures
                it0.obeys_prophetic_iter_laws() ==> given == it0.remaining(),
        {
            let ghost before = given;
            let ghost rem = it.remaining();
            match it.next() {
                Some(v) => {
                    proof {
                        // it fits: every value of the type does, or it
                        // is one of those the iterator had to give
                        if it0.obeys_prophetic_iter_laws() {
                            assert(it0.remaining()[before.len() as int] == v);
                        }
                    }
                    self.append(buf, v);
                    proof {
                        given = before.push(v);
                        assert(self@ =~= old(self)@ + given);
                        if it.obeys_prophetic_iter_laws() {
                            assert(given + it.remaining() =~= before + rem);
                        }
                    }
                },
                None => {
                    proof {
                        if it.obeys_prophetic_iter_laws() {
                            assert(given =~= before + rem);
                        }
                    }
                    break ;
                },
            }
        }
    }

    /// Whether every value given is `value`: so it is of a state that was
    /// given nothing, whatever `value` is. No value given is one that
    /// does not fit.
    pub fn is_uniform(&self, value: T) -> (r: bool)
        requires
            exists|buf: Seq<u8>| self.on(buf),
        ensures
            r == only(self@, value),
    {
        let ghost vs = dvs(self.vals@);
        proof {
            let buf = choose|buf: Seq<u8>| self.on(buf);
            assert(self.s.on(buf));
            assert(rz(0, self.given()).len() == self.s@.len());
        }
        if self.s.is_empty() {
            return true;
        }
        match <T as VDelta>::try_i64(value) {
            Some(k) => {
                let r = is_kept(self.uniform, k);
                proof {
                    T::lemma_delta(value, k);
                    if r {
                        assert forall|i: int| 0 <= i < self.vals@.len() implies self.vals@[i] == value by {
                            assert(vs[i] == k);
                            T::lemma_delta(self.vals@[i], k);
                        }
                    } else {
                        // some value given is kept as another thing
                        let i = if self.uniform is Some {
                            0
                        } else {
                            choose|i: int| 0 <= i < vs.len() && (#[trigger] vs[i]) != vs[0]
                        };
                        assert(vs[i] != k || vs[0] != k);
                        assert(self.vals@[i] != value || self.vals@[0] != value);
                    }
                }
                r
            },
            None => {
                proof {
                    assert(self.vals@[0].fits());
                }
                false
            },
        }
    }

    /// Write to `buf` what is left: `buf` then holds, after what it held
    /// before the state wrote to it, the encoding of the values given,
    /// which is the bytes a delta column of those values saves.
    ///
    /// This panics if two of the numbers given are 2^63 or more apart,
    /// as the old encoder does: a delta column holds no such values.
    pub fn finish(self, buf: &mut Vec<u8>)
        requires
            self.on(old(buf)@),
        ensures
            final(buf)@ == self.before(old(buf)@) + VerifiedDeltaColumn::<T, C>::encoding(self@),
            VerifiedDeltaColumn::<T, C>::valid(VerifiedDeltaColumn::<T, C>::encoding(self@)),
            VerifiedDeltaColumn::<T, C>::values(VerifiedDeltaColumn::<T, C>::encoding(self@)) == self@,
            delta_span_ok(self@),
    {
        let ghost vals = self.vals@;
        let ghost vs = dvs(vals);
        let ghost ds = self.given();
        let ghost given = self.s@;
        if is_wide::<T>() && self.low <= self.high && self.high.checked_sub(self.low).is_none() {
            values_too_wide(Ghost(!span_ok(vs)));
        }
        proof {
            // no two numbers are 2^63 apart: the type has none that
            // are, or the least and the greatest given are not
            assert forall|i: int, j: int| 0 <= i < vals.len() && 0 <= j < vals.len() implies delta_near(
                #[trigger] vals[i],
                #[trigger] vals[j],
            ) by {
                if vs[i] is Some && vs[j] is Some {
                    T::lemma_delta(vals[i], vs[i]);
                    T::lemma_delta(vals[j], vs[j]);
                    lemma_between(vs, i);
                    lemma_between(vs, j);
                }
            }
        }
        self.s.finish(buf);
        proof {
            let e = VerifiedColumn::<<T as DKind<C>>::I, C>::encoding(given);
            // the column underneath holds the differences given
            let mv = <DE<T, C> as Model<DX<T, C>>>::values(e);
            let f = |o: <DE<T, C> as Slabs>::V|
                <<T as DKind<C>>::B as DeltaWrite<'static, DE<T, C>, DX<T, C>>>::dvo(o);
            <<DE<T, C> as Slabs>::V as VGet>::lemma_svs(mv);
            assert(mv.map_values(f) =~= ds) by {
                assert forall|i: int| 0 <= i < mv.len() implies f(mv[i]) == ds[i] by {
                    <T as DKind<C>>::lemma_d_of(mv[i]);
                }
            }
            // and they are read as the values, each one of the type's
            assert(VerifiedDeltaColumn::<T, C>::saved_kept(e) == vs);
            lemma_kept(vals);
            VerifiedDeltaColumn::<T, C>::lemma_encoding(e);
        }
    }
}

/// A delta encoder: values go in one after another, and the bytes of a
/// delta column of them come out. It mirrors
/// [`DeltaEncoder`](crate::DeltaEncoder).
///
/// What an encoder has to be to be used is `wf`: every encoder made
/// here is, and stays so through every call that returns.
pub struct VerifiedDeltaEncoder<'a, T: VDeltaValue<C>, C: VCodec = Leb128> {
    data: Vec<u8>,
    state: VerifiedDeltaEncoderState<'a, T, C>,
}

impl<'a, T: VDeltaValue<C>, C: VCodec> View for VerifiedDeltaEncoder<'a, T, C> {
    type V = Seq<T>;

    /// The values given so far.
    closed spec fn view(&self) -> Seq<T> {
        self.state@
    }
}

impl<'a, T: VDeltaValue<C>, C: VCodec> Default for VerifiedDeltaEncoder<'a, T, C> {
    fn default() -> (r: Self)
        ensures
            r.wf(),
            r@ == Seq::<T>::empty(),
    {
        Self::new()
    }
}

impl<'a, T: VDeltaValue<C>, C: VCodec> VerifiedDeltaEncoder<'a, T, C> {
    /// The encoder's bytes are those its state has written, and nothing
    /// before them.
    pub closed spec fn wf(&self) -> bool {
        self.state.on(self.data@) && self.state.before(self.data@) == Seq::<u8>::empty()
    }

    /// An encoder that was given nothing.
    pub fn new() -> (r: Self)
        ensures
            r.wf(),
            r@ == Seq::<T>::empty(),
    {
        VerifiedDeltaEncoder { data: Vec::new(), state: VerifiedDeltaEncoderState::new() }
    }

    /// The number of values given.
    #[inline(always)]
    pub fn len(&self) -> (r: usize)
        requires
            self.wf(),
        ensures
            r == self@.len(),
    {
        self.state.len()
    }

    /// Whether no value was given.
    #[inline(always)]
    pub fn is_empty(&self) -> (r: bool)
        requires
            self.wf(),
        ensures
            r == (self@.len() == 0),
    {
        self.state.is_empty()
    }

    /// Every value given is one a delta column holds.
    pub proof fn lemma_fits(&self)
        requires
            self.wf(),
        ensures
            all_fit(self@),
    {
        self.state.lemma_fits(self.data@);
    }

    /// Give the encoder `value`, which must be one a delta column holds
    /// (`fits`).
    #[inline(always)]
    pub fn append(&mut self, value: T)
        requires
            old(self).wf(),
            value.fits(),
        ensures
            final(self).wf(),
            final(self)@ == old(self)@.push(value),
    {
        self.state.append(&mut self.data, value);
    }

    /// Give the encoder `value`, `n` times over.
    #[inline(always)]
    pub fn append_n(&mut self, value: T, n: usize)
        requires
            old(self).wf(),
            value.fits(),
        ensures
            final(self).wf(),
            final(self)@ == old(self)@ + Seq::new(n as nat, |_i: int| value),
    {
        self.state.append_n(&mut self.data, value, n);
    }

    /// [`append`](Self::append) by another name, as the old encoder has
    /// it.
    #[inline(always)]
    pub fn append_owned(&mut self, value: T)
        requires
            old(self).wf(),
            value.fits(),
        ensures
            final(self).wf(),
            final(self)@ == old(self)@.push(value),
    {
        self.append(value);
    }

    /// Give the encoder every value of `iter`, in order. The values must
    /// be known to fit (`delta_iter_fits`).
    pub fn extend<I: IntoIterator<Item = T>>(&mut self, iter: I)
        requires
            old(self).wf(),
            forall|it: I::IntoIter| #[trigger] (I::into_iter).ensures((iter,), it) ==> delta_iter_fits(it),
        ensures
            final(self).wf(),
            exists|it: I::IntoIter| #[trigger]
                (I::into_iter).ensures((iter,), it) && (it.obeys_prophetic_iter_laws() ==> final(self)@ == old(
                    self,
                )@ + it.remaining()),
    {
        self.state.extend(&mut self.data, iter);
    }

    /// The bytes of a delta column of the values given: its encoding,
    /// which a column of those values saves and which loads as them.
    ///
    /// Like every method that writes the values out, this panics if two
    /// of the numbers given are 2^63 or more apart, as the old encoder
    /// does: a delta column holds no such values.
    pub fn save(self) -> (r: Vec<u8>)
        requires
            self.wf(),
        ensures
            VerifiedDeltaColumn::<T, C>::valid(r@),
            VerifiedDeltaColumn::<T, C>::values(r@) == self@,
            r@ == VerifiedDeltaColumn::<T, C>::encoding(self@),
            delta_span_ok(self@),
    {
        let ghost vs = self@;
        let mut data = self.data;
        self.state.finish(&mut data);
        proof {
            assert(data@ =~= VerifiedDeltaColumn::<T, C>::encoding(vs));
        }
        data
    }

    /// Save the values given after what `out` holds: `out` is then what
    /// it was, and after that the encoding of the values. The range
    /// returned is where in `out` that is.
    pub fn save_to(self, out: &mut Vec<u8>) -> (r: Range<usize>)
        requires
            self.wf(),
        ensures
            final(out)@ == old(out)@ + VerifiedDeltaColumn::<T, C>::encoding(self@),
            VerifiedDeltaColumn::<T, C>::valid(VerifiedDeltaColumn::<T, C>::encoding(self@)),
            VerifiedDeltaColumn::<T, C>::values(VerifiedDeltaColumn::<T, C>::encoding(self@)) == self@,
            delta_span_ok(self@),
            r.start == old(out)@.len(),
            r.end == final(out)@.len(),
    {
        broadcast use vstd::slice::group_slice_axioms;

        let start = out.len();
        let data = self.save();
        out.extend_from_slice(data.as_slice());
        proof {
            assert(out@ =~= old(out)@ + data@);
        }
        start..out.len()
    }

    /// [`save_to`](Self::save_to), unless every value given is `value`:
    /// then nothing is written, and the range is empty. So is it for an
    /// encoder that was given nothing, whatever `value` is.
    pub fn save_to_unless(self, out: &mut Vec<u8>, value: T) -> (r: Range<usize>)
        requires
            self.wf(),
        ensures
            final(out)@ == if only(self@, value) {
                old(out)@
            } else {
                old(out)@ + VerifiedDeltaColumn::<T, C>::encoding(self@)
            },
            !only(self@, value) ==> {
                &&& VerifiedDeltaColumn::<T, C>::valid(VerifiedDeltaColumn::<T, C>::encoding(self@))
                &&& VerifiedDeltaColumn::<T, C>::values(VerifiedDeltaColumn::<T, C>::encoding(self@)) == self@
                &&& delta_span_ok(self@)
            },
            r.start == old(out)@.len(),
            r.end == final(out)@.len(),
    {
        if self.state.is_uniform(value) {
            let n = out.len();
            return n..n;
        }
        self.save_to(out)
    }

    /// The bytes of a delta column of the values of `iter`, which must
    /// be known to fit (`delta_iter_fits`).
    pub fn encode<I: IntoIterator<Item = T>>(iter: I) -> (r: Vec<u8>)
        requires
            forall|it: I::IntoIter| #[trigger] (I::into_iter).ensures((iter,), it) ==> delta_iter_fits(it),
        ensures
            VerifiedDeltaColumn::<T, C>::valid(r@),
            r@ == VerifiedDeltaColumn::<T, C>::encoding(VerifiedDeltaColumn::<T, C>::values(r@)),
            delta_span_ok(VerifiedDeltaColumn::<T, C>::values(r@)),
            exists|it: I::IntoIter| #[trigger]
                (I::into_iter).ensures((iter,), it) && (it.obeys_prophetic_iter_laws() ==> VerifiedDeltaColumn::<
                    T,
                    C,
                >::values(r@) == it.remaining()),
    {
        let mut enc = Self::new();
        enc.extend(iter);
        proof {
            assert(Seq::<T>::empty() + enc@ =~= enc@);
        }
        enc.save()
    }

    /// Write the bytes of a delta column of the values of `iter` after
    /// what `out` holds, with no buffer between. The range returned is
    /// where in `out` they are.
    pub fn encode_to<I: IntoIterator<Item = T>>(out: &mut Vec<u8>, iter: I) -> (r: Range<usize>)
        requires
            forall|it: I::IntoIter| #[trigger] (I::into_iter).ensures((iter,), it) ==> delta_iter_fits(it),
        ensures
            ({
                let d = final(out)@.skip(old(out)@.len() as int);
                let w = VerifiedDeltaColumn::<T, C>::values(d);
                &&& final(out)@ == old(out)@ + d
                &&& VerifiedDeltaColumn::<T, C>::valid(d)
                &&& d == VerifiedDeltaColumn::<T, C>::encoding(w)
                &&& delta_span_ok(w)
                &&& exists|it: I::IntoIter| #[trigger]
                    (I::into_iter).ensures((iter,), it) && (it.obeys_prophetic_iter_laws() ==> w == it.remaining())
            }),
            r.start == old(out)@.len(),
            r.end == final(out)@.len(),
    {
        let start = out.len();
        let mut state = VerifiedDeltaEncoderState::<T, C>::new();
        state.extend(out, iter);
        let ghost vs = state@;
        state.finish(out);
        proof {
            let d = out@.skip(start as int);
            assert(d =~= VerifiedDeltaColumn::<T, C>::encoding(vs));
            assert(out@ =~= old(out)@ + d);
            assert(Seq::<T>::empty() + vs =~= vs);
        }
        start..out.len()
    }

    /// [`encode_to`](Self::encode_to), unless every value of `iter` is
    /// `value`: then `out` is left as it was, and the range is empty. So
    /// is it for an `iter` with nothing to give.
    pub fn encode_to_unless<I: IntoIterator<Item = T>>(out: &mut Vec<u8>, iter: I, value: T) -> (r: Range<usize>)
        requires
            forall|it: I::IntoIter| #[trigger] (I::into_iter).ensures((iter,), it) ==> delta_iter_fits(it),
        ensures
            ({
                let d = final(out)@.skip(old(out)@.len() as int);
                let w = VerifiedDeltaColumn::<T, C>::values(d);
                &&& final(out)@ == old(out)@ + d
                &&& VerifiedDeltaColumn::<T, C>::valid(d)
                &&& d == VerifiedDeltaColumn::<T, C>::encoding(w)
                &&& delta_span_ok(w)
                &&& exists|it: I::IntoIter| #[trigger]
                    (I::into_iter).ensures((iter,), it) && (it.obeys_prophetic_iter_laws() ==> {
                        &&& (final(out)@ == old(out)@) == only(it.remaining(), value)
                        &&& !only(it.remaining(), value) ==> w == it.remaining()
                    })
            }),
            r.start == old(out)@.len(),
            r.end == final(out)@.len(),
    {
        let start = out.len();
        let mut state = VerifiedDeltaEncoderState::<T, C>::new();
        state.extend(out, iter);
        let ghost vs = state@;
        proof {
            assert(Seq::<T>::empty() + vs =~= vs);
        }
        if state.is_uniform(value) {
            proof {
                state.lemma_before(out@);
            }
            out.truncate(start);
            proof {
                assert(out@ =~= old(out)@);
                let d = out@.skip(start as int);
                assert(d =~= Seq::<u8>::empty());
                VerifiedDeltaColumn::<T, C>::lemma_no_bytes();
                VerifiedDeltaColumn::<T, C>::lemma_encoding(d);
                assert(out@ =~= old(out)@ + d);
            }
            return start..start;
        }
        state.finish(out);
        proof {
            let d = out@.skip(start as int);
            assert(d =~= VerifiedDeltaColumn::<T, C>::encoding(vs));
            assert(out@ =~= old(out)@ + d);
            if out@ == old(out)@ {
                assert(d =~= Seq::<u8>::empty());
                VerifiedDeltaColumn::<T, C>::lemma_no_bytes();
            }
        }
        start..out.len()
    }
}

} // verus!
impl<'a, T: VDeltaValue<C>, C: VCodec> std::fmt::Debug for VerifiedDeltaEncoder<'a, T, C> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("VerifiedDeltaEncoder")
            .field("len", &self.state.len())
            .field("abs", &self.state.abs)
            .finish()
    }
}
