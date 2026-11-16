//! The column iterator's core ([`crate::Iter`]), verified with Verus.
//!
//! An iterator reads a window of a column: it stands at `pos`, with
//! `items_left` more to give.  The column stands for its slabs' values
//! one after another (`vals_of`).  The iterator's invariant (`wf`) ties
//! its slab, the decoder it holds on that slab and its counters to `pos`,
//! and each operation is proved to give exactly the values left in the
//! window (`out`), in order — past the end of the column there are none.
//!
//! The value search (`seek_value`) is proved against the same model: if
//! the window is sorted, as a comparison with the target sees it
//! (`Probe`, `monotone`), the range it returns is exactly the values at
//! the target (`found`).  The comparison is the value type's `Ord`
//! (`Cmp`), trusted to agree with `==`.
//!
//! The slab decoders are the encodings' own, through each encoding's
//! decoder model (`Decode`, `Decoders`, named by
//! `ColumnEncoding::ReadModel`), which is verified against its format:
//! RLE columns' (`rle::dec::RleReads`) and bool columns'
//! (`bool_wire::BoolReads`).  The index lookup a long seek uses is given
//! as a closure whose answers are assumed right (`find_ok`; the index
//! itself is verified, see [`crate::index::IndexOps`]).
//!
//! Suspending keeps a position that always resumes (`suspended`,
//! `resumes`); resuming takes the index itself and succeeds exactly when
//! the position kept is one of the column's (`resume`), and then the
//! iterator has the same values left as the one suspended, if the slabs
//! are the same (`lemma_resumed`).
//!
//! The constructors also take it as given that there are fewer than
//! `usize::MAX` slabs. A slab is not zero-sized, so no slice of them is
//! that long, but Verus does not model that; it lets the step to the next
//! slab test `slab_idx + 1`, which compiles to a tighter loop than the
//! equivalent subtraction.

use crate::column::Slab;
#[cfg(verus_keep_ghost)]
use crate::cursor::{lemma_concat_append, slab_lens};
#[cfg(verus_keep_ghost)]
use crate::fenwick::{lemma_sum_le, lemma_sum_one, lemma_sum_split, sum, total};
#[cfg(verus_keep_ghost)]
use crate::index::wlens;
use crate::index::IndexOps;
#[cfg(verus_keep_ghost)]
use crate::rle::writer::concat;
use crate::Run;
#[allow(unused_imports)]
use vstd::prelude::*;

verus! {

// ── The decoders ───────────────────────────────────────────────────────────

/// A slab decoder, specified against the values it has left.
pub(crate) trait Decode: Sized {
    type Item: Copy;

    /// What the decoder keeps while reading.
    spec fn inv(&self) -> bool;

    /// The values still to come.
    spec fn rest(&self) -> Seq<Self::Item>;

    fn d_next(&mut self) -> (r: Option<Self::Item>)
        requires
            old(self).inv(),
        ensures
            final(self).inv(),
            match r {
                Some(v) => old(self).rest().len() > 0 && v == old(self).rest()[0] && final(self).rest()
                    == old(self).rest().skip(1),
                None => old(self).rest().len() == 0 && final(self).rest() == old(self).rest(),
            },
        no_unwind
    ;

    fn d_nth(&mut self, n: usize) -> (r: Option<Self::Item>)
        requires
            old(self).inv(),
        ensures
            final(self).inv(),
            n < old(self).rest().len() ==> r == Some(old(self).rest()[n as int]) && final(self).rest()
                == old(self).rest().skip(n + 1),
            n >= old(self).rest().len() ==> r is None && final(self).rest().len() == 0,
        no_unwind
    ;

    /// The next run of equal values, at most `max` of them.
    fn d_run_max(&mut self, max: usize) -> (r: Option<Run<Self::Item>>)
        requires
            old(self).inv(),
            max >= 1,
        ensures
            final(self).inv(),
            match r {
                Some(run) => {
                    &&& 1 <= run.count <= max
                    &&& run.count <= old(self).rest().len()
                    &&& old(self).rest().take(run.count as int) == Seq::new(
                        run.count as nat,
                        |_i: int| run.value,
                    )
                    &&& final(self).rest() == old(self).rest().skip(run.count as int)
                    // maximal, short of `max`: the next value differs
                    &&& run.count < max && run.count < old(self).rest().len()
                        ==> old(self).rest()[run.count as int] != run.value
                },
                None => old(self).rest().len() == 0 && final(self).rest() == old(self).rest(),
            },
        no_unwind
    ;

    fn d_eq(a: &Self::Item, b: &Self::Item) -> (r: bool)
        ensures
            r == (*a == *b),
    ;

    /// A copy, to read ahead with.
    fn dup(&self) -> (r: Self)
        ensures
            r.inv() == self.inv(),
            r.rest() == self.rest(),
    ;

    /// Append all the values still to come to `out`.
    fn d_drain(&mut self, out: &mut Vec<Self::Item>)
        requires
            old(self).inv(),
        ensures
            final(self).inv(),
            final(self).rest().len() == 0,
            final(out)@ == old(out)@ + old(self).rest(),
    ;
}

/// Append `n` copies of `v` to `out`.
#[inline(always)]
pub(crate) fn push_n<T: Copy>(out: &mut Vec<T>, v: T, n: usize)
    ensures
        final(out)@ == old(out)@ + Seq::new(n as nat, |_i: int| v),
{
    let ghost o0 = out@;
    let mut i: usize = 0;
    proof {
        assert(out@ =~= o0 + Seq::new(0, |_i: int| v));
    }
    while i < n
        invariant
            i <= n,
            out@ == o0 + Seq::new(i as nat, |_i: int| v),
        decreases n - i,
    {
        out.push(v);
        i += 1;
        proof {
            assert(out@ =~= o0 + Seq::new(i as nat, |_i: int| v));
        }
    }
}

/// `&s[at..]`, which Verus is shown not to unwind.
///
/// Verus knows of `split_at_checked` that it does not unwind, and not of
/// a range index. The first arm and the `None` are proved not to be
/// taken, so what the first calls need not be known. They are there for
/// the compiler, which makes of the whole what it makes of `&s[at..]`
/// alone: the test, the panic it guards, and the slice.
#[inline(always)]
#[track_caller]
pub(crate) fn tail<T>(s: &[T], at: usize) -> (r: &[T])
    requires
        at <= s@.len(),
    ensures
        r@ == s@.skip(at as int),
    no_unwind
{
    if at > s.len() {
        &s[at..]
    } else {
        match s.split_at_checked(at) {
            Some(p) => p.1,
            None => s,
        }
    }
}

/// `&s[..to]`, which Verus is shown not to unwind: as `tail`.
#[inline(always)]
#[track_caller]
pub(crate) fn head<T>(s: &[T], to: usize) -> (r: &[T])
    requires
        to <= s@.len(),
    ensures
        r@ == s@.take(to as int),
    no_unwind
{
    if to > s.len() {
        &s[..to]
    } else {
        match s.split_at_checked(to) {
            Some(p) => p.0,
            None => s,
        }
    }
}

/// What an option known to hold a value holds: `o.unwrap()`, which
/// Verus is shown not to unwind. The `None` arm is proved not to be
/// taken, and is there for the compiler, as in `tail`.
#[inline(always)]
#[track_caller]
#[allow(clippy::unnecessary_literal_unwrap)]
pub(crate) fn known<T>(o: Option<T>) -> (r: T)
    requires
        o is Some,
    ensures
        o == Some(r),
    no_unwind
{
    match o {
        Some(v) => v,
        None => None.unwrap(),
    }
}

/// No bytes: `&[]`, which Verus is shown not to unwind. Verus knows of
/// an array's `as_slice` that it does not unwind, and not of the
/// coercion that `&[]` as a slice asks for.
#[inline(always)]
pub(crate) fn no_bytes<'a>() -> (r: &'a [u8])
    ensures
        r@.len() == 0,
    no_unwind
{
    let none: &'static [u8; 0] = &[];
    none.as_slice()
}

/// Decoders on a column's slabs, which live for `'a`.
pub(crate) trait Decoders<'a> {
    type Tail: Copy + Clone + std::fmt::Debug + Default;

    type D: Decode;

    /// A slab is well-formed.
    spec fn ok(s: Slab<Self::Tail>) -> bool;

    /// A slab's values.
    spec fn vals(s: Slab<Self::Tail>) -> Seq<<Self::D as Decode>::Item>;

    fn open(s: &'a Slab<Self::Tail>) -> (d: Self::D)
        requires
            Self::ok(*s),
        ensures
            d.inv(),
            d.rest() == Self::vals(*s),
        no_unwind
    ;

    /// A decoder with nothing to give.
    fn none() -> (d: Self::D)
        ensures
            d.inv(),
            d.rest().len() == 0,
        no_unwind
    ;

    /// A test of a slab's metadata, its bytes unread, that fails only
    /// for a slab holding two values side by side that differ.
    fn maybe_uniform(s: &'a Slab<Self::Tail>) -> (r: bool)
        requires
            Self::ok(*s),
        ensures
            !r ==> exists|i: int|
                0 < i < Self::vals(*s).len() && Self::vals(*s)[i - 1] != #[trigger] Self::vals(*s)[i],
    ;
}

// ── Searching ──────────────────────────────────────────────────────────────

/// Where a value lies against a search target.
pub(crate) enum Side {
    Below,
    At,
    Above,
}

/// A search target, as values compare to it (trusted: the value type's
/// `Ord`, which agrees with `==`).
pub(crate) trait Probe<V> {
    /// Below the target (negative), at it (0), or above it (positive).
    spec fn side(&self, v: V) -> int;

    /// The values at the target are one value.
    proof fn axiom_at(&self, a: V, b: V)
        requires
            self.side(a) == 0,
            self.side(b) == 0,
        ensures
            a == b,
    ;

    fn side_of(&self, v: &V) -> (r: Side)
        ensures
            (r is Below) == (self.side(*v) < 0),
            (r is At) == (self.side(*v) == 0),
            (r is Above) == (self.side(*v) > 0),
    ;
}

/// Sorted, as the probe sees them: the values' sides never go down.
pub(crate) open spec fn monotone<V, P: Probe<V>>(p: P, w: Seq<V>) -> bool {
    forall|i: int, j: int| 0 <= i <= j < w.len() ==> #[trigger] p.side(w[i]) <= #[trigger] p.side(w[j])
}

/// The probe a search runs with: a comparison of each value against the
/// target (`v.cmp(&target)`).
pub(crate) struct Cmp<F>(pub(crate) F);

/// Where `c` puts `v` against its target: the comparison is pure, so this
/// is a function.
pub(crate) uninterp spec fn cmp_side<V, F>(c: Cmp<F>, v: V) -> int;

impl<V, F: Fn(&V) -> core::cmp::Ordering> Probe<V> for Cmp<F> {
    closed spec fn side(&self, v: V) -> int {
        cmp_side(*self, v)
    }

    // trusted: the value type's `Ord` agrees with `==`
    #[verifier::external_body]
    proof fn axiom_at(&self, a: V, b: V) {
    }

    // trusted: the comparison is pure
    #[verifier::external_body]
    #[inline(always)]
    fn side_of(&self, v: &V) -> Side {
        match (self.0)(v) {
            core::cmp::Ordering::Less => Side::Below,
            core::cmp::Ordering::Equal => Side::At,
            core::cmp::Ordering::Greater => Side::Above,
        }
    }
}

/// What a search finds: the values before `a` are below the target,
/// those from `a` to `b` at it, and the rest above it.
pub(crate) open spec fn found<V, P: Probe<V>>(p: P, w: Seq<V>, a: int, b: int) -> bool {
    forall|k: int| #![trigger w[k]] 0 <= k < w.len() ==> {
        &&& (k < a) == (p.side(w[k]) < 0)
        &&& (k < b) == (p.side(w[k]) <= 0)
    }
}

/// Below the target up to `a`, then a maximal run at it: found.
proof fn lemma_found_at<V, P: Probe<V>>(p: P, w: Seq<V>, a: int, c: int)
    requires
        monotone(p, w),
        0 <= a,
        1 <= c,
        a + c <= w.len(),
        forall|k: int| 0 <= k < a ==> p.side(#[trigger] w[k]) < 0,
        forall|k: int| a <= k < a + c ==> #[trigger] w[k] == w[a],
        p.side(w[a]) == 0,
        a + c < w.len() ==> w[a + c] != w[a],
    ensures
        found(p, w, a, a + c),
{
    if a + c < w.len() {
        assert(p.side(w[a]) <= p.side(w[a + c]));
        if p.side(w[a + c]) == 0 {
            p.axiom_at(w[a + c], w[a]);
        }
    }
    assert forall|k: int| #![trigger w[k]] 0 <= k < w.len() implies {
        &&& (k < a) == (p.side(w[k]) < 0)
        &&& (k < a + c) == (p.side(w[k]) <= 0)
    } by {
        if a + c <= k {
            assert(p.side(w[a + c]) <= p.side(w[k]));
        }
    }
}

/// Below the target up to `a`, then above it: found, empty, at `a`.
proof fn lemma_found_above<V, P: Probe<V>>(p: P, w: Seq<V>, a: int)
    requires
        monotone(p, w),
        0 <= a <= w.len(),
        forall|k: int| 0 <= k < a ==> p.side(#[trigger] w[k]) < 0,
        a < w.len() ==> p.side(w[a]) > 0,
    ensures
        found(p, w, a, a),
{
    assert forall|k: int| #![trigger w[k]] 0 <= k < w.len() implies {
        &&& (k < a) == (p.side(w[k]) < 0)
        &&& (k < a) == (p.side(w[k]) <= 0)
    } by {
        if a <= k {
            assert(p.side(w[a]) <= p.side(w[k]));
        }
    }
}

/// A slab's first value is the column's at the slab's start.
proof fn lemma_head<'a, X: Decoders<'a>>(s: Seq<Slab<X::Tail>>, k: int)
    requires
        lens_ok::<X>(s),
        0 <= k < s.len(),
        s[k].len > 0,
    ensures
        sum(slab_lens(s), 0, k) < total(slab_lens(s)),
        vals_of::<X>(s)[sum(slab_lens(s), 0, k) as int] == X::vals(s[k])[0],
{
    lemma_from::<X>(s, k, 0);
    let v = vals_of::<X>(s);
    let a = sum(slab_lens(s), 0, k) as int;
    assert(X::vals(s[k]).skip(0) =~= X::vals(s[k]));
    assert(v.skip(a)[0] == v[a]);
}

// ── The column model ───────────────────────────────────────────────────────

/// Each slab's values.
pub(crate) open spec fn each<'a, X: Decoders<'a>>(s: Seq<Slab<X::Tail>>) -> Seq<
    Seq<<X::D as Decode>::Item>,
> {
    s.map_values(|x: Slab<X::Tail>| X::vals(x))
}

/// The slabs' values, one after another.
pub(crate) open spec fn vals_of<'a, X: Decoders<'a>>(s: Seq<Slab<X::Tail>>) -> Seq<
    <X::D as Decode>::Item,
> {
    concat(each::<X>(s))
}

/// Each slab is well-formed and holds as many values as it says.
pub(crate) open spec fn lens_ok<'a, X: Decoders<'a>>(s: Seq<Slab<X::Tail>>) -> bool {
    forall|i: int|
        #![trigger X::vals(s[i])]
        #![trigger X::ok(s[i])]
        0 <= i < s.len() ==> X::ok(s[i]) && X::vals(s[i]).len() == s[i].len
}

/// The index's answer `r` for item `t`: the slab holding it and the
/// item's offset there, or past the end, a slab count of at least the
/// column's and a position at or past its end.
pub(crate) open spec fn find_ok<T: Copy + Clone + std::fmt::Debug + Default>(
    s: Seq<Slab<T>>,
    t: usize,
    r: (usize, usize),
) -> bool {
    &&& r.0 <= s.len()
    &&& r.0 < s.len() ==> sum(slab_lens(s), 0, r.0 as int) + r.1 == t && r.1 < s[r.0 as int].len
    &&& r.0 == s.len() ==> r.1 <= t && t - r.1 >= total(slab_lens(s))
}

/// The index's answers `far` for item `t`, as a move to it takes them:
/// the slab `far.0` holding the item and the item's offset there `far.1`
/// (as `find_ok` has them), and `far.2` the item the iterator stands at
/// when it has gone to that slab: the slab's first, or past the last
/// slab the column's end.
pub(crate) open spec fn far_ok<T: Copy + Clone + std::fmt::Debug + Default>(
    s: Seq<Slab<T>>,
    t: usize,
    far: (usize, usize, usize),
) -> bool {
    &&& find_ok(s, t, (far.0, far.1))
    &&& far.0 < s.len() ==> far.2 == t - far.1
    &&& far.0 >= s.len() ==> far.2 == sum(slab_lens(s), 0, s.len() as int)
}

/// The values of `v` in the window `a..b`: none if it starts at or past
/// the end of `v` or is empty, and up to the end of `v` if it runs past.
pub open spec fn window<V>(v: Seq<V>, a: int, b: int) -> Seq<V> {
    if a >= v.len() || b <= a {
        Seq::empty()
    } else if b <= v.len() {
        v.subrange(a, b)
    } else {
        v.subrange(a, v.len() as int)
    }
}

/// What is left of `o` after moving `k` items on: all of it if `k` is
/// not positive, none if `k` is past its end.
pub open spec fn past<V>(o: Seq<V>, k: int) -> Seq<V> {
    if k <= 0 {
        o
    } else if k <= o.len() {
        o.skip(k)
    } else {
        Seq::empty()
    }
}

/// Moving `k` on in a window is the window from `k` later.
pub(crate) proof fn lemma_window_past<V>(v: Seq<V>, a: int, b: int, k: int)
    requires
        0 <= a,
        0 <= k,
    ensures
        past(window(v, a, b), k) == window(v, a + k, b),
{
    let w = window(v, a, b);
    if k > 0 && k <= w.len() {
        assert(w.skip(k) =~= window(v, a + k, b));
    } else if k > 0 {
        assert(window(v, a + k, b) =~= Seq::<V>::empty());
    }
}

/// A window that lies in `v` is that range of it.
pub(crate) proof fn lemma_window_in<V>(v: Seq<V>, a: int, b: int)
    requires
        0 <= a <= b <= v.len(),
    ensures
        window(v, a, b) == v.subrange(a, b),
{
    assert(window(v, a, b) =~= v.subrange(a, b));
}

/// A range of `v` from its `n`th on is the range that starts `n` later.
pub(crate) proof fn lemma_sub_skip<V>(v: Seq<V>, a: int, b: int, n: int)
    requires
        0 <= a,
        0 <= n,
        a + n <= b <= v.len(),
    ensures
        v.subrange(a, b).skip(n) == v.subrange(a + n, b),
{
    assert(v.subrange(a, b).skip(n) =~= v.subrange(a + n, b));
}

/// Moving `a` on and then `b` on is moving `a + b` on.
pub(crate) proof fn lemma_skip_add<V>(o: Seq<V>, a: int, b: int)
    requires
        0 <= a,
        0 <= b,
        a + b <= o.len(),
    ensures
        o.skip(a).skip(b) == o.skip(a + b),
{
    assert(o.skip(a).skip(b) =~= o.skip(a + b));
}

/// Moving no items on leaves all of them.
pub(crate) proof fn lemma_skip_none<V>(o: Seq<V>)
    ensures
        o.skip(0) == o,
{
    assert(o.skip(0) =~= o);
}

/// A run of `v`, `a` long, with a run of `v`, `b` long, after it is one
/// run of `v`, `a + b` long.
pub(crate) proof fn lemma_run_join<V>(o: Seq<V>, v: V, a: int, b: int)
    requires
        0 <= a,
        0 <= b,
        a + b <= o.len(),
        o.take(a) == Seq::new(a as nat, |_i: int| v),
        o.skip(a).take(b) == Seq::new(b as nat, |_i: int| v),
    ensures
        o.take(a + b) == Seq::new((a + b) as nat, |_i: int| v),
{
    let t = o.take(a + b);
    assert forall|i: int| 0 <= i < t.len() implies t[i] == v by {
        if i < a {
            assert(o.take(a)[i] == v);
        } else {
            assert(o.skip(a).take(b)[i - a] == v);
        }
    }
    assert(t =~= Seq::new((a + b) as nat, |_i: int| v));
}

proof fn lemma_concat_at<V>(cs: Seq<Seq<V>>, k: int)
    requires
        0 <= k < cs.len(),
    ensures
        concat(cs) == concat(cs.take(k)) + cs[k] + concat(cs.skip(k + 1)),
{
    assert(cs =~= cs.take(k) + (seq![cs[k]] + cs.skip(k + 1)));
    lemma_concat_append(cs.take(k), seq![cs[k]] + cs.skip(k + 1));
    lemma_concat_append(seq![cs[k]], cs.skip(k + 1));
    let e = Seq::<Seq<V>>::empty();
    assert(seq![cs[k]].drop_last() =~= e);
    assert(concat(e) == Seq::<V>::empty());
    assert(concat(seq![cs[k]]) =~= cs[k]);
    assert(concat(cs) =~= concat(cs.take(k)) + cs[k] + concat(cs.skip(k + 1)));
}

proof fn lemma_concat_len<V>(cs: Seq<Seq<V>>, k: int)
    requires
        0 <= k <= cs.len(),
    ensures
        concat(cs.take(k)).len() == sum(cs.map_values(|c: Seq<V>| c.len()), 0, k),
    decreases k,
{
    if k > 0 {
        lemma_concat_len(cs, k - 1);
        assert(cs.take(k).drop_last() =~= cs.take(k - 1));
    } else {
        assert(cs.take(0) =~= Seq::<Seq<V>>::empty());
    }
}

/// The column's values before slab `k` are the slabs' before it.
pub(crate) proof fn lemma_upto<'a, X: Decoders<'a>>(s: Seq<Slab<X::Tail>>, k: int)
    requires
        lens_ok::<X>(s),
        0 <= k <= s.len(),
    ensures
        vals_of::<X>(s).take(sum(slab_lens(s), 0, k) as int) == vals_of::<X>(s.take(k)),
{
    let cs = each::<X>(s);
    assert(cs.map_values(|c: Seq<<X::D as Decode>::Item>| c.len()) =~= slab_lens(s));
    lemma_concat_len(cs, k);
    assert(cs.take(k) =~= each::<X>(s.take(k)));
    if k < s.len() {
        lemma_concat_at(cs, k);
        assert(vals_of::<X>(s).take(sum(slab_lens(s), 0, k) as int) =~= concat(cs.take(k)));
    } else {
        assert(cs.take(k) =~= cs);
        assert(vals_of::<X>(s).take(sum(slab_lens(s), 0, k) as int) =~= vals_of::<X>(s));
    }
}

/// From slab `k` at offset `j` on, the column holds the rest of slab `k`
/// and the slabs after it, and the values before number the items before.
pub(crate) proof fn lemma_from<'a, X: Decoders<'a>>(s: Seq<Slab<X::Tail>>, k: int, j: int)
    requires
        lens_ok::<X>(s),
        0 <= k < s.len(),
        0 <= j <= s[k].len,
    ensures
        vals_of::<X>(s).len() == total(slab_lens(s)),
        vals_of::<X>(s).skip(sum(slab_lens(s), 0, k) + j) == X::vals(s[k]).skip(j) + vals_of::<X>(
            s.skip(k + 1),
        ),
        sum(slab_lens(s), 0, k) + s[k].len <= total(slab_lens(s)),
{
    let cs = each::<X>(s);
    assert(cs.map_values(|c: Seq<<X::D as Decode>::Item>| c.len()) =~= slab_lens(s));
    lemma_concat_at(cs, k);
    lemma_concat_len(cs, k);
    lemma_concat_len(cs, cs.len() as int);
    assert(cs.take(cs.len() as int) =~= cs);
    assert(cs.skip(k + 1) =~= each::<X>(s.skip(k + 1)));
    let v = X::vals(s[k]);
    let pre = concat(cs.take(k));
    let post = vals_of::<X>(s.skip(k + 1));
    assert((pre + v + post).skip(pre.len() + j) =~= v.skip(j) + post);
    lemma_sum_split(slab_lens(s), 0, k, k + 1);
    lemma_sum_one(slab_lens(s), k);
    lemma_sum_le(slab_lens(s), 0, k + 1, 0, s.len() as int);
}

// ── The iterator ───────────────────────────────────────────────────────────

/// A suspended position — slab `si`, `consumed` items into it, item
/// `pos` of the column, `n` left in the window — that is a position of a
/// column whose slabs hold `lens` items: inside a slab, or past the last
/// at or past the column's end.
pub(crate) open spec fn resumes(lens: Seq<nat>, si: usize, consumed: usize, pos: usize, n: usize) -> bool {
    if si < lens.len() {
        &&& consumed <= lens[si as int]
        &&& pos == sum(lens, 0, si as int) + consumed
        &&& pos + n <= usize::MAX
    } else {
        pos >= total(lens)
    }
}

/// The iterator's state; [`crate::Iter`] pairs it with the slabs.
///
/// The field order is fixed because it changes the code LLVM makes for
/// the inlined loops: with the two counters a run takes from side by side
/// they update as a pair, and `pos` apart keeps `next` from pairing it
/// with `slab_remaining` (a fifth slower on bools).
#[repr(C)]
pub(crate) struct ICore<D> {
    pub(crate) items_left: usize,
    pub(crate) slab_remaining: usize,
    pub(crate) decoder: D,
    pub(crate) slab_idx: usize,
    pub(crate) pos: usize,
}

impl<D: Decode> ICore<D> {
    /// The iterator stands at `pos` of the slabs `s`: in slab `slab_idx`,
    /// `slab_remaining` from its end, with a decoder there — or past the
    /// last slab, at or past the column's end.  (Fewer than `usize::MAX`
    /// slabs, so stepping past one cannot overflow; see the module docs.)
    pub(crate) open spec fn wf<'a, X: Decoders<'a, D = D>>(&self, s: Seq<Slab<X::Tail>>) -> bool {
        let si = self.slab_idx as int;
        &&& lens_ok::<X>(s)
        &&& total(slab_lens(s)) <= usize::MAX
        &&& s.len() < usize::MAX
        &&& self.pos + self.items_left <= usize::MAX
        &&& self.decoder.inv()
        &&& si <= s.len()
        &&& si < s.len() ==> {
            &&& self.slab_remaining <= s[si].len
            &&& self.pos == sum(slab_lens(s), 0, si) + (s[si].len - self.slab_remaining)
            &&& self.decoder.rest() == X::vals(s[si]).skip(s[si].len - self.slab_remaining)
        }
        &&& si == s.len() ==> {
            &&& self.slab_remaining == 0
            &&& self.decoder.rest().len() == 0
            &&& self.pos >= total(slab_lens(s))
        }
    }

    /// A copy of the iterator, to read ahead with: the same in every
    /// way the specifications see.
    #[inline(always)]
    pub(crate) fn dup(&self) -> (r: Self)
        ensures
            r.items_left == self.items_left,
            r.slab_remaining == self.slab_remaining,
            r.slab_idx == self.slab_idx,
            r.pos == self.pos,
            r.decoder.inv() == self.decoder.inv(),
            r.decoder.rest() == self.decoder.rest(),
    {
        ICore {
            items_left: self.items_left,
            slab_remaining: self.slab_remaining,
            decoder: self.decoder.dup(),
            slab_idx: self.slab_idx,
            pos: self.pos,
        }
    }

    /// The values the iterator has left to give.
    pub(crate) open spec fn out<'a, X: Decoders<'a, D = D>>(&self, s: Seq<Slab<X::Tail>>) -> Seq<
        D::Item,
    > {
        let v = vals_of::<X>(s);
        if self.pos >= v.len() {
            Seq::empty()
        } else if self.pos + self.items_left <= v.len() {
            v.subrange(self.pos as int, self.pos + self.items_left)
        } else {
            v.subrange(self.pos as int, v.len() as int)
        }
    }

    /// Standing in a slab, the column from `pos` on is the decoder's
    /// values, then the slabs after.
    pub(crate) proof fn lemma_here<'a, X: Decoders<'a, D = D>>(&self, s: Seq<Slab<X::Tail>>)
        requires
            self.wf::<X>(s),
        ensures
            vals_of::<X>(s).len() == total(slab_lens(s)),
            self.slab_idx < s.len() ==> {
                &&& self.decoder.rest().len() == self.slab_remaining
                &&& vals_of::<X>(s).skip(self.pos as int) == self.decoder.rest() + vals_of::<X>(
                    s.skip(self.slab_idx + 1),
                )
                &&& self.pos + self.slab_remaining <= total(slab_lens(s))
            },
    {
        if self.slab_idx < s.len() {
            let si = self.slab_idx as int;
            lemma_from::<X>(s, si, s[si].len - self.slab_remaining);
        } else {
            if s.len() > 0 {
                lemma_from::<X>(s, 0, 0);
            } else {
                assert(each::<X>(s) =~= Seq::<Seq<D::Item>>::empty());
                assert(slab_lens(s) =~= Seq::<nat>::empty());
            }
        }
    }

    /// What is left is the column's values in the window.
    pub(crate) proof fn lemma_out_window<'a, X: Decoders<'a, D = D>>(&self, s: Seq<Slab<X::Tail>>)
        ensures
            self.out::<X>(s) == window(vals_of::<X>(s), self.pos as int, self.pos + self.items_left),
    {
        assert(self.out::<X>(s) =~= window(vals_of::<X>(s), self.pos as int, self.pos + self.items_left));
    }

    /// How many values are left, for an iterator that stands in the
    /// column or at its end: as many as the window has, or the column
    /// from where it stands.
    pub(crate) proof fn lemma_out_len<'a, X: Decoders<'a, D = D>>(&self, s: Seq<Slab<X::Tail>>)
        requires
            self.wf::<X>(s),
        ensures
            vals_of::<X>(s).len() == total(slab_lens(s)),
            self.pos <= total(slab_lens(s)) ==> self.out::<X>(s).len() == if self.pos + self.items_left <= total(
                slab_lens(s),
            ) {
                self.items_left as int
            } else {
                total(slab_lens(s)) - self.pos
            },
    {
        self.lemma_here::<X>(s);
    }

    /// With no items left in the window, nothing is left.
    proof fn lemma_none_left<'a, X: Decoders<'a, D = D>>(&self, s: Seq<Slab<X::Tail>>)
        requires
            self.items_left == 0,
        ensures
            self.out::<X>(s).len() == 0,
    {
    }

    /// At the end of the last slab: nothing is left.
    proof fn lemma_end_out<'a, X: Decoders<'a, D = D>>(&self, s: Seq<Slab<X::Tail>>)
        requires
            self.wf::<X>(s),
            self.slab_remaining == 0,
            self.slab_idx + 1 >= s.len(),
        ensures
            self.out::<X>(s).len() == 0,
            self.pos >= total(slab_lens(s)),
    {
        self.lemma_here::<X>(s);
        if self.slab_idx < s.len() {
            assert(s.skip(self.slab_idx + 1) =~= Seq::<Slab<X::Tail>>::empty());
            assert(each::<X>(s.skip(self.slab_idx + 1)) =~= Seq::<Seq<D::Item>>::empty());
        }
    }

    /// An iterator `k` further on in the same window has what is left
    /// after `k` items.
    pub(crate) proof fn lemma_moved<'a, X: Decoders<'a, D = D>>(&self, after: Self, s: Seq<Slab<X::Tail>>, k: int)
        requires
            0 <= k <= self.items_left,
            after.pos == self.pos + k,
            after.items_left == self.items_left - k,
        ensures
            k <= self.out::<X>(s).len() ==> after.out::<X>(s) == self.out::<X>(s).skip(k),
            k >= self.out::<X>(s).len() ==> after.out::<X>(s).len() == 0,
    {
        self.lemma_out_window::<X>(s);
        after.lemma_out_window::<X>(s);
        lemma_window_past(vals_of::<X>(s), self.pos as int, self.pos + self.items_left, k);
        lemma_skip_none(self.out::<X>(s));
    }

    /// After a jump ahead to a slab that holds the item `n` on from the
    /// iterator, `j` on from where it lands: the item is in both windows,
    /// with the same window after it.
    pub(crate) proof fn lemma_jumped<'a, X: Decoders<'a, D = D>>(
        &self,
        after: Self,
        s: Seq<Slab<X::Tail>>,
        n: int,
        j: int,
    )
        requires
            self.wf::<X>(s),
            after.wf::<X>(s),
            after.slab_idx < s.len(),
            self.pos <= after.pos,
            after.items_left == self.items_left - (after.pos - self.pos),
            after.pos + j == self.pos + n,
            0 <= j < after.slab_remaining,
            n < self.items_left,
        ensures
            j < after.out::<X>(s).len(),
            n < self.out::<X>(s).len(),
            after.out::<X>(s)[j] == self.out::<X>(s)[n],
            after.out::<X>(s).skip(j + 1) == self.out::<X>(s).skip(n + 1),
    {
        after.lemma_here::<X>(s);
        self.lemma_moved::<X>(after, s, after.pos - self.pos);
        lemma_skip_add(self.out::<X>(s), after.pos - self.pos, j + 1);
    }

    /// Standing in a slab, the window's values up to the slab's end are
    /// the decoder's.
    proof fn lemma_out_at<'a, X: Decoders<'a, D = D>>(&self, s: Seq<Slab<X::Tail>>, i: int)
        requires
            self.wf::<X>(s),
            self.slab_idx < s.len(),
            0 <= i < self.out::<X>(s).len(),
            i < self.slab_remaining,
        ensures
            self.decoder.rest().len() == self.slab_remaining,
            self.out::<X>(s)[i] == self.decoder.rest()[i],
    {
        self.lemma_here::<X>(s);
        let vv = vals_of::<X>(s);
        assert(self.out::<X>(s)[i] == vv[self.pos + i]);
        assert(vv[self.pos + i] == vv.skip(self.pos as int)[i]);
    }

    /// Taking the decoder's next `c` values, which are the window's next
    /// `c`, leaves the iterator `after`.
    proof fn lemma_taken<'a, X: Decoders<'a, D = D>>(&self, after: Self, s: Seq<Slab<X::Tail>>, c: int)
        requires
            self.wf::<X>(s),
            self.slab_idx < s.len(),
            0 <= c <= self.items_left,
            c <= self.slab_remaining,
            after.items_left == self.items_left - c,
            after.slab_remaining == self.slab_remaining - c,
            after.pos == self.pos + c,
            after.slab_idx == self.slab_idx,
            after.decoder.inv(),
            after.decoder.rest() == self.decoder.rest().skip(c),
        ensures
            after.wf::<X>(s),
            self.decoder.rest().len() == self.slab_remaining,
            c <= self.out::<X>(s).len() <= self.items_left,
            self.out::<X>(s).take(c) == self.decoder.rest().take(c),
            after.out::<X>(s) == self.out::<X>(s).skip(c),
    {
        self.lemma_here::<X>(s);
        let si = self.slab_idx as int;
        let o = self.out::<X>(s);
        let rest = self.decoder.rest();
        lemma_skip_add(X::vals(s[si]), s[si].len - self.slab_remaining, c);
        assert forall|i: int| 0 <= i < c implies o.take(c)[i] == rest.take(c)[i] by {
            self.lemma_out_at::<X>(s, i);
        }
        assert(o.take(c) =~= rest.take(c));
        assert(after.out::<X>(s) =~= o.skip(c));
    }

    /// Stepping from the end of a slab to the start of the next, with a
    /// decoder opened there, leaves the iterator `after`, with the same
    /// values left.
    proof fn lemma_stepped<'a, X: Decoders<'a, D = D>>(&self, after: Self, s: Seq<Slab<X::Tail>>)
        requires
            self.wf::<X>(s),
            self.slab_remaining == 0,
            self.slab_idx + 1 < s.len(),
            after.items_left == self.items_left,
            after.pos == self.pos,
            after.slab_idx == self.slab_idx + 1,
            after.slab_remaining == s[after.slab_idx as int].len,
            after.decoder.inv(),
            after.decoder.rest() == X::vals(s[after.slab_idx as int]),
        ensures
            after.wf::<X>(s),
            after.out::<X>(s) == self.out::<X>(s),
    {
        let si = self.slab_idx as int;
        lemma_sum_split(slab_lens(s), 0, si, si + 1);
        lemma_sum_one(slab_lens(s), si);
        lemma_skip_none(X::vals(s[si + 1]));
    }

    /// The position an iterator keeps when suspended: its slab, how far
    /// into it, its place in the column, and what is left of its window.
    pub(crate) fn suspended<'a, X: Decoders<'a, D = D>>(&self, slabs: &'a [Slab<X::Tail>]) -> (r: (usize, usize, usize, usize))
        requires
            self.wf::<X>(slabs@),
        ensures
            r == (self.slab_idx, if self.slab_idx < slabs@.len() { (slabs@[self.slab_idx as int].len - self.slab_remaining) as usize } else { 0 }, self.pos, self.items_left),
            resumes(slab_lens(slabs@), r.0, r.1, r.2, r.3),
    {
        let consumed = if self.slab_idx < slabs.len() {
            slabs[self.slab_idx].len - self.slab_remaining
        } else {
            0
        };
        (self.slab_idx, consumed, self.pos, self.items_left)
    }

    /// Resume at a suspended position — slab `si`, `consumed` items into
    /// it, item `pos` of the column, `n` left in the window — if it is a
    /// position of these slabs (`resumes`); `None` if not. In a slab, the
    /// iterator stands there with `n` to give; past the last slab, at or
    /// past the column's end, it is idle there. The slabs' item counts
    /// come from the index, which must count them (`wlens`).
    pub(crate) fn resume<'a, X: Decoders<'a, D = D>, W, I: IndexOps<W>>(
        slabs: &'a [Slab<X::Tail>],
        ix: &I,
        si: usize,
        consumed: usize,
        pos: usize,
        n: usize,
    ) -> (r: Option<Self>)
        requires
            lens_ok::<X>(slabs@),
            slabs@.len() < usize::MAX,
            total(slab_lens(slabs@)) <= usize::MAX,
            ix.ok(),
            wlens::<W, I>(ix.ws()) == slab_lens(slabs@),
        ensures
            r is Some <==> resumes(slab_lens(slabs@), si, consumed, pos, n),
            r matches Some(it) ==> {
                &&& it.wf::<X>(slabs@)
                &&& it.pos == pos
                &&& it.slab_idx == si || (si >= slabs@.len() && it.slab_idx == slabs@.len())
                &&& si < slabs@.len() ==> it.items_left == n
                &&& si >= slabs@.len() ==> it.out::<X>(slabs@).len() == 0
            },
    {
        let ghost lens = slab_lens(slabs@);
        if si >= slabs.len() {
            // past the last slab: at or past the column's end
            let end = ix.ix_before(ix.ix_len());
            proof {
                assert(lens.len() == slabs@.len());
            }
            if pos < end {
                return None;
            }
            return Some(Self::idle::<X>(slabs, pos));
        }
        let start = ix.ix_before(si);
        proof {
            lemma_sum_split(lens, 0, si as int, lens.len() as int);
            lemma_sum_split(lens, si as int, si + 1, lens.len() as int);
            lemma_sum_one(lens, si as int);
        }
        if consumed > slabs[si].len || start + consumed != pos || pos > usize::MAX - n {
            return None;
        }
        Some(Self::at::<X>(slabs, si, consumed, pos, n))
    }

    /// Suspend and resume round-trip: an iterator resumed, over the same
    /// slabs, from what [`Self::suspended`] kept (which always resumes) has
    /// the same values left to give.
    pub(crate) proof fn lemma_resumed<'a, X: Decoders<'a, D = D>>(&self, it: Self, s: Seq<Slab<X::Tail>>)
        requires
            self.wf::<X>(s),
            it.wf::<X>(s),
            it.pos == self.pos,
            self.slab_idx < s.len() ==> it.items_left == self.items_left,
            self.slab_idx >= s.len() ==> it.out::<X>(s).len() == 0,
        ensures
            it.out::<X>(s) == self.out::<X>(s),
    {
        self.lemma_here::<X>(s);
        if self.slab_idx >= s.len() {
            assert(self.out::<X>(s) =~= Seq::<D::Item>::empty());
            assert(it.out::<X>(s) =~= Seq::<D::Item>::empty());
        }
    }

    /// An iterator with nothing in hand, standing at `pos` at or past the
    /// end.
    pub(crate) fn idle<'a, X: Decoders<'a, D = D>>(slabs: &'a [Slab<X::Tail>], pos: usize) -> (it: Self)
        requires
            lens_ok::<X>(slabs@),
            slabs@.len() < usize::MAX,
            total(slab_lens(slabs@)) <= usize::MAX,
            pos >= total(slab_lens(slabs@)),
        ensures
            it.wf::<X>(slabs@),
            it.out::<X>(slabs@) == Seq::<D::Item>::empty(),
            it.pos == pos,
            it.slab_idx == slabs@.len(),
            it.items_left == 0,
    {
        let it = ICore { slab_idx: slabs.len(), decoder: X::none(), items_left: 0, slab_remaining: 0, pos };
        proof {
            it.lemma_here::<X>(slabs@);
        }
        it
    }

    /// An iterator over no slabs (`Iter::default`).
    #[inline(always)]
    pub(crate) fn empty<'a, X: Decoders<'a, D = D>>() -> (it: Self)
        ensures
            it.wf::<X>(Seq::empty()),
            it.out::<X>(Seq::empty()) == Seq::<D::Item>::empty(),
            it.pos == 0,
            it.items_left == 0,
    {
        let it = ICore { slab_idx: 0, decoder: X::none(), items_left: 0, slab_remaining: 0, pos: 0 };
        proof {
            let s = Seq::<Slab<X::Tail>>::empty();
            assert(slab_lens(s) =~= Seq::<nat>::empty());
            it.lemma_here::<X>(s);
        }
        it
    }

    /// An iterator at item `at` of slab `si`, with `n` to give.
    pub(crate) fn at<'a, X: Decoders<'a, D = D>>(
        slabs: &'a [Slab<X::Tail>],
        si: usize,
        at: usize,
        pos: usize,
        n: usize,
    ) -> (it: Self)
        requires
            lens_ok::<X>(slabs@),
            slabs@.len() < usize::MAX,
            total(slab_lens(slabs@)) <= usize::MAX,
            si < slabs@.len(),
            at <= slabs@[si as int].len,
            pos == sum(slab_lens(slabs@), 0, si as int) + at,
            pos + n <= usize::MAX,
        ensures
            it.wf::<X>(slabs@),
            it.pos == pos,
            it.items_left == n,
            it.slab_idx == si,
    {
        let slab = &slabs[si];
        let mut decoder = X::open(slab);
        if at > 0 {
            decoder.d_nth(at - 1);
        }
        proof {
            let v = X::vals(slabs@[si as int]);
            if at > 0 {
                assert(v.skip(at - 1 + 1) =~= v.skip(at as int));
            } else {
                assert(v.skip(0) =~= v);
            }
        }
        ICore { slab_idx: si, decoder, items_left: n, slab_remaining: slab.len - at, pos }
    }

    /// Yield the next value of the window.
    #[inline]
    pub(crate) fn next<'a, X: Decoders<'a, D = D>>(&mut self, slabs: &'a [Slab<X::Tail>]) -> (r: Option<D::Item>)
        requires
            old(self).wf::<X>(slabs@),
        ensures
            final(self).wf::<X>(slabs@),
            match r {
                Some(v) => {
                    &&& old(self).out::<X>(slabs@).len() > 0
                    &&& v == old(self).out::<X>(slabs@)[0]
                    &&& final(self).out::<X>(slabs@) == old(self).out::<X>(slabs@).skip(1)
                    &&& final(self).pos == old(self).pos + 1
                    &&& final(self).items_left == old(self).items_left - 1
                },
                None => old(self).out::<X>(slabs@).len() == 0 && final(self).out::<X>(slabs@).len() == 0
                    && final(self).pos == old(self).pos && final(self).items_left == 0,
            },
    {
        self.next_inline::<X>(slabs)
    }

    /// [`next`](Self::next), in line in its caller whatever the size: for
    /// a loop that does nothing but read.
    #[inline(always)]
    pub(crate) fn next_inline<'a, X: Decoders<'a, D = D>>(&mut self, slabs: &'a [Slab<X::Tail>]) -> (r: Option<D::Item>)
        requires
            old(self).wf::<X>(slabs@),
        ensures
            final(self).wf::<X>(slabs@),
            match r {
                Some(v) => {
                    &&& old(self).out::<X>(slabs@).len() > 0
                    &&& v == old(self).out::<X>(slabs@)[0]
                    &&& final(self).out::<X>(slabs@) == old(self).out::<X>(slabs@).skip(1)
                    &&& final(self).pos == old(self).pos + 1
                    &&& final(self).items_left == old(self).items_left - 1
                },
                None => old(self).out::<X>(slabs@).len() == 0 && final(self).out::<X>(slabs@).len() == 0
                    && final(self).pos == old(self).pos && final(self).items_left == 0,
            },
        no_unwind
    {
        let ghost c0 = *self;
        proof {
            self.lemma_here::<X>(slabs@);
        }
        if self.items_left == 0 {
            return None;
        }
        loop
            invariant
                c0 == *old(self),
                self.wf::<X>(slabs@),
                self.items_left == c0.items_left,
                self.items_left > 0,
                self.pos == c0.pos,
                self.out::<X>(slabs@) == c0.out::<X>(slabs@),
            decreases slabs@.len() - self.slab_idx,
        {
            proof {
                self.lemma_here::<X>(slabs@);
            }
            let ghost c1 = *self;
            if let Some(v) = self.decoder.d_next() {
                self.items_left -= 1;
                self.slab_remaining -= 1;
                self.pos += 1;
                proof {
                    c1.lemma_taken::<X>(*self, slabs@, 1);
                    c1.lemma_out_at::<X>(slabs@, 0);
                }
                return Some(v);
            }
            if self.slab_idx + 1 >= slabs.len() {
                proof {
                    c1.lemma_end_out::<X>(slabs@);
                }
                self.slab_idx = slabs.len();
                self.items_left = 0;
                return None;
            }
            self.slab_idx += 1;
            self.slab_remaining = slabs[self.slab_idx].len;
            self.decoder = X::open(&slabs[self.slab_idx]);
            proof {
                c1.lemma_stepped::<X>(*self, slabs@);
            }
        }
    }

    /// Jump to the start of slab `si`, item `start` of the column; past the
    /// last slab, stop at `start`.  Returns whether there is a slab.
    pub(crate) fn advance_to_slab<'a, X: Decoders<'a, D = D>>(&mut self, slabs: &'a [Slab<X::Tail>], si: usize, start: usize) -> (r: bool)
        requires
            old(self).wf::<X>(slabs@),
            si <= slabs@.len(),
            si < slabs@.len() ==> {
                &&& start == sum(slab_lens(slabs@), 0, si as int)
                &&& old(self).pos <= start
                &&& start - old(self).pos <= old(self).items_left
            },
            si == slabs@.len() ==> start >= total(slab_lens(slabs@)),
        ensures
            final(self).wf::<X>(slabs@),
            r == (si < slabs@.len()),
            final(self).pos == start,
            final(self).slab_idx == si,
            r ==> {
                &&& final(self).slab_remaining == slabs@[si as int].len
                &&& final(self).items_left == old(self).items_left - (start - old(self).pos)
            },
            !r ==> final(self).items_left == 0,
        no_unwind
    {
        if si >= slabs.len() {
            self.slab_idx = si;
            self.pos = start;
            self.items_left = 0;
            // else a later re-extend decodes items already passed
            self.slab_remaining = 0;
            self.decoder = X::none();
            return false;
        }
        let skipped = start - self.pos;
        self.pos = start;
        self.slab_idx = si;
        self.slab_remaining = slabs[si].len;
        self.decoder = X::open(&slabs[si]);
        self.items_left -= skipped;
        proof {
            lemma_skip_none(X::vals(slabs@[si as int]));
        }
        true
    }

    /// What a move in the window needs of an iterator that stands well.
    proof fn lemma_nth_pre<'a, X: Decoders<'a, D = D>>(&self, s: Seq<Slab<X::Tail>>)
        requires
            self.wf::<X>(s),
        ensures
            self.decoder.inv(),
            self.pos + self.items_left <= usize::MAX,
            self.slab_remaining > 0 ==> self.slab_idx < s.len(),
    {
    }

    /// The index's answer `r` for the item `n` on, which is past the
    /// iterator's slab: a slab that starts at or after the iterator, or
    /// the end, and then the window ends before the item.
    proof fn lemma_nth_found<'a, X: Decoders<'a, D = D>>(
        &self,
        s: Seq<Slab<X::Tail>>,
        n: int,
        target: usize,
        r: (usize, usize),
    )
        requires
            self.wf::<X>(s),
            self.slab_remaining <= n < self.items_left,
            target == self.pos + n,
            find_ok(s, target, r),
        ensures
            r.0 <= s.len(),
            sum(slab_lens(s), 0, s.len() as int) == total(slab_lens(s)),
            r.0 < s.len() ==> {
                &&& r.1 <= target
                &&& target - r.1 == sum(slab_lens(s), 0, r.0 as int)
                &&& self.pos <= target - r.1
                &&& r.1 < s[r.0 as int].len
            },
            r.0 == s.len() ==> self.out::<X>(s).len() <= n,
    {
        let lens = slab_lens(s);
        let si = r.0;
        self.lemma_here::<X>(s);
        if si < s.len() {
            // the slab holding `target` starts at or after the cursor
            if self.slab_idx < s.len() {
                let cur = self.slab_idx as int;
                lemma_sum_split(lens, 0, cur, cur + 1);
                lemma_sum_one(lens, cur);
                if (si as int) <= cur {
                    lemma_sum_le(lens, 0, si + 1, 0, cur + 1);
                    lemma_sum_split(lens, 0, si as int, si + 1);
                    lemma_sum_one(lens, si as int);
                } else {
                    lemma_sum_le(lens, 0, cur + 1, 0, si as int);
                }
            } else {
                lemma_from::<X>(s, si as int, 0);
            }
        }
    }

    /// The item `n` on, taken as the `j`th of the slab the iterator `mid`
    /// stands in, at or after this one: it is the window's `n`th, and
    /// `after` has the window past it.
    proof fn lemma_nth_at<'a, X: Decoders<'a, D = D>>(
        &self,
        mid: Self,
        after: Self,
        s: Seq<Slab<X::Tail>>,
        n: int,
        j: int,
        r: Option<D::Item>,
    )
        requires
            self.wf::<X>(s),
            mid.wf::<X>(s),
            mid.slab_idx < s.len(),
            self.pos <= mid.pos,
            mid.items_left == self.items_left - (mid.pos - self.pos),
            mid.pos + j == self.pos + n,
            0 <= j < mid.slab_remaining,
            n < self.items_left,
            after.items_left == mid.items_left - (j + 1),
            after.slab_remaining == mid.slab_remaining - (j + 1),
            after.pos == mid.pos + j + 1,
            after.slab_idx == mid.slab_idx,
            after.decoder.inv(),
            j < mid.decoder.rest().len() ==> r == Some(mid.decoder.rest()[j]) && after.decoder.rest()
                == mid.decoder.rest().skip(j + 1),
        ensures
            after.wf::<X>(s),
            n < self.out::<X>(s).len(),
            r == Some(self.out::<X>(s)[n]),
            after.out::<X>(s) == self.out::<X>(s).skip(n + 1),
            after.pos <= total(slab_lens(s)),
    {
        hide(ICore::wf);
        hide(ICore::out);
        self.lemma_jumped::<X>(mid, s, n, j);
        mid.lemma_here::<X>(s);
        mid.lemma_taken::<X>(after, s, j + 1);
        mid.lemma_out_at::<X>(s, j);
    }

    /// The item the index is to be asked for, to move `n` items on in the
    /// window (`nth(n)`): none when the move ends in the slab the
    /// iterator stands in, or the window is empty.
    pub(crate) open spec fn far_spec(&self, n: usize) -> Option<usize> {
        if self.items_left == 0 {
            None
        } else {
            let k = if n >= self.items_left {
                (self.items_left - 1) as usize
            } else {
                n
            };
            if k < self.slab_remaining {
                None
            } else {
                Some((self.pos + k) as usize)
            }
        }
    }

    /// `far_spec`, computed: what `nth_at` is to be given the index's
    /// answers for.
    #[inline(always)]
    pub(crate) fn far_target(&self, n: usize) -> (r: Option<usize>)
        requires
            self.pos + self.items_left <= usize::MAX,
        ensures
            r == self.far_spec(n),
        no_unwind
    {
        if self.items_left == 0 {
            return None;
        }
        let k = if n >= self.items_left {
            self.items_left - 1
        } else {
            n
        };
        if k < self.slab_remaining {
            None
        } else {
            Some(self.pos + k)
        }
    }

    /// `nth_at`, for `n` inside the window.
    #[verifier::spinoff_prover]
    #[inline(always)]
    fn nth_in_at<'a, X: Decoders<'a, D = D>>(
        &mut self,
        slabs: &'a [Slab<X::Tail>],
        n: usize,
        far: (usize, usize, usize),
    ) -> (r: Option<D::Item>)
        requires
            old(self).wf::<X>(slabs@),
            n < old(self).items_left,
            n >= old(self).slab_remaining ==> far_ok(slabs@, (old(self).pos + n) as usize, far),
        ensures
            final(self).wf::<X>(slabs@),
            n < old(self).out::<X>(slabs@).len() ==> {
                &&& r == Some(old(self).out::<X>(slabs@)[n as int])
                &&& final(self).out::<X>(slabs@) == old(self).out::<X>(slabs@).skip(n + 1)
                &&& final(self).pos == old(self).pos + n + 1
                &&& final(self).pos + final(self).items_left == old(self).pos + old(self).items_left
            },
            n >= old(self).out::<X>(slabs@).len() ==> {
                &&& r is None
                &&& final(self).out::<X>(slabs@).len() == 0
                &&& final(self).items_left == 0
                &&& old(self).pos <= total(slab_lens(slabs@)) ==> final(self).pos == old(self).pos + old(self).out::<X>(
                    slabs@,
                ).len()
            },
            // past the data, it stops at the end
            old(self).pos <= total(slab_lens(slabs@)) ==> final(self).pos <= total(slab_lens(slabs@)),
        no_unwind
    {
        hide(ICore::wf);
        hide(ICore::out);
        hide(find_ok);
        hide(sum);
        let ghost c0 = *self;
        let ghost s = slabs@;
        proof {
            self.lemma_nth_pre::<X>(s);
        }
        // Fast path: target is within the current slab.
        if n < self.slab_remaining {
            self.items_left -= n + 1;
            self.slab_remaining -= n + 1;
            self.pos += n + 1;
            let r = self.decoder.d_nth(n);
            proof {
                c0.lemma_nth_at::<X>(c0, *self, s, n as int, n as int, r);
            }
            return r;
        }
        // The column's index gave the slab, in O(log S); past the data
        // (the window can run past it), stop at its end.
        let (si, off, start) = far;
        proof {
            c0.lemma_nth_found::<X>(s, n as int, (c0.pos + n) as usize, (si, off));
        }
        if !self.advance_to_slab::<X>(slabs, si, start) {
            proof {
                self.lemma_none_left::<X>(s);
                // the window ran past the data, and this is the data's
                // end
                c0.lemma_out_len::<X>(s);
            }
            return None;
        }
        let ghost c1 = *self;
        proof {
            c1.lemma_nth_pre::<X>(s);
        }
        self.slab_remaining -= off + 1;
        self.items_left -= off + 1;
        self.pos += off + 1;
        let r = self.decoder.d_nth(off);
        proof {
            c0.lemma_nth_at::<X>(c1, *self, s, n as int, off as int, r);
        }
        r
    }

    /// The `n`th value left in the window (0-based), consuming it and those
    /// before it; past the window, consume the window. Past the data (the
    /// window can run past it), stop at its end. `far` holds the index's
    /// answers for the item `far_target(n)` names, if it names one: the
    /// caller asks the index, and this function calls nothing that can
    /// unwind.
    #[inline(always)]
    pub(crate) fn nth_at<'a, X: Decoders<'a, D = D>>(
        &mut self,
        slabs: &'a [Slab<X::Tail>],
        n: usize,
        far: (usize, usize, usize),
    ) -> (r: Option<D::Item>)
        requires
            old(self).wf::<X>(slabs@),
            old(self).far_spec(n) matches Some(t) ==> far_ok(slabs@, t, far),
        ensures
            final(self).wf::<X>(slabs@),
            n < old(self).out::<X>(slabs@).len() ==> {
                &&& r == Some(old(self).out::<X>(slabs@)[n as int])
                &&& final(self).out::<X>(slabs@) == old(self).out::<X>(slabs@).skip(n + 1)
                &&& final(self).pos == old(self).pos + n + 1
                &&& final(self).pos + final(self).items_left == old(self).pos + old(self).items_left
            },
            n >= old(self).out::<X>(slabs@).len() ==> {
                &&& r is None
                &&& final(self).out::<X>(slabs@).len() == 0
                &&& final(self).items_left == 0
                &&& old(self).pos <= total(slab_lens(slabs@)) ==> final(self).pos == old(self).pos + old(self).out::<X>(
                    slabs@,
                ).len()
            },
            // past the data (the window can run past it), it stops at the end
            old(self).pos <= total(slab_lens(slabs@)) ==> final(self).pos <= total(slab_lens(slabs@)),
        no_unwind
    {
        if n >= self.items_left {
            proof {
                assert(self.out::<X>(slabs@).len() <= self.items_left);
            }
            if self.items_left > 0 {
                let k = self.items_left - 1;
                let ghost c0 = *self;
                self.nth_in_at::<X>(slabs, k, far);
                proof {
                    if k < c0.out::<X>(slabs@).len() {
                        assert(c0.out::<X>(slabs@).skip(k + 1).len() == 0);
                    }
                }
            }
            return None;
        }
        self.nth_in_at::<X>(slabs, n, far)
    }

    /// `nth`, for `n` inside the window.
    #[verifier::spinoff_prover]
    fn nth_in<'a, X: Decoders<'a, D = D>, F: Fn(usize) -> (usize, usize), G: Fn(usize) -> usize>(
        &mut self,
        slabs: &'a [Slab<X::Tail>],
        n: usize,
        find: &F,
        start_of: &G,
    ) -> (r: Option<D::Item>)
        requires
            old(self).wf::<X>(slabs@),
            n < old(self).items_left,
            forall|t: usize| #[trigger] find.requires((t,)),
            forall|t: usize, x: (usize, usize)| #[trigger] find.ensures((t,), x) ==> find_ok(slabs@, t, x),
            forall|i: usize| i <= slabs@.len() ==> #[trigger] start_of.requires((i,)),
            forall|i: usize, n: usize|
                #[trigger] start_of.ensures((i,), n) ==> i <= slabs@.len() ==> n == sum(
                    slab_lens(slabs@),
                    0,
                    i as int,
                ),
        ensures
            final(self).wf::<X>(slabs@),
            n < old(self).out::<X>(slabs@).len() ==> {
                &&& r == Some(old(self).out::<X>(slabs@)[n as int])
                &&& final(self).out::<X>(slabs@) == old(self).out::<X>(slabs@).skip(n + 1)
                &&& final(self).pos == old(self).pos + n + 1
            },
            n >= old(self).out::<X>(slabs@).len() ==> r is None && final(self).out::<X>(slabs@).len() == 0,
            // past the data, it stops at the end
            old(self).pos <= total(slab_lens(slabs@)) ==> final(self).pos <= total(slab_lens(slabs@)),
    {
        hide(ICore::wf);
        hide(ICore::out);
        hide(find_ok);
        hide(sum);
        let ghost c0 = *self;
        let ghost s = slabs@;
        proof {
            self.lemma_nth_pre::<X>(s);
        }
        // Fast path: target is within the current slab.
        if n < self.slab_remaining {
            self.items_left -= n + 1;
            self.slab_remaining -= n + 1;
            self.pos += n + 1;
            let r = self.decoder.d_nth(n);
            proof {
                c0.lemma_nth_at::<X>(c0, *self, s, n as int, n as int, r);
            }
            return r;
        }
        // Use the column's index for an O(log S) slab lookup.
        let target = self.pos + n;
        let (si, off) = find(target);
        proof {
            c0.lemma_nth_found::<X>(s, n as int, target, (si, off));
        }
        // past the data (the window can run past it), stop at its end
        let start = if si < slabs.len() {
            target - off
        } else {
            start_of(slabs.len())
        };
        if !self.advance_to_slab::<X>(slabs, si, start) {
            proof {
                self.lemma_none_left::<X>(s);
            }
            return None;
        }
        let ghost c1 = *self;
        proof {
            c1.lemma_nth_pre::<X>(s);
        }
        self.slab_remaining -= off + 1;
        self.items_left -= off + 1;
        self.pos += off + 1;
        let r = self.decoder.d_nth(off);
        proof {
            c0.lemma_nth_at::<X>(c1, *self, s, n as int, off as int, r);
        }
        r
    }

    /// The `n`th value left in the window (0-based), consuming it and those
    /// before it; past the window, consume the window. Past the data (the
    /// window can run past it), stop at its end. `find` is the index lookup,
    /// and `start_of(i)` the items before slab `i`.
    pub(crate) fn nth<'a, X: Decoders<'a, D = D>, F: Fn(usize) -> (usize, usize), G: Fn(usize) -> usize>(
        &mut self,
        slabs: &'a [Slab<X::Tail>],
        n: usize,
        find: &F,
        start_of: &G,
    ) -> (r: Option<D::Item>)
        requires
            old(self).wf::<X>(slabs@),
            forall|t: usize| #[trigger] find.requires((t,)),
            forall|t: usize, x: (usize, usize)| #[trigger] find.ensures((t,), x) ==> find_ok(slabs@, t, x),
            forall|i: usize| i <= slabs@.len() ==> #[trigger] start_of.requires((i,)),
            forall|i: usize, n: usize|
                #[trigger] start_of.ensures((i,), n) ==> i <= slabs@.len() ==> n == sum(
                    slab_lens(slabs@),
                    0,
                    i as int,
                ),
        ensures
            final(self).wf::<X>(slabs@),
            n < old(self).out::<X>(slabs@).len() ==> {
                &&& r == Some(old(self).out::<X>(slabs@)[n as int])
                &&& final(self).out::<X>(slabs@) == old(self).out::<X>(slabs@).skip(n + 1)
                &&& final(self).pos == old(self).pos + n + 1
            },
            n >= old(self).out::<X>(slabs@).len() ==> r is None && final(self).out::<X>(slabs@).len() == 0,
            // past the data (the window can run past it), it stops at the end
            old(self).pos <= total(slab_lens(slabs@)) ==> final(self).pos <= total(slab_lens(slabs@)),
    {
        if n >= self.items_left {
            proof {
                assert(self.out::<X>(slabs@).len() <= self.items_left);
            }
            if self.items_left > 0 {
                let k = self.items_left - 1;
                let ghost c0 = *self;
                self.nth_in::<X, F, G>(slabs, k, find, start_of);
                proof {
                    if k < c0.out::<X>(slabs@).len() {
                        assert(c0.out::<X>(slabs@).skip(k + 1).len() == 0);
                    }
                }
            }
            return None;
        }
        self.nth_in::<X, F, G>(slabs, n, find, start_of)
    }

    /// The next run of equal values in the window, at most `max`, within
    /// one slab: what a `fold` passes on.
    #[verifier::spinoff_prover]
    #[inline(always)]
    pub(crate) fn run_once<'a, X: Decoders<'a, D = D>>(&mut self, slabs: &'a [Slab<X::Tail>], max: usize) -> (r: Option<Run<D::Item>>)
        requires
            old(self).wf::<X>(slabs@),
            max >= 1,
            old(self).items_left > 0,
        ensures
            final(self).wf::<X>(slabs@),
            match r {
                Some(run) => {
                    let o = old(self).out::<X>(slabs@);
                    &&& 1 <= run.count <= max
                    &&& run.count <= o.len()
                    &&& o.take(run.count as int) == Seq::new(run.count as nat, |_i: int| run.value)
                    &&& final(self).out::<X>(slabs@) == o.skip(run.count as int)
                    &&& final(self).pos == old(self).pos + run.count
                    &&& final(self).items_left == old(self).items_left - run.count
                    // maximal, unless cut by `max` or the slab's end
                    &&& run.count < max && run.count < o.len() && final(self).slab_remaining > 0
                        ==> o[run.count as int] != run.value
                },
                None => old(self).out::<X>(slabs@).len() == 0 && final(self).out::<X>(slabs@).len() == 0
                    && final(self).pos == old(self).pos && final(self).items_left == 0,
            },
        no_unwind
    {
        let ghost c0 = *self;
        let ghost s = slabs@;
        loop
            invariant
                c0 == *old(self),
                s == slabs@,
                self.wf::<X>(s),
                self.pos == c0.pos,
                self.items_left == c0.items_left,
                self.items_left > 0,
                max >= 1,
                self.out::<X>(s) == c0.out::<X>(s),
            decreases s.len() - self.slab_idx,
        {
            proof {
                self.lemma_here::<X>(s);
            }
            let cap = if self.items_left < self.slab_remaining {
                self.items_left
            } else {
                self.slab_remaining
            };
            let cap = if cap < max {
                cap
            } else {
                max
            };
            if cap > 0 {
                let ghost c1 = *self;
                if let Some(run) = self.decoder.d_run_max(cap) {
                    let count = run.count;
                    self.items_left -= count;
                    self.slab_remaining -= count;
                    self.pos += count;
                    proof {
                        c1.lemma_taken::<X>(*self, s, count as int);
                        if count < max && count < c1.out::<X>(s).len() && self.slab_remaining > 0 {
                            c1.lemma_out_at::<X>(s, count as int);
                        }
                    }
                    return Some(run);
                }
            }
            if self.slab_idx + 1 >= slabs.len() {
                proof {
                    self.lemma_end_out::<X>(s);
                }
                self.slab_idx = slabs.len();
                self.items_left = 0;
                return None;
            }
            let ghost c1 = *self;
            self.slab_idx += 1;
            self.slab_remaining = slabs[self.slab_idx].len;
            self.decoder = X::open(&slabs[self.slab_idx]);
            proof {
                c1.lemma_stepped::<X>(*self, s);
            }
        }
    }

    /// Append what is left of the window to `out`: each slab that the
    /// window takes to its end in one call of the slab's decoder, and
    /// the values of a slab in which the window ends one by one.
    #[inline(always)]
    pub(crate) fn drain_into<'a, X: Decoders<'a, D = D>>(&mut self, slabs: &'a [Slab<X::Tail>], out: &mut Vec<D::Item>)
        requires
            old(self).wf::<X>(slabs@),
        ensures
            final(self).wf::<X>(slabs@),
            final(out)@ == old(out)@ + old(self).out::<X>(slabs@),
            final(self).out::<X>(slabs@).len() == 0,
    {
        let ghost c0 = *self;
        let ghost s = slabs@;
        let ghost o0 = out@;
        loop
            invariant
                s == slabs@,
                self.wf::<X>(s),
                out@ + self.out::<X>(s) == o0 + c0.out::<X>(s),
            decreases s.len() - self.slab_idx,
        {
            proof {
                self.lemma_here::<X>(s);
            }
            let k = self.slab_remaining;
            if self.items_left == 0 || k > self.items_left {
                break ;
            }
            if k > 0 {
                let ghost c1 = *self;
                let ghost o1 = out@;
                self.decoder.d_drain(out);
                self.items_left -= k;
                self.slab_remaining = 0;
                self.pos += k;
                proof {
                    let r = c1.decoder.rest();
                    assert(self.decoder.rest() =~= r.skip(k as int));
                    c1.lemma_taken::<X>(*self, s, k as int);
                    let w = c1.out::<X>(s);
                    assert(r.take(k as int) =~= r);
                    assert(w =~= w.take(k as int) + w.skip(k as int));
                    assert(out@ + self.out::<X>(s) =~= o1 + w);
                }
            }
            if self.slab_idx + 1 >= slabs.len() {
                break ;
            }
            let ghost c1 = *self;
            self.slab_idx += 1;
            self.slab_remaining = slabs[self.slab_idx].len;
            self.decoder = X::open(&slabs[self.slab_idx]);
            proof {
                c1.lemma_stepped::<X>(*self, s);
            }
        }
        loop
            invariant
                s == slabs@,
                self.wf::<X>(s),
                out@ + self.out::<X>(s) == o0 + c0.out::<X>(s),
            ensures
                self.wf::<X>(s),
                out@ == o0 + c0.out::<X>(s),
                self.out::<X>(s).len() == 0,
            decreases self.out::<X>(s).len(),
        {
            let ghost o1 = out@;
            let ghost w = self.out::<X>(s);
            match self.next::<X>(slabs) {
                Some(v) => {
                    out.push(v);
                    proof {
                        assert(out@ + w.skip(1) =~= o1 + w);
                    }
                },
                None => {
                    proof {
                        assert(out@ + w =~= out@);
                    }
                    break ;
                },
            }
        }
    }

    /// Merge into a run of `value`, `total` long, the runs of the same
    /// value that follow in the slabs after this one, up to `left` more:
    /// `next_run_max` after its first run.
    #[inline(always)]
    #[verifier::spinoff_prover]
    pub(crate) fn merge_on<'a, X: Decoders<'a, D = D>>(
        &mut self,
        slabs: &'a [Slab<X::Tail>],
        value: D::Item,
        total: usize,
        left: usize,
        Ghost(o0): Ghost<Seq<D::Item>>,
    ) -> (t: usize)
        requires
            old(self).wf::<X>(slabs@),
            1 <= total <= o0.len(),
            o0.take(total as int) == Seq::new(total as nat, |_i: int| value),
            old(self).out::<X>(slabs@) == o0.skip(total as int),
            total + left <= usize::MAX,
            old(self).slab_remaining > 0 && left > 0 && total < o0.len() ==> o0[total as int] != value,
        ensures
            final(self).wf::<X>(slabs@),
            total <= t <= total + left,
            t <= o0.len(),
            o0.take(t as int) == Seq::new(t as nat, |_i: int| value),
            final(self).out::<X>(slabs@) == o0.skip(t as int),
            final(self).pos == old(self).pos + (t - total),
            final(self).items_left == old(self).items_left - (t - total),
            t < total + left && t < o0.len() ==> o0[t as int] != value,
    {
        let ghost s = slabs@;
        let ghost p0 = self.pos;
        let ghost end0 = self.pos + self.items_left;
        let ghost total0 = total;
        let ghost max0 = (total + left) as usize;
        let mut total_count = total;
        let mut max = left;
        loop
            invariant
                s == slabs@,
                self.wf::<X>(s),
                1 <= total_count <= o0.len(),
                o0.take(total_count as int) == Seq::new(total_count as nat, |_i: int| value),
                self.out::<X>(s) == o0.skip(total_count as int),
                self.pos + total0 == p0 + total_count,
                self.pos + self.items_left == end0,
                total_count + max == max0,
                total0 <= total_count,
                // not at a slab's end, the run is already maximal
                self.slab_remaining > 0 && total_count < max0 && total_count < o0.len()
                    ==> o0[total_count as int] != value,
            ensures
                self.wf::<X>(s),
                1 <= total_count <= o0.len(),
                o0.take(total_count as int) == Seq::new(total_count as nat, |_i: int| value),
                self.out::<X>(s) == o0.skip(total_count as int),
                self.pos + total0 == p0 + total_count,
                total_count + max == max0,
                total0 <= total_count,
                total_count < max0 && total_count < o0.len() ==> o0[total_count as int] != value,
            decreases s.len() - self.slab_idx,
        {
            if !(self.slab_remaining == 0 && self.items_left > 0) {
                proof {
                    if self.items_left == 0 {
                        self.lemma_none_left::<X>(s);
                    }
                }
                break;
            }
            if self.slab_idx + 1 >= slabs.len() {
                proof {
                    self.lemma_end_out::<X>(s);
                    assert(total_count < max0 && total_count < o0.len() ==> o0[total_count as int] != value);
                }
                break;
            }
            let ghost c1 = *self;
            self.slab_idx += 1;
            self.slab_remaining = slabs[self.slab_idx].len;
            self.decoder = X::open(&slabs[self.slab_idx]);
            proof {
                c1.lemma_stepped::<X>(*self, s);
            }
            let cap = if self.items_left < self.slab_remaining {
                self.items_left
            } else {
                self.slab_remaining
            };
            let cap = if cap < max {
                cap
            } else {
                max
            };
            if cap == 0 {
                if max == 0 {
                    proof {
                        assert(total_count < max0 && total_count < o0.len() ==> o0[total_count as int] != value);
                    }
                    break;
                }
                // an empty slab: step over it
                continue;
            }
            let ghost c1 = *self;
            if let Some(next) = self.decoder.d_run_max(cap) {
                if D::d_eq(&next.value, &value) {
                    let c = next.count;
                    let ghost t0 = total_count as int;
                    total_count += c;
                    max -= c;
                    self.items_left -= c;
                    self.slab_remaining -= c;
                    self.pos += c;
                    proof {
                        c1.lemma_taken::<X>(*self, s, c as int);
                        lemma_run_join(o0, value, t0, c as int);
                        lemma_skip_add(o0, t0, c as int);
                        if self.slab_remaining > 0 && total_count < max0 && total_count < o0.len() {
                            // short of the slab's end: cut by the decoder,
                            // whose run is maximal, or by the window
                            c1.lemma_out_at::<X>(s, c as int);
                            assert(o0[total_count as int] == c1.out::<X>(s)[c as int]);
                        }
                    }
                    continue;
                } else {
                    proof {
                        // the next value is this run's, and differs
                        if total_count < o0.len() {
                            c1.lemma_out_at::<X>(s, 0);
                            assert(c1.decoder.rest().take(next.count as int)[0] == next.value);
                            assert(o0[total_count as int] == c1.out::<X>(s)[0]);
                        }
                    }
                    // value doesn't match — reset the decoder so the run
                    // it consumed can be re-read by later calls
                    self.decoder = X::open(&slabs[self.slab_idx]);
                    proof {
                        lemma_skip_none(X::vals(s[self.slab_idx as int]));
                    }
                }
            }
            break;
        }
        total_count
    }

    /// The next run of equal values in the window, at most `max`, merged
    /// across slab boundaries.
    #[allow(clippy::question_mark)] // `?` puts the merge loop's proof over its rlimit
    #[verifier::spinoff_prover]
    #[inline(always)]
    pub(crate) fn next_run_max<'a, X: Decoders<'a, D = D>>(&mut self, slabs: &'a [Slab<X::Tail>], max: usize) -> (r: Option<Run<D::Item>>)
        requires
            old(self).wf::<X>(slabs@),
        ensures
            final(self).wf::<X>(slabs@),
            match r {
                Some(run) => {
                    let o = old(self).out::<X>(slabs@);
                    &&& 1 <= run.count <= max
                    &&& run.count <= o.len()
                    &&& o.take(run.count as int) == Seq::new(run.count as nat, |_i: int| run.value)
                    &&& final(self).out::<X>(slabs@) == o.skip(run.count as int)
                    &&& final(self).pos == old(self).pos + run.count
                    // maximal, unless cut by `max`: the next value differs
                    &&& run.count < max && run.count < o.len() ==> o[run.count as int] != run.value
                },
                None => (max == 0 || old(self).out::<X>(slabs@).len() == 0) && final(self).pos == old(self).pos,
            },
    {
        let ghost c0 = *self;
        let ghost s = slabs@;
        if max == 0 || self.items_left == 0 {
            proof {
                assert(self.out::<X>(s).len() <= self.items_left);
            }
            return None;
        }
        let first = match self.run_once::<X>(slabs, max) {
            Some(r) => r,
            None => return None,
        };
        let ghost o0 = c0.out::<X>(s);
        let total_count = self.merge_on::<X>(slabs, first.value, first.count, max - first.count, Ghost(o0));
        Some(Run { count: total_count, value: first.value })
    }

    /// The value search ([`crate::Iter::seek_to_value`]).  With the window
    /// sorted as `probe` sees it, returns the range `(a, b)` of the window
    /// at the target: below it before `a`, above it from `b`.  If the
    /// window is not sorted, some range within it.
    ///
    /// After the first run, a binary search over the slabs' first values
    /// finds the last slab that starts below the target, and a scan of
    /// runs goes on from there.  `find` is the index lookup, as for `nth`,
    /// and `start_of(i)` the items before slab `i`.
    #[verifier::spinoff_prover]
    pub(crate) fn seek_value<'a, X: Decoders<'a, D = D>, P: Probe<D::Item>, F: Fn(usize) -> (usize, usize), G: Fn(usize) -> usize>(
        &mut self,
        slabs: &'a [Slab<X::Tail>],
        probe: &P,
        find: &F,
        start_of: &G,
    ) -> (r: (usize, usize))
        requires
            old(self).wf::<X>(slabs@),
            forall|t: usize| #[trigger] find.requires((t,)),
            forall|t: usize, x: (usize, usize)| #[trigger] find.ensures((t,), x) ==> find_ok(slabs@, t, x),
            forall|i: usize| i <= slabs@.len() ==> #[trigger] start_of.requires((i,)),
            forall|i: usize, n: usize|
                #[trigger] start_of.ensures((i,), n) ==> i <= slabs@.len() ==> n == sum(
                    slab_lens(slabs@),
                    0,
                    i as int,
                ),
        ensures
            ({
                let w = old(self).out::<X>(slabs@);
                let p0 = old(self).pos;
                &&& p0 <= r.0 <= r.1 <= p0 + w.len()
                &&& monotone(*probe, w) ==> found(*probe, w, r.0 - p0, r.1 - p0)
            }),
    {
        hide(monotone);
        let ghost s = slabs@;
        let ghost c0 = *self;
        let ghost w = c0.out::<X>(s);
        proof {
            c0.lemma_here::<X>(s);
            assert(w.len() <= c0.items_left);
        }
        let start = self.pos;
        let end = self.pos + self.items_left;
        let first = match self.next_run_max::<X>(slabs, self.items_left) {
            Some(r) => r,
            None => return (start, start),
        };
        let ghost count = first.count as int;
        proof {
            assert forall|k: int| 0 <= k < count implies #[trigger] w[k] == first.value by {
                assert(w.take(count)[k] == w[k]);
            }
        }
        let si_start = self.slab_idx;
        match probe.side_of(&first.value) {
            Side::At => {
                proof {
                    if monotone(*probe, w) {
                        lemma_found_at(*probe, w, 0, count);
                    }
                }
                return (start, start + first.count);
            },
            Side::Above => {
                proof {
                    if monotone(*probe, w) {
                        lemma_found_above(*probe, w, 0);
                    }
                }
                return (start, start);
            },
            Side::Below => {},
        }
        // the slab holding the window's last item, or the last slab if the
        // window runs past the column's end
        let f = find(end - 1);
        let si_end = if f.0 < slabs.len() {
            f.0
        } else {
            slabs.len() - 1
        };
        // the last slab after this one, up to `si_end`, whose first value
        // is below the target
        let mut lo = si_start + 1;
        let mut hi = si_end + 1;
        let mut candidate: Option<usize> = None;
        while lo < hi
            invariant
                lens_ok::<X>(s),
                s == slabs@,
                si_end < s.len(),
                si_start < lo,
                hi <= si_end + 1,
                match candidate {
                    Some(i) => {
                        &&& si_start < i <= si_end
                        &&& s[i as int].len > 0
                        &&& probe.side(X::vals(s[i as int])[0]) < 0
                    },
                    None => true,
                },
            decreases hi - lo,
        {
            let mid = lo + (hi - lo) / 2;
            let mut dec = X::open(&slabs[mid]);
            match dec.d_next() {
                Some(head) => match probe.side_of(&head) {
                    Side::Below => {
                        candidate = Some(mid);
                        lo = mid + 1;
                    },
                    // an equal first value may still have equal values
                    // before it, at the end of the slab before
                    Side::At | Side::Above => hi = mid,
                },
                // an empty slab: there are none in a column with values
                None => hi = mid,
            }
        }
        // the target, if anywhere, is at or after that slab's first value
        if let Some(i) = candidate {
            let at = start_of(i);
            proof {
                self.lemma_jump::<X, P>(s, c0, *probe, count, i as int, (end - 1) as usize, f);
            }
            self.advance_to_slab::<X>(slabs, i, at);
            proof {
                assert(self.out::<X>(s) =~= w.skip(at - c0.pos));
            }
        }
        let mut pos = self.pos;
        loop
            invariant
                self.wf::<X>(s),
                s == slabs@,
                c0 == *old(self),
                w == c0.out::<X>(s),
                pos == self.pos,
                c0.pos <= pos <= c0.pos + w.len(),
                self.out::<X>(s) == w.skip(pos - c0.pos),
                monotone(*probe, w) ==> forall|k: int| 0 <= k < pos - c0.pos ==> probe.side(#[trigger] w[k]) < 0,
            decreases c0.pos + w.len() - pos,
        {
            let ghost o = self.out::<X>(s);
            proof {
                assert(o.len() <= self.items_left);
            }
            match self.next_run_max::<X>(slabs, self.items_left) {
                Some(run) => {
                    let ghost a = pos - c0.pos;
                    proof {
                        assert forall|k: int| a <= k < a + run.count implies #[trigger] w[k] == run.value by {
                            assert(o.take(run.count as int)[k - a] == o[k - a]);
                        }
                        assert(o[0] == w[a]);
                    }
                    match probe.side_of(&run.value) {
                        Side::Below => {
                            proof {
                                assert(self.out::<X>(s) =~= w.skip(pos + run.count - c0.pos));
                            }
                            pos += run.count;
                        },
                        Side::Above => {
                            proof {
                                if monotone(*probe, w) {
                                    lemma_found_above(*probe, w, a);
                                }
                            }
                            return (pos, pos);
                        },
                        Side::At => {
                            proof {
                                if monotone(*probe, w) {
                                    if a + run.count < w.len() {
                                        assert(o[run.count as int] == w[a + run.count]);
                                    }
                                    lemma_found_at(*probe, w, a, run.count as int);
                                }
                            }
                            return (pos, pos + run.count);
                        },
                    }
                },
                None => {
                    proof {
                        if monotone(*probe, w) {
                            lemma_found_above(*probe, w, w.len() as int);
                        }
                    }
                    return (pos, pos);
                },
            }
        }
    }

    /// The jump to slab `i`, whose first value is below the target, from
    /// here, after the window's first run of `count` values: slab `i`
    /// starts inside what is left of the window, and if the window is
    /// sorted, every value before it is below the target too.  `f` is the
    /// index's answer for the window's last item, `last`.
    proof fn lemma_jump<'a, X: Decoders<'a, D = D>, P: Probe<D::Item>>(
        &self,
        s: Seq<Slab<X::Tail>>,
        c0: Self,
        probe: P,
        count: int,
        i: int,
        last: usize,
        f: (usize, usize),
    )
        requires
            c0.wf::<X>(s),
            self.wf::<X>(s),
            1 <= count <= c0.out::<X>(s).len(),
            c0.out::<X>(s).len() <= c0.items_left,
            self.pos == c0.pos + count,
            self.out::<X>(s) == c0.out::<X>(s).skip(count),
            self.slab_idx < i < s.len(),
            s[i].len > 0,
            probe.side(X::vals(s[i])[0]) < 0,
            last == c0.pos + c0.items_left - 1,
            find_ok(s, last, f),
            f.0 < s.len() ==> i <= f.0,
        ensures
            ({
                let w = c0.out::<X>(s);
                let v = vals_of::<X>(s);
                let at = sum(slab_lens(s), 0, i);
                &&& self.pos <= at < c0.pos + w.len()
                &&& at - self.pos < self.items_left
                &&& c0.pos + w.len() <= v.len()
                &&& w == v.subrange(c0.pos as int, c0.pos + w.len())
                &&& c0.pos + w.len() == if self.pos + self.items_left < v.len() {
                    self.pos + self.items_left
                } else {
                    v.len() as int
                }
                &&& monotone(probe, w) ==> forall|k: int|
                    0 <= k < at - c0.pos ==> probe.side(#[trigger] w[k]) < 0
            }),
    {
        let lens = slab_lens(s);
        let w = c0.out::<X>(s);
        let v = vals_of::<X>(s);
        let at = sum(lens, 0, i) as int;
        c0.lemma_here::<X>(s);
        lemma_head::<X>(s, i);
        // here, at most at the end of this slab, so at most at slab `i`
        let si = self.slab_idx as int;
        lemma_sum_split(lens, 0, si, si + 1);
        lemma_sum_one(lens, si);
        lemma_sum_le(lens, 0, si + 1, 0, i);
        // slab `i` starts at or before the window's last item
        if f.0 < s.len() {
            lemma_sum_le(lens, 0, i, 0, f.0 as int);
        }
        let o = self.out::<X>(s);
        assert(o.len() == w.len() - count);
        assert(o.len() <= self.items_left);
        assert(w =~= v.subrange(c0.pos as int, c0.pos + w.len()));
        assert(w[at - c0.pos] == v[at]);
        if monotone(probe, w) {
            assert forall|k: int| 0 <= k < at - c0.pos implies probe.side(#[trigger] w[k]) < 0 by {
                assert(probe.side(w[k]) <= probe.side(w[at - c0.pos]));
            }
        }
    }

    /// Set the window to end at `end`.
    #[inline]
    #[allow(unused_variables)] // `slabs` is for the spec
    pub(crate) fn set_max<'a, X: Decoders<'a, D = D>>(&mut self, slabs: &'a [Slab<X::Tail>], end: usize)
        requires
            old(self).wf::<X>(slabs@),
        ensures
            final(self).wf::<X>(slabs@),
            final(self).pos == old(self).pos,
            final(self).items_left == if end >= old(self).pos { end - old(self).pos } else { 0 },
            final(self).slab_idx == old(self).slab_idx,
        no_unwind
    {
        self.items_left = end.saturating_sub(self.pos);
    }

    /// Advance so that the item at `target` comes next; at or past it
    /// already, stay.
    #[inline]
    pub(crate) fn advance_to<'a, X: Decoders<'a, D = D>, F: Fn(usize) -> (usize, usize), G: Fn(usize) -> usize>(
        &mut self,
        slabs: &'a [Slab<X::Tail>],
        target: usize,
        find: &F,
        start_of: &G,
    )
        requires
            old(self).wf::<X>(slabs@),
            lookups(slabs@, *find, *start_of),
        ensures
            final(self).wf::<X>(slabs@),
            final(self).out::<X>(slabs@) == past(old(self).out::<X>(slabs@), target - old(self).pos),
            target <= old(self).pos ==> final(self).pos == old(self).pos,
            old(self).pos < target <= old(self).pos + old(self).out::<X>(slabs@).len() ==> final(self).pos
                == target,
    {
        if target > self.pos {
            self.nth::<X, F, G>(slabs, target - self.pos - 1, find, start_of);
        }
    }

    /// Move the window to `start..end`. It moves only forwards: from a
    /// `start` before the iterator, the window starts where it stands.
    #[inline]
    pub(crate) fn shift<'a, X: Decoders<'a, D = D>, F: Fn(usize) -> (usize, usize), G: Fn(usize) -> usize>(
        &mut self,
        slabs: &'a [Slab<X::Tail>],
        start: usize,
        end: usize,
        find: &F,
        start_of: &G,
    )
        requires
            old(self).wf::<X>(slabs@),
            lookups(slabs@, *find, *start_of),
        ensures
            final(self).wf::<X>(slabs@),
            final(self).out::<X>(slabs@) == window(
                vals_of::<X>(slabs@),
                if start <= old(self).pos { old(self).pos as int } else { start as int },
                end as int,
            ),
            start <= old(self).pos ==> final(self).pos == old(self).pos,
            old(self).pos < start <= end && start <= vals_of::<X>(slabs@).len() ==> final(self).pos == start,
    {
        let ghost p = self.pos as int;
        self.set_max::<X>(slabs, end);
        proof {
            self.lemma_out_window::<X>(slabs@);
            if end < p {
                assert(window(vals_of::<X>(slabs@), p, p) =~= window(vals_of::<X>(slabs@), p, end as int));
            }
            if start > p {
                lemma_window_past(vals_of::<X>(slabs@), p, end as int, start - p);
            }
        }
        self.advance_to::<X, F, G>(slabs, start, find, start_of);
    }

    /// Move the window to `start..end` and give its first value; `start`
    /// is at or after the iterator.
    #[inline]
    pub(crate) fn shift_next<'a, X: Decoders<'a, D = D>, F: Fn(usize) -> (usize, usize), G: Fn(usize) -> usize>(
        &mut self,
        slabs: &'a [Slab<X::Tail>],
        start: usize,
        end: usize,
        find: &F,
        start_of: &G,
    ) -> (r: Option<D::Item>)
        requires
            old(self).wf::<X>(slabs@),
            lookups(slabs@, *find, *start_of),
            start >= old(self).pos,
        ensures
            final(self).wf::<X>(slabs@),
            ({
                let w = window(vals_of::<X>(slabs@), start as int, end as int);
                &&& w.len() > 0 ==> {
                    &&& r == Some(w[0])
                    &&& final(self).out::<X>(slabs@) == w.skip(1)
                    &&& final(self).pos == start + 1
                }
                &&& w.len() == 0 ==> r is None && final(self).out::<X>(slabs@).len() == 0
            }),
    {
        let ghost p = self.pos as int;
        self.set_max::<X>(slabs, end);
        let ghost o1 = self.out::<X>(slabs@);
        proof {
            self.lemma_out_window::<X>(slabs@);
            if end < p {
                assert(window(vals_of::<X>(slabs@), p, p) =~= window(vals_of::<X>(slabs@), p, end as int));
            }
            lemma_window_past(vals_of::<X>(slabs@), p, end as int, start - p);
        }
        let r = self.nth::<X, F, G>(slabs, start - self.pos, find, start_of);
        proof {
            let (w, n) = (window(vals_of::<X>(slabs@), start as int, end as int), start - p);
            if n < o1.len() {
                assert(w =~= o1.skip(n));
                assert(w.skip(1) =~= o1.skip(n + 1));
            }
        }
        r
    }

    /// Move `amount` items on.
    #[inline]
    pub(crate) fn advance_by<'a, X: Decoders<'a, D = D>, F: Fn(usize) -> (usize, usize), G: Fn(usize) -> usize>(
        &mut self,
        slabs: &'a [Slab<X::Tail>],
        amount: usize,
        find: &F,
        start_of: &G,
    )
        requires
            old(self).wf::<X>(slabs@),
            lookups(slabs@, *find, *start_of),
        ensures
            final(self).wf::<X>(slabs@),
            final(self).out::<X>(slabs@) == past(old(self).out::<X>(slabs@), amount as int),
            amount <= old(self).out::<X>(slabs@).len() ==> final(self).pos == old(self).pos + amount,
    {
        let ghost o = self.out::<X>(slabs@);
        proof {
            self.lemma_out_window::<X>(slabs@);
            // saturated, both move past everything left
            if amount > usize::MAX - self.pos {
                assert(past(o, usize::MAX - self.pos) =~= past(o, amount as int));
            }
        }
        self.advance_to::<X, F, G>(slabs, self.pos.saturating_add(amount), find, start_of);
    }

    /// Give the item at `at` and move past it; `None`, moving nowhere, if
    /// `at` is behind the iterator.
    #[inline]
    pub(crate) fn seek_to<'a, X: Decoders<'a, D = D>, F: Fn(usize) -> (usize, usize), G: Fn(usize) -> usize>(
        &mut self,
        slabs: &'a [Slab<X::Tail>],
        at: usize,
        find: &F,
        start_of: &G,
    ) -> (r: Option<D::Item>)
        requires
            old(self).wf::<X>(slabs@),
            lookups(slabs@, *find, *start_of),
        ensures
            final(self).wf::<X>(slabs@),
            at < old(self).pos ==> r is None && *final(self) == *old(self),
            at >= old(self).pos ==> {
                let (o, k) = (old(self).out::<X>(slabs@), at - old(self).pos);
                &&& k < o.len() ==> r == Some(o[k]) && final(self).out::<X>(slabs@) == o.skip(k + 1)
                    && final(self).pos == at + 1
                &&& k >= o.len() ==> r is None && final(self).out::<X>(slabs@).len() == 0
            },
    {
        if at >= self.pos {
            self.nth::<X, F, G>(slabs, at - self.pos, find, start_of)
        } else {
            None
        }
    }

    /// The next run of equal values in the window: as many as there
    /// are, up to the window's end.
    #[inline(always)]
    pub(crate) fn next_run<'a, X: Decoders<'a, D = D>>(&mut self, slabs: &'a [Slab<X::Tail>]) -> (r: Option<Run<D::Item>>)
        requires
            old(self).wf::<X>(slabs@),
        ensures
            final(self).wf::<X>(slabs@),
            runs_from::<X>(*old(self), *final(self), slabs@, r),
    {
        let max = self.items_left;
        proof {
            assert(self.out::<X>(slabs@).len() <= max);
        }
        self.next_run_max::<X>(slabs, max)
    }

    /// Move the window to `start..end` (only forwards, as `shift`) and
    /// give its first run.
    #[inline]
    pub(crate) fn shift_run<'a, X: Decoders<'a, D = D>, F: Fn(usize) -> (usize, usize), G: Fn(usize) -> usize>(
        &mut self,
        slabs: &'a [Slab<X::Tail>],
        start: usize,
        end: usize,
        find: &F,
        start_of: &G,
    ) -> (r: Option<Run<D::Item>>)
        requires
            old(self).wf::<X>(slabs@),
            lookups(slabs@, *find, *start_of),
        ensures
            final(self).wf::<X>(slabs@),
            ({
                let w = window(
                    vals_of::<X>(slabs@),
                    if start <= old(self).pos { old(self).pos as int } else { start as int },
                    end as int,
                );
                &&& first_run(w, r)
                &&& r matches Some(run) ==> final(self).out::<X>(slabs@) == w.skip(run.count as int)
            }),
    {
        self.shift::<X, F, G>(slabs, start, end, find, start_of);
        self.next_run::<X>(slabs)
    }

    /// Advance to the item at `at` and give the run from there; `None`,
    /// moving nowhere, if `at` is behind the iterator.
    #[inline]
    pub(crate) fn seek_run<'a, X: Decoders<'a, D = D>, F: Fn(usize) -> (usize, usize), G: Fn(usize) -> usize>(
        &mut self,
        slabs: &'a [Slab<X::Tail>],
        at: usize,
        find: &F,
        start_of: &G,
    ) -> (r: Option<Run<D::Item>>)
        requires
            old(self).wf::<X>(slabs@),
            lookups(slabs@, *find, *start_of),
        ensures
            final(self).wf::<X>(slabs@),
            at < old(self).pos ==> r is None && *final(self) == *old(self),
            at >= old(self).pos ==> {
                let w = past(old(self).out::<X>(slabs@), at - old(self).pos);
                &&& first_run(w, r)
                &&& r matches Some(run) ==> final(self).out::<X>(slabs@) == w.skip(run.count as int)
            },
    {
        if at < self.pos {
            return None;
        }
        self.advance_to::<X, F, G>(slabs, at, find, start_of);
        self.next_run::<X>(slabs)
    }
}

/// The run `r` is the first of `o`, as long as its values are equal; or
/// `o` is empty.
pub(crate) open spec fn first_run<V>(o: Seq<V>, r: Option<Run<V>>) -> bool {
    match r {
        Some(run) => {
            &&& 1 <= run.count <= o.len()
            &&& o.take(run.count as int) == Seq::new(run.count as nat, |_i: int| run.value)
            &&& run.count < o.len() ==> o[run.count as int] != run.value
        },
        None => o.len() == 0,
    }
}

/// The run `r` is the next one the window `c0` has left (`first_run`),
/// and `c` the iterator past it.
pub(crate) open spec fn runs_from<'a, X: Decoders<'a>>(
    c0: ICore<X::D>,
    c: ICore<X::D>,
    s: Seq<Slab<X::Tail>>,
    r: Option<Run<<X::D as Decode>::Item>>,
) -> bool {
    &&& first_run(c0.out::<X>(s), r)
    &&& r matches Some(run) ==> {
        &&& c.out::<X>(s) == c0.out::<X>(s).skip(run.count as int)
        &&& c.pos == c0.pos + run.count
    }
}

/// The index lookups `find` and `start_of` answer right for slabs `s`:
/// what a long seek (`nth`) is given.
pub(crate) open spec fn lookups<T: Copy + Clone + std::fmt::Debug + Default, F: Fn(usize) -> (usize, usize), G: Fn(usize) -> usize>(
    s: Seq<Slab<T>>,
    find: F,
    start_of: G,
) -> bool {
    &&& forall|t: usize| #[trigger] find.requires((t,))
    &&& forall|t: usize, x: (usize, usize)| #[trigger] find.ensures((t,), x) ==> find_ok(s, t, x)
    &&& forall|i: usize| i <= s.len() ==> #[trigger] start_of.requires((i,))
    &&& forall|i: usize, n: usize|
        #[trigger] start_of.ensures((i,), n) ==> i <= s.len() ==> n == sum(slab_lens(s), 0, i as int)
}

} // verus!
