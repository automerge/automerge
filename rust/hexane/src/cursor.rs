//! The column cursor's core ([`crate::edit::Edit`]), verified with Verus.
//!
//! A column is a vector of slabs, a slab index and a total length; the
//! cursor walks it forward, carrying, inserting and deleting values, with
//! at most one slab's rebuild open.  The column stands for its slabs'
//! values one after another (`vals_of`); `col_ok` says every slab is
//! well-formed, the index holds each slab's weight (`ws_ok`: as the
//! weight function computes it, so its item count too) and `total_len`
//! their sum.  A new column gets its index and total from `col_index`,
//! which checks its slabs' weights against them; that the slabs are
//! well-formed is up to what made them (the loaders, `fill`, the
//! encoders).  The cursor keeps, as ghost state, the column's values when
//! it opened (`o`) and the values it has produced (`p`), and its
//! invariant (`wf`) says the column's values are always `p` followed by
//! the original values not yet passed — with the open rebuild, if any,
//! holding the end of `p`.  Each operation is proved to keep `col_ok` and
//! `wf` and to do exactly what it says to `p` and the original position.
//! A whole splice, the chain behind `Column::splice` and the mutators
//! built on it, is one verified function ([`splice_feed`]): open,
//! delete, insert every run a feed gives until it ends, close.
//!
//! The slab-level operations are the encodings' own, through each
//! encoding's slab model ([`Slabs`], named by `SlabEdit::Model`), which
//! is verified against its format: bool columns'
//! ([`BoolModel`](crate::bool_edit::BoolModel)) and RLE columns'
//! ([`RleModel`](crate::rle::edit::RleModel)).  Trusted, in the bridges
//! at the bottom of this file: the dispatch of closures and inserted
//! values to an encoding's rebuild (`Visiting`, `Inserting`), that a
//! caller's iterator keeps the count a [`RunFeed`] promises (`Feeding`),
//! and the weight functions (`Weigh`: what a slab's weight is, `wrel`,
//! and that it counts its items).  The index is verified ([`IndexOps`]).
//!
//! This covers delta columns too.  `col_ok` asks for a `plain` index,
//! one whose shape needs only its item counts to fit, and the B-tree is
//! one for every aggregate: its aggregates are exact wherever they are
//! representable (`valid`), and only its queries need them to be.  A
//! delta column's index sums its deltas, which need not fit `i64` while
//! an edit is under way (a delete that drops a whole slab joins its
//! neighbours in the index before the delta that bridges them is
//! written); they fit again once the edit has written the column's
//! values, and the column's readers ask for that (`dix_ok`).

// Verus has no specs for `Vec::is_empty`, slices from `&Vec` or `vec![x]`,
// and the cursor's operations take the column's fields one by one.
#![allow(
    clippy::len_zero,
    clippy::ptr_arg,
    clippy::vec_init_then_push,
    clippy::too_many_arguments,
    clippy::type_complexity
)]

use crate::column::Slab;
#[cfg(verus_keep_ghost)]
use crate::fenwick::{lemma_sum_le, lemma_sum_one, lemma_sum_split, sum, total};
#[cfg(verus_keep_ghost)]
use crate::index::wlens;
use crate::index::IndexOps;
#[cfg(verus_keep_ghost)]
use crate::merge_walk::framed;
use crate::merge_walk::{merge_around, Merger};
#[cfg(verus_keep_ghost)]
use crate::rle::writer::concat;
use std::marker::PhantomData;
#[allow(unused_imports)]
use vstd::prelude::*;

verus! {

// ── The encodings' slab operations ─────────────────────────────────────────

/// The slab operations a cursor drives, specified against each slab's
/// values: an encoding's slab model, named by [`SlabEdit::Model`], and
/// verified against its format.
pub(crate) trait Slabs {
    type Tail: Copy + Clone + std::fmt::Debug + Default;

    /// An open rebuild of one slab.
    type Rebuild;

    /// A reader position in a slab.
    type State;

    /// A value, for specifications.
    type V;

    /// A well-formed slab: canonical bytes, and metadata that match them.
    spec fn ok(s: Slab<Self::Tail>) -> bool;

    /// A slab's values.
    spec fn vals(s: Slab<Self::Tail>) -> Seq<Self::V>;

    proof fn lemma_len(s: Slab<Self::Tail>)
        requires
            Self::ok(s),
        ensures
            Self::vals(s).len() == s.len,
    ;

    /// A rebuild is open, reading the slab bytes `rb_data`: it has written
    /// `rb_vals` and read `rb_read` of the slab's values.
    spec fn rb_inv(e: Self::Rebuild) -> bool;

    spec fn rb_data(e: Self::Rebuild) -> Seq<u8>;

    spec fn rb_vals(e: Self::Rebuild) -> Seq<Self::V>;

    spec fn rb_read(e: Self::Rebuild) -> nat;

    /// `st` reads the slab bytes `data` at value `pos`.
    spec fn st_at(data: Seq<u8>, st: Self::State, pos: nat) -> bool;

    /// How many runs a cursor carries through an open rebuild before
    /// closing it instead.
    fn carry_budget() -> usize;

    /// A rebuild with nothing open.
    fn idle() -> Self::Rebuild;

    fn empty_slab() -> (s: Slab<Self::Tail>)
        ensures
            Self::ok(s),
            Self::vals(s) == Seq::<Self::V>::empty(),
    ;

    /// Open a rebuild of `s` at value `at`.
    fn reset(e: &mut Self::Rebuild, s: &Slab<Self::Tail>, at: usize, max_segments: usize)
        requires
            Self::ok(*s),
            at <= s.len,
            max_segments >= 2,
        ensures
            Self::rb_inv(*final(e)),
            Self::rb_data(*final(e)) == s.data@,
            Self::rb_vals(*final(e)) == Self::vals(*s).take(at as int),
            Self::rb_read(*final(e)) == at,
    ;

    /// Write the rebuild back into `s`, returning the slabs it spilled
    /// into: they hold the values written, then the slab's unread ones.
    fn close(e: &mut Self::Rebuild, s: &mut Slab<Self::Tail>) -> (spilled: Vec<Slab<Self::Tail>>)
        requires
            Self::rb_inv(*old(e)),
            Self::ok(*old(s)),
            Self::rb_data(*old(e)) == old(s).data@,
            Self::rb_read(*old(e)) <= old(s).len,
            Self::rb_vals(*old(e)).len() + (old(s).len - Self::rb_read(*old(e))) + 3 <= usize::MAX,
        ensures
            Self::ok(*final(s)),
            forall|i: int| 0 <= i < spilled@.len() ==> #[trigger] Self::ok(spilled@[i]),
            Self::vals(*final(s)) + concat(spilled@.map_values(|x: Slab<Self::Tail>| Self::vals(x)))
                == Self::rb_vals(*old(e)) + Self::vals(*old(s)).skip(Self::rb_read(*old(e)) as int),
    ;

    /// Append `b`'s values to `a`'s.
    fn merge(a: &mut Slab<Self::Tail>, b: Slab<Self::Tail>)
        requires
            Self::ok(*old(a)),
            Self::ok(b),
            old(a).len + b.len <= usize::MAX,
        ensures
            Self::ok(*final(a)),
            Self::vals(*final(a)) == Self::vals(*old(a)) + Self::vals(b),
    ;

    /// The open rebuild's reader, at the value it has read up to.
    fn read_state(e: &Self::Rebuild) -> (st: &Self::State)
        requires
            Self::rb_inv(*e),
        ensures
            Self::st_at(Self::rb_data(*e), *st, Self::rb_read(*e)),
    ;

    fn state_at(s: &Slab<Self::Tail>, at: usize) -> (st: Self::State)
        requires
            Self::ok(*s),
            at <= s.len,
        ensures
            Self::st_at(s.data@, st, at as nat),
    ;

    fn state_advance(s: &Slab<Self::Tail>, st: &mut Self::State, n: usize, pos: Ghost<nat>)
        requires
            Self::ok(*s),
            Self::st_at(s.data@, *old(st), pos@),
            pos@ + n <= s.len,
        ensures
            Self::st_at(s.data@, *final(st), (pos@ + n) as nat),
    ;
}

/// Carrying and dropping a slab's values through a rebuild, reporting
/// each run to the visitor (a delta column keeps its running value so).
pub(crate) trait Visitor<E: Slabs> {
    /// Carry `n` of the slab's values, spending at most `runs` pushes;
    /// returns the ones left uncarried.
    fn pass(&mut self, e: &mut E::Rebuild, s: &Slab<E::Tail>, n: usize, runs: usize) -> (left: usize)
        requires
            E::rb_inv(*old(e)),
            E::ok(*s),
            E::rb_data(*old(e)) == s.data@,
            E::rb_read(*old(e)) + n <= s.len,
            E::rb_vals(*old(e)).len() + n + 3 <= usize::MAX,
        ensures
            E::rb_inv(*final(e)),
            E::rb_data(*final(e)) == s.data@,
            left <= n,
            E::rb_read(*final(e)) == E::rb_read(*old(e)) + (n - left),
            E::rb_vals(*final(e)) == E::rb_vals(*old(e)) + E::vals(*s).subrange(
                E::rb_read(*old(e)) as int,
                E::rb_read(*old(e)) + (n - left),
            ),
    ;

    /// Drop `n` of the slab's values.
    fn discard(&mut self, e: &mut E::Rebuild, s: &Slab<E::Tail>, n: usize)
        requires
            E::rb_inv(*old(e)),
            E::ok(*s),
            E::rb_data(*old(e)) == s.data@,
            E::rb_read(*old(e)) + n <= s.len,
        ensures
            E::rb_inv(*final(e)),
            E::rb_data(*final(e)) == s.data@,
            E::rb_read(*final(e)) == E::rb_read(*old(e)) + n,
            E::rb_vals(*final(e)) == E::rb_vals(*old(e)),
    ;
}

/// A value a rebuild can take.
pub(crate) trait Insertable<E: Slabs>: Sized {
    spec fn val(&self) -> E::V;

    /// Write `n` copies.
    fn insert_into(self, e: &mut E::Rebuild, n: usize)
        requires
            E::rb_inv(*old(e)),
            E::rb_vals(*old(e)).len() + n + 3 <= usize::MAX,
        ensures
            E::rb_inv(*final(e)),
            E::rb_data(*final(e)) == E::rb_data(*old(e)),
            E::rb_read(*final(e)) == E::rb_read(*old(e)),
            E::rb_vals(*final(e)) == E::rb_vals(*old(e)) + Seq::new(n as nat, |_i: int| self.val()),
    ;
}

/// Told the weight of each slab a delete drops whole.
pub(crate) trait WeightSink<W> {
    fn dropped(&mut self, w: &W);
}

/// A read model `X` that sees each slab as this edit model does: valid
/// when it is, and holding as many values. Each encoding's two models are
/// the same parse of a slab's bytes, read borrowed and owned
/// (`rle::lift`), so this is proved of both.
pub(crate) trait ReadsAgree<'a, X: crate::iter_core::Decoders<'a, Tail = <Self as Slabs>::Tail>>: Slabs + Sized {
    /// A value read, owned: as the edit model has it.
    spec fn own(v: <X::D as crate::iter_core::Decode>::Item) -> Self::V;

    /// A value read, to insert into a rebuild.
    type Ins: Insertable<Self>;

    /// What inserting `v` writes: its owned form.
    fn ins(v: <X::D as crate::iter_core::Decode>::Item) -> (x: Self::Ins)
        ensures
            x.val() == Self::own(v),
    ;

    proof fn lemma_reads(s: Slab<Self::Tail>)
        requires
            Self::ok(s),
        ensures
            X::ok(s),
            X::vals(s).len() == s.len,
            Self::vals(s) == X::vals(s).map_values(|v: <X::D as crate::iter_core::Decode>::Item| Self::own(v)),
    ;
}

/// A column's values, read and owned, are its values in the edit model.
pub(crate) proof fn lemma_vals_own<'a, E: ReadsAgree<'a, X>, X: crate::iter_core::Decoders<'a, Tail = E::Tail>>(s: Seq<Slab<E::Tail>>)
    requires
        all_ok::<E>(s),
    ensures
        vals_of::<E>(s) == crate::iter_core::vals_of::<X>(s).map_values(|v: <X::D as crate::iter_core::Decode>::Item| E::own(v)),
    decreases s.len(),
{
    if s.len() == 0 {
        assert(s.map_values(|x: Slab<E::Tail>| E::vals(x)) =~= Seq::<Seq<E::V>>::empty());
        assert(crate::iter_core::each::<X>(s) =~= Seq::<Seq<<X::D as crate::iter_core::Decode>::Item>>::empty());
        assert(crate::iter_core::vals_of::<X>(s).map_values(|v: <X::D as crate::iter_core::Decode>::Item| E::own(v)) =~= Seq::<E::V>::empty());
    } else {
        let p = s.drop_last();
        let x = s.last();
        assert forall|i: int| 0 <= i < p.len() implies E::ok(#[trigger] p[i]) by {
            assert(p[i] == s[i]);
        }
        lemma_vals_own::<E, X>(p);
        E::lemma_reads(x);
        assert(s.map_values(|y: Slab<E::Tail>| E::vals(y)).drop_last() =~= p.map_values(|y: Slab<E::Tail>| E::vals(y)));
        assert(crate::iter_core::each::<X>(s).drop_last() =~= crate::iter_core::each::<X>(p));
        let a = crate::iter_core::vals_of::<X>(p);
        let b = X::vals(x);
        assert((a + b).map_values(|v: <X::D as crate::iter_core::Decode>::Item| E::own(v))
            =~= a.map_values(|v: <X::D as crate::iter_core::Decode>::Item| E::own(v)) + b.map_values(|v: <X::D as crate::iter_core::Decode>::Item| E::own(v)));
    }
}

/// Slabs well-formed in an edit model are, in a read model that agrees
/// with it, what the readers need (`lens_ok`).
pub(crate) proof fn lemma_lens_ok<'a, E: ReadsAgree<'a, X>, X: crate::iter_core::Decoders<'a, Tail = E::Tail>>(s: Seq<Slab<E::Tail>>)
    requires
        all_ok::<E>(s),
    ensures
        crate::iter_core::lens_ok::<X>(s),
{
    assert forall|i: int| 0 <= i < s.len() implies X::ok(s[i]) && X::vals(s[i]).len() == s[i].len by {
        E::lemma_reads(s[i]);
    }
}

/// A slab's weight in the index: one that counts its items.
pub(crate) trait Weigh<E: Slabs, W> {
    /// `w` is slab `s`'s weight: what the column's index records for it.
    spec fn wrel(w: W, s: Slab<E::Tail>) -> bool;

    fn compute<I: IndexOps<W>>(s: &Slab<E::Tail>) -> (w: W)
        requires
            E::ok(*s),
        ensures
            I::wlen(w) == s.len,
            Self::wrel(w, *s),
    ;

    /// Panic unless `index` can take `w` as slab `i`'s weight, leaving a
    /// column of `len` items: weights can overflow together where none
    /// does alone (prefix sums). It specifies nothing, as it can only
    /// panic.
    fn check_set<I: IndexOps<W>>(index: &I, i: usize, w: &W, len: usize)
        requires
            index.ok(),
            i < index.ws().len(),
    ;

    /// Likewise for `new` in place of slabs `start..end`'s weights.
    fn check_splice<I: IndexOps<W>>(index: &I, start: usize, end: usize, new: &Vec<W>, len: usize)
        requires
            index.ok(),
            start <= end <= index.ws().len(),
    ;

    /// What the model asks of the column's values as a whole, beyond each
    /// slab's weight. Only a delta column asks anything: that every step
    /// between two of its running sums fits `i64`, as its readers need
    /// (`weights::DeltaModel`).
    spec fn dom_ok(s: Seq<Slab<E::Tail>>) -> bool;

    /// Whether the column of `slabs`, `len` items, is in the domain
    /// (`dom_ok`): exactly, reading every value where the model asks
    /// anything.
    fn dom_check(slabs: &Vec<Slab<E::Tail>>, len: usize) -> (r: bool)
        requires
            all_ok::<E>(slabs@),
            len == total(slab_lens(slabs@)),
        ensures
            r ==> Self::dom_ok(slabs@),
    ;

    /// A column of no values is in the domain.
    proof fn lemma_dom_empty(s: Seq<Slab<E::Tail>>)
        requires
            all_ok::<E>(s),
            vals_of::<E>(s).len() == 0,
        ensures
            Self::dom_ok(s),
    ;
}

// ── The column model ───────────────────────────────────────────────────────

/// The slabs' values, one after another.
pub(crate) open spec fn vals_of<E: Slabs>(s: Seq<Slab<E::Tail>>) -> Seq<E::V> {
    concat(s.map_values(|x: Slab<E::Tail>| E::vals(x)))
}

/// The slabs' item counts.
pub(crate) open spec fn slab_lens<T: Copy + Clone + std::fmt::Debug + Default>(s: Seq<Slab<T>>) -> Seq<nat> {
    s.map_values(|x: Slab<T>| x.len as nat)
}

pub(crate) open spec fn all_ok<E: Slabs>(s: Seq<Slab<E::Tail>>) -> bool {
    forall|i: int| 0 <= i < s.len() ==> #[trigger] E::ok(s[i])
}

/// A column: well-formed slabs, an index of their item counts, and their
/// total.
/// Each slab's weight in the index is its own (`G::wrel`).
pub(crate) open spec fn ws_ok<E: Slabs, W, G: Weigh<E, W>>(ws: Seq<W>, s: Seq<Slab<E::Tail>>) -> bool {
    &&& ws.len() == s.len()
    &&& forall|i: int| 0 <= i < s.len() ==> #[trigger] G::wrel(ws[i], s[i])
}

pub(crate) open spec fn col_ok<E: Slabs, W, I: IndexOps<W>, G: Weigh<E, W>>(
    s: Seq<Slab<E::Tail>>,
    ix: I,
    total_len: usize,
    max_segments: usize,
) -> bool {
    &&& all_ok::<E>(s)
    &&& ix.ok()
    &&& wlens::<W, I>(ix.ws()) == slab_lens(s)
    &&& ws_ok::<E, W, G>(ix.ws(), s)
    &&& total_len == total(slab_lens(s))
    &&& max_segments >= 2
    &&& I::plain()
}

/// A new column's index and item total, from its slabs and their
/// weights; `None` if a weight does not count its slab's items or the
/// items do not fit a `usize`.  With well-formed slabs and a budget of at
/// least 2 segments, the result is a column (`col_ok`).  Every way of
/// building a `Column` from slabs goes through here.
#[inline]
pub(crate) fn col_index<T: Copy + Clone + std::fmt::Debug + Default, W, I: IndexOps<W>>(
    slabs: &[Slab<T>],
    ws: Vec<W>,
) -> (r: Option<(I, usize)>)
    requires
        // a delta index's sums are not bounded by its item counts (see the
        // module docs); its loader checks them, which is not proved
        !I::plain() ==> I::fits(ws@),
    ensures
        match r {
            Some((ix, n)) => {
                &&& ix.ok()
                &&& ix.ws() == ws@
                &&& wlens::<W, I>(ix.ws()) == slab_lens(slabs@)
                &&& n == total(slab_lens(slabs@))
            },
            None => wlens::<W, I>(ws@) != slab_lens(slabs@) || total(slab_lens(slabs@)) > usize::MAX,
        },
{
    let ghost lens = slab_lens(slabs@);
    if ws.len() != slabs.len() {
        return None;
    }
    let mut n: usize = 0;
    let mut i: usize = 0;
    while i < slabs.len()
        invariant
            ws@.len() == slabs@.len(),
            lens == slab_lens(slabs@),
            0 <= i <= slabs@.len(),
            n == sum(lens, 0, i as int),
            forall|j: int| 0 <= j < i ==> #[trigger] I::wlen(ws@[j]) == lens[j],
        decreases slabs.len() - i,
    {
        let len = slabs[i].len;
        if I::w_len(&ws[i]) != len {
            proof {
                assert(wlens::<W, I>(ws@)[i as int] != lens[i as int]);
            }
            return None;
        }
        proof {
            lemma_sum_split(lens, 0, i as int, i + 1);
            lemma_sum_one(lens, i as int);
        }
        n = match n.checked_add(len) {
            Some(m) => m,
            None => {
                proof {
                    lemma_sum_le(lens, 0, i + 1, 0, lens.len() as int);
                }
                return None;
            },
        };
        i += 1;
    }
    proof {
        assert(wlens::<W, I>(ws@) =~= lens);
        if I::plain() {
            I::lemma_fits(ws@);
        }
    }
    Some((I::ix_from(ws), n))
}

pub(crate) proof fn lemma_concat_append<V>(a: Seq<Seq<V>>, b: Seq<Seq<V>>)
    ensures
        concat(a + b) == concat(a) + concat(b),
    decreases b.len(),
{
    if b.len() == 0 {
        assert(a + b =~= a);
        assert(concat(a) + concat(b) =~= concat(a));
    } else {
        lemma_concat_append(a, b.drop_last());
        assert((a + b).drop_last() =~= a + b.drop_last());
        assert(concat(a + b) =~= concat(a) + concat(b));
    }
}

pub(crate) proof fn lemma_vals_cat<E: Slabs>(a: Seq<Slab<E::Tail>>, b: Seq<Slab<E::Tail>>)
    ensures
        vals_of::<E>(a + b) == vals_of::<E>(a) + vals_of::<E>(b),
{
    let f = |x: Slab<E::Tail>| E::vals(x);
    assert((a + b).map_values(f) =~= a.map_values(f) + b.map_values(f));
    lemma_concat_append(a.map_values(f), b.map_values(f));
}

pub(crate) proof fn lemma_vals_unit<E: Slabs>(x: Slab<E::Tail>)
    ensures
        vals_of::<E>(seq![x]) == E::vals(x),
        vals_of::<E>(Seq::<Slab<E::Tail>>::empty()) == Seq::<E::V>::empty(),
{
    let e = Seq::<Seq<E::V>>::empty();
    assert(seq![x].map_values(|y: Slab<E::Tail>| E::vals(y)) =~= e.push(E::vals(x)));
    assert(e.push(E::vals(x)).drop_last() =~= e);
    assert(Seq::<Slab<E::Tail>>::empty().map_values(|y: Slab<E::Tail>| E::vals(y)) =~= e);
    assert(concat(e) == Seq::<E::V>::empty());
    assert(Seq::<E::V>::empty() + E::vals(x) =~= E::vals(x));
}

pub(crate) proof fn lemma_vals_split<E: Slabs>(s: Seq<Slab<E::Tail>>, k: int)
    requires
        0 <= k <= s.len(),
    ensures
        vals_of::<E>(s) == vals_of::<E>(s.take(k)) + vals_of::<E>(s.skip(k)),
{
    assert(s =~= s.take(k) + s.skip(k));
    lemma_vals_cat::<E>(s.take(k), s.skip(k));
}

/// Slab `k` sits between the slabs before and after it.
pub(crate) proof fn lemma_vals_at<E: Slabs>(s: Seq<Slab<E::Tail>>, k: int)
    requires
        0 <= k < s.len(),
    ensures
        vals_of::<E>(s) == vals_of::<E>(s.take(k)) + E::vals(s[k]) + vals_of::<E>(s.skip(k + 1)),
{
    lemma_vals_split::<E>(s, k);
    lemma_vals_split::<E>(s.skip(k), 1);
    assert(s.skip(k).take(1) =~= seq![s[k]]);
    assert(s.skip(k).skip(1) =~= s.skip(k + 1));
    lemma_vals_unit::<E>(s[k]);
    assert(vals_of::<E>(s) =~= vals_of::<E>(s.take(k)) + E::vals(s[k]) + vals_of::<E>(s.skip(k + 1)));
}

/// The values before slab `k` number the item counts before it.
pub(crate) proof fn lemma_vals_len<E: Slabs>(s: Seq<Slab<E::Tail>>, k: int)
    requires
        all_ok::<E>(s),
        0 <= k <= s.len(),
    ensures
        vals_of::<E>(s.take(k)).len() == sum(slab_lens(s), 0, k),
    decreases k,
{
    if k == 0 {
        assert(s.take(0) =~= Seq::<Slab<E::Tail>>::empty());
        lemma_vals_unit::<E>(s[0]);
    } else {
        lemma_vals_len::<E>(s, k - 1);
        assert(s.take(k) =~= s.take(k - 1) + seq![s[k - 1]]);
        lemma_vals_cat::<E>(s.take(k - 1), seq![s[k - 1]]);
        lemma_vals_unit::<E>(s[k - 1]);
        E::lemma_len(s[k - 1]);
    }
}

pub(crate) proof fn lemma_vals_total<E: Slabs>(s: Seq<Slab<E::Tail>>)
    requires
        all_ok::<E>(s),
    ensures
        vals_of::<E>(s).len() == total(slab_lens(s)),
{
    lemma_vals_len::<E>(s, s.len() as int);
    assert(s.take(s.len() as int) =~= s);
}

// ── Merging ─────────────────────────────────────────────────────────────────

/// Two adjacent slabs merge when either is under a quarter of the segment
/// budget (at least one, so an emptied slab always can) and together they
/// fit it.
pub(crate) struct SlabMerge<E> {
    pub(crate) max_segments: usize,
    pub(crate) _e: PhantomData<E>,
}

impl<E: Slabs> Merger<Slab<E::Tail>> for SlabMerge<E> {
    type X = E::V;

    open spec fn content(s: Seq<Slab<E::Tail>>) -> Seq<E::V> {
        vals_of::<E>(s)
    }

    open spec fn inv(&self, s: Seq<Slab<E::Tail>>) -> bool {
        all_ok::<E>(s) && total(slab_lens(s)) <= usize::MAX
    }

    // The check is inlined into the walk; the merge itself is not, so a
    // walk that merges nothing (the usual case) pays no call per pair.
    #[inline(always)]
    fn try_pair(&self, v: &mut Vec<Slab<E::Tail>>, a: usize) -> bool {
        if a >= v.len() || v.len() - a < 2 {
            return false;
        }
        let max = self.max_segments;
        // clamped so that even at tiny max_segments a slab emptied by a
        // deletion (0 segments) always merges away
        let min = if max / 4 >= 1 {
            max / 4
        } else {
            1
        };
        let sa = v[a].segments;
        let sb = v[a + 1].segments;
        if (sa < min || sb < min) && sa <= max && sb <= max - sa {
            merge_pair::<E>(v, a);
            true
        } else {
            false
        }
    }
}

/// Merge slab `a + 1` into slab `a`.
#[inline(never)]
fn merge_pair<E: Slabs>(v: &mut Vec<Slab<E::Tail>>, a: usize)
    requires
        a + 1 < old(v)@.len(),
        all_ok::<E>(old(v)@),
        total(slab_lens(old(v)@)) <= usize::MAX,
    ensures
        all_ok::<E>(final(v)@),
        total(slab_lens(final(v)@)) <= usize::MAX,
        vals_of::<E>(final(v)@) == vals_of::<E>(old(v)@),
        final(v)@.len() == old(v)@.len() - 1,
        final(v)@.take(a as int) == old(v)@.take(a as int),
        final(v)@.skip(a + 1) == old(v)@.skip(a + 2),
{
    let ghost s0 = v@;
    let _ = v.len();
    proof {
        lemma_vals_len::<E>(s0, a as int);
        lemma_vals_len::<E>(s0, a + 2);
        lemma_sum_split(slab_lens(s0), 0, a as int, a + 2);
        lemma_sum_split(slab_lens(s0), a as int, a + 1, a + 2);
        lemma_sum_one(slab_lens(s0), a as int);
        lemma_sum_one(slab_lens(s0), a + 1);
        lemma_sum_le(slab_lens(s0), 0, a + 2, 0, s0.len() as int);
    }
    let b = v.remove(a + 1);
    E::merge(&mut v[a], b);
    proof {
        let s1 = v@;
        assert(s1.take(a as int) =~= s0.take(a as int));
        assert(s1.skip(a + 1) =~= s0.skip(a + 2));
        lemma_vals_at::<E>(s0, a as int);
        lemma_vals_at::<E>(s0.skip(a + 1), 0);
        assert(s0.skip(a + 1).take(0) =~= Seq::<Slab<E::Tail>>::empty());
        assert(s0.skip(a + 1).skip(1) =~= s0.skip(a + 2));
        lemma_vals_unit::<E>(s0[a as int]);
        lemma_vals_at::<E>(s1, a as int);
        assert(vals_of::<E>(s1) =~= vals_of::<E>(s0));
        assert forall|i: int| 0 <= i < s1.len() implies #[trigger] E::ok(s1[i]) by {
            if i < a {
                assert(s1[i] == s0[i]);
            } else if i > a {
                assert(s1[i] == s0[i + 1]);
            }
        }
        lemma_vals_total::<E>(s0);
        lemma_vals_total::<E>(s1);
    }
}

/// An insert would take the column past `usize::MAX - 3` items, which
/// the rebuilds can't count.  Trusted only in that a panic does not
/// return.
#[verifier::external_body]
#[cold]
#[inline(never)]
fn too_long(Ghost(why): Ghost<bool>)
    requires
        why,
    ensures
        false,
{
    panic!("column too long to edit")
}

/// A sum over a suffix is the suffix's total.
proof fn lemma_sum_tail(s: Seq<nat>, k: int)
    requires
        0 <= k <= s.len(),
    ensures
        sum(s, k, s.len() as int) == total(s.skip(k)),
    decreases s.len() - k,
{
    if k < s.len() {
        lemma_sum_tail(s, k + 1);
        lemma_sum_split(s, k, k + 1, s.len() as int);
        lemma_sum_one(s, k);
        let t = s.skip(k);
        lemma_sum_split(t, 0, 1, t.len() as int);
        lemma_sum_one(t, 0);
        lemma_sum_shift(t, 1, t.len() as int);
        assert(t.skip(1) =~= s.skip(k + 1));
    } else {
        assert(s.skip(k) =~= Seq::<nat>::empty());
    }
}

/// Summing from `a` is summing the suffix from 0.
proof fn lemma_sum_shift(s: Seq<nat>, a: int, b: int)
    requires
        0 <= a <= b <= s.len(),
    ensures
        sum(s, a, b) == sum(s.skip(a), 0, b - a),
    decreases b - a,
{
    if b > a {
        lemma_sum_shift(s, a, b - 1);
    }
}

/// Sums are totals of subranges.
proof fn lemma_sum_sub(s: Seq<nat>, a: int, b: int)
    requires
        0 <= a <= b <= s.len(),
    ensures
        sum(s, a, b) == total(s.subrange(a, b)),
    decreases b - a,
{
    if b > a {
        lemma_sum_sub(s, a, b - 1);
        let t = s.subrange(a, b);
        assert(t.subrange(0, t.len() - 1) =~= s.subrange(a, b - 1));
        lemma_sum_prefix(t, t.len() - 1);
    }
}

/// A sum over a prefix ignores the rest.
proof fn lemma_sum_prefix(s: Seq<nat>, k: int)
    requires
        0 <= k <= s.len(),
    ensures
        sum(s, 0, k) == total(s.subrange(0, k)),
    decreases k,
{
    if k > 0 {
        lemma_sum_prefix(s, k - 1);
        let t = s.subrange(0, k);
        lemma_sum_prefix(t, k - 1);
        assert(t.subrange(0, k - 1) =~= s.subrange(0, k - 1));
        assert(sum(t, 0, k) == sum(t, 0, k - 1) + t[k - 1]);
        lemma_sum_agree(s, t, k - 1);
    }
}

/// Sequences that agree up to `k` sum the same up to `k`.
proof fn lemma_sum_agree(s: Seq<nat>, t: Seq<nat>, k: int)
    requires
        0 <= k <= s.len(),
        k <= t.len(),
        forall|i: int| 0 <= i < k ==> s[i] == t[i],
    ensures
        sum(s, 0, k) == sum(t, 0, k),
    decreases k,
{
    if k > 0 {
        lemma_sum_agree(s, t, k - 1);
    }
}

/// A rebuild's write-back: slab `si` of `s0` became `e1` and the slabs it
/// spilled into, `sp`.
proof fn lemma_spliced<E: Slabs>(
    s0: Seq<Slab<E::Tail>>,
    s1: Seq<Slab<E::Tail>>,
    si: int,
    e1: Slab<E::Tail>,
    sp: Seq<Slab<E::Tail>>,
)
    requires
        0 <= si < s0.len(),
        all_ok::<E>(s0),
        E::ok(e1),
        forall|i: int| 0 <= i < sp.len() ==> #[trigger] E::ok(sp[i]),
        s1 == s0.take(si) + (seq![e1] + sp) + s0.skip(si + 1),
    ensures
        all_ok::<E>(s1),
        vals_of::<E>(s1) == vals_of::<E>(s0.take(si)) + (E::vals(e1) + concat(
            sp.map_values(|x: Slab<E::Tail>| E::vals(x)),
        )) + vals_of::<E>(s0.skip(si + 1)),
        total(slab_lens(s1)) + s0[si].len == total(slab_lens(s0)) + sum(slab_lens(s1), si, si + 1 + sp.len()),
        s1.take(si) == s0.take(si),
        s1.skip(si + 1 + sp.len()) == s0.skip(si + 1),
{
    let mid = seq![e1] + sp;
    let end0 = si + 1 + sp.len();
    lemma_vals_cat::<E>(s0.take(si) + mid, s0.skip(si + 1));
    lemma_vals_cat::<E>(s0.take(si), mid);
    lemma_vals_cat::<E>(seq![e1], sp);
    lemma_vals_unit::<E>(e1);
    assert forall|i: int| 0 <= i < s1.len() implies #[trigger] E::ok(s1[i]) by {
        if i < si {
            assert(s1[i] == s0[i]);
        } else if i == si {
        } else if i < end0 {
            assert(s1[i] == sp[i - si - 1]);
        } else {
            assert(s1[i] == s0[i - sp.len()]);
        }
    }
    assert(s1.take(si) =~= s0.take(si));
    assert(s1.skip(end0) =~= s0.skip(si + 1));
    let l0 = slab_lens(s0);
    let l1 = slab_lens(s1);
    lemma_sum_split(l1, 0, si, end0);
    lemma_sum_split(l1, 0, end0, l1.len() as int);
    lemma_sum_split(l0, 0, si, si + 1);
    lemma_sum_split(l0, 0, si + 1, l0.len() as int);
    lemma_sum_one(l0, si);
    lemma_sum_agree(l0, l1, si);
    lemma_sum_sub(l1, end0, l1.len() as int);
    lemma_sum_sub(l0, si + 1, l0.len() as int);
    assert(l1.subrange(end0, l1.len() as int) =~= l0.subrange(si + 1, l0.len() as int));
    assert(l1.take(l1.len() as int) =~= l1);
}

// ── The cursor ─────────────────────────────────────────────────────────────

/// A reader trailing the cursor in its slab.
pub(crate) struct Reader<S> {
    pub(crate) state: S,
    pub(crate) at: usize,
}

/// The cursor's state; [`crate::edit::Edit`] pairs it with the column.
pub(crate) struct Cursor<E: Slabs> {
    /// index of the slab the cursor is standing in
    pub(crate) slab: usize,
    /// item offset of the cursor within that slab, in the slab's
    /// *original* (pre-edit) coordinates
    pub(crate) in_slab: usize,
    /// absolute cursor position in the column's *original* coordinates
    pub(crate) orig: usize,
    /// the matching position in the column as edited so far
    pub(crate) out: usize,
    /// the rebuild, reset onto each slab this cursor edits
    pub(crate) edit: E::Rebuild,
    /// whether `edit` is pointed at the current slab
    pub(crate) dirty: bool,
    /// original items after the cursor
    pub(crate) remaining: usize,
    /// slabs this cursor has rebuilt
    pub(crate) rebuilds: usize,
    /// the reader for the current slab, at or before `in_slab`
    pub(crate) dec: Option<Reader<E::State>>,
    /// the column's values when the cursor opened
    #[allow(dead_code)]
    pub(crate) o: Ghost<Seq<E::V>>,
    /// the values the cursor has produced
    #[allow(dead_code)]
    pub(crate) p: Ghost<Seq<E::V>>,
}

impl<E: Slabs> Cursor<E> {
    /// The cursor stands in the slabs `s`: the column's values are what it
    /// has produced, then the original values from `orig` on.
    pub(crate) open spec fn wf(&self, s: Seq<Slab<E::Tail>>) -> bool {
        let si = self.slab as int;
        &&& 1 <= s.len()
        &&& si < s.len()
        &&& self.in_slab <= s[si].len
        &&& self.orig + self.remaining == self.o@.len()
        &&& self.o@.len() <= usize::MAX
        &&& self.out == self.p@.len()
        &&& self.out + self.remaining + 3 <= usize::MAX
        &&& self.p@ == vals_of::<E>(s.take(si)) + if self.dirty {
            E::rb_vals(self.edit)
        } else {
            E::vals(s[si]).take(self.in_slab as int)
        }
        &&& self.o@.skip(self.orig as int) == E::vals(s[si]).skip(self.in_slab as int) + vals_of::<E>(
            s.skip(si + 1),
        )
        &&& self.dirty ==> {
            &&& E::rb_inv(self.edit)
            &&& E::rb_data(self.edit) == s[si].data@
            &&& E::rb_read(self.edit) == self.in_slab
        }
        &&& self.dec is Some ==> {
            &&& E::st_at(s[si].data@, self.dec->Some_0.state, self.dec->Some_0.at as nat)
            &&& self.dec->Some_0.at <= self.in_slab
        }
    }

    /// The column's values with the cursor's edits: what it has produced,
    /// then the original values from `orig` on.
    pub(crate) open spec fn now(&self) -> Seq<E::V> {
        self.p@ + self.o@.skip(self.orig as int)
    }

    /// With values still to come, the cursor stands on one — never at its
    /// slab's end, where a reader has nothing to give.
    pub(crate) open spec fn on_value(&self, s: Seq<Slab<E::Tail>>) -> bool {
        self.remaining > 0 ==> self.in_slab < s[self.slab as int].len
    }

    /// The cursor and the column it stands in.
    pub(crate) open spec fn ok<W, I: IndexOps<W>, G: Weigh<E, W>>(
        &self,
        s: Seq<Slab<E::Tail>>,
        ix: I,
        total_len: usize,
        max_segments: usize,
    ) -> bool {
        col_ok::<E, W, I, G>(s, ix, total_len, max_segments) && self.wf(s)
    }

    /// The same edit, the cursor elsewhere in the same column.
    pub(crate) open spec fn same(&self, c: Self) -> bool {
        self.o == c.o && self.p == c.p && self.orig == c.orig && self.remaining == c.remaining
    }

    /// With no rebuild open, the slabs hold the column's values.
    pub(crate) proof fn lemma_clean(&self, s: Seq<Slab<E::Tail>>)
        requires
            self.wf(s),
            all_ok::<E>(s),
            !self.dirty,
        ensures
            vals_of::<E>(s) == self.now(),
    {
        let si = self.slab as int;
        lemma_vals_at::<E>(s, si);
        let v = E::vals(s[si]);
        E::lemma_len(s[si]);
        assert(v =~= v.take(self.in_slab as int) + v.skip(self.in_slab as int));
        assert(vals_of::<E>(s) =~= self.now());
    }

    /// Open a cursor at item `at` of the column.
    pub(crate) fn new<W, I: IndexOps<W>, G: Weigh<E, W>>(
        slabs: &mut Vec<Slab<E::Tail>>,
        index: &mut I,
        total_len: usize,
        Ghost(max_segments): Ghost<usize>,
        at: usize,
    ) -> (c: Self)
        requires
            col_ok::<E, W, I, G>(old(slabs)@, *old(index), total_len, max_segments),
            at <= total_len,
            total_len + 3 <= usize::MAX,
        ensures
            c.ok::<W, I, G>(final(slabs)@, *final(index), total_len, max_segments),
            vals_of::<E>(final(slabs)@) == vals_of::<E>(old(slabs)@),
            total(slab_lens(final(slabs)@)) == total(slab_lens(old(slabs)@)),
            c.o@ == vals_of::<E>(old(slabs)@),
            c.orig == at,
            c.p@ == c.o@.take(at as int),
            !c.dirty,
            c.on_value(final(slabs)@),
    {
        proof {
            lemma_vals_total::<E>(slabs@);
        }
        if slabs.len() == 0 {
            let empty = E::empty_slab();
            let w = G::compute::<I>(&empty);
            let mut ws = Vec::new();
            ws.push(w);
            proof {
                E::lemma_len(empty);
                assert(wlens::<W, I>(index.ws()).len() == 0);
                let n = index.ws().take(0) + ws@ + index.ws().skip(0);
                assert(n =~= seq![w]);
                assert(wlens::<W, I>(n) =~= seq![0nat]);
                assert(sum(seq![0nat], 0, 0) == 0);
                lemma_sum_one(seq![0nat], 0);
                I::lemma_fits(n);
            }
            index.ix_splice(0, 0, ws);
            slabs.push(empty);
            proof {
                assert(slabs@ =~= seq![empty]);
                lemma_vals_unit::<E>(empty);
                assert(wlens::<W, I>(index.ws()) =~= slab_lens(slabs@));
                assert(slab_lens(slabs@) =~= seq![0nat]);
                lemma_sum_one(seq![0nat], 0);
            }
        }
        let ghost s = slabs@;
        let ghost o = vals_of::<E>(s);
        let n = slabs.len();
        // the front of the column needs no lookup — and `col.edit()`
        // followed by a seek is the common chain, which would otherwise
        // pay for positioning twice
        let (slab, in_slab) = if at == 0 && (total_len == 0 || slabs[0].len > 0) {
            (0, 0)
        } else {
            let (si, off) = index.ix_find(at);
            if si >= n {
                (n - 1, slabs[n - 1].len)
            } else {
                (si, off)
            }
        };
        proof {
            lemma_vals_total::<E>(s);
            let lens = slab_lens(s);
            assert(wlens::<W, I>(index.ws()) == lens);
            if at > 0 && slab == n - 1 && in_slab == s[n - 1].len {
                lemma_sum_split(lens, 0, n - 1, n as int);
                lemma_sum_one(lens, n - 1);
                assert(lens.len() == n);
            }
            lemma_vals_len::<E>(s, slab as int);
            lemma_vals_at::<E>(s, slab as int);
            E::lemma_len(s[slab as int]);
            if at == 0 {
                assert(sum(lens, 0, 0) == 0);
            }
            let v = E::vals(s[slab as int]);
            assert(o.take(at as int) =~= vals_of::<E>(s.take(slab as int)) + v.take(in_slab as int));
            assert(o.skip(at as int) =~= v.skip(in_slab as int) + vals_of::<E>(s.skip(slab + 1)));
        }
        Cursor {
            slab,
            in_slab,
            orig: at,
            out: at,
            edit: E::idle(),
            dirty: false,
            remaining: total_len - at,
            rebuilds: 0,
            dec: None,
            o: Ghost(o),
            p: Ghost(o.take(at as int)),
        }
    }

    /// Point `slab`/`in_slab` at `out`, looked up through the index.  Only
    /// valid with no rebuild open: `out` is a position in the column as it
    /// stands.
    pub(crate) fn locate_out<W, I: IndexOps<W>, G: Weigh<E, W>>(
        &mut self,
        slabs: &Vec<Slab<E::Tail>>,
        index: &I,
        total_len: usize,
        Ghost(max_segments): Ghost<usize>,
    )
        requires
            col_ok::<E, W, I, G>(slabs@, *index, total_len, max_segments),
            slabs@.len() >= 1,
            !old(self).dirty,
            vals_of::<E>(slabs@) == old(self).now(),
            old(self).orig + old(self).remaining == old(self).o@.len(),
            old(self).o@.len() <= usize::MAX,
            old(self).out == old(self).p@.len(),
            old(self).out + old(self).remaining + 3 <= usize::MAX,
        ensures
            final(self).ok::<W, I, G>(slabs@, *index, total_len, max_segments),
            final(self).same(*old(self)),
            final(self).out == old(self).out,
            !final(self).dirty,
            final(self).dec is None,
            final(self).edit == old(self).edit,
            final(self).rebuilds == old(self).rebuilds,
            final(self).remaining > 0 ==> final(self).in_slab < slabs@[final(self).slab as int].len,
    {
        let ghost s = slabs@;
        let ghost lens = slab_lens(s);
        proof {
            lemma_vals_total::<E>(s);
            assert(self.o@.skip(self.orig as int).len() == self.remaining);
        }
        let at_end = self.out >= total_len;
        let n = slabs.len();
        let (si, off) = if at_end {
            (n - 1, slabs[n - 1].len)
        } else {
            index.ix_find(self.out)
        };
        self.slab = if si < n - 1 {
            si
        } else {
            n - 1
        };
        self.in_slab = off;
        self.dec = None;
        proof {
            assert(wlens::<W, I>(index.ws()) == lens);
            if at_end {
                lemma_sum_split(lens, 0, n - 1, n as int);
                lemma_sum_one(lens, n - 1);
            }
            let k = self.slab as int;
            lemma_vals_len::<E>(s, k);
            lemma_vals_at::<E>(s, k);
            E::lemma_len(s[k]);
            let v = E::vals(s[k]);
            let now = self.p@ + self.o@.skip(self.orig as int);
            assert(now.take(self.out as int) =~= self.p@);
            assert(now.skip(self.out as int) =~= self.o@.skip(self.orig as int));
            assert(self.p@ =~= vals_of::<E>(s.take(k)) + v.take(self.in_slab as int));
            assert(self.o@.skip(self.orig as int) =~= v.skip(self.in_slab as int) + vals_of::<E>(
                s.skip(k + 1),
            ));
        }
    }

    /// Move `n` items with no rebuild open.  The slabs in between are
    /// never visited — stepping through them would make a long seek
    /// O(slabs) — so the new position is looked up once.
    ///
    /// A move that stays inside the current slab skips the lookup and
    /// carries the reader along instead.
    pub(crate) fn jump<W, I: IndexOps<W>, G: Weigh<E, W>>(
        &mut self,
        slabs: &Vec<Slab<E::Tail>>,
        index: &I,
        total_len: usize,
        Ghost(max_segments): Ghost<usize>,
        n: usize,
    ) -> (step: usize)
        requires
            old(self).ok::<W, I, G>(slabs@, *index, total_len, max_segments),
            !old(self).dirty,
        ensures
            final(self).ok::<W, I, G>(slabs@, *index, total_len, max_segments),
            !final(self).dirty,
            step == if n <= old(self).remaining {
                n
            } else {
                old(self).remaining
            },
            final(self).o == old(self).o,
            final(self).orig == old(self).orig + step,
            final(self).remaining == old(self).remaining - step,
            final(self).p@ == old(self).p@ + old(self).o@.subrange(
                old(self).orig as int,
                old(self).orig + step,
            ),
            final(self).rebuilds == old(self).rebuilds,
            final(self).on_value(slabs@),
    {
        let step = if n <= self.remaining {
            n
        } else {
            self.remaining
        };
        let ghost c0 = *self;
        let ghost si = self.slab as int;
        let ghost v = E::vals(slabs@[si]);
        proof {
            E::lemma_len(slabs@[si]);
            assert(self.o@.subrange(self.orig as int, self.orig + step) =~= self.o@.skip(
                self.orig as int,
            ).take(step as int));
        }
        // strictly inside: landing exactly on the slab's end would rest
        // the cursor where no value can be read, which `locate_out`
        // resolves by moving on to the next slab
        if step < slabs[self.slab].len - self.in_slab {
            self.in_slab += step;
            self.orig += step;
            self.out += step;
            self.remaining -= step;
            self.p = Ghost(self.p@ + c0.o@.subrange(c0.orig as int, c0.orig + step));
            proof {
                let rest = vals_of::<E>(slabs@.skip(si + 1));
                let tail = v.skip(c0.in_slab as int);
                assert(c0.o@.skip(c0.orig as int) == tail + rest);
                assert((tail + rest).take(step as int) =~= tail.take(step as int));
                assert((tail + rest).skip(step as int) =~= tail.skip(step as int) + rest);
                assert(tail.take(step as int) =~= v.subrange(c0.in_slab as int, self.in_slab as int));
                assert(tail.skip(step as int) =~= v.skip(self.in_slab as int));
                assert(v.take(self.in_slab as int) =~= v.take(c0.in_slab as int) + v.subrange(
                    c0.in_slab as int,
                    self.in_slab as int,
                ));
                assert(self.o@.skip(self.orig as int) =~= c0.o@.skip(c0.orig as int).skip(step as int));
                assert(self.p@ =~= vals_of::<E>(slabs@.take(si)) + v.take(self.in_slab as int));
            }
            return step;
        }
        proof {
            c0.lemma_clean(slabs@);
        }
        self.orig += step;
        self.out += step;
        self.remaining -= step;
        self.p = Ghost(self.p@ + c0.o@.subrange(c0.orig as int, c0.orig + step));
        proof {
            assert(self.now() =~= c0.now());
        }
        self.locate_out::<W, I, G>(slabs, index, total_len, Ghost(max_segments));
        step
    }

    /// The column is too long to edit: a rebuild counts its values in a
    /// `usize`, with room for three more.
    proof fn lemma_room(&self, s: Seq<Slab<E::Tail>>)
        requires
            self.wf(s),
            all_ok::<E>(s),
        ensures
            E::vals(s[self.slab as int]).len() == s[self.slab as int].len,
            s[self.slab as int].len - self.in_slab <= self.remaining,
            self.dirty ==> E::rb_vals(self.edit).len() <= self.out,
    {
        let si = self.slab as int;
        E::lemma_len(s[si]);
        assert(self.o@.skip(self.orig as int).len() == self.remaining);
        lemma_vals_cat::<E>(s.take(si), Seq::empty());
    }

    /// Begin a rebuild of the current slab if one is not already open.
    pub(crate) fn open(&mut self, slabs: &Vec<Slab<E::Tail>>, max_segments: usize)
        requires
            old(self).wf(slabs@),
            all_ok::<E>(slabs@),
            max_segments >= 2,
        ensures
            final(self).wf(slabs@),
            final(self).dirty,
            final(self).same(*old(self)),
            final(self).out == old(self).out,
            final(self).slab == old(self).slab,
            final(self).in_slab == old(self).in_slab,
            final(self).rebuilds == old(self).rebuilds,
    {
        if self.dirty {
            return;
        }
        // the rebuild derives its own reader from the partition it has to
        // walk anyway, so opening costs one pass over the slab
        E::reset(&mut self.edit, &slabs[self.slab], self.in_slab, max_segments);
        self.dirty = true;
    }

    /// Insert `n` copies of `x` at the cursor.
    pub(crate) fn insert<X: Insertable<E>>(&mut self, slabs: &Vec<Slab<E::Tail>>, max_segments: usize, x: X, n: usize)
        requires
            old(self).wf(slabs@),
            all_ok::<E>(slabs@),
            max_segments >= 2,
        ensures
            final(self).wf(slabs@),
            final(self).o == old(self).o,
            final(self).orig == old(self).orig,
            final(self).remaining == old(self).remaining,
            final(self).p@ == old(self).p@ + Seq::new(n as nat, |_i: int| x.val()),
            final(self).slab == old(self).slab,
            final(self).in_slab == old(self).in_slab,
            final(self).rebuilds == old(self).rebuilds,
    {
        let ghost xv = x.val();
        if n == 0 {
            assert(self.p@ + Seq::new(0, |_i: int| xv) =~= self.p@);
            return;
        }
        if n > usize::MAX - 3 - (self.out + self.remaining) {
            too_long(Ghost(self.out + self.remaining + n + 3 > usize::MAX));
        }
        // the test is here rather than only in `open`, so an insert into an
        // open rebuild — every one after the first — makes no call
        if !self.dirty {
            self.open(slabs, max_segments);
        }
        proof {
            self.lemma_room(slabs@);
        }
        x.insert_into(&mut self.edit, n);
        self.out += n;
        self.p = Ghost(self.p@ + Seq::new(n as nat, |_i: int| xv));
        proof {
            let si = self.slab as int;
            assert(self.p@ =~= vals_of::<E>(slabs@.take(si)) + E::rb_vals(self.edit));
        }
    }

    /// Carry `n` items of the current slab through the open rebuild,
    /// returning the ones it declined to carry (see
    /// [`Slabs::carry_budget`]).
    pub(crate) fn step<F: Visitor<E>>(&mut self, slabs: &Vec<Slab<E::Tail>>, n: usize, f: &mut F) -> (left: usize)
        requires
            old(self).wf(slabs@),
            all_ok::<E>(slabs@),
            old(self).dirty,
            old(self).in_slab + n <= slabs@[old(self).slab as int].len,
        ensures
            final(self).wf(slabs@),
            final(self).dirty,
            left <= n,
            final(self).o == old(self).o,
            final(self).orig == old(self).orig + (n - left),
            final(self).remaining == old(self).remaining - (n - left),
            final(self).p@ == old(self).p@ + old(self).o@.subrange(
                old(self).orig as int,
                old(self).orig + (n - left),
            ),
            final(self).slab == old(self).slab,
            final(self).in_slab == old(self).in_slab + (n - left),
            final(self).rebuilds == old(self).rebuilds,
    {
        let ghost c0 = *self;
        if n == 0 {
            assert(self.p@ + self.o@.subrange(self.orig as int, self.orig as int) =~= self.p@);
            return 0;
        }
        proof {
            self.lemma_room(slabs@);
        }
        let budget = E::carry_budget();
        let left = f.pass(&mut self.edit, &slabs[self.slab], n, budget);
        let done = n - left;
        self.in_slab += done;
        self.orig += done;
        self.out += done;
        self.remaining -= done;
        self.p = Ghost(self.p@ + c0.o@.subrange(c0.orig as int, c0.orig + done));
        proof {
            let si = self.slab as int;
            let v = E::vals(slabs@[si]);
            E::lemma_len(slabs@[si]);
            let rest = vals_of::<E>(slabs@.skip(si + 1));
            let tail = v.skip(c0.in_slab as int);
            assert(c0.o@.skip(c0.orig as int) == tail + rest);
            assert(c0.o@.subrange(c0.orig as int, c0.orig + done) =~= (tail + rest).take(done as int));
            assert((tail + rest).take(done as int) =~= v.subrange(c0.in_slab as int, self.in_slab as int));
            assert((tail + rest).skip(done as int) =~= v.skip(self.in_slab as int) + rest);
            assert(self.o@.skip(self.orig as int) =~= c0.o@.skip(c0.orig as int).skip(done as int));
            assert(self.p@ =~= vals_of::<E>(slabs@.take(si)) + E::rb_vals(self.edit));
        }
        left
    }

    /// Bring the index back in step with the slabs: they are the slabs
    /// `s_old` it indexes, but for `[start, end)`.
    fn reconcile<W, I: IndexOps<W>, G: Weigh<E, W>>(
        slabs: &Vec<Slab<E::Tail>>,
        index: &mut I,
        start: usize,
        end: usize,
        len: usize,
        Ghost(s_old): Ghost<Seq<Slab<E::Tail>>>,
    )
        requires
            old(index).ok(),
            I::plain(),
            wlens::<W, I>(old(index).ws()) == slab_lens(s_old),
            ws_ok::<E, W, G>(old(index).ws(), s_old),
            start < end <= slabs@.len(),
            end + s_old.len() - slabs@.len() <= s_old.len(),
            start < end + s_old.len() - slabs@.len(),
            slabs@.take(start as int) == s_old.take(start as int),
            slabs@.skip(end as int) == s_old.skip(end + s_old.len() - slabs@.len()),
            total(slab_lens(slabs@)) <= usize::MAX,
            all_ok::<E>(slabs@),
        ensures
            final(index).ok(),
            wlens::<W, I>(final(index).ws()) == slab_lens(slabs@),
            ws_ok::<E, W, G>(final(index).ws(), slabs@),
    {
        let ghost s = slabs@;
        let ghost ws = index.ws();
        let ghost old_end = end + s_old.len() - s.len();
        proof {
            assert(wlens::<W, I>(ws).len() == ws.len());
            assert(slab_lens(s_old).len() == s_old.len());
            assert(ws.len() == s_old.len());
        }
        if index.ix_len() == slabs.len() && end - start == 1 {
            let w = G::compute::<I>(&slabs[start]);
            proof {
                assert(old_end == end);
                let n = ws.update(start as int, w);
                assert(n.len() == s.len());
                assert forall|i: int| 0 <= i < s.len() implies #[trigger] wlens::<W, I>(n)[i] == slab_lens(s)[i] by {
                    assert(wlens::<W, I>(ws)[i] == I::wlen(ws[i]));
                    assert(slab_lens(s_old)[i] == s_old[i].len);
                    if i < start {
                        assert(s.take(start as int)[i] == s_old.take(start as int)[i]);
                    } else if i > start {
                        assert(s.skip(end as int)[i - end] == s_old.skip(end as int)[i - end]);
                    }
                }
                assert(wlens::<W, I>(n) =~= slab_lens(s));
                assert forall|i: int| 0 <= i < s.len() implies #[trigger] G::wrel(n[i], s[i]) by {
                    if i < start {
                        assert(s.take(start as int)[i] == s_old.take(start as int)[i]);
                    } else if i > start {
                        assert(s.skip(end as int)[i - end] == s_old.skip(end as int)[i - end]);
                    }
                }
                I::lemma_fits(n);
            }
            G::check_set::<I>(index, start, &w, len);
            index.ix_set(start, w);
        } else {
            let mut new: Vec<W> = Vec::with_capacity(end - start);
            let mut k = start;
            while k < end
                invariant
                    start <= k <= end <= s.len(),
                    s == slabs@,
                    all_ok::<E>(s),
                    new@.len() == k - start,
                    forall|i: int| 0 <= i < new@.len() ==> #[trigger] I::wlen(new@[i]) == s[start + i].len,
                    forall|i: int| 0 <= i < new@.len() ==> #[trigger] G::wrel(new@[i], s[start + i]),
                decreases end - k,
            {
                new.push(G::compute::<I>(&slabs[k]));
                k += 1;
            }
            let oe = index.ix_len() - (slabs.len() - end);
            proof {
                assert(oe == old_end);
                let n = ws.take(start as int) + new@ + ws.skip(oe as int);
                assert(n.len() == s.len());
                assert forall|i: int| 0 <= i < s.len() implies #[trigger] wlens::<W, I>(n)[i] == slab_lens(s)[i] by {
                    if i < start {
                        assert(wlens::<W, I>(ws)[i] == I::wlen(ws[i]));
                        assert(slab_lens(s_old)[i] == s_old[i].len);
                        assert(s.take(start as int)[i] == s_old.take(start as int)[i]);
                    } else if i >= end {
                        let j = oe + (i - end);
                        assert(wlens::<W, I>(ws)[j] == I::wlen(ws[j]));
                        assert(slab_lens(s_old)[j] == s_old[j].len);
                        assert(s.skip(end as int)[i - end] == s_old.skip(oe as int)[i - end]);
                    }
                }
                assert(wlens::<W, I>(n) =~= slab_lens(s));
                assert forall|i: int| 0 <= i < s.len() implies #[trigger] G::wrel(n[i], s[i]) by {
                    if i < start {
                        assert(n[i] == ws[i]);
                        assert(s.take(start as int)[i] == s_old.take(start as int)[i]);
                        assert(G::wrel(ws[i], s_old[i]));
                    } else if i >= end {
                        let j = oe + (i - end);
                        assert(n[i] == ws[j]);
                        assert(s.skip(end as int)[i - end] == s_old.skip(oe as int)[i - end]);
                        assert(G::wrel(ws[j], s_old[j]));
                    } else {
                        let k = i - start;
                        assert(n[i] == new@[k]);
                        assert(G::wrel(new@[k], s[start + k]));
                    }
                }
                I::lemma_fits(n);
            }
            G::check_splice::<I>(index, start, oe, &new, len);
            index.ix_splice(start, oe, new);
        }
    }

    /// Finish the open rebuild: write it back, coalesce neighbours and
    /// update the index — each once, however many edits the slab took.
    #[verifier::spinoff_prover]
    #[verifier::rlimit(30)]
    pub(crate) fn close<W, I: IndexOps<W>, G: Weigh<E, W>>(
        &mut self,
        slabs: &mut Vec<Slab<E::Tail>>,
        index: &mut I,
        total_len: &mut usize,
        max_segments: usize,
        counter: &mut usize,
    )
        requires
            old(self).ok::<W, I, G>(old(slabs)@, *old(index), *old(total_len), max_segments),
        ensures
            final(self).ok::<W, I, G>(final(slabs)@, *final(index), *final(total_len), max_segments),
            !final(self).dirty,
            final(self).same(*old(self)),
            final(self).out == old(self).out,
            old(self).dirty && final(self).remaining > 0 ==> final(self).in_slab < final(slabs)@[final(self).slab as int].len,
            !old(self).dirty ==> *final(self) == *old(self) && final(slabs)@ == old(slabs)@ && *final(index) == *old(index) && *final(total_len) == *old(total_len),
    {
        if !self.dirty {
            return;
        }
        let ghost c0 = *self;
        let ghost s0 = slabs@;
        let _ = slabs.len();
        let si = self.slab;
        let removed = slabs[si].len;
        proof {
            self.lemma_room(s0);
            lemma_vals_at::<E>(s0, si as int);
            lemma_vals_total::<E>(s0);
        }
        self.dirty = false;
        self.rebuilds = self.rebuilds.wrapping_add(1);
        // `edit` and the slabs are separate, so the rebuild closes in
        // place against the slab it belongs to — nothing is moved
        let spilled = E::close(&mut self.edit, &mut slabs[si]);
        let ghost sp = spilled@;
        let ghost e1 = slabs@[si as int];
        let spill = spilled.len();
        if spill > 0 {
            crate::index::splice_vec(slabs, si + 1, si + 1, spilled);
        } else {
            proof {
                assert(spilled@ =~= Seq::<Slab<E::Tail>>::empty());
                assert(slabs@ =~= s0.take(si + 1).update(si as int, e1) + spilled@ + s0.skip(si + 1));
            }
        }
        let ghost s1 = slabs@;
        let _ = slabs.len();
        let end0 = si + 1 + spill;
        proof {
            assert(s1 =~= s0.take(si as int) + (seq![e1] + sp) + s0.skip(si + 1));
            lemma_spliced::<E>(s0, s1, si as int, e1, sp);
            assert(vals_of::<E>(s1) =~= c0.now());
            lemma_vals_total::<E>(s1);
            assert(c0.now().len() == c0.out + c0.remaining);
            lemma_sum_le(slab_lens(s1), si as int, end0 as int, 0, s1.len() as int);
        }
        let mut kept: usize = 0;
        let mut k = si;
        while k < end0
            invariant
                si <= k <= end0 <= s1.len(),
                s1 == slabs@,
                kept == sum(slab_lens(s1), si as int, k as int),
                sum(slab_lens(s1), si as int, end0 as int) <= usize::MAX,
            decreases end0 - k,
        {
            proof {
                lemma_sum_split(slab_lens(s1), si as int, k as int, k + 1);
                lemma_sum_one(slab_lens(s1), k as int);
                lemma_sum_split(slab_lens(s1), si as int, k + 1, end0 as int);
            }
            kept += slabs[k].len;
            k += 1;
        }
        *total_len = *total_len - removed + kept;
        let m = SlabMerge::<E> { max_segments, _e: PhantomData };
        let (start, end, _) = merge_around(&m, slabs, si, end0);
        let ghost s2 = slabs@;
        proof {
            let drop = s1.len() - s2.len();
            assert(s2.take(start as int) =~= s0.take(start as int)) by {
                assert(s2.take(start as int) == s1.take(start as int));
                assert(s1.take(start as int) =~= s1.take(si as int).take(start as int));
                assert(s0.take(start as int) =~= s0.take(si as int).take(start as int));
            }
            assert(s2.skip(end as int) =~= s0.skip(end + s0.len() - s2.len())) by {
                assert(s2.skip(end as int) == s1.skip(end + drop));
                assert(s1.skip(end + drop) =~= s1.skip(end0 as int).skip(end + drop - end0));
                assert(s0.skip(end + s0.len() - s2.len()) =~= s0.skip(si + 1).skip(end + drop - end0));
            }
            lemma_vals_total::<E>(s2);
        }
        Self::reconcile::<W, I, G>(slabs, index, start, end, *total_len, Ghost(s0));
        // the write-back can merge, split or drop slabs, so `slab` and
        // `in_slab` mean nothing until they are looked up again
        self.locate_out::<W, I, G>(slabs, index, *total_len, Ghost(max_segments));
        *counter = counter.wrapping_add(1);
    }

    /// Bring the reader to `in_slab`: forward from where it stands, or from
    /// the slab's start if the cursor arrived through the index.  Paid per
    /// read, over the ground since the last one.
    pub(crate) fn load_reader(&mut self, slabs: &Vec<Slab<E::Tail>>)
        requires
            old(self).wf(slabs@),
            all_ok::<E>(slabs@),
        ensures
            final(self).wf(slabs@),
            final(self).dec is Some,
            final(self).dec->Some_0.at == final(self).in_slab,
            final(self).same(*old(self)),
            final(self).out == old(self).out,
            final(self).slab == old(self).slab,
            final(self).in_slab == old(self).in_slab,
            final(self).dirty == old(self).dirty,
            final(self).edit == old(self).edit,
            final(self).rebuilds == old(self).rebuilds,
    {
        let slab = &slabs[self.slab];
        let at = self.in_slab;
        match &mut self.dec {
            Some(r) => {
                if r.at < at {
                    E::state_advance(slab, &mut r.state, at - r.at, Ghost(r.at as nat));
                    r.at = at;
                }
            },
            None => {
                self.dec = Some(Reader { state: E::state_at(slab, at), at });
            },
        }
    }

    /// Standing on a value, the cursor stands on the next original one.
    pub(crate) proof fn lemma_here(&self, s: Seq<Slab<E::Tail>>)
        requires
            self.wf(s),
            all_ok::<E>(s),
            self.in_slab < s[self.slab as int].len,
        ensures
            self.orig < self.o@.len(),
            E::vals(s[self.slab as int])[self.in_slab as int] == self.o@[self.orig as int],
    {
        let si = self.slab as int;
        E::lemma_len(s[si]);
        let v = E::vals(s[si]);
        assert(v.skip(self.in_slab as int).len() > 0);
        assert(self.o@.skip(self.orig as int).len() > 0);
        assert(self.o@.skip(self.orig as int)[0] == (v.skip(self.in_slab as int) + vals_of::<E>(s.skip(si + 1)))[0]);
    }

    /// Runs in the next `n` items, estimated from the current slab's
    /// density.
    fn est_runs(&self, slabs: &Vec<Slab<E::Tail>>, n: usize) -> usize
        requires
            self.slab < slabs@.len(),
    {
        let s = &slabs[self.slab];
        if s.len == 0 {
            return 0;
        }
        n.saturating_mul(s.segments) / s.len
    }

    /// Move forward `n` original items, carrying them; returns how many it
    /// jumped (moved through the index without visiting).
    ///
    /// Inlined into its one caller, `Edit::advance_with`, which then takes
    /// the column whole rather than field by field.
    #[inline(always)]
    pub(crate) fn advance_with<W, I: IndexOps<W>, G: Weigh<E, W>, F: Visitor<E>>(
        &mut self,
        slabs: &mut Vec<Slab<E::Tail>>,
        index: &mut I,
        total_len: &mut usize,
        max_segments: usize,
        counter: &mut usize,
        n: usize,
        f: &mut F,
    ) -> (jumped: usize)
        requires
            old(self).ok::<W, I, G>(old(slabs)@, *old(index), *old(total_len), max_segments),
            old(self).on_value(old(slabs)@),
        ensures
            final(self).ok::<W, I, G>(final(slabs)@, *final(index), *final(total_len), max_segments),
            final(self).on_value(final(slabs)@),
            ({
                let k = if n <= old(self).remaining { n } else { old(self).remaining };
                &&& final(self).o == old(self).o
                &&& final(self).orig == old(self).orig + k
                &&& final(self).p@ == old(self).p@ + old(self).o@.subrange(old(self).orig as int, old(self).orig + k)
                &&& jumped <= k
            }),
    {
        let ghost c0 = *self;
        if self.dirty && self.est_runs(slabs, n) >= E::carry_budget() {
            self.close::<W, I, G>(slabs, index, total_len, max_segments, counter);
        }
        let mut want = n;
        let mut jumped: usize = 0;
        proof {
            assert(c0.o@.subrange(c0.orig as int, c0.orig as int) =~= Seq::<E::V>::empty());
            assert(self.p@ =~= c0.p@ + c0.o@.subrange(c0.orig as int, self.orig as int));
        }
        while want > 0
            invariant_except_break
                self.orig + want == c0.orig + n,
            invariant
                self.ok::<W, I, G>(slabs@, *index, *total_len, max_segments),
                self.on_value(slabs@),
                self.o == c0.o,
                c0.orig <= self.orig,
                self.orig + self.remaining == c0.orig + c0.remaining,
                want <= n,
                jumped <= self.orig - c0.orig,
                self.p@ == c0.p@ + c0.o@.subrange(c0.orig as int, self.orig as int),
            ensures
                self.orig == c0.orig + n || (self.remaining == 0 && self.orig <= c0.orig + n),
            decreases want * 2 + if self.dirty { 1int } else { 0 },
        {
            if !self.dirty {
                let ghost c1 = *self;
                let step = self.jump::<W, I, G>(slabs, index, *total_len, Ghost(max_segments), want);
                jumped += step;
                proof {
                    assert(c0.o@.subrange(c0.orig as int, self.orig as int) =~= c0.o@.subrange(
                        c0.orig as int,
                        c1.orig as int,
                    ) + c0.o@.subrange(c1.orig as int, self.orig as int));
                    assert(self.p@ =~= c0.p@ + c0.o@.subrange(c0.orig as int, self.orig as int));
                }
                break;
            }
            let avail = slabs[self.slab].len - self.in_slab;
            let take = if want < avail {
                want
            } else {
                avail
            };
            let ghost c1 = *self;
            let left = self.step(slabs, take, f);
            proof {
                assert(c0.o@.subrange(c0.orig as int, self.orig as int) =~= c0.o@.subrange(
                    c0.orig as int,
                    c1.orig as int,
                ) + c0.o@.subrange(c1.orig as int, self.orig as int));
                assert(self.p@ =~= c0.p@ + c0.o@.subrange(c0.orig as int, self.orig as int));
            }
            want -= take - left;
            if left > 0 {
                // the carry budget ran out; the loop jumps the rest
                self.close::<W, I, G>(slabs, index, total_len, max_segments, counter);
            } else if take == avail {
                // the slab is done: close it, which re-locates — the
                // write-back can merge, split or drop slabs, so the
                // absolute cursor is the only way back
                self.close::<W, I, G>(slabs, index, total_len, max_segments, counter);
                if self.remaining == 0 {
                    break;
                }
            }
        }
        jumped
    }

    /// Delete `n` original items at the cursor (all there are, if fewer).
    /// A whole slab with no rebuild reading it is dropped outright, and
    /// `g` told its weight; `f` sees every run a rebuild drops.
    #[inline(always)]
    pub(crate) fn delete_with<W, I: IndexOps<W>, G: Weigh<E, W>, F: Visitor<E>, S: WeightSink<W>>(
        &mut self,
        slabs: &mut Vec<Slab<E::Tail>>,
        index: &mut I,
        total_len: &mut usize,
        max_segments: usize,
        counter: &mut usize,
        n: usize,
        f: &mut F,
        g: &mut S,
    )
        requires
            old(self).ok::<W, I, G>(old(slabs)@, *old(index), *old(total_len), max_segments),
            old(self).on_value(old(slabs)@),
        ensures
            final(self).ok::<W, I, G>(final(slabs)@, *final(index), *final(total_len), max_segments),
            final(self).on_value(final(slabs)@),
            final(self).o == old(self).o,
            final(self).p == old(self).p,
            final(self).orig == old(self).orig + if n <= old(self).remaining { n } else { old(self).remaining },
    {
        let ghost c0 = *self;
        // a delete off the end of the column takes what is there
        let mut left = if n <= self.remaining {
            n
        } else {
            self.remaining
        };
        let mut dropped = false;
        while left > 0
            invariant
                self.ok::<W, I, G>(slabs@, *index, *total_len, max_segments),
                self.on_value(slabs@),
                self.o == c0.o,
                self.p == c0.p,
                left <= self.remaining,
                self.orig + left == c0.orig + if n <= c0.remaining { n } else { c0.remaining },
                dropped ==> !self.dirty,
                c0 == *old(self),
            decreases left * 2 + if self.in_slab == slabs@[self.slab as int].len { 1int } else { 0 },
        {
            let avail = slabs[self.slab].len - self.in_slab;
            proof {
                self.lemma_room(slabs@);
            }
            if left < avail {
                self.open(slabs, max_segments);
                let ghost c1 = *self;
                f.discard(&mut self.edit, &slabs[self.slab], left);
                self.took(Ghost(slabs), left, Ghost(c1));
                return;
            }
            // a whole slab, and no rebuild reading it: drop it outright.
            // The column always keeps one slab, so the last one is
            // emptied through the rebuild below instead.
            if self.in_slab == 0 && !self.dirty && slabs.len() > 1 {
                let w = G::compute::<I>(&slabs[self.slab]);
                g.dropped(&w);
                self.drop_slab::<W, I, G>(slabs, index, total_len, Ghost(max_segments));
                left -= avail;
                dropped = true;
                *counter = counter.wrapping_add(1);
                continue;
            }
            // the delete reaches this slab's end: finish it here and go on
            self.open(slabs, max_segments);
            let ghost c1 = *self;
            f.discard(&mut self.edit, &slabs[self.slab], avail);
            self.took(Ghost(slabs), avail, Ghost(c1));
            left -= avail;
            // `close` re-locates, which is what carries the cursor into
            // the next slab
            self.close::<W, I, G>(slabs, index, total_len, max_segments, counter);
        }
        if dropped {
            // dropping slabs can leave undersized neighbours meeting
            let ghost s0 = slabs@;
            proof {
                lemma_vals_total::<E>(s0);
                self.lemma_clean(s0);
                assert(self.now().len() == self.out + self.remaining);
            }
            let _ = slabs.len();
            let m = SlabMerge::<E> { max_segments, _e: PhantomData };
            let (start, end, _) = merge_around(&m, slabs, self.slab, self.slab + 1);
            proof {
                lemma_vals_total::<E>(slabs@);
            }
            Self::reconcile::<W, I, G>(slabs, index, start, end, *total_len, Ghost(s0));
            self.locate_out::<W, I, G>(slabs, index, *total_len, Ghost(max_segments));
            *counter = counter.wrapping_add(1);
        }
    }

    /// Drop the current slab whole: the cursor stands at its start with no
    /// rebuild open, and it is not the column's only slab.
    fn drop_slab<W, I: IndexOps<W>, G: Weigh<E, W>>(
        &mut self,
        slabs: &mut Vec<Slab<E::Tail>>,
        index: &mut I,
        total_len: &mut usize,
        Ghost(max_segments): Ghost<usize>,
    )
        requires
            old(self).ok::<W, I, G>(old(slabs)@, *old(index), *old(total_len), max_segments),
            !old(self).dirty,
            old(self).in_slab == 0,
            old(slabs)@.len() > 1,
        ensures
            final(self).ok::<W, I, G>(final(slabs)@, *final(index), *final(total_len), max_segments),
            !final(self).dirty,
            final(self).o == old(self).o,
            final(self).p == old(self).p,
            final(self).orig == old(self).orig + old(slabs)@[old(self).slab as int].len,
            final(self).remaining == old(self).remaining - old(slabs)@[old(self).slab as int].len,
            final(self).remaining > 0 ==> final(self).in_slab < final(slabs)@[final(self).slab as int].len,
    {
        let ghost c0 = *self;
        let ghost s0 = slabs@;
        let _ = slabs.len();
        let si = self.slab;
        let len = slabs[si].len;
        proof {
            self.lemma_room(s0);
            lemma_vals_at::<E>(s0, si as int);
            lemma_vals_total::<E>(s0);
            let v = E::vals(s0[si as int]);
            assert(v.skip(0) =~= v);
            assert(v.take(0) =~= Seq::<E::V>::empty());
        }
        slabs.remove(si);
        let _ = slabs.len();
        let ghost ws = index.ws();
        proof {
            let n = ws.take(si as int) + Seq::<W>::empty() + ws.skip(si + 1);
            assert(wlens::<W, I>(ws).len() == ws.len());
            assert(wlens::<W, I>(n) =~= slab_lens(slabs@)) by {
                assert forall|i: int| 0 <= i < slabs@.len() implies #[trigger] wlens::<W, I>(n)[i] == slab_lens(slabs@)[i] by {
                    let j = if i < si { i } else { i + 1 };
                    assert(wlens::<W, I>(ws)[j] == I::wlen(ws[j]));
                    assert(slab_lens(s0)[j] == s0[j].len);
                }
            }
            let s1 = slabs@;
            assert(s1 =~= s0.take(si as int) + s0.skip(si + 1));
            lemma_vals_cat::<E>(s0.take(si as int), s0.skip(si + 1));
            assert forall|i: int| 0 <= i < s1.len() implies #[trigger] E::ok(s1[i]) by {
                if i < si {
                    assert(s1[i] == s0[i]);
                } else {
                    assert(s1[i] == s0[i + 1]);
                }
            }
            lemma_vals_total::<E>(s1);
            E::lemma_len(s0[si as int]);
            I::lemma_fits(n);
        }
        index.ix_splice(si, si + 1, Vec::new());
        *total_len -= len;
        self.orig += len;
        self.remaining -= len;
        proof {
            let v = E::vals(s0[si as int]);
            let rest = vals_of::<E>(s0.skip(si + 1));
            assert(c0.o@.skip(c0.orig as int) == v.skip(0) + rest);
            assert(v.skip(0) =~= v);
            assert(self.o@.skip(self.orig as int) =~= c0.o@.skip(c0.orig as int).skip(len as int));
            assert((v + rest).skip(len as int) =~= rest);
            assert(self.p@ =~= vals_of::<E>(s0.take(si as int)) + v.take(0));
            assert(self.p@ =~= vals_of::<E>(s0.take(si as int)));
            assert(self.now() =~= vals_of::<E>(slabs@));
        }
        self.locate_out::<W, I, G>(slabs, index, *total_len, Ghost(max_segments));
    }

    /// Account for `n` items the open rebuild has dropped.
    fn took(&mut self, Ghost(slabs): Ghost<&Vec<Slab<E::Tail>>>, n: usize, Ghost(c): Ghost<Self>)
        requires
            c.wf(slabs@),
            c.dirty,
            all_ok::<E>(slabs@),
            c.in_slab + n <= slabs@[c.slab as int].len,
            old(self).dirty,
            old(self).slab == c.slab,
            old(self).in_slab == c.in_slab,
            old(self).orig == c.orig,
            old(self).out == c.out,
            old(self).remaining == c.remaining,
            old(self).dec == c.dec,
            old(self).o == c.o,
            old(self).p == c.p,
            E::rb_inv(old(self).edit),
            E::rb_data(old(self).edit) == E::rb_data(c.edit),
            E::rb_read(old(self).edit) == c.in_slab + n,
            E::rb_vals(old(self).edit) == E::rb_vals(c.edit),
        ensures
            final(self).wf(slabs@),
            final(self).dirty,
            final(self).slab == c.slab,
            final(self).orig == c.orig + n,
            final(self).o == c.o,
            final(self).p == c.p,
            final(self).out == c.out,
            final(self).in_slab == c.in_slab + n,
            final(self).remaining == c.remaining - n,
            final(self).rebuilds == old(self).rebuilds,
    {
        proof {
            c.lemma_room(slabs@);
        }
        self.in_slab += n;
        self.orig += n;
        self.remaining -= n;
        proof {
            let si = c.slab as int;
            let v = E::vals(slabs@[si]);
            let rest = vals_of::<E>(slabs@.skip(si + 1));
            assert((v.skip(c.in_slab as int) + rest).skip(n as int) =~= v.skip(self.in_slab as int) + rest);
            assert(self.o@.skip(self.orig as int) =~= c.o@.skip(c.orig as int).skip(n as int));
        }
    }
}

/// Runs to insert, from outside: the values a splice writes. Nothing is
/// assumed of the values, only that the feed keeps count of them.
pub(crate) trait RunFeed<E: Slabs> {
    type X: Insertable<E>;

    /// What the feed keeps fixed as it runs.
    type K;

    /// The feed's own invariant, which it keeps from run to run.
    spec fn inv(&self) -> bool;

    /// What stays fixed.
    spec fn key(&self) -> Self::K;

    /// The values given so far, each run as its value repeated.
    spec fn given(&self) -> Seq<E::V>;

    /// The feed has said it has no more.
    spec fn ended(&self) -> bool;

    fn next_run(&mut self) -> (r: Option<(Self::X, usize)>)
        requires
            old(self).inv(),
        ensures
            final(self).inv(),
            final(self).key() == old(self).key(),
            match r {
                Some((x, n)) => final(self).given() == old(self).given() + Seq::new(n as nat, |_i: int| x.val()),
                None => final(self).given() == old(self).given() && final(self).ended(),
            },
    ;
}

/// One splice, as a cursor's steps: open at `at`, delete `del` items,
/// insert the runs `feed` gives until it runs out, and close. The column
/// stays a column, and its values become the original ones before `at`,
/// every value `feed` gives until it ends, then the originals from
/// `at + del`. Ends when `feed` does.
#[verifier::exec_allows_no_decreases_clause]
#[inline(always)]
pub(crate) fn splice_feed<E: Slabs, W, I: IndexOps<W>, G: Weigh<E, W>, F: Visitor<E>, S: WeightSink<W>, R: RunFeed<E>>(
    slabs: &mut Vec<Slab<E::Tail>>,
    index: &mut I,
    total_len: &mut usize,
    max_segments: usize,
    counter: &mut usize,
    at: usize,
    del: usize,
    f: &mut F,
    g: &mut S,
    feed: &mut R,
) -> (ins: Ghost<Seq<E::V>>)
    requires
        col_ok::<E, W, I, G>(old(slabs)@, *old(index), *old(total_len), max_segments),
        at <= *old(total_len),
        del <= *old(total_len) - at,
        *old(total_len) + 3 <= usize::MAX,
        old(feed).inv(),
    ensures
        col_ok::<E, W, I, G>(final(slabs)@, *final(index), *final(total_len), max_segments),
        *final(total_len) + 3 <= usize::MAX,
        vals_of::<E>(final(slabs)@) == vals_of::<E>(old(slabs)@).take(at as int) + ins@
            + vals_of::<E>(old(slabs)@).skip(at + del),
        final(feed).given() == old(feed).given() + ins@,
        final(feed).ended(),
        final(feed).inv(),
        final(feed).key() == old(feed).key(),
{
    let ghost o = vals_of::<E>(slabs@);
    proof {
        lemma_vals_total::<E>(slabs@);
    }
    let mut cur = Cursor::<E>::new::<W, I, G>(slabs, index, *total_len, Ghost(max_segments), at);
    cur.delete_with::<W, I, G, F, S>(slabs, index, total_len, max_segments, counter, del, f, g);
    let ghost mut ins: Seq<E::V> = Seq::empty();
    let ghost g0 = feed.given();
    let ghost k0 = feed.key();
    loop
        invariant
            cur.ok::<W, I, G>(slabs@, *index, *total_len, max_segments),
            cur.o@ == o,
            cur.orig == at + del,
            cur.p@ == o.take(at as int) + ins,
            feed.given() == g0 + ins,
            feed.inv(),
            feed.key() == k0,
        ensures
            cur.ok::<W, I, G>(slabs@, *index, *total_len, max_segments),
            cur.o@ == o,
            cur.orig == at + del,
            cur.p@ == o.take(at as int) + ins,
            feed.given() == g0 + ins,
            feed.ended(),
            feed.inv(),
            feed.key() == k0,
    {
        match feed.next_run() {
            None => break,
            Some((x, n)) => {
                let ghost xv = x.val();
                cur.insert(slabs, max_segments, x, n);
                proof {
                    let run = Seq::new(n as nat, |_i: int| xv);
                    assert(g0 + (ins + run) =~= (g0 + ins) + run);
                    ins = ins + run;
                    assert(cur.p@ =~= o.take(at as int) + ins);
                }
            },
        }
    }
    cur.close::<W, I, G>(slabs, index, total_len, max_segments, counter);
    proof {
        cur.lemma_clean(slabs@);
        lemma_vals_total::<E>(slabs@);
    }
    Ghost(ins)
}

} // verus!

// ── Trusted bridges ────────────────────────────────────────────────────────
//
// Outside `verus!`, so Verus takes them on trust; their spec items exist
// only when Verus compiles the crate.  Each is a one-line forward: a
// closure or a value to the encoding's rebuild (which forwards in turn to
// verified functions with the contracts above), and a slab to its
// weight.

use crate::edit::{EditSlab, SlabEdit};
use crate::encoding::ColumnEncoding;
use crate::{AsColumnRef, ColumnValueRef};

/// A closure seeing each run a cursor carries or drops, as the
/// [`Visitor`] of encoding `E`'s slab model: it hands the closure to the
/// encoding's rebuild.
pub(crate) struct Visiting<'f, E, F>(&'f mut F, PhantomData<fn() -> E>);

impl<'f, E, F> Visiting<'f, E, F> {
    #[inline(always)]
    pub(crate) fn new(f: &'f mut F) -> Self {
        Visiting(f, PhantomData)
    }
}

impl<E: SlabEdit, F> Visitor<<E as SlabEdit>::Model> for Visiting<'_, E, F>
where
    F: FnMut(<<E as ColumnEncoding>::Value as ColumnValueRef>::Get<'_>, usize),
{
    #[inline(always)]
    fn pass(
        &mut self,
        e: &mut <E as SlabEdit>::Edit,
        s: &Slab<<E as ColumnEncoding>::Tail>,
        n: usize,
        runs: usize,
    ) -> usize {
        e.pass(s, n, runs, self.0)
    }

    #[inline(always)]
    fn discard(
        &mut self,
        e: &mut <E as SlabEdit>::Edit,
        s: &Slab<<E as ColumnEncoding>::Tail>,
        n: usize,
    ) {
        e.delete(s, n, self.0)
    }
}

/// A value to insert, as the [`Insertable`] of encoding `E`'s slab model.
pub(crate) struct Inserting<E, X>(X, PhantomData<fn() -> E>);

impl<E, X> Inserting<E, X> {
    #[inline(always)]
    pub(crate) fn new(x: X) -> Self {
        Inserting(x, PhantomData)
    }
}

impl<E: SlabEdit, X: AsColumnRef<<E as ColumnEncoding>::Value>> Insertable<<E as SlabEdit>::Model>
    for Inserting<E, X>
{
    #[cfg(verus_keep_ghost)]
    fn val(&self) -> <<E as SlabEdit>::Model as Slabs>::V {
        unimplemented!()
    }

    #[inline(always)]
    fn insert_into(self, e: &mut <E as SlabEdit>::Edit, n: usize) {
        e.insert(self.0, n)
    }
}

/// The runs a splice inserts, from the caller's iterator, as the
/// [`RunFeed`] of encoding `E`'s slab model.
pub(crate) struct Feeding<E, It>(It, PhantomData<fn() -> E>);

impl<E, It> Feeding<E, It> {
    #[inline(always)]
    pub(crate) fn new(it: It) -> Self {
        Feeding(it, PhantomData)
    }
}

impl<
        E: SlabEdit,
        V: AsColumnRef<<E as ColumnEncoding>::Value>,
        It: Iterator<Item = (V, usize)>,
    > RunFeed<<E as SlabEdit>::Model> for Feeding<E, It>
{
    type X = Inserting<E, V>;

    type K = ();

    #[cfg(verus_keep_ghost)]
    fn inv(&self) -> bool {
        unimplemented!()
    }

    #[cfg(verus_keep_ghost)]
    fn key(&self) {
        unimplemented!()
    }

    #[cfg(verus_keep_ghost)]
    fn given(&self) -> Seq<<<E as SlabEdit>::Model as Slabs>::V> {
        unimplemented!()
    }

    #[cfg(verus_keep_ghost)]
    fn ended(&self) -> bool {
        unimplemented!()
    }

    #[inline(always)]
    fn next_run(&mut self) -> Option<(Inserting<E, V>, usize)> {
        self.0.next().map(|(v, n)| (Inserting::new(v), n))
    }
}

impl<W, G: FnMut(&W)> WeightSink<W> for G {
    #[inline(always)]
    fn dropped(&mut self, w: &W) {
        self(w)
    }
}
