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

    /// A slab is its bytes, counts and tail: another slab with those of a
    /// well-formed slab is well formed, with the same values (a clone).
    proof fn lemma_copy(a: Slab<Self::Tail>, b: Slab<Self::Tail>)
        requires
            Self::ok(a),
            a.same(b),
        ensures
            Self::ok(b),
            Self::vals(b) == Self::vals(a),
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

    /// [`Self::state_advance`] where that is cheap (for bool slabs, inside
    /// the run in hand): whether it did; otherwise `st` is left as it was.
    fn state_skip(s: &Slab<Self::Tail>, st: &mut Self::State, n: usize, pos: Ghost<nat>) -> (r: bool)
        requires
            Self::ok(*s),
            Self::st_at(s.data@, *old(st), pos@),
            pos@ + n <= s.len,
        ensures
            r ==> Self::st_at(s.data@, *final(st), (pos@ + n) as nat),
            !r ==> *final(st) == *old(st),
    ;
}

/// Carrying and dropping a slab's values through a rebuild, reporting
/// each run to the visitor (a delta column keeps its running value so),
/// and seeing each slab a delete drops whole.
pub(crate) trait Visitor<E: Slabs> {
    /// The values the visitor has seen carried or dropped, in order.
    spec fn seen(&self) -> Seq<E::V>;

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
            final(self).seen() == old(self).seen() + E::vals(*s).subrange(
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
            final(self).seen() == old(self).seen() + E::vals(*s).subrange(
                E::rb_read(*old(e)) as int,
                E::rb_read(*old(e)) + n,
            ),
    ;

    /// A delete drops slab `s` whole, unread.
    fn dropped(&mut self, s: &Slab<E::Tail>)
        requires
            E::ok(*s),
        ensures
            final(self).seen() == old(self).seen() + E::vals(*s),
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

    /// Whether the weights `ws` of a column of `len` items add up to a
    /// total the weights can hold (a prefix column's sum fits its prefix
    /// type): a build checks it before its index adds them up. Not part
    /// of the proof: it can only fail.
    fn total_ok(ws: &[W], len: usize) -> bool;

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

/// Item `at` of the slabs' values is item `off` of slab `k`: the values
/// before it are those of the slabs before `k` and slab `k`'s first `off`,
/// and the values from it on are the rest of slab `k`'s and those of the
/// slabs after it.
proof fn lemma_vals_cut<E: Slabs>(s: Seq<Slab<E::Tail>>, k: int, off: int, at: int)
    requires
        all_ok::<E>(s),
        0 <= k < s.len(),
        0 <= off <= s[k].len,
        at == sum(slab_lens(s), 0, k) + off,
    ensures
        vals_of::<E>(s).take(at) == vals_of::<E>(s.take(k)) + E::vals(s[k]).take(off),
        vals_of::<E>(s).skip(at) == E::vals(s[k]).skip(off) + vals_of::<E>(s.skip(k + 1)),
{
    lemma_vals_len::<E>(s, k);
    lemma_vals_at::<E>(s, k);
    E::lemma_len(s[k]);
    let o = vals_of::<E>(s);
    let v = E::vals(s[k]);
    assert(o.take(at) =~= vals_of::<E>(s.take(k)) + v.take(off));
    assert(o.skip(at) =~= v.skip(off) + vals_of::<E>(s.skip(k + 1)));
}

/// A column of no slabs is given its first: an empty slab `e`, of weight
/// `w`.  The index can hold the weight, and the one slab is a column's,
/// with the values and the item total of none.
proof fn lemma_first_slab<E: Slabs, W, I: IndexOps<W>, G: Weigh<E, W>>(
    s0: Seq<Slab<E::Tail>>,
    ws0: Seq<W>,
    wv: Seq<W>,
    e: Slab<E::Tail>,
    w: W,
)
    requires
        I::plain(),
        s0.len() == 0,
        ws0.len() == 0,
        wv == Seq::<W>::empty().push(w),
        E::ok(e),
        E::vals(e) == Seq::<E::V>::empty(),
        I::wlen(w) == e.len,
        G::wrel(w, e),
    ensures
        ({
            let ws = ws0.take(0) + wv + ws0.skip(0);
            let s = s0.push(e);
            &&& I::fits(ws)
            &&& all_ok::<E>(s)
            &&& wlens::<W, I>(ws) == slab_lens(s)
            &&& ws_ok::<E, W, G>(ws, s)
            &&& total(slab_lens(s)) == total(slab_lens(s0))
            &&& vals_of::<E>(s) == vals_of::<E>(s0)
        }),
{
    let ws = ws0.take(0) + wv + ws0.skip(0);
    let s = s0.push(e);
    E::lemma_len(e);
    assert(ws =~= seq![w]);
    assert(s =~= seq![e]);
    assert(s0 =~= Seq::<Slab<E::Tail>>::empty());
    lemma_vals_unit::<E>(e);
    assert(wlens::<W, I>(ws) =~= seq![0nat]);
    assert(slab_lens(s) =~= seq![0nat]);
    assert(sum(seq![0nat], 0, 0) == 0);
    lemma_sum_one(seq![0nat], 0);
    I::lemma_fits(ws);
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
    hide(vals_of);
    hide(all_ok);
    hide(sum);
    let ghost s0 = v@;
    let _ = v.len();
    proof {
        lemma_pair_fits::<E>(s0, a as int);
    }
    let b = v.remove(a + 1);
    E::merge(&mut v[a], b);
    proof {
        assert(v@ =~= s0.remove(a + 1).update(a as int, v@[a as int]));
        lemma_pair_merged::<E>(s0, v@, a as int);
    }
}

/// Slabs `a` and `a + 1` of a column whose items fit a `usize`: both are
/// well-formed, and their items together fit one.
proof fn lemma_pair_fits<E: Slabs>(s0: Seq<Slab<E::Tail>>, a: int)
    requires
        0 <= a,
        a + 1 < s0.len(),
        all_ok::<E>(s0),
        total(slab_lens(s0)) <= usize::MAX,
    ensures
        E::ok(s0[a]),
        E::ok(s0[a + 1]),
        s0[a].len + s0[a + 1].len <= usize::MAX,
{
    hide(sum);
    lemma_sum_split(slab_lens(s0), 0, a, a + 2);
    lemma_sum_split(slab_lens(s0), a, a + 1, a + 2);
    lemma_sum_one(slab_lens(s0), a);
    lemma_sum_one(slab_lens(s0), a + 1);
    lemma_sum_le(slab_lens(s0), 0, a + 2, 0, s0.len() as int);
    assert(sum(slab_lens(s0), 0, s0.len() as int) == total(slab_lens(s0)));
}

/// The slabs `s1` are the slabs `s0` with slab `a + 1` taken out and its
/// values put after slab `a`'s: a column of the same values.
proof fn lemma_pair_merged<E: Slabs>(s0: Seq<Slab<E::Tail>>, s1: Seq<Slab<E::Tail>>, a: int)
    requires
        0 <= a,
        a + 1 < s0.len(),
        all_ok::<E>(s0),
        total(slab_lens(s0)) <= usize::MAX,
        s1 == s0.remove(a + 1).update(a, s1[a]),
        E::ok(s1[a]),
        E::vals(s1[a]) == E::vals(s0[a]) + E::vals(s0[a + 1]),
    ensures
        all_ok::<E>(s1),
        total(slab_lens(s1)) <= usize::MAX,
        vals_of::<E>(s1) == vals_of::<E>(s0),
        s1.len() == s0.len() - 1,
        s1.take(a) == s0.take(a),
        s1.skip(a + 1) == s0.skip(a + 2),
{
    hide(vals_of);
    hide(sum);
    assert(s1.take(a) =~= s0.take(a));
    assert(s1.skip(a + 1) =~= s0.skip(a + 2));
    lemma_vals_at::<E>(s0, a);
    lemma_vals_at::<E>(s0.skip(a + 1), 0);
    assert(s0.skip(a + 1).take(0) =~= Seq::<Slab<E::Tail>>::empty());
    assert(s0.skip(a + 1).skip(1) =~= s0.skip(a + 2));
    lemma_vals_unit::<E>(s0[a]);
    lemma_vals_at::<E>(s1, a);
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

/// One more whole slab to drop: slabs `i..j + 1` hold the values of
/// slabs `i..j` and then slab `j`'s, which are as many as it counts.
proof fn lemma_drop_more<E: Slabs>(s: Seq<Slab<E::Tail>>, i: int, j: int, seen: Seq<E::V>)
    requires
        all_ok::<E>(s),
        0 <= i <= j < s.len(),
    ensures
        E::ok(s[j]),
        vals_of::<E>(s.subrange(i, j + 1)) == vals_of::<E>(s.subrange(i, j)) + E::vals(s[j]),
        vals_of::<E>(s.subrange(i, j + 1)).len() == vals_of::<E>(s.subrange(i, j)).len() + s[j].len,
        seen + vals_of::<E>(s.subrange(i, j)) + E::vals(s[j]) == seen + vals_of::<E>(s.subrange(i, j + 1)),
{
    assert(s.subrange(i, j + 1) =~= s.subrange(i, j) + seq![s[j]]);
    lemma_vals_cat::<E>(s.subrange(i, j), seq![s[j]]);
    lemma_vals_unit::<E>(s[j]);
    E::lemma_len(s[j]);
    assert(seen + vals_of::<E>(s.subrange(i, j)) + E::vals(s[j]) =~= seen + vals_of::<E>(s.subrange(i, j + 1)));
}

/// Slabs `i..j` among the others: the slabs hold the values before
/// theirs, theirs and the values after; the slabs from `i` on hold
/// theirs and the values after; and without them, the slabs hold the
/// values before and after.
proof fn lemma_vals_three<E: Slabs>(s: Seq<Slab<E::Tail>>, i: int, j: int)
    requires
        0 <= i <= j <= s.len(),
    ensures
        vals_of::<E>(s) == vals_of::<E>(s.take(i)) + vals_of::<E>(s.subrange(i, j)) + vals_of::<E>(s.skip(j)),
        vals_of::<E>(s.skip(i)) == vals_of::<E>(s.subrange(i, j)) + vals_of::<E>(s.skip(j)),
        vals_of::<E>(s.take(i) + s.skip(j)) == vals_of::<E>(s.take(i)) + vals_of::<E>(s.skip(j)),
{
    let t = s.skip(i);
    lemma_vals_split::<E>(s, i);
    lemma_vals_split::<E>(t, j - i);
    assert(t.take(j - i) =~= s.subrange(i, j));
    assert(t.skip(j - i) =~= s.skip(j));
    lemma_vals_cat::<E>(s.take(i), s.skip(j));
    assert(vals_of::<E>(s) =~= vals_of::<E>(s.take(i)) + vals_of::<E>(s.subrange(i, j)) + vals_of::<E>(s.skip(j)));
}

/// The slabs from `i` on hold slab `i`'s values and then those of the
/// slabs after it.
proof fn lemma_vals_head<E: Slabs>(s: Seq<Slab<E::Tail>>, i: int)
    requires
        0 <= i < s.len(),
    ensures
        vals_of::<E>(s.skip(i)) == E::vals(s[i]) + vals_of::<E>(s.skip(i + 1)),
{
    let t = s.skip(i);
    lemma_vals_split::<E>(t, 1);
    assert(t.take(1) =~= seq![s[i]]);
    assert(t.skip(1) =~= s.skip(i + 1));
    lemma_vals_unit::<E>(s[i]);
}

/// A column without its slabs `i..j` is a column: the slabs left are
/// well-formed, the weights left are theirs and count their items, an
/// index can hold them, and the items are fewer by those that went.
proof fn lemma_cut<E: Slabs, W, I: IndexOps<W>, G: Weigh<E, W>>(
    s: Seq<Slab<E::Tail>>,
    ws: Seq<W>,
    i: int,
    j: int,
)
    requires
        all_ok::<E>(s),
        wlens::<W, I>(ws) == slab_lens(s),
        ws_ok::<E, W, G>(ws, s),
        I::plain(),
        total(slab_lens(s)) <= usize::MAX,
        s.len() <= usize::MAX,
        0 <= i <= j <= s.len(),
    ensures
        ({
            let s1 = s.take(i) + s.skip(j);
            let w1 = ws.take(i) + Seq::<W>::empty() + ws.skip(j);
            &&& all_ok::<E>(s1)
            &&& wlens::<W, I>(w1) == slab_lens(s1)
            &&& ws_ok::<E, W, G>(w1, s1)
            &&& I::fits(w1)
            &&& vals_of::<E>(s.subrange(i, j)).len() <= total(slab_lens(s))
            &&& total(slab_lens(s1)) == total(slab_lens(s)) - vals_of::<E>(s.subrange(i, j)).len()
            &&& vals_of::<E>(s1) == vals_of::<E>(s.take(i)) + vals_of::<E>(s.skip(j))
        }),
{
    let k = j - i;
    let s1 = s.take(i) + s.skip(j);
    let w1 = ws.take(i) + Seq::<W>::empty() + ws.skip(j);
    assert(wlens::<W, I>(ws).len() == ws.len());
    assert(wlens::<W, I>(w1) =~= slab_lens(s1)) by {
        assert forall|x: int| 0 <= x < s1.len() implies #[trigger] wlens::<W, I>(w1)[x] == slab_lens(s1)[x] by {
            let y = if x < i { x } else { x + k };
            assert(wlens::<W, I>(ws)[y] == I::wlen(ws[y]));
            assert(slab_lens(s)[y] == s[y].len);
        }
    }
    assert forall|x: int| 0 <= x < s1.len() implies #[trigger] E::ok(s1[x]) by {
        if x < i {
            assert(s1[x] == s[x]);
        } else {
            assert(s1[x] == s[x + k]);
        }
    }
    assert forall|x: int| 0 <= x < s1.len() implies #[trigger] G::wrel(w1[x], s1[x]) by {
        let y = if x < i { x } else { x + k };
        assert(G::wrel(ws[y], s[y]));
    }
    lemma_vals_three::<E>(s, i, j);
    lemma_vals_total::<E>(s);
    lemma_vals_total::<E>(s1);
    assert(w1.len() == s1.len() <= s.len());
    I::lemma_fits(w1);
}

/// The slabs from `si` on, short of slab `end`, that are whole in the
/// next `left` items: `f` sees each as one a delete drops.  Returns the
/// slab after the last of them and the items they hold together.
fn whole_slabs<E: Slabs, F: Visitor<E>>(
    slabs: &Vec<Slab<E::Tail>>,
    si: usize,
    end: usize,
    left: usize,
    f: &mut F,
) -> (r: (usize, usize))
    requires
        all_ok::<E>(slabs@),
        si <= end <= slabs@.len(),
    ensures
        si <= r.0 <= end,
        r.1 <= left,
        r.1 == vals_of::<E>(slabs@.subrange(si as int, r.0 as int)).len(),
        final(f).seen() == old(f).seen() + vals_of::<E>(slabs@.subrange(si as int, r.0 as int)),
        si < end && slabs@[si as int].len <= left ==> si < r.0 && slabs@[si as int].len <= r.1,
{
    let mut sj = si;
    let mut gone: usize = 0;
    proof {
        assert(slabs@.subrange(si as int, si as int) =~= Seq::<Slab<E::Tail>>::empty());
        lemma_vals_unit::<E>(slabs@[0]);
        assert(f.seen() + Seq::<E::V>::empty() =~= f.seen());
    }
    while sj < end && slabs[sj].len <= left - gone
        invariant
            all_ok::<E>(slabs@),
            si <= sj <= end <= slabs@.len(),
            gone <= left,
            sj == si ==> gone == 0,
            sj > si ==> gone >= slabs@[si as int].len,
            gone == vals_of::<E>(slabs@.subrange(si as int, sj as int)).len(),
            f.seen() == old(f).seen() + vals_of::<E>(slabs@.subrange(si as int, sj as int)),
        decreases end - sj,
    {
        proof {
            lemma_drop_more::<E>(slabs@, si as int, sj as int, old(f).seen());
        }
        f.dropped(&slabs[sj]);
        gone += slabs[sj].len;
        sj += 1;
    }
    (sj, gone)
}

/// Two runs of `o` that meet, after `seen`, are one run after it.
proof fn lemma_run_joined<V>(seen: Seq<V>, o: Seq<V>, a: int, b: int, c: int)
    requires
        0 <= a <= b <= c <= o.len(),
    ensures
        seen + o.subrange(a, b) + o.subrange(b, c) == seen + o.subrange(a, c),
{
    assert(seen + o.subrange(a, b) + o.subrange(b, c) =~= seen + o.subrange(a, c));
}

/// A rebuild's write-back, `s0` to `s1`, and then a merge walk around the
/// slabs written, `s1` to `s2`: the slabs before `start` and from `end` on
/// are still those of `s0`, the latter moved by what the write-back added
/// and the walk merged away.
proof fn lemma_walked<T>(s0: Seq<T>, s1: Seq<T>, s2: Seq<T>, si: int, end0: int, start: int, end: int)
    requires
        0 <= si < s0.len(),
        s1.len() >= s0.len(),
        end0 == si + 1 + (s1.len() - s0.len()),
        s1.take(si) == s0.take(si),
        s1.skip(end0) == s0.skip(si + 1),
        start <= si,
        framed(s2, s1, start, end, end0),
    ensures
        start < end <= s2.len(),
        end + s0.len() - s2.len() <= s0.len(),
        start < end + s0.len() - s2.len(),
        s2.take(start) == s0.take(start),
        s2.skip(end) == s0.skip(end + s0.len() - s2.len()),
{
    let drop = s1.len() - s2.len();
    assert(s2.take(start) =~= s0.take(start)) by {
        assert(s1.take(start) =~= s1.take(si).take(start));
        assert(s0.take(start) =~= s0.take(si).take(start));
    }
    assert(s2.skip(end) =~= s0.skip(end + s0.len() - s2.len())) by {
        assert(s1.skip(end + drop) =~= s1.skip(end0).skip(end + drop - end0));
        assert(s0.skip(end + s0.len() - s2.len()) =~= s0.skip(si + 1).skip(end + drop - end0));
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

    /// The next `n` original values are the slab's from the cursor on.
    pub(crate) proof fn lemma_ahead(&self, s: Seq<Slab<E::Tail>>, n: int)
        requires
            self.wf(s),
            all_ok::<E>(s),
            0 <= n,
            self.in_slab + n <= s[self.slab as int].len,
        ensures
            self.o@.subrange(self.orig as int, self.orig + n) == E::vals(s[self.slab as int]).subrange(
                self.in_slab as int,
                self.in_slab + n,
            ),
    {
        let si = self.slab as int;
        let v = E::vals(s[si]);
        E::lemma_len(s[si]);
        let rest = vals_of::<E>(s.skip(si + 1));
        let tail = v.skip(self.in_slab as int);
        assert(self.o@.skip(self.orig as int) == tail + rest);
        assert(self.o@.subrange(self.orig as int, self.orig + n) =~= (tail + rest).take(n));
        assert((tail + rest).take(n) =~= v.subrange(self.in_slab as int, self.in_slab + n));
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
    /// A cursor over nothing, to stand in for one whose edit has ended
    /// (`EditCore::spent`).
    pub(crate) fn spent() -> Self {
        Cursor {
            slab: 0,
            in_slab: 0,
            orig: 0,
            out: 0,
            edit: E::idle(),
            dirty: false,
            remaining: 0,
            rebuilds: 0,
            dec: None,
            o: Ghost(Seq::empty()),
            p: Ghost(Seq::empty()),
        }
    }

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
        // the slabs' values are reasoned about through lemmas alone
        hide(vals_of);
        if slabs.len() == 0 {
            let empty = E::empty_slab();
            let w = G::compute::<I>(&empty);
            let mut ws = Vec::new();
            ws.push(w);
            proof {
                lemma_first_slab::<E, W, I, G>(slabs@, index.ws(), ws@, empty, w);
            }
            index.ix_splice(0, 0, ws);
            slabs.push(empty);
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
            if at > 0 && slab == n - 1 && in_slab == s[n - 1].len {
                lemma_sum_split(lens, 0, n - 1, n as int);
                lemma_sum_one(lens, n - 1);
            }
            if at == 0 {
                assert(sum(lens, 0, 0) == 0);
            }
            lemma_vals_cut::<E>(s, slab as int, in_slab as int, at as int);
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
        hide(Cursor::wf);
        hide(Cursor::now);
        hide(vals_of);
        hide(all_ok);
        hide(ws_ok);
        hide(sum);
        let ghost s = slabs@;
        let ghost c0 = *self;
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
            assert(wlens::<W, I>(index.ws()) == slab_lens(s));
            c0.lemma_located(*self, s, total_len, (si, off));
        }
    }

    /// Where `locate_out` leaves the cursor, `after`, given `r`: the last
    /// slab's end if `out` is the column's, and else the index's answer
    /// for item `out`.
    proof fn lemma_located(&self, after: Self, s: Seq<Slab<E::Tail>>, total_len: usize, r: (usize, usize))
        requires
            all_ok::<E>(s),
            s.len() >= 1,
            total_len == total(slab_lens(s)),
            !self.dirty,
            vals_of::<E>(s) == self.now(),
            self.orig + self.remaining == self.o@.len(),
            self.o@.len() <= usize::MAX,
            self.out == self.p@.len(),
            self.out + self.remaining + 3 <= usize::MAX,
            self.out >= total_len ==> r.0 == s.len() - 1 && r.1 == s[s.len() - 1].len,
            self.out < total_len ==> {
                &&& r.0 <= s.len()
                &&& r.0 < s.len() ==> sum(slab_lens(s), 0, r.0 as int) + r.1 == self.out && r.1 < slab_lens(
                    s,
                )[r.0 as int]
                &&& r.0 == s.len() ==> self.out >= total(slab_lens(s))
            },
            after.slab == if r.0 < s.len() - 1 {
                r.0
            } else {
                (s.len() - 1) as usize
            },
            after.in_slab == r.1,
            after.dec is None,
            after.orig == self.orig,
            after.remaining == self.remaining,
            after.out == self.out,
            after.o == self.o,
            after.p == self.p,
            after.dirty == self.dirty,
        ensures
            after.wf(s),
            after.remaining > 0 ==> after.in_slab < s[after.slab as int].len,
    {
        hide(vals_of);
        hide(sum);
        let lens = slab_lens(s);
        let n = s.len() as int;
        let k = after.slab as int;
        let rest = self.o@.skip(self.orig as int);
        lemma_vals_total::<E>(s);
        assert(rest.len() == self.remaining);
        assert(sum(lens, 0, n) == total(lens));
        if self.out >= total_len {
            lemma_sum_split(lens, 0, n - 1, n);
            lemma_sum_one(lens, n - 1);
        }
        lemma_vals_cut::<E>(s, k, after.in_slab as int, self.out as int);
        assert((self.p@ + rest).take(self.out as int) =~= self.p@);
        assert((self.p@ + rest).skip(self.out as int) =~= rest);
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
        hide(Cursor::wf);
        hide(Cursor::now);
        hide(vals_of);
        let step = if n <= self.remaining {
            n
        } else {
            self.remaining
        };
        let ghost c0 = *self;
        proof {
            c0.lemma_stands_in(slabs@);
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
                c0.lemma_moved_in(*self, slabs@, step as int);
            }
            return step;
        }
        self.orig += step;
        self.out += step;
        self.remaining -= step;
        self.p = Ghost(self.p@ + c0.o@.subrange(c0.orig as int, c0.orig + step));
        proof {
            c0.lemma_moved_on(*self, slabs@, step as int);
        }
        self.locate_out::<W, I, G>(slabs, index, total_len, Ghost(max_segments));
        step
    }

    /// What a move needs of a cursor that stands in the slabs: its slab is
    /// one of them, and its counts have room.
    proof fn lemma_stands_in(&self, s: Seq<Slab<E::Tail>>)
        requires
            self.wf(s),
        ensures
            1 <= s.len(),
            self.slab < s.len(),
            self.in_slab <= s[self.slab as int].len,
            self.orig + self.remaining == self.o@.len(),
            self.o@.len() <= usize::MAX,
            self.out == self.p@.len(),
            self.out + self.remaining + 3 <= usize::MAX,
    {
    }

    /// A move of `step` that stays inside the slab, short of its end,
    /// leaves the cursor `after` standing in the slabs.
    proof fn lemma_moved_in(&self, after: Self, s: Seq<Slab<E::Tail>>, step: int)
        requires
            self.wf(s),
            all_ok::<E>(s),
            !self.dirty,
            0 <= step <= self.remaining,
            step < s[self.slab as int].len - self.in_slab,
            after.slab == self.slab,
            after.in_slab == self.in_slab + step,
            after.orig == self.orig + step,
            after.out == self.out + step,
            after.remaining == self.remaining - step,
            after.p@ == self.p@ + self.o@.subrange(self.orig as int, self.orig + step),
            after.o == self.o,
            after.dirty == self.dirty,
            after.dec == self.dec,
        ensures
            after.wf(s),
    {
        hide(vals_of);
        let si = self.slab as int;
        let v = E::vals(s[si]);
        E::lemma_len(s[si]);
        let rest = vals_of::<E>(s.skip(si + 1));
        let tail = v.skip(self.in_slab as int);
        let i1 = self.in_slab + step;
        assert(self.o@.skip(self.orig as int) == tail + rest);
        assert(self.o@.subrange(self.orig as int, self.orig + step) =~= (tail + rest).take(step));
        assert((tail + rest).take(step) =~= v.subrange(self.in_slab as int, i1));
        assert(after.o@.skip(after.orig as int) =~= (tail + rest).skip(step));
        assert((tail + rest).skip(step) =~= v.skip(i1) + rest);
        assert(v.take(i1) =~= v.take(self.in_slab as int) + v.subrange(self.in_slab as int, i1));
        assert(after.p@ =~= vals_of::<E>(s.take(si)) + v.take(i1));
    }

    /// A move of `step` with no rebuild open, before the slab it ends in
    /// is looked up: the slabs still hold the column's values as the
    /// cursor `after` has them.
    proof fn lemma_moved_on(&self, after: Self, s: Seq<Slab<E::Tail>>, step: int)
        requires
            self.wf(s),
            all_ok::<E>(s),
            !self.dirty,
            0 <= step <= self.remaining,
            after.orig == self.orig + step,
            after.p@ == self.p@ + self.o@.subrange(self.orig as int, self.orig + step),
            after.o == self.o,
        ensures
            vals_of::<E>(s) == after.now(),
    {
        hide(vals_of);
        self.lemma_clean(s);
        assert(after.now() =~= self.now());
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
            final(f).seen() == old(f).seen() + old(self).o@.subrange(
                old(self).orig as int,
                old(self).orig + (n - left),
            ),
    {
        let ghost c0 = *self;
        if n == 0 {
            assert(self.p@ + self.o@.subrange(self.orig as int, self.orig as int) =~= self.p@);
            assert(f.seen() + self.o@.subrange(self.orig as int, self.orig as int) =~= f.seen());
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
        // what the cursor and the slabs hold is reasoned about through
        // lemmas alone: unfolding them here only gives the solver room to
        // wander
        hide(Cursor::wf);
        hide(vals_of);
        if !self.dirty {
            return;
        }
        let ghost c0 = *self;
        let ghost s0 = slabs@;
        let _ = slabs.len();
        let si = self.slab;
        proof {
            c0.lemma_closing(s0);
        }
        let removed = slabs[si].len;
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
                assert(slabs@ == s0.update(si as int, e1));
            }
        }
        let ghost s1 = slabs@;
        let _ = slabs.len();
        let end0 = si + 1 + spill;
        proof {
            c0.lemma_written_back(s0, s1, e1, sp);
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
            lemma_walked(s0, s1, s2, si as int, end0 as int, start as int, end as int);
            lemma_vals_total::<E>(s1);
            lemma_vals_total::<E>(s2);
        }
        Self::reconcile::<W, I, G>(slabs, index, start, end, *total_len, Ghost(s0));
        // the write-back can merge, split or drop slabs, so `slab` and
        // `in_slab` mean nothing until they are looked up again
        self.locate_out::<W, I, G>(slabs, index, *total_len, Ghost(max_segments));
        *counter = counter.wrapping_add(1);
    }

    /// Where a cursor stands and what it counts.
    proof fn lemma_stands(&self, s: Seq<Slab<E::Tail>>)
        requires
            self.wf(s),
        ensures
            self.slab < s.len(),
            self.in_slab <= s[self.slab as int].len,
            self.orig + self.remaining == self.o@.len(),
            self.o@.len() <= usize::MAX,
            self.out == self.p@.len(),
            self.out + self.remaining + 3 <= usize::MAX,
    {
    }

    /// What closing a cursor's open rebuild rests on: the rebuild reads
    /// the cursor's slab, and what it has written and has yet to read
    /// leave room for three more values.
    proof fn lemma_closing(&self, s: Seq<Slab<E::Tail>>)
        requires
            self.wf(s),
            all_ok::<E>(s),
            self.dirty,
        ensures
            self.slab < s.len(),
            E::ok(s[self.slab as int]),
            E::rb_inv(self.edit),
            E::rb_data(self.edit) == s[self.slab as int].data@,
            E::rb_read(self.edit) == self.in_slab,
            self.in_slab <= s[self.slab as int].len,
            E::rb_vals(self.edit).len() + (s[self.slab as int].len - self.in_slab) + 3 <= usize::MAX,
            self.orig + self.remaining == self.o@.len(),
            self.o@.len() <= usize::MAX,
            self.out == self.p@.len(),
            self.out + self.remaining + 3 <= usize::MAX,
    {
        self.lemma_room(s);
    }

    /// The open rebuild written back: the cursor's slab in `s0` became
    /// `e1` and the slabs it spilled into, `sp`, which hold what the
    /// rebuild wrote and then the slab's values from the cursor on.  The
    /// slabs `s1` this leaves hold the column's values as edited; they are
    /// the old slabs but for the ones written, whose item counts take the
    /// old slab's place in the total.
    proof fn lemma_written_back(
        &self,
        s0: Seq<Slab<E::Tail>>,
        s1: Seq<Slab<E::Tail>>,
        e1: Slab<E::Tail>,
        sp: Seq<Slab<E::Tail>>,
    )
        requires
            self.wf(s0),
            all_ok::<E>(s0),
            self.dirty,
            E::ok(e1),
            forall|i: int| 0 <= i < sp.len() ==> #[trigger] E::ok(sp[i]),
            E::vals(e1) + concat(sp.map_values(|x: Slab<E::Tail>| E::vals(x))) == E::rb_vals(self.edit) + E::vals(
                s0[self.slab as int],
            ).skip(self.in_slab as int),
            ({
                let m = s0.update(self.slab as int, e1);
                if sp.len() > 0 {
                    s1 == m.take(self.slab + 1) + sp + m.skip(self.slab + 1)
                } else {
                    s1 == m
                }
            }),
        ensures
            ({
                let si = self.slab as int;
                let end0 = si + 1 + sp.len();
                let (l0, l1) = (slab_lens(s0), slab_lens(s1));
                &&& s1.len() == s0.len() + sp.len()
                &&& all_ok::<E>(s1)
                &&& vals_of::<E>(s1) == self.now()
                &&& total(l1) == self.out + self.remaining
                &&& s0[si].len <= total(l0)
                &&& total(l0) - s0[si].len + sum(l1, si, end0) == total(l1)
                &&& s1.take(si) == s0.take(si)
                &&& s1.skip(end0) == s0.skip(si + 1)
            }),
    {
        let si = self.slab as int;
        let end0 = si + 1 + sp.len();
        assert(s1 =~= s0.take(si) + (seq![e1] + sp) + s0.skip(si + 1));
        lemma_spliced::<E>(s0, s1, si, e1, sp);
        assert(vals_of::<E>(s1) =~= self.now());
        lemma_vals_total::<E>(s1);
        assert(self.now().len() == self.out + self.remaining);
        lemma_sum_le(slab_lens(s1), si, end0, 0, s1.len() as int);
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
    /// A move that stays inside the current slab with no rebuild open only
    /// counts, and is done first, without the rest
    /// ([`Self::advance_long`]): its caller can take that case inline and
    /// leave the rest out of line (`EditCore::advance_with`).
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
                // what the visitor saw: all but the jump, which is last
                &&& final(f).seen() == old(f).seen() + old(self).o@.subrange(
                    old(self).orig as int,
                    old(self).orig + k - jumped,
                )
                // a jump is the last step, and leaves no rebuild open
                &&& jumped > 0 ==> !final(self).dirty
            }),
    {
        if !self.dirty && n < slabs[self.slab].len - self.in_slab {
            let ghost c0 = *self;
            let ghost f0 = f.seen();
            let step = self.jump::<W, I, G>(slabs, index, *total_len, Ghost(max_segments), n);
            proof {
                self.lemma_room(slabs@);
                c0.lemma_room(slabs@);
                assert(c0.o@.subrange(c0.orig as int, c0.orig + step - step) =~= Seq::<E::V>::empty());
                assert(f0 + Seq::<E::V>::empty() =~= f0);
            }
            return step;
        }
        self.advance_long::<W, I, G, F>(slabs, index, total_len, max_segments, counter, n, f)
    }

    /// [`Self::advance_with`], for any move.
    #[inline(always)]
    fn advance_long<W, I: IndexOps<W>, G: Weigh<E, W>, F: Visitor<E>>(
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
                // what the visitor saw: all but the jump, which is last
                &&& final(f).seen() == old(f).seen() + old(self).o@.subrange(
                    old(self).orig as int,
                    old(self).orig + k - jumped,
                )
                // a jump is the last step, and leaves no rebuild open
                &&& jumped > 0 ==> !final(self).dirty
            }),
    {
        let ghost c0 = *self;
        proof {
            assert(f.seen() + c0.o@.subrange(c0.orig as int, c0.orig as int) =~= f.seen());
        }
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
                jumped == 0,
                f.seen() == old(f).seen() + c0.o@.subrange(c0.orig as int, self.orig as int),
            invariant
                self.ok::<W, I, G>(slabs@, *index, *total_len, max_segments),
                self.on_value(slabs@),
                self.o == c0.o,
                c0.orig <= self.orig,
                self.orig + self.remaining == c0.orig + c0.remaining,
                want <= n,
                jumped <= self.orig - c0.orig,
                self.p@ == c0.p@ + c0.o@.subrange(c0.orig as int, self.orig as int),
                f.seen() == old(f).seen() + c0.o@.subrange(c0.orig as int, self.orig - jumped),
                jumped > 0 ==> !self.dirty,
            ensures
                self.orig == c0.orig + n || (self.remaining == 0 && self.orig <= c0.orig + n),
                jumped <= self.orig - c0.orig,
                f.seen() == old(f).seen() + c0.o@.subrange(c0.orig as int, self.orig - jumped),
                jumped > 0 ==> !self.dirty,
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
            let ghost f1 = f.seen();
            let left = self.step(slabs, take, f);
            proof {
                assert(c0.o@.subrange(c0.orig as int, self.orig as int) =~= c0.o@.subrange(
                    c0.orig as int,
                    c1.orig as int,
                ) + c0.o@.subrange(c1.orig as int, self.orig as int));
                assert(self.p@ =~= c0.p@ + c0.o@.subrange(c0.orig as int, self.orig as int));
                assert(f.seen() =~= old(f).seen() + c0.o@.subrange(c0.orig as int, self.orig as int));
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
    pub(crate) fn delete_with<W, I: IndexOps<W>, G: Weigh<E, W>, F: Visitor<E>>(
        &mut self,
        slabs: &mut Vec<Slab<E::Tail>>,
        index: &mut I,
        total_len: &mut usize,
        max_segments: usize,
        counter: &mut usize,
        n: usize,
        f: &mut F,
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
            final(f).seen() == old(f).seen() + old(self).o@.subrange(old(self).orig as int, final(self).orig as int),
    {
        // what the cursor and the slabs hold is reasoned about through
        // lemmas alone: unfolding them here only gives the solver room to
        // wander
        hide(Cursor::wf);
        hide(vals_of);
        let ghost c0 = *self;
        proof {
            c0.lemma_stands(slabs@);
            assert(f.seen() + c0.o@.subrange(c0.orig as int, c0.orig as int) =~= f.seen());
        }
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
                c0.orig <= self.orig,
                f.seen() == old(f).seen() + c0.o@.subrange(c0.orig as int, self.orig as int),
            decreases left * 2 + if self.in_slab == slabs@[self.slab as int].len { 1int } else { 0 },
        {
            proof {
                self.lemma_stands(slabs@);
                self.lemma_room(slabs@);
            }
            let avail = slabs[self.slab].len - self.in_slab;
            if left < avail {
                // tested here rather than only in `open`, so a delete in a
                // rebuild already open makes no call, as `insert`
                if !self.dirty {
                    self.open(slabs, max_segments);
                }
                let ghost c1 = *self;
                proof {
                    c1.lemma_closing(slabs@);
                    c1.lemma_ahead(slabs@, left as int);
                }
                f.discard(&mut self.edit, &slabs[self.slab], left);
                self.took(Ghost(slabs), left, Ghost(c1));
                proof {
                    lemma_run_joined(old(f).seen(), c0.o@, c0.orig as int, c1.orig as int, self.orig as int);
                }
                return;
            }
            // a whole slab, and no rebuild reading it: drop it outright,
            // with the whole slabs after it that the delete covers.
            // The column always keeps one slab, so the last one is
            // emptied through the rebuild below instead.
            if self.in_slab == 0 && !self.dirty && slabs.len() > 1 {
                let ghost c1 = *self;
                let gone = self.drop_slabs::<W, I, G, F>(slabs, index, total_len, left, f, Ghost(max_segments));
                proof {
                    lemma_run_joined(old(f).seen(), c0.o@, c0.orig as int, c1.orig as int, self.orig as int);
                }
                left -= gone;
                dropped = true;
                *counter = counter.wrapping_add(1);
                continue;
            }
            // the delete reaches this slab's end: finish it here and go on
            if !self.dirty {
                self.open(slabs, max_segments);
            }
            let ghost c1 = *self;
            proof {
                c1.lemma_closing(slabs@);
                c1.lemma_ahead(slabs@, avail as int);
            }
            f.discard(&mut self.edit, &slabs[self.slab], avail);
            self.took(Ghost(slabs), avail, Ghost(c1));
            proof {
                lemma_run_joined(old(f).seen(), c0.o@, c0.orig as int, c1.orig as int, self.orig as int);
            }
            left -= avail;
            // the whole slabs after this one that the delete covers go
            // first, while the rebuild is open
            if left > 0 && self.slab + 1 < slabs.len() && slabs[self.slab + 1].len <= left {
                let ghost c2 = *self;
                let gone = self.drop_after::<W, I, G, F>(slabs, index, total_len, left, f, Ghost(max_segments));
                proof {
                    lemma_run_joined(old(f).seen(), c0.o@, c0.orig as int, c2.orig as int, self.orig as int);
                }
                left -= gone;
            }
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
                self.lemma_stands(s0);
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

    /// Take slabs `si..sj`, of `gone` items together, out of a column:
    /// out of its slabs and out of its index at once, however many they
    /// are.
    fn cut_slabs<W, I: IndexOps<W>, G: Weigh<E, W>>(
        slabs: &mut Vec<Slab<E::Tail>>,
        index: &mut I,
        total_len: &mut usize,
        si: usize,
        sj: usize,
        gone: usize,
        Ghost(max_segments): Ghost<usize>,
    )
        requires
            col_ok::<E, W, I, G>(old(slabs)@, *old(index), *old(total_len), max_segments),
            si < sj <= old(slabs)@.len() <= usize::MAX,
            gone == vals_of::<E>(old(slabs)@.subrange(si as int, sj as int)).len(),
        ensures
            col_ok::<E, W, I, G>(final(slabs)@, *final(index), *final(total_len), max_segments),
            final(slabs)@ == old(slabs)@.take(si as int) + old(slabs)@.skip(sj as int),
            vals_of::<E>(final(slabs)@) == vals_of::<E>(old(slabs)@.take(si as int)) + vals_of::<E>(
                old(slabs)@.skip(sj as int),
            ),
            *final(total_len) == *old(total_len) - gone,
    {
        let ghost s0 = slabs@;
        proof {
            lemma_cut::<E, W, I, G>(s0, index.ws(), si as int, sj as int);
        }
        if sj == si + 1 {
            slabs.remove(si);
            assert(slabs@ =~= s0.take(si as int) + s0.skip(sj as int));
        } else {
            crate::index::splice_vec(slabs, si, sj, Vec::new());
            assert(slabs@ =~= s0.take(si as int) + s0.skip(sj as int));
        }
        index.ix_splice(si, sj, Vec::new());
        *total_len -= gone;
    }

    /// Drop the current slab whole, and the slabs after it for as long as
    /// they are whole in the next `left` items: the cursor stands at the
    /// first one's start with no rebuild open, the first one is whole in
    /// `left`, and it is not the column's only slab.  One slab stays,
    /// whatever `left` is.  `f` sees each slab dropped.
    fn drop_slabs<W, I: IndexOps<W>, G: Weigh<E, W>, F: Visitor<E>>(
        &mut self,
        slabs: &mut Vec<Slab<E::Tail>>,
        index: &mut I,
        total_len: &mut usize,
        left: usize,
        f: &mut F,
        Ghost(max_segments): Ghost<usize>,
    ) -> (gone: usize)
        requires
            old(self).ok::<W, I, G>(old(slabs)@, *old(index), *old(total_len), max_segments),
            !old(self).dirty,
            old(self).in_slab == 0,
            old(slabs)@.len() > 1,
            old(slabs)@[old(self).slab as int].len <= left,
        ensures
            final(self).ok::<W, I, G>(final(slabs)@, *final(index), *final(total_len), max_segments),
            !final(self).dirty,
            final(self).o == old(self).o,
            final(self).p == old(self).p,
            old(slabs)@[old(self).slab as int].len <= gone <= left,
            gone <= old(self).remaining,
            final(self).orig == old(self).orig + gone,
            final(self).remaining == old(self).remaining - gone,
            final(self).remaining > 0 ==> final(self).in_slab < final(slabs)@[final(self).slab as int].len,
            final(f).seen() == old(f).seen() + old(self).o@.subrange(
                old(self).orig as int,
                old(self).orig + gone,
            ),
    {
        let ghost c0 = *self;
        let ghost s0 = slabs@;
        let n = slabs.len();
        let si = self.slab;
        // the column always keeps one slab
        let end = if si == 0 {
            n - 1
        } else {
            n
        };
        let (sj, gone) = whole_slabs::<E, F>(slabs, si, end, left, f);
        let ghost mid = vals_of::<E>(s0.subrange(si as int, sj as int));
        let ghost rest = vals_of::<E>(s0.skip(sj as int));
        proof {
            lemma_vals_head::<E>(s0, si as int);
            lemma_vals_three::<E>(s0, si as int, sj as int);
            let v = E::vals(s0[si as int]);
            assert(v.skip(0) =~= v);
            assert(v.take(0) =~= Seq::<E::V>::empty());
            // what the cursor has yet to give starts with the slabs that go
            assert(c0.o@.skip(c0.orig as int) == mid + rest);
            assert(c0.o@.skip(c0.orig as int).len() == c0.remaining);
            assert(c0.o@.subrange(c0.orig as int, c0.orig + gone) =~= mid) by {
                assert(c0.o@.subrange(c0.orig as int, c0.orig + gone) =~= c0.o@.skip(c0.orig as int).take(
                    gone as int,
                ));
                assert((mid + rest).take(gone as int) =~= mid);
            }
        }
        Self::cut_slabs::<W, I, G>(slabs, index, total_len, si, sj, gone, Ghost(max_segments));
        self.orig += gone;
        self.remaining -= gone;
        proof {
            assert(self.o@.skip(self.orig as int) =~= c0.o@.skip(c0.orig as int).skip(gone as int));
            assert((mid + rest).skip(gone as int) =~= rest);
            assert(self.p@ =~= vals_of::<E>(s0.take(si as int)));
            assert(self.now() =~= vals_of::<E>(slabs@));
        }
        self.locate_out::<W, I, G>(slabs, index, *total_len, Ghost(max_segments));
        gone
    }

    /// With the open rebuild at its slab's end, drop the slabs after it
    /// for as long as they are whole in the next `left` items, the first
    /// of which is.  They go before the rebuild is closed: closed first,
    /// a slab left short would take in the slab after it, and the delete
    /// would rebuild that one too, and so on through every slab it covers.
    /// `f` sees each slab dropped.
    fn drop_after<W, I: IndexOps<W>, G: Weigh<E, W>, F: Visitor<E>>(
        &mut self,
        slabs: &mut Vec<Slab<E::Tail>>,
        index: &mut I,
        total_len: &mut usize,
        left: usize,
        f: &mut F,
        Ghost(max_segments): Ghost<usize>,
    ) -> (gone: usize)
        requires
            old(self).ok::<W, I, G>(old(slabs)@, *old(index), *old(total_len), max_segments),
            old(self).dirty,
            old(self).in_slab == old(slabs)@[old(self).slab as int].len,
            old(self).slab + 1 < old(slabs)@.len(),
            old(slabs)@[old(self).slab + 1].len <= left,
        ensures
            final(self).ok::<W, I, G>(final(slabs)@, *final(index), *final(total_len), max_segments),
            final(self).dirty,
            final(self).o == old(self).o,
            final(self).p == old(self).p,
            gone <= left,
            gone <= old(self).remaining,
            final(self).orig == old(self).orig + gone,
            final(self).remaining == old(self).remaining - gone,
            final(f).seen() == old(f).seen() + old(self).o@.subrange(
                old(self).orig as int,
                old(self).orig + gone,
            ),
    {
        let ghost c0 = *self;
        let ghost s0 = slabs@;
        let n = slabs.len();
        let si = self.slab + 1;
        let (sj, gone) = whole_slabs::<E, F>(slabs, si, n, left, f);
        let ghost mid = vals_of::<E>(s0.subrange(si as int, sj as int));
        let ghost rest = vals_of::<E>(s0.skip(sj as int));
        proof {
            lemma_vals_three::<E>(s0, si as int, sj as int);
            let v = E::vals(s0[c0.slab as int]);
            E::lemma_len(s0[c0.slab as int]);
            assert(v.skip(c0.in_slab as int) =~= Seq::<E::V>::empty());
            // what the cursor has yet to give starts with the slabs that go
            assert(c0.o@.skip(c0.orig as int) =~= mid + rest);
            assert(c0.o@.skip(c0.orig as int).len() == c0.remaining);
            assert(c0.o@.subrange(c0.orig as int, c0.orig + gone) =~= mid) by {
                assert(c0.o@.subrange(c0.orig as int, c0.orig + gone) =~= c0.o@.skip(c0.orig as int).take(
                    gone as int,
                ));
                assert((mid + rest).take(gone as int) =~= mid);
            }
        }
        Self::cut_slabs::<W, I, G>(slabs, index, total_len, si, sj, gone, Ghost(max_segments));
        self.orig += gone;
        self.remaining -= gone;
        proof {
            let s1 = slabs@;
            assert(s1.take(c0.slab as int) =~= s0.take(c0.slab as int));
            assert(s1.skip(si as int) =~= s0.skip(sj as int));
            assert(s1[c0.slab as int] == s0[c0.slab as int]);
            assert(self.o@.skip(self.orig as int) =~= c0.o@.skip(c0.orig as int).skip(gone as int));
            assert((mid + rest).skip(gone as int) =~= rest);
            assert(Seq::<E::V>::empty() + rest =~= rest);
        }
        gone
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

    /// The feed's own invariant, which it keeps from run to run. It may
    /// speak of what the feed is yet to give (a caller's iterator, by
    /// what Verus says it will return): no step of a splice hangs on it.
    #[verifier::prophetic]
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
pub(crate) fn splice_feed<E: Slabs, W, I: IndexOps<W>, G: Weigh<E, W>, F: Visitor<E>, R: RunFeed<E>>(
    slabs: &mut Vec<Slab<E::Tail>>,
    index: &mut I,
    total_len: &mut usize,
    max_segments: usize,
    counter: &mut usize,
    at: usize,
    del: usize,
    f: &mut F,
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
    cur.delete_with::<W, I, G, F>(slabs, index, total_len, max_segments, counter, del, f);
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
/// encoding's rebuild. A second one, `D`, sees each slab a delete drops
/// whole.
pub(crate) struct Visiting<'f, E, F, D = NoDrops>(&'f mut F, D, PhantomData<fn() -> E>);

/// Slabs dropped whole, unseen.
pub(crate) struct NoDrops;

/// Seeing each slab a delete drops whole.
pub(crate) trait Drops<T: Copy + Clone + std::fmt::Debug + Default> {
    fn dropped(&mut self, s: &Slab<T>);
}

impl<T: Copy + Clone + std::fmt::Debug + Default> Drops<T> for NoDrops {
    #[inline(always)]
    fn dropped(&mut self, _s: &Slab<T>) {}
}

impl<T: Copy + Clone + std::fmt::Debug + Default, D: FnMut(&Slab<T>)> Drops<T> for D {
    #[inline(always)]
    fn dropped(&mut self, s: &Slab<T>) {
        self(s)
    }
}

impl<'f, E, F> Visiting<'f, E, F> {
    #[inline(always)]
    pub(crate) fn new(f: &'f mut F) -> Self {
        Visiting(f, NoDrops, PhantomData)
    }
}

impl<'f, E, F, D> Visiting<'f, E, F, D> {
    #[inline(always)]
    pub(crate) fn with_drops(f: &'f mut F, d: D) -> Self {
        Visiting(f, d, PhantomData)
    }
}

impl<E: SlabEdit, F, D: Drops<<E as ColumnEncoding>::Tail>> Visitor<<E as SlabEdit>::Model>
    for Visiting<'_, E, F, D>
where
    F: FnMut(<<E as ColumnEncoding>::Value as ColumnValueRef>::Get<'_>, usize),
{
    #[cfg(verus_keep_ghost)]
    fn seen(&self) -> Seq<<<E as SlabEdit>::Model as Slabs>::V> {
        unimplemented!()
    }

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

    #[inline(always)]
    fn dropped(&mut self, s: &Slab<<E as ColumnEncoding>::Tail>) {
        self.1.dropped(s)
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
