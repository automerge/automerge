//! The public edit cursor's state, verified with Verus.
//!
//! [`crate::edit::Edit`] holds an [`EditCore`]: the column's parts, taken
//! out of its core, and the verified cursor over them (`cursor::Cursor`).
//! Its invariant (`EditCore::inv`) is the cursor's: it stands in the
//! parts, which are a column (`Cursor::ok`), at a value or the end
//! (`on_value`). Opening an edit establishes it ([`EditCore::begin`]),
//! every step requires and keeps it, and ending the edit hands the parts
//! back to the column checked to be in its weight model's domain
//! ([`EditCore::end`]). Ending takes the edit by value, so no step can
//! follow it, and none needs to check that the parts are still there.
//!
//! Trusted: that the glue only chains these methods, with the invariant
//! each leaves (the fields are private here), and uses none after a step
//! that panicked, which a caught panic could (`AssertUnwindSafe`'s
//! contract). Making the invariant a type invariant instead would prove
//! that too, but Verus allows a `&mut` into a type-invariant struct's
//! fields only for calls that cannot unwind, which the cursor's steps can
//! (they allocate): the state would have to be boxed and moved out and
//! back for each step, which costs a few instructions per step (3-13% on
//! the cheapest steps).

use crate::column::Slab;
use crate::column_core::{ColumnCore, Parts};
#[cfg(verus_keep_ghost)]
use crate::cursor::{lemma_vals_total, vals_of};
use crate::cursor::{Cursor, Insertable, Slabs, Visitor, Weigh};
use crate::index::IndexOps;
use std::marker::PhantomData;
use vstd::prelude::*;

verus! {

/// A column being edited: its parts and the cursor over them.
pub(crate) struct EditCore<E: Slabs, W, I: IndexOps<W>, G: Weigh<E, W>> {
    /// the column's parts, which ending the edit hands back
    parts: Parts<E::Tail, I>,
    cur: Cursor<E>,
    max_segments: usize,
    _p: PhantomData<(W, G)>,
}

/// The cursor stands in the parts, which are a column, at a value or at
/// the end.
pub(crate) open spec fn state_ok<E: Slabs, W, I: IndexOps<W>, G: Weigh<E, W>>(
    p: Parts<E::Tail, I>,
    cur: Cursor<E>,
    max_segments: usize,
) -> bool {
    &&& cur.ok::<W, I, G>(p.slabs@, p.index, p.total_len, max_segments)
    &&& cur.on_value(p.slabs@)
}

impl<E: Slabs, W, I: IndexOps<W>, G: Weigh<E, W>> EditCore<E, W, I, G> {
    /// The cursor stands in the parts while it has them.
    pub(crate) closed spec fn inv(&self) -> bool {
        &&& self.max_segments >= 2
        &&& self.cur.orig + self.cur.remaining == self.cur.o@.len()
        &&& state_ok::<E, W, I, G>(self.parts, self.cur, self.max_segments)
    }

    /// The cursor stands within the original values.
    pub(crate) proof fn lemma_at(&self)
        requires
            self.inv(),
        ensures
            0 <= self.at() <= self.orig_vals().len(),
    {
    }

    /// The edit's segment budget.
    pub(crate) closed spec fn budget(&self) -> usize {
        self.max_segments
    }

    /// The edit holds the column's parts: always, as ending it takes it
    /// whole ([`end`](Self::end)).
    pub(crate) closed spec fn held(&self) -> bool {
        true
    }

    /// The column's values when the edit opened.
    pub(crate) closed spec fn orig_vals(&self) -> Seq<E::V> {
        self.cur.o@
    }

    /// The cursor's position in them.
    pub(crate) closed spec fn at(&self) -> int {
        self.cur.orig as int
    }

    /// What the edit has made of the values before the cursor.
    pub(crate) closed spec fn written(&self) -> Seq<E::V> {
        self.cur.p@
    }

    /// The column's values with the edit's changes: what it has written,
    /// then the original values from the cursor on.
    pub(crate) open spec fn now(&self) -> Seq<E::V> {
        self.written() + self.orig_vals().skip(self.at())
    }

    /// How many items a step of `n` takes: all `n`, or what is left.
    pub(crate) open spec fn step_len(&self, n: usize) -> int {
        let left = self.orig_vals().len() - self.at();
        if n <= left {
            n as int
        } else {
            left
        }
    }

    /// A rebuild is open: the slab the cursor stands in is being written
    /// anew.
    pub(crate) closed spec fn rebuilding(&self) -> bool {
        self.cur.dirty
    }

    /// `st` is a reader of `slab` at the next original value.
    pub(crate) open spec fn reads(&self, st: E::State, slab: Slab<E::Tail>) -> bool {
        &&& E::ok(slab)
        &&& exists|k: nat|
            #![trigger E::st_at(slab.data@, st, k)]
            k < E::vals(slab).len() && E::st_at(slab.data@, st, k) && E::vals(slab)[k as int]
                == self.orig_vals()[self.at()]
    }

    /// A step kept the original values, and the edit holds the parts.
    pub(crate) open spec fn kept(&self, old: Self) -> bool {
        &&& self.inv()
        &&& self.held()
        &&& self.orig_vals() == old.orig_vals()
    }

    /// Edit `col` from item `at`: its parts, taken out of it, and a cursor
    /// over them. Panics unless `at` is in the column.
    pub(crate) fn begin(col: &mut ColumnCore<E, W, I, G>, at: usize) -> (e: Self)
        ensures
            e.inv(),
            e.held(),
            e.orig_vals() == old(col)@,
            e.at() == at,
            e.written() == old(col)@.take(at as int),
            old(col).ready(),
            e.budget() == old(col).budget(),
    {
        if at > col.len() {
            out_of_bounds();
        }
        col.lemma_column();
        let max_segments = col.max_segments();
        let mut p = col.take_parts();
        proof {
            lemma_vals_total::<E>(p.slabs@);
        }
        let cur = Cursor::<E>::new::<W, I, G>(&mut p.slabs, &mut p.index, p.total_len, Ghost(max_segments), at);
        EditCore { parts: p, cur, max_segments, _p: PhantomData }
    }

    /// The cursor's position in the column's original coordinates.
    #[inline(always)]
    pub(crate) fn pos(&self) -> (r: usize)
        ensures
            r == self.at(),
    {
        self.cur.orig
    }

    /// The cursor's position in the column as edited so far.
    #[inline(always)]
    pub(crate) fn out_pos(&self) -> (r: usize)
        ensures
            self.inv() && self.held() ==> r == self.written().len(),
    {
        self.cur.out
    }

    /// Original items still ahead of the cursor.
    #[inline(always)]
    pub(crate) fn remaining(&self) -> (r: usize)
        ensures
            self.inv() ==> r == self.orig_vals().len() - self.at(),
    {
        self.cur.remaining
    }

    /// Slabs the cursor has rebuilt.
    #[inline(always)]
    pub(crate) fn rebuilds(&self) -> usize {
        self.cur.rebuilds
    }

    /// Move forward `n` items, carrying them, `f` told of each run;
    /// returns how many it jumped through the index without reading.
    ///
    /// A move inside the slab with no rebuild open is taken here, inline;
    /// anything else out of line ([`Self::advance_slow`]), so the common
    /// move needs no frame and the rest no more than three arguments.
    #[inline(always)]
    pub(crate) fn advance_with<F: Visitor<E>>(&mut self, n: usize, f: &mut F) -> (jumped: usize)
        requires
            old(self).inv(),
        ensures
            final(self).kept(*old(self)),
            final(self).at() == old(self).at() + old(self).step_len(n),
            final(self).written() == old(self).written() + old(self).orig_vals().subrange(
                old(self).at(),
                old(self).at() + old(self).step_len(n),
            ),
            // `f` is shown all of them but the last `jumped`
            jumped <= old(self).step_len(n),
            jumped > 0 ==> !final(self).rebuilding(),
            final(f).seen() == old(f).seen() + old(self).orig_vals().subrange(
                old(self).at(),
                old(self).at() + old(self).step_len(n) - jumped,
            ),
    {
        if !self.cur.dirty && n < self.parts.slabs[self.cur.slab].len - self.cur.in_slab {
            let ms = self.max_segments;
            let p = &mut self.parts;
            return self.cur.advance_with::<W, I, G, F>(&mut p.slabs, &mut p.index, &mut p.total_len, ms, &mut p.counter, n, f);
        }
        self.advance_slow(n, f)
    }

    /// [`Self::advance_with`], for any move.
    #[inline(never)]
    fn advance_slow<F: Visitor<E>>(&mut self, n: usize, f: &mut F) -> (jumped: usize)
        requires
            old(self).inv(),
        ensures
            final(self).kept(*old(self)),
            final(self).at() == old(self).at() + old(self).step_len(n),
            final(self).written() == old(self).written() + old(self).orig_vals().subrange(
                old(self).at(),
                old(self).at() + old(self).step_len(n),
            ),
            // `f` is shown all of them but the last `jumped`
            jumped <= old(self).step_len(n),
            jumped > 0 ==> !final(self).rebuilding(),
            final(f).seen() == old(f).seen() + old(self).orig_vals().subrange(
                old(self).at(),
                old(self).at() + old(self).step_len(n) - jumped,
            ),
    {
        let ms = self.max_segments;
        let p = &mut self.parts;
        self.cur.advance_with::<W, I, G, F>(&mut p.slabs, &mut p.index, &mut p.total_len, ms, &mut p.counter, n, f)
    }

    /// Insert `n` copies of `x` at the cursor.
    #[inline(always)]
    pub(crate) fn insert<X: Insertable<E>>(&mut self, x: X, n: usize)
        requires
            old(self).inv(),
        ensures
            final(self).kept(*old(self)),
            final(self).at() == old(self).at(),
            final(self).written() == old(self).written() + Seq::new(n as nat, |_i: int| x.val()),
    {
        let ms = self.max_segments;
        self.cur.insert(&self.parts.slabs, ms, x, n);
    }

    /// Delete `n` items at the cursor, `f` shown each run a rebuild drops
    /// and each slab dropped whole: all the items deleted, in order.
    #[inline(always)]
    pub(crate) fn delete_with<F: Visitor<E>>(&mut self, n: usize, f: &mut F)
        requires
            old(self).inv(),
        ensures
            final(self).kept(*old(self)),
            final(self).at() == old(self).at() + old(self).step_len(n),
            final(self).written() == old(self).written(),
            final(f).seen() == old(f).seen() + old(self).orig_vals().subrange(
                old(self).at(),
                old(self).at() + old(self).step_len(n),
            ),
    {
        let ms = self.max_segments;
        let p = &mut self.parts;
        self.cur.delete_with::<W, I, G, F>(&mut p.slabs, &mut p.index, &mut p.total_len, ms, &mut p.counter, n, f);
    }

    /// Close the open rebuild, if any.
    #[inline(always)]
    pub(crate) fn close(&mut self)
        requires
            old(self).inv(),
        ensures
            final(self).inv(),
            final(self).held() == old(self).held(),
            final(self).orig_vals() == old(self).orig_vals(),
            final(self).at() == old(self).at(),
            final(self).written() == old(self).written(),
            !final(self).rebuilding(),
    {
        if self.cur.dirty {
            let ms = self.max_segments;
            let p = &mut self.parts;
            self.cur.close::<W, I, G>(&mut p.slabs, &mut p.index, &mut p.total_len, ms, &mut p.counter);
        }
    }

    /// Whether a rebuild is open: then its own reader stands at the
    /// cursor ([`rebuild_reader`](Self::rebuild_reader)), else the
    /// cursor's is brought to it ([`reader`](Self::reader)).
    #[inline(always)]
    pub(crate) fn dirty(&self) -> (r: bool)
        ensures
            r == self.rebuilding(),
    {
        self.cur.dirty
    }

    /// The open rebuild's reader, at the cursor, and the slab it stands
    /// in, or `None` at the end or with no rebuild open.
    #[inline(always)]
    pub(crate) fn rebuild_reader(&self) -> (r: Option<(&E::State, &Slab<E::Tail>)>)
        requires
            self.inv(),
        ensures
            r matches Some((st, slab)) ==> self.reads(*st, *slab) && self.at() < self.orig_vals().len(),
            r is None ==> self.at() == self.orig_vals().len() || !self.rebuilding(),
    {
        if self.cur.remaining == 0 || !self.cur.dirty {
            return None;
        }
        proof {
            self.cur.lemma_here(self.parts.slabs@);
            E::lemma_len(self.parts.slabs@[self.cur.slab as int]);
        }
        Some((E::read_state(&self.cur.edit), &self.parts.slabs[self.cur.slab]))
    }

    /// The cursor's reader, brought to the cursor, and the slab it stands
    /// in, or `None` at the end; with no rebuild open.
    ///
    /// Where the reader only has to move inside the run it holds, that is
    /// done here, inline; anything else is [`Self::reader_long`], out of
    /// line, so the common read needs no frame.
    #[inline(always)]
    pub(crate) fn reader(&mut self) -> (r: Option<(&E::State, &Slab<E::Tail>)>)
        requires
            old(self).inv(),
        ensures
            final(self).inv(),
            r matches Some((st, slab)) ==> final(self).reads(*st, *slab) && old(self).at() < old(self).orig_vals().len(),
            r is None ==> old(self).at() == old(self).orig_vals().len(),
            final(self).rebuilding() == old(self).rebuilding(),
            final(self).held() == old(self).held(),
            final(self).orig_vals() == old(self).orig_vals(),
            final(self).at() == old(self).at(),
            final(self).written() == old(self).written(),
    {
        if self.cur.remaining == 0 {
            return None;
        }
        let at = self.cur.in_slab;
        let slab = &self.parts.slabs[self.cur.slab];
        let skipped = match &mut self.cur.dec {
            Some(r) => {
                if r.at <= at {
                    let n = at - r.at;
                    let ghost pos = r.at as nat;
                    proof {
                        E::lemma_len(*slab);
                    }
                    if E::state_skip(slab, &mut r.state, n, Ghost(pos)) {
                        r.at = at;
                        true
                    } else {
                        false
                    }
                } else {
                    false
                }
            },
            None => false,
        };
        if skipped {
            proof {
                self.cur.lemma_here(self.parts.slabs@);
                E::lemma_len(self.parts.slabs@[self.cur.slab as int]);
            }
            return match &self.cur.dec {
                Some(r) => Some((&r.state, &self.parts.slabs[self.cur.slab])),
                None => unreached(),
            };
        }
        self.reader_long()
    }

    /// [`Self::reader`], wherever the reader stands.
    #[inline(never)]
    fn reader_long(&mut self) -> (r: Option<(&E::State, &Slab<E::Tail>)>)
        requires
            old(self).inv(),
        ensures
            final(self).inv(),
            r matches Some((st, slab)) ==> final(self).reads(*st, *slab) && old(self).at() < old(self).orig_vals().len(),
            r is None ==> old(self).at() == old(self).orig_vals().len(),
            final(self).rebuilding() == old(self).rebuilding(),
            final(self).held() == old(self).held(),
            final(self).orig_vals() == old(self).orig_vals(),
            final(self).at() == old(self).at(),
            final(self).written() == old(self).written(),
    {
        if self.cur.remaining == 0 {
            return None;
        }
        self.cur.load_reader(&self.parts.slabs);
        proof {
            self.cur.lemma_here(self.parts.slabs@);
            E::lemma_len(self.parts.slabs@[self.cur.slab as int]);
        }
        match &self.cur.dec {
            Some(r) => Some((&r.state, &self.parts.slabs[self.cur.slab])),
            None => None,
        }
    }

    /// The column's parts, with no rebuild open: a column (`col_ok`)
    /// holding the edit's values.
    #[inline(always)]
    pub(crate) fn clean_parts(&self) -> (r: (&Vec<Slab<E::Tail>>, &I, usize))
        requires
            self.inv(),
            self.held(),
            !self.rebuilding(),
        ensures
            crate::cursor::col_ok::<E, W, I, G>(r.0@, *r.1, r.2, self.budget()),
            r.0@ == self.slabs_now(),
            vals_of::<E>(r.0@) == self.now(),
            r.2 == self.now().len(),
            r.2 + 3 <= usize::MAX,
    {
        proof {
            self.cur.lemma_clean(self.parts.slabs@);
            lemma_vals_total::<E>(self.parts.slabs@);
        }
        (&self.parts.slabs, &self.parts.index, self.parts.total_len)
    }

    /// The slabs the edit holds, as the cursor has left them.
    pub(crate) closed spec fn slabs_now(&self) -> Seq<Slab<E::Tail>> {
        self.parts.slabs@
    }

    /// The edit is closed and its column in the weight model's domain
    /// (as a delta edit checks before it ends).
    pub(crate) open spec fn clean_in_domain(&self) -> bool {
        self.held() ==> !self.rebuilding() && G::dom_ok(self.slabs_now())
    }

    /// Close the edit: the parts, a column again.
    fn finish(self) -> (p: Parts<E::Tail, I>)
        requires
            self.inv(),
        ensures
            crate::cursor::col_ok::<E, W, I, G>(p.slabs@, p.index, p.total_len, self.max_segments),
            p.total_len + 3 <= usize::MAX,
            self.clean_in_domain() ==> G::dom_ok(p.slabs@),
            vals_of::<E>(p.slabs@) == self.now(),
    {
        let ms = self.max_segments;
        let mut cur = self.cur;
        let mut p = self.parts;
        cur.close::<W, I, G>(&mut p.slabs, &mut p.index, &mut p.total_len, ms, &mut p.counter);
        proof {
            cur.lemma_clean(p.slabs@);
            lemma_vals_total::<E>(p.slabs@);
        }
        p
    }

    /// End the edit: close it and hand the column its parts back, the
    /// column checked to be in the weight model's domain (free but for a
    /// delta column, whose edit cursor checks it more cheaply and ends
    /// with [`end_in_domain`](Self::end_in_domain)).
    pub(crate) fn end(self, col: &mut ColumnCore<E, W, I, G>)
        requires
            self.inv(),
        ensures
            final(col)@ == self.now(),
    {
        let ms = self.max_segments;
        let p = self.finish();
        if !G::dom_check(&p.slabs, p.total_len) {
            out_of_domain();
        }
        col.restore_budget(p, ms);
    }

    /// [`end`](Self::end), for an edit closed already and checked to have
    /// left its column in the domain.
    pub(crate) fn end_in_domain(self, col: &mut ColumnCore<E, W, I, G>)
        requires
            self.inv(),
            self.clean_in_domain(),
        ensures
            final(col)@ == self.now(),
    {
        let ms = self.max_segments;
        let p = self.finish();
        col.restore_budget(p, ms);
    }

    /// An edit of nothing, for the glue to leave in place of one it ends
    /// (`Edit`'s `drop`): no step may be called on it, for it has no
    /// invariant, and it holds nothing to drop.
    pub(crate) fn spent() -> Self {
        EditCore {
            parts: Parts { slabs: Vec::new(), index: I::spent(), total_len: 0, counter: 0 },
            cur: Cursor::spent(),
            max_segments: 2,
            _p: PhantomData,
        }
    }
}

/// Panics: an edit opened past the column's end.
#[verifier::external_body]
#[cold]
#[inline(never)]
fn out_of_bounds()
    ensures
        false,
{
    panic!("edit_at out of bounds")
}

/// Panics: an edit that left a delta column's values outside its domain.
#[verifier::external_body]
#[cold]
#[inline(never)]
fn out_of_domain()
    ensures
        false,
{
    panic!("delta values out of domain: they would span 2^63 or more")
}

/// Never returns: after a call that does not.
#[verifier::external_body]
#[inline(always)]
fn unreached<A>() -> A
    requires
        false,
{
    unreachable!()
}

} // verus!
