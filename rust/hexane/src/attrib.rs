//! A load's slab weights, accumulated as the runs stream past, verified
//! with Verus.
//!
//! [`ColumnLoadIter`](crate::ColumnLoadIter) builds the weight of each slab
//! the loader cuts from the runs it pulls, instead of decoding the slabs
//! again once they are cut. That needs each run attributed to the slab
//! that holds it. The loaders (`rle::cut::Load`, `bool_wire::BoolLoad`) are
//! proved to cut a slab only at the end of a run, after which the slabs
//! cut hold exactly the values pulled, and to add at most one last slab
//! when they finish.
//!
//! Proved here: given those facts about each run, every weight is the
//! weight, as its weight model accumulates them (`Accumulate`, verified
//! for each model in `weights`), of exactly its slab's values.

#[cfg(verus_keep_ghost)]
pub(crate) use crate::rle::writer::concat;
use crate::PackError;
use vstd::prelude::*;

verus! {

/// A weight model's accumulation of runs into a slab's weight.
pub(crate) trait Accumulate<V, W> {
    /// `w` is the weight of values `vs`.
    spec fn wvals(w: W, vs: Seq<V>) -> bool;

    /// The weight of no values.
    fn new_zero() -> (w: W)
        ensures
            Self::wvals(w, Seq::empty()),
    ;

    /// Fold a run of `n` copies of `v` into `w`, the weight of `vs`; or
    /// fail (a sum that doesn't fit, say).
    fn accumulate_run(w: &mut W, n: usize, v: V, Ghost(vs): Ghost<Seq<V>>) -> (r: Result<(), PackError>)
        requires
            Self::wvals(*old(w), vs),
            n >= 1,
        ensures
            r is Ok ==> Self::wvals(*final(w), vs + Seq::new(n as nat, |_i: int| v)),
    ;
}

/// The weights of the slabs a load has cut, and of the one it is cutting.
pub(crate) struct Attribution<V, W> {
    pub(crate) weights: Vec<W>,
    pub(crate) cur: W,
    /// the values of each slab cut, as the loader reported them, and of
    /// the one being cut
    #[allow(dead_code)]
    pub(crate) done_vals: Ghost<Seq<Seq<V>>>,
    #[allow(dead_code)]
    pub(crate) cur_vals: Ghost<Seq<V>>,
}

impl<V, W> Attribution<V, W> {
    pub(crate) open spec fn inv<A: Accumulate<V, W>>(&self) -> bool {
        &&& self.weights@.len() == self.done_vals@.len()
        &&& forall|i: int| 0 <= i < self.weights@.len() ==> A::wvals(#[trigger] self.weights@[i], self.done_vals@[i])
        &&& A::wvals(self.cur, self.cur_vals@)
    }

    /// The values attributed so far.
    pub(crate) open spec fn pulled(&self) -> Seq<V> {
        concat(self.done_vals@) + self.cur_vals@
    }

    pub(crate) fn new<A: Accumulate<V, W>>() -> (a: Self)
        ensures
            a.inv::<A>(),
            a.done_vals@.len() == 0,
            a.pulled().len() == 0,
    {
        Attribution {
            weights: Vec::new(),
            cur: A::new_zero(),
            done_vals: Ghost(Seq::empty()),
            cur_vals: Ghost(Seq::empty()),
        }
    }

    /// The loader pulled a run of `n` copies of `v`. If that completed a
    /// slab, it is `cut`, and holds `vals` (the loader cuts only at a run's
    /// end, and the slabs cut then hold every value pulled).
    #[inline(always)]
    pub(crate) fn run<A: Accumulate<V, W>>(&mut self, n: usize, v: V, cut: bool, Ghost(vals): Ghost<Seq<V>>)
        -> (r: Result<(), PackError>)
        requires
            old(self).inv::<A>(),
            n >= 1,
            cut ==> concat(old(self).done_vals@) + vals == old(self).pulled() + Seq::new(n as nat, |_i: int| v),
        ensures
            r is Ok ==> {
                &&& final(self).inv::<A>()
                &&& final(self).pulled() == old(self).pulled() + Seq::new(n as nat, |_i: int| v)
                &&& cut ==> final(self).done_vals@ == old(self).done_vals@.push(vals)
                &&& !cut ==> final(self).done_vals@ == old(self).done_vals@
            },
    {
        let ghost d = self.done_vals@;
        let ghost c0 = self.cur_vals@;
        let ghost run_vals = Seq::new(n as nat, |_i: int| v);
        match A::accumulate_run(&mut self.cur, n, v, Ghost(c0)) {
            Ok(()) => {},
            Err(e) => return Err(e),
        }
        if cut {
            let mut w = A::new_zero();
            core::mem::swap(&mut self.cur, &mut w);
            self.weights.push(w);
            proof {
                // the slab's values are those the runs left over
                assert(vals =~= c0 + run_vals) by {
                    assert(concat(d) + vals =~= concat(d) + (c0 + run_vals));
                    assert(vals =~= (concat(d) + vals).skip(concat(d).len() as int));
                    assert((concat(d) + (c0 + run_vals)).skip(concat(d).len() as int) =~= c0 + run_vals);
                }
                self.done_vals = Ghost(d.push(vals));
                self.cur_vals = Ghost(Seq::empty());
                assert(d.push(vals).drop_last() =~= d);
                assert(concat(d.push(vals)) =~= concat(d) + vals);
                assert(concat(d.push(vals)) + Seq::<V>::empty() =~= concat(d) + c0 + run_vals);
                assert forall|i: int| 0 <= i < self.weights@.len() implies A::wvals(
                    #[trigger] self.weights@[i],
                    self.done_vals@[i],
                ) by {
                    if i < self.weights@.len() - 1 {
                        assert(self.weights@[i] == old(self).weights@[i]);
                    }
                }
            }
        } else {
            proof {
                self.cur_vals = Ghost(c0 + run_vals);
                assert(concat(d) + (c0 + run_vals) =~= concat(d) + c0 + run_vals);
                assert forall|i: int| 0 <= i < self.weights@.len() implies A::wvals(
                    #[trigger] self.weights@[i],
                    self.done_vals@[i],
                ) by {
                    assert(self.weights@[i] == old(self).weights@[i]);
                }
            }
        }
        Ok(())
    }

    /// The load is done: the weights of its slabs, the last being the one
    /// being cut if the loader cut it (`last`, holding `vals`, the values
    /// left over), or none if every value pulled is in a slab already.
    pub(crate) fn finish<A: Accumulate<V, W>>(self, last: bool, Ghost(vals): Ghost<Seq<V>>) -> (ws: Vec<W>)
        requires
            self.inv::<A>(),
            last ==> concat(self.done_vals@) + vals == self.pulled(),
            !last ==> concat(self.done_vals@) == self.pulled(),
        ensures
            ws@.len() == self.weights@.len() + (if last { 1int } else { 0 }),
            forall|i: int| 0 <= i < self.weights@.len() ==> #[trigger] ws@[i] == self.weights@[i],
            forall|i: int| 0 <= i < self.weights@.len() ==> A::wvals(#[trigger] ws@[i], self.done_vals@[i]),
            last ==> A::wvals(ws@[ws@.len() - 1], vals),
    {
        let ghost d = self.done_vals@;
        let ghost c = self.cur_vals@;
        let mut weights = self.weights;
        let ghost w0 = weights@;
        if last {
            weights.push(self.cur);
            proof {
                assert(vals =~= (concat(d) + vals).skip(concat(d).len() as int));
                assert((concat(d) + c).skip(concat(d).len() as int) =~= c);
            }
        }
        proof {
            assert forall|i: int| 0 <= i < w0.len() implies #[trigger] weights@[i] == w0[i] by {}
        }
        weights
    }
}

} // verus!
