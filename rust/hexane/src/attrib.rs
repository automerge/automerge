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
//! accumulation, in order, of runs that hold exactly its slab's values.
//! Trusted: that a weight function's `accumulate_run` folds a run into a
//! weight as `Accumulate::acc` says (each type's own; the README lists
//! what is assumed of it), and the glue that hands the loader's facts to
//! [`Attribution`].

use crate::PackError;
use vstd::prelude::*;

verus! {

/// A weight function's accumulation of runs into a slab's weight.
pub(crate) trait Accumulate<V, W> {
    /// The weight of no values.
    spec fn zero() -> W;

    /// `w` with a run of `n` copies of `v` folded in.
    spec fn acc(w: W, n: nat, v: V) -> W;

    fn new_zero() -> (w: W)
        ensures
            w == Self::zero(),
    ;

    /// Fold a run in, or fail (a sum that doesn't fit, say).
    fn accumulate_run(w: &mut W, n: usize, v: V) -> (r: Result<(), PackError>)
        ensures
            r is Ok ==> *final(w) == Self::acc(*old(w), n as nat, v),
    ;
}

/// `runs` folded into `w`, in order.
pub(crate) open spec fn fold_runs<V, W, A: Accumulate<V, W>>(w: W, runs: Seq<(nat, V)>) -> W
    decreases runs.len(),
{
    if runs.len() == 0 {
        w
    } else {
        A::acc(fold_runs::<V, W, A>(w, runs.drop_last()), runs.last().0, runs.last().1)
    }
}

/// The values of `runs`, in order.
pub(crate) open spec fn expand<V>(runs: Seq<(nat, V)>) -> Seq<V>
    decreases runs.len(),
{
    if runs.len() == 0 {
        Seq::empty()
    } else {
        expand(runs.drop_last()) + Seq::new(runs.last().0, |_i: int| runs.last().1)
    }
}

/// The values of slabs, one after another.
pub(crate) open spec fn concat<V>(vs: Seq<Seq<V>>) -> Seq<V>
    decreases vs.len(),
{
    if vs.len() == 0 {
        Seq::empty()
    } else {
        concat(vs.drop_last()) + vs.last()
    }
}

/// The weights of the slabs a load has cut, and of the one it is cutting.
pub(crate) struct Attribution<V, W> {
    pub(crate) weights: Vec<W>,
    pub(crate) cur: W,
    /// the runs of each slab cut, and of the one being cut
    #[allow(dead_code)]
    pub(crate) done_runs: Ghost<Seq<Seq<(nat, V)>>>,
    #[allow(dead_code)]
    pub(crate) cur_runs: Ghost<Seq<(nat, V)>>,
    /// the values of each slab cut, as the loader reported them
    #[allow(dead_code)]
    pub(crate) done_vals: Ghost<Seq<Seq<V>>>,
}

impl<V, W> Attribution<V, W> {
    pub(crate) open spec fn inv<A: Accumulate<V, W>>(&self) -> bool {
        &&& self.weights@.len() == self.done_runs@.len()
        &&& self.done_vals@.len() == self.done_runs@.len()
        &&& forall|i: int| 0 <= i < self.weights@.len() ==> #[trigger] self.weights@[i] == fold_runs::<V, W, A>(
            A::zero(),
            self.done_runs@[i],
        ) && expand(self.done_runs@[i]) == self.done_vals@[i]
        &&& self.cur == fold_runs::<V, W, A>(A::zero(), self.cur_runs@)
    }

    /// The values attributed so far.
    pub(crate) open spec fn pulled(&self) -> Seq<V> {
        concat(self.done_vals@) + expand(self.cur_runs@)
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
            done_runs: Ghost(Seq::empty()),
            cur_runs: Ghost(Seq::empty()),
            done_vals: Ghost(Seq::empty()),
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
        let ghost c0 = self.cur_runs@;
        let ghost c1 = c0.push((n as nat, v));
        let ghost run_vals = Seq::new(n as nat, |_i: int| v);
        match A::accumulate_run(&mut self.cur, n, v) {
            Ok(()) => {},
            Err(e) => return Err(e),
        }
        proof {
            assert(c1.drop_last() =~= c0);
            assert(expand(c1) == expand(c0) + run_vals);
        }
        if cut {
            let mut w = A::new_zero();
            core::mem::swap(&mut self.cur, &mut w);
            self.weights.push(w);
            proof {
                // the slab's values are those its runs left over
                assert(concat(d) + vals == concat(d) + (expand(c0) + run_vals));
                assert(vals =~= (concat(d) + vals).skip(concat(d).len() as int));
                assert((concat(d) + (expand(c0) + run_vals)).skip(concat(d).len() as int) =~= expand(c0) + run_vals);
                self.done_runs = Ghost(self.done_runs@.push(c1));
                self.cur_runs = Ghost(Seq::empty());
                self.done_vals = Ghost(d.push(vals));
                assert(d.push(vals).drop_last() =~= d);
                assert(concat(d.push(vals)) + expand(Seq::<(nat, V)>::empty()) =~= concat(d) + vals);
                assert forall|i: int| 0 <= i < self.weights@.len() implies #[trigger] self.weights@[i] == fold_runs::<
                    V,
                    W,
                    A,
                >(A::zero(), self.done_runs@[i]) && expand(self.done_runs@[i]) == self.done_vals@[i] by {
                    if i < self.weights@.len() - 1 {
                        assert(self.weights@[i] == old(self).weights@[i]);
                    }
                }
            }
        } else {
            proof {
                self.cur_runs = Ghost(c1);
                assert(concat(d) + expand(c1) =~= concat(d) + expand(c0) + run_vals);
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
            // each weight accumulates runs holding exactly its slab's values
            forall|i: int| #![trigger ws@[i]] 0 <= i < ws@.len() ==> exists|runs: Seq<(nat, V)>| #![trigger fold_runs::<V, W, A>(A::zero(), runs)]
                ws@[i] == fold_runs::<V, W, A>(A::zero(), runs)
                && expand(runs) == (if i < self.done_vals@.len() { self.done_vals@[i] } else { vals }),
    {
        let ghost done_runs = self.done_runs@;
        let ghost cur_runs = self.cur_runs@;
        let ghost d = self.done_vals@;
        let mut weights = self.weights;
        let ghost w0 = weights@;
        if last {
            weights.push(self.cur);
            proof {
                assert(vals =~= (concat(d) + vals).skip(concat(d).len() as int));
                assert((concat(d) + expand(cur_runs)).skip(concat(d).len() as int) =~= expand(cur_runs));
            }
        }
        proof {
            assert forall|i: int| #![trigger weights@[i]] 0 <= i < weights@.len() implies exists|runs: Seq<(nat, V)>| #![trigger fold_runs::<V, W, A>(A::zero(), runs)]
                weights@[i] == fold_runs::<V, W, A>(A::zero(), runs)
                && expand(runs) == (if i < d.len() { d[i] } else { vals }) by {
                if i < w0.len() {
                    assert(weights@[i] == w0[i]);
                    let runs = done_runs[i];
                    assert(weights@[i] == fold_runs::<V, W, A>(A::zero(), runs));
                } else {
                    let runs = cur_runs;
                    assert(weights@[i] == fold_runs::<V, W, A>(A::zero(), runs));
                }
            }
        }
        weights
    }
}

} // verus!
