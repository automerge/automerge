//! A column's load, verified with Verus: the slabs a loader cuts and the
//! weights accumulated as their runs stream past, which make a column.
//!
//! A loader ([`Loads`]: `rle::cut::Load`, `bool_wire::BoolLoad`) reads an
//! encoded column run by run, validating it and cutting it into slabs. It
//! is proved to cut a slab only at the end of a run, and to finish with
//! well-formed slabs holding the values read.
//!
//! A [`LoadCore`] drives a loader, and, where the column's weight model
//! accumulates weights as the runs pass (`weights::LoadWeigh`), attributes
//! each run to the slab being cut (`attrib::Attribution`). Proved: the
//! column it finishes with is built from well-formed slabs, each weighed
//! by its own weight (accumulated from exactly its values, or computed),
//! and in the weight model's domain, which the model's load checks
//! establish: `ColumnCore::from_slabs`' preconditions, from the bytes.

use crate::attrib::Attribution;
#[cfg(verus_keep_ghost)]
use crate::attrib::{concat, Accumulate};
use crate::column::Slab;
use crate::column_core::ColumnCore;
#[cfg(verus_keep_ghost)]
use crate::cursor::Weigh;
#[cfg(verus_keep_ghost)]
use crate::cursor::{all_ok, lemma_concat_append, slab_lens, ws_ok};
use crate::cursor::{ReadsAgree, Slabs};
#[cfg(verus_keep_ghost)]
use crate::fenwick::{lemma_sum_le, lemma_sum_one, lemma_sum_split, sum, total};
use crate::index::IndexOps;
#[cfg(verus_keep_ghost)]
use crate::iter_core::{each, vals_of};
use crate::iter_core::{Decode, Decoders};
use crate::weights::LoadWeigh;
use crate::{PackError, Run};
use std::marker::PhantomData;
use vstd::prelude::*;

verus! {

/// A loader: reads an encoded column run by run, validating it, and cuts
/// what it has read into slabs of the edit model `E`, whose values the
/// read model `X` gives.
pub(crate) trait Loads<'a, E: ReadsAgree<'a, X>, X: Decoders<'a, Tail = <E as Slabs>::Tail>>: Sized {
    spec fn inv(&self) -> bool;

    /// The values of the runs read so far.
    spec fn pulled(&self) -> Seq<<X::D as Decode>::Item>;

    /// The slabs cut so far.
    spec fn done_slabs(&self) -> Seq<Slab<E::Tail>>;

    /// The input is read to its end.
    spec fn at_end(&self) -> bool;

    /// A load of `data`, into slabs of about `max_segments / 2` segments.
    fn start(data: &'a [u8], max_segments: usize) -> (l: Self)
        requires
            max_segments >= 2,
        ensures
            l.inv(),
            l.pulled().len() == 0,
            l.done_slabs().len() == 0,
    ;

    /// The slabs cut so far are well-formed, and hold the first of the
    /// values pulled.
    proof fn lemma_done(&self)
        requires
            self.inv(),
        ensures
            all_ok::<E>(self.done_slabs()),
            vals_of::<X>(self.done_slabs()).len() <= self.pulled().len(),
            self.pulled().take(vals_of::<X>(self.done_slabs()).len() as int) == vals_of::<X>(self.done_slabs()),
    ;

    /// The next run, or `None` at the end of the input. A slab is cut
    /// only at the end of a run, and then holds what that run left over.
    fn try_next_run(&mut self) -> (r: Result<Option<Run<<X::D as Decode>::Item>>, PackError>)
        requires
            old(self).inv(),
        ensures
            r is Ok ==> final(self).inv(),
            r matches Ok(Some(run)) ==> run.count > 0 && final(self).pulled() == old(self).pulled() + Seq::new(
                run.count as nat,
                |_j: int| run.value,
            ),
            r matches Ok(None) ==> final(self).pulled() == old(self).pulled() && final(self).done_slabs()
                == old(self).done_slabs() && final(self).at_end(),
            r matches Ok(Some(_)) ==> final(self).done_slabs() == old(self).done_slabs() || {
                &&& final(self).done_slabs().len() == old(self).done_slabs().len() + 1
                &&& final(self).done_slabs().drop_last() == old(self).done_slabs()
                &&& vals_of::<X>(final(self).done_slabs()) == final(self).pulled()
            },
    ;

    fn slabs_completed(&self) -> (n: usize)
        ensures
            n == self.done_slabs().len(),
    ;

    /// Read and validate the rest, and cut it: the slabs, those cut
    /// already first; read to the end, at most one more holds the values
    /// pulled since.
    fn finalize(self) -> (r: Result<Vec<Slab<E::Tail>>, PackError>)
        requires
            self.inv(),
        ensures
            r matches Ok(slabs) ==> {
                &&& all_ok::<E>(slabs@)
                &&& slabs@.len() >= self.done_slabs().len()
                &&& slabs@.take(self.done_slabs().len() as int) == self.done_slabs()
                &&& self.at_end() ==> slabs@.len() <= self.done_slabs().len() + 1 && vals_of::<X>(slabs@)
                    == self.pulled()
            },
    ;
}


/// The error a load reports for a column of `len` items where it expected
/// `expected`.
#[verifier::external_body]
#[cold]
fn wrong_length(len: usize, expected: usize) -> PackError {
    PackError::InvalidLength(len, expected)
}

/// A column's load in progress: the loader, and the weights of the slabs
/// it cuts, accumulated as their runs stream past (where the weight model
/// `G` accumulates them).
pub(crate) struct LoadCore<'a, E, X: Decoders<'a>, W, G, LD> {
    pub(crate) ld: LD,
    pub(crate) attr: Attribution<<X::D as Decode>::Item, W>,
    pub(crate) _p: PhantomData<(E, G)>,
}

impl<'a, E, X, W, G, LD> LoadCore<'a, E, X, W, G, LD> where
    E: ReadsAgree<'a, X>,
    X: Decoders<'a, Tail = <E as Slabs>::Tail>,
    G: LoadWeigh<'a, E, X, W>,
    LD: Loads<'a, E, X>,
 {
    /// The loader's, and the weights so far are those of the values
    /// pulled, attributed to the slabs as the loader cut them.
    pub(crate) open spec fn inv(&self) -> bool {
        &&& self.ld.inv()
        &&& self.attr.inv::<G>()
        &&& G::accumulates() ==> {
            &&& self.attr.pulled() == self.ld.pulled()
            &&& self.attr.done_vals@ == each::<X>(self.ld.done_slabs())
        }
    }

    /// A load of `data`, into slabs of about `max_segments / 2`
    /// segments.
    pub(crate) fn start(data: &'a [u8], max_segments: usize) -> (c: Self)
        requires
            max_segments >= 2,
        ensures
            c.inv(),
    {
        let ld = LD::start(data, max_segments);
        let attr = Attribution::new::<G>();
        proof {
            assert(attr.pulled() =~= ld.pulled());
            assert(attr.done_vals@ =~= each::<X>(ld.done_slabs()));
        }
        LoadCore { ld, attr, _p: PhantomData }
    }

    /// The next run, or `None` at the end of the input; its items are
    /// attributed to the slab being cut.
    #[inline(always)]
    pub(crate) fn next_run(&mut self) -> (r: Result<Option<Run<<X::D as Decode>::Item>>, PackError>)
        requires
            old(self).inv(),
        ensures
            r is Ok ==> final(self).inv(),
            r matches Ok(None) ==> final(self).ld.at_end() && final(self).ld.pulled() == old(self).ld.pulled(),
    {
        let ghost (d0, p0) = (self.ld.done_slabs(), self.ld.pulled());
        proof {
            self.ld.lemma_done();
        }
        let r = self.ld.try_next_run();
        match r {
            Ok(Some(run)) => {
                if G::accumulates_exec() {
                    let cut = self.attr.weights.len() < self.ld.slabs_completed();
                    let ghost d1 = self.ld.done_slabs();
                    let ghost vals = if cut { X::vals(d1.last()) } else { Seq::empty() };
                    proof {
                        if cut {
                            assert(d1 =~= d0.push(d1.last()));
                            assert(each::<X>(d1) =~= each::<X>(d0).push(X::vals(d1.last())));
                            assert(each::<X>(d1).drop_last() =~= each::<X>(d0));
                        }
                    }
                    match self.attr.run::<G>(run.count, run.value, cut, Ghost(vals)) {
                        Ok(()) => {},
                        Err(e) => return Err(e),
                    }
                    proof {
                        if cut {
                            assert(self.attr.done_vals@ =~= each::<X>(d1));
                        }
                    }
                }
                Ok(Some(run))
            },
            Ok(None) => Ok(None),
            Err(e) => Err(e),
        }
    }

    /// Read the rest, and finish: the column of the slabs the loader cut,
    /// each weighed by its own weight, with segment budget
    /// `max_segments`, if it holds `expected` items (where given) and
    /// passes the weight model's load checks (`lo..=hi`: a delta
    /// column's domain).
    #[verifier::exec_allows_no_decreases_clause]
    pub(crate) fn finish<I: IndexOps<W>>(
        self,
        max_segments: usize,
        expected: Option<usize>,
        lo: i64,
        hi: i64,
    ) -> (r: Result<ColumnCore<E, W, I, G>, PackError>)
        requires
            self.inv(),
        ensures
            r matches Ok(c) ==> c.ready() && c.budget() == max_segments,
    {
        let mut core = self;
        let acc = G::accumulates_exec();
        if acc {
            // the runs left must pass through the attribution too
            loop
                invariant
                    core.inv(),
                    acc == G::accumulates(),
                ensures
                    core.inv(),
                    core.ld.at_end(),
            {
                match core.next_run() {
                    Ok(Some(_)) => {},
                    Ok(None) => break,
                    Err(e) => return Err(e),
                }
            }
        }
        let ghost (d, p) = (core.ld.done_slabs(), core.ld.pulled());
        proof {
            core.ld.lemma_done();
        }
        let ld = core.ld;
        let attr = core.attr;
        let slabs = ld.finalize()?;
        let ghost s = slabs@;
        // the column's length, which must fit
        let mut len: usize = 0;
        let mut i: usize = 0;
        while i < slabs.len()
            invariant
                s == slabs@,
                0 <= i <= s.len(),
                len == sum(slab_lens(s), 0, i as int),
            decreases s.len() - i,
        {
            proof {
                lemma_sum_split(slab_lens(s), 0, i as int, i + 1);
                lemma_sum_one(slab_lens(s), i as int);
            }
            if len > usize::MAX - slabs[i].len {
                return Err(wrong_length(usize::MAX, usize::MAX - 3));
            }
            len += slabs[i].len;
            i += 1;
        }
        if let Some(e) = expected {
            if len != e {
                return Err(wrong_length(len, e));
            }
        }
        if len > usize::MAX - 3 {
            return Err(wrong_length(len, usize::MAX - 3));
        }
        // the weights: accumulated as the runs passed, or computed
        let ws = if acc {
            let last = attr.weights.len() < slabs.len();
            let ghost vals = if last { X::vals(s.last()) } else { Seq::empty() };
            proof {
                assert(attr.done_vals@ =~= each::<X>(d));
                assert(concat(attr.done_vals@) == vals_of::<X>(d));
                if last {
                    assert(s =~= d.push(s.last())) by {
                        assert(s.take(d.len() as int) =~= d);
                    }
                    assert(each::<X>(s) =~= each::<X>(d).push(X::vals(s.last())));
                    assert(each::<X>(s).drop_last() =~= each::<X>(d));
                } else {
                    assert(s =~= d) by {
                        assert(s.take(d.len() as int) =~= d);
                    }
                }
            }
            attr.finish::<G>(last, Ghost(vals))
        } else {
            let mut ws: Vec<W> = Vec::with_capacity(slabs.len());
            let mut k: usize = 0;
            while k < slabs.len()
                invariant
                    s == slabs@,
                    all_ok::<E>(s),
                    0 <= k <= s.len(),
                    ws@.len() == k,
                    forall|j: int| 0 <= j < k ==> G::wrel(#[trigger] ws@[j], s[j]),
                decreases s.len() - k,
            {
                ws.push(G::compute::<I>(&slabs[k]));
                k += 1;
            }
            ws
        };
        proof {
            if acc {
                assert forall|j: int| 0 <= j < ws@.len() implies G::wvals(#[trigger] ws@[j], X::vals(s[j])) by {
                    if j < d.len() {
                        assert(attr.done_vals@[j] == X::vals(d[j]));
                        assert(s[j] == d[j]);
                    }
                }
            }
            assert forall|j: int| 0 <= j < ws@.len() implies #[trigger] G::wrel(ws@[j], s[j]) by {
                if acc {
                    G::lemma_wrel(ws@[j], s[j]);
                }
            }
            lemma_slab_total(s, len);
        }
        match G::load_check(ws.as_slice(), slabs.as_slice(), len, lo, hi) {
            Ok(()) => {},
            Err(e) => return Err(e),
        }
        match ColumnCore::<E, W, I, G>::from_slabs(slabs, ws, max_segments) {
            Some(c) => Ok(c),
            None => Err(wrong_length(len, usize::MAX - 3)),
        }
    }
}

/// The slabs' lengths' sum, all of them.
proof fn lemma_slab_total<T: Copy + Clone + std::fmt::Debug + Default>(s: Seq<Slab<T>>, len: usize)
    requires
        len == sum(slab_lens(s), 0, s.len() as int),
    ensures
        len == total(slab_lens(s)),
{
}

} // verus!
