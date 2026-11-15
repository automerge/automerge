//! What a value type's slabs are to the verified public API, verified
//! with Verus.
//!
//! A column's core is written against a slab model
//! ([`Slabs`]): run-length slabs for most value
//! types ([`RleModel`]), run counts for `bool` ([`BoolModel`]). [`Model`]
//! is what [`VerifiedColumn`](super::VerifiedColumn) needs of either, and
//! is proved of both: what a saved column is, that there is one for each
//! sequence of values, how long it can be, and a save and a load that
//! keep to it.
//!
//! The values here are the model's own. What a client reads and what its
//! specifications speak of is settled a layer up
//! ([`VGet`](super::VGet)): for a number the three are one type, for
//! bytes they are not.

// Verus proofs read more plainly with `x = x + n`.
#![allow(clippy::len_zero, clippy::assign_op_pattern)]

use super::codec::VCodec;
#[cfg(verus_keep_ghost)]
use super::counts::lemma_bool_size;
#[cfg(verus_keep_ghost)]
use super::value::{lemma_opt_sizes, lemma_plain_sizes};
use super::value::{VOptWrite, VPack, VWrite};
use crate::bool_edit::{BoolEdit, BoolLoader, BoolModel};
#[cfg(verus_keep_ghost)]
use crate::bool_unique::lemma_bool_slab_unique;
#[cfg(verus_keep_ghost)]
use crate::bool_wire::{bool_counts, bool_valid, bool_values, lemma_empty_valid, lemma_values_len};
use crate::bool_wire::{BSlab, BoolReads, CountsOf};
use crate::btree::SlabBTree;
use crate::column::Slab;
use crate::column_core::ColumnCore;
#[cfg(verus_keep_ghost)]
use crate::cursor::{all_ok, slab_lens, vals_of};
use crate::cursor::{ReadsAgree, Slabs, Visitor};
use crate::iter_core::{Decode, Decoders};
use crate::load_core::LoadCore;
use crate::rle::cut::Load;
#[cfg(verus_keep_ghost)]
use crate::rle::cut::{head_bound, lemma_slab_is_parse, small};
use crate::rle::dec::RleReads;
#[cfg(verus_keep_ghost)]
use crate::rle::dec::{
    lemma_empty_slab, lemma_groups_values_concat, slab_groups, slab_values, valid_slab,
};
use crate::rle::edit::{RleModel, RleRebuild, Visit};
use crate::rle::lift::Lift;
#[cfg(verus_keep_ghost)]
use crate::rle::model::{encs, group_bytes, groups_bytes, groups_len, parses, Group};
use crate::rle::model::{RleOps, WriteOps};
#[cfg(verus_keep_ghost)]
use crate::rle::read::tail_fits;
#[cfg(verus_keep_ghost)]
use crate::rle::size::{heads_bound, lemma_slab_size};
#[cfg(verus_keep_ghost)]
use crate::rle::state::copies;
use crate::rle::state::VOf;
#[cfg(verus_keep_ghost)]
use crate::rle::unique::lemma_slab_unique;
use crate::rle::RleTail;
use crate::weights::LenModel;
use crate::PackError;
use std::marker::PhantomData;
#[allow(unused_imports)]
use vstd::prelude::*;

verus! {

/// A column's core over the slab model `E`, as the verified public API
/// holds it: weighed by its length alone.
pub(crate) type Core<E> = ColumnCore<E, usize, SlabBTree<usize>, LenModel>;

/// A slab model whose columns the verified public API can hold, read
/// through the decoders `X`.
pub(crate) trait Model<X>: Slabs + Sized + for <'a>ReadsAgree<'a, X> where
    X: for <'a>Decoders<'a, Tail = <Self as Slabs>::Tail>,
 {
    /// A visitor of the model's rebuilds that looks at nothing.
    type Blind: Visitor<Self>;

    /// `d` is a saved column: the canonical encoding of some values.
    spec fn valid(d: Seq<u8>) -> bool;

    /// The values of the saved column `d`.
    spec fn values(d: Seq<u8>) -> Seq<Self::V>;

    /// No column of the values `vs` saves to more bytes than this.
    spec fn size_bound(vs: Seq<Self::V>) -> nat;

    /// Two saved columns of the same values are the same bytes.
    proof fn lemma_unique(x: Seq<u8>, y: Seq<u8>)
        requires
            Self::valid(x),
            Self::valid(y),
            Self::values(x) == Self::values(y),
        ensures
            x == y,
    ;

    /// A saved column is within the bound of its values.
    proof fn lemma_size(d: Seq<u8>)
        requires
            Self::valid(d),
        ensures
            d.len() <= Self::size_bound(Self::values(d)),
    ;

    /// No bytes are a saved column, of no values.
    proof fn lemma_none()
        ensures
            Self::valid(Seq::<u8>::empty()),
            Self::values(Seq::<u8>::empty()) == Seq::<Self::V>::empty(),
    ;

    /// Whether an insert of one value is compiled apart from a splice of
    /// a slice's values, or as a splice of a slice of one. It decides
    /// nothing but speed, and is whichever was measured to run fewer
    /// instructions for the model.
    const ONE_APART: bool;

    /// A visitor that has seen nothing.
    fn blind() -> Self::Blind
    ;

    /// A slab of `len` copies of the value `v`.
    fn fill<'s>(len: usize, v: <<X as Decoders<'s>>::D as Decode>::Item) -> (s: Slab<Self::Tail>)
        requires
            1 <= len <= i64::MAX,
            len < usize::MAX - 1,
        ensures
            Self::ok(s),
            Self::vals(s) == Seq::new(len as nat, |_i: int| <Self as ReadsAgree<'s, X>>::own(v)),
            s.len == len,
    ;

    /// The slabs' values, saved: appended to `out`. `total` is their
    /// item count.
    fn save(slabs: &[Slab<Self::Tail>], total: usize, out: &mut Vec<u8>)
        requires
            all_ok::<Self>(slabs@),
            total == crate::fenwick::total(slab_lens(slabs@)),
        ensures
            old(out)@.len() <= final(out)@.len(),
            final(out)@.take(old(out)@.len() as int) == old(out)@,
            ({
                let x = final(out)@.skip(old(out)@.len() as int);
                &&& Self::valid(x)
                &&& Self::values(x) == vals_of::<Self>(slabs@)
            }),
    ;

    /// The column saved as `bytes`, in slabs of at most `max_segments`
    /// runs, if it holds `expected` values (where given). An error
    /// unless `bytes` is a saved column of that many; and no error for
    /// one of that many short of 4 GiB by a header's length whose values
    /// a `usize` can count with three to spare.
    fn load(bytes: &[u8], max_segments: usize, expected: Option<usize>) -> (r: Result<Core<Self>, PackError>)
        requires
            max_segments >= 2,
        ensures
            r matches Ok(c) ==> {
                &&& c.ready()
                &&& c.budget() == max_segments
                &&& Self::valid(bytes@)
                &&& c@ == Self::values(bytes@)
                &&& expected matches Some(e) ==> c@.len() == e
            },
            ({
                &&& Self::valid(bytes@)
                &&& bytes@.len() <= 0xffff_fff5
                &&& Self::values(bytes@).len() <= usize::MAX - 3
                &&& expected matches Some(e) ==> Self::values(bytes@).len() == e
            }) ==> r is Ok,
    ;

    /// The saved column `bytes`, read through: how many values it holds
    /// and in how many runs. An error unless `bytes` is a saved column;
    /// and no error for one of less than 4 GiB (the last run of a longer
    /// one may be more than a slab can note of its tail).
    fn validate(bytes: &[u8]) -> (r: Result<(usize, usize), PackError>)
        ensures
            r matches Ok(n) ==> Self::valid(bytes@) && n.0 == Self::values(bytes@).len(),
            Self::valid(bytes@) && bytes@.len() <= u32::MAX ==> r is Ok,
    ;
}

// ── Visitors that look at nothing ───────────────────────────────────────────

/// A walk's visitor that looks at nothing: it only keeps count, for the
/// proof.
pub(crate) struct Blind<T> {
    #[allow(dead_code)]
    seen: Ghost<Seq<T>>,
}

impl<T> Visit<T> for Blind<T> {
    closed spec fn seen(&self) -> Seq<T> {
        self.seen@
    }

    #[inline(always)]
    #[allow(unused_variables)]  // `v` and `n` are for the spec
    fn visit(&mut self, v: T, n: usize) {
        *self = Blind { seen: Ghost(self.seen@ + copies(n as nat, v)) };
    }
}

/// A cursor's visitor that looks at nothing, over run-length slabs: a
/// splice of the verified public API has no use for the values it moves
/// over or deletes.
#[verifier::reject_recursive_types(L)]
pub struct NoVisit<L: for <'t>Lift<'t, W = L>> {
    #[allow(dead_code)]
    seen: Ghost<Seq<VOf<'static, L>>>,
}

impl<L: for <'t>Lift<'t, W = L>> Visitor<RleModel<L>> for NoVisit<L> {
    closed spec fn seen(&self) -> Seq<VOf<'static, L>> {
        self.seen@
    }

    #[inline(always)]
    fn pass<'s>(&mut self, e: &mut RleRebuild<L>, s: &'s Slab<RleTail>, n: usize, runs: usize) -> (left: usize) {
        let ghost a = RleModel::<L>::rb_read(*e) as int;
        let mut d = Blind::<VOf<'s, L>> { seen: Ghost(Seq::empty()) };
        let left = crate::rle::edit::pass::<L, Blind<VOf<'s, L>>>(e, s, n, runs, &mut d);
        *self = NoVisit { seen: Ghost(self.seen@ + RleModel::<L>::vals(*s).subrange(a, a + (n - left))) };
        left
    }

    #[inline(always)]
    fn discard<'s>(&mut self, e: &mut RleRebuild<L>, s: &'s Slab<RleTail>, n: usize) {
        let ghost a = RleModel::<L>::rb_read(*e) as int;
        let mut d = Blind::<VOf<'s, L>> { seen: Ghost(Seq::empty()) };
        crate::rle::edit::discard::<L, Blind<VOf<'s, L>>>(e, s, n, &mut d);
        *self = NoVisit { seen: Ghost(self.seen@ + RleModel::<L>::vals(*s).subrange(a, a + n)) };
    }

    #[inline(always)]
    #[allow(unused_variables)]  // `s` is for the spec
    fn dropped(&mut self, s: &Slab<RleTail>) {
        *self = NoVisit { seen: Ghost(self.seen@ + RleModel::<L>::vals(*s)) };
    }
}

/// A cursor's visitor that looks at nothing, over `bool` slabs.
#[verifier::reject_recursive_types(C)]
pub struct NoVisitBool<C> {
    #[allow(dead_code)]
    seen: Ghost<Seq<bool>>,
    _p: PhantomData<C>,
}

impl<C: CountsOf> Visitor<BoolModel<C>> for NoVisitBool<C> {
    closed spec fn seen(&self) -> Seq<bool> {
        self.seen@
    }

    #[inline(always)]
    #[allow(unused_variables)]  // a `bool` slab's walk has no budget of runs
    fn pass(&mut self, e: &mut BoolEdit<C>, s: &BSlab, n: usize, runs: usize) -> (left: usize) {
        let ghost a = BoolModel::<C>::rb_read(*e) as int;
        let mut d = Blind::<bool> { seen: Ghost(Seq::empty()) };
        let left = crate::bool_edit::pass::<C, Blind<bool>>(e, s, n, &mut d);
        *self = NoVisitBool {
            seen: Ghost(self.seen@ + BoolModel::<C>::vals(*s).subrange(a, a + (n - left))),
            _p: PhantomData,
        };
        left
    }

    #[inline(always)]
    fn discard(&mut self, e: &mut BoolEdit<C>, s: &BSlab, n: usize) {
        let ghost a = BoolModel::<C>::rb_read(*e) as int;
        let mut d = Blind::<bool> { seen: Ghost(Seq::empty()) };
        crate::bool_edit::discard::<C, Blind<bool>>(e, s, n, &mut d);
        *self = NoVisitBool { seen: Ghost(self.seen@ + BoolModel::<C>::vals(*s).subrange(a, a + n)), _p: PhantomData };
    }

    #[inline(always)]
    #[allow(unused_variables)]  // `s` is for the spec
    fn dropped(&mut self, s: &BSlab) {
        *self = NoVisitBool { seen: Ghost(self.seen@ + BoolModel::<C>::vals(*s)), _p: PhantomData };
    }
}

// ── Run-length slabs ────────────────────────────────────────────────────────

/// A writer whose values are owned as they are read, with headers and
/// counts no longer than ten bytes.
pub(crate) trait Plain: for <'s>Lift<'s, W = Self> {
    /// A value read is the value written.
    proof fn lemma_plain(v: VOf<'static, Self>)
        ensures
            <Self as Lift<'static>>::lift(v) == v,
    ;

    /// No header and no count is longer than ten bytes.
    proof fn lemma_heads()
        ensures
            heads_bound::<<Self as WriteOps<'static>>::R>(10),
    ;
}

impl<T: VPack, C: VCodec> Plain for VWrite<T, C> {
    proof fn lemma_plain(v: T) {
    }

    proof fn lemma_heads() {
        lemma_plain_sizes::<T, C>(Seq::empty());
    }
}

impl<T: VPack, C: VCodec> Plain for VOptWrite<T, C> {
    proof fn lemma_plain(v: Option<T>) {
    }

    proof fn lemma_heads() {
        lemma_opt_sizes::<T, C>(Seq::empty());
    }
}

impl<L, O> Model<RleReads<O>> for RleModel<L> where
    L: Plain + for <'s>Lift<'s, W = L> + for <'s>WriteOps<'s, R = O>,
    O: for <'b>RleOps<'b>,
 {
    type Blind = NoVisit<L>;

    /// The code that appends a run to a slab is compiled into the one
    /// splice that calls it. A second copy of the splice would be a
    /// second caller, and leave it out of line for both: a column built
    /// by one splice would then cost up to a quarter more.
    const ONE_APART: bool = false;

    open spec fn valid(d: Seq<u8>) -> bool {
        valid_slab::<O>(d)
    }

    open spec fn values(d: Seq<u8>) -> Seq<VOf<'static, L>> {
        slab_values::<O>(d)
    }

    /// The values' own bytes, and for each a header and a count of ten
    /// bytes.
    open spec fn size_bound(vs: Seq<VOf<'static, L>>) -> nat {
        20 * vs.len() + encs::<O>(vs).len()
    }

    proof fn lemma_unique(x: Seq<u8>, y: Seq<u8>) {
        lemma_slab_unique::<O>(x, y);
    }

    proof fn lemma_size(d: Seq<u8>) {
        let vs = slab_values::<O>(d);
        L::lemma_heads();
        lemma_slab_size::<O>(d, 10);
        assert(2 * 10 * vs.len() == 20 * vs.len()) by (nonlinear_arith);
    }

    proof fn lemma_none() {
        lemma_empty_slab::<O>();
    }

    #[inline(always)]
    fn blind() -> NoVisit<L> {
        NoVisit { seen: Ghost(Seq::empty()) }
    }

    #[inline(always)]
    fn fill<'s>(len: usize, v: VOf<'s, L>) -> (s: Slab<RleTail>) {
        let s = crate::rle::edit::fill::<L>(<L as WriteOps<'s>>::cow_ref(v), len);
        proof {
            let o = <L as Lift<'s>>::lift(v);
            assert(copies(len as nat, o) =~= Seq::new(len as nat, |_i: int| o));
        }
        s
    }

    #[inline(always)]
    fn save(slabs: &[Slab<RleTail>], total: usize, out: &mut Vec<u8>) {
        crate::rle::edit::save_slabs::<L>(slabs, total, out)
    }

    #[inline(always)]
    fn load(bytes: &[u8], max_segments: usize, expected: Option<usize>) -> (r: Result<Core<Self>, PackError>) {
        let ld: LoadCore<'_, RleModel<L>, RleReads<O>, usize, LenModel, Load<'_, L>> = LoadCore::start(
            bytes,
            max_segments,
        );
        match ld.finish::<SlabBTree<usize>>(max_segments, expected, i64::MIN, i64::MAX) {
            Ok(core) => {
                proof {
                    let o = slab_values::<O>(bytes@);
                    assert(core@.len() == o.len());
                    assert forall|i: int| 0 <= i < o.len() implies core@[i] == #[trigger] o[i] by {
                        L::lemma_plain(o[i]);
                    }
                    assert(core@ =~= o);
                }
                Ok(core)
            },
            Err(e) => {
                proof {
                    if valid_slab::<O>(bytes@) && bytes@.len() <= 0xffff_fff5 && slab_values::<O>(bytes@).len()
                        <= usize::MAX - 3 && (expected matches Some(e) ==> slab_values::<O>(bytes@).len() == e) {
                        L::lemma_heads();
                        assert(head_bound::<O>(10));
                        assert(small::<O>(bytes@));
                    }
                }
                Err(e)
            },
        }
    }

    #[inline(always)]
    fn validate(bytes: &[u8]) -> (r: Result<(usize, usize), PackError>) {
        rle_validate::<O>(bytes)
    }
}

/// A run-length slab read through
/// ([`validate`](crate::rle::read::validate)), in the terms of a saved
/// column: the number of its values and of its runs.
#[inline(always)]
fn rle_validate<'a, O: RleOps<'a>>(data: &'a [u8]) -> (r: Result<(usize, usize), PackError>)
    ensures
        r matches Ok(n) ==> valid_slab::<O>(data@) && n.0 == slab_values::<O>(data@).len(),
        valid_slab::<O>(data@) && data@.len() <= u32::MAX ==> r is Ok,
{
    match crate::rle::read::validate::<O>(data) {
        Ok(s) => {
            proof {
                // the parse the check found is the slab's
                let gs = choose|gs: Seq<Group<O::V>>| parses::<O>(gs, data@) && s.len == groups_len(gs);
                lemma_slab_is_parse::<O>(gs, data@);
                lemma_groups_values_concat(gs, Seq::empty(), O::null());
            }
            Ok((s.len, s.segments))
        },
        Err(e) => {
            proof {
                if valid_slab::<O>(data@) && data@.len() <= u32::MAX {
                    // the last group is part of the bytes, so as short
                    let gs = slab_groups::<O>(data@);
                    assert(parses::<O>(gs, data@));
                    reveal(parses);
                    if gs.len() > 0 {
                        assert(groups_bytes::<O>(gs) == groups_bytes::<O>(gs.drop_last()) + group_bytes::<O>(gs.last()));
                    }
                    assert(tail_fits::<O>(gs));
                }
            }
            Err(e)
        },
    }
}

// ── `bool` slabs ────────────────────────────────────────────────────────────

impl<C: CountsOf> Model<BoolReads<C>> for BoolModel<C> {
    type Blind = NoVisitBool<C>;

    /// An insert of one value compiled for that alone is a little
    /// shorter, and costs a splice of many nothing.
    const ONE_APART: bool = true;

    open spec fn valid(d: Seq<u8>) -> bool {
        bool_valid::<C::K>(d)
    }

    open spec fn values(d: Seq<u8>) -> Seq<bool> {
        bool_values::<C::K>(d)
    }

    /// Ten bytes a run, which is at least one value but for the first.
    open spec fn size_bound(vs: Seq<bool>) -> nat {
        10 * vs.len() + 10
    }

    proof fn lemma_unique(x: Seq<u8>, y: Seq<u8>) {
        lemma_bool_slab_unique::<C::K>(x, y);
    }

    proof fn lemma_size(d: Seq<u8>) {
        lemma_bool_size::<C::K>(d);
    }

    proof fn lemma_none() {
        lemma_empty_valid::<C::K>();
    }

    #[inline(always)]
    fn blind() -> NoVisitBool<C> {
        NoVisitBool { seen: Ghost(Seq::empty()), _p: PhantomData }
    }

    #[inline(always)]
    fn fill<'s>(len: usize, v: bool) -> (s: BSlab) {
        crate::bool_edit::fill::<C>(len, v)
    }

    #[inline(always)]
    fn save(slabs: &[BSlab], total: usize, out: &mut Vec<u8>) {
        crate::bool_edit::save_slabs::<C>(slabs, total, out)
    }

    #[inline(always)]
    fn load(bytes: &[u8], max_segments: usize, expected: Option<usize>) -> (r: Result<Core<Self>, PackError>) {
        let ld: LoadCore<'_, BoolModel<C>, BoolReads<C>, usize, LenModel, BoolLoader<'_, C>> = LoadCore::start(
            bytes,
            max_segments,
        );
        match ld.finish::<SlabBTree<usize>>(max_segments, expected, i64::MIN, i64::MAX) {
            Ok(core) => {
                proof {
                    let o = bool_values::<C::K>(bytes@);
                    assert(o.map_values(|v: bool| v) =~= o);
                }
                Ok(core)
            },
            Err(e) => Err(e),
        }
    }

    #[inline(always)]
    fn validate(bytes: &[u8]) -> (r: Result<(usize, usize), PackError>) {
        match crate::bool_wire::validate::<C::K>(bytes) {
            Ok(i) => {
                proof {
                    lemma_values_len(bool_counts::<C::K>(bytes@));
                }
                Ok((i.len, i.segments))
            },
            Err(e) => Err(e),
        }
    }
}

} // verus!
