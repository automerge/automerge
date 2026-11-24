//! What a value type's slabs are to the verified public API, verified
//! with Verus.
//!
//! A column's core is written against a slab model
//! ([`Slabs`]): run-length slabs for most value
//! types ([`RleModel`]), run counts for `bool` ([`BoolModel`]). [`Model`]
//! is what [`Column`](super::Column) needs of either, and
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

use super::codec::Codec;
#[cfg(verus_keep_ghost)]
use super::counts::lemma_bool_size;
#[cfg(verus_keep_ghost)]
use super::value::{lemma_opt_sizes, lemma_plain_sizes};
use super::value::{RleValue, VOptWrite, VWrite};
#[cfg(verus_keep_ghost)]
use crate::bool_edit::run_of;
use crate::bool_edit::{BoolEdit, BoolLoader, BoolModel, BoolRecoder};
use crate::bool_enc::{BEncoder, BStream};
#[cfg(verus_keep_ghost)]
use crate::bool_unique::lemma_bool_slab_unique;
#[cfg(verus_keep_ghost)]
use crate::bool_wire::{
    bool_counts, bool_valid, bool_values, bslabs_values, lemma_empty_valid, lemma_values_len,
};
use crate::bool_wire::{BSlab, BoolDecoder, BoolReads, CountsOf};
use crate::btree::{SlabAggregate, SlabBTree};
use crate::column::Slab;
use crate::column_core::ColumnCore;
#[cfg(verus_keep_ghost)]
use crate::cursor::{all_ok, lemma_vals_total, lemma_vals_unit, slab_lens, vals_of};
use crate::cursor::{ReadsAgree, Slabs, Visitor};
use crate::index::IndexOps;
use crate::iter_core::{Decode, Decoders};
use crate::load_core::{LoadCore, Loads};
use crate::remap::Encodes;
use crate::rle::cut::Load;
#[cfg(verus_keep_ghost)]
use crate::rle::cut::{head_bound, lemma_slab_is_parse, small};
#[cfg(verus_keep_ghost)]
use crate::rle::dec::{
    lemma_empty_slab, lemma_groups_values_concat, slab_groups, slab_values, valid_slab,
};
use crate::rle::dec::{RDecoder, RleReads};
#[cfg(verus_keep_ghost)]
use crate::rle::edit::lemma_valid_of;
use crate::rle::edit::{RleModel, RleRebuild, Visit};
use crate::rle::enc::{REncoder, RStream};
use crate::rle::lift::Lift;
#[cfg(verus_keep_ghost)]
use crate::rle::lift::{lemma_lift_encodes, lift_seq};
#[cfg(verus_keep_ghost)]
use crate::rle::model::{encs, group_bytes, groups_bytes, groups_len, parses, Group};
use crate::rle::model::{RleOps, WriteOps};
#[cfg(verus_keep_ghost)]
use crate::rle::postfix::encodes;
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
use crate::weights::{LenModel, LoadWeigh};
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

    /// The value a reader of the slab `s` stands at, where it stands at
    /// the slab's `k`th value: as a read gives it, borrowing the slab.
    fn peek<'s>(s: &'s Slab<Self::Tail>, st: &<Self as Slabs>::State, k: Ghost<nat>) -> (v: <<X as Decoders<
        's,
    >>::D as Decode>::Item)
        requires
            Self::ok(*s),
            Self::st_at(s.data@, *st, k@),
            k@ < Self::vals(*s).len(),
        ensures
            <Self as ReadsAgree<'s, X>>::own(v) == Self::vals(*s)[k@ as int],
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

    /// The column saved as `bytes`, weighed by the model `G`, in slabs
    /// of at most `max_segments` runs, if it holds `expected` values
    /// (where given). An error unless `bytes` is a saved column of that
    /// many; and no error for one of that many short of 4 GiB by a
    /// header's length whose values a `usize` can count with three to
    /// spare, and which the weight model does not refuse. The model's
    /// checks are made with the domain `lo..=hi` (a delta column's),
    /// and the column loaded is what they say (`LoadWeigh::bounded`).
    fn load<'a, W: SlabAggregate, I: IndexOps<W>, G: LoadWeigh<'a, Self, X, W>>(
        bytes: &'a [u8],
        max_segments: usize,
        expected: Option<usize>,
        lo: i64,
        hi: i64,
    ) -> (r: Result<ColumnCore<Self, W, I, G>, PackError>)
        requires
            max_segments >= 2,
        ensures
            r matches Ok(c) ==> {
                &&& c.ready()
                &&& c.budget() == max_segments
                &&& Self::valid(bytes@)
                &&& c@ == Self::values(bytes@)
                &&& expected matches Some(e) ==> c@.len() == e
                &&& G::bounded(c.slabs_spec(), lo, hi)
            },
            ({
                &&& Self::valid(bytes@)
                &&& bytes@.len() <= 0xffff_fff5
                &&& Self::values(bytes@).len() <= usize::MAX - 3
                &&& expected matches Some(e) ==> Self::values(bytes@).len() == e
                &&& !G::may_refuse(Self::values(bytes@))
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

    /// A decoder on the saved column `d`, as they are that read a slab:
    /// it has all the column's values to give.
    fn decoder<'s>(d: &'s [u8]) -> (r: <X as Decoders<'s>>::D)
        requires
            Self::valid(d@),
        ensures
            r.inv(),
            r.rest().map_values(|v: <<X as Decoders<'s>>::D as Decode>::Item| <Self as ReadsAgree<'s, X>>::own(v))
                == Self::values(d@),
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
    pub(crate) seen: Ghost<Seq<VOf<'static, L>>>,
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

    /// Owning tells values apart: no two have one owned copy.
    proof fn lemma_lift_inj<'s>(v: VOf<'s, Self>, w: VOf<'s, Self>)
        ensures
            <Self as Lift<'s>>::lift(v) == <Self as Lift<'s>>::lift(w) ==> v == w,
    ;
}

impl<T: RleValue, C: Codec> Plain for VWrite<T, C> {
    proof fn lemma_plain(v: T) {
    }

    proof fn lemma_heads() {
        lemma_plain_sizes::<T, C>(Seq::empty());
    }

    proof fn lemma_lift_inj<'s>(v: T, w: T) {
    }
}

impl<T: RleValue, C: Codec> Plain for VOptWrite<T, C> {
    proof fn lemma_plain(v: Option<T>) {
    }

    proof fn lemma_heads() {
        lemma_opt_sizes::<T, C>(Seq::empty());
    }

    proof fn lemma_lift_inj<'s>(v: Option<T>, w: Option<T>) {
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
    fn peek<'s>(s: &'s Slab<RleTail>, st: &crate::rle::dec::RleDecoderState, k: Ghost<nat>) -> (v: VOf<'s, L>) {
        crate::rle::edit::peek_at::<L>(s, st, k)
    }

    #[inline(always)]
    fn save(slabs: &[Slab<RleTail>], total: usize, out: &mut Vec<u8>) {
        crate::rle::edit::save_slabs::<L>(slabs, total, out)
    }

    #[inline(always)]
    fn load<'a, W: SlabAggregate, I: IndexOps<W>, G: LoadWeigh<'a, Self, RleReads<O>, W>>(
        bytes: &'a [u8],
        max_segments: usize,
        expected: Option<usize>,
        lo: i64,
        hi: i64,
    ) -> (r: Result<ColumnCore<Self, W, I, G>, PackError>) {
        let ld: LoadCore<'a, RleModel<L>, RleReads<O>, W, G, Load<'a, L>> = LoadCore::start(
            bytes,
            max_segments,
        );
        proof {
            // what the column holds of the values read is those values
            let o = slab_values::<O>(bytes@);
            let m = o.map_values(
                |v: <O as RleOps<'a>>::V| <RleModel<L> as ReadsAgree<'a, RleReads<O>>>::own(v),
            );
            assert forall|i: int| 0 <= i < o.len() implies m[i] == #[trigger] o[i] by {
                L::lemma_plain(o[i]);
            }
            assert(m =~= o);
        }
        match ld.finish::<I>(max_segments, expected, lo, hi) {
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
                        <= usize::MAX - 3 && (expected matches Some(e) ==> slab_values::<O>(bytes@).len() == e)
                        && !G::may_refuse(slab_values::<O>(bytes@)) {
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

    #[inline(always)]
    fn decoder<'s>(d: &'s [u8]) -> (r: RDecoder<'s, O>) {
        let r = RDecoder::on(d);
        proof {
            let o = slab_values::<O>(d@);
            let m = r.rest().map_values(
                |v: <O as RleOps<'s>>::V| <RleModel<L> as ReadsAgree<'s, RleReads<O>>>::own(v),
            );
            assert forall|i: int| 0 <= i < o.len() implies m[i] == #[trigger] o[i] by {
                L::lemma_plain(o[i]);
            }
            assert(m =~= o);
        }
        r
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
    fn peek(s: &BSlab, st: &crate::bool_wire::BoolDecoderState, k: Ghost<nat>) -> (v: bool) {
        crate::bool_edit::peek_slab::<C>(s, st, k)
    }

    #[inline(always)]
    fn save(slabs: &[BSlab], total: usize, out: &mut Vec<u8>) {
        crate::bool_edit::save_slabs::<C>(slabs, total, out)
    }

    #[inline(always)]
    fn load<'a, W: SlabAggregate, I: IndexOps<W>, G: LoadWeigh<'a, Self, BoolReads<C>, W>>(
        bytes: &'a [u8],
        max_segments: usize,
        expected: Option<usize>,
        lo: i64,
        hi: i64,
    ) -> (r: Result<ColumnCore<Self, W, I, G>, PackError>) {
        let ld: LoadCore<'a, BoolModel<C>, BoolReads<C>, W, G, BoolLoader<'a, C>> = LoadCore::start(
            bytes,
            max_segments,
        );
        proof {
            let o = bool_values::<C::K>(bytes@);
            assert(o.map_values(|v: bool| v) =~= o);
            assert(o.map_values(|v: bool| <BoolModel<C> as ReadsAgree<'a, BoolReads<C>>>::own(v)) =~= o);
        }
        match ld.finish::<I>(max_segments, expected, lo, hi) {
            Ok(core) => Ok(core),
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

    #[inline(always)]
    fn decoder<'s>(d: &'s [u8]) -> (r: BoolDecoder<'s, C>) {
        let r = BoolDecoder::on(d);
        proof {
            let o = bool_values::<C::K>(d@);
            assert(o.map_values(|v: bool| v) =~= o);
        }
        r
    }
}

// ── Encoders ────────────────────────────────────────────────────────────────

/// A slab model with an encoder that takes values one after another,
/// in the form `X` reads them in for the lifetime `'a`, and gives the
/// saved column of them.
pub(crate) trait Streams<'a, X>: Model<X> where
    X: for <'b>Decoders<'b, Tail = <Self as Slabs>::Tail>,
 {
    /// The encoder. What it keeps, what it has been given, and how it
    /// takes a run are [`Encodes`]'s to say.
    type Enc: Encodes<Self>;

    /// The encoder's segment budget, if it has one: it cuts what it is
    /// given into slabs within it.
    spec fn budget(e: Self::Enc) -> Option<usize>;

    /// The encoder has cut no slab: it holds what it was given in one
    /// piece.
    spec fn whole(e: Self::Enc) -> bool;

    /// An encoder that has been given nothing. It has no segment
    /// budget, so it cuts no slab.
    fn fresh() -> (e: Self::Enc)
        ensures
            e.e_inv(),
            e.e_values().len() == 0,
            Self::budget(e) is None,
            Self::whole(e),
    ;

    /// An encoder that has been given nothing, with the segment budget
    /// `max`. (That a budget is at least 2 is the public constructor's
    /// to require: nothing here depends on it.)
    fn budgeted(max: usize) -> (e: Self::Enc)
        ensures
            e.e_inv(),
            e.e_values().len() == 0,
            Self::budget(e) == Some(max),
    ;

    /// The encoder's segment budget.
    fn budget_of(e: &Self::Enc) -> (r: Option<usize>)
        ensures
            r == Self::budget(*e),
    ;

    /// Give the encoder `n` copies of `x`: [`Encodes::put`], which also
    /// keeps the budget, and without one cuts no slab.
    fn give(e: &mut Self::Enc, x: <Self::Enc as Encodes<Self>>::In, n: usize)
        requires
            old(e).e_inv(),
        ensures
            final(e).e_inv(),
            final(e).e_values() == old(e).e_values() + Seq::new(
                n as nat,
                |_i: int| <Self::Enc as Encodes<Self>>::val(x),
            ),
            Self::budget(*final(e)) == Self::budget(*old(e)),
            Self::budget(*old(e)) is None && Self::whole(*old(e)) ==> Self::whole(*final(e)),
    ;

    /// Whether the encoder has cut no slab and every value it was given
    /// is `x` (or it was given none): true only if every value is, and
    /// true if every value is and no slab was cut.
    fn all_are(e: &Self::Enc, x: &<Self::Enc as Encodes<Self>>::In) -> (r: bool)
        requires
            e.e_inv(),
        ensures
            r ==> e.e_values() == Seq::new(e.e_values().len(), |_i: int| <Self::Enc as Encodes<Self>>::val(*x)),
            Self::whole(*e) && e.e_values() == Seq::new(
                e.e_values().len(),
                |_i: int| <Self::Enc as Encodes<Self>>::val(*x),
            ) ==> r,
    ;

    /// How many values the encoder has been given.
    fn count(e: &Self::Enc) -> (n: usize)
        requires
            e.e_inv(),
        ensures
            n == e.e_values().len(),
    ;

    /// A value as it is read, for the encoder to take: it takes what
    /// the value is owned.
    fn given(v: <<X as Decoders<'a>>::D as Decode>::Item) -> (x: <Self::Enc as Encodes<Self>>::In)
        ensures
            <Self::Enc as Encodes<Self>>::val(x) == <Self as ReadsAgree<'a, X>>::own(v),
    ;

    /// The saved column of the values the encoder was given.
    fn bytes(e: Self::Enc) -> (r: Vec<u8>)
        requires
            e.e_inv(),
        ensures
            Self::valid(r@),
            Self::values(r@) == e.e_values(),
    ;

    /// The saved column of the values the encoder was given, appended
    /// to `out`.
    fn write(e: Self::Enc, out: &mut Vec<u8>)
        requires
            e.e_inv(),
        ensures
            old(out)@.len() <= final(out)@.len(),
            final(out)@.take(old(out)@.len() as int) == old(out)@,
            ({
                let x = final(out)@.skip(old(out)@.len() as int);
                &&& Self::valid(x)
                &&& Self::values(x) == e.e_values()
            }),
    ;
}

impl<'a, L, O> Streams<'a, RleReads<O>> for RleModel<L> where
    L: Plain + for <'s>Lift<'s, W = L> + for <'s>WriteOps<'s, R = O>,
    O: for <'b>RleOps<'b>,
 {
    type Enc = REncoder<'a, L>;

    open spec fn budget(e: REncoder<'a, L>) -> Option<usize> {
        e.max_segments
    }

    open spec fn whole(e: REncoder<'a, L>) -> bool {
        e.slabs@.len() == 0
    }

    #[inline(always)]
    fn fresh() -> (e: REncoder<'a, L>) {
        REncoder::new()
    }

    #[inline(always)]
    fn budgeted(max: usize) -> (e: REncoder<'a, L>) {
        let mut e = REncoder::new();
        e.max_segments = Some(max);
        e
    }

    #[inline(always)]
    fn budget_of(e: &REncoder<'a, L>) -> (r: Option<usize>) {
        e.max_segments
    }

    #[inline(always)]
    fn give(e: &mut REncoder<'a, L>, x: <L as WriteOps<'a>>::Cow, n: usize) {
        e.append(x, n);
        proof {
            assert(lift_seq::<L>(copies(n as nat, <L as WriteOps<'a>>::val(x))) =~= Seq::new(
                n as nat,
                |_i: int| <L as Lift<'a>>::lift(<L as WriteOps<'a>>::val(x)),
            ));
        }
    }

    #[inline(always)]
    fn all_are(e: &REncoder<'a, L>, x: &<L as WriteOps<'a>>::Cow) -> (r: bool) {
        let r = e.single_run_of(x);
        proof {
            let v = <L as WriteOps<'a>>::val(*x);
            let lv = <L as Lift<'a>>::lift(v);
            let n = e.values().len();
            lemma_vals_unit::<RleModel<L>>(arbitrary());
            if e.slabs@.len() == 0 {
                assert(e.slabs@ =~= Seq::empty());
                assert(e.values() =~= lift_seq::<L>(e.cur@));
                if r {
                    assert(e.values() =~= Seq::new(n, |_i: int| lv));
                }
                if e.values() == Seq::new(n, |_i: int| lv) {
                    assert forall|i: int| 0 <= i < e.cur@.len() implies e.cur@[i] == v by {
                        assert(lift_seq::<L>(e.cur@)[i] == lv);
                        <L as Plain>::lemma_lift_inj(e.cur@[i], v);
                    }
                    assert(e.cur@ =~= copies(e.cur@.len(), v));
                }
            }
        }
        r
    }

    #[inline(always)]
    fn count(e: &REncoder<'a, L>) -> (n: usize) {
        e.len
    }

    #[inline(always)]
    fn given(v: VOf<'a, L>) -> (x: <L as WriteOps<'a>>::Cow) {
        <L as WriteOps<'a>>::cow_ref(v)
    }

    #[inline(always)]
    fn bytes(e: REncoder<'a, L>) -> (r: Vec<u8>) {
        let mut e = e;
        if e.slabs.len() == 0 {
            // no slab was cut: the bytes in hand are the column
            let ghost cur = e.cur@;
            let r = e.take_bytes();
            proof {
                let segs = choose|segs: int| encodes::<<L as WriteOps<'a>>::R>(r@, cur, segs);
                lemma_lift_encodes::<L>(r@, cur, segs);
                lemma_valid_of::<O>(r@, lift_seq::<L>(cur), segs);
            }
            return r;
        }
        let total = e.len;
        let slabs = e.take_slabs();
        let mut out: Vec<u8> = Vec::new();
        crate::rle::edit::save_slabs::<L>(slabs.as_slice(), total, &mut out);
        proof {
            assert(out@.skip(0) =~= out@);
        }
        out
    }

    #[inline(always)]
    fn write(e: REncoder<'a, L>, out: &mut Vec<u8>) {
        let mut e = e;
        if e.slabs.len() == 0 {
            let ghost cur = e.cur@;
            let ghost o0 = out@;
            let r = e.take_bytes();
            out.extend_from_slice(r.as_slice());
            proof {
                let segs = choose|segs: int| encodes::<<L as WriteOps<'a>>::R>(r@, cur, segs);
                lemma_lift_encodes::<L>(r@, cur, segs);
                lemma_valid_of::<O>(r@, lift_seq::<L>(cur), segs);
                assert(out@.skip(o0.len() as int) =~= r@);
                assert(out@.take(o0.len() as int) =~= o0);
            }
            return ;
        }
        let total = e.len;
        let slabs = e.take_slabs();
        crate::rle::edit::save_slabs::<L>(slabs.as_slice(), total, out);
    }
}

impl<'a, C: CountsOf> Streams<'a, BoolReads<C>> for BoolModel<C> {
    type Enc = BoolRecoder<C>;

    open spec fn budget(e: BoolRecoder<C>) -> Option<usize> {
        e.0.max_segments
    }

    open spec fn whole(e: BoolRecoder<C>) -> bool {
        e.0.slabs@.len() == 0
    }

    #[inline(always)]
    fn fresh() -> (e: BoolRecoder<C>) {
        BoolRecoder(BEncoder::new::<C::K>(None), PhantomData)
    }

    #[inline(always)]
    fn budgeted(max: usize) -> (e: BoolRecoder<C>) {
        BoolRecoder(BEncoder::new::<C::K>(Some(max)), PhantomData)
    }

    #[inline(always)]
    fn budget_of(e: &BoolRecoder<C>) -> (r: Option<usize>) {
        e.0.max_segments
    }

    #[inline(always)]
    fn give(e: &mut BoolRecoder<C>, x: bool, n: usize) {
        e.0.append::<C::K>(x, n);
        proof {
            assert(run_of(n as nat, x) =~= Seq::new(n as nat, |_i: int| x));
        }
    }

    #[inline(always)]
    fn all_are(e: &BoolRecoder<C>, x: &bool) -> (r: bool) {
        if e.0.slabs.len() != 0 {
            return false;
        }
        proof {
            assert(e.0.slabs@ =~= Seq::<BSlab>::empty());
            assert(bslabs_values::<C::K>(Seq::<BSlab>::empty()) =~= Seq::<bool>::empty());
            assert(e.0.values::<C::K>() =~= e.0.state.vals@);
        }
        let r = e.0.state.all_equal_pre_finish(*x);
        proof {
            let vs = e.0.state.vals@;
            if r {
                assert(vs =~= Seq::new(vs.len(), |_i: int| *x));
            }
        }
        r
    }

    #[inline(always)]
    fn count(e: &BoolRecoder<C>) -> (n: usize) {
        e.0.state.len
    }

    #[inline(always)]
    fn given(v: bool) -> (x: bool) {
        v
    }

    #[inline(always)]
    fn bytes(e: BoolRecoder<C>) -> (r: Vec<u8>) {
        let mut e = e;
        if e.0.slabs.len() == 0 {
            return e.0.into_bytes::<C::K>();
        }
        let total = e.0.state.len;
        let slabs = <BoolRecoder<C> as Encodes<BoolModel<C>>>::take(&mut e);
        proof {
            lemma_vals_total::<BoolModel<C>>(slabs@);
        }
        let mut out: Vec<u8> = Vec::new();
        crate::bool_edit::save_slabs::<C>(slabs.as_slice(), total, &mut out);
        proof {
            assert(out@.skip(0) =~= out@);
        }
        out
    }

    #[inline(always)]
    fn write(e: BoolRecoder<C>, out: &mut Vec<u8>) {
        let mut e = e;
        if e.0.slabs.len() == 0 {
            e.0.write_to::<C::K>(out);
            return ;
        }
        let total = e.0.state.len;
        let slabs = <BoolRecoder<C> as Encodes<BoolModel<C>>>::take(&mut e);
        proof {
            lemma_vals_total::<BoolModel<C>>(slabs@);
        }
        crate::bool_edit::save_slabs::<C>(slabs.as_slice(), total, out);
    }
}

/// A model's stream into a buffer of its caller's: nothing is held but
/// the run in hand, and what is written goes after what the buffer had.
pub(crate) trait Flows<'a, X>: Streams<'a, X> where
    X: for <'b>Decoders<'b, Tail = <Self as Slabs>::Tail>,
 {
    type Flow;

    /// `buf` is the buffer the stream is writing into.
    spec fn f_on(s: Self::Flow, buf: Seq<u8>) -> bool;

    /// What `buf` held before the stream wrote to it.
    spec fn f_head(s: Self::Flow, buf: Seq<u8>) -> Seq<u8>;

    spec fn f_values(s: Self::Flow) -> Seq<Self::V>;

    proof fn lemma_f_head(s: Self::Flow, buf: Seq<u8>)
        requires
            Self::f_on(s, buf),
        ensures
            Self::f_head(s, buf).len() <= buf.len(),
            buf.take(Self::f_head(s, buf).len() as int) == Self::f_head(s, buf),
    ;

    fn f_new() -> (s: Self::Flow)
        ensures
            Self::f_values(s).len() == 0,
            forall|buf: Seq<u8>| #[trigger] Self::f_on(s, buf),
            forall|buf: Seq<u8>| #[trigger] Self::f_head(s, buf) == buf,
    ;

    fn f_give(s: &mut Self::Flow, buf: &mut Vec<u8>, x: <Self::Enc as Encodes<Self>>::In, n: usize)
        requires
            Self::f_on(*old(s), old(buf)@),
        ensures
            Self::f_on(*final(s), final(buf)@),
            Self::f_head(*final(s), final(buf)@) == Self::f_head(*old(s), old(buf)@),
            Self::f_values(*final(s)) == Self::f_values(*old(s)) + Seq::new(
                n as nat,
                |_i: int| <Self::Enc as Encodes<Self>>::val(x),
            ),
    ;

    fn f_all_are(s: &Self::Flow, x: &<Self::Enc as Encodes<Self>>::In) -> (r: bool)
        requires
            exists|buf: Seq<u8>| Self::f_on(*s, buf),
        ensures
            r == (Self::f_values(*s) == Seq::new(
                Self::f_values(*s).len(),
                |_i: int| <Self::Enc as Encodes<Self>>::val(*x),
            )),
    ;

    fn f_count(s: &Self::Flow) -> (n: usize)
        requires
            exists|buf: Seq<u8>| Self::f_on(*s, buf),
        ensures
            n == Self::f_values(*s).len(),
    ;

    fn f_finish(s: Self::Flow, buf: &mut Vec<u8>)
        requires
            Self::f_on(s, old(buf)@),
        ensures
            ({
                let h = Self::f_head(s, old(buf)@);
                let d = final(buf)@.skip(h.len() as int);
                &&& h.len() <= final(buf)@.len()
                &&& final(buf)@.take(h.len() as int) == h
                &&& Self::valid(d)
                &&& Self::values(d) == Self::f_values(s)
            }),
    ;
}

impl<'a, L, O> Flows<'a, RleReads<O>> for RleModel<L> where
    L: Plain + for <'s>Lift<'s, W = L> + for <'s>WriteOps<'s, R = O>,
    O: for <'b>RleOps<'b>,
 {
    type Flow = RStream<'a, L>;

    open spec fn f_on(s: RStream<'a, L>, buf: Seq<u8>) -> bool {
        s.on(buf)
    }

    open spec fn f_head(s: RStream<'a, L>, buf: Seq<u8>) -> Seq<u8> {
        s.head_of(buf)
    }

    open spec fn f_values(s: RStream<'a, L>) -> Seq<Self::V> {
        lift_seq::<L>(s.cur@)
    }

    proof fn lemma_f_head(s: RStream<'a, L>, buf: Seq<u8>) {
        if s.fresh() {
            assert(buf.take(buf.len() as int) =~= buf);
        }
    }

    #[inline(always)]
    fn f_new() -> (s: RStream<'a, L>) {
        RStream::new()
    }

    #[inline(always)]
    fn f_give(s: &mut RStream<'a, L>, buf: &mut Vec<u8>, x: <L as WriteOps<'a>>::Cow, n: usize) {
        s.append(buf, x, n);
        proof {
            assert(lift_seq::<L>(copies(n as nat, <L as WriteOps<'a>>::val(x))) =~= Seq::new(
                n as nat,
                |_i: int| <L as Lift<'a>>::lift(<L as WriteOps<'a>>::val(x)),
            ));
            let more = copies(n as nat, <L as WriteOps<'a>>::val(x));
            assert(lift_seq::<L>(old(s).cur@ + more) =~= lift_seq::<L>(old(s).cur@) + lift_seq::<L>(more));
        }
    }

    #[inline(always)]
    fn f_all_are(s: &RStream<'a, L>, x: &<L as WriteOps<'a>>::Cow) -> (r: bool) {
        let r = s.single_run_of(x);
        proof {
            let v = <L as WriteOps<'a>>::val(*x);
            let lv = <L as Lift<'a>>::lift(v);
            let vs = lift_seq::<L>(s.cur@);
            if r {
                assert(vs =~= Seq::new(vs.len(), |_i: int| lv));
            }
            if vs == Seq::new(vs.len(), |_i: int| lv) {
                assert forall|i: int| 0 <= i < s.cur@.len() implies s.cur@[i] == v by {
                    assert(lift_seq::<L>(s.cur@)[i] == lv);
                    <L as Plain>::lemma_lift_inj(s.cur@[i], v);
                }
                assert(s.cur@ =~= copies(s.cur@.len(), v));
            }
        }
        r
    }

    #[inline(always)]
    fn f_count(s: &RStream<'a, L>) -> (n: usize) {
        s.len
    }

    #[inline(always)]
    fn f_finish(s: RStream<'a, L>, buf: &mut Vec<u8>) {
        let mut s = s;
        let ghost cur = s.cur@;
        let ghost h = s.head_of(buf@);
        s.finish(buf);
        proof {
            let d = buf@.skip(h.len() as int);
            let segs = choose|segs: int| encodes::<<L as WriteOps<'a>>::R>(d, cur, segs);
            lemma_lift_encodes::<L>(d, cur, segs);
            lemma_valid_of::<O>(d, lift_seq::<L>(cur), segs);
        }
    }
}

impl<'a, C: CountsOf> Flows<'a, BoolReads<C>> for BoolModel<C> {
    type Flow = BStream;

    open spec fn f_on(s: BStream, buf: Seq<u8>) -> bool {
        s.on::<C::K>(buf)
    }

    open spec fn f_head(s: BStream, buf: Seq<u8>) -> Seq<u8> {
        s.head_of(buf)
    }

    open spec fn f_values(s: BStream) -> Seq<bool> {
        s.state.vals@
    }

    proof fn lemma_f_head(s: BStream, buf: Seq<u8>) {
        assert(buf.take(s.head_of(buf).len() as int) =~= s.head_of(buf));
    }

    #[inline(always)]
    fn f_new() -> (s: BStream) {
        BStream::new::<C::K>()
    }

    #[inline(always)]
    fn f_give(s: &mut BStream, buf: &mut Vec<u8>, x: bool, n: usize) {
        s.append::<C::K>(buf, x, n);
        proof {
            assert(run_of(n as nat, x) =~= Seq::new(n as nat, |_i: int| x));
        }
    }

    #[inline(always)]
    fn f_all_are(s: &BStream, x: &bool) -> (r: bool) {
        let r = s.all_are(*x);
        proof {
            let vs = s.state.vals@;
            if r {
                assert(vs =~= Seq::new(vs.len(), |_i: int| *x));
            }
        }
        r
    }

    #[inline(always)]
    fn f_count(s: &BStream) -> (n: usize) {
        s.state.len
    }

    #[inline(always)]
    fn f_finish(s: BStream, buf: &mut Vec<u8>) {
        let ghost h = s.head_of(buf@);
        s.finish::<C::K>(buf);
        proof {
            assert(buf@.take(h.len() as int) =~= h);
        }
    }
}

/// A model's loader, for input that lives for `'a`: it reads a saved
/// column run by run, and cuts it into the model's slabs.
pub(crate) trait Loading<'a, X>: Model<X> where
    X: for <'b>Decoders<'b, Tail = <Self as Slabs>::Tail>,
 {
    type Ld: Loads<'a, Self, X>;

    /// The loader reads saved columns as the model does: what it finds
    /// valid is a saved column, it takes every saved column short of
    /// 4 GiB by a header's length, and the values it reads are the
    /// column's.
    proof fn lemma_loader(d: Seq<u8>)
        ensures
            <Self::Ld as Loads<'a, Self, X>>::valid(d) ==> <Self as Model<X>>::valid(d),
            <Self as Model<X>>::valid(d) && d.len() <= 0xffff_fff5 ==> <Self::Ld as Loads<'a, Self, X>>::accepts(d),
            <Self::Ld as Loads<'a, Self, X>>::values_of(d).map_values(
                |v: <<X as Decoders<'a>>::D as Decode>::Item| <Self as ReadsAgree<'a, X>>::own(v),
            ) == <Self as Model<X>>::values(d),
    ;
}

impl<'a, L, O> Loading<'a, RleReads<O>> for RleModel<L> where
    L: Plain + for <'s>Lift<'s, W = L> + for <'s>WriteOps<'s, R = O>,
    O: for <'b>RleOps<'b>,
 {
    type Ld = Load<'a, L>;

    proof fn lemma_loader(d: Seq<u8>) {
        if valid_slab::<O>(d) && d.len() <= 0xffff_fff5 {
            L::lemma_heads();
            assert(head_bound::<O>(10));
            assert(small::<O>(d));
        }
        let o = slab_values::<O>(d);
        let m = o.map_values(|v: <O as RleOps<'a>>::V| <RleModel<L> as ReadsAgree<'a, RleReads<O>>>::own(v));
        assert forall|i: int| 0 <= i < o.len() implies m[i] == #[trigger] o[i] by {
            L::lemma_plain(o[i]);
        }
        assert(m =~= o);
    }
}

impl<'a, C: CountsOf> Loading<'a, BoolReads<C>> for BoolModel<C> {
    type Ld = BoolLoader<'a, C>;

    proof fn lemma_loader(d: Seq<u8>) {
    }
}

} // verus!
