//! The API and its specification. Its documentation, the guide to
//! reviewing the specification among it, is the crate's (`lib.rs`).

// Verus proofs read more plainly with `x = x + n`, and Verus has no
// spec for `vec![x]`.
#![allow(
    clippy::len_zero,
    clippy::assign_op_pattern,
    clippy::vec_init_then_push
)]

mod acc;
mod acc_iter;
#[cfg(feature = "bijou64")]
pub mod bijou;
pub(crate) mod bytes;
#[cfg(verus_keep_ghost)]
mod client_proof;
pub(crate) mod codec;
mod copy;
pub(crate) mod counts;
mod decoder;
mod delta;
mod edit;
mod encoder;
pub(crate) mod kind;
pub mod leb128;
mod load;
pub(crate) mod model;
mod prefix;
mod prefix_iter;
mod shift;
pub(crate) mod sums;
pub(crate) mod text;
pub(crate) mod value;

// spec functions exist only when Verus compiles the crate
#[cfg(verus_keep_ghost)]
pub use crate::iter_core::{past, window};
#[cfg(verus_keep_ghost)]
pub use crate::raw::spliced_cut;
pub use crate::raw::{RawColumn, RawColumnIter};
#[cfg(verus_keep_ghost)]
pub use crate::rle::writer::concat;
#[cfg(verus_keep_ghost)]
pub use crate::splices::{
    applied, copied, end_of, in_order, min_i, produced, ranges_ascend, splice_does, splices_do,
};
#[cfg(verus_keep_ghost)]
pub use crate::window::windows;
#[cfg(verus_keep_ghost)]
pub use acc::{accumulated, lemma_acc_cat, lemma_acc_range, lemma_acc_run, lemma_cancel, times};
pub use acc::{AccColumn, AccIndex, Accum, Accumulated, Adds, VGroup};
pub use acc_iter::{AccIter, AccIterState, AccSeek, AccValue};
#[cfg(verus_keep_ghost)]
pub use bytes::{below_at, bytes_below, total_len, total_len_opt};
pub use codec::{lebsize, ulebsize, Codec};
pub use decoder::{decoder, decoder_in, try_decoder, try_decoder_in, Decoder};
#[cfg(verus_keep_ghost)]
pub use decoder::{
    head_len, head_run_max, lemma_head_len, lemma_head_run_max, scan_of, scan_taken,
};
#[cfg(verus_keep_ghost)]
pub use delta::{
    all_dom, all_fit, delta_at, delta_below, delta_finds, delta_iter_fits, delta_near,
    delta_prefix, delta_run_fits, delta_run_in_range, delta_run_kept, delta_run_prefix,
    delta_run_units, delta_run_values, delta_runs_at, delta_runs_fit, delta_runs_kept,
    delta_runs_values, delta_span_ok, delta_units, delta_units_of_runs, delta_within, deltas, dom,
    dvs, in_dom, lemma_delta_finds, lemma_delta_finds_first, lemma_delta_run_at,
    lemma_delta_run_exact, lemma_delta_run_fits, lemma_delta_runs_at, lemma_delta_runs_one,
    lemma_delta_runs_push, lemma_delta_within_one, vals,
};
pub use delta::{
    Delta, DeltaColumn, DeltaDecoder, DeltaEdit, DeltaEncoder, DeltaEncoderState, DeltaIter,
    DeltaIterState, DeltaNum, DeltaRuns, DeltaValue, FindByRange,
};
pub use edit::Edit;
pub use encoder::{Encoder, EncoderState};
pub use kind::{Index, LenBitIndex, LenIndex, VKeeps};
pub use load::ColumnLoadIter;
pub use prefix::{PrefixColumn, SumIndex};
pub use prefix_iter::{PrefixIter, PrefixIterState, PrefixSeek, PrefixedValue};
#[cfg(verus_keep_ghost)]
pub use shift::{lemma_singles, lemma_svs_windows, run_units, unit_runs, units_of_runs};
pub use shift::{Ranges, Shiftable, Unshift};
#[cfg(verus_keep_ghost)]
pub use sums::{
    lemma_sum_bound, lemma_sum_cat, lemma_sum_mono, lemma_sum_range, lemma_sum_run, lemma_sums_fit,
    lemma_sums_fit_take, lemma_sums_fit_unsigned, summed, sums_fit, u32_room,
};
pub use sums::{Amount, PrefixValue, Sum, UnsignedPrefix};
#[cfg(verus_keep_ghost)]
pub use text::{total_str_len, total_str_len_opt};
pub use value::{refused, Newtype, RleValue};

#[cfg(verus_keep_ghost)]
use crate::bool_edit::BoolModel;
#[cfg(verus_keep_ghost)]
use crate::bool_wire::BoolReads;
#[cfg(verus_keep_ghost)]
use crate::btree::SlabAggregate;
use crate::column::Slab;
use crate::cursor::Slabs;
#[cfg(verus_keep_ghost)]
use crate::cursor::{slab_lens, Weigh};
#[cfg(verus_keep_ghost)]
use crate::cursor::{BoundedFeed, ReadsAgree, RunFeed};
#[cfg(verus_keep_ghost)]
use crate::fenwick::{sum, total};
#[cfg(verus_keep_ghost)]
use crate::index::IndexOps;
#[cfg(verus_keep_ghost)]
use crate::iter_core::vals_of;
#[cfg(verus_keep_ghost)]
use crate::iter_core::{Decode, Probe};
use crate::iter_core::{Decoders, ICore};
#[cfg(verus_keep_ghost)]
use crate::remap::{Encodes, Mapping};
use crate::{Leb128, LoadOpts, MaybeFill, PackError, Run};
#[cfg(verus_keep_ghost)]
use counts::Bools;
use kind::KCore;
#[cfg(verus_keep_ghost)]
use model::{Model, Streams};
use std::cmp::Ordering;
#[cfg(verus_keep_ghost)]
use std::ops::Bound;
use std::ops::{Range, RangeBounds};
#[allow(unused_imports)]
use vstd::prelude::*;
#[cfg(verus_keep_ghost)]
use vstd::std_specs::iter::{FromIteratorSpecImpl, IteratorSpec};
#[cfg(verus_keep_ghost)]
use vstd::std_specs::range::RangeBoundsSpec;

mod seal {
    use super::model::{Flows, Loading, Model, Streams};
    use super::{Form, Items, VGet, VSpec};
    use crate::cursor::Slabs;
    use crate::iter_core::Decoders;

    vstd::prelude::verus! {

    /// A type sums are kept in is one of this crate's: no other can
    /// name this trait.
    pub trait SumSeal {

    }

    /// A number a delta column can hold is one of this crate's: no
    /// other can name this trait.
    pub trait DeltaSeal {

    }

    /// A value type of a delta column is one of those numbers or an
    /// `Option` of one: no other can name this trait.
    pub trait DeltaValSeal {

    }

    /// A value type a prefix column can hold is one of this crate's,
    /// or says what its values add to a sum: no other can name this
    /// trait.
    pub trait SummedSeal {

    }

    /// A value type an accumulator column can hold says what its values
    /// add, or is an `Option` of one that does: no other can name this
    /// trait.
    pub trait AccSeal {

    }

    /// A form a column gives its values in is one of this crate's: no
    /// other can name this trait.
    pub trait GetSeal {

    }

    /// What a column's values are in a specification is this crate's to
    /// say: no other can name this trait.
    pub trait SpecSeal {

    }

    /// What a value type is to the verified core, under codec `C`. A
    /// value type of the verified public API is one of this crate's, or
    /// one that says which of this crate's it is stored as: no other
    /// can name this trait.
    #[allow(private_bounds)]
    pub trait Sealed<C>: Sized + VSpec + for <'a>Form<'a> {
        /// How the values are read: as the form the type gives them in.
        type X: for <'a>Items<'a, Item = <Self as Form<'a>>::Get> + for <'a>Decoders<
            'a,
            Tail = <Self::E as Slabs>::Tail,
        >;

        /// The slabs that hold them, and whose encoder takes them.
        type E: Slabs<V: VGet<S = <Self as VSpec>::S>> + Model<Self::X> + for <'a>Streams<'a, Self::X> + for <'a>Flows<'a, Self::X> + for <'a>Loading<'a, Self::X>;
    }

    } // verus!
}

use crate::trusted::invalid_resume;
mod imp;
#[allow(unused_imports)]
use imp::*;
#[allow(unused_imports)]
pub(crate) use imp::{Items, Owned};

verus! {

// ── What a value is to a client ─────────────────────────────────────────────

/// The form in which a column gives a value and takes one: the value
/// itself for a number or a `bool`, for bytes a borrowed slice, and for
/// a string a `&str`. The forms are this crate's: no other type can
/// implement this trait.
///
/// A column's specifications speak of values as `S`: `v.sv()` is the
/// value `v` there. For a number that is the number, for a slice it is
/// the slice's bytes and for a `&str` its characters (`v@`).
pub trait VGet: Copy + seal::GetSeal {
    /// What a value is in a specification.
    type S;

    /// The value, as specifications have it.
    spec fn sv(self) -> Self::S;

    /// Values, as specifications have them: each one's
    /// [`sv`](Self::sv). (For a form that is its own `S` this is `vs`
    /// itself, and is defined so.)
    spec fn svs(vs: Seq<Self>) -> Seq<Self::S>;

    /// `svs` is `sv` of each value.
    proof fn lemma_svs(vs: Seq<Self>)
        ensures
            Self::svs(vs).len() == vs.len(),
            forall|i: int| 0 <= i < vs.len() ==> (#[trigger] Self::svs(vs)[i]) == vs[i].sv(),
    ;

    /// Two values that are the same in a specification are the same.
    proof fn lemma_sv_inj(a: Self, b: Self)
        requires
            a.sv() == b.sv(),
        ensures
            a == b,
    ;
}

impl<T: RleValue> VGet for T {
    type S = T;

    open spec fn sv(self) -> T {
        self
    }

    open spec fn svs(vs: Seq<T>) -> Seq<T> {
        vs
    }

    proof fn lemma_svs(vs: Seq<T>) {
    }

    proof fn lemma_sv_inj(a: T, b: T) {
    }
}

impl<T: RleValue> VGet for Option<T> {
    type S = Option<T>;

    open spec fn sv(self) -> Option<T> {
        self
    }

    open spec fn svs(vs: Seq<Option<T>>) -> Seq<Option<T>> {
        vs
    }

    proof fn lemma_svs(vs: Seq<Option<T>>) {
    }

    proof fn lemma_sv_inj(a: Option<T>, b: Option<T>) {
    }
}

impl VGet for bool {
    type S = bool;

    open spec fn sv(self) -> bool {
        self
    }

    open spec fn svs(vs: Seq<bool>) -> Seq<bool> {
        vs
    }

    proof fn lemma_svs(vs: Seq<bool>) {
    }

    proof fn lemma_sv_inj(a: bool, b: bool) {
    }
}

/// A type of values, and what a value of it is in a specification.
/// Every [`ColumnValueRef`] is one, and no other type: the trait cannot be
/// implemented outside this crate.
pub trait VSpec: Sized + seal::SpecSeal {
    /// What a value is in a specification: for a number, the number,
    /// for a byte string its bytes (`Seq<u8>`) and for a string its
    /// characters (`Seq<char>`). A column of the type is a sequence of
    /// these (`col@`).
    type S;

    /// What the value `v` of the type is in a specification: `v` itself
    /// for a number, an `Option` of one or a `bool`, for a byte string
    /// its bytes and for a string its characters (`v@`).
    spec fn sv_of(v: Self) -> Self::S;

    /// Whether `a` comes before `b` in the order of the type's values.
    /// Numbers are in the order of numbers, and a type stored as a
    /// number ([`Newtype`]) in the order of the numbers it is stored as
    /// ([`RleValue::num`]); `false` comes before `true`; byte strings are
    /// in the order of slices ([`bytes_below`]: byte by byte, and a
    /// byte string before every longer one that starts with it);
    /// strings are in that order of their UTF-8 bytes (`bytes_below` of
    /// `encode_utf8` of their characters); and a null comes before every
    /// value.
    spec fn below(a: Self::S, b: Self::S) -> bool;

    /// The order is a strict total order: no value comes before itself,
    /// what comes before a value comes before all that come after it,
    /// and of two values that differ one comes before the other.
    proof fn lemma_below(a: Self::S, b: Self::S, c: Self::S)
        ensures
            !Self::below(a, a),
            Self::below(a, b) && Self::below(b, c) ==> Self::below(a, c),
            a == b || Self::below(a, b) || Self::below(b, a),
    ;
}

/// The form in which a column of this type gives a value and takes
/// one, where what the value borrows lives for `'a`.
pub trait Form<'a>: VSpec {
    /// The form: for a number, an `Option` of one or a `bool`, the
    /// type itself; for `Vec<u8>`, `&'a [u8]`; for `Option<Vec<u8>>`,
    /// `Option<&'a [u8]>`; for `String`, `&'a str`; for
    /// `Option<String>`, `Option<&'a str>`.
    type Get: VGet<S = <Self as VSpec>::S>;
}

/// The form in which a column of `V` gives a value and takes one
/// ([`Form`]): `V` itself for a number, an `Option` of one or a
/// `bool`, a borrowed slice for a byte string and a `&str` for a
/// string.
pub type Get<'a, V> = <V as Form<'a>>::Get;

impl<T: RleValue> VSpec for T {
    type S = T;

    open spec fn sv_of(v: T) -> T {
        v
    }

    open spec fn below(a: T, b: T) -> bool {
        T::num(a) < T::num(b)
    }

    proof fn lemma_below(a: T, b: T, c: T) {
        T::lemma_num(a, b);
    }
}

impl<'a, T: RleValue> Form<'a> for T {
    type Get = T;
}

impl<T: RleValue> VSpec for Option<T> {
    type S = Option<T>;

    open spec fn sv_of(v: Option<T>) -> Option<T> {
        v
    }

    open spec fn below(a: Option<T>, b: Option<T>) -> bool {
        match (a, b) {
            (None, Some(_)) => true,
            (Some(x), Some(y)) => T::num(x) < T::num(y),
            _ => false,
        }
    }

    proof fn lemma_below(a: Option<T>, b: Option<T>, c: Option<T>) {
        imp::vspec_lemma_below_option_t::<T>(a, b, c)
    }
}

impl<'a, T: RleValue> Form<'a> for Option<T> {
    type Get = Option<T>;
}

impl VSpec for bool {
    type S = bool;

    open spec fn sv_of(v: bool) -> bool {
        v
    }

    open spec fn below(a: bool, b: bool) -> bool {
        !a && b
    }

    proof fn lemma_below(a: bool, b: bool, c: bool) {
    }
}

impl<'a> Form<'a> for bool {
    type Get = bool;
}

/// The value `v`, given to a column of `V`, is in a specification the
/// value the column then holds.
#[doc(hidden)]
pub closed spec fn own_kept<'a, V: seal::Sealed<C>, C>(v: Get<'a, V>) -> bool {
    <V::E as ReadsAgree<'a, V::X>>::own(v).sv() == v.sv()
}

// ── The saved form ──────────────────────────────────────────────────────────

/// `d` is a saved column of `V` under codec `C`
/// ([`Column::valid`]).
#[doc(hidden)]
pub closed spec fn saved<V: seal::Sealed<C>, C>(d: Seq<u8>) -> bool {
    <V::E as Model<V::X>>::valid(d)
}

/// The values of the saved column `d` ([`Column::values`]).
#[doc(hidden)]
pub closed spec fn saved_values<V: seal::Sealed<C>, C>(d: Seq<u8>) -> Seq<<V as VSpec>::S> {
    <Held<V, C> as VGet>::svs(<V::E as Model<V::X>>::values(d))
}

/// A value type a [`Column`] can hold, its numbers written with
/// codec `C`.
///
/// These are `u64`, `i64`, `u32`, `i32`, `usize`, `NonZeroU32`, every
/// type that says which of them it is stored as ([`Newtype`]) and every
/// type with an encoding of its own ([`RleValue`]); byte strings
/// (`Vec<u8>`); an `Option` of any of those, whose `None` is a column's
/// null; strings (`String`, and `Option<String>`); and `bool`.
#[allow(private_bounds)]
pub trait ColumnValueRef<C: Codec = Leb128>: seal::Sealed<C> + Owned<C> {
    /// No column of the values `vs` saves to more bytes than this.
    spec fn size_bound(vs: Seq<Self::S>) -> nat;

    /// A saved column is within the bound of its values.
    #[doc(hidden)]
    proof fn lemma_size(d: Seq<u8>)
        requires
            saved::<Self, C>(d),
        ensures
            d.len() <= Self::size_bound(saved_values::<Self, C>(d)),
    ;

    /// A value given to a column is, in a specification, the value the
    /// column then holds.
    #[doc(hidden)]
    proof fn lemma_own<'a>(v: Get<'a, Self>)
        ensures
            own_kept::<Self, C>(v),
    ;

    /// Whether two values are the same value. They need not live as long
    /// as each other: one may be a column's, the other a caller's.
    fn same<'a, 'b>(a: Get<'a, Self>, b: Get<'b, Self>) -> (r: bool)
        ensures
            r == (a.sv() == b.sv()),
    ;

    /// How `a` stands to `b` in the order of the values
    /// (`VSpec::below`): `Less` where it comes before `b`,
    /// `Greater` where it comes after, and `Equal` where they are the
    /// same value.
    fn compare<'a, 'b>(a: Get<'a, Self>, b: Get<'b, Self>) -> (r: Ordering)
        ensures
            (r is Less) == Self::below(a.sv(), b.sv()),
            (r is Greater) == Self::below(b.sv(), a.sv()),
            (r is Equal) == (a.sv() == b.sv()),
    ;
}

impl<T: RleValue, C: Codec> ColumnValueRef<C> for T {
    /// Ten bytes a value, and a header and a count of ten bytes each.
    open spec fn size_bound(vs: Seq<T>) -> nat {
        30 * vs.len()
    }

    proof fn lemma_size(d: Seq<u8>) {
        imp::vvalue_lemma_size_t::<T, C>(d)
    }

    proof fn lemma_own<'a>(v: T) {
    }

    #[inline(always)]
    fn same<'a, 'b>(a: T, b: T) -> (r: bool) {
        <T as RleValue>::same(&a, &b)
    }

    #[inline(always)]
    fn compare<'a, 'b>(a: T, b: T) -> (r: Ordering) {
        imp::vvalue_compare_t::<T>(a, b)
    }
}

impl<T: RleValue, C: Codec> ColumnValueRef<C> for Option<T> {
    /// Ten bytes a value, and a header and a count of ten bytes each.
    open spec fn size_bound(vs: Seq<Option<T>>) -> nat {
        30 * vs.len()
    }

    proof fn lemma_size(d: Seq<u8>) {
        imp::vvalue_lemma_size_option_t::<T, C>(d)
    }

    proof fn lemma_own<'a>(v: Option<T>) {
    }

    #[inline(always)]
    fn same<'a, 'b>(a: Option<T>, b: Option<T>) -> (r: bool) {
        imp::vvalue_same_option_t::<T>(a, b)
    }

    #[inline(always)]
    fn compare<'a, 'b>(a: Option<T>, b: Option<T>) -> (r: Ordering) {
        imp::vvalue_compare_option_t::<T>(a, b)
    }
}

impl<C: Codec> ColumnValueRef<C> for bool {
    /// Ten bytes a run, which is at least one value but for the first.
    open spec fn size_bound(vs: Seq<bool>) -> nat {
        10 * vs.len() + 10
    }

    proof fn lemma_size(d: Seq<u8>) {
        <BoolModel<Bools<C>> as Model<BoolReads<Bools<C>>>>::lemma_size(d);
    }

    proof fn lemma_own<'a>(v: bool) {
    }

    #[inline(always)]
    fn same<'a, 'b>(a: bool, b: bool) -> (r: bool) {
        a == b
    }

    #[inline(always)]
    fn compare<'a, 'b>(a: bool, b: bool) -> (r: Ordering) {
        imp::vvalue_compare_bool(a, b)
    }
}

/// The values of the runs `rs`, as a column holds them: each run's value
/// as many times as its count says, the runs in order. A run of count 0
/// is no value.
pub open spec fn run_values<G: VGet>(rs: Seq<Run<G>>) -> Seq<G::S>
    decreases rs.len(),
{
    if rs.len() == 0 {
        Seq::empty()
    } else {
        run_values(rs.drop_last()) + Seq::new(rs.last().count as nat, |_i: int| rs.last().value.sv())
    }
}

/// The values of runs and one more run.
pub proof fn lemma_run_values_push<G: VGet>(rs: Seq<Run<G>>, r: Run<G>)
    ensures
        run_values(rs.push(r)) == run_values(rs) + Seq::new(r.count as nat, |_i: int| r.value.sv()),
{
    imp::lemma_run_values_push_impl::<G>(rs, r)
}

/// The values of runs one after another.
pub proof fn lemma_run_values_add<G: VGet>(a: Seq<Run<G>>, b: Seq<Run<G>>)
    ensures
        run_values(a + b) == run_values(a) + run_values(b),
    decreases b.len(),
{
    imp::lemma_run_values_add_impl::<G>(a, b)
}

/// What an iterator `i` that a function is handed gives, as Verus says
/// what the standard library does with one: `p` holds of the values that
/// the iterator `i` becomes (`into_iter`) is to give (`remaining()`), if
/// that iterator keeps to Verus's laws of iterators
/// (`obeys_prophetic_iter_laws`: what `next` returns is the first of what
/// remains, and `None` when nothing does). Of an iterator that does not,
/// it says nothing.
#[verifier::prophetic]
pub open spec fn gives<I: IntoIterator>(i: I, p: spec_fn(Seq<I::Item>) -> bool) -> bool {
    exists|it: I::IntoIter| #[trigger]
        (I::into_iter).ensures((i,), it) && (it.obeys_prophetic_iter_laws() ==> p(it.remaining()))
}

/// The places `start..end` of a range of a column of `len` values, as
/// [`Column::iter_range`] and its kin take it: clamped to the column. A
/// range past the end ends there, and
/// one that ends before it starts is the empty one at its start.
pub open spec fn range_in(range: Range<usize>, len: nat) -> (int, int) {
    let s = if range.start <= len {
        range.start as int
    } else {
        len as int
    };
    let e = if range.end <= len {
        range.end as int
    } else {
        len as int
    };
    (s, if e < s {
        s
    } else {
        e
    })
}

/// The first place of a range given by its bounds, as a search takes it
/// ([`Column::scope_to_value`], [`Iter::seek_to_value`]): an `Excluded`
/// start is the place after it, but for `usize::MAX`, past which there is
/// none.
pub open spec fn bound_start(start: Bound<&usize>) -> int {
    match start {
        Bound::Included(n) => *n as int,
        Bound::Excluded(n) => if *n == usize::MAX {
            usize::MAX as int
        } else {
            *n + 1
        },
        Bound::Unbounded => 0,
    }
}

/// The place after the last of a range given by its bounds, as a search
/// takes it: an `Included` end is the place after it (but for
/// `usize::MAX`), and an `Unbounded` one is `max`, the place after the
/// last there is. A range that ends before it starts (`bound_start`) is
/// the empty one at its start.
pub open spec fn bound_end(start: Bound<&usize>, end: Bound<&usize>, max: int) -> int {
    let e = match end {
        Bound::Included(n) => if *n == usize::MAX {
            usize::MAX as int
        } else {
            *n + 1
        },
        Bound::Excluded(n) => *n as int,
        Bound::Unbounded => max,
    };
    if e < bound_start(start) {
        bound_start(start)
    } else {
        e
    }
}

/// The places `start..end` of a range given by its bounds, within a
/// column of `len` values ([`Column::scope_to_value`]): `bound_start` and
/// `bound_end`, each clamped to the column.
pub open spec fn bounds_in(start: Bound<&usize>, end: Bound<&usize>, len: nat) -> (int, int) {
    (if bound_start(start) <= len {
        bound_start(start)
    } else {
        len as int
    }, if bound_end(start, end, len as int) <= len {
        bound_end(start, end, len as int)
    } else {
        len as int
    })
}

/// The fill that a load makes of the saved bytes `d`, with the length
/// and the fill value its options hold: its length and its value, where
/// `d` is empty, both are given, and a column can be that long (a run
/// counts at most `i64::MAX` values, and a `usize` a column's values
/// with three to spare). Where it makes none, the load reads `d`.
pub open spec fn fill_of<G>(d: Seq<u8>, length: Option<usize>, fill: Option<G>) -> Option<(usize, G)> {
    match (length, fill) {
        (Some(len), Some(v)) => if d.len() == 0 && len <= i64::MAX && len <= usize::MAX - 3 {
            Some((len, v))
        } else {
            None
        },
        _ => None,
    }
}

/// Every value of `vs` is `v`: so of no values.
pub open spec fn only<S>(vs: Seq<S>, v: S) -> bool {
    forall|i: int| 0 <= i < vs.len() ==> vs[i] == v
}

/// The values `vs` are in order ([`VSpec::below`]): none comes before
/// one that is before it in `vs`.
pub open spec fn sorted<V: VSpec>(vs: Seq<V::S>) -> bool {
    forall|i: int, j: int| 0 <= i <= j < vs.len() ==> !V::below(#[trigger] vs[j], #[trigger] vs[i])
}

/// The values `vs` are in order as far as the value `t` can tell: those
/// that come before `t` are first in `vs`, and those that come after
/// `t` last. What a search for `t` needs
/// ([`Column::scope_to_value`]), and what values in order
/// ([`sorted`]) are for every `t` ([`lemma_sorted`]).
pub open spec fn sorted_for<V: VSpec>(vs: Seq<V::S>, t: V::S) -> bool {
    forall|i: int, j: int|
        0 <= i <= j < vs.len() ==> {
            &&& V::below(#[trigger] vs[j], t) ==> V::below(#[trigger] vs[i], t)
            &&& V::below(t, vs[i]) ==> V::below(t, vs[j])
        }
}

/// Values in order are in order as far as any value can tell.
pub proof fn lemma_sorted<V: VSpec>(vs: Seq<V::S>, t: V::S)
    requires
        sorted::<V>(vs),
    ensures
        sorted_for::<V>(vs, t),
{
    imp::lemma_sorted_impl::<V>(vs, t)
}

/// The function `f` gave, for a value that is `a` in a specification,
/// one that is `b` ([`Column::remap`]): there are values `v` and
/// `w` of the type, `a` and `b` in a specification (`VSpec::sv_of`),
/// such that `f`'s `ensures` holds of the call `f(v)` and its result
/// `w`.
pub open spec fn mapped<V: VSpec, F: Fn(V) -> V>(f: F, a: V::S, b: V::S) -> bool {
    exists|v: V, w: V| V::sv_of(v) == a && #[trigger] call_ensures(f, (v,), w) && V::sv_of(w) == b
}

/// The numbers `lens` added up: how many values slabs of those lengths
/// hold between them.
pub open spec fn sum_of(lens: Seq<usize>) -> nat
    decreases lens.len(),
{
    if lens.len() == 0 {
        0
    } else {
        sum_of(lens.drop_last()) + lens.last() as nat
    }
}

/// What a column counts of edits (`Column::edits`) after one
/// more, where it counted `n`: one more, and 0 after the greatest number
/// a `usize` holds, for the count wraps around.
pub open spec fn after_edit(n: usize) -> usize {
    if n == usize::MAX {
        0
    } else {
        (n + 1) as usize
    }
}

/// What a column counts of edits after a call, where it counted `n`
/// before: [`after_edit`] of `n` where the call was an edit (`edited`),
/// and `n` where it was none. Each method that changes a column says
/// what it counts after it so, and which calls are edits.
pub open spec fn after_call(n: usize, edited: bool) -> usize {
    if edited {
        after_edit(n)
    } else {
        n
    }
}

/// What a column counts of edits after `k` more, where it counted `n`.
pub open spec fn after_edits(n: usize, k: nat) -> usize
    decreases k,
{
    if k == 0 {
        n
    } else {
        after_edit(after_edits(n, (k - 1) as nat))
    }
}

/// After `k` edits, of which there are no more than `usize::MAX`, a
/// column counts `k` more than it did, less `usize::MAX + 1` where that
/// is more than a `usize` holds. So after one edit or more it does not
/// count what it did: a state taken before them is refused
/// (`IterState::lemma_fits`).
pub proof fn lemma_after_edits(n: usize, k: nat)
    requires
        k <= usize::MAX,
    ensures
        after_edits(n, k) == if n + k <= usize::MAX {
            n + k
        } else {
            n + k - usize::MAX - 1
        },
        k > 0 ==> after_edits(n, k) != n,
    decreases k,
{
    imp::lemma_after_edits_impl(n, k)
}

/// A column of values of type `V`: the sequence `self@`. Its numbers are
/// written with the codec `C`.
///
/// The third parameter is the kind of index the column is kept with
/// ([`Index`]). A plain column's is [`LenIndex`], which need not be
/// written: `Column<V>` and `Column<V, C>` are plain
/// columns. Every method is of every kind's, the constructors and the
/// loads too: `Column::<V>::new()` is a plain column, and where
/// the type is known otherwise, `Column::new()` is of the kind
/// it says. A column kept with a [`SumIndex`] is a prefix column
/// ([`PrefixColumn`]); one kept with a [`LenBitIndex`] is a
/// plain column whose index is a Fenwick tree.
pub struct Column<V: ColumnValueRef<C>, C: Codec = Leb128, K: Index<V, C> = LenIndex> {
    core: KCore<V, C, K>,
}

impl<V: ColumnValueRef<C>, C: Codec, K: Index<V, C>> View for Column<V, C, K> {
    type V = Seq<V::S>;

    closed spec fn view(&self) -> Seq<V::S> {
        <Held<V, C> as VGet>::svs(self.core@)
    }
}

impl<V: ColumnValueRef<C>, C: Codec, K: Index<V, C>> Default for Column<V, C, K> {
    /// An empty column.
    fn default() -> (r: Self)
        ensures
            r@ == Seq::<V::S>::empty(),
    {
        Self::new()
    }
}

impl<V: ColumnValueRef<C>, C: Codec, K: Index<V, C>> Clone for Column<V, C, K> {
    /// A column of its own with the same values, which counts the edits
    /// this one does.
    fn clone(&self) -> (r: Self)
        ensures
            r@ == self@,
            r.edits() == self.edits(),
    {
        Column { core: self.core.duplicate() }
    }
}


impl<'v, V: ColumnValueRef<C>, C: Codec, K: Index<V, C>> Extend<Get<'v, V>> for Column<V, C, K> {
    /// Append the values that `iter` gives: what
    /// [`splice`](Column::splice) does at the column's
    /// end, and specified as that is. The column counts an edit more if
    /// it is the longer for it.
    #[inline(always)]
    fn extend<I: IntoIterator<Item = Get<'v, V>>>(&mut self, iter: I)
        ensures
            gives(iter, |xs: Seq<I::Item>| final(self)@ == old(self)@ + VGet::svs(xs)),
            final(self).edits() == after_call(old(self).edits(), final(self)@.len() != old(self)@.len()),
    {
        self.extend_impl::<I>(iter)
    }
}

#[cfg(verus_keep_ghost)]
impl<'v, V: ColumnValueRef<C>, C: Codec, K: Index<V, C>> FromIteratorSpecImpl<Get<'v, V>> for Column<V, C, K> {
    /// A column collected from an iterator holds the values that the
    /// iterator was to give.
    open spec fn from_iter_ensures(remaining: Seq<Get<'v, V>>, will_return_none: bool, s: Self) -> bool {
        s@ == <Get<'v, V> as VGet>::svs(remaining)
    }
}

impl<'v, V: ColumnValueRef<C>, C: Codec, K: Index<V, C>> FromIterator<Get<'v, V>> for Column<V, C, K> {
    /// A column of the values that `iter` gives, which is what
    /// `collect()` returns. Verus's specification of `FromIterator`
    /// states it for an iterator that keeps to what Verus asks of every
    /// iterator: the column then holds the values the iterator was to
    /// give (`from_iter_ensures`).
    #[inline(always)]
    fn from_iter<I: IntoIterator<Item = Get<'v, V>>>(iter: I) -> (s: Self) {
        Self::from_iter_impl::<I>(iter)
    }
}

// The saved form. It is the same for every kind of index, and is
// spoken of as a plain column's: `Column::<V, C>::valid(d)`.
impl<V: ColumnValueRef<C>, C: Codec> Column<V, C> {
    /// `d` is a saved column: the canonical encoding of some values.
    pub closed spec fn valid(d: Seq<u8>) -> bool {
        saved::<V, C>(d)
    }

    /// The values of the saved column `d`.
    pub closed spec fn values(d: Seq<u8>) -> Seq<V::S> {
        saved_values::<V, C>(d)
    }

    /// The bytes a column of the values `vs` saves to.
    pub closed spec fn encoding(vs: Seq<V::S>) -> Seq<u8> {
        choose|d: Seq<u8>| Self::valid(d) && Self::values(d) == vs
    }

    /// A saved column that [`load`](Column::load) is sure to
    /// take: its bytes are short of 4 GiB by a header's length, and a
    /// `usize` can count its values with three to spare.
    pub open spec fn loadable(d: Seq<u8>) -> bool {
        &&& Self::valid(d)
        &&& d.len() <= 0xffff_fff5
        &&& Self::values(d).len() <= usize::MAX - 3
    }

    /// Two saved columns of the same values are the same bytes.
    pub proof fn lemma_save_unique(x: Seq<u8>, y: Seq<u8>)
        requires
            Self::valid(x),
            Self::valid(y),
            Self::values(x) == Self::values(y),
        ensures
            x == y,
    {
        Self::lemma_save_unique_impl(x, y)
    }

    /// A saved column is the encoding of its values.
    pub proof fn lemma_encoding(d: Seq<u8>)
        requires
            Self::valid(d),
        ensures
            Self::encoding(Self::values(d)) == d,
    {
        Self::lemma_encoding_impl(d)
    }

    /// No bytes are a saved column: the one of no values.
    pub proof fn lemma_no_bytes()
        ensures
            Self::valid(Seq::<u8>::empty()),
            Self::values(Seq::<u8>::empty()) == Seq::<V::S>::empty(),
    {
        Self::lemma_no_bytes_impl()
    }
}

// The constructors.
impl<V: ColumnValueRef<C>, C: Codec, K: Index<V, C>> Column<V, C, K> {
    /// An empty column.
    #[inline(always)]
    pub fn new() -> (r: Self)
        ensures
            r@ == Seq::<V::S>::empty(),
    {
        Self::new_impl()
    }

    /// An empty column whose slabs hold at most `max_segments` runs
    /// each.
    #[inline(always)]
    pub fn with_max_segments(max_segments: usize) -> (r: Self)
        requires
            max_segments >= 2,
        ensures
            r@ == Seq::<V::S>::empty(),
    {
        Self::with_max_segments_impl(max_segments)
    }

    /// A column of `values`.
    #[inline(always)]
    pub fn from_values(values: &[Get<'_, V>]) -> (r: Self)
        ensures
            r@ == VGet::svs(values@),
    {
        Self::from_values_impl(values)
    }

    /// A column of `values` whose slabs hold at most `max_segments` runs
    /// each.
    #[inline(always)]
    pub fn from_values_with_max_segments(values: &[Get<'_, V>], max_segments: usize) -> (r: Self)
        requires
            max_segments >= 2,
        ensures
            r@ == VGet::svs(values@),
    {
        Self::from_values_with_max_segments_impl(values, max_segments)
    }

    /// A column of `len` copies of `value`.
    ///
    /// The length is one that a column's run can count, and a `usize`
    /// with three to spare (which says more only where a `usize` has 32
    /// bits).
    #[inline(always)]
    pub fn fill(len: usize, value: Get<'_, V>) -> (r: Self)
        requires
            len <= i64::MAX,
            len <= usize::MAX - 3,
        ensures
            r@ == Seq::new(len as nat, |_i: int| value.sv()),
    {
        Self::fill_impl(len, value)
    }
}

impl<V: ColumnValueRef<C>, C: Codec, K: Index<V, C>> Column<V, C, K> {
    /// The number of values.
    #[inline(always)]
    pub fn len(&self) -> (r: usize)
        ensures
            r == self@.len(),
    {
        self.len_impl()
    }

    /// Whether the column holds no value.
    #[inline(always)]
    pub fn is_empty(&self) -> (r: bool)
        ensures
            r == (self@.len() == 0),
    {
        self.is_empty_impl()
    }

    /// The count of the column's edits, which a suspended iterator's
    /// state keeps and [`IterState::try_resume`] compares. Each
    /// method that changes the column says what it is after the call:
    /// one more (`after_edit`) where the call was an edit, and what it
    /// was where the call was none.
    pub closed spec fn edits(&self) -> usize {
        self.core.count()
    }

    /// Delete `del` values at `at` and insert `values` there: as
    /// [`splice`](Self::splice) does with the values of a slice.
    ///
    /// The column counts an edit more (`after_edit` of its `edits()`),
    /// unless the call deletes no value and inserts none.
    #[inline(always)]
    pub fn splice_slice(&mut self, at: usize, del: usize, values: &[Get<'_, V>])
        requires
            at <= old(self)@.len(),
            del <= old(self)@.len() - at,
        ensures
            final(self)@ == old(self)@.take(at as int) + VGet::svs(values@) + old(self)@.skip(at + del),
            final(self).edits() == after_call(old(self).edits(), del > 0 || values@.len() > 0),
    {
        self.splice_slice_impl(at, del, values)
    }

    /// Delete `del` values at `at` and insert there the values that
    /// `values` gives, as an iterator.
    ///
    /// The specification speaks of what the iterator gives (`gives`):
    /// of an iterator that keeps to what Verus asks of every iterator, as
    /// those of the standard library that Verus specifies do. Of another
    /// it says nothing but that `self` is a column, and what it counts.
    ///
    /// The column counts an edit more unless the call deletes no value
    /// and inserts none, which is where `del` is 0 and the column is as
    /// long as it was. That holds of every iterator.
    #[inline(always)]
    pub fn splice<'v, I: IntoIterator<Item = Get<'v, V>>>(&mut self, at: usize, del: usize, values: I)
        requires
            at <= old(self)@.len(),
            del <= old(self)@.len() - at,
        ensures
            gives(values, |xs: Seq<I::Item>| final(self)@ == old(self)@.take(at as int) + VGet::svs(xs) + old(self)@.skip(at + del)),
            final(self).edits() == after_call(old(self).edits(), del > 0 || final(self)@.len() != old(self)@.len()),
    {
        self.splice_impl::<I>(at, del, values)
    }

    /// Delete `del` values at `at` and insert there the runs that `runs`
    /// gives, as an iterator: of each run its value, as many times as its
    /// count says (`run_values`). A run of count 0 inserts nothing.
    ///
    /// The specification speaks of the iterator as
    /// [`splice`](Self::splice)'s does. The counts are the
    /// caller's: with ones that make the column longer than it can count,
    /// or a run longer than the format can write, this panics.
    ///
    /// The column counts an edit more unless the call deletes no value
    /// and inserts none, as [`splice`](Self::splice)'s does:
    /// runs that are all of count 0 insert none.
    #[inline(always)]
    pub fn splice_runs<'v, I: IntoIterator<Item = Run<Get<'v, V>>>>(&mut self, at: usize, del: usize, runs: I)
        requires
            at <= old(self)@.len(),
            del <= old(self)@.len() - at,
        ensures
            gives(runs, |xs: Seq<I::Item>| final(self)@ == old(self)@.take(at as int) + run_values(xs) + old(self)@.skip(at + del)),
            final(self).edits() == after_call(old(self).edits(), del > 0 || final(self)@.len() != old(self)@.len()),
    {
        self.splice_runs_impl::<I>(at, del, runs)
    }

    /// Insert `value` before the value at `at`. The column counts an
    /// edit more.
    #[inline(always)]
    pub fn insert(&mut self, at: usize, value: Get<'_, V>)
        requires
            at <= old(self)@.len(),
        ensures
            final(self)@ == old(self)@.insert(at as int, value.sv()),
            final(self).edits() == after_edit(old(self).edits()),
    {
        self.insert_impl(at, value)
    }

    /// Add `value` at the end. The column counts an edit more.
    #[inline(always)]
    pub fn push(&mut self, value: Get<'_, V>)
        ensures
            final(self)@ == old(self)@.push(value.sv()),
            final(self).edits() == after_edit(old(self).edits()),
    {
        self.push_impl(value)
    }

    /// Delete the value at `at`. The column counts an edit more.
    #[inline(always)]
    pub fn remove(&mut self, at: usize)
        requires
            at < old(self)@.len(),
        ensures
            final(self)@ == old(self)@.remove(at as int),
            final(self).edits() == after_edit(old(self).edits()),
    {
        self.remove_impl(at)
    }

    /// Keep the first `len` values, where there are more: then the
    /// column counts an edit more, and where there are no more it counts
    /// what it did.
    #[inline(always)]
    pub fn truncate(&mut self, len: usize)
        ensures
            final(self)@ == if len < old(self)@.len() {
                old(self)@.take(len as int)
            } else {
                old(self)@
            },
            final(self).edits() == after_call(old(self).edits(), len < old(self)@.len()),
    {
        self.truncate_impl(len)
    }

    /// Delete the `n` values at `at`. The column counts an edit more,
    /// unless `n` is 0.
    #[inline(always)]
    pub fn remove_n(&mut self, at: usize, n: usize)
        requires
            at <= old(self)@.len(),
            n <= old(self)@.len() - at,
        ensures
            final(self)@ == old(self)@.take(at as int) + old(self)@.skip(at + n),
            final(self).edits() == after_call(old(self).edits(), n > 0),
    {
        self.remove_n_impl(at, n)
    }

    /// Delete every value. The column keeps its segment budget.
    ///
    /// Of a column with no value this is no edit: the column counts what
    /// it did, and what was suspended of it resumes still. Of any other the column
    /// counts an edit more.
    #[inline(always)]
    pub fn clear(&mut self)
        ensures
            final(self)@ == Seq::<V::S>::empty(),
            final(self).edits() == after_call(old(self).edits(), old(self)@.len() > 0),
    {
        self.clear_impl()
    }

    /// How many slabs hold the column. That is a fact of how the column
    /// lies in memory and not of its values: nothing is specified of it.
    #[inline(always)]
    pub fn slab_count(&self) -> usize {
        self.slab_count_impl()
    }

    /// How many values each slab holds, slab by slab. They add up to the
    /// column's length (`sum_of`). How the values are spread over the
    /// slabs is a fact of how the column lies in memory: nothing else is
    /// specified of it.
    #[inline(always)]
    pub fn slab_lens(&self) -> (r: Vec<usize>)
        ensures
            sum_of(r@) == self@.len(),
    {
        self.slab_lens_impl()
    }

    /// How many runs each slab holds, slab by slab. That is a fact of
    /// how the column lies in memory: nothing is specified of it.
    #[inline(always)]
    pub fn slab_segments(&self) -> Vec<usize> {
        self.slab_segments_impl()
    }

    /// The column's kind of index is kept for the values the column
    /// holds (`VKeeps::keeps`; of a plain column this says nothing).
    /// So what a column saves loads as a column of its kind, if it
    /// loads as a plain one.
    pub proof fn lemma_kept(tracked &self)
        ensures
            K::keeps(self@),
    {
        self.lemma_kept_impl()
    }

    /// The value at `i`, if the column has one there.
    #[inline(always)]
    pub fn get(&self, i: usize) -> (r: Option<Get<'_, V>>)
        ensures
            i < self@.len() ==> (r matches Some(v) && v.sv() == self@[i as int]),
            i >= self@.len() ==> r is None,
    {
        self.get_impl(i)
    }

    /// The values.
    #[inline(always)]
    pub fn to_vec(&self) -> (r: Vec<Get<'_, V>>)
        ensures
            VGet::svs(r@) == self@,
    {
        self.to_vec_impl()
    }

    /// Whether every value is `value`: so of an empty column.
    #[inline(always)]
    pub fn is_only(&self, value: Get<'_, V>) -> (r: bool)
        ensures
            r == only(self@, value.sv()),
    {
        self.is_only_impl(value)
    }

    /// Where `value` is among the values of `range`, which are taken to
    /// be in order: the range of the places that hold it or, where none
    /// does, the empty range at the place where it would stand. `range`
    /// is clamped to the column (`bounds_in`): call `start..end` the
    /// places it names there.
    ///
    /// The search looks at few of the values: it is a search by halves
    /// over the column's slabs and then a walk along the runs of one or
    /// a few of them. The values need be in order only as far as
    /// `value` can tell (`sorted_for`), which values in order
    /// (`sorted`) are for every `value` (`lemma_sorted`). Where
    /// they are not, the range is some range within `start..end`.
    #[inline(always)]
    pub fn scope_to_value<R: RangeBounds<usize>>(&self, value: Get<'_, V>, range: R) -> (r: Range<usize>)
        ensures
            ({
                let (start, end) = bounds_in(range.spec_start_bound(), range.spec_end_bound(), self@.len());
                &&& start <= r.start <= r.end <= end
                &&& sorted_for::<V>(self@.subrange(start, end), value.sv()) ==> forall|k: int|
                    start <= k < end ==> {
                        &&& (k < r.start) == V::below(#[trigger] self@[k], value.sv())
                        &&& (r.start <= k < r.end) == (self@[k] == value.sv())
                        &&& (r.end <= k) == V::below(value.sv(), self@[k])
                    }
            }),
    {
        self.scope_to_value_impl::<R>(value, range)
    }

    /// Replace each value by what `f` gives for it. `f` is called once
    /// for a run of equal values that lie together, and what it gives
    /// stands for all of them: so every place holds what `f` gave for
    /// the value that was there (`mapped`), and where `f` can give
    /// more than one value for a value, places apart may get different
    /// ones. `f` has to accept every value of the column, and need
    /// accept no other.
    ///
    /// If `f` panics, the column is as it was. The column keeps its
    /// segment budget, and its slabs are cut anew, as those of a loaded
    /// column are. It counts an edit more, whatever `f` gives and of a
    /// column with no value too.
    #[inline(always)]
    pub fn remap<F: Fn(V) -> V>(&mut self, f: F)
        requires
            forall|v: V| old(self)@.contains(V::sv_of(v)) ==> #[trigger] call_requires(f, (v,)),
        ensures
            final(self)@.len() == old(self)@.len(),
            forall|i: int| 0 <= i < old(self)@.len() ==> mapped(f, old(self)@[i], #[trigger] final(self)@[i]),
            final(self).edits() == after_edit(old(self).edits()),
    {
        self.remap_impl::<F>(f)
    }

    /// An iterator over the values.
    #[inline(always)]
    pub fn iter(&self) -> (it: Iter<'_, V, C, K>)
        ensures
            it.rest() == self@,
            it.column() == self@,
            it.source() == Some(*self),
            it.pos() == 0,
            it.end_pos() == self@.len(),
    {
        self.iter_impl()
    }

    /// An iterator over the values of `range`, clamped to the column
    /// (`range_in`): over `start..end`.
    #[inline(always)]
    pub fn iter_range(&self, range: Range<usize>) -> (it: Iter<'_, V, C, K>)
        ensures
            ({
                let (start, end) = range_in(range, self@.len());
                &&& it.rest() == self@.subrange(start, end)
                &&& it.column() == self@
                &&& it.source() == Some(*self)
                &&& it.pos() == start
                &&& it.end_pos() == end
            }),
    {
        self.iter_range_impl(range)
    }

    /// The column, saved: the encoding of its values.
    #[inline(always)]
    pub fn save(&self) -> (r: Vec<u8>)
        ensures
            Column::<V, C>::valid(r@),
            Column::<V, C>::values(r@) == self@,
            r@ == Column::<V, C>::encoding(self@),
            r@.len() <= V::size_bound(self@),
    {
        self.save_impl()
    }

    /// Save the column after what `out` holds: `out` is then what it
    /// was, and after that the encoding of the column's values. The
    /// range returned is where in `out` that is.
    #[inline(always)]
    pub fn save_to(&self, out: &mut Vec<u8>) -> (r: Range<usize>)
        ensures
            final(out)@ == old(out)@ + Column::<V, C>::encoding(self@),
            Column::<V, C>::valid(Column::<V, C>::encoding(self@)),
            Column::<V, C>::values(Column::<V, C>::encoding(self@)) == self@,
            Column::<V, C>::encoding(self@).len() <= V::size_bound(self@),
            r.start == old(out)@.len(),
            r.end == final(out)@.len(),
    {
        self.save_to_impl(out)
    }

    /// [`save_to`](Self::save_to), unless every value of the column is
    /// `value`: then nothing is written, and the range is empty. So is
    /// it for an empty column, whatever `value` is.
    #[inline(always)]
    pub fn save_to_unless(&self, out: &mut Vec<u8>, value: Get<'_, V>) -> (r: Range<usize>)
        ensures
            final(out)@ == if only(self@, value.sv()) {
                old(out)@
            } else {
                old(out)@ + Column::<V, C>::encoding(self@)
            },
            !only(self@, value.sv()) ==> {
                &&& Column::<V, C>::valid(Column::<V, C>::encoding(self@))
                &&& Column::<V, C>::values(Column::<V, C>::encoding(self@)) == self@
                &&& Column::<V, C>::encoding(self@).len() <= V::size_bound(self@)
            },
            r.start == old(out)@.len(),
            r.end == final(out)@.len(),
    {
        self.save_to_unless_impl(out, value)
    }

    /// Save the column and read the bytes through, as a load does: an
    /// error if they are not a saved column.
    ///
    /// That they are one is proved of [`save`](Self::save). So this
    /// returns no error for a column that saves to less than 4 GiB, as
    /// one does whose values' `size_bound` is less. (A longer column
    /// saves to a saved column too. But its last run may be more bytes
    /// than a slab can count, and the read refuses that.)
    #[inline(always)]
    pub fn validate_encoding(&self) -> (r: Result<(), PackError>)
        ensures
            Column::<V, C>::encoding(self@).len() <= u32::MAX ==> r is Ok,
            V::size_bound(self@) <= u32::MAX ==> r is Ok,
    {
        self.validate_encoding_impl()
    }
}

// The loads. What is saved is spoken of as a plain column's, whatever
// the kind of the column loaded.
impl<V: ColumnValueRef<C>, C: Codec, K: Index<V, C>> Column<V, C, K> {
    /// The column saved as `bytes`. An error unless `bytes` is a saved
    /// column; and no error for one that is `loadable` and whose values
    /// the column's kind of index is kept for (`VKeeps::keeps`: for a
    /// plain column, any).
    #[inline(always)]
    pub fn load(bytes: &[u8]) -> (r: Result<Self, PackError>)
        ensures
            Column::<V, C>::loadable(bytes@) && K::keeps(Column::<V, C>::values(bytes@)) ==> r is Ok,
            r matches Ok(c) ==> Column::<V, C>::valid(bytes@) && c@ == Column::<V, C>::values(bytes@),
    {
        Self::load_impl(bytes)
    }

    /// [`load`](Self::load) with options ([`LoadOpts`]): how many values
    /// the column is to have, a value to fill it with, and how many runs
    /// a slab may hold.
    ///
    /// With a length and a fill value, empty `bytes` load as that many
    /// copies of the value, if a column can be that long (`fill_of`). If
    /// it cannot, or with no fill value, `bytes` load as what they are;
    /// and a column of another length than the one asked for is an error
    /// ([`PackError::InvalidLength`]), as empty `bytes` are for every
    /// length but 0.
    ///
    /// A slab holds two runs or more, which is a `requires`. The fill
    /// value need not live as long as `bytes` do, nor `bytes` as long as
    /// it: the column owns what it holds.
    #[inline(always)]
    pub fn load_with<'a, F: MaybeFill<Get<'a, V>>>(bytes: &[u8], opts: LoadOpts<F>) -> (r: Result<Self, PackError>)
        requires
            opts.max_segments() >= 2,
        ensures
            match fill_of(bytes@, opts.length(), opts.fill().value()) {
                Some((len, v)) => r matches Ok(c) && c@ == Seq::new(len as nat, |_i: int| v.sv()),
                None => {
                    &&& r matches Ok(c) ==> {
                        &&& Column::<V, C>::valid(bytes@)
                        &&& c@ == Column::<V, C>::values(bytes@)
                        &&& opts.length() matches Some(len) ==> c@.len() == len
                    }
                    &&& Column::<V, C>::loadable(bytes@) && K::keeps(Column::<V, C>::values(bytes@))
                        && (opts.length() matches Some(len) ==> Column::<V, C>::values(bytes@).len() == len)
                        ==> r is Ok
                },
            },
    {
        Self::load_with_impl::<F>(bytes, opts)
    }
}

// ── The iterator ────────────────────────────────────────────────────────────

/// `r` is what folding `f` over the values `vs` from `init` gives: what
/// `f` has made of `init` and then of each value in turn.
pub open spec fn folded<B, T, F: FnMut(B, T) -> B>(f: F, init: B, vs: Seq<T>, r: B) -> bool
    decreases vs.len(),
{
    if vs.len() == 0 {
        r == init
    } else {
        exists|b: B| #[trigger] f.ensures((init, vs[0]), b) && folded(f, b, vs.drop_first(), r)
    }
}

/// The values `o` begin with a run of `n` values `v`, which ends there:
/// `o` has no more, or its next value is another.
pub open spec fn head_run<S>(o: Seq<S>, v: S, n: int) -> bool {
    &&& 1 <= n <= o.len()
    &&& forall|i: int| 0 <= i < n ==> #[trigger] o[i] == v
    &&& n < o.len() ==> o[n] != v
}

/// The values `o` as runs: each run is of as many equal values as lie
/// together, so that no two runs side by side are of one value.
pub open spec fn runs_of<T>(o: Seq<T>) -> Seq<Run<T>>
    decreases o.len(),
{
    if o.len() == 0 {
        Seq::empty()
    } else {
        let r = runs_of(o.drop_first());
        if r.len() > 0 && r[0].value == o[0] {
            seq![Run { count: (r[0].count + 1) as usize, value: o[0] }] + r.drop_first()
        } else {
            seq![Run { count: 1, value: o[0] }] + r
        }
    }
}

/// Values have a first run if there are any: it is of the first value,
/// and no longer than the values are.
pub proof fn lemma_runs_first<T>(o: Seq<T>)
    requires
        o.len() <= usize::MAX,
    ensures
        (runs_of(o).len() == 0) == (o.len() == 0),
        o.len() > 0 ==> runs_of(o)[0].value == o[0] && 1 <= runs_of(o)[0].count <= o.len(),
    decreases o.len(),
{
    imp::lemma_runs_first_impl::<T>(o)
}

/// If `o` begins with a run of `n` values `v` that ends there
/// (`head_run`), the runs of `o` are that run and the runs of what
/// comes after it.
pub proof fn lemma_runs_of<T>(o: Seq<T>, v: T, n: usize)
    requires
        head_run(o, v, n as int),
        o.len() <= usize::MAX,
    ensures
        runs_of(o) == seq![Run { count: n, value: v }] + runs_of(o.skip(n as int)),
    decreases n,
{
    imp::lemma_runs_of_impl::<T>(o, v, n)
}

/// The values of the runs of `o` are `o`'s.
pub proof fn lemma_runs_of_values<G: VGet>(o: Seq<G>)
    requires
        o.len() <= usize::MAX,
    ensures
        run_values(runs_of(o)) == G::svs(o),
    decreases o.len(),
{
    imp::lemma_runs_of_values_impl::<G>(o)
}

/// An iterator over a column's values, which
/// [`Column::iter`] and [`Column::iter_range`] give.
///
/// It is a `std::iter::Iterator`. What it has left to give is in a
/// specification `items()`, and as values `rest()`. Verus's
/// specifications of the standard library's iterator functions speak of
/// `remaining()`, which is `items()`.
pub struct Iter<'a, V: ColumnValueRef<C>, C: Codec = Leb128, K: Index<V, C> = LenIndex> {
    it: ICore<<V::X as Decoders<'a>>::D>,
    slabs: &'a [Slab<<V::E as Slabs>::Tail>],
    /// The column the slabs are of; a default iterator has none.
    col: Option<&'a KCore<V, C, K>>,
}

impl<'a, V: ColumnValueRef<C>, C: Codec, K: Index<V, C>> Iter<'a, V, C, K> {
    /// What the iterator has left to give.
    pub closed spec fn items(&self) -> Seq<Get<'a, V>> {
        self.it.out::<V::X>(self.slabs@)
    }

    /// The values the iterator has left to give.
    pub open spec fn rest(&self) -> Seq<V::S> {
        <Get<'a, V> as VGet>::svs(self.items())
    }

    /// The values of the column the iterator reads: all of them, whatever
    /// the iterator's window.
    pub closed spec fn column(&self) -> Seq<V::S> {
        <Get<'a, V> as VGet>::svs(vals_of::<V::X>(self.slabs@))
    }

    /// The column the iterator reads, as it was when the iterator was
    /// made of it; a default iterator reads none. No method of the
    /// iterator changes it.
    pub closed spec fn source(&self) -> Option<Column<V, C, K>> {
        match self.col {
            Some(c) => Some(Column { core: *c }),
            None => None,
        }
    }

    /// [`pos`](Self::pos), as a specification sees it: there it is
    /// written `pos()`.
    pub closed spec fn spec_pos(&self) -> usize {
        self.it.pos
    }

    /// [`end_pos`](Self::end_pos), as a specification sees it: there it
    /// is written `end_pos()`.
    pub closed spec fn spec_end_pos(&self) -> usize {
        (self.it.pos + self.it.items_left) as usize
    }

    /// The iterator has no more values left than a `usize` counts, and
    /// as many in either form.
    pub proof fn lemma_items_len(&self)
        ensures
            self.items().len() <= usize::MAX,
            self.rest().len() == self.items().len(),
    {
        self.lemma_items_len_impl()
    }

    /// What the iterator reads is what its column holds: of
    /// [`source`](Self::source) and [`column`](Self::column), the first
    /// tells the second.
    pub proof fn lemma_source(tracked &self)
        ensures
            match self.source() {
                Some(c) => self.column() == c@,
                None => self.column().len() == 0,
            },
    {
        self.lemma_source_impl()
    }

    /// Where the iterator stands, and what it has left: its column's
    /// values from [`pos`](Self::pos) up to [`end_pos`](Self::end_pos), as
    /// far as the column goes (`window`). It stands in the column or at
    /// its end.
    pub proof fn lemma_rest(tracked &self)
        ensures
            self.pos() <= self.end_pos(),
            self.pos() <= self.column().len(),
            self.rest() == window(self.column(), self.pos() as int, self.end_pos() as int),
    {
        self.lemma_rest_impl()
    }

    /// The index in the column of the value `next` gives, if it gives
    /// one.
    ///
    /// What the iterator has left is the column's values from here up to
    /// [`end_pos`](Self::end_pos), as far as the column goes. The
    /// iterator stands in the column or at its end
    /// (`lemma_rest`).
    #[verifier::when_used_as_spec(spec_pos)]
    #[inline(always)]
    pub fn pos(&self) -> (r: usize)
        ensures
            r == self.pos(),
    {
        self.pos_impl()
    }

    /// Where the iterator's window ends: the index in the column that
    /// its last value comes before. It can lie past the column's end
    /// ([`set_max`](Self::set_max)), and the iterator then gives what the
    /// column has. Once the iterator has given all it has, its window
    /// ends where it stands.
    #[verifier::when_used_as_spec(spec_end_pos)]
    #[inline(always)]
    pub fn end_pos(&self) -> (r: usize)
        ensures
            r == self.end_pos(),
    {
        self.end_pos_impl()
    }

    /// The index of the slab the iterator stands in. This is how the
    /// column lies in memory, of which no specification speaks.
    #[inline(always)]
    pub fn get_slab(&self) -> usize {
        self.get_slab_impl()
    }

    /// Where the iterator stands and where its window ends, to keep
    /// without the borrow of the column:
    /// [`IterState::try_resume`] gives an iterator from it.
    ///
    /// The state fits the column the iterator reads: resumed on that
    /// column, as it is, it gives an iterator that stands where this one
    /// does and has the same values left.
    #[inline(always)]
    pub fn suspend(&self) -> (s: IterState)
        ensures
            s.pos() == self.pos(),
            s.end_pos() == self.end_pos(),
            self.rest() == window(self.column(), s.pos() as int, s.end_pos() as int),
            match self.source() {
                Some(c) => s.fits(&c),
                None => true,
            },
    {
        self.suspend_impl()
    }

    /// Make the window end at `end`: the iterator then gives the
    /// column's values from where it stands up to `end`, and none if it
    /// stands at `end` or past it.
    ///
    /// This can widen the window as well as narrow it. With an `end`
    /// past the column's, the iterator gives what the column has.
    #[inline(always)]
    pub fn set_max(&mut self, end: usize)
        ensures
            final(self).column() == old(self).column(),
            final(self).source() == old(self).source(),
            final(self).pos() == old(self).pos(),
            final(self).end_pos() == if end >= old(self).pos() {
                end
            } else {
                old(self).pos()
            },
            final(self).rest() == window(old(self).column(), old(self).pos() as int, end as int),
    {
        self.set_max_impl(end)
    }

    /// Move on so that the value at index `target` of the column comes
    /// next. The iterator moves only forwards: at `target` or past it
    /// already, it stays. With a `target` past what it has left, it
    /// gives all it has, and stands at the end of that with nothing left.
    ///
    /// Few of the values passed over are looked at ([`nth`](Self::nth)).
    #[inline(always)]
    pub fn advance_to(&mut self, target: usize)
        ensures
            final(self).column() == old(self).column(),
            final(self).source() == old(self).source(),
            final(self).rest() == past(old(self).rest(), target - old(self).pos()),
            target <= old(self).pos() ==> {
                &&& final(self).pos() == old(self).pos()
                &&& final(self).end_pos() == old(self).end_pos()
            },
            old(self).pos() < target <= old(self).pos() + old(self).rest().len() ==> {
                &&& final(self).pos() == target
                &&& final(self).end_pos() == old(self).end_pos()
            },
            target > old(self).pos() + old(self).rest().len() ==> {
                &&& final(self).pos() == old(self).pos() + old(self).rest().len()
                &&& final(self).end_pos() == final(self).pos()
            },
    {
        self.advance_to_impl(target)
    }

    /// Move `amount` values on: past as many as the iterator has left,
    /// if it has fewer.
    #[inline(always)]
    pub fn advance_by(&mut self, amount: usize)
        ensures
            final(self).column() == old(self).column(),
            final(self).source() == old(self).source(),
            final(self).rest() == past(old(self).rest(), amount as int),
            amount <= old(self).rest().len() ==> {
                &&& final(self).pos() == old(self).pos() + amount
                &&& final(self).end_pos() == old(self).end_pos()
            },
            amount > old(self).rest().len() ==> {
                &&& final(self).pos() == old(self).pos() + old(self).rest().len()
                &&& final(self).end_pos() == final(self).pos()
            },
    {
        self.advance_by_impl(amount)
    }

    /// Move the window to `range`: the iterator then gives the column's
    /// values there. It moves only forwards: if `range` starts before
    /// the iterator, the window starts where the iterator stands.
    ///
    /// If `range` starts past its own end or the column's, the iterator
    /// has nothing left, and stands where `range` ends or the column
    /// does, whichever comes first, if that is ahead of it.
    #[inline(always)]
    pub fn shift(&mut self, range: Range<usize>)
        ensures
            final(self).column() == old(self).column(),
            final(self).source() == old(self).source(),
            final(self).rest() == window(
                old(self).column(),
                if range.start <= old(self).pos() {
                    old(self).pos() as int
                } else {
                    range.start as int
                },
                range.end as int,
            ),
            range.start <= old(self).pos() ==> {
                &&& final(self).pos() == old(self).pos()
                &&& final(self).end_pos() == if range.end >= old(self).pos() {
                    range.end
                } else {
                    old(self).pos()
                }
            },
            old(self).pos() < range.start <= range.end && range.start <= old(self).column().len() ==> {
                &&& final(self).pos() == range.start
                &&& final(self).end_pos() == range.end
            },
            old(self).pos() < range.start && (range.start > range.end || range.start > old(self).column().len()) ==> {
                &&& final(self).pos() == if range.end <= old(self).pos() {
                    old(self).pos() as int
                } else if range.end <= old(self).column().len() {
                    range.end as int
                } else {
                    old(self).column().len() as int
                }
                &&& final(self).end_pos() == final(self).pos()
            },
    {
        self.shift_impl(range)
    }

    /// Move the window to `range` and give its first value, if it has
    /// one. `range` is to start at the iterator or after it.
    ///
    /// If the window has no value, the iterator has nothing left, and
    /// stands where [`shift`](Self::shift) leaves it then.
    #[inline(always)]
    pub fn shift_next(&mut self, range: Range<usize>) -> (r: Option<Get<'a, V>>)
        requires
            range.start >= old(self).pos(),
        ensures
            final(self).column() == old(self).column(),
            final(self).source() == old(self).source(),
            ({
                let w = window(old(self).column(), range.start as int, range.end as int);
                &&& w.len() > 0 ==> {
                    &&& r matches Some(v) && v.sv() == w[0]
                    &&& final(self).rest() == w.skip(1)
                    &&& final(self).pos() == range.start + 1
                    &&& final(self).end_pos() == range.end
                }
                &&& w.len() == 0 ==> {
                    &&& r is None
                    &&& final(self).rest().len() == 0
                    &&& final(self).pos() == if range.end <= old(self).pos() {
                        old(self).pos() as int
                    } else if range.end <= old(self).column().len() {
                        range.end as int
                    } else {
                        old(self).column().len() as int
                    }
                    &&& final(self).end_pos() == final(self).pos()
                }
            }),
    {
        self.shift_next_impl(range)
    }

    /// The next run: the value the iterator gives next, and how many
    /// times it comes before another value does or the iterator has no
    /// more. The iterator moves past the whole run.
    ///
    /// A run is as long as the values make it: how the column lies in
    /// memory does not cut it short.
    #[inline(always)]
    pub fn next_run(&mut self) -> (r: Option<Run<Get<'a, V>>>)
        ensures
            final(self).column() == old(self).column(),
            final(self).source() == old(self).source(),
            match r {
                Some(run) => {
                    &&& head_run(old(self).items(), run.value, run.count as int)
                    &&& head_run(old(self).rest(), run.value.sv(), run.count as int)
                    &&& final(self).items() == old(self).items().skip(run.count as int)
                    &&& final(self).rest() == old(self).rest().skip(run.count as int)
                    &&& final(self).pos() == old(self).pos() + run.count
                    &&& final(self).end_pos() == old(self).end_pos()
                },
                None => {
                    &&& old(self).items().len() == 0
                    &&& old(self).rest().len() == 0
                    &&& final(self).items().len() == 0
                    &&& final(self).rest().len() == 0
                    &&& final(self).pos() == old(self).pos()
                    &&& final(self).end_pos() == old(self).pos()
                },
            },
    {
        self.next_run_impl()
    }

    /// An iterator of the runs the iterator has left to give
    /// ([`next_run`](Self::next_run)): what
    /// [`splice_runs`](Column::splice_runs) takes.
    #[inline(always)]
    pub fn runs(self) -> (r: Runs<'a, V, C, K>)
        ensures
            r.iter() == self,
    {
        self.runs_impl()
    }

    /// Move on to the first of the values left that is `target`, and
    /// past it: its index in the column is given. If none of them is
    /// `target`, the iterator gives all it has, as
    /// [`advance_to`](Self::advance_to) leaves it then.
    ///
    /// The values are read a run at a time.
    // a Verus loop with an invariant: `loop` + `match`
    #[allow(clippy::while_let_loop)]
    #[inline(always)]
    pub fn scan_to_value(&mut self, target: Get<'_, V>) -> (r: Option<usize>)
        ensures
            final(self).column() == old(self).column(),
            final(self).source() == old(self).source(),
            match r {
                Some(at) => {
                    &&& old(self).pos() <= at < old(self).pos() + old(self).rest().len()
                    &&& old(self).rest()[at - old(self).pos()] == target.sv()
                    &&& forall|i: int| 0 <= i < at - old(self).pos() ==> #[trigger] old(self).rest()[i] != target.sv()
                    &&& final(self).items() == old(self).items().skip(at - old(self).pos() + 1)
                    &&& final(self).rest() == old(self).rest().skip(at - old(self).pos() + 1)
                    &&& final(self).pos() == at + 1
                    &&& final(self).end_pos() == old(self).end_pos()
                },
                None => {
                    &&& forall|i: int| 0 <= i < old(self).rest().len() ==> #[trigger] old(self).rest()[i] != target.sv()
                    &&& final(self).items().len() == 0
                    &&& final(self).rest().len() == 0
                    &&& final(self).pos() == old(self).pos() + old(self).rest().len()
                    &&& final(self).end_pos() == final(self).pos()
                },
            },
    {
        self.scan_to_value_impl(target)
    }

    /// Search the values left for `target`, where they are in order,
    /// and move on to it: given is the range of the places in the column
    /// that hold it, and the iterator gives the first of them next. If
    /// no value is `target`, the range is the empty one at the place
    /// where it would stand, and the iterator stands there. The window
    /// keeps its end, unless `range` starts after all that the iterator
    /// has left: the iterator then moves to its end, and has nothing
    /// left.
    ///
    /// The search is over `range` of the column, as far as that lies
    /// in what the iterator has left: the iterator moves on to where
    /// `range` starts first ([`advance_to`](Self::advance_to)), and
    /// looks no further than where it ends. The bounds are read as
    /// `bound_start` and `bound_end` say: `..` is all the iterator has
    /// left.
    ///
    /// Few of the values are looked at
    /// ([`scope_to_value`](Column::scope_to_value)). They need be
    /// in order only as far as `target` can tell (`sorted_for`); where
    /// they are not, the range is some range of the places searched.
    #[inline(always)]
    pub fn seek_to_value<R: RangeBounds<usize>>(&mut self, target: Get<'_, V>, range: R) -> (r: Range<usize>)
        ensures
            final(self).column() == old(self).column(),
            final(self).source() == old(self).source(),
            ({
                let (p, o, t) = (old(self).pos(), old(self).rest(), target.sv());
                // where the range starts and ends
                let (rs, re) = (
                    bound_start(range.spec_start_bound()),
                    bound_end(range.spec_start_bound(), range.spec_end_bound(), old(self).end_pos() as int),
                );
                // how far the iterator moves before it searches, and the
                // values searched
                let a = if rs <= p {
                    0
                } else if rs - p <= o.len() {
                    rs - p
                } else {
                    o.len() as int
                };
                let w = window(o, a, re - p);
                &&& p + a <= r.start <= r.end <= p + a + w.len()
                &&& final(self).pos() == r.start
                &&& final(self).rest() == o.skip(r.start - p)
                &&& final(self).end_pos() == if rs <= p + o.len() {
                    old(self).end_pos()
                } else {
                    final(self).pos()
                }
                &&& sorted_for::<V>(w, t) ==> forall|j: int|
                    a <= j < a + w.len() ==> {
                        &&& (p + j < r.start) == V::below(#[trigger] o[j], t)
                        &&& (r.start <= p + j < r.end) == (o[j] == t)
                        &&& (r.end <= p + j) == V::below(t, o[j])
                    }
            }),
    {
        self.seek_to_value_impl::<R>(target, range)
    }
}

#[cfg(verus_keep_ghost)]
impl<'a, V: ColumnValueRef<C>, C: Codec, K: Index<V, C>> vstd::std_specs::iter::IteratorSpecImpl for Iter<'a, V, C, K> {
    /// The iterator keeps to what Verus asks of an iterator: `next` gives
    /// `remaining()`, one value after another, and then `None`.
    open spec fn obeys_prophetic_iter_laws(&self) -> bool {
        true
    }

    open spec fn remaining(&self) -> Seq<Get<'a, V>> {
        self.items()
    }

    open spec fn will_return_none(&self) -> bool {
        true
    }

    open spec fn decrease(&self) -> Option<nat> {
        Some(self.items().len())
    }

    open spec fn peek(&self, index: int) -> Option<Get<'a, V>> {
        if 0 <= index < self.items().len() {
            Some(self.items()[index])
        } else {
            None
        }
    }
}

impl<'a, V: ColumnValueRef<C>, C: Codec, K: Index<V, C>> Iterator for Iter<'a, V, C, K> {
    type Item = Get<'a, V>;

    /// The next value, if one is left.
    #[inline(always)]
    fn next(&mut self) -> (r: Option<Get<'a, V>>)
        ensures
            match r {
                Some(v) => {
                    &&& old(self).rest().len() > 0
                    &&& v.sv() == old(self).rest()[0]
                    &&& final(self).rest() == old(self).rest().skip(1)
                    &&& final(self).pos() == old(self).pos() + 1
                    &&& final(self).end_pos() == old(self).end_pos()
                },
                None => {
                    &&& old(self).rest().len() == 0
                    &&& final(self).rest().len() == 0
                    &&& final(self).pos() == old(self).pos()
                    &&& final(self).end_pos() == old(self).pos()
                },
            },
            final(self).column() == old(self).column(),
            final(self).source() == old(self).source(),
    {
        self.next_impl()
    }

    /// The `n`th of the values left, counted from 0, if there are that
    /// many: it and the values before it are then given. If there are
    /// not, none is left.
    ///
    /// Few of the values passed over are looked at: a long move asks the
    /// column's index for the slab to go to, and within a slab a whole
    /// run is passed over at once.
    #[inline(always)]
    fn nth(&mut self, n: usize) -> (r: Option<Get<'a, V>>)
        ensures
            n < old(self).items().len() ==> {
                &&& r == Some(old(self).items()[n as int])
                &&& final(self).items() == old(self).items().skip(n + 1)
                &&& r matches Some(v) && v.sv() == old(self).rest()[n as int]
                &&& final(self).rest() == old(self).rest().skip(n + 1)
                &&& final(self).pos() == old(self).pos() + n + 1
                &&& final(self).end_pos() == old(self).end_pos()
            },
            n >= old(self).items().len() ==> {
                &&& r is None
                &&& final(self).items().len() == 0
                &&& final(self).pos() == old(self).pos() + old(self).items().len()
                &&& final(self).end_pos() == final(self).pos()
            },
            final(self).column() == old(self).column(),
            final(self).source() == old(self).source(),
    {
        self.nth_impl(n)
    }

    /// How many values are left: exactly.
    #[inline(always)]
    fn size_hint(&self) -> (r: (usize, Option<usize>))
        ensures
            r.0 == self.items().len(),
            r.0 == self.rest().len(),
            r.1 == Some(r.0),
    {
        self.size_hint_impl()
    }

    /// What `f` makes of `init` and each of the values left in turn. `f`
    /// is to take any value with any accumulator.
    ///
    /// The values are read a run at a time, as are those the standard
    /// library reads through `fold` (`count`, `last`, `sum`, `for_each`
    /// among them): faster than a loop over `next` where values come in
    /// runs, and slower where few values repeat.
    #[inline(always)]
    fn fold<B, F>(self, init: B, f: F) -> (r: B) where F: FnMut(B, Get<'a, V>) -> B
        requires
            forall|b: B, v: Get<'a, V>| #[trigger] f.requires((b, v)),
        ensures
            folded(f, init, self.items(), r),
    {
        self.fold_impl::<B, F>(init, f)
    }
}

#[cfg(verus_keep_ghost)]
impl<'a, V: ColumnValueRef<C>, C: Codec, K: Index<V, C>> vstd::std_specs::iter::ExactSizeIteratorSpecImpl for Iter<'a, V, C, K> {
    open spec fn exact_len(&self) -> usize {
        self.items().len() as usize
    }
}

impl<'a, V: ColumnValueRef<C>, C: Codec, K: Index<V, C>> ExactSizeIterator for Iter<'a, V, C, K> {
    /// How many values are left.
    #[inline]
    fn len(&self) -> (len: usize)
        ensures
            len == self.items().len(),
            len == self.rest().len(),
    {
        self.size_hint().0
    }
}

impl<'a, V: ColumnValueRef<C>, C: Codec, K: Index<V, C>> Clone for Iter<'a, V, C, K> {
    /// An iterator of its own with the same values left.
    #[inline(always)]
    fn clone(&self) -> (r: Self)
        ensures
            r.items() == self.items(),
            r.column() == self.column(),
            r.source() == self.source(),
            r.pos() == self.pos(),
            r.end_pos() == self.end_pos(),
    {
        self.clone_impl()
    }
}

impl<'a, V: ColumnValueRef<C>, C: Codec, K: Index<V, C>> Default for Iter<'a, V, C, K> {
    /// An iterator with nothing to give.
    #[inline(always)]
    fn default() -> (r: Self)
        ensures
            r.items().len() == 0,
            r.rest().len() == 0,
            r.column().len() == 0,
            r.source() is None,
            r.pos() == 0,
            r.end_pos() == 0,
    {
        Self::default_impl()
    }
}

impl<'a, V: ColumnValueRef<C>, C: Codec, K: Index<V, C>> IntoIterator for &'a Column<V, C, K> {
    type Item = Get<'a, V>;

    type IntoIter = Iter<'a, V, C, K>;

    /// An iterator over the values ([`Column::iter`]).
    #[inline(always)]
    fn into_iter(self) -> (it: Iter<'a, V, C, K>)
        ensures
            it.rest() == self@,
            it.column() == self@,
            it.source() == Some(*self),
            it.pos() == 0,
            it.end_pos() == self@.len(),
    {
        self.iter()
    }
}


/// Where a [`Iter`] stood and where its window ended, kept
/// without the borrow of the column: [`Iter::suspend`] gives it,
/// and [`try_resume`](Self::try_resume) an iterator from it.
///
/// It holds the iterator's place in the column's slabs, and how many
/// slabs and edits the column counted then. It is of no value type: a
/// state fits any column whose slabs it is a place in.
pub struct IterState {
    counter: usize,
    num_slabs: usize,
    slab_idx: usize,
    pos: usize,
    items_left: usize,
    /// How many values the iterator had given of the slab it stood in.
    slab_consumed: usize,
}

impl IterState {
    /// Where the iterator stood: its `pos()`.
    pub closed spec fn pos(&self) -> usize {
        self.pos
    }

    /// Where the iterator's window ended: its `end_pos()`.
    pub closed spec fn end_pos(&self) -> usize {
        (self.pos + self.items_left) as usize
    }

    /// The count of edits of the column the state was taken of
    /// (`Column::edits`).
    pub closed spec fn edits(&self) -> usize {
        self.counter
    }

    /// The state is a place in `column` as it is: the column counts the
    /// edits and the slabs that the state does, the state's place is one
    /// in those slabs, and it is in the column or at its end.
    /// [`try_resume`](Self::try_resume) gives an iterator exactly then.
    pub closed spec fn fits<V: ColumnValueRef<C>, C: Codec, K: Index<V, C>>(&self, column: &Column<V, C, K>) -> bool {
        let lens = slab_lens(column.core.slabs_spec());
        &&& self.counter == column.core.count()
        &&& self.num_slabs == lens.len()
        &&& crate::iter_core::resumes(lens, self.slab_idx, self.slab_consumed, self.pos, self.items_left)
        &&& self.pos <= total(lens)
    }

    /// A state fits only a column that counts the edits it does. A
    /// column that has been edited since counts others
    /// (`lemma_after_edits`), so the state does not fit it, and
    /// [`try_resume`](Self::try_resume) refuses it.
    pub proof fn lemma_fits<V: ColumnValueRef<C>, C: Codec, K: Index<V, C>>(&self, column: &Column<V, C, K>)
        requires
            self.fits(column),
        ensures
            self.edits() == column.edits(),
    {
        self.lemma_fits_impl::<V, C, K>(column)
    }

    /// An iterator of `column` that stands where the suspended one stood,
    /// with its window, if the state fits the column (`fits`); an error
    /// ([`PackError::InvalidResume`]) if it does not.
    ///
    /// A state fits the column it was taken of, as long as that is not
    /// edited ([`Iter::suspend`]). The iterator given is one of
    /// `column` whatever column the state was taken of: it gives the
    /// values of `column` in the state's window. Where the state stands
    /// at the column's end, the iterator's window ends there.
    ///
    /// An edit moves the column's count of edits
    /// (`Column::edits`), as each method that edits says, so a
    /// state taken before an edit is refused after it (`lemma_fits`),
    /// and after more edits unless they are `usize::MAX + 1` or more
    /// (`lemma_after_edits`).
    ///
    /// Panics where an edit's panic
    /// left `column` without its parts and the state would fit it.
    #[inline(always)]
    pub fn try_resume<'a, V: ColumnValueRef<C>, C: Codec, K: Index<V, C>>(&self, column: &'a Column<V, C, K>) -> (r: Result<
        Iter<'a, V, C, K>,
        PackError,
    >)
        ensures
            r is Ok <==> self.fits(column),
            r is Ok ==> self.edits() == column.edits(),
            r matches Ok(it) ==> {
                &&& it.source() == Some(*column)
                &&& it.column() == column@
                &&& it.pos() == self.pos()
                &&& it.rest() == window(column@, self.pos() as int, self.end_pos() as int)
                &&& it.end_pos() == self.end_pos() || (self.pos() == column@.len() && it.end_pos() == self.pos())
            },
    {
        self.try_resume_impl::<V, C, K>(column)
    }
}

/// An iterator of the runs of a column's values, which
/// [`Iter::runs`] gives: each run is of as many equal values as
/// lie together in what the iterator had left (`runs_of`).
pub struct Runs<'a, V: ColumnValueRef<C>, C: Codec = Leb128, K: Index<V, C> = LenIndex>(
    Iter<'a, V, C, K>,
);

impl<'a, V: ColumnValueRef<C>, C: Codec, K: Index<V, C>> Runs<'a, V, C, K> {
    /// The iterator of values whose runs these are, where it stands now.
    pub closed spec fn iter(&self) -> Iter<'a, V, C, K> {
        self.0
    }
}

#[cfg(verus_keep_ghost)]
impl<'a, V: ColumnValueRef<C>, C: Codec, K: Index<V, C>> vstd::std_specs::iter::IteratorSpecImpl for Runs<'a, V, C, K> {
    /// The iterator keeps to what Verus asks of an iterator: `next` gives
    /// `remaining()`, one run after another, and then `None`.
    open spec fn obeys_prophetic_iter_laws(&self) -> bool {
        true
    }

    open spec fn remaining(&self) -> Seq<Run<Get<'a, V>>> {
        runs_of(self.iter().items())
    }

    open spec fn will_return_none(&self) -> bool {
        true
    }

    open spec fn decrease(&self) -> Option<nat> {
        Some(self.iter().items().len())
    }

    open spec fn peek(&self, index: int) -> Option<Run<Get<'a, V>>> {
        if 0 <= index < runs_of(self.iter().items()).len() {
            Some(runs_of(self.iter().items())[index])
        } else {
            None
        }
    }
}

impl<'a, V: ColumnValueRef<C>, C: Codec, K: Index<V, C>> Iterator for Runs<'a, V, C, K> {
    type Item = Run<Get<'a, V>>;

    /// The next run, if one is left ([`Iter::next_run`]).
    #[inline(always)]
    fn next(&mut self) -> (r: Option<Run<Get<'a, V>>>)
        ensures
            final(self).iter().column() == old(self).iter().column(),
            final(self).iter().source() == old(self).iter().source(),
            match r {
                Some(run) => {
                    &&& head_run(old(self).iter().items(), run.value, run.count as int)
                    &&& head_run(old(self).iter().rest(), run.value.sv(), run.count as int)
                    &&& final(self).iter().items() == old(self).iter().items().skip(run.count as int)
                    &&& final(self).iter().rest() == old(self).iter().rest().skip(run.count as int)
                    &&& final(self).iter().pos() == old(self).iter().pos() + run.count
                    &&& final(self).iter().end_pos() == old(self).iter().end_pos()
                },
                None => {
                    &&& old(self).iter().items().len() == 0
                    &&& old(self).iter().rest().len() == 0
                    &&& final(self).iter().items().len() == 0
                    &&& final(self).iter().rest().len() == 0
                    &&& final(self).iter().pos() == old(self).iter().pos()
                    &&& final(self).iter().end_pos() == old(self).iter().pos()
                },
            },
    {
        self.next_impl()
    }
}

} // verus!
