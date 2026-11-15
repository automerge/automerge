//! A verified public API for columns.
//!
//! [`VerifiedColumn`] is a column whose every operation carries a
//! specification that [Verus](https://github.com/verus-lang/verus) has
//! checked, stated in terms of the column's values as a sequence
//! (`col@`). A client that is itself checked by Verus can so prove what
//! its own code does with a column: that what it reads is what it wrote,
//! and that a column it saved loads again as the same values and saves
//! back to the same bytes (`client_proof` is such a client).
//!
//! It runs the same verified code as [`Column`](crate::Column): the same
//! slabs, index, cursor and loader. What differs is what stands around
//! that code. `Column` reaches a value type and a codec through its
//! packing traits, and takes its closures and iterators through small
//! adapters; Verus sees neither, and takes on trust that they do what the
//! specifications say. Here the codec (`codec`), the value types (`value`,
//! `counts`, `bytes`) and all that lies between this API and the core are
//! verified too.
//!
//! # Value types
//!
//! A `VerifiedColumn<V>` holds values of a type `V` that is a
//! [`VValue`]:
//!
//! * a number: `u64`, `i64`, `u32`, `i32`, `usize` or `NonZeroU32`;
//! * `bool`;
//! * a client's own type that is stored as one of those numbers
//!   ([`VNewtype`]). The client says which number each value is, and a
//!   load refuses bytes that hold a number that is none of its values;
//! * a byte string: `Vec<u8>`;
//! * `Option<T>` of a number, a client's type or a byte string `T`:
//!   `None` is a column's null.
//!
//! A column takes and gives a value in the type's form, [`Get<'a, V>`](Get).
//! For a number, an `Option` of one or a `bool` that is the type itself.
//! A column of `Vec<u8>` takes a byte string as a slice (`&[u8]`) and
//! gives one as a slice borrowed from the column, with no copy; one of
//! `Option<Vec<u8>>` takes and gives `Option<&[u8]>`.
//!
//! In a specification a column is the sequence of its values (`col@`),
//! each as its type's [`VSpec::S`]: a number as itself, a byte string as
//! its bytes (`Seq<u8>`). What a value in a column's form is there is its
//! `sv`, and `svs` of several ([`VGet`]): for a slice `v` that is `v@`,
//! and for a number the number.
//!
//! The second parameter of `VerifiedColumn<V, C>` is the codec the
//! column's numbers are written with, a [`VCodec`]. It is [`Leb128`]
//! where none is named, which is for now the only one.
//!
//! # What is proved
//!
//! Of every method, its `ensures`, for every input that meets its
//! `requires`, for every value type and codec. In particular:
//!
//! * [`splice`](VerifiedColumn::splice) leaves the values before `at`,
//!   the values given, then the values from `at + del`;
//! * [`get`](VerifiedColumn::get), [`to_vec`](VerifiedColumn::to_vec) and
//!   the [iterator](VerifiedIter) give the column's values;
//! * [`save`](VerifiedColumn::save) writes the one encoding of the
//!   column's values (`encoding`): bytes that are `valid` and hold those
//!   `values`, and no more of them than the values' `size_bound`. Two
//!   columns of the same values so save to the same bytes
//!   (`lemma_save_unique`);
//! * [`load`](VerifiedColumn::load) gives a column only for valid bytes,
//!   and then the column of their values; and it does give one for all
//!   valid bytes short of 4 GiB that hold values a `usize` can count
//!   (`loadable`).
//!
//! `valid`, `values`, `encoding` and `loadable` are specification
//! functions of a column type (`VerifiedColumn::<V>::valid(bytes)`),
//! `size_bound` is one of a value type (for byte strings it counts their
//! bytes, as `total_len` of them), and `lemma_save_unique` and
//! `lemma_encoding` are lemmas: they exist only for Verus, and a normal
//! build compiles them away.
//!
//! The specifications say what a saved column is in their own terms.
//! That those are the bytes of [`Column`](crate::Column) for the same
//! values is not proved but tested (`tests/verified_api.rs`): for each
//! value type the two save the same bytes, each loads what the other
//! saved, and they refuse the same bytes with the same error.
//!
//! # What is trusted
//!
//! Verus and its solver Z3, and Verus's specifications of the standard
//! library: of `Vec` and slices, and for a column of `NonZeroU32` those
//! of `NonZero`, which Verus keeps behind its feature
//! `nonzero_internals` (the crate is checked with it on). A column of
//! byte strings leans on more of them than one of numbers does: that a
//! slice is its bytes and no more, that a part of a slice is that part of
//! its bytes, and the copy of a slice into a `Vec`. Of this crate,
//! the functions Verus is told to take as they are, which are few and
//! small, and are those the verified core of [`Column`](crate::Column)
//! has too:
//!
//! * the helpers that panic, which say why and do nothing else. A method
//!   panics where a column would outgrow what its arithmetic can count
//!   (some `usize::MAX` values, or a slab of 4 GiB), and, like every Rust
//!   function, a method of this API may run out of memory;
//! * the constructors of [`PackError`], of which nothing is assumed
//!   ([`refused`] is the one for a client's type);
//! * two specifications of standard-library calls that Verus has none
//!   for: replacing a range of a `Vec` of bytes (`rle::writer::splice_bytes`)
//!   and of a `Vec` of slabs (`index::splice_vec`);
//! * three helpers of which nothing is assumed: an addition of counts
//!   whose sum the proofs take as unknown (`rle::writer::add`), the reuse
//!   of a buffer (`recycle`), and a constant (`carry_budget`).
//!
//! No value type's encoding is trusted, nor the codec's, and there is no
//! unverified code between this API and the core: every other function on
//! a path through this API is checked.
//!
//! A client's own value type is checked with the client: Verus proves
//! what [`VNewtype`] asks of it when it is run on the client's crate. A
//! client that is compiled without Verus has that to keep by itself, as
//! it has every `requires` of this API.
//!
//! That a [`splice`](VerifiedColumn::splice) returns is not proved: it
//! runs the core's loop over the values to insert, which Verus is not
//! asked to show ends. (It ends when the slice does.) Every other loop
//! is proved to end.

// Verus proofs read more plainly with `x = x + n`.
#![allow(clippy::len_zero, clippy::assign_op_pattern)]

pub(crate) mod bytes;
#[cfg(verus_keep_ghost)]
mod client_proof;
pub(crate) mod codec;
pub(crate) mod counts;
pub(crate) mod model;
pub(crate) mod value;

// spec functions exist only when Verus compiles the crate
#[cfg(verus_keep_ghost)]
pub use bytes::{total_len, total_len_opt};
pub use codec::VCodec;
pub use value::{refused, VNewtype, VPack};

use crate::bool_edit::BoolModel;
use crate::bool_wire::BoolReads;
use crate::column::Slab;
use crate::copy::NoRuns;
use crate::cursor::{ReadsAgree, RunFeed, Slabs};
use crate::iter_core::{Decode, Decoders, ICore};
#[cfg(verus_keep_ghost)]
use crate::rle::dec::slab_values;
use crate::rle::dec::RleReads;
use crate::rle::edit::RleModel;
use crate::{Leb128, PackError};
use counts::VB;
use model::{Core, Model};
use std::marker::PhantomData;
#[cfg(verus_keep_ghost)]
use value::{lemma_opt_sizes, lemma_plain_sizes};
use value::{VOps, VOptOps, VOptWrite, VWrite};
#[allow(unused_imports)]
use vstd::prelude::*;

mod seal {
    use super::model::Model;
    use super::{Items, VForm, VGet, VSpec};
    use crate::cursor::Slabs;
    use crate::iter_core::Decoders;

    vstd::prelude::verus! {

    /// A codec of the verified public API is one of this crate's: no
    /// other can name this trait.
    pub trait CodecSeal {

    }

    /// A type that packs as one number of a codec is one of this
    /// crate's, or says which of this crate's it is: no other can name
    /// this trait.
    pub trait PackSeal {

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
    pub trait Sealed<C>: Sized + VSpec + for <'a>VForm<'a> {
        /// How the values are read: as the form the type gives them in.
        type X: for <'a>Items<'a, Item = <Self as VForm<'a>>::Get> + for <'a>Decoders<
            'a,
            Tail = <Self::E as Slabs>::Tail,
        >;

        /// The slabs that hold them.
        type E: Slabs<V: VGet<S = <Self as VSpec>::S>> + Model<Self::X>;
    }

    } // verus!
}

verus! {

/// A slab's segment budget, where none is given: that of
/// [`Column`](crate::Column).
const MAX_SEG: usize = 64;

// ── What a value is to a client ─────────────────────────────────────────────

/// The form in which a column gives a value and takes one: the value
/// itself for a number or a `bool`, and for bytes a borrowed slice. The
/// forms are this crate's: no other type can implement this trait.
///
/// A column's specifications speak of values as `S`: `v.sv()` is the
/// value `v` there. For a number that is the number, and for a slice it
/// is the slice's bytes (`v@`).
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

impl<T: VPack> seal::GetSeal for T {

}

impl<T: VPack> VGet for T {
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

impl<T: VPack> seal::GetSeal for Option<T> {

}

impl<T: VPack> VGet for Option<T> {
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

impl seal::GetSeal for bool {

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
/// Every [`VValue`] is one, and no other type: the trait cannot be
/// implemented outside this crate.
pub trait VSpec: seal::SpecSeal {
    /// What a value is in a specification: for a number, the number,
    /// and for a byte string its bytes (`Seq<u8>`). A column of the type
    /// is a sequence of these (`col@`).
    type S;
}

/// The form in which a column of this type gives a value and takes
/// one, where what the value borrows lives for `'a`.
pub trait VForm<'a>: VSpec {
    /// The form: for a number, an `Option` of one or a `bool`, the
    /// type itself; for `Vec<u8>`, `&'a [u8]`; for `Option<Vec<u8>>`,
    /// `Option<&'a [u8]>`.
    type Get: VGet<S = <Self as VSpec>::S>;
}

/// The form in which a column of `V` gives a value and takes one
/// ([`VForm`]): `V` itself for a number, an `Option` of one or a
/// `bool`, and a borrowed slice for a byte string.
pub type Get<'a, V> = <V as VForm<'a>>::Get;

impl<T: VPack> seal::SpecSeal for T {

}

impl<T: VPack> VSpec for T {
    type S = T;
}

impl<'a, T: VPack> VForm<'a> for T {
    type Get = T;
}

impl<T: VPack> seal::SpecSeal for Option<T> {

}

impl<T: VPack> VSpec for Option<T> {
    type S = Option<T>;
}

impl<'a, T: VPack> VForm<'a> for Option<T> {
    type Get = Option<T>;
}

impl seal::SpecSeal for bool {

}

impl VSpec for bool {
    type S = bool;
}

impl<'a> VForm<'a> for bool {
    type Get = bool;
}

/// What decoders give, as a type of theirs: a bound on it can then
/// name the lifetime `'a`, which one on `Decode::Item` cannot. Every
/// `Decoders` has it.
pub(crate) trait Items<'a>: Decoders<'a, D: Decode<Item = <Self as Items<'a>>::Item>> {
    type Item: Copy;
}

impl<'a, X: Decoders<'a>> Items<'a> for X {
    type Item = <X::D as Decode>::Item;
}

/// A value as a column of `V` holds it.
#[cfg(verus_keep_ghost)]
type Held<V, C> = <<V as seal::Sealed<C>>::E as Slabs>::V;

/// The value `v`, given to a column of `V`, is in a specification the
/// value the column then holds.
#[doc(hidden)]
pub closed spec fn own_kept<'a, V: seal::Sealed<C>, C>(v: Get<'a, V>) -> bool {
    <V::E as ReadsAgree<'a, V::X>>::own(v).sv() == v.sv()
}

/// `svs` goes through a concatenation.
proof fn lemma_svs_add<T: VGet>(a: Seq<T>, b: Seq<T>)
    ensures
        T::svs(a + b) == T::svs(a) + T::svs(b),
{
    T::lemma_svs(a);
    T::lemma_svs(b);
    T::lemma_svs(a + b);
    assert(T::svs(a + b) =~= T::svs(a) + T::svs(b));
}

/// `svs` goes through a range.
proof fn lemma_svs_sub<T: VGet>(a: Seq<T>, i: int, j: int)
    requires
        0 <= i <= j <= a.len(),
    ensures
        T::svs(a.subrange(i, j)) == T::svs(a).subrange(i, j),
{
    T::lemma_svs(a);
    T::lemma_svs(a.subrange(i, j));
    assert(T::svs(a.subrange(i, j)) =~= T::svs(a).subrange(i, j));
}

/// No values are no values.
proof fn lemma_svs_none<T: VGet>()
    ensures
        T::svs(Seq::<T>::empty()) == Seq::<T::S>::empty(),
{
    T::lemma_svs(Seq::<T>::empty());
    assert(T::svs(Seq::<T>::empty()) =~= Seq::<T::S>::empty());
}

/// One value is that value.
proof fn lemma_svs_one<T: VGet>(v: T)
    ensures
        T::svs(seq![v]) == seq![v.sv()],
{
    T::lemma_svs(seq![v]);
    assert(T::svs(seq![v]) =~= seq![v.sv()]);
}

/// Values that are the same in a specification are the same.
proof fn lemma_svs_inj<T: VGet>(a: Seq<T>, b: Seq<T>)
    requires
        T::svs(a) == T::svs(b),
    ensures
        a == b,
{
    T::lemma_svs(a);
    T::lemma_svs(b);
    let x = T::svs(a);
    let y = T::svs(b);
    assert forall|i: int| 0 <= i < a.len() implies a[i] == b[i] by {
        assert(x[i] == a[i].sv());
        assert(y[i] == b[i].sv());
        T::lemma_sv_inj(a[i], b[i]);
    }
    assert(a =~= b);
}

/// A splice of values is that splice of what they are in a
/// specification.
proof fn lemma_svs_spliced<T: VGet>(a: Seq<T>, at: int, del: int, ins: Seq<T>)
    requires
        0 <= at <= a.len(),
        0 <= del <= a.len() - at,
    ensures
        T::svs(a.take(at) + ins + a.skip(at + del)) == T::svs(a).take(at) + T::svs(ins) + T::svs(a).skip(at + del),
{
    T::lemma_svs(a);
    lemma_svs_add(a.take(at), ins);
    lemma_svs_add(a.take(at) + ins, a.skip(at + del));
    lemma_svs_sub(a, 0, at);
    lemma_svs_sub(a, at + del, a.len() as int);
}

/// A value read and put with those read before it is the values read,
/// and the one after it left to read.
proof fn lemma_svs_step<T: VGet>(a: Seq<T>, o: Seq<T>)
    requires
        o.len() > 0,
    ensures
        T::svs(a.push(o[0])) + T::svs(o.skip(1)) == T::svs(a) + T::svs(o),
{
    assert(a.push(o[0]) + o.skip(1) =~= a + o);
    lemma_svs_add(a.push(o[0]), o.skip(1));
    lemma_svs_add(a, o);
}

/// The values `vs`, given to a column of `V`, as it holds them.
spec fn held<'v, V: seal::Sealed<C>, C>(vs: Seq<Get<'v, V>>) -> Seq<Held<V, C>> {
    Seq::new(vs.len(), |i: int| <V::E as ReadsAgree<'v, V::X>>::own(vs[i]))
}

/// What a column holds for the values given are, in a specification,
/// those values.
proof fn lemma_held<'v, V: VValue<C>, C: VCodec>(vs: Seq<Get<'v, V>>)
    ensures
        <Held<V, C> as VGet>::svs(held::<V, C>(vs)) == <Get<'v, V> as VGet>::svs(vs),
{
    let h = held::<V, C>(vs);
    let a = <Held<V, C> as VGet>::svs(h);
    let b = <Get<'v, V> as VGet>::svs(vs);
    <Held<V, C> as VGet>::lemma_svs(h);
    <Get<'v, V> as VGet>::lemma_svs(vs);
    assert forall|i: int| 0 <= i < vs.len() implies a[i] == b[i] by {
        V::lemma_own(vs[i]);
    }
    assert(a =~= b);
}

// ── The saved form ──────────────────────────────────────────────────────────

/// `d` is a saved column of `V` under codec `C`
/// ([`VerifiedColumn::valid`]).
#[doc(hidden)]
pub closed spec fn saved<V: seal::Sealed<C>, C>(d: Seq<u8>) -> bool {
    <V::E as Model<V::X>>::valid(d)
}

/// The values of the saved column `d` ([`VerifiedColumn::values`]).
#[doc(hidden)]
pub closed spec fn saved_values<V: seal::Sealed<C>, C>(d: Seq<u8>) -> Seq<<V as VSpec>::S> {
    <Held<V, C> as VGet>::svs(<V::E as Model<V::X>>::values(d))
}

/// A value type a [`VerifiedColumn`] can hold, its numbers written with
/// codec `C`.
///
/// These are `u64`, `i64`, `u32`, `i32`, `usize`, `NonZeroU32` and every
/// type that says which of them it is stored as ([`VNewtype`]); byte
/// strings (`Vec<u8>`); an `Option` of any of those, whose `None` is a
/// column's null; and `bool`.
pub trait VValue<C: VCodec = Leb128>: seal::Sealed<C> {
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
}

impl<T: VPack, C: VCodec> seal::Sealed<C> for T {
    type X = RleReads<VOps<T, C>>;

    type E = RleModel<VWrite<T, C>>;
}

impl<T: VPack, C: VCodec> VValue<C> for T {
    /// Ten bytes a value, and a header and a count of ten bytes each.
    open spec fn size_bound(vs: Seq<T>) -> nat {
        30 * vs.len()
    }

    proof fn lemma_size(d: Seq<u8>) {
        <RleModel<VWrite<T, C>> as Model<RleReads<VOps<T, C>>>>::lemma_size(d);
        lemma_plain_sizes::<T, C>(slab_values::<VOps<T, C>>(d));
    }

    proof fn lemma_own<'a>(v: T) {
    }
}

impl<T: VPack, C: VCodec> seal::Sealed<C> for Option<T> {
    type X = RleReads<VOptOps<T, C>>;

    type E = RleModel<VOptWrite<T, C>>;
}

impl<T: VPack, C: VCodec> VValue<C> for Option<T> {
    /// Ten bytes a value, and a header and a count of ten bytes each.
    open spec fn size_bound(vs: Seq<Option<T>>) -> nat {
        30 * vs.len()
    }

    proof fn lemma_size(d: Seq<u8>) {
        <RleModel<VOptWrite<T, C>> as Model<RleReads<VOptOps<T, C>>>>::lemma_size(d);
        lemma_opt_sizes::<T, C>(slab_values::<VOptOps<T, C>>(d));
    }

    proof fn lemma_own<'a>(v: Option<T>) {
    }
}

impl<C: VCodec> seal::Sealed<C> for bool {
    type X = BoolReads<VB<C>>;

    type E = BoolModel<VB<C>>;
}

impl<C: VCodec> VValue<C> for bool {
    /// Ten bytes a run, which is at least one value but for the first.
    open spec fn size_bound(vs: Seq<bool>) -> nat {
        10 * vs.len() + 10
    }

    proof fn lemma_size(d: Seq<u8>) {
        <BoolModel<VB<C>> as Model<BoolReads<VB<C>>>>::lemma_size(d);
    }

    proof fn lemma_own<'a>(v: bool) {
    }
}

// ── What a splice is given ──────────────────────────────────────────────────

/// The values a splice inserts, from a slice: each a run of one.
#[verifier::reject_recursive_types(C)]
struct SliceFeed<'a, 'v, V: VValue<C>, C: VCodec> {
    values: &'a [Get<'v, V>],
    /// how many have been given
    pos: usize,
    _c: PhantomData<C>,
}

impl<'a, 'v, V: VValue<C>, C: VCodec> RunFeed<V::E> for SliceFeed<'a, 'v, V, C> {
    type X = <V::E as ReadsAgree<'v, V::X>>::Ins;

    /// The values to give: a feed keeps its slice.
    type K = Seq<Get<'v, V>>;

    closed spec fn inv(&self) -> bool {
        self.pos <= self.values@.len()
    }

    closed spec fn key(&self) -> Seq<Get<'v, V>> {
        self.values@
    }

    closed spec fn given(&self) -> Seq<Held<V, C>> {
        held::<V, C>(self.values@.take(self.pos as int))
    }

    closed spec fn ended(&self) -> bool {
        self.pos == self.values@.len()
    }

    #[inline(always)]
    fn next_run(&mut self) -> (r: Option<(<V::E as ReadsAgree<'v, V::X>>::Ins, usize)>) {
        if self.pos < self.values.len() {
            let v = self.values[self.pos];
            proof {
                let own = <V::E as ReadsAgree<'v, V::X>>::own(v);
                assert(held::<V, C>(self.values@.take(self.pos + 1)) =~= held::<V, C>(
                    self.values@.take(self.pos as int),
                ) + Seq::new(1, |_i: int| own));
            }
            self.pos = self.pos + 1;
            Some((<V::E as ReadsAgree<'v, V::X>>::ins(v), 1))
        } else {
            None
        }
    }
}

/// What a splice of a slice's values leaves, as a client has it.
proof fn lemma_spliced<'v, V: VValue<C>, C: VCodec>(
    a: Seq<Held<V, C>>,
    at: int,
    del: int,
    vs: Seq<Get<'v, V>>,
    ins: Seq<Held<V, C>>,
)
    requires
        0 <= at <= a.len(),
        0 <= del <= a.len() - at,
        held::<V, C>(vs.take(0)) + ins == held::<V, C>(vs.take(vs.len() as int)),
    ensures
        <Held<V, C> as VGet>::svs(a.take(at) + ins + a.skip(at + del)) == <Held<V, C> as VGet>::svs(a).take(at)
            + <Get<'v, V> as VGet>::svs(vs) + <Held<V, C> as VGet>::svs(a).skip(at + del),
{
    assert(vs.take(vs.len() as int) =~= vs);
    assert(ins =~= held::<V, C>(vs));
    lemma_svs_spliced(a, at, del, ins);
    lemma_held::<V, C>(vs);
}

// ── The column ──────────────────────────────────────────────────────────────

/// A column of values of type `V`: the sequence `self@`. Its numbers are
/// written with the codec `C`.
pub struct VerifiedColumn<V: VValue<C>, C: VCodec = Leb128> {
    core: Core<V::E>,
}

impl<V: VValue<C>, C: VCodec> View for VerifiedColumn<V, C> {
    type V = Seq<V::S>;

    closed spec fn view(&self) -> Seq<V::S> {
        <Held<V, C> as VGet>::svs(self.core@)
    }
}

impl<V: VValue<C>, C: VCodec> Default for VerifiedColumn<V, C> {
    /// An empty column.
    fn default() -> (r: Self)
        ensures
            r@ == Seq::<V::S>::empty(),
    {
        Self::new()
    }
}

// The saved form
impl<V: VValue<C>, C: VCodec> VerifiedColumn<V, C> {
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

    /// A saved column that [`load`](VerifiedColumn::load) is sure to
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
        lemma_svs_inj(<V::E as Model<V::X>>::values(x), <V::E as Model<V::X>>::values(y));
        <V::E as Model<V::X>>::lemma_unique(x, y);
    }

    /// A saved column is the encoding of its values.
    pub proof fn lemma_encoding(d: Seq<u8>)
        requires
            Self::valid(d),
        ensures
            Self::encoding(Self::values(d)) == d,
    {
        let e = Self::encoding(Self::values(d));
        assert(Self::valid(e) && Self::values(e) == Self::values(d));
        Self::lemma_save_unique(e, d);
    }
}

impl<V: VValue<C>, C: VCodec> VerifiedColumn<V, C> {
    /// An empty column.
    pub fn new() -> (r: Self)
        ensures
            r@ == Seq::<V::S>::empty(),
    {
        Self::with_max_segments(MAX_SEG)
    }

    /// An empty column whose slabs hold at most `max_segments` runs
    /// each.
    pub fn with_max_segments(max_segments: usize) -> (r: Self)
        requires
            max_segments >= 2,
        ensures
            r@ == Seq::<V::S>::empty(),
    {
        let core = Core::<V::E>::with_budget(max_segments);
        proof {
            assert(core@ =~= Seq::empty());
            lemma_svs_none::<Held<V, C>>();
        }
        VerifiedColumn { core }
    }

    /// The number of values.
    #[inline(always)]
    pub fn len(&self) -> (r: usize)
        ensures
            r == self@.len(),
    {
        proof {
            <Held<V, C> as VGet>::lemma_svs(self.core@);
        }
        self.core.len()
    }

    /// Whether the column holds no value.
    #[inline(always)]
    pub fn is_empty(&self) -> (r: bool)
        ensures
            r == (self@.len() == 0),
    {
        proof {
            <Held<V, C> as VGet>::lemma_svs(self.core@);
        }
        self.core.len() == 0
    }

    /// Delete `del` values at `at` and insert `values` there.
    pub fn splice(&mut self, at: usize, del: usize, values: &[Get<'_, V>])
        requires
            at <= old(self)@.len(),
            del <= old(self)@.len() - at,
        ensures
            final(self)@ == old(self)@.take(at as int) + VGet::svs(values@) + old(self)@.skip(at + del),
    {
        proof {
            <Held<V, C> as VGet>::lemma_svs(self.core@);
        }
        let mut f = <V::E as Model<V::X>>::blind();
        let mut feed = SliceFeed::<V, C> { values, pos: 0, _c: PhantomData };
        let _ins = self.core.splice(at, del, &mut f, &mut feed);
        proof {
            lemma_spliced::<V, C>(old(self).core@, at as int, del as int, values@, _ins@);
        }
    }

    /// Delete `del` values at `at`.
    fn delete(&mut self, at: usize, del: usize)
        requires
            at <= old(self)@.len(),
            del <= old(self)@.len() - at,
        ensures
            final(self)@ == old(self)@.take(at as int) + old(self)@.skip(at + del),
    {
        proof {
            <Held<V, C> as VGet>::lemma_svs(self.core@);
        }
        let mut f = <V::E as Model<V::X>>::blind();
        let mut feed = NoRuns::<<V::E as ReadsAgree<'static, V::X>>::Ins> { none: None };
        let _ins = self.core.splice(at, del, &mut f, &mut feed);
        proof {
            let a = old(self).core@;
            let none = Seq::<Held<V, C>>::empty();
            assert(_ins@ =~= none);
            lemma_svs_spliced(a, at as int, del as int, none);
            lemma_svs_none::<Held<V, C>>();
            assert(self@ =~= old(self)@.take(at as int) + old(self)@.skip(at + del));
        }
    }

    /// Insert `value` before the value at `at`.
    #[inline(always)]
    pub fn insert(&mut self, at: usize, value: Get<'_, V>)
        requires
            at <= old(self)@.len(),
        ensures
            final(self)@ == old(self)@.insert(at as int, value.sv()),
    {
        // Which of the two it is, is a constant of the column's type: the
        // other is not compiled.
        if <V::E as Model<V::X>>::ONE_APART {
            self.insert_apart(at, value);
        } else {
            let one = [value];
            self.splice(at, 0, one.as_slice());
            proof {
                assert(one@ =~= seq![value]);
                lemma_svs_one(value);
                assert(self@ =~= old(self)@.insert(at as int, value.sv()));
            }
        }
    }

    /// `insert`, out of line: a splice of one value that deletes none,
    /// compiled for that alone, apart from `splice`.
    fn insert_apart(&mut self, at: usize, value: Get<'_, V>)
        requires
            at <= old(self)@.len(),
        ensures
            final(self)@ == old(self)@.insert(at as int, value.sv()),
    {
        proof {
            <Held<V, C> as VGet>::lemma_svs(self.core@);
        }
        let one = [value];
        let values = one.as_slice();
        let mut f = <V::E as Model<V::X>>::blind();
        let mut feed = SliceFeed::<V, C> { values, pos: 0, _c: PhantomData };
        let _ins = self.core.splice(at, 0, &mut f, &mut feed);
        proof {
            lemma_spliced::<V, C>(old(self).core@, at as int, 0, values@, _ins@);
            assert(one@ =~= seq![value]);
            lemma_svs_one(value);
            assert(self@ =~= old(self)@.insert(at as int, value.sv()));
        }
    }

    /// Add `value` at the end.
    #[inline(always)]
    pub fn push(&mut self, value: Get<'_, V>)
        ensures
            final(self)@ == old(self)@.push(value.sv()),
    {
        proof {
            <Held<V, C> as VGet>::lemma_svs(self.core@);
        }
        let at = self.core.len();
        self.insert(at, value);
        proof {
            assert(self@ =~= old(self)@.push(value.sv()));
        }
    }

    /// Delete the value at `at`.
    #[inline(always)]
    pub fn remove(&mut self, at: usize)
        requires
            at < old(self)@.len(),
        ensures
            final(self)@ == old(self)@.remove(at as int),
    {
        self.delete(at, 1);
        proof {
            assert(self@ =~= old(self)@.remove(at as int));
        }
    }

    /// Keep the first `len` values, where there are more.
    #[inline(always)]
    pub fn truncate(&mut self, len: usize)
        ensures
            final(self)@ == if len < old(self)@.len() {
                old(self)@.take(len as int)
            } else {
                old(self)@
            },
    {
        proof {
            <Held<V, C> as VGet>::lemma_svs(self.core@);
        }
        let n = self.core.len();
        if len < n {
            self.delete(len, n - len);
            proof {
                assert(self@ =~= old(self)@.take(len as int));
            }
        }
    }

    /// A reader of the values `start..end`.
    #[inline(always)]
    fn reader(&self, start: usize, end: usize) -> (it: ICore<<V::X as Decoders<'_>>::D>)
        requires
            start <= end <= self@.len(),
        ensures
            it.wf::<V::X>(self.core.slabs_spec()),
            VGet::svs(it.out::<V::X>(self.core.slabs_spec())) == self@.subrange(start as int, end as int),
    {
        proof {
            <Held<V, C> as VGet>::lemma_svs(self.core@);
        }
        let it = self.core.reader::<V::X>(start, end);
        proof {
            let o = it.out::<V::X>(self.core.slabs_spec());
            let s = self.core@.subrange(start as int, end as int);
            assert(s =~= held::<V, C>(o));
            lemma_held::<V, C>(o);
            lemma_svs_sub(self.core@, start as int, end as int);
        }
        it
    }

    /// The value at `i`, if the column has one there.
    pub fn get(&self, i: usize) -> (r: Option<Get<'_, V>>)
        ensures
            i < self@.len() ==> (r matches Some(v) && v.sv() == self@[i as int]),
            i >= self@.len() ==> r is None,
    {
        proof {
            <Held<V, C> as VGet>::lemma_svs(self.core@);
        }
        if i >= self.core.len() {
            return None;
        }
        // the column's length is short of `usize::MAX`
        self.core.lemma_column();
        let slabs = self.core.slabs();
        let mut it = self.reader(i, i + 1);
        proof {
            let o = it.out::<V::X>(slabs@);
            let s = VGet::svs(o);
            VGet::lemma_svs(o);
            assert(s[0] == self@[i as int]);
            assert(s[0] == o[0].sv());
        }
        it.next::<V::X>(slabs)
    }

    /// The values.
    pub fn to_vec(&self) -> (r: Vec<Get<'_, V>>)
        ensures
            VGet::svs(r@) == self@,
    {
        proof {
            <Held<V, C> as VGet>::lemma_svs(self.core@);
        }
        let n = self.core.len();
        let slabs = self.core.slabs();
        let mut it = self.reader(0, n);
        let mut out: Vec<Get<'_, V>> = Vec::with_capacity(n);
        proof {
            assert(self@.subrange(0, n as int) =~= self@);
            lemma_svs_none::<Get<'_, V>>();
            assert(VGet::svs(out@) + VGet::svs(it.out::<V::X>(slabs@)) =~= self@);
        }
        loop
            invariant
                it.wf::<V::X>(slabs@),
                VGet::svs(out@) + VGet::svs(it.out::<V::X>(slabs@)) == self@,
            ensures
                VGet::svs(out@) == self@,
            decreases it.out::<V::X>(slabs@).len(),
        {
            let ghost o = it.out::<V::X>(slabs@);
            match it.next_inline::<V::X>(slabs) {
                Some(v) => {
                    proof {
                        lemma_svs_step(out@, o);
                    }
                    out.push(v);
                },
                None => {
                    proof {
                        VGet::lemma_svs(o);
                        assert(VGet::svs(out@) + VGet::svs(o) =~= VGet::svs(out@));
                    }
                    break ;
                },
            }
        }
        out
    }

    /// An iterator over the values.
    #[inline(always)]
    pub fn iter(&self) -> (it: VerifiedIter<'_, V, C>)
        ensures
            it.wf(),
            it.rest() == self@,
    {
        proof {
            <Held<V, C> as VGet>::lemma_svs(self.core@);
        }
        let n = self.core.len();
        proof {
            assert(self@.subrange(0, n as int) =~= self@);
        }
        VerifiedIter { it: self.reader(0, n), slabs: self.core.slabs() }
    }

    /// An iterator over the values `start..end`.
    #[inline(always)]
    pub fn iter_range(&self, start: usize, end: usize) -> (it: VerifiedIter<'_, V, C>)
        requires
            start <= end <= self@.len(),
        ensures
            it.wf(),
            it.rest() == self@.subrange(start as int, end as int),
    {
        VerifiedIter { it: self.reader(start, end), slabs: self.core.slabs() }
    }

    /// The column, saved: the encoding of its values.
    pub fn save(&self) -> (r: Vec<u8>)
        ensures
            Self::valid(r@),
            Self::values(r@) == self@,
            r@ == Self::encoding(self@),
            r@.len() <= V::size_bound(self@),
    {
        self.core.lemma_column();
        let mut out: Vec<u8> = Vec::new();
        <V::E as Model<V::X>>::save(self.core.slabs(), self.core.len(), &mut out);
        proof {
            assert(out@.skip(0) =~= out@);
            Self::lemma_encoding(out@);
            V::lemma_size(out@);
        }
        out
    }

    /// The column saved as `bytes`. An error unless `bytes` is a saved
    /// column; and no error for one that is `loadable`.
    pub fn load(bytes: &[u8]) -> (r: Result<Self, PackError>)
        ensures
            Self::loadable(bytes@) ==> r is Ok,
            r matches Ok(c) ==> Self::valid(bytes@) && c@ == Self::values(bytes@),
    {
        proof {
            <Held<V, C> as VGet>::lemma_svs(<V::E as Model<V::X>>::values(bytes@));
        }
        match <V::E as Model<V::X>>::load(bytes, MAX_SEG) {
            Ok(core) => Ok(VerifiedColumn { core }),
            Err(e) => Err(e),
        }
    }
}

// ── The iterator ────────────────────────────────────────────────────────────

/// An iterator over a column's values.
pub struct VerifiedIter<'a, V: VValue<C>, C: VCodec = Leb128> {
    it: ICore<<V::X as Decoders<'a>>::D>,
    slabs: &'a [Slab<<V::E as Slabs>::Tail>],
}

impl<'a, V: VValue<C>, C: VCodec> VerifiedIter<'a, V, C> {
    /// The values the iterator has left to give.
    pub closed spec fn rest(&self) -> Seq<V::S> {
        <Get<'a, V> as VGet>::svs(self.it.out::<V::X>(self.slabs@))
    }

    /// The iterator stands in its column: as every iterator a column
    /// gives does, and keeps doing.
    pub closed spec fn wf(&self) -> bool {
        self.it.wf::<V::X>(self.slabs@)
    }

    /// The next value, if one is left.
    #[inline(always)]
    #[allow(clippy::should_implement_trait)]  // `Iterator::next` has no `requires`
    pub fn next(&mut self) -> (r: Option<Get<'a, V>>)
        requires
            old(self).wf(),
        ensures
            final(self).wf(),
            match r {
                Some(v) => {
                    &&& old(self).rest().len() > 0
                    &&& v.sv() == old(self).rest()[0]
                    &&& final(self).rest() == old(self).rest().skip(1)
                },
                None => old(self).rest().len() == 0 && final(self).rest().len() == 0,
            },
    {
        let ghost o = self.it.out::<V::X>(self.slabs@);
        let r = self.it.next_inline::<V::X>(self.slabs);
        proof {
            <Get<'a, V> as VGet>::lemma_svs(o);
            <Get<'a, V> as VGet>::lemma_svs(self.it.out::<V::X>(self.slabs@));
            if r is Some {
                lemma_svs_sub(o, 1, o.len() as int);
            }
        }
        r
    }
}

} // verus!

// one budget for both APIs
const _: () = assert!(MAX_SEG == crate::column::DEFAULT_MAX_SEG);
