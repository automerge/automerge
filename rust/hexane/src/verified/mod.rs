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
//! and for a number the number. What a value of the type itself is
//! there (a `Vec<u8>` for a byte string) is its `VSpec::sv_of`.
//!
//! The values of a type are in an order (`VSpec::below`), which a
//! search goes by: numbers in the order of numbers, `false` before
//! `true`, byte strings in the order of their bytes with a string before
//! every longer one that starts with it (`bytes_below`), and `None`
//! before every `Some`. That is the order Rust has for these types
//! (`Ord`). A client's type is in the order of the numbers it is stored
//! as, whatever order it may have in Rust.
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
//! * [`splice_iter`](VerifiedColumn::splice_iter) does so with the values
//!   an iterator gives, and [`splice_runs`](VerifiedColumn::splice_runs)
//!   with runs, each a value and how many times to repeat it (their
//!   values are `run_values`). `extend` appends what an iterator gives,
//!   and `collect()` makes a column of it;
//! * [`from_values`](VerifiedColumn::from_values) gives a column of the
//!   values of a slice, and [`fill`](VerifiedColumn::fill) one of a value
//!   repeated;
//! * [`remove_n`](VerifiedColumn::remove_n),
//!   [`truncate`](VerifiedColumn::truncate) and
//!   [`clear`](VerifiedColumn::clear) leave the values a `splice` that
//!   deletes the same ones would;
//! * [`get`](VerifiedColumn::get) and [`to_vec`](VerifiedColumn::to_vec)
//!   give the column's values;
//! * the [iterator](VerifiedIter) is one of the standard library's
//!   (`Iterator` and `ExactSizeIterator`, and a borrowed column is an
//!   `IntoIterator`). It has the column's values left to give, or those
//!   of a range (`rest`), and `next` gives the first of them and leaves
//!   the others. It so keeps to Verus's laws of iterators, and a `for`
//!   loop over it reads those values, as do the standard library's
//!   functions that Verus has a specification of (`zip`, `skip`, `take`
//!   among them). `nth` gives the value that many places on, `len` and
//!   `size_hint` the number left, and `fold` what the client's function
//!   makes of the values in turn (`folded`), as that function's own
//!   `ensures` says it;
//! * [`is_only`](VerifiedColumn::is_only) says whether every value of the
//!   column is the one given (`only`), and [`VValue::same`] whether two
//!   values in a column's form are one value;
//! * [`scope_to_value`](VerifiedColumn::scope_to_value) finds a value
//!   among values in order (`sorted`): it gives the range of the places
//!   that hold it, which has the values that come before it on one side
//!   and those that come after it on the other, and is so the empty
//!   range at the place where the value would stand where none holds
//!   it. The values need be in order only as far as the value looked for
//!   can tell (`sorted_for`). Of values in no order it gives some range
//!   within the one searched. [`VValue::compare`] says how two values in
//!   a column's form stand in the order;
//! * [`remap`](VerifiedColumn::remap) puts in each place what a function
//!   of the client's gave for the value that was there (`mapped`). The
//!   function is handed a value of the type itself (for a byte string a
//!   `Vec<u8>` of its own) and has to accept every value of the column.
//!   Nothing is assumed of it: what it gives is said by its own
//!   `ensures`, and by nothing else;
//! * `clone` gives a column with the same values. It is a column of its
//!   own, as Rust's ownership has it: an edit of either says nothing of
//!   the other, whose values so stay what they were;
//! * [`save`](VerifiedColumn::save) writes the one encoding of the
//!   column's values (`encoding`): bytes that are `valid` and hold those
//!   `values`, and no more of them than the values' `size_bound`. Two
//!   columns of the same values so save to the same bytes
//!   (`lemma_save_unique`). [`save_to`](VerifiedColumn::save_to) writes
//!   that encoding after what a buffer holds, which it keeps, and
//!   [`save_to_unless`](VerifiedColumn::save_to_unless) writes nothing
//!   where every value is the one given;
//! * [`validate_encoding`](VerifiedColumn::validate_encoding) saves the
//!   column and reads the bytes through, and returns no error for a
//!   column that saves to less than 4 GiB: what it looks for at run
//!   time is proved of `save`;
//! * [`load`](VerifiedColumn::load) gives a column only for valid bytes,
//!   and then the column of their values; and it does give one for all
//!   valid bytes short of 4 GiB that hold values a `usize` can count
//!   (`loadable`);
//! * [`load_with`](VerifiedColumn::load_with) is `load` with options
//!   ([`LoadOpts`]). Asked for a length, it gives a column only of that
//!   many values, and does give one for loadable bytes that hold that
//!   many. Given a fill value too, it makes of no bytes a column of the
//!   value that many times, for every length a column can have
//!   (`fill_of` says when); for a longer one it returns an error, since
//!   no bytes are the saved column of no values (`lemma_no_bytes`);
//! * [`slab_lens`](VerifiedColumn::slab_lens) gives numbers that add up
//!   to the column's length (`sum_of`).
//!
//! `valid`, `values`, `encoding` and `loadable` are specification
//! functions of a column type (`VerifiedColumn::<V>::valid(bytes)`),
//! `size_bound` is one of a value type (for byte strings it counts their
//! bytes, as `total_len` of them), `only`, `sorted`, `sorted_for`,
//! `bytes_below`, `mapped`, `folded`, `run_values`, `fill_of` and `sum_of`
//! are of this module, and `lemma_save_unique`, `lemma_encoding`, `lemma_no_bytes`,
//! `lemma_sorted`, `lemma_run_values_push` and `lemma_run_values_add`
//! are lemmas, as is `VSpec::lemma_below` (the order is a strict total
//! order): they exist only for Verus, and a normal build compiles
//! them away. What options hold is said by `length()`, `fill()` and
//! `max_segments()` of a [`LoadOpts`], and what fill value there is by
//! `value()` of its fill state ([`MaybeFill`]).
//!
//! What a method does with an iterator is stated as Verus states what
//! the standard library does with one: of the values the iterator is to
//! give (`remaining()`), for an iterator that keeps to Verus's laws of
//! iterators (`obeys_prophetic_iter_laws`: what `next` returns is the
//! first of what remains, and `None` when nothing does). Of an iterator
//! that does not, a method promises nothing but that the column is still
//! a column. What a client's iterator is to give, the client proves from
//! the specification of that iterator: of a `Vec` handed over, that it
//! gives the vector's items.
//!
//! The specifications say what a saved column is in their own terms.
//! That those are the bytes of [`Column`](crate::Column) for the same
//! values is not proved but tested (`tests/verified_api.rs`): for each
//! value type the two save the same bytes, each loads what the other
//! saved, and they refuse the same bytes with the same error.
//!
//! How a column lies in memory is no part of any specification:
//! [`slab_count`](VerifiedColumn::slab_count),
//! [`slab_lens`](VerifiedColumn::slab_lens),
//! [`slab_segments`](VerifiedColumn::slab_segments) and what
//! [`dump_slabs`](VerifiedColumn::dump_slabs) and `Debug` print tell of
//! it, and promise nothing but that the lengths add up. Nor is how
//! often [`remap`](VerifiedColumn::remap) calls its function: once for
//! each run of equal values, as `Column`'s does, which is tested. After
//! the same edits a `VerifiedColumn` has the slabs of a
//! [`Column`](crate::Column), each as long and of as many runs, and
//! `dump_slabs` prints the same lines. That is tested too, but for one
//! case: a column that [`clear`](VerifiedColumn::clear) has emptied has
//! no slab, where `Column` keeps an empty one, until the next edit.
//!
//! # What is trusted
//!
//! Verus and its solver Z3, and Verus's specifications of the standard
//! library: of `Vec` and slices, and for a column of `NonZeroU32` those
//! of `NonZero`, which Verus keeps behind its feature
//! `nonzero_internals` (the crate is checked with it on). A column of
//! byte strings leans on more of them than one of numbers does: that a
//! slice is its bytes and no more, that a part of a slice is that part of
//! its bytes, and the copy of a slice into a `Vec`. `clone` leans on
//! those of the clones it is made of: that a clone of a `Vec` is as long
//! and holds a clone of each item, that a clone of an `Option` is one of
//! what it holds, and that a clone of an integer is that integer. The
//! methods that take an iterator lean on those of iterators: what `next`
//! returns, and what `IntoIterator` and `FromIterator` are. A client
//! that reads a column's iterator through a function of the standard
//! library leans on Verus's specification of that function, or of the
//! `for` loop: what is proved here is what the iterator's own `next`,
//! `nth`, `size_hint`, `len` and `fold` do. (Of the standard library's
//! `count`, `last`, `sum` and `for_each`, which run that `fold`, Verus
//! has no specification.) Of `Extend`
//! Verus has none, and this crate tells it that the trait has its one
//! method (`ExExtend`), which assumes nothing of an implementation. A
//! search leans on that of the comparison of two integers (`cmp`); byte
//! strings it compares in code that is checked, which leans on that of
//! the equality of two slices (`==`).
//!
//! Of this crate, the functions Verus is told to take as they are, which
//! are few and small, and are those the verified core of
//! [`Column`](crate::Column) has too:
//!
//! * the helpers that panic, which say why and do nothing else. A method
//!   panics where a column would outgrow what its arithmetic can count
//!   (some `usize::MAX` values, or a slab of 4 GiB) or what the format
//!   can write (a run of more than `i64::MAX` equal values that are
//!   neither nulls nor `bool`s, which
//!   [`splice_runs`](VerifiedColumn::splice_runs) can ask for in one
//!   call), and, like every Rust function, a method of this API may run
//!   out of memory;
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
//! a path through this API is checked. The options of a load
//! ([`LoadOpts`]) are one type for both APIs, and are checked too:
//! nothing is assumed of what they hold.
//!
//! Three functions of a column are not checked, and nothing is specified
//! of them. They read the column through checked functions that ask
//! nothing of a caller, and then print or compare: `Debug`;
//! [`dump_slabs`](VerifiedColumn::dump_slabs), whose printing of what
//! checked code has read of each slab is not checked; and
//! `check_invariants`, a tool for tests that these docs leave out, as
//! [`Column`](crate::Column)'s do. It looks at run time for what is
//! proved of every column (a slab's bytes hold the values and runs it
//! counts, and the index counts the slabs' lengths) and for the layout
//! policy, which is not proved (a slab's runs are within the budget,
//! and no two neighbours should have been merged), and panics if it
//! finds otherwise. As `Column`'s, it takes every column as one that
//! edits have made, and panics for some that were loaded: a load leaves
//! a short last slab unmerged.
//!
//! That a load returns an error for what a column cannot hold, and does
//! not panic, is not proved: Verus is not asked to show that the helpers
//! that panic are out of a load's reach. It is tested
//! (`tests/verified_api.rs`), on targets of 64 and of 32 bits: for a fill
//! longer than a column, and for bytes of more values than one holds.
//!
//! A client's own value type is checked with the client: Verus proves
//! what [`VNewtype`] asks of it when it is run on the client's crate. A
//! client that is compiled without Verus has that to keep by itself, as
//! it has every `requires` of this API.
//!
//! That a splice returns is not proved: it runs the core's loop over the
//! values to insert, which Verus is not asked to show ends. It ends when
//! the slice does, or the iterator: with an iterator that never ends,
//! [`splice_iter`](VerifiedColumn::splice_iter),
//! [`splice_runs`](VerifiedColumn::splice_runs), `extend` and `collect()`
//! do not return (they run until memory is out, or a column's length is
//! more than it can count, which panics). Every other loop is proved to
//! end.

// Verus proofs read more plainly with `x = x + n`, and Verus has no
// spec for `vec![x]`.
#![allow(
    clippy::len_zero,
    clippy::assign_op_pattern,
    clippy::vec_init_then_push
)]

pub(crate) mod bytes;
#[cfg(verus_keep_ghost)]
mod client_proof;
pub(crate) mod codec;
pub(crate) mod counts;
pub(crate) mod model;
pub(crate) mod value;

// spec functions exist only when Verus compiles the crate
#[cfg(verus_keep_ghost)]
pub use bytes::{below_at, bytes_below, total_len, total_len_opt};
pub use codec::VCodec;
pub use value::{refused, VNewtype, VPack};

use crate::bool_edit::{BoolModel, BoolRecoder};
use crate::bool_wire::BoolReads;
#[cfg(verus_keep_ghost)]
use crate::btree::SlabBTree;
use crate::column::Slab;
use crate::copy::NoRuns;
#[cfg(verus_keep_ghost)]
use crate::cursor::{lemma_lens_ok, lemma_vals_own, lemma_vals_unit, slab_lens};
use crate::cursor::{ReadsAgree, RunFeed, Slabs};
#[cfg(verus_keep_ghost)]
use crate::fenwick::sum;
#[cfg(verus_keep_ghost)]
use crate::index::wlens;
use crate::index::IndexOps;
#[cfg(verus_keep_ghost)]
use crate::iter_core::{far_ok, find_ok, found, lemma_from, lens_ok, monotone, vals_of};
use crate::iter_core::{Decode, Decoders, ICore, Probe, Side};
use crate::remap::{Encodes, Mapping};
#[cfg(verus_keep_ghost)]
use crate::rle::dec::slab_values;
use crate::rle::dec::RleReads;
use crate::rle::edit::RleModel;
use crate::rle::enc::REncoder;
use crate::{Leb128, LoadOpts, MaybeFill, PackError, Run};
use counts::VB;
use model::{Core, Model};
use std::cmp::Ordering;
use std::marker::PhantomData;
use std::ops::Range;
#[cfg(verus_keep_ghost)]
use value::{lemma_opt_sizes, lemma_plain_sizes};
use value::{VOps, VOptOps, VOptWrite, VWrite};
#[allow(unused_imports)]
use vstd::prelude::*;
#[cfg(verus_keep_ghost)]
use vstd::std_specs::iter::{FromIteratorSpecImpl, IteratorSpec};

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
pub trait VSpec: Sized + seal::SpecSeal {
    /// What a value is in a specification: for a number, the number,
    /// and for a byte string its bytes (`Seq<u8>`). A column of the type
    /// is a sequence of these (`col@`).
    type S;

    /// What the value `v` of the type is in a specification: `v` itself
    /// for a number, an `Option` of one or a `bool`, and for a byte
    /// string its bytes (`v@`).
    spec fn sv_of(v: Self) -> Self::S;

    /// Whether `a` comes before `b` in the order of the type's values.
    /// Numbers are in the order of numbers, and a type stored as a
    /// number ([`VNewtype`]) in the order of the numbers it is stored as
    /// ([`VPack::num`]); `false` comes before `true`; byte strings are
    /// in the order of slices ([`bytes_below`]: byte by byte, and a
    /// byte string before every longer one that starts with it); and a
    /// null comes before every value.
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

impl<'a, T: VPack> VForm<'a> for T {
    type Get = T;
}

impl<T: VPack> seal::SpecSeal for Option<T> {

}

impl<T: VPack> VSpec for Option<T> {
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
        if let (Some(x), Some(y)) = (a, b) {
            T::lemma_num(x, y);
        }
    }
}

impl<'a, T: VPack> VForm<'a> for Option<T> {
    type Get = Option<T>;
}

impl seal::SpecSeal for bool {

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

/// How the values of a type go through a remap: read from a column,
/// handed to the client's function owned, and what it gives appended.
pub(crate) trait Owned<C>: seal::Sealed<C> {
    /// The encoder that a remap of a column appends to.
    type N: Encodes<Self::E>;

    /// A value read from a column, owned.
    fn to_owned<'a>(g: <Self as VForm<'a>>::Get) -> (v: Self)
        ensures
            Self::sv_of(v) == g.sv(),
    ;

    /// An owned value, as the encoder appends it.
    fn owned_in(w: Self) -> (x: <Self::N as Encodes<Self::E>>::In)
        ensures
            <Self::N as Encodes<Self::E>>::val(x).sv() == Self::sv_of(w),
    ;
}

/// A value type a [`VerifiedColumn`] can hold, its numbers written with
/// codec `C`.
///
/// These are `u64`, `i64`, `u32`, `i32`, `usize`, `NonZeroU32` and every
/// type that says which of them it is stored as ([`VNewtype`]); byte
/// strings (`Vec<u8>`); an `Option` of any of those, whose `None` is a
/// column's null; and `bool`.
#[allow(private_bounds)]
pub trait VValue<C: VCodec = Leb128>: seal::Sealed<C> + Owned<C> {
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

impl<T: VPack, C: VCodec> seal::Sealed<C> for T {
    type X = RleReads<VOps<T, C>>;

    type E = RleModel<VWrite<T, C>>;
}

impl<T: VPack, C: VCodec> Owned<C> for T {
    type N = REncoder<'static, VWrite<T, C>>;

    #[inline(always)]
    fn to_owned<'a>(g: T) -> (v: T) {
        g
    }

    #[inline(always)]
    fn owned_in(w: T) -> (x: T) {
        w
    }
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

    #[inline(always)]
    fn same<'a, 'b>(a: T, b: T) -> (r: bool) {
        <T as VPack>::same(&a, &b)
    }

    #[inline(always)]
    fn compare<'a, 'b>(a: T, b: T) -> (r: Ordering) {
        proof {
            T::lemma_num(a, b);
        }
        <T as VPack>::compare(&a, &b)
    }
}

impl<T: VPack, C: VCodec> seal::Sealed<C> for Option<T> {
    type X = RleReads<VOptOps<T, C>>;

    type E = RleModel<VOptWrite<T, C>>;
}

impl<T: VPack, C: VCodec> Owned<C> for Option<T> {
    type N = REncoder<'static, VOptWrite<T, C>>;

    #[inline(always)]
    fn to_owned<'a>(g: Option<T>) -> (v: Option<T>) {
        g
    }

    #[inline(always)]
    fn owned_in(w: Option<T>) -> (x: Option<T>) {
        w
    }
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

    #[inline(always)]
    fn same<'a, 'b>(a: Option<T>, b: Option<T>) -> (r: bool) {
        match (a, b) {
            (Some(x), Some(y)) => <T as VPack>::same(&x, &y),
            (None, None) => true,
            _ => false,
        }
    }

    #[inline(always)]
    fn compare<'a, 'b>(a: Option<T>, b: Option<T>) -> (r: Ordering) {
        match (a, b) {
            (Some(x), Some(y)) => {
                proof {
                    T::lemma_num(x, y);
                }
                <T as VPack>::compare(&x, &y)
            },
            (None, None) => Ordering::Equal,
            (None, Some(_)) => Ordering::Less,
            (Some(_), None) => Ordering::Greater,
        }
    }
}

impl<C: VCodec> seal::Sealed<C> for bool {
    type X = BoolReads<VB<C>>;

    type E = BoolModel<VB<C>>;
}

impl<C: VCodec> Owned<C> for bool {
    type N = BoolRecoder<VB<C>>;

    #[inline(always)]
    fn to_owned<'a>(g: bool) -> (v: bool) {
        g
    }

    #[inline(always)]
    fn owned_in(w: bool) -> (x: bool) {
        w
    }
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

    #[inline(always)]
    fn same<'a, 'b>(a: bool, b: bool) -> (r: bool) {
        a == b
    }

    #[inline(always)]
    fn compare<'a, 'b>(a: bool, b: bool) -> (r: Ordering) {
        if a == b {
            Ordering::Equal
        } else if b {
            Ordering::Less
        } else {
            Ordering::Greater
        }
    }
}

// ── Searching ───────────────────────────────────────────────────────────────

/// A search target in a column of `V`: where a value stands to it in
/// the order of the values. Nothing here is trusted: the comparison is
/// the value type's own, which is verified.
#[verifier::reject_recursive_types(C)]
struct VProbe<'t, V: VValue<C>, C: VCodec> {
    target: Get<'t, V>,
    _c: PhantomData<C>,
}

impl<'a, 't, V: VValue<C>, C: VCodec> Probe<Get<'a, V>> for VProbe<'t, V, C> {
    closed spec fn side(&self, v: Get<'a, V>) -> int {
        if V::below(v.sv(), self.target.sv()) {
            -1
        } else if V::below(self.target.sv(), v.sv()) {
            1
        } else {
            0
        }
    }

    proof fn axiom_at(&self, a: Get<'a, V>, b: Get<'a, V>) {
        let t = self.target.sv();
        // neither before the target nor after it: the target's value
        V::lemma_below(a.sv(), t, t);
        V::lemma_below(b.sv(), t, t);
        VGet::lemma_sv_inj(a, b);
    }

    #[inline(always)]
    fn side_of(&self, v: &Get<'a, V>) -> Side {
        proof {
            // not both before the target and after it
            V::lemma_below(v.sv(), self.target.sv(), v.sv());
        }
        match V::compare(*v, self.target) {
            Ordering::Less => Side::Below,
            Ordering::Equal => Side::At,
            Ordering::Greater => Side::Above,
        }
    }
}

// ── Remapping ───────────────────────────────────────────────────────────────

/// A client's function as a remap applies it (`Mapping`): to a value
/// read from the column, owned, and what it gives in the form the
/// encoder appends. Nothing is assumed of the function: what it asks of
/// a value and what it gives are its own `requires` and `ensures`.
#[verifier::reject_recursive_types(V)]
#[verifier::reject_recursive_types(C)]
#[verifier::reject_recursive_types(F)]
struct VMap<'f, V, C, F> {
    f: &'f F,
    _c: PhantomData<(V, C)>,
}

impl<'a, 'f, V: VValue<C>, C: VCodec, F: Fn(V) -> V> Mapping<Get<'a, V>, <V::N as Encodes<V::E>>::In> for VMap<
    'f,
    V,
    C,
    F,
> {
    closed spec fn pre(&self, g: Get<'a, V>) -> bool {
        forall|v: V| V::sv_of(v) == g.sv() ==> #[trigger] call_requires(*self.f, (v,))
    }

    closed spec fn maps(&self, g: Get<'a, V>, x: <V::N as Encodes<V::E>>::In) -> bool {
        mapped(*self.f, g.sv(), <V::N as Encodes<V::E>>::val(x).sv())
    }

    #[inline(always)]
    fn apply(&self, g: Get<'a, V>) -> (x: <V::N as Encodes<V::E>>::In) {
        let v = V::to_owned(g);
        let w = (self.f)(v);
        V::owned_in(w)
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

/// The values a splice inserts, from a caller's iterator: each a run of
/// one.
#[verifier::reject_recursive_types(C)]
struct IterFeed<'v, V: VValue<C>, C: VCodec, I: Iterator<Item = Get<'v, V>>> {
    it: I,
    /// the iterator as it was handed over
    #[allow(dead_code)]
    init: Ghost<I>,
    /// what it has given since
    #[allow(dead_code)]
    given: Ghost<Seq<Get<'v, V>>>,
    /// it has said that it has no more
    #[allow(dead_code)]
    ended: Ghost<bool>,
    _c: PhantomData<C>,
}

impl<'v, V: VValue<C>, C: VCodec, I: Iterator<Item = Get<'v, V>>> RunFeed<V::E> for IterFeed<'v, V, C, I> {
    type X = <V::E as ReadsAgree<'v, V::X>>::Ins;

    /// The iterator as it was handed over: a feed keeps that.
    type K = I;

    /// Where the iterator keeps to what Verus asks of one, what it has
    /// given and what it is yet to give are together what it was to give
    /// when it was handed over.
    #[verifier::prophetic]
    closed spec fn inv(&self) -> bool {
        &&& self.it.obeys_prophetic_iter_laws() == self.init@.obeys_prophetic_iter_laws()
        &&& self.it.obeys_prophetic_iter_laws() ==> {
            &&& self.given@ + self.it.remaining() == self.init@.remaining()
            &&& self.ended@ ==> self.it.remaining().len() == 0
        }
    }

    closed spec fn key(&self) -> I {
        self.init@
    }

    closed spec fn given(&self) -> Seq<Held<V, C>> {
        held::<V, C>(self.given@)
    }

    closed spec fn ended(&self) -> bool {
        self.ended@
    }

    #[inline(always)]
    fn next_run(&mut self) -> (r: Option<(<V::E as ReadsAgree<'v, V::X>>::Ins, usize)>) {
        let ghost before = self.given@;
        let ghost rem = self.it.remaining();
        match self.it.next() {
            Some(v) => {
                proof {
                    let own = <V::E as ReadsAgree<'v, V::X>>::own(v);
                    self.given@ = before.push(v);
                    assert(held::<V, C>(before.push(v)) =~= held::<V, C>(before) + Seq::new(1, |_i: int| own));
                    if self.it.obeys_prophetic_iter_laws() {
                        assert(self.given@ + self.it.remaining() =~= before + rem);
                    }
                }
                Some((<V::E as ReadsAgree<'v, V::X>>::ins(v), 1))
            },
            None => {
                proof {
                    self.ended@ = true;
                }
                None
            },
        }
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
    assert(rs.push(r).drop_last() =~= rs);
}

/// The values of runs one after another.
pub proof fn lemma_run_values_add<G: VGet>(a: Seq<Run<G>>, b: Seq<Run<G>>)
    ensures
        run_values(a + b) == run_values(a) + run_values(b),
    decreases b.len(),
{
    if b.len() == 0 {
        assert(a + b =~= a);
        assert(run_values(a) + run_values(b) =~= run_values(a));
    } else {
        lemma_run_values_add(a, b.drop_last());
        assert((a + b).drop_last() =~= a + b.drop_last());
        let run = Seq::new(b.last().count as nat, |_i: int| b.last().value.sv());
        assert(run_values(a) + run_values(b.drop_last()) + run =~= run_values(a) + (run_values(b.drop_last()) + run));
    }
}

/// The values of the runs `rs`, given to a column of `V`, as it holds
/// them.
spec fn held_runs<'v, V: seal::Sealed<C>, C>(rs: Seq<Run<Get<'v, V>>>) -> Seq<Held<V, C>>
    decreases rs.len(),
{
    if rs.len() == 0 {
        Seq::empty()
    } else {
        held_runs::<V, C>(rs.drop_last()) + Seq::new(
            rs.last().count as nat,
            |_i: int| <V::E as ReadsAgree<'v, V::X>>::own(rs.last().value),
        )
    }
}

/// What a column holds for the runs given are, in a specification, the
/// runs' values.
proof fn lemma_held_runs<'v, V: VValue<C>, C: VCodec>(rs: Seq<Run<Get<'v, V>>>)
    ensures
        <Held<V, C> as VGet>::svs(held_runs::<V, C>(rs)) == run_values(rs),
    decreases rs.len(),
{
    if rs.len() == 0 {
        lemma_svs_none::<Held<V, C>>();
    } else {
        let r = rs.last();
        let before = held_runs::<V, C>(rs.drop_last());
        let own = <V::E as ReadsAgree<'v, V::X>>::own(r.value);
        let run = Seq::new(r.count as nat, |_i: int| own);
        lemma_held_runs::<V, C>(rs.drop_last());
        lemma_svs_add(before, run);
        <Held<V, C> as VGet>::lemma_svs(run);
        V::lemma_own(r.value);
        assert(<Held<V, C> as VGet>::svs(run) =~= Seq::new(r.count as nat, |_i: int| r.value.sv()));
    }
}

/// The values a splice inserts, from a caller's iterator of runs.
#[verifier::reject_recursive_types(C)]
struct RunsFeed<'v, V: VValue<C>, C: VCodec, I: Iterator<Item = Run<Get<'v, V>>>> {
    it: I,
    /// the iterator as it was handed over
    #[allow(dead_code)]
    init: Ghost<I>,
    /// what it has given since
    #[allow(dead_code)]
    given: Ghost<Seq<Run<Get<'v, V>>>>,
    /// it has said that it has no more
    #[allow(dead_code)]
    ended: Ghost<bool>,
    _c: PhantomData<C>,
}

impl<'v, V: VValue<C>, C: VCodec, I: Iterator<Item = Run<Get<'v, V>>>> RunFeed<V::E> for RunsFeed<'v, V, C, I> {
    type X = <V::E as ReadsAgree<'v, V::X>>::Ins;

    /// The iterator as it was handed over: a feed keeps that.
    type K = I;

    /// As [`IterFeed`]'s, of runs.
    #[verifier::prophetic]
    closed spec fn inv(&self) -> bool {
        &&& self.it.obeys_prophetic_iter_laws() == self.init@.obeys_prophetic_iter_laws()
        &&& self.it.obeys_prophetic_iter_laws() ==> {
            &&& self.given@ + self.it.remaining() == self.init@.remaining()
            &&& self.ended@ ==> self.it.remaining().len() == 0
        }
    }

    closed spec fn key(&self) -> I {
        self.init@
    }

    closed spec fn given(&self) -> Seq<Held<V, C>> {
        held_runs::<V, C>(self.given@)
    }

    closed spec fn ended(&self) -> bool {
        self.ended@
    }

    #[inline(always)]
    fn next_run(&mut self) -> (r: Option<(<V::E as ReadsAgree<'v, V::X>>::Ins, usize)>) {
        let ghost before = self.given@;
        let ghost rem = self.it.remaining();
        match self.it.next() {
            Some(run) => {
                proof {
                    self.given@ = before.push(run);
                    assert(before.push(run).drop_last() =~= before);
                    if self.it.obeys_prophetic_iter_laws() {
                        assert(self.given@ + self.it.remaining() =~= before + rem);
                    }
                }
                Some((<V::E as ReadsAgree<'v, V::X>>::ins(run.value), run.count))
            },
            None => {
                proof {
                    self.ended@ = true;
                }
                None
            },
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

/// Slab `k` holds the values of the column that come after those of
/// the slabs before it.
proof fn lemma_slab_vals<'a, X: Decoders<'a>>(s: Seq<Slab<X::Tail>>, k: int)
    requires
        lens_ok::<X>(s),
        0 <= k < s.len(),
    ensures
        sum(slab_lens(s), 0, k + 1) == sum(slab_lens(s), 0, k) + s[k].len,
        sum(slab_lens(s), 0, k + 1) <= vals_of::<X>(s).len(),
        X::vals(s[k]).len() == s[k].len,
        forall|i: int|
            0 <= i < s[k].len ==> #[trigger] X::vals(s[k])[i] == vals_of::<X>(s)[sum(
                slab_lens(s),
                0,
                k,
            ) + i],
{
    lemma_from::<X>(s, k, 0);
    let v = X::vals(s[k]);
    let base = sum(slab_lens(s), 0, k) as int;
    assert(v.skip(0) =~= v);
    assert forall|i: int| 0 <= i < s[k].len implies #[trigger] v[i] == vals_of::<X>(s)[base + i] by {
        assert(vals_of::<X>(s).skip(base)[i] == v[i]);
    }
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
/// ([`VerifiedColumn::scope_to_value`]), and what values in order
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
    assert forall|i: int, j: int| 0 <= i <= j < vs.len() implies {
        &&& V::below(#[trigger] vs[j], t) ==> V::below(#[trigger] vs[i], t)
        &&& V::below(t, vs[i]) ==> V::below(t, vs[j])
    } by {
        // `vs[i]` is `vs[j]` or comes before it
        V::lemma_below(vs[i], vs[j], t);
        V::lemma_below(t, vs[i], vs[j]);
    }
}

/// The function `f` gave, for a value that is `a` in a specification,
/// one that is `b` ([`VerifiedColumn::remap`]): there are values `v` and
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

/// How many values a slab holds.
fn len_of<T: Copy + Clone + std::fmt::Debug + Default>(s: &Slab<T>) -> (n: usize)
    ensures
        n == s.len,
{
    s.len
}

/// How many runs a slab holds.
fn segments_in<T: Copy + Clone + std::fmt::Debug + Default>(s: &Slab<T>) -> usize {
    s.segments
}

/// The lengths of slabs, collected as [`Column`](crate::Column) collects
/// them. The functions mapped have names because Verus proves nothing of
/// a closure here whose argument is of a generic type.
#[inline]
fn lens_of<T: Copy + Clone + std::fmt::Debug + Default>(slabs: &[Slab<T>]) -> (r: Vec<usize>)
    ensures
        r@.len() == slabs@.len(),
        forall|k: int| 0 <= k < r@.len() ==> r@[k] == (#[trigger] slabs@[k]).len,
{
    let it = slabs.iter();
    let r: Vec<usize> = it.map(len_of::<T>).collect();
    proof {
        assert forall|k: int| 0 <= k < r@.len() implies r@[k] == (#[trigger] slabs@[k]).len by {
            assert(*it.remaining()[k] == slabs@[k]);
        }
    }
    r
}

/// The segment counts of slabs, collected likewise.
#[inline]
fn segments_of<T: Copy + Clone + std::fmt::Debug + Default>(slabs: &[Slab<T>]) -> Vec<usize> {
    slabs.iter().map(segments_in::<T>).collect()
}

/// Numbers that are the lengths of slabs add up as the slabs' lengths do.
proof fn lemma_sum_of_lens<T: Copy + Clone + std::fmt::Debug + Default>(lens: Seq<usize>, slabs: Seq<Slab<T>>, k: int)
    requires
        lens.len() == slabs.len(),
        forall|i: int| 0 <= i < lens.len() ==> lens[i] == (#[trigger] slabs[i]).len,
        0 <= k <= lens.len(),
    ensures
        sum_of(lens.take(k)) == sum(slab_lens(slabs), 0, k),
    decreases k,
{
    if k > 0 {
        lemma_sum_of_lens(lens, slabs, k - 1);
        assert(lens.take(k).drop_last() =~= lens.take(k - 1));
    }
}

/// What [`VerifiedColumn::dump_slabs`] prints of a slab: the values and
/// the runs it counts, its first run and its last.
struct SlabRuns<G> {
    len: usize,
    segments: usize,
    first: Option<Run<G>>,
    last: Option<Run<G>>,
}

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

impl<V: VValue<C>, C: VCodec> Clone for VerifiedColumn<V, C> {
    /// A column of its own with the same values.
    fn clone(&self) -> (r: Self)
        ensures
            r@ == self@,
    {
        VerifiedColumn { core: self.core.duplicate() }
    }
}

/// What Verus is told of the standard library's `Extend`: that it is a
/// trait with this method. Of an implementation it assumes nothing: what
/// [`VerifiedColumn`]'s does is stated there, and proved. (Verus's
/// library specifies `IntoIterator` and `FromIterator`, and not this
/// trait.)
#[verifier::external_trait_specification]
#[doc(hidden)]
pub trait ExExtend<A> {
    type ExternalTraitSpecificationFor: core::iter::Extend<A>;

    fn extend<T>(&mut self, iter: T) where T: IntoIterator<Item = A>;
}

impl<'v, V: VValue<C>, C: VCodec> Extend<Get<'v, V>> for VerifiedColumn<V, C> {
    /// Append the values that `iter` gives: what
    /// [`splice_iter`](VerifiedColumn::splice_iter) does at the column's
    /// end, and specified as that is.
    fn extend<I: IntoIterator<Item = Get<'v, V>>>(&mut self, iter: I)
        ensures
            exists|it: I::IntoIter| #[trigger]
                (I::into_iter).ensures((iter,), it) && (it.obeys_prophetic_iter_laws() ==> final(self)@ == old(
                    self,
                )@ + VGet::svs(it.remaining())),
    {
        let len = self.len();
        self.splice_iter(len, 0, iter);
        proof {
            let it = choose|it: I::IntoIter| #[trigger]
                (I::into_iter).ensures((iter,), it) && (it.obeys_prophetic_iter_laws() ==> self@ == old(self)@.take(
                    len as int,
                ) + VGet::svs(it.remaining()) + old(self)@.skip(len + 0));
            let vs = <Get<'v, V> as VGet>::svs(it.remaining());
            assert(old(self)@.take(len as int) + vs + old(self)@.skip(len + 0) =~= old(self)@ + vs);
        }
    }
}

#[cfg(verus_keep_ghost)]
impl<'v, V: VValue<C>, C: VCodec> FromIteratorSpecImpl<Get<'v, V>> for VerifiedColumn<V, C> {
    /// A column collected from an iterator holds the values that the
    /// iterator was to give.
    open spec fn from_iter_ensures(remaining: Seq<Get<'v, V>>, will_return_none: bool, s: Self) -> bool {
        s@ == <Get<'v, V> as VGet>::svs(remaining)
    }
}

impl<'v, V: VValue<C>, C: VCodec> FromIterator<Get<'v, V>> for VerifiedColumn<V, C> {
    /// A column of the values that `iter` gives, which is what
    /// `collect()` returns. Verus's specification of `FromIterator`
    /// states it for an iterator that keeps to what Verus asks of every
    /// iterator: the column then holds the values the iterator was to
    /// give (`from_iter_ensures`).
    fn from_iter<I: IntoIterator<Item = Get<'v, V>>>(iter: I) -> (s: Self) {
        let mut s = Self::new();
        s.splice_iter(0, 0, iter);
        proof {
            let it = choose|it: I::IntoIter| #[trigger]
                (I::into_iter).ensures((iter,), it) && (it.obeys_prophetic_iter_laws() ==> s@ == Seq::<
                    V::S,
                >::empty().take(0) + VGet::svs(it.remaining()) + Seq::<V::S>::empty().skip(0));
            let vs = <Get<'v, V> as VGet>::svs(it.remaining());
            assert(Seq::<V::S>::empty().take(0) + vs + Seq::<V::S>::empty().skip(0) =~= vs);
        }
        s
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

    /// No bytes are a saved column: the one of no values.
    pub proof fn lemma_no_bytes()
        ensures
            Self::valid(Seq::<u8>::empty()),
            Self::values(Seq::<u8>::empty()) == Seq::<V::S>::empty(),
    {
        <V::E as Model<V::X>>::lemma_none();
        lemma_svs_none::<Held<V, C>>();
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

    /// A column of `values`.
    pub fn from_values(values: &[Get<'_, V>]) -> (r: Self)
        ensures
            r@ == VGet::svs(values@),
    {
        Self::from_values_with_max_segments(values, MAX_SEG)
    }

    /// A column of `values` whose slabs hold at most `max_segments` runs
    /// each.
    pub fn from_values_with_max_segments(values: &[Get<'_, V>], max_segments: usize) -> (r: Self)
        requires
            max_segments >= 2,
        ensures
            r@ == VGet::svs(values@),
    {
        let mut r = Self::with_max_segments(max_segments);
        r.splice(0, 0, values);
        proof {
            let vs = VGet::svs(values@);
            assert(Seq::<V::S>::empty().take(0) + vs + Seq::<V::S>::empty().skip(0) =~= vs);
        }
        r
    }

    /// A column of `len` copies of `value`.
    ///
    /// The length is one that a column's run can count, and a `usize`
    /// with three to spare (which says more only where a `usize` has 32
    /// bits).
    pub fn fill(len: usize, value: Get<'_, V>) -> (r: Self)
        requires
            len <= i64::MAX,
            len <= usize::MAX - 3,
        ensures
            r@ == Seq::new(len as nat, |_i: int| value.sv()),
    {
        Self::filled(len, value, MAX_SEG)
    }

    /// [`fill`](Self::fill), in slabs of at most `max_segments` runs.
    #[inline(always)]
    fn filled(len: usize, value: Get<'_, V>, max_segments: usize) -> (r: Self)
        requires
            len <= i64::MAX,
            len <= usize::MAX - 3,
            max_segments >= 2,
        ensures
            r@ == Seq::new(len as nat, |_i: int| value.sv()),
    {
        if len == 0 {
            let r = Self::with_max_segments(max_segments);
            proof {
                assert(r@ =~= Seq::new(len as nat, |_i: int| value.sv()));
            }
            return r;
        }
        let slab = <V::E as Model<V::X>>::fill(len, value);
        let ghost s = slab;
        let mut slabs = Vec::with_capacity(1);
        slabs.push(slab);
        proof {
            assert(slabs@ =~= seq![s]);
            lemma_vals_unit::<V::E>(s);
        }
        let core = Core::<V::E>::from_cut(slabs, max_segments);
        proof {
            let vs = Seq::new(len as nat, |_i: int| value.sv());
            <Held<V, C> as VGet>::lemma_svs(core@);
            V::lemma_own(value);
            assert(<Held<V, C> as VGet>::svs(core@) =~= vs);
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

    /// Delete `del` values at `at` and insert there the values that
    /// `values` gives, as an iterator.
    ///
    /// The specification speaks of the iterator `values` becomes (`it`),
    /// and of what Verus says that one will give (`it.remaining()`). It
    /// holds for an iterator that keeps to what Verus asks of every
    /// iterator (`obeys_prophetic_iter_laws`), as those of the standard
    /// library that Verus specifies do; of another it says nothing but
    /// that `self` is a column.
    pub fn splice_iter<'v, I: IntoIterator<Item = Get<'v, V>>>(&mut self, at: usize, del: usize, values: I)
        requires
            at <= old(self)@.len(),
            del <= old(self)@.len() - at,
        ensures
            exists|it: I::IntoIter| #[trigger]
                (I::into_iter).ensures((values,), it) && (it.obeys_prophetic_iter_laws() ==> final(self)@ == old(
                    self,
                )@.take(at as int) + VGet::svs(it.remaining()) + old(self)@.skip(at + del)),
    {
        proof {
            <Held<V, C> as VGet>::lemma_svs(self.core@);
        }
        let it = values.into_iter();
        let ghost it0 = it;
        let mut f = <V::E as Model<V::X>>::blind();
        let mut feed = IterFeed::<V, C, I::IntoIter> {
            it,
            init: Ghost(it0),
            given: Ghost(Seq::empty()),
            ended: Ghost(false),
            _c: PhantomData,
        };
        proof {
            assert(feed.given@ + feed.it.remaining() =~= it0.remaining());
        }
        let _ins = self.core.splice(at, del, &mut f, &mut feed);
        proof {
            if it0.obeys_prophetic_iter_laws() {
                let vs = it0.remaining();
                assert(feed.given@ =~= feed.given@ + feed.it.remaining());
                assert(vs.take(0) =~= Seq::<Get<'v, V>>::empty());
                assert(vs.take(vs.len() as int) =~= vs);
                lemma_spliced::<V, C>(old(self).core@, at as int, del as int, vs, _ins@);
            }
        }
    }

    /// Delete `del` values at `at` and insert there the runs that `runs`
    /// gives, as an iterator: of each run its value, as many times as its
    /// count says (`run_values`). A run of count 0 inserts nothing.
    ///
    /// The specification speaks of the iterator as
    /// [`splice_iter`](Self::splice_iter)'s does. The counts are the
    /// caller's: with ones that make the column longer than it can count,
    /// or a run longer than the format can write, this panics.
    pub fn splice_runs<'v, I: IntoIterator<Item = Run<Get<'v, V>>>>(&mut self, at: usize, del: usize, runs: I)
        requires
            at <= old(self)@.len(),
            del <= old(self)@.len() - at,
        ensures
            exists|it: I::IntoIter| #[trigger]
                (I::into_iter).ensures((runs,), it) && (it.obeys_prophetic_iter_laws() ==> final(self)@ == old(
                    self,
                )@.take(at as int) + run_values(it.remaining()) + old(self)@.skip(at + del)),
    {
        proof {
            <Held<V, C> as VGet>::lemma_svs(self.core@);
        }
        let it = runs.into_iter();
        let ghost it0 = it;
        let mut f = <V::E as Model<V::X>>::blind();
        let mut feed = RunsFeed::<V, C, I::IntoIter> {
            it,
            init: Ghost(it0),
            given: Ghost(Seq::empty()),
            ended: Ghost(false),
            _c: PhantomData,
        };
        proof {
            assert(feed.given@ + feed.it.remaining() =~= it0.remaining());
        }
        let _ins = self.core.splice(at, del, &mut f, &mut feed);
        proof {
            if it0.obeys_prophetic_iter_laws() {
                let rs = it0.remaining();
                assert(feed.given@ =~= feed.given@ + feed.it.remaining());
                assert(_ins@ =~= Seq::<Held<V, C>>::empty() + _ins@);
                lemma_svs_spliced(old(self).core@, at as int, del as int, _ins@);
                lemma_held_runs::<V, C>(rs);
            }
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

    /// Delete the `n` values at `at`.
    #[inline(always)]
    pub fn remove_n(&mut self, at: usize, n: usize)
        requires
            at <= old(self)@.len(),
            n <= old(self)@.len() - at,
        ensures
            final(self)@ == old(self)@.take(at as int) + old(self)@.skip(at + n),
    {
        if n > 0 {
            self.delete(at, n);
        } else {
            proof {
                assert(self@ =~= self@.take(at as int) + self@.skip(at as int));
            }
        }
    }

    /// Delete every value. The column keeps its segment budget.
    pub fn clear(&mut self)
        ensures
            final(self)@ == Seq::<V::S>::empty(),
    {
        // a new column, and not a delete of every value, which would
        // leave the last slab to be emptied through a rebuild. The edit
        // counter goes on from where it is, as after any other edit.
        self.core.lemma_column();
        let mut core = Core::<V::E>::with_budget(self.core.max_segments());
        core.set_counter(self.core.counter().wrapping_add(1));
        proof {
            assert(core@ =~= Seq::empty());
            lemma_svs_none::<Held<V, C>>();
        }
        self.core = core;
    }

    /// How many slabs hold the column. That is a fact of how the column
    /// lies in memory and not of its values: nothing is specified of it.
    pub fn slab_count(&self) -> usize {
        self.core.slabs().len()
    }

    /// How many values each slab holds, slab by slab. They add up to the
    /// column's length (`sum_of`). How the values are spread over the
    /// slabs is a fact of how the column lies in memory: nothing else is
    /// specified of it.
    pub fn slab_lens(&self) -> (r: Vec<usize>)
        ensures
            sum_of(r@) == self@.len(),
    {
        self.core.lemma_column();
        let slabs = self.core.slabs();
        proof {
            <Held<V, C> as VGet>::lemma_svs(self.core@);
        }
        let r = lens_of(slabs);
        proof {
            lemma_sum_of_lens(r@, slabs@, r@.len() as int);
            assert(r@.take(r@.len() as int) =~= r@);
        }
        r
    }

    /// How many runs each slab holds, slab by slab. That is a fact of
    /// how the column lies in memory: nothing is specified of it.
    pub fn slab_segments(&self) -> Vec<usize> {
        segments_of(self.core.slabs())
    }

    /// Slab `i` as [`dump_slabs`](Self::dump_slabs) prints it: the
    /// counts it holds, its first run and its last, which are `None` for
    /// a slab of no values. `None` if the column has no slab `i`.
    // a Verus loop with an invariant: `loop` + `match`
    #[allow(clippy::while_let_loop)]
    fn slab_runs(&self, i: usize) -> Option<SlabRuns<Get<'_, V>>> {
        self.core.lemma_column();
        let slabs = self.core.slabs();
        if i >= slabs.len() {
            return None;
        }
        proof {
            lemma_lens_ok::<V::E, V::X>(slabs@);
        }
        let s = &slabs[i];
        let mut d = <V::X as Decoders>::open(s);
        let first = d.d_run_max(usize::MAX);
        let mut last = first;
        if first.is_some() {
            loop
                invariant
                    d.inv(),
                decreases d.rest().len(),
            {
                match d.d_run_max(usize::MAX) {
                    Some(run) => {
                        last = Some(run);
                    },
                    None => {
                        break ;
                    },
                }
            }
        }
        Some(SlabRuns { len: s.len, segments: s.segments, first, last })
    }

    /// How many slabs the column's index counts. For a column that
    /// holds values: an emptied one may be without its index (after a
    /// panic in an edit), and this panics then.
    fn index_len(&self) -> usize {
        self.core.lemma_column();
        self.core.index().ix_len()
    }

    /// How many values the column's index counts before slab `i`, or
    /// `None` if it counts fewer than `i` slabs. For a column that holds
    /// values, as [`index_len`](Self::index_len).
    fn index_before(&self, i: usize) -> Option<usize> {
        self.core.lemma_column();
        let ix = self.core.index();
        if i <= ix.ix_len() {
            Some(ix.ix_before(i))
        } else {
            None
        }
    }

    /// A reader of the values `start..end`.
    #[inline(always)]
    fn reader(&self, start: usize, end: usize) -> (it: ICore<<V::X as Decoders<'_>>::D>)
        requires
            start <= end <= self@.len(),
        ensures
            it.wf::<V::X>(self.core.slabs_spec()),
            it.pos == start,
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
        let ghost o = it.out::<V::X>(slabs@);
        it.drain_into::<V::X>(slabs, &mut out);
        proof {
            assert(out@ =~= o);
            assert(self@.subrange(0, n as int) =~= self@);
        }
        out
    }

    /// Whether every value is `value`: so of an empty column.
    pub fn is_only(&self, value: Get<'_, V>) -> (r: bool)
        ensures
            r == only(self@, value.sv()),
    {
        self.core.lemma_column();
        let slabs = self.core.slabs();
        let ghost vs = vals_of::<V::X>(slabs@);
        proof {
            lemma_lens_ok::<V::E, V::X>(slabs@);
            lemma_vals_own::<V::E, V::X>(slabs@);
            assert(self.core@ =~= held::<V, C>(vs));
            lemma_held::<V, C>(vs);
            VGet::lemma_svs(vs);
        }
        // slab by slab: a slab of one value is one run of it
        let mut j: usize = 0;
        while j < slabs.len()
            invariant
                j <= slabs@.len(),
                lens_ok::<V::X>(slabs@),
                vs == vals_of::<V::X>(slabs@),
                vs.len() == self@.len(),
                vs.len() < usize::MAX,
                forall|i: int| 0 <= i < vs.len() ==> self@[i] == (#[trigger] vs[i]).sv(),
                forall|i: int| 0 <= i < sum(slab_lens(slabs@), 0, j as int) ==> self@[i] == value.sv(),
            decreases slabs@.len() - j,
        {
            let s = &slabs[j];
            let ghost base = sum(slab_lens(slabs@), 0, j as int) as int;
            let ghost sv = <V::X as Decoders>::vals(*s);
            proof {
                lemma_slab_vals::<V::X>(slabs@, j as int);
            }
            if !<V::X as Decoders>::maybe_uniform(s) {
                proof {
                    // two of the slab's values differ, so one is not
                    // `value`
                    let i = choose|i: int| 0 < i < sv.len() && sv[i - 1] != #[trigger] sv[i];
                    let a = sv[i - 1];
                    let b = sv[i];
                    assert(a == vs[base + i - 1]);
                    assert(b == vs[base + i]);
                    if a.sv() == b.sv() {
                        VGet::lemma_sv_inj(a, b);
                    }
                    if self@[base + i - 1] == value.sv() {
                        assert(self@[base + i] != value.sv());
                    }
                }
                return false;
            }
            let mut d = <V::X as Decoders>::open(s);
            if let Some(run) = d.d_run_max(usize::MAX) {
                if run.count != s.len || !V::same(run.value, value) {
                    proof {
                        // the slab's first value is another, or the one
                        // at which its first run ends
                        let k: int = if run.value.sv() == value.sv() {
                            run.count as int
                        } else {
                            0
                        };
                        let w = sv[k];
                        assert(sv.take(run.count as int)[0] == run.value);
                        if w.sv() == run.value.sv() {
                            VGet::lemma_sv_inj(w, run.value);
                        }
                        assert(w == vs[base + k]);
                        assert(self@[base + k] != value.sv());
                    }
                    return false;
                }
                proof {
                    assert forall|i: int| base <= i < base + s.len implies self@[i]
                        == value.sv() by {
                        assert(sv.take(run.count as int)[i - base] == run.value);
                        assert(sv[i - base] == vs[i]);
                    }
                }
            }
            j += 1;
        }
        true
    }

    /// Where `value` is among the values `start..end`, which are taken
    /// to be in order: the range of the places that hold it or, where
    /// none does, the empty range at the place where it would stand.
    ///
    /// The search looks at few of the values: it is a search by halves
    /// over the column's slabs and then a walk along the runs of one or
    /// a few of them. The values need be in order only as far as
    /// `value` can tell (`sorted_for`), which values in order
    /// (`sorted`) are for every `value` (`lemma_sorted`). Where
    /// they are not, the range is some range within `start..end`.
    pub fn scope_to_value(&self, value: Get<'_, V>, start: usize, end: usize) -> (r: Range<usize>)
        requires
            start <= end <= self@.len(),
        ensures
            start <= r.start <= r.end <= end,
            sorted_for::<V>(self@.subrange(start as int, end as int), value.sv()) ==> forall|k: int|
                start <= k < end ==> {
                    &&& (k < r.start) == V::below(#[trigger] self@[k], value.sv())
                    &&& (k < r.end) == !V::below(value.sv(), self@[k])
                    &&& (r.start <= k < r.end) == (self@[k] == value.sv())
                },
    {
        if start == end {
            // nothing to look at (and a column that an edit's panic left
            // without its parts has no index to ask)
            return start..start;
        }
        self.core.lemma_column();
        let slabs = self.core.slabs();
        let index = self.core.index();
        let mut it = self.reader(start, end);
        let ghost w = it.out::<V::X>(slabs@);
        let ghost sub = self@.subrange(start as int, end as int);
        let probe = VProbe::<V, C> { target: value, _c: PhantomData };
        let find = |t: usize| -> (r: (usize, usize))
            requires
                index.ok(),
                wlens::<usize, SlabBTree<usize>>(index.ws()) == slab_lens(slabs@),
            ensures
                find_ok(slabs@, t, r),
            {
                let r = index.ix_find(t);
                proof {
                    assert(index.ws().map_values(|w: usize| <SlabBTree<usize> as IndexOps<usize>>::wlen(w))
                        =~= wlens::<usize, SlabBTree<usize>>(index.ws()));
                }
                // past the slabs, the whole position lies beyond them
                if r.0 >= slabs.len() {
                    (slabs.len(), 0)
                } else {
                    r
                }
            };
        let start_of = |i: usize| -> (n: usize)
            requires
                index.ok(),
                wlens::<usize, SlabBTree<usize>>(index.ws()) == slab_lens(slabs@),
                i <= slabs@.len(),
            ensures
                n == sum(slab_lens(slabs@), 0, i as int),
            {
                let n = index.ix_before(i);
                proof {
                    assert(index.ws().map_values(|w: usize| <SlabBTree<usize> as IndexOps<usize>>::wlen(w))
                        =~= wlens::<usize, SlabBTree<usize>>(index.ws()));
                }
                n
            };
        let r = it.seek_value::<V::X, _, _, _>(slabs, &probe, &find, &start_of);
        proof {
            let t = value.sv();
            VGet::lemma_svs(w);
            assert(VGet::svs(w) == sub);
            if sorted_for::<V>(sub, t) {
                assert forall|i: int, j: int| 0 <= i <= j < w.len() implies #[trigger] probe.side(w[i])
                    <= #[trigger] probe.side(w[j]) by {
                    assert(sub[i] == w[i].sv());
                    assert(sub[j] == w[j].sv());
                }
                assert(monotone(probe, w));
                assert(found(probe, w, r.0 - start, r.1 - start));
                assert forall|k: int| start <= k < end implies {
                    &&& (k < r.0) == V::below(#[trigger] self@[k], t)
                    &&& (k < r.1) == !V::below(t, self@[k])
                    &&& (r.0 <= k < r.1) == (self@[k] == t)
                } by {
                    let x = self@[k];
                    assert(sub[k - start] == x);
                    assert(w[k - start].sv() == x);
                    // not both before `t` and after it, and where
                    // neither, `t`
                    V::lemma_below(x, t, x);
                    V::lemma_below(t, t, t);
                }
            }
        }
        r.0..r.1
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
    /// column are.
    pub fn remap<F: Fn(V) -> V>(&mut self, f: F)
        requires
            forall|v: V| old(self)@.contains(V::sv_of(v)) ==> #[trigger] call_requires(f, (v,)),
        ensures
            final(self)@.len() == old(self)@.len(),
            forall|i: int| 0 <= i < old(self)@.len() ==> mapped(f, old(self)@[i], #[trigger] final(self)@[i]),
    {
        let counter = self.core.counter();
        let m = VMap::<V, C, F> { f: &f, _c: PhantomData };
        let ghost o = vals_of::<V::X>(self.core.slabs_spec());
        self.core.lemma_column();
        proof {
            // the values read are the column's
            lemma_vals_own::<V::E, V::X>(self.core.slabs_spec());
            assert(self.core@ =~= held::<V, C>(o));
            lemma_held::<V, C>(o);
            VGet::lemma_svs(o);
            assert forall|i: int| 0 <= i < self.core@.len() implies m.pre(#[trigger] o[i]) by {
                assert(self@[i] == o[i].sv());
            }
        }
        let (core, Ghost(ws)) = self.core.remap::<V::X, V::N, VMap<V, C, F>>(&m);
        proof {
            <Held<V, C> as VGet>::lemma_svs(core@);
            assert forall|i: int| 0 <= i < o.len() implies mapped(
                f,
                old(self)@[i],
                #[trigger] <Held<V, C> as VGet>::svs(core@)[i],
            ) by {
                assert(m.maps(o[i], ws[i]));
                assert(old(self)@[i] == o[i].sv());
            }
        }
        self.core = core;
        // a write like any other: the edit counter goes on from where it
        // was
        self.core.set_counter(counter.wrapping_add(1));
    }

    /// An iterator over the values.
    #[inline(always)]
    pub fn iter(&self) -> (it: VerifiedIter<'_, V, C>)
        ensures
            it.rest() == self@,
    {
        proof {
            <Held<V, C> as VGet>::lemma_svs(self.core@);
        }
        let n = self.core.len();
        proof {
            assert(self@.subrange(0, n as int) =~= self@);
        }
        VerifiedIter { it: self.reader(0, n), slabs: self.core.slabs(), col: Some(&self.core) }
    }

    /// An iterator over the values `start..end`.
    #[inline(always)]
    pub fn iter_range(&self, start: usize, end: usize) -> (it: VerifiedIter<'_, V, C>)
        requires
            start <= end <= self@.len(),
        ensures
            it.rest() == self@.subrange(start as int, end as int),
    {
        VerifiedIter { it: self.reader(start, end), slabs: self.core.slabs(), col: Some(&self.core) }
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

    /// Save the column after what `out` holds: `out` is then what it
    /// was, and after that the encoding of the column's values. The
    /// range returned is where in `out` that is.
    pub fn save_to(&self, out: &mut Vec<u8>) -> (r: Range<usize>)
        ensures
            final(out)@ == old(out)@ + Self::encoding(self@),
            Self::valid(Self::encoding(self@)),
            Self::values(Self::encoding(self@)) == self@,
            Self::encoding(self@).len() <= V::size_bound(self@),
            r.start == old(out)@.len(),
            r.end == final(out)@.len(),
    {
        self.core.lemma_column();
        let start = out.len();
        <V::E as Model<V::X>>::save(self.core.slabs(), self.core.len(), out);
        proof {
            let d = out@.skip(start as int);
            Self::lemma_encoding(d);
            V::lemma_size(d);
            assert(out@ =~= old(out)@ + d);
        }
        start..out.len()
    }

    /// [`save_to`](Self::save_to), unless every value of the column is
    /// `value`: then nothing is written, and the range is empty. So is
    /// it for an empty column, whatever `value` is.
    pub fn save_to_unless(&self, out: &mut Vec<u8>, value: Get<'_, V>) -> (r: Range<usize>)
        ensures
            final(out)@ == if only(self@, value.sv()) {
                old(out)@
            } else {
                old(out)@ + Self::encoding(self@)
            },
            !only(self@, value.sv()) ==> {
                &&& Self::valid(Self::encoding(self@))
                &&& Self::values(Self::encoding(self@)) == self@
                &&& Self::encoding(self@).len() <= V::size_bound(self@)
            },
            r.start == old(out)@.len(),
            r.end == final(out)@.len(),
    {
        if self.is_only(value) {
            out.len()..out.len()
        } else {
            self.save_to(out)
        }
    }

    /// Save the column and read the bytes through, as a load does: an
    /// error if they are not a saved column.
    ///
    /// That they are one is proved of [`save`](Self::save). So this
    /// returns no error for a column that saves to less than 4 GiB, as
    /// one does whose values' `size_bound` is less. (A longer column
    /// saves to a saved column too. But its last run may be more bytes
    /// than a slab can count, and the read refuses that.)
    pub fn validate_encoding(&self) -> (r: Result<(), PackError>)
        ensures
            Self::encoding(self@).len() <= u32::MAX ==> r is Ok,
            V::size_bound(self@) <= u32::MAX ==> r is Ok,
    {
        let bytes = self.save();
        match <V::E as Model<V::X>>::validate(bytes.as_slice()) {
            Ok(_) => Ok(()),
            Err(e) => Err(e),
        }
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
        match <V::E as Model<V::X>>::load(bytes, MAX_SEG, None) {
            Ok(core) => Ok(VerifiedColumn { core }),
            Err(e) => Err(e),
        }
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
    /// A slab holds two runs or more: [`Column`](crate::Column) panics
    /// when asked for fewer, and here that is a `requires`. The fill
    /// value need not live as long as `bytes` do, nor `bytes` as long as
    /// it: the column owns what it holds.
    pub fn load_with<'a, F: MaybeFill<Get<'a, V>>>(bytes: &[u8], opts: LoadOpts<F>) -> (r: Result<Self, PackError>)
        requires
            opts.max_segments() >= 2,
        ensures
            match fill_of(bytes@, opts.length(), opts.fill().value()) {
                Some((len, v)) => r matches Ok(c) && c@ == Seq::new(len as nat, |_i: int| v.sv()),
                None => {
                    &&& r matches Ok(c) ==> {
                        &&& Self::valid(bytes@)
                        &&& c@ == Self::values(bytes@)
                        &&& opts.length() matches Some(len) ==> c@.len() == len
                    }
                    &&& Self::loadable(bytes@) && (opts.length() matches Some(len) ==> Self::values(bytes@).len() == len)
                        ==> r is Ok
                },
            },
    {
        let (length, fill, max_segments) = opts.parts();
        if bytes.len() == 0 {
            if let Some(len) = length {
                if len as u64 <= i64::MAX as u64 && len <= usize::MAX - 3 {
                    if let Some(v) = fill.fill_value() {
                        return Ok(Self::filled(len, v, max_segments));
                    }
                }
            }
        }
        proof {
            <Held<V, C> as VGet>::lemma_svs(<V::E as Model<V::X>>::values(bytes@));
        }
        match <V::E as Model<V::X>>::load(bytes, max_segments, length) {
            Ok(core) => Ok(VerifiedColumn { core }),
            Err(e) => Err(e),
        }
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

/// A fold of `vs` is `f` on its first value and a fold of the others.
proof fn lemma_folded_step<B, T, F: FnMut(B, T) -> B>(f: F, a: B, vs: Seq<T>, b: B, r: B)
    requires
        vs.len() > 0,
        f.ensures((a, vs[0]), b),
        folded(f, b, vs.drop_first(), r),
    ensures
        folded(f, a, vs, r),
{
}

/// An iterator over a column's values, which
/// [`VerifiedColumn::iter`] and [`VerifiedColumn::iter_range`] give.
///
/// It is a `std::iter::Iterator`. What it has left to give is in a
/// specification `items()`, and as values `rest()`. Verus's
/// specifications of the standard library's iterator functions speak of
/// `remaining()`, which is `items()`.
pub struct VerifiedIter<'a, V: VValue<C>, C: VCodec = Leb128> {
    it: ICore<<V::X as Decoders<'a>>::D>,
    slabs: &'a [Slab<<V::E as Slabs>::Tail>],
    /// The column the slabs are of; a default iterator has none.
    col: Option<&'a Core<V::E>>,
}

impl<'a, V: VValue<C>, C: VCodec> VerifiedIter<'a, V, C> {
    /// The iterator stands in the slabs of its column.
    #[verifier::type_invariant]
    spec fn inv(&self) -> bool {
        &&& self.it.wf::<V::X>(self.slabs@)
        &&& match self.col {
            Some(c) => self.slabs@ == c.slabs_spec(),
            None => self.slabs@.len() == 0,
        }
    }

    /// What the iterator has left to give.
    pub closed spec fn items(&self) -> Seq<Get<'a, V>> {
        self.it.out::<V::X>(self.slabs@)
    }

    /// The values the iterator has left to give.
    pub open spec fn rest(&self) -> Seq<V::S> {
        <Get<'a, V> as VGet>::svs(self.items())
    }

    /// Where item `t` of the column is, as the column's index has it:
    /// what a long move asks before it goes. (A column that an edit's
    /// panic left without its parts has no slabs and no index, and every
    /// item is past its end.)
    #[inline(always)]
    fn look(&self, t: usize) -> (far: (usize, usize, usize))
        ensures
            far_ok(self.slabs@, t, far),
    {
        proof {
            use_type_invariant(self);
        }
        let n = self.slabs.len();
        match self.col {
            Some(c) => {
                c.lemma_column();
                match c.index_opt() {
                    Some(ix) => {
                        let r = ix.ix_find(t);
                        proof {
                            assert(ix.ws().map_values(|w: usize| <SlabBTree<usize> as IndexOps<usize>>::wlen(w))
                                =~= wlens::<usize, SlabBTree<usize>>(ix.ws()));
                        }
                        if r.0 >= n {
                            // past the slabs, the whole position lies
                            // beyond them
                            (n, 0, c.len())
                        } else {
                            (r.0, r.1, t - r.1)
                        }
                    },
                    None => (n, 0, 0),
                }
            },
            None => (n, 0, 0),
        }
    }

    /// `nth`, for a move that may leave the slab the iterator stands in:
    /// a function of its own, so that a caller of `nth` has the short
    /// move inline and calls for the long one.
    #[inline(never)]
    fn nth_far(&mut self, n: usize) -> (r: Option<Get<'a, V>>)
        ensures
            n < old(self).items().len() ==> {
                &&& r == Some(old(self).items()[n as int])
                &&& final(self).items() == old(self).items().skip(n + 1)
                &&& r matches Some(v) && v.sv() == old(self).rest()[n as int]
                &&& final(self).rest() == old(self).rest().skip(n + 1)
            },
            n >= old(self).items().len() ==> r is None && final(self).items().len() == 0,
    {
        proof {
            use_type_invariant(&*self);
        }
        let ghost o = self.it.out::<V::X>(self.slabs@);
        let far = match self.it.far_target(n) {
            Some(t) => self.look(t),
            None => (0, 0, 0),
        };
        let r = self.it.nth_at::<V::X>(self.slabs, n, far);
        proof {
            <Get<'a, V> as VGet>::lemma_svs(o);
            <Get<'a, V> as VGet>::lemma_svs(self.it.out::<V::X>(self.slabs@));
            if n < o.len() {
                lemma_svs_sub(o, n + 1, o.len() as int);
            }
        }
        r
    }
}

#[cfg(verus_keep_ghost)]
impl<'a, V: VValue<C>, C: VCodec> vstd::std_specs::iter::IteratorSpecImpl for VerifiedIter<'a, V, C> {
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

impl<'a, V: VValue<C>, C: VCodec> Iterator for VerifiedIter<'a, V, C> {
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
                },
                None => old(self).rest().len() == 0 && final(self).rest().len() == 0,
            },
    {
        proof {
            use_type_invariant(&*self);
        }
        let ghost o = self.it.out::<V::X>(self.slabs@);
        let r = self.it.next_inline::<V::X>(self.slabs);
        proof {
            let o1 = self.it.out::<V::X>(self.slabs@);
            <Get<'a, V> as VGet>::lemma_svs(o);
            <Get<'a, V> as VGet>::lemma_svs(o1);
            if r is Some {
                lemma_svs_sub(o, 1, o.len() as int);
                assert(o.skip(1) =~= o.drop_first());
            } else {
                assert(o1 =~= o);
            }
        }
        r
    }

    /// The `n`th of the values left, counted from 0, if there are that
    /// many: it and the values before it are then given. If there are
    /// not, none is left.
    ///
    /// Few of the values passed over are looked at: a long move asks the
    /// column's index for the slab to go to, and within a slab a whole
    /// run is passed over at once.
    #[inline]
    fn nth(&mut self, n: usize) -> (r: Option<Get<'a, V>>)
        ensures
            n < old(self).items().len() ==> {
                &&& r == Some(old(self).items()[n as int])
                &&& final(self).items() == old(self).items().skip(n + 1)
                &&& r matches Some(v) && v.sv() == old(self).rest()[n as int]
                &&& final(self).rest() == old(self).rest().skip(n + 1)
            },
            n >= old(self).items().len() ==> r is None && final(self).items().len() == 0,
    {
        proof {
            use_type_invariant(&*self);
        }
        if n < self.it.items_left && n < self.it.slab_remaining {
            // within the slab the iterator stands in, and its window:
            // the index is not asked
            let ghost o = self.it.out::<V::X>(self.slabs@);
            let r = self.it.nth_at::<V::X>(self.slabs, n, (0, 0, 0));
            proof {
                <Get<'a, V> as VGet>::lemma_svs(o);
                <Get<'a, V> as VGet>::lemma_svs(self.it.out::<V::X>(self.slabs@));
                if n < o.len() {
                    lemma_svs_sub(o, n + 1, o.len() as int);
                }
            }
            r
        } else {
            self.nth_far(n)
        }
    }

    /// How many values are left: exactly.
    #[inline]
    fn size_hint(&self) -> (r: (usize, Option<usize>))
        ensures
            r.0 == self.items().len(),
            r.0 == self.rest().len(),
            r.1 == Some(r.0),
    {
        proof {
            use_type_invariant(self);
            self.it.lemma_here::<V::X>(self.slabs@);
            <Get<'a, V> as VGet>::lemma_svs(self.items());
        }
        // the window can run past the column's end
        let left = match self.col {
            Some(col) => {
                col.lemma_column();
                self.it.items_left.min(col.len().saturating_sub(self.it.pos))
            },
            None => 0,
        };
        (left, Some(left))
    }

    /// What `f` makes of `init` and each of the values left in turn. `f`
    /// is to take any value with any accumulator.
    ///
    /// The values are read a run at a time, as are those the standard
    /// library reads through `fold` (`count`, `last`, `sum`, `for_each`
    /// among them): faster than a loop over `next` where values come in
    /// runs, and slower where few values repeat.
    fn fold<B, F>(self, init: B, f: F) -> (r: B) where F: FnMut(B, Get<'a, V>) -> B
        requires
            forall|b: B, v: Get<'a, V>| #[trigger] f.requires((b, v)),
        ensures
            folded(f, init, self.items(), r),
    {
        let ghost all = self.it.out::<V::X>(self.slabs@);
        let ghost f0 = f;
        let mut me = self;
        let mut f = f;
        let mut acc = init;
        loop
            invariant
                f == f0,
                forall|b: B, v: Get<'a, V>| #[trigger] f.requires((b, v)),
                forall|r: B| folded(f, acc, me.it.out::<V::X>(me.slabs@), r) ==> folded(f, init, all, r),
            ensures
                folded(f, init, all, acc),
            decreases me.it.items_left,
        {
            proof {
                use_type_invariant(&me);
            }
            if me.it.items_left == 0 {
                proof {
                    assert(me.it.out::<V::X>(me.slabs@).len() == 0);
                }
                break ;
            }
            let max = me.it.items_left;
            let ghost o = me.it.out::<V::X>(me.slabs@);
            match me.it.run_once::<V::X>(me.slabs, max) {
                Some(run) => {
                    proof {
                        assert(o.skip(0) =~= o);
                    }
                    let mut k: usize = 0;
                    while k < run.count
                        invariant
                            k <= run.count <= o.len(),
                            o.take(run.count as int) == Seq::new(run.count as nat, |i: int| run.value),
                            f == f0,
                            forall|b: B, v: Get<'a, V>| #[trigger] f.requires((b, v)),
                            forall|r: B| folded(f, acc, o.skip(k as int), r) ==> folded(f, init, all, r),
                        decreases run.count - k,
                    {
                        let ghost a = acc;
                        acc = f(acc, run.value);
                        proof {
                            let now = o.skip(k as int);
                            assert(now[0] == o.take(run.count as int)[k as int]);
                            assert(now.drop_first() =~= o.skip(k + 1));
                            assert forall|r: B| folded(f, acc, o.skip(k + 1), r) implies folded(f, init, all, r) by {
                                lemma_folded_step(f, a, now, acc, r);
                            }
                        }
                        k += 1;
                    }
                },
                None => {
                    break ;
                },
            }
        }
        acc
    }
}

#[cfg(verus_keep_ghost)]
impl<'a, V: VValue<C>, C: VCodec> vstd::std_specs::iter::ExactSizeIteratorSpecImpl for VerifiedIter<'a, V, C> {
    open spec fn exact_len(&self) -> usize {
        self.items().len() as usize
    }
}

impl<'a, V: VValue<C>, C: VCodec> ExactSizeIterator for VerifiedIter<'a, V, C> {
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

impl<'a, V: VValue<C>, C: VCodec> Clone for VerifiedIter<'a, V, C> {
    /// An iterator of its own with the same values left.
    fn clone(&self) -> (r: Self)
        ensures
            r.items() == self.items(),
    {
        proof {
            use_type_invariant(self);
        }
        VerifiedIter { it: self.it.dup(), slabs: self.slabs, col: self.col }
    }
}

impl<'a, V: VValue<C>, C: VCodec> Default for VerifiedIter<'a, V, C> {
    /// An iterator with nothing to give.
    fn default() -> (r: Self)
        ensures
            r.items().len() == 0,
            r.rest().len() == 0,
    {
        let r = VerifiedIter { it: ICore::empty::<V::X>(), slabs: &[], col: None };
        proof {
            <Get<'a, V> as VGet>::lemma_svs(r.items());
        }
        r
    }
}

impl<'a, V: VValue<C>, C: VCodec> IntoIterator for &'a VerifiedColumn<V, C> {
    type Item = Get<'a, V>;

    type IntoIter = VerifiedIter<'a, V, C>;

    /// An iterator over the values ([`VerifiedColumn::iter`]).
    #[inline(always)]
    fn into_iter(self) -> (it: VerifiedIter<'a, V, C>)
        ensures
            it.rest() == self@,
    {
        self.iter()
    }
}

} // verus!

impl<V: VValue<C>, C: VCodec> std::fmt::Debug for VerifiedIter<'_, V, C> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("VerifiedIter")
            .field("pos", &self.it.pos)
            .field("items_left", &self.it.items_left)
            .finish()
    }
}

// The two functions of this block print and compare, and Verus does not
// check them. They read the column through checked functions that ask
// nothing of a caller.
impl<V: VValue<C>, C: VCodec> VerifiedColumn<V, C> {
    /// Print a line for each slab, as [`Column`](crate::Column) does:
    /// where in the column the slab starts, how many values and runs it
    /// holds, its first run and its last.
    ///
    /// The numbers and the runs are read by checked code; the printing
    /// is not checked. Nothing is specified of what is printed: it tells
    /// how the column lies in memory.
    pub fn dump_slabs<'a>(&'a self)
    where
        Get<'a, V>: std::fmt::Debug,
    {
        let mut offset = 0;
        let mut i = 0;
        while let Some(s) = self.slab_runs(i) {
            log!(
                "  slab[{i}]: offset={offset} len={} segments={} first={:?} last={:?}",
                s.len,
                s.segments,
                s.first,
                s.last
            );
            offset += s.len;
            i += 1;
        }
    }

    /// Check at run time how the column lies in memory, and panic with
    /// what is wrong, as `Column::check_invariants` does: every slab
    /// holds values, and no more runs than the budget (but for one value
    /// too long to share a slab); its bytes read through as the values
    /// and runs it counts; no two neighbours should have been merged;
    /// the lengths add up to the column's; and the index counts the
    /// slabs and their lengths.
    ///
    /// A tool for tests. Verus does not check it, and it promises
    /// nothing. What it compares of the bytes, the lengths and the index
    /// is proved of every column; the budget and the merging are the
    /// layout policy, which is not proved.
    ///
    /// The merging is the policy of edits, and a load does not keep it:
    /// it cuts slabs of half the budget, and the last slab holds what is
    /// left, however little. So this panics for some columns that were
    /// loaded, with nothing wrong with them, as `Column::check_invariants`
    /// does for the same columns.
    #[doc(hidden)]
    pub fn check_invariants(&self) {
        let slabs = self.core.slabs();
        let max = self.core.max_segments();
        let min = (max / 4).max(1);
        if self.core.len() == 0 {
            assert!(slabs.len() <= 1, "empty column holds {} slabs", slabs.len());
            return;
        }
        for (i, slab) in slabs.iter().enumerate() {
            assert_ne!(slab.len, 0, "slab {i} of {} is empty", slabs.len());
            assert!(
                slab.segments <= max || slab.segments == 1,
                "slab {i} has {} segments, over the budget of {max} \
                 (only a single oversize value may exceed it)",
                slab.segments
            );
            let (len, segments) = <V::E as Model<V::X>>::validate(&slab.data)
                .unwrap_or_else(|e| panic!("slab {i} does not parse: {e}"));
            assert_eq!(len, slab.len, "slab {i} cached len");
            assert_eq!(segments, slab.segments, "slab {i} cached segments");
        }
        for i in 1..slabs.len() {
            let (a, b) = (slabs[i - 1].segments, slabs[i].segments);
            assert!(
                !((a < min || b < min) && a + b <= max),
                "slabs {} and {i} ({a} and {b} segments) should have been merged \
                 (min {min}, max {max})",
                i - 1
            );
        }
        assert_eq!(
            self.core.len(),
            slabs.iter().map(|s| s.len).sum::<usize>(),
            "total_len"
        );
        assert_eq!(self.index_len(), slabs.len(), "index length");
        let mut before = 0usize;
        for (i, slab) in slabs.iter().enumerate() {
            assert_eq!(
                self.index_before(i),
                Some(before),
                "index items_before({i})"
            );
            before += slab.len;
        }
    }
}

impl<V: VValue<C>, C: VCodec> std::fmt::Debug for VerifiedColumn<V, C> {
    /// How many values and how many slabs, as `Column` prints itself.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("VerifiedColumn")
            .field("len", &self.core.len())
            .field("slabs", &self.core.slabs().len())
            .finish()
    }
}

// one budget for both APIs
const _: () = assert!(MAX_SEG == crate::column::DEFAULT_MAX_SEG);

#[cfg(test)]
mod tests {
    use super::*;

    /// For each window `start..end` of a column of `vals` that lies in
    /// several slabs: `ICore::drain_into` appends the window's values to
    /// what the vector held, and leaves nothing to read.
    macro_rules! windows {
        ($t:ty, $vals:expr) => {{
            let vals = $vals;
            let col = VerifiedColumn::<$t>::from_values_with_max_segments(&vals, 4);
            let slabs = col.core.slabs();
            assert!(slabs.len() > 3, "{} slabs", slabs.len());
            for start in 0..=vals.len() {
                for end in start..=vals.len() {
                    let mut out = Vec::new();
                    let mut it = col.reader(0, start);
                    it.drain_into::<<$t as seal::Sealed<Leb128>>::X>(slabs, &mut out);
                    let mut it = col.reader(start, end);
                    it.drain_into::<<$t as seal::Sealed<Leb128>>::X>(slabs, &mut out);
                    assert_eq!(out[..], vals[..end], "{start}..{end}");
                    assert!(it.next::<<$t as seal::Sealed<Leb128>>::X>(slabs).is_none());
                }
            }
        }};
    }

    /// `to_vec` reads a whole column, and so every slab to its end. Here
    /// the windows end anywhere, inside a slab and inside a run.
    #[test]
    fn drain_into_appends_a_window() {
        let nums: Vec<u64> = (0..90u64)
            .map(|i| if i % 9 < 4 { i / 9 } else { i * 1000 })
            .collect();
        windows!(u64, nums.clone());
        let opts: Vec<Option<u64>> = nums
            .iter()
            .enumerate()
            .map(|(i, v)| (i % 5 != 0 && i % 13 != 6).then_some(*v))
            .collect();
        windows!(Option<u64>, opts);
        let bools: Vec<bool> = (0..90).map(|i| i % 7 < 3 || i % 11 == 0).collect();
        windows!(bool, bools);
        let owned: Vec<Vec<u8>> = nums.iter().map(|v| v.to_string().into_bytes()).collect();
        let lent: Vec<&[u8]> = owned.iter().map(|v| &v[..]).collect();
        windows!(Vec<u8>, lent);
    }
}
