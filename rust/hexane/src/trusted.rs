//! What the verified API takes on trust, all of it in one place.
//!
//! Verus checks every function on a path of `hexane::verified` but those
//! here: it takes each as it is, its body unchecked and its contract
//! assumed. Most assume nothing but that they exist: the helpers that
//! panic, whose contract is that they do not return (which a panic does
//! not), and the constructors of errors and the other helpers, whose
//! contracts say nothing of what they give. What is assumed of substance
//! is this:
//!
//! * three specifications of the standard library that Verus has none
//!   of: replacing a range of a `Vec` with other items (`splice_vec`,
//!   `splice_bytes`) and taking a range out of one (`drain_slabs`);
//! * of strings, the specification of `core::str::from_utf8`, with the
//!   name of its error type, and that two `str`s of the same characters
//!   are one `str` (`axiom_str_same`).
//!
//! Besides what is here, the verified API rests on Verus, on its solver
//! Z3, and on Verus's library `vstd` and its specifications of the
//! standard library: the module docs of `hexane::verified` ("What is
//! trusted") say which of those it uses.
//!
//! The old API takes more on trust, in code that no function of the
//! verified API runs, and that is left where it is: its bridges to a
//! value type's and a codec's own code (`rle::model::Ops`,
//! `rle::state::WOps` and their kin, outside `verus!`); the sums of a
//! prefix column of a client's type `P` (`btree::axiom_prefix_add`,
//! `prefix_core::axiom_prefix_sub`, and `PrefixSlabWeight`'s operations in
//! `btree.rs`, `prefix.rs` and `weights.rs`, among them a helper that
//! panics, `weights::unreached`); the order of a client's comparison
//! (`iter_core::Cmp`); and the weights of the Fenwick index that only its
//! overflow checks read (`index`'s `ix_weight` and `ix_total`, and
//! `btree`'s `slab_agg`).

#![allow(dead_code)]

use crate::column::Splice;
use crate::delta::DeltaRun;
use crate::leb128_codec::DecodeError;
use crate::raw::RawSlab;
use crate::verified::{Codec, Column, ColumnValueRef};
#[cfg(feature = "bijou64")]
use crate::Bijou64;
use crate::{Leb128, PackError};
#[allow(unused_imports)]
use vstd::prelude::*;
#[allow(unused_imports)]
use vstd::string::*;
#[cfg(verus_keep_ghost)]
use vstd::utf8::valid_utf8;

verus! {

// ── Specifications of the standard library that Verus has none of ──────────

// from src/index.rs
/// `v[start..end] = new`.
#[verifier::external_body]
#[inline(always)]
pub(crate) fn splice_vec<T>(v: &mut Vec<T>, start: usize, end: usize, new: Vec<T>)
    requires
        start <= end <= old(v)@.len(),
    ensures
        final(v)@ == old(v)@.take(start as int) + new@ + old(v)@.skip(end as int),
{
    v.splice(start..end, new);
}

// from src/rle/writer.rs
/// `vec[index..index + del] = bytes`.
#[verifier::external_body]
#[inline(always)]
pub(crate) fn splice_bytes(vec: &mut Vec<u8>, index: usize, del: usize, bytes: &[u8])
    requires index + del <= old(vec).len(),
    ensures final(vec)@ == old(vec)@.take(index as int) + bytes@ + old(vec)@.skip(index + del),
{
    crate::column::splice_bytes(vec, index, del, bytes)
}

// from src/raw.rs
/// `v.drain(start..end)`.
#[verifier::external_body]
#[inline(always)]
pub(crate) fn drain_slabs(v: &mut Vec<RawSlab>, start: usize, end: usize)
    requires
        start <= end <= old(v)@.len(),
    ensures
        final(v)@ == old(v)@.take(start as int) + old(v)@.skip(end as int),
{
    v.drain(start..end);
}

// ── Strings ────────────────────────────────────────────────────────────────

// from src/verified/text.rs
#[verifier::external_type_specification]
#[verifier::external_body]
#[allow(dead_code)]
pub struct ExUtf8Error(core::str::Utf8Error);

// from src/verified/text.rs
/// Trusted: what the standard library's `from_utf8` does. The `str` it
/// gives is of the bytes it was given, it gives one wherever those are
/// valid UTF-8, and it does not panic. (That it gives one only where
/// they are valid UTF-8 is not assumed: the bytes of a `str` are, which
/// vstd proves.)
pub assume_specification<'a>[ core::str::from_utf8 ](v: &'a [u8]) -> (r: Result<
    &'a str,
    core::str::Utf8Error,
>)
    ensures
        match r {
            Ok(s) => s.spec_bytes() == v@,
            Err(_) => !valid_utf8(v@),
        },
    no_unwind
;

// from src/verified/text.rs
/// Trusted: two `str`s of the same characters are one `str`. A `str` is
/// its characters and nothing more, as a slice is its elements (which
/// vstd says, in `axiom_slice_ext_equal`; of `str` it does not).
#[verifier::external_body]
pub(crate) proof fn axiom_str_same(a: &str, b: &str)
    requires
        a@ == b@,
    ensures
        a == b,
{
}

// ── Types Verus is told of, of which nothing is assumed ────────────────────

// from src/rle/model.rs
#[verifier::external_type_specification]
#[verifier::external_body]
#[allow(dead_code)]
pub struct ExPackError(PackError);

// from src/rle/model.rs
#[verifier::external_type_specification]
#[allow(dead_code)]
pub struct ExRun<V>(crate::Run<V>);

// from src/splices.rs
#[verifier::external_type_specification]
#[allow(dead_code)]
pub struct ExSplice(Splice);

// from src/verified/codec.rs
#[verifier::external_type_specification]
#[allow(dead_code)]
pub struct ExLeb128(Leb128);

// from src/verified/bijou.rs
#[cfg(feature = "bijou64")]
#[verifier::external_type_specification]
#[allow(dead_code)]
pub struct ExBijou64(Bijou64);

// from src/verified/delta.rs
/// [`DeltaRun`], as the proofs see it: its three fields.
#[verifier::external_type_specification]
#[allow(dead_code)]
pub struct ExDeltaRun(DeltaRun);

// from src/verified/mod.rs
/// What Verus is told of the standard library's `Extend`: that it is a
/// trait with this method. Of an implementation it assumes nothing: what
/// [`Column`]'s does is stated there, and proved. (Verus's
/// library specifies `IntoIterator` and `FromIterator`, and not this
/// trait.)
#[verifier::external_trait_specification]
#[doc(hidden)]
pub trait ExExtend<A> {
    type ExternalTraitSpecificationFor: core::iter::Extend<A>;

    fn extend<T>(&mut self, iter: T) where T: IntoIterator<Item = A>;
}

// ── Helpers of which nothing is assumed ────────────────────────────────────

// from src/rle/writer.rs
/// Slab metadata arithmetic (lengths, segment counts): unspecified, and
/// computed exactly as before.
#[verifier::external_body]
#[inline(always)]
pub(crate) fn add(a: usize, b: usize) -> usize {
    a + b
}

// from src/bool_edit.rs
/// The rebuild's write buffer, to reuse: nothing is claimed of it, as
/// opening a rebuild takes any buffer. (`mem::take` is what the
/// original did; a swap with a new `Vec` compiles to a slower open.)
#[verifier::external_body]
#[inline(always)]
pub(crate) fn take_buf(v: &mut Vec<u8>) -> Vec<u8> {
    std::mem::take(v)
}

// from src/rle/writer.rs (the body of `Splice::recycle`)
/// Keep `bytes` as the write buffer `buf` if it holds more. Nothing is
/// claimed: a rebuild reopens before it writes again.
#[verifier::external_body]
pub(crate) fn recycle_buf(buf: &mut Vec<u8>, mut bytes: Vec<u8>) {
    if bytes.capacity() > buf.capacity() {
        bytes.clear();
        *buf = bytes;
    }
}

// from src/bool_edit.rs and src/rle/edit.rs (the body of each
// `Slabs::carry_budget`)
/// How many runs a rebuild carries over from a slab it cuts, before it
/// cuts it again. Nothing is claimed of the number.
#[verifier::external_body]
#[inline(always)]
pub(crate) fn carry_budget() -> usize {
    crate::edit::RUN_BUDGET
}

// ── The helpers that panic: each says why, and does not return ─────────────

// from src/column_core.rs
/// Panics: a segment budget below 2, which no column can keep.
#[verifier::external_body]
#[cold]
#[inline(never)]
pub(crate) fn budget_too_small()
    ensures
        false,
{
    panic!("max_segments must be at least 2")
}

// from src/column_core.rs
/// Panics: a column used after a panic in the middle of an edit, which
/// left it without its parts.
#[verifier::external_body]
#[cold]
#[inline(never)]
pub(crate) fn poisoned()
    ensures
        false,
{
    panic!("column used after a panic during an edit")
}

// from src/column_core.rs
/// Panics: a column of `usize::MAX` slabs, which no memory holds (each
/// slab takes some), but which Verus does not rule out.
#[verifier::external_body]
#[cold]
#[inline(never)]
pub(crate) fn column_core_too_many_slabs()
    ensures
        false,
{
    panic!("a column of usize::MAX slabs")
}

// from src/column_core.rs
/// Panics: an edit that left a column's values outside its domain (a
/// delta column's spanning 2^63 or more), which no column can hold. The
/// column is left without its parts.
#[verifier::external_body]
#[cold]
#[inline(never)]
pub(crate) fn column_core_out_of_domain()
    ensures
        false,
{
    panic!("delta values out of domain: they would span 2^63 or more")
}

// from src/column_core.rs
/// Panics: a delta column of more than `i64::MAX` items, whose steps
/// cannot all fit `i64`. The column is left without its parts.
#[verifier::external_body]
#[cold]
#[inline(never)]
pub(crate) fn too_many_deltas()
    ensures
        false,
{
    panic!("a delta column of more than i64::MAX items")
}

// from src/column_core.rs
/// Panics: a column of more items than an edit can count.
#[verifier::external_body]
#[cold]
#[inline(never)]
pub(crate) fn too_many_items()
    ensures
        false,
{
    panic!("a column of more than usize::MAX - 3 items")
}

// from src/column_core.rs
/// Never returns: after a call that does not.
#[verifier::external_body]
#[inline(always)]
pub(crate) fn column_core_unreached<A>() -> A
    requires
        false,
{
    unreachable!()
}

// from src/edit_core.rs
/// Panics: an edit opened past the column's end.
#[verifier::external_body]
#[cold]
#[inline(never)]
pub(crate) fn edit_core_out_of_bounds()
    ensures
        false,
{
    panic!("edit_at out of bounds")
}

// from src/edit_core.rs
/// Panics: an edit that left a delta column's values outside its domain.
#[verifier::external_body]
#[cold]
#[inline(never)]
pub(crate) fn edit_core_out_of_domain()
    ensures
        false,
{
    panic!("delta values out of domain: they would span 2^63 or more")
}

// from src/edit_core.rs
/// Never returns: after a call that does not.
#[verifier::external_body]
#[inline(always)]
pub(crate) fn edit_core_unreached<A>() -> A
    requires
        false,
{
    unreachable!()
}

// from src/cursor.rs
/// An insert would take the column past `usize::MAX - 3` items, which
/// the rebuilds can't count.  Trusted only in that a panic does not
/// return.
#[verifier::external_body]
#[cold]
#[inline(never)]
pub(crate) fn cursor_too_long(Ghost(why): Ghost<bool>)
    requires
        why,
    ensures
        false,
{
    panic!("column too long to edit")
}

// from src/btree.rs
/// Panics: the arena has used every `NodeId`.  Trusted only in that a
/// panic does not return.
#[verifier::external_body]
pub(crate) fn out_of_node_ids()
    ensures false,
{
    panic!("SlabBTree: out of node ids")
}

// from src/bool_enc.rs
/// Appending would take an encoder past `usize::MAX - 3` values, or
/// `usize::MAX` runs, which it can't count.  Trusted only in that a panic does not
/// return.
#[verifier::external_body]
#[cold]
#[inline(never)]
pub(crate) fn bool_enc_too_many_values(Ghost(why): Ghost<bool>)
    requires
        why,
    ensures
        false,
{
    panic!("encoder too long to append to")
}

// from src/rle/writer.rs
/// Panics: a run of `n` bytes, 4 GiB or more, which no tail record holds.
#[verifier::external_body]
#[cold]
#[inline(never)]
pub(crate) fn rle_writer_run_too_long(n: usize)
    ensures
        false,
{
    panic!("a run of {n} bytes is not encodable: a run must stay under 4 GiB")
}

// from src/rle/state.rs
/// Panics: a repeat run longer than `i64::MAX` has no encoding.  Trusted
/// only in that a panic does not return.
#[verifier::external_body]
#[cold]
#[inline(never)]
pub(crate) fn rle_state_run_too_long(count: usize)
    ensures false,
{
    panic!("a run of {count} equal values is not encodable")
}

// from src/rle/state.rs
/// Panics: a literal run of more than `i64::MAX` values has no encoding
/// (nor the memory to hold one).  Trusted only in that a panic does not
/// return.
#[verifier::external_body]
#[cold]
#[inline(never)]
pub(crate) fn lit_too_long()
    ensures false,
{
    panic!("a literal run of more than i64::MAX values is not encodable")
}

// from src/rle/enc.rs
/// Appending would take an encoder past `usize::MAX - 2` values, which
/// it can't count.  Trusted only in that a panic does not return.
#[verifier::external_body]
#[cold]
#[inline(never)]
pub(crate) fn rle_enc_too_many_values(Ghost(why): Ghost<bool>)
    requires
        why,
    ensures
        false,
{
    panic!("encoder too long to append to")
}

// from src/rle/edit.rs
/// A slab too long for the format's byte arithmetic: no slab is, as a
/// slab's bytes are bounded by its segment budget, but the specifications
/// do not say so.
#[cold]
#[inline(never)]
#[verifier::external_body]
pub(crate) fn slab_too_long()
    ensures false,
{
    panic!("RLE slab too long")
}

// from src/rle/edit.rs
/// A null value in a column that isn't nullable: none of the value types
/// has one.  Trusted only in that a panic does not return.
#[verifier::external_body]
#[cold]
#[inline(never)]
pub(crate) fn null_in_column(Ghost(why): Ghost<bool>)
    requires
        why,
    ensures
        false,
{
    panic!("null value in a column that is not nullable")
}

// from src/splices.rs
/// Panics: the splices are out of order or out of bounds.
#[verifier::external_body]
#[cold]
#[inline(never)]
pub(crate) fn bad_splices(why: &str)
    ensures
        false,
{
    panic!("{}", why)
}

// from src/splices.rs
/// Panics: a normalized list used for columns of other lengths than it
/// was made for.
#[verifier::external_body]
#[cold]
#[inline(never)]
pub(crate) fn wrong_columns()
    ensures
        false,
{
    panic!("splices normalized for columns of other lengths")
}

// from src/window.rs
/// Panics: `Shiftable::ranges` was given a range starting behind the
/// iterator.
#[verifier::external_body]
#[cold]
#[inline(never)]
pub(crate) fn ranges_out_of_order()
    ensures
        false,
{
    panic!("ranges must be ascending and non-overlapping")
}

// from src/window.rs
/// Panics: `shift_next` to a range starting behind the iterator.
#[verifier::external_body]
#[cold]
#[inline(never)]
pub(crate) fn shift_backwards()
    ensures
        false,
{
    panic!("assertion failed: range.start >= self.get_pos()")
}

// from src/prefix_core.rs
/// Panics: `take_nth` ran out of values. Never: `n` is within the window.
#[verifier::external_body]
pub(crate) fn prefix_core_past_window()
    requires
        false,
{
    panic!("same_slab_nth called with n > slab len");
}

// from src/prefix_search.rs
/// Panics: one past the end of a column of `usize::MAX` items has no
/// index.
#[verifier::external_body]
pub(crate) fn past_usize()
    ensures
        false,
{
    panic!("get_index_for_prefix: one past a column of usize::MAX items has no index")
}

// from src/delta/diter.rs
/// Panics: `d_take_nth` ran out of values. Never: `n` is within the
/// window.
#[verifier::external_body]
pub(crate) fn diter_past_window()
    requires
        false,
{
    panic!("d_take_nth called with n past the window");
}

// from src/delta/dedit_core.rs
/// Panics: a column of `usize::MAX` slabs, which no memory holds.
#[verifier::external_body]
#[cold]
#[inline(never)]
pub(crate) fn dedit_too_many_slabs()
    ensures
        false,
{
    panic!("a column of usize::MAX slabs")
}

// from src/delta/dedit_core.rs
/// Panics: a delta column of more than `i64::MAX` items.
#[verifier::external_body]
#[cold]
#[inline(never)]
pub(crate) fn dedit_too_long()
    ensures
        false,
{
    panic!("a delta column of more than i64::MAX items")
}

// from src/delta/dedit_core.rs
/// Panics: an edit that left a delta column's values outside its domain.
#[verifier::external_body]
#[cold]
#[inline(never)]
pub(crate) fn dedit_out_of_domain()
    ensures
        false,
{
    panic!("delta values out of domain: they would span 2^63 or more")
}

// from src/delta/dedit_core.rs
/// Panics: a delta edit opened past the column's end.
#[verifier::external_body]
#[cold]
#[inline(never)]
pub(crate) fn dedit_out_of_bounds()
    ensures
        false,
{
    panic!("edit_at out of bounds")
}

// from src/delta/stream.rs
/// Values spanning 2^63 or more: not in the delta domain.  Trusted only
/// in that a panic does not return.
#[verifier::external_body]
#[cold]
#[inline(never)]
pub(crate) fn values_too_wide(Ghost(why): Ghost<bool>)
    requires
        why,
    ensures
        false,
{
    panic!("delta values out of domain: they span 2^63 or more")
}

// from src/raw.rs
/// Panics with `msg`.  Trusted only in that a panic does not return.
/// Every caller proves `why`: the condition its documentation gives for
/// the panic, so none panics otherwise.
#[verifier::external_body]
#[cold]
#[inline(never)]
pub(crate) fn fail(msg: &str, Ghost(why): Ghost<bool>)
    requires
        why,
    ensures
        false,
{
    panic!("{msg}")
}

// from src/raw.rs
#[verifier::external_body]
#[cold]
#[inline(never)]
pub(crate) fn fail_take(n: usize, available: usize, Ghost(why): Ghost<bool>)
    requires
        why,
    ensures
        false,
{
    panic!("RawColumnIter::take would cross slab boundary: want {n}, have {available}")
}

// from src/raw.rs
#[verifier::external_body]
#[cold]
#[inline(never)]
pub(crate) fn fail_seek(pos: usize, target: usize, Ghost(why): Ghost<bool>)
    requires
        why,
    ensures
        false,
{
    panic!("RawColumnIter::seek_to is forward-only (at {pos} want {target})")
}

// from src/raw.rs
/// Panics: a splice out of bounds, or one that would leave the column
/// more than `usize::MAX` bytes long.
#[verifier::external_body]
#[cold]
#[inline(never)]
pub(crate) fn splice_failed(e: PackError)
    ensures
        false,
{
    panic!("splice range out of bounds: {e:?}")
}

// from src/verified/encoder.rs
/// Panics: what an encoder saved is no column that loads. A caller has
/// to show that it is not one a load is sure to take
/// (`Column::loadable`): that the values `vs` save
/// to bytes too long for it, or are more than a column counts. The
/// words are those of the `unwrap` in
/// [`EncoderApi::into_column`](crate::EncoderApi::into_column).
#[verifier::external_body]
#[cold]
#[inline(never)]
pub(crate) fn unloadable<V: ColumnValueRef<C>, C: Codec>(e: PackError, Ghost(vs): Ghost<Seq<V::S>>) -> Column<V, C>
    requires
        Column::<V, C>::encoding(vs).len() > 0xffff_fff5 || vs.len() > usize::MAX - 3,
    ensures
        false,
{
    panic!("called `Result::unwrap()` on an `Err` value: {e:?}")
}

// ── The constructors of errors (`PackError`), of which nothing is assumed ───

// from src/load_core.rs
/// The error a load reports for a column of `len` items where it expected
/// `expected`.
#[verifier::external_body]
#[cold]
pub(crate) fn wrong_length(len: usize, expected: usize) -> PackError {
    PackError::InvalidLength(len, expected)
}

// from src/load_core.rs
/// The error a load reports once an earlier step of it failed.
#[verifier::external_body]
#[cold]
pub(crate) fn load_failed() -> PackError {
    PackError::BadFormat
}

// from src/bool_wire.rs
#[verifier::external_body]
pub(crate) fn bool_wire_bad_format() -> PackError {
    PackError::BadFormat
}

// from src/bool_wire.rs
#[verifier::external_body]
pub(crate) fn zero_count(run_index: usize, value: bool) -> PackError {
    PackError::InvalidValue(format!("run {run_index} (value={value}): zero count in non-first run"))
}

// from src/bool_wire.rs
#[verifier::external_body]
pub(crate) fn trailing_zero(run_index: usize, value: bool) -> PackError {
    PackError::InvalidValue(format!("run {run_index} (value={value}): trailing zero-count run"))
}

// from src/bool_wire.rs
#[verifier::external_body]
pub(crate) fn bool_wire_too_long() -> PackError {
    PackError::InvalidValue("length overflows usize".into())
}

// from src/raw.rs
#[verifier::external_body]
#[cold]
#[inline(never)]
pub(crate) fn splice_oob(index: usize, del: usize, len: usize) -> PackError {
    PackError::InvalidValue(format!("raw splice out of bounds: index={index} del={del} len={len}"))
}

// from src/raw.rs
#[verifier::external_body]
#[cold]
#[inline(never)]
pub(crate) fn splice_too_long(len: usize, del: usize, insert: usize) -> PackError {
    PackError::InvalidValue(format!(
        "raw splice too long: len={len} del={del} insert={insert} overflows usize"
    ))
}

// from src/raw.rs
#[verifier::external_body]
#[cold]
#[inline(never)]
pub(crate) fn get_oob(start: usize, end: usize, len: usize) -> PackError {
    PackError::InvalidValue(format!(
        "raw get out of bounds: range={:?} len={}",
        start..end, len
    ))
}

// from src/raw.rs
#[verifier::external_body]
#[cold]
#[inline(never)]
pub(crate) fn get_cross(start: usize, end: usize, slab: usize, offset: usize, slab_len: usize) -> PackError {
    PackError::InvalidValue(format!(
        "raw get spans slab boundary: range={:?} slab={} offset={} slab_len={}",
        start..end, slab, offset, slab_len,
    ))
}

// from src/rle/cut.rs
/// The error for a slab whose last run its tail record can't hold.
#[verifier::external_body]
#[cold]
#[inline(never)]
pub(crate) fn over_4gib() -> PackError {
    PackError::InvalidValue("a run must stay under 4 GiB".to_string())
}

// from src/rle/read.rs
/// The error for input that decodes but is not canonical.
#[verifier::external_body]
pub(crate) fn invalid(msg: &'static str) -> PackError {
    PackError::InvalidValue(msg.to_string())
}

// from src/weights.rs
/// Panics: a prefix sum that overflows its type.
#[verifier::external_body]
#[cold]
#[inline(never)]
pub(crate) fn sum_overflows()
    ensures
        false,
{
    panic!("prefix sum overflows its type")
}

// from src/weights.rs
/// The error a load reports for a slab whose item count overflows.
#[verifier::external_body]
#[cold]
pub(crate) fn count_overflows() -> PackError {
    PackError::BadFormat
}

// from src/weights.rs
/// The error a load reports for a prefix sum that overflows its type.
#[verifier::external_body]
#[cold]
pub(crate) fn prefix_sum_overflows() -> PackError {
    PackError::InvalidValue("prefix sum overflows its type".into())
}

// from src/weights.rs
/// The error a load reports for a delta running sum that overflows.
#[verifier::external_body]
#[cold]
pub(crate) fn delta_sum_overflows() -> PackError {
    PackError::InvalidValue("delta running sum overflows i64".into())
}

// from src/weights.rs
/// The error a load reports for a delta value outside its type's domain.
#[verifier::external_body]
#[cold]
pub(crate) fn delta_out_of_domain() -> PackError {
    PackError::InvalidValue("delta value out of domain".into())
}

// from src/weights.rs
/// The error a load reports for delta values spanning 2^63 or more.
#[verifier::external_body]
#[cold]
pub(crate) fn delta_too_wide() -> PackError {
    PackError::InvalidValue("delta values out of domain: they span 2^63 or more".into())
}

// from src/weights.rs
/// The error a load reports for a delta column of more than `i64::MAX`
/// items.
#[verifier::external_body]
#[cold]
pub(crate) fn delta_too_long(len: usize) -> PackError {
    PackError::InvalidLength(len, i64::MAX as usize)
}

// from src/verified/bytes.rs
/// The error for bytes that do not start with a byte string.
#[verifier::external_body]
#[cold]
pub(crate) fn bytes_bad_format() -> PackError {
    PackError::BadFormat
}

// from src/verified/codec.rs
/// The error for bytes that do not start with a canonical LEB128 number.
#[verifier::external_body]
#[cold]
pub(crate) fn bad_number(e: DecodeError) -> PackError {
    PackError::InvalidNumber(e)
}

// from src/verified/text.rs
/// The error for bytes that are not UTF-8 where a string should be.
#[verifier::external_body]
#[cold]
pub(crate) fn bad_utf8() -> PackError {
    PackError::InvalidUtf8
}

// from src/verified/value.rs
/// The error of a
/// [`VNewtype::from_base`](crate::verified::VNewtype::from_base) for a
/// base value its type has no value for. Nothing is assumed of it.
#[verifier::external_body]
#[cold]
pub fn refused(msg: &'static str) -> PackError {
    PackError::InvalidValue(msg.into())
}

// from src/verified/mod.rs
/// The error of a
/// [`IterState::try_resume`](crate::verified::IterState::try_resume)
/// on a column that the state does not fit. Nothing is assumed of it.
#[verifier::external_body]
#[cold]
pub(crate) fn invalid_resume() -> PackError {
    PackError::InvalidResume
}

} // verus!
