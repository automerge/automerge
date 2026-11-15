//! Clients of the verified public API, checked by Verus: the API's
//! acceptance test.
//!
//! They know of a column only what the API's specifications say, and
//! hold no assumption of their own (no `assume`, no `admit`, no
//! `external_body`). From that alone Verus proves that a column gives
//! back the values put in, and that it loads from its saved bytes as the
//! same values, which save to the same bytes: for every value type and
//! codec ([`round_trip`]), and so for numbers, for values that may be
//! null, for `bool`, for byte strings, and for a type of the client's own
//! that is stored as a number and is not every number ([`Action`]). And
//! that a column loaded from any bytes at all saves back to those bytes
//! ([`load_then_save`]). And that a clone is a column of its own with
//! the same values ([`clone_then_diverge`]). And that a column made of
//! what iterators give holds what was handed to them
//! ([`collect_then_extend`]), and one made of runs each run's value as
//! often as its count says ([`runs_in`]). And that a saved column loads
//! when its own length is asked for and does not when another is
//! ([`load_with_a_length`]), and that no bytes load as a fill value as
//! often as asked for, or as an error where no column is that long
//! ([`load_a_fill`]).
//!
//! This module exists only while Verus checks the crate.

use super::{
    lemma_run_values_add, lemma_run_values_push, only, refused, run_values, total_len, Get, VCodec,
    VGet, VNewtype, VValue, VerifiedColumn,
};
use crate::{Leb128, LoadOpts, PackError, Run};
use vstd::prelude::*;

verus! {

/// A column of the values `input`: it gives them back, and loads from
/// its saved bytes as the same values, which save to the same bytes.
/// The values are few enough for `load` to be sure to take what they
/// save to.
///
/// The values are in the form a column of `V` takes and gives them in
/// ([`Get`]: for a number, the number), and what they are in a
/// specification is [`VGet::svs`] of them (for numbers, themselves).
#[allow(dead_code)]
fn round_trip<'v, V: VValue<C>, C: VCodec>(input: &Vec<Get<'v, V>>)
    requires
        V::size_bound(VGet::svs(input@)) <= 0xffff_fff5,
        input@.len() <= usize::MAX - 3,
{
    let mut col = VerifiedColumn::<V, C>::new();
    col.splice(0, 0, input.as_slice());
    let out = col.to_vec();
    assert(VGet::svs(out@) == VGet::svs(input@));

    let bytes = col.save();
    let back = VerifiedColumn::<V, C>::load(bytes.as_slice());
    proof {
        // as many values in a specification as were put in
        VGet::lemma_svs(input@);
    }
    assert(back is Ok);
    assert(back->Ok_0@ == VGet::svs(input@));
    // `save` runs, so it is called rather than named in an assertion
    let again = back.unwrap().save();
    assert(again@ == bytes@);
}

/// Numbers.
#[allow(dead_code)]
fn round_trip_u64(input: &Vec<u64>)
    requires
        input@.len() < 1_000_000,
{
    round_trip::<u64, Leb128>(input);
}

/// Values that may be null.
#[allow(dead_code)]
fn round_trip_nullable(input: &Vec<Option<i64>>)
    requires
        input@.len() < 1_000_000,
{
    round_trip::<Option<i64>, Leb128>(input);
}

/// `bool`, whose bound is another.
#[allow(dead_code)]
fn round_trip_bool(input: &Vec<bool>)
    requires
        input@.len() < 1_000_000,
{
    round_trip::<bool, Leb128>(input);
}

/// A value type of the client's own: one of four actions, each stored as
/// a number. Any other number is no action, and bytes that hold one do
/// not load as a column of actions.
#[derive(Clone, Copy)]
#[allow(dead_code)]
pub enum Action {
    Make,
    Set,
    Delete,
    Increment,
}

impl VNewtype for Action {
    type Base = u64;

    open spec fn base(v: Action) -> u64 {
        match v {
            Action::Make => 0,
            Action::Set => 1,
            Action::Delete => 3,
            Action::Increment => 4,
        }
    }

    proof fn lemma_base(v: Action, w: Action) {
    }

    fn to_base(self) -> u64 {
        match self {
            Action::Make => 0,
            Action::Set => 1,
            Action::Delete => 3,
            Action::Increment => 4,
        }
    }

    fn from_base(b: u64) -> Result<Action, PackError> {
        match b {
            0 => Ok(Action::Make),
            1 => Ok(Action::Set),
            3 => Ok(Action::Delete),
            4 => Ok(Action::Increment),
            _ => Err(refused("not an action")),
        }
    }
}

/// The client's own type, and nulls of it.
#[allow(dead_code)]
fn round_trip_action(input: &Vec<Action>, nullable: &Vec<Option<Action>>)
    requires
        input@.len() < 1_000_000,
        nullable@.len() < 1_000_000,
{
    round_trip::<Action, Leb128>(input);
    round_trip::<Option<Action>, Leb128>(nullable);
}

/// What a client reads is what it wrote: an edit, then a read of each
/// value.
#[allow(dead_code)]
fn edit_and_read(a: Action, b: Action) {
    let mut col = VerifiedColumn::<Option<Action>>::new();
    col.push(Some(a));
    col.push(Some(b));
    col.insert(1, None);
    assert(col@ == seq![Some(a), None, Some(b)]);
    let x = col.get(0);
    let y = col.get(1);
    let z = col.get(3);
    assert(x == Some(Some(a)));
    assert(y == Some(None::<Action>));
    assert(z is None);
    col.remove(0);
    assert(col@ == seq![None, Some(b)]);
    let mut it = col.iter();
    let first = it.next();
    let second = it.next();
    let third = it.next();
    assert(first == Some(None::<Action>));
    assert(second == Some(Some(b)));
    assert(third is None);
}

/// The bytes of strings of at most `m` bytes each: a client's own
/// induction over [`total_len`], which it can read.
proof fn lemma_total(vs: Seq<Seq<u8>>, m: nat)
    requires
        forall|i: int| 0 <= i < vs.len() ==> (#[trigger] vs[i]).len() <= m,
    ensures
        total_len(vs) <= m * vs.len(),
    decreases vs.len(),
{
    if vs.len() > 0 {
        lemma_total(vs.drop_last(), m);
        assert(m * (vs.len() - 1) + m == m * vs.len()) by (nonlinear_arith);
    }
}

/// Byte strings. A column takes and gives them as borrowed slices, and
/// what a specification says of a slice is its bytes. A thousand strings
/// of at most a thousand bytes each are few enough for `load`.
#[allow(dead_code)]
fn round_trip_bytes(input: &Vec<&[u8]>)
    requires
        input@.len() <= 1000,
        forall|i: int| 0 <= i < input@.len() ==> (#[trigger] input@[i])@.len() <= 1000,
{
    proof {
        <&[u8] as VGet>::lemma_svs(input@);
        lemma_total(<&[u8] as VGet>::svs(input@), 1000);
    }
    round_trip::<Vec<u8>, Leb128>(input);
}

/// What a client reads of a column of byte strings is the bytes it
/// wrote: from the column, and from one loaded from its saved bytes.
#[allow(dead_code)]
fn bytes_in_bytes_out(a: &[u8], b: &[u8])
    requires
        a@.len() <= 1000,
        b@.len() <= 1000,
{
    let mut col = VerifiedColumn::<Vec<u8>>::new();
    col.push(a);
    col.push(b);
    col.insert(1, a);
    assert(col@ == seq![a@, a@, b@]);
    let x = col.get(2);
    let z = col.get(3);
    assert(x matches Some(v) && v@ == b@);
    assert(z is None);
    let all = col.to_vec();
    proof {
        <&[u8] as VGet>::lemma_svs(all@);
        assert(<&[u8] as VGet>::svs(all@)[1] == a@);
    }
    assert(all@.len() == 3);
    assert(all@[1]@ == a@);

    let bytes = col.save();
    proof {
        lemma_total(col@, 1000);
    }
    let back = VerifiedColumn::<Vec<u8>>::load(bytes.as_slice());
    assert(back is Ok);
    let back = back.unwrap();
    let mut it = back.iter_range(1, 3);
    let first = it.next();
    let second = it.next();
    let third = it.next();
    assert(first matches Some(v) && v@ == a@);
    assert(second matches Some(v) && v@ == b@);
    assert(third is None);
}

/// Byte strings that may be null: the empty string is a value, and is
/// not the null.
#[allow(dead_code)]
fn bytes_or_null(a: &[u8], none: &[u8])
    requires
        none@.len() == 0,
{
    let mut col = VerifiedColumn::<Option<Vec<u8>>>::new();
    col.push(Some(a));
    col.push(None);
    col.push(Some(none));
    assert(col@ == seq![Some(a@), None, Some(none@)]);
    let x = col.get(0);
    let y = col.get(1);
    let z = col.get(2);
    assert(x matches Some(Some(v)) && v@ == a@);
    assert(y matches Some(None));
    assert(z matches Some(Some(v)) && v@.len() == 0);
    col.remove(0);
    col.truncate(1);
    assert(col@ == seq![None::<Seq<u8>>]);
}

/// What loads, saves back to the bytes it was loaded from: no two byte
/// strings load as the same column. For any bytes, of any provenance.
#[allow(dead_code)]
fn load_then_save<V: VValue<C>, C: VCodec>(bytes: &[u8]) {
    if let Ok(col) = VerifiedColumn::<V, C>::load(bytes) {
        proof {
            VerifiedColumn::<V, C>::lemma_encoding(bytes@);
        }
        let again = col.save();
        assert(again@ == bytes@);
    }
}

/// Counting and cutting: a column's length, a range of it, and what is
/// left of it cut short.
#[allow(dead_code)]
fn count_and_cut(input: &Vec<u32>)
    requires
        input@.len() >= 3,
{
    let mut col = VerifiedColumn::<u32>::default();
    let none = col.is_empty();
    assert(none);
    col.splice(0, 0, input.as_slice());
    assert(col@ == input@);
    let n = col.len();
    let none = col.is_empty();
    assert(n == input@.len());
    assert(!none);
    let mut it = col.iter_range(1, 3);
    let x = it.next();
    let y = it.next();
    let z = it.next();
    assert(x == Some(input@[1]));
    assert(y == Some(input@[2]));
    assert(z is None);
    col.truncate(2);
    assert(col@ == input@.take(2));
    col.truncate(5);
    assert(col@ == input@.take(2));
    let mut small = VerifiedColumn::<bool>::with_max_segments(2);
    small.push(true);
    assert(small@ == seq![true]);
}

/// Two columns saved into one buffer, one after the other, as a document
/// holds its columns: each loads from its own range of the buffer as the
/// values it had. For every value type and codec.
#[allow(dead_code)]
fn two_in_one_buffer<'v, V: VValue<C>, C: VCodec>(xs: &Vec<Get<'v, V>>, ys: &Vec<Get<'v, V>>)
    requires
        V::size_bound(VGet::svs(xs@)) <= 0xffff_fff5,
        V::size_bound(VGet::svs(ys@)) <= 0xffff_fff5,
        xs@.len() <= usize::MAX - 3,
        ys@.len() <= usize::MAX - 3,
{
    let mut a = VerifiedColumn::<V, C>::new();
    a.splice(0, 0, xs.as_slice());
    let mut b = VerifiedColumn::<V, C>::new();
    b.splice(0, 0, ys.as_slice());
    proof {
        VGet::lemma_svs(xs@);
        VGet::lemma_svs(ys@);
    }

    let mut out: Vec<u8> = Vec::new();
    out.push(0xff);
    let ra = a.save_to(&mut out);
    let rb = b.save_to(&mut out);
    let ghost ea = VerifiedColumn::<V, C>::encoding(a@);
    let ghost eb = VerifiedColumn::<V, C>::encoding(b@);
    // the buffer is what it held, then the one column, then the other
    assert(out@ == seq![0xffu8] + ea + eb);
    assert(ra.start == 1 && ra.end == rb.start && rb.end == out@.len());

    let da = vstd::slice::slice_subrange(out.as_slice(), ra.start, ra.end);
    let db = vstd::slice::slice_subrange(out.as_slice(), rb.start, rb.end);
    assert(da@ == ea);
    assert(db@ == eb);
    let la = VerifiedColumn::<V, C>::load(da);
    let lb = VerifiedColumn::<V, C>::load(db);
    assert(la is Ok && la->Ok_0@ == VGet::svs(xs@));
    assert(lb is Ok && lb->Ok_0@ == VGet::svs(ys@));
}

/// A column of one value only, and one that is not: what `is_only` says
/// of each, and that a column of the one value alone is not written.
/// For every value type and codec.
#[allow(dead_code)]
fn one_value_only<'v, V: VValue<C>, C: VCodec>(a: Get<'v, V>, b: Get<'v, V>)
    requires
        a.sv() != b.sv(),
{
    let same = V::same(a, b);
    assert(!same);

    let mut col = VerifiedColumn::<V, C>::new();
    // an empty column holds no value that is not `a`
    let none = col.is_only(a);
    assert(none);
    col.push(a);
    col.push(a);
    assert(col@ == seq![a.sv(), a.sv()]);
    let all_a = col.is_only(a);
    let all_b = col.is_only(b);
    assert(all_a);
    assert(!all_b) by {
        assert(col@[0] != b.sv());
    }

    // a column of `a` alone is not written
    let mut out: Vec<u8> = Vec::new();
    out.push(7);
    let r = col.save_to_unless(&mut out, a);
    assert(out@ == seq![7u8]);
    assert(r.start == 1 && r.end == 1);

    // one that holds another value is
    col.push(b);
    let mixed = col.is_only(a);
    assert(!mixed) by {
        assert(col@[2] != a.sv());
    }
    let r = col.save_to_unless(&mut out, a);
    assert(!only(col@, a.sv()));
    assert(out@ == seq![7u8] + VerifiedColumn::<V, C>::encoding(col@));
    assert(r.start == 1 && r.end == out@.len());

    // the other value taken out again
    col.remove_n(2, 1);
    assert(col@ == seq![a.sv(), a.sv()]);
    col.remove_n(1, 0);
    assert(col@ == seq![a.sv(), a.sv()]);
    col.remove_n(0, 2);
    assert(col@.len() == 0);
    col.push(b);
    col.clear();
    assert(col@.len() == 0);
    let _slabs = col.slab_count();
}

/// `is_only` of a client's type, and of byte strings: for those, a null
/// is a value like another, and the empty string is not the null.
#[allow(dead_code)]
fn only_nulls(x: &[u8], none: &[u8])
    requires
        none@.len() == 0,
{
    one_value_only::<Action, Leb128>(Action::Set, Action::Delete);
    one_value_only::<Option<u64>, Leb128>(None, Some(0));
    one_value_only::<bool, Leb128>(false, true);
    one_value_only::<Option<Vec<u8>>, Leb128>(None, Some(none));

    let mut col = VerifiedColumn::<Option<Vec<u8>>>::new();
    col.push(None);
    col.push(None);
    let nulls = col.is_only(None);
    assert(nulls);
    // the unwritten column of a document: all null
    let mut out: Vec<u8> = Vec::new();
    let r = col.save_to_unless(&mut out, None);
    assert(out@.len() == 0 && r.start == r.end);
    let empties = col.is_only(Some(none));
    assert(!empties) by {
        assert(col@[0] != Some(none@));
    }
}

/// A clone is a column of its own with the same values: the two save to
/// the same bytes, and an edit of one leaves the other as it was. For
/// every value type and codec.
#[allow(dead_code)]
fn clone_then_diverge<'v, V: VValue<C>, C: VCodec>(xs: &Vec<Get<'v, V>>, a: Get<'v, V>) {
    let mut col = VerifiedColumn::<V, C>::new();
    col.splice(0, 0, xs.as_slice());
    let ghost vs = VGet::svs(xs@);
    assert(col@ == vs);

    let mut twin = col.clone();
    assert(twin@ == vs);
    // the same values, so the same bytes
    let b = col.save();
    let t = twin.save();
    assert(t@ == b@);

    // an edit of the clone leaves the column as it was
    twin.push(a);
    assert(twin@ == vs.push(a.sv()));
    assert(col@ == vs);

    // and an edit of the column leaves the clone
    col.clear();
    assert(col@.len() == 0);
    assert(twin@ == vs.push(a.sv()));
}

/// A clone of a clone, of byte strings that may be null: each of the
/// three gives what it holds after the others are edited, and a slice
/// read from one is the bytes written to the first.
#[allow(dead_code)]
fn clone_of_clone(x: &[u8]) {
    let mut col = VerifiedColumn::<Option<Vec<u8>>>::new();
    col.push(Some(x));
    col.push(None);
    let mut twin = col.clone();
    let third = twin.clone();
    twin.remove(0);
    col.truncate(1);
    assert(col@ == seq![Some(x@)]);
    assert(twin@ == seq![None::<Seq<u8>>]);
    assert(third@ == seq![Some(x@), None]);
    let from_col = col.get(0);
    let from_twin = twin.get(0);
    let from_third = third.get(0);
    assert(from_col matches Some(Some(v)) && v@ == x@);
    assert(from_twin matches Some(None));
    assert(from_third matches Some(Some(v)) && v@ == x@);

    // an empty column's clone is empty, and a column on its own
    let empty = VerifiedColumn::<bool>::new();
    let mut other = empty.clone();
    other.push(true);
    assert(empty@.len() == 0);
    assert(other@ == seq![true]);
}

/// A column of what iterators give. Collected from a vector's items it
/// holds the vector's values, as the column `from_values` makes of them
/// does, and the two save to the same bytes; extended by a second
/// vector's items it holds the first's values and then the second's; and
/// a third's go in between them. For every value type and codec.
///
/// The client hands vectors over, of which Verus's library says what
/// their iterators give. It proves nothing of an iterator itself.
#[allow(dead_code)]
fn collect_then_extend<'v, V: VValue<C>, C: VCodec>(a: Vec<Get<'v, V>>, b: Vec<Get<'v, V>>, c: Vec<Get<'v, V>>) {
    let ghost (av, bv, cv) = (VGet::svs(a@), VGet::svs(b@), VGet::svs(c@));
    proof {
        VGet::lemma_svs(a@);
    }
    let by_slice = VerifiedColumn::<V, C>::from_values(a.as_slice());
    assert(by_slice@ == av);
    let mut col: VerifiedColumn<V, C> = a.into_iter().collect();
    assert(col@ == av);
    // the same values, so the same bytes
    let x = by_slice.save();
    let y = col.save();
    assert(x@ == y@);

    col.extend(b);
    assert(col@ == av + bv);

    let at = by_slice.len();
    col.splice_iter(at, 0, c);
    assert(col@ == av + cv + bv) by {
        assert((av + bv).take(at as int) =~= av);
        assert((av + bv).skip(at + 0) =~= bv);
    }
}

/// The same of numbers, of byte strings and of byte strings that may be
/// null, whose vectors hold slices.
#[allow(dead_code)]
fn collect_each(a: Vec<u64>, b: Vec<&[u8]>, c: Vec<Option<&[u8]>>, more: Vec<&[u8]>) {
    let ghost bv = VGet::svs(b@);
    let nums: VerifiedColumn<u64> = a.into_iter().collect();
    assert(nums@ == a@);
    let mut strs: VerifiedColumn<Vec<u8>> = b.into_iter().collect();
    assert(strs@ == bv);
    let opts = VerifiedColumn::<Option<Vec<u8>>>::from_iter(c);
    assert(opts@ == VGet::svs(c@));
    strs.extend(more);
    assert(strs@ == bv + VGet::svs(more@));
}

/// How many values runs are: the sum of their counts.
spec fn counted<G>(rs: Seq<Run<G>>) -> nat
    decreases rs.len(),
{
    if rs.len() == 0 {
        0
    } else {
        counted(rs.drop_last()) + rs.last().count as nat
    }
}

/// Runs are as many values as their counts say: a client's own
/// induction over `run_values`.
proof fn lemma_counted<G: VGet>(rs: Seq<Run<G>>)
    ensures
        run_values(rs).len() == counted(rs),
    decreases rs.len(),
{
    if rs.len() > 0 {
        lemma_counted(rs.drop_last());
    }
}

/// A column of one value repeated, and runs spliced into it: each run's
/// value is there as often as its count says, a run of count 0 is no
/// value, and the column is as much longer as the runs' counts are
/// together. For every value type and codec.
#[allow(dead_code)]
fn runs_in<'v, V: VValue<C>, C: VCodec>(a: Get<'v, V>, b: Get<'v, V>, c: Get<'v, V>, more: Vec<Run<Get<'v, V>>>)
    requires
        a.sv() != b.sv(),
{
    let mut col = VerifiedColumn::<V, C>::fill(3, a);
    assert(col@ =~= seq![a.sv(), a.sv(), a.sv()]);
    let all_a = col.is_only(a);
    assert(all_a);
    let empty = VerifiedColumn::<V, C>::fill(0, a);
    assert(empty@.len() == 0);

    let mut runs: Vec<Run<Get<'v, V>>> = Vec::new();
    runs.push(Run { count: 2, value: b });
    runs.push(Run { count: 0, value: c });
    runs.push(Run { count: 1, value: c });
    let ghost rs = runs@;
    proof {
        let none = Seq::<Run<Get<'v, V>>>::empty();
        lemma_run_values_push(none, rs[0]);
        lemma_run_values_push(none.push(rs[0]), rs[1]);
        lemma_run_values_push(none.push(rs[0]).push(rs[1]), rs[2]);
        assert(rs =~= none.push(rs[0]).push(rs[1]).push(rs[2]));
        assert(run_values(rs) =~= seq![b.sv(), b.sv(), c.sv()]);
    }
    // the middle `a` replaced by the runs
    col.splice_runs(1, 1, runs);
    assert(col@ =~= seq![a.sv(), b.sv(), b.sv(), c.sv(), a.sv()]);
    let all_a = col.is_only(a);
    assert(!all_a) by {
        assert(col@[1] != a.sv());
    }
    let second = col.get(2);
    assert(second matches Some(v) && v.sv() == b.sv());

    // a caller's runs, whatever they are, at the front
    let ghost before = col@;
    let ghost n = counted(more@);
    proof {
        lemma_counted(more@);
    }
    col.splice_runs(0, 0, more);
    assert(col@ =~= run_values(more@) + before);
    assert(col@.len() == n + 5);
}

/// Runs of byte strings and of numbers, given as one iterator after
/// another: the values of both, in order.
#[allow(dead_code)]
fn runs_of_slices(x: &[u8], k: usize, a: Vec<Run<u64>>, b: Vec<Run<u64>>) {
    let mut col = VerifiedColumn::<Option<Vec<u8>>>::new();
    let mut runs: Vec<Run<Option<&[u8]>>> = Vec::new();
    runs.push(Run { count: k, value: Some(x) });
    proof {
        lemma_run_values_push(Seq::<Run<Option<&[u8]>>>::empty(), runs@[0]);
        assert(runs@ =~= Seq::<Run<Option<&[u8]>>>::empty().push(runs@[0]));
    }
    col.splice_runs(0, 0, runs);
    assert(col@ =~= Seq::new(k as nat, |_i: int| Some(x@)));
    let all = col.is_only(Some(x));
    assert(all);

    let mut nums = VerifiedColumn::<u64>::new();
    nums.splice_runs(0, 0, a);
    let len = nums.len();
    nums.splice_runs(len, 0, b);
    proof {
        lemma_run_values_add(a@, b@);
    }
    assert(nums@ =~= run_values(a@ + b@));
}

/// A load with options. A saved column loads, with its own length asked
/// for, as the values it had, whatever the fill value and the size of a
/// slab; and with another length asked for it does not load. For every
/// value type and codec.
#[allow(dead_code)]
fn load_with_a_length<'v, V: VValue<C>, C: VCodec>(input: &Vec<Get<'v, V>>, v: Get<'v, V>, other: usize)
    requires
        V::size_bound(VGet::svs(input@)) <= 0xffff_fff5,
        input@.len() <= usize::MAX - 3,
        other != input@.len(),
{
    let mut col = VerifiedColumn::<V, C>::new();
    col.splice(0, 0, input.as_slice());
    let n = col.len();
    let bytes = col.save();
    proof {
        VGet::lemma_svs(input@);
        // no bytes are no values: so these bytes are none if the column
        // is empty, and only then
        VerifiedColumn::<V, C>::lemma_no_bytes();
        if n == 0 {
            assert(col@ =~= Seq::empty());
            VerifiedColumn::<V, C>::lemma_save_unique(bytes@, Seq::empty());
        } else if bytes@.len() == 0 {
            assert(bytes@ =~= Seq::empty());
        }
    }

    let back = VerifiedColumn::<V, C>::load_with(bytes.as_slice(), LoadOpts::new().with_length(n));
    assert(back is Ok && back->Ok_0@ == col@);

    // a fill value fills nothing but an empty column, with no values
    let back = VerifiedColumn::<V, C>::load_with(bytes.as_slice(), LoadOpts::new().with_length(n).with_fill(v));
    assert(back is Ok && back->Ok_0@ =~= col@);

    let back = VerifiedColumn::<V, C>::load_with(bytes.as_slice(), LoadOpts::new().with_length(n).with_max_segments(2));
    assert(back is Ok && back->Ok_0@ == col@);

    let back = VerifiedColumn::<V, C>::load_with(bytes.as_slice(), LoadOpts::new().with_length(other));
    assert(back is Err);
}

/// No bytes, loaded with a length and a fill value: that many of the
/// value, where a column can be that long, and an error where it
/// cannot. With a length and no fill value they load only as the empty
/// column, and with a fill value and no length as that too. For every
/// value type and codec.
#[allow(dead_code)]
fn load_a_fill<'v, V: VValue<C>, C: VCodec>(v: Get<'v, V>, n: usize) {
    let none: Vec<u8> = Vec::new();
    proof {
        VerifiedColumn::<V, C>::lemma_no_bytes();
    }
    let r = VerifiedColumn::<V, C>::load_with(none.as_slice(), LoadOpts::new().with_length(n).with_fill(v));
    if n as u64 <= i64::MAX as u64 && n <= usize::MAX - 3 {
        assert(r is Ok);
        let col = r.unwrap();
        let len = col.len();
        assert(len == n);
        let all = col.is_only(v);
        assert(all);
        if n > 0 {
            let last = col.get(n - 1);
            assert(last matches Some(x) && x.sv() == v.sv());
        }
    } else {
        assert(r is Err);
    }

    let r = VerifiedColumn::<V, C>::load_with(none.as_slice(), LoadOpts::new().with_length(n));
    assert(r is Ok <==> n == 0);

    let r = VerifiedColumn::<V, C>::load_with(none.as_slice(), LoadOpts::new().with_fill(v));
    assert(r is Ok && r->Ok_0@.len() == 0);
}

/// Fills of nulls, of a byte string and of a number, one that is longer
/// than any column, and a load with the options a `LoadOpts` starts
/// with.
#[allow(dead_code)]
fn load_fills(x: &[u8]) {
    let none: Vec<u8> = Vec::new();
    let r = VerifiedColumn::<Option<Vec<u8>>>::load_with(
        none.as_slice(),
        LoadOpts::new().with_length(3).with_fill(None),
    );
    assert(r is Ok && r->Ok_0@ =~= seq![None::<Seq<u8>>, None, None]);

    let r = VerifiedColumn::<Vec<u8>>::load_with(
        none.as_slice(),
        LoadOpts::default().with_fill(x).with_max_segments(4).with_length(2),
    );
    assert(r is Ok && r->Ok_0@ =~= seq![x@, x@]);

    let r = VerifiedColumn::<u64>::load_with(none.as_slice(), LoadOpts::new().with_length(5).with_fill(7));
    assert(r is Ok && r->Ok_0@ =~= seq![7u64, 7, 7, 7, 7]);

    proof {
        VerifiedColumn::<u64>::lemma_no_bytes();
    }
    let r = VerifiedColumn::<u64>::load_with(none.as_slice(), LoadOpts::new().with_length(usize::MAX).with_fill(7));
    assert(r is Err);

    // and with no option, no bytes are a column of no values
    let r = VerifiedColumn::<u64>::load_with(none.as_slice(), LoadOpts::default());
    assert(r is Ok && r->Ok_0@.len() == 0);
}

} // verus!
