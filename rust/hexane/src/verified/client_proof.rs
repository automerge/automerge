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
//! ([`load_a_fill`]). And that the slabs of a column are within it, each
//! starting where the lengths before it add up to, and that a column
//! finds no fault in what it saves ([`slabs_and_validation`],
//! [`load_then_validate`]). And that a search of values in order finds
//! where a value stands among them, so that a column a client keeps in
//! order by inserting each value where the search says stays in order
//! ([`insert_in_order`]): for every value type and codec, and so for
//! numbers in the order of numbers ([`find_number`]), for the client's
//! type in the order of the numbers it is stored as ([`find_action`]),
//! for byte strings in the order of their bytes, nulls first
//! ([`find_bytes`], [`find_by_a_byte`]), and for `false` before `true`
//! and a null before every number ([`find_flags_and_nulls`]). And that
//! the search needs the values in order only as far as the value it
//! looks for can tell ([`find_the_gap`]), and that a client can ask how
//! two values stand in the order ([`later_of`]). And that a remap puts
//! in each place what the client's function gives for the value there,
//! as that function's own specification says it: one that gives its
//! argument back leaves a column of any value type as it was
//! ([`remap_as_it_is`]); a closure may ask of its argument what the
//! client can show of the column's values ([`double_the_small`],
//! [`one_more`]), may be a named function ([`step_the_actions`]), may
//! use what it captured ([`longer_and_emptied`], for byte strings), and
//! may say of its result less than which value it is
//! ([`turned_and_raised`]). And that a column's iterator is one of the
//! standard library's: a `for` loop over it, or over the column, reads
//! the column's values in order ([`read_in_a_loop`], [`all_below`]), two
//! columns can be read side by side with `zip` ([`agree`]), `nth` steps
//! over values and `len` counts those left ([`every_other`],
//! [`the_third`]), a `fold` gives what the client's function makes of
//! the values in turn ([`the_greatest`]), and a copy of an iterator
//! reads on from the same place on its own ([`two_readers`]). And that
//! an iterator knows where in its column it stands and where its window
//! ends, and has left the column's values between the two
//! ([`where_it_stands`], [`window_len`]), and that once it gives
//! nothing its window ends where it stands ([`read_out`]); and that its
//! window moves on to a range of the column, which a `for` loop then
//! reads ([`in_the_range`]), and from one place to another to read the
//! value at each ([`at_two_places`]); and where every move leaves it
//! that goes past what it has ([`moved_past`]). A call says all this of
//! the iterator it makes or moves, so a client need not ask the iterator
//! ([`told_by_the_call`]); and a client that does ask may ask for the
//! window's end alone ([`ends_within`]). And that an iterator gives its
//! values by runs: the first run of a column is of its first value, as
//! often as the column has it there and no more often
//! ([`the_first_run`]); the runs are those of the iterator's window, and
//! each moves the iterator on by its count ([`runs_in_a_window`],
//! [`run_by_run`]); a `for` loop over the runs collects the column's
//! values, in runs none of which is empty and no two of which that
//! follow one another are of one value ([`in_runs`]); the counts of
//! the runs add up to the number of values ([`count_by_runs`]); and the
//! runs of one column, spliced into another, copy it
//! ([`copied_by_runs`]); and a run, and the iterator of runs, say each
//! of these things of the iterator on its own ([`told_by_a_run`],
//! [`told_by_the_runs`], `no_more_than_counted`). And that an iterator
//! finds a value among those it has left. A scan gives the first place
//! that holds the value, or says that none does, and leaves the iterator
//! past that place, or with nothing ([`told_by_a_scan`]): so the first
//! place of a value in a range of a column is found
//! ([`first_place_in`]), and by scan after scan every place of it
//! ([`all_places`]). And a search of values in order gives the range of
//! the places that hold the value, within the range of the column that
//! the client names, and leaves the iterator at its start with the
//! window it had ([`told_by_a_seek`], [`find_in_the_rest`]), whatever
//! range is named: one that ends behind the iterator, or starts past
//! its window, is searched nowhere ([`seek_out_of_range`]). The search
//! is the column's own, and the value found is the one read next
//! ([`seek_then_read`]). And that an iterator can be suspended, given
//! up, and resumed. The state it suspends to stands where it does, and
//! fits the column it reads, whatever moves it has made
//! ([`told_by_a_suspend`], [`of_a_range`], [`after_every_move`]); a
//! state that fits a column is sure to resume on it, and a reader goes
//! on where it stopped ([`stop_and_go_on`]); so a client can keep the
//! readers of its columns between calls as states, and step them call
//! by call ([`step_both`]). An iterator that a state resumes to is one
//! of the column it is resumed on, whatever column the state was taken
//! of, and gives that column's values ([`told_by_a_resume`],
//! [`resumed_on_any`]); the state of an iterator of no column is
//! promised no column ([`of_no_column`]). And that an edit of a column
//! ends what was suspended of it: a column refuses a state taken before
//! a push ([`ended_by_a_push`]), and before any call that deletes a
//! value or inserts one, or a remap ([`ended_by_each_edit`]); before as
//! many edits as a client makes in a loop ([`ended_by_many`]), and
//! before edits that undo each other ([`ended_though_undone`]). Calls
//! that delete no value and insert none are no edits ([`no_edits`],
//! [`cleared`]), and a copy of a column counts its own
//! ([`a_copy_edited`]). Each call that changes a column says what the
//! column counts after it ([`counted_by_a_splice`],
//! [`counted_by_a_splice_iter`], [`counted_by_a_splice_runs`],
//! [`counted_by_an_extend`], [`counted_by_an_insert`],
//! [`counted_by_a_push`], [`counted_by_a_remove`],
//! [`counted_by_a_truncate`], [`counted_by_a_remove_n`],
//! [`counted_by_a_clear`], [`counted_by_a_remap`],
//! [`counted_by_a_clone`]), and what the count is after an edit is
//! there to compute with ([`the_count_after`]).
//!
//! And of a [`VerifiedEncoder`]: that values appended one by one are
//! saved as a column of them, in the one encoding of them
//! ([`encoded_as_saved`]), so that an encoder and a column save the same
//! bytes for the same values ([`encoder_or_column`]), and what an
//! encoder saves loads as a column of the values it was given
//! ([`encode_then_load`]); that a column read into an encoder by its
//! iterator is saved as the column saves itself ([`reencoded`]); that
//! runs given with `append_n` are saved as their values
//! ([`encoded_by_runs`]); that each way an encoder takes a value gives
//! it that value, of numbers and of byte strings, borrowed and owned
//! ([`encode_numbers`], [`encode_strings`]); and that a client can keep
//! encoders between calls, hand them to its functions and write them
//! one after another into one buffer, where each is found again
//! ([`log_both`], [`Rows`], [`rows_written`]). An encoder's functions
//! require of it that it is one (`wf()`), which `new` says and each
//! function keeps: the loops and the signatures of these clients carry
//! it. And each function of an encoder says each of these things on its
//! own ([`told_by_a_new_encoder`], [`told_by_the_count`],
//! [`told_by_an_append`], [`told_by_an_append_n`], [`told_by_an_owned`],
//! [`told_by_owned_n`], [`told_by_an_extend`],
//! [`told_by_an_encoders_save`], [`told_by_an_encoders_save_to`]).
//!
//! And of an encoder's segment budget: that the column of an encoder is
//! the column of the values it was given, with a budget and without,
//! and saves what the encoder saves ([`encoded_into_column`],
//! [`one_column_three_ways`]); that the budget is no part of what is
//! saved ([`a_budget_saves_the_same`]); that a `save_to_unless` writes
//! nothing for an encoder of nothing but the value named, and writes
//! the values where another is among them ([`zeros_take_no_bytes`],
//! [`others_take_bytes`]); and that the range it returns tells a reader
//! who has no encoder which of the two happened, and where the encoding
//! is ([`read_from_the_range`]). Each function says so on its own
//! ([`told_by_a_budget`], [`told_by_no_budget`], [`told_of_a_budget`],
//! [`told_by_an_encoders_save_to_unless`],
//! [`told_by_an_encoders_column`]).
//!
//! And of an encoder's remap forms: that a function which gives each
//! value back saves what `save_to` saves ([`saved_as_it_is`]); that
//! numbers are doubled as they are saved by a function that takes
//! only the numbers given, and load as the doubles
//! ([`doubled_as_saved`]); that the client's own type goes through a
//! function that has a name ([`stepped_as_saved`]); that `bool`s and
//! byte strings go through, two encoders into one buffer, each found
//! again by its range ([`turned_and_longer`]); that where the function
//! can give only the value named, nothing is written, whatever the
//! encoder's budget ([`zeroed_takes_no_bytes`],
//! [`nulled_takes_no_bytes`]), and where it gives each value back and
//! one is another value, they are written ([`kept_and_written`]); and
//! that a reader with the range alone knows which happened
//! ([`read_from_the_remapped_range`]). Each of the two says so on its
//! own ([`told_by_a_remapping_save`],
//! [`told_by_a_remapping_save_unless`]).
//!
//! And of a [`VerifiedEncoderState`], which writes into a buffer of its
//! caller's as the values come: that values given one by one are
//! written as a column of them, after what the buffer held
//! ([`streamed_as_saved`]); that a state, an encoder, a column and
//! `encode` give the same bytes for the same values
//! ([`one_encoding_four_ways`]), which load as a column of them
//! ([`stream_then_load`]); that byte strings go through, borrowed and
//! owned, two states one after the other into one buffer
//! ([`strings_streamed`]); that a state knows whether all it was given
//! is one value, and counts what it was given ([`a_single_run`]); that
//! a client can keep a state with its buffer between calls ([`Log`],
//! [`logged`]); and that a client can give up what a state wrote by
//! cutting the buffer back, and so build its own `encode_to_unless`
//! ([`given_up`], [`written_unless_one`]). A state's functions require
//! of the buffer that it is the state's (`on`), which `new` says of any
//! buffer and each function keeps: the loops and the types of these
//! clients carry it. And each function of a state says each of these
//! things on its own ([`told_by_a_new_state`],
//! [`told_by_a_states_count`], [`told_by_a_states_append`],
//! [`told_by_a_states_append_n`], [`told_by_a_states_owned`],
//! [`told_by_a_states_extend`], [`told_by_a_single_run`],
//! [`told_by_a_finish`], [`told_by_the_bytes_before`]).
//!
//! And of the static forms: that `encode_to` writes two columns into
//! one buffer, each found again by its range
//! ([`two_columns_one_buffer`]); that `encode_to_unless` leaves the
//! buffer for values that are all the one named, as nulls are, or no
//! values at all ([`all_the_one_leave_the_buffer`],
//! [`nulls_leave_the_buffer`]), and writes them where another is among
//! them ([`another_is_written`]); and that the range it returns tells a
//! reader which of the two happened ([`read_from_the_encoded_range`]).
//! Each says so on its own ([`told_by_an_encode`],
//! [`told_by_an_encode_to`], [`told_by_an_encode_to_unless`]).
//!
//! And of strings, which a column takes and gives as `&str` and of
//! which a specification says the characters: that a column gives back
//! what was put in and loads from what it saves ([`round_trip_text`],
//! [`text_in_text_out`], [`load_text`]), that a string read is the
//! string written ([`the_string_itself`]), for strings the client wrote
//! out ([`fruit`]) and for one whose length it finds out as it runs
//! ([`saved_if_short`]); that strings are in the order of their UTF-8
//! bytes, in which the search finds them and a client can ask how two
//! stand ([`find_text`], [`find_fruit`], [`text_compared`]); that a
//! remap is given a `String` of its own ([`exclaimed`]); and that an
//! encoder and a state take strings borrowed and owned
//! ([`encode_text`], [`text_streamed`]).
//!
//! This module exists only while Verus checks the crate.

use super::{
    after_edit, after_edits, below_at, bytes_below, folded, head_run, lemma_after_edits,
    lemma_run_values_add, lemma_run_values_push, lemma_runs_first, lemma_runs_of_values,
    lemma_sorted, mapped, only, refused, run_values, runs_of, sorted, sorted_for, sum_of,
    total_len, total_str_len, window, Get, VCodec, VGet, VNewtype, VSpec, VValue, VerifiedColumn,
    VerifiedEncoder, VerifiedEncoderState, VerifiedIter, VerifiedIterState,
};
use crate::{Leb128, LoadOpts, PackError, Run};
use std::cmp::Ordering;
use std::ops::Range;
use vstd::prelude::*;
#[cfg(verus_keep_ghost)]
use vstd::utf8::{encode_utf8, is_ascii_chars_encode_utf8};

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

/// Of lengths added up ([`sum_of`]): the first `i + 1` are the first `i`
/// and one more, and are no more than all of them. A client proves it
/// from the definition.
proof fn lemma_sum_of(lens: Seq<usize>, i: int)
    requires
        0 <= i < lens.len(),
    ensures
        sum_of(lens.take(i + 1)) == sum_of(lens.take(i)) + lens[i],
        sum_of(lens.take(i + 1)) <= sum_of(lens),
    decreases lens.len(),
{
    assert(lens.take(i + 1).drop_last() =~= lens.take(i));
    if i + 1 == lens.len() {
        assert(lens.take(i + 1) =~= lens);
    } else {
        lemma_sum_of(lens.drop_last(), i);
        assert(lens.drop_last().take(i + 1) =~= lens.take(i + 1));
    }
}

/// The slabs of a column are within it. A client adds up their lengths
/// to find where each slab starts, as `dump_slabs` prints it: the sum is
/// never more than a `usize` counts, every slab ends within the column,
/// and the sum of all is the column's length. And a column whose values
/// save to less than 4 GiB finds no fault in what it saves. For every
/// value type and codec.
#[allow(dead_code)]
fn slabs_and_validation<'v, V: VValue<C>, C: VCodec>(input: &Vec<Get<'v, V>>)
    requires
        V::size_bound(VGet::svs(input@)) <= u32::MAX,
{
    let mut col = VerifiedColumn::<V, C>::new();
    col.splice(0, 0, input.as_slice());
    proof {
        VGet::lemma_svs(input@);
    }
    assert(col@ =~= VGet::svs(input@));

    let n = col.len();
    let lens = col.slab_lens();
    let mut offset: usize = 0;
    let mut i: usize = 0;
    while i < lens.len()
        invariant
            i <= lens@.len(),
            offset == sum_of(lens@.take(i as int)),
            sum_of(lens@) == n,
        decreases lens@.len() - i,
    {
        proof {
            lemma_sum_of(lens@, i as int);
        }
        // the slab ends within the column
        assert(offset + lens@[i as int] <= n);
        offset = offset + lens[i];
        i += 1;
    }
    assert(lens@.take(i as int) =~= lens@);
    assert(offset == n && n == input@.len());

    let r = col.validate_encoding();
    assert(r is Ok);
}

/// The slabs of numbers and of `bool`s.
#[allow(dead_code)]
fn slabs_of_each(a: &Vec<u64>, b: &Vec<bool>)
    requires
        a@.len() < 1_000_000,
        b@.len() < 1_000_000,
{
    slabs_and_validation::<u64, Leb128>(a);
    slabs_and_validation::<bool, Leb128>(b);
}

/// A column loaded from less than 4 GiB of bytes finds no fault in what
/// it saves, which is those bytes. For any bytes, of any provenance.
#[allow(dead_code)]
fn load_then_validate<V: VValue<C>, C: VCodec>(bytes: &[u8])
    requires
        bytes@.len() <= u32::MAX,
{
    if let Ok(col) = VerifiedColumn::<V, C>::load(bytes) {
        proof {
            VerifiedColumn::<V, C>::lemma_encoding(bytes@);
        }
        let r = col.validate_encoding();
        assert(r is Ok);
    }
}

/// How two values stand in the order is a thing a client can ask
/// ([`VValue::compare`]): the later of two is one of them, and neither
/// comes after it. For every value type and codec.
#[allow(dead_code)]
fn later_of<'v, V: VValue<C>, C: VCodec>(a: Get<'v, V>, b: Get<'v, V>) -> (r: Get<'v, V>)
    ensures
        r.sv() == a.sv() || r.sv() == b.sv(),
        !V::below(r.sv(), a.sv()),
        !V::below(r.sv(), b.sv()),
{
    proof {
        V::lemma_below(a.sv(), b.sv(), a.sv());
        V::lemma_below(b.sv(), b.sv(), b.sv());
    }
    match V::compare(a, b) {
        Ordering::Less => b,
        _ => a,
    }
}

/// A column a client keeps in order. It asks where a value stands among
/// values in order and inserts it there, after those equal to it: the
/// column is then the old one with the value at that place, and is in
/// order still. For every value type and codec, from the laws of the
/// order alone ([`VSpec::lemma_below`]).
#[allow(dead_code)]
fn insert_in_order<'v, V: VValue<C>, C: VCodec>(col: &mut VerifiedColumn<V, C>, x: Get<'v, V>) -> (at:
    usize)
    requires
        sorted::<V>(old(col)@),
    ensures
        at <= old(col)@.len(),
        final(col)@ == old(col)@.insert(at as int, x.sv()),
        sorted::<V>(final(col)@),
{
    let n = col.len();
    let ghost before = col@;
    let ghost t = x.sv();
    proof {
        lemma_sorted::<V>(before, t);
        assert(before.subrange(0, n as int) =~= before);
    }
    let r = col.scope_to_value(x, 0, n);
    let at = r.end;
    col.insert(at, x);
    proof {
        let after = col@;
        assert forall|i: int, j: int| 0 <= i <= j < after.len() implies !V::below(
            #[trigger] after[j],
            #[trigger] after[i],
        ) by {
            // what was at `k` before: the search says how it stands to `t`
            let was = |k: int| before[k];
            if i == at && j == at {
                V::lemma_below(t, t, t);
            } else if j == at {
                // before the place: not after `t`
                assert(after[i] == was(i));
            } else if i == at {
                // from the place on: after `t`, so not before it
                assert(after[j] == was(j - 1));
                V::lemma_below(t, was(j - 1), t);
            } else {
                // both were there before, in this order
                let a = if i < at { i } else { i - 1 };
                let b = if j < at { j } else { j - 1 };
                assert(after[i] == before[a] && after[j] == before[b]);
            }
        }
    }
    at
}

/// Numbers in order as a client says it of numbers, with `<=`: the
/// search gives the places of `x`, with the lesser numbers before them
/// and the greater ones after. A client says so with `<` and `>`: what
/// the order of a number type is can be read ([`VSpec::below`]).
#[allow(dead_code)]
fn find_number(col: &VerifiedColumn<u32>, x: u32) -> (r: Range<usize>)
    requires
        forall|i: int, j: int| 0 <= i <= j < col@.len() ==> col@[i] <= col@[j],
    ensures
        r.start <= r.end <= col@.len(),
        forall|k: int| 0 <= k < r.start ==> col@[k] < x,
        forall|k: int| r.start <= k < r.end ==> col@[k] == x,
        forall|k: int| r.end <= k < col@.len() ==> col@[k] > x,
{
    let n = col.len();
    proof {
        assert(sorted::<u32>(col@));
        lemma_sorted::<u32>(col@, x);
        assert(col@.subrange(0, n as int) =~= col@);
    }
    col.scope_to_value(x, 0, n)
}

/// The client's own type is in the order of the numbers it is stored
/// as, which is the client's to choose ([`VNewtype::base`]): `Set` (1)
/// is before `Delete` (3). In a column of some `Set`s and then some
/// `Delete`s, the search for `Delete` gives the `Delete`s; that for
/// `Increment` (4), which is after both, the empty range at the end;
/// and a search of the `Delete`s alone for `Make` (0), the empty range
/// where they start.
#[allow(dead_code)]
fn find_action(sets: usize, deletes: usize)
    requires
        sets + deletes <= 1000,
{
    let mut col = VerifiedColumn::<Action>::new();
    let mut i: usize = 0;
    while i < sets
        invariant
            i <= sets,
            col@.len() == i,
            only(col@, Action::Set),
        decreases sets - i,
    {
        col.push(Action::Set);
        i += 1;
    }
    let mut j: usize = 0;
    while j < deletes
        invariant
            j <= deletes,
            col@.len() == sets + j,
            forall|k: int| 0 <= k < sets ==> col@[k] == Action::Set,
            forall|k: int| sets <= k < sets + j ==> col@[k] == Action::Delete,
        decreases deletes - j,
    {
        col.push(Action::Delete);
        j += 1;
    }
    let n = col.len();
    proof {
        assert(sorted::<Action>(col@));
        lemma_sorted::<Action>(col@, Action::Delete);
        lemma_sorted::<Action>(col@, Action::Increment);
        assert(col@.subrange(0, n as int) =~= col@);
    }
    let r = col.scope_to_value(Action::Delete, 0, n);
    proof {
        if sets > 0 {
            assert(col@[sets - 1] == Action::Set);
        }
        if deletes > 0 {
            assert(col@[sets as int] == Action::Delete);
            assert(col@[n - 1] == Action::Delete);
        }
    }
    assert(r.start == sets && r.end == n);

    let r = col.scope_to_value(Action::Increment, 0, n);
    proof {
        if n > 0 {
            assert(col@[n - 1] == Action::Set || col@[n - 1] == Action::Delete);
        }
    }
    assert(r.start == n && r.end == n);

    // a part of the column: its values need be in order, no others
    proof {
        let part = col@.subrange(sets as int, n as int);
        assert(sorted::<Action>(part));
        lemma_sorted::<Action>(part, Action::Make);
    }
    let r = col.scope_to_value(Action::Make, sets, n);
    proof {
        if deletes > 0 {
            assert(col@[sets as int] == Action::Delete);
        }
    }
    assert(r.start == sets && r.end == sets);
}

/// Byte strings are in the order of their bytes, a string before every
/// longer one that starts with it ([`bytes_below`]), and the null is
/// before them all. So a null, a string and a longer one that starts
/// with it are in order, and the search finds each at its place: the
/// client proves the order from its definition and its laws, which it
/// can read.
#[allow(dead_code)]
fn find_bytes(a: &[u8], b: &[u8])
    requires
        a@.len() < b@.len(),
        b@.take(a@.len() as int) == a@,
{
    let mut col = VerifiedColumn::<Option<Vec<u8>>>::new();
    col.push(None);
    col.push(Some(a));
    col.push(Some(b));
    assert(col@ == seq![None, Some(a@), Some(b@)]);
    proof {
        // `a` is before `b`: all of it is how `b` starts
        assert(a@.take(a@.len() as int) =~= a@);
        assert(below_at(a@, b@, a@.len() as int));
        assert(bytes_below(a@, b@));
        // so `b` is not before `a`, and neither is before itself
        <Vec<u8> as VSpec>::lemma_below(a@, b@, a@);
        <Vec<u8> as VSpec>::lemma_below(b@, b@, b@);
        assert(sorted::<Option<Vec<u8>>>(col@));
        lemma_sorted::<Option<Vec<u8>>>(col@, Some(a@));
        lemma_sorted::<Option<Vec<u8>>>(col@, None);
        lemma_sorted::<Option<Vec<u8>>>(col@, Some(b@));
        assert(col@.subrange(0, 3) =~= col@);
        assert(col@[0] is None && col@[1] == Some(a@) && col@[2] == Some(b@));
    }
    let r = col.scope_to_value(Some(a), 0, 3);
    assert(r.start == 1 && r.end == 2);
    let r = col.scope_to_value(None, 0, 3);
    assert(r.start == 0 && r.end == 1);
    let r = col.scope_to_value(Some(b), 0, 3);
    assert(r.start == 2 && r.end == 3);
}

/// Of two byte strings that are the same up to a place where one has
/// the lower byte, that one is first, whatever their lengths and
/// whatever comes after ([`below_at`]): the search finds each at its
/// place.
#[allow(dead_code)]
fn find_by_a_byte(a: &[u8], b: &[u8], i: usize)
    requires
        i < a@.len(),
        i < b@.len(),
        a@.take(i as int) == b@.take(i as int),
        a@[i as int] < b@[i as int],
{
    let mut col = VerifiedColumn::<Vec<u8>>::new();
    col.push(a);
    col.push(b);
    assert(col@ == seq![a@, b@]);
    proof {
        assert(below_at(a@, b@, i as int));
        assert(bytes_below(a@, b@));
        <Vec<u8> as VSpec>::lemma_below(a@, b@, a@);
        <Vec<u8> as VSpec>::lemma_below(b@, b@, b@);
        assert(sorted::<Vec<u8>>(col@));
        lemma_sorted::<Vec<u8>>(col@, a@);
        lemma_sorted::<Vec<u8>>(col@, b@);
        assert(col@.subrange(0, 2) =~= col@);
        assert(col@[0] == a@ && col@[1] == b@);
    }
    let r = col.scope_to_value(a, 0, 2);
    assert(r.start == 0 && r.end == 1);
    let r = col.scope_to_value(b, 0, 2);
    assert(r.start == 1 && r.end == 2);
}

/// `false` is before `true`, and a null before every number: in a
/// column of some `false`s and then some `true`s the search for `true`
/// gives the `true`s, and in one of nulls and then a number the search
/// for the null gives the nulls.
#[allow(dead_code)]
fn find_flags_and_nulls(falses: usize, trues: usize, x: u32)
    requires
        falses + trues <= 1000,
{
    let mut col = VerifiedColumn::<bool>::new();
    let mut i: usize = 0;
    while i < falses + trues
        invariant
            i <= falses + trues <= 1000,
            col@.len() == i,
            forall|k: int| 0 <= k < i ==> col@[k] == (k >= falses),
        decreases falses + trues - i,
    {
        col.push(i >= falses);
        i += 1;
    }
    let n = col.len();
    proof {
        assert(sorted::<bool>(col@));
        lemma_sorted::<bool>(col@, true);
        assert(col@.subrange(0, n as int) =~= col@);
    }
    let r = col.scope_to_value(true, 0, n);
    proof {
        if falses > 0 {
            assert(col@[falses - 1] == false);
        }
        if trues > 0 {
            assert(col@[falses as int] == true);
            assert(col@[n - 1] == true);
        }
    }
    assert(r.start == falses && r.end == n);

    let mut col = VerifiedColumn::<Option<u32>>::new();
    col.push(None);
    col.push(None);
    col.push(Some(x));
    assert(col@ == seq![None, None, Some(x)]);
    proof {
        assert(sorted::<Option<u32>>(col@));
        lemma_sorted::<Option<u32>>(col@, None);
        lemma_sorted::<Option<u32>>(col@, Some(x));
        assert(col@.subrange(0, 3) =~= col@);
        assert(col@[0] is None && col@[1] is None && col@[2] == Some(x));
    }
    let r = col.scope_to_value(None, 0, 3);
    assert(r.start == 0 && r.end == 2);
    let r = col.scope_to_value(Some(x), 0, 3);
    assert(r.start == 2 && r.end == 3);
}

/// The search needs the values in order only as far as the value it
/// looks for can tell ([`sorted_for`]). Of numbers of which the first
/// `m` are less than `x` and the others greater, in whatever order
/// among themselves, it gives the empty range at `m`.
#[allow(dead_code)]
fn find_the_gap(col: &VerifiedColumn<i64>, x: i64, m: usize) -> (r: Range<usize>)
    requires
        m <= col@.len(),
        forall|k: int| 0 <= k < m ==> col@[k] < x,
        forall|k: int| m <= k < col@.len() ==> col@[k] > x,
    ensures
        r.start == m && r.end == m,
{
    let n = col.len();
    proof {
        assert(col@.subrange(0, n as int) =~= col@);
        assert(sorted_for::<i64>(col@, x));
    }
    let r = col.scope_to_value(x, 0, n);
    proof {
        if m > 0 {
            assert(col@[m - 1] < x);
        }
        if m < n {
            assert(col@[m as int] > x);
        }
    }
    r
}

/// A remap by a function that gives each value back leaves a column as
/// it was: for every value type and codec. The client knows of the
/// function only what it wrote as its `ensures`, and of the column only
/// that each place holds what the function gave for the value that was
/// there (`mapped`).
#[allow(dead_code)]
fn remap_as_it_is<V: VValue<C>, C: VCodec>(col: &mut VerifiedColumn<V, C>)
    ensures
        final(col)@ == old(col)@,
{
    let f = |v: V| -> (w: V)
        ensures
            w == v,
        { v };
    col.remap(f);
    assert(col@ =~= old(col)@);
}

/// Numbers doubled, by a function that does not take every number: the
/// client has to show that it takes those of the column, and no more.
#[allow(dead_code)]
fn double_the_small(col: &mut VerifiedColumn<u64>)
    requires
        forall|i: int| 0 <= i < old(col)@.len() ==> old(col)@[i] <= 1000,
    ensures
        final(col)@.len() == old(col)@.len(),
        forall|i: int| 0 <= i < old(col)@.len() ==> final(col)@[i] == 2 * old(col)@[i],
{
    let f = |v: u64| -> (w: u64)
        requires
            v <= 1000,
        ensures
            w == 2 * v,
        { 2 * v };
    col.remap(f);
    // the column is a column still
    col.push(7);
    let last = col.get(col.len() - 1);
    assert(last == Some(7u64));
    col.remove(col.len() - 1);
    assert(col@ =~= col@.push(7).drop_last());
}

/// The action after an action.
pub open spec fn after(a: Action) -> Action {
    match a {
        Action::Make => Action::Set,
        Action::Set => Action::Delete,
        Action::Delete => Action::Increment,
        Action::Increment => Action::Make,
    }
}

#[allow(dead_code)]
fn next_action(a: Action) -> (b: Action)
    ensures
        b == after(a),
{
    match a {
        Action::Make => Action::Set,
        Action::Set => Action::Delete,
        Action::Delete => Action::Increment,
        Action::Increment => Action::Make,
    }
}

/// The client's own type, remapped by a function that has a name; and
/// nulls of it, which the function leaves.
#[allow(dead_code)]
fn step_the_actions(col: &mut VerifiedColumn<Action>, nullable: &mut VerifiedColumn<Option<Action>>)
    ensures
        final(col)@.len() == old(col)@.len(),
        forall|i: int| 0 <= i < old(col)@.len() ==> final(col)@[i] == after(old(col)@[i]),
        final(nullable)@.len() == old(nullable)@.len(),
        forall|i: int|
            0 <= i < old(nullable)@.len() ==> final(nullable)@[i] == match old(nullable)@[i] {
                Some(a) => Some(after(a)),
                None => None,
            },
{
    col.remap(next_action);
    let f = |v: Option<Action>| -> (w: Option<Action>)
        ensures
            w == match v {
                Some(a) => Some(after(a)),
                None => None,
            },
        {
            match v {
                Some(a) => Some(next_action(a)),
                None => None,
            }
        };
    nullable.remap(f);
}

/// Byte strings, each with one more byte at its end; and byte strings
/// or nulls, of which the nulls stay and the strings are emptied. The
/// function is given a string of its own, and may hand back that one.
#[allow(dead_code)]
fn longer_and_emptied(col: &mut VerifiedColumn<Vec<u8>>, nullable: &mut VerifiedColumn<Option<Vec<u8>>>, b: u8)
    ensures
        final(col)@.len() == old(col)@.len(),
        forall|i: int| 0 <= i < old(col)@.len() ==> final(col)@[i] == old(col)@[i].push(b),
        final(nullable)@.len() == old(nullable)@.len(),
        forall|i: int|
            0 <= i < old(nullable)@.len() ==> final(nullable)@[i] == match old(nullable)@[i] {
                Some(_) => Some(Seq::<u8>::empty()),
                None => None,
            },
{
    let f = |v: Vec<u8>| -> (w: Vec<u8>)
        ensures
            w@ == v@.push(b),
        {
            let mut v = v;
            v.push(b);
            v
        };
    col.remap(f);
    let g = |v: Option<Vec<u8>>| -> (w: Option<Vec<u8>>)
        ensures
            v is None ==> w is None,
            v is Some ==> w is Some && w->0@ == Seq::<u8>::empty(),
        {
            match v {
                Some(_) => Some(Vec::new()),
                None => None,
            }
        };
    nullable.remap(g);
    assert forall|i: int| 0 <= i < old(nullable)@.len() implies nullable@[i] == match old(nullable)@[i] {
        Some(_) => Some(Seq::<u8>::empty()),
        None => None,
    } by {
        assert(mapped(g, old(nullable)@[i], nullable@[i]));
    }
}

/// `bool`s turned over; and numbers by a function of which the client
/// says only that it gives no less than it is given, so that it knows
/// only that of the column.
#[allow(dead_code)]
fn turned_and_raised(flags: &mut VerifiedColumn<bool>, col: &mut VerifiedColumn<i64>, floor: i64)
    ensures
        final(flags)@.len() == old(flags)@.len(),
        forall|i: int| 0 <= i < old(flags)@.len() ==> final(flags)@[i] == !old(flags)@[i],
        final(col)@.len() == old(col)@.len(),
        forall|i: int| 0 <= i < old(col)@.len() ==> final(col)@[i] >= old(col)@[i] && final(col)@[i] >= floor,
{
    let f = |v: bool| -> (w: bool)
        ensures
            w == !v,
        { !v };
    flags.remap(f);
    let g = |v: i64| -> (w: i64)
        ensures
            w >= v,
            w >= floor,
        {
            if v < floor {
                floor
            } else {
                v
            }
        };
    col.remap(g);
}

/// Numbers or nulls: a function that takes every value but the greatest
/// number, of a column that the client knows to be without it.
#[allow(dead_code)]
fn one_more(col: &mut VerifiedColumn<Option<u64>>)
    requires
        forall|i: int| 0 <= i < old(col)@.len() ==> old(col)@[i] != Some(u64::MAX),
    ensures
        final(col)@.len() == old(col)@.len(),
        forall|i: int|
            0 <= i < old(col)@.len() ==> final(col)@[i] == match old(col)@[i] {
                Some(x) => Some((x + 1) as u64),
                None => None,
            },
{
    let f = |v: Option<u64>| -> (w: Option<u64>)
        requires
            v != Some(u64::MAX),
        ensures
            w == match v {
                Some(x) => Some((x + 1) as u64),
                None => None,
            },
        {
            match v {
                Some(x) => Some(x + 1),
                None => None,
            }
        };
    col.remap(f);
}

/// A `for` loop over a column's iterator gives the column's values, in
/// order: here each goes into a `Vec`, which then holds what the column
/// does. For every value type and codec.
#[allow(dead_code)]
fn read_in_a_loop<'a, V: VValue<C>, C: VCodec>(col: &'a VerifiedColumn<V, C>) -> (out: Vec<Get<'a, V>>)
    ensures
        <Get<'a, V> as VGet>::svs(out@) == col@,
{
    let it = col.iter();
    let ghost all = it.items();
    let mut out: Vec<Get<'a, V>> = Vec::new();
    for x in at: it
        invariant
            at.seq() == all,
            out@ == all.take(at.index()),
    {
        out.push(x);
        proof {
            assert(all.take(at.index() + 1) =~= all.take(at.index()).push(x));
        }
    }
    proof {
        assert(all.take(all.len() as int) =~= all);
    }
    out
}

/// A `for` loop over a column itself: whether every number of it is
/// below `m`.
#[allow(dead_code)]
fn all_below(col: &VerifiedColumn<u32>, m: u32) -> (r: bool)
    ensures
        r == forall|i: int| 0 <= i < col@.len() ==> col@[i] < m,
{
    let mut r = true;
    for x in at: col
        invariant
            at.seq() == col@,
            r == forall|i: int| 0 <= i < at.index() ==> col@[i] < m,
    {
        if x >= m {
            r = false;
        }
    }
    r
}

/// Two columns read side by side, with the standard library's `zip`:
/// whether they hold the same numbers.
#[allow(dead_code)]
fn agree(a: &VerifiedColumn<u64>, b: &VerifiedColumn<u64>) -> (r: bool)
    requires
        a@.len() == b@.len(),
    ensures
        r == (a@ == b@),
{
    let mut same = true;
    for pair in at: a.iter().zip(b.iter())
        invariant
            a@.len() == b@.len(),
            at.seq() == a@.zip_with(b@),
            same == forall|i: int| 0 <= i < at.index() ==> a@[i] == b@[i],
    {
        if pair.0 != pair.1 {
            same = false;
        }
    }
    proof {
        if same {
            assert(a@ =~= b@);
        }
    }
    same
}

/// Steps over values: `nth` gives the value that many places on and
/// leaves the iterator behind it, and an iterator knows how many values
/// it has left.
#[allow(dead_code)]
fn every_other(col: &VerifiedColumn<i64>)
    requires
        col@.len() == 5,
{
    let mut it = col.iter();
    let n = it.len();
    assert(n == 5);
    let a = it.nth(1);
    assert(a == Some(col@[1]));
    let h = it.size_hint();
    assert(h == (3usize, Some(3usize)));
    let b = it.nth(1);
    assert(b == Some(col@[3]));
    let c = it.next();
    assert(c == Some(col@[4]));
    let d = it.nth(0);
    assert(d is None);
    let m = it.len();
    assert(m == 0);
}

/// The value two places on, if a column is that long: for every value
/// type and codec.
#[allow(dead_code)]
fn the_third<'a, V: VValue<C>, C: VCodec>(col: &'a VerifiedColumn<V, C>) -> (r: Option<Get<'a, V>>)
    ensures
        match r {
            Some(v) => col@.len() > 2 && v.sv() == col@[2],
            None => col@.len() <= 2,
        },
{
    let mut it = col.iter();
    let n = it.len();
    assert(n == col@.len());
    it.nth(2)
}

/// What a fold makes of numbers with a function that gives back no less
/// than it was given is no less than any of them: a client's own
/// induction over [`folded`], which it can read.
proof fn lemma_no_less<F: FnMut(u64, u64) -> u64>(f: F, m: u64, vs: Seq<u64>, r: u64)
    requires
        forall|a: u64, v: u64, b: u64| #[trigger] f.ensures((a, v), b) ==> b >= a && b >= v,
        folded(f, m, vs, r),
    ensures
        r >= m,
        forall|i: int| 0 <= i < vs.len() ==> r >= vs[i],
    decreases vs.len(),
{
    if vs.len() > 0 {
        let b = choose|b: u64| #[trigger] f.ensures((m, vs[0]), b) && folded(f, b, vs.drop_first(), r);
        lemma_no_less(f, b, vs.drop_first(), r);
        assert forall|i: int| 0 <= i < vs.len() implies r >= vs[i] by {
            if i > 0 {
                assert(vs[i] == vs.drop_first()[i - 1]);
            }
        }
    }
}

/// A fold: a number that is no less than any of a column's, by a closure
/// that keeps the greater of two.
#[allow(dead_code)]
fn the_greatest(col: &VerifiedColumn<u64>) -> (r: u64)
    ensures
        forall|i: int| 0 <= i < col@.len() ==> r >= col@[i],
{
    let f = |m: u64, v: u64| -> (b: u64)
        ensures
            b == if v > m { v } else { m },
        {
            if v > m {
                v
            } else {
                m
            }
        };
    let r = col.iter().fold(0u64, f);
    proof {
        lemma_no_less(f, 0, col@, r);
    }
    r
}

/// A copy of an iterator goes on from where the iterator is, and each
/// then goes its own way; and an iterator of no column gives nothing.
/// For every value type and codec.
#[allow(dead_code)]
fn two_readers<'a, V: VValue<C>, C: VCodec>(col: &'a VerifiedColumn<V, C>)
    requires
        col@.len() == 3,
{
    let mut a = col.iter();
    let first = a.next();
    let mut b = a.clone();
    let x = a.next();
    let y = b.next();
    assert(first is Some && x is Some && y is Some);
    proof {
        <Get<'a, V> as VGet>::lemma_sv_inj(x->0, y->0);
    }
    assert(x == y);
    assert((x->0).sv() == col@[1]);
    let z = a.next();
    assert(z is Some && (z->0).sv() == col@[2]);
    let left_a = a.len();
    let left_b = b.len();
    assert(left_a == 0 && left_b == 1);
    let mut none = VerifiedIter::<V, C>::default();
    let n = none.next();
    assert(n is None);
}

/// An iterator's window moved to a range of the column: a `for` loop
/// then reads the column's values there. For every value type and codec.
#[allow(dead_code)]
fn in_the_range<'a, V: VValue<C>, C: VCodec>(col: &'a VerifiedColumn<V, C>, a: usize, b: usize) -> (out: Vec<
    Get<'a, V>,
>)
    requires
        a <= b <= col@.len(),
    ensures
        <Get<'a, V> as VGet>::svs(out@) == col@.subrange(a as int, b as int),
{
    let mut it = col.iter();
    it.shift(a..b);
    proof {
        assert(window(col@, a as int, b as int) =~= col@.subrange(a as int, b as int));
    }
    let ghost all = it.items();
    let mut out: Vec<Get<'a, V>> = Vec::new();
    for x in at: it
        invariant
            at.seq() == all,
            out@ == all.take(at.index()),
    {
        out.push(x);
        proof {
            assert(all.take(at.index() + 1) =~= all.take(at.index()).push(x));
        }
    }
    proof {
        assert(all.take(all.len() as int) =~= all);
    }
    out
}

/// Where an iterator stands and where its window ends: a step moves the
/// one, `set_max` the other, and what is left is the column's values
/// between the two.
#[allow(dead_code)]
fn where_it_stands(col: &VerifiedColumn<u64>)
    requires
        col@.len() == 10,
{
    let mut it = col.iter_range(2, 8);
    let (p, e) = (it.pos(), it.end_pos());
    assert(p == 2 && e == 8);
    let a = it.next();
    assert(a == Some(col@[2]));
    it.advance_by(2);
    let p = it.pos();
    assert(p == 5);
    // a place behind the iterator: it stays
    it.advance_to(4);
    let (p, n) = (it.pos(), it.len());
    assert(p == 5 && n == 3);
    // a wider window, which ends past the column: what the column has
    it.set_max(20);
    let (e, n) = (it.end_pos(), it.len());
    assert(e == 20 && n == 5);
    let b = it.next();
    assert(b == Some(col@[5]));
    // a window that ends behind the iterator: nothing, and the window
    // then ends where the iterator stands
    it.set_max(3);
    let c = it.next();
    let (p, e) = (it.pos(), it.end_pos());
    assert(c is None && p == 6 && e == 6);
}

/// An iterator read until it gives nothing stands at the end of what it
/// gave, and its window ends there: where the range ended, or the
/// column if that came first. For every value type and codec.
#[allow(dead_code)]
fn read_out<V: VValue<C>, C: VCodec>(col: &VerifiedColumn<V, C>, a: usize, b: usize) -> (r: (usize, usize, usize))
    requires
        a <= b,
        a <= col@.len(),
    ensures
        // how many values it gave, where it then stands, and where its
        // window ends
        r.0 == r.1 - a,
        r.1 == if b <= col@.len() {
            b
        } else {
            col@.len() as usize
        },
        r.2 == r.1,
{
    let mut it = col.iter();
    it.shift(a..b);
    let mut n: usize = 0;
    loop
        invariant_except_break
            it.end_pos() == b,
        invariant
            it.column() == col@,
            it.pos() == a + n,
        ensures
            it.end_pos() == it.pos(),
            b <= col@.len() ==> it.pos() == b,
            b > col@.len() ==> it.pos() == col@.len(),
        decreases it.rest().len(),
    {
        // what is left is the column's values in the window
        let _ = it.pos();
        match it.next() {
            Some(_) => {
                n += 1;
            },
            None => {
                break ;
            },
        }
    }
    (n, it.pos(), it.end_pos())
}

/// The values at two places, read with one iterator whose window moves
/// from the one to the other, and how many values the second window then
/// has left: for every value type and codec.
#[allow(dead_code)]
fn at_two_places<'a, V: VValue<C>, C: VCodec>(col: &'a VerifiedColumn<V, C>, i: usize, j: usize, n: usize) -> (r: (
    Get<'a, V>,
    Get<'a, V>,
    usize,
))
    requires
        i < j,
        0 < n,
        j + n <= col@.len(),
    ensures
        r.0.sv() == col@[i as int],
        r.1.sv() == col@[j as int],
        r.2 == n - 1,
{
    // a column is no longer than a `usize` counts
    let _ = col.len();
    let mut it = col.iter();
    let x = it.shift_next(i..i + 1);
    let y = it.shift_next(j..j + n);
    let left = it.len();
    let p = it.pos();
    assert(p == j + 1);
    (x.unwrap(), y.unwrap(), left)
}

/// How long an iterator's window is: that many values it has left,
/// unless the window ends past the column and it has fewer.
#[allow(dead_code)]
fn window_len<V: VValue<C>, C: VCodec>(it: &VerifiedIter<'_, V, C>) -> (r: usize)
    ensures
        r >= it.rest().len(),
        it.end_pos() <= it.column().len() ==> r == it.rest().len(),
{
    let (p, e) = (it.pos(), it.end_pos());
    e - p
}

/// Where each move leaves an iterator and its window, when it moves past
/// what the iterator has, to a window with no value, or not at all; and
/// where a copy, an iterator over a whole column and an iterator of no
/// column stand.
#[allow(dead_code)]
fn moved_past(col: &VerifiedColumn<u64>)
    requires
        col@.len() == 10,
{
    // past the window's end: the iterator stands there with nothing left
    let mut it = col.iter_range(2, 8);
    it.advance_by(7);
    let (p, e, n) = (it.pos(), it.end_pos(), it.len());
    assert(p == 8 && e == 8 && n == 0);
    // a copy stands where the iterator does, and then moves on its own
    let mut it = col.iter_range(2, 8);
    let _ = it.next();
    let mut b = it.clone();
    let (p, e) = (b.pos(), b.end_pos());
    assert(p == 3 && e == 8);
    b.advance_to(9);
    let (p, e) = (b.pos(), b.end_pos());
    assert(p == 8 && e == 8);
    it.advance_to(5);
    let (p, e) = (it.pos(), it.end_pos());
    assert(p == 5 && e == 8);
    let v = it.next();
    assert(v == Some(col@[5]));
    // `nth`: past the value it gives, or at the end of what there was
    let mut it = col.into_iter();
    let (p, e) = (it.pos(), it.end_pos());
    assert(p == 0 && e == 10);
    let v = it.nth(3);
    let (p, e, n) = (it.pos(), it.end_pos(), it.len());
    assert(v == Some(col@[3]) && p == 4 && e == 10 && n == 6);
    let v = it.nth(6);
    let (p, e) = (it.pos(), it.end_pos());
    assert(v is None && p == 10 && e == 10);
    // a window past the column: the iterator stands at the column's end
    let mut it = col.iter_range(0, 5);
    it.shift(12..15);
    let (p, e, n) = (it.pos(), it.end_pos(), it.len());
    assert(p == 10 && e == 10 && n == 0);
    // a window that starts past its own end: where that end is, if ahead
    let mut it = col.iter_range(1, 5);
    it.shift(7..4);
    let (p, e) = (it.pos(), it.end_pos());
    assert(p == 4 && e == 4);
    it.shift(9..2);
    let (p, e) = (it.pos(), it.end_pos());
    assert(p == 4 && e == 4);
    // a window behind the iterator: from where the iterator stands
    let mut it = col.iter_range(6, 10);
    it.shift(2..8);
    let (p, e, n) = (it.pos(), it.end_pos(), it.len());
    assert(p == 6 && e == 8 && n == 2);
    // `shift_next` to a window with no value
    let mut it = col.iter();
    let v = it.shift_next(3..3);
    let (p, e) = (it.pos(), it.end_pos());
    assert(v is None && p == 3 && e == 3);
    let v = it.shift_next(11..14);
    let (p, e, n) = (it.pos(), it.end_pos(), it.len());
    assert(v is None && p == 10 && e == 10 && n == 0);
    // an iterator of no column
    let mut d = VerifiedIter::<u64>::default();
    let (p, e) = (d.pos(), d.end_pos());
    assert(p == 0 && e == 0);
    d.set_max(4);
    let (p, e, n) = (d.pos(), d.end_pos(), d.len());
    assert(p == 0 && e == 4 && n == 0);
}

/// What a call says by itself, with no question put to the iterator:
/// which column a new iterator or a copy reads, where it stands and
/// where its window ends, and what a move leaves it to give.
#[allow(dead_code)]
fn told_by_the_call(col: &VerifiedColumn<u64>)
    requires
        col@.len() == 10,
{
    // an iterator over a whole column
    let mut it = col.into_iter();
    assert(it.pos() == 0 && it.end_pos() == 10 && it.column() == col@);
    // a copy, of an iterator that has moved and whose window ends past
    // the column
    let _ = it.next();
    it.set_max(20);
    let b = it.clone();
    assert(b.pos() == 1 && b.end_pos() == 20 && b.column() == col@);
    assert(b.source() == it.source());
    // an iterator of no column
    let d = VerifiedIter::<u64>::default();
    assert(d.pos() == 0 && d.end_pos() == 0);
    // a move on, and two moves of the window: to one with values, and
    // to one with none
    let mut it = col.iter_range(2, 8);
    it.advance_by(2);
    assert(it.rest() =~= col@.subrange(4, 8));
    let v = it.shift_next(5..7);
    assert(v == Some(col@[5]) && it.end_pos() == 7);
    assert(it.rest() =~= col@.subrange(6, 7));
    let v = it.shift_next(7..7);
    assert(v is None && it.rest().len() == 0);
}

/// Whether an iterator's window ends in its column, of which `len` is
/// the length: asked of the window's end alone. If it does, the iterator
/// has left as many values as lie between where it stands and that end.
#[allow(dead_code)]
fn ends_within<V: VValue<C>, C: VCodec>(it: &VerifiedIter<'_, V, C>, len: usize) -> (r: bool)
    requires
        len == it.column().len(),
    ensures
        r == (it.end_pos() <= it.column().len()),
        r ==> it.rest().len() == it.end_pos() - it.pos(),
        it.pos() <= it.end_pos(),
        it.pos() <= len,
{
    it.end_pos() <= len
}

/// The first run of a column, as its iterator gives it: one value, as
/// often as the column has it at its start and no more often; and no run
/// only of a column with no value. For every value type and codec.
#[allow(dead_code)]
fn the_first_run<'a, V: VValue<C>, C: VCodec>(col: &'a VerifiedColumn<V, C>) -> (r: Option<Run<Get<'a, V>>>)
    ensures
        match r {
            Some(run) => {
                &&& 1 <= run.count <= col@.len()
                &&& forall|i: int| 0 <= i < run.count ==> col@[i] == run.value.sv()
                &&& run.count < col@.len() ==> col@[run.count as int] != run.value.sv()
            },
            None => col@.len() == 0,
        },
{
    let mut it = col.iter();
    it.next_run()
}

/// Runs as an iterator's window has them. Of three of one value and two
/// of another, an iterator over all gives a run of three and a run of
/// two; one over the middle three gives a run of two and a run of one,
/// the runs of the window and not of the column. Each run moves the
/// iterator on by its count and leaves the window's end where it was;
/// and an iterator that gives no run has its window end where it stands.
/// For every value type and codec.
#[allow(dead_code)]
fn runs_in_a_window<'v, V: VValue<C>, C: VCodec>(a: Get<'v, V>, b: Get<'v, V>)
    requires
        a.sv() != b.sv(),
{
    let mut col = VerifiedColumn::<V, C>::fill(3, a);
    let mut bs: Vec<Run<Get<'v, V>>> = Vec::new();
    bs.push(Run { count: 2, value: b });
    proof {
        lemma_run_values_push(Seq::<Run<Get<'v, V>>>::empty(), bs@[0]);
        assert(bs@ =~= Seq::<Run<Get<'v, V>>>::empty().push(bs@[0]));
    }
    col.splice_runs(3, 0, bs);
    assert(col@ =~= seq![a.sv(), a.sv(), a.sv(), b.sv(), b.sv()]);

    // the whole column, with a window that ends past it
    let mut it = col.iter();
    it.set_max(20);
    assert(it.rest() =~= col@);
    let r = it.next_run();
    assert(r matches Some(run) && run.count == 3 && run.value.sv() == a.sv()) by {
        assert(col@[0] == a.sv() && col@[1] == a.sv() && col@[2] == a.sv() && col@[3] == b.sv());
    }
    let (p, e) = (it.pos(), it.end_pos());
    assert(p == 3 && e == 20);
    let ghost left = it.rest();
    assert(left =~= seq![b.sv(), b.sv()]);
    let r = it.next_run();
    assert(r matches Some(run) && run.count == 2 && run.value.sv() == b.sv()) by {
        assert(left[0] == b.sv() && left[1] == b.sv());
    }
    let (p, e) = (it.pos(), it.end_pos());
    assert(p == 5 && e == 20);
    let r = it.next_run();
    let (p, e) = (it.pos(), it.end_pos());
    assert(r is None && p == 5 && e == 5);

    // the middle three
    let mut it = col.iter_range(1, 4);
    assert(it.rest() =~= seq![a.sv(), a.sv(), b.sv()]);
    let r = it.next_run();
    assert(r matches Some(run) && run.count == 2 && run.value.sv() == a.sv()) by {
        assert(col@.subrange(1, 4)[0] == a.sv() && col@.subrange(1, 4)[1] == a.sv());
        assert(col@.subrange(1, 4)[2] == b.sv());
    }
    let ghost left = it.rest();
    assert(left =~= seq![b.sv()]);
    let r = it.next_run();
    assert(r matches Some(run) && run.count == 1 && run.value.sv() == b.sv()) by {
        assert(left[0] == b.sv());
    }
    let (p, e) = (it.pos(), it.end_pos());
    assert(p == 4 && e == 4);
    let r = it.next_run();
    assert(r is None);
}

/// Runs are apart: each is of one value or more, and no two that follow
/// one another are of the same value. A client's own induction over
/// `runs_of`.
proof fn lemma_runs_apart<T>(o: Seq<T>)
    requires
        o.len() <= usize::MAX,
    ensures
        forall|i: int| 0 <= i < runs_of(o).len() ==> (#[trigger] runs_of(o)[i]).count >= 1,
        forall|i: int| 0 < i < runs_of(o).len() ==> runs_of(o)[i - 1].value != (#[trigger] runs_of(o)[i]).value,
    decreases o.len(),
{
    if o.len() > 0 {
        let d = o.drop_first();
        lemma_runs_apart(d);
        lemma_runs_first(d);
        let r = runs_of(d);
        let rs = runs_of(o);
        if r.len() > 0 && r[0].value == o[0] {
            assert forall|i: int| 0 < i < rs.len() implies rs[i - 1].value != (#[trigger] rs[i]).value by {
                assert(rs[i] == r[i]);
                if i > 1 {
                    assert(rs[i - 1] == r[i - 1]);
                }
            }
            assert forall|i: int| 0 <= i < rs.len() implies (#[trigger] rs[i]).count >= 1 by {
                if i > 0 {
                    assert(rs[i] == r[i]);
                }
            }
        } else {
            assert forall|i: int| 0 < i < rs.len() implies rs[i - 1].value != (#[trigger] rs[i]).value by {
                assert(rs[i] == r[i - 1]);
                if i > 1 {
                    assert(rs[i - 1] == r[i - 2]);
                }
            }
            assert forall|i: int| 0 <= i < rs.len() implies (#[trigger] rs[i]).count >= 1 by {
                if i > 0 {
                    assert(rs[i] == r[i - 1]);
                }
            }
        }
    }
}

/// A column's values as runs: a `for` loop over the runs of its iterator
/// collects them. They are the column's values, each run's as often as
/// its count says; none is of no value, and no two that follow one
/// another are of the same value. For every value type and codec.
#[allow(dead_code)]
fn in_runs<'a, V: VValue<C>, C: VCodec>(col: &'a VerifiedColumn<V, C>) -> (out: Vec<Run<Get<'a, V>>>)
    ensures
        run_values(out@) == col@,
        forall|i: int| 0 <= i < out@.len() ==> (#[trigger] out@[i]).count >= 1,
        forall|i: int| 0 < i < out@.len() ==> out@[i - 1].value.sv() != (#[trigger] out@[i]).value.sv(),
{
    let it = col.iter();
    let ghost all = it.items();
    let ghost rs = runs_of(all);
    proof {
        it.lemma_items_len();
    }
    let mut out: Vec<Run<Get<'a, V>>> = Vec::new();
    for run in at: it.runs()
        invariant
            at.seq() == rs,
            out@ == rs.take(at.index()),
    {
        out.push(run);
        proof {
            assert(rs.take(at.index() + 1) =~= rs.take(at.index()).push(run));
        }
    }
    proof {
        assert(rs.take(rs.len() as int) =~= rs);
        lemma_runs_of_values(all);
        lemma_runs_apart(all);
        assert forall|i: int| 0 < i < out@.len() implies out@[i - 1].value.sv() != (#[trigger] out@[i]).value.sv() by {
            if out@[i - 1].value.sv() == out@[i].value.sv() {
                <Get<'a, V> as VGet>::lemma_sv_inj(out@[i - 1].value, out@[i].value);
            }
        }
    }
    out
}

/// The first runs of some are no more values than all of them.
proof fn lemma_counted_take<G>(rs: Seq<Run<G>>, k: int)
    requires
        0 <= k <= rs.len(),
    ensures
        counted(rs.take(k)) <= counted(rs),
    decreases rs.len() - k,
{
    if k < rs.len() {
        lemma_counted_take(rs, k + 1);
        assert(rs.take(k + 1).drop_last() =~= rs.take(k));
    } else {
        assert(rs.take(k) =~= rs);
    }
}

/// One run more is its count more, and still no more than all of them.
proof fn lemma_counted_step<G>(rs: Seq<Run<G>>, k: int)
    requires
        0 <= k < rs.len(),
    ensures
        counted(rs.take(k + 1)) == counted(rs.take(k)) + rs[k].count,
        counted(rs.take(k + 1)) <= counted(rs),
{
    lemma_counted_take(rs, k + 1);
    assert(rs.take(k + 1).drop_last() =~= rs.take(k));
    assert(rs.take(k + 1).last() == rs[k]);
}

/// How many values a range of a column has, counted by its runs: their
/// counts add up to the length of the range, and adding them does not
/// overflow. For every value type and codec.
#[allow(dead_code)]
fn count_by_runs<V: VValue<C>, C: VCodec>(col: &VerifiedColumn<V, C>, a: usize, b: usize) -> (n: usize)
    requires
        a <= b <= col@.len(),
    ensures
        n == b - a,
{
    let it = col.iter_range(a, b);
    let ghost all = it.items();
    let ghost rs = runs_of(all);
    proof {
        it.lemma_items_len();
        lemma_runs_of_values(all);
        lemma_counted(rs);
    }
    let mut n: usize = 0;
    for run in at: it.runs()
        invariant
            at.seq() == rs,
            counted(rs) == b - a,
            n == counted(rs.take(at.index())),
    {
        proof {
            lemma_counted_step(rs, at.index());
        }
        n = n + run.count;
    }
    proof {
        assert(rs.take(rs.len() as int) =~= rs);
    }
    n
}

/// A column copied by its runs: the runs of its iterator, spliced into a
/// new column, make a column of the same values. For every value type
/// and codec.
#[allow(dead_code)]
fn copied_by_runs<V: VValue<C>, C: VCodec>(col: &VerifiedColumn<V, C>) -> (r: VerifiedColumn<V, C>)
    ensures
        r@ == col@,
{
    let mut r = VerifiedColumn::<V, C>::new();
    let it = col.iter();
    proof {
        it.lemma_items_len();
        lemma_runs_of_values(it.items());
    }
    r.splice_runs(0, 0, it.runs());
    assert(r@ =~= col@);
    r
}

/// Runs asked for one at a time, and what they say of the iterator they
/// read: after a run it stands past the run in the same column, with
/// the same end of its window, and has left what it had less the run.
#[allow(dead_code)]
fn run_by_run(col: &VerifiedColumn<bool>)
    requires
        col@.len() > 0,
{
    let mut rs = col.iter_range(0, col.len()).runs();
    assert(rs.iter().pos() == 0 && rs.iter().column() == col@);
    let r = rs.next();
    assert(r matches Some(run) && run.value == col@[0] && rs.iter().pos() == run.count && rs.iter().end_pos()
        == col@.len() && rs.iter().column() == col@ && rs.iter().rest() =~= col@.skip(run.count as int)) by {
        assert(col@.subrange(0, col@.len() as int) =~= col@);
    }
    let ghost left = rs.iter().rest();
    let r = rs.next();
    assert(r is None ==> left.len() == 0 && rs.iter().end_pos() == rs.iter().pos());
    assert(r matches Some(run) ==> run.value == left[0] && run.value != col@[0]);
}

/// What `next_run` says by itself, of any iterator, each thing asked for
/// on its own and with no lemma: the run is the first of the values the
/// iterator had, as it gives them and as specifications have them; and
/// what is left is those less the run.
#[allow(dead_code)]
fn told_by_a_run<'a, V: VValue<C>, C: VCodec>(it: VerifiedIter<'a, V, C>) -> (r: Option<Run<Get<'a, V>>>)
    ensures
        match r {
            Some(run) => head_run(it.items(), run.value, run.count as int) && head_run(
                it.rest(),
                run.value.sv(),
                run.count as int,
            ),
            None => it.items().len() == 0 && it.rest().len() == 0,
        },
{
    let mut at = it;
    let r = at.next_run();
    match r {
        Some(run) => {
            assert(at.items() == it.items().skip(run.count as int));
            assert(at.rest() == it.rest().skip(run.count as int));
            assert(at.pos() == it.pos() + run.count);
            assert(at.end_pos() == it.end_pos());
        },
        None => {
            assert(at.items().len() == 0);
            assert(at.rest().len() == 0);
            assert(at.pos() == it.pos());
            assert(at.end_pos() == it.pos());
        },
    }
    assert(at.column() == it.column());
    r
}

/// The same of the iterator of runs: what its `next` says of the
/// iterator of values whose runs it gives.
#[allow(dead_code)]
fn told_by_the_runs<'a, V: VValue<C>, C: VCodec>(it: VerifiedIter<'a, V, C>) -> (r: Option<Run<Get<'a, V>>>)
    ensures
        match r {
            Some(run) => head_run(it.items(), run.value, run.count as int) && head_run(
                it.rest(),
                run.value.sv(),
                run.count as int,
            ),
            None => it.items().len() == 0 && it.rest().len() == 0,
        },
{
    let mut rs = it.runs();
    let r = rs.next();
    match r {
        Some(run) => {
            assert(rs.iter().items() == it.items().skip(run.count as int));
            assert(rs.iter().rest() == it.rest().skip(run.count as int));
            assert(rs.iter().pos() == it.pos() + run.count);
            assert(rs.iter().end_pos() == it.end_pos());
        },
        None => {
            assert(rs.iter().items().len() == 0);
            assert(rs.iter().rest().len() == 0);
            assert(rs.iter().pos() == it.pos());
            assert(rs.iter().end_pos() == it.pos());
        },
    }
    assert(rs.iter().column() == it.column());
    assert(rs.iter().source() == it.source());
    r
}

/// Any iterator has left no more values than a `usize` counts, and as
/// many as a specification has of them.
proof fn no_more_than_counted<'a, V: VValue<C>, C: VCodec>(it: VerifiedIter<'a, V, C>)
    ensures
        it.items().len() <= usize::MAX,
        it.rest().len() == it.items().len(),
{
    it.lemma_items_len();
}

/// What `scan_to_value` says by itself, of any iterator, each thing asked
/// for on its own and with no lemma: where the first of the values left
/// is that is `x`, or that none of them is; and where that leaves the
/// iterator.
#[allow(dead_code)]
fn told_by_a_scan<'a, V: VValue<C>, C: VCodec>(it: VerifiedIter<'a, V, C>, x: Get<'_, V>) -> (r: Option<usize>)
    ensures
        match r {
            Some(k) => {
                &&& it.pos() <= k < it.pos() + it.rest().len()
                &&& it.rest()[k - it.pos()] == x.sv()
                &&& forall|i: int| 0 <= i < k - it.pos() ==> it.rest()[i] != x.sv()
            },
            None => forall|i: int| 0 <= i < it.rest().len() ==> it.rest()[i] != x.sv(),
        },
{
    let mut at = it;
    let r = at.scan_to_value(x);
    match r {
        Some(k) => {
            assert(at.items() == it.items().skip(k - it.pos() + 1));
            assert(at.rest() == it.rest().skip(k - it.pos() + 1));
            assert(at.pos() == k + 1);
            assert(at.end_pos() == it.end_pos());
        },
        None => {
            assert(at.items().len() == 0);
            assert(at.rest().len() == 0);
            assert(at.pos() == it.pos() + it.rest().len());
            assert(at.end_pos() == at.pos());
        },
    }
    assert(at.column() == it.column());
    r
}

/// The first place of `x` among the values `a..b` of a column, said of
/// the column's values alone: a scan of an iterator over that window.
/// For every value type and codec.
#[allow(dead_code)]
fn first_place_in<V: VValue<C>, C: VCodec>(col: &VerifiedColumn<V, C>, x: Get<'_, V>, a: usize, b: usize) -> (r:
    Option<usize>)
    requires
        a <= b <= col@.len(),
    ensures
        match r {
            Some(k) => a <= k < b && col@[k as int] == x.sv() && forall|i: int| a <= i < k ==> col@[i] != x.sv(),
            None => forall|i: int| a <= i < b ==> col@[i] != x.sv(),
        },
{
    let mut it = col.iter_range(a, b);
    let ghost o = it.rest();
    let r = it.scan_to_value(x);
    proof {
        assert forall|i: int| a <= i < b implies col@[i] == #[trigger] o[i - a] by {}
        match r {
            Some(k) => {
                assert(col@[k as int] == o[k - a]);
                assert forall|i: int| a <= i < k implies col@[i] != x.sv() by {
                    assert(col@[i] == o[i - a]);
                }
            },
            None => {
                assert forall|i: int| a <= i < b implies col@[i] != x.sv() by {
                    assert(col@[i] == o[i - a]);
                }
            },
        }
    }
    r
}

/// Every place of `x` in a column, by scans that each go on from where
/// the last one stopped: the places come in rising order, each holds
/// `x`, and no other place does. For every value type and codec.
#[allow(dead_code)]
fn all_places<V: VValue<C>, C: VCodec>(col: &VerifiedColumn<V, C>, x: Get<'_, V>) -> (out: Vec<usize>)
    ensures
        forall|i: int, j: int| 0 <= i < j < out@.len() ==> out@[i] < out@[j],
        forall|i: int| 0 <= i < out@.len() ==> out@[i] < col@.len() && col@[out@[i] as int] == x.sv(),
        forall|k: int| 0 <= k < col@.len() && col@[k] == x.sv() ==> exists|i: int| 0 <= i < out@.len() && out@[i] == k,
{
    let mut out: Vec<usize> = Vec::new();
    let mut it = col.iter();
    loop
        invariant
            it.column() == col@,
            it.pos() <= col@.len(),
            it.rest() == col@.skip(it.pos() as int),
            forall|i: int, j: int| 0 <= i < j < out@.len() ==> out@[i] < out@[j],
            forall|i: int| 0 <= i < out@.len() ==> out@[i] < it.pos() && col@[out@[i] as int] == x.sv(),
            forall|k: int|
                0 <= k < it.pos() && col@[k] == x.sv() ==> exists|i: int| 0 <= i < out@.len() && out@[i] == k,
        ensures
            forall|i: int, j: int| 0 <= i < j < out@.len() ==> out@[i] < out@[j],
            forall|i: int| 0 <= i < out@.len() ==> out@[i] < col@.len() && col@[out@[i] as int] == x.sv(),
            forall|k: int|
                0 <= k < col@.len() && col@[k] == x.sv() ==> exists|i: int| 0 <= i < out@.len() && out@[i] == k,
        decreases col@.len() - it.pos(),
    {
        let ghost p = it.pos() as int;
        let ghost o = it.rest();
        let ghost before = out@;
        match it.scan_to_value(x) {
            Some(k) => {
                out.push(k);
                proof {
                    assert(col@[k as int] == o[k - p]);
                    assert(it.rest() =~= col@.skip(k + 1));
                    assert forall|j: int| 0 <= j < it.pos() && col@[j] == x.sv() implies exists|i: int|
                        0 <= i < out@.len() && out@[i] == j by {
                        if j < p {
                            let i = choose|i: int| 0 <= i < before.len() && before[i] == j;
                            assert(out@[i] == j);
                        } else if j < k {
                            assert(col@[j] == o[j - p]);
                        } else {
                            assert(out@[before.len() as int] == j);
                        }
                    }
                }
            },
            None => {
                proof {
                    assert forall|j: int| 0 <= j < col@.len() && col@[j] == x.sv() implies exists|i: int|
                        0 <= i < out@.len() && out@[i] == j by {
                        if j >= p {
                            assert(col@[j] == o[j - p]);
                        }
                    }
                }
                break ;
            },
        }
    }
    out
}

/// What `seek_to_value` says by itself, of any iterator and a range that
/// lies in what the iterator has left, each thing asked for on its own
/// and with no lemma: where `x` stands among the values of the range,
/// if they are in order as far as `x` can tell; and where that leaves
/// the iterator.
#[allow(dead_code)]
fn told_by_a_seek<'a, V: VValue<C>, C: VCodec>(it: VerifiedIter<'a, V, C>, x: Get<'_, V>, range: Range<usize>) -> (r:
    Range<usize>)
    requires
        it.pos() <= range.start <= range.end <= it.pos() + it.rest().len(),
        sorted_for::<V>(it.rest().subrange(range.start - it.pos(), range.end - it.pos()), x.sv()),
    ensures
        range.start <= r.start <= r.end <= range.end,
        forall|j: int|
            range.start - it.pos() <= j < range.end - it.pos() ==> {
                &&& (it.pos() + j < r.start) == V::below(#[trigger] it.rest()[j], x.sv())
                &&& (it.pos() + j < r.end) == !V::below(x.sv(), it.rest()[j])
                &&& (r.start <= it.pos() + j < r.end) == (it.rest()[j] == x.sv())
            },
{
    let mut at = it;
    let r = at.seek_to_value(x, range);
    assert(at.column() == it.column());
    assert(at.pos() == r.start);
    assert(at.rest() == it.rest().skip(r.start - it.pos()));
    assert(at.end_pos() == it.end_pos());
    r
}

/// A search over a range that does not lie in what the iterator has
/// left, of any iterator and whatever its values are: the search is of
/// no value and gives an empty range. A range that starts past what the
/// iterator has left takes the iterator to the end of that, where its
/// window then ends; an empty range, or one that ends before it starts,
/// takes it to where the range starts, or leaves it where it is if that
/// is behind it; and so does a range that ends behind the iterator.
#[allow(dead_code)]
fn seek_out_of_range<'a, V: VValue<C>, C: VCodec>(it: VerifiedIter<'a, V, C>, x: Get<'_, V>, range: Range<usize>) -> (r:
    Range<usize>)
    ensures
        r.start == r.end || (range.start < range.end && it.pos() < range.end && range.start < it.pos()
            + it.rest().len()),
        range.start > it.pos() + it.rest().len() ==> r.start == it.pos() + it.rest().len(),
        it.pos() <= range.start <= it.pos() + it.rest().len() && range.end <= range.start ==> r.start
            == range.start,
        range.start <= it.pos() && range.end <= it.pos() ==> r.start == it.pos(),
{
    let mut at = it;
    let r = at.seek_to_value(x, range);
    assert(range.start > it.pos() + it.rest().len() ==> at.rest().len() == 0 && at.end_pos() == at.pos());
    assert(range.start <= it.pos() + it.rest().len() ==> at.end_pos() == it.end_pos());
    assert(at.pos() == r.start);
    r
}

/// Where `x` stands among all the values an iterator has left, which
/// are in order: the search over `0..usize::MAX` is over all of them,
/// wherever the iterator stands and wherever its window ends. The
/// iterator is left at the first place of `x`, or where `x` would
/// stand, with the same end to its window. For every value type and
/// codec.
#[allow(dead_code)]
fn find_in_the_rest<V: VValue<C>, C: VCodec>(it: &mut VerifiedIter<'_, V, C>, x: Get<'_, V>) -> (r: Range<usize>)
    requires
        sorted::<V>(old(it).rest()),
    ensures
        final(it).column() == old(it).column(),
        old(it).pos() <= r.start <= r.end <= old(it).pos() + old(it).rest().len(),
        forall|j: int|
            0 <= j < old(it).rest().len() ==> {
                &&& (old(it).pos() + j < r.start) == V::below(#[trigger] old(it).rest()[j], x.sv())
                &&& (old(it).pos() + j < r.end) == !V::below(x.sv(), old(it).rest()[j])
                &&& (r.start <= old(it).pos() + j < r.end) == (old(it).rest()[j] == x.sv())
            },
        final(it).pos() == r.start,
        final(it).end_pos() == old(it).end_pos(),
        final(it).rest() == old(it).rest().skip(r.start - old(it).pos()),
        sorted::<V>(final(it).rest()),
{
    // asked for what it says of the window
    let _ = it.pos();
    proof {
        lemma_sorted::<V>(it.rest(), x.sv());
    }
    it.seek_to_value(x, 0..usize::MAX)
}

/// Numbers in order, sought by an iterator: the range is the one the
/// column's own search gives, the lesser numbers are before it and the
/// greater ones after, and the iterator then gives the first `x`, if
/// the column has one.
#[allow(dead_code)]
fn seek_then_read(col: &VerifiedColumn<u64>, x: u64) -> (r: Range<usize>)
    requires
        forall|i: int, j: int| 0 <= i <= j < col@.len() ==> col@[i] <= col@[j],
    ensures
        r.start <= r.end <= col@.len(),
        forall|k: int| 0 <= k < r.start ==> col@[k] < x,
        forall|k: int| r.start <= k < r.end ==> col@[k] == x,
        forall|k: int| r.end <= k < col@.len() ==> col@[k] > x,
{
    let n = col.len();
    proof {
        assert(sorted::<u64>(col@));
        lemma_sorted::<u64>(col@, x);
        assert(col@.subrange(0, n as int) =~= col@);
    }
    let mut it = col.iter();
    let r = it.seek_to_value(x, 0..n);
    let s = col.scope_to_value(x, 0, n);
    assert(r.start == s.start && r.end == s.end) by {
        if r.start != s.start {
            let k = if r.start < s.start { r.start as int } else { s.start as int };
            assert((k < r.start) == (col@[k] < x));
        }
        if r.end != s.end {
            let k = if r.end < s.end { r.end as int } else { s.end as int };
            assert((k < r.end) == !(x < col@[k]));
        }
    }
    let v = it.next();
    assert(r.start < r.end ==> v == Some(x)) by {
        if r.start < r.end {
            assert(col@.skip(r.start as int)[0] == col@[r.start as int]);
        }
    }
    assert(r.start == n ==> v is None);
    r
}

/// What `suspend` says by itself, of any iterator, each thing asked for
/// on its own: where the state stands and where its window ends, that
/// the iterator has left its column's values in that window, and that
/// the state fits the column the iterator reads.
#[allow(dead_code)]
fn told_by_a_suspend<'a, V: VValue<C>, C: VCodec>(it: &VerifiedIter<'a, V, C>, col: &VerifiedColumn<V, C>) -> (s:
    VerifiedIterState)
    ensures
        s.pos() == it.pos(),
        s.end_pos() == it.end_pos(),
        it.rest() == window(it.column(), s.pos() as int, s.end_pos() as int),
        it.source() == Some(*col) ==> s.fits(col),
{
    it.suspend()
}

/// What `try_resume` says by itself, of any state and any column, each
/// thing asked for on its own: an iterator is given exactly when the
/// state fits the column, and only when the two count the same edits;
/// and the iterator given reads this column, stands where the state
/// does, and has left this column's values in the state's window, which
/// is its own window too unless the state stands at the column's end.
#[allow(dead_code)]
fn told_by_a_resume<'a, V: VValue<C>, C: VCodec>(s: &VerifiedIterState, col: &'a VerifiedColumn<V, C>) -> (r: Result<
    VerifiedIter<'a, V, C>,
    PackError,
>)
    ensures
        r is Ok <==> s.fits(col),
        r is Ok ==> s.edits() == col.edits(),
        r matches Ok(it) ==> it.source() == Some(*col),
        r matches Ok(it) ==> it.column() == col@,
        r matches Ok(it) ==> it.pos() == s.pos(),
        r matches Ok(it) ==> it.rest() == window(col@, s.pos() as int, s.end_pos() as int),
        r matches Ok(it) ==> it.end_pos() == s.end_pos() || (s.pos() == col@.len() && it.end_pos() == s.pos()),
{
    s.try_resume(col)
}

/// What `lemma_source` says by itself, of any iterator, each case asked
/// for on its own: an iterator that reads a column has that column's
/// values for its own, and one that reads none has none.
#[allow(dead_code)]
fn told_by_the_source<'a, V: VValue<C>, C: VCodec>(it: &VerifiedIter<'a, V, C>, col: &VerifiedColumn<V, C>)
    ensures
        it.source() == Some(*col) ==> it.column() == col@,
        it.source() is None ==> it.column().len() == 0,
{
    proof {
        it.lemma_source();
    }
}

/// An iterator handed over, of which no more is known than the column
/// it reads. Suspended and resumed on that column, it is sure to be
/// given back, where it stood and with the same values left.
#[allow(dead_code)]
fn handed_over<'a, V: VValue<C>, C: VCodec>(it: VerifiedIter<'_, V, C>, col: &'a VerifiedColumn<V, C>) -> (on:
    VerifiedIter<'a, V, C>)
    requires
        it.source() == Some(*col),
    ensures
        on.pos() == it.pos(),
        on.rest() == it.rest(),
{
    let s = it.suspend();
    proof {
        it.lemma_source();
    }
    let got = s.try_resume(col);
    got.unwrap()
}

/// A reader that stops and goes on. It reads the value at `k`, and is
/// suspended: the iterator is given up, and with it the borrow of the
/// column. Resumed on the column, which nothing has edited, it is sure
/// to be given, and reads the value at `k + 1`. For every value type
/// and codec.
#[allow(dead_code)]
fn stop_and_go_on<'a, V: VValue<C>, C: VCodec>(col: &'a VerifiedColumn<V, C>, k: usize) -> (r: (
    Option<Get<'a, V>>,
    Option<Get<'a, V>>,
))
    requires
        k + 1 < col@.len(),
    ensures
        r.0 matches Some(v) && v.sv() == col@[k as int],
        r.1 matches Some(v) && v.sv() == col@[k + 1],
{
    let mut it = col.iter();
    proof {
        // it has as many values left as the column has
        it.lemma_items_len();
    }
    let a = it.nth(k);
    let s = it.suspend();
    let got = s.try_resume(col);
    assert(got is Ok);
    let mut on = got.unwrap();
    assert(on.rest() == it.rest());
    assert(on.rest() == col@.skip(k + 1));
    let b = on.next();
    (a, b)
}

/// The states of an iterator over a range of a column and of one over
/// all of it fit the column, and stand and end where the iterators do.
#[allow(dead_code)]
fn of_a_range<V: VValue<C>, C: VCodec>(col: &VerifiedColumn<V, C>, a: usize, b: usize) -> (r: (
    VerifiedIterState,
    VerifiedIterState,
))
    requires
        a <= b <= col@.len(),
    ensures
        r.0.fits(col) && r.0.pos() == a && r.0.end_pos() == b,
        r.1.fits(col) && r.1.pos() == 0 && r.1.end_pos() == col@.len(),
{
    let s = col.iter_range(a, b).suspend();
    let t = col.into_iter().suspend();
    (s, t)
}

/// An iterator of no column: it reads none, and its state stands at 0
/// with nothing in its window. Nothing says that the state fits a
/// column; if it does fit one, the iterator given stands at 0 with
/// nothing to give.
#[allow(dead_code)]
fn of_no_column<V: VValue<C>, C: VCodec>(col: &VerifiedColumn<V, C>) -> (r: bool)
{
    let it = VerifiedIter::<V, C>::default();
    assert(it.source() is None);
    let s = it.suspend();
    assert(s.pos() == 0 && s.end_pos() == 0);
    match s.try_resume(col) {
        Ok(on) => {
            assert(on.pos() == 0);
            assert(on.rest().len() == 0);
            true
        },
        Err(_) => false,
    }
}

/// No move of an iterator changes the column it reads: of any iterator,
/// each move asked on its own.
#[allow(dead_code)]
fn reads_the_same_column<'a, V: VValue<C>, C: VCodec>(
    it: &mut VerifiedIter<'a, V, C>,
    which: u8,
    n: usize,
    x: Get<'_, V>,
)
    ensures
        final(it).source() == old(it).source(),
{
    match which {
        0 => it.set_max(n),
        1 => it.advance_to(n),
        2 => it.advance_by(n),
        3 => it.shift(n..n),
        4 => {
            let p = it.pos();
            let _ = it.shift_next(p..n);
        },
        5 => {
            let _ = it.next_run();
        },
        6 => {
            let _ = it.scan_to_value(x);
        },
        7 => {
            let _ = it.seek_to_value(x, 0..n);
        },
        8 => {
            let _ = it.next();
        },
        _ => {
            let _ = it.nth(n);
        },
    }
}

/// Whatever moves an iterator makes, it reads the column it was made
/// of: so does a copy of it, and what that suspends to fits the column.
#[allow(dead_code)]
fn after_every_move(col: &VerifiedColumn<u64>, x: u64) -> (s: VerifiedIterState)
    ensures
        s.fits(col),
{
    let mut it = col.iter();
    it.set_max(7);
    it.advance_to(2);
    it.advance_by(1);
    it.shift(3..9);
    let p = it.pos();
    let _ = it.shift_next(p..usize::MAX);
    let _ = it.next_run();
    let _ = it.scan_to_value(x);
    let _ = it.seek_to_value(x, 0..usize::MAX);
    let _ = it.next();
    let _ = it.nth(1);
    let other = it.clone();
    other.suspend()
}

/// A state of any iterator, resumed on any column, whether or not it
/// is the one the state was taken of: if an iterator is given, the
/// value it gives is that column's at the state's place.
#[allow(dead_code)]
fn resumed_on_any<'a, V: VValue<C>, C: VCodec>(s: &VerifiedIterState, col: &'a VerifiedColumn<V, C>) -> (r: Option<
    Get<'a, V>,
>)
    ensures
        r matches Some(v) ==> s.pos() < col@.len() && v.sv() == col@[s.pos() as int],
{
    match s.try_resume(col) {
        Ok(it) => {
            let mut it = it;
            it.next()
        },
        Err(_) => None,
    }
}

/// The readers of two columns, kept between calls as states.
pub struct Paused {
    pub numbers: VerifiedIterState,
    pub flags: VerifiedIterState,
}

/// A client that keeps the readers of its columns between calls as
/// states, and in each call resumes them, reads a value of each and
/// suspends them again. States that fit their columns are sure to
/// resume, give the columns' values at their places, and are given back
/// one place on, fitting their columns still: so the next call can be
/// made. Nothing is given where one of the windows has no value left.
#[allow(dead_code)]
fn step_both(numbers: &VerifiedColumn<u64>, flags: &VerifiedColumn<bool>, p: Paused) -> (r: Option<(u64, bool, Paused)>)
    requires
        p.numbers.fits(numbers),
        p.flags.fits(flags),
    ensures
        match r {
            Some((x, y, q)) => {
                &&& p.numbers.pos() < numbers@.len() && x == numbers@[p.numbers.pos() as int]
                &&& p.flags.pos() < flags@.len() && y == flags@[p.flags.pos() as int]
                &&& q.numbers.fits(numbers) && q.numbers.pos() == p.numbers.pos() + 1
                &&& q.flags.fits(flags) && q.flags.pos() == p.flags.pos() + 1
            },
            None => window(numbers@, p.numbers.pos() as int, p.numbers.end_pos() as int).len() == 0 || window(
                flags@,
                p.flags.pos() as int,
                p.flags.end_pos() as int,
            ).len() == 0,
        },
{
    let mut a = p.numbers.try_resume(numbers).unwrap();
    let mut b = p.flags.try_resume(flags).unwrap();
    match (a.next(), b.next()) {
        (Some(x), Some(y)) => Some((x, y, Paused { numbers: a.suspend(), flags: b.suspend() })),
        _ => None,
    }
}

// ── The encoder ─────────────────────────────────────────────────────────────
/// What `new` and `default` say of the encoder they give.
#[allow(dead_code)]
fn told_by_a_new_encoder<'a, V: VValue<C>, C: VCodec>() -> (r: (VerifiedEncoder<'a, V, C>, VerifiedEncoder<'a, V, C>))
    ensures
        r.0.wf(),
        r.0@.len() == 0,
        r.1.wf(),
        r.1@.len() == 0,
{
    let a = VerifiedEncoder::<V, C>::new();
    let b = VerifiedEncoder::<V, C>::default();
    (a, b)
}

/// What `len` and `is_empty` say.
#[allow(dead_code)]
fn told_by_the_count<'a, V: VValue<C>, C: VCodec>(enc: &VerifiedEncoder<'a, V, C>) -> (r: (usize, bool))
    requires
        enc.wf(),
    ensures
        r.0 == enc@.len(),
        r.1 == (enc@.len() == 0),
{
    (enc.len(), enc.is_empty())
}

/// What `append` says: the encoder is one still, and has one value
/// more, the one given, after those it had.
#[allow(dead_code)]
fn told_by_an_append<'a, V: VValue<C>, C: VCodec>(enc: &mut VerifiedEncoder<'a, V, C>, v: Get<'a, V>)
    requires
        old(enc).wf(),
    ensures
        final(enc).wf(),
        final(enc)@.len() == old(enc)@.len() + 1,
        final(enc)@.last() == v.sv(),
        final(enc)@.drop_last() == old(enc)@,
{
    enc.append(v);
    assert(enc@.drop_last() =~= old(enc)@);
}

/// What `append_n` says: `n` values more, each the one given, after
/// those the encoder had.
#[allow(dead_code)]
fn told_by_an_append_n<'a, V: VValue<C>, C: VCodec>(enc: &mut VerifiedEncoder<'a, V, C>, v: Get<'a, V>, n: usize)
    requires
        old(enc).wf(),
    ensures
        final(enc).wf(),
        final(enc)@.len() == old(enc)@.len() + n,
        final(enc)@.take(old(enc)@.len() as int) == old(enc)@,
        forall|i: int| old(enc)@.len() <= i < final(enc)@.len() ==> final(enc)@[i] == v.sv(),
{
    enc.append_n(v, n);
    assert(enc@.take(old(enc)@.len() as int) =~= old(enc)@);
}

/// What `append_owned` says: as `append`, of the value handed over.
#[allow(dead_code)]
fn told_by_an_owned<'a, V: VValue<C>, C: VCodec>(enc: &mut VerifiedEncoder<'a, V, C>, w: V)
    requires
        old(enc).wf(),
    ensures
        final(enc).wf(),
        final(enc)@.len() == old(enc)@.len() + 1,
        final(enc)@.last() == V::sv_of(w),
        final(enc)@.drop_last() == old(enc)@,
{
    enc.append_owned(w);
    assert(enc@.drop_last() =~= old(enc)@);
}

/// What `append_n_owned` says: as `append_n`, of the value handed over.
#[allow(dead_code)]
fn told_by_owned_n<'a, V: VValue<C>, C: VCodec>(enc: &mut VerifiedEncoder<'a, V, C>, w: V, n: usize)
    requires
        old(enc).wf(),
    ensures
        final(enc).wf(),
        final(enc)@.len() == old(enc)@.len() + n,
        final(enc)@.take(old(enc)@.len() as int) == old(enc)@,
        forall|i: int| old(enc)@.len() <= i < final(enc)@.len() ==> final(enc)@[i] == V::sv_of(w),
{
    enc.append_n_owned(w, n);
    assert(enc@.take(old(enc)@.len() as int) =~= old(enc)@);
}

/// What `extend` says of a vector handed over: its values, after those
/// the encoder had.
#[allow(dead_code)]
fn told_by_an_extend<'a, V: VValue<C>, C: VCodec>(enc: &mut VerifiedEncoder<'a, V, C>, vs: Vec<Get<'a, V>>)
    requires
        old(enc).wf(),
    ensures
        final(enc).wf(),
        final(enc)@ == old(enc)@ + VGet::svs(vs@),
{
    enc.extend(vs);
}

/// What an encoder's `save` says: the bytes are a saved column, of the
/// values given, the one encoding of them, and within the bound of
/// their size.
#[allow(dead_code)]
fn told_by_an_encoders_save<'a, V: VValue<C>, C: VCodec>(enc: VerifiedEncoder<'a, V, C>) -> (r: Vec<u8>)
    requires
        enc.wf(),
    ensures
        VerifiedColumn::<V, C>::valid(r@),
        VerifiedColumn::<V, C>::values(r@) == enc@,
        r@ == VerifiedColumn::<V, C>::encoding(enc@),
        r@.len() <= V::size_bound(enc@),
{
    enc.save()
}

/// What an encoder's `save_to` says: the buffer is what it was and then
/// the encoding of the values given, which is a saved column of them
/// within the bound of their size, and the range returned is where.
#[allow(dead_code)]
fn told_by_an_encoders_save_to<'a, V: VValue<C>, C: VCodec>(enc: VerifiedEncoder<'a, V, C>, out: &mut Vec<u8>) -> (r:
    Range<usize>)
    requires
        enc.wf(),
    ensures
        final(out)@ == old(out)@ + VerifiedColumn::<V, C>::encoding(enc@),
        VerifiedColumn::<V, C>::valid(VerifiedColumn::<V, C>::encoding(enc@)),
        VerifiedColumn::<V, C>::values(VerifiedColumn::<V, C>::encoding(enc@)) == enc@,
        VerifiedColumn::<V, C>::encoding(enc@).len() <= V::size_bound(enc@),
        r.start == old(out)@.len(),
        r.end == final(out)@.len(),
{
    enc.save_to(out)
}

/// Values appended one by one are saved as a column of them: the bytes
/// are a saved column, of those values, and the one encoding of them.
/// The loop carries `enc.wf()`, and what the encoder has been given so
/// far.
#[allow(dead_code)]
fn encoded_as_saved<'v, V: VValue<C>, C: VCodec>(input: &Vec<Get<'v, V>>) -> (r: Vec<u8>)
    ensures
        VerifiedColumn::<V, C>::valid(r@),
        VerifiedColumn::<V, C>::values(r@) == VGet::svs(input@),
        r@ == VerifiedColumn::<V, C>::encoding(VGet::svs(input@)),
        r@.len() <= V::size_bound(VGet::svs(input@)),
{
    let mut enc = VerifiedEncoder::<V, C>::new();
    let mut i: usize = 0;
    proof {
        VGet::lemma_svs(input@.take(0));
        assert(enc@ =~= VGet::svs(input@.take(0)));
    }
    while i < input.len()
        invariant
            enc.wf(),
            i <= input@.len(),
            enc@ == VGet::svs(input@.take(i as int)),
        decreases input@.len() - i,
    {
        enc.append(input[i]);
        proof {
            VGet::lemma_svs(input@.take(i as int));
            VGet::lemma_svs(input@.take(i + 1));
            assert(enc@ =~= VGet::svs(input@.take(i + 1)));
        }
        i += 1;
    }
    proof {
        assert(input@.take(i as int) =~= input@);
    }
    enc.save()
}

/// An encoder and a column save the same bytes for the same values.
#[allow(dead_code)]
fn encoder_or_column<'v, V: VValue<C>, C: VCodec>(input: &Vec<Get<'v, V>>) {
    let a = encoded_as_saved::<V, C>(input);
    let col = VerifiedColumn::<V, C>::from_values(input.as_slice());
    let b = col.save();
    assert(a@ == b@);
}

/// What an encoder saves loads as a column of the values given, where
/// their number and size are a column's to load.
#[allow(dead_code)]
fn encode_then_load<'v, V: VValue<C>, C: VCodec>(input: &Vec<Get<'v, V>>) -> (r: VerifiedColumn<V, C>)
    requires
        V::size_bound(VGet::svs(input@)) <= 0xffff_fff5,
        input@.len() <= usize::MAX - 3,
    ensures
        r@ == VGet::svs(input@),
{
    let bytes = encoded_as_saved::<V, C>(input);
    proof {
        VGet::lemma_svs(input@);
    }
    let back = VerifiedColumn::<V, C>::load(bytes.as_slice());
    assert(back is Ok);
    back.unwrap()
}

/// A column's values, given to an encoder by the column's iterator, are
/// saved as the column saves them.
#[allow(dead_code)]
fn reencoded<V: VValue<C>, C: VCodec>(col: &VerifiedColumn<V, C>) -> (r: Vec<u8>)
    ensures
        r@ == VerifiedColumn::<V, C>::encoding(col@),
{
    let mut enc = VerifiedEncoder::<V, C>::new();
    enc.extend(col.iter());
    let bytes = col.save();
    let r = enc.save();
    assert(r@ == bytes@);
    r
}

/// Runs given to an encoder with `append_n`, run by run: what is saved
/// holds each run's value as often as its count says.
#[allow(dead_code)]
fn encoded_by_runs<'v, V: VValue<C>, C: VCodec>(runs: &Vec<Run<Get<'v, V>>>) -> (r: Vec<u8>)
    ensures
        VerifiedColumn::<V, C>::valid(r@),
        VerifiedColumn::<V, C>::values(r@) == run_values(runs@),
{
    let mut enc = VerifiedEncoder::<V, C>::new();
    let mut i: usize = 0;
    proof {
        assert(enc@ =~= run_values(runs@.take(0)));
    }
    while i < runs.len()
        invariant
            enc.wf(),
            i <= runs@.len(),
            enc@ == run_values(runs@.take(i as int)),
        decreases runs@.len() - i,
    {
        let run = &runs[i];
        enc.append_n(run.value, run.count);
        proof {
            lemma_run_values_push(runs@.take(i as int), runs@[i as int]);
            assert(runs@.take(i + 1) =~= runs@.take(i as int).push(runs@[i as int]));
        }
        i += 1;
    }
    proof {
        assert(runs@.take(i as int) =~= runs@);
    }
    enc.save()
}

/// Numbers given in each of the ways an encoder takes them: what is
/// saved holds them in the order given, and the encoder counts them.
#[allow(dead_code)]
fn encode_numbers() -> (r: Vec<u8>)
    ensures
        VerifiedColumn::<u64>::valid(r@),
        VerifiedColumn::<u64>::values(r@) == seq![7u64, 7, 7, 9, 1, 2, 3],
{
    let mut enc = VerifiedEncoder::<u64>::new();
    let none = enc.is_empty();
    assert(none);
    enc.append(7);
    enc.append_n(7, 2);
    enc.append_owned(9);
    // no copy of a value is no value
    enc.append_n_owned(5, 0);
    let mut more: Vec<u64> = Vec::new();
    more.push(1);
    more.push(2);
    more.push(3);
    proof {
        <u64 as VGet>::lemma_svs(more@);
        assert(<u64 as VGet>::svs(more@) =~= seq![1u64, 2, 3]);
    }
    enc.extend(more);
    assert(enc@ =~= seq![7u64, 7, 7, 9, 1, 2, 3]);
    let n = enc.len();
    assert(n == 7);
    let none = enc.is_empty();
    assert(!none);
    enc.save()
}

/// Byte strings, borrowed and owned, with nulls and without: an encoder
/// takes each, and what is saved holds their bytes.
#[allow(dead_code)]
fn encode_strings<'a>(a: &'a [u8], b: Vec<u8>, c: Vec<u8>) -> (r: (Vec<u8>, Vec<u8>))
    ensures
        VerifiedColumn::<Vec<u8>>::values(r.0@) == seq![a@, b@, a@, a@],
        VerifiedColumn::<Option<Vec<u8>>>::values(r.1@) == seq![Some(a@), None, None, Some(c@)],
{
    let mut x = VerifiedEncoder::<Vec<u8>>::new();
    x.append(a);
    x.append_owned(b);
    x.append_n(a, 2);
    assert(x@ =~= seq![a@, b@, a@, a@]);
    let mut y = VerifiedEncoder::<Option<Vec<u8>>>::new();
    y.append(Some(a));
    y.append_n(None, 2);
    y.append_owned(Some(c));
    assert(y@ =~= seq![Some(a@), None, None, Some(c@)]);
    (x.save(), y.save())
}

/// A client's function that is handed encoders: it requires that they
/// are encoders (`wf()`), and says that they still are.
#[allow(dead_code)]
fn log_both<'a>(numbers: &mut VerifiedEncoder<'a, u64>, flags: &mut VerifiedEncoder<'a, bool>, x: u64, f: bool)
    requires
        old(numbers).wf(),
        old(flags).wf(),
    ensures
        final(numbers).wf(),
        final(flags).wf(),
        final(numbers)@ == old(numbers)@.push(x),
        final(flags)@ == old(flags)@.push(f),
{
    numbers.append(x);
    flags.append(f);
}

/// A client's writer of rows: an encoder for each column of a table.
pub struct Rows<'a> {
    pub ids: VerifiedEncoder<'a, u64>,
    pub names: VerifiedEncoder<'a, Vec<u8>>,
    pub flags: VerifiedEncoder<'a, bool>,
}

/// Where the three columns of a table were written.
pub struct Written {
    pub ids: Range<usize>,
    pub names: Range<usize>,
    pub flags: Range<usize>,
}

impl<'a> Rows<'a> {
    /// What the client keeps of its writer: the three are encoders, and
    /// have been given as many values each.
    pub open spec fn wf(&self) -> bool {
        &&& self.ids.wf()
        &&& self.names.wf()
        &&& self.flags.wf()
        &&& self.ids@.len() == self.names@.len()
        &&& self.ids@.len() == self.flags@.len()
    }

    /// A writer with no row.
    pub fn new() -> (r: Self)
        ensures
            r.wf(),
            r.ids@.len() == 0,
    {
        Rows { ids: VerifiedEncoder::new(), names: VerifiedEncoder::new(), flags: VerifiedEncoder::new() }
    }

    /// One row more.
    pub fn add(&mut self, id: u64, name: &'a [u8], flag: bool)
        requires
            old(self).wf(),
        ensures
            final(self).wf(),
            final(self).ids@ == old(self).ids@.push(id),
            final(self).names@ == old(self).names@.push(name@),
            final(self).flags@ == old(self).flags@.push(flag),
    {
        self.ids.append(id);
        self.names.append(name);
        self.flags.append(flag);
    }

    /// The table, written to `out` after what it holds: the three
    /// columns one after another, each where its range says, and each
    /// the encoding of the values its encoder was given, which is a
    /// saved column of them.
    pub fn finish(self, out: &mut Vec<u8>) -> (r: Written)
        requires
            self.wf(),
        ensures
            final(out)@.len() >= old(out)@.len(),
            final(out)@.take(old(out)@.len() as int) == old(out)@,
            r.ids.start == old(out)@.len(),
            r.ids.end == r.names.start,
            r.names.end == r.flags.start,
            r.flags.end == final(out)@.len(),
            r.ids.start <= r.ids.end <= r.names.end <= r.flags.end,
            final(out)@.subrange(r.ids.start as int, r.ids.end as int) == VerifiedColumn::<u64>::encoding(self.ids@),
            final(out)@.subrange(r.names.start as int, r.names.end as int) == VerifiedColumn::<Vec<u8>>::encoding(
                self.names@,
            ),
            final(out)@.subrange(r.flags.start as int, r.flags.end as int) == VerifiedColumn::<bool>::encoding(
                self.flags@,
            ),
            VerifiedColumn::<u64>::values(VerifiedColumn::<u64>::encoding(self.ids@)) == self.ids@,
            VerifiedColumn::<Vec<u8>>::values(VerifiedColumn::<Vec<u8>>::encoding(self.names@)) == self.names@,
            VerifiedColumn::<bool>::values(VerifiedColumn::<bool>::encoding(self.flags@)) == self.flags@,
    {
        let Rows { ids, names, flags } = self;
        let ghost (a, b, c) = (
            VerifiedColumn::<u64>::encoding(ids@),
            VerifiedColumn::<Vec<u8>>::encoding(names@),
            VerifiedColumn::<bool>::encoding(flags@),
        );
        let x = ids.save_to(out);
        let y = names.save_to(out);
        let z = flags.save_to(out);
        proof {
            let o = old(out)@;
            assert(out@ =~= o + a + b + c);
            assert(out@.take(o.len() as int) =~= o);
            assert(out@.subrange(x.start as int, x.end as int) =~= a);
            assert(out@.subrange(y.start as int, y.end as int) =~= b);
            assert(out@.subrange(z.start as int, z.end as int) =~= c);
        }
        Written { ids: x, names: y, flags: z }
    }
}

/// Two rows written, and the column of numbers read back from where the
/// writer says it is: if those bytes load, they load as the numbers
/// given.
#[allow(dead_code)]
fn rows_written<'a>(first: &'a [u8], second: &'a [u8], out: &mut Vec<u8>) -> (r: Option<VerifiedColumn<u64>>)
    ensures
        r matches Some(col) ==> col@ == seq![1u64, 2],
{
    let mut rows = Rows::new();
    rows.add(1, first, true);
    rows.add(2, second, false);
    assert(rows.ids@ =~= seq![1u64, 2]);
    let w = rows.finish(out);
    let bytes = vstd::slice::slice_subrange(out.as_slice(), w.ids.start, w.ids.end);
    match VerifiedColumn::<u64>::load(bytes) {
        Ok(col) => Some(col),
        Err(_) => None,
    }
}

// ── The encoder's segment budget ────────────────────────────────────────────
/// What `with_max_segments` says of the encoder it gives.
#[allow(dead_code)]
fn told_by_a_budget<'a, V: VValue<C>, C: VCodec>(max: usize) -> (r: VerifiedEncoder<'a, V, C>)
    requires
        max >= 2,
    ensures
        r.wf(),
        r@.len() == 0,
        r.max_segments() == Some(max),
{
    VerifiedEncoder::<V, C>::with_max_segments(max)
}

/// What `with_segments` says of the encoder it gives: its budget is the
/// one a column has that was given none.
#[allow(dead_code)]
fn told_by_the_default_budget<'a, V: VValue<C>, C: VCodec>() -> (r: VerifiedEncoder<'a, V, C>)
    ensures
        r.wf(),
        r@.len() == 0,
        r.max_segments() == Some(64usize),
{
    VerifiedEncoder::<V, C>::with_segments()
}

/// What `new` and `default` say of the budget: there is none.
#[allow(dead_code)]
fn told_by_no_budget<'a, V: VValue<C>, C: VCodec>() -> (r: (VerifiedEncoder<'a, V, C>, VerifiedEncoder<'a, V, C>))
    ensures
        r.0.max_segments() is None,
        r.1.max_segments() is None,
{
    let a = VerifiedEncoder::<V, C>::new();
    let b = VerifiedEncoder::<V, C>::default();
    (a, b)
}

/// What each way of giving values says of the budget: it is kept.
#[allow(dead_code)]
fn told_of_a_budget<'a, V: VValue<C>, C: VCodec>(
    enc: &mut VerifiedEncoder<'a, V, C>,
    v: Get<'a, V>,
    w: V,
    x: V,
    n: usize,
    vs: Vec<Get<'a, V>>,
)
    requires
        old(enc).wf(),
    ensures
        final(enc).wf(),
        final(enc).max_segments() == old(enc).max_segments(),
{
    enc.append(v);
    assert(enc.max_segments() == old(enc).max_segments());
    enc.append_n(v, n);
    assert(enc.max_segments() == old(enc).max_segments());
    enc.append_owned(w);
    assert(enc.max_segments() == old(enc).max_segments());
    enc.append_n_owned(x, n);
    assert(enc.max_segments() == old(enc).max_segments());
    enc.extend(vs);
}

/// What an encoder's `save_to_unless` says. Either the buffer is what
/// it was, and then every value given is `v`; or it is not, and then it
/// is what it was and after that the encoding of the values, a saved
/// column of them within the bound of their size. An encoder without a
/// budget leaves the buffer whenever every value is `v`. And the range
/// returned is what was added.
#[allow(dead_code)]
fn told_by_an_encoders_save_to_unless<'a, V: VValue<C>, C: VCodec>(
    enc: VerifiedEncoder<'a, V, C>,
    out: &mut Vec<u8>,
    v: Get<'a, V>,
) -> (r: Range<usize>)
    requires
        enc.wf(),
    ensures
        final(out)@ == old(out)@ ==> only(enc@, v.sv()),
        enc.max_segments() is None && only(enc@, v.sv()) ==> final(out)@ == old(out)@,
        final(out)@ != old(out)@ ==> {
            &&& final(out)@ == old(out)@ + VerifiedColumn::<V, C>::encoding(enc@)
            &&& VerifiedColumn::<V, C>::valid(VerifiedColumn::<V, C>::encoding(enc@))
            &&& VerifiedColumn::<V, C>::values(VerifiedColumn::<V, C>::encoding(enc@)) == enc@
            &&& VerifiedColumn::<V, C>::encoding(enc@).len() <= V::size_bound(enc@)
        },
        r.start == old(out)@.len(),
        r.end == final(out)@.len(),
{
    enc.save_to_unless(out, v)
}

/// What an encoder's `into_column` says: a column of the values given.
#[allow(dead_code)]
fn told_by_an_encoders_column<'a, V: VValue<C>, C: VCodec>(enc: VerifiedEncoder<'a, V, C>) -> (r: VerifiedColumn<V, C>)
    requires
        enc.wf(),
    ensures
        r@ == enc@,
{
    enc.into_column()
}

/// What the range of a `save_to_unless` tells its reader, who has the
/// range and not the encoder: an empty range, that every value was `v`
/// and nothing was written; another, that the bytes there are the
/// encoding of the values. This holds of an encoder with a budget and
/// of one without.
#[allow(dead_code)]
fn read_from_the_range<'a, V: VValue<C>, C: VCodec>(enc: VerifiedEncoder<'a, V, C>, out: &mut Vec<u8>, v: Get<'a, V>) -> (r:
    Range<usize>)
    requires
        enc.wf(),
    ensures
        r.start <= r.end <= final(out)@.len(),
        r.start == r.end ==> only(enc@, v.sv()) && final(out)@ == old(out)@,
        r.start < r.end ==> final(out)@.subrange(r.start as int, r.end as int) == VerifiedColumn::<V, C>::encoding(enc@),
{
    let r = enc.save_to_unless(out, v);
    proof {
        let d = VerifiedColumn::<V, C>::encoding(enc@);
        if out@ != old(out)@ {
            assert(out@.subrange(r.start as int, r.end as int) =~= d);
            assert(d.len() > 0) by {
                if d.len() == 0 {
                    assert(out@ =~= old(out)@);
                }
            }
        }
    }
    r
}

/// A column of zeros takes no bytes: an encoder (without a budget) that
/// was given nothing but zeros writes nothing when asked to save unless
/// all is zero.
#[allow(dead_code)]
fn zeros_take_no_bytes(n: usize, m: usize, out: &mut Vec<u8>)
    ensures
        final(out)@ == old(out)@,
{
    let mut enc = VerifiedEncoder::<u64>::new();
    enc.append_n(0, n);
    enc.append_n_owned(0, m);
    assert(only(enc@, 0u64));
    let r = enc.save_to_unless(out, 0);
    assert(r.start == r.end);
}

/// And one that was given another value too writes them all, whatever
/// its budget.
#[allow(dead_code)]
fn others_take_bytes(with: Option<usize>, out: &mut Vec<u8>) -> (r: Range<usize>)
    requires
        with matches Some(max) ==> max >= 2,
    ensures
        final(out)@ == old(out)@ + VerifiedColumn::<u64>::encoding(seq![0u64, 5]),
        VerifiedColumn::<u64>::values(final(out)@.subrange(r.start as int, r.end as int)) == seq![0u64, 5],
{
    let mut enc = match with {
        Some(max) => VerifiedEncoder::<u64>::with_max_segments(max),
        None => VerifiedEncoder::<u64>::new(),
    };
    enc.append(0);
    enc.append(5);
    assert(enc@ =~= seq![0u64, 5]);
    assert(!only(enc@, 0u64)) by {
        assert(enc@[1] == 5);
    }
    let r = enc.save_to_unless(out, 0);
    proof {
        let d = VerifiedColumn::<u64>::encoding(seq![0u64, 5]);
        assert(out@.subrange(r.start as int, r.end as int) =~= d);
    }
    r
}

/// An encoder that was given nothing writes nothing, with a budget or
/// without, whatever the value it is to save unless: no values are
/// saved as no bytes.
#[allow(dead_code)]
fn nothing_given_nothing_written<'a, V: VValue<C>, C: VCodec>(with: Option<usize>, out: &mut Vec<u8>, v: Get<'a, V>) -> (r:
    Range<usize>)
    requires
        with matches Some(max) ==> max >= 2,
    ensures
        final(out)@ == old(out)@,
        r.start == r.end,
{
    let enc = match with {
        Some(max) => VerifiedEncoder::<V, C>::with_max_segments(max),
        None => VerifiedEncoder::<V, C>::new(),
    };
    let r = enc.save_to_unless(out, v);
    proof {
        VerifiedColumn::<V, C>::lemma_no_bytes();
        VerifiedColumn::<V, C>::lemma_encoding(Seq::<u8>::empty());
        assert(out@ =~= old(out)@);
    }
    r
}

/// Values appended one by one to an encoder, with a budget or without,
/// and the column made of them: it holds those values. The loop
/// carries `enc.wf()` and what the encoder has been given, and nothing
/// of the budget.
#[allow(dead_code)]
fn encoded_into_column<'v, V: VValue<C>, C: VCodec>(input: &Vec<Get<'v, V>>, with: Option<usize>) -> (r: VerifiedColumn<
    V,
    C,
>)
    requires
        with matches Some(max) ==> max >= 2,
    ensures
        r@ == VGet::svs(input@),
{
    let mut enc = match with {
        Some(max) => VerifiedEncoder::<V, C>::with_max_segments(max),
        None => VerifiedEncoder::<V, C>::new(),
    };
    let mut i: usize = 0;
    proof {
        VGet::lemma_svs(input@.take(0));
        assert(enc@ =~= VGet::svs(input@.take(0)));
    }
    while i < input.len()
        invariant
            enc.wf(),
            i <= input@.len(),
            enc@ == VGet::svs(input@.take(i as int)),
        decreases input@.len() - i,
    {
        enc.append(input[i]);
        proof {
            VGet::lemma_svs(input@.take(i as int));
            VGet::lemma_svs(input@.take(i + 1));
            assert(enc@ =~= VGet::svs(input@.take(i + 1)));
        }
        i += 1;
    }
    proof {
        assert(input@.take(i as int) =~= input@);
    }
    enc.into_column()
}

/// An encoder's column is the column of its values however it is made:
/// with a budget, without one, and from the values at once, the three
/// columns have the same values and save the same bytes, which are what
/// the encoder saves.
#[allow(dead_code)]
fn one_column_three_ways<'v, V: VValue<C>, C: VCodec>(input: &Vec<Get<'v, V>>, max: usize) -> (r: Vec<u8>)
    requires
        max >= 2,
    ensures
        r@ == VerifiedColumn::<V, C>::encoding(VGet::svs(input@)),
{
    let a = encoded_into_column::<V, C>(input, Some(max));
    let b = encoded_into_column::<V, C>(input, None);
    let c = VerifiedColumn::<V, C>::from_values(input.as_slice());
    assert(a@ == b@ && b@ == c@);
    let x = a.save();
    let y = b.save();
    let z = c.save();
    assert(x@ == y@ && y@ == z@);
    let via = encoded_as_saved::<V, C>(input);
    assert(x@ == via@);
    x
}

/// An encoder with a budget saves what one without saves: the budget
/// is how the encoder keeps the values, and no part of what they are
/// saved as.
#[allow(dead_code)]
fn a_budget_saves_the_same<'a>(a: &'a [u8], n: usize) -> (r: (Vec<u8>, Vec<u8>))
    ensures
        r.0@ == r.1@,
{
    let mut x = VerifiedEncoder::<Option<Vec<u8>>>::new();
    let mut y = VerifiedEncoder::<Option<Vec<u8>>>::with_max_segments(2);
    x.append(Some(a));
    y.append(Some(a));
    x.append_n(None, n);
    y.append_n(None, n);
    assert(x@ == y@);
    (x.save(), y.save())
}

// ── The encoder's remap forms ───────────────────────────────────────────────
/// What an encoder's `save_to_and_remap` says. The buffer is what it
/// was and after that a saved column, the encoding of its values and
/// within the bound of their size; those are as many values as were
/// given, each what the function gave for the value given in its place.
/// And the range returned is what was added.
#[allow(dead_code)]
fn told_by_a_remapping_save<'a, V: VValue<C>, C: VCodec, F: Fn(V) -> V>(
    enc: VerifiedEncoder<'a, V, C>,
    out: &mut Vec<u8>,
    f: F,
) -> (r: Range<usize>)
    requires
        enc.wf(),
        forall|v: V| enc@.contains(V::sv_of(v)) ==> #[trigger] call_requires(f, (v,)),
    ensures
        ({
            let d = final(out)@.skip(old(out)@.len() as int);
            let w = VerifiedColumn::<V, C>::values(d);
            &&& final(out)@ == old(out)@ + d
            &&& VerifiedColumn::<V, C>::valid(d)
            &&& d == VerifiedColumn::<V, C>::encoding(w)
            &&& d.len() <= V::size_bound(w)
            &&& w.len() == enc@.len()
            &&& forall|i: int| 0 <= i < enc@.len() ==> mapped(f, enc@[i], #[trigger] w[i])
        }),
        r.start == old(out)@.len(),
        r.end == final(out)@.len(),
{
    enc.save_to_and_remap(out, f)
}

/// What an encoder's `save_to_unless_and_remap` says. Either the
/// buffer is what it was, and then the function gave `v` for every
/// value given; or it is not, and then all that `save_to_and_remap`
/// says holds. A function that can give nothing but `v` for the values
/// given leaves the buffer, whether the encoder has a budget or not.
/// And the range returned is what was added.
#[allow(dead_code)]
fn told_by_a_remapping_save_unless<'a, V: VValue<C>, C: VCodec, F: Fn(V) -> V>(
    enc: VerifiedEncoder<'a, V, C>,
    out: &mut Vec<u8>,
    v: Get<'a, V>,
    f: F,
) -> (r: Range<usize>)
    requires
        enc.wf(),
        forall|x: V| enc@.contains(V::sv_of(x)) ==> #[trigger] call_requires(f, (x,)),
    ensures
        final(out)@ == old(out)@ ==> forall|i: int| 0 <= i < enc@.len() ==> mapped(f, #[trigger] enc@[i], v.sv()),
        (forall|i: int, b: V::S| 0 <= i < enc@.len() && #[trigger] mapped(f, enc@[i], b) ==> b == v.sv())
            ==> final(out)@ == old(out)@,
        final(out)@ != old(out)@ ==> {
            let d = final(out)@.skip(old(out)@.len() as int);
            let w = VerifiedColumn::<V, C>::values(d);
            &&& final(out)@ == old(out)@ + d
            &&& VerifiedColumn::<V, C>::valid(d)
            &&& d == VerifiedColumn::<V, C>::encoding(w)
            &&& d.len() <= V::size_bound(w)
            &&& w.len() == enc@.len()
            &&& forall|i: int| 0 <= i < enc@.len() ==> mapped(f, enc@[i], #[trigger] w[i])
        },
        r.start == old(out)@.len(),
        r.end == final(out)@.len(),
{
    enc.save_to_unless_and_remap(out, v, f)
}

/// An encoder saved through a function that gives each value back
/// saves what `save_to` saves: the encoding of the values given. For
/// every value type and codec, with a budget or without.
#[allow(dead_code)]
fn saved_as_it_is<'a, V: VValue<C>, C: VCodec>(enc: VerifiedEncoder<'a, V, C>, out: &mut Vec<u8>) -> (r: Range<usize>)
    requires
        enc.wf(),
    ensures
        final(out)@ == old(out)@ + VerifiedColumn::<V, C>::encoding(enc@),
        r.start == old(out)@.len(),
        r.end == final(out)@.len(),
{
    let f = |v: V| -> (w: V)
        ensures
            w == v,
        { v };
    let r = enc.save_to_and_remap(out, f);
    proof {
        let w = VerifiedColumn::<V, C>::values(out@.skip(old(out)@.len() as int));
        assert(w =~= enc@);
    }
    r
}

/// Numbers doubled as they are saved, by a function that does not take
/// every number: the client has to show that it takes those given, and
/// no more. If what was written loads, it loads as the doubles.
#[allow(dead_code)]
fn doubled_as_saved(input: &Vec<u64>, with: Option<usize>, out: &mut Vec<u8>) -> (r: Option<VerifiedColumn<u64>>)
    requires
        with matches Some(max) ==> max >= 2,
        forall|i: int| 0 <= i < input@.len() ==> input@[i] <= 1000,
    ensures
        r matches Some(col) ==> {
            &&& col@.len() == input@.len()
            &&& forall|i: int| 0 <= i < input@.len() ==> col@[i] == 2 * input@[i]
        },
{
    let mut enc = match with {
        Some(max) => VerifiedEncoder::<u64>::with_max_segments(max),
        None => VerifiedEncoder::<u64>::new(),
    };
    let mut i: usize = 0;
    while i < input.len()
        invariant
            enc.wf(),
            i <= input@.len(),
            enc@ == input@.take(i as int),
        decreases input@.len() - i,
    {
        enc.append(input[i]);
        proof {
            assert(enc@ =~= input@.take(i + 1));
        }
        i += 1;
    }
    proof {
        assert(input@.take(i as int) =~= input@);
    }
    let f = |v: u64| -> (w: u64)
        requires
            v <= 1000,
        ensures
            w == 2 * v,
        { 2 * v };
    let r = enc.save_to_and_remap(out, f);
    let bytes = vstd::slice::slice_subrange(out.as_slice(), r.start, r.end);
    proof {
        assert(bytes@ =~= out@.skip(old(out)@.len() as int));
    }
    match VerifiedColumn::<u64>::load(bytes) {
        Ok(col) => {
            proof {
                assert forall|i: int| 0 <= i < input@.len() implies col@[i] == 2 * input@[i] by {
                    assert(mapped(f, enc@[i], col@[i]));
                }
            }
            Some(col)
        },
        Err(_) => None,
    }
}

/// The client's own type, saved through a function that has a name.
#[allow(dead_code)]
fn stepped_as_saved(enc: VerifiedEncoder<'_, Action>, out: &mut Vec<u8>) -> (r: Range<usize>)
    requires
        enc.wf(),
    ensures
        r.start <= r.end <= final(out)@.len(),
        ({
            let w = VerifiedColumn::<Action>::values(final(out)@.subrange(r.start as int, r.end as int));
            &&& w.len() == enc@.len()
            &&& forall|i: int| 0 <= i < enc@.len() ==> w[i] == after(enc@[i])
        }),
{
    let r = enc.save_to_and_remap(out, next_action);
    proof {
        let d = out@.skip(old(out)@.len() as int);
        let w = VerifiedColumn::<Action>::values(d);
        assert(out@.subrange(r.start as int, r.end as int) =~= d);
        assert forall|i: int| 0 <= i < enc@.len() implies w[i] == after(enc@[i]) by {
            assert(mapped(next_action, enc@[i], w[i]));
        }
    }
    r
}

/// `bool`s turned over and byte strings made a byte longer, as two
/// encoders are saved one after the other into one buffer; each is
/// found again by its range.
#[allow(dead_code)]
fn turned_and_longer<'a>(
    flags: VerifiedEncoder<'a, bool>,
    names: VerifiedEncoder<'a, Vec<u8>>,
    b: u8,
    out: &mut Vec<u8>,
) -> (r: (Range<usize>, Range<usize>))
    requires
        flags.wf(),
        names.wf(),
    ensures
        r.0.start <= r.0.end <= r.1.start <= r.1.end <= final(out)@.len(),
        ({
            let x = VerifiedColumn::<bool>::values(final(out)@.subrange(r.0.start as int, r.0.end as int));
            let y = VerifiedColumn::<Vec<u8>>::values(final(out)@.subrange(r.1.start as int, r.1.end as int));
            &&& x.len() == flags@.len()
            &&& forall|i: int| 0 <= i < flags@.len() ==> x[i] == !flags@[i]
            &&& y.len() == names@.len()
            &&& forall|i: int| 0 <= i < names@.len() ==> y[i] == names@[i].push(b)
        }),
{
    let f = |v: bool| -> (w: bool)
        ensures
            w == !v,
        { !v };
    let g = |v: Vec<u8>| -> (w: Vec<u8>)
        ensures
            w@ == v@.push(b),
        {
            let mut v = v;
            v.push(b);
            v
        };
    let ghost o = out@;
    let p = flags.save_to_and_remap(out, f);
    let ghost mid = out@;
    let q = names.save_to_and_remap(out, g);
    proof {
        let d = mid.skip(o.len() as int);
        let e = out@.skip(mid.len() as int);
        let x = VerifiedColumn::<bool>::values(d);
        let y = VerifiedColumn::<Vec<u8>>::values(e);
        assert(out@.subrange(p.start as int, p.end as int) =~= d);
        assert(out@.subrange(q.start as int, q.end as int) =~= e);
        assert forall|i: int| 0 <= i < flags@.len() implies x[i] == !flags@[i] by {
            assert(mapped(f, flags@[i], x[i]));
        }
        assert forall|i: int| 0 <= i < names@.len() implies y[i] == names@[i].push(b) by {
            assert(mapped(g, names@[i], y[i]));
        }
    }
    (p, q)
}

/// Numbers all made zero take no bytes: an encoder saved through a
/// function that gives zero for every number writes nothing when asked
/// to save unless all is zero. That holds whatever the encoder was
/// given and whatever its budget, which a plain `save_to_unless` does
/// not say of an encoder with one.
#[allow(dead_code)]
fn zeroed_takes_no_bytes(enc: VerifiedEncoder<'_, u64>, out: &mut Vec<u8>)
    requires
        enc.wf(),
    ensures
        final(out)@ == old(out)@,
{
    let f = |v: u64| -> (w: u64)
        ensures
            w == 0,
        { 0 };
    let r = enc.save_to_unless_and_remap(out, 0, f);
    assert(r.start == r.end);
}

/// And so do byte strings all made null.
#[allow(dead_code)]
fn nulled_takes_no_bytes<'a>(enc: VerifiedEncoder<'a, Option<Vec<u8>>>, out: &mut Vec<u8>)
    requires
        enc.wf(),
    ensures
        final(out)@ == old(out)@,
{
    let f = |v: Option<Vec<u8>>| -> (w: Option<Vec<u8>>)
        ensures
            w is None,
        { None };
    enc.save_to_unless_and_remap(out, None, f);
}

/// Where the function gives each value back, and one of them is not
/// the value named, the values are written: all of them, as `save_to`
/// writes them. For every value type and codec.
#[allow(dead_code)]
fn kept_and_written<'a, V: VValue<C>, C: VCodec>(enc: VerifiedEncoder<'a, V, C>, out: &mut Vec<u8>, v: Get<'a, V>) -> (r:
    Range<usize>)
    requires
        enc.wf(),
        !only(enc@, v.sv()),
    ensures
        final(out)@ == old(out)@ + VerifiedColumn::<V, C>::encoding(enc@),
        final(out)@ != old(out)@,
{
    let f = |x: V| -> (w: V)
        ensures
            w == x,
        { x };
    let r = enc.save_to_unless_and_remap(out, v, f);
    proof {
        if out@ == old(out)@ {
            assert forall|i: int| 0 <= i < enc@.len() implies enc@[i] == v.sv() by {
                assert(mapped(f, enc@[i], v.sv()));
            }
        }
        let w = VerifiedColumn::<V, C>::values(out@.skip(old(out)@.len() as int));
        assert(w =~= enc@);
    }
    r
}

/// What the range of a `save_to_unless_and_remap` tells its reader,
/// who has the range and not the encoder: an empty range, that the
/// function gave `v` for every value and nothing was written; another,
/// that the bytes there are a saved column of what the function gave.
#[allow(dead_code)]
fn read_from_the_remapped_range<'a, V: VValue<C>, C: VCodec, F: Fn(V) -> V>(
    enc: VerifiedEncoder<'a, V, C>,
    out: &mut Vec<u8>,
    v: Get<'a, V>,
    f: F,
) -> (r: Range<usize>)
    requires
        enc.wf(),
        forall|x: V| enc@.contains(V::sv_of(x)) ==> #[trigger] call_requires(f, (x,)),
    ensures
        r.start <= r.end <= final(out)@.len(),
        r.start == r.end ==> final(out)@ == old(out)@ && forall|i: int|
            0 <= i < enc@.len() ==> mapped(f, #[trigger] enc@[i], v.sv()),
        r.start < r.end ==> {
            let d = final(out)@.subrange(r.start as int, r.end as int);
            let w = VerifiedColumn::<V, C>::values(d);
            &&& VerifiedColumn::<V, C>::valid(d)
            &&& w.len() == enc@.len()
            &&& forall|i: int| 0 <= i < enc@.len() ==> mapped(f, enc@[i], #[trigger] w[i])
        },
{
    let r = enc.save_to_unless_and_remap(out, v, f);
    proof {
        if out@ != old(out)@ {
            let d = out@.skip(old(out)@.len() as int);
            assert(out@.subrange(r.start as int, r.end as int) =~= d);
            assert(d.len() > 0) by {
                if d.len() == 0 {
                    assert(out@ =~= old(out)@);
                }
            }
        }
    }
    r
}

// ── The state that writes into its caller's buffer ──────────────────────────
/// What `new` and `default` say of the state they give: it was given
/// nothing, and may start on any buffer, all of which is then what was
/// there before it.
#[allow(dead_code)]
fn told_by_a_new_state<'a, V: VValue<C>, C: VCodec>() -> (r: (
    VerifiedEncoderState<'a, V, C>,
    VerifiedEncoderState<'a, V, C>,
))
    ensures
        r.0@.len() == 0,
        r.1@.len() == 0,
        forall|buf: Seq<u8>| #[trigger] r.0.on(buf),
        forall|buf: Seq<u8>| #[trigger] r.0.before(buf) == buf,
        forall|buf: Seq<u8>| #[trigger] r.1.on(buf),
        forall|buf: Seq<u8>| #[trigger] r.1.before(buf) == buf,
{
    let a = VerifiedEncoderState::<V, C>::new();
    let b = VerifiedEncoderState::<V, C>::default();
    (a, b)
}

/// What a state's `len` and `is_empty` say: the number of values it
/// was given. They take no buffer, and require that there is one the
/// state is on.
#[allow(dead_code)]
fn told_by_a_states_count<'a, V: VValue<C>, C: VCodec>(s: &VerifiedEncoderState<'a, V, C>, buf: &Vec<u8>) -> (r: (
    usize,
    bool,
))
    requires
        s.on(buf@),
    ensures
        r.0 == s@.len(),
        r.1 == (s@.len() == 0),
{
    (s.len(), s.is_empty())
}

/// What a state's `append` says: the value, after those the state had;
/// the buffer is still the state's, and what was there before the state
/// is what was.
#[allow(dead_code)]
fn told_by_a_states_append<'a, V: VValue<C>, C: VCodec>(
    s: &mut VerifiedEncoderState<'a, V, C>,
    buf: &mut Vec<u8>,
    v: Get<'a, V>,
)
    requires
        old(s).on(old(buf)@),
    ensures
        final(s).on(final(buf)@),
        final(s).before(final(buf)@) == old(s).before(old(buf)@),
        final(s)@.len() == old(s)@.len() + 1,
        final(s)@.last() == v.sv(),
        final(s)@.drop_last() == old(s)@,
{
    s.append(buf, v);
    assert(s@.drop_last() =~= old(s)@);
}

/// What a state's `append_n` says: the value `n` times, after those the
/// state had.
#[allow(dead_code)]
fn told_by_a_states_append_n<'a, V: VValue<C>, C: VCodec>(
    s: &mut VerifiedEncoderState<'a, V, C>,
    buf: &mut Vec<u8>,
    v: Get<'a, V>,
    n: usize,
)
    requires
        old(s).on(old(buf)@),
    ensures
        final(s).on(final(buf)@),
        final(s).before(final(buf)@) == old(s).before(old(buf)@),
        final(s)@.len() == old(s)@.len() + n,
        final(s)@.take(old(s)@.len() as int) == old(s)@,
        forall|i: int| old(s)@.len() <= i < final(s)@.len() ==> final(s)@[i] == v.sv(),
{
    s.append_n(buf, v, n);
    assert(s@.take(old(s)@.len() as int) =~= old(s)@);
}

/// What a state's `append_owned` and `append_n_owned` say: as `append`
/// and `append_n`, of the value handed over.
#[allow(dead_code)]
fn told_by_a_states_owned<'a, V: VValue<C>, C: VCodec>(
    s: &mut VerifiedEncoderState<'a, V, C>,
    buf: &mut Vec<u8>,
    v: V,
    w: V,
    n: usize,
)
    requires
        old(s).on(old(buf)@),
    ensures
        final(s).on(final(buf)@),
        final(s).before(final(buf)@) == old(s).before(old(buf)@),
        final(s)@.len() == old(s)@.len() + 1 + n,
        final(s)@.take(old(s)@.len() as int) == old(s)@,
        final(s)@[old(s)@.len() as int] == V::sv_of(v),
        forall|i: int| old(s)@.len() < i < final(s)@.len() ==> final(s)@[i] == V::sv_of(w),
{
    s.append_owned(buf, v);
    s.append_n_owned(buf, w, n);
    assert(s@.take(old(s)@.len() as int) =~= old(s)@);
}

/// What a state's `extend` says of a vector handed over: its values,
/// after those the state had.
#[allow(dead_code)]
fn told_by_a_states_extend<'a, V: VValue<C>, C: VCodec>(
    s: &mut VerifiedEncoderState<'a, V, C>,
    buf: &mut Vec<u8>,
    vs: Vec<Get<'a, V>>,
)
    requires
        old(s).on(old(buf)@),
    ensures
        final(s).on(final(buf)@),
        final(s).before(final(buf)@) == old(s).before(old(buf)@),
        final(s)@ == old(s)@ + VGet::svs(vs@),
{
    s.extend(buf, vs);
}

/// What `is_single_run_of` says: whether every value given is the one
/// asked for, either way.
#[allow(dead_code)]
fn told_by_a_single_run<'a, V: VValue<C>, C: VCodec>(
    s: &VerifiedEncoderState<'a, V, C>,
    buf: &Vec<u8>,
    v: Get<'a, V>,
) -> (r: bool)
    requires
        s.on(buf@),
    ensures
        r ==> forall|i: int| 0 <= i < s@.len() ==> s@[i] == v.sv(),
        !r ==> exists|i: int| 0 <= i < s@.len() && s@[i] != v.sv(),
{
    s.is_single_run_of(v)
}

/// What a state's `finish` says: the buffer is what was there before
/// the state and then the encoding of the values given, which is a
/// saved column of them within the bound of their size.
#[allow(dead_code)]
fn told_by_a_finish<'a, V: VValue<C>, C: VCodec>(s: VerifiedEncoderState<'a, V, C>, buf: &mut Vec<u8>)
    requires
        s.on(old(buf)@),
    ensures
        final(buf)@ == s.before(old(buf)@) + VerifiedColumn::<V, C>::encoding(s@),
        VerifiedColumn::<V, C>::valid(VerifiedColumn::<V, C>::encoding(s@)),
        VerifiedColumn::<V, C>::values(VerifiedColumn::<V, C>::encoding(s@)) == s@,
        VerifiedColumn::<V, C>::encoding(s@).len() <= V::size_bound(s@),
{
    s.finish(buf);
}

/// What `lemma_before` says: what was in the buffer before the state
/// is still its start.
#[allow(dead_code)]
proof fn told_by_the_bytes_before<'a, V: VValue<C>, C: VCodec>(s: VerifiedEncoderState<'a, V, C>, buf: Seq<u8>)
    requires
        s.on(buf),
    ensures
        s.before(buf).len() <= buf.len(),
        forall|i: int| 0 <= i < s.before(buf).len() ==> buf[i] == s.before(buf)[i],
{
    s.lemma_before(buf);
    assert forall|i: int| 0 <= i < s.before(buf).len() implies buf[i] == s.before(buf)[i] by {
        assert(buf.take(s.before(buf).len() as int)[i] == buf[i]);
    }
}

/// Values given one by one to a state are written as a column of them:
/// the buffer is what it was and then the one encoding of the values.
/// The loop carries that the buffer is the state's (`on`), what was
/// there before the state, and what the state has been given so far.
#[allow(dead_code)]
fn streamed_as_saved<'v, V: VValue<C>, C: VCodec>(input: &Vec<Get<'v, V>>, buf: &mut Vec<u8>) -> (r: Range<usize>)
    ensures
        final(buf)@ == old(buf)@ + VerifiedColumn::<V, C>::encoding(VGet::svs(input@)),
        r.start == old(buf)@.len(),
        r.end == final(buf)@.len(),
        VerifiedColumn::<V, C>::valid(final(buf)@.subrange(r.start as int, r.end as int)),
        VerifiedColumn::<V, C>::values(final(buf)@.subrange(r.start as int, r.end as int)) == VGet::svs(input@),
{
    let start = buf.len();
    let mut s = VerifiedEncoderState::<V, C>::new();
    let mut i: usize = 0;
    proof {
        VGet::lemma_svs(input@.take(0));
        assert(s@ =~= VGet::svs(input@.take(0)));
    }
    while i < input.len()
        invariant
            s.on(buf@),
            s.before(buf@) == old(buf)@,
            i <= input@.len(),
            s@ == VGet::svs(input@.take(i as int)),
        decreases input@.len() - i,
    {
        s.append(buf, input[i]);
        proof {
            VGet::lemma_svs(input@.take(i as int));
            VGet::lemma_svs(input@.take(i + 1));
            assert(s@ =~= VGet::svs(input@.take(i + 1)));
        }
        i += 1;
    }
    proof {
        assert(input@.take(i as int) =~= input@);
    }
    s.finish(buf);
    proof {
        let d = VerifiedColumn::<V, C>::encoding(VGet::svs(input@));
        assert(buf@.subrange(start as int, buf@.len() as int) =~= d);
    }
    start..buf.len()
}

/// A state, an encoder, a column and `encode` give the same bytes for
/// the same values.
#[allow(dead_code)]
fn one_encoding_four_ways<'v, V: VValue<C>, C: VCodec>(input: Vec<Get<'v, V>>) -> (r: Vec<u8>)
    ensures
        r@ == VerifiedColumn::<V, C>::encoding(VGet::svs(input@)),
{
    let mut a: Vec<u8> = Vec::new();
    streamed_as_saved::<V, C>(&input, &mut a);
    let b = encoded_as_saved::<V, C>(&input);
    let col = VerifiedColumn::<V, C>::from_values(input.as_slice());
    let c = col.save();
    let d = VerifiedEncoder::<V, C>::encode(input);
    assert(a@ =~= b@);
    assert(b@ == c@);
    assert(c@ == d@);
    d
}

/// What `encode` gives loads as a column of the values given, where
/// their number and size are a column's to load.
#[allow(dead_code)]
fn stream_then_load<'v, V: VValue<C>, C: VCodec>(input: Vec<Get<'v, V>>) -> (r: VerifiedColumn<V, C>)
    requires
        V::size_bound(VGet::svs(input@)) <= 0xffff_fff5,
        input@.len() <= usize::MAX - 3,
    ensures
        r@ == VGet::svs(input@),
{
    proof {
        VGet::lemma_svs(input@);
    }
    let bytes = VerifiedEncoder::<V, C>::encode(input);
    let back = VerifiedColumn::<V, C>::load(bytes.as_slice());
    assert(back is Ok);
    back.unwrap()
}

/// Byte strings, borrowed and owned, with nulls and without, through
/// two states one after the other into one buffer: it holds what it
/// did, then the first column, then the second.
#[allow(dead_code)]
fn strings_streamed<'a>(a: &'a [u8], b: Vec<u8>, c: Vec<u8>, buf: &mut Vec<u8>) -> (r: usize)
    ensures
        final(buf)@ == old(buf)@ + VerifiedColumn::<Vec<u8>>::encoding(seq![a@, b@, a@, a@]) + VerifiedColumn::<
            Option<Vec<u8>>,
        >::encoding(seq![Some(a@), None, None, Some(c@)]),
        r == old(buf)@.len() + VerifiedColumn::<Vec<u8>>::encoding(seq![a@, b@, a@, a@]).len(),
{
    let mut x = VerifiedEncoderState::<Vec<u8>>::new();
    x.append(buf, a);
    x.append_owned(buf, b);
    x.append_n(buf, a, 2);
    assert(x@ =~= seq![a@, b@, a@, a@]);
    x.finish(buf);
    let mid = buf.len();
    let mut y = VerifiedEncoderState::<Option<Vec<u8>>>::new();
    y.append(buf, Some(a));
    y.append_n(buf, None, 2);
    y.append_owned(buf, Some(c));
    assert(y@ =~= seq![Some(a@), None, None, Some(c@)]);
    y.finish(buf);
    mid
}

/// A state knows whether all it was given is one value: so it is of a
/// state that was given nothing, whatever the value, and of one run,
/// and no longer once another value came. And it counts what it was
/// given.
#[allow(dead_code)]
fn a_single_run(buf: &mut Vec<u8>)
    ensures
        final(buf)@ == old(buf)@ + VerifiedColumn::<u64>::encoding(seq![7u64, 7, 7, 7, 8]),
{
    let mut s = VerifiedEncoderState::<u64>::new();
    assert(s.on(buf@));
    let none = s.is_single_run_of(3);
    assert(none);
    let empty = s.is_empty();
    assert(empty);
    s.append_n(buf, 7, 4);
    assert(s@ =~= seq![7u64, 7, 7, 7]);
    let sevens = s.is_single_run_of(7);
    assert(sevens);
    let eights = s.is_single_run_of(8);
    assert(!eights) by {
        assert(s@[0] == 7);
    }
    s.append(buf, 8);
    assert(s@ =~= seq![7u64, 7, 7, 7, 8]);
    let still = s.is_single_run_of(7);
    assert(!still) by {
        assert(s@[4] == 8);
    }
    let n = s.len();
    assert(n == 5);
    s.finish(buf);
}

/// A client's log of numbers: a buffer, and the state that writes into
/// it. The client keeps the two together between calls.
pub struct Log<'a> {
    pub buf: Vec<u8>,
    pub s: VerifiedEncoderState<'a, u64>,
}

impl<'a> Log<'a> {
    /// What the client keeps of its log: the buffer is the state's.
    pub open spec fn wf(&self) -> bool {
        self.s.on(self.buf@)
    }

    /// What the buffer held when the log was made.
    pub open spec fn head(&self) -> Seq<u8> {
        self.s.before(self.buf@)
    }

    /// A log with no number, after what `buf` holds.
    pub fn new(buf: Vec<u8>) -> (r: Self)
        ensures
            r.wf(),
            r.s@.len() == 0,
            r.head() == buf@,
    {
        Log { buf, s: VerifiedEncoderState::new() }
    }

    /// One number more.
    pub fn add(&mut self, x: u64)
        requires
            old(self).wf(),
        ensures
            final(self).wf(),
            final(self).head() == old(self).head(),
            final(self).s@ == old(self).s@.push(x),
    {
        self.s.append(&mut self.buf, x);
    }

    /// The buffer: what it held when the log was made, and then the
    /// numbers as a saved column.
    pub fn finish(self) -> (r: Vec<u8>)
        requires
            self.wf(),
        ensures
            r@ == self.head() + VerifiedColumn::<u64>::encoding(self.s@),
            VerifiedColumn::<u64>::values(VerifiedColumn::<u64>::encoding(self.s@)) == self.s@,
    {
        let Log { mut buf, s } = self;
        s.finish(&mut buf);
        buf
    }
}

/// Three numbers logged after a header, and read back from behind it:
/// if those bytes load, they load as the numbers.
#[allow(dead_code)]
fn logged(header: Vec<u8>) -> (r: Option<VerifiedColumn<u64>>)
    ensures
        r matches Some(col) ==> col@ == seq![4u64, 5, 6],
{
    let at = header.len();
    let ghost h = header@;
    let mut log = Log::new(header);
    log.add(4);
    log.add(5);
    log.add(6);
    assert(log.s@ =~= seq![4u64, 5, 6]);
    let out = log.finish();
    proof {
        let d = VerifiedColumn::<u64>::encoding(seq![4u64, 5, 6]);
        assert(out@ =~= h + d);
        assert(out@.subrange(at as int, out@.len() as int) =~= d);
    }
    let bytes = vstd::slice::slice_subrange(out.as_slice(), at, out.len());
    match VerifiedColumn::<u64>::load(bytes) {
        Ok(col) => Some(col),
        Err(_) => None,
    }
}

/// A client gives up what a state wrote: it cuts the buffer back to
/// where the state started, and the buffer is what it was. That needs
/// `lemma_before`: what was there before the state is still there.
#[allow(dead_code)]
fn given_up<'a, V: VValue<C>, C: VCodec>(vs: Vec<Get<'a, V>>, buf: &mut Vec<u8>)
    ensures
        final(buf)@ == old(buf)@,
{
    let start = buf.len();
    let mut s = VerifiedEncoderState::<V, C>::new();
    s.extend(buf, vs);
    proof {
        s.lemma_before(buf@);
    }
    buf.truncate(start);
    assert(buf@ =~= old(buf)@);
}

/// A client's own `encode_to_unless`, of the state's functions alone:
/// the values are written unless each is `v`, and the client is told
/// which.
#[allow(dead_code)]
fn written_unless_one<'a, V: VValue<C>, C: VCodec>(vs: Vec<Get<'a, V>>, v: Get<'a, V>, buf: &mut Vec<u8>) -> (r: bool)
    ensures
        r == only(VGet::svs(vs@), v.sv()),
        r ==> final(buf)@ == old(buf)@,
        !r ==> final(buf)@ == old(buf)@ + VerifiedColumn::<V, C>::encoding(VGet::svs(vs@)),
{
    let start = buf.len();
    let mut s = VerifiedEncoderState::<V, C>::new();
    s.extend(buf, vs);
    assert(s@ =~= VGet::svs(vs@));
    if s.is_single_run_of(v) {
        proof {
            s.lemma_before(buf@);
        }
        buf.truncate(start);
        assert(buf@ =~= old(buf)@);
        true
    } else {
        s.finish(buf);
        false
    }
}

// ── The static forms ────────────────────────────────────────────────────────
/// What `encode` says of a vector handed over: the bytes are a saved
/// column of its values, the one encoding of them, within the bound of
/// their size.
#[allow(dead_code)]
fn told_by_an_encode<'a, V: VValue<C>, C: VCodec>(vs: Vec<Get<'a, V>>) -> (r: Vec<u8>)
    ensures
        VerifiedColumn::<V, C>::valid(r@),
        VerifiedColumn::<V, C>::values(r@) == VGet::svs(vs@),
        r@ == VerifiedColumn::<V, C>::encoding(VGet::svs(vs@)),
        r@.len() <= V::size_bound(VGet::svs(vs@)),
{
    VerifiedEncoder::<V, C>::encode(vs)
}

/// What `encode_to` says of a vector handed over: the buffer is what it
/// was and then the encoding of the vector's values, which is a saved
/// column of them within the bound of their size, and the range
/// returned is where.
#[allow(dead_code)]
fn told_by_an_encode_to<'a, V: VValue<C>, C: VCodec>(buf: &mut Vec<u8>, vs: Vec<Get<'a, V>>) -> (r: Range<usize>)
    ensures
        final(buf)@ == old(buf)@ + VerifiedColumn::<V, C>::encoding(VGet::svs(vs@)),
        VerifiedColumn::<V, C>::valid(VerifiedColumn::<V, C>::encoding(VGet::svs(vs@))),
        VerifiedColumn::<V, C>::values(VerifiedColumn::<V, C>::encoding(VGet::svs(vs@))) == VGet::svs(vs@),
        VerifiedColumn::<V, C>::encoding(VGet::svs(vs@)).len() <= V::size_bound(VGet::svs(vs@)),
        r.start == old(buf)@.len(),
        r.end == final(buf)@.len(),
{
    VerifiedEncoder::<V, C>::encode_to(buf, vs)
}

/// What `encode_to_unless` says of a vector handed over. The buffer is
/// what it was exactly if every value is `v`; if one is not, it is
/// what it was and after that the encoding of the values, a saved
/// column of them within the bound of their size. And the range
/// returned is what was added.
#[allow(dead_code)]
fn told_by_an_encode_to_unless<'a, V: VValue<C>, C: VCodec>(buf: &mut Vec<u8>, vs: Vec<Get<'a, V>>, v: Get<'a, V>) -> (r:
    Range<usize>)
    ensures
        (final(buf)@ == old(buf)@) == only(VGet::svs(vs@), v.sv()),
        final(buf)@ != old(buf)@ ==> {
            &&& final(buf)@ == old(buf)@ + VerifiedColumn::<V, C>::encoding(VGet::svs(vs@))
            &&& VerifiedColumn::<V, C>::valid(VerifiedColumn::<V, C>::encoding(VGet::svs(vs@)))
            &&& VerifiedColumn::<V, C>::values(VerifiedColumn::<V, C>::encoding(VGet::svs(vs@))) == VGet::svs(vs@)
            &&& VerifiedColumn::<V, C>::encoding(VGet::svs(vs@)).len() <= V::size_bound(VGet::svs(vs@))
        },
        r.start == old(buf)@.len(),
        r.end == final(buf)@.len(),
{
    VerifiedEncoder::<V, C>::encode_to_unless(buf, vs, v)
}

/// Two columns written into one buffer by `encode_to`, one after the
/// other, and each found again by its range.
#[allow(dead_code)]
fn two_columns_one_buffer(ids: Vec<u64>, flags: Vec<bool>, buf: &mut Vec<u8>) -> (r: (Range<usize>, Range<usize>))
    ensures
        final(buf)@.len() >= old(buf)@.len(),
        final(buf)@.take(old(buf)@.len() as int) == old(buf)@,
        r.0.start == old(buf)@.len(),
        r.0.end == r.1.start,
        r.1.end == final(buf)@.len(),
        r.0.start <= r.0.end <= r.1.end,
        VerifiedColumn::<u64>::values(final(buf)@.subrange(r.0.start as int, r.0.end as int)) == ids@,
        VerifiedColumn::<bool>::values(final(buf)@.subrange(r.1.start as int, r.1.end as int)) == flags@,
{
    let ghost (a, b) = (VerifiedColumn::<u64>::encoding(ids@), VerifiedColumn::<bool>::encoding(flags@));
    let x = VerifiedEncoder::<u64>::encode_to(buf, ids);
    let y = VerifiedEncoder::<bool>::encode_to(buf, flags);
    proof {
        let o = old(buf)@;
        assert(buf@ =~= o + a + b);
        assert(buf@.take(o.len() as int) =~= o);
        assert(buf@.subrange(x.start as int, x.end as int) =~= a);
        assert(buf@.subrange(y.start as int, y.end as int) =~= b);
    }
    (x, y)
}

/// Values that are all the one named take no bytes: `encode_to_unless`
/// leaves the buffer.
#[allow(dead_code)]
fn all_the_one_leave_the_buffer<'a, V: VValue<C>, C: VCodec>(vs: Vec<Get<'a, V>>, v: Get<'a, V>, buf: &mut Vec<u8>)
    requires
        forall|i: int| 0 <= i < vs@.len() ==> (#[trigger] vs@[i]).sv() == v.sv(),
    ensures
        final(buf)@ == old(buf)@,
{
    proof {
        VGet::lemma_svs(vs@);
        assert(only(VGet::svs(vs@), v.sv()));
    }
    let r = VerifiedEncoder::<V, C>::encode_to_unless(buf, vs, v);
    assert(r.start == r.end);
}

/// So a column of nulls takes none, and neither does a column of no
/// values, whatever the value named.
#[allow(dead_code)]
fn nulls_leave_the_buffer(buf: &mut Vec<u8>)
    ensures
        final(buf)@ == old(buf)@,
{
    let mut nulls: Vec<Option<u64>> = Vec::new();
    nulls.push(None);
    nulls.push(None);
    all_the_one_leave_the_buffer::<Option<u64>, Leb128>(nulls, None, buf);
    let none: Vec<u64> = Vec::new();
    all_the_one_leave_the_buffer::<u64, Leb128>(none, 9, buf);
}

/// And where another value is among them, they are all written.
#[allow(dead_code)]
fn another_is_written(buf: &mut Vec<u8>) -> (r: Range<usize>)
    ensures
        final(buf)@ == old(buf)@ + VerifiedColumn::<u64>::encoding(seq![0u64, 5]),
        VerifiedColumn::<u64>::values(final(buf)@.subrange(r.start as int, r.end as int)) == seq![0u64, 5],
{
    let mut vs: Vec<u64> = Vec::new();
    vs.push(0);
    vs.push(5);
    assert(vs@ =~= seq![0u64, 5]);
    let r = VerifiedEncoder::<u64>::encode_to_unless(buf, vs, 0);
    proof {
        let d = VerifiedColumn::<u64>::encoding(seq![0u64, 5]);
        assert(buf@.subrange(r.start as int, r.end as int) =~= d);
    }
    r
}

/// What the range of an `encode_to_unless` tells its reader, who has
/// the range and the buffer: an empty range, that every value was `v`
/// and nothing was written; another, that the bytes there are the
/// encoding of the values.
#[allow(dead_code)]
fn read_from_the_encoded_range<'a, V: VValue<C>, C: VCodec>(vs: Vec<Get<'a, V>>, buf: &mut Vec<u8>, v: Get<'a, V>) -> (r:
    Range<usize>)
    ensures
        r.start <= r.end <= final(buf)@.len(),
        r.start == r.end ==> only(VGet::svs(vs@), v.sv()) && final(buf)@ == old(buf)@,
        r.start < r.end ==> final(buf)@.subrange(r.start as int, r.end as int) == VerifiedColumn::<V, C>::encoding(
            VGet::svs(vs@),
        ),
{
    let r = VerifiedEncoder::<V, C>::encode_to_unless(buf, vs, v);
    proof {
        let d = buf@.skip(old(buf)@.len() as int);
        assert(buf@.subrange(r.start as int, r.end as int) =~= d);
        if r.start == r.end {
            assert(buf@ =~= old(buf)@);
        }
    }
    r
}

// ── Strings ─────────────────────────────────────────────────────────────────

/// The bytes of strings of at most `m` bytes each: a client's own
/// induction over [`total_str_len`], which it can read.
proof fn lemma_total_str(vs: Seq<Seq<char>>, m: nat)
    requires
        forall|i: int| 0 <= i < vs.len() ==> encode_utf8(#[trigger] vs[i]).len() <= m,
    ensures
        total_str_len(vs) <= m * vs.len(),
    decreases vs.len(),
{
    if vs.len() > 0 {
        lemma_total_str(vs.drop_last(), m);
        assert(m * (vs.len() - 1) + m == m * vs.len()) by (nonlinear_arith);
    }
}

/// Strings. A column takes and gives them as `&str`, and what a
/// specification says of one is its characters; how long one is, is
/// how many bytes it is in UTF-8. A thousand strings of at most a
/// thousand bytes each are few enough for `load`.
#[allow(dead_code)]
fn round_trip_text(input: &Vec<&str>)
    requires
        input@.len() <= 1000,
        forall|i: int| 0 <= i < input@.len() ==> encode_utf8((#[trigger] input@[i])@).len() <= 1000,
{
    proof {
        <&str as VGet>::lemma_svs(input@);
        lemma_total_str(<&str as VGet>::svs(input@), 1000);
    }
    round_trip::<String, Leb128>(input);
}

/// What a client reads of a column of strings is the characters it
/// wrote: from the column, and from one loaded from its saved bytes.
#[allow(dead_code)]
fn text_in_text_out(a: &str, b: &str)
    requires
        encode_utf8(a@).len() <= 1000,
        encode_utf8(b@).len() <= 1000,
{
    let mut col = VerifiedColumn::<String>::new();
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
        <&str as VGet>::lemma_svs(all@);
        assert(<&str as VGet>::svs(all@)[1] == a@);
    }
    assert(all@.len() == 3);
    assert(all@[1]@ == a@);

    let bytes = col.save();
    proof {
        lemma_total_str(col@, 1000);
    }
    let back = VerifiedColumn::<String>::load(bytes.as_slice());
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

/// A string read is the string written, and not only as long: no two
/// strings have the same characters.
#[allow(dead_code)]
fn the_string_itself<'a>(col: &mut VerifiedColumn<String>, a: &'a str) -> (r: bool)
    requires
        old(col)@.len() < 1000,
    ensures
        r,
{
    col.push(a);
    let n = col.len();
    let x = col.get(n - 1);
    assert(x matches Some(v) && v@ == a@);
    match x {
        Some(v) => {
            proof {
                <&str as VGet>::lemma_sv_inj(v, a);
            }
            assert(v == a);
            // and so the standard library's comparison says, as it runs
            v == a
        },
        None => false,
    }
}

/// Strings the client wrote out: a column holds their characters, the
/// empty one among them, and what it saves holds them.
#[allow(dead_code)]
fn fruit() -> (r: Vec<u8>)
    ensures
        VerifiedColumn::<String>::values(r@) == seq!["apple"@, "pear"@, ""@, "pear"@],
{
    let mut col = VerifiedColumn::<String>::new();
    col.push("apple");
    col.push("pear");
    col.push("");
    col.push("pear");
    let x = col.get(1);
    let y = col.get(2);
    proof {
        reveal_strlit("pear");
        reveal_strlit("");
    }
    assert(x matches Some(v) && v@.len() == 4 && v@[0] == 'p' && v@[3] == 'r');
    assert(y matches Some(v) && v@.len() == 0);
    col.save()
}

/// A client that does not know how long its string is finds out as it
/// runs, from the string's bytes: one of at most a thousand bytes is
/// saved as bytes that `load` is sure to take.
#[allow(dead_code)]
fn saved_if_short(a: &str) -> (r: Option<Vec<u8>>)
    ensures
        r matches Some(bytes) ==> VerifiedColumn::<String>::values(bytes@) == seq![a@],
        encode_utf8(a@).len() <= 1000 ==> r is Some,
{
    if a.as_bytes().len() > 1000 {
        return None;
    }
    let mut col = VerifiedColumn::<String>::new();
    col.push(a);
    let bytes = col.save();
    proof {
        lemma_total_str(col@, 1000);
    }
    let back = VerifiedColumn::<String>::load(bytes.as_slice());
    assert(back is Ok);
    assert(back->Ok_0@ == seq![a@]);
    Some(bytes)
}

/// Strings are in the order of their UTF-8 bytes ([`bytes_below`] of
/// them), which is the order Rust has for `str`. Of two strings in
/// that order the search finds each at its place: the client proves
/// that the column is in order from the order's definition and its
/// laws, which it can read.
#[allow(dead_code)]
fn find_text(a: &str, b: &str)
    requires
        bytes_below(encode_utf8(a@), encode_utf8(b@)),
{
    let mut col = VerifiedColumn::<String>::new();
    col.push(a);
    col.push(b);
    assert(col@ == seq![a@, b@]);
    proof {
        <String as VSpec>::lemma_below(a@, b@, a@);
        <String as VSpec>::lemma_below(b@, b@, b@);
        assert(sorted::<String>(col@));
        lemma_sorted::<String>(col@, a@);
        lemma_sorted::<String>(col@, b@);
        assert(col@.subrange(0, 2) =~= col@);
        assert(col@[0] == a@ && col@[1] == b@);
    }
    let r = col.scope_to_value(a, 0, 2);
    assert(r.start == 0 && r.end == 1);
    let r = col.scope_to_value(b, 0, 2);
    assert(r.start == 1 && r.end == 2);
}

/// Of two strings it wrote out, a client can say which is first: these
/// are ASCII, so their bytes are their characters, and `a` is before
/// `p`.
#[allow(dead_code)]
fn find_fruit() {
    let a = "apple";
    let p = "pear";
    proof {
        reveal_strlit("apple");
        reveal_strlit("pear");
        is_ascii_chars_encode_utf8(a@);
        is_ascii_chars_encode_utf8(p@);
        assert(encode_utf8(a@)[0] == 'a' as u8);
        assert(encode_utf8(p@)[0] == 'p' as u8);
        assert(encode_utf8(a@).take(0) =~= encode_utf8(p@).take(0));
        assert(below_at(encode_utf8(a@), encode_utf8(p@), 0));
    }
    find_text(a, p);
}

/// A client can ask how two strings stand in the order, and is told by
/// their bytes.
#[allow(dead_code)]
fn text_compared(a: &str, b: &str) -> (r: Ordering)
    ensures
        (r is Less) == bytes_below(encode_utf8(a@), encode_utf8(b@)),
        (r is Equal) == (a@ == b@),
{
    <String as VValue<Leb128>>::compare(a, b)
}

/// Strings, each with a `!` at its end: the function is given a
/// `String` of its own, and may hand back that one.
#[allow(dead_code)]
fn exclaimed(col: &mut VerifiedColumn<String>)
    ensures
        final(col)@.len() == old(col)@.len(),
        forall|i: int| 0 <= i < old(col)@.len() ==> final(col)@[i] == old(col)@[i].push('!'),
{
    let f = |v: String| -> (w: String)
        ensures
            w@ == v@.push('!'),
        {
            let mut v = v;
            v.push('!');
            v
        };
    col.remap(f);
}

/// Strings, borrowed and owned: an encoder takes each, and what is
/// saved holds their characters.
#[allow(dead_code)]
fn encode_text<'a>(a: &'a str, b: String) -> (r: Vec<u8>)
    ensures
        VerifiedColumn::<String>::values(r@) == seq![a@, b@, a@, a@],
{
    let mut x = VerifiedEncoder::<String>::new();
    x.append(a);
    x.append_owned(b);
    x.append_n(a, 2);
    assert(x@ =~= seq![a@, b@, a@, a@]);
    x.save()
}

/// Strings, borrowed and owned, through a state into a buffer: it
/// holds what it did, then the column of them.
#[allow(dead_code)]
fn text_streamed<'a>(a: &'a str, b: String, buf: &mut Vec<u8>)
    ensures
        final(buf)@ == old(buf)@ + VerifiedColumn::<String>::encoding(seq![a@, b@, a@, a@]),
{
    let mut x = VerifiedEncoderState::<String>::new();
    x.append(buf, a);
    x.append_owned(buf, b);
    x.append_n(buf, a, 2);
    assert(x@ =~= seq![a@, b@, a@, a@]);
    x.finish(buf);
}

/// What loads as strings, saves back to the bytes it was loaded from:
/// for any bytes, of any provenance.
#[allow(dead_code)]
fn load_text(bytes: &[u8]) {
    load_then_save::<String, Leb128>(bytes);
}

/// An edit ends what was suspended. The state of an iterator fits the
/// column the iterator reads, and so counts the edits that one does;
/// after a push the column counts one more, and refuses the state. For
/// every value type and codec.
#[allow(dead_code)]
fn ended_by_a_push<V: VValue<C>, C: VCodec>(col: &mut VerifiedColumn<V, C>, v: Get<'_, V>) -> (r: bool)
    ensures
        !r,
        final(col)@ == old(col)@.push(v.sv()),
{
    let s = col.iter().suspend();
    proof {
        s.lemma_fits(&*col);
    }
    col.push(v);
    s.try_resume(col).is_ok()
}

/// So does each call that deletes a value or inserts one, and a remap:
/// `which` picks one of twelve, and after each the column refuses a
/// state taken before it. A value put in the place of another is an
/// edit whether or not the two are equal, a splice that deletes a value
/// is one whatever its iterator gives, and a remap is one though it
/// leaves each value as it was.
#[allow(dead_code)]
fn ended_by_each_edit<'v, V: VValue<C>, C: VCodec>(
    col: &mut VerifiedColumn<V, C>,
    v: Get<'v, V>,
    more: Vec<Get<'v, V>>,
    runs: Vec<Run<Get<'v, V>>>,
    which: u8,
) -> (r: bool)
    requires
        old(col)@.len() >= 2,
        more@.len() > 0,
    ensures
        !r,
{
    let s = col.iter_range(1, 2).suspend();
    proof {
        s.lemma_fits(&*col);
        VGet::lemma_svs(more@);
    }
    let n = col.len();
    let one = [v];
    if which == 0 {
        col.insert(1, v);
    } else if which == 1 {
        col.remove(0);
    } else if which == 2 {
        col.truncate(n - 1);
    } else if which == 3 {
        col.remove_n(0, 2);
    } else if which == 4 {
        col.clear();
    } else if which == 5 {
        col.splice(0, 1, one.as_slice());
    } else if which == 6 {
        col.splice(n, 0, one.as_slice());
    } else if which == 7 {
        col.splice(1, 1, more.as_slice());
    } else if which == 8 {
        col.splice_iter(0, 1, more);
    } else if which == 9 {
        col.splice_runs(0, 2, runs);
    } else if which == 10 {
        col.extend(more);
        assert(col@.len() == n + more@.len());
    } else {
        col.remap(|x: V| x);
    }
    s.try_resume(col).is_ok()
}

/// Calls that delete no value and insert none are no edits: the column
/// counts what it did, and holds what it did. A truncate at the
/// column's length, a delete of no value, a splice of nothing for
/// nothing, an extend by nothing, and splices of what an iterator with
/// no item gives, values or runs.
#[allow(dead_code)]
fn no_edits<'v, V: VValue<C>, C: VCodec>(col: &mut VerifiedColumn<V, C>, at: usize, none: &[Get<'v, V>])
    requires
        at <= old(col)@.len(),
        none@.len() == 0,
    ensures
        final(col).edits() == old(col).edits(),
        final(col)@ == old(col)@,
{
    proof {
        VGet::lemma_svs(none@);
    }
    let n = col.len();
    col.truncate(n);
    col.truncate(usize::MAX);
    col.remove_n(at, 0);
    assert(col@ =~= old(col)@);
    col.splice(at, 0, none);
    assert(col@ =~= old(col)@);
    let nothing: Vec<Get<'v, V>> = Vec::new();
    proof {
        VGet::lemma_svs(nothing@);
    }
    col.extend(nothing);
    assert(col@ =~= old(col)@);
    let nothing: Vec<Get<'v, V>> = Vec::new();
    proof {
        VGet::lemma_svs(nothing@);
    }
    col.splice_iter(at, 0, nothing);
    assert(col@ =~= old(col)@);
    let no_runs: Vec<Run<Get<'v, V>>> = Vec::new();
    col.splice_runs(at, 0, no_runs);
    assert(col@ =~= old(col)@);
}

/// A column of no value is not edited by a `clear`: it counts what it
/// did. One of any value is.
#[allow(dead_code)]
fn cleared<V: VValue<C>, C: VCodec>(col: &mut VerifiedColumn<V, C>)
    ensures
        final(col)@.len() == 0,
        old(col)@.len() == 0 ==> final(col).edits() == old(col).edits(),
        old(col)@.len() > 0 ==> final(col).edits() != old(col).edits(),
{
    col.clear();
}

/// A state is refused after more edits than one: here after `k` pushes,
/// for any `k` but 0. The client counts the edits in a loop, and the
/// column's count after `k` of them, for any `k` a `usize` holds, is
/// not what it was (`lemma_after_edits`).
#[allow(dead_code)]
fn ended_by_many<V: VValue<C>, C: VCodec>(col: &mut VerifiedColumn<V, C>, v: Get<'_, V>, k: usize) -> (r: bool)
    requires
        k > 0,
    ensures
        !r,
        final(col)@.len() == old(col)@.len() + k,
{
    let s = col.iter().suspend();
    proof {
        s.lemma_fits(&*col);
    }
    let ghost e = col.edits();
    let mut i: usize = 0;
    while i < k
        invariant
            i <= k,
            col.edits() == after_edits(e, i as nat),
            col@.len() == old(col)@.len() + i,
        decreases k - i,
    {
        col.push(v);
        i += 1;
    }
    proof {
        lemma_after_edits(e, k as nat);
    }
    s.try_resume(col).is_ok()
}

/// Edits that undo each other are edits still: after a push and the
/// removal of what was pushed the column holds what it did, and refuses
/// the state taken before the two.
#[allow(dead_code)]
fn ended_though_undone<V: VValue<C>, C: VCodec>(col: &mut VerifiedColumn<V, C>, v: Get<'_, V>) -> (r: bool)
    ensures
        !r,
        final(col)@ == old(col)@,
{
    let s = col.iter().suspend();
    proof {
        s.lemma_fits(&*col);
    }
    let ghost e = col.edits();
    let n = col.len();
    col.push(v);
    col.remove(n);
    assert(col@ =~= old(col)@);
    proof {
        assert(col.edits() == after_edits(e, 2)) by {
            reveal_with_fuel(after_edits, 3);
        }
        lemma_after_edits(e, 2);
    }
    s.try_resume(col).is_ok()
}

/// A copy of a column counts the edits the column does, and each counts
/// its own from there: after an edit of the copy, the copy refuses a
/// state of the column, and the column, which nothing has edited, gives
/// an iterator for it still.
#[allow(dead_code)]
fn a_copy_edited<V: VValue<C>, C: VCodec>(col: &VerifiedColumn<V, C>, v: Get<'_, V>) -> (r: (bool, bool))
    ensures
        !r.0,
        r.1,
{
    let s = col.iter().suspend();
    proof {
        s.lemma_fits(col);
    }
    let mut other = col.clone();
    other.push(v);
    let on_copy = s.try_resume(&other).is_ok();
    let on_column = s.try_resume(col).is_ok();
    (on_copy, on_column)
}

/// What each call that changes a column says it counts after it, asked
/// for on its own. A splice of a slice: an edit more if it deletes a
/// value or the slice has one.
#[allow(dead_code)]
fn counted_by_a_splice<V: VValue<C>, C: VCodec>(
    col: &mut VerifiedColumn<V, C>,
    at: usize,
    del: usize,
    values: &[Get<'_, V>],
)
    requires
        at <= old(col)@.len(),
        del <= old(col)@.len() - at,
    ensures
        del > 0 || values@.len() > 0 ==> final(col).edits() == after_edit(old(col).edits()),
        del == 0 && values@.len() == 0 ==> final(col).edits() == old(col).edits(),
{
    col.splice(at, del, values);
}

/// A splice of what an iterator gives, of any iterator, and one of
/// runs: an edit more if it deletes a value or leaves the column of
/// another length.
#[allow(dead_code)]
fn counted_by_a_splice_iter<'v, V: VValue<C>, C: VCodec, I: IntoIterator<Item = Get<'v, V>>>(
    col: &mut VerifiedColumn<V, C>,
    at: usize,
    del: usize,
    values: I,
)
    requires
        at <= old(col)@.len(),
        del <= old(col)@.len() - at,
    ensures
        del > 0 || final(col)@.len() != old(col)@.len() ==> final(col).edits() == after_edit(old(col).edits()),
        del == 0 && final(col)@.len() == old(col)@.len() ==> final(col).edits() == old(col).edits(),
{
    col.splice_iter(at, del, values);
}

#[allow(dead_code)]
fn counted_by_a_splice_runs<'v, V: VValue<C>, C: VCodec, I: IntoIterator<Item = Run<Get<'v, V>>>>(
    col: &mut VerifiedColumn<V, C>,
    at: usize,
    del: usize,
    runs: I,
)
    requires
        at <= old(col)@.len(),
        del <= old(col)@.len() - at,
    ensures
        del > 0 || final(col)@.len() != old(col)@.len() ==> final(col).edits() == after_edit(old(col).edits()),
        del == 0 && final(col)@.len() == old(col)@.len() ==> final(col).edits() == old(col).edits(),
{
    col.splice_runs(at, del, runs);
}

/// An `extend`, by any iterator: an edit more if the column is the
/// longer for it.
#[allow(dead_code)]
fn counted_by_an_extend<'v, V: VValue<C>, C: VCodec, I: IntoIterator<Item = Get<'v, V>>>(
    col: &mut VerifiedColumn<V, C>,
    values: I,
)
    ensures
        final(col)@.len() != old(col)@.len() ==> final(col).edits() == after_edit(old(col).edits()),
        final(col)@.len() == old(col)@.len() ==> final(col).edits() == old(col).edits(),
{
    col.extend(values);
}

/// An insert, a push and a removal: an edit more, each.
#[allow(dead_code)]
fn counted_by_an_insert<V: VValue<C>, C: VCodec>(col: &mut VerifiedColumn<V, C>, at: usize, v: Get<'_, V>)
    requires
        at <= old(col)@.len(),
    ensures
        final(col).edits() == after_edit(old(col).edits()),
{
    col.insert(at, v);
}

#[allow(dead_code)]
fn counted_by_a_push<V: VValue<C>, C: VCodec>(col: &mut VerifiedColumn<V, C>, v: Get<'_, V>)
    ensures
        final(col).edits() == after_edit(old(col).edits()),
{
    col.push(v);
}

#[allow(dead_code)]
fn counted_by_a_remove<V: VValue<C>, C: VCodec>(col: &mut VerifiedColumn<V, C>, at: usize)
    requires
        at < old(col)@.len(),
    ensures
        final(col).edits() == after_edit(old(col).edits()),
{
    col.remove(at);
}

/// A `truncate`: an edit more if the column was longer than asked for.
#[allow(dead_code)]
fn counted_by_a_truncate<V: VValue<C>, C: VCodec>(col: &mut VerifiedColumn<V, C>, len: usize)
    ensures
        len < old(col)@.len() ==> final(col).edits() == after_edit(old(col).edits()),
        len >= old(col)@.len() ==> final(col).edits() == old(col).edits(),
{
    col.truncate(len);
}

/// A `remove_n`: an edit more unless it removes no value.
#[allow(dead_code)]
fn counted_by_a_remove_n<V: VValue<C>, C: VCodec>(col: &mut VerifiedColumn<V, C>, at: usize, n: usize)
    requires
        at <= old(col)@.len(),
        n <= old(col)@.len() - at,
    ensures
        n > 0 ==> final(col).edits() == after_edit(old(col).edits()),
        n == 0 ==> final(col).edits() == old(col).edits(),
{
    col.remove_n(at, n);
}

/// A `clear`: an edit more if the column held a value.
#[allow(dead_code)]
fn counted_by_a_clear<V: VValue<C>, C: VCodec>(col: &mut VerifiedColumn<V, C>)
    ensures
        old(col)@.len() > 0 ==> final(col).edits() == after_edit(old(col).edits()),
        old(col)@.len() == 0 ==> final(col).edits() == old(col).edits(),
{
    col.clear();
}

/// A remap: an edit more, whatever the function and whatever the
/// column.
#[allow(dead_code)]
fn counted_by_a_remap<V: VValue<C>, C: VCodec, F: Fn(V) -> V>(col: &mut VerifiedColumn<V, C>, f: F)
    requires
        forall|v: V| old(col)@.contains(V::sv_of(v)) ==> #[trigger] call_requires(f, (v,)),
    ensures
        final(col).edits() == after_edit(old(col).edits()),
{
    col.remap(f);
}

/// And a copy: what the column counts.
#[allow(dead_code)]
fn counted_by_a_clone<V: VValue<C>, C: VCodec>(col: &VerifiedColumn<V, C>) -> (r: VerifiedColumn<V, C>)
    ensures
        r.edits() == col.edits(),
        r@ == col@,
{
    col.clone()
}

/// What the count is after an edit, and after several: one more, and 0
/// after the greatest; and not what it was after any number of edits
/// from one to `usize::MAX`.
#[allow(dead_code)]
proof fn the_count_after(n: usize, k: nat)
    ensures
        n < usize::MAX ==> after_edit(n) == n + 1,
        after_edit(usize::MAX) == 0,
        after_edit(n) != n,
        after_edits(n, 0) == n,
        after_edits(n, 1) == after_edit(n),
        0 < k <= usize::MAX ==> after_edits(n, k) != n,
        after_edits(n, usize::MAX as nat + 1) == n,
{
    reveal_with_fuel(after_edits, 2);
    if 0 < k <= usize::MAX {
        lemma_after_edits(n, k);
    }
    lemma_after_edits(n, usize::MAX as nat);
    assert(after_edits(n, usize::MAX as nat + 1) == after_edit(after_edits(n, usize::MAX as nat)));
}

} // verus!
