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
//! A column is of a kind of index ([`VIndex`]), and what is said here
//! of every value type and codec is said of every kind too: a client
//! that is given a column or an iterator takes one of any kind, and so
//! do those of [`round_trip`], [`load_then_save`],
//! [`clone_then_diverge`], [`collect_then_extend`], [`runs_in`],
//! [`runs_in_a_window`], [`slabs_and_validation`] and
//! [`load_then_validate`], which make their own. A column's kind is
//! kept for the values it holds, so what it saves loads as a column of
//! its kind ([`round_trip`]); bytes load as a column of a kind where
//! they would load as a plain column and the kind is kept for their
//! values, and a plain column is kept for any ([`load_as_a_kind`],
//! [`load_plain`]); and the runs of a column of one kind make a column
//! of another ([`copied_by_runs`]).
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
//! And of a [`VerifiedDecoder`], which reads a saved column from its
//! bytes with no column in between: that a `for` loop over it gives
//! the values the bytes are the saved column of ([`read_all`],
//! [`decode_all`]), which are those of the column that saved them and
//! those an encoder was given ([`decoded_as_held`],
//! [`encoded_then_decoded`]); that of bytes of any provenance a
//! decoder is given only if they are a saved column
//! ([`decode_unknown`]), as a load takes them
//! ([`decoded_or_loaded`]); that it steps over values
//! ([`the_sixth`]); that the runs it gives, given to an encoder, are
//! saved as the bytes that were decoded ([`recoded_by_runs`]), that
//! two runs one after the other are of different values
//! ([`runs_differ`]), and that a run can be asked for in parts
//! ([`a_run_in_parts`]); that nulls and `bool`s are read
//! ([`nulls_and_flags_decoded`]), and strings and byte strings
//! borrowed from the bytes ([`first_text`], [`first_bytes`]); that a
//! scan of values in order gives how many come before a value and how
//! many of it there are ([`find_decoded`]), which is what a column's
//! search finds ([`scanned_as_searched`]); and where a decoder stands
//! after a scan ([`after_a_scan`], [`the_run_at_the_head`]). Each
//! function of a decoder says so on its own ([`told_by_a_decoder`],
//! [`told_by_a_try`], [`told_by_a_decoded_value`],
//! [`told_by_a_decoders_nth`], [`told_by_an_advance`],
//! [`told_by_a_decoded_run`], [`told_by_a_bounded_run`],
//! [`told_by_a_decoders_scan`], [`told_by_a_decoders_copy`],
//! [`told_by_a_decoders_loop`]).
//!
//! And of a prefix column ([`VerifiedPrefixColumn`]), a column kept
//! with the sums of its values, which the clients above of a column of
//! any kind of index take too: that bytes load as one where they load
//! as a plain column and the sums of their values fit the type they
//! are kept in, and nowhere else ([`load_with_sums`]); that the sums
//! of few enough values always fit ([`load_short_sums`]), those of
//! `u64`s and of `i64`s whatever their number ([`load_wide_sums`]),
//! and those of 2^32 + 1 `u32`s ([`load_small_sums`]); that the sums
//! of what a prefix column holds fit ([`the_sums_of_a_column`]), after
//! an edit too ([`push_within_the_sums`]), so that an edit which would
//! leave other sums does not return ([`a_push_past_the_sums`],
//! [`a_fill_past_the_sums`]); that a prefix column saves the bytes a
//! plain column of its values does ([`saved_alike`]); and that a plain
//! column's values are copied into a prefix column only if their sums
//! fit ([`summed_up`]).
//!
//! And of what a prefix column answers from its sums: that the sum for
//! a position is what the values before it add up to
//! ([`the_sum_before`]), that the sum through a position adds the
//! value there ([`the_sum_through`]), and that the sum of a range is
//! the difference of two of them ([`the_sum_of_a_range`]), which does
//! not overflow ([`a_range_by_its_ends`]); that the sums take any
//! position ([`sums_past_the_end`]) and follow an edit
//! ([`a_sum_after_a_push`]); and, of sums that are never negative,
//! that the place found for a sum is the first whose sum reaches it
//! ([`the_place_of_a_sum`], [`a_sum_and_its_place`]), which finds in a
//! column of flags the `k`th that is set ([`the_kth_true`]), and that
//! the two searches' specifications say what each finds
//! ([`the_two_places`]).
//!
//! And of a prefix column's iterator ([`VerifiedPrefixIter`]), which
//! gives each value with the sums before it and through it: that a
//! `for` loop over it gives the running sums of the column
//! ([`running_sums`]), counted from the column's start though it reads
//! only a range ([`sums_in_a_range`]); that one value is read with its
//! sums ([`a_value_with_its_sums`]), which are the sums the column
//! answers with for its position ([`sums_two_ways`]); that a step adds
//! to the running sum what the value it gives adds
//! ([`a_step_with_its_sums`]), and values stepped over are summed too
//! ([`two_skips`]); that an iterator whose sum is started again where
//! a range starts has, where the range ends, the range's sum
//! ([`summed_by_moving`]); that reading on until the running sum is
//! past a limit finds the first position whose sum is
//! ([`first_over`]); and where each move leaves the iterator, its
//! window and its sum ([`moved_with_sums`]). An iterator a client is
//! handed says these things too, wherever it stands
//! ([`told_of_the_running_sum`], [`told_of_the_sum_before`],
//! [`told_of_what_is_left`]), and each function says its part on its
//! own ([`told_by_a_prefix_iter`], [`told_by_a_prefix_iter_range`],
//! [`told_by_a_get_prefixed`], [`told_by_a_prefixed_value`],
//! [`told_by_a_prefixed_next`], [`told_by_a_prefixed_nth`],
//! [`told_by_a_prefix_iters_place`], [`told_by_a_prefix_iters_count`],
//! [`told_by_a_prefix_size_hint`], [`told_by_a_prefix_iters_end`],
//! [`told_by_a_reset`], [`told_by_a_prefix_set_max`],
//! [`told_by_a_prefix_advance_to`], [`told_by_a_prefix_advance_by`],
//! [`told_by_a_prefix_shift`], [`told_by_a_prefix_shift_next`]).
//!
//! The iterator reads by runs and reads on to a sum, and is put away
//! and taken up again: that a column read run by run is summed and
//! counted ([`summed_by_runs`]), each run adding what its value adds as
//! many times as it is long ([`a_run_with_its_sums`]); that the runs of
//! a range make a column of the range's values
//! ([`sums_copied_by_runs`]); that the first place whose sum is more
//! than a number is found with all before it at most that number
//! ([`found_by_its_sum`]), and is the place reading on finds
//! ([`first_over_two_ways`]); that `delta` and `sum_range` say the same
//! of a range ([`a_range_two_ways`]); that an iterator suspended and
//! resumed on its column is given back as it was
//! ([`handed_over_with_its_sum`]), so that a sum is taken in two goes
//! ([`summed_in_two_goes`]), and that an edit ends what was suspended
//! ([`sums_ended_by_a_push`]); and what each call leaves in a column of
//! ten values ([`found_with_sums`]). Each function says its part on its
//! own here too ([`told_by_a_prefix_next_run`],
//! [`told_by_a_prefix_runs`], [`told_by_a_delta_nth`],
//! [`told_by_an_advance_prefix`], [`told_by_a_prefix_suspend`],
//! [`told_by_a_prefix_resume`], [`told_by_a_prefix_states_fit`],
//! [`told_by_a_prefix_clone`], [`told_by_a_prefix_default`],
//! [`told_by_a_column_delta`], [`told_by_a_prefix_seek`]).
//!
//! A prefix column adds up a type of the client's own once the client
//! says what a value of it adds, in which type the sums are kept, and
//! how many values cannot overflow that type ([`Meta`], whose values
//! each say how long they are, with [`a_meta`] to make one): that the
//! sums are then of the lengths ([`lengths_added_up`], by the client's
//! own unfolding of the sum, [`the_sum_of_three`]), a null adding
//! nothing ([`a_null_has_no_length`]); that bytes of no more values
//! than the client said load, and that whatever loads has sums that fit
//! ([`load_lengths`]); that a push past them does not return
//! ([`a_length_past_the_sums`]); that the value in which a place of the
//! whole lies is found ([`the_value_at`]); and that what is proved of
//! every summed type is proved of the client's and of its nulls
//! ([`sums_of_the_clients_own`]).
//!
//! An accumulator column adds its values up in a type of the client's
//! own, whose laws the client proves: [`Flags`], which of 64 kinds of
//! mark are open, added up of [`Mark`]s that each open a kind or close
//! it; and [`Board`], the lamps of a board that [`Step`]s change and
//! turn round, which two steps leave differently in either order
//! ([`the_order_matters`]). What the column answers is then what its
//! values add up to in the order it holds them ([`marks_added_up`], by
//! the client's own unfolding, [`the_acc_of_three`]; and
//! [`steps_in_order`]), a null adding nothing
//! ([`a_null_adds_no_mark`]); a range's answer, added after that of
//! what is before it, is that of what is before its end
//! ([`a_range_added_after`]), and is found by taking away as well
//! ([`a_range_by_taking_away`]); whatever loads as a plain column loads
//! as one ([`load_as_an_acc`]), and a plain column's runs make one
//! ([`added_up`]); a value put in adds what it says
//! ([`a_mark_put_in`]); and a column is added up a run at a time
//! ([`added_by_runs`]). Each function says its part on its own here too
//! ([`told_by_an_acc_prefix`], [`told_by_an_acc_total`],
//! [`told_by_an_acc_range`], [`told_by_add_times`]).
//!
//! An accumulator column's iterator keeps what the values it has passed
//! add up to, and gives each value with the sums before it and through
//! it ([`running_accs`], [`accs_in_a_range`],
//! [`a_value_with_its_accs`], [`a_step_with_its_accs`]), which are the
//! sums the column answers with ([`accs_two_ways`],
//! [`a_running_acc_two_ways`]). Its sum counts the values it steps
//! over, in their order, and counts from where it is started again
//! ([`added_by_moving`], [`two_skips_added`], and [`moved_with_accs`]
//! in a column of [`Step`]s, whose order matters:
//! [`steps_read_in_order`]). It reads a run at a time, the run's value
//! added as many times over as the run is long
//! ([`added_by_run_moves`], [`a_run_with_its_accs`],
//! [`a_run_of_marks`]), and its runs make a column
//! ([`accs_copied_by_runs`]); it finds a value further on with what is
//! passed over ([`found_with_accs`], [`a_range_added_two_ways`]); and
//! it is suspended and resumed with its sum
//! ([`handed_over_with_its_acc`], [`added_in_two_goes`],
//! [`accs_ended_by_a_push`]). Each function says its part on its own
//! here as well ([`told_by_an_acc_iter`] and those after it), and what
//! is proved of every type is proved of the client's own
//! ([`accs_of_the_clients_own`]).
//!
//! This module exists only while Verus checks the crate.

use super::{
    accumulated, after_edit, after_edits, all_fit, below_at, bytes_below, delta_at, delta_finds,
    delta_iter_fits, delta_near, delta_prefix, delta_run_fits, delta_run_in_range, delta_run_kept,
    delta_run_values, delta_runs_at, delta_runs_fit, delta_runs_kept, delta_runs_values,
    delta_span_ok, delta_within, deltas, folded, head_len, head_run, head_run_max, lemma_acc_cat,
    lemma_acc_range, lemma_acc_run, lemma_after_edits, lemma_cancel, lemma_delta_finds,
    lemma_delta_finds_first, lemma_delta_run_at, lemma_delta_run_exact, lemma_delta_run_fits,
    lemma_delta_runs_at, lemma_delta_runs_one, lemma_delta_runs_push, lemma_delta_within_one,
    lemma_head_len, lemma_head_run_max, lemma_run_values_add, lemma_run_values_push,
    lemma_runs_first, lemma_runs_of_values, lemma_sorted, lemma_sum_bound, lemma_sum_mono,
    lemma_sum_range, lemma_sum_run, lemma_sums_fit, mapped, only, past, refused, run_values,
    runs_of, scan_of, scan_taken, sorted, sorted_for, spliced_cut, sum_of, summed, sums_fit, times,
    total_len, total_str_len, window, AccIndex, Get, LenIndex, RawColumn, RawColumnIter, SumIndex,
    VAccum, VAccumulated, VAdds, VAmount, VCodec, VCount, VDelta, VDeltaValue, VGet, VGroup,
    VIndex, VKeeps, VNewtype, VSpec, VSum, VSummed, VValue, VerifiedAccColumn, VerifiedAccIter,
    VerifiedAccIterState, VerifiedAccSeek, VerifiedAccValue, VerifiedColumn, VerifiedDecoder,
    VerifiedDeltaColumn, VerifiedDeltaDecoder, VerifiedDeltaEncoder, VerifiedDeltaEncoderState,
    VerifiedDeltaIter, VerifiedDeltaIterState, VerifiedDeltaRuns, VerifiedEncoder,
    VerifiedEncoderState, VerifiedFindByRange, VerifiedIter, VerifiedIterState,
    VerifiedPrefixColumn, VerifiedPrefixIter, VerifiedPrefixIterState, VerifiedPrefixSeek,
    VerifiedPrefixedValue, VerifiedRuns,
};
use crate::{DeltaRun, Leb128, LoadOpts, PackError, Run};
use std::cmp::Ordering;
use std::ops::{Bound, Range, RangeBounds};
use vstd::prelude::*;
use vstd::slice::{slice_subrange, slice_to_vec};
#[cfg(verus_keep_ghost)]
use vstd::std_specs::iter::IteratorSpec;
#[cfg(verus_keep_ghost)]
use vstd::std_specs::range::RangeBoundsSpec;
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
fn round_trip<'v, V: VValue<C>, C: VCodec, K: VIndex<V, C>>(input: &Vec<Get<'v, V>>)
    requires
        V::size_bound(VGet::svs(input@)) <= 0xffff_fff5,
        input@.len() <= usize::MAX - 3,
{
    let mut col = VerifiedColumn::<V, C, K>::new();
    col.splice(0, 0, input.as_slice());
    let out = col.to_vec();
    assert(VGet::svs(out@) == VGet::svs(input@));

    let bytes = col.save();
    proof {
        // the column's kind is kept for its values, so they load as one
        // of its kind
        col.lemma_kept();
    }
    let back = VerifiedColumn::<V, C, K>::load(bytes.as_slice());
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
    round_trip::<u64, Leb128, LenIndex>(input);
}

/// Values that may be null.
#[allow(dead_code)]
fn round_trip_nullable(input: &Vec<Option<i64>>)
    requires
        input@.len() < 1_000_000,
{
    round_trip::<Option<i64>, Leb128, LenIndex>(input);
}

/// `bool`, whose bound is another.
#[allow(dead_code)]
fn round_trip_bool(input: &Vec<bool>)
    requires
        input@.len() < 1_000_000,
{
    round_trip::<bool, Leb128, LenIndex>(input);
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
    round_trip::<Action, Leb128, LenIndex>(input);
    round_trip::<Option<Action>, Leb128, LenIndex>(nullable);
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
    round_trip::<Vec<u8>, Leb128, LenIndex>(input);
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
fn load_then_save<V: VValue<C>, C: VCodec, K: VIndex<V, C>>(bytes: &[u8]) {
    if let Ok(col) = VerifiedColumn::<V, C, K>::load(bytes) {
        proof {
            VerifiedColumn::<V, C>::lemma_encoding(bytes@);
        }
        let again = col.save();
        assert(again@ == bytes@);
    }
}

/// Bytes loaded as a column of a kind of index: they load where they
/// would load as a plain column and the kind is kept for their values
/// ([`VKeeps::keeps`]), and a column they load as is of their values,
/// for which its kind is kept. For every value type, codec and kind.
#[allow(dead_code)]
fn load_as_a_kind<V: VValue<C>, C: VCodec, K: VIndex<V, C>>(bytes: &[u8]) -> (r: Option<VerifiedColumn<V, C, K>>)
    ensures
        VerifiedColumn::<V, C>::loadable(bytes@) && K::keeps(VerifiedColumn::<V, C>::values(bytes@)) ==> r is Some,
        r matches Some(c) ==> c@ == VerifiedColumn::<V, C>::values(bytes@) && K::keeps(c@),
{
    match VerifiedColumn::<V, C, K>::load(bytes) {
        Ok(c) => {
            proof {
                c.lemma_kept();
            }
            Some(c)
        },
        Err(_) => None,
    }
}

/// A plain column is kept for any values: bytes that are `loadable`
/// load as one.
#[allow(dead_code)]
fn load_plain<V: VValue<C>, C: VCodec>(bytes: &[u8]) -> (r: Option<VerifiedColumn<V, C>>)
    ensures
        VerifiedColumn::<V, C>::loadable(bytes@) ==> r is Some,
        r matches Some(c) ==> c@ == VerifiedColumn::<V, C>::values(bytes@),
{
    load_as_a_kind::<V, C, LenIndex>(bytes)
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
fn clone_then_diverge<'v, V: VValue<C>, C: VCodec, K: VIndex<V, C>>(xs: &Vec<Get<'v, V>>, a: Get<'v, V>) {
    let mut col = VerifiedColumn::<V, C, K>::new();
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
fn collect_then_extend<'v, V: VValue<C>, C: VCodec, K: VIndex<V, C>>(a: Vec<Get<'v, V>>, b: Vec<Get<'v, V>>, c: Vec<Get<'v, V>>) {
    let ghost (av, bv, cv) = (VGet::svs(a@), VGet::svs(b@), VGet::svs(c@));
    proof {
        VGet::lemma_svs(a@);
    }
    let by_slice = VerifiedColumn::<V, C, K>::from_values(a.as_slice());
    assert(by_slice@ == av);
    let mut col: VerifiedColumn<V, C, K> = a.into_iter().collect();
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
fn runs_in<'v, V: VValue<C>, C: VCodec, K: VIndex<V, C>>(a: Get<'v, V>, b: Get<'v, V>, c: Get<'v, V>, more: Vec<Run<Get<'v, V>>>)
    requires
        a.sv() != b.sv(),
{
    let mut col = VerifiedColumn::<V, C, K>::fill(3, a);
    assert(col@ =~= seq![a.sv(), a.sv(), a.sv()]);
    let all_a = col.is_only(a);
    assert(all_a);
    let empty = VerifiedColumn::<V, C, K>::fill(0, a);
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
fn slabs_and_validation<'v, V: VValue<C>, C: VCodec, K: VIndex<V, C>>(input: &Vec<Get<'v, V>>)
    requires
        V::size_bound(VGet::svs(input@)) <= u32::MAX,
{
    let mut col = VerifiedColumn::<V, C, K>::new();
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
    slabs_and_validation::<u64, Leb128, LenIndex>(a);
    slabs_and_validation::<bool, Leb128, LenIndex>(b);
}

/// A column loaded from less than 4 GiB of bytes finds no fault in what
/// it saves, which is those bytes. For any bytes, of any provenance.
#[allow(dead_code)]
fn load_then_validate<V: VValue<C>, C: VCodec, K: VIndex<V, C>>(bytes: &[u8])
    requires
        bytes@.len() <= u32::MAX,
{
    if let Ok(col) = VerifiedColumn::<V, C, K>::load(bytes) {
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
fn insert_in_order<'v, V: VValue<C>, C: VCodec, K: VIndex<V, C>>(col: &mut VerifiedColumn<V, C, K>, x: Get<'v, V>) -> (at:
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
fn remap_as_it_is<V: VValue<C>, C: VCodec, K: VIndex<V, C>>(col: &mut VerifiedColumn<V, C, K>)
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
fn read_in_a_loop<'a, V: VValue<C>, C: VCodec, K: VIndex<V, C>>(col: &'a VerifiedColumn<V, C, K>) -> (out: Vec<Get<'a, V>>)
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
fn the_third<'a, V: VValue<C>, C: VCodec, K: VIndex<V, C>>(col: &'a VerifiedColumn<V, C, K>) -> (r: Option<Get<'a, V>>)
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
fn two_readers<'a, V: VValue<C>, C: VCodec, K: VIndex<V, C>>(col: &'a VerifiedColumn<V, C, K>)
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
    let mut none = VerifiedIter::<V, C, K>::default();
    let n = none.next();
    assert(n is None);
}

/// An iterator's window moved to a range of the column: a `for` loop
/// then reads the column's values there. For every value type and codec.
#[allow(dead_code)]
fn in_the_range<'a, V: VValue<C>, C: VCodec, K: VIndex<V, C>>(col: &'a VerifiedColumn<V, C, K>, a: usize, b: usize) -> (out: Vec<
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
fn read_out<V: VValue<C>, C: VCodec, K: VIndex<V, C>>(col: &VerifiedColumn<V, C, K>, a: usize, b: usize) -> (r: (usize, usize, usize))
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
fn at_two_places<'a, V: VValue<C>, C: VCodec, K: VIndex<V, C>>(col: &'a VerifiedColumn<V, C, K>, i: usize, j: usize, n: usize) -> (r: (
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
fn window_len<V: VValue<C>, C: VCodec, K: VIndex<V, C>>(it: &VerifiedIter<'_, V, C, K>) -> (r: usize)
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
fn ends_within<V: VValue<C>, C: VCodec, K: VIndex<V, C>>(it: &VerifiedIter<'_, V, C, K>, len: usize) -> (r: bool)
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
fn the_first_run<'a, V: VValue<C>, C: VCodec, K: VIndex<V, C>>(col: &'a VerifiedColumn<V, C, K>) -> (r: Option<Run<Get<'a, V>>>)
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
fn runs_in_a_window<'v, V: VValue<C>, C: VCodec, K: VIndex<V, C>>(a: Get<'v, V>, b: Get<'v, V>)
    requires
        a.sv() != b.sv(),
{
    let mut col = VerifiedColumn::<V, C, K>::fill(3, a);
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
fn in_runs<'a, V: VValue<C>, C: VCodec, K: VIndex<V, C>>(col: &'a VerifiedColumn<V, C, K>) -> (out: Vec<Run<Get<'a, V>>>)
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
fn count_by_runs<V: VValue<C>, C: VCodec, K: VIndex<V, C>>(col: &VerifiedColumn<V, C, K>, a: usize, b: usize) -> (n: usize)
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
/// and codec, and from a column of any kind of index into one of any.
#[allow(dead_code)]
fn copied_by_runs<V: VValue<C>, C: VCodec, K: VIndex<V, C>, K2: VIndex<V, C>>(col: &VerifiedColumn<V, C, K>) -> (r:
    VerifiedColumn<V, C, K2>)
    ensures
        r@ == col@,
{
    let mut r = VerifiedColumn::<V, C, K2>::new();
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
fn told_by_a_run<'a, V: VValue<C>, C: VCodec, K: VIndex<V, C>>(it: VerifiedIter<'a, V, C, K>) -> (r: Option<Run<Get<'a, V>>>)
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
fn told_by_the_runs<'a, V: VValue<C>, C: VCodec, K: VIndex<V, C>>(it: VerifiedIter<'a, V, C, K>) -> (r: Option<Run<Get<'a, V>>>)
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
proof fn no_more_than_counted<'a, V: VValue<C>, C: VCodec, K: VIndex<V, C>>(it: VerifiedIter<'a, V, C, K>)
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
fn told_by_a_scan<'a, V: VValue<C>, C: VCodec, K: VIndex<V, C>>(it: VerifiedIter<'a, V, C, K>, x: Get<'_, V>) -> (r: Option<usize>)
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
fn first_place_in<V: VValue<C>, C: VCodec, K: VIndex<V, C>>(col: &VerifiedColumn<V, C, K>, x: Get<'_, V>, a: usize, b: usize) -> (r:
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
fn all_places<V: VValue<C>, C: VCodec, K: VIndex<V, C>>(col: &VerifiedColumn<V, C, K>, x: Get<'_, V>) -> (out: Vec<usize>)
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
fn told_by_a_seek<'a, V: VValue<C>, C: VCodec, K: VIndex<V, C>>(it: VerifiedIter<'a, V, C, K>, x: Get<'_, V>, range: Range<usize>) -> (r:
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
fn seek_out_of_range<'a, V: VValue<C>, C: VCodec, K: VIndex<V, C>>(it: VerifiedIter<'a, V, C, K>, x: Get<'_, V>, range: Range<usize>) -> (r:
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
fn find_in_the_rest<V: VValue<C>, C: VCodec, K: VIndex<V, C>>(it: &mut VerifiedIter<'_, V, C, K>, x: Get<'_, V>) -> (r: Range<usize>)
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
fn told_by_a_suspend<'a, V: VValue<C>, C: VCodec, K: VIndex<V, C>>(it: &VerifiedIter<'a, V, C, K>, col: &VerifiedColumn<V, C, K>) -> (s:
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
fn told_by_a_resume<'a, V: VValue<C>, C: VCodec, K: VIndex<V, C>>(s: &VerifiedIterState, col: &'a VerifiedColumn<V, C, K>) -> (r: Result<
    VerifiedIter<'a, V, C, K>,
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
fn told_by_the_source<'a, V: VValue<C>, C: VCodec, K: VIndex<V, C>>(it: &VerifiedIter<'a, V, C, K>, col: &VerifiedColumn<V, C, K>)
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
fn handed_over<'a, V: VValue<C>, C: VCodec, K: VIndex<V, C>>(it: VerifiedIter<'_, V, C, K>, col: &'a VerifiedColumn<V, C, K>) -> (on:
    VerifiedIter<'a, V, C, K>)
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
fn stop_and_go_on<'a, V: VValue<C>, C: VCodec, K: VIndex<V, C>>(col: &'a VerifiedColumn<V, C, K>, k: usize) -> (r: (
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
fn of_a_range<V: VValue<C>, C: VCodec, K: VIndex<V, C>>(col: &VerifiedColumn<V, C, K>, a: usize, b: usize) -> (r: (
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
fn of_no_column<V: VValue<C>, C: VCodec, K: VIndex<V, C>>(col: &VerifiedColumn<V, C, K>) -> (r: bool)
{
    let it = VerifiedIter::<V, C, K>::default();
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
fn reads_the_same_column<'a, V: VValue<C>, C: VCodec, K: VIndex<V, C>>(
    it: &mut VerifiedIter<'a, V, C, K>,
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
fn resumed_on_any<'a, V: VValue<C>, C: VCodec, K: VIndex<V, C>>(s: &VerifiedIterState, col: &'a VerifiedColumn<V, C, K>) -> (r: Option<
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
fn reencoded<V: VValue<C>, C: VCodec, K: VIndex<V, C>>(col: &VerifiedColumn<V, C, K>) -> (r: Vec<u8>)
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
    round_trip::<String, Leb128, LenIndex>(input);
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
    load_then_save::<String, Leb128, LenIndex>(bytes);
}

/// An edit ends what was suspended. The state of an iterator fits the
/// column the iterator reads, and so counts the edits that one does;
/// after a push the column counts one more, and refuses the state. For
/// every value type and codec.
#[allow(dead_code)]
fn ended_by_a_push<V: VValue<C>, C: VCodec, K: VIndex<V, C>>(col: &mut VerifiedColumn<V, C, K>, v: Get<'_, V>) -> (r: bool)
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
fn ended_by_each_edit<'v, V: VValue<C>, C: VCodec, K: VIndex<V, C>>(
    col: &mut VerifiedColumn<V, C, K>,
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
fn no_edits<'v, V: VValue<C>, C: VCodec, K: VIndex<V, C>>(col: &mut VerifiedColumn<V, C, K>, at: usize, none: &[Get<'v, V>])
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
fn cleared<V: VValue<C>, C: VCodec, K: VIndex<V, C>>(col: &mut VerifiedColumn<V, C, K>)
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
fn ended_by_many<V: VValue<C>, C: VCodec, K: VIndex<V, C>>(col: &mut VerifiedColumn<V, C, K>, v: Get<'_, V>, k: usize) -> (r: bool)
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
fn ended_though_undone<V: VValue<C>, C: VCodec, K: VIndex<V, C>>(col: &mut VerifiedColumn<V, C, K>, v: Get<'_, V>) -> (r: bool)
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
fn a_copy_edited<V: VValue<C>, C: VCodec, K: VIndex<V, C>>(col: &VerifiedColumn<V, C, K>, v: Get<'_, V>) -> (r: (bool, bool))
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
fn counted_by_a_splice<V: VValue<C>, C: VCodec, K: VIndex<V, C>>(
    col: &mut VerifiedColumn<V, C, K>,
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
fn counted_by_a_splice_iter<'v, V: VValue<C>, C: VCodec, K: VIndex<V, C>, I: IntoIterator<Item = Get<'v, V>>>(
    col: &mut VerifiedColumn<V, C, K>,
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
fn counted_by_a_splice_runs<'v, V: VValue<C>, C: VCodec, K: VIndex<V, C>, I: IntoIterator<Item = Run<Get<'v, V>>>>(
    col: &mut VerifiedColumn<V, C, K>,
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
fn counted_by_an_extend<'v, V: VValue<C>, C: VCodec, K: VIndex<V, C>, I: IntoIterator<Item = Get<'v, V>>>(
    col: &mut VerifiedColumn<V, C, K>,
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
fn counted_by_an_insert<V: VValue<C>, C: VCodec, K: VIndex<V, C>>(col: &mut VerifiedColumn<V, C, K>, at: usize, v: Get<'_, V>)
    requires
        at <= old(col)@.len(),
    ensures
        final(col).edits() == after_edit(old(col).edits()),
{
    col.insert(at, v);
}

#[allow(dead_code)]
fn counted_by_a_push<V: VValue<C>, C: VCodec, K: VIndex<V, C>>(col: &mut VerifiedColumn<V, C, K>, v: Get<'_, V>)
    ensures
        final(col).edits() == after_edit(old(col).edits()),
{
    col.push(v);
}

#[allow(dead_code)]
fn counted_by_a_remove<V: VValue<C>, C: VCodec, K: VIndex<V, C>>(col: &mut VerifiedColumn<V, C, K>, at: usize)
    requires
        at < old(col)@.len(),
    ensures
        final(col).edits() == after_edit(old(col).edits()),
{
    col.remove(at);
}

/// A `truncate`: an edit more if the column was longer than asked for.
#[allow(dead_code)]
fn counted_by_a_truncate<V: VValue<C>, C: VCodec, K: VIndex<V, C>>(col: &mut VerifiedColumn<V, C, K>, len: usize)
    ensures
        len < old(col)@.len() ==> final(col).edits() == after_edit(old(col).edits()),
        len >= old(col)@.len() ==> final(col).edits() == old(col).edits(),
{
    col.truncate(len);
}

/// A `remove_n`: an edit more unless it removes no value.
#[allow(dead_code)]
fn counted_by_a_remove_n<V: VValue<C>, C: VCodec, K: VIndex<V, C>>(col: &mut VerifiedColumn<V, C, K>, at: usize, n: usize)
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
fn counted_by_a_clear<V: VValue<C>, C: VCodec, K: VIndex<V, C>>(col: &mut VerifiedColumn<V, C, K>)
    ensures
        old(col)@.len() > 0 ==> final(col).edits() == after_edit(old(col).edits()),
        old(col)@.len() == 0 ==> final(col).edits() == old(col).edits(),
{
    col.clear();
}

/// A remap: an edit more, whatever the function and whatever the
/// column.
#[allow(dead_code)]
fn counted_by_a_remap<V: VValue<C>, C: VCodec, K: VIndex<V, C>, F: Fn(V) -> V>(col: &mut VerifiedColumn<V, C, K>, f: F)
    requires
        forall|v: V| old(col)@.contains(V::sv_of(v)) ==> #[trigger] call_requires(f, (v,)),
    ensures
        final(col).edits() == after_edit(old(col).edits()),
{
    col.remap(f);
}

/// And a copy: what the column counts.
#[allow(dead_code)]
fn counted_by_a_clone<V: VValue<C>, C: VCodec, K: VIndex<V, C>>(col: &VerifiedColumn<V, C, K>) -> (r: VerifiedColumn<V, C, K>)
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

/// What a decoder has left, read by a `for` loop into a `Vec`: a
/// decoder is an iterator of the standard library's, as a column's
/// iterator is.
#[allow(dead_code)]
fn read_all<'a, V: VValue<C>, C: VCodec>(dec: VerifiedDecoder<'a, V, C>) -> (out: Vec<Get<'a, V>>)
    ensures
        out@ == dec.items(),
        <Get<'a, V> as VGet>::svs(out@) == dec.rest(),
{
    let ghost all = dec.items();
    let mut out: Vec<Get<'a, V>> = Vec::new();
    for x in at: dec
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

/// A decoder gives the values of a saved column from its bytes, in
/// order, each borrowed from the bytes where it borrows: the `Vec` they
/// are read into holds the values the bytes are the saved column of.
/// For every value type and codec.
#[allow(dead_code)]
fn decode_all<'a, V: VValue<C>, C: VCodec>(bytes: &'a [u8]) -> (out: Vec<Get<'a, V>>)
    requires
        VerifiedColumn::<V, C>::valid(bytes@),
    ensures
        <Get<'a, V> as VGet>::svs(out@) == VerifiedColumn::<V, C>::values(bytes@),
{
    read_all(VerifiedDecoder::<V, C>::new(bytes))
}

/// What a column saves, a decoder gives back: each value it reads from
/// the saved bytes is the column's own at that place, as `same` finds
/// when it runs, and it reads as many as the column has. For every
/// value type and codec.
#[allow(dead_code)]
fn decoded_as_held<V: VValue<C>, C: VCodec, K: VIndex<V, C>>(col: &VerifiedColumn<V, C, K>) -> (r: bool)
    ensures
        r,
{
    let bytes = col.save();
    let mut dec = VerifiedDecoder::<V, C>::new(bytes.as_slice());
    let n = col.len();
    let mut i: usize = 0;
    let mut r = true;
    proof {
        assert(col@.skip(0) =~= col@);
    }
    while i < n
        invariant
            i <= n,
            n == col@.len(),
            dec.rest() == col@.skip(i as int),
            r,
        decreases n - i,
    {
        let x = dec.next();
        let y = col.get(i);
        match (x, y) {
            (Some(a), Some(b)) => {
                if !V::same(a, b) {
                    r = false;
                }
            },
            _ => {
                r = false;
            },
        }
        proof {
            assert(col@.skip(i as int).skip(1) =~= col@.skip(i + 1));
        }
        i += 1;
    }
    let last = dec.next();
    r && last.is_none()
}

/// What an encoder saves, a decoder reads as the values the encoder
/// was given, one for one. For every value type and codec.
#[allow(dead_code)]
fn encoded_then_decoded<'v, V: VValue<C>, C: VCodec>(input: &Vec<Get<'v, V>>) -> (r: bool)
    ensures
        r,
{
    let bytes = encoded_as_saved::<V, C>(input);
    let dec = VerifiedDecoder::<V, C>::new(bytes.as_slice());
    let ghost all = dec.items();
    proof {
        dec.lemma_items_len();
        VGet::lemma_svs(input@);
    }
    let n = input.len();
    let mut i: usize = 0;
    let mut r = true;
    for x in at: dec
        invariant
            at.seq() == all,
            VGet::svs(all) == VGet::svs(input@),
            all.len() == n == input@.len(),
            i == at.index(),
            r,
    {
        proof {
            VGet::lemma_svs(all);
            VGet::lemma_svs(input@);
            assert(VGet::svs(all)[i as int] == x.sv());
            assert(VGet::svs(input@)[i as int] == input@[i as int].sv());
        }
        if !V::same(x, input[i]) {
            r = false;
        }
        i += 1;
    }
    r && i == n
}

/// Bytes of which nothing is known: `try_new` reads them through
/// first, and gives a decoder only of a saved column, whose values the
/// decoder then gives; and it gives one for every saved column of less
/// than 4 GiB.
#[allow(dead_code)]
fn decode_unknown<'a, V: VValue<C>, C: VCodec>(bytes: &'a [u8]) -> (r: Option<Vec<Get<'a, V>>>)
    ensures
        r matches Some(out) ==> VerifiedColumn::<V, C>::valid(bytes@) && <Get<'a, V> as VGet>::svs(out@)
            == VerifiedColumn::<V, C>::values(bytes@),
        VerifiedColumn::<V, C>::valid(bytes@) && bytes@.len() <= u32::MAX ==> r is Some,
{
    match VerifiedDecoder::<V, C>::try_new(bytes) {
        Ok(dec) => Some(read_all(dec)),
        Err(_) => None,
    }
}

/// A decoder and a load read the same bytes alike. Where both take
/// them, the decoder has the loaded column's values to give. Bytes that
/// load and are refused a decoder are 4 GiB or more; and bytes that a
/// load refuses and a decoder is given of are a saved column that a load
/// is not sure to take (`loadable`).
#[allow(dead_code)]
fn decoded_or_loaded<V: VValue<C>, C: VCodec>(bytes: &[u8]) -> (r: bool)
    ensures
        r,
{
    let col = VerifiedColumn::<V, C>::load(bytes);
    let dec = VerifiedDecoder::<V, C>::try_new(bytes);
    match (col, dec) {
        (Ok(col), Ok(dec)) => {
            assert(dec.rest() == col@);
            true
        },
        (Ok(_), Err(_)) => bytes.len() > 0xffff_ffff,
        (Err(_), Ok(_)) => {
            assert(VerifiedColumn::<V, C>::valid(bytes@));
            assert(!VerifiedColumn::<V, C>::loadable(bytes@));
            true
        },
        (Err(_), Err(_)) => true,
    }
}

/// A decoder steps over values: over two with `advance_by` and over
/// three more with `nth`, which so gives the sixth of the saved
/// numbers, if there are six, and nothing if there are not.
#[allow(dead_code)]
fn the_sixth(bytes: &[u8]) -> (r: Option<u64>)
    requires
        VerifiedColumn::<u64>::valid(bytes@),
    ensures
        ({
            let vs = VerifiedColumn::<u64>::values(bytes@);
            if vs.len() > 5 {
                r == Some(vs[5])
            } else {
                r is None
            }
        }),
{
    let mut dec = VerifiedDecoder::<u64>::new(bytes);
    let ghost vs = dec.rest();
    dec.advance_by(2);
    proof {
        dec.lemma_items_len();
    }
    let r = dec.nth(3);
    proof {
        if vs.len() > 5 {
            assert(vs.skip(2)[3] == vs[5]);
            assert(dec.rest() =~= vs.skip(6));
        } else {
            assert(dec.rest().len() == 0);
        }
    }
    r
}

/// A saved column decoded run by run, each run given to an encoder as
/// it comes: what the encoder saves is the bytes that were decoded.
/// For every value type and codec, and for any saved column: also one
/// of more values than a column holds.
#[allow(dead_code)]
fn recoded_by_runs<'a, V: VValue<C>, C: VCodec>(bytes: &'a [u8]) -> (r: Vec<u8>)
    requires
        VerifiedColumn::<V, C>::valid(bytes@),
    ensures
        r@ == bytes@,
{
    let mut dec = VerifiedDecoder::<V, C>::new(bytes);
    let ghost all = dec.rest();
    let mut enc = VerifiedEncoder::<'a, V, C>::new();
    proof {
        assert(enc@ + dec.rest() =~= all);
    }
    loop
        invariant
            enc.wf(),
            enc@ + dec.rest() == all,
        ensures
            enc@ == all,
        decreases dec.rest().len(),
    {
        let ghost left = dec.rest();
        let ghost had = enc@;
        match dec.next_run() {
            Some(run) => {
                enc.append_n(run.value, run.count);
                proof {
                    let n = run.count as int;
                    assert(left.take(n) =~= Seq::new(n as nat, |_i: int| run.value.sv()));
                    assert(left =~= left.take(n) + left.skip(n));
                    assert(enc@ + dec.rest() =~= had + left);
                }
            },
            None => {
                proof {
                    assert(enc@ + dec.rest() =~= enc@);
                }
                break ;
            },
        }
    }
    let r = enc.save();
    proof {
        VerifiedColumn::<V, C>::lemma_save_unique(r@, bytes@);
    }
    r
}

/// Two runs that a decoder gives one after the other are of different
/// values, unless the first is one cut off at `usize::MAX` values; and
/// after it has given no run it gives none. For every value type and
/// codec.
#[allow(dead_code)]
fn runs_differ<'a, V: VValue<C>, C: VCodec>(dec: &mut VerifiedDecoder<'a, V, C>) -> (r: bool)
    ensures
        r,
{
    let ghost left = dec.rest();
    let a = dec.next_run();
    let b = dec.next_run();
    match (a, b) {
        (Some(x), Some(y)) => {
            proof {
                let n = x.count as int;
                assert(left.skip(n)[0] == y.value.sv());
                assert(left[n] == y.value.sv());
            }
            x.count == usize::MAX || !V::same(x.value, y.value)
        },
        (None, Some(_)) => false,
        _ => true,
    }
}

/// A run asked for in parts: five sevens and a nine, the run asked for
/// three values at a time, come as three sevens, two sevens (the run
/// ends there) and, asked for whole, one nine. A run of no more than 0
/// values is no run, and the decoder stays where it is.
#[allow(dead_code)]
fn a_run_in_parts() {
    let mut enc = VerifiedEncoder::<u64>::new();
    enc.append_n(7, 5);
    enc.append(9);
    let bytes = enc.save();
    let mut dec = VerifiedDecoder::<u64>::new(bytes.as_slice());
    let ghost o = dec.rest();
    assert(o =~= seq![7u64, 7, 7, 7, 7, 9]);
    assert(o[0] == 7 && o[1] == 7 && o[2] == 7 && o[3] == 7 && o[4] == 7 && o[5] == 9);

    let none = dec.next_run_max(0);
    assert(none is None && dec.rest() == o);

    let a = dec.next_run_max(3);
    assert(a matches Some(run) && run.value == 7 && run.count == 3);
    let ghost o3 = dec.rest();
    assert(o3 =~= seq![7u64, 7, 9]);
    assert(o3[0] == 7 && o3[1] == 7 && o3[2] == 9);

    let b = dec.next_run_max(3);
    assert(b matches Some(run) && run.value == 7 && run.count == 2);
    let ghost o5 = dec.rest();
    assert(o5 =~= seq![9u64]);
    assert(o5[0] == 9);

    let c = dec.next_run();
    assert(c matches Some(run) && run.value == 9 && run.count == 1);
    assert(dec.rest().len() == 0);

    let d = dec.next_run();
    assert(d is None);
}

/// Nulls and `bool`s decoded: three nulls come as one run of a null,
/// and the number after them as a number; and of two `true`s and a
/// `false` the third is the `false`.
#[allow(dead_code)]
fn nulls_and_flags_decoded() {
    let mut enc = VerifiedEncoder::<Option<u64>>::new();
    enc.append_n(None, 3);
    enc.append(Some(4));
    let bytes = enc.save();
    let mut dec = VerifiedDecoder::<Option<u64>>::new(bytes.as_slice());
    let ghost o = dec.rest();
    assert(o =~= seq![None, None, None, Some(4u64)]);
    assert(o[0] is None && o[1] is None && o[2] is None && o[3] == Some(4u64));
    let a = dec.next_run();
    assert(a matches Some(run) && run.value is None && run.count == 3);
    assert(dec.rest() =~= seq![Some(4u64)]);
    let b = dec.next();
    assert(b == Some(Some(4u64)));
    let c = dec.next();
    assert(c is None);

    let mut flags = VerifiedEncoder::<bool>::new();
    flags.append_n(true, 2);
    flags.append(false);
    let bytes = flags.save();
    let mut dec = VerifiedDecoder::<bool>::new(bytes.as_slice());
    assert(dec.rest() =~= seq![true, true, false]);
    proof {
        dec.lemma_items_len();
    }
    let third = dec.nth(2);
    assert(third == Some(false));
    assert(dec.rest().len() == 0);
}

/// What a decoder gives borrows from the bytes and not from the
/// decoder: the first string of a saved column of strings is there
/// when the decoder that read it is gone.
#[allow(dead_code)]
fn first_text<'a>(bytes: &'a [u8]) -> (r: Option<&'a str>)
    requires
        VerifiedColumn::<String>::valid(bytes@),
    ensures
        ({
            let vs = VerifiedColumn::<String>::values(bytes@);
            if vs.len() > 0 {
                r matches Some(s) && s@ == vs[0]
            } else {
                r is None
            }
        }),
{
    let mut dec = VerifiedDecoder::<String>::new(bytes);
    dec.next()
}

/// The same of byte strings that may be null: the first of them is a
/// null, or a slice of the saved column's bytes.
#[allow(dead_code)]
fn first_bytes<'a>(bytes: &'a [u8]) -> (r: Option<Option<&'a [u8]>>)
    requires
        VerifiedColumn::<Option<Vec<u8>>>::valid(bytes@),
    ensures
        ({
            let vs = VerifiedColumn::<Option<Vec<u8>>>::values(bytes@);
            if vs.len() > 0 {
                r matches Some(v) && match v {
                    Some(b) => vs[0] == Some(b@),
                    None => vs[0] is None,
                }
            } else {
                r is None
            }
        }),
{
    let mut dec = VerifiedDecoder::<Option<Vec<u8>>>::new(bytes);
    dec.next()
}

/// A scan of saved numbers that are in order, as a client says it of
/// numbers: `scan_for` gives how many are less than `x` and how many
/// are `x`, and those after them are greater.
#[allow(dead_code)]
fn find_decoded(bytes: &[u8], x: u32) -> (r: (usize, usize))
    requires
        VerifiedColumn::<u32>::valid(bytes@),
        VerifiedColumn::<u32>::values(bytes@).len() <= usize::MAX,
        ({
            let vs = VerifiedColumn::<u32>::values(bytes@);
            forall|i: int, j: int| 0 <= i <= j < vs.len() ==> vs[i] <= vs[j]
        }),
    ensures
        ({
            let vs = VerifiedColumn::<u32>::values(bytes@);
            &&& r.0 + r.1 <= vs.len()
            &&& forall|k: int| 0 <= k < r.0 ==> vs[k] < x
            &&& forall|k: int| r.0 <= k < r.0 + r.1 ==> vs[k] == x
            &&& forall|k: int| r.0 + r.1 <= k < vs.len() ==> vs[k] > x
        }),
{
    let mut dec = VerifiedDecoder::<u32>::new(bytes);
    let ghost vs = dec.rest();
    let r = dec.scan_for(x, usize::MAX);
    proof {
        let s = r.0 as int;
        let c = r.1 as int;
        if c > 0 {
            assert forall|k: int| s <= k < s + c implies vs[k] == x by {
                assert(vs.skip(s)[k - s] == x);
            }
            if s + c < vs.len() {
                assert(vs.skip(s)[c] != x);
                assert(vs[s + c - 1] == x);
                assert(vs[s + c] > x);
            }
        } else if s < vs.len() {
            assert(vs[s] > x);
        }
    }
    r
}

/// A decoder's scan finds what a column's search finds: of a column
/// whose values are in order, the scan of what the column saved passes
/// as many values as are before the range that `scope_to_value` gives,
/// and finds as many of the target as that range is long. For every
/// value type and codec, from what the order is said to be
/// ([`VSpec::lemma_below`]).
#[allow(dead_code)]
fn scanned_as_searched<V: VValue<C>, C: VCodec, K: VIndex<V, C>>(col: &VerifiedColumn<V, C, K>, x: Get<'_, V>) -> (r: bool)
    requires
        sorted::<V>(col@),
    ensures
        r,
{
    let n = col.len();
    let bytes = col.save();
    let mut dec = VerifiedDecoder::<V, C>::new(bytes.as_slice());
    let (skipped, count) = dec.scan_for(x, usize::MAX);
    proof {
        lemma_sorted::<V>(col@, x.sv());
        assert(col@.subrange(0, n as int) =~= col@);
    }
    let found = col.scope_to_value(x, 0, n);
    proof {
        let o = col@;
        let t = x.sv();
        let s = skipped as int;
        let c = count as int;
        let a = found.start as int;
        let b = found.end as int;
        V::lemma_below(t, t, t);
        // the scan passed the values before the range, and no more
        if s < a {
            assert(V::below(o[s], t));
            if c > 0 {
                assert(o.skip(s)[0] == t);
            } else {
                V::lemma_below(t, o[s], t);
            }
            assert(false);
        }
        if s > a {
            assert(V::below(o[a], t));
            assert(false);
        }
        // and found the range
        if c > 0 {
            assert(o.skip(s)[c - 1] == t);
            assert(o[s + c - 1] == t);
            if s + c < b {
                assert(o[s + c] == t);
                assert(o.skip(s)[c] != t);
                assert(false);
            }
        } else if a < b {
            assert(o[a] == t);
            assert(false);
        }
    }
    found.start == skipped && found.end - found.start == count
}

/// Where a decoder stands after a scan. Of the numbers 1, 1, 5, 5, 7:
/// a scan for 5 passes two and finds two, and the decoder gives the 7
/// next. A scan for 3 passes two and finds none, and the decoder gives
/// the 7 next too: it has read the run of 5 to see that it is not 3,
/// and does not give it again (`scan_taken`). A scan for 7 of three
/// values at most passes three and finds none, and the decoder gives
/// the second 5 next. Each on a copy of the decoder, which reads on
/// its own.
#[allow(dead_code)]
fn after_a_scan() {
    let mut enc = VerifiedEncoder::<u32>::new();
    enc.append_n(1, 2);
    enc.append_n(5, 2);
    enc.append(7);
    let bytes = enc.save();
    let dec = VerifiedDecoder::<u32>::new(bytes.as_slice());
    let ghost o = dec.rest();
    assert(o =~= seq![1u32, 1, 5, 5, 7]);
    assert(o[0] == 1 && o[1] == 1 && o[2] == 5 && o[3] == 5 && o[4] == 7);

    let mut first = dec.clone();
    let a = first.scan_for(5, usize::MAX);
    proof {
        if a.1 > 0 {
            assert(o.skip(a.0 as int)[0] == 5);
            assert(o.skip(2)[1] == 5 && o.skip(2)[2] == 7);
        }
    }
    assert(a.0 == 2 && a.1 == 2);
    assert(first.rest() =~= seq![7u32]);
    let x = first.next();
    assert(x == Some(7u32));

    let mut second = dec.clone();
    let b = second.scan_for(3, usize::MAX);
    proof {
        if b.1 > 0 {
            assert(o.skip(b.0 as int)[0] == 3);
        }
    }
    assert(b.0 == 2 && b.1 == 0);
    proof {
        let rest = o.skip(2);
        assert(rest[0] == 5 && rest[1] == 5 && rest[2] == 7);
        assert(head_run_max(rest, 5u32, 2, usize::MAX - 2));
        lemma_head_len(rest, 5u32, 2, usize::MAX - 2);
        assert(scan_taken(o, usize::MAX as int, 2, 0) == 4);
    }
    assert(second.rest() =~= seq![7u32]);
    let y = second.next();
    assert(y == Some(7u32));

    let mut third = dec.clone();
    let c = third.scan_for(7, 3);
    proof {
        if c.1 > 0 {
            assert(o.skip(c.0 as int)[0] == 7);
        }
    }
    assert(c.0 == 3 && c.1 == 0);
    assert(third.rest() =~= seq![5u32, 7]);
    let z = third.next();
    assert(z == Some(5u32));

    // and the decoder they are copies of has given nothing
    assert(dec.rest() == o);
}

/// What `new` says by itself: the decoder has the saved column's values
/// to give.
#[allow(dead_code)]
fn told_by_a_decoder<'a, V: VValue<C>, C: VCodec>(bytes: &'a [u8]) -> (r: VerifiedDecoder<'a, V, C>)
    requires
        VerifiedColumn::<V, C>::valid(bytes@),
    ensures
        r.rest() == VerifiedColumn::<V, C>::values(bytes@),
        r.items().len() == VerifiedColumn::<V, C>::values(bytes@).len(),
{
    let r = VerifiedDecoder::<V, C>::new(bytes);
    proof {
        r.lemma_items_len();
    }
    r
}

/// What `try_new` says: a decoder is one of a saved column, with that
/// column's values to give; and a saved column of less than 4 GiB is
/// given one.
#[allow(dead_code)]
fn told_by_a_try<'a, V: VValue<C>, C: VCodec>(bytes: &'a [u8]) -> (r: Result<VerifiedDecoder<'a, V, C>, PackError>)
    ensures
        r matches Ok(d) ==> VerifiedColumn::<V, C>::valid(bytes@) && d.rest() == VerifiedColumn::<V, C>::values(
            bytes@,
        ),
        VerifiedColumn::<V, C>::valid(bytes@) && bytes@.len() <= u32::MAX ==> r is Ok,
        r is Err ==> !VerifiedColumn::<V, C>::valid(bytes@) || bytes@.len() > u32::MAX,
{
    VerifiedDecoder::<V, C>::try_new(bytes)
}

/// What `next` says of any decoder, each thing on its own and with no
/// lemma: the value is the first of those the decoder had, as it gives
/// them and as specifications have them, and those after it are left;
/// and a decoder that gives nothing had nothing, and has nothing.
#[allow(dead_code)]
fn told_by_a_decoded_value<'a, V: VValue<C>, C: VCodec>(dec: &mut VerifiedDecoder<'a, V, C>) -> (r: Option<Get<'a, V>>)
    ensures
        match r {
            Some(v) => {
                &&& old(dec).items().len() > 0
                &&& v == old(dec).items()[0]
                &&& final(dec).items() == old(dec).items().drop_first()
                &&& old(dec).rest().len() > 0
                &&& v.sv() == old(dec).rest()[0]
                &&& final(dec).rest() == old(dec).rest().skip(1)
            },
            None => {
                &&& old(dec).items().len() == 0
                &&& old(dec).rest().len() == 0
                &&& final(dec).items() == old(dec).items()
                &&& final(dec).rest().len() == 0
            },
        },
{
    dec.next()
}

/// What `nth` says: the value at that place of those the decoder had,
/// in either form, and those after it left; or nothing, and nothing
/// left, where it had no more than `n`.
#[allow(dead_code)]
fn told_by_a_decoders_nth<'a, V: VValue<C>, C: VCodec>(dec: &mut VerifiedDecoder<'a, V, C>, n: usize) -> (r: Option<
    Get<'a, V>,
>)
    ensures
        n < old(dec).rest().len() ==> {
            &&& r matches Some(v) && v == old(dec).items()[n as int] && v.sv() == old(dec).rest()[n as int]
            &&& final(dec).items() == old(dec).items().skip(n + 1)
            &&& final(dec).rest() == old(dec).rest().skip(n + 1)
        },
        n >= old(dec).rest().len() ==> {
            &&& r is None
            &&& final(dec).items().len() == 0
            &&& final(dec).rest().len() == 0
        },
{
    proof {
        dec.lemma_items_len();
    }
    dec.nth(n)
}

/// What `advance_by` says: the values after the first `n` are left, in
/// either form; all of them where `n` is 0, and none where the decoder
/// had fewer.
#[allow(dead_code)]
fn told_by_an_advance<'a, V: VValue<C>, C: VCodec>(dec: &mut VerifiedDecoder<'a, V, C>, n: usize)
    ensures
        n <= old(dec).rest().len() ==> final(dec).rest() == old(dec).rest().skip(n as int),
        n <= old(dec).items().len() ==> final(dec).items() == old(dec).items().skip(n as int),
        n == 0 ==> final(dec).rest() == old(dec).rest() && final(dec).items() == old(dec).items(),
        n > old(dec).rest().len() ==> final(dec).rest().len() == 0 && final(dec).items().len() == 0,
{
    proof {
        dec.lemma_items_len();
        assert(dec.rest().skip(0) =~= dec.rest());
        assert(dec.items().skip(0) =~= dec.items());
    }
    dec.advance_by(n);
}

/// What `next_run` says: the run is the first of the values the decoder
/// had, in either form, counted up to `usize::MAX`; what is left is
/// those less the run; and no run is given only of no values.
#[allow(dead_code)]
fn told_by_a_decoded_run<'a, V: VValue<C>, C: VCodec>(dec: &mut VerifiedDecoder<'a, V, C>) -> (r: Option<
    Run<Get<'a, V>>,
>)
    ensures
        match r {
            Some(run) => {
                &&& head_run_max(old(dec).items(), run.value, run.count as int, usize::MAX as int)
                &&& head_run_max(old(dec).rest(), run.value.sv(), run.count as int, usize::MAX as int)
                &&& run.count >= 1
                &&& run.value.sv() == old(dec).rest()[0]
                &&& final(dec).items() == old(dec).items().skip(run.count as int)
                &&& final(dec).rest() == old(dec).rest().skip(run.count as int)
                &&& final(dec).rest().len() == old(dec).rest().len() - run.count
            },
            None => {
                &&& old(dec).items().len() == 0
                &&& old(dec).rest().len() == 0
                &&& final(dec).items() == old(dec).items()
                &&& final(dec).rest() == old(dec).rest()
            },
        },
{
    let r = dec.next_run();
    proof {
        if let Some(run) = r {
            assert(old(dec).rest()[0] == run.value.sv());
        }
    }
    r
}

/// What `next_run_max` says: as `next_run`, counted up to `max`; and
/// no run is given only of no values or for a `max` of 0, which leaves
/// the decoder as it was.
#[allow(dead_code)]
fn told_by_a_bounded_run<'a, V: VValue<C>, C: VCodec>(dec: &mut VerifiedDecoder<'a, V, C>, max: usize) -> (r: Option<
    Run<Get<'a, V>>,
>)
    ensures
        match r {
            Some(run) => {
                &&& head_run_max(old(dec).items(), run.value, run.count as int, max as int)
                &&& head_run_max(old(dec).rest(), run.value.sv(), run.count as int, max as int)
                &&& 1 <= run.count <= max
                &&& final(dec).items() == old(dec).items().skip(run.count as int)
                &&& final(dec).rest() == old(dec).rest().skip(run.count as int)
            },
            None => {
                &&& max == 0 || old(dec).rest().len() == 0
                &&& final(dec).items() == old(dec).items()
                &&& final(dec).rest() == old(dec).rest()
            },
        },
        max == 0 ==> r is None,
        max > 0 && old(dec).rest().len() > 0 ==> r is Some,
{
    proof {
        dec.lemma_items_len();
    }
    dec.next_run_max(max)
}

/// What `scan_for` says: what it returns is `scan_of` the values the
/// decoder had, and what is left is those less `scan_taken`; so the
/// values passed are before the target, and where the target was found
/// the run of it is passed too.
#[allow(dead_code)]
fn told_by_a_decoders_scan<'a, V: VValue<C>, C: VCodec>(
    dec: &mut VerifiedDecoder<'a, V, C>,
    x: Get<'_, V>,
    max: usize,
) -> (r: (usize, usize))
    ensures
        scan_of::<V>(old(dec).rest(), x.sv(), max as int, r.0 as int, r.1 as int),
        r.0 <= max,
        r.0 <= old(dec).rest().len(),
        forall|i: int| 0 <= i < r.0 ==> V::below(old(dec).rest()[i], x.sv()),
        r.1 > 0 ==> {
            &&& r.0 + r.1 <= max
            &&& r.0 + r.1 <= old(dec).rest().len()
            &&& forall|i: int| r.0 <= i < r.0 + r.1 ==> old(dec).rest()[i] == x.sv()
            &&& final(dec).rest() == old(dec).rest().skip(r.0 + r.1)
            &&& final(dec).items() == old(dec).items().skip(r.0 + r.1)
        },
        r.1 == 0 && r.0 < max && r.0 < old(dec).rest().len() ==> V::below(x.sv(), old(dec).rest()[r.0 as int]),
        r.1 == 0 && (r.0 == max || r.0 == old(dec).rest().len()) ==> final(dec).rest() == old(dec).rest().skip(
            r.0 as int,
        ),
{
    let ghost o = dec.rest();
    let r = dec.scan_for(x, max);
    proof {
        let s = r.0 as int;
        if r.1 > 0 {
            assert forall|i: int| s <= i < s + r.1 implies o[i] == x.sv() by {
                assert(o.skip(s)[i - s] == x.sv());
            }
        }
    }
    r
}

/// What `clone` says: the copy has the same values left, in either
/// form; and reading the copy leaves the decoder where it was.
#[allow(dead_code)]
fn told_by_a_decoders_copy<'a, V: VValue<C>, C: VCodec>(dec: &VerifiedDecoder<'a, V, C>) -> (r: VerifiedDecoder<
    'a,
    V,
    C,
>)
    ensures
        r.items() == dec.items(),
        r.rest() == dec.rest(),
{
    let mut other = dec.clone();
    let _ = other.next();
    assert(other.rest().len() == 0 || other.rest() == dec.rest().skip(1));
    dec.clone()
}

/// What a `for` loop over a decoder may say in its invariant of the
/// value it is about to be given: that it is the next of those the
/// decoder had (Verus's `peek` of an iterator). So this one knows, of
/// the value it kept, that it is the last.
#[allow(dead_code)]
fn told_by_a_decoders_loop<'a, V: VValue<C>, C: VCodec>(dec: VerifiedDecoder<'a, V, C>) -> (last: Option<Get<'a, V>>)
    ensures
        match last {
            Some(v) => dec.items().len() > 0 && v == dec.items().last(),
            None => dec.items().len() == 0,
        },
{
    let ghost all = dec.items();
    let mut last: Option<Get<'a, V>> = None;
    for x in at: dec
        invariant
            at.seq() == all,
            at.index() < all.len() ==> x == all[at.index()],
            match last {
                Some(v) => at.index() > 0 && v == all[at.index() - 1],
                None => at.index() == 0,
            },
    {
        last = Some(x);
    }
    last
}

/// A run counted up to `max` is one run and no other, and `head_len`
/// is its length: so the client knows what `scan_taken` is where it
/// knows the values.
#[allow(dead_code)]
proof fn the_run_at_the_head<S>(o: Seq<S>, v: S, n: int, w: S, m: int, max: int)
    requires
        head_run_max(o, v, n, max),
    ensures
        v == o[0],
        head_len(o, max) == n,
        head_run_max(o, w, m, max) ==> w == v && m == n,
{
    lemma_head_len(o, v, n, max);
    if head_run_max(o, w, m, max) {
        lemma_head_run_max(o, v, n, w, m, max);
    }
}

/// Bytes loaded as a prefix column: they load where they would load as
/// a plain column and the sums of their values fit the type they are
/// kept in, and nowhere else; a column they load as is of their values,
/// whose sums fit. For every value type that is summed, and every
/// codec.
#[allow(dead_code)]
fn load_with_sums<V: VSummed + VValue<C>, C: VCodec>(bytes: &[u8]) -> (r: Option<VerifiedPrefixColumn<V, C>>)
    ensures
        VerifiedColumn::<V, C>::loadable(bytes@) && sums_fit::<V>(VerifiedColumn::<V, C>::values(bytes@)) ==> r is Some,
        r matches Some(c) ==> c@ == VerifiedColumn::<V, C>::values(bytes@) && sums_fit::<V>(c@),
        !sums_fit::<V>(VerifiedColumn::<V, C>::values(bytes@)) ==> r is None,
{
    load_as_a_kind::<V, C, SumIndex>(bytes)
}

/// The sums of no more than `V::safe()` values fit, whatever the
/// values: bytes of so few load as a prefix column where they load as a
/// plain one.
#[allow(dead_code)]
fn load_short_sums<V: VSummed + VValue<C>, C: VCodec>(bytes: &[u8]) -> (r: Option<VerifiedPrefixColumn<V, C>>)
    ensures
        VerifiedColumn::<V, C>::loadable(bytes@) && VerifiedColumn::<V, C>::values(bytes@).len() <= V::safe() ==> r is Some,
{
    proof {
        let vs = VerifiedColumn::<V, C>::values(bytes@);
        if vs.len() <= V::safe() {
            lemma_sums_fit::<V>(vs);
        }
    }
    load_with_sums::<V, C>(bytes)
}

/// `u64`s are summed in a `u128`, and `i64`s in an `i128`, which no
/// column is long enough to overflow: whatever loads as a plain column
/// of them loads as a prefix column.
#[allow(dead_code)]
fn load_wide_sums(bytes: &[u8], signed: &[u8]) -> (r: (Option<VerifiedPrefixColumn<u64>>, Option<VerifiedPrefixColumn<Option<i64>>>))
    ensures
        VerifiedColumn::<u64>::loadable(bytes@) ==> r.0 is Some,
        VerifiedColumn::<Option<i64>>::loadable(signed@) ==> r.1 is Some,
{
    (load_short_sums::<u64, Leb128>(bytes), load_short_sums::<Option<i64>, Leb128>(signed))
}

/// `u32`s are summed in a `u64`: 2^32 + 1 of them cannot overflow it,
/// and more can.
#[allow(dead_code)]
fn load_small_sums(bytes: &[u8]) -> (r: Option<VerifiedPrefixColumn<u32>>)
    ensures
        VerifiedColumn::<u32>::loadable(bytes@) && VerifiedColumn::<u32>::values(bytes@).len() <= 0x1_0000_0001 ==> r is Some,
        r matches Some(c) ==> summed::<u32>(c@) <= u64::MAX,
{
    let r = load_short_sums::<u32, Leb128>(bytes);
    match &r {
        Some(c) => {
            proof {
                the_sums_of_a_column(c, c@.len() as int);
                assert(c@.take(c@.len() as int) =~= c@);
            }
        },
        None => {},
    }
    r
}

/// What a prefix column holds has sums that fit: what the first `k` of
/// its values add up to is a sum of the type, for every `k`.
#[allow(dead_code)]
proof fn the_sums_of_a_column<V: VSummed + VValue<C>, C: VCodec>(tracked col: &VerifiedPrefixColumn<V, C>, k: int)
    requires
        0 <= k <= col@.len(),
    ensures
        V::Sum::lo() <= summed::<V>(col@.take(k)) <= V::Sum::hi(),
{
    col.lemma_kept();
}

/// What is pushed onto a prefix column is in it, and adds what it adds
/// to a sum that still fits.
#[allow(dead_code)]
fn push_within_the_sums<V: VSummed + VValue<C>, C: VCodec>(col: &mut VerifiedPrefixColumn<V, C>, v: Get<'_, V>)
    ensures
        final(col)@ == old(col)@.push(v.sv()),
        summed::<V>(final(col)@) == summed::<V>(old(col)@) + V::amount(v.sv()),
        V::Sum::lo() <= summed::<V>(final(col)@) <= V::Sum::hi(),
{
    col.push(v);
    proof {
        the_sums_of_a_column(col, col@.len() as int);
        assert(col@.take(col@.len() as int) =~= col@);
        assert(col@.drop_last() =~= old(col)@);
    }
}

/// A push that would take the sum of a column of `u32`s past a `u64`
/// does not return.
#[allow(dead_code)]
fn a_push_past_the_sums(col: &mut VerifiedPrefixColumn<u32>, v: u32)
    requires
        summed::<u32>(old(col)@) + v > u64::MAX,
    ensures
        false,
{
    push_within_the_sums(col, v);
}

/// Nor does a `fill` of more `u32::MAX`s than a `u64` can add up: there
/// is no such prefix column.
#[allow(dead_code)]
fn a_fill_past_the_sums(len: usize) -> (r: VerifiedPrefixColumn<u32>)
    requires
        0x1_0000_0001 < len <= i64::MAX,
        len <= usize::MAX - 3,
    ensures
        false,
{
    let c = VerifiedPrefixColumn::<u32>::fill(len, u32::MAX);
    proof {
        the_sums_of_a_column(&c, c@.len() as int);
        assert(c@.take(c@.len() as int) =~= c@);
        assert(c@ =~= Seq::new(len as nat, |_i: int| u32::MAX));
        lemma_sum_run::<u32>(len as nat, u32::MAX);
        assert(u32::MAX * len > u64::MAX) by (nonlinear_arith)
            requires
                len > 0x1_0000_0001,
        ;
    }
    c
}

/// A prefix column and a plain column of the same values save the same
/// bytes.
#[allow(dead_code)]
fn saved_alike<V: VSummed + VValue<C>, C: VCodec>(plain: &VerifiedColumn<V, C>, sums: &VerifiedPrefixColumn<V, C>) -> (r: (
    Vec<u8>,
    Vec<u8>,
))
    requires
        plain@ == sums@,
    ensures
        r.0@ == r.1@,
{
    (plain.save(), sums.save())
}

/// A plain column's values as a prefix column, made of its runs. The
/// copy does not return unless their sums fit: where it does, they
/// fit.
#[allow(dead_code)]
fn summed_up<V: VSummed + VValue<C>, C: VCodec>(plain: &VerifiedColumn<V, C>) -> (r: VerifiedPrefixColumn<V, C>)
    ensures
        r@ == plain@,
        sums_fit::<V>(plain@),
{
    let r = copied_by_runs::<V, C, LenIndex, SumIndex>(plain);
    proof {
        r.lemma_kept();
    }
    r
}

/// The sum a prefix column answers with for a position is what the
/// values before the position add up to: an integer, whatever the type
/// it is kept in.
#[allow(dead_code)]
fn the_sum_before<V: VSummed + VValue<C>, C: VCodec>(col: &VerifiedPrefixColumn<V, C>, i: usize) -> (r: V::Sum)
    requires
        i <= col@.len(),
    ensures
        r.as_int() == summed::<V>(col@.take(i as int)),
{
    col.get_prefix(i)
}

/// The sum through a position is the sum before it and what the value
/// there adds.
#[allow(dead_code)]
fn the_sum_through<V: VSummed + VValue<C>, C: VCodec>(col: &VerifiedPrefixColumn<V, C>, i: usize) -> (r: (V::Sum, V::Sum))
    requires
        i < col@.len(),
    ensures
        r.1.as_int() == r.0.as_int() + V::amount(col@[i as int]),
{
    let before = col.get_prefix(i);
    let through = col.get_total(i);
    proof {
        assert(col@.take(i + 1).drop_last() =~= col@.take(i as int));
    }
    (before, through)
}

/// The sum of a range of positions is what the values there add up to,
/// which is the sum before its end less the sum before its start.
#[allow(dead_code)]
fn the_sum_of_a_range<V: VSummed + VValue<C>, C: VCodec>(col: &VerifiedPrefixColumn<V, C>, start: usize, end: usize) -> (r: (
    V::Sum,
    V::Sum,
    V::Sum,
))
    requires
        start <= end <= col@.len(),
    ensures
        r.2.as_int() == summed::<V>(col@.subrange(start as int, end as int)),
        r.2.as_int() == r.1.as_int() - r.0.as_int(),
{
    let a = col.get_prefix(start);
    let b = col.get_prefix(end);
    let s = col.sum_range(start, end);
    proof {
        lemma_sum_range::<V>(col@, start as int, end as int);
    }
    (a, b, s)
}

/// In a column of `i64`s, whose sums are `i128`s: the sum of a range is
/// the sum before its end less the sum before its start, and to take
/// the one from the other does not overflow, for what the range adds up
/// to is a sum of the type.
#[allow(dead_code)]
fn a_range_by_its_ends(col: &VerifiedPrefixColumn<i64>, start: usize, end: usize) -> (r: i128)
    requires
        start <= end <= col@.len(),
    ensures
        r == summed::<i64>(col@.subrange(start as int, end as int)),
{
    // a column's length is a `usize`
    let _ = col.len();
    let a = col.get_prefix(start);
    let b = col.get_prefix(end);
    proof {
        col.lemma_kept();
        lemma_sum_range::<i64>(col@, start as int, end as int);
    }
    b - a
}

/// The sums take any position: past the end they are the sum of all the
/// values, and a range that holds no position adds up to nothing.
#[allow(dead_code)]
fn sums_past_the_end<V: VSummed + VValue<C>, C: VCodec>(col: &VerifiedPrefixColumn<V, C>) -> (r: (V::Sum, V::Sum, V::Sum, V::Sum))
    ensures
        r.0.as_int() == summed::<V>(col@),
        r.1.as_int() == summed::<V>(col@),
        r.2.as_int() == summed::<V>(col@),
        r.3.as_int() == 0,
{
    let n = col.len();
    proof {
        assert(col@.subrange(0, col@.len() as int) =~= col@);
    }
    (col.get_prefix(usize::MAX), col.get_total(n), col.sum_range(0, usize::MAX), col.sum_range(5, 3))
}

/// A push adds to the sum of a prefix column what the value adds.
#[allow(dead_code)]
fn a_sum_after_a_push<V: VSummed + VValue<C>, C: VCodec>(col: &mut VerifiedPrefixColumn<V, C>, v: Get<'_, V>) -> (r: (
    V::Sum,
    V::Sum,
))
    ensures
        final(col)@ == old(col)@.push(v.sv()),
        r.1.as_int() == r.0.as_int() + V::amount(v.sv()),
{
    let before = col.get_prefix(col.len());
    col.push(v);
    let after = col.get_prefix(col.len());
    proof {
        assert(col@.drop_last() =~= old(col)@);
    }
    (before, after)
}

/// Where the sums of a column are never negative, the first position
/// that the values before it add up to `target` or more, if there is
/// one: no earlier position's do, and if there is none, all the values
/// add up to less.
#[allow(dead_code)]
fn the_place_of_a_sum<V: VSummed + VValue<C>, C: VCodec>(col: &VerifiedPrefixColumn<V, C>, target: V::Sum) -> (r: Option<usize>)
    where V::Sum: VCount
    ensures
        match r {
            Some(i) => {
                &&& i <= col@.len()
                &&& target.as_int() <= summed::<V>(col@.take(i as int))
                &&& forall|j: int| 0 <= j < i ==> summed::<V>(#[trigger] col@.take(j)) < target.as_int()
            },
            None => summed::<V>(col@) < target.as_int(),
        },
{
    let i = col.get_index_for_prefix(target);
    if i <= col.len() {
        Some(i)
    } else {
        proof {
            assert(col@.take(col@.len() as int) =~= col@);
        }
        None
    }
}

/// In a column of `u64`s, whose sums are `u128`s: the sum before the
/// place found for a target is the target or more, and the sum before
/// the place ahead of it is less.
#[allow(dead_code)]
fn a_sum_and_its_place(col: &VerifiedPrefixColumn<u64>, target: u128) -> (r: Option<(u128, u128)>)
    ensures
        r matches Some((less, more)) ==> less < target <= more,
{
    let i = col.get_index_for_prefix(target);
    if 0 < i && i <= col.len() {
        Some((col.get_prefix(i - 1), col.get_prefix(i)))
    } else {
        None
    }
}

/// The place of the `k`th `true` of a column of flags, counting from
/// zero, where there is one: the flag there is `true`, and `k` of the
/// flags before it are. Where there is none, no more than `k` flags of
/// the column are `true`.
#[allow(dead_code)]
fn the_kth_true(col: &VerifiedPrefixColumn<bool>, k: usize) -> (r: Option<usize>)
    requires
        k < usize::MAX,
    ensures
        match r {
            Some(i) => i < col@.len() && col@[i as int] && summed::<bool>(col@.take(i as int)) == k,
            None => summed::<bool>(col@) <= k,
        },
{
    let i = col.get_index_for_total(k + 1);
    if i < col.len() {
        proof {
            assert(col@.take(i + 1).drop_last() =~= col@.take(i as int));
            assert(col@.take(0).len() == 0);
        }
        Some(i)
    } else {
        proof {
            assert(col@.take(col@.len() as int) =~= col@);
        }
        None
    }
}

/// The two searches say enough to tell what each finds: the place for
/// a sum through a position is the one ahead of the place for a sum
/// before a position, and the first place where that is the first.
#[allow(dead_code)]
fn the_two_places<V: VSummed + VValue<C>, C: VCodec>(col: &VerifiedPrefixColumn<V, C>, target: V::Sum) -> (r: (usize, usize))
    where V::Sum: VCount
    ensures
        r.0 == 0 ==> r.1 == 0,
        r.0 > 0 ==> r.1 == r.0 - 1,
{
    let before = col.get_index_for_prefix(target);
    let through = col.get_index_for_total(target);
    proof {
        if before == 0 {
            // a target that no values reach: no sum is less
            assert(col@.take(0).len() == 0);
            if through > 0 {
                lemma_sum_bound::<V>(col@.take(through as int));
                V::Sum::lemma_count();
            }
        } else {
            if through < before - 1 {
                assert(summed::<V>(col@.take(through + 1)) < target.as_int());
            }
            if through >= before {
                assert(summed::<V>(col@.take(before as int)) < target.as_int());
            }
        }
    }
    (before, through)
}

/// A `for` loop over a prefix column's iterator gives each value with
/// the running sum through it: here the sums go into a `Vec`, which
/// then holds, for each position, what the values through it add up to.
/// For every summed type and codec.
#[allow(dead_code)]
fn running_sums<V: VSummed + VValue<C>, C: VCodec>(col: &VerifiedPrefixColumn<V, C>) -> (out: Vec<V::Sum>)
    ensures
        out@.len() == col@.len(),
        forall|i: int| 0 <= i < out@.len() ==> (#[trigger] out@[i]).as_int() == summed::<V>(col@.take(i + 1)),
{
    let it = col.prefix_iter();
    let ghost all = it.items();
    proof {
        it.lemma_items();
    }
    let mut out: Vec<V::Sum> = Vec::new();
    for pv in at: it
        invariant
            at.seq() == all,
            out@.len() == at.index(),
            forall|i: int| 0 <= i < out@.len() ==> #[trigger] out@[i] == all[i].total(),
    {
        out.push(pv.total());
    }
    out
}

/// The values of a range of a prefix column, each with what the values
/// before it add up to: counted from the column's start, though the
/// iterator reads only the range.
#[allow(dead_code)]
fn sums_in_a_range<'a, V: VSummed + VValue<C>, C: VCodec>(col: &'a VerifiedPrefixColumn<V, C>, a: usize, b: usize) -> (out: Vec<
    (Get<'a, V>, V::Sum),
>)
    requires
        a <= b <= col@.len(),
    ensures
        out@.len() == b - a,
        forall|i: int|
            0 <= i < out@.len() ==> {
                &&& (#[trigger] out@[i]).0.sv() == col@[a + i]
                &&& out@[i].1.as_int() == summed::<V>(col@.take(a + i))
            },
{
    let it = col.prefix_iter_range(a, b);
    let ghost all = it.items();
    proof {
        it.lemma_items();
    }
    let mut out: Vec<(Get<'a, V>, V::Sum)> = Vec::new();
    for pv in at: it
        invariant
            at.seq() == all,
            out@.len() == at.index(),
            forall|i: int|
                0 <= i < out@.len() ==> (#[trigger] out@[i]).0 == all[i].value && out@[i].1 == all[i].prefix(),
    {
        out.push((pv.value, pv.prefix()));
    }
    out
}

/// One value of a prefix column with its sums: the value at the
/// position, what the values before it add up to and what those through
/// it do, which is more by what the value adds.
#[allow(dead_code)]
fn a_value_with_its_sums<'a, V: VSummed + VValue<C>, C: VCodec>(col: &'a VerifiedPrefixColumn<V, C>, i: usize) -> (r: (
    V::Sum,
    Get<'a, V>,
    V::Sum,
))
    requires
        i < col@.len(),
    ensures
        r.1.sv() == col@[i as int],
        r.0.as_int() == summed::<V>(col@.take(i as int)),
        r.2.as_int() == summed::<V>(col@.take(i + 1)),
        r.2.as_int() == r.0.as_int() + V::amount(col@[i as int]),
{
    let pv = col.get_prefixed(i).unwrap();
    (pv.prefix(), pv.value, pv.total())
}

/// The sums that come with a value are the sums the column answers
/// with for its position.
#[allow(dead_code)]
fn sums_two_ways<V: VSummed + VValue<C>, C: VCodec>(col: &VerifiedPrefixColumn<V, C>, i: usize) -> (r: (
    (V::Sum, V::Sum),
    (V::Sum, V::Sum),
))
    requires
        i < col@.len(),
    ensures
        r.0 == r.1,
{
    let pv = col.get_prefixed(i).unwrap();
    let (a, b) = (pv.prefix(), pv.total());
    let (c, d) = (col.get_prefix(i), col.get_total(i));
    proof {
        // sums that are the same number are the same sum
        V::Sum::lemma_as_int(a);
        V::Sum::lemma_as_int(b);
        V::Sum::lemma_as_int(c);
        V::Sum::lemma_as_int(d);
    }
    ((a, b), (c, d))
}

/// A step of a prefix column's iterator, wherever it stands: the sum
/// before the value it gives is the running sum as it was, the sum
/// through the value is the running sum after, and the second is more
/// by what the value adds.
#[allow(dead_code)]
fn a_step_with_its_sums<'a, V: VSummed + VValue<C>, C: VCodec>(it: &mut VerifiedPrefixIter<'a, V, C>) -> (r: Option<
    (V::Sum, Get<'a, V>, V::Sum),
>)
    ensures
        match r {
            Some((before, v, through)) => {
                &&& old(it).rest().len() > 0
                &&& v.sv() == old(it).rest()[0]
                &&& before == old(it).total()
                &&& through == final(it).total()
                &&& through.as_int() == before.as_int() + V::amount(v.sv())
                &&& final(it).rest() == old(it).rest().skip(1)
            },
            None => old(it).rest().len() == 0 && final(it).total() == old(it).total(),
        },
{
    let before = it.total();
    match it.next() {
        Some(pv) => {
            let through = it.total();
            Some((before, pv.value, through))
        },
        None => None,
    }
}

/// What the values of a range add up to, read with an iterator: moved
/// to where the range starts, its sum started again there, and moved to
/// where the range ends. It is what the column answers with for the
/// range.
#[allow(dead_code)]
fn summed_by_moving<V: VSummed + VValue<C>, C: VCodec>(col: &VerifiedPrefixColumn<V, C>, a: usize, b: usize) -> (r: (
    V::Sum,
    V::Sum,
))
    requires
        a <= b <= col@.len(),
    ensures
        r.0.as_int() == summed::<V>(col@.subrange(a as int, b as int)),
        r.0 == r.1,
{
    let mut it = col.prefix_iter();
    it.advance_to(a);
    it.reset_prefix();
    it.advance_to(b);
    let moved = it.total();
    let asked = col.sum_range(a, b);
    proof {
        V::Sum::lemma_as_int(moved);
        V::Sum::lemma_as_int(asked);
    }
    (moved, asked)
}

/// Two values read with one iterator, which steps over the values
/// before each: the sums through them count the values stepped over.
#[allow(dead_code)]
fn two_skips<V: VSummed + VValue<C>, C: VCodec>(col: &VerifiedPrefixColumn<V, C>, i: usize, j: usize) -> (r: (V::Sum, V::Sum))
    requires
        i < j < col@.len(),
    ensures
        r.0.as_int() == summed::<V>(col@.take(i + 1)),
        r.1.as_int() == summed::<V>(col@.take(j + 1)),
{
    let mut it = col.prefix_iter();
    let a = it.nth(i).unwrap();
    let b = it.nth(j - i - 1).unwrap();
    (a.total(), b.total())
}

/// In a column of `u64`s, the first position that the values through
/// it add up to more than `limit`, found by reading on until the
/// running sum is past it: no sum before is, and if there is no such
/// position, no sum of the column is.
#[allow(dead_code)]
fn first_over(col: &VerifiedPrefixColumn<u64>, limit: u128) -> (r: Option<usize>)
    ensures
        match r {
            Some(i) => {
                &&& i < col@.len()
                &&& summed::<u64>(col@.take(i + 1)) > limit
                &&& forall|j: int| 0 <= j <= i ==> summed::<u64>(#[trigger] col@.take(j)) <= limit
            },
            None => forall|j: int| 0 <= j <= col@.len() ==> summed::<u64>(#[trigger] col@.take(j)) <= limit,
        },
{
    let mut it = col.prefix_iter();
    proof {
        assert(col@.take(0).len() == 0);
    }
    loop
        invariant
            it.column() == col@,
            it.base_pos() == 0,
            it.end_pos() == col@.len(),
            forall|j: int| 0 <= j <= it.pos() ==> summed::<u64>(#[trigger] col@.take(j)) <= limit,
        decreases it.rest().len(),
    {
        let i = it.pos();
        match it.next() {
            Some(pv) => {
                if pv.total() > limit {
                    return Some(i);
                }
            },
            None => {
                return None;
            },
        }
    }
}

/// Where each move leaves a prefix column's iterator, its window and
/// its running sum: the sum is of the values the iterator has passed,
/// whether it read them or stepped over them, and a move of the window
/// alone leaves it.
#[allow(dead_code)]
fn moved_with_sums(col: &VerifiedPrefixColumn<u64>)
    requires
        col@.len() == 10,
{
    let ghost upto = |k: int| summed::<u64>(col@.take(k));
    // an iterator of a range: its sum counts from the column's start
    let mut it = col.prefix_iter_range(2, 8);
    let (p, e, n, w, t) = (it.pos(), it.end_pos(), it.len(), it.items_left(), it.total());
    assert(p == 2 && e == 8 && n == 6 && w == 6 && t == upto(2));
    // a step
    let a = it.next().unwrap();
    let t = it.total();
    assert(a.value == col@[2] && a.prefix() == upto(2) && a.total() == upto(3) && t == upto(3));
    // values stepped over are summed
    it.advance_by(2);
    let (p, t) = (it.pos(), it.total());
    assert(p == 5 && t == upto(5));
    // a place behind the iterator: it stays, and so does its sum
    it.advance_to(4);
    let (p, t2) = (it.pos(), it.total());
    assert(p == 5 && t2 == t);
    // a wider window, which ends past the column: the sum stays
    it.set_max(20);
    let (e, n, w, t3) = (it.end_pos(), it.len(), it.items_left(), it.total());
    assert(e == 20 && n == 5 && w == 15 && t3 == t);
    // past what it has: all of it is passed, and summed
    it.advance_by(9);
    let (p, e, n, t) = (it.pos(), it.end_pos(), it.len(), it.total());
    assert(p == 10 && e == 10 && n == 0);
    assert(t == summed::<u64>(col@)) by {
        assert(col@.take(10) =~= col@);
    }
    // the window moved to a range, and then to one that starts behind
    // the iterator
    let mut it = col.prefix_iter();
    it.shift(3..7);
    let (p, e, n, t) = (it.pos(), it.end_pos(), it.len(), it.total());
    assert(p == 3 && e == 7 && n == 4 && t == upto(3));
    it.shift(1..5);
    let (p, e, n, t) = (it.pos(), it.end_pos(), it.len(), it.total());
    assert(p == 3 && e == 5 && n == 2 && t == upto(3));
    // the first value of a window, with its sums
    let b = it.shift_next(6..9).unwrap();
    let (p, e, n, t) = (it.pos(), it.end_pos(), it.len(), it.total());
    assert(b.value == col@[6] && b.prefix() == upto(6) && b.total() == upto(7));
    assert(p == 7 && e == 9 && n == 2 && t == upto(7));
    // a window with no value
    let c = it.shift_next(9..9);
    let (p, e, t) = (it.pos(), it.end_pos(), it.total());
    assert(c is None && p == 9 && e == 9 && t == upto(9));
    // the sum started again: it counts from where the iterator stands
    let mut it = col.prefix_iter_range(4, 10);
    it.reset_prefix();
    let (p, e, t) = (it.pos(), it.end_pos(), it.total());
    assert(p == 4 && e == 10 && t == 0);
    let d = it.nth(2).unwrap();
    let t = it.total();
    assert(d.value == col@[6] && t == d.total());
    assert(d.prefix() == summed::<u64>(col@.subrange(4, 6)) && d.total() == summed::<u64>(col@.subrange(4, 7)));
}

/// Of an iterator a client is handed, wherever it stands and wherever
/// its sum counts from: the running sum is what the column's values
/// from there up to the iterator add up to.
#[allow(dead_code)]
fn told_of_the_running_sum<V: VSummed + VValue<C>, C: VCodec>(it: &VerifiedPrefixIter<'_, V, C>) -> (r: V::Sum)
    ensures
        it.base_pos() <= it.pos() <= it.column().len(),
        r == it.total(),
        r.as_int() == summed::<V>(it.column().subrange(it.base_pos() as int, it.pos() as int)),
{
    it.total()
}

/// The running sum under its other name: before the value the iterator
/// is to give next.
#[allow(dead_code)]
fn told_of_the_sum_before<V: VSummed + VValue<C>, C: VCodec>(it: &VerifiedPrefixIter<'_, V, C>) -> (r: V::Sum)
    ensures
        it.base_pos() <= it.pos() <= it.column().len(),
        r == it.total(),
        r.as_int() == summed::<V>(it.column().subrange(it.base_pos() as int, it.pos() as int)),
{
    it.prefix()
}

/// Of an iterator a client is handed, what it has left to give: each of
/// the values left, with the sums before it and through it; and that
/// the sums of the column it reads fit, and that it reads the values
/// of its column.
#[allow(dead_code)]
fn told_of_what_is_left<V: VSummed + VValue<C>, C: VCodec>(it: &VerifiedPrefixIter<'_, V, C>)
    ensures
        it.items().len() == it.rest().len(),
        forall|i: int|
            0 <= i < it.items().len() ==> {
                &&& (#[trigger] it.items()[i]).value.sv() == it.rest()[i]
                &&& it.items()[i].total().as_int() == it.sum_to(it.pos() + i + 1)
                &&& it.items()[i].prefix().as_int() == it.sum_to(it.pos() + i)
                &&& it.items()[i].total().as_int() == it.items()[i].prefix().as_int() + V::amount(it.rest()[i])
            },
        it.base_pos() <= it.pos() <= it.column().len(),
        it.total().as_int() == it.sum_to(it.pos() as int),
        sums_fit::<V>(it.column()),
        match it.source() {
            Some(c) => it.column() == c@,
            None => it.column().len() == 0,
        },
{
    proof {
        it.lemma_items();
        it.lemma_total();
        it.lemma_source();
    }
}

/// What `prefix_iter` says by itself, of any prefix column: the
/// iterator reads the column, has all its values left, and its sum
/// counts from the column's start and is nothing yet.
#[allow(dead_code)]
fn told_by_a_prefix_iter<'a, V: VSummed + VValue<C>, C: VCodec>(col: &'a VerifiedPrefixColumn<V, C>) -> (it: VerifiedPrefixIter<
    'a,
    V,
    C,
>)
    ensures
        it.rest() == col@,
        it.column() == col@,
        it.source() == Some(*col),
        it.pos() == 0,
        it.end_pos() == col@.len(),
        it.base_pos() == 0,
        it.total().as_int() == 0,
{
    col.prefix_iter()
}

/// What `prefix_iter_range` says by itself: the iterator has the values
/// of the range left, and its sum counts from the column's start, so it
/// is what the values before the range add up to.
#[allow(dead_code)]
fn told_by_a_prefix_iter_range<'a, V: VSummed + VValue<C>, C: VCodec>(
    col: &'a VerifiedPrefixColumn<V, C>,
    start: usize,
    end: usize,
) -> (it: VerifiedPrefixIter<'a, V, C>)
    requires
        start <= end <= col@.len(),
    ensures
        it.rest() == col@.subrange(start as int, end as int),
        it.column() == col@,
        it.source() == Some(*col),
        it.pos() == start,
        it.end_pos() == end,
        it.base_pos() == 0,
        it.total().as_int() == summed::<V>(col@.take(start as int)),
{
    col.prefix_iter_range(start, end)
}

/// What `get_prefixed` says by itself, of any position.
#[allow(dead_code)]
fn told_by_a_get_prefixed<'a, V: VSummed + VValue<C>, C: VCodec>(col: &'a VerifiedPrefixColumn<V, C>, index: usize) -> (r: Option<
    VerifiedPrefixedValue<'a, V>,
>)
    ensures
        index < col@.len() ==> {
            &&& r matches Some(pv)
            &&& pv.value.sv() == col@[index as int]
            &&& pv.total().as_int() == summed::<V>(col@.take(index + 1))
            &&& pv.prefix().as_int() == summed::<V>(col@.take(index as int))
            &&& pv.total().as_int() == pv.prefix().as_int() + V::amount(pv.value.sv())
        },
        index >= col@.len() ==> r is None,
{
    col.get_prefixed(index)
}

/// What a value with its sums says of them: the sum through the value
/// is the one it holds, and the sum before is that less what the value
/// adds, in the arithmetic of the sums' type.
#[allow(dead_code)]
fn told_by_a_prefixed_value<'a, V: VSummed>(pv: &VerifiedPrefixedValue<'a, V>) -> (r: (V::Sum, V::Sum))
    ensures
        r.0 == pv.total,
        r.1 == V::Sum::of_int(pv.total.as_int() - V::amount(pv.value.sv())),
        r.0 == pv.total(),
        r.1 == pv.prefix(),
{
    let copy = pv.clone();
    assert(copy == *pv);
    (pv.total(), copy.prefix())
}

/// What `next` says by itself of a prefix column's iterator: in the
/// words of the iterator's own specification, and in those of Verus's
/// specification of an iterator.
#[allow(dead_code)]
fn told_by_a_prefixed_next<'a, V: VSummed + VValue<C>, C: VCodec>(it: &mut VerifiedPrefixIter<'a, V, C>) -> (r: Option<
    VerifiedPrefixedValue<'a, V>,
>)
    ensures
        match r {
            Some(pv) => {
                &&& old(it).rest().len() > 0
                &&& pv.value.sv() == old(it).rest()[0]
                &&& pv.prefix() == old(it).total()
                &&& pv.total() == final(it).total()
                &&& pv.prefix().as_int() == old(it).sum_to(old(it).pos() as int)
                &&& pv.total().as_int() == old(it).sum_to(old(it).pos() + 1)
                &&& pv.total().as_int() == pv.prefix().as_int() + V::amount(pv.value.sv())
                &&& final(it).rest() == old(it).rest().skip(1)
                &&& final(it).pos() == old(it).pos() + 1
                &&& final(it).end_pos() == old(it).end_pos()
            },
            None => {
                &&& old(it).rest().len() == 0
                &&& final(it).rest().len() == 0
                &&& final(it).pos() == old(it).pos()
                &&& final(it).end_pos() == old(it).pos()
                &&& final(it).total() == old(it).total()
            },
        },
        final(it).column() == old(it).column(),
        final(it).source() == old(it).source(),
        final(it).base_pos() == old(it).base_pos(),
        old(it).items().len() > 0 ==> r == Some(old(it).items()[0]) && final(it).items() == old(it).items().drop_first(),
        old(it).items().len() == 0 ==> r is None && final(it).items() == old(it).items(),
{
    it.next()
}

/// What `nth` says by itself of a prefix column's iterator.
#[allow(dead_code)]
fn told_by_a_prefixed_nth<'a, V: VSummed + VValue<C>, C: VCodec>(it: &mut VerifiedPrefixIter<'a, V, C>, n: usize) -> (r: Option<
    VerifiedPrefixedValue<'a, V>,
>)
    ensures
        n < old(it).rest().len() ==> {
            &&& r == Some(old(it).items()[n as int])
            &&& r matches Some(pv)
            &&& pv.value.sv() == old(it).rest()[n as int]
            &&& pv.total() == final(it).total()
            &&& pv.total().as_int() == old(it).sum_to(old(it).pos() + n + 1)
            &&& pv.prefix().as_int() == old(it).sum_to(old(it).pos() + n)
            &&& pv.total().as_int() == pv.prefix().as_int() + V::amount(pv.value.sv())
            &&& final(it).items() == old(it).items().skip(n + 1)
            &&& final(it).rest() == old(it).rest().skip(n + 1)
            &&& final(it).pos() == old(it).pos() + n + 1
            &&& final(it).end_pos() == old(it).end_pos()
        },
        n >= old(it).rest().len() ==> {
            &&& r is None
            &&& final(it).rest().len() == 0
            &&& final(it).pos() == old(it).pos() + old(it).rest().len()
            &&& final(it).end_pos() == final(it).pos()
        },
        final(it).column() == old(it).column(),
        final(it).source() == old(it).source(),
        final(it).base_pos() == old(it).base_pos(),
        final(it).total().as_int() == old(it).sum_to(final(it).pos() as int),
{
    it.nth(n)
}

/// What the questions put to a prefix column's iterator say by
/// themselves. Asked where it stands, it says where: within its
/// window, in its column and not before where its sum counts from.
#[allow(dead_code)]
fn told_by_a_prefix_iters_place<V: VSummed + VValue<C>, C: VCodec>(it: &VerifiedPrefixIter<'_, V, C>) -> (r: usize)
    ensures
        r == it.pos(),
        it.base_pos() <= it.pos() <= it.end_pos(),
        it.pos() <= it.column().len(),
        it.rest() == window(it.column(), it.pos() as int, it.end_pos() as int),
{
    it.pos()
}

/// Asked how far its window reaches and how many values it has left,
/// an iterator says each: the first counts positions, which may be
/// past the column, and the second values.
#[allow(dead_code)]
fn told_by_a_prefix_iters_count<V: VSummed + VValue<C>, C: VCodec>(it: &VerifiedPrefixIter<'_, V, C>) -> (r: (
    usize,
    usize,
))
    ensures
        r.0 == it.end_pos() - it.pos(),
        r.1 == it.rest().len(),
        r.1 == it.items().len(),
{
    (it.items_left(), it.len())
}

/// Asked how many values it may yet give, an iterator says exactly how
/// many it has left, as its least and as its most.
#[allow(dead_code)]
fn told_by_a_prefix_size_hint<V: VSummed + VValue<C>, C: VCodec>(it: &VerifiedPrefixIter<'_, V, C>) -> (r: (
    usize,
    Option<usize>,
))
    ensures
        r.0 == it.rest().len(),
        r.0 == it.items().len(),
        r.1 == Some(r.0),
{
    it.size_hint()
}

/// Asked for the window's end alone, an iterator says as much.
#[allow(dead_code)]
fn told_by_a_prefix_iters_end<V: VSummed + VValue<C>, C: VCodec>(it: &VerifiedPrefixIter<'_, V, C>) -> (r: usize)
    ensures
        r == it.end_pos(),
        it.base_pos() <= it.pos() <= it.end_pos(),
        it.pos() <= it.column().len(),
        it.rest() == window(it.column(), it.pos() as int, it.end_pos() as int),
{
    it.end_pos()
}

/// What `reset_prefix` says by itself: the iterator and its window stay,
/// and its sum counts from where it stands and is nothing yet.
#[allow(dead_code)]
fn told_by_a_reset<V: VSummed + VValue<C>, C: VCodec>(it: &mut VerifiedPrefixIter<'_, V, C>)
    ensures
        final(it).rest() == old(it).rest(),
        final(it).column() == old(it).column(),
        final(it).source() == old(it).source(),
        final(it).pos() == old(it).pos(),
        final(it).end_pos() == old(it).end_pos(),
        final(it).base_pos() == old(it).pos(),
        final(it).total().as_int() == 0,
{
    it.reset_prefix()
}

/// What `set_max` says by itself of a prefix column's iterator.
#[allow(dead_code)]
fn told_by_a_prefix_set_max<V: VSummed + VValue<C>, C: VCodec>(it: &mut VerifiedPrefixIter<'_, V, C>, end: usize)
    ensures
        final(it).column() == old(it).column(),
        final(it).source() == old(it).source(),
        final(it).base_pos() == old(it).base_pos(),
        final(it).total() == old(it).total(),
        final(it).pos() == old(it).pos(),
        final(it).end_pos() == if end >= old(it).pos() {
            end
        } else {
            old(it).pos()
        },
        final(it).rest() == window(old(it).column(), old(it).pos() as int, end as int),
{
    it.set_max(end)
}

/// What `advance_to` says by itself of a prefix column's iterator.
#[allow(dead_code)]
fn told_by_a_prefix_advance_to<V: VSummed + VValue<C>, C: VCodec>(it: &mut VerifiedPrefixIter<'_, V, C>, target: usize)
    ensures
        final(it).column() == old(it).column(),
        final(it).source() == old(it).source(),
        final(it).base_pos() == old(it).base_pos(),
        final(it).total().as_int() == old(it).sum_to(final(it).pos() as int),
        final(it).rest() == past(old(it).rest(), target - old(it).pos()),
        target <= old(it).pos() ==> {
            &&& final(it).pos() == old(it).pos()
            &&& final(it).end_pos() == old(it).end_pos()
        },
        old(it).pos() < target <= old(it).pos() + old(it).rest().len() ==> {
            &&& final(it).pos() == target
            &&& final(it).end_pos() == old(it).end_pos()
        },
        target > old(it).pos() + old(it).rest().len() ==> {
            &&& final(it).pos() == old(it).pos() + old(it).rest().len()
            &&& final(it).end_pos() == final(it).pos()
        },
{
    it.advance_to(target)
}

/// What `advance_by` says by itself of a prefix column's iterator.
#[allow(dead_code)]
fn told_by_a_prefix_advance_by<V: VSummed + VValue<C>, C: VCodec>(it: &mut VerifiedPrefixIter<'_, V, C>, amount: usize)
    ensures
        final(it).column() == old(it).column(),
        final(it).source() == old(it).source(),
        final(it).base_pos() == old(it).base_pos(),
        final(it).total().as_int() == old(it).sum_to(final(it).pos() as int),
        final(it).rest() == past(old(it).rest(), amount as int),
        amount <= old(it).rest().len() ==> {
            &&& final(it).pos() == old(it).pos() + amount
            &&& final(it).end_pos() == old(it).end_pos()
        },
        amount > old(it).rest().len() ==> {
            &&& final(it).pos() == old(it).pos() + old(it).rest().len()
            &&& final(it).end_pos() == final(it).pos()
        },
{
    it.advance_by(amount)
}

/// What `shift` says by itself of a prefix column's iterator.
#[allow(dead_code)]
fn told_by_a_prefix_shift<V: VSummed + VValue<C>, C: VCodec>(it: &mut VerifiedPrefixIter<'_, V, C>, range: Range<usize>)
    ensures
        final(it).column() == old(it).column(),
        final(it).source() == old(it).source(),
        final(it).base_pos() == old(it).base_pos(),
        final(it).total().as_int() == old(it).sum_to(final(it).pos() as int),
        final(it).rest() == window(
            old(it).column(),
            if range.start <= old(it).pos() {
                old(it).pos() as int
            } else {
                range.start as int
            },
            range.end as int,
        ),
        range.start <= old(it).pos() ==> {
            &&& final(it).pos() == old(it).pos()
            &&& final(it).end_pos() == if range.end >= old(it).pos() {
                range.end
            } else {
                old(it).pos()
            }
        },
        old(it).pos() < range.start <= range.end && range.start <= old(it).column().len() ==> {
            &&& final(it).pos() == range.start
            &&& final(it).end_pos() == range.end
        },
        old(it).pos() < range.start && (range.start > range.end || range.start > old(it).column().len()) ==> {
            &&& final(it).pos() == if range.end <= old(it).pos() {
                old(it).pos() as int
            } else if range.end <= old(it).column().len() {
                range.end as int
            } else {
                old(it).column().len() as int
            }
            &&& final(it).end_pos() == final(it).pos()
        },
{
    it.shift(range)
}

/// What `shift_next` says by itself of a prefix column's iterator.
#[allow(dead_code)]
fn told_by_a_prefix_shift_next<'a, V: VSummed + VValue<C>, C: VCodec>(
    it: &mut VerifiedPrefixIter<'a, V, C>,
    range: Range<usize>,
) -> (r: Option<VerifiedPrefixedValue<'a, V>>)
    requires
        range.start >= old(it).pos(),
    ensures
        final(it).column() == old(it).column(),
        final(it).source() == old(it).source(),
        final(it).base_pos() == old(it).base_pos(),
        final(it).total().as_int() == old(it).sum_to(final(it).pos() as int),
        ({
            let w = window(old(it).column(), range.start as int, range.end as int);
            &&& w.len() > 0 ==> {
                &&& r matches Some(pv)
                &&& pv.value.sv() == w[0]
                &&& pv.total() == final(it).total()
                &&& pv.total().as_int() == old(it).sum_to(range.start + 1)
                &&& pv.prefix().as_int() == old(it).sum_to(range.start as int)
                &&& pv.total().as_int() == pv.prefix().as_int() + V::amount(pv.value.sv())
                &&& final(it).rest() == w.skip(1)
                &&& final(it).pos() == range.start + 1
                &&& final(it).end_pos() == range.end
            }
            &&& w.len() == 0 ==> {
                &&& r is None
                &&& final(it).rest().len() == 0
                &&& final(it).pos() == if range.end <= old(it).pos() {
                    old(it).pos() as int
                } else if range.end <= old(it).column().len() {
                    range.end as int
                } else {
                    old(it).column().len() as int
                }
                &&& final(it).end_pos() == final(it).pos()
            }
        }),
{
    it.shift_next(range)
}

/// What a prefix column's values add up to, read a run at a time: when
/// no run is left, the running sum is the sum of the column, and the
/// runs' counts have added up to its length. For every summed type and
/// codec.
#[allow(dead_code)]
fn summed_by_runs<V: VSummed + VValue<C>, C: VCodec>(col: &VerifiedPrefixColumn<V, C>) -> (r: (V::Sum, usize))
    ensures
        r.0.as_int() == summed::<V>(col@),
        r.1 == col@.len(),
{
    let mut it = col.prefix_iter();
    let mut count: usize = 0;
    loop
        invariant
            it.column() == col@,
            it.base_pos() == 0,
            it.rest().len() == col@.len() - it.pos(),
            count == it.pos(),
        decreases it.rest().len(),
    {
        match it.next_run() {
            Some(run) => {
                count = count + run.count;
            },
            None => {
                let sum = it.total();
                proof {
                    assert(col@.subrange(0, col@.len() as int) =~= col@);
                }
                return (sum, count);
            },
        }
    }
}

/// A run of a prefix column's iterator, wherever it stands: the values
/// the iterator had next are the run's, as many as the run is long and
/// no more, and the running sum grows by what the run's value adds, as
/// many times as the run is long.
#[allow(dead_code)]
fn a_run_with_its_sums<'a, V: VSummed + VValue<C>, C: VCodec>(it: &mut VerifiedPrefixIter<'a, V, C>) -> (r: Option<
    (usize, Get<'a, V>, V::Sum),
>)
    ensures
        match r {
            Some((n, v, through)) => {
                &&& 1 <= n <= old(it).rest().len()
                &&& forall|i: int| 0 <= i < n ==> #[trigger] old(it).rest()[i] == v.sv()
                &&& n < old(it).rest().len() ==> old(it).rest()[n as int] != v.sv()
                &&& through == final(it).total()
                &&& through.as_int() == old(it).total().as_int() + V::amount(v.sv()) * n
                &&& final(it).rest() == old(it).rest().skip(n as int)
            },
            None => old(it).rest().len() == 0 && final(it).total() == old(it).total(),
        },
{
    match it.next_run() {
        Some(run) => Some((run.count, run.value.value, run.value.total())),
        None => None,
    }
}

/// A range of a prefix column copied by its runs: the runs its iterator
/// has left are those of the values it has left, and spliced into a new
/// column they make a column of those values.
#[allow(dead_code)]
fn sums_copied_by_runs<V: VSummed + VValue<C>, C: VCodec>(col: &VerifiedPrefixColumn<V, C>, a: usize, b: usize) -> (r:
    VerifiedColumn<V, C, LenIndex>)
    requires
        a <= b <= col@.len(),
    ensures
        r@ == col@.subrange(a as int, b as int),
{
    let mut r = VerifiedColumn::<V, C, LenIndex>::new();
    let it = col.prefix_iter_range(a, b);
    proof {
        it.plain().lemma_items_len();
        lemma_runs_of_values(it.plain().items());
    }
    r.splice_runs(0, 0, it.runs());
    assert(r@ =~= col@.subrange(a as int, b as int));
    r
}

/// Where the sums of a column are never negative, the first place that
/// the values through it add up to more than `n`, found by the column's
/// index: the values before it add up to `n` at most; and if there is no
/// such place, all the column's values do.
#[allow(dead_code)]
fn found_by_its_sum<'a, V: VSummed + VValue<C>, C: VCodec>(col: &'a VerifiedPrefixColumn<V, C>, n: V::Sum) -> (r: Option<
    (usize, Get<'a, V>),
>)
    where V::Sum: VCount
    ensures
        match r {
            Some((i, v)) => {
                &&& i < col@.len()
                &&& v.sv() == col@[i as int]
                &&& summed::<V>(col@.take(i as int)) <= n.as_int() < summed::<V>(col@.take(i + 1))
            },
            None => summed::<V>(col@) <= n.as_int(),
        },
{
    let mut it = col.prefix_iter();
    match it.advance_prefix(n) {
        Some(seek) => Some((seek.pos, seek.pv.value)),
        None => None,
    }
}

/// In a column of `u64`s, the search by the index finds the place that
/// reading on finds ([`first_over`]), or none where that finds none.
#[allow(dead_code)]
fn first_over_two_ways(col: &VerifiedPrefixColumn<u64>, limit: u128) -> (r: (Option<usize>, Option<usize>))
    ensures
        r.0 == r.1,
{
    let read = first_over(col, limit);
    let found = match found_by_its_sum(col, limit) {
        Some((i, _)) => Some(i),
        None => None,
    };
    proof {
        <u128 as VCount>::lemma_count();
        match (read, found) {
            (Some(i), Some(k)) => {
                if i < k {
                    // the sums of a column of `u64`s do not go down
                    lemma_sum_mono::<u64>(col@.take(k as int), i + 1);
                    assert(col@.take(k as int).take(i + 1) =~= col@.take(i + 1));
                } else if k < i {
                    assert(summed::<u64>(col@.take(k + 1)) <= limit);
                }
            },
            (Some(i), None) => {
                lemma_sum_mono::<u64>(col@, i + 1);
            },
            (None, Some(k)) => {
                assert(summed::<u64>(col@.take(k + 1)) <= limit);
            },
            (None, None) => {},
        }
    }
    (read, found)
}

/// What the values of a range add up to, said two ways: by `delta`,
/// asked for the value after the range, and by `sum_range`.
#[allow(dead_code)]
fn a_range_two_ways<V: VSummed + VValue<C>, C: VCodec>(col: &VerifiedPrefixColumn<V, C>, a: usize, b: usize) -> (r: (
    V::Sum,
    V::Sum,
))
    requires
        a <= b < col@.len(),
    ensures
        r.0.as_int() == summed::<V>(col@.subrange(a as int, b as int)),
        r.0 == r.1,
{
    let seek = col.delta(a, b).unwrap();
    let asked = col.sum_range(a, b);
    proof {
        V::Sum::lemma_as_int(seek.delta);
        V::Sum::lemma_as_int(asked);
    }
    (seek.delta, asked)
}

/// A prefix column's iterator handed over, of which no more is known
/// than the column it reads. Suspended and resumed on that column, it
/// is sure to be given back: where it stood, with the same values left
/// and the same running sum, counted from the same place.
#[allow(dead_code)]
fn handed_over_with_its_sum<'a, V: VSummed + VValue<C>, C: VCodec>(
    it: VerifiedPrefixIter<'_, V, C>,
    col: &'a VerifiedPrefixColumn<V, C>,
) -> (on: VerifiedPrefixIter<'a, V, C>)
    requires
        it.source() == Some(*col),
    ensures
        on.pos() == it.pos(),
        on.rest() == it.rest(),
        on.base_pos() == it.base_pos(),
        on.total() == it.total(),
{
    let s = it.suspend();
    let was = it.total();
    proof {
        it.lemma_source();
    }
    let got = s.try_resume(col);
    let on = got.unwrap();
    let now = on.total();
    proof {
        // sums that are the same number are the same sum
        V::Sum::lemma_as_int(was);
        V::Sum::lemma_as_int(now);
    }
    on
}

/// A reader of sums that stops and goes on. It sums the values of
/// `a..m`, and is suspended: the iterator is given up, and with it the
/// borrow of the column. Resumed on the column, which nothing has
/// edited, it is sure to be given, and its sum goes on to count from
/// `a`: read to `b`, it is what the values of `a..b` add up to.
#[allow(dead_code)]
fn summed_in_two_goes<V: VSummed + VValue<C>, C: VCodec>(col: &VerifiedPrefixColumn<V, C>, a: usize, m: usize, b: usize) -> (r:
    V::Sum)
    requires
        a <= m <= b <= col@.len(),
    ensures
        r.as_int() == summed::<V>(col@.subrange(a as int, b as int)),
{
    let mut it = col.prefix_iter();
    it.advance_to(a);
    it.reset_prefix();
    it.advance_to(m);
    let s = it.suspend();
    let got = s.try_resume(col);
    assert(got is Ok);
    let mut on = got.unwrap();
    on.advance_to(b);
    on.total()
}

/// An edit ends what was suspended of a prefix column's iterator too.
/// The state fits the column the iterator reads, and so counts the
/// edits that one does; after a push the column counts one more, and
/// refuses the state.
#[allow(dead_code)]
fn sums_ended_by_a_push<V: VSummed + VValue<C>, C: VCodec>(col: &mut VerifiedPrefixColumn<V, C>, v: Get<'_, V>) -> (r: bool)
    ensures
        !r,
        final(col)@ == old(col)@.push(v.sv()),
{
    let s = col.prefix_iter().suspend();
    proof {
        s.lemma_fits(&*col);
    }
    col.push(v);
    s.try_resume(col).is_ok()
}

/// Values found with their sums, an iterator and its copy, and an
/// iterator of no column: what each call leaves, in a column of ten
/// `u64`s.
#[allow(dead_code)]
fn found_with_sums(col: &VerifiedPrefixColumn<u64>)
    requires
        col@.len() == 10,
{
    let ghost upto = |k: int| summed::<u64>(col@.take(k));
    // a value further on, with where it is and what is passed over
    let mut it = col.prefix_iter_range(2, 9);
    let a = it.delta_nth(3).unwrap();
    let (p, e, t) = (it.pos(), it.end_pos(), it.total());
    assert(a.pos == 5 && a.pv.value == col@[5] && p == 6 && e == 9 && t == upto(6));
    assert(a.pv.prefix() == upto(5) && a.pv.total() == upto(6));
    assert(a.delta == summed::<u64>(col@.subrange(2, 5))) by {
        assert(col@.subrange(2, 9).take(3) =~= col@.subrange(2, 5));
    }
    // a copy is an iterator of its own: it moves, and this one stays
    let mut twin = it.clone();
    let b = twin.next().unwrap();
    let (p, q, t, u) = (it.pos(), twin.pos(), it.total(), twin.total());
    assert(b.value == col@[6] && p == 6 && q == 7 && t == upto(6) && u == upto(7));
    // more than it has left: none is found, and all is passed
    let c = it.delta_nth(3);
    let (p, e, t) = (it.pos(), it.end_pos(), it.total());
    assert(c is None && p == 9 && e == 9 && t == upto(9));
    // the column asked for the same value
    let d = col.delta(2, 5).unwrap();
    assert(d.pos == 5 && d.pv.value == col@[5] && d.delta == a.delta && d.pv.total() == upto(6));
    let f = col.delta(2, 10);
    assert(f is None);
    // an iterator of no column: nothing to give, to find or to pass
    let mut no = VerifiedPrefixIter::<u64>::default();
    let (p, e, n, t) = (no.pos(), no.end_pos(), no.len(), no.total());
    assert(p == 0 && e == 0 && n == 0 && t == 0);
    let x = no.next_run();
    let y = no.advance_prefix(5);
    let z = no.delta_nth(0);
    let (p, t) = (no.pos(), no.total());
    assert(x is None && y is None && z is None && p == 0 && t == 0);
}

/// What `next_run` says by itself of a prefix column's iterator.
#[allow(dead_code)]
fn told_by_a_prefix_next_run<'a, V: VSummed + VValue<C>, C: VCodec>(it: &mut VerifiedPrefixIter<'a, V, C>) -> (r: Option<
    Run<VerifiedPrefixedValue<'a, V>>,
>)
    ensures
        final(it).column() == old(it).column(),
        final(it).source() == old(it).source(),
        final(it).base_pos() == old(it).base_pos(),
        final(it).total().as_int() == old(it).sum_to(final(it).pos() as int),
        match r {
            Some(run) => {
                &&& head_run(old(it).rest(), run.value.value.sv(), run.count as int)
                &&& run.value == old(it).items()[run.count - 1]
                &&& run.value.total() == final(it).total()
                &&& run.value.total().as_int() == old(it).sum_to(old(it).pos() + run.count)
                &&& run.value.prefix().as_int() == old(it).sum_to(old(it).pos() + run.count - 1)
                &&& run.value.total().as_int() == old(it).total().as_int() + V::amount(run.value.value.sv()) * run.count
                &&& final(it).items() == old(it).items().skip(run.count as int)
                &&& final(it).rest() == old(it).rest().skip(run.count as int)
                &&& final(it).pos() == old(it).pos() + run.count
                &&& final(it).end_pos() == old(it).end_pos()
            },
            None => {
                &&& old(it).rest().len() == 0
                &&& final(it).rest().len() == 0
                &&& final(it).pos() == old(it).pos()
                &&& final(it).end_pos() == old(it).pos()
                &&& final(it).total() == old(it).total()
            },
        },
{
    it.next_run()
}

/// What `runs` says by itself: the iterator of runs reads on from the
/// iterator it is made of, which is the prefix column's without its
/// sums, and so stands where that stood with the same values left.
#[allow(dead_code)]
fn told_by_a_prefix_runs<'a, V: VSummed + VValue<C>, C: VCodec>(it: VerifiedPrefixIter<'a, V, C>) -> (r: VerifiedRuns<
    'a,
    V,
    C,
    SumIndex,
>)
    ensures
        r.iter() == it.plain(),
        r.iter().rest() == it.rest(),
        r.iter().column() == it.column(),
        r.iter().source() == it.source(),
        r.iter().pos() == it.pos(),
        r.iter().end_pos() == it.end_pos(),
{
    it.runs()
}

/// What `delta_nth` says by itself of a prefix column's iterator.
#[allow(dead_code)]
fn told_by_a_delta_nth<'a, V: VSummed + VValue<C>, C: VCodec>(it: &mut VerifiedPrefixIter<'a, V, C>, n: usize) -> (r: Option<
    VerifiedPrefixSeek<'a, V>,
>)
    ensures
        n < old(it).rest().len() ==> {
            &&& r matches Some(seek)
            &&& seek.pos == old(it).pos() + n
            &&& seek.pv == old(it).items()[n as int]
            &&& seek.pv.value.sv() == old(it).rest()[n as int]
            &&& seek.delta.as_int() == summed::<V>(old(it).rest().take(n as int))
            &&& seek.pv.total() == final(it).total()
            &&& seek.pv.total().as_int() == old(it).sum_to(old(it).pos() + n + 1)
            &&& seek.pv.prefix().as_int() == old(it).sum_to(old(it).pos() + n)
            &&& seek.pv.prefix().as_int() == old(it).total().as_int() + seek.delta.as_int()
            &&& seek.pv.total().as_int() == seek.pv.prefix().as_int() + V::amount(seek.pv.value.sv())
            &&& final(it).items() == old(it).items().skip(n + 1)
            &&& final(it).rest() == old(it).rest().skip(n + 1)
            &&& final(it).pos() == old(it).pos() + n + 1
            &&& final(it).end_pos() == old(it).end_pos()
        },
        n >= old(it).rest().len() ==> {
            &&& r is None
            &&& final(it).rest().len() == 0
            &&& final(it).pos() == old(it).pos() + old(it).rest().len()
            &&& final(it).end_pos() == final(it).pos()
        },
        final(it).column() == old(it).column(),
        final(it).source() == old(it).source(),
        final(it).base_pos() == old(it).base_pos(),
        final(it).total().as_int() == old(it).sum_to(final(it).pos() as int),
{
    it.delta_nth(n)
}

/// What `advance_prefix` says by itself of a prefix column's iterator,
/// where sums are never negative.
#[allow(dead_code)]
fn told_by_an_advance_prefix<'a, V: VSummed + VValue<C>, C: VCodec>(it: &mut VerifiedPrefixIter<'a, V, C>, n: V::Sum) -> (r:
    Option<VerifiedPrefixSeek<'a, V>>)
    where V::Sum: VCount
    ensures
        final(it).column() == old(it).column(),
        final(it).source() == old(it).source(),
        final(it).base_pos() == old(it).base_pos(),
        final(it).total().as_int() == old(it).sum_to(final(it).pos() as int),
        match r {
            Some(seek) => {
                let k = seek.pos - old(it).pos();
                &&& 0 <= k < old(it).rest().len()
                &&& seek.pv == old(it).items()[k]
                &&& seek.pv.value.sv() == old(it).rest()[k]
                &&& n.as_int() < summed::<V>(old(it).rest().take(k + 1))
                &&& forall|j: int| 0 <= j <= k ==> summed::<V>(#[trigger] old(it).rest().take(j)) <= n.as_int()
                &&& seek.delta.as_int() == summed::<V>(old(it).rest().take(k))
                &&& seek.pv.total() == final(it).total()
                &&& seek.pv.total().as_int() == old(it).sum_to(seek.pos + 1)
                &&& seek.pv.prefix().as_int() == old(it).sum_to(seek.pos as int)
                &&& seek.pv.prefix().as_int() == old(it).total().as_int() + seek.delta.as_int()
                &&& seek.pv.total().as_int() == seek.pv.prefix().as_int() + V::amount(seek.pv.value.sv())
                &&& final(it).items() == old(it).items().skip(k + 1)
                &&& final(it).rest() == old(it).rest().skip(k + 1)
                &&& final(it).pos() == seek.pos + 1
                &&& final(it).end_pos() == old(it).end_pos()
            },
            None => {
                &&& summed::<V>(old(it).rest()) <= n.as_int()
                &&& final(it).rest().len() == 0
                &&& final(it).pos() == old(it).pos() + old(it).rest().len()
                &&& final(it).end_pos() == final(it).pos()
            },
        },
{
    it.advance_prefix(n)
}

/// What `suspend` says by itself of a prefix column's iterator, and
/// what its state says of where its sum counts from.
#[allow(dead_code)]
fn told_by_a_prefix_suspend<'a, V: VSummed + VValue<C>, C: VCodec>(
    it: &VerifiedPrefixIter<'a, V, C>,
    col: &VerifiedPrefixColumn<V, C>,
) -> (s: VerifiedPrefixIterState<V>)
    ensures
        s.pos() == it.pos(),
        s.end_pos() == it.end_pos(),
        s.base_pos() == it.base_pos(),
        it.rest() == window(it.column(), s.pos() as int, s.end_pos() as int),
        it.source() == Some(*col) ==> s.fits(col),
        s.base_pos() <= s.pos(),
{
    let s = it.suspend();
    proof {
        s.lemma_base();
    }
    s
}

/// What `try_resume` says by itself, of any state of a prefix column's
/// iterator and any prefix column, each thing asked for on its own.
#[allow(dead_code)]
fn told_by_a_prefix_resume<'a, V: VSummed + VValue<C>, C: VCodec>(
    s: &VerifiedPrefixIterState<V>,
    col: &'a VerifiedPrefixColumn<V, C>,
) -> (r: Result<VerifiedPrefixIter<'a, V, C>, PackError>)
    ensures
        r is Ok <==> s.fits(col),
        r is Ok ==> s.edits() == col.edits(),
        r matches Ok(it) ==> it.source() == Some(*col),
        r matches Ok(it) ==> it.column() == col@,
        r matches Ok(it) ==> it.pos() == s.pos(),
        r matches Ok(it) ==> it.rest() == window(col@, s.pos() as int, s.end_pos() as int),
        r matches Ok(it) ==> it.end_pos() == s.end_pos() || (s.pos() == col@.len() && it.end_pos() == s.pos()),
        r matches Ok(it) ==> it.base_pos() == s.base_pos(),
        r matches Ok(it) ==> it.total().as_int() == summed::<V>(col@.subrange(s.base_pos() as int, s.pos() as int)),
{
    s.try_resume(col)
}

/// What a state that fits a column says of it: the two count the same
/// edits, and the state is a place in the column as a plain iterator's
/// state is.
#[allow(dead_code)]
fn told_by_a_prefix_states_fit<V: VSummed + VValue<C>, C: VCodec>(s: &VerifiedPrefixIterState<V>, col: &VerifiedPrefixColumn<V, C>)
    requires
        s.fits(col),
    ensures
        s.edits() == col.edits(),
        s.plain().fits(col),
        s.pos() == s.plain().pos(),
        s.end_pos() == s.plain().end_pos(),
{
    proof {
        s.lemma_fits(col);
    }
}

/// What `clone` says by itself of a prefix column's iterator.
#[allow(dead_code)]
fn told_by_a_prefix_clone<'a, V: VSummed + VValue<C>, C: VCodec>(it: &VerifiedPrefixIter<'a, V, C>) -> (r: VerifiedPrefixIter<
    'a,
    V,
    C,
>)
    ensures
        r.items() == it.items(),
        r.rest() == it.rest(),
        r.column() == it.column(),
        r.source() == it.source(),
        r.pos() == it.pos(),
        r.end_pos() == it.end_pos(),
        r.base_pos() == it.base_pos(),
        r.total() == it.total(),
{
    it.clone()
}

/// What `default` says by itself of a prefix column's iterator.
#[allow(dead_code)]
fn told_by_a_prefix_default<'a, V: VSummed + VValue<C>, C: VCodec>() -> (r: VerifiedPrefixIter<'a, V, C>)
    ensures
        r.items().len() == 0,
        r.rest().len() == 0,
        r.column().len() == 0,
        r.source() is None,
        r.pos() == 0,
        r.end_pos() == 0,
        r.base_pos() == 0,
        r.total().as_int() == 0,
{
    VerifiedPrefixIter::<V, C>::default()
}

/// What `delta` says by itself, of any two positions in order.
#[allow(dead_code)]
fn told_by_a_column_delta<'a, V: VSummed + VValue<C>, C: VCodec>(col: &'a VerifiedPrefixColumn<V, C>, from: usize, to: usize) -> (r:
    Option<VerifiedPrefixSeek<'a, V>>)
    requires
        from <= to,
    ensures
        to < col@.len() ==> {
            &&& r matches Some(seek)
            &&& seek.pos == to
            &&& seek.pv.value.sv() == col@[to as int]
            &&& seek.delta.as_int() == summed::<V>(col@.subrange(from as int, to as int))
            &&& seek.pv.total().as_int() == summed::<V>(col@.take(to + 1))
            &&& seek.pv.prefix().as_int() == summed::<V>(col@.take(to as int))
            &&& seek.pv.total().as_int() == seek.pv.prefix().as_int() + V::amount(seek.pv.value.sv())
        },
        to >= col@.len() ==> r is None,
{
    col.delta(from, to)
}

/// What a value found says of itself: its parts are there to read, and
/// a copy of it is the same.
#[allow(dead_code)]
fn told_by_a_prefix_seek<'a, V: VSummed>(seek: &VerifiedPrefixSeek<'a, V>) -> (r: (usize, V::Sum, VerifiedPrefixedValue<'a, V>))
    ensures
        r.0 == seek.pos,
        r.1 == seek.delta,
        r.2 == seek.pv,
{
    let copy = seek.clone();
    assert(copy == *seek);
    (copy.pos, seek.delta, seek.pv)
}

// ── A client's own type in a prefix column ─────────────────────────────────

/// A value type of the client's own for a prefix column to add up: one
/// number that says what a value is, in its low four bits, and how long
/// it is, in the bits above them. A prefix column of them adds up the
/// lengths, in a `u64`. Sixteen lengths cannot overflow one, so a
/// column of no more values is not checked, and a longer one is.
#[derive(Clone, Copy)]
#[allow(dead_code)]
pub struct Meta(pub u64);

impl VNewtype for Meta {
    type Base = u64;

    open spec fn base(v: Meta) -> u64 {
        v.0
    }

    proof fn lemma_base(v: Meta, w: Meta) {
    }

    fn to_base(self) -> u64 {
        self.0
    }

    fn from_base(b: u64) -> Result<Meta, PackError> {
        Ok(Meta(b))
    }
}

impl VAmount for Meta {
    type Sum = u64;

    open spec fn amount_of(v: Meta) -> int {
        (v.0 >> 4) as int
    }

    open spec fn amount_max() -> nat {
        0x0fff_ffff_ffff_ffff
    }

    open spec fn room() -> nat {
        16
    }

    proof fn lemma_amount_of(v: Meta) {
        let x = v.0;
        assert(x >> 4 <= 0x0fff_ffff_ffff_ffffu64) by (bit_vector);
    }

    proof fn lemma_room() {
    }

    fn room_len() -> usize {
        16
    }

    fn part_of(v: Meta) -> u64 {
        v.0 >> 4
    }
}

/// How long a value is: what it adds to the sums of a prefix column.
#[allow(dead_code)]
spec fn length_of(v: Meta) -> int {
    (v.0 >> 4) as int
}

/// A value of the kind `kind` that is `len` long.
#[allow(dead_code)]
fn a_meta(kind: u8, len: u64) -> (r: Meta)
    requires
        kind < 16,
        len <= 0x0fff_ffff_ffff_ffff,
    ensures
        length_of(r) == len,
        r.0 & 15 == kind,
{
    let k = kind as u64;
    assert(((len << 4) | k) >> 4 == len && ((len << 4) | k) & 15 == k) by (bit_vector)
        requires
            k < 16,
            len <= 0x0fff_ffff_ffff_ffffu64,
    ;
    Meta((len << 4) | k)
}

/// What one, two and three values add up to: a client's own unfolding
/// of [`summed`], which it can read.
#[allow(dead_code)]
proof fn the_sum_of_three<V: VSummed>(a: <V as VSpec>::S, b: <V as VSpec>::S, c: <V as VSpec>::S)
    ensures
        summed::<V>(seq![a]) == V::amount(a),
        summed::<V>(seq![a, b]) == V::amount(a) + V::amount(b),
        summed::<V>(seq![a, b, c]) == V::amount(a) + V::amount(b) + V::amount(c),
{
    assert(summed::<V>(Seq::<<V as VSpec>::S>::empty()) == 0);
    assert(seq![a].drop_last() =~= Seq::<<V as VSpec>::S>::empty());
    assert(summed::<V>(seq![a]) == V::amount(a));
    assert(seq![a, b].drop_last() =~= seq![a]);
    assert(summed::<V>(seq![a, b]) == V::amount(a) + V::amount(b));
    assert(seq![a, b, c].drop_last() =~= seq![a, b]);
}

/// Three of the client's values in a prefix column: the sum before the
/// third is the lengths of the first two, and the sum through it is
/// the lengths of all three.
#[allow(dead_code)]
fn lengths_added_up(a: Meta, b: Meta, c: Meta) -> (r: (u64, u64))
    ensures
        r.0 == length_of(a) + length_of(b),
        r.1 == length_of(a) + length_of(b) + length_of(c),
{
    let mut col = VerifiedPrefixColumn::<Meta>::new();
    col.push(a);
    col.push(b);
    col.push(c);
    proof {
        the_sum_of_three::<Meta>(a, b, c);
        assert(col@ =~= seq![a, b, c]);
        assert(col@.take(2) =~= seq![a, b]);
        assert(col@.take(3) =~= seq![a, b, c]);
    }
    (col.get_prefix(2), col.get_total(2))
}

/// A null of the client's type adds nothing.
#[allow(dead_code)]
fn a_null_has_no_length(a: Meta, b: Meta) -> (r: u64)
    ensures
        r == length_of(a) + length_of(b),
{
    let mut col = VerifiedPrefixColumn::<Option<Meta>>::new();
    col.push(Some(a));
    col.push(None);
    col.push(Some(b));
    proof {
        the_sum_of_three::<Option<Meta>>(Some(a), None, Some(b));
        assert(col@ =~= seq![Some(a), None, Some(b)]);
        assert(col@.take(3) =~= col@);
    }
    col.get_prefix(3)
}

/// Sixteen lengths cannot overflow a `u64`: bytes of no more of the
/// client's values load as a prefix column wherever they load as a
/// plain one. And whatever loads as one has lengths that add up to a
/// `u64`.
#[allow(dead_code)]
fn load_lengths(bytes: &[u8]) -> (r: Option<VerifiedPrefixColumn<Meta>>)
    ensures
        VerifiedColumn::<Meta>::loadable(bytes@) && VerifiedColumn::<Meta>::values(bytes@).len() <= 16 ==> r is Some,
        r matches Some(c) ==> summed::<Meta>(c@) <= u64::MAX,
{
    let r = load_short_sums::<Meta, Leb128>(bytes);
    match &r {
        Some(c) => {
            proof {
                the_sums_of_a_column(c, c@.len() as int);
                assert(c@.take(c@.len() as int) =~= c@);
            }
        },
        None => {},
    }
    r
}

/// A push that would take the lengths of a column past a `u64` does
/// not return.
#[allow(dead_code)]
fn a_length_past_the_sums(col: &mut VerifiedPrefixColumn<Meta>, v: Meta)
    requires
        summed::<Meta>(old(col)@) + length_of(v) > u64::MAX,
    ensures
        false,
{
    push_within_the_sums(col, v);
}

/// The value in which the place `n` of the whole lies, the lengths
/// counted from zero: the values before it are together `n` long at
/// most, and with it they are longer. Where there is none, all the
/// values are together `n` long at most.
#[allow(dead_code)]
fn the_value_at(col: &VerifiedPrefixColumn<Meta>, n: u64) -> (r: Option<usize>)
    requires
        n < u64::MAX,
    ensures
        match r {
            Some(i) => i < col@.len() && summed::<Meta>(col@.take(i as int)) <= n < summed::<Meta>(col@.take(i + 1)),
            None => summed::<Meta>(col@) <= n,
        },
{
    let i = col.get_index_for_total(n + 1);
    proof {
        assert(col@.take(0).len() == 0);
        assert(col@.take(col@.len() as int) =~= col@);
    }
    if i < col.len() {
        Some(i)
    } else {
        None
    }
}

/// What is proved of every summed type is proved of the client's own
/// and of its nulls: the running sums of a column of them, the place
/// of a sum in it, and a column of nulls and values summed run by run.
#[allow(dead_code)]
fn sums_of_the_clients_own(
    col: &VerifiedPrefixColumn<Meta>,
    nullable: &VerifiedPrefixColumn<Option<Meta>>,
    target: u64,
) -> (r: (Vec<u64>, Option<usize>, (u64, usize)))
    ensures
        r.0@.len() == col@.len(),
        forall|i: int| 0 <= i < r.0@.len() ==> #[trigger] r.0@[i] == summed::<Meta>(col@.take(i + 1)),
        r.1 matches Some(i) ==> i <= col@.len() && target <= summed::<Meta>(col@.take(i as int)),
        r.1 is None ==> summed::<Meta>(col@) < target,
        r.2.0 == summed::<Option<Meta>>(nullable@),
        r.2.1 == nullable@.len(),
{
    (
        running_sums::<Meta, Leb128>(col),
        the_place_of_a_sum::<Meta, Leb128>(col, target),
        summed_by_runs::<Option<Meta>, Leb128>(nullable),
    )
}

// ── Accumulator columns ─────────────────────────────────────────────────────

/// An accumulator of the client's own: which of 64 kinds of mark are
/// open, a bit for each. A mark of a kind opens it where it is closed
/// and closes it where it is open, so two of them add up to the
/// exclusive or of their bits, and one is taken away from another as it
/// is added.
#[derive(Debug, Clone, Copy)]
#[allow(dead_code)]
pub struct Flags(pub u64);

impl VGroup for Flags {
    open spec fn zero() -> Flags {
        Flags(0)
    }

    open spec fn plus(a: Flags, b: Flags) -> Flags {
        Flags(a.0 ^ b.0)
    }

    open spec fn minus(a: Flags, b: Flags) -> Flags {
        Flags(a.0 ^ b.0)
    }

    proof fn lemma_plus(a: Flags, b: Flags, c: Flags) {
        let (x, y, z) = (a.0, b.0, c.0);
        assert((x ^ y) ^ z == x ^ (y ^ z) && 0u64 ^ x == x && x ^ 0u64 == x) by (bit_vector);
    }

    proof fn lemma_minus(a: Flags, b: Flags) {
        let (x, y) = (a.0, b.0);
        assert(y ^ (x ^ y) == x) by (bit_vector);
    }
}

/// An odd number of copies of some flags add up to the flags, and an
/// even number of them to none.
proof fn lemma_flags_times(n: nat, b: Flags)
    ensures
        times::<Flags>(n, b) == if n % 2 == 1 {
            b
        } else {
            Flags(0)
        },
    decreases n,
{
    if n > 0 {
        lemma_flags_times((n - 1) as nat, b);
        let x = b.0;
        assert(0u64 ^ x == x && x ^ x == 0u64) by (bit_vector);
    }
}

impl VAccum for Flags {
    fn new_zero() -> Flags {
        Flags(0)
    }

    fn add_acc(&mut self, b: &Flags) {
        self.0 = self.0 ^ b.0;
    }

    fn sub_acc(&mut self, b: &Flags) {
        self.0 = self.0 ^ b.0;
    }

    fn dup(&self) -> Flags {
        *self
    }

    /// A run of a mark adds its flags where the run is of an odd
    /// number, and nothing where it is of an even one: no loop.
    fn add_times(&mut self, b: &Flags, n: usize) {
        proof {
            lemma_flags_times(n as nat, *b);
            let x = self.0;
            assert(x ^ 0u64 == x) by (bit_vector);
        }
        if n % 2 == 1 {
            self.0 = self.0 ^ b.0;
        }
    }
}

/// A value type of the client's own for an accumulator column to add
/// up: a mark, of the kind that its number says, one of 64. What it
/// adds is the flag of its kind.
#[derive(Clone, Copy)]
#[allow(dead_code)]
pub struct Mark(pub u64);

impl VNewtype for Mark {
    type Base = u64;

    open spec fn base(v: Mark) -> u64 {
        v.0
    }

    proof fn lemma_base(v: Mark, w: Mark) {
    }

    fn to_base(self) -> u64 {
        self.0
    }

    fn from_base(b: u64) -> Result<Mark, PackError> {
        Ok(Mark(b))
    }
}

impl VAdds for Mark {
    type Acc = Flags;

    open spec fn adds_of(v: Mark) -> Flags {
        Flags(1u64 << (v.0 % 64))
    }

    fn acc_of(v: Mark) -> Flags {
        Flags(1u64 << (v.0 % 64))
    }
}

/// The flag of a mark's kind: what the mark adds.
#[allow(dead_code)]
spec fn flag_of(v: Mark) -> u64 {
    1u64 << (v.0 % 64)
}

/// A row of 64 lamps as it lies across a board that is turned round, or
/// one that is not: turned round, its two halves change places.
pub open spec fn across(turned: bool, x: u64) -> u64 {
    if turned {
        (x >> 32) | (x << 32)
    } else {
        x
    }
}

/// [`across`], computed.
#[allow(dead_code)]
fn put_across(turned: bool, x: u64) -> (r: u64)
    ensures
        r == across(turned, x),
{
    if turned {
        (x >> 32) | (x << 32)
    } else {
        x
    }
}

/// An accumulator whose sums depend on the order: a board of 64 lamps,
/// which of them are lit and whether the board is turned round. A step
/// changes some lamps and may turn the board round; a step taken after
/// the board is turned round changes the lamps that lie across from
/// its own. So a step that turns the board and one that changes a lamp
/// leave another lamp lit in one order than in the other.
#[derive(Debug, Clone, Copy)]
#[allow(dead_code)]
pub struct Board {
    pub lit: u64,
    pub turned: bool,
}

impl VGroup for Board {
    open spec fn zero() -> Board {
        Board { lit: 0, turned: false }
    }

    open spec fn plus(a: Board, b: Board) -> Board {
        Board { lit: a.lit ^ across(a.turned, b.lit), turned: a.turned != b.turned }
    }

    open spec fn minus(a: Board, b: Board) -> Board {
        Board { lit: across(b.turned, a.lit ^ b.lit), turned: a.turned != b.turned }
    }

    proof fn lemma_plus(a: Board, b: Board, c: Board) {
        let (x, y, z) = (a.lit, b.lit, c.lit);
        let sy = (y >> 32) | (y << 32);
        let sz = (z >> 32) | (z << 32);
        let w = y ^ sz;
        assert(0u64 ^ x == x && x ^ 0u64 == x && (0u64 >> 32) | (0u64 << 32) == 0u64) by (bit_vector);
        // neither of the first two turns the board, or the second does
        assert((x ^ y) ^ z == x ^ (y ^ z) && (x ^ y) ^ sz == x ^ (y ^ sz)) by (bit_vector);
        // the first does
        assert((x ^ sy) ^ sz == x ^ (((y ^ z) >> 32) | ((y ^ z) << 32))) by (bit_vector)
            requires
                sy == (y >> 32) | (y << 32),
                sz == (z >> 32) | (z << 32),
        ;
        // both do
        assert((x ^ sy) ^ z == x ^ ((w >> 32) | (w << 32))) by (bit_vector)
            requires
                sy == (y >> 32) | (y << 32),
                sz == (z >> 32) | (z << 32),
                w == y ^ sz,
        ;
    }

    proof fn lemma_minus(a: Board, b: Board) {
        let (x, y) = (a.lit, b.lit);
        let w = x ^ y;
        let s = (w >> 32) | (w << 32);
        assert(y ^ w == x && y ^ ((s >> 32) | (s << 32)) == x) by (bit_vector)
            requires
                w == x ^ y,
                s == (w >> 32) | (w << 32),
        ;
    }
}

impl VAccum for Board {
    fn new_zero() -> Board {
        Board { lit: 0, turned: false }
    }

    fn add_acc(&mut self, b: &Board) {
        *self = Board { lit: self.lit ^ put_across(self.turned, b.lit), turned: self.turned != b.turned };
    }

    fn sub_acc(&mut self, b: &Board) {
        *self = Board { lit: put_across(b.turned, self.lit ^ b.lit), turned: self.turned != b.turned };
    }

    fn dup(&self) -> Board {
        *self
    }

    // `add_times` is left as the trait gives it: a run adds its step
    // one time after another
}

/// A value type of the client's own that a [`Board`] adds up: a step,
/// which turns the board round where its number is odd, and changes the
/// lamps that the rest of its number says.
#[derive(Clone, Copy)]
#[allow(dead_code)]
pub struct Step(pub u64);

impl VNewtype for Step {
    type Base = u64;

    open spec fn base(v: Step) -> u64 {
        v.0
    }

    proof fn lemma_base(v: Step, w: Step) {
    }

    fn to_base(self) -> u64 {
        self.0
    }

    fn from_base(b: u64) -> Result<Step, PackError> {
        Ok(Step(b))
    }
}

impl VAdds for Step {
    type Acc = Board;

    open spec fn adds_of(v: Step) -> Board {
        Board { lit: v.0 >> 1, turned: v.0 & 1 == 1 }
    }

    fn acc_of(v: Step) -> Board {
        Board { lit: v.0 >> 1, turned: v.0 & 1 == 1 }
    }
}

/// What one, two and three values add up to, in the order they are in:
/// a client's own unfolding of [`accumulated`], which it can read.
#[allow(dead_code)]
proof fn the_acc_of_three<V: VAccumulated>(a: <V as VSpec>::S, b: <V as VSpec>::S, c: <V as VSpec>::S)
    ensures
        accumulated::<V>(seq![a]) == V::adds(a),
        accumulated::<V>(seq![a, b]) == <V::Acc as VGroup>::plus(V::adds(a), V::adds(b)),
        accumulated::<V>(seq![a, b, c]) == <V::Acc as VGroup>::plus(
            <V::Acc as VGroup>::plus(V::adds(a), V::adds(b)),
            V::adds(c),
        ),
{
    <V::Acc as VGroup>::lemma_plus(V::adds(a), V::adds(a), V::adds(a));
    assert(accumulated::<V>(Seq::<<V as VSpec>::S>::empty()) == <V::Acc as VGroup>::zero());
    assert(seq![a].drop_last() =~= Seq::<<V as VSpec>::S>::empty());
    assert(accumulated::<V>(seq![a]) == V::adds(a));
    assert(seq![a, b].drop_last() =~= seq![a]);
    assert(accumulated::<V>(seq![a, b]) == <V::Acc as VGroup>::plus(V::adds(a), V::adds(b)));
    assert(seq![a, b, c].drop_last() =~= seq![a, b]);
}

/// Three marks in an accumulator column: before the third, the kinds of
/// the first two are open, or closed again where they are one kind; and
/// through it, those of all three.
#[allow(dead_code)]
fn marks_added_up(a: Mark, b: Mark, c: Mark) -> (r: (Flags, Flags))
    ensures
        r.0 == Flags(flag_of(a) ^ flag_of(b)),
        r.1 == Flags(flag_of(a) ^ flag_of(b) ^ flag_of(c)),
{
    let mut col = VerifiedAccColumn::<Mark>::new();
    col.push(a);
    col.push(b);
    col.push(c);
    proof {
        the_acc_of_three::<Mark>(a, b, c);
        assert(col@ =~= seq![a, b, c]);
        assert(col@.take(2) =~= seq![a, b]);
        assert(col@.take(3) =~= seq![a, b, c]);
    }
    (col.get_prefix(2), col.get_total(2))
}

/// A null among the marks adds nothing.
#[allow(dead_code)]
fn a_null_adds_no_mark(a: Mark, b: Mark) -> (r: Flags)
    ensures
        r == Flags(flag_of(a) ^ flag_of(b)),
{
    let mut col = VerifiedAccColumn::<Option<Mark>>::new();
    col.push(Some(a));
    col.push(None);
    col.push(Some(b));
    proof {
        the_acc_of_three::<Option<Mark>>(Some(a), None, Some(b));
        assert(col@ =~= seq![Some(a), None, Some(b)]);
        assert(col@.take(3) =~= col@);
        let (x, y) = (flag_of(a), flag_of(b));
        assert((x ^ 0u64) ^ y == x ^ y) by (bit_vector);
    }
    col.get_prefix(3)
}

/// What `get_prefix` says of an accumulator column, on its own: what
/// the values before the position add up to, and what all of them do
/// where the position is past the end. For every type an accumulator
/// column holds, and every codec.
#[allow(dead_code)]
fn told_by_an_acc_prefix<V: VAccumulated + VValue<C>, C: VCodec>(col: &VerifiedAccColumn<V, C>, index: usize) -> (r:
    V::Acc)
    ensures
        index <= col@.len() ==> r == accumulated::<V>(col@.take(index as int)),
        index >= col@.len() ==> r == accumulated::<V>(col@),
{
    col.get_prefix(index)
}

/// What `get_total` says, on its own: what the values through the
/// position add up to.
#[allow(dead_code)]
fn told_by_an_acc_total<V: VAccumulated + VValue<C>, C: VCodec>(col: &VerifiedAccColumn<V, C>, index: usize) -> (r:
    V::Acc)
    ensures
        index < col@.len() ==> r == accumulated::<V>(col@.take(index + 1)),
        index >= col@.len() ==> r == accumulated::<V>(col@),
{
    col.get_total(index)
}

/// What `sum_range` says, on its own: what the values of the range add
/// up to, of those of them that the column has, and nothing where there
/// are none.
#[allow(dead_code)]
fn told_by_an_acc_range<V: VAccumulated + VValue<C>, C: VCodec>(
    col: &VerifiedAccColumn<V, C>,
    start: usize,
    end: usize,
) -> (r: V::Acc)
    ensures
        start <= end <= col@.len() ==> r == accumulated::<V>(col@.subrange(start as int, end as int)),
        start <= col@.len() <= end ==> r == accumulated::<V>(col@.subrange(start as int, col@.len() as int)),
        start >= end || start >= col@.len() ==> r == <V::Acc as VGroup>::zero(),
{
    col.sum_range(start, end)
}

/// What `add_times` says, on its own: the accumulator with `b` added
/// `n` times over, for every accumulator, whether it adds them as the
/// trait gives it or in a way of its own.
#[allow(dead_code)]
fn told_by_add_times<A: VAccum>(a: &mut A, b: &A, n: usize)
    ensures
        *final(a) == A::plus(*old(a), times::<A>(n as nat, *b)),
{
    a.add_times(b, n);
}

/// What an accumulator's own functions say, each on its own, for every
/// accumulator: what a client that is given one counts on. Here, that
/// a new one is what no values add up to.
#[allow(dead_code)]
fn told_by_new_zero<A: VAccum>() -> (r: A)
    ensures
        r == A::zero(),
{
    A::new_zero()
}

/// What `add_acc` says, on its own: the accumulator, and then `b`.
#[allow(dead_code)]
fn told_by_add_acc<A: VAccum>(a: &mut A, b: &A)
    ensures
        *final(a) == A::plus(*old(a), *b),
{
    a.add_acc(b);
}

/// What `sub_acc` says, on its own: what, added after `b`, gives the
/// accumulator there was.
#[allow(dead_code)]
fn told_by_sub_acc<A: VAccum>(a: &mut A, b: &A)
    ensures
        *final(a) == A::minus(*old(a), *b),
{
    a.sub_acc(b);
}

/// What `dup` says, on its own: a copy.
#[allow(dead_code)]
fn told_by_dup<A: VAccum>(a: &A) -> (r: A)
    ensures
        r == *a,
{
    a.dup()
}

/// What `acc_of` says, on its own: what the value adds, for every type
/// of a client's that says it.
#[allow(dead_code)]
fn told_by_acc_of<T: VAdds>(v: T) -> (r: <T as VAdds>::Acc)
    ensures
        r == T::adds_of(v),
{
    T::acc_of(v)
}

/// Something added to an accumulator, and what there was taken away
/// again: what is left is what was added, whatever the accumulator and
/// whether or not it adds the same in either order. What is taken away
/// is what was there first.
#[allow(dead_code)]
fn added_then_taken_away<A: VAccum>(a: &A, b: &A) -> (r: A)
    ensures
        r == *b,
{
    let mut r = a.dup();
    r.add_acc(b);
    r.sub_acc(a);
    proof {
        A::lemma_minus(A::plus(*a, *b), *a);
        lemma_cancel::<A>(*a, r, *b);
    }
    r
}

/// What the values before a range add up to, and then what those of the
/// range do, is what those before its end do: the three answers of a
/// column agree, whatever the accumulator and in the order it adds.
#[allow(dead_code)]
fn a_range_added_after<V: VAccumulated + VValue<C>, C: VCodec>(
    col: &VerifiedAccColumn<V, C>,
    start: usize,
    end: usize,
) -> (r: (V::Acc, V::Acc, V::Acc))
    requires
        start <= end <= col@.len(),
    ensures
        <V::Acc as VGroup>::plus(r.0, r.1) == r.2,
        r.2 == accumulated::<V>(col@.take(end as int)),
{
    proof {
        lemma_acc_range::<V>(col@, start as int, end as int);
    }
    (col.get_prefix(start), col.sum_range(start, end), col.get_prefix(end))
}

/// What a range adds up to, found by taking away: what the values
/// before its start add up to, taken from what those before its end do.
/// It is what `sum_range` answers, by the laws of the accumulator
/// alone.
#[allow(dead_code)]
fn a_range_by_taking_away<V: VAccumulated + VValue<C>, C: VCodec>(
    col: &VerifiedAccColumn<V, C>,
    start: usize,
    end: usize,
) -> (r: V::Acc)
    requires
        start <= end <= col@.len(),
    ensures
        r == accumulated::<V>(col@.subrange(start as int, end as int)),
{
    let before = col.get_prefix(start);
    let mut r = col.get_prefix(end);
    r.sub_acc(&before);
    proof {
        lemma_acc_range::<V>(col@, start as int, end as int);
        <V::Acc as VGroup>::lemma_minus(accumulated::<V>(col@.take(end as int)), before);
        lemma_cancel::<V::Acc>(before, r, accumulated::<V>(col@.subrange(start as int, end as int)));
    }
    r
}

/// Nothing overflows in an accumulator column: bytes load as one
/// wherever they load as a plain column, with the values a plain column
/// would have of them.
#[allow(dead_code)]
fn load_as_an_acc<V: VAccumulated + VValue<C>, C: VCodec>(bytes: &[u8]) -> (r: Option<VerifiedAccColumn<V, C>>)
    ensures
        VerifiedColumn::<V, C>::loadable(bytes@) ==> r is Some,
        r matches Some(c) ==> c@ == VerifiedColumn::<V, C>::values(bytes@),
{
    load_as_a_kind::<V, C, AccIndex>(bytes)
}

/// A plain column's values as an accumulator column, made of its runs:
/// there is one of any values.
#[allow(dead_code)]
fn added_up<V: VAccumulated + VValue<C>, C: VCodec>(plain: &VerifiedColumn<V, C>) -> (r: VerifiedAccColumn<V, C>)
    ensures
        r@ == plain@,
{
    copied_by_runs::<V, C, LenIndex, AccIndex>(plain)
}

/// A mark put in at a position of a column: the kinds open through it
/// are those that were open before the position, with the mark's kind
/// opened, or closed where it was open.
#[allow(dead_code)]
fn a_mark_put_in(col: &mut VerifiedAccColumn<Mark>, at: usize, v: Mark) -> (r: Flags)
    requires
        at <= old(col)@.len(),
    ensures
        final(col)@ == old(col)@.insert(at as int, v),
        r == Flags(accumulated::<Mark>(old(col)@.take(at as int)).0 ^ flag_of(v)),
{
    col.insert(at, v);
    proof {
        assert(col@.take(at + 1).drop_last() =~= old(col)@.take(at as int));
        assert(col@.take(at + 1).last() == v);
    }
    col.get_total(at)
}

/// What a column's values add up to, added a run at a time: a run adds
/// what its value does, as many times over as the run is long, which is
/// the accumulator's to do in one step where it has a shorter way. For
/// every type of a client's that says what its values add.
#[allow(dead_code)]
fn added_by_runs<T: VAdds, C: VCodec>(col: &VerifiedAccColumn<T, C>) -> (r: <T as VAdds>::Acc)
    ensures
        r == accumulated::<T>(col@),
{
    let it = col.iter();
    let ghost all = it.items();
    let ghost rs = runs_of(all);
    proof {
        it.lemma_items_len();
        lemma_runs_of_values(all);
        assert(run_values(rs.take(0)).len() == 0);
    }
    let mut acc = <<T as VAdds>::Acc as VAccum>::new_zero();
    for run in at: it.runs()
        invariant
            at.seq() == rs,
            acc == accumulated::<T>(run_values(rs.take(at.index()))),
    {
        proof {
            let done = rs.take(at.index());
            let more = Seq::new(run.count as nat, |_i: int| run.value.sv());
            assert(rs.take(at.index() + 1) =~= done.push(run));
            lemma_run_values_push(done, run);
            lemma_acc_cat::<T>(run_values(done), more);
            lemma_acc_run::<T>(run.count as nat, run.value.sv());
        }
        let b = T::acc_of(run.value);
        acc.add_times(&b, run.count);
    }
    proof {
        assert(rs.take(rs.len() as int) =~= rs);
    }
    acc
}

/// Two accumulators need not add up to the same in either order: a
/// board turned round and then a lamp changed is not the board with the
/// lamp changed and then turned round.
#[allow(dead_code)]
proof fn the_order_matters()
    ensures
        exists|a: Board, b: Board| Board::plus(a, b) != Board::plus(b, a),
{
    let a = Board { lit: 1, turned: true };
    let b = Board { lit: 2, turned: false };
    assert(1u64 ^ ((2u64 >> 32) | (2u64 << 32)) == 0x2_0000_0001u64 && 2u64 ^ 1u64 == 3u64) by (bit_vector);
    assert(Board::plus(a, b) != Board::plus(b, a));
}

/// The same two steps in a column, in either order: an accumulator
/// column adds its values in the order it holds them, and here the two
/// orders leave different lamps lit.
#[allow(dead_code)]
fn steps_in_order() -> (r: (Board, Board))
    ensures
        r.0 == (Board { lit: 0x2_0000_0001, turned: true }),
        r.1 == (Board { lit: 3, turned: true }),
{
    let turn = Step(3);
    let light = Step(4);
    let mut one = VerifiedAccColumn::<Step>::new();
    one.push(turn);
    one.push(light);
    let mut other = VerifiedAccColumn::<Step>::new();
    other.push(light);
    other.push(turn);
    proof {
        the_acc_of_three::<Step>(turn, light, turn);
        the_acc_of_three::<Step>(light, turn, turn);
        assert(one@ =~= seq![turn, light]);
        assert(other@ =~= seq![light, turn]);
        assert(one@.take(2) =~= seq![turn, light]);
        assert(other@.take(2) =~= seq![light, turn]);
        assert(3u64 >> 1 == 1u64 && 3u64 & 1 == 1u64 && 4u64 >> 1 == 2u64 && 4u64 & 1 == 0u64) by (bit_vector);
        assert(1u64 ^ ((2u64 >> 32) | (2u64 << 32)) == 0x2_0000_0001u64 && 2u64 ^ 1u64 == 3u64) by (bit_vector);
    }
    (one.get_prefix(2), other.get_prefix(2))
}

// ── The iterator of an accumulator column ───────────────────────────────────

/// A `for` loop over an accumulator column's iterator gives each value
/// with what the values through it add up to: here those go into a
/// `Vec`, which then holds, for each position, what the values through
/// it add up to. For every type an accumulator column holds, and every
/// codec.
#[allow(dead_code)]
fn running_accs<V: VAccumulated + VValue<C>, C: VCodec>(col: &VerifiedAccColumn<V, C>) -> (out: Vec<V::Acc>)
    ensures
        out@.len() == col@.len(),
        forall|i: int| 0 <= i < out@.len() ==> #[trigger] out@[i] == accumulated::<V>(col@.take(i + 1)),
{
    let it = col.prefix_iter();
    let ghost all = it.items();
    proof {
        it.lemma_items();
    }
    let mut out: Vec<V::Acc> = Vec::new();
    for pv in at: it
        invariant
            at.seq() == all,
            out@.len() == at.index(),
            forall|i: int| 0 <= i < out@.len() ==> #[trigger] out@[i] == all[i].total(),
    {
        out.push(pv.total());
    }
    out
}

/// The values of a range of an accumulator column, each with what the
/// values before it add up to: counted from the column's start, though
/// the iterator reads only the range.
#[allow(dead_code)]
fn accs_in_a_range<'a, V: VAccumulated + VValue<C>, C: VCodec>(col: &'a VerifiedAccColumn<V, C>, a: usize, b: usize) -> (out:
    Vec<(Get<'a, V>, V::Acc)>)
    requires
        a <= b <= col@.len(),
    ensures
        out@.len() == b - a,
        forall|i: int|
            0 <= i < out@.len() ==> {
                &&& (#[trigger] out@[i]).0.sv() == col@[a + i]
                &&& out@[i].1 == accumulated::<V>(col@.take(a + i))
            },
{
    let it = col.prefix_iter_range(a, b);
    let ghost all = it.items();
    proof {
        it.lemma_items();
    }
    let mut out: Vec<(Get<'a, V>, V::Acc)> = Vec::new();
    for pv in at: it
        invariant
            at.seq() == all,
            out@.len() == at.index(),
            forall|i: int|
                0 <= i < out@.len() ==> (#[trigger] out@[i]).0 == all[i].value && out@[i].1 == all[i].prefix(),
    {
        out.push((pv.value, pv.prefix()));
    }
    out
}

/// One value of an accumulator column with its sums: the value at the
/// position, what the values before it add up to and what those through
/// it do, which is the first and then what the value adds.
#[allow(dead_code)]
fn a_value_with_its_accs<'a, V: VAccumulated + VValue<C>, C: VCodec>(col: &'a VerifiedAccColumn<V, C>, i: usize) -> (r: (
    V::Acc,
    Get<'a, V>,
    V::Acc,
))
    requires
        i < col@.len(),
    ensures
        r.1.sv() == col@[i as int],
        r.0 == accumulated::<V>(col@.take(i as int)),
        r.2 == accumulated::<V>(col@.take(i + 1)),
        r.2 == <V::Acc as VGroup>::plus(r.0, V::adds(col@[i as int])),
{
    let pv = col.get_prefixed(i).unwrap();
    (pv.prefix(), pv.value, pv.total())
}

/// The sums that come with a value are the sums the column answers
/// with for its position.
#[allow(dead_code)]
fn accs_two_ways<V: VAccumulated + VValue<C>, C: VCodec>(col: &VerifiedAccColumn<V, C>, i: usize) -> (r: (
    (V::Acc, V::Acc),
    (V::Acc, V::Acc),
))
    requires
        i < col@.len(),
    ensures
        r.0 == r.1,
{
    let pv = col.get_prefixed(i).unwrap();
    ((pv.prefix(), pv.total()), (col.get_prefix(i), col.get_total(i)))
}

/// A step of an accumulator column's iterator, wherever it stands: the
/// sum before the value it gives is the running sum as it was, the sum
/// through the value is the running sum after, and the second is the
/// first and then what the value adds.
#[allow(dead_code)]
fn a_step_with_its_accs<'a, V: VAccumulated + VValue<C>, C: VCodec>(it: &mut VerifiedAccIter<'a, V, C>) -> (r: Option<
    (V::Acc, Get<'a, V>, V::Acc),
>)
    ensures
        match r {
            Some((before, v, through)) => {
                &&& old(it).rest().len() > 0
                &&& v.sv() == old(it).rest()[0]
                &&& before == old(it).total()
                &&& through == final(it).total()
                &&& through == <V::Acc as VGroup>::plus(before, V::adds(v.sv()))
                &&& final(it).rest() == old(it).rest().skip(1)
            },
            None => old(it).rest().len() == 0 && final(it).total() == old(it).total(),
        },
{
    let before = it.total();
    match it.next() {
        Some(pv) => {
            let through = it.total();
            Some((before, pv.value, through))
        },
        None => None,
    }
}

/// What the values of a range add up to, read with an iterator: moved
/// to where the range starts, its sum started again there, and moved to
/// where the range ends. It is what the column answers with for the
/// range.
#[allow(dead_code)]
fn added_by_moving<V: VAccumulated + VValue<C>, C: VCodec>(col: &VerifiedAccColumn<V, C>, a: usize, b: usize) -> (r: (
    V::Acc,
    V::Acc,
))
    requires
        a <= b <= col@.len(),
    ensures
        r.0 == accumulated::<V>(col@.subrange(a as int, b as int)),
        r.0 == r.1,
{
    let mut it = col.prefix_iter();
    it.advance_to(a);
    it.reset_prefix();
    it.advance_to(b);
    (it.total(), col.sum_range(a, b))
}

/// Two values read with one iterator, which steps over the values
/// before each: the sums through them count the values stepped over.
#[allow(dead_code)]
fn two_skips_added<V: VAccumulated + VValue<C>, C: VCodec>(col: &VerifiedAccColumn<V, C>, i: usize, j: usize) -> (r: (
    V::Acc,
    V::Acc,
))
    requires
        i < j < col@.len(),
    ensures
        r.0 == accumulated::<V>(col@.take(i + 1)),
        r.1 == accumulated::<V>(col@.take(j + 1)),
{
    let mut it = col.prefix_iter();
    let a = it.nth(i).unwrap();
    let b = it.nth(j - i - 1).unwrap();
    (a.total(), b.total())
}

/// The running sum of an iterator whose sum counts from the column's
/// start is what the column answers with for where it stands.
#[allow(dead_code)]
fn a_running_acc_two_ways<V: VAccumulated + VValue<C>, C: VCodec>(
    it: &VerifiedAccIter<'_, V, C>,
    col: &VerifiedAccColumn<V, C>,
) -> (r: (V::Acc, V::Acc))
    requires
        it.source() == Some(*col),
        it.base_pos() == 0,
    ensures
        r.0 == r.1,
{
    proof {
        it.lemma_source();
    }
    (it.total(), col.get_prefix(it.pos()))
}

/// Where each move leaves an accumulator column's iterator, its window
/// and its running sum: the sum is of the values the iterator has
/// passed, in their order, whether it read them or stepped over them,
/// and a move of the window alone leaves it. In a column of ten
/// [`Step`]s, whose sums depend on the order.
#[allow(dead_code)]
fn moved_with_accs(col: &VerifiedAccColumn<Step>)
    requires
        col@.len() == 10,
{
    let ghost upto = |k: int| accumulated::<Step>(col@.take(k));
    // an iterator of a range: its sum counts from the column's start
    let mut it = col.prefix_iter_range(2, 8);
    let (p, e, n, w, t) = (it.pos(), it.end_pos(), it.len(), it.items_left(), it.total());
    assert(p == 2 && e == 8 && n == 6 && w == 6 && t == upto(2));
    // a step
    let a = it.next().unwrap();
    let t = it.total();
    assert(a.value == col@[2] && a.prefix() == upto(2) && a.total() == upto(3) && t == upto(3));
    // values stepped over are added
    it.advance_by(2);
    let (p, t) = (it.pos(), it.total());
    assert(p == 5 && t == upto(5));
    // a place behind the iterator: it stays, and so does its sum
    it.advance_to(4);
    let (p, t2) = (it.pos(), it.total());
    assert(p == 5 && t2 == t);
    // a wider window, which ends past the column: the sum stays
    it.set_max(20);
    let (e, n, w, t3) = (it.end_pos(), it.len(), it.items_left(), it.total());
    assert(e == 20 && n == 5 && w == 15 && t3 == t);
    // past what it has: all of it is passed, and added
    it.advance_by(9);
    let (p, e, n, t) = (it.pos(), it.end_pos(), it.len(), it.total());
    assert(p == 10 && e == 10 && n == 0);
    assert(t == accumulated::<Step>(col@)) by {
        assert(col@.take(10) =~= col@);
    }
    // the window moved to a range, and then to one that starts behind
    // the iterator
    let mut it = col.prefix_iter();
    it.shift(3..7);
    let (p, e, n, t) = (it.pos(), it.end_pos(), it.len(), it.total());
    assert(p == 3 && e == 7 && n == 4 && t == upto(3));
    it.shift(1..5);
    let (p, e, n, t) = (it.pos(), it.end_pos(), it.len(), it.total());
    assert(p == 3 && e == 5 && n == 2 && t == upto(3));
    // the first value of a window, with its sums
    let b = it.shift_next(6..9).unwrap();
    let (p, e, n, t) = (it.pos(), it.end_pos(), it.len(), it.total());
    assert(b.value == col@[6] && b.prefix() == upto(6) && b.total() == upto(7));
    assert(p == 7 && e == 9 && n == 2 && t == upto(7));
    // a window with no value
    let c = it.shift_next(9..9);
    let (p, e, t) = (it.pos(), it.end_pos(), it.total());
    assert(c is None && p == 9 && e == 9 && t == upto(9));
    // the sum started again: it counts from where the iterator stands
    let mut it = col.prefix_iter_range(4, 10);
    it.reset_prefix();
    let (p, e, t) = (it.pos(), it.end_pos(), it.total());
    assert(p == 4 && e == 10 && t == (Board { lit: 0, turned: false }));
    let d = it.nth(2).unwrap();
    let t = it.total();
    assert(d.value == col@[6] && t == d.total());
    assert(d.prefix() == accumulated::<Step>(col@.subrange(4, 6)) && d.total() == accumulated::<Step>(col@.subrange(4, 7)));
}

/// Of an iterator a client is handed, wherever it stands and wherever
/// its sum counts from: the running sum is what the column's values
/// from there up to the iterator add up to.
#[allow(dead_code)]
fn told_of_the_running_acc<V: VAccumulated + VValue<C>, C: VCodec>(it: &VerifiedAccIter<'_, V, C>) -> (r: V::Acc)
    ensures
        it.base_pos() <= it.pos() <= it.column().len(),
        r == it.total(),
        r == accumulated::<V>(it.column().subrange(it.base_pos() as int, it.pos() as int)),
{
    it.total()
}

/// The running sum under its other name: before the value the iterator
/// is to give next.
#[allow(dead_code)]
fn told_of_the_acc_before<V: VAccumulated + VValue<C>, C: VCodec>(it: &VerifiedAccIter<'_, V, C>) -> (r: V::Acc)
    ensures
        it.base_pos() <= it.pos() <= it.column().len(),
        r == it.total(),
        r == accumulated::<V>(it.column().subrange(it.base_pos() as int, it.pos() as int)),
{
    it.prefix()
}

/// Of an iterator a client is handed, what it has left to give: each of
/// the values left, with the sums before it and through it; and that it
/// reads the values of its column.
#[allow(dead_code)]
fn told_of_the_accs_left<V: VAccumulated + VValue<C>, C: VCodec>(it: &VerifiedAccIter<'_, V, C>)
    ensures
        it.items().len() == it.rest().len(),
        forall|i: int|
            0 <= i < it.items().len() ==> {
                &&& (#[trigger] it.items()[i]).value.sv() == it.rest()[i]
                &&& it.items()[i].total() == it.acc_to(it.pos() + i + 1)
                &&& it.items()[i].prefix() == it.acc_to(it.pos() + i)
                &&& it.items()[i].total() == <V::Acc as VGroup>::plus(it.items()[i].prefix(), V::adds(it.rest()[i]))
            },
        it.base_pos() <= it.pos() <= it.column().len(),
        it.total() == it.acc_to(it.pos() as int),
        match it.source() {
            Some(c) => it.column() == c@,
            None => it.column().len() == 0,
        },
{
    proof {
        it.lemma_items();
        it.lemma_total();
        it.lemma_source();
    }
}

/// What `prefix_iter` says by itself, of any accumulator column: the
/// iterator reads the column, has all its values left, and its sum
/// counts from the column's start and is what no values add up to.
#[allow(dead_code)]
fn told_by_an_acc_iter<'a, V: VAccumulated + VValue<C>, C: VCodec>(col: &'a VerifiedAccColumn<V, C>) -> (it: VerifiedAccIter<
    'a,
    V,
    C,
>)
    ensures
        it.rest() == col@,
        it.column() == col@,
        it.source() == Some(*col),
        it.pos() == 0,
        it.end_pos() == col@.len(),
        it.base_pos() == 0,
        it.total() == <V::Acc as VGroup>::zero(),
{
    col.prefix_iter()
}

/// What `prefix_iter_range` says by itself: the iterator has the values
/// of the range left, and its sum counts from the column's start, so it
/// is what the values before the range add up to.
#[allow(dead_code)]
fn told_by_an_acc_iter_range<'a, V: VAccumulated + VValue<C>, C: VCodec>(
    col: &'a VerifiedAccColumn<V, C>,
    start: usize,
    end: usize,
) -> (it: VerifiedAccIter<'a, V, C>)
    requires
        start <= end <= col@.len(),
    ensures
        it.rest() == col@.subrange(start as int, end as int),
        it.column() == col@,
        it.source() == Some(*col),
        it.pos() == start,
        it.end_pos() == end,
        it.base_pos() == 0,
        it.total() == accumulated::<V>(col@.take(start as int)),
{
    col.prefix_iter_range(start, end)
}

/// What `get_prefixed` says by itself of an accumulator column, of any
/// position.
#[allow(dead_code)]
fn told_by_an_acc_get_prefixed<'a, V: VAccumulated + VValue<C>, C: VCodec>(col: &'a VerifiedAccColumn<V, C>, index: usize) -> (r:
    Option<VerifiedAccValue<'a, V>>)
    ensures
        index < col@.len() ==> {
            &&& r matches Some(pv)
            &&& pv.value.sv() == col@[index as int]
            &&& pv.total() == accumulated::<V>(col@.take(index + 1))
            &&& pv.prefix() == accumulated::<V>(col@.take(index as int))
            &&& pv.total() == <V::Acc as VGroup>::plus(pv.prefix(), V::adds(pv.value.sv()))
        },
        index >= col@.len() ==> r is None,
{
    col.get_prefixed(index)
}

/// What a value with its sums says of the sum through it: a copy of the
/// one it holds.
#[allow(dead_code)]
fn told_by_an_acc_values_total<'a, V: VAccumulated>(pv: &VerifiedAccValue<'a, V>) -> (r: V::Acc)
    ensures
        r == pv.total,
        r == pv.total(),
{
    pv.total()
}

/// What a value with its sums says of the sum before it: the sum
/// through it, and then what undoes what the value adds, which is what
/// gives nothing when added after that.
#[allow(dead_code)]
fn told_by_an_acc_values_prefix<'a, V: VAccumulated>(pv: &VerifiedAccValue<'a, V>) -> (r: V::Acc)
    ensures
        r == pv.prefix(),
        r == <V::Acc as VGroup>::plus(
            pv.total,
            <V::Acc as VGroup>::minus(<V::Acc as VGroup>::zero(), V::adds(pv.value.sv())),
        ),
{
    pv.prefix()
}

/// What a copy of a value with its sums is: the same.
#[allow(dead_code)]
fn told_by_an_acc_values_clone<'a, V: VAccumulated>(pv: &VerifiedAccValue<'a, V>) -> (r: VerifiedAccValue<'a, V>)
    ensures
        r == *pv,
        r.value == pv.value,
        r.total == pv.total,
{
    pv.clone()
}

/// Where the sum through a value is some sum and then what the value
/// adds, that sum is the one before the value: of any value with its
/// sums, a client's own as well.
#[allow(dead_code)]
proof fn told_of_the_sum_undone<'a, V: VAccumulated>(pv: VerifiedAccValue<'a, V>, p: V::Acc)
    requires
        pv.total() == <V::Acc as VGroup>::plus(p, V::adds(pv.value.sv())),
    ensures
        pv.prefix() == p,
{
    pv.lemma_prefix(p);
}

/// What `next` says by itself of an accumulator column's iterator: in
/// the words of the iterator's own specification, and in those of
/// Verus's specification of an iterator.
#[allow(dead_code)]
fn told_by_an_acc_next<'a, V: VAccumulated + VValue<C>, C: VCodec>(it: &mut VerifiedAccIter<'a, V, C>) -> (r: Option<
    VerifiedAccValue<'a, V>,
>)
    ensures
        match r {
            Some(pv) => {
                &&& old(it).rest().len() > 0
                &&& pv.value.sv() == old(it).rest()[0]
                &&& pv.prefix() == old(it).total()
                &&& pv.total() == final(it).total()
                &&& pv.prefix() == old(it).acc_to(old(it).pos() as int)
                &&& pv.total() == old(it).acc_to(old(it).pos() + 1)
                &&& pv.total() == <V::Acc as VGroup>::plus(pv.prefix(), V::adds(pv.value.sv()))
                &&& final(it).rest() == old(it).rest().skip(1)
                &&& final(it).pos() == old(it).pos() + 1
                &&& final(it).end_pos() == old(it).end_pos()
            },
            None => {
                &&& old(it).rest().len() == 0
                &&& final(it).rest().len() == 0
                &&& final(it).pos() == old(it).pos()
                &&& final(it).end_pos() == old(it).pos()
                &&& final(it).total() == old(it).total()
            },
        },
        final(it).column() == old(it).column(),
        final(it).source() == old(it).source(),
        final(it).base_pos() == old(it).base_pos(),
        old(it).items().len() > 0 ==> r == Some(old(it).items()[0]) && final(it).items() == old(it).items().drop_first(),
        old(it).items().len() == 0 ==> r is None && final(it).items() == old(it).items(),
{
    it.next()
}

/// What `nth` says by itself of an accumulator column's iterator.
#[allow(dead_code)]
fn told_by_an_acc_nth<'a, V: VAccumulated + VValue<C>, C: VCodec>(it: &mut VerifiedAccIter<'a, V, C>, n: usize) -> (r: Option<
    VerifiedAccValue<'a, V>,
>)
    ensures
        n < old(it).rest().len() ==> {
            &&& r == Some(old(it).items()[n as int])
            &&& r matches Some(pv)
            &&& pv.value.sv() == old(it).rest()[n as int]
            &&& pv.total() == final(it).total()
            &&& pv.total() == old(it).acc_to(old(it).pos() + n + 1)
            &&& pv.prefix() == old(it).acc_to(old(it).pos() + n)
            &&& pv.total() == <V::Acc as VGroup>::plus(pv.prefix(), V::adds(pv.value.sv()))
            &&& final(it).items() == old(it).items().skip(n + 1)
            &&& final(it).rest() == old(it).rest().skip(n + 1)
            &&& final(it).pos() == old(it).pos() + n + 1
            &&& final(it).end_pos() == old(it).end_pos()
        },
        n >= old(it).rest().len() ==> {
            &&& r is None
            &&& final(it).rest().len() == 0
            &&& final(it).pos() == old(it).pos() + old(it).rest().len()
            &&& final(it).end_pos() == final(it).pos()
        },
        final(it).column() == old(it).column(),
        final(it).source() == old(it).source(),
        final(it).base_pos() == old(it).base_pos(),
        final(it).total() == old(it).acc_to(final(it).pos() as int),
{
    it.nth(n)
}

/// What the questions put to an accumulator column's iterator say by
/// themselves. Asked where it stands, it says where: within its
/// window, in its column and not before where its sum counts from.
#[allow(dead_code)]
fn told_by_an_acc_iters_place<V: VAccumulated + VValue<C>, C: VCodec>(it: &VerifiedAccIter<'_, V, C>) -> (r: usize)
    ensures
        r == it.pos(),
        it.base_pos() <= it.pos() <= it.end_pos(),
        it.pos() <= it.column().len(),
        it.rest() == window(it.column(), it.pos() as int, it.end_pos() as int),
{
    it.pos()
}

/// Asked how far its window reaches, an iterator says how many
/// positions, which may be past the column.
#[allow(dead_code)]
fn told_by_an_acc_iters_reach<V: VAccumulated + VValue<C>, C: VCodec>(it: &VerifiedAccIter<'_, V, C>) -> (r: usize)
    ensures
        r == it.end_pos() - it.pos(),
{
    it.items_left()
}

/// Asked how many values it has left, an iterator says how many.
#[allow(dead_code)]
fn told_by_an_acc_iters_len<V: VAccumulated + VValue<C>, C: VCodec>(it: &VerifiedAccIter<'_, V, C>) -> (r: usize)
    ensures
        r == it.rest().len(),
        r == it.items().len(),
{
    it.len()
}

/// Asked how many values it may yet give, an iterator says exactly how
/// many it has left, as its least and as its most.
#[allow(dead_code)]
fn told_by_an_acc_size_hint<V: VAccumulated + VValue<C>, C: VCodec>(it: &VerifiedAccIter<'_, V, C>) -> (r: (
    usize,
    Option<usize>,
))
    ensures
        r.0 == it.rest().len(),
        r.0 == it.items().len(),
        r.1 == Some(r.0),
{
    it.size_hint()
}

/// Asked for the window's end alone, an iterator says as much.
#[allow(dead_code)]
fn told_by_an_acc_iters_end<V: VAccumulated + VValue<C>, C: VCodec>(it: &VerifiedAccIter<'_, V, C>) -> (r: usize)
    ensures
        r == it.end_pos(),
        it.base_pos() <= it.pos() <= it.end_pos(),
        it.pos() <= it.column().len(),
        it.rest() == window(it.column(), it.pos() as int, it.end_pos() as int),
{
    it.end_pos()
}

/// What `reset_prefix` says by itself: the iterator and its window stay,
/// and its sum counts from where it stands and is what no values add
/// up to.
#[allow(dead_code)]
fn told_by_an_acc_reset<V: VAccumulated + VValue<C>, C: VCodec>(it: &mut VerifiedAccIter<'_, V, C>)
    ensures
        final(it).rest() == old(it).rest(),
        final(it).column() == old(it).column(),
        final(it).source() == old(it).source(),
        final(it).pos() == old(it).pos(),
        final(it).end_pos() == old(it).end_pos(),
        final(it).base_pos() == old(it).pos(),
        final(it).total() == <V::Acc as VGroup>::zero(),
{
    it.reset_prefix()
}

/// What `set_max` says by itself of an accumulator column's iterator.
#[allow(dead_code)]
fn told_by_an_acc_set_max<V: VAccumulated + VValue<C>, C: VCodec>(it: &mut VerifiedAccIter<'_, V, C>, end: usize)
    ensures
        final(it).column() == old(it).column(),
        final(it).source() == old(it).source(),
        final(it).base_pos() == old(it).base_pos(),
        final(it).total() == old(it).total(),
        final(it).pos() == old(it).pos(),
        final(it).end_pos() == if end >= old(it).pos() {
            end
        } else {
            old(it).pos()
        },
        final(it).rest() == window(old(it).column(), old(it).pos() as int, end as int),
{
    it.set_max(end)
}

/// What `advance_to` says by itself of an accumulator column's iterator.
#[allow(dead_code)]
fn told_by_an_acc_advance_to<V: VAccumulated + VValue<C>, C: VCodec>(it: &mut VerifiedAccIter<'_, V, C>, target: usize)
    ensures
        final(it).column() == old(it).column(),
        final(it).source() == old(it).source(),
        final(it).base_pos() == old(it).base_pos(),
        final(it).total() == old(it).acc_to(final(it).pos() as int),
        final(it).rest() == past(old(it).rest(), target - old(it).pos()),
        target <= old(it).pos() ==> {
            &&& final(it).pos() == old(it).pos()
            &&& final(it).end_pos() == old(it).end_pos()
        },
        old(it).pos() < target <= old(it).pos() + old(it).rest().len() ==> {
            &&& final(it).pos() == target
            &&& final(it).end_pos() == old(it).end_pos()
        },
        target > old(it).pos() + old(it).rest().len() ==> {
            &&& final(it).pos() == old(it).pos() + old(it).rest().len()
            &&& final(it).end_pos() == final(it).pos()
        },
{
    it.advance_to(target)
}

/// What `advance_by` says by itself of an accumulator column's iterator.
#[allow(dead_code)]
fn told_by_an_acc_advance_by<V: VAccumulated + VValue<C>, C: VCodec>(it: &mut VerifiedAccIter<'_, V, C>, amount: usize)
    ensures
        final(it).column() == old(it).column(),
        final(it).source() == old(it).source(),
        final(it).base_pos() == old(it).base_pos(),
        final(it).total() == old(it).acc_to(final(it).pos() as int),
        final(it).rest() == past(old(it).rest(), amount as int),
        amount <= old(it).rest().len() ==> {
            &&& final(it).pos() == old(it).pos() + amount
            &&& final(it).end_pos() == old(it).end_pos()
        },
        amount > old(it).rest().len() ==> {
            &&& final(it).pos() == old(it).pos() + old(it).rest().len()
            &&& final(it).end_pos() == final(it).pos()
        },
{
    it.advance_by(amount)
}

/// What `shift` says by itself of an accumulator column's iterator.
#[allow(dead_code)]
fn told_by_an_acc_shift<V: VAccumulated + VValue<C>, C: VCodec>(it: &mut VerifiedAccIter<'_, V, C>, range: Range<usize>)
    ensures
        final(it).column() == old(it).column(),
        final(it).source() == old(it).source(),
        final(it).base_pos() == old(it).base_pos(),
        final(it).total() == old(it).acc_to(final(it).pos() as int),
        final(it).rest() == window(
            old(it).column(),
            if range.start <= old(it).pos() {
                old(it).pos() as int
            } else {
                range.start as int
            },
            range.end as int,
        ),
        range.start <= old(it).pos() ==> {
            &&& final(it).pos() == old(it).pos()
            &&& final(it).end_pos() == if range.end >= old(it).pos() {
                range.end
            } else {
                old(it).pos()
            }
        },
        old(it).pos() < range.start <= range.end && range.start <= old(it).column().len() ==> {
            &&& final(it).pos() == range.start
            &&& final(it).end_pos() == range.end
        },
        old(it).pos() < range.start && (range.start > range.end || range.start > old(it).column().len()) ==> {
            &&& final(it).pos() == if range.end <= old(it).pos() {
                old(it).pos() as int
            } else if range.end <= old(it).column().len() {
                range.end as int
            } else {
                old(it).column().len() as int
            }
            &&& final(it).end_pos() == final(it).pos()
        },
{
    it.shift(range)
}

/// What `shift_next` says by itself of an accumulator column's
/// iterator.
#[allow(dead_code)]
fn told_by_an_acc_shift_next<'a, V: VAccumulated + VValue<C>, C: VCodec>(
    it: &mut VerifiedAccIter<'a, V, C>,
    range: Range<usize>,
) -> (r: Option<VerifiedAccValue<'a, V>>)
    requires
        range.start >= old(it).pos(),
    ensures
        final(it).column() == old(it).column(),
        final(it).source() == old(it).source(),
        final(it).base_pos() == old(it).base_pos(),
        final(it).total() == old(it).acc_to(final(it).pos() as int),
        ({
            let w = window(old(it).column(), range.start as int, range.end as int);
            &&& w.len() > 0 ==> {
                &&& r matches Some(pv)
                &&& pv.value.sv() == w[0]
                &&& pv.total() == final(it).total()
                &&& pv.total() == old(it).acc_to(range.start + 1)
                &&& pv.prefix() == old(it).acc_to(range.start as int)
                &&& pv.total() == <V::Acc as VGroup>::plus(pv.prefix(), V::adds(pv.value.sv()))
                &&& final(it).rest() == w.skip(1)
                &&& final(it).pos() == range.start + 1
                &&& final(it).end_pos() == range.end
            }
            &&& w.len() == 0 ==> {
                &&& r is None
                &&& final(it).rest().len() == 0
                &&& final(it).pos() == if range.end <= old(it).pos() {
                    old(it).pos() as int
                } else if range.end <= old(it).column().len() {
                    range.end as int
                } else {
                    old(it).column().len() as int
                }
                &&& final(it).end_pos() == final(it).pos()
            }
        }),
{
    it.shift_next(range)
}

/// What an accumulator column's values add up to, read a run at a time:
/// when no run is left, the running sum is that of the column, and the
/// runs' counts have added up to its length. For every type an
/// accumulator column holds, and every codec.
#[allow(dead_code)]
fn added_by_run_moves<V: VAccumulated + VValue<C>, C: VCodec>(col: &VerifiedAccColumn<V, C>) -> (r: (V::Acc, usize))
    ensures
        r.0 == accumulated::<V>(col@),
        r.1 == col@.len(),
{
    let mut it = col.prefix_iter();
    let mut count: usize = 0;
    loop
        invariant
            it.column() == col@,
            it.base_pos() == 0,
            it.rest().len() == col@.len() - it.pos(),
            count == it.pos(),
        decreases it.rest().len(),
    {
        match it.next_run() {
            Some(run) => {
                count = count + run.count;
            },
            None => {
                let sum = it.total();
                proof {
                    assert(col@.subrange(0, col@.len() as int) =~= col@);
                }
                return (sum, count);
            },
        }
    }
}

/// A run of an accumulator column's iterator, wherever it stands: the
/// values the iterator had next are the run's, as many as the run is
/// long and no more, and the running sum is as it was and then what the
/// run's value adds, as many times over as the run is long.
#[allow(dead_code)]
fn a_run_with_its_accs<'a, V: VAccumulated + VValue<C>, C: VCodec>(it: &mut VerifiedAccIter<'a, V, C>) -> (r: Option<
    (usize, Get<'a, V>, V::Acc),
>)
    ensures
        match r {
            Some((n, v, through)) => {
                &&& 1 <= n <= old(it).rest().len()
                &&& forall|i: int| 0 <= i < n ==> #[trigger] old(it).rest()[i] == v.sv()
                &&& n < old(it).rest().len() ==> old(it).rest()[n as int] != v.sv()
                &&& through == final(it).total()
                &&& through == <V::Acc as VGroup>::plus(old(it).total(), times::<V::Acc>(n as nat, V::adds(v.sv())))
                &&& final(it).rest() == old(it).rest().skip(n as int)
            },
            None => old(it).rest().len() == 0 && final(it).total() == old(it).total(),
        },
{
    match it.next_run() {
        Some(run) => Some((run.count, run.value.value, run.value.total())),
        None => None,
    }
}

/// A run of marks read by the iterator: an even number of marks of one
/// kind leave open the kinds that were, and an odd number open the one
/// kind or close it, by what the client proved of its accumulator added
/// many times over ([`lemma_flags_times`]).
#[allow(dead_code)]
fn a_run_of_marks(it: &mut VerifiedAccIter<'_, Mark>) -> (r: Option<(usize, Mark, Flags)>)
    ensures
        match r {
            Some((n, v, through)) => {
                &&& through == final(it).total()
                &&& n % 2 == 0 ==> through == old(it).total()
                &&& n % 2 == 1 ==> through == Flags(old(it).total().0 ^ flag_of(v))
            },
            None => final(it).total() == old(it).total(),
        },
{
    match it.next_run() {
        Some(run) => {
            proof {
                lemma_flags_times(run.count as nat, Flags(flag_of(run.value.value)));
                let x = old(it).total().0;
                assert(x ^ 0u64 == x) by (bit_vector);
            }
            Some((run.count, run.value.value, run.value.total()))
        },
        None => None,
    }
}

/// A range of an accumulator column copied by its runs: the runs its
/// iterator has left are those of the values it has left, and spliced
/// into a new column they make a column of those values.
#[allow(dead_code)]
fn accs_copied_by_runs<V: VAccumulated + VValue<C>, C: VCodec>(col: &VerifiedAccColumn<V, C>, a: usize, b: usize) -> (r:
    VerifiedColumn<V, C, LenIndex>)
    requires
        a <= b <= col@.len(),
    ensures
        r@ == col@.subrange(a as int, b as int),
{
    let mut r = VerifiedColumn::<V, C, LenIndex>::new();
    let it = col.prefix_iter_range(a, b);
    proof {
        it.plain().lemma_items_len();
        lemma_runs_of_values(it.plain().items());
    }
    r.splice_runs(0, 0, it.runs());
    assert(r@ =~= col@.subrange(a as int, b as int));
    r
}

/// What the values of a range add up to, said two ways: by `delta`,
/// asked for the value after the range, and by `sum_range`.
#[allow(dead_code)]
fn a_range_added_two_ways<V: VAccumulated + VValue<C>, C: VCodec>(col: &VerifiedAccColumn<V, C>, a: usize, b: usize) -> (r: (
    V::Acc,
    V::Acc,
))
    requires
        a <= b < col@.len(),
    ensures
        r.0 == accumulated::<V>(col@.subrange(a as int, b as int)),
        r.0 == r.1,
{
    let seek = col.delta(a, b).unwrap();
    (seek.delta, col.sum_range(a, b))
}

/// An accumulator column's iterator handed over, of which no more is
/// known than the column it reads. Suspended and resumed on that
/// column, it is sure to be given back: where it stood, with the same
/// values left and the same running sum, counted from the same place.
#[allow(dead_code)]
fn handed_over_with_its_acc<'a, V: VAccumulated + VValue<C>, C: VCodec>(
    it: VerifiedAccIter<'_, V, C>,
    col: &'a VerifiedAccColumn<V, C>,
) -> (on: VerifiedAccIter<'a, V, C>)
    requires
        it.source() == Some(*col),
    ensures
        on.pos() == it.pos(),
        on.rest() == it.rest(),
        on.base_pos() == it.base_pos(),
        on.total() == it.total(),
{
    let s = it.suspend();
    proof {
        it.lemma_source();
        it.lemma_total();
    }
    let got = s.try_resume(col);
    got.unwrap()
}

/// A reader of sums that stops and goes on. It adds up the values of
/// `a..m`, and is suspended: the iterator is given up, and with it the
/// borrow of the column. Resumed on the column, which nothing has
/// edited, it is sure to be given, and its sum goes on to count from
/// `a`: read to `b`, it is what the values of `a..b` add up to.
#[allow(dead_code)]
fn added_in_two_goes<V: VAccumulated + VValue<C>, C: VCodec>(col: &VerifiedAccColumn<V, C>, a: usize, m: usize, b: usize) -> (r:
    V::Acc)
    requires
        a <= m <= b <= col@.len(),
    ensures
        r == accumulated::<V>(col@.subrange(a as int, b as int)),
{
    let mut it = col.prefix_iter();
    it.advance_to(a);
    it.reset_prefix();
    it.advance_to(m);
    let s = it.suspend();
    let got = s.try_resume(col);
    assert(got is Ok);
    let mut on = got.unwrap();
    on.advance_to(b);
    on.total()
}

/// An edit ends what was suspended of an accumulator column's iterator
/// too. The state fits the column the iterator reads, and so counts the
/// edits that one does; after a push the column counts one more, and
/// refuses the state.
#[allow(dead_code)]
fn accs_ended_by_a_push<V: VAccumulated + VValue<C>, C: VCodec>(col: &mut VerifiedAccColumn<V, C>, v: Get<'_, V>) -> (r: bool)
    ensures
        !r,
        final(col)@ == old(col)@.push(v.sv()),
{
    let s = col.prefix_iter().suspend();
    proof {
        s.lemma_fits(&*col);
    }
    col.push(v);
    s.try_resume(col).is_ok()
}

/// Values found with their sums, an iterator and its copy, and an
/// iterator of no column: what each call leaves, in a column of ten
/// [`Mark`]s.
#[allow(dead_code)]
fn found_with_accs(col: &VerifiedAccColumn<Mark>)
    requires
        col@.len() == 10,
{
    let ghost upto = |k: int| accumulated::<Mark>(col@.take(k));
    // a value further on, with where it is and what is passed over
    let mut it = col.prefix_iter_range(2, 9);
    let a = it.delta_nth(3).unwrap();
    let (p, e, t) = (it.pos(), it.end_pos(), it.total());
    assert(a.pos == 5 && a.pv.value == col@[5] && p == 6 && e == 9 && t == upto(6));
    assert(a.pv.prefix() == upto(5) && a.pv.total() == upto(6));
    assert(a.delta == accumulated::<Mark>(col@.subrange(2, 5))) by {
        assert(col@.subrange(2, 9).take(3) =~= col@.subrange(2, 5));
    }
    // a copy is an iterator of its own: it moves, and this one stays
    let mut twin = it.clone();
    let b = twin.next().unwrap();
    let (p, q, t, u) = (it.pos(), twin.pos(), it.total(), twin.total());
    assert(b.value == col@[6] && p == 6 && q == 7 && t == upto(6) && u == upto(7));
    // more than it has left: none is found, and all is passed
    let c = it.delta_nth(3);
    let (p, e, t) = (it.pos(), it.end_pos(), it.total());
    assert(c is None && p == 9 && e == 9 && t == upto(9));
    // the column asked for the same value
    let d = col.delta(2, 5).unwrap();
    assert(d.pos == 5 && d.pv.value == col@[5] && d.delta == a.delta && d.pv.total() == upto(6));
    let f = col.delta(2, 10);
    assert(f is None);
    // an iterator of no column: nothing to give, to find or to pass
    let mut no = VerifiedAccIter::<Mark>::default();
    let (p, e, n, t) = (no.pos(), no.end_pos(), no.len(), no.total());
    assert(p == 0 && e == 0 && n == 0 && t == Flags(0));
    let x = no.next_run();
    let z = no.delta_nth(0);
    let (p, t) = (no.pos(), no.total());
    assert(x is None && z is None && p == 0 && t == Flags(0));
}

/// The same two steps in either order, each column read for its second
/// value with its sums: before it is what the first step adds, and
/// through it the two in the order the column holds them, which leaves
/// other lamps lit in one column than in the other
/// ([`steps_in_order`]).
#[allow(dead_code)]
fn steps_read_in_order() -> (r: ((Board, Board), (Board, Board)))
    ensures
        r.0 == (Board { lit: 1, turned: true }, Board { lit: 0x2_0000_0001, turned: true }),
        r.1 == (Board { lit: 2, turned: false }, Board { lit: 3, turned: true }),
{
    let turn = Step(3);
    let light = Step(4);
    let mut one = VerifiedAccColumn::<Step>::new();
    one.push(turn);
    one.push(light);
    let mut other = VerifiedAccColumn::<Step>::new();
    other.push(light);
    other.push(turn);
    proof {
        the_acc_of_three::<Step>(turn, light, turn);
        the_acc_of_three::<Step>(light, turn, turn);
        assert(one@ =~= seq![turn, light]);
        assert(other@ =~= seq![light, turn]);
        assert(one@.take(1) =~= seq![turn]);
        assert(other@.take(1) =~= seq![light]);
        assert(one@.take(2) =~= seq![turn, light]);
        assert(other@.take(2) =~= seq![light, turn]);
        assert(3u64 >> 1 == 1u64 && 3u64 & 1 == 1u64 && 4u64 >> 1 == 2u64 && 4u64 & 1 == 0u64) by (bit_vector);
        assert(1u64 ^ ((2u64 >> 32) | (2u64 << 32)) == 0x2_0000_0001u64 && 2u64 ^ 1u64 == 3u64) by (bit_vector);
    }
    let a = one.get_prefixed(1).unwrap();
    let b = other.get_prefixed(1).unwrap();
    ((a.prefix(), a.total()), (b.prefix(), b.total()))
}

/// What `next_run` says by itself of an accumulator column's iterator.
#[allow(dead_code)]
fn told_by_an_acc_next_run<'a, V: VAccumulated + VValue<C>, C: VCodec>(it: &mut VerifiedAccIter<'a, V, C>) -> (r: Option<
    Run<VerifiedAccValue<'a, V>>,
>)
    ensures
        final(it).column() == old(it).column(),
        final(it).source() == old(it).source(),
        final(it).base_pos() == old(it).base_pos(),
        final(it).total() == old(it).acc_to(final(it).pos() as int),
        match r {
            Some(run) => {
                &&& head_run(old(it).rest(), run.value.value.sv(), run.count as int)
                &&& run.value == old(it).items()[run.count - 1]
                &&& run.value.total() == final(it).total()
                &&& run.value.total() == old(it).acc_to(old(it).pos() + run.count)
                &&& run.value.prefix() == old(it).acc_to(old(it).pos() + run.count - 1)
                &&& run.value.total() == <V::Acc as VGroup>::plus(
                    old(it).total(),
                    times::<V::Acc>(run.count as nat, V::adds(run.value.value.sv())),
                )
                &&& final(it).items() == old(it).items().skip(run.count as int)
                &&& final(it).rest() == old(it).rest().skip(run.count as int)
                &&& final(it).pos() == old(it).pos() + run.count
                &&& final(it).end_pos() == old(it).end_pos()
            },
            None => {
                &&& old(it).rest().len() == 0
                &&& final(it).rest().len() == 0
                &&& final(it).pos() == old(it).pos()
                &&& final(it).end_pos() == old(it).pos()
                &&& final(it).total() == old(it).total()
            },
        },
{
    it.next_run()
}

/// What `runs` says by itself: the iterator of runs reads on from the
/// iterator it is made of, which is the accumulator column's without
/// its sums, and so stands where that stood with the same values left.
#[allow(dead_code)]
fn told_by_an_acc_runs<'a, V: VAccumulated + VValue<C>, C: VCodec>(it: VerifiedAccIter<'a, V, C>) -> (r: VerifiedRuns<
    'a,
    V,
    C,
    AccIndex,
>)
    ensures
        r.iter() == it.plain(),
        r.iter().rest() == it.rest(),
        r.iter().column() == it.column(),
        r.iter().source() == it.source(),
        r.iter().pos() == it.pos(),
        r.iter().end_pos() == it.end_pos(),
{
    it.runs()
}

/// What `delta_nth` says by itself of an accumulator column's iterator.
#[allow(dead_code)]
fn told_by_an_acc_delta_nth<'a, V: VAccumulated + VValue<C>, C: VCodec>(it: &mut VerifiedAccIter<'a, V, C>, n: usize) -> (r:
    Option<VerifiedAccSeek<'a, V>>)
    ensures
        n < old(it).rest().len() ==> {
            &&& r matches Some(seek)
            &&& seek.pos == old(it).pos() + n
            &&& seek.pv == old(it).items()[n as int]
            &&& seek.pv.value.sv() == old(it).rest()[n as int]
            &&& seek.delta == accumulated::<V>(old(it).rest().take(n as int))
            &&& seek.pv.total() == final(it).total()
            &&& seek.pv.total() == old(it).acc_to(old(it).pos() + n + 1)
            &&& seek.pv.prefix() == old(it).acc_to(old(it).pos() + n)
            &&& seek.pv.prefix() == <V::Acc as VGroup>::plus(old(it).total(), seek.delta)
            &&& seek.pv.total() == <V::Acc as VGroup>::plus(seek.pv.prefix(), V::adds(seek.pv.value.sv()))
            &&& final(it).items() == old(it).items().skip(n + 1)
            &&& final(it).rest() == old(it).rest().skip(n + 1)
            &&& final(it).pos() == old(it).pos() + n + 1
            &&& final(it).end_pos() == old(it).end_pos()
        },
        n >= old(it).rest().len() ==> {
            &&& r is None
            &&& final(it).rest().len() == 0
            &&& final(it).pos() == old(it).pos() + old(it).rest().len()
            &&& final(it).end_pos() == final(it).pos()
        },
        final(it).column() == old(it).column(),
        final(it).source() == old(it).source(),
        final(it).base_pos() == old(it).base_pos(),
        final(it).total() == old(it).acc_to(final(it).pos() as int),
{
    it.delta_nth(n)
}

/// What `suspend` says by itself of an accumulator column's iterator,
/// and what its state says of where its sum counts from.
#[allow(dead_code)]
fn told_by_an_acc_suspend<'a, V: VAccumulated + VValue<C>, C: VCodec>(
    it: &VerifiedAccIter<'a, V, C>,
    col: &VerifiedAccColumn<V, C>,
) -> (s: VerifiedAccIterState<V>)
    ensures
        s.pos() == it.pos(),
        s.end_pos() == it.end_pos(),
        s.base_pos() == it.base_pos(),
        it.rest() == window(it.column(), s.pos() as int, s.end_pos() as int),
        it.source() == Some(*col) ==> s.fits(col),
        s.base_pos() <= s.pos(),
{
    let s = it.suspend();
    proof {
        s.lemma_base();
    }
    s
}

/// What `try_resume` says by itself, of any state of an accumulator
/// column's iterator and any accumulator column, each thing asked for
/// on its own.
#[allow(dead_code)]
fn told_by_an_acc_resume<'a, V: VAccumulated + VValue<C>, C: VCodec>(
    s: &VerifiedAccIterState<V>,
    col: &'a VerifiedAccColumn<V, C>,
) -> (r: Result<VerifiedAccIter<'a, V, C>, PackError>)
    ensures
        r is Ok <==> s.fits(col),
        r is Ok ==> s.edits() == col.edits(),
        r matches Ok(it) ==> it.source() == Some(*col),
        r matches Ok(it) ==> it.column() == col@,
        r matches Ok(it) ==> it.pos() == s.pos(),
        r matches Ok(it) ==> it.rest() == window(col@, s.pos() as int, s.end_pos() as int),
        r matches Ok(it) ==> it.end_pos() == s.end_pos() || (s.pos() == col@.len() && it.end_pos() == s.pos()),
        r matches Ok(it) ==> it.base_pos() == s.base_pos(),
        r matches Ok(it) ==> it.total() == accumulated::<V>(col@.subrange(s.base_pos() as int, s.pos() as int)),
{
    s.try_resume(col)
}

/// What a state that fits a column says of it: the two count the same
/// edits, and the state is a place in the column as a plain iterator's
/// state is.
#[allow(dead_code)]
fn told_by_an_acc_states_fit<V: VAccumulated + VValue<C>, C: VCodec>(s: &VerifiedAccIterState<V>, col: &VerifiedAccColumn<V, C>)
    requires
        s.fits(col),
    ensures
        s.edits() == col.edits(),
        s.plain().fits(col),
        s.pos() == s.plain().pos(),
        s.end_pos() == s.plain().end_pos(),
{
    proof {
        s.lemma_fits(col);
    }
}

/// What `clone` says by itself of an accumulator column's iterator.
#[allow(dead_code)]
fn told_by_an_acc_clone<'a, V: VAccumulated + VValue<C>, C: VCodec>(it: &VerifiedAccIter<'a, V, C>) -> (r: VerifiedAccIter<
    'a,
    V,
    C,
>)
    ensures
        r.items() == it.items(),
        r.rest() == it.rest(),
        r.column() == it.column(),
        r.source() == it.source(),
        r.pos() == it.pos(),
        r.end_pos() == it.end_pos(),
        r.base_pos() == it.base_pos(),
        r.total() == it.total(),
{
    it.clone()
}

/// What `default` says by itself of an accumulator column's iterator.
#[allow(dead_code)]
fn told_by_an_acc_default<'a, V: VAccumulated + VValue<C>, C: VCodec>() -> (r: VerifiedAccIter<'a, V, C>)
    ensures
        r.items().len() == 0,
        r.rest().len() == 0,
        r.column().len() == 0,
        r.source() is None,
        r.pos() == 0,
        r.end_pos() == 0,
        r.base_pos() == 0,
        r.total() == <V::Acc as VGroup>::zero(),
{
    VerifiedAccIter::<V, C>::default()
}

/// What `delta` says by itself of an accumulator column, of any two
/// positions in order.
#[allow(dead_code)]
fn told_by_an_acc_delta<'a, V: VAccumulated + VValue<C>, C: VCodec>(col: &'a VerifiedAccColumn<V, C>, from: usize, to: usize) -> (r:
    Option<VerifiedAccSeek<'a, V>>)
    requires
        from <= to,
    ensures
        to < col@.len() ==> {
            &&& r matches Some(seek)
            &&& seek.pos == to
            &&& seek.pv.value.sv() == col@[to as int]
            &&& seek.delta == accumulated::<V>(col@.subrange(from as int, to as int))
            &&& seek.pv.total() == accumulated::<V>(col@.take(to + 1))
            &&& seek.pv.prefix() == accumulated::<V>(col@.take(to as int))
            &&& seek.pv.total() == <V::Acc as VGroup>::plus(seek.pv.prefix(), V::adds(seek.pv.value.sv()))
        },
        to >= col@.len() ==> r is None,
{
    col.delta(from, to)
}

/// What a copy of a value found is: the same, part for part.
#[allow(dead_code)]
fn told_by_an_acc_seek<'a, V: VAccumulated>(seek: &VerifiedAccSeek<'a, V>) -> (r: VerifiedAccSeek<'a, V>)
    ensures
        r == *seek,
        r.pos == seek.pos,
        r.delta == seek.delta,
        r.pv == seek.pv,
{
    seek.clone()
}

/// The parts of a value found are there to take: where it is, what was
/// passed over, the value and the sum through it.
#[allow(dead_code)]
fn the_parts_of_a_find<'a, V: VAccumulated>(seek: VerifiedAccSeek<'a, V>) -> (r: (usize, V::Acc, Get<'a, V>, V::Acc))
    ensures
        r.0 == seek.pos,
        r.1 == seek.delta,
        r.2 == seek.pv.value,
        r.3 == seek.pv.total,
{
    (seek.pos, seek.delta, seek.pv.value, seek.pv.total)
}

/// What is proved of every type an accumulator column holds is proved
/// of the client's own and of its nulls: the running sums of a column
/// of marks, a column of nulls and marks added up run by run, and a
/// range of steps added up by moving, in the order the column holds
/// them.
#[allow(dead_code)]
fn accs_of_the_clients_own(
    col: &VerifiedAccColumn<Mark>,
    nullable: &VerifiedAccColumn<Option<Mark>>,
    steps: &VerifiedAccColumn<Step>,
    a: usize,
    b: usize,
) -> (r: (Vec<Flags>, (Flags, usize), (Board, Board)))
    requires
        a <= b <= steps@.len(),
    ensures
        r.0@.len() == col@.len(),
        forall|i: int| 0 <= i < r.0@.len() ==> #[trigger] r.0@[i] == accumulated::<Mark>(col@.take(i + 1)),
        r.1.0 == accumulated::<Option<Mark>>(nullable@),
        r.1.1 == nullable@.len(),
        r.2.0 == accumulated::<Step>(steps@.subrange(a as int, b as int)),
        r.2.0 == r.2.1,
{
    (
        running_accs::<Mark, Leb128>(col),
        added_by_run_moves::<Option<Mark>, Leb128>(nullable),
        added_by_moving::<Step, Leb128>(steps, a, b),
    )
}

// ── A delta column ──────────────────────────────────────────────────────────

/// A delta column built from values holds those values: for every type a
/// delta column holds, and every codec.
#[allow(dead_code)]
fn delta_built<T: VDeltaValue<C>, C: VCodec>(values: &[T]) -> (col: VerifiedDeltaColumn<T, C>)
    requires
        all_fit(values@),
    ensures
        col@ == values@,
{
    VerifiedDeltaColumn::from_values(values)
}

/// A new delta column is empty.
#[allow(dead_code)]
fn delta_new<T: VDeltaValue<C>, C: VCodec>() -> (col: VerifiedDeltaColumn<T, C>)
    ensures
        col@.len() == 0,
{
    VerifiedDeltaColumn::new()
}

/// How many values a delta column holds.
#[allow(dead_code)]
fn delta_len<T: VDeltaValue<C>, C: VCodec>(col: &VerifiedDeltaColumn<T, C>) -> (n: usize)
    ensures
        n == col@.len(),
{
    col.len()
}

/// Whether a delta column holds none.
#[allow(dead_code)]
fn delta_is_empty<T: VDeltaValue<C>, C: VCodec>(col: &VerifiedDeltaColumn<T, C>) -> (r: bool)
    ensures
        r == (col@.len() == 0),
{
    col.is_empty()
}

/// Reading a delta column gives the value kept at an index, and nothing
/// past the end.
#[allow(dead_code)]
fn delta_read<T: VDeltaValue<C>, C: VCodec>(col: &VerifiedDeltaColumn<T, C>, i: usize) -> (r: Option<T>)
    ensures
        i < col@.len() ==> r == Some(col@[i as int]),
        i >= col@.len() ==> r is None,
{
    col.get(i)
}

/// A value read from a delta column is one a delta column can hold, so
/// it can go into another.
#[allow(dead_code)]
fn delta_read_fits<T: VDeltaValue<C>, C: VCodec>(col: &VerifiedDeltaColumn<T, C>, i: usize) -> (r: Option<T>)
    ensures
        r matches Some(v) ==> v.fits(),
{
    proof {
        col.lemma_fits();
    }
    col.get(i)
}

/// A splice replaces `del` values at `at` by the given ones and leaves
/// the rest as they were.
#[allow(dead_code)]
fn delta_spliced<T: VDeltaValue<C>, C: VCodec>(col: &mut VerifiedDeltaColumn<T, C>, at: usize, del: usize, values: &[T])
    requires
        at <= old(col)@.len(),
        del <= old(col)@.len() - at,
        all_fit(values@),
    ensures
        final(col)@ == old(col)@.take(at as int) + values@ + old(col)@.skip(at + del),
{
    col.splice(at, del, values);
}

/// One value put in place of another.
#[allow(dead_code)]
fn delta_set<T: VDeltaValue<C>, C: VCodec>(col: &mut VerifiedDeltaColumn<T, C>, i: usize, v: T)
    requires
        i < old(col)@.len(),
        v.fits(),
    ensures
        final(col)@ == old(col)@.update(i as int, v),
{
    let mut one: Vec<T> = Vec::new();
    one.push(v);
    col.splice(i, 1, one.as_slice());
    assert(col@ =~= old(col)@.update(i as int, v));
}

/// A `u64` up to `i64::MAX` goes at the end of a column of them; a
/// larger one a delta column cannot hold.
#[allow(dead_code)]
fn delta_push_u64(col: &mut VerifiedDeltaColumn<u64>, v: u64)
    requires
        v <= 0x7fff_ffff_ffff_ffff,
    ensures
        final(col)@ == old(col)@.push(v),
{
    let n = col.len();
    let mut one: Vec<u64> = Vec::new();
    one.push(v);
    col.splice(n, 0, one.as_slice());
    assert(col@ =~= old(col)@.push(v));
}

/// A column of `Option<i32>` takes a null, and any `i32`.
#[allow(dead_code)]
fn delta_push_opt(col: &mut VerifiedDeltaColumn<Option<i32>>, v: Option<i32>)
    ensures
        final(col)@ == old(col)@.push(v),
{
    let n = col.len();
    let mut one: Vec<Option<i32>> = Vec::new();
    one.push(v);
    col.splice(n, 0, one.as_slice());
    assert(col@ =~= old(col)@.push(v));
}

/// The values of a delta column, read one at a time into a `Vec`.
#[allow(dead_code)]
fn delta_values<T: VDeltaValue<C>, C: VCodec>(col: &VerifiedDeltaColumn<T, C>) -> (out: Vec<T>)
    ensures
        out@ == col@,
{
    let n = col.len();
    let mut out: Vec<T> = Vec::new();
    let mut i = 0;
    while i < n
        invariant
            i <= n == col@.len(),
            out@ == col@.take(i as int),
        decreases n - i,
    {
        match col.get(i) {
            Some(v) => out.push(v),
            None => {},
        }
        assert(col@.take(i + 1) =~= col@.take(i as int).push(col@[i as int]));
        i += 1;
    }
    assert(col@.take(n as int) =~= col@);
    out
}

/// The default delta column is empty.
#[allow(dead_code)]
fn delta_default<T: VDeltaValue<C>, C: VCodec>() -> (r: VerifiedDeltaColumn<T, C>)
    ensures
        r@.len() == 0,
{
    Default::default()
}

// ── A delta column: its simple edits and reads ──────────────────────────────

/// A value pushed onto a delta column and popped again comes back, and
/// leaves the column's values as they were. The column counts two edits.
#[allow(dead_code)]
fn delta_push_pop<T: VDeltaValue<C>, C: VCodec>(col: &mut VerifiedDeltaColumn<T, C>, v: T) -> (r: Option<T>)
    requires
        v.fits(),
    ensures
        r == Some(v),
        final(col)@ == old(col)@,
        final(col).edits() == after_edits(old(col).edits(), 2),
        final(col).edits() != old(col).edits(),
{
    col.push(v);
    let r = col.pop();
    proof {
        assert(old(col)@.push(v).drop_last() =~= old(col)@);
        assert(col.edits() == after_edits(old(col).edits(), 2)) by {
            reveal_with_fuel(after_edits, 3);
        }
        lemma_after_edits(old(col).edits(), 2);
    }
    r
}

/// A pop of an empty delta column gives nothing and is no edit.
#[allow(dead_code)]
fn delta_pop_empty<T: VDeltaValue<C>, C: VCodec>() -> (r: Option<T>)
    ensures
        r is None,
{
    let mut col = VerifiedDeltaColumn::<T, C>::new();
    let ghost e = col.edits();
    let r = col.pop();
    assert(col.edits() == e);
    r
}

/// A value inserted and then removed where it was put leaves the column
/// as it was, but for the two edits it counts.
#[allow(dead_code)]
fn delta_insert_remove<T: VDeltaValue<C>, C: VCodec>(col: &mut VerifiedDeltaColumn<T, C>, at: usize, v: T)
    requires
        at <= old(col)@.len(),
        v.fits(),
    ensures
        final(col)@ == old(col)@,
        final(col).edits() == after_edits(old(col).edits(), 2),
{
    col.insert(at, v);
    assert(col@[at as int] == v);
    col.remove(at);
    assert(old(col)@.insert(at as int, v).remove(at as int) =~= old(col)@);
    assert(col.edits() == after_edits(old(col).edits(), 2)) by {
        reveal_with_fuel(after_edits, 3);
    }
}

/// A value inserted is read where it was put, and the values after it
/// are read one further on.
#[allow(dead_code)]
fn delta_inserted<T: VDeltaValue<C>, C: VCodec>(col: &mut VerifiedDeltaColumn<T, C>, at: usize, v: T, i: usize) -> (r:
    Option<T>)
    requires
        at <= old(col)@.len(),
        v.fits(),
    ensures
        i == at ==> r == Some(v),
        at < i <= old(col)@.len() ==> r == Some(old(col)@[i - 1]),
        i < at ==> r == Some(old(col)@[i as int]),
        i > old(col)@.len() ==> r is None,
        final(col).edits() == after_edit(old(col).edits()),
{
    col.insert(at, v);
    col.get(i)
}

/// To remove past the end of a delta column does nothing, and is no
/// edit: a suspended reader of the column would still be good.
#[allow(dead_code)]
fn delta_remove_past<T: VDeltaValue<C>, C: VCodec>(col: &mut VerifiedDeltaColumn<T, C>)
    ensures
        final(col)@ == old(col)@,
        final(col).edits() == old(col).edits(),
{
    let n = col.len();
    col.remove(n);
}

/// A delta column truncated is no longer than asked, and keeps its
/// first values.
#[allow(dead_code)]
fn delta_truncated<T: VDeltaValue<C>, C: VCodec>(col: &mut VerifiedDeltaColumn<T, C>, len: usize, i: usize) -> (r:
    Option<T>)
    ensures
        final(col)@.len() <= len,
        final(col)@.len() <= old(col)@.len(),
        i < len && i < old(col)@.len() ==> r == Some(old(col)@[i as int]),
        i >= len ==> r is None,
        len < old(col)@.len() ==> final(col).edits() == after_edit(old(col).edits()),
        len >= old(col)@.len() ==> final(col).edits() == old(col).edits() && final(col)@ == old(col)@,
{
    col.truncate(len);
    col.get(i)
}

/// A range removed from a delta column: what was after it follows what
/// was before it.
#[allow(dead_code)]
fn delta_remove_range<T: VDeltaValue<C>, C: VCodec>(col: &mut VerifiedDeltaColumn<T, C>, at: usize, n: usize)
    requires
        at <= old(col)@.len(),
        n <= old(col)@.len() - at,
    ensures
        final(col)@.len() == old(col)@.len() - n,
        forall|i: int| 0 <= i < at ==> final(col)@[i] == old(col)@[i],
        forall|i: int| at <= i < final(col)@.len() ==> final(col)@[i] == old(col)@[i + n],
        n > 0 ==> final(col).edits() == after_edit(old(col).edits()),
        n == 0 ==> final(col).edits() == old(col).edits(),
{
    col.remove_n(at, n);
}

/// A splice of a delta column that deletes nothing and inserts nothing
/// is no edit, and any other is one: the column no longer counts what it
/// did.
#[allow(dead_code)]
fn delta_splice_counts<T: VDeltaValue<C>, C: VCodec>(
    col: &mut VerifiedDeltaColumn<T, C>,
    at: usize,
    del: usize,
    values: &[T],
)
    requires
        at <= old(col)@.len(),
        del <= old(col)@.len() - at,
        all_fit(values@),
    ensures
        del == 0 && values@.len() == 0 ==> final(col).edits() == old(col).edits(),
        del > 0 || values@.len() > 0 ==> final(col).edits() != old(col).edits(),
{
    col.splice(at, del, values);
}

/// A delta column cleared is empty and can be filled again.
#[allow(dead_code)]
fn delta_cleared<T: VDeltaValue<C>, C: VCodec>(col: &mut VerifiedDeltaColumn<T, C>, values: &[T])
    requires
        all_fit(values@),
    ensures
        final(col)@ == values@,
{
    col.clear();
    col.splice(0, 0, values);
    assert(Seq::<T>::empty().take(0) + values@ + Seq::<T>::empty().skip(0) =~= values@);
}

/// To clear a delta column is an edit of one that has values, and none
/// of an empty one.
#[allow(dead_code)]
fn delta_clear_counts<T: VDeltaValue<C>, C: VCodec>(col: &mut VerifiedDeltaColumn<T, C>)
    ensures
        old(col)@.len() > 0 ==> final(col).edits() != old(col).edits(),
        old(col)@.len() == 0 ==> final(col).edits() == old(col).edits(),
{
    col.clear();
}

/// The first and the last value of a delta column are the ones read at
/// its ends.
#[allow(dead_code)]
fn delta_ends<T: VDeltaValue<C>, C: VCodec>(col: &VerifiedDeltaColumn<T, C>) -> (r: (Option<T>, Option<T>))
    ensures
        col@.len() == 0 ==> r.0 is None && r.1 is None,
        col@.len() > 0 ==> r.0 == Some(col@[0]) && r.1 == Some(col@[col@.len() - 1]),
{
    (col.first(), col.last())
}

/// All the values of a delta column at once are the values read one by
/// one.
#[allow(dead_code)]
fn delta_all<T: VDeltaValue<C>, C: VCodec>(col: &VerifiedDeltaColumn<T, C>, i: usize) -> (r: bool)
    requires
        i < col@.len(),
    ensures
        r,
{
    let all = col.to_vec();
    let one = col.get(i);
    all.len() == col.len() && one.is_some()
}

/// A column built from values gives those values back.
#[allow(dead_code)]
fn delta_round_trip<T: VDeltaValue<C>, C: VCodec>(values: &[T]) -> (r: Vec<T>)
    requires
        all_fit(values@),
    ensures
        r@ == values@,
{
    let col = VerifiedDeltaColumn::<T, C>::from_values(values);
    col.to_vec()
}

/// A clone of a delta column has its values and counts its edits, and
/// an edit of the clone is none of the column.
#[allow(dead_code)]
fn delta_cloned<T: VDeltaValue<C>, C: VCodec>(col: &VerifiedDeltaColumn<T, C>, v: T) -> (r: VerifiedDeltaColumn<T, C>)
    requires
        v.fits(),
    ensures
        r@ == col@.push(v),
        r.edits() == after_edit(col.edits()),
{
    let mut other = col.clone();
    other.push(v);
    other
}

/// A delta column of small slabs holds what one of the usual slabs
/// does.
#[allow(dead_code)]
fn delta_small_slabs<T: VDeltaValue<C>, C: VCodec>(values: &[T]) -> (r: VerifiedDeltaColumn<T, C>)
    requires
        all_fit(values@),
    ensures
        r@ == values@,
{
    let mut col = VerifiedDeltaColumn::<T, C>::with_max_segments(2);
    col.splice(0, 0, values);
    assert(Seq::<T>::empty().take(0) + values@ + Seq::<T>::empty().skip(0) =~= values@);
    col
}

/// The last value of one delta column moved to the end of another: what
/// a column gives is a value a column can hold.
#[allow(dead_code)]
fn delta_move_last<T: VDeltaValue<C>, C: VCodec>(a: &mut VerifiedDeltaColumn<T, C>, b: &mut VerifiedDeltaColumn<T, C>)
    ensures
        old(a)@.len() == 0 ==> final(a)@ == old(a)@ && final(b)@ == old(b)@,
        old(a)@.len() > 0 ==> final(a)@ == old(a)@.drop_last() && final(b)@ == old(b)@.push(old(a)@.last()),
{
    proof {
        a.lemma_fits();
    }
    match a.pop() {
        Some(v) => {
            b.push(v);
        },
        None => {},
    }
}

// ── A delta column: saved and loaded ────────────────────────────────────────

/// A delta column saved: the bytes are a saved delta column of its
/// values, and the one encoding of them. For every type a delta column
/// holds, and every codec.
#[allow(dead_code)]
fn delta_saved<T: VDeltaValue<C>, C: VCodec>(col: &VerifiedDeltaColumn<T, C>) -> (r: Vec<u8>)
    ensures
        VerifiedDeltaColumn::<T, C>::valid(r@),
        VerifiedDeltaColumn::<T, C>::values(r@) == col@,
        r@ == VerifiedDeltaColumn::<T, C>::encoding(col@),
{
    col.save()
}

/// What a saved delta column loads as, where it loads, is the column
/// saved.
#[allow(dead_code)]
fn delta_save_then_load<T: VDeltaValue<C>, C: VCodec>(col: &VerifiedDeltaColumn<T, C>) -> (r: Option<
    VerifiedDeltaColumn<T, C>,
>)
    ensures
        r matches Some(c) ==> c@ == col@,
{
    let bytes = col.save();
    match VerifiedDeltaColumn::<T, C>::load(bytes.as_slice()) {
        Ok(c) => Some(c),
        Err(_) => None,
    }
}

/// What loads as a delta column saves back to the bytes it was loaded
/// from: no two byte strings load as the same delta column. For any
/// bytes, of any provenance.
#[allow(dead_code)]
fn delta_load_then_save<T: VDeltaValue<C>, C: VCodec>(bytes: &[u8]) {
    if let Ok(col) = VerifiedDeltaColumn::<T, C>::load(bytes) {
        proof {
            VerifiedDeltaColumn::<T, C>::lemma_encoding(bytes@);
        }
        let again = col.save();
        assert(again@ == bytes@);
    }
}

/// Two delta columns of the same values save to the same bytes, however
/// each came to hold them; two of different values to different bytes.
#[allow(dead_code)]
fn delta_same_values_same_bytes<T: VDeltaValue<C>, C: VCodec>(
    a: &VerifiedDeltaColumn<T, C>,
    b: &VerifiedDeltaColumn<T, C>,
) -> (r: (Vec<u8>, Vec<u8>))
    ensures
        (a@ == b@) == (r.0@ == r.1@),
{
    (a.save(), b.save())
}

/// A delta column that holds no values saves to no bytes.
#[allow(dead_code)]
fn delta_empty_saves_nothing<T: VDeltaValue<C>, C: VCodec>() -> (r: Vec<u8>)
    ensures
        r@.len() == 0,
{
    let col = VerifiedDeltaColumn::<T, C>::new();
    let r = col.save();
    proof {
        VerifiedDeltaColumn::<T, C>::lemma_no_bytes();
        assert(col@ =~= Seq::<T>::empty());
        VerifiedDeltaColumn::<T, C>::lemma_save_unique(r@, Seq::<u8>::empty());
    }
    r
}

/// And what no bytes load as, where they load, holds no values.
#[allow(dead_code)]
fn delta_load_no_bytes<T: VDeltaValue<C>, C: VCodec>() -> (r: Option<VerifiedDeltaColumn<T, C>>)
    ensures
        r matches Some(c) ==> c@.len() == 0,
{
    let none: Vec<u8> = Vec::new();
    proof {
        VerifiedDeltaColumn::<T, C>::lemma_no_bytes();
    }
    match VerifiedDeltaColumn::<T, C>::load(none.as_slice()) {
        Ok(c) => Some(c),
        Err(_) => None,
    }
}

/// A delta column saved after what a buffer holds: the buffer is what it
/// was and then the column's encoding, and the range returned is where.
#[allow(dead_code)]
fn delta_saved_after<T: VDeltaValue<C>, C: VCodec>(col: &VerifiedDeltaColumn<T, C>, out: &mut Vec<u8>) -> (r: Range<
    usize,
>)
    ensures
        final(out)@.take(old(out)@.len() as int) == old(out)@,
        final(out)@.subrange(r.start as int, r.end as int) == VerifiedDeltaColumn::<T, C>::encoding(col@),
        VerifiedDeltaColumn::<T, C>::valid(final(out)@.subrange(r.start as int, r.end as int)),
        VerifiedDeltaColumn::<T, C>::values(final(out)@.subrange(r.start as int, r.end as int)) == col@,
{
    let r = col.save_to(out);
    proof {
        let e = VerifiedDeltaColumn::<T, C>::encoding(col@);
        assert((old(out)@ + e).take(old(out)@.len() as int) =~= old(out)@);
        assert((old(out)@ + e).subrange(r.start as int, r.end as int) =~= e);
    }
    r
}

/// Two delta columns saved into one buffer, and the second loaded from
/// where its range says it is: where it loads, it is the second.
#[allow(dead_code)]
fn delta_saved_in_one_buffer<T: VDeltaValue<C>, C: VCodec>(
    a: &VerifiedDeltaColumn<T, C>,
    b: &VerifiedDeltaColumn<T, C>,
) -> (r: Option<VerifiedDeltaColumn<T, C>>)
    ensures
        r matches Some(c) ==> c@ == b@,
{
    let mut out: Vec<u8> = Vec::new();
    let _ = a.save_to(&mut out);
    let ghost mid = out@;
    let rb = b.save_to(&mut out);
    let s = out.as_slice();
    let part = vstd::slice::slice_subrange(s, rb.start, rb.end);
    proof {
        let e = VerifiedDeltaColumn::<T, C>::encoding(b@);
        assert((mid + e).subrange(rb.start as int, rb.end as int) =~= e);
    }
    match VerifiedDeltaColumn::<T, C>::load(part) {
        Ok(c) => Some(c),
        Err(_) => None,
    }
}

/// Whether a delta column holds one value only: so it does when empty,
/// whatever the value.
#[allow(dead_code)]
fn delta_only<T: VDeltaValue<C>, C: VCodec>(col: &VerifiedDeltaColumn<T, C>, v: T) -> (r: bool)
    requires
        v.fits(),
    ensures
        r == only(col@, v),
        col@.len() == 0 ==> r,
        r && col@.len() > 0 ==> col@[0] == v && col@.last() == v,
{
    col.is_only(v)
}

/// A column of one value only, and one that is not: what `is_only` says
/// of each, and that a column of the one value alone is not written.
#[allow(dead_code)]
fn delta_one_value_only<T: VDeltaValue<C>, C: VCodec>(a: T, b: T)
    requires
        a.fits(),
        b.fits(),
        a != b,
{
    let mut col = VerifiedDeltaColumn::<T, C>::new();
    // an empty column holds no value that is not `a`
    let none = col.is_only(a);
    assert(none);
    col.push(a);
    col.push(a);
    assert(col@ =~= seq![a, a]);
    let all_a = col.is_only(a);
    let all_b = col.is_only(b);
    assert(all_a);
    assert(!all_b) by {
        assert(col@[0] != b);
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
        assert(col@[2] != a);
    }
    let ghost e = VerifiedDeltaColumn::<T, C>::encoding(col@);
    let r = col.save_to_unless(&mut out, a);
    assert(out@ == seq![7u8] + e);
    assert(r.start == 1 && r.end == 1 + e.len());
    assert(VerifiedDeltaColumn::<T, C>::valid(e));
    assert(VerifiedDeltaColumn::<T, C>::values(e) =~= seq![a, a, b]);
}

/// A column of nulls only is not written where the null is the value
/// left out, and is written where a number is.
#[allow(dead_code)]
fn delta_only_nulls(n: u32, out: &mut Vec<u8>) -> (r: (Range<usize>, Range<usize>))
    ensures
        r.0.start == old(out)@.len(),
        r.0.end == old(out)@.len(),
        final(out)@ == old(out)@ + VerifiedDeltaColumn::<Option<u32>>::encoding(seq![None::<u32>, None::<u32>]),
{
    let nulls: [Option<u32>; 2] = [None, None];
    let col = VerifiedDeltaColumn::<Option<u32>>::from_values(nulls.as_slice());
    assert(col@ =~= seq![None::<u32>, None::<u32>]);
    assert(only(col@, None::<u32>));
    let r0 = col.save_to_unless(out, None);
    assert(!only(col@, Some(n))) by {
        assert(col@[0] != Some(n));
    }
    let r1 = col.save_to_unless(out, Some(n));
    (r0, r1)
}

/// Whatever the bytes, a number read from a delta column they load as
/// is one a delta column holds: of `u64`s, none above `i64::MAX`.
#[allow(dead_code)]
fn delta_loaded_fits(bytes: &[u8], i: usize) -> (r: Option<u64>)
    ensures
        r matches Some(v) ==> v <= 0x7fff_ffff_ffff_ffff,
{
    match VerifiedDeltaColumn::<u64>::load(bytes) {
        Ok(col) => {
            proof {
                col.lemma_fits();
            }
            col.get(i)
        },
        Err(_) => None,
    }
}

/// And so what loads can be edited as any delta column is: a value read
/// from it put back where it was read is no change to its values.
#[allow(dead_code)]
fn delta_loaded_edits<T: VDeltaValue<C>, C: VCodec>(bytes: &[u8]) -> (r: Option<VerifiedDeltaColumn<T, C>>)
    ensures
        r matches Some(c) ==> c@ == VerifiedDeltaColumn::<T, C>::values(bytes@),
{
    match VerifiedDeltaColumn::<T, C>::load(bytes) {
        Ok(mut col) => {
            let ghost was = col@;
            proof {
                col.lemma_fits();
            }
            match col.get(0) {
                Some(v) => {
                    col.remove(0);
                    col.insert(0, v);
                    assert(col@ =~= was);
                },
                None => {},
            }
            Some(col)
        },
        Err(_) => None,
    }
}

// ── Delta columns: runs, and a load with options ────────────────────────────

/// A run put at the end of a column of `u32`: three numbers after 10,
/// each two more than the one before it. And the column counts an edit.
#[allow(dead_code)]
fn delta_run_of_three<C: VCodec>(col: &mut VerifiedDeltaColumn<u32, C>)
    ensures
        final(col)@ == old(col)@ + seq![12u32, 14u32, 16u32],
        final(col).edits() == after_edit(old(col).edits()),
{
    let run = DeltaRun { prefix: 10, delta: Some(2), count: 3 };
    let n = col.len();
    let mut runs: Vec<DeltaRun> = Vec::new();
    runs.push(run);
    proof {
        lemma_delta_run_exact(run, 0);
        lemma_delta_run_exact(run, 1);
        lemma_delta_run_exact(run, 2);
        assert(delta_run_values::<u32>(run) =~= seq![12u32, 14u32, 16u32]);
        lemma_delta_run_fits::<u32>(run);
        lemma_delta_runs_one::<u32>(run);
        assert(runs@ =~= seq![run]);
    }
    col.splice_runs(n, 0, runs.as_slice());
    assert(col@ =~= old(col)@ + seq![12u32, 14u32, 16u32]);
}

/// A run that falls, at the front of a column of `i32`: -5 and then -10.
#[allow(dead_code)]
fn delta_run_falls(col: &mut VerifiedDeltaColumn<i32>)
    ensures
        final(col)@ == seq![-5i32, -10i32] + old(col)@,
{
    let run = DeltaRun { prefix: 0, delta: Some(-5), count: 2 };
    let mut runs: Vec<DeltaRun> = Vec::new();
    runs.push(run);
    proof {
        lemma_delta_run_exact(run, 0);
        lemma_delta_run_exact(run, 1);
        assert(delta_run_values::<i32>(run) =~= seq![-5i32, -10i32]);
        lemma_delta_run_fits::<i32>(run);
        lemma_delta_runs_one::<i32>(run);
        assert(runs@ =~= seq![run]);
    }
    col.splice_runs(0, 0, runs.as_slice());
    assert(col@ =~= seq![-5i32, -10i32] + old(col)@);
}

/// A run checked as it comes, for every type and codec: one that passes
/// the check of the old API goes into the column, and its values are
/// the run's; another is left out, and the column is what it was.
#[allow(dead_code)]
fn delta_run_checked<T: VDeltaValue<C>, C: VCodec>(
    col: &mut VerifiedDeltaColumn<T, C>,
    at: usize,
    run: DeltaRun,
) -> (r: bool)
    requires
        at <= old(col)@.len(),
    ensures
        r == delta_run_in_range::<T>(run),
        r ==> final(col)@ == old(col)@.take(at as int) + delta_run_values::<T>(run) + old(col)@.skip(at as int),
        !r ==> final(col)@ == old(col)@,
{
    if VerifiedDeltaColumn::<T, C>::run_fits(&run) {
        let mut runs: Vec<DeltaRun> = Vec::new();
        runs.push(run);
        proof {
            lemma_delta_runs_one::<T>(run);
            assert(runs@ =~= seq![run]);
        }
        col.splice_runs(at, 0, runs.as_slice());
        true
    } else {
        false
    }
}

/// Two runs put in at once, in place of `del` values: the values of the
/// first and then those of the second.
#[allow(dead_code)]
fn delta_two_runs<T: VDeltaValue<C>, C: VCodec>(
    col: &mut VerifiedDeltaColumn<T, C>,
    at: usize,
    del: usize,
    a: DeltaRun,
    b: DeltaRun,
)
    requires
        at <= old(col)@.len(),
        del <= old(col)@.len() - at,
        delta_run_fits::<T>(a),
        delta_run_fits::<T>(b),
    ensures
        final(col)@ == old(col)@.take(at as int) + delta_run_values::<T>(a) + delta_run_values::<T>(b) + old(
            col,
        )@.skip(at + del),
{
    let mut runs: Vec<DeltaRun> = Vec::new();
    runs.push(a);
    runs.push(b);
    proof {
        lemma_delta_runs_one::<T>(a);
        lemma_delta_runs_push::<T>(seq![a], b);
        assert(runs@ =~= seq![a].push(b));
    }
    col.splice_runs(at, del, runs.as_slice());
    assert(col@ =~= old(col)@.take(at as int) + delta_run_values::<T>(a) + delta_run_values::<T>(b) + old(col)@.skip(
        at + del,
    ));
}

/// No runs, and a run of no values, add nothing: so where nothing is
/// deleted either, the column is what it was and counts no edit. A run
/// of no values passes the check whatever it is of.
#[allow(dead_code)]
fn delta_no_runs<T: VDeltaValue<C>, C: VCodec>(
    col: &mut VerifiedDeltaColumn<T, C>,
    at: usize,
    prefix: i64,
    delta: Option<i64>,
)
    requires
        at <= old(col)@.len(),
    ensures
        final(col)@ == old(col)@,
        final(col).edits() == old(col).edits(),
{
    let run = DeltaRun { prefix, delta, count: 0 };
    let none: Vec<DeltaRun> = Vec::new();
    proof {
        lemma_delta_runs_one::<T>(run);
        assert(none@ =~= Seq::<DeltaRun>::empty());
    }
    col.splice_runs(at, 0, none.as_slice());
    assert(col@ =~= old(col)@);

    let ok = VerifiedDeltaColumn::<T, C>::run_fits(&run);
    assert(ok);
    let mut runs: Vec<DeltaRun> = Vec::new();
    runs.push(run);
    proof {
        assert(runs@ =~= seq![run]);
        assert(delta_run_values::<T>(run) =~= Seq::<T>::empty());
    }
    col.splice_runs(at, 0, runs.as_slice());
    assert(col@ =~= old(col)@);
}

/// A run of nulls at the front of a column of `Option<i64>`: that many
/// nulls, whatever its prefix. A column of `i64` has no null, and the
/// check refuses the run for it.
#[allow(dead_code)]
fn delta_null_run(col: &mut VerifiedDeltaColumn<Option<i64>>, prefix: i64)
    ensures
        final(col)@ == seq![None::<i64>, None] + old(col)@,
{
    let run = DeltaRun { prefix, delta: None, count: 2 };
    let ok = VerifiedDeltaColumn::<Option<i64>>::run_fits(&run);
    assert(ok);
    let no = VerifiedDeltaColumn::<i64>::run_fits(&run);
    assert(!no);
    let mut runs: Vec<DeltaRun> = Vec::new();
    runs.push(run);
    proof {
        lemma_delta_runs_one::<Option<i64>>(run);
        assert(runs@ =~= seq![run]);
        assert(delta_run_values::<Option<i64>>(run) =~= seq![None::<i64>, None]);
    }
    col.splice_runs(0, 0, runs.as_slice());
    assert(col@ =~= seq![None::<i64>, None] + old(col)@);
}

/// A run of a column of `i64` wraps as an `i64` does, as it does in the
/// old API: the number after the greatest is the least. The check passes
/// every run of numbers for such a column.
#[allow(dead_code)]
fn delta_run_wraps(col: &mut VerifiedDeltaColumn<i64>, run: DeltaRun) -> (r: bool)
    ensures
        r == (run.delta is Some || run.count == 0),
        run == (DeltaRun { prefix: i64::MAX, delta: Some(1), count: 1 }) ==> final(col)@ == old(col)@.push(i64::MIN),
        !r ==> final(col)@ == old(col)@,
{
    let n = col.len();
    if VerifiedDeltaColumn::<i64>::run_fits(&run) {
        let mut runs: Vec<DeltaRun> = Vec::new();
        runs.push(run);
        proof {
            lemma_delta_runs_one::<i64>(run);
            assert(runs@ =~= seq![run]);
            if run == (DeltaRun { prefix: i64::MAX, delta: Some(1), count: 1 }) {
                assert(delta_run_values::<i64>(run) =~= seq![i64::MIN]);
            }
        }
        col.splice_runs(n, 0, runs.as_slice());
        proof {
            if run == (DeltaRun { prefix: i64::MAX, delta: Some(1), count: 1 }) {
                assert(col@ =~= old(col)@.push(i64::MIN));
            }
        }
        true
    } else {
        false
    }
}

/// No bytes loaded with a length and a fill, as a column of `u64`: the
/// fill is the difference between neighbours, so with the fill 2 three
/// values are 2, 4 and 6. (That the load succeeds is not said: what it
/// is, where it does.)
#[allow(dead_code)]
fn delta_load_a_fill<C: VCodec>() -> (r: Option<VerifiedDeltaColumn<u64, C>>)
    ensures
        r matches Some(c) ==> c@ == seq![2u64, 4, 6],
{
    let none: Vec<u8> = Vec::new();
    let ghost run = DeltaRun { prefix: 0, delta: Some(2), count: 3 };
    proof {
        lemma_delta_run_exact(run, 0);
        lemma_delta_run_exact(run, 1);
        lemma_delta_run_exact(run, 2);
        assert(delta_run_values::<u64>(run) =~= seq![2u64, 4, 6]);
    }
    match VerifiedDeltaColumn::<u64, C>::load_with(none.as_slice(), LoadOpts::new().with_length(3).with_fill(2i64)) {
        Ok(c) => Some(c),
        Err(_) => None,
    }
}

/// A fill of nulls, for a type that has them: no bytes load as that
/// many nulls.
#[allow(dead_code)]
fn delta_load_null_fill() -> (r: Option<VerifiedDeltaColumn<Option<u32>>>)
    ensures
        r matches Some(c) ==> c@ == seq![None::<u32>, None, None],
{
    let none: Vec<u8> = Vec::new();
    let ghost run = DeltaRun { prefix: 0, delta: None, count: 3 };
    proof {
        assert(delta_run_values::<Option<u32>>(run) =~= seq![None::<u32>, None, None]);
    }
    match VerifiedDeltaColumn::<Option<u32>>::load_with(
        none.as_slice(),
        LoadOpts::new().with_max_segments(4).with_length(3).with_fill(None::<i64>),
    ) {
        Ok(c) => Some(c),
        Err(_) => None,
    }
}

/// Loads with a length, for every type and codec. Without a fill, what
/// loads is what the bytes hold, and has the length. With a fill it has
/// the length too, and is what the bytes hold unless they are empty.
#[allow(dead_code)]
fn delta_load_with_length<T: VDeltaValue<C>, C: VCodec>(bytes: &[u8], len: usize, fill: T::Fill) -> (r: (
    Option<VerifiedDeltaColumn<T, C>>,
    Option<VerifiedDeltaColumn<T, C>>,
))
    ensures
        r.0 matches Some(c) ==> VerifiedDeltaColumn::<T, C>::valid(bytes@) && c@ == VerifiedDeltaColumn::<T, C>::values(
            bytes@,
        ) && c@.len() == len,
        r.1 matches Some(c) ==> c@.len() == len,
        r.1 matches Some(c) ==> bytes@.len() > 0 ==> c@ == VerifiedDeltaColumn::<T, C>::values(bytes@),
{
    let a = match VerifiedDeltaColumn::<T, C>::load_with(bytes, LoadOpts::new().with_length(len)) {
        Ok(c) => Some(c),
        Err(_) => None,
    };
    let b = match VerifiedDeltaColumn::<T, C>::load_with(bytes, LoadOpts::new().with_length(len).with_fill(fill)) {
        Ok(c) => Some(c),
        Err(_) => None,
    };
    (a, b)
}

/// A load with the options a `LoadOpts` starts with says what a load
/// says; and no bytes load, where they do, as the length 0 alone unless
/// there is a fill.
#[allow(dead_code)]
fn delta_load_with_defaults<T: VDeltaValue<C>, C: VCodec>(bytes: &[u8], len: usize) -> (r: Option<
    VerifiedDeltaColumn<T, C>,
>)
    ensures
        r matches Some(c) ==> VerifiedDeltaColumn::<T, C>::valid(bytes@) && c@ == VerifiedDeltaColumn::<T, C>::values(
            bytes@,
        ),
{
    let none: Vec<u8> = Vec::new();
    proof {
        VerifiedDeltaColumn::<T, C>::lemma_no_bytes();
    }
    let r = VerifiedDeltaColumn::<T, C>::load_with(none.as_slice(), LoadOpts::new().with_length(len));
    assert(r is Ok ==> len == 0);
    match VerifiedDeltaColumn::<T, C>::load_with(bytes, LoadOpts::new()) {
        Ok(c) => Some(c),
        Err(_) => None,
    }
}

/// What a fill loads as is the run of it, put into a new column: both
/// columns, where the run passes the check and the fill loads.
#[allow(dead_code)]
fn delta_fill_is_a_run(len: usize, d: i64) -> (r: Option<(VerifiedDeltaColumn<u64>, VerifiedDeltaColumn<u64>)>)
    ensures
        r matches Some((a, b)) ==> a@ == b@ && a@.len() == len,
{
    let none: Vec<u8> = Vec::new();
    let run = DeltaRun { prefix: 0, delta: Some(d), count: len };
    if len as u64 <= i64::MAX as u64 && len <= usize::MAX - 3 && VerifiedDeltaColumn::<u64>::run_fits(&run) {
        match VerifiedDeltaColumn::<u64>::load_with(none.as_slice(), LoadOpts::new().with_length(len).with_fill(d)) {
            Ok(c) => {
                let mut col = VerifiedDeltaColumn::<u64>::new();
                let mut runs: Vec<DeltaRun> = Vec::new();
                runs.push(run);
                proof {
                    lemma_delta_runs_one::<u64>(run);
                    assert(runs@ =~= seq![run]);
                }
                col.splice_runs(0, 0, runs.as_slice());
                assert(col@ =~= c@);
                Some((col, c))
            },
            Err(_) => None,
        }
    } else {
        None
    }
}

/// Any run whose first and last values lie between two `i64`s: its `k`-th
/// value is worked out with no wrapping, and lies between them too.
#[allow(dead_code)]
proof fn delta_run_kth(r: DeltaRun, k: int, lo: int, hi: int)
    requires
        0 <= k < r.count,
        r.delta is Some,
        i64::MIN <= lo <= r.prefix + r.delta->0 <= hi <= i64::MAX,
        lo <= r.prefix + r.delta->0 * r.count <= hi,
    ensures
        delta_run_kept(r)[k] matches Some(x) && x == r.prefix + (k + 1) * r.delta->0 && lo <= x <= hi,
{
    lemma_delta_run_exact(r, k);
}

/// One more run after any runs: its values come after theirs, in what is
/// kept and in what is read, and where they all fit so do the runs
/// together.
#[allow(dead_code)]
proof fn delta_runs_more<T: VDelta>(rs: Seq<DeltaRun>, r: DeltaRun)
    requires
        delta_runs_fit::<T>(rs),
        delta_run_fits::<T>(r),
    ensures
        delta_runs_kept(rs.push(r)) == delta_runs_kept(rs) + delta_run_kept(r),
        delta_runs_values::<T>(rs.push(r)) == delta_runs_values::<T>(rs) + delta_run_values::<T>(r),
        delta_runs_fit::<T>(rs.push(r)),
{
    lemma_delta_runs_push::<T>(rs, r);
}

/// One run alone has its own values and fits as it does; no runs have no
/// values, and fit.
#[allow(dead_code)]
proof fn delta_runs_ends<T: VDelta>(r: DeltaRun)
    ensures
        delta_runs_values::<T>(seq![r]) == delta_run_values::<T>(r),
        delta_runs_values::<T>(Seq::<DeltaRun>::empty()) == Seq::<T>::empty(),
        delta_runs_fit::<T>(seq![r]) == delta_run_fits::<T>(r),
        delta_runs_fit::<T>(Seq::<DeltaRun>::empty()),
{
    lemma_delta_runs_one::<T>(r);
}

// ── Delta columns: the iterator ─────────────────────────────────────────────

/// A `for` loop over a delta column's iterator gives the column's
/// values, in order: here each goes into a `Vec`, which then holds what
/// the column does. For every type a delta column holds, and every codec.
#[allow(dead_code)]
fn delta_read_in_a_loop<T: VDeltaValue<C>, C: VCodec>(col: &VerifiedDeltaColumn<T, C>) -> (out: Vec<T>)
    ensures
        out@ == col@,
{
    let it = col.iter();
    let ghost all = it.rest();
    let mut out: Vec<T> = Vec::new();
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

/// A `for` loop over a delta column itself: whether its numbers are in
/// order, none less than one before it.
#[allow(dead_code)]
fn delta_ascending(col: &VerifiedDeltaColumn<u64>) -> (r: bool)
    ensures
        r == forall|i: int, j: int| 0 <= i <= j < col@.len() ==> col@[i] <= col@[j],
{
    let mut r = true;
    let mut prev: Option<u64> = None;
    for x in at: col
        invariant
            at.seq() == col@,
            prev == if at.index() > 0 {
                Some(col@[at.index() - 1])
            } else {
                None
            },
            r == forall|i: int, j: int| 0 <= i <= j < at.index() ==> col@[i] <= col@[j],
    {
        if let Some(p) = prev {
            if x < p {
                r = false;
            }
        }
        prev = Some(x);
    }
    r
}

/// Two delta columns read side by side, with the standard library's
/// `zip`: whether they hold the same numbers.
#[allow(dead_code)]
fn delta_agree(a: &VerifiedDeltaColumn<i64>, b: &VerifiedDeltaColumn<i64>) -> (r: bool)
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

/// A delta column copied value by value. What its iterator gives is what
/// a delta column takes, since each is a value the column held: for
/// every type and codec.
#[allow(dead_code)]
fn delta_copied_by_iter<T: VDeltaValue<C>, C: VCodec>(col: &VerifiedDeltaColumn<T, C>) -> (r: VerifiedDeltaColumn<T, C>)
    ensures
        r@ == col@,
{
    let mut r: VerifiedDeltaColumn<T, C> = VerifiedDeltaColumn::new();
    proof {
        col.lemma_fits();
        assert(r@ =~= col@.take(0));
    }
    for x in at: col
        invariant
            at.seq() == col@,
            all_fit(col@),
            r@ == col@.take(at.index()),
    {
        r.push(x);
        proof {
            assert(col@.take(at.index() + 1) =~= col@.take(at.index()).push(x));
        }
    }
    proof {
        assert(col@.take(col@.len() as int) =~= col@);
    }
    r
}

/// Steps over the values of a delta column: `nth` gives the value that
/// many places on and leaves the iterator behind it, and an iterator
/// knows how many values it has left.
#[allow(dead_code)]
fn delta_every_other(col: &VerifiedDeltaColumn<i64>)
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

/// The value two places on, if a delta column is that long: for every
/// type and codec.
#[allow(dead_code)]
fn delta_the_third<T: VDeltaValue<C>, C: VCodec>(col: &VerifiedDeltaColumn<T, C>) -> (r: Option<T>)
    ensures
        match r {
            Some(v) => col@.len() > 2 && v == col@[2],
            None => col@.len() <= 2,
        },
{
    let mut it = col.iter();
    it.nth(2)
}

/// A copy of a delta column's iterator goes on from where the iterator
/// is, and each then goes its own way; and an iterator of no column
/// gives nothing. For every type and codec.
#[allow(dead_code)]
fn delta_two_readers<T: VDeltaValue<C>, C: VCodec>(col: &VerifiedDeltaColumn<T, C>)
    requires
        col@.len() == 3,
{
    let mut a = col.iter();
    let first = a.next();
    assert(first == Some(col@[0]));
    let mut b = a.clone();
    let x = a.next();
    let y = b.next();
    assert(x == Some(col@[1]) && y == x);
    let z = a.next();
    assert(z == Some(col@[2]));
    let left_a = a.len();
    let left_b = b.len();
    assert(left_a == 0 && left_b == 1);
    let mut none = VerifiedDeltaIter::<T, C>::default();
    let n = none.next();
    assert(n is None);
}

/// The window of a delta column's iterator moved to a range of the
/// column: a `for` loop then reads the column's values there. For every
/// type and codec.
#[allow(dead_code)]
fn delta_in_the_range<T: VDeltaValue<C>, C: VCodec>(col: &VerifiedDeltaColumn<T, C>, a: usize, b: usize) -> (out: Vec<T>)
    requires
        a <= b <= col@.len(),
    ensures
        out@ == col@.subrange(a as int, b as int),
{
    let mut it = col.iter();
    it.shift(a..b);
    proof {
        assert(window(col@, a as int, b as int) =~= col@.subrange(a as int, b as int));
    }
    let ghost all = it.rest();
    let mut out: Vec<T> = Vec::new();
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

/// Where a delta column's iterator stands and where its window ends: a
/// step moves the one, `set_max` the other, and what is left is the
/// column's values between the two.
#[allow(dead_code)]
fn delta_where_it_stands(col: &VerifiedDeltaColumn<u64>)
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

/// A delta column's iterator read until it gives nothing stands at the
/// end of what it gave, and its window ends there: where the range
/// ended, or the column if that came first. For every type and codec.
#[allow(dead_code)]
fn delta_read_out<T: VDeltaValue<C>, C: VCodec>(col: &VerifiedDeltaColumn<T, C>, a: usize, b: usize) -> (r: (usize, usize, usize))
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
        // the iterator stands in its column
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

/// The values of a delta column at two places, read with one iterator
/// whose window moves from the one to the other, and how many values the
/// second window then has left: for every type and codec.
#[allow(dead_code)]
fn delta_at_two_places<T: VDeltaValue<C>, C: VCodec>(col: &VerifiedDeltaColumn<T, C>, i: usize, j: usize, n: usize) -> (r: (
    T,
    T,
    usize,
))
    requires
        i < j,
        0 < n,
        j + n <= col@.len(),
    ensures
        r.0 == col@[i as int],
        r.1 == col@[j as int],
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

/// How long the window of a delta column's iterator is: that many values
/// it has left, unless the window ends past the column and it has fewer.
#[allow(dead_code)]
fn delta_window_len<T: VDeltaValue<C>, C: VCodec>(it: &VerifiedDeltaIter<'_, T, C>) -> (r: usize)
    ensures
        r >= it.rest().len(),
        it.end_pos() <= it.column().len() ==> r == it.rest().len(),
{
    let (p, e) = (it.pos(), it.end_pos());
    e - p
}

/// Where each move leaves a delta column's iterator and its window, when
/// it moves past what the iterator has, to a window with no value, or
/// not at all; and where a copy, an iterator over a whole column and an
/// iterator of no column stand.
#[allow(dead_code)]
fn delta_moved_past(col: &VerifiedDeltaColumn<u64>)
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
    let mut d = VerifiedDeltaIter::<u64>::default();
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
fn delta_told_by_the_call(col: &VerifiedDeltaColumn<u64>)
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
    let d = VerifiedDeltaIter::<u64>::default();
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

/// What the column an iterator reads says of the iterator's values: an
/// iterator that reads a column has that column's values for its own,
/// and one that reads none has none.
#[allow(dead_code)]
fn delta_told_by_the_source<T: VDeltaValue<C>, C: VCodec>(it: &VerifiedDeltaIter<'_, T, C>, col: &VerifiedDeltaColumn<T, C>)
    ensures
        it.source() == Some(*col) ==> it.column() == col@,
        it.source() is None ==> it.column().len() == 0,
{
    proof {
        it.lemma_source();
    }
}

/// What `iter` says by itself of a delta column: the iterator has all
/// the column's values left, and stands at its start.
#[allow(dead_code)]
fn delta_told_by_iter<'a, T: VDeltaValue<C>, C: VCodec>(col: &'a VerifiedDeltaColumn<T, C>) -> (it: VerifiedDeltaIter<'a, T, C>)
    ensures
        it.rest() == col@,
        it.column() == col@,
        it.source() == Some(*col),
        it.pos() == 0,
        it.end_pos() == col@.len(),
{
    col.iter()
}

/// What a `for` loop over a delta column starts from: the same.
#[allow(dead_code)]
fn delta_told_by_into_iter<'a, T: VDeltaValue<C>, C: VCodec>(col: &'a VerifiedDeltaColumn<T, C>) -> (it: VerifiedDeltaIter<'a, T, C>)
    ensures
        it.rest() == col@,
        it.column() == col@,
        it.source() == Some(*col),
        it.pos() == 0,
        it.end_pos() == col@.len(),
{
    col.into_iter()
}

/// What `iter_range` says by itself: the iterator has the values of the
/// range left, and its window is the range.
#[allow(dead_code)]
fn delta_told_by_iter_range<'a, T: VDeltaValue<C>, C: VCodec>(col: &'a VerifiedDeltaColumn<T, C>, start: usize, end: usize) -> (it:
    VerifiedDeltaIter<'a, T, C>)
    requires
        start <= end <= col@.len(),
    ensures
        it.rest() == col@.subrange(start as int, end as int),
        it.column() == col@,
        it.source() == Some(*col),
        it.pos() == start,
        it.end_pos() == end,
{
    col.iter_range(start, end)
}

/// What `next` says by itself of a delta column's iterator: in the words
/// of the iterator's own specification, and in those of Verus's
/// specification of an iterator.
#[allow(dead_code)]
fn delta_told_by_next<T: VDeltaValue<C>, C: VCodec>(it: &mut VerifiedDeltaIter<'_, T, C>) -> (r: Option<T>)
    ensures
        match r {
            Some(v) => {
                &&& old(it).rest().len() > 0
                &&& v == old(it).rest()[0]
                &&& final(it).rest() == old(it).rest().skip(1)
                &&& final(it).pos() == old(it).pos() + 1
                &&& final(it).end_pos() == old(it).end_pos()
            },
            None => {
                &&& old(it).rest().len() == 0
                &&& final(it).rest().len() == 0
                &&& final(it).pos() == old(it).pos()
                &&& final(it).end_pos() == old(it).pos()
            },
        },
        final(it).column() == old(it).column(),
        final(it).source() == old(it).source(),
        old(it).rest().len() > 0 ==> r == Some(old(it).rest()[0]) && final(it).rest() == old(it).rest().drop_first(),
        old(it).rest().len() == 0 ==> r is None && final(it).rest() == old(it).rest(),
{
    it.next()
}

/// What `nth` says by itself of a delta column's iterator.
#[allow(dead_code)]
fn delta_told_by_nth<T: VDeltaValue<C>, C: VCodec>(it: &mut VerifiedDeltaIter<'_, T, C>, n: usize) -> (r: Option<T>)
    ensures
        n < old(it).rest().len() ==> {
            &&& r == Some(old(it).rest()[n as int])
            &&& final(it).rest() == old(it).rest().skip(n + 1)
            &&& final(it).pos() == old(it).pos() + n + 1
            &&& final(it).end_pos() == old(it).end_pos()
        },
        n >= old(it).rest().len() ==> {
            &&& r is None
            &&& final(it).rest().len() == 0
            &&& final(it).pos() == old(it).pos() + old(it).rest().len()
            &&& final(it).end_pos() == final(it).pos()
        },
        final(it).column() == old(it).column(),
        final(it).source() == old(it).source(),
{
    it.nth(n)
}

/// What the questions put to a delta column's iterator say by
/// themselves. Asked where it stands, it says where: within its window
/// and in its column, with the column's values between there and the
/// window's end left.
#[allow(dead_code)]
fn delta_told_by_an_iters_place<T: VDeltaValue<C>, C: VCodec>(it: &VerifiedDeltaIter<'_, T, C>) -> (r: usize)
    ensures
        r == it.pos(),
        it.pos() <= it.end_pos(),
        it.pos() <= it.column().len(),
        it.rest() == window(it.column(), it.pos() as int, it.end_pos() as int),
{
    it.pos()
}

/// Asked for the window's end alone, an iterator says as much.
#[allow(dead_code)]
fn delta_told_by_an_iters_end<T: VDeltaValue<C>, C: VCodec>(it: &VerifiedDeltaIter<'_, T, C>) -> (r: usize)
    ensures
        r == it.end_pos(),
        it.pos() <= it.end_pos(),
        it.pos() <= it.column().len(),
        it.rest() == window(it.column(), it.pos() as int, it.end_pos() as int),
{
    it.end_pos()
}

/// Asked how many values it has left, an iterator says how many.
#[allow(dead_code)]
fn delta_told_by_an_iters_len<T: VDeltaValue<C>, C: VCodec>(it: &VerifiedDeltaIter<'_, T, C>) -> (r: usize)
    ensures
        r == it.rest().len(),
{
    it.len()
}

/// Asked how many values it may yet give, an iterator says exactly how
/// many it has left, as its least and as its most.
#[allow(dead_code)]
fn delta_told_by_a_size_hint<T: VDeltaValue<C>, C: VCodec>(it: &VerifiedDeltaIter<'_, T, C>) -> (r: (usize, Option<usize>))
    ensures
        r.0 == it.rest().len(),
        r.1 == Some(r.0),
{
    it.size_hint()
}

/// What `set_max` says by itself: the iterator stays, and its window
/// ends where asked, or where the iterator stands if that is further on.
#[allow(dead_code)]
fn delta_told_by_set_max<T: VDeltaValue<C>, C: VCodec>(it: &mut VerifiedDeltaIter<'_, T, C>, end: usize)
    ensures
        final(it).column() == old(it).column(),
        final(it).source() == old(it).source(),
        final(it).pos() == old(it).pos(),
        final(it).end_pos() == if end >= old(it).pos() {
            end
        } else {
            old(it).pos()
        },
        final(it).rest() == window(old(it).column(), old(it).pos() as int, end as int),
{
    it.set_max(end)
}

/// What `advance_to` says by itself: of a place behind the iterator, in
/// what it has left, and past it.
#[allow(dead_code)]
fn delta_told_by_advance_to<T: VDeltaValue<C>, C: VCodec>(it: &mut VerifiedDeltaIter<'_, T, C>, target: usize)
    ensures
        final(it).column() == old(it).column(),
        final(it).source() == old(it).source(),
        final(it).rest() == past(old(it).rest(), target - old(it).pos()),
        target <= old(it).pos() ==> {
            &&& final(it).pos() == old(it).pos()
            &&& final(it).end_pos() == old(it).end_pos()
        },
        old(it).pos() < target <= old(it).pos() + old(it).rest().len() ==> {
            &&& final(it).pos() == target
            &&& final(it).end_pos() == old(it).end_pos()
        },
        target > old(it).pos() + old(it).rest().len() ==> {
            &&& final(it).pos() == old(it).pos() + old(it).rest().len()
            &&& final(it).end_pos() == final(it).pos()
        },
{
    it.advance_to(target)
}

/// What `advance_by` says by itself: of no more values than the iterator
/// has left, and of more.
#[allow(dead_code)]
fn delta_told_by_advance_by<T: VDeltaValue<C>, C: VCodec>(it: &mut VerifiedDeltaIter<'_, T, C>, amount: usize)
    ensures
        final(it).column() == old(it).column(),
        final(it).source() == old(it).source(),
        final(it).rest() == past(old(it).rest(), amount as int),
        amount <= old(it).rest().len() ==> {
            &&& final(it).pos() == old(it).pos() + amount
            &&& final(it).end_pos() == old(it).end_pos()
        },
        amount > old(it).rest().len() ==> {
            &&& final(it).pos() == old(it).pos() + old(it).rest().len()
            &&& final(it).end_pos() == final(it).pos()
        },
{
    it.advance_by(amount)
}

/// What `shift` says by itself: of a range that starts behind the
/// iterator, of one ahead of it in the column, and of one that starts
/// past its own end or the column's.
#[allow(dead_code)]
fn delta_told_by_shift<T: VDeltaValue<C>, C: VCodec>(it: &mut VerifiedDeltaIter<'_, T, C>, range: Range<usize>)
    ensures
        final(it).column() == old(it).column(),
        final(it).source() == old(it).source(),
        final(it).rest() == window(
            old(it).column(),
            if range.start <= old(it).pos() {
                old(it).pos() as int
            } else {
                range.start as int
            },
            range.end as int,
        ),
        range.start <= old(it).pos() ==> {
            &&& final(it).pos() == old(it).pos()
            &&& final(it).end_pos() == if range.end >= old(it).pos() {
                range.end
            } else {
                old(it).pos()
            }
        },
        old(it).pos() < range.start <= range.end && range.start <= old(it).column().len() ==> {
            &&& final(it).pos() == range.start
            &&& final(it).end_pos() == range.end
        },
        old(it).pos() < range.start && (range.start > range.end || range.start > old(it).column().len()) ==> {
            &&& final(it).pos() == if range.end <= old(it).pos() {
                old(it).pos() as int
            } else if range.end <= old(it).column().len() {
                range.end as int
            } else {
                old(it).column().len() as int
            }
            &&& final(it).end_pos() == final(it).pos()
        },
{
    it.shift(range)
}

/// What `shift_next` says by itself: of a window with a value, and of
/// one with none.
#[allow(dead_code)]
fn delta_told_by_shift_next<T: VDeltaValue<C>, C: VCodec>(it: &mut VerifiedDeltaIter<'_, T, C>, range: Range<usize>) -> (r: Option<T>)
    requires
        range.start >= old(it).pos(),
    ensures
        final(it).column() == old(it).column(),
        final(it).source() == old(it).source(),
        ({
            let w = window(old(it).column(), range.start as int, range.end as int);
            &&& w.len() > 0 ==> {
                &&& r == Some(w[0])
                &&& final(it).rest() == w.skip(1)
                &&& final(it).pos() == range.start + 1
                &&& final(it).end_pos() == range.end
            }
            &&& w.len() == 0 ==> {
                &&& r is None
                &&& final(it).rest().len() == 0
                &&& final(it).pos() == if range.end <= old(it).pos() {
                    old(it).pos() as int
                } else if range.end <= old(it).column().len() {
                    range.end as int
                } else {
                    old(it).column().len() as int
                }
                &&& final(it).end_pos() == final(it).pos()
            }
        }),
{
    it.shift_next(range)
}

/// What `clone` says by itself of a delta column's iterator: the copy
/// reads the same column, stands at the same place, and has the same
/// values left.
#[allow(dead_code)]
fn delta_told_by_an_iters_clone<'a, T: VDeltaValue<C>, C: VCodec>(it: &VerifiedDeltaIter<'a, T, C>) -> (r: VerifiedDeltaIter<'a, T, C>)
    ensures
        r.rest() == it.rest(),
        r.column() == it.column(),
        r.source() == it.source(),
        r.pos() == it.pos(),
        r.end_pos() == it.end_pos(),
{
    it.clone()
}

/// What `default` says by itself: an iterator of no column, with nothing
/// to give.
#[allow(dead_code)]
fn delta_told_by_an_iters_default<'a, T: VDeltaValue<C>, C: VCodec>() -> (r: VerifiedDeltaIter<'a, T, C>)
    ensures
        r.rest().len() == 0,
        r.column().len() == 0,
        r.source() is None,
        r.pos() == 0,
        r.end_pos() == 0,
{
    VerifiedDeltaIter::default()
}

// ── Delta columns: runs of differences, and a suspended iterator ────────────

/// A delta column's values as the runs it keeps them in: a `for` loop
/// over the runs of its iterator collects them. They are the runs of
/// the column's differences; their values are the column's; and each is
/// a run the column's type can keep. For every type and codec.
#[allow(dead_code)]
fn delta_in_runs<T: VDeltaValue<C>, C: VCodec>(col: &VerifiedDeltaColumn<T, C>) -> (out: Vec<DeltaRun>)
    ensures
        out@ == delta_runs_at(col@, 0, runs_of(deltas(col@))),
        delta_runs_values::<T>(out@) == col@,
        delta_runs_fit::<T>(out@),
{
    let n = col.len();
    let ghost rs = delta_runs_at(col@, 0, runs_of(window(deltas(col@), 0, n as int)));
    let mut out: Vec<DeltaRun> = Vec::new();
    for run in at: col.iter().runs()
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
        col.lemma_fits();
        lemma_delta_runs_at::<T>(col@, 0, n as int);
        assert(window(col@, 0, n as int) =~= col@);
        assert(window(deltas(col@), 0, n as int) =~= deltas(col@));
    }
    out
}

/// A delta column copied by its runs: what `runs()` gives is what
/// `splice_runs` takes, and the copy holds the column's values.
#[allow(dead_code)]
fn delta_copied_by_runs<T: VDeltaValue<C>, C: VCodec>(col: &VerifiedDeltaColumn<T, C>) -> (r: VerifiedDeltaColumn<T, C>)
    ensures
        r@ == col@,
{
    let runs = delta_in_runs(col);
    let mut r: VerifiedDeltaColumn<T, C> = VerifiedDeltaColumn::new();
    r.splice_runs(0, 0, runs.as_slice());
    assert(r@ =~= col@);
    r
}

/// The same copy made a run at a time, with `next_run`: each run goes
/// in at the copy's end as the iterator gives it. A run holds the
/// values the iterator moved past, it is one the type can keep, and it
/// is at least one value long, so the loop ends.
#[allow(dead_code)]
fn delta_copied_run_by_run<T: VDeltaValue<C>, C: VCodec>(col: &VerifiedDeltaColumn<T, C>) -> (r: VerifiedDeltaColumn<
    T,
    C,
>)
    ensures
        r@ == col@,
{
    let mut r: VerifiedDeltaColumn<T, C> = VerifiedDeltaColumn::new();
    let mut it = col.iter();
    assert(r@ + it.rest() =~= col@);
    loop
        invariant
            r@ + it.rest() == col@,
        ensures
            r@ == col@,
        decreases it.rest().len(),
    {
        let ghost left = it.rest();
        let ghost (c, p, e) = (it.column(), it.pos() as int, it.end_pos() as int);
        match it.next_run() {
            Some(run) => {
                let n = r.len();
                let ghost before = r@;
                let mut one: Vec<DeltaRun> = Vec::new();
                one.push(run);
                proof {
                    lemma_delta_runs_one::<T>(run);
                    assert(one@ =~= seq![run]);
                    // the run is no longer than what was left
                    assert(window(deltas(c), p, e).len() == left.len());
                }
                r.splice_runs(n, 0, one.as_slice());
                proof {
                    assert(r@ =~= before + left.take(run.count as int));
                    assert(left.take(run.count as int) + left.skip(run.count as int) =~= left);
                    assert(r@ + it.rest() =~= col@);
                }
            },
            None => {
                assert(r@ + it.rest() =~= r@);
                break ;
            },
        }
    }
    r
}

/// The values of a column from `a` to `b` put into another by their
/// runs, at index `at` of it: the runs of a part of one column are runs
/// of any column of the type, since a run says what its values are and
/// not where they came from.
#[allow(dead_code)]
fn delta_moved_by_runs<T: VDeltaValue<C>, C: VCodec>(
    src: &VerifiedDeltaColumn<T, C>,
    a: usize,
    b: usize,
    dst: &mut VerifiedDeltaColumn<T, C>,
    at: usize,
)
    requires
        a <= b <= src@.len(),
        at <= old(dst)@.len(),
    ensures
        final(dst)@ == old(dst)@.take(at as int) + src@.subrange(a as int, b as int) + old(dst)@.skip(at as int),
{
    let _n = src.len();
    let ghost rs = delta_runs_at(src@, a as int, runs_of(window(deltas(src@), a as int, b as int)));
    let mut runs: Vec<DeltaRun> = Vec::new();
    for run in at: src.iter_range(a, b).runs()
        invariant
            at.seq() == rs,
            runs@ == rs.take(at.index()),
    {
        runs.push(run);
        proof {
            assert(rs.take(at.index() + 1) =~= rs.take(at.index()).push(run));
        }
    }
    proof {
        assert(rs.take(rs.len() as int) =~= rs);
        src.lemma_fits();
        lemma_delta_runs_at::<T>(src@, a as int, b as int);
        assert(window(src@, a as int, b as int) =~= src@.subrange(a as int, b as int));
    }
    dst.splice_runs(at, 0, runs.as_slice());
}

/// The differences that the values 5, 7, 9 and 9 are kept as: the first
/// from 0, each of the others from the value before it. And the number
/// before each, which a run that begins there has for its `prefix`.
#[allow(dead_code)]
proof fn delta_diffs_of_four()
    ensures
        deltas(seq![5u64, 7u64, 9u64, 9u64]) == seq![Some(5i64), Some(2i64), Some(2i64), Some(0i64)],
        delta_prefix(seq![5u64, 7u64, 9u64, 9u64], 0) == 0,
        delta_prefix(seq![5u64, 7u64, 9u64, 9u64], 1) == 5,
        delta_prefix(seq![5u64, 7u64, 9u64, 9u64], 3) == 9,
        delta_prefix(seq![5u64, 7u64, 9u64, 9u64], 4) == 9,
{
    let vs = seq![5u64, 7u64, 9u64, 9u64];
    assert(delta_prefix(vs, 0) == 0);
    assert(delta_prefix(vs, 1) == 5);
    assert(delta_prefix(vs, 2) == 7);
    assert(delta_prefix(vs, 3) == 9);
    assert(delta_prefix(vs, 4) == 9);
    assert(delta_at(vs, 0) == Some(5i64));
    assert(delta_at(vs, 1) == Some(2i64));
    assert(delta_at(vs, 2) == Some(2i64));
    assert(delta_at(vs, 3) == Some(0i64));
    assert(deltas(vs) =~= seq![Some(5i64), Some(2i64), Some(2i64), Some(0i64)]);
}

/// The differences where a value is null: a null has none, and the
/// number before the value after it is the last number before the null.
/// A difference may be less than nothing.
#[allow(dead_code)]
proof fn delta_diffs_with_a_null()
    ensures
        deltas(seq![Some(3u32), None, Some(1u32)]) == seq![Some(3i64), None, Some(-2i64)],
        delta_prefix(seq![Some(3u32), None, Some(1u32)], 2) == 3,
        delta_prefix(seq![Some(3u32), None, Some(1u32)], 3) == 1,
{
    let vs = seq![Some(3u32), None, Some(1u32)];
    assert(delta_prefix(vs, 0) == 0);
    assert(delta_prefix(vs, 1) == 3);
    assert(delta_prefix(vs, 2) == 3);
    assert(delta_prefix(vs, 3) == 1);
    assert(delta_at(vs, 0) == Some(3i64));
    assert(delta_at(vs, 1) is None);
    assert(delta_at(vs, 2) == Some(-2i64));
    assert(deltas(vs) =~= seq![Some(3i64), None, Some(-2i64)]);
}

/// The runs of a column of 5, 7, 9 and 9, as `next_run` gives them: 5
/// alone, from 0; then two values each 2 more than the one before, from
/// 5; then one value the same as the one before it, 9; and then none.
#[allow(dead_code)]
fn delta_runs_of_four(col: &VerifiedDeltaColumn<u64>)
    requires
        col@ == seq![5u64, 7u64, 9u64, 9u64],
{
    proof {
        delta_diffs_of_four();
    }
    let ghost ds = deltas(col@);
    let mut it = col.iter();
    let a = it.next_run();
    assert(a == Some(DeltaRun { prefix: 0, delta: Some(5i64), count: 1 })) by {
        let w = window(ds, 0, 4);
        assert(w =~= ds);
        assert(a is Some);
        if a->0.count > 1 {
            assert(w[1] == a->0.delta);
        }
    }
    let b = it.next_run();
    assert(b == Some(DeltaRun { prefix: 5, delta: Some(2i64), count: 2 })) by {
        let w = window(ds, 1, 4);
        assert(w =~= ds.subrange(1, 4));
        assert(b is Some);
        assert(w[0] == b->0.delta);
        if b->0.count > 2 {
            assert(w[2] == b->0.delta);
        }
        if b->0.count < 2 {
            assert(w[1] != b->0.delta);
        }
    }
    let c = it.next_run();
    assert(c == Some(DeltaRun { prefix: 9, delta: Some(0i64), count: 1 })) by {
        let w = window(ds, 3, 4);
        assert(w =~= ds.subrange(3, 4));
        assert(c is Some);
        assert(w[0] == c->0.delta);
    }
    let d = it.next_run();
    assert(d is None);
    assert(it.pos() == 4 && it.rest().len() == 0);
}

/// Nulls lie in a run of their own, which has no difference, and the
/// run after it counts from the last number before them: here from
/// none, so from 0.
#[allow(dead_code)]
fn delta_nulls_in_a_run(col: &VerifiedDeltaColumn<Option<u32>>)
    requires
        col@ == seq![None, None, Some(3u32)],
{
    let ghost ds = deltas(col@);
    proof {
        assert(delta_prefix(col@, 0) == 0);
        assert(delta_prefix(col@, 1) == 0);
        assert(delta_prefix(col@, 2) == 0);
        assert(delta_at(col@, 0) is None);
        assert(delta_at(col@, 1) is None);
        assert(delta_at(col@, 2) == Some(3i64));
        assert(ds =~= seq![None, None, Some(3i64)]);
    }
    let mut it = col.iter();
    let a = it.next_run();
    assert(a == Some(DeltaRun { prefix: 0, delta: None, count: 2 })) by {
        let w = window(ds, 0, 3);
        assert(w =~= ds);
        assert(a is Some);
        assert(w[0] == a->0.delta);
        if a->0.count > 2 {
            assert(w[2] == a->0.delta);
        }
        if a->0.count < 2 {
            assert(w[1] != a->0.delta);
        }
    }
    let b = it.next_run();
    assert(b == Some(DeltaRun { prefix: 0, delta: Some(3i64), count: 1 })) by {
        let w = window(ds, 2, 3);
        assert(w =~= ds.subrange(2, 3));
        assert(b is Some);
        assert(w[0] == b->0.delta);
    }
}

/// The number before a run is the value before it: a run read from the
/// `i`-th value on of a column of `i64` has the value before that for
/// its `prefix`, or 0 where there is none; and its `delta` is how much
/// the `i`-th value is more, as an `i64`.
#[allow(dead_code)]
fn delta_the_number_before(col: &VerifiedDeltaColumn<i64>, i: usize) -> (r: Option<DeltaRun>)
    requires
        i < col@.len(),
    ensures
        r matches Some(run) && run.prefix == (if i == 0 {
            0
        } else {
            col@[i - 1]
        }) && run.delta == Some(vstd::wrapping::i64_specs::signed_crop(col@[i as int] - run.prefix)),
{
    let n = col.len();
    let mut it = col.iter_range(i, n);
    let r = it.next_run();
    proof {
        let w = window(deltas(col@), i as int, n as int);
        assert(delta_prefix(col@, i as int) == (if i == 0 {
            0
        } else {
            col@[i - 1]
        }));
        assert(r is Some);
        assert(w[0] == r->0.delta);
        assert(w[0] == delta_at(col@, i as int));
    }
    r
}

/// No two runs that follow one another are of one difference: a run
/// ends where the difference changes or the window does. And the second
/// run's number before it is the one at the place where the first
/// ended.
#[allow(dead_code)]
fn delta_two_runs_differ<T: VDeltaValue<C>, C: VCodec>(it: &mut VerifiedDeltaIter<'_, T, C>) -> (r: (
    Option<DeltaRun>,
    Option<DeltaRun>,
))
    ensures
        r.0 is Some && r.1 is Some ==> r.0->0.delta != r.1->0.delta && r.1->0.prefix == delta_prefix(
            old(it).column(),
            old(it).pos() + r.0->0.count,
        ),
        r.0 is None ==> r.1 is None,
{
    let ghost (c, p, e) = (it.column(), it.pos() as int, it.end_pos() as int);
    let a = it.next_run();
    let b = it.next_run();
    proof {
        if a is Some && b is Some {
            let n = a->0.count as int;
            let w = window(deltas(c), p, e);
            let w2 = window(deltas(c), p + n, e);
            assert(w2[0] == b->0.delta);
            assert(w2[0] == w[n]);
        }
    }
    (a, b)
}

/// `lemma_delta_run_at` for any values and any place: where `n` values
/// from index `a` on each differ by `d` from the one before, the run of
/// `n` differences `d` with the number before index `a` has those
/// values, and is one the type can keep.
#[allow(dead_code)]
proof fn delta_a_run_between<T: VDelta>(vs: Seq<T>, a: int, n: usize, d: Option<i64>)
    requires
        all_fit(vs),
        0 <= a,
        a + n <= vs.len(),
        forall|j: int| a <= j < a + n ==> delta_at(vs, j) == d,
    ensures
        delta_run_values::<T>(DeltaRun { prefix: delta_prefix(vs, a), delta: d, count: n }) == vs.subrange(a, a + n),
        delta_run_fits::<T>(DeltaRun { prefix: delta_prefix(vs, a), delta: d, count: n }),
{
    lemma_delta_run_at::<T>(vs, a, n, d);
}

/// `lemma_delta_runs_at` for any values and any part of them: the runs
/// of the differences from `a` to `b`, each with the number before it,
/// have the values from `a` to `b`, and the type can keep them.
#[allow(dead_code)]
proof fn delta_runs_between<T: VDelta>(vs: Seq<T>, a: int, b: int)
    requires
        all_fit(vs),
        0 <= a <= b <= vs.len(),
        vs.len() <= usize::MAX,
    ensures
        delta_runs_values::<T>(delta_runs_at(vs, a, runs_of(deltas(vs).subrange(a, b)))) == vs.subrange(a, b),
        delta_runs_fit::<T>(delta_runs_at(vs, a, runs_of(deltas(vs).subrange(a, b)))),
{
    lemma_delta_runs_at::<T>(vs, a, b);
    assert(window(vs, a, b) =~= vs.subrange(a, b));
    assert(window(deltas(vs), a, b) =~= deltas(vs).subrange(a, b));
}

/// The runs of some differences, with the numbers before them: none of
/// none, and of one run and more that run, with the number before index
/// `a`, and then the others from where it ends.
#[allow(dead_code)]
proof fn delta_runs_from<T: VDelta>(vs: Seq<T>, a: int, r: Run<Option<i64>>, rs: Seq<Run<Option<i64>>>)
    ensures
        delta_runs_at(vs, a, Seq::empty()) == Seq::<DeltaRun>::empty(),
        delta_runs_at(vs, a, seq![r] + rs) == seq![
            DeltaRun { prefix: delta_prefix(vs, a), delta: r.value, count: r.count },
        ] + delta_runs_at(vs, a + r.count, rs),
{
    let all = seq![r] + rs;
    assert(all[0] == r);
    assert(all.drop_first() =~= rs);
}

/// A delta column read in two goes: up to `k` values, then the iterator
/// is put away as a state, which borrows nothing, and an iterator
/// resumed from the state gives the rest. Nothing edits the column
/// between, so the state is one the column takes.
#[allow(dead_code)]
fn delta_read_in_two_goes<T: VDeltaValue<C>, C: VCodec>(col: &VerifiedDeltaColumn<T, C>, k: usize) -> (out: Vec<T>)
    ensures
        out@ == col@,
{
    let mut out: Vec<T> = Vec::new();
    let mut it = col.iter();
    let mut i: usize = 0;
    assert(out@ + it.rest() =~= col@);
    while i < k
        invariant
            out@ + it.rest() == col@,
            it.source() == Some(*col),
            it.column() == col@,
        decreases k - i,
    {
        let ghost (had, left) = (out@, it.rest());
        match it.next() {
            Some(x) => {
                out.push(x);
                assert(out@ + it.rest() =~= had + left);
            },
            None => {},
        }
        i += 1;
    }
    let s = it.suspend();
    let ghost had = out@;
    let ghost left = it.rest();
    match s.try_resume(col) {
        Ok(rest) => {
            for x in at: rest
                invariant
                    at.seq() == left,
                    out@ == had + left.take(at.index()),
            {
                out.push(x);
                proof {
                    assert(left.take(at.index() + 1) =~= left.take(at.index()).push(x));
                }
            }
            proof {
                assert(left.take(left.len() as int) =~= left);
            }
        },
        Err(_) => {
            assert(false);
        },
    }
    out
}

/// An iterator of a part of a delta column, suspended and resumed on
/// the column: the one resumed stands where the suspended one stood,
/// reads the same column, and has the values left that it had.
#[allow(dead_code)]
fn delta_resumed<'a, T: VDeltaValue<C>, C: VCodec>(col: &'a VerifiedDeltaColumn<T, C>, a: usize, b: usize) -> (r:
    Option<VerifiedDeltaIter<'a, T, C>>)
    requires
        a <= b <= col@.len(),
    ensures
        r matches Some(it) && it.pos() == a && it.end_pos() == b && it.rest() == col@.subrange(a as int, b as int)
            && it.source() == Some(*col),
{
    let it = col.iter_range(a, b);
    let s = it.suspend();
    match s.try_resume(col) {
        Ok(r) => {
            assert(r.rest() =~= col@.subrange(a as int, b as int));
            Some(r)
        },
        Err(_) => None,
    }
}

/// A state is refused once the column is edited: the column counts an
/// edit more, and a state fits only a column that counts the edits it
/// does (`lemma_fits`).
#[allow(dead_code)]
fn delta_ended_by_an_edit<T: VDeltaValue<C>, C: VCodec>(col: &mut VerifiedDeltaColumn<T, C>, v: T) -> (r: bool)
    requires
        v.fits(),
    ensures
        !r,
        final(col)@ == old(col)@.push(v),
{
    let s = col.iter().suspend();
    proof {
        s.lemma_fits(&*col);
    }
    col.push(v);
    s.try_resume(col).is_ok()
}

/// And after more edits than one: here after `k` pushes, for any `k`
/// but 0. The column's count after `k` edits, for any `k` a `usize`
/// holds, is not what it was (`lemma_after_edits`).
#[allow(dead_code)]
fn delta_ended_by_many<T: VDeltaValue<C>, C: VCodec>(col: &mut VerifiedDeltaColumn<T, C>, v: T, k: usize) -> (r: bool)
    requires
        v.fits(),
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
            v.fits(),
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

/// A copy of a column counts the edits the column does, and each counts
/// its own from there: after an edit of the copy, the copy refuses a
/// state of the column, and the column, which nothing has edited, gives
/// an iterator for it still.
#[allow(dead_code)]
fn delta_a_copy_edited<T: VDeltaValue<C>, C: VCodec>(col: &VerifiedDeltaColumn<T, C>, v: T) -> (r: (bool, bool))
    requires
        v.fits(),
    ensures
        !r.0,
        r.1,
{
    let s = col.iter().suspend();
    proof {
        s.lemma_fits(col);
    }
    let mut copy = col.clone();
    copy.push(v);
    let of_copy = s.try_resume(&copy).is_ok();
    let of_col = s.try_resume(col).is_ok();
    (of_copy, of_col)
}

/// A state resumed on another column than the one it was taken of,
/// where that one takes it: the iterator is the other column's, and
/// gives that column's values in the state's window. For every type and
/// codec.
#[allow(dead_code)]
fn delta_resumed_elsewhere<T: VDeltaValue<C>, C: VCodec>(
    s: &VerifiedDeltaIterState,
    other: &VerifiedDeltaColumn<T, C>,
) -> (r: Option<Vec<T>>)
    ensures
        r is Some <==> s.fits(other),
        r matches Some(v) ==> v@ == window(other@, s.pos() as int, s.end_pos() as int),
{
    match s.try_resume(other) {
        Ok(it) => {
            let ghost all = it.rest();
            let mut out: Vec<T> = Vec::new();
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
            Some(out)
        },
        Err(_) => None,
    }
}

/// An iterator that has given all its window, suspended and resumed:
/// the one resumed has none left either, and its window ends where it
/// stands.
#[allow(dead_code)]
fn delta_resumed_at_the_end<T: VDeltaValue<C>, C: VCodec>(col: &VerifiedDeltaColumn<T, C>, a: usize, b: usize)
    requires
        a <= b <= col@.len(),
{
    let mut it = col.iter_range(a, b);
    it.advance_to(b);
    assert(it.pos() == b && it.rest().len() == 0);
    let s = it.suspend();
    match s.try_resume(col) {
        Ok(r) => {
            assert(r.pos() == b && r.end_pos() == b);
            assert(r.rest().len() == 0);
        },
        Err(_) => {
            assert(false);
        },
    }
}

/// What `next_run` says by itself of a delta column's iterator, each
/// thing asked for on its own and with no lemma.
#[allow(dead_code)]
fn delta_told_by_next_run<T: VDeltaValue<C>, C: VCodec>(it: &mut VerifiedDeltaIter<'_, T, C>) -> (r: Option<DeltaRun>)
    ensures
        final(it).column() == old(it).column(),
        final(it).source() == old(it).source(),
        r matches Some(run) ==> head_run(
            window(deltas(old(it).column()), old(it).pos() as int, old(it).end_pos() as int),
            run.delta,
            run.count as int,
        ),
        r matches Some(run) ==> run.prefix == delta_prefix(old(it).column(), old(it).pos() as int),
        r matches Some(run) ==> delta_run_values::<T>(run) == old(it).rest().take(run.count as int),
        r matches Some(run) ==> delta_run_fits::<T>(run),
        r matches Some(run) ==> final(it).rest() == old(it).rest().skip(run.count as int),
        r matches Some(run) ==> final(it).pos() == old(it).pos() + run.count,
        r matches Some(run) ==> final(it).end_pos() == old(it).end_pos(),
        r is None ==> old(it).rest().len() == 0,
        r is None ==> final(it).rest().len() == 0,
        r is None ==> final(it).pos() == old(it).pos(),
        r is None ==> final(it).end_pos() == old(it).pos(),
{
    it.next_run()
}

/// What `runs` says by itself: the iterator of runs is of the iterator
/// it was made of, as that stood.
#[allow(dead_code)]
fn delta_told_by_runs<'a, T: VDeltaValue<C>, C: VCodec>(it: VerifiedDeltaIter<'a, T, C>) -> (r: VerifiedDeltaRuns<
    'a,
    T,
    C,
>)
    ensures
        r.iter() == it,
{
    it.runs()
}

/// What `next` says by itself of an iterator of a delta column's runs:
/// what `next_run` says of the iterator of values it is of.
#[allow(dead_code)]
fn delta_told_by_a_runs_next<T: VDeltaValue<C>, C: VCodec>(rs: &mut VerifiedDeltaRuns<'_, T, C>) -> (r: Option<
    DeltaRun,
>)
    ensures
        final(rs).iter().column() == old(rs).iter().column(),
        final(rs).iter().source() == old(rs).iter().source(),
        r matches Some(run) ==> head_run(
            window(deltas(old(rs).iter().column()), old(rs).iter().pos() as int, old(rs).iter().end_pos() as int),
            run.delta,
            run.count as int,
        ),
        r matches Some(run) ==> run.prefix == delta_prefix(old(rs).iter().column(), old(rs).iter().pos() as int),
        r matches Some(run) ==> delta_run_values::<T>(run) == old(rs).iter().rest().take(run.count as int),
        r matches Some(run) ==> delta_run_fits::<T>(run),
        r matches Some(run) ==> final(rs).iter().rest() == old(rs).iter().rest().skip(run.count as int),
        r matches Some(run) ==> final(rs).iter().pos() == old(rs).iter().pos() + run.count,
        r matches Some(run) ==> final(rs).iter().end_pos() == old(rs).iter().end_pos(),
        r is None ==> old(rs).iter().rest().len() == 0,
        r is None ==> final(rs).iter().rest().len() == 0,
        r is None ==> final(rs).iter().pos() == old(rs).iter().pos(),
        r is None ==> final(rs).iter().end_pos() == old(rs).iter().pos(),
{
    rs.next()
}

/// What `suspend` says by itself: the state has the iterator's place
/// and its window's end, the iterator's values left are its column's
/// between the two, and the state fits the column the iterator reads.
#[allow(dead_code)]
fn delta_told_by_suspend<T: VDeltaValue<C>, C: VCodec>(
    it: &VerifiedDeltaIter<'_, T, C>,
    col: &VerifiedDeltaColumn<T, C>,
) -> (s: VerifiedDeltaIterState)
    ensures
        s.pos() == it.pos(),
        s.end_pos() == it.end_pos(),
        it.rest() == window(it.column(), s.pos() as int, s.end_pos() as int),
        it.source() == Some(*col) ==> s.fits(col),
{
    it.suspend()
}

/// What `try_resume` says by itself: it gives an iterator exactly where
/// the state fits the column, and then one of that column, at the
/// state's place, with that column's values in the state's window left;
/// its window ends where the state's does, or, at the column's end,
/// where it stands.
#[allow(dead_code)]
fn delta_told_by_try_resume<'a, T: VDeltaValue<C>, C: VCodec>(
    s: &VerifiedDeltaIterState,
    col: &'a VerifiedDeltaColumn<T, C>,
) -> (r: Result<VerifiedDeltaIter<'a, T, C>, PackError>)
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

/// What `lemma_fits` says by itself of a state: one that fits a column
/// counts the edits the column does.
#[allow(dead_code)]
proof fn delta_told_by_a_states_fit<T: VDeltaValue<C>, C: VCodec>(
    s: &VerifiedDeltaIterState,
    col: &VerifiedDeltaColumn<T, C>,
)
    requires
        s.fits(col),
    ensures
        s.edits() == col.edits(),
{
    s.lemma_fits(col);
}

// ── Delta columns: values found by a scan, and by a search of sorted ones ───

/// The first place of `x` among the values `a..b` of a delta column,
/// said of the column's values alone: a scan of an iterator over that
/// window. For every type and codec.
#[allow(dead_code)]
fn delta_first_place_in<T: VDeltaValue<C>, C: VCodec>(col: &VerifiedDeltaColumn<T, C>, x: T, a: usize, b: usize) -> (r:
    Option<usize>)
    requires
        a <= b <= col@.len(),
    ensures
        match r {
            Some(k) => a <= k < b && col@[k as int] == x && forall|i: int| a <= i < k ==> col@[i] != x,
            None => forall|i: int| a <= i < b ==> col@[i] != x,
        },
{
    let mut it = col.iter_range(a, b);
    let ghost o = it.rest();
    let r = it.scan_to_value(x);
    proof {
        match r {
            Some(k) => {
                assert(col@[k as int] == o[k - a]);
                assert forall|i: int| a <= i < k implies col@[i] != x by {
                    assert(col@[i] == o[i - a]);
                }
            },
            None => {
                assert forall|i: int| a <= i < b implies col@[i] != x by {
                    assert(col@[i] == o[i - a]);
                }
            },
        }
    }
    r
}

/// Every place of `x` in a delta column, by scans that each go on from
/// where the last one stopped: the places come in rising order, each
/// holds `x`, and no other place does. For every type and codec.
#[allow(dead_code)]
fn delta_all_places<T: VDeltaValue<C>, C: VCodec>(col: &VerifiedDeltaColumn<T, C>, x: T) -> (out: Vec<usize>)
    ensures
        forall|i: int, j: int| 0 <= i < j < out@.len() ==> out@[i] < out@[j],
        forall|i: int| 0 <= i < out@.len() ==> out@[i] < col@.len() && col@[out@[i] as int] == x,
        forall|k: int| 0 <= k < col@.len() && col@[k] == x ==> exists|i: int| 0 <= i < out@.len() && out@[i] == k,
{
    let mut out: Vec<usize> = Vec::new();
    let mut it = col.iter();
    assert(it.rest() =~= col@.skip(0));
    loop
        invariant
            it.column() == col@,
            it.pos() <= col@.len(),
            it.rest() == col@.skip(it.pos() as int),
            forall|i: int, j: int| 0 <= i < j < out@.len() ==> out@[i] < out@[j],
            forall|i: int| 0 <= i < out@.len() ==> out@[i] < it.pos() && col@[out@[i] as int] == x,
            forall|k: int| 0 <= k < it.pos() && col@[k] == x ==> exists|i: int| 0 <= i < out@.len() && out@[i] == k,
        ensures
            forall|i: int, j: int| 0 <= i < j < out@.len() ==> out@[i] < out@[j],
            forall|i: int| 0 <= i < out@.len() ==> out@[i] < col@.len() && col@[out@[i] as int] == x,
            forall|k: int|
                0 <= k < col@.len() && col@[k] == x ==> exists|i: int| 0 <= i < out@.len() && out@[i] == k,
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
                    assert forall|j: int| 0 <= j < it.pos() && col@[j] == x implies exists|i: int|
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
                    assert forall|j: int| 0 <= j < col@.len() && col@[j] == x implies exists|i: int|
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

/// A number too large for a delta column is at no place of one: a scan
/// for it finds nothing, whatever the column holds.
#[allow(dead_code)]
fn delta_scan_unfitting(col: &VerifiedDeltaColumn<u64>) -> (r: Option<usize>)
    ensures
        r is None,
{
    proof {
        col.lemma_fits();
    }
    let mut it = col.iter();
    let r = it.scan_to_value(u64::MAX);
    proof {
        if let Some(k) = r {
            assert(col@[k as int].fits());
        }
    }
    r
}

/// The first null of a column that has nulls: a scan for the null
/// value.
#[allow(dead_code)]
fn delta_first_null(col: &VerifiedDeltaColumn<Option<i64>>) -> (r: Option<usize>)
    ensures
        match r {
            Some(k) => k < col@.len() && col@[k as int] is None && forall|i: int| 0 <= i < k ==> col@[i] is Some,
            None => forall|i: int| 0 <= i < col@.len() ==> col@[i] is Some,
        },
{
    let mut it = col.iter();
    it.scan_to_value(None)
}

/// The first of a column's numbers from `lo` to `hi`, both included,
/// said of the numbers alone: the bounds of a scan, written as a range
/// of Rust's, are plain inequalities for a column of plain numbers.
#[allow(dead_code)]
fn delta_first_in_bounds(col: &VerifiedDeltaColumn<u64>, lo: u64, hi: u64) -> (r: Option<(usize, u64)>)
    ensures
        match r {
            Some((k, v)) => {
                &&& k < col@.len()
                &&& col@[k as int] == v
                &&& lo <= v <= hi
                &&& forall|i: int| 0 <= i < k ==> (col@[i] < lo || col@[i] > hi)
            },
            None => forall|i: int| 0 <= i < col@.len() ==> (col@[i] < lo || col@[i] > hi),
        },
{
    let mut it = col.iter();
    it.scan_to_range(lo..=hi)
}

/// The first of a column's numbers that is at least `lo`: a range with
/// no upper bound, of signed numbers.
#[allow(dead_code)]
fn delta_first_at_least(col: &VerifiedDeltaColumn<i64>, lo: i64) -> (r: Option<(usize, i64)>)
    ensures
        match r {
            Some((k, v)) => k < col@.len() && col@[k as int] == v && v >= lo && forall|i: int| 0 <= i < k ==> col@[i] < lo,
            None => forall|i: int| 0 <= i < col@.len() ==> col@[i] < lo,
        },
{
    let mut it = col.iter();
    it.scan_to_range(lo..)
}

/// The first of a column's numbers greater than `a` and at most `b`:
/// bounds given as a pair, the lower one excluded.
#[allow(dead_code)]
fn delta_first_between(col: &VerifiedDeltaColumn<i32>, a: i32, b: i32) -> (r: Option<(usize, i32)>)
    ensures
        match r {
            Some((k, v)) => {
                &&& k < col@.len()
                &&& col@[k as int] == v
                &&& a < v <= b
                &&& forall|i: int| 0 <= i < k ==> (col@[i] <= a || col@[i] > b)
            },
            None => forall|i: int| 0 <= i < col@.len() ==> (col@[i] <= a || col@[i] > b),
        },
{
    let mut it = col.iter();
    it.scan_to_range((Bound::Excluded(a), Bound::Included(b)))
}

/// The first number below `hi` of a column that has nulls: a null is
/// within no bounds, so the scan passes over the nulls though a null
/// comes before every number.
#[allow(dead_code)]
fn delta_first_below(col: &VerifiedDeltaColumn<Option<u32>>, hi: u32) -> (r: Option<(usize, u32)>)
    ensures
        match r {
            Some((k, n)) => {
                &&& k < col@.len()
                &&& col@[k as int] == Some(n)
                &&& n < hi
                &&& forall|i: int| 0 <= i < k ==> ((#[trigger] col@[i]) matches Some(m) ==> m >= hi)
            },
            None => forall|i: int| 0 <= i < col@.len() ==> ((#[trigger] col@[i]) matches Some(m) ==> m >= hi),
        },
{
    let mut it = col.iter();
    match it.scan_to_range(..Some(hi)) {
        Some((k, Some(n))) => Some((k, n)),
        Some((_, None)) => {
            // no null is within bounds
            assert(false);
            None
        },
        None => None,
    }
}

/// The first number of a column that has nulls: every number is within
/// the bounds of the range that has none, and no null is.
#[allow(dead_code)]
fn delta_any_number(col: &VerifiedDeltaColumn<Option<i64>>) -> (r: Option<(usize, Option<i64>)>)
    ensures
        match r {
            Some((k, v)) => {
                &&& k < col@.len()
                &&& col@[k as int] == v
                &&& v is Some
                &&& forall|i: int| 0 <= i < k ==> col@[i] is None
            },
            None => forall|i: int| 0 <= i < col@.len() ==> col@[i] is None,
        },
{
    let mut it = col.iter();
    it.scan_to_range(..)
}

/// A null is within no bounds, not even those that are the null value
/// at both ends: such a scan finds nothing, whatever the column holds.
#[allow(dead_code)]
fn delta_null_in_no_bounds(col: &VerifiedDeltaColumn<Option<u64>>) -> (r: Option<(usize, Option<u64>)>)
    ensures
        r is None,
{
    let mut it = col.iter();
    it.scan_to_range(None..=None)
}

/// Numbers in order, sought by an iterator: the range is the one the
/// column's own search gives, the lesser numbers are before it and the
/// greater ones after, and the iterator then gives the first `x`, if
/// the column has one.
#[allow(dead_code)]
fn delta_seek_then_read(col: &VerifiedDeltaColumn<u64>, x: u64) -> (r: Range<usize>)
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
        assert(col@.subrange(0, n as int) =~= col@);
        assert(sorted::<u64>(col@));
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
            assert(col@.subrange(r.start as int, n as int)[0] == col@[r.start as int]);
        }
    }
    assert(r.start == n ==> v is None);
    r
}

/// Where `x` stands among the values `a..b` of a delta column, which
/// are in order: the search of an iterator and the column's own give
/// the same range, and the iterator is left at its start with the
/// values up to `b` to give. For every type and codec.
#[allow(dead_code)]
fn delta_sought_between<T: VDeltaValue<C>, C: VCodec>(col: &VerifiedDeltaColumn<T, C>, x: T, a: usize, b: usize) -> (r:
    Range<usize>)
    requires
        a <= b <= col@.len(),
        sorted::<T>(col@.subrange(a as int, b as int)),
    ensures
        a <= r.start <= r.end <= b,
        forall|k: int| a <= k < r.start ==> T::below(col@[k], x),
        forall|k: int| r.start <= k < r.end ==> col@[k] == x,
        forall|k: int| r.end <= k < b ==> T::below(x, col@[k]),
{
    let mut it = col.iter();
    let r = it.seek_to_value(x, a..b);
    let s = col.scope_to_value(x, a, b);
    assert(r.start == s.start && r.end == s.end) by {
        if r.start != s.start {
            let k = if r.start < s.start { r.start as int } else { s.start as int };
            assert((k < r.start) == T::below(col@[k], x));
        }
        if r.end != s.end {
            let k = if r.end < s.end { r.end as int } else { s.end as int };
            assert((k < r.end) == !T::below(x, col@[k]));
        }
    }
    assert(it.pos() == r.start);
    assert(it.rest() == col@.subrange(r.start as int, b as int));
    r
}

/// Where `x` stands among all the values a delta column's iterator has
/// left, which are in order: a search up to the window's end is over
/// all of them. The iterator is left at the first place of `x`, or
/// where `x` would stand; and, for an `x` a delta column can hold, with
/// the same end to its window. For every type and codec.
#[allow(dead_code)]
fn delta_find_in_the_rest<T: VDeltaValue<C>, C: VCodec>(it: &mut VerifiedDeltaIter<'_, T, C>, x: T) -> (r: Range<usize>)
    requires
        sorted::<T>(old(it).rest()),
    ensures
        final(it).column() == old(it).column(),
        old(it).pos() <= r.start <= r.end <= old(it).pos() + old(it).rest().len(),
        forall|j: int|
            0 <= j < old(it).rest().len() ==> {
                &&& (old(it).pos() + j < r.start) == T::below(#[trigger] old(it).rest()[j], x)
                &&& (old(it).pos() + j < r.end) == !T::below(x, old(it).rest()[j])
                &&& (r.start <= old(it).pos() + j < r.end) == (old(it).rest()[j] == x)
            },
        final(it).pos() == r.start,
        final(it).rest() == old(it).rest().skip(r.start - old(it).pos()),
        x.fits() ==> final(it).end_pos() == old(it).end_pos(),
{
    let ghost (p, c, o) = (it.pos() as int, it.column(), it.rest());
    let end = it.end_pos();
    let ghost t = p + o.len();
    assert(o =~= c.subrange(p, t));
    let r = it.seek_to_value(x, 0..end);
    proof {
        assert forall|j: int| 0 <= j < o.len() implies {
            &&& (p + j < r.start) == T::below(#[trigger] o[j], x)
            &&& (p + j < r.end) == !T::below(x, o[j])
            &&& (r.start <= p + j < r.end) == (o[j] == x)
        } by {
            assert(o[j] == c[p + j]);
        }
        assert(it.rest() =~= o.skip(r.start - p));
    }
    r
}

/// The nulls of a column in order are its first values: the search for
/// the null value gives the range of them, which starts the column.
#[allow(dead_code)]
fn delta_seek_nulls_first(col: &VerifiedDeltaColumn<Option<u64>>) -> (r: Range<usize>)
    requires
        sorted::<Option<u64>>(col@),
    ensures
        r.start == 0,
        r.end <= col@.len(),
        forall|k: int| 0 <= k < r.end ==> col@[k] is None,
        forall|k: int| r.end <= k < col@.len() ==> col@[k] is Some,
{
    let n = col.len();
    assert(col@.subrange(0, n as int) =~= col@);
    let r = col.scope_to_value(None, 0, n);
    proof {
        if n > 0 {
            assert((0 < r.start) == <Option<u64> as VSpec>::below(col@[0], None));
        }
        assert forall|k: int| 0 <= k < col@.len() implies (k < r.end) == (col@[k] is None) by {
            assert((r.start <= k < r.end) == (col@[k] == None::<u64>));
        }
    }
    r
}

/// A number too large for a delta column comes after all the numbers
/// of one: the search for it among numbers in order gives the empty
/// range at their end.
#[allow(dead_code)]
fn delta_seek_unfitting(col: &VerifiedDeltaColumn<u64>) -> (r: Range<usize>)
    requires
        forall|i: int, j: int| 0 <= i <= j < col@.len() ==> col@[i] <= col@[j],
    ensures
        r.start == col@.len(),
        r.end == col@.len(),
{
    let n = col.len();
    proof {
        col.lemma_fits();
        assert(col@.subrange(0, n as int) =~= col@);
        assert(sorted::<u64>(col@));
    }
    let r = col.scope_to_value(u64::MAX, 0, n);
    proof {
        if n > 0 {
            assert(col@[n - 1].fits());
            assert((n - 1 < r.start) == (col@[n - 1] < u64::MAX));
        }
    }
    r
}

/// A value too large for a delta column is found nowhere by an
/// iterator's search, in order or not: the iterator is left where the
/// search ends, with nothing to give. For every type and codec.
#[allow(dead_code)]
fn delta_sought_unfitting<T: VDeltaValue<C>, C: VCodec>(it: &mut VerifiedDeltaIter<'_, T, C>, x: T, range: Range<usize>) -> (r:
    Range<usize>)
    requires
        !x.fits(),
    ensures
        r.start == r.end,
        final(it).pos() == r.start,
        final(it).end_pos() == r.start,
        final(it).rest().len() == 0,
{
    let ghost (p, c) = (it.pos() as int, it.column());
    let r = it.seek_to_value(x, range);
    // asked for what it says of the window
    let _ = it.pos();
    proof {
        // nothing is left of the places searched
        let t = delta_sought_to(p, c.len() as int, range);
        assert(it.rest() == c.subrange(r.start as int, t));
        assert(c.subrange(r.start as int, t).len() == t - r.start);
    }
    r
}

/// A range that starts past its end is the empty one at its start: the
/// search moves the iterator there, if that is ahead of it and in the
/// column, and finds nothing. For every type and codec.
#[allow(dead_code)]
fn delta_sought_backwards<T: VDeltaValue<C>, C: VCodec>(it: &mut VerifiedDeltaIter<'_, T, C>, x: T, range: Range<usize>) -> (r:
    Range<usize>)
    requires
        range.start > range.end,
    ensures
        r.start == r.end,
        r.start == if range.start <= old(it).pos() {
            old(it).pos() as int
        } else if range.start <= old(it).column().len() {
            range.start as int
        } else {
            old(it).column().len() as int
        },
        final(it).pos() == r.start,
        final(it).rest().len() == 0,
{
    it.seek_to_value(x, range)
}

/// What `scan_to_value` says by itself of a delta column's iterator,
/// each thing asked for on its own and with no lemma.
#[allow(dead_code)]
fn delta_told_by_scan_to_value<T: VDeltaValue<C>, C: VCodec>(it: &mut VerifiedDeltaIter<'_, T, C>, x: T) -> (r: Option<usize>)
    ensures
        final(it).column() == old(it).column(),
        final(it).source() == old(it).source(),
        r matches Some(at) ==> old(it).pos() <= at < old(it).pos() + old(it).rest().len(),
        r matches Some(at) ==> old(it).rest()[at - old(it).pos()] == x,
        r matches Some(at) ==> forall|i: int| 0 <= i < at - old(it).pos() ==> old(it).rest()[i] != x,
        r matches Some(at) ==> final(it).rest() == old(it).rest().skip(at - old(it).pos() + 1),
        r matches Some(at) ==> final(it).pos() == at + 1,
        r matches Some(_) ==> final(it).end_pos() == old(it).end_pos(),
        r is None ==> forall|i: int| 0 <= i < old(it).rest().len() ==> old(it).rest()[i] != x,
        r is None ==> final(it).rest().len() == 0,
        r is None ==> final(it).pos() == old(it).pos() + old(it).rest().len(),
        r is None ==> final(it).end_pos() == final(it).pos(),
{
    it.scan_to_value(x)
}

/// What `scan_to_range` says by itself of a delta column's iterator and
/// any bounds, each thing asked for on its own and with no lemma.
#[allow(dead_code)]
fn delta_told_by_scan_to_range<T: VDeltaValue<C>, C: VCodec, R: RangeBounds<T>>(
    it: &mut VerifiedDeltaIter<'_, T, C>,
    range: R,
) -> (r: Option<(usize, T)>)
    ensures
        final(it).column() == old(it).column(),
        final(it).source() == old(it).source(),
        r matches Some((at, _)) ==> old(it).pos() <= at < old(it).pos() + old(it).rest().len(),
        r matches Some((at, v)) ==> old(it).rest()[at - old(it).pos()] == v,
        r matches Some((_, v)) ==> delta_within::<T>(range.spec_start_bound(), range.spec_end_bound(), v),
        r matches Some((at, _)) ==> forall|i: int|
            0 <= i < at - old(it).pos() ==> !delta_within::<T>(
                range.spec_start_bound(),
                range.spec_end_bound(),
                #[trigger] old(it).rest()[i],
            ),
        r matches Some((at, _)) ==> final(it).rest() == old(it).rest().skip(at - old(it).pos() + 1),
        r matches Some((at, _)) ==> final(it).pos() == at + 1,
        r matches Some(_) ==> final(it).end_pos() == old(it).end_pos(),
        r is None ==> forall|i: int|
            0 <= i < old(it).rest().len() ==> !delta_within::<T>(
                range.spec_start_bound(),
                range.spec_end_bound(),
                #[trigger] old(it).rest()[i],
            ),
        r is None ==> final(it).rest().len() == 0,
        r is None ==> final(it).pos() == old(it).pos() + old(it).rest().len(),
        r is None ==> final(it).end_pos() == final(it).pos(),
{
    it.scan_to_range(range)
}

/// Where a search of a delta column's iterator is asked to end: where
/// `range` ends, or where it starts if that is past its end.
spec fn delta_sought_end(range: Range<usize>) -> int {
    if range.start <= range.end { range.end as int } else { range.start as int }
}

/// Where the places end that such a search goes over, by an iterator
/// that stands at `p` of a column of `n` values.
spec fn delta_sought_to(p: int, n: int, range: Range<usize>) -> int {
    let e = delta_sought_end(range);
    if e <= p { p } else if e <= n { e } else { n }
}

/// Where those places start.
spec fn delta_sought_from(p: int, n: int, range: Range<usize>) -> int {
    let t = delta_sought_to(p, n, range);
    if range.start <= p { p } else if range.start <= t { range.start as int } else { t }
}

/// What `seek_to_value` says by itself of a delta column's iterator and
/// any range, each thing asked for on its own and with no lemma: which
/// places of the column are searched, where `x` stands among their
/// values if those are in order, and where that leaves the iterator.
#[allow(dead_code)]
fn delta_told_by_seek_to_value<T: VDeltaValue<C>, C: VCodec>(
    it: &mut VerifiedDeltaIter<'_, T, C>,
    x: T,
    range: Range<usize>,
) -> (r: Range<usize>)
    ensures
        final(it).column() == old(it).column(),
        final(it).source() == old(it).source(),
        old(it).pos() <= delta_sought_from(old(it).pos() as int, old(it).column().len() as int, range),
        delta_sought_from(old(it).pos() as int, old(it).column().len() as int, range) <= r.start,
        r.start <= r.end,
        r.end <= delta_sought_to(old(it).pos() as int, old(it).column().len() as int, range),
        delta_sought_to(old(it).pos() as int, old(it).column().len() as int, range) <= old(it).column().len(),
        final(it).pos() == r.start,
        final(it).rest() == old(it).column().subrange(
            r.start as int,
            delta_sought_to(old(it).pos() as int, old(it).column().len() as int, range),
        ),
        final(it).end_pos() == if range.start > delta_sought_to(
            old(it).pos() as int,
            old(it).column().len() as int,
            range,
        ) || !x.fits() {
            r.start as int
        } else if delta_sought_end(range) >= old(it).pos() {
            delta_sought_end(range)
        } else {
            old(it).pos() as int
        },
        ({
            let c = old(it).column();
            let s = delta_sought_from(old(it).pos() as int, c.len() as int, range);
            let t = delta_sought_to(old(it).pos() as int, c.len() as int, range);
            sorted::<T>(c.subrange(s, t)) ==> forall|k: int| s <= k < t ==> (k < r.start) == T::below(#[trigger] c[k], x)
        }),
        ({
            let c = old(it).column();
            let s = delta_sought_from(old(it).pos() as int, c.len() as int, range);
            let t = delta_sought_to(old(it).pos() as int, c.len() as int, range);
            sorted::<T>(c.subrange(s, t)) ==> forall|k: int| s <= k < t ==> (k < r.end) == !T::below(x, #[trigger] c[k])
        }),
        ({
            let c = old(it).column();
            let s = delta_sought_from(old(it).pos() as int, c.len() as int, range);
            let t = delta_sought_to(old(it).pos() as int, c.len() as int, range);
            sorted::<T>(c.subrange(s, t)) ==> forall|k: int|
                s <= k < t ==> (r.start <= k < r.end) == (#[trigger] c[k] == x)
        }),
{
    it.seek_to_value(x, range)
}

/// What `scope_to_value` says by itself of a delta column, each thing
/// asked for on its own and with no lemma.
#[allow(dead_code)]
fn delta_told_by_scope_to_value<T: VDeltaValue<C>, C: VCodec>(
    col: &VerifiedDeltaColumn<T, C>,
    x: T,
    start: usize,
    end: usize,
) -> (r: Range<usize>)
    requires
        start <= end <= col@.len(),
    ensures
        start <= r.start,
        r.start <= r.end,
        r.end <= end,
        sorted::<T>(col@.subrange(start as int, end as int)) ==> forall|k: int|
            start <= k < end ==> (k < r.start) == T::below(#[trigger] col@[k], x),
        sorted::<T>(col@.subrange(start as int, end as int)) ==> forall|k: int|
            start <= k < end ==> (k < r.end) == !T::below(x, #[trigger] col@[k]),
        sorted::<T>(col@.subrange(start as int, end as int)) ==> forall|k: int|
            start <= k < end ==> (r.start <= k < r.end) == (#[trigger] col@[k] == x),
{
    col.scope_to_value(x, start, end)
}

// ── Delta columns: the places of values, found by the column's index ────────

/// Every place of the number `x` in a delta column, found by the
/// column's index: a `for` loop over a finder collects them. They come
/// in rising order, each holds `x`, and no other place does. For every
/// type and codec.
#[allow(dead_code)]
fn delta_found_all<T: VDeltaValue<C>, C: VCodec>(col: &VerifiedDeltaColumn<T, C>, x: T) -> (out: Vec<usize>)
    requires
        x.dv() is Some,
    ensures
        forall|i: int, j: int| 0 <= i < j < out@.len() ==> out@[i] < out@[j],
        forall|i: int| 0 <= i < out@.len() ==> out@[i] < col@.len() && col@[out@[i] as int] == x,
        forall|k: int| 0 <= k < col@.len() && col@[k] == x ==> exists|i: int| 0 <= i < out@.len() && out@[i] == k,
{
    let _n = col.len();
    let ghost fs = delta_finds::<T>(col@, Bound::Included(&x), Bound::Included(&x), 0);
    let mut out: Vec<usize> = Vec::new();
    for place in at: col.find_by_value(x)
        invariant
            at.seq() == fs,
            out@ == fs.take(at.index()),
    {
        out.push(place);
        proof {
            assert(fs.take(at.index() + 1) =~= fs.take(at.index()).push(place));
        }
    }
    proof {
        assert(fs.take(fs.len() as int) =~= fs);
        col.lemma_fits();
        lemma_delta_finds::<T>(col@, Bound::Included(&x), Bound::Included(&x), 0);
        assert forall|k: int| 0 <= k < col@.len() implies delta_within::<T>(
            Bound::Included(&x),
            Bound::Included(&x),
            #[trigger] col@[k],
        ) == (col@[k] == x) by {
            lemma_delta_within_one::<T>(x, col@[k]);
        }
        assert forall|k: int| 0 <= k < col@.len() && col@[k] == x implies exists|i: int|
            0 <= i < out@.len() && out@[i] == k by {
            assert(fs.contains(k as usize));
            let w = choose|w: int| 0 <= w < fs.len() && fs[w] == k as usize;
            assert(out@[w] == k);
        }
    }
    out
}

/// The places of a column's numbers from `lo` up to `hi`, the first
/// included and the second not, said of the numbers alone: the bounds
/// of a find, written as a range of Rust's, are plain inequalities for
/// a column of plain numbers.
#[allow(dead_code)]
fn delta_found_between(col: &VerifiedDeltaColumn<u64>, lo: u64, hi: u64) -> (out: Vec<usize>)
    ensures
        forall|i: int, j: int| 0 <= i < j < out@.len() ==> out@[i] < out@[j],
        forall|i: int| 0 <= i < out@.len() ==> out@[i] < col@.len() && lo <= col@[out@[i] as int] < hi,
        forall|k: int|
            0 <= k < col@.len() && lo <= (#[trigger] col@[k]) && col@[k] < hi ==> exists|i: int|
                0 <= i < out@.len() && out@[i] == k,
{
    let _n = col.len();
    let finder = col.find_by_range(lo..hi);
    let ghost fs = delta_finds::<u64>(col@, Bound::Included(&lo), Bound::Excluded(&hi), 0);
    assert(finder.finds() == fs);
    let mut out: Vec<usize> = Vec::new();
    for place in at: finder
        invariant
            at.seq() == fs,
            out@ == fs.take(at.index()),
    {
        out.push(place);
        proof {
            assert(fs.take(at.index() + 1) =~= fs.take(at.index()).push(place));
        }
    }
    proof {
        assert(fs.take(fs.len() as int) =~= fs);
        col.lemma_fits();
        lemma_delta_finds::<u64>(col@, Bound::Included(&lo), Bound::Excluded(&hi), 0);
        assert forall|k: int| 0 <= k < col@.len() && lo <= (#[trigger] col@[k]) && col@[k] < hi implies exists|i: int|
            0 <= i < out@.len() && out@[i] == k by {
            assert(fs.contains(k as usize));
            let w = choose|w: int| 0 <= w < fs.len() && fs[w] == k as usize;
            assert(out@[w] == k);
        }
    }
    out
}

/// The first place of a number that is at most `hi` in a column that
/// has nulls: a null is within no bounds, so no null is found though a
/// null comes before every number.
#[allow(dead_code)]
fn delta_found_at_most(col: &VerifiedDeltaColumn<Option<i32>>, hi: i32) -> (r: Option<usize>)
    ensures
        match r {
            Some(k) => {
                &&& k < col@.len()
                &&& col@[k as int] matches Some(n) && n <= hi
                &&& forall|i: int| 0 <= i < k ==> ((#[trigger] col@[i]) matches Some(m) ==> m > hi)
            },
            None => forall|i: int| 0 <= i < col@.len() ==> ((#[trigger] col@[i]) matches Some(m) ==> m > hi),
        },
{
    let _n = col.len();
    let top = Some(hi);
    let mut finder = col.find_by_range(..=top);
    let r = finder.next();
    proof {
        col.lemma_fits();
        lemma_delta_finds_first::<Option<i32>>(col@, Bound::Unbounded, Bound::Included(&top), 0);
    }
    r
}

/// The first two places of the number `x` in a delta column, by two
/// calls of a finder: the second call goes on from where the first one
/// found `x`. For every type and codec.
#[allow(dead_code)]
fn delta_found_twice<T: VDeltaValue<C>, C: VCodec>(col: &VerifiedDeltaColumn<T, C>, x: T) -> (r: (
    Option<usize>,
    Option<usize>,
))
    requires
        x.dv() is Some,
    ensures
        match r.0 {
            Some(a) => {
                &&& a < col@.len()
                &&& col@[a as int] == x
                &&& forall|j: int| 0 <= j < a ==> col@[j] != x
                &&& match r.1 {
                    Some(b) => {
                        &&& a < b < col@.len()
                        &&& col@[b as int] == x
                        &&& forall|j: int| a < j < b ==> col@[j] != x
                    },
                    None => forall|j: int| a < j < col@.len() ==> col@[j] != x,
                }
            },
            None => r.1 is None && forall|j: int| 0 <= j < col@.len() ==> col@[j] != x,
        },
{
    let _n = col.len();
    let mut finder = col.find_by_value(x);
    let ghost fs = finder.finds();
    let a = finder.next();
    let b = finder.next();
    proof {
        col.lemma_fits();
        assert forall|k: int| 0 <= k < col@.len() implies delta_within::<T>(
            Bound::Included(&x),
            Bound::Included(&x),
            #[trigger] col@[k],
        ) == (col@[k] == x) by {
            lemma_delta_within_one::<T>(x, col@[k]);
        }
        lemma_delta_finds_first::<T>(col@, Bound::Included(&x), Bound::Included(&x), 0);
        if fs.len() > 0 {
            lemma_delta_finds_first::<T>(col@, Bound::Included(&x), Bound::Included(&x), fs[0] + 1);
        }
    }
    (a, b)
}

/// A null is found nowhere, though the column may hold nulls: neither
/// by `find_first` nor by a finder.
#[allow(dead_code)]
fn delta_find_null(col: &VerifiedDeltaColumn<Option<u64>>) -> (r: (Option<usize>, Option<usize>))
    ensures
        r.0 is None,
        r.1 is None,
{
    let _n = col.len();
    let first = col.find_first(None);
    let mut finder = col.find_by_value(None);
    let next = finder.next();
    proof {
        let null: Option<u64> = None;
        col.lemma_fits();
        lemma_delta_finds_first::<Option<u64>>(col@, Bound::Included(&null), Bound::Included(&null), 0);
    }
    (first, next)
}

/// A number no delta column can hold is found nowhere.
#[allow(dead_code)]
fn delta_find_unfitting(col: &VerifiedDeltaColumn<u64>) -> (r: Option<usize>)
    ensures
        r is None,
{
    let r = col.find_first(u64::MAX);
    proof {
        col.lemma_fits();
    }
    r
}

/// The first place of `x` in a delta column of plain numbers, said of
/// the numbers alone.
#[allow(dead_code)]
fn delta_find_first_number(col: &VerifiedDeltaColumn<i64>, x: i64) -> (r: Option<usize>)
    ensures
        match r {
            Some(k) => k < col@.len() && col@[k as int] == x && forall|i: int| 0 <= i < k ==> col@[i] != x,
            None => forall|i: int| 0 <= i < col@.len() ==> col@[i] != x,
        },
{
    col.find_first(x)
}

/// A finder made by `default` has nothing to give.
#[allow(dead_code)]
fn delta_find_nothing() -> (r: Option<usize>)
    ensures
        r is None,
{
    let mut finder: VerifiedFindByRange<'static, u64> = VerifiedFindByRange::default();
    finder.next()
}

/// What `find_by_range` says by itself of a delta column and any
/// bounds.
#[allow(dead_code)]
fn delta_told_by_find_by_range<T: VDeltaValue<C>, C: VCodec, R: RangeBounds<T>>(
    col: &VerifiedDeltaColumn<T, C>,
    range: R,
) -> (r: VerifiedFindByRange<'_, T, C>)
    ensures
        r.finds() == delta_finds::<T>(col@, range.spec_start_bound(), range.spec_end_bound(), 0),
{
    col.find_by_range(range)
}

/// What `find_by_value` says by itself of a delta column.
#[allow(dead_code)]
fn delta_told_by_find_by_value<T: VDeltaValue<C>, C: VCodec>(col: &VerifiedDeltaColumn<T, C>, x: T) -> (r:
    VerifiedFindByRange<'_, T, C>)
    ensures
        r.finds() == delta_finds::<T>(col@, Bound::Included(&x), Bound::Included(&x), 0),
{
    col.find_by_value(x)
}

/// What `find_first` says by itself of a delta column, each thing asked
/// for on its own and with no lemma.
#[allow(dead_code)]
fn delta_told_by_find_first<T: VDeltaValue<C>, C: VCodec>(col: &VerifiedDeltaColumn<T, C>, x: T) -> (r: Option<usize>)
    ensures
        r matches Some(k) ==> k < col@.len(),
        r matches Some(k) ==> col@[k as int] == x,
        r matches Some(_) ==> x.dv() is Some,
        r matches Some(k) ==> forall|j: int| 0 <= j < k ==> col@[j] != x,
        r is None && x.dv() is Some ==> forall|j: int| 0 <= j < col@.len() ==> col@[j] != x,
{
    col.find_first(x)
}

/// What a finder's `next` says by itself: the first of the places it
/// had left, which leaves the rest; and nothing where none was left.
#[allow(dead_code)]
fn delta_told_by_find_next<T: VDeltaValue<C>, C: VCodec>(finder: &mut VerifiedFindByRange<'_, T, C>) -> (r: Option<usize>)
    ensures
        old(finder).finds().len() > 0 ==> r == Some(old(finder).finds()[0]),
        old(finder).finds().len() > 0 ==> final(finder).finds() == old(finder).finds().drop_first(),
        old(finder).finds().len() == 0 ==> r is None,
        old(finder).finds().len() == 0 ==> final(finder).finds() == old(finder).finds(),
{
    finder.next()
}

/// What `default` says by itself of a finder.
#[allow(dead_code)]
fn delta_told_by_find_default<T: VDeltaValue<C>, C: VCodec>() -> (r: VerifiedFindByRange<'static, T, C>)
    ensures
        r.finds() == Seq::<usize>::empty(),
{
    VerifiedFindByRange::default()
}


// ── Delta columns: the encoder ──────────────────────────────────────────────

/// What `new` and `default` say of the delta encoder they give: it is
/// one to use (`wf`), and was given nothing.
#[allow(dead_code)]
fn delta_told_by_a_new_encoder<'a, T: VDeltaValue<C>, C: VCodec>() -> (r: (
    VerifiedDeltaEncoder<'a, T, C>,
    VerifiedDeltaEncoder<'a, T, C>,
))
    ensures
        r.0.wf(),
        r.1.wf(),
        r.0@.len() == 0,
        r.1@.len() == 0,
{
    let a = VerifiedDeltaEncoder::<T, C>::new();
    let b = VerifiedDeltaEncoder::<T, C>::default();
    (a, b)
}

/// What a delta encoder's `len` and `is_empty` say: the number of
/// values it was given.
#[allow(dead_code)]
fn delta_told_by_an_encoders_count<'a, T: VDeltaValue<C>, C: VCodec>(enc: &VerifiedDeltaEncoder<'a, T, C>) -> (r: (
    usize,
    bool,
))
    requires
        enc.wf(),
    ensures
        r.0 == enc@.len(),
        r.1 == (enc@.len() == 0),
{
    (enc.len(), enc.is_empty())
}

/// What `append`, `append_owned` and `append_n` say: the values, after
/// those the encoder had, which is still one to use. Each value given
/// must be one a delta column holds (`fits`).
#[allow(dead_code)]
fn delta_told_by_an_encoders_appends<'a, T: VDeltaValue<C>, C: VCodec>(
    enc: &mut VerifiedDeltaEncoder<'a, T, C>,
    v: T,
    w: T,
    n: usize,
)
    requires
        old(enc).wf(),
        v.fits(),
        w.fits(),
    ensures
        final(enc).wf(),
        final(enc)@.len() == old(enc)@.len() + 2 + n,
        final(enc)@.take(old(enc)@.len() as int) == old(enc)@,
        final(enc)@[old(enc)@.len() as int] == v,
        final(enc)@[old(enc)@.len() as int + 1] == w,
        forall|i: int| old(enc)@.len() + 2 <= i < final(enc)@.len() ==> final(enc)@[i] == v,
{
    enc.append(v);
    enc.append_owned(w);
    enc.append_n(v, n);
    assert(enc@.take(old(enc)@.len() as int) =~= old(enc)@);
}

/// What a delta encoder's `extend` says of a vector handed over, whose
/// values fit: its values, after those the encoder had.
#[allow(dead_code)]
fn delta_told_by_an_encoders_extend<'a, T: VDeltaValue<C>, C: VCodec>(
    enc: &mut VerifiedDeltaEncoder<'a, T, C>,
    vs: Vec<T>,
)
    requires
        old(enc).wf(),
        all_fit(vs@),
    ensures
        final(enc).wf(),
        final(enc)@ == old(enc)@ + vs@,
{
    enc.extend(vs);
}

/// What `extend` asks of any iterator, and says of it: that its values
/// are known to fit (`delta_iter_fits`), and, of one that keeps the
/// laws Verus has for an iterator, that what it had to give was given.
#[allow(dead_code)]
fn delta_told_by_an_extend_of_any<'a, T: VDeltaValue<C>, C: VCodec, I: Iterator<Item = T>>(
    enc: &mut VerifiedDeltaEncoder<'a, T, C>,
    iter: I,
)
    requires
        old(enc).wf(),
        delta_iter_fits(iter),
    ensures
        final(enc).wf(),
        iter.obeys_prophetic_iter_laws() ==> final(enc)@ == old(enc)@ + iter.remaining(),
{
    enc.extend(iter);
}

/// What a delta encoder's `save` says: the bytes are a saved delta
/// column, of the values given, and the one encoding of them; and no
/// two numbers given are 2^63 or more apart, the encoder panicking for
/// such.
#[allow(dead_code)]
fn delta_told_by_an_encoders_save<'a, T: VDeltaValue<C>, C: VCodec>(enc: VerifiedDeltaEncoder<'a, T, C>) -> (r: Vec<u8>)
    requires
        enc.wf(),
    ensures
        VerifiedDeltaColumn::<T, C>::valid(r@),
        VerifiedDeltaColumn::<T, C>::values(r@) == enc@,
        r@ == VerifiedDeltaColumn::<T, C>::encoding(enc@),
        delta_span_ok(enc@),
{
    enc.save()
}

/// What a delta encoder's `save_to` says: the buffer is what it was and
/// then the encoding of the values given, which is a saved delta column
/// of them, and the range returned is where.
#[allow(dead_code)]
fn delta_told_by_an_encoders_save_to<'a, T: VDeltaValue<C>, C: VCodec>(
    enc: VerifiedDeltaEncoder<'a, T, C>,
    out: &mut Vec<u8>,
) -> (r: Range<usize>)
    requires
        enc.wf(),
    ensures
        final(out)@ == old(out)@ + VerifiedDeltaColumn::<T, C>::encoding(enc@),
        VerifiedDeltaColumn::<T, C>::valid(VerifiedDeltaColumn::<T, C>::encoding(enc@)),
        VerifiedDeltaColumn::<T, C>::values(VerifiedDeltaColumn::<T, C>::encoding(enc@)) == enc@,
        delta_span_ok(enc@),
        r.start == old(out)@.len(),
        r.end == final(out)@.len(),
        final(out)@.subrange(r.start as int, r.end as int) == VerifiedDeltaColumn::<T, C>::encoding(enc@),
{
    let ghost d = VerifiedDeltaColumn::<T, C>::encoding(enc@);
    let r = enc.save_to(out);
    assert(out@.subrange(r.start as int, r.end as int) =~= d);
    r
}

/// What a delta encoder's `save_to_unless` says: nothing is written
/// where every value given is `v`, as where none was given; and what
/// `save_to` writes is, where one is not. The value need not fit: none
/// given is one that does not.
#[allow(dead_code)]
fn delta_told_by_an_encoders_save_to_unless<'a, T: VDeltaValue<C>, C: VCodec>(
    enc: VerifiedDeltaEncoder<'a, T, C>,
    out: &mut Vec<u8>,
    v: T,
) -> (r: Range<usize>)
    requires
        enc.wf(),
    ensures
        (forall|i: int| 0 <= i < enc@.len() ==> enc@[i] == v) ==> final(out)@ == old(out)@,
        (exists|i: int| 0 <= i < enc@.len() && enc@[i] != v) ==> {
            &&& final(out)@ == old(out)@ + VerifiedDeltaColumn::<T, C>::encoding(enc@)
            &&& VerifiedDeltaColumn::<T, C>::valid(VerifiedDeltaColumn::<T, C>::encoding(enc@))
            &&& VerifiedDeltaColumn::<T, C>::values(VerifiedDeltaColumn::<T, C>::encoding(enc@)) == enc@
            &&& delta_span_ok(enc@)
        },
        r.start == old(out)@.len(),
        r.end == final(out)@.len(),
{
    enc.save_to_unless(out, v)
}

/// What `lemma_fits` says of a delta encoder: every value given is one
/// a delta column holds. So a value that is not is none of them, and
/// `save_to_unless` of such a value writes what an encoder has, if it
/// was given anything.
#[allow(dead_code)]
fn delta_unless_a_misfit<'a, T: VDeltaValue<C>, C: VCodec>(
    enc: VerifiedDeltaEncoder<'a, T, C>,
    out: &mut Vec<u8>,
    v: T,
) -> (r: Range<usize>)
    requires
        enc.wf(),
        !v.fits(),
        enc@.len() > 0,
    ensures
        all_fit(enc@),
        final(out)@ == old(out)@ + VerifiedDeltaColumn::<T, C>::encoding(enc@),
{
    let ghost vs = enc@;
    proof {
        enc.lemma_fits();
        assert(vs[0].fits());
        assert(!only(vs, v));
    }
    enc.save_to_unless(out, v)
}

/// Values appended one by one are saved as a delta column of them: the
/// bytes are a saved column, of those values, and the one encoding of
/// them. The loop carries `enc.wf()`, and what the encoder has been
/// given so far.
#[allow(dead_code)]
fn delta_encoded_as_saved<T: VDeltaValue<C>, C: VCodec>(input: &Vec<T>) -> (r: Vec<u8>)
    requires
        all_fit(input@),
    ensures
        VerifiedDeltaColumn::<T, C>::valid(r@),
        VerifiedDeltaColumn::<T, C>::values(r@) == input@,
        r@ == VerifiedDeltaColumn::<T, C>::encoding(input@),
        delta_span_ok(input@),
{
    let mut enc = VerifiedDeltaEncoder::<T, C>::new();
    let mut i: usize = 0;
    proof {
        assert(enc@ =~= input@.take(0));
    }
    while i < input.len()
        invariant
            enc.wf(),
            i <= input@.len(),
            all_fit(input@),
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
    enc.save()
}

/// A delta encoder and a delta column save the same bytes for the same
/// values.
#[allow(dead_code)]
fn delta_encoder_or_column<T: VDeltaValue<C>, C: VCodec>(input: &Vec<T>)
    requires
        all_fit(input@),
{
    let a = delta_encoded_as_saved::<T, C>(input);
    let col = VerifiedDeltaColumn::<T, C>::from_values(input.as_slice());
    let b = col.save();
    assert(a@ == b@);
}

/// What a delta encoder saves, loaded: where it loads, it is a column
/// of the values given. (Of a delta load it is proved that what it
/// gives is right, not that it takes whatever was saved.)
#[allow(dead_code)]
fn delta_encode_then_load<T: VDeltaValue<C>, C: VCodec>(input: &Vec<T>) -> (r: Option<VerifiedDeltaColumn<T, C>>)
    requires
        all_fit(input@),
    ensures
        r matches Some(col) ==> col@ == input@,
{
    let bytes = delta_encoded_as_saved::<T, C>(input);
    match VerifiedDeltaColumn::<T, C>::load(bytes.as_slice()) {
        Ok(col) => Some(col),
        Err(_) => None,
    }
}

/// A delta column's values, given to an encoder by the column's
/// iterator, are saved as the column saves them. What the iterator has
/// to give fits, the column having held it (`lemma_fits`).
#[allow(dead_code)]
fn delta_reencoded<T: VDeltaValue<C>, C: VCodec>(col: &VerifiedDeltaColumn<T, C>) -> (r: Vec<u8>)
    ensures
        r@ == VerifiedDeltaColumn::<T, C>::encoding(col@),
{
    let mut enc = VerifiedDeltaEncoder::<T, C>::new();
    proof {
        col.lemma_fits();
    }
    enc.extend(col.iter());
    let bytes = col.save();
    let r = enc.save();
    assert(r@ == bytes@);
    r
}

/// The same by a `for` loop over the column, each value appended as it
/// comes.
#[allow(dead_code)]
fn delta_reencoded_in_a_loop<T: VDeltaValue<C>, C: VCodec>(col: &VerifiedDeltaColumn<T, C>) -> (r: Vec<u8>)
    ensures
        r@ == VerifiedDeltaColumn::<T, C>::encoding(col@),
{
    let mut enc = VerifiedDeltaEncoder::<T, C>::new();
    proof {
        col.lemma_fits();
        assert(enc@ =~= col@.take(0));
    }
    for x in at: col
        invariant
            at.seq() == col@,
            all_fit(col@),
            enc.wf(),
            enc@ == col@.take(at.index()),
    {
        enc.append(x);
        proof {
            assert(col@.take(at.index() + 1) =~= col@.take(at.index()).push(x));
        }
    }
    proof {
        assert(col@.take(col@.len() as int) =~= col@);
    }
    let bytes = col.save();
    let r = enc.save();
    assert(r@ == bytes@);
    r
}

/// What `encode` says of a vector whose values fit: the bytes of a
/// delta column of them.
#[allow(dead_code)]
fn delta_told_by_encode<T: VDeltaValue<C>, C: VCodec>(vs: Vec<T>) -> (r: Vec<u8>)
    requires
        all_fit(vs@),
    ensures
        VerifiedDeltaColumn::<T, C>::valid(r@),
        VerifiedDeltaColumn::<T, C>::values(r@) == vs@,
        r@ == VerifiedDeltaColumn::<T, C>::encoding(vs@),
        delta_span_ok(vs@),
{
    VerifiedDeltaEncoder::<T, C>::encode(vs)
}

/// What `encode_to` says of a vector whose values fit: the buffer is
/// what it was and then the encoding of the values, and the range
/// returned is where.
#[allow(dead_code)]
fn delta_told_by_encode_to<T: VDeltaValue<C>, C: VCodec>(out: &mut Vec<u8>, vs: Vec<T>) -> (r: Range<usize>)
    requires
        all_fit(vs@),
    ensures
        final(out)@ == old(out)@ + VerifiedDeltaColumn::<T, C>::encoding(vs@),
        VerifiedDeltaColumn::<T, C>::valid(VerifiedDeltaColumn::<T, C>::encoding(vs@)),
        VerifiedDeltaColumn::<T, C>::values(VerifiedDeltaColumn::<T, C>::encoding(vs@)) == vs@,
        delta_span_ok(vs@),
        r.start == old(out)@.len(),
        r.end == final(out)@.len(),
{
    VerifiedDeltaEncoder::<T, C>::encode_to(out, vs)
}

/// What `encode_to_unless` says of a vector whose values fit: the
/// buffer is left as it was where every value is `v`, and only there;
/// and holds the encoding of the values after what it held, where one
/// is not.
#[allow(dead_code)]
fn delta_told_by_encode_to_unless<T: VDeltaValue<C>, C: VCodec>(
    out: &mut Vec<u8>,
    vs: Vec<T>,
    v: T,
) -> (r: Range<usize>)
    requires
        all_fit(vs@),
    ensures
        (final(out)@ == old(out)@) == only(vs@, v),
        !only(vs@, v) ==> {
            &&& final(out)@ == old(out)@ + VerifiedDeltaColumn::<T, C>::encoding(vs@)
            &&& VerifiedDeltaColumn::<T, C>::valid(VerifiedDeltaColumn::<T, C>::encoding(vs@))
            &&& VerifiedDeltaColumn::<T, C>::values(VerifiedDeltaColumn::<T, C>::encoding(vs@)) == vs@
            &&& delta_span_ok(vs@)
        },
        r.start == old(out)@.len(),
        r.end == final(out)@.len(),
{
    VerifiedDeltaEncoder::<T, C>::encode_to_unless(out, vs, v)
}

/// Any iterator over `u32`s can be encoded, whatever is known of it:
/// every `u32` is a value a delta column holds. What is then known of
/// the bytes is that they are a saved delta column, the one encoding of
/// its values.
#[allow(dead_code)]
fn delta_encode_any_iterator<I: Iterator<Item = u32>>(iter: I) -> (r: Vec<u8>)
    ensures
        VerifiedDeltaColumn::<u32>::valid(r@),
        r@ == VerifiedDeltaColumn::<u32>::encoding(VerifiedDeltaColumn::<u32>::values(r@)),
{
    VerifiedDeltaEncoder::<u32>::encode(iter)
}

/// `u64`s with nulls, some perhaps too great for a delta column: each
/// is checked as it goes to the encoder, and so nothing need be known
/// of them beforehand. Bytes come back where every number is at most
/// `i64::MAX`, and only there, and are the encoding of the values.
#[allow(dead_code)]
fn delta_encode_checked(vs: &Vec<Option<u64>>) -> (r: Option<Vec<u8>>)
    ensures
        r matches Some(bytes) ==> bytes@ == VerifiedDeltaColumn::<Option<u64>>::encoding(vs@),
        r is Some ==> forall|i: int| 0 <= i < vs@.len() && (#[trigger] vs@[i]) is Some ==> vs@[i]->Some_0 <= i64::MAX,
        r is None ==> exists|i: int| 0 <= i < vs@.len() && (#[trigger] vs@[i]) is Some && vs@[i]->Some_0 > i64::MAX,
{
    let mut enc = VerifiedDeltaEncoder::<Option<u64>>::new();
    let mut i: usize = 0;
    proof {
        assert(enc@ =~= vs@.take(0));
    }
    while i < vs.len()
        invariant
            enc.wf(),
            i <= vs@.len(),
            enc@ == vs@.take(i as int),
            forall|k: int| 0 <= k < i && (#[trigger] vs@[k]) is Some ==> vs@[k]->Some_0 <= i64::MAX,
        decreases vs@.len() - i,
    {
        let v = vs[i];
        if let Some(n) = v {
            if n > i64::MAX as u64 {
                proof {
                    assert(vs@[i as int] is Some && vs@[i as int]->Some_0 > i64::MAX);
                }
                return None;
            }
        }
        enc.append(v);
        proof {
            assert(enc@ =~= vs@.take(i + 1));
        }
        i += 1;
    }
    proof {
        assert(vs@.take(i as int) =~= vs@);
    }
    Some(enc.save())
}

/// Two numbers an encoder of `i64`s has saved are less than 2^63 apart,
/// whichever two: so `save` says, having returned.
#[allow(dead_code)]
fn delta_saved_within_a_span<'a>(enc: VerifiedDeltaEncoder<'a, i64>, i: usize, j: usize) -> (r: Vec<u8>)
    requires
        enc.wf(),
        i < enc@.len(),
        j < enc@.len(),
    ensures
        enc@[j as int] - enc@[i as int] <= i64::MAX,
{
    let ghost vs = enc@;
    let r = enc.save();
    assert(delta_near(vs[i as int], vs[j as int]));
    r
}

/// Numbers 2^63 apart are no delta column's. An encoder takes them, each
/// being a value that fits, and panics when it is to save them: what
/// comes after its `save` is never reached, and the proof knows it.
#[allow(dead_code)]
fn delta_too_wide_never_saved() -> (r: Vec<u8>)
    ensures
        false,
{
    let mut enc = VerifiedDeltaEncoder::<i64>::new();
    enc.append(i64::MIN);
    enc.append(0);
    let ghost vs = enc@;
    let r = enc.save();
    assert(delta_near(vs[0], vs[1]));
    r
}

// ── Delta columns: the state that writes into its caller's buffer ───────────

/// What `new` and `default` say of the delta state they give: it was
/// given nothing, and may start on any buffer, all of which is then
/// what was there before it.
#[allow(dead_code)]
fn delta_told_by_a_new_state<'a, T: VDeltaValue<C>, C: VCodec>() -> (r: (
    VerifiedDeltaEncoderState<'a, T, C>,
    VerifiedDeltaEncoderState<'a, T, C>,
))
    ensures
        r.0@.len() == 0,
        r.1@.len() == 0,
        forall|buf: Seq<u8>| #[trigger] r.0.on(buf),
        forall|buf: Seq<u8>| #[trigger] r.0.before(buf) == buf,
        forall|buf: Seq<u8>| #[trigger] r.1.on(buf),
        forall|buf: Seq<u8>| #[trigger] r.1.before(buf) == buf,
{
    let a = VerifiedDeltaEncoderState::<T, C>::new();
    let b = VerifiedDeltaEncoderState::<T, C>::default();
    (a, b)
}

/// What a delta state's `len` and `is_empty` say: the number of values
/// it was given. They take no buffer, and require that there is one the
/// state is on.
#[allow(dead_code)]
fn delta_told_by_a_states_count<'a, T: VDeltaValue<C>, C: VCodec>(
    s: &VerifiedDeltaEncoderState<'a, T, C>,
    buf: &Vec<u8>,
) -> (r: (usize, bool))
    requires
        s.on(buf@),
    ensures
        r.0 == s@.len(),
        r.1 == (s@.len() == 0),
{
    (s.len(), s.is_empty())
}

/// What a delta state's `append` says: the value, after those the state
/// had; the buffer is still the state's, and what was there before the
/// state is what was.
#[allow(dead_code)]
fn delta_told_by_a_states_append<'a, T: VDeltaValue<C>, C: VCodec>(
    s: &mut VerifiedDeltaEncoderState<'a, T, C>,
    buf: &mut Vec<u8>,
    v: T,
)
    requires
        old(s).on(old(buf)@),
        v.fits(),
    ensures
        final(s).on(final(buf)@),
        final(s).before(final(buf)@) == old(s).before(old(buf)@),
        final(s)@.len() == old(s)@.len() + 1,
        final(s)@.last() == v,
        final(s)@.drop_last() == old(s)@,
{
    s.append(buf, v);
    assert(s@.drop_last() =~= old(s)@);
}

/// What a delta state's `append_n` says: the value `n` times, after
/// those the state had.
#[allow(dead_code)]
fn delta_told_by_a_states_append_n<'a, T: VDeltaValue<C>, C: VCodec>(
    s: &mut VerifiedDeltaEncoderState<'a, T, C>,
    buf: &mut Vec<u8>,
    v: T,
    n: usize,
)
    requires
        old(s).on(old(buf)@),
        v.fits(),
    ensures
        final(s).on(final(buf)@),
        final(s).before(final(buf)@) == old(s).before(old(buf)@),
        final(s)@.len() == old(s)@.len() + n,
        final(s)@.take(old(s)@.len() as int) == old(s)@,
        forall|i: int| old(s)@.len() <= i < final(s)@.len() ==> final(s)@[i] == v,
{
    s.append_n(buf, v, n);
    assert(s@.take(old(s)@.len() as int) =~= old(s)@);
}

/// What a delta state's `extend` says of a vector handed over, whose
/// values fit: its values, after those the state had.
#[allow(dead_code)]
fn delta_told_by_a_states_extend<'a, T: VDeltaValue<C>, C: VCodec>(
    s: &mut VerifiedDeltaEncoderState<'a, T, C>,
    buf: &mut Vec<u8>,
    vs: Vec<T>,
)
    requires
        old(s).on(old(buf)@),
        all_fit(vs@),
    ensures
        final(s).on(final(buf)@),
        final(s).before(final(buf)@) == old(s).before(old(buf)@),
        final(s)@ == old(s)@ + vs@,
{
    s.extend(buf, vs);
}

/// What a delta state's `lemma_fits` says: every value given is one a
/// delta column holds.
#[allow(dead_code)]
proof fn delta_told_by_an_encoder_states_fit<'a, T: VDeltaValue<C>, C: VCodec>(
    s: VerifiedDeltaEncoderState<'a, T, C>,
    buf: Seq<u8>,
    i: int,
)
    requires
        s.on(buf),
        0 <= i < s@.len(),
    ensures
        s@[i].fits(),
{
    s.lemma_fits(buf);
}

/// What `is_uniform` says: whether every value given is the one asked
/// for, either way.
#[allow(dead_code)]
fn delta_told_by_is_uniform<'a, T: VDeltaValue<C>, C: VCodec>(
    s: &VerifiedDeltaEncoderState<'a, T, C>,
    buf: &Vec<u8>,
    v: T,
) -> (r: bool)
    requires
        s.on(buf@),
    ensures
        r ==> forall|i: int| 0 <= i < s@.len() ==> s@[i] == v,
        !r ==> exists|i: int| 0 <= i < s@.len() && s@[i] != v,
{
    s.is_uniform(v)
}

/// What a delta state's `finish` says: the buffer is what was there
/// before the state and then the encoding of the values given, which is
/// a saved delta column of them; and no two numbers given are 2^63 or
/// more apart.
#[allow(dead_code)]
fn delta_told_by_a_finish<'a, T: VDeltaValue<C>, C: VCodec>(s: VerifiedDeltaEncoderState<'a, T, C>, buf: &mut Vec<u8>)
    requires
        s.on(old(buf)@),
    ensures
        final(buf)@ == s.before(old(buf)@) + VerifiedDeltaColumn::<T, C>::encoding(s@),
        VerifiedDeltaColumn::<T, C>::valid(VerifiedDeltaColumn::<T, C>::encoding(s@)),
        VerifiedDeltaColumn::<T, C>::values(VerifiedDeltaColumn::<T, C>::encoding(s@)) == s@,
        delta_span_ok(s@),
{
    s.finish(buf);
}

/// What a delta state's `lemma_before` says: what was in the buffer
/// before the state is still its start.
#[allow(dead_code)]
proof fn delta_told_by_the_bytes_before<'a, T: VDeltaValue<C>, C: VCodec>(
    s: VerifiedDeltaEncoderState<'a, T, C>,
    buf: Seq<u8>,
)
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

/// Values given one by one to a delta state are written as a delta
/// column of them: the buffer is what it was and then the one encoding
/// of the values. The loop carries that the buffer is the state's
/// (`on`), what was there before the state, and what the state has been
/// given so far.
#[allow(dead_code)]
fn delta_streamed_as_saved<T: VDeltaValue<C>, C: VCodec>(input: &Vec<T>, buf: &mut Vec<u8>) -> (r: Range<usize>)
    requires
        all_fit(input@),
    ensures
        final(buf)@ == old(buf)@ + VerifiedDeltaColumn::<T, C>::encoding(input@),
        r.start == old(buf)@.len(),
        r.end == final(buf)@.len(),
        VerifiedDeltaColumn::<T, C>::valid(final(buf)@.subrange(r.start as int, r.end as int)),
        VerifiedDeltaColumn::<T, C>::values(final(buf)@.subrange(r.start as int, r.end as int)) == input@,
        delta_span_ok(input@),
{
    let start = buf.len();
    let mut s = VerifiedDeltaEncoderState::<T, C>::new();
    let mut i: usize = 0;
    proof {
        assert(s@ =~= input@.take(0));
    }
    while i < input.len()
        invariant
            s.on(buf@),
            s.before(buf@) == old(buf)@,
            i <= input@.len(),
            all_fit(input@),
            s@ == input@.take(i as int),
        decreases input@.len() - i,
    {
        s.append(buf, input[i]);
        proof {
            assert(s@ =~= input@.take(i + 1));
        }
        i += 1;
    }
    proof {
        assert(input@.take(i as int) =~= input@);
    }
    s.finish(buf);
    proof {
        let d = VerifiedDeltaColumn::<T, C>::encoding(input@);
        assert(buf@.subrange(start as int, buf@.len() as int) =~= d);
    }
    start..buf.len()
}

/// A delta state, a delta encoder, a delta column and `encode` give the
/// same bytes for the same values.
#[allow(dead_code)]
fn delta_one_encoding_four_ways<T: VDeltaValue<C>, C: VCodec>(input: Vec<T>) -> (r: Vec<u8>)
    requires
        all_fit(input@),
    ensures
        r@ == VerifiedDeltaColumn::<T, C>::encoding(input@),
{
    let mut a: Vec<u8> = Vec::new();
    delta_streamed_as_saved::<T, C>(&input, &mut a);
    let b = delta_encoded_as_saved::<T, C>(&input);
    let col = VerifiedDeltaColumn::<T, C>::from_values(input.as_slice());
    let c = col.save();
    let d = VerifiedDeltaEncoder::<T, C>::encode(input);
    assert(a@ =~= b@);
    assert(b@ == c@);
    assert(c@ == d@);
    d
}

/// Numbers, and then numbers with nulls, through two delta states one
/// after the other into one buffer: it holds what it did, then the
/// first column, then the second.
#[allow(dead_code)]
fn delta_two_streamed(buf: &mut Vec<u8>) -> (r: usize)
    ensures
        final(buf)@ == old(buf)@ + VerifiedDeltaColumn::<u32>::encoding(seq![7u32, 7, 9]) + VerifiedDeltaColumn::<
            Option<i64>,
        >::encoding(seq![Some(-5i64), None, None, Some(i64::MIN)]),
        r == old(buf)@.len() + VerifiedDeltaColumn::<u32>::encoding(seq![7u32, 7, 9]).len(),
{
    let mut x = VerifiedDeltaEncoderState::<u32>::new();
    x.append_n(buf, 7, 2);
    x.append(buf, 9);
    assert(x@ =~= seq![7u32, 7, 9]);
    x.finish(buf);
    let mid = buf.len();
    let mut y = VerifiedDeltaEncoderState::<Option<i64>>::new();
    y.append(buf, Some(-5));
    y.append_n(buf, None, 2);
    y.append(buf, Some(i64::MIN));
    assert(y@ =~= seq![Some(-5i64), None, None, Some(i64::MIN)]);
    y.finish(buf);
    mid
}

/// A delta state knows whether all it was given is one value: so it is
/// of a state that was given nothing, whatever the value, and of one
/// run, and no longer once another value came. And it counts what it
/// was given.
#[allow(dead_code)]
fn delta_uniform_until_another(buf: &mut Vec<u8>) -> (r: (bool, bool, bool, bool, usize))
    ensures
        r.0,
        r.1,
        !r.2,
        !r.3,
        r.4 == 4,
{
    let mut s = VerifiedDeltaEncoderState::<Option<u64>>::new();
    assert(s.on(buf@));
    let a = s.is_uniform(Some(3));
    s.append_n(buf, Some(3), 3);
    let b = s.is_uniform(Some(3));
    // a number that does not fit is none the state was given
    let c = s.is_uniform(Some(u64::MAX));
    assert(s@[0] == Some(3u64));
    s.append(buf, None);
    let d = s.is_uniform(Some(3));
    assert(s@[3] == None::<u64>);
    let n = s.len();
    s.finish(buf);
    (a, b, c, d, n)
}

// ── Delta columns: the decoder ──────────────────────────────────────────

/// A delta decoder read to its end by a `for` loop: the `Vec` holds the
/// values the decoder had left, in order.
#[allow(dead_code)]
fn delta_read_all<'a, T: VDeltaValue<C>, C: VCodec>(dec: VerifiedDeltaDecoder<'a, T, C>) -> (out: Vec<T>)
    ensures
        out@ == dec.rest(),
{
    let ghost all = dec.rest();
    let mut out: Vec<T> = Vec::new();
    for x in at: dec
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

/// A decoder gives the values of a saved delta column from its bytes,
/// with no column built: the values the bytes are the saved column of.
/// For every value type and codec.
#[allow(dead_code)]
fn delta_decode_all<'a, T: VDeltaValue<C>, C: VCodec>(bytes: &'a [u8]) -> (out: Vec<T>)
    requires
        VerifiedDeltaColumn::<T, C>::valid(bytes@),
    ensures
        out@ == VerifiedDeltaColumn::<T, C>::values(bytes@),
{
    delta_read_all(VerifiedDeltaDecoder::<T, C>::new(bytes))
}

/// What a delta column saves, a decoder gives back: the column's
/// values.
#[allow(dead_code)]
fn delta_decoded_as_held<T: VDeltaValue<C>, C: VCodec>(col: &VerifiedDeltaColumn<T, C>) -> (out: Vec<T>)
    ensures
        out@ == col@,
{
    let bytes = col.save();
    delta_decode_all::<T, C>(bytes.as_slice())
}

/// What a delta encoder saves, a decoder gives back: the values the
/// encoder was given, with no column between the two.
#[allow(dead_code)]
fn delta_encoded_then_decoded<T: VDeltaValue<C>, C: VCodec>(input: &Vec<T>) -> (out: Vec<T>)
    requires
        all_fit(input@),
    ensures
        out@ == input@,
{
    let bytes = delta_encoded_as_saved::<T, C>(input);
    delta_decode_all::<T, C>(bytes.as_slice())
}

/// Bytes of which nothing is known: `try_new` checks them, and where it
/// gives a decoder the bytes are a saved delta column, and the decoder
/// gives its values.
#[allow(dead_code)]
fn delta_decode_unknown<'a, T: VDeltaValue<C>, C: VCodec>(bytes: &'a [u8]) -> (r: Option<Vec<T>>)
    ensures
        r matches Some(out) ==> {
            &&& VerifiedDeltaColumn::<T, C>::valid(bytes@)
            &&& out@ == VerifiedDeltaColumn::<T, C>::values(bytes@)
        },
{
    match VerifiedDeltaDecoder::<T, C>::try_new(bytes) {
        Ok(dec) => Some(delta_read_all(dec)),
        Err(_) => None,
    }
}

/// Bytes a load takes are bytes a decoder may read with no check of its
/// own, and it gives the values of the column loaded.
#[allow(dead_code)]
fn delta_decoded_or_loaded<T: VDeltaValue<C>, C: VCodec>(bytes: &[u8]) -> (r: Option<
    (VerifiedDeltaColumn<T, C>, Vec<T>),
>)
    ensures
        r matches Some(p) ==> p.1@ == p.0@,
{
    match VerifiedDeltaColumn::<T, C>::load(bytes) {
        Ok(col) => {
            let out = delta_decode_all::<T, C>(bytes);
            Some((col, out))
        },
        Err(_) => None,
    }
}

/// What `new` says of the decoder it gives: it is of the column the
/// bytes are the saved one of, at its first value, with every value
/// left to give.
#[allow(dead_code)]
fn delta_told_by_a_decoder<'a, T: VDeltaValue<C>, C: VCodec>(bytes: &'a [u8]) -> (r: VerifiedDeltaDecoder<'a, T, C>)
    requires
        VerifiedDeltaColumn::<T, C>::valid(bytes@),
    ensures
        r.column() == VerifiedDeltaColumn::<T, C>::values(bytes@),
        r.pos() == 0,
        r.rest() == VerifiedDeltaColumn::<T, C>::values(bytes@),
{
    VerifiedDeltaDecoder::<T, C>::new(bytes)
}

/// What `try_new` says: where it gives a decoder, the bytes are a saved
/// delta column, and the decoder is the one `new` gives.
#[allow(dead_code)]
fn delta_told_by_a_checked_decoder<'a, T: VDeltaValue<C>, C: VCodec>(bytes: &'a [u8]) -> (r: Result<
    VerifiedDeltaDecoder<'a, T, C>,
    PackError,
>)
    ensures
        r matches Ok(d) ==> {
            &&& VerifiedDeltaColumn::<T, C>::valid(bytes@)
            &&& d.column() == VerifiedDeltaColumn::<T, C>::values(bytes@)
            &&& d.pos() == 0
            &&& d.rest() == VerifiedDeltaColumn::<T, C>::values(bytes@)
        },
{
    VerifiedDeltaDecoder::<T, C>::try_new(bytes)
}

/// What `next` says, in the places of the column: the value at `pos()`,
/// and the decoder one place on; or, at the end, none, and the decoder
/// where it was.
#[allow(dead_code)]
fn delta_told_by_a_decoders_next<'a, T: VDeltaValue<C>, C: VCodec>(dec: &mut VerifiedDeltaDecoder<'a, T, C>) -> (r:
    Option<T>)
    ensures
        final(dec).column() == old(dec).column(),
        match r {
            Some(v) => {
                &&& old(dec).pos() < old(dec).column().len()
                &&& v == old(dec).column()[old(dec).pos() as int]
                &&& final(dec).pos() == old(dec).pos() + 1
                &&& final(dec).rest() == old(dec).rest().skip(1)
            },
            None => {
                &&& old(dec).pos() == old(dec).column().len()
                &&& final(dec).pos() == old(dec).pos()
            },
        },
{
    proof {
        dec.lemma_column();
    }
    dec.next()
}

/// What `nth` says, in the places of the column: the value `n` places
/// on, and the decoder past it; or, where the column ends before that,
/// none, and the decoder at the end.
#[allow(dead_code)]
fn delta_told_by_a_decoders_nth<'a, T: VDeltaValue<C>, C: VCodec>(
    dec: &mut VerifiedDeltaDecoder<'a, T, C>,
    n: usize,
) -> (r: Option<T>)
    ensures
        final(dec).column() == old(dec).column(),
        old(dec).pos() + n < old(dec).column().len() ==> {
            &&& r == Some(old(dec).column()[old(dec).pos() + n])
            &&& final(dec).pos() == old(dec).pos() + n + 1
        },
        old(dec).pos() + n >= old(dec).column().len() ==> {
            &&& r is None
            &&& final(dec).pos() == old(dec).column().len()
        },
{
    proof {
        dec.lemma_column();
    }
    dec.nth(n)
}

/// What `advance_by` says: the decoder is `n` places on, or at the end
/// of the column where that comes first, and what it has left is what
/// it had `past` the first `n`.
#[allow(dead_code)]
fn delta_told_by_a_decoders_advance<'a, T: VDeltaValue<C>, C: VCodec>(
    dec: &mut VerifiedDeltaDecoder<'a, T, C>,
    n: usize,
)
    ensures
        final(dec).column() == old(dec).column(),
        final(dec).rest() == past(old(dec).rest(), n as int),
        old(dec).pos() + n <= old(dec).column().len() ==> final(dec).pos() == old(dec).pos() + n,
        old(dec).pos() + n > old(dec).column().len() ==> final(dec).pos() == old(dec).column().len(),
{
    dec.advance_by(n)
}

/// What `clone` says: a decoder of the same column, at the same place.
#[allow(dead_code)]
fn delta_told_by_a_decoders_copy<'a, T: VDeltaValue<C>, C: VCodec>(dec: &VerifiedDeltaDecoder<'a, T, C>) -> (r:
    VerifiedDeltaDecoder<'a, T, C>)
    ensures
        r.column() == dec.column(),
        r.pos() == dec.pos(),
        r.rest() == dec.rest(),
{
    dec.clone()
}

/// What `next_delta_run_max` says: a run of the column's differences
/// (`deltas`) from `pos()` on, as long as it can be within `max`, and
/// the decoder past it; or none, where `max` is 0 or the decoder is at
/// the end, and the decoder where it was.
#[allow(dead_code)]
fn delta_told_by_a_decoders_run<'a, T: VDeltaValue<C>, C: VCodec>(
    dec: &mut VerifiedDeltaDecoder<'a, T, C>,
    max: usize,
) -> (r: Option<Run<Option<i64>>>)
    ensures
        final(dec).column() == old(dec).column(),
        match r {
            Some(run) => {
                &&& head_run_max(
                    deltas(old(dec).column()).skip(old(dec).pos() as int),
                    run.value,
                    run.count as int,
                    max as int,
                )
                &&& final(dec).pos() == old(dec).pos() + run.count
                &&& final(dec).rest() == old(dec).rest().skip(run.count as int)
            },
            None => {
                &&& max == 0 || old(dec).pos() == old(dec).column().len()
                &&& final(dec).pos() == old(dec).pos()
            },
        },
{
    proof {
        dec.lemma_column();
    }
    dec.next_delta_run_max(max)
}

/// A run of differences a decoder reads stands for the values it passes
/// over: as a `DeltaRun` after the number before it (`delta_prefix`),
/// it is one a column takes (`delta_run_fits`), and its values are the
/// column's at those places, the first of those the decoder had left.
#[allow(dead_code)]
fn delta_run_stands_for<'a, T: VDeltaValue<C>, C: VCodec>(dec: &mut VerifiedDeltaDecoder<'a, T, C>, max: usize) -> (r:
    Option<Run<Option<i64>>>)
    ensures
        final(dec).column() == old(dec).column(),
        r matches Some(run) ==> {
            let (col, p) = (old(dec).column(), old(dec).pos() as int);
            let dr = DeltaRun { prefix: delta_prefix(col, p), delta: run.value, count: run.count };
            &&& p + run.count <= col.len()
            &&& final(dec).pos() == p + run.count
            &&& delta_run_fits::<T>(dr)
            &&& delta_run_values::<T>(dr) == col.subrange(p, p + run.count)
            &&& delta_run_values::<T>(dr) == old(dec).rest().take(run.count as int)
        },
{
    let ghost (col, p) = (dec.column(), dec.pos() as int);
    proof {
        dec.lemma_column();
    }
    let r = dec.next_delta_run_max(max);
    proof {
        if let Some(run) = r {
            assert forall|j: int| p <= j < p + run.count implies delta_at(col, j) == run.value by {
                assert(deltas(col).skip(p)[j - p] == run.value);
            }
            lemma_delta_run_at::<T>(col, p, run.count, run.value);
            assert(col.subrange(p, p + run.count) =~= col.skip(p).take(run.count as int));
        }
    }
    r
}

/// A run passed over and then a value read: the value is the column's
/// at the place after the run.
#[allow(dead_code)]
fn delta_run_then_next<'a, T: VDeltaValue<C>, C: VCodec>(dec: &mut VerifiedDeltaDecoder<'a, T, C>, max: usize) -> (r:
    Option<(usize, T)>)
    ensures
        r matches Some(p) ==> {
            &&& old(dec).pos() + p.0 < old(dec).column().len()
            &&& p.1 == old(dec).column()[old(dec).pos() + p.0]
        },
{
    match dec.next_delta_run_max(max) {
        Some(run) => {
            proof {
                dec.lemma_column();
            }
            match dec.next() {
                Some(v) => Some((run.count, v)),
                None => None,
            }
        },
        None => None,
    }
}

/// The value `n` places on, read by a copy of the decoder moved there:
/// the decoder itself stays where it is.
#[allow(dead_code)]
fn delta_peek_ahead<'a, T: VDeltaValue<C>, C: VCodec>(dec: &VerifiedDeltaDecoder<'a, T, C>, n: usize) -> (r: Option<T>)
    ensures
        dec.pos() + n < dec.column().len() ==> r == Some(dec.column()[dec.pos() + n]),
        dec.pos() + n >= dec.column().len() ==> r is None,
{
    let mut ahead = dec.clone();
    proof {
        dec.lemma_column();
    }
    ahead.advance_by(n);
    ahead.next()
}

/// The first run of differences read from saved bytes, spliced into an
/// empty column as a run after 0: that column holds the first values of
/// the saved one. So a decoder's runs are runs a column takes.
#[allow(dead_code)]
fn delta_first_run_rebuilt<'a, T: VDeltaValue<C>, C: VCodec>(bytes: &'a [u8]) -> (col: VerifiedDeltaColumn<T, C>)
    requires
        VerifiedDeltaColumn::<T, C>::valid(bytes@),
    ensures
        col@.len() <= VerifiedDeltaColumn::<T, C>::values(bytes@).len(),
        col@ == VerifiedDeltaColumn::<T, C>::values(bytes@).take(col@.len() as int),
{
    let mut dec = VerifiedDeltaDecoder::<T, C>::new(bytes);
    let ghost vs = dec.column();
    let mut col = VerifiedDeltaColumn::<T, C>::new();
    proof {
        dec.lemma_column();
    }
    match dec.next_delta_run_max(usize::MAX) {
        Some(run) => {
            let r = DeltaRun { prefix: 0, delta: run.value, count: run.count };
            let mut runs: Vec<DeltaRun> = Vec::new();
            runs.push(r);
            proof {
                assert forall|j: int| 0 <= j < run.count implies delta_at(vs, j) == run.value by {
                    assert(deltas(vs).skip(0)[j] == run.value);
                }
                lemma_delta_run_at::<T>(vs, 0, run.count, run.value);
                lemma_delta_runs_one::<T>(r);
                assert(runs@ =~= seq![r]);
            }
            col.splice_runs(0, 0, runs.as_slice());
            proof {
                assert(col@ =~= vs.subrange(0, run.count as int));
                assert(vs.subrange(0, run.count as int) =~= vs.take(run.count as int));
            }
        },
        None => {
            assert(col@ =~= vs.take(0));
        },
    }
    col
}

// ── Raw columns ───────────────────────────────────────────────────────────────

/// A raw column made of two byte strings, the second spliced in after
/// the first, holds the first and then the second.
#[allow(dead_code)]
fn raw_built(a: &[u8], b: &[u8]) -> (col: RawColumn)
    requires
        a@.len() + b@.len() <= usize::MAX,
    ensures
        col.wf(),
        col@ == a@ + b@,
{
    let mut col = RawColumn::new();
    let first = col.try_splice_slice(0, 0, a);
    assert(first is Ok);
    let n = col.len();
    let second = col.try_splice_slice(n, 0, b);
    assert(second is Ok);
    proof {
        assert(col@ =~= a@ + b@);
    }
    col
}

/// A range of a raw column replaced by other bytes: the column is as
/// long as that makes it, and the bytes before the range and after it
/// are the ones it had.
#[allow(dead_code)]
fn raw_replaced(col: &mut RawColumn, start: usize, end: usize, with: &[u8])
    requires
        old(col).wf(),
        start <= end <= old(col)@.len(),
        old(col)@.len() - (end - start) + with@.len() <= usize::MAX,
    ensures
        final(col).wf(),
        final(col)@.len() == old(col)@.len() - (end - start) + with@.len(),
        final(col)@.subrange(start as int, start + with@.len()) == with@,
        forall|i: int| 0 <= i < start ==> final(col)@[i] == old(col)@[i],
        forall|i: int| end <= i < old(col)@.len() ==> final(col)@[i - (end - start) + with@.len()] == old(col)@[i],
{
    let r = col.try_splice_slice(start, end - start, with);
    assert(r is Ok);
    proof {
        assert(col@.subrange(start as int, start + with@.len()) =~= with@);
    }
}

/// Bytes cut out of a raw column and put back where they were leave the
/// column's bytes as they were.
#[allow(dead_code)]
fn raw_cut_and_pasted(col: &mut RawColumn, start: usize, end: usize)
    requires
        old(col).wf(),
        start <= end <= old(col)@.len(),
    ensures
        final(col).wf(),
        final(col)@ == old(col)@,
{
    // a column is no longer than a `usize` can say, which `len` tells
    let _n = col.len();
    let all = col.save();
    let part = slice_subrange(all.as_slice(), start, end);
    let cut = col.try_splice_slice(start, end - start, &[]);
    assert(cut is Ok);
    let pasted = col.try_splice_slice(start, 0, part);
    assert(pasted is Ok);
    proof {
        assert(col@ =~= old(col)@);
    }
}

/// A raw column emptied by one splice holds nothing.
#[allow(dead_code)]
fn raw_emptied(col: &mut RawColumn)
    requires
        old(col).wf(),
    ensures
        final(col).wf(),
        final(col)@.len() == 0,
{
    let n = col.len();
    let r = col.try_splice_slice(0, n, &[]);
    assert(r is Ok);
    let empty = col.is_empty();
    assert(empty);
}

/// Any bytes at all load as a raw column, which saves to those bytes.
#[allow(dead_code)]
fn raw_round_trip(data: &[u8]) -> (out: Vec<u8>)
    ensures
        out@ == data@,
{
    match RawColumn::load(data) {
        Ok(col) => col.save(),
        Err(_) => {
            assert(false);
            Vec::new()
        },
    }
}

/// A raw column loaded from what another saved holds the same bytes.
#[allow(dead_code)]
fn raw_saved_then_loaded(col: &RawColumn) -> (out: RawColumn)
    requires
        col.wf(),
    ensures
        out.wf(),
        out@ == col@,
{
    let bytes = col.save();
    match RawColumn::load(bytes.as_slice()) {
        Ok(out) => out,
        Err(_) => {
            assert(false);
            RawColumn::new()
        },
    }
}

/// Two raw columns saved into one buffer, one after the other: each is
/// where its range says.
#[allow(dead_code)]
fn raw_two_in_one_buffer(a: &RawColumn, b: &RawColumn) -> (out: (Vec<u8>, Range<usize>, Range<usize>))
    ensures
        out.1.start <= out.1.end == out.2.start <= out.2.end == out.0@.len(),
        out.0@.subrange(out.1.start as int, out.1.end as int) == a@,
        out.0@.subrange(out.2.start as int, out.2.end as int) == b@,
{
    let mut buf: Vec<u8> = Vec::new();
    let first = a.save_to(&mut buf);
    let second = b.save_to(&mut buf);
    proof {
        assert(buf@.subrange(first.start as int, first.end as int) =~= a@);
        assert(buf@.subrange(second.start as int, second.end as int) =~= b@);
    }
    (buf, first, second)
}

/// A range read from a raw column, where the column can give it: the
/// bytes of that range. A range of no bytes within the column is always
/// given.
#[allow(dead_code)]
fn raw_read<'a>(col: &'a RawColumn, start: usize, end: usize) -> (out: Option<&'a [u8]>)
    requires
        col.wf(),
    ensures
        out matches Some(s) ==> start <= end <= col@.len() && s@ == col@.subrange(start as int, end as int),
        start == end && end <= col@.len() ==> out is Some,
{
    match col.try_get(start..end) {
        Ok(s) => Some(s),
        Err(_) => None,
    }
}

/// A range that `try_get` gave is one `get` gives: the call is proved
/// not to panic, and the two slices hold the same bytes.
#[allow(dead_code)]
fn raw_read_again<'a>(col: &'a RawColumn, start: usize, end: usize) -> (out: Option<(&'a [u8], &'a [u8])>)
    requires
        col.wf(),
    ensures
        out matches Some(p) ==> p.0@ == p.1@ && p.0@ == col@.subrange(start as int, end as int),
{
    match col.try_get(start..end) {
        Ok(first) => {
            let second = col.get(start..end);
            Some((first, second))
        },
        Err(_) => None,
    }
}

/// Two values read from the front of a raw column, one after the other:
/// together they are the column's first bytes.
#[allow(dead_code)]
fn raw_two_values<'a>(col: &'a RawColumn, n1: usize, n2: usize) -> (out: (&'a [u8], &'a [u8]))
    requires
        col.wf(),
    ensures
        n1 + n2 <= col@.len(),
        out.0@ + out.1@ == col@.take(n1 + n2),
{
    let mut it = col.iter();
    let a = it.take(n1);
    let b = it.take(n2);
    proof {
        assert(a@ + b@ =~= col@.take(n1 + n2));
    }
    (a, b)
}

/// The bytes at a place of a raw column, read by an iterator made there.
#[allow(dead_code)]
fn raw_taken_at<'a>(col: &'a RawColumn, pos: usize, n: usize) -> (out: &'a [u8])
    requires
        col.wf(),
    ensures
        pos + n <= col@.len(),
        out@ == col@.subrange(pos as int, pos + n),
{
    let mut it = col.iter_at(pos);
    it.take(n)
}

/// An iterator moved over some bytes reads what follows them, and says
/// where it is.
#[allow(dead_code)]
fn raw_skipped<'a>(col: &'a RawColumn, k: usize, n: usize) -> (out: (&'a [u8], usize))
    requires
        col.wf(),
    ensures
        out.0@ == col@.subrange(k as int, k + n),
        out.1 == k + n,
        k + n <= col@.len(),
{
    let mut it = col.iter();
    it.skip(k);
    let s = it.take(n);
    (s, it.pos())
}

/// An iterator moved on to a place reads the bytes there; it only moves
/// on.
#[allow(dead_code)]
fn raw_sought<'a>(col: &'a RawColumn, from: usize, to: usize, n: usize) -> (out: &'a [u8])
    requires
        col.wf(),
    ensures
        from <= to,
        to + n <= col@.len(),
        out@ == col@.subrange(to as int, to + n),
{
    let mut it = col.iter_at(from);
    it.seek_to(to);
    it.take(n)
}

/// A clone of a raw column is a column of its own: bytes given to it
/// leave the one it was made of as it was.
#[allow(dead_code)]
fn raw_clone_then_diverge(col: &RawColumn, more: &[u8]) -> (out: RawColumn)
    requires
        col.wf(),
        col@.len() + more@.len() <= usize::MAX,
    ensures
        out.wf(),
        out@ == col@ + more@,
{
    let mut out = col.clone();
    let n = out.len();
    let r = out.try_splice_slice(n, 0, more);
    assert(r is Ok);
    proof {
        assert(out@ =~= col@ + more@);
    }
    out
}

/// A clone of an iterator reads what the iterator does.
#[allow(dead_code)]
fn raw_read_twice<'a>(col: &'a RawColumn, pos: usize, n: usize) -> (out: (&'a [u8], &'a [u8]))
    requires
        col.wf(),
    ensures
        out.0@ == out.1@,
        out.0@ == col@.subrange(pos as int, pos + n),
{
    let mut a = col.iter_at(pos);
    let mut b = a.clone();
    let x = a.take(n);
    let y = b.take(n);
    (x, y)
}

/// What `new`, `with_max_segments` and `default` say.
#[allow(dead_code)]
fn raw_told_by_new(max: usize) -> (out: (RawColumn, RawColumn, RawColumn))
    ensures
        out.0.wf() && out.1.wf() && out.2.wf(),
        out.0@ == Seq::<u8>::empty(),
        out.1@ == Seq::<u8>::empty(),
        out.2@ == Seq::<u8>::empty(),
{
    (RawColumn::new(), RawColumn::with_max_segments(max), RawColumn::default())
}

/// What `len` and `is_empty` say.
#[allow(dead_code)]
fn raw_told_by_len(col: &RawColumn) -> (out: (usize, bool))
    requires
        col.wf(),
    ensures
        out.0 == col@.len(),
        out.1 == (col@.len() == 0),
{
    (col.len(), col.is_empty())
}

/// What `try_splice_slice` says.
#[allow(dead_code)]
fn raw_told_by_try_splice_slice(col: &mut RawColumn, index: usize, del: usize, bytes: &[u8]) -> (r: Result<
    (),
    PackError,
>)
    requires
        old(col).wf(),
    ensures
        final(col).wf(),
        r is Ok <==> index + del <= old(col)@.len() && old(col)@.len() - del + bytes@.len() <= usize::MAX,
        r is Ok ==> final(col)@ == old(col)@.take(index as int) + bytes@ + old(col)@.skip(index + del),
        r is Err ==> final(col)@ == old(col)@,
{
    col.try_splice_slice(index, del, bytes)
}

/// What `splice_slice` says, of a call that returns.
#[allow(dead_code)]
fn raw_told_by_splice_slice(col: &mut RawColumn, index: usize, del: usize, bytes: &[u8])
    requires
        old(col).wf(),
    ensures
        final(col).wf(),
        final(col)@ == old(col)@.take(index as int) + bytes@ + old(col)@.skip(index + del),
{
    col.splice_slice(index, del, bytes)
}

/// What `try_get` says.
#[allow(dead_code)]
fn raw_told_by_try_get<'a>(col: &'a RawColumn, range: Range<usize>) -> (r: Result<&'a [u8], PackError>)
    requires
        col.wf(),
    ensures
        r is Ok <==> range.start <= range.end <= col@.len() && (range.start == range.end || col.in_one_slab(
            range.start as int,
            range.end as int,
        )),
        r is Ok ==> r->Ok_0@ == col@.subrange(range.start as int, range.end as int),
{
    col.try_get(range)
}

/// What `get` says.
#[allow(dead_code)]
fn raw_told_by_get<'a>(col: &'a RawColumn, range: Range<usize>) -> (out: &'a [u8])
    requires
        col.wf(),
        range.start <= range.end <= col@.len(),
        range.start == range.end || col.in_one_slab(range.start as int, range.end as int),
    ensures
        out@ == col@.subrange(range.start as int, range.end as int),
{
    col.get(range)
}

/// What `iter` and `iter_at` say.
#[allow(dead_code)]
fn raw_told_by_iter<'a>(col: &'a RawColumn, pos: usize) -> (out: (RawColumnIter<'a>, RawColumnIter<'a>))
    requires
        col.wf(),
    ensures
        out.0.wf() && out.1.wf(),
        out.0.bytes() == col@,
        out.1.bytes() == col@,
        out.0.spec_pos() == 0,
        out.1.spec_pos() == pos,
        pos <= col@.len(),
{
    (col.iter(), col.iter_at(pos))
}

/// What `save` and `save_to` say.
#[allow(dead_code)]
fn raw_told_by_save(col: &RawColumn, out: &mut Vec<u8>) -> (r: (Vec<u8>, Range<usize>))
    requires
        col.wf(),
    ensures
        r.0@ == col@,
        final(out)@ == old(out)@ + col@,
        r.1.start == old(out)@.len(),
        r.1.end == final(out)@.len(),
{
    let saved = col.save();
    let range = col.save_to(out);
    (saved, range)
}

/// What `load` and `load_with_max_segments` say.
#[allow(dead_code)]
fn raw_told_by_load(data: &[u8], max: usize) -> (out: (Result<RawColumn, PackError>, Result<RawColumn, PackError>))
    ensures
        out.0 is Ok && out.1 is Ok,
        out.0->Ok_0.wf() && out.1->Ok_0.wf(),
        out.0->Ok_0@ == data@,
        out.1->Ok_0@ == data@,
{
    (RawColumn::load(data), RawColumn::load_with_max_segments(data, max))
}

/// What `clone` says.
#[allow(dead_code)]
fn raw_told_by_clone(col: &RawColumn) -> (out: RawColumn)
    ensures
        out@ == col@,
        col.wf() ==> out.wf(),
{
    col.clone()
}

/// What an iterator's `pos` and `take` say.
#[allow(dead_code)]
fn raw_told_by_take<'a>(it: &mut RawColumnIter<'a>, n: usize) -> (out: (usize, &'a [u8]))
    requires
        old(it).wf(),
    ensures
        out.0 == old(it).spec_pos(),
        old(it).fits(n as nat),
        final(it).wf(),
        final(it).bytes() == old(it).bytes(),
        old(it).spec_pos() + n <= old(it).bytes().len(),
        final(it).spec_pos() == old(it).spec_pos() + n,
        out.1@ == old(it).bytes().subrange(old(it).spec_pos() as int, old(it).spec_pos() + n),
{
    let p = it.pos();
    let s = it.take(n);
    (p, s)
}

/// What an iterator's `skip` says.
#[allow(dead_code)]
fn raw_told_by_skip<'a>(it: &mut RawColumnIter<'a>, n: usize)
    requires
        old(it).wf(),
    ensures
        final(it).wf(),
        final(it).bytes() == old(it).bytes(),
        old(it).spec_pos() + n <= old(it).bytes().len(),
        final(it).spec_pos() == old(it).spec_pos() + n,
{
    it.skip(n)
}

/// What an iterator's `seek_to` says.
#[allow(dead_code)]
fn raw_told_by_seek_to<'a>(it: &mut RawColumnIter<'a>, target: usize)
    requires
        old(it).wf(),
    ensures
        final(it).wf(),
        final(it).bytes() == old(it).bytes(),
        old(it).spec_pos() <= target <= old(it).bytes().len(),
        final(it).spec_pos() == target,
{
    it.seek_to(target)
}

/// What an iterator's `clone` and `default` say.
#[allow(dead_code)]
fn raw_told_by_an_iterators_clone<'a>(it: &RawColumnIter<'a>) -> (out: (RawColumnIter<'a>, RawColumnIter<'a>))
    ensures
        out.0 == *it,
        out.1.wf(),
        out.1.bytes() == Seq::<u8>::empty(),
        out.1.spec_pos() == 0,
{
    (it.clone(), RawColumnIter::default())
}

// ── Where a raw column's slabs meet ────────────────────────────────

/// A value put into a raw column by one splice is read back whole by
/// `get`: no two slabs meet strictly inside the bytes one splice puts
/// in, so the read is proved not to panic.
#[allow(dead_code)]
fn raw_put_then_read(col: &mut RawColumn, index: usize, value: &[u8]) -> (out: Vec<u8>)
    requires
        old(col).wf(),
        index <= old(col)@.len(),
        old(col)@.len() + value@.len() <= usize::MAX,
    ensures
        final(col).wf(),
        final(col)@ == old(col)@.take(index as int) + value@ + old(col)@.skip(index as int),
        out@ == value@,
{
    let r = col.try_splice_slice(index, 0, value);
    assert(r is Ok);
    let end = index + value.len();
    proof {
        if index < end {
            col.lemma_in_one_slab(index as int, end as int);
        }
        assert(col@.subrange(index as int, end as int) =~= value@);
    }
    let s = col.get(index..end);
    slice_to_vec(s)
}

/// Two values put into a new raw column one after the other, and each
/// read back whole by `get`: neither read can panic.
#[allow(dead_code)]
fn raw_two_values_put(a: &[u8], b: &[u8]) -> (out: (RawColumn, Vec<u8>, Vec<u8>))
    requires
        a@.len() + b@.len() <= usize::MAX,
    ensures
        out.0.wf(),
        out.0@ == a@ + b@,
        out.1@ == a@,
        out.2@ == b@,
{
    let mut col = RawColumn::new();
    let first = col.try_splice_slice(0, 0, a);
    assert(first is Ok);
    let ghost one = col;
    let la = a.len();
    let second = col.try_splice_slice(la, 0, b);
    assert(second is Ok);
    let end = la + b.len();
    proof {
        assert(col@ =~= a@ + b@);
        // the slabs meet at most where the two values do
        assert forall|x: int| #[trigger] one.cuts().contains(x) implies x == 0 || x == la by {}
        assert forall|x: int| #[trigger] col.cuts().contains(x) implies x == 0 || x == la || x == end by {
            if x >= end && one.cuts().contains(x - b@.len()) {
                assert(x - b@.len() == la);
            }
        }
        if 0 < la {
            col.lemma_in_one_slab(0, la as int);
        }
        if la < end {
            col.lemma_in_one_slab(la as int, end as int);
        }
        assert(col@.subrange(0, la as int) =~= a@);
        assert(col@.subrange(la as int, end as int) =~= b@);
    }
    let x = slice_to_vec(col.get(0..la));
    let y = slice_to_vec(col.get(la..end));
    (col, x, y)
}

/// The bytes a raw column was loaded from are read back by one `get`,
/// however many they are: a loaded column is one slab.
#[allow(dead_code)]
fn raw_loaded_value_read(data: &[u8]) -> (out: Vec<u8>)
    ensures
        out@ == data@,
{
    let r = RawColumn::load(data);
    let col = r.unwrap();
    let n = data.len();
    proof {
        if 0 < n {
            col.lemma_in_one_slab(0, n as int);
        }
        assert(col@.subrange(0, n as int) =~= data@);
    }
    slice_to_vec(col.get(0..n))
}

/// A range that one raw column gives by `get`, its clone gives too: the
/// slabs of a clone meet where the column's do.
#[allow(dead_code)]
fn raw_clone_reads_alike(col: &RawColumn, start: usize, end: usize) -> (out: (Vec<u8>, Vec<u8>))
    requires
        col.wf(),
        start < end <= col@.len(),
        col.in_one_slab(start as int, end as int),
    ensures
        out.0@ == out.1@,
        out.0@ == col@.subrange(start as int, end as int),
{
    let twin = col.clone();
    proof {
        col.lemma_in_one_slab(start as int, end as int);
        twin.lemma_in_one_slab(start as int, end as int);
    }
    let x = slice_to_vec(col.get(start..end));
    let y = slice_to_vec(twin.get(start..end));
    (x, y)
}

/// Where value `i` begins, of values of these lengths kept one after
/// another.
spec fn raw_start(lens: Seq<usize>, i: int) -> int
    decreases i,
{
    if i <= 0 {
        0
    } else {
        raw_start(lens, i - 1) + lens[i - 1]
    }
}

/// A raw column that holds values of these lengths one after another,
/// each of them whole: its slabs meet only where two values do. This is
/// what a caller that keeps the lengths beside the column, as Automerge
/// does, has to keep true; the clients below show that it can.
spec fn raw_holds(col: RawColumn, lens: Seq<usize>) -> bool {
    &&& col.wf()
    &&& col@.len() == raw_start(lens, lens.len() as int)
    &&& forall|x: int| #[trigger]
        col.cuts().contains(x) ==> exists|j: int| 0 <= j <= lens.len() && x == #[trigger] raw_start(lens, j)
}

/// Values begin in order.
proof fn lemma_raw_start_le(lens: Seq<usize>, i: int, j: int)
    requires
        0 <= i <= j <= lens.len(),
    ensures
        raw_start(lens, i) <= raw_start(lens, j),
    decreases j - i,
{
    if i < j {
        lemma_raw_start_le(lens, i, j - 1);
    }
}

/// Lengths that are alike before `k` have their values begin alike up
/// to it.
proof fn lemma_raw_start_same(a: Seq<usize>, b: Seq<usize>, k: int, i: int)
    requires
        0 <= i <= k,
        k <= a.len(),
        k <= b.len(),
        forall|j: int| 0 <= j < k ==> a[j] == b[j],
    ensures
        raw_start(a, i) == raw_start(b, i),
    decreases i,
{
    if i > 0 {
        lemma_raw_start_same(a, b, k, i - 1);
    }
}

/// A value of `n` bytes put in as value `i` has the values after it
/// begin `n` later.
proof fn lemma_raw_start_inserted(lens: Seq<usize>, i: int, n: usize, j: int)
    requires
        0 <= i <= lens.len(),
        i < j <= lens.len() + 1,
    ensures
        raw_start(lens.insert(i, n), j) == raw_start(lens, j - 1) + n,
    decreases j,
{
    let new = lens.insert(i, n);
    if j == i + 1 {
        lemma_raw_start_same(new, lens, i, i);
    } else {
        lemma_raw_start_inserted(lens, i, n, j - 1);
        assert(new[j - 1] == lens[j - 2]);
    }
}

/// Value `i` taken out has the values after it begin its length sooner.
proof fn lemma_raw_start_removed(lens: Seq<usize>, i: int, j: int)
    requires
        0 <= i < lens.len(),
        i <= j <= lens.len() - 1,
    ensures
        raw_start(lens.remove(i), j) == raw_start(lens, j + 1) - lens[i],
    decreases j,
{
    let new = lens.remove(i);
    if j == i {
        lemma_raw_start_same(new, lens, i, i);
    } else {
        lemma_raw_start_removed(lens, i, j - 1);
        assert(new[j - 1] == lens[j]);
    }
}

/// A value read whole from a raw column that holds values: `get` is
/// proved not to panic, because no two slabs meet inside a value.
#[allow(dead_code)]
fn raw_value_read<'a>(col: &'a RawColumn, lens: &Vec<usize>, i: usize, start: usize) -> (out: &'a [u8])
    requires
        raw_holds(*col, lens@),
        i < lens@.len(),
        start == raw_start(lens@, i as int),
    ensures
        out@ == col@.subrange(start as int, start + lens@[i as int]),
{
    let n = lens[i];
    let _len = col.len();
    proof {
        lemma_raw_start_le(lens@, i + 1, lens@.len() as int);
        assert(raw_start(lens@, i + 1) == start + n);
        if n > 0 {
            assert forall|x: int| start < x < start + n implies !#[trigger] col.cuts().contains(x) by {
                if col.cuts().contains(x) {
                    let j = choose|j: int| 0 <= j <= lens@.len() && x == #[trigger] raw_start(lens@, j);
                    if j <= i {
                        lemma_raw_start_le(lens@, j, i as int);
                    } else {
                        lemma_raw_start_le(lens@, i + 1, j);
                    }
                }
            }
            col.lemma_in_one_slab(start as int, start + n);
        }
    }
    col.get(start..start + n)
}

/// A value put into a raw column that holds values, as value `i` of
/// them: the column holds its values still, the new one among them. So
/// after any number of such splices each value is read whole.
#[allow(dead_code)]
fn raw_value_put(col: &mut RawColumn, lens: &mut Vec<usize>, i: usize, start: usize, value: &[u8])
    requires
        raw_holds(*old(col), old(lens)@),
        i <= old(lens)@.len(),
        start == raw_start(old(lens)@, i as int),
        old(col)@.len() + value@.len() <= usize::MAX,
    ensures
        raw_holds(*final(col), final(lens)@),
        final(lens)@ == old(lens)@.insert(i as int, value@.len() as usize),
        final(col)@ == old(col)@.take(start as int) + value@ + old(col)@.skip(start as int),
        start == raw_start(final(lens)@, i as int),
{
    let ghost a = lens@;
    proof {
        lemma_raw_start_le(a, i as int, a.len() as int);
    }
    let r = col.try_splice_slice(start, 0, value);
    assert(r is Ok);
    let n = value.len();
    lens.insert(i, n);
    proof {
        let b = lens@;
        lemma_raw_start_inserted(a, i as int, n, a.len() as int + 1);
        lemma_raw_start_inserted(a, i as int, n, i + 1);
        lemma_raw_start_same(b, a, i as int, i as int);
        assert forall|x: int| #[trigger] col.cuts().contains(x) implies exists|j: int|
            0 <= j <= b.len() && x == #[trigger] raw_start(b, j) by {
            if x <= start && old(col).cuts().contains(x) {
                let j = choose|j: int| 0 <= j <= a.len() && x == #[trigger] raw_start(a, j);
                if j <= i {
                    lemma_raw_start_same(b, a, i as int, j);
                    assert(x == raw_start(b, j));
                } else {
                    lemma_raw_start_le(a, i as int, j);
                    assert(x == raw_start(b, i as int));
                }
            } else if x >= start + n && old(col).cuts().contains(x - n) {
                let j = choose|j: int| 0 <= j <= a.len() && x - n == #[trigger] raw_start(a, j);
                if j >= i {
                    lemma_raw_start_inserted(a, i as int, n, j + 1);
                    assert(x == raw_start(b, j + 1));
                } else {
                    lemma_raw_start_le(a, j, i as int);
                    assert(x == raw_start(b, i + 1));
                }
            } else {
                assert(x == raw_start(b, i as int) || x == raw_start(b, i + 1));
            }
        }
    }
}

/// Value `i` taken out of a raw column that holds values: the column
/// holds the others still, each whole.
#[allow(dead_code)]
fn raw_value_taken_out(col: &mut RawColumn, lens: &mut Vec<usize>, i: usize, start: usize)
    requires
        raw_holds(*old(col), old(lens)@),
        i < old(lens)@.len(),
        start == raw_start(old(lens)@, i as int),
    ensures
        raw_holds(*final(col), final(lens)@),
        final(lens)@ == old(lens)@.remove(i as int),
        final(col)@ == old(col)@.take(start as int) + old(col)@.skip(start + old(lens)@[i as int]),
{
    let ghost a = lens@;
    let n = lens[i];
    let _len = col.len();
    proof {
        lemma_raw_start_le(a, i + 1, a.len() as int);
        assert(raw_start(a, i + 1) == start + n);
    }
    let r = col.try_splice_slice(start, n, &[]);
    assert(r is Ok);
    lens.remove(i);
    proof {
        let b = lens@;
        assert(col@ =~= old(col)@.take(start as int) + old(col)@.skip(start + n));
        lemma_raw_start_removed(a, i as int, a.len() - 1);
        lemma_raw_start_same(b, a, i as int, i as int);
        assert forall|x: int| #[trigger] col.cuts().contains(x) implies exists|j: int|
            0 <= j <= b.len() && x == #[trigger] raw_start(b, j) by {
            if x <= start && old(col).cuts().contains(x) {
                let j = choose|j: int| 0 <= j <= a.len() && x == #[trigger] raw_start(a, j);
                if j <= i {
                    lemma_raw_start_same(b, a, i as int, j);
                    assert(x == raw_start(b, j));
                } else {
                    lemma_raw_start_le(a, i as int, j);
                    assert(x == raw_start(b, i as int));
                }
            } else if x >= start && old(col).cuts().contains(x + n) {
                let j = choose|j: int| 0 <= j <= a.len() && x + n == #[trigger] raw_start(a, j);
                if j >= i + 1 {
                    lemma_raw_start_removed(a, i as int, j - 1);
                    assert(x == raw_start(b, j - 1));
                } else {
                    lemma_raw_start_le(a, j, i as int);
                    assert(x == raw_start(b, i as int));
                }
            } else {
                assert(x == raw_start(b, i as int));
            }
        }
    }
}

/// What `new`, `with_max_segments` and `default` say of the cuts: a new
/// column has none but its start.
#[allow(dead_code)]
fn raw_cuts_told_by_new(max: usize) -> (out: (RawColumn, RawColumn, RawColumn))
    ensures
        forall|x: int| #[trigger] out.0.cuts().contains(x) ==> x == 0,
        forall|x: int| #[trigger] out.1.cuts().contains(x) ==> x == 0,
        forall|x: int| #[trigger] out.2.cuts().contains(x) ==> x == 0,
{
    (RawColumn::new(), RawColumn::with_max_segments(max), RawColumn::default())
}

/// What the loads say of the cuts: a loaded column has none but its
/// two ends.
#[allow(dead_code)]
fn raw_cuts_told_by_load(data: &[u8], max: usize) -> (out: (RawColumn, RawColumn))
    ensures
        out.0@ == data@,
        forall|x: int| #[trigger] out.0.cuts().contains(x) ==> x == 0 || x == data@.len(),
        forall|x: int| #[trigger] out.1.cuts().contains(x) ==> x == 0 || x == data@.len(),
{
    let a = RawColumn::load(data);
    let b = RawColumn::load_with_max_segments(data, max);
    (a.unwrap(), b.unwrap())
}

/// What `clone` says of the cuts.
#[allow(dead_code)]
fn raw_cuts_told_by_clone(col: &RawColumn) -> (out: RawColumn)
    ensures
        out.cuts() == col.cuts(),
{
    col.clone()
}

/// What `try_splice_slice` says of the cuts.
#[allow(dead_code)]
fn raw_cuts_told_by_try_splice_slice(col: &mut RawColumn, index: usize, del: usize, bytes: &[u8]) -> (r: Result<
    (),
    PackError,
>)
    requires
        old(col).wf(),
    ensures
        r is Ok ==> forall|x: int| #[trigger]
            final(col).cuts().contains(x) ==> spliced_cut(
                old(col).cuts(),
                index as int,
                del as int,
                bytes@.len() as int,
                x,
            ),
        r is Err ==> final(col).cuts() == old(col).cuts(),
{
    col.try_splice_slice(index, del, bytes)
}

/// What `splice_slice` says of the cuts, of a call that returns.
#[allow(dead_code)]
fn raw_cuts_told_by_splice_slice(col: &mut RawColumn, index: usize, del: usize, bytes: &[u8])
    requires
        old(col).wf(),
    ensures
        forall|x: int| #[trigger]
            final(col).cuts().contains(x) ==> spliced_cut(
                old(col).cuts(),
                index as int,
                del as int,
                bytes@.len() as int,
                x,
            ),
{
    col.splice_slice(index, del, bytes)
}

/// What `lemma_in_one_slab` says: bytes lie within one slab exactly when
/// no cut is strictly between them.
#[allow(dead_code)]
fn raw_told_by_the_cuts(col: &RawColumn, start: usize, end: usize)
    requires
        start < end <= col@.len(),
    ensures
        col.in_one_slab(start as int, end as int) <==> forall|x: int|
            start < x < end ==> !#[trigger] col.cuts().contains(x),
{
    proof {
        col.lemma_in_one_slab(start as int, end as int);
    }
}

} // verus!
