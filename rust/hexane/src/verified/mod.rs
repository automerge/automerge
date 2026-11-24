//! A verified public API for columns.
//!
//! [`Column`] is a column whose every operation carries a
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
//! [`Encoder`] takes values one after another and gives the
//! saved column of them, with no column in between.
//! [`EncoderState`] does so into a buffer of its client's,
//! holding only the run in hand. [`Decoder`] gives a saved
//! column's values one after another from its bytes, with no column in
//! between either.
//!
//! [`Edit`] is a cursor that edits a column in one pass from
//! front to back ([`edit`](Column::edit),
//! [`edit_at`](Column::edit_at)): it passes over values, puts
//! values in and takes values out where it stands, and gives the
//! column its values when it is finished
//! ([`finish`](Edit::finish)), as it must be. It edits a
//! column of any kind of index.
//!
//! # Reviewing the specification
//!
//! What this API promises is its specification, and Verus has checked
//! that the code keeps it. To review the promises, read the
//! specification: none of the code that keeps them, and none of the
//! proofs, need be read.
//!
//! The specification is in the files of this module that are not named
//! `imp.rs` (and but for `client_proof.rs` and `model.rs`, below): this
//! file, for the value types, the column and its iterator; `value.rs`,
//! `bytes.rs`, `text.rs`, `codec.rs` and `counts.rs`, for the value types
//! and the codecs, and `bijou.rs` and `leb128.rs`, which name a codec;
//! `kind.rs`, for the kinds of index; `encoder.rs`, `decoder.rs`,
//! `edit.rs`, `load.rs` and `copy.rs`; `prefix.rs`, `prefix_iter.rs` and
//! `sums.rs`, for prefix columns, and `acc.rs` and `acc_iter.rs`, for
//! accumulator columns; `shift.rs`; and `delta.rs` and the files of
//! `delta/`, for delta columns. Of the crate it is in `trusted.rs` (all
//! that is trusted), `raw.rs` (the raw column), `load_opts.rs` (the
//! options of a load), and a few definitions
//! that the specifications use: `window` and `past` (`iter_core.rs`),
//! `concat` (`rle/writer.rs`), `windows` (`window.rs`) and those of
//! `splices.rs` (for `copy_ranges`).
//!
//! In those files:
//!
//! * a public function is its signature and its contract (`requires`,
//!   `ensures`). Its body is one call: of the function of the same name
//!   with `_impl` after it, in the file's module `imp` (`decoder/imp.rs`
//!   for `decoder.rs`, and `imp.rs` for this file), which has the same
//!   contract word for word and is proved there. A body that is one
//!   expression of its own (a call of the codec's, an empty proof) is
//!   left as it was. No body need be read;
//! * an open specification function (`open spec fn`) is a definition, to
//!   be read;
//! * a closed one (`closed spec fn`) is how the specification speaks of
//!   the state of a value, as an iterator's `items()` is what it has
//!   left: its body is the implementation's, which a client cannot see,
//!   and what it means is what the contracts and lemmas that name it say;
//! * a lemma (`proof fn`) states a fact that a client may use in its
//!   proofs. It is proved, and so cannot be wrong: it is there to be
//!   used, and may help to read what is said of a closed function;
//! * a trait that a client implements (`RleValue`, `VNewtype`, `Codec`,
//!   `VAmount`, `VGroup`, `VAccum`, `VAdds`, `Shiftable`) says in its
//!   methods' contracts what it asks of the client, which Verus proves of
//!   the client's implementation;
//! * what is trusted, listed in [What is trusted](#what-is-trusted), is
//!   in one file, `src/trusted.rs`, to be read whole.
//!
//! `client_proof.rs` is a client of this API and no part of it: it shows
//! what a client can prove with the specification, and is checked again
//! as a crate outside hexane (`verus-client`). `model.rs` and the `imp`
//! files are the implementation.
//!
//! # Value types
//!
//! A `Column<V>` holds values of a type `V` that is a
//! [`ColumnValueRef`]:
//!
//! * a number: `u64`, `i64`, `u32`, `i32`, `usize` or `NonZeroU32`;
//! * `bool`;
//! * a client's own type that is stored as one of those numbers
//!   ([`VNewtype`]). The client says which number each value is, and a
//!   load refuses bytes that hold a number that is none of its values;
//! * a client's own type with an encoding of its own, of one to ten
//!   bytes a value ([`RleValue`]): the client writes and reads the bytes,
//!   and proves that they keep the laws a column relies on;
//! * a byte string: `Vec<u8>`;
//! * `Option<T>` of a number, a client's type or a byte string `T`:
//!   `None` is a column's null;
//! * a string: `String`, and `Option<String>`, whose `None` is a
//!   column's null.
//!
//! A column takes and gives a value in the type's form, [`Get<'a, V>`](Get).
//! For a number, an `Option` of one or a `bool` that is the type itself.
//! A column of `Vec<u8>` takes a byte string as a slice (`&[u8]`) and
//! gives one as a slice borrowed from the column, with no copy; one of
//! `Option<Vec<u8>>` takes and gives `Option<&[u8]>`. A column of
//! `String` takes a string as a `&str` and gives one as a `&str`
//! borrowed from the column, with no copy; one of `Option<String>`
//! takes and gives `Option<&str>`.
//!
//! A column of strings holds UTF-8, as a `str` does. On the wire a
//! string is the byte string of its UTF-8, and a load refuses bytes
//! where a string is not UTF-8 ([`PackError::InvalidUtf8`]). This
//! module has no `unsafe`, so what makes a `&str` of a column's bytes is
//! the standard library's check of them (`core::str::from_utf8`), at
//! each read: it is proved to find them UTF-8, and it looks at every
//! byte of the string. [`Column`](crate::Column) reads a string without
//! that check.
//!
//! In a specification a column is the sequence of its values (`col@`),
//! each as its type's [`VSpec::S`]: a number as itself, a byte string as
//! its bytes (`Seq<u8>`), a string as its characters (`Seq<char>`). What
//! a value in a column's form is there is its `sv`, and `svs` of several
//! ([`VGet`]): for a slice or a `&str` `v` that is `v@`, and for a number
//! the number. What a value of the type itself is there (a `Vec<u8>` for
//! a byte string, a `String` for a string) is its `VSpec::sv_of`.
//!
//! The values of a type are in an order (`VSpec::below`), which a
//! search goes by: numbers in the order of numbers, `false` before
//! `true`, byte strings in the order of their bytes with a string before
//! every longer one that starts with it (`bytes_below`), strings in
//! that order of their UTF-8 bytes (`bytes_below` of
//! `vstd::utf8::encode_utf8` of their characters), and `None` before
//! every `Some`. That is the order Rust has for these types (`Ord`). A
//! client's type is in the order of the numbers it is stored as,
//! whatever order it may have in Rust.
//!
//! The second parameter of `Column<V, C>` is the codec the
//! column's numbers are written with, a [`Codec`]. It is [`Leb128`]
//! where none is named. The other of this crate's is `Bijou64`, behind
//! the feature `bijou64`, and a client may write its own ([`Codec`]
//! says what it must keep). The modules [`leb128`] and `bijou` give
//! every column type with its codec named, so that one import picks
//! the codec. [`ulebsize`] and [`lebsize`] say how many bytes a number
//! takes in LEB128.
//!
//! # Prefix columns
//!
//! A [`PrefixColumn`] is a column kept with the sums of its
//! values: a `Column` whose third parameter, the kind of index
//! it is kept with ([`VIndex`]), is [`SumIndex`], where a plain
//! column's is [`LenIndex`]. Its values are of a type that is summed
//! ([`PrefixValue`]): `u64`, `i64`, `u32` or `NonZeroU32`, or an `Option`
//! of one, whose `None` adds nothing, or `bool`, whose `true` adds
//! one. The sums are kept in a `u128` for `u64`, an `i128` for `i64`,
//! a `u64` for `u32` and `NonZeroU32`, and a `usize` for `bool`
//! ([`PrefixValue::Sum`]).
//!
//! Every method of a column is a prefix column's, with the
//! specification it has of a plain one, and a prefix column saves the
//! bytes a plain column of its values does. What differs is that there
//! is a prefix column only of values whose sums fit the type they are
//! kept in (`sums_fit`: what the first `k` of them add up to, `summed`,
//! is a value of the type, for every `k`). A prefix column says so of
//! its values (`lemma_kept`). A load takes a saved column of such
//! values where it would take it as a plain column, and refuses one of
//! other values; and an edit that would leave other values does not
//! return: it panics. No column is long enough for that to happen to
//! sums of `u64`s, of `i64`s or of `bool`s (`lemma_sums_fit`). One of
//! `u32`s is, if it has more than 2^32 + 1 of them.
//!
//! A prefix column answers from its sums, without reading the values
//! they are of: what the values before a position add up to
//! ([`get_prefix`](Column::get_prefix)), what those through it
//! do ([`get_total`](Column::get_total)), and what those of a
//! range do ([`sum_range`](Column::sum_range)). Their
//! specifications say the answer as an integer, `summed` of the values,
//! which the sum returned is (`VSum::as_int`). Where the sums are never
//! negative ([`UnsignedPrefix`]: every summed type but `i64` and `Option<i64>`),
//! they do not fall from one position to the next, and the column is
//! searched by sum: for the first position that the values before it
//! add up to a target
//! ([`get_index_for_prefix`](Column::get_index_for_prefix)), and
//! for the first that the values through it do
//! ([`get_index_for_total`](Column::get_index_for_total)).
//!
//! A prefix column is also read with its sums, value by value:
//! [`prefix_iter`](Column::prefix_iter) gives a
//! [`PrefixIter`], an iterator that moves as a [`Iter`]
//! does and keeps the sum of the values it has passed. What it gives is
//! each value as a [`PrefixedValue`], with what the values
//! through it add up to and what those before it do; a long move
//! ([`nth`](Iterator::nth), `advance_to`, `shift`) takes the sum of
//! what it passes over from the index. Its specifications say the
//! running sum as an integer too: it is `summed` of the column's values
//! from where the sum counts from (the column's start, or where
//! [`reset_prefix`](PrefixIter::reset_prefix) was called) up to
//! the iterator. [`get_prefixed`](Column::get_prefixed) is one
//! value read so.
//!
//! The iterator reads by runs too
//! ([`next_run`](PrefixIter::next_run)), and reads on to a
//! value it is asked for by count
//! ([`delta_nth`](PrefixIter::delta_nth)) or by sum
//! ([`advance_prefix`](PrefixIter::advance_prefix), the first
//! value at which what it has passed adds up to more than a number):
//! what it finds is a [`PrefixSeek`], the value with its
//! position and with what the values passed over add up to. It is put
//! away and taken up again as a [`PrefixIterState`], which
//! keeps where the sum counts from; the sum itself is asked of the
//! column's index on resuming. [`delta`](Column::delta) is the
//! value at a position with the sum of a range before it.
//!
//! That a prefix column refuses the saved columns
//! [`PrefixColumn`](crate::PrefixColumn) refuses, with the same error,
//! and that its edits panic where `PrefixColumn`'s do, with the same
//! words, is tested, as is that an edit whose sums fit does not panic.
//!
//! # Accumulator columns
//!
//! A [`AccColumn`] is a column kept with what its values add up
//! to in a type its client gives: a `Column` whose kind of
//! index is [`AccIndex`]. The type is an accumulator ([`VAccum`]), and
//! its client proves the laws of it ([`VGroup`]): that its addition is
//! associative, that its zero adds nothing, and that what was added can
//! be taken away again. Two accumulators need not add up to the same in
//! either order. The column's values are of a type of the client's own
//! that says what a value adds ([`VAdds`]), or an `Option` of one, whose
//! `None` adds nothing ([`VAccumulated`]).
//!
//! Every method of a column is an accumulator column's, with the
//! specification it has of a plain one, and an accumulator column saves
//! the bytes a plain column of its values does. There is one of any
//! values, since nothing overflows: a load takes whatever it would take
//! as a plain column, and no edit panics for what the values add up to.
//! From its index the column answers what the values before a position
//! add up to (`get_prefix`), what those through it do (`get_total`) and
//! what those of a range do (`sum_range`): each is `accumulated` of
//! those values, which adds them in the order the column holds them.
//!
//! `prefix_iter` of an accumulator column gives a [`AccIter`]:
//! an iterator that keeps what the values it has passed add up to, and
//! gives each value with what the values through it add up to and what
//! those before it do ([`AccValue`]). It moves as a
//! [`PrefixIter`] does, but for the search by sum, which an
//! accumulator has no order for. A move that finds a value gives it
//! with what the values passed over add up to ([`AccSeek`]),
//! and the iterator's place is kept without the borrow of the column
//! in a [`AccIterState`]. An accumulator need not be `Copy`:
//! what the iterator gives holds a copy of its running sum, made as
//! the accumulator says one is (`dup`).
//!
//! [`PrefixColumn`](crate::PrefixColumn) takes the laws of its prefix
//! type on trust. Here they are the client's to prove, and what the
//! column answers is proved from them: Verus checks a client's
//! accumulator when it is run on the client's crate.
//!
//! # Raw columns
//!
//! [`RawColumn`] is the one column type the two APIs share: it is
//! [`hexane::RawColumn`](crate::RawColumn), whose module is verified as
//! it stands, given here with its specifications. A raw column is a
//! sequence of bytes and no more (its view): a splice is a splice of
//! that sequence, a read gives its bytes, it saves as its bytes and
//! loads from any. It does not know where a value begins or ends. A
//! read gives a slice of the column's own memory, which is kept in
//! slabs, and so can give only bytes that lie within one slab
//! (`in_one_slab`; `fits` for a [`RawColumnIter`]).
//!
//! Where the slabs meet is specified too (`cuts`). A new column has no
//! cut within it, nor has a loaded one, and a splice leaves a cut only
//! where one was or at an end of the bytes it puts in (`spliced_cut`);
//! and bytes lie within one slab exactly when no cut is strictly
//! between them (`lemma_in_one_slab`). So a caller that splices only
//! where its values meet can prove that each value lies within one
//! slab, and that `get` gives it without a panic.
//!
//! Its methods say what they do of every input, of a raw column
//! (`wf()`, which a raw column has when it is made and keeps): where one
//! panics, as `splice_slice` does out of range, its specification is of
//! the calls that return. Besides `wf()`, `get` alone has a `requires`
//! in its place. The two
//! methods that take an iterator, `splice` of chunks and `copy_ranges`,
//! are not verified code and cannot be called from it; `splice_slice`
//! does what `splice` does with the chunks put together.
//!
//! # What is proved
//!
//! Of every method, its `ensures`, for every input that meets its
//! `requires`, for every value type and codec. In particular:
//!
//! * [`splice`](Column::splice) leaves the values before `at`,
//!   the values given, then the values from `at + del`;
//! * [`splice_iter`](Column::splice_iter) does so with the values
//!   an iterator gives, and [`splice_runs`](Column::splice_runs)
//!   with runs, each a value and how many times to repeat it (their
//!   values are `run_values`). `extend` appends what an iterator gives,
//!   and `collect()` makes a column of it;
//! * [`from_values`](Column::from_values) gives a column of the
//!   values of a slice, and [`fill`](Column::fill) one of a value
//!   repeated;
//! * [`remove_n`](Column::remove_n),
//!   [`truncate`](Column::truncate) and
//!   [`clear`](Column::clear) leave the values a `splice` that
//!   deletes the same ones would;
//! * [`get`](Column::get) and [`to_vec`](Column::to_vec)
//!   give the column's values;
//! * the [iterator](Iter) is one of the standard library's
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
//! * the iterator says where it stands: [`pos`](Iter::pos) is
//!   the index in the column of the value it gives next, and
//!   [`end_pos`](Iter::end_pos) is where its window ends. What it
//!   has left is the values of its column (`column()`) between the two,
//!   as far as the column goes (`window`), and `next` and `nth` say where
//!   they leave it. [`set_max`](Iter::set_max) moves the window's
//!   end, [`advance_to`](Iter::advance_to) and
//!   [`advance_by`](Iter::advance_by) move the iterator on (what
//!   it has left is then `past` what it had),
//!   [`shift`](Iter::shift) moves the window to a range ahead,
//!   and [`shift_next`](Iter::shift_next) gives the first value
//!   there;
//! * [`next_run`](Iter::next_run) gives the value the iterator
//!   gives next and how many times it comes before another value does
//!   or the iterator has no more (`head_run`), and moves past them all.
//!   [`runs`](Iter::runs) makes of the iterator one of such
//!   runs ([`Runs`]). It gives the runs of what the iterator
//!   had left (`runs_of`), which are those values
//!   (`lemma_runs_of_values`) with no two runs side by side of one
//!   value, and are what `splice_runs` takes;
//! * [`suspend`](Iter::suspend) gives what an iterator needs to
//!   go on later ([`IterState`]), which holds no borrow of the
//!   column, and [`try_resume`](IterState::try_resume) gives an
//!   iterator from it exactly when the state fits the column it is
//!   given (`fits`): one that stands where the first stood and has left
//!   that column's values in the same window. The state of an iterator
//!   fits the column the iterator reads (`source()`), which no method of
//!   the iterator changes, and whose values are the iterator's
//!   (`lemma_source`);
//! * [`is_only`](Column::is_only) says whether every value of the
//!   column is the one given (`only`), and [`ColumnValueRef::same`] whether two
//!   values in a column's form are one value;
//! * [`scope_to_value`](Column::scope_to_value) finds a value
//!   among values in order (`sorted`): it gives the range of the places
//!   that hold it, which has the values that come before it on one side
//!   and those that come after it on the other, and is so the empty
//!   range at the place where the value would stand where none holds
//!   it. The values need be in order only as far as the value looked for
//!   can tell (`sorted_for`). Of values in no order it gives some range
//!   within the one searched. [`ColumnValueRef::compare`] says how two values in
//!   a column's form stand in the order;
//! * [`remap`](Column::remap) puts in each place what a function
//!   of the client's gave for the value that was there (`mapped`). The
//!   function is handed a value of the type itself (for a byte string a
//!   `Vec<u8>` of its own, for a string a `String`) and has to accept
//!   every value of the column.
//!   Nothing is assumed of it: what it gives is said by its own
//!   `ensures`, and by nothing else;
//! * `clone` gives a column with the same values. It is a column of its
//!   own, as Rust's ownership has it: an edit of either says nothing of
//!   the other, whose values so stay what they were;
//! * [`save`](Column::save) writes the one encoding of the
//!   column's values (`encoding`): bytes that are `valid` and hold those
//!   `values`, and no more of them than the values' `size_bound`. Two
//!   columns of the same values so save to the same bytes
//!   (`lemma_save_unique`). [`save_to`](Column::save_to) writes
//!   that encoding after what a buffer holds, which it keeps, and
//!   [`save_to_unless`](Column::save_to_unless) writes nothing
//!   where every value is the one given;
//! * [`validate_encoding`](Column::validate_encoding) saves the
//!   column and reads the bytes through, and returns no error for a
//!   column that saves to less than 4 GiB: what it looks for at run
//!   time is proved of `save`;
//! * [`load`](Column::load) gives a column only for valid bytes,
//!   and then the column of their values; and it does give one for all
//!   valid bytes short of 4 GiB that hold values a `usize` can count
//!   (`loadable`);
//! * [`load_with`](Column::load_with) is `load` with options
//!   ([`LoadOpts`]). Asked for a length, it gives a column only of that
//!   many values, and does give one for loadable bytes that hold that
//!   many. Given a fill value too, it makes of no bytes a column of the
//!   value that many times, for every length a column can have
//!   (`fill_of` says when); for a longer one it returns an error, since
//!   no bytes are the saved column of no values (`lemma_no_bytes`);
//! * [`slab_lens`](Column::slab_lens) gives numbers that add up
//!   to the column's length (`sum_of`);
//! * a [`Encoder`] is specified by the values it has been given
//!   (`enc@`): each `append` adds its value to them, `append_n` adds it
//!   that many times, and `extend` adds what an iterator gives. Its
//!   [`save`](Encoder::save) writes the `encoding` of them, which
//!   is what a column of those values saves, and
//!   [`save_to`](Encoder::save_to) writes it after what a buffer
//!   holds. An encoder asks of its client what a column does not: every
//!   method requires `wf()`, which `new` gives and every method keeps;
//! * a [`EncoderState`] is specified by the values it has been
//!   given too (`state@`), and writes into a buffer that each call is
//!   handed. Its [`finish`](EncoderState::finish) leaves the
//!   buffer holding what it held before the state wrote to it
//!   (`before`) and then the `encoding` of the values. What it asks of
//!   its client is that the buffer be the one it has written to, with
//!   those bytes as they were left (`on`): every method that takes the
//!   buffer requires it, a state that was given nothing has it of any
//!   buffer, and nothing checks it as the program runs. The static
//!   forms ([`encode`](Encoder::encode),
//!   [`encode_to`](Encoder::encode_to),
//!   [`encode_to_unless`](Encoder::encode_to_unless)) give a
//!   state the values of an iterator and finish it;
//! * a [`Decoder`] is specified, as an iterator of a column
//!   is, by what it has left to give (`items()`, and as values
//!   `rest()`). [`new`](Decoder::new) requires that the bytes
//!   be a saved column (`valid`) and gives a decoder that has its
//!   `values` left, all of them;
//!   [`try_new`](Decoder::try_new) reads the bytes through
//!   first, and gives such a decoder or an error. `next`, `nth` and
//!   [`advance_by`](Decoder::advance_by) are an iterator's.
//!   [`next_run_max`](Decoder::next_run_max) gives the next
//!   run counted up to a bound (`head_run_max`), and
//!   [`next_run`](Decoder::next_run) up to `usize::MAX`;
//!   [`scan_for`](Decoder::scan_for) passes the runs that come
//!   before a target and says how many values those were and how many
//!   of the target follow (`scan_of`), and how far it has moved
//!   (`scan_taken`);
//! * a [`Edit`] is specified by the values of the column it
//!   was opened on (`orig()`), where it stands among them (`at()`) and
//!   the values it has written (`written()`): each step says what it
//!   adds to those written and how far it goes on, and
//!   [`finish`](Edit::finish) gives the column the values
//!   written and then the original ones from where the cursor stands
//!   (`now()`). A cursor asks of its client what an encoder does:
//!   every method requires `wf()`, which opening it gives and every
//!   step keeps. A seek goes forward only, which it requires; and a
//!   cursor that is not finished leaves its column without its values,
//!   as a panic in the middle of an edit does.
//!
//! `valid`, `values`, `encoding` and `loadable` are specification
//! functions of a column type (`Column::<V>::valid(bytes)`),
//! `on` and `before` are of a state (`state.on(bytes)`),
//! `size_bound` is one of a value type (for byte strings it counts their
//! bytes, as `total_len` of them, and for strings their UTF-8 bytes, as
//! `total_str_len`; `total_len_opt` and `total_str_len_opt` where they
//! may be null), `only`, `sorted`, `sorted_for`,
//! `bytes_below`, `mapped`, `folded`, `window`, `past`, `head_run`,
//! `head_run_max`, `head_len`, `scan_of`, `scan_taken`,
//! `runs_of`, `run_values`, `gives`, `fill_of` and `sum_of` are of this
//! module,
//! and `lemma_save_unique`, `lemma_encoding`, `lemma_no_bytes`,
//! `lemma_sorted`, `lemma_run_values_push`, `lemma_run_values_add`,
//! `lemma_runs_first`, `lemma_runs_of`, `lemma_runs_of_values`,
//! `lemma_head_run_max` and `lemma_head_len` are
//! lemmas, as are `VSpec::lemma_below` (the order is a strict total
//! order), `Iter::lemma_items_len`, `Iter::lemma_rest`,
//! `Decoder::lemma_items_len`,
//! `Iter::lemma_source` and
//! `EncoderState::lemma_before`: they exist only for Verus, and
//! a normal build compiles them away. What options hold is
//! said by `length()`, `fill()` and `max_segments()` of a [`LoadOpts`],
//! and what fill value there is by `value()` of its fill state
//! ([`MaybeFill`]).
//!
//! What a method does with an iterator is stated as Verus states what
//! the standard library does with one: of the values the iterator is to
//! give (`remaining()`), for an iterator that keeps to Verus's laws of
//! iterators (`obeys_prophetic_iter_laws`: what `next` returns is the
//! first of what remains, and `None` when nothing does). `gives` says
//! so once, and the contracts of those methods use it. Of an iterator
//! that does not keep to the laws, a method promises nothing but that
//! the column is still a column. What a client's iterator is to give, the client proves from
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
//! [`slab_count`](Column::slab_count),
//! [`slab_lens`](Column::slab_lens),
//! [`slab_segments`](Column::slab_segments) and what
//! [`dump_slabs`](Column::dump_slabs) and `Debug` print tell of
//! it, as does an iterator's [`get_slab`](Iter::get_slab), and
//! promise nothing but that the lengths add up. Nor is how
//! often [`remap`](Column::remap) calls its function: once for
//! each run of equal values, as `Column`'s does, which is tested. After
//! the same edits a `Column` has the slabs of a
//! [`Column`](crate::Column), each as long and of as many runs, and
//! `dump_slabs` prints the same lines. That is tested too, but for one
//! case: a column that [`clear`](Column::clear) has emptied has
//! no slab, where `Column` keeps an empty one, until the next edit.
//!
//! That an edit of a column ends what was suspended of it is proved. A
//! state fits only a column that counts the edits the state does
//! (`edits()`, `lemma_fits`), and each method that changes a column says
//! what the column counts after it: one more (`after_edit`) where the
//! call was an edit, and what it counted before where it was none
//! (`after_call` of the count and whether the call was an edit). A
//! call is an edit if it deletes a value or inserts one, a value put in
//! the place of its equal too, and a `remap` is one always. A cursor is
//! one edit, counted when it is finished, if a step of it deleted a
//! value or inserted one; a value it replaced with its equal
//! ([`replace_with`](Edit::replace_with)) is none. The count is
//! a `usize` that wraps around, as `Column`'s is: after one edit or
//! more, and no more of them than `usize::MAX`, it is not what it was
//! (`lemma_after_edits`), so a state taken before them is refused. The
//! column sets the count itself after each call. That the calls which
//! end what was suspended of a `Column` are those that end it here is
//! tested, as is that after a call which is no edit a state resumes
//! where `Column`'s does. And a state that does resume cannot misread:
//! the iterator given has the values of the column it was resumed on,
//! in the state's window, whatever column the state was taken of.
//!
//! # What is trusted
//!
//! Every function that Verus takes on trust on a path of this API is in
//! one file, `src/trusted.rs`, which says what each assumes. (Code
//! outside `verus!` is not checked at all. Of that, this API runs only
//! the functions named below as not checked. What the old API takes on
//! trust besides, and no function of this API runs, is left where it
//! is; `src/trusted.rs` says where.)
//!
//! Verus and its solver Z3, and Verus's specifications of the standard
//! library: of `Vec` and slices, and for a column of `NonZeroU32` those
//! of `NonZero`, which Verus keeps behind its feature
//! `nonzero_internals` (the crate is checked with it on). A column of
//! byte strings leans on more of them than one of numbers does: that a
//! slice is its bytes and no more, that a part of a slice is that part of
//! its bytes, and the copy of a slice into a `Vec`. One of strings
//! leans on those of `str` and `String` besides: that the bytes of a
//! `str` are the UTF-8 of its characters (`as_bytes`), what the copy of a
//! `str` into a `String` holds, and what a `String` lends (`as_str`). Of
//! UTF-8 itself nothing is assumed: Verus's library proves what is used
//! of it here (`vstd::utf8`). `clone` leans on
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
//! Of this crate, the functions Verus is told to take as they are (all in
//! `src/trusted.rs`), which are few and small, and are those the verified
//! core of [`Column`](crate::Column) has too:
//!
//! * the helpers that panic, which say why and do nothing else. A method
//!   panics where a column would outgrow what its arithmetic can count
//!   (some `usize::MAX` values, or a slab of 4 GiB) or what the format
//!   can write (a run of more than `i64::MAX` equal values that are
//!   neither nulls nor `bool`s, which
//!   [`splice_runs`](Column::splice_runs) can ask for in one
//!   call), and, like every Rust function, a method of this API may run
//!   out of memory;
//! * the constructors of [`PackError`], of which nothing is assumed
//!   ([`refused`] is the one for a client's type);
//! * three specifications of standard-library calls that Verus has none
//!   for: replacing a range of a `Vec` of bytes (`splice_bytes`) and of a
//!   `Vec` of slabs (`splice_vec`), and taking a range out of a raw
//!   column's `Vec` of slabs (`drain_slabs`);
//! * for a column of strings, two more things that Verus does not say.
//!   One is a specification of the standard
//!   library's `core::str::from_utf8`: that the `str` it gives is of the
//!   bytes it was given, that it gives one for all bytes that are UTF-8,
//!   and that it does not panic. (With it goes the name of its error
//!   type, `Utf8Error`, of which nothing is said.) Verus wants the
//!   specification of a public function to be public: a crate that is
//!   checked together with this one has this specification of
//!   `from_utf8`, and cannot give one of its own. The other is that two
//!   `str`s of the same characters are one `str` (`axiom_str_same`),
//!   which Verus says of slices and not of `str`;
//! * four helpers of which nothing is assumed: an addition of counts
//!   whose sum the proofs take as unknown (`add`), the reuse of a buffer
//!   (`recycle_buf`), the taking of one (`take_buf`), and a constant
//!   (`carry_budget`).
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
//! [`dump_slabs`](Column::dump_slabs), whose printing of what
//! checked code has read of each slab is not checked; and
//! `check_invariants`, a tool for tests that these docs leave out, as
//! [`Column`](crate::Column)'s do. It looks at run time for what is
//! proved of every column (a slab's bytes hold the values and runs it
//! counts, and the index counts the slabs' lengths) and for the layout
//! policy, which is not proved (a slab's runs are within the budget,
//! and no two neighbours should have been merged), and panics if it
//! finds otherwise. As `Column`'s, it takes every column as one that
//! edits have made, and panics for some that were loaded: a load leaves
//! a short last slab unmerged. `Debug` of a [`Encoder`] is not
//! checked either: it prints the number that the checked `len` returns.
//!
//! That a load returns an error for what a column cannot hold, and does
//! not panic, is not proved: Verus is not asked to show that the helpers
//! that panic are out of a load's reach. It is tested
//! (`tests/verified_api.rs`), on targets of 64 and of 32 bits: for a fill
//! longer than a column, and for bytes of more values than one holds.
//!
//! A client's own value type is checked with the client: Verus proves
//! what [`VNewtype`] asks of it when it is run on the client's crate,
//! or [`RleValue`] of one with an encoding of its own, and so what
//! [`VAmount`] asks of one that a prefix column adds up, and what
//! [`VGroup`], [`VAccum`] and [`VAdds`] ask of an accumulator and of the
//! values added up in it. So too what [`Codec`] asks of a client's own
//! codec, and [`Shiftable`] of a client's own repositionable iterator.
//! A client that is compiled without Verus has that to keep by itself,
//! as it has every `requires` of this API.
//!
//! That a splice returns is proved where it takes a slice:
//! [`splice`](Column::splice), `from_values`, `insert`, `push`,
//! `remove`, `remove_n`, `truncate` and `clear` of every column, every
//! splice of a delta column (which takes only slices), and
//! `copy_ranges`. (Verus checks that each loop ends; that a function
//! calls only functions that end, it does not: each loop on these paths
//! has a measure.) A splice of a caller's iterator or runs runs the
//! core's loop over the values to insert, which Verus is not asked to
//! show ends. It ends when the iterator does: with an iterator that
//! never ends,
//! [`splice_iter`](Column::splice_iter),
//! [`splice_runs`](Column::splice_runs), `extend` and `collect()`
//! do not return (they run until memory is out, or a column's length is
//! more than it can count, which panics). Nor is it proved that an edit
//! cursor's [`insert_runs`](Edit::insert_runs) returns, or an
//! encoder's [`extend`](Encoder::extend), or a state's
//! ([`EncoderState::extend`]), or a static form that is built on
//! one: each is a loop over the client's iterator. Every other loop is
//! proved to end.

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
pub use acc::{AccColumn, AccIndex, VAccum, VAccumulated, VAdds, VGroup};
pub use acc_iter::{AccIter, AccIterState, AccSeek, AccValue};
#[cfg(verus_keep_ghost)]
pub use bytes::{below_at, bytes_below, total_len, total_len_opt};
pub use codec::{lebsize, ulebsize, Codec};
pub use decoder::Decoder;
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
    DeltaColumn, DeltaDecoder, DeltaEdit, DeltaEncoder, DeltaEncoderState, DeltaIter,
    DeltaIterState, DeltaRuns, DeltaValue, FindByRange, VDelta, VDeltaNum,
};
pub use edit::Edit;
pub use encoder::{Encoder, EncoderState};
pub use kind::{LenBitIndex, LenIndex, VIndex, VKeeps};
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
pub use sums::{PrefixValue, UnsignedPrefix, VAmount, VSum};
#[cfg(verus_keep_ghost)]
pub use text::{total_str_len, total_str_len_opt};
pub use value::{refused, RleValue, VNewtype};

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
use counts::VB;
use kind::KCore;
#[cfg(verus_keep_ghost)]
use model::{Model, Streams};
use std::cmp::Ordering;
use std::ops::Range;
#[allow(unused_imports)]
use vstd::prelude::*;
#[cfg(verus_keep_ghost)]
use vstd::std_specs::iter::{FromIteratorSpecImpl, IteratorSpec};

mod seal {
    use super::model::{Flows, Loading, Model, Streams};
    use super::{Items, VForm, VGet, VSpec};
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
    pub trait Sealed<C>: Sized + VSpec + for <'a>VForm<'a> {
        /// How the values are read: as the form the type gives them in.
        type X: for <'a>Items<'a, Item = <Self as VForm<'a>>::Get> + for <'a>Decoders<
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
    /// number ([`VNewtype`]) in the order of the numbers it is stored as
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
pub trait VForm<'a>: VSpec {
    /// The form: for a number, an `Option` of one or a `bool`, the
    /// type itself; for `Vec<u8>`, `&'a [u8]`; for `Option<Vec<u8>>`,
    /// `Option<&'a [u8]>`; for `String`, `&'a str`; for
    /// `Option<String>`, `Option<&'a str>`.
    type Get: VGet<S = <Self as VSpec>::S>;
}

/// The form in which a column of `V` gives a value and takes one
/// ([`VForm`]): `V` itself for a number, an `Option` of one or a
/// `bool`, a borrowed slice for a byte string and a `&str` for a
/// string.
pub type Get<'a, V> = <V as VForm<'a>>::Get;

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

impl<'a, T: RleValue> VForm<'a> for T {
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

impl<'a, T: RleValue> VForm<'a> for Option<T> {
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

impl<'a> VForm<'a> for bool {
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
/// type that says which of them it is stored as ([`VNewtype`]) and every
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
/// ([`VIndex`]). A plain column's is [`LenIndex`], which need not be
/// written: `Column<V>` and `Column<V, C>` are plain
/// columns. Every method is of every kind's, the constructors and the
/// loads too: `Column::<V>::new()` is a plain column, and where
/// the type is known otherwise, `Column::new()` is of the kind
/// it says. A column kept with a [`SumIndex`] is a prefix column
/// ([`PrefixColumn`]); one kept with a [`LenBitIndex`] is a
/// plain column whose index is a Fenwick tree, as the old API's
/// `Column<V, C, LenWeight, BitIndex<usize>>`.
pub struct Column<V: ColumnValueRef<C>, C: Codec = Leb128, K: VIndex<V, C> = LenIndex> {
    core: KCore<V, C, K>,
}

impl<V: ColumnValueRef<C>, C: Codec, K: VIndex<V, C>> View for Column<V, C, K> {
    type V = Seq<V::S>;

    closed spec fn view(&self) -> Seq<V::S> {
        <Held<V, C> as VGet>::svs(self.core@)
    }
}

impl<V: ColumnValueRef<C>, C: Codec, K: VIndex<V, C>> Default for Column<V, C, K> {
    /// An empty column.
    fn default() -> (r: Self)
        ensures
            r@ == Seq::<V::S>::empty(),
    {
        Self::new()
    }
}

impl<V: ColumnValueRef<C>, C: Codec, K: VIndex<V, C>> Clone for Column<V, C, K> {
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


impl<'v, V: ColumnValueRef<C>, C: Codec, K: VIndex<V, C>> Extend<Get<'v, V>> for Column<V, C, K> {
    /// Append the values that `iter` gives: what
    /// [`splice_iter`](Column::splice_iter) does at the column's
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
impl<'v, V: ColumnValueRef<C>, C: Codec, K: VIndex<V, C>> FromIteratorSpecImpl<Get<'v, V>> for Column<V, C, K> {
    /// A column collected from an iterator holds the values that the
    /// iterator was to give.
    open spec fn from_iter_ensures(remaining: Seq<Get<'v, V>>, will_return_none: bool, s: Self) -> bool {
        s@ == <Get<'v, V> as VGet>::svs(remaining)
    }
}

impl<'v, V: ColumnValueRef<C>, C: Codec, K: VIndex<V, C>> FromIterator<Get<'v, V>> for Column<V, C, K> {
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
impl<V: ColumnValueRef<C>, C: Codec, K: VIndex<V, C>> Column<V, C, K> {
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

impl<V: ColumnValueRef<C>, C: Codec, K: VIndex<V, C>> Column<V, C, K> {
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

    /// Delete `del` values at `at` and insert `values` there.
    ///
    /// The column counts an edit more (`after_edit` of its `edits()`),
    /// unless the call deletes no value and inserts none.
    #[inline(always)]
    pub fn splice(&mut self, at: usize, del: usize, values: &[Get<'_, V>])
        requires
            at <= old(self)@.len(),
            del <= old(self)@.len() - at,
        ensures
            final(self)@ == old(self)@.take(at as int) + VGet::svs(values@) + old(self)@.skip(at + del),
            final(self).edits() == after_call(old(self).edits(), del > 0 || values@.len() > 0),
    {
        self.splice_impl(at, del, values)
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
    pub fn splice_iter<'v, I: IntoIterator<Item = Get<'v, V>>>(&mut self, at: usize, del: usize, values: I)
        requires
            at <= old(self)@.len(),
            del <= old(self)@.len() - at,
        ensures
            gives(values, |xs: Seq<I::Item>| final(self)@ == old(self)@.take(at as int) + VGet::svs(xs) + old(self)@.skip(at + del)),
            final(self).edits() == after_call(old(self).edits(), del > 0 || final(self)@.len() != old(self)@.len()),
    {
        self.splice_iter_impl::<I>(at, del, values)
    }

    /// Delete `del` values at `at` and insert there the runs that `runs`
    /// gives, as an iterator: of each run its value, as many times as its
    /// count says (`run_values`). A run of count 0 inserts nothing.
    ///
    /// The specification speaks of the iterator as
    /// [`splice_iter`](Self::splice_iter)'s does. The counts are the
    /// caller's: with ones that make the column longer than it can count,
    /// or a run longer than the format can write, this panics.
    ///
    /// The column counts an edit more unless the call deletes no value
    /// and inserts none, as [`splice_iter`](Self::splice_iter)'s does:
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
    /// Of a column with no value this is no edit, as it is none of
    /// [`Column`](crate::Column): the column counts what it did, and
    /// what was suspended of it resumes still. Of any other the column
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
    #[inline(always)]
    pub fn scope_to_value(&self, value: Get<'_, V>, start: usize, end: usize) -> (r: Range<usize>)
        requires
            start <= end <= self@.len(),
        ensures
            start <= r.start <= r.end <= end,
            sorted_for::<V>(self@.subrange(start as int, end as int), value.sv()) ==> forall|k: int|
                start <= k < end ==> {
                    &&& (k < r.start) == V::below(#[trigger] self@[k], value.sv())
                    &&& (r.start <= k < r.end) == (self@[k] == value.sv())
                    &&& (r.end <= k) == V::below(value.sv(), self@[k])
                },
    {
        self.scope_to_value_impl(value, start, end)
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

    /// An iterator over the values `start..end`.
    #[inline(always)]
    pub fn iter_range(&self, start: usize, end: usize) -> (it: Iter<'_, V, C, K>)
        requires
            start <= end <= self@.len(),
        ensures
            it.rest() == self@.subrange(start as int, end as int),
            it.column() == self@,
            it.source() == Some(*self),
            it.pos() == start,
            it.end_pos() == end,
    {
        self.iter_range_impl(start, end)
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
impl<V: ColumnValueRef<C>, C: Codec, K: VIndex<V, C>> Column<V, C, K> {
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
    /// A slab holds two runs or more: [`Column`](crate::Column) panics
    /// when asked for fewer, and here that is a `requires`. The fill
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
pub struct Iter<'a, V: ColumnValueRef<C>, C: Codec = Leb128, K: VIndex<V, C> = LenIndex> {
    it: ICore<<V::X as Decoders<'a>>::D>,
    slabs: &'a [Slab<<V::E as Slabs>::Tail>],
    /// The column the slabs are of; a default iterator has none.
    col: Option<&'a KCore<V, C, K>>,
}

impl<'a, V: ColumnValueRef<C>, C: Codec, K: VIndex<V, C>> Iter<'a, V, C, K> {
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
    /// looks no further than where it ends. `0..usize::MAX` is all the
    /// iterator has left.
    ///
    /// Few of the values are looked at
    /// ([`scope_to_value`](Column::scope_to_value)). They need be
    /// in order only as far as `target` can tell (`sorted_for`); where
    /// they are not, the range is some range of the places searched.
    #[inline(always)]
    pub fn seek_to_value(&mut self, target: Get<'_, V>, range: Range<usize>) -> (r: Range<usize>)
        ensures
            final(self).column() == old(self).column(),
            final(self).source() == old(self).source(),
            ({
                let (p, o, t) = (old(self).pos(), old(self).rest(), target.sv());
                // how far the iterator moves before it searches, and the
                // values searched
                let a = if range.start <= p {
                    0
                } else if range.start - p <= o.len() {
                    range.start - p
                } else {
                    o.len() as int
                };
                let w = window(o, a, range.end - p);
                &&& p + a <= r.start <= r.end <= p + a + w.len()
                &&& final(self).pos() == r.start
                &&& final(self).rest() == o.skip(r.start - p)
                &&& final(self).end_pos() == if range.start <= p + o.len() {
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
        self.seek_to_value_impl(target, range)
    }
}

#[cfg(verus_keep_ghost)]
impl<'a, V: ColumnValueRef<C>, C: Codec, K: VIndex<V, C>> vstd::std_specs::iter::IteratorSpecImpl for Iter<'a, V, C, K> {
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

impl<'a, V: ColumnValueRef<C>, C: Codec, K: VIndex<V, C>> Iterator for Iter<'a, V, C, K> {
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
impl<'a, V: ColumnValueRef<C>, C: Codec, K: VIndex<V, C>> vstd::std_specs::iter::ExactSizeIteratorSpecImpl for Iter<'a, V, C, K> {
    open spec fn exact_len(&self) -> usize {
        self.items().len() as usize
    }
}

impl<'a, V: ColumnValueRef<C>, C: Codec, K: VIndex<V, C>> ExactSizeIterator for Iter<'a, V, C, K> {
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

impl<'a, V: ColumnValueRef<C>, C: Codec, K: VIndex<V, C>> Clone for Iter<'a, V, C, K> {
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

impl<'a, V: ColumnValueRef<C>, C: Codec, K: VIndex<V, C>> Default for Iter<'a, V, C, K> {
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

impl<'a, V: ColumnValueRef<C>, C: Codec, K: VIndex<V, C>> IntoIterator for &'a Column<V, C, K> {
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
    pub closed spec fn fits<V: ColumnValueRef<C>, C: Codec, K: VIndex<V, C>>(&self, column: &Column<V, C, K>) -> bool {
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
    pub proof fn lemma_fits<V: ColumnValueRef<C>, C: Codec, K: VIndex<V, C>>(&self, column: &Column<V, C, K>)
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
    /// Panics, as [`Column`](crate::Column)'s does, where an edit's panic
    /// left `column` without its parts and the state would fit it.
    #[inline(always)]
    pub fn try_resume<'a, V: ColumnValueRef<C>, C: Codec, K: VIndex<V, C>>(&self, column: &'a Column<V, C, K>) -> (r: Result<
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
pub struct Runs<'a, V: ColumnValueRef<C>, C: Codec = Leb128, K: VIndex<V, C> = LenIndex>(
    Iter<'a, V, C, K>,
);

impl<'a, V: ColumnValueRef<C>, C: Codec, K: VIndex<V, C>> Runs<'a, V, C, K> {
    /// The iterator of values whose runs these are, where it stands now.
    pub closed spec fn iter(&self) -> Iter<'a, V, C, K> {
        self.0
    }
}

#[cfg(verus_keep_ghost)]
impl<'a, V: ColumnValueRef<C>, C: Codec, K: VIndex<V, C>> vstd::std_specs::iter::IteratorSpecImpl for Runs<'a, V, C, K> {
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

impl<'a, V: ColumnValueRef<C>, C: Codec, K: VIndex<V, C>> Iterator for Runs<'a, V, C, K> {
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
