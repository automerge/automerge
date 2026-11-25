#![doc = include_str!("../README.md")]
//!
//! ## The API and its specification
//!
//! [`Column`] is a column whose every operation carries a
//! specification that [Verus](https://github.com/verus-lang/verus) has
//! checked, stated in terms of the column's values as a sequence
//! (`col@`). A client that is itself checked by Verus can so prove what
//! its own code does with a column: that what it reads is what it wrote,
//! and that a column it saved loads again as the same values and saves
//! back to the same bytes (`client_proof` is such a client).
//!
//! Under it is the crate's verified core: its slabs, index, cursor and
//! loader. The codec (`codec`), the value types (`value`, `counts`,
//! `bytes`) and all that lies between the API and the core are verified
//! too.
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
//! ## Reviewing the specification
//!
//! What this API promises is its specification, and Verus has checked
//! that the code keeps it. To review the promises, read the
//! specification: none of the code that keeps them, and none of the
//! proofs, need be read.
//!
//! The specification is in the files of `src/verified` that are not named
//! `imp.rs` (and but for `client_proof.rs` and `model.rs`, below): this
//! `mod.rs`, for the value types, the column and its iterator; `value.rs`,
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
//!   for `decoder.rs`, and `imp.rs` for `mod.rs`), which has the same
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
//! * a trait that a client implements (`RleValue`, `Newtype`, `Codec`,
//!   `Amount`, `VGroup`, `Accum`, `Adds`, `Shiftable`) says in its
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
//! ## Value types
//!
//! A `Column<V>` holds values of a type `V` that is a
//! [`ColumnValueRef`]:
//!
//! * a number: `u64`, `i64`, `u32`, `i32`, `usize` or `NonZeroU32`;
//! * `bool`;
//! * a client's own type that is stored as one of those numbers
//!   ([`Newtype`]). The client says which number each value is, and a
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
//! where a string is not UTF-8 ([`PackError::InvalidUtf8`]). The
//! crate has no `unsafe`, so what makes a `&str` of a column's bytes is
//! the standard library's check of them (`core::str::from_utf8`), at
//! each read: it is proved to find them UTF-8, and it looks at every
//! byte of the string.
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
//! ## Prefix columns
//!
//! A [`PrefixColumn`] is a column kept with the sums of its
//! values: a `Column` whose third parameter, the kind of index
//! it is kept with ([`Index`]), is [`SumIndex`], where a plain
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
//! which the sum returned is (`Sum::as_int`). Where the sums are never
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
//! ## Accumulator columns
//!
//! A [`AccColumn`] is a column kept with what its values add up
//! to in a type its client gives: a `Column` whose kind of
//! index is [`AccIndex`]. The type is an accumulator ([`Accum`]), and
//! its client proves the laws of it ([`VGroup`]): that its addition is
//! associative, that its zero adds nothing, and that what was added can
//! be taken away again. Two accumulators need not add up to the same in
//! either order. The column's values are of a type of the client's own
//! that says what a value adds ([`Adds`]), or an `Option` of one, whose
//! `None` adds nothing ([`Accumulated`]).
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
//! ## Raw columns
//!
//! [`RawColumn`] is a column of bytes, whose module (`raw.rs`) is
//! verified with its specifications. A raw column is a
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
//! ## What is proved
//!
//! Of every method, its `ensures`, for every input that meets its
//! `requires`, for every value type and codec. In particular:
//!
//! * [`splice`](Column::splice) leaves the values before `at`,
//!   the values an iterator gives, then the values from `at + del`;
//! * [`splice_slice`](Column::splice_slice) does so with the values
//!   of a slice, and [`splice_runs`](Column::splice_runs)
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
//! `runs_of`, `run_values`, `gives`, `fill_of` and `sum_of` are of
//! `src/verified/mod.rs`,
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
//! That those are the bytes of automerge's format for the same values is
//! not proved but tested (`tests/verified_api.rs`): while hexane's earlier
//! implementation existed, the tests ran it beside each check (the same
//! bytes saved, each loading what the other saved, the same bytes
//! refused with the same error), and they keep a digest of what it did,
//! which each test checks still (`tests/golden`).
//!
//! How a column lies in memory is no part of any specification:
//! [`slab_count`](Column::slab_count),
//! [`slab_lens`](Column::slab_lens),
//! [`slab_segments`](Column::slab_segments) and what
//! [`dump_slabs`](Column::dump_slabs) and `Debug` print tell of
//! it, as does an iterator's [`get_slab`](Iter::get_slab), and
//! promise nothing but that the lengths add up. Nor is how
//! often [`remap`](Column::remap) calls its function: once for
//! each run of equal values, which is tested. That after the same edits
//! a column has the slabs the earlier implementation made, each as long
//! and of as many runs, is tested too (the digests), but for one case: a
//! column that [`clear`](Column::clear) has emptied has no slab, where
//! that implementation kept an empty one, until the next edit.
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
//! a `usize` that wraps around: after one edit or
//! more, and no more of them than `usize::MAX`, it is not what it was
//! (`lemma_after_edits`), so a state taken before them is refused. The
//! column sets the count itself after each call. That the calls which
//! end what was suspended are those that ended it in the earlier
//! implementation is tested (the digests), as is that after a call which
//! is no edit a state resumes where it did there. And a state that does
//! resume cannot misread:
//! the iterator given has the values of the column it was resumed on,
//! in the state's window, whatever column the state was taken of.
//!
//! ## What is trusted
//!
//! Every function that Verus takes on trust on a path of this API is in
//! one file, `src/trusted.rs`, which says what each assumes. (Code
//! outside `verus!` is not checked at all. Of that, the crate runs only
//! the functions named below as not checked.)
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
//! `src/trusted.rs`), which are few and small:
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
//! unverified code between the API and the core: every other function on
//! a path through it is checked. The options of a load ([`LoadOpts`]) are
//! checked too: nothing is assumed of what they hold.
//!
//! Three functions of a column are not checked, and nothing is specified
//! of them. They read the column through checked functions that ask
//! nothing of a caller, and then print or compare: `Debug`;
//! [`dump_slabs`](Column::dump_slabs), whose printing of what
//! checked code has read of each slab is not checked; and
//! `check_invariants`, a tool for tests that these docs leave out. It
//! looks at run time for what is
//! proved of every column (a slab's bytes hold the values and runs it
//! counts, and the index counts the slabs' lengths) and for the layout
//! policy, which is not proved (a slab's runs are within the budget,
//! and no two neighbours should have been merged), and panics if it
//! finds otherwise. It takes every column as one that
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
//! what [`Newtype`] asks of it when it is run on the client's crate,
//! or [`RleValue`] of one with an encoding of its own, and so what
//! [`Amount`] asks of one that a prefix column adds up, and what
//! [`VGroup`], [`Accum`] and [`Adds`] ask of an accumulator and of the
//! values added up in it. So too what [`Codec`] asks of a client's own
//! codec, and [`Shiftable`] of a client's own repositionable iterator.
//! A client that is compiled without Verus has that to keep by itself,
//! as it has every `requires` of this API.
//!
//! That a splice returns is proved where it takes a slice:
//! [`splice_slice`](Column::splice_slice), `from_values`, `insert`,
//! `push`, `remove`, `remove_n`, `truncate` and `clear` of every column,
//! [`DeltaColumn::splice_slice`] and `splice_runs`, and
//! `copy_ranges`. (Verus checks that each loop ends; that a function
//! calls only functions that end, it does not: each loop on these paths
//! has a measure.) A splice of a caller's iterator or runs runs the
//! core's loop over the values to insert, which Verus is not asked to
//! show ends. It ends when the iterator does: with an iterator that
//! never ends,
//! [`splice`](Column::splice),
//! [`splice_runs`](Column::splice_runs), `extend` and `collect()`
//! (and a delta column's [`splice`](DeltaColumn::splice)) do not return (they run until memory is out, or a column's length is
//! more than it can count, which panics). Nor is it proved that an edit
//! cursor's [`insert_runs`](Edit::insert_runs) returns, or an
//! encoder's [`extend`](Encoder::extend), or a state's
//! ([`EncoderState::extend`]), or a static form that is built on
//! one: each is a loop over the client's iterator. Every other loop is
//! proved to end.

#[doc(hidden)]
#[macro_export]
macro_rules! log {
     ( $( $t:tt )* ) => {
          {
            use $crate::__log;
            __log!( $( $t )* );
          }
     }
 }

#[cfg(all(feature = "wasm", target_family = "wasm"))]
#[doc(hidden)]
#[macro_export]
macro_rules! __log {
     ( $( $t:tt )* ) => {
         web_sys::console::log_1(&format!( $( $t )* ).into());
     }
 }

#[cfg(not(all(feature = "wasm", target_family = "wasm")))]
#[doc(hidden)]
#[macro_export]
macro_rules! __log {
     ( $( $t:tt )* ) => {
         println!( $( $t )* );
     }
 }

// The verified core of every column. Each module is verified with Verus;
// what they take on trust is in `trusted`.
pub(crate) mod attrib;
#[cfg(feature = "bijou64")]
mod bijou64;
pub(crate) mod bool_edit;
pub(crate) mod bool_enc;
pub(crate) mod bool_unique;
pub(crate) mod bool_wire;
pub(crate) mod btree;
pub(crate) mod codec;
pub(crate) mod column;
pub(crate) mod column_core;
pub(crate) mod copy;
pub(crate) mod cursor;
pub(crate) mod delta;
pub(crate) mod edit_core;
mod error;
pub(crate) mod fenwick;
pub(crate) mod index;
pub(crate) mod iter_core;
mod leb128_codec;
pub(crate) mod load_core;
mod load_opts;
pub(crate) mod merge_walk;
pub(crate) mod prefix_core;
pub(crate) mod prefix_search;
pub(crate) mod raw;
pub(crate) mod remap;
pub(crate) mod rle;
pub(crate) mod splices;
pub(crate) mod trusted;
pub(crate) mod weights;
pub(crate) mod window;

// The API, with its specification: every public function of it has a
// contract that Verus proves. Its items are the crate's, at its root.
mod verified;

pub use verified::*;

#[cfg(feature = "bijou64")]
pub use codec::Bijou64;
pub use codec::{Leb128, Leb128Error};
pub use column::Splice;
pub use delta::DeltaRun;
pub use error::PackError;
pub use load_opts::{Fill, LoadOpts, MaybeFill, NoFill};

/// Sealing for the plumbing traits of the core (`SlabWeight`), which no
/// other crate implements.
mod sealed {
    vstd::prelude::verus! {
    pub trait Sealed {}
    } // verus!
}

#[cfg(test)]
mod edit_fuzz;
#[cfg(test)]
mod splice_fuzz;
#[cfg(test)]
mod tests;

// ── Run ─────────────────────────────────────────────────────────────────────

/// A run of identical values from a column iterator.
///
/// For repeat runs, `count` is the number of remaining items (including the
/// current one) and `value` is the repeated value.  For null runs, `count`
/// is the remaining null count and `value` is the type's null representation.
/// For literal runs, `count` is always 1 since each item is distinct.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
pub struct Run<V> {
    pub count: usize,
    pub value: V,
}
