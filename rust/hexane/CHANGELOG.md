## Unreleased

### Breaking

The columns are now the verified API: every public function has a
contract, which [Verus](https://github.com/verus-lang/verus) proves (see
the README's "Formal verification", and the crate's documentation). The
earlier implementation, and the glue it took on trust, are **removed**. The types and functions keep their names wherever
they had a counterpart: `Column`, `Iter`, `IterState`, `Runs`,
`ColumnLoadIter`, `Edit`, `PrefixColumn`, `PrefixIter`, `PrefixIterState`,
`PrefixedValue`, `PrefixSeek`, `DeltaColumn`, `DeltaIter`,
`DeltaIterState`, `DeltaRuns`, `DeltaEdit`, `FindByRange`, `Encoder`,
`EncoderState`, `Decoder`, `DeltaEncoder`, `DeltaEncoderState`,
`DeltaDecoder`, `decoder`, `try_decoder`, `decoder_in`, `try_decoder_in`,
`RawColumn`, `Shiftable`, `Unshift`, `Ranges`, `Codec`, `ColumnValueRef`,
`RleValue`, `DeltaValue`, `PrefixValue`, `UnsignedPrefix`, `lebsize`,
`ulebsize`, `LoadOpts`, `Run`, `DeltaRun`, `Splice`, `PackError`,
`Leb128`, `Bijou64`, and the `leb128` and `bijou` modules. They are at
the crate root. What changed for a
client:

* Values are given and taken in their borrowed form, `Get<'a, V>`: `&str`
  for a `String` column, `&[u8]` for `Vec<u8>`, the value itself for
  numbers and `bool`. `AsColumnRef` is gone.
* A client's own value type implements `Newtype` (stored as one of the
  built-in numbers: `to_base`, `from_base`) or `RleValue` (an encoding of
  its own: `same`, `compare`, `try_unpack`, `unpack`, `value_len`, and
  `pack(&self, &mut Vec<u8>) -> usize`), not `ColumnValue` and the old
  `RleValue`. A number that is no value of the type is refused on load.
  A client's codec implements the new `Codec`. `ColumnEncoding`,
  `EncoderApi`, `RunDecoder`, `LoadIterApi`, `RunSrc` and `VarBuf` are
  gone: `encode_to` and the encoders' other functions are their own
  methods.
* `from_values` takes a slice; `splice` takes an iterator of borrowed
  values, and `splice_slice` a slice. A `DeltaColumn`'s `splice` and
  `extend` panic, before the column changes, on a value it cannot hold.
* A `PrefixColumn<T>` is a `Column<T, Leb128, SumIndex>`: its own `iter`,
  `iter_range` and `get` read its values (`values()` is gone), and
  `prefix_iter`, `prefix_iter_range` and `get_prefixed` read them with
  their sums. Its values are the built-in numbers, `bool` and their
  options, or a client's type that implements `Amount` (what a value
  adds, and the integer type the sums are kept in). A client's own
  accumulator, which `PrefixValue::Prefix` was, is an `AccColumn<T>`,
  with `Adds` (what a value adds), `Accum` and `VGroup`. A column whose
  sums would not fit is refused on load (also a load of an empty column
  with a fill, `LoadOpts::with_fill`, whose sums would not fit), and an
  edit that would make one panics.
  Where a column cannot know its sums fit (it has more values than
  `Amount::room_len`), an edit checks: it reads the values it put in and
  adds their sum to the column's, and only where that does not fit (the
  values it took out may have made room) does it read the whole column.
  `Amount::part_of` must not unwind (`no_unwind`, which Verus proves of
  a client checked by it).
* An iterator names the kind of index of its column: `Iter<'a, V, C, K>`,
  with `K` the default `LenIndex` for a plain column and `SumIndex` for a
  prefix column's values. The kinds derive `Clone`, `Copy`, `Debug` and
  `Default`. `LenBitIndex` keeps a plain column with a Fenwick tree, as
  `BitIndex` did.
* Some run-time checks are now `requires` of a contract (a splice past
  the end, a decoder of bytes that are no saved column, an encoder's
  segment budget under 2): a client checked by Verus proves them, and any
  other keeps them itself, or gets an unspecified result or a panic.
  Nothing is ever unsafe: the crate has no `unsafe`, and strings are read
  with `from_utf8`, checked.
* Gone, with no counterpart: `Column::find_by_value` and
  `find_by_value_range` (of the plain column with a delta weight;
  `DeltaColumn::find_by_value` and `find_by_range` remain), and
  `ItemPerValue` (an `Unshift` over the run adapters cannot be shifted).

### Added

* `AccColumn`, `AccIter` and the accumulator traits; `Newtype` and
  `Amount` for a client's own types.
* `DeltaColumn::splice` of an iterator, `Extend` and `FromIterator`.
* `tests/golden`: digests of what the earlier implementation wrote, read
  and refused in every test of `tests/verified_api.rs`, recorded while it
  ran beside the verified API, which each test still checks.

## 1.0.0-alpha.2 - 7th July 2026

### Breaking

* The 0.2-era cursor API (`ColumnData<C>`, `UIntCursor`, `DeltaCursor`,
  `BooleanCursor`, `Slab`/`SpanTree`, `Packable`, …) has been **removed**
* The v1 API is promoted to the crate root: `hexane::v1::Column` is now
  `hexane::Column`, and the `v1` module is gone
* `PrefixColumn`/`PrefixIter` yield `PrefixedValue` (`.value`, `.prefix()`,
  `.total()`) instead of `(prefix, value)` tuples; `PrefixSeek` is now
  `{ pos, delta, pv }`
* Removed `PrefixColumn::{seek, get_delta, get_value, value_iter}`
  (use `delta`, `values().get()`, `values().iter()`);
  `prefix_delta` is renamed `sum_range`
* `DeltaColumn::save_to_unless` takes `T` and compares realized values
* `Column::with_max_segments` (and `load`) reject budgets below 2

### Added

* `remove_n(index, n)` on all column types
* `DeltaValue::try_to_i64`; out-of-domain `find_by_value` targets return
  an empty iterator instead of panicking
* Golden wire-format tests freezing the (v0-compatible) byte format
* B-tree deletion underflow handling (empty-node removal, root collapse,
  leaf sibling merging) with a structural invariant checker in the fuzzers

### Fixed

* Silent B-tree corruption on deletes exactly covering an internal node's
  span, and O(N) full rebuilds on whole-leaf deletes (quadratic under
  sequential deletion)
* `BoolEncoder::into_slab` tail metadata for runs longer than 127
* Delta columns: load-time validation of realized values for
  `u64`/`usize`/`i32` (including running-sum overflow from hostile input),
  and a documented 2^63 value-domain contract
* Major performance work: memcpy-based slab splice, O(log S) `slab_start`,
  index descents without root-aggregate merges, and direct
  encoder-to-column slab handoff (`remap` 15-20% faster, seeks ~2x)


## 0.2.1 - 25th March 2026

### Added

* Adding the `slow_path_assertions` feature so that slow path assertions can
  be disabled by default


## 0.2.0 - 6th March 2026

### Added

* Lots of documentation
* `ColumnCursor::encode_unless_empty` which replaces the use of the `force`
  parameter of `ColumnCursor::encode`
* `ColGroupIter::shift_acc`
* Re-export `HasMinMax` and `RunIter`

### Fixed

* `ColumnCursor::splice` correctly handles deletions around slab boundaries


### Breaking

* `ColumnCursor::encode` no longer takes a boolean `force` parameter and the
  generic bounds are now
  `<M: MaybePackable<'a, Self::Item>, I: IntoIterator<Item = M>>` (more or less
  a generalization of the previous bounds)
* Removed `Encoder::finish`, use `into_column_data` or `save_to`
* `ColGroupIter::nth` now advances by item count rather than accumulator count,
  use `ColGroupIter::shift_acc` to advance by accumulator count
* `ColumnCursor::encode_unless_empty` is a new required method on
  `ColumnCursor`
