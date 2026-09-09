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
