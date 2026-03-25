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
