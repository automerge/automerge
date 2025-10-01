# 0.7.0

This is a big change since the last rust release. The major addition is that
we now use the same columnar compression at runtime as we do on disk. This
means that steady state memory usage for automerge documents is typically
100x lower than it was and much more predictable from the on disk size. This
change doesn't affect the API at all. There have been othe API changes though.
Here's an exhaustive list:

## Added

* `ReadDoc::iter` and `ReadDoc::iter_at` for iterating over the whole document
* `{AutoCommit,  Automerge}::apply_changes_batch` which efficiently applies a batch of changes at once.
* `{AutoCommit, Automerge}::bundle` which compacts a set of changes in a similar
  manner to `Automerge::save` but without needing to include _every_ change.
  This is an experimental API that may change in the future.
* `{AutoCommit}::is_empty()` to determine if there are any commits at all in a document
* `{Automerge, AutoCommit}::{get_changes_meta, get_change_meta_by_hash}` to
  return just the metadata for a change by hash. This is useful because we no
  longer store the raw bytes of every change in the document and instead
  decompress changes on demand. This means that if all you need is the metadata
  it's more efficient to use `get_changes_meta` and `get_change_meta_by_hash`
  than the corresponding `get_changes` and `get_change_by_hash` methods.
* `Transactable::update_spans` now takes an `UpdateSpansConfig` which allows you
  to specify how new marks which are created during the diff should have their
  "expand" config set
* `Change::actors` which returns the actor IDs in a change
* `MarkSet::non_deleted_marks` returns a wrapper type which can be used to
  compare two `MarkSet`s without reference to any deleted marks
* `Stats::{num_actors, cargo_package_name, cargo_package_version, rustc_version}`

## Changed

* `Automerge::{get_last_local_change, get_change, get_change_by_hash}` returns a
  `Change` not an `&Change`. As noted above, this is because we don't store the
  changes in the document any more and instead decompress them on demand.
* `hydrate::Text::new` and `hydrate::Value::text` now take a `TextEncoding`
  argument rather than a `TextRepresentation` argument
* `hydrate::Value::{apply_patches, apply}` now takes a `TextEncoding` argument
  rather then a `TextRepresentation` argument
* The `MapRangeItem::key` field is now a `Cow<'a, str>` not a `&'a str`
* `ListRangeItem` and `MapRangeItem` no longer have a `R: RangeBound`
  parameter.
* The `{ListRangeItem, MapRangeItem}::value` field is now a `ValueRef` not a
  `Value`
* `Span::Text` now has named fields rather than being a tuple variant
* `Mark` no longer has a lifetime type parameter and there is no longer a `Mark::into_owned` method
* `Mark::data` has become `Mark::{name, value}`
* `MarkData` has been removed to `OldMarkData` and should generally not be used,
  use the `Mark::{name, value}` fields directly instead

## Removed

* `{AutoCommit, Automerge}::visualise_optree` - the internal storage of the
  document is no longer a b-tree so we don't need this debug feature any more
* The `{ListRangeItem, MapRangeItem}::id` field is gone
* The `TextRepresentation` type no longer exists.
  * `AutoCommit::{get_text_rep, set_text_rep, with_text_rep}` have been removed
  * `Automerge::{transact_and_log_patches, transact_and_log_patches_with, current_state, diff}` no longer take a `TextRepresentation` argument
  * `PatchLog::new` no longer takes a `TextRepresentation` argument

## Fixed

* Many small bugs with the spans and updateSpans methods and patch
  generation

# 0.6.1

* Fix a bug where `{Automerge, AutoCommit}::get_marks` would return removed marks
  as mark with value `ScalarValue::Null` rather than not returning them at all.

# 0.6.0

* Add the ability to set the text encoding used when calculating the indices
  into text objects via the Automerge::new_with_encoding constructor.
* Update the cursor API to allow creating cursors which point at the beginning
  or end of the text and to allow configuring how the cursor position is
  resolved when the original character which the cursor referenced has been
  deleted.

# 0.5.12

* Allow empty keys in maps
* Add `SyncState::has_our_changes` to indicate whether we think the other end
  has everything we have.
* Add `ReadDoc::stats` to obtain basic statistics about a document (number of
  operations and changes)
* Allow configuring the character widths used for the wasm32 target by
  introducing the utf-16 indexing feature flag

# 0.5.11

* Fixed a bug where actor IDs were written incorrectly to the save document
  format rendering it impossible to load the document

# 0.5.10

The primary feature of this release is a set of methods for managing block
markers in rich text. These methods are:

* ReadDoc::{spans, spans_at} which return spans of text grouped by marks and
  separated by block markers
* Transactable::{split_block, join_block, update_block} which allow you to
  create, remove, and update block markers in a text sequence
* Transactable::update_spans, which allows you to update all the block markers
  and text in a text sequence in one go. Analogous to update_text for block
  structure

These methods are not well documented as they have primarily been written to
support the JS implementation. Documentation and examples will follow in future
releases

Other changes:

* Fix a bug where marks which were set to not expand at the end still produced
  splice patches containing the mark when inserting at the end of the mark
* Fix a bug where splicing into the end of a mark which was set to expand did
  not produce patches containing the marks when receiving the change from a
  remote
* Fix a bug where "undeleted" objects were not emitted in patches when patching
  in "reverse" - i.e. when the before heads were topoligically after the after
  heads when calling `diff`

# 0.5.9

* Fix a bug introduced in 0.5.8 which caused an error when loading a saved
  document which contained empty commits
* Improve performance when diffing documents which contain a large number of
  objects

# 0.5.8

* Fix a bug where the logic to rollback a transaction on error could panic
* Fix a bug where marks were calculated incorrectly when viewing a document at
  a particular set of heads (i.e. not the "current" heads)
* Update the `LoadOptions::migrate_strings` logic to no-op if there are no
  strings to convert

# 0.5.7

* Update itertools dependency to 0.12.0
* Fix a bug in `Read::get_marks` which caused it to ignore any heads passed to
  it and always return the latest marks (only relevant if you ever passed
  `Some(_)` as the heads argument of `Read::get_marks(objid, index, heads)`)

# 0.5.6

* Add `Transactable::update_text`, which calculates a diff between the current
  value of a text field and a new value and converts that diff into a set of
  splice operations

# 0.5.5

* Fix a sync protocol backwards compatibility gotcha which caused 0.5.4 peers
  to emit messages which older peers could not understand in some circumstances.

# 0.5.4

* more performance improvements when loading documents with large numbers
  of objects
* modify the sync protocol to send the entire document in the first message if
  the other end has nothing. This dramatically improves the speed of initial
  sync. This is a backwards compatible change.

# 0.5.3

* numerous performance improvements
* Add `ReadDoc::get_marks` to get the marks active at a particular index in a
  sequence
* make `generate_sync_message` always return at least one sync message so that
  even if you are already if the other end has no changes to send you, they
  still tell you that.

# 0.5.2

* Fix a bug where sync messages were not generated even though sync was not
  complete
* Fix a bug where adding a mark to the last character in a text string failed
  to produce a patch
* Add `Automerge::load_with_options` and `AutoCommit::load_with_options` and
  deprecate `Automerge::load_with` and `AutoCommit::load_with`. Add an option
  to convert `ScalarValue::Str` values to `ObjType::Text` on load
* Expose `VerficationMode`

# 0.5.1

* Make `AutoCommit` and `PatchLog` `Send`
* Make `Automerge::load_incremental_log_patches` `pub`

# 0.5.0

* Added `Cursor` for managing references to positions in sequences and text
* Remove `OpObserver` and instead expose a patch based API. Anywhere you
  previously used an `OpObserver` there will now be a method name
  `<method>_log_patches` which accepts a `PatchLog` to add patches to. The
  `PatchLog` can then be turned into a `Vec<Patch>` with
  `Automerge::make_patches`. Also add `AutoCommit::{diff, diff_incremental}`
  for managing the common case of an incrementally updated materialized view.
* Add `Transactable::change_at` for creating a transaction which operates on
  the document as at some heads. Also add `Autocommig::{isolate,integrate}`
  which uses this functionality.
