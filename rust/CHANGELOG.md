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
