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
