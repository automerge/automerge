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
