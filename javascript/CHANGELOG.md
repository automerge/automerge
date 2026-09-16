# Changelog

## 3.5.0

Changes since 3.4.1.

### Added

* It is now possible to add metadata to a change which identifies the "author"
  of the change. This is an opaque (from Automerge's perspective) hex string.
  The author is recorded in the change metadata and can be retrieved from
  the change history using `getAuthor` and `getAuthors`.

### Changed

* Assigning a `__proto__` key now throws a `RangeError`, including in nested
  objects passed to batch insertion. Batch validation only traverses own
  enumerable properties, not inherited ones.

### Fixed

* Large list insertions no longer fail with "Maximum call stack size exceeded"
  when applying consolidated patches. The WASM bindings apply insertions in
  bounded chunks rather than passing every element to a single `splice` call.
* `applyPatch` and `applyPatches` support inserting and updating embedded
  rich-text blocks. When applied to plain JavaScript strings, blocks are
  represented by object replacement characters and updates to their contents
  are ignored. When applied inside an Automerge `change` callback, block data
  is preserved.
* Patches that expose an existing text object include its marks, embedded
  blocks, and block contents. This fixes diffs that restore deleted rich text.
* Fixed a performance regression in large changes caused by repeatedly scanning
  pending operations to check object visibility.
* Fixed list insertion positioning when a counter increment precedes a trailing
  insert during change application, which could corrupt operation grouping and
  break subsequent change reconstruction.
* Experimental `getFragments` and fragment metadata APIs no longer include the
  fragment's head in `checkpoints`. Checkpoints exclude both the head and
  boundary hashes; the head remains in `members`.
* Corrected documentation examples to use `clone` instead of the nonexistent
  `fork` API and fixed TypeScript example formatting.
