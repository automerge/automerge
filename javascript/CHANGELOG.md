# Changelog

## Unreleased

### Added

* `Automerge.maskAuthor(doc, author, heads)`, `Automerge.revealAuthor(doc,
  author)` and `Automerge.isAuthorMasked(doc, author)` hide and restore an
  author's changes beyond a set of heads; the patch callback receives the
  resulting patches.

### Changed

* Patches describe the net change between two states of the document. Their
  order and grouping can differ from before (for example several splices
  arrive in final-position order, and a conflict may arrive as a put marked
  `conflict: true`), but applying them still takes the old state to the new.
* The experimental fragment getters are now `getFragments(doc, query)` and
  `getFragment(doc, head, opts)`. `query` takes `levels`, or `after` and
  `partial`, and `bytes: false` returns metadata only. They replace
  `getFragmentMetadata`, `getFragmentMeta`, `getFragmentMetadataAfter`,
  `getFragmentsAfter` and `getCommits`: loose commits are level-0
  fragments, and `getFragments` lists every level unless `levels` is given.
* `addFragments` is renamed `applyFragments`, and the experimental
  `syncFragments(doc, fragments)` exchanges fragments with a peer, returning
  the new document and the fragments to send back.

### Removed

* The experimental `addCommits` function and `Commit` type: loose commits
  are level-0 fragments, which `applyFragments` takes.

* The experimental `saveBundle` function. `readBundle` remains but reads only
  the bundles `bundleFragmentMetadata` produces, not the BundleV0 format
  `saveBundle` wrote in 3.2 to 3.5; `load` and `loadIncremental` still load
  those.

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
