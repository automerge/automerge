/**
 * # The next API
 *
 * This module contains new features we are working on which are backwards
 * incompatible with the current API of Automerge. This module will become the
 * API of the next major version of Automerge
 *
 * ## Differences from stable
 *
 * In the stable API text objects are represented using the {@link Text} class.
 * This means you must decide up front whether your string data might need
 * concurrent merges in the future and if you change your mind you have to
 * figure out how to migrate your data. In the unstable API the `Text` class is
 * gone and all `string`s are represented using the text CRDT, allowing for
 * concurrent changes. Modifying a string is done using the {@link splice}
 * function. You can still access the old behaviour of strings which do not
 * support merging behaviour via the {@link RawString} class.
 *
 * This leads to the following differences from `stable`:
 *
 * * There is no `unstable.Text` class, all strings are text objects
 * * Reading strings in an `unstable` document is the same as reading any other
 *   javascript string
 * * To modify strings in an `unstable` document use {@link splice}
 * * The {@link AutomergeValue} type does not include the {@link Text}
 *   class but the  {@link RawString} class is included in the {@link ScalarValue}
 *   type
 *
 * ## CHANGELOG
 * * Rename this module to `next` to reflect our increased confidence in it
 *   and stability commitment to it
 * * Introduce this module to expose the new API which has no `Text` class
 *
 *
 * @module
 */

export {
  Counter,
  type Doc,
  Int,
  Uint,
  Float64,
  type Patch,
  type PatchCallback,
  type Mark,
  type MarkSet,
  type MarkRange,
  type MarkValue,
  type AutomergeValue,
  type ScalarValue,
  type PatchSource,
  type PatchInfo,
} from "./next_types.js"

import type {
  Cursor,
  Mark,
  MarkSet,
  MarkRange,
  MarkValue,
} from "./next_types.js"

import { type PatchCallback } from "./stable.js"

import { type UnstableConflicts as Conflicts } from "./conflicts.js"
import { unstableConflictAt } from "./conflicts.js"
import type { InternalState } from "./internal_state.js"

export type {
  PutPatch,
  DelPatch,
  SpliceTextPatch,
  InsertPatch,
  IncPatch,
  MarkPatch,
  SyncMessage,
  Heads,
  Cursor,
  Span,
} from "./wasm_types.js"

import type { Span, MaterializeValue } from "./wasm_types.js"

export type {
  ActorId,
  Change,
  ChangeOptions,
  Prop,
  DecodedChange,
  DecodedSyncMessage,
  ApplyOptions,
  ChangeFn,
  ChangeAtResult,
  MaterializeValue,
  SyncState,
} from "./stable.js"
export {
  view,
  free,
  getHeads,
  change,
  changeAt,
  emptyChange,
  loadIncremental,
  saveIncremental,
  save,
  merge,
  getActorId,
  getLastLocalChange,
  getChanges,
  getAllChanges,
  applyChanges,
  getHistory,
  equals,
  encodeSyncState,
  decodeSyncState,
  generateSyncMessage,
  receiveSyncMessage,
  initSyncState,
  encodeChange,
  decodeChange,
  encodeSyncMessage,
  decodeSyncMessage,
  getMissingDeps,
  dump,
  toJS,
  isAutomerge,
  getObjectId,
  diff,
  insertAt,
  deleteAt,
  saveSince,
  initializeWasm,
  initializeBase64Wasm,
  wasmInitialized,
  isWasmInitialized,
  hasHeads,
} from "./stable.js"

export type InitOptions<T> = {
  /** The actor ID to use for this document, a random one will be generated if `null` is passed */
  actor?: ActorId
  freeze?: boolean
  /** A callback which will be called with the initial patch once the document has finished loading */
  patchCallback?: PatchCallback<T>
  /** @hidden */
  unchecked?: boolean
  /** Allow loading a document with missing changes */
  allowMissingChanges?: boolean
  /** Whether to convert raw string to text objects
   *
   * @remarks
   * This is useful if you have some documents which were created using the older API which represented
   * text as the `Text` class and you are migrating to the new API where text is just a `string`. In
   * this case the strings from the old document will appear as `RawString`s in the new document. This
   * option will convert those `RawString`s to `Text` objects. This conversion is achieved by rewriting
   * all the old string fields to new text fields
   **/
  convertRawStringsToText?: boolean
}

import { ActorId, Doc } from "./stable.js"
import * as stable from "./stable.js"
export { RawString } from "./raw_string.js"

/** @hidden */
export const getBackend = stable.getBackend

import { _is_proxy, _state, _obj, _clear_cache } from "./internal_state.js"

/**
 * Create a new automerge document
 *
 * @typeParam T - The type of value contained in the document. This will be the
 *     type that is passed to the change closure in {@link change}
 * @param _opts - Either an actorId or an {@link InitOptions} (which may
 *     contain an actorId). If this is null the document will be initialised with a
 *     random actor ID
 */
export function init<T>(_opts?: ActorId | InitOptions<T>): Doc<T> {
  const opts = importOpts(_opts)
  opts.enableTextV2 = true
  return stable.init(opts)
}

/**
 * Make a full writable copy of an automerge document
 *
 * @remarks
 * Unlike {@link view} this function makes a full copy of the memory backing
 * the document and can thus be passed to {@link change}. It also generates a
 * new actor ID so that changes made in the new document do not create duplicate
 * sequence numbers with respect to the old document. If you need control over
 * the actor ID which is generated you can pass the actor ID as the second
 * argument
 *
 * @typeParam T - The type of the value contained in the document
 * @param doc - The document to clone
 * @param _opts - Either an actor ID to use for the new doc or an {@link InitOptions}
 */
export function clone<T>(
  doc: Doc<T>,
  _opts?: ActorId | InitOptions<T>,
): Doc<T> {
  const opts = importOpts(_opts)
  opts.enableTextV2 = true
  return stable.clone(doc, opts)
}

/**
 * Create an automerge document from a POJO
 *
 * @param initialState - The initial state which will be copied into the document
 * @typeParam T - The type of the value passed to `from` _and_ the type the resulting document will contain
 * @typeParam actor - The actor ID of the resulting document, if this is null a random actor ID will be used
 *
 * @example
 * ```
 * const doc = automerge.from({
 *     tasks: [
 *         {description: "feed dogs", done: false}
 *     ]
 * })
 * ```
 */
export function from<T extends Record<string, unknown>>(
  initialState: T | Doc<T>,
  _opts?: ActorId | InitOptions<T>,
): Doc<T> {
  const opts = importOpts(_opts)
  opts.enableTextV2 = true
  return stable.from(initialState, opts)
}

/**
 * Load an automerge document from a compressed document produce by {@link save}
 *
 * @typeParam T - The type of the value which is contained in the document.
 *                Note that no validation is done to make sure this type is in
 *                fact the type of the contained value so be a bit careful
 * @param data  - The compressed document
 * @param _opts - Either an actor ID or some {@link InitOptions}, if the actor
 *                ID is null a random actor ID will be created
 *
 * Note that `load` will throw an error if passed incomplete content (for
 * example if you are receiving content over the network and don't know if you
 * have the complete document yet). If you need to handle incomplete content use
 * {@link init} followed by {@link loadIncremental}.
 */
export function load<T>(
  data: Uint8Array,
  _opts?: ActorId | InitOptions<T>,
): Doc<T> {
  const opts = importOpts(_opts)
  opts.enableTextV2 = true
  if (opts.patchCallback) {
    return stable.loadIncremental(stable.init(opts), data)
  } else {
    return stable.load(data, opts)
  }
}

function importOpts<T>(
  _actor?: ActorId | InitOptions<T>,
): stable.InitOptions<T> {
  if (typeof _actor === "object") {
    return _actor
  } else {
    return { actor: _actor }
  }
}

function cursorToIndex<T>(
  state: InternalState<T>,
  value: string,
  index: number | Cursor,
): number {
  if (typeof index == "string") {
    if (/^[0-9]+@[0-9a-zA-z]+$/.test(index)) {
      return state.handle.getCursorPosition(value, index)
    } else {
      throw new RangeError("index must be a number or cursor")
    }
  } else {
    return index
  }
}

/**
 * Modify a string
 *
 * @typeParam T - The type of the value contained in the document
 * @param doc - The document to modify
 * @param path - The path to the string to modify
 * @param index - The position (as a {@link Cursor} or index) to edit.
 *   If a cursor is used then the edit happens such that the cursor will
 *   now point to the end of the newText, so you can continue to reuse
 *   the same cursor for multiple calls to splice.
 * @param del - The number of code units to delete, a positive number
 *   deletes N characters after the cursor, a negative number deletes
 *   N characters before the cursor.
 * @param newText - The string to insert (if any).
 */
export function splice<T>(
  doc: Doc<T>,
  path: stable.Prop[],
  index: number | Cursor,
  del: number,
  newText?: string,
) {
  const objPath = absoluteObjPath(doc, path, "splice")
  if (!_is_proxy(doc)) {
    throw new RangeError("object cannot be modified outside of a change block")
  }
  const state = _state(doc, false)
  _clear_cache(doc)

  index = cursorToIndex(state, objPath, index)

  try {
    return state.handle.splice(objPath, index, del, newText)
  } catch (e) {
    throw new RangeError(`Cannot splice: ${e}`)
  }
}

/**
 * Update the value of a string
 *
 * @typeParam T - The type of the value contained in the document
 * @param doc - The document to modify
 * @param path - The path to the string to modify
 * @param newText - The new text to update the value to
 *
 * @remarks
 * This will calculate a diff between the current value and the new value and
 * then convert that diff into calls to {@link splice}. This will produce results
 * which don't merge as well as directly capturing the user input actions, but
 * sometimes it's not possible to capture user input and this is the best you
 * can do.
 *
 * This is an experimental API and may change in the future.
 *
 * @beta
 */
export function updateText(
  doc: Doc<unknown>,
  path: stable.Prop[],
  newText: string,
) {
  const objPath = absoluteObjPath(doc, path, "updateText")
  if (!_is_proxy(doc)) {
    throw new RangeError("object cannot be modified outside of a change block")
  }
  const state = _state(doc, false)
  _clear_cache(doc)

  try {
    return state.handle.updateText(objPath, newText)
  } catch (e) {
    throw new RangeError(`Cannot updateText: ${e}`)
  }
}

/**
 * Return the text + block markers at a given path
 *
 * @remarks
 * Rich text in automerge is represented as a sequence of characters with block
 * markers appearing inline with the text, and inline formatting spans overlaid
 * on the whole sequence. Block markers are normal automerge maps, but they are
 * only visible via either the {@link block} function or the {@link spans}
 * function. This function returns the current state of the spans
 */
export function spans<T>(doc: Doc<T>, path: stable.Prop[]): Span[] {
  const state = _state(doc, false)
  const objPath = absoluteObjPath(doc, path, "spans")

  try {
    return state.handle.spans(objPath, state.heads)
  } catch (e) {
    throw new RangeError(`Cannot splice: ${e}`)
  }
}

/**
 * Get the block marker at the given index
 */
export function block<T>(
  doc: Doc<T>,
  path: stable.Prop[],
  index: number | Cursor,
) {
  const objPath = absoluteObjPath(doc, path, "splitBlock")
  const state = _state(doc, false)

  index = cursorToIndex(state, objPath, index)

  try {
    return state.handle.getBlock(objPath, index)
  } catch (e) {
    throw new RangeError(`Cannot get block: ${e}`)
  }
}

/**
 * Insert a new block marker at the given index
 */
export function splitBlock<T>(
  doc: Doc<T>,
  path: stable.Prop[],
  index: number | Cursor,
  block: { [key: string]: MaterializeValue },
) {
  if (!_is_proxy(doc)) {
    throw new RangeError("object cannot be modified outside of a change block")
  }
  const objPath = absoluteObjPath(doc, path, "splitBlock")
  const state = _state(doc, false)
  _clear_cache(doc)

  index = cursorToIndex(state, objPath, index)

  try {
    state.handle.splitBlock(objPath, index, block)
  } catch (e) {
    throw new RangeError(`Cannot splice: ${e}`)
  }
}

/**
 * Delete the block marker at the given index
 */
export function joinBlock<T>(
  doc: Doc<T>,
  path: stable.Prop[],
  index: number | Cursor,
) {
  if (!_is_proxy(doc)) {
    throw new RangeError("object cannot be modified outside of a change block")
  }
  const objPath = absoluteObjPath(doc, path, "joinBlock")
  const state = _state(doc, false)
  _clear_cache(doc)

  index = cursorToIndex(state, objPath, index)

  try {
    state.handle.joinBlock(objPath, index)
  } catch (e) {
    throw new RangeError(`Cannot joinBlock: ${e}`)
  }
}

/**
 * Update the block marker at the given index
 */
export function updateBlock<T>(
  doc: Doc<T>,
  path: stable.Prop[],
  index: number | Cursor,
  block: { [key: string]: MaterializeValue },
) {
  if (!_is_proxy(doc)) {
    throw new RangeError("object cannot be modified outside of a change block")
  }
  const objPath = absoluteObjPath(doc, path, "updateBlock")
  const state = _state(doc, false)
  _clear_cache(doc)

  index = cursorToIndex(state, objPath, index)

  try {
    state.handle.updateBlock(objPath, index, block)
  } catch (e) {
    throw new RangeError(`Cannot updateBlock: ${e}`)
  }
}

/**
 * Update the spans at the given path
 *
 * @remarks
 * Like {@link updateText} this will diff `newSpans` against the current state
 * of the text at `path` and perform a reasonably minimal number of operations
 * required to update the spans to the new state.
 */
export function updateSpans<T>(
  doc: Doc<T>,
  path: stable.Prop[],
  newSpans: Span[],
) {
  if (!_is_proxy(doc)) {
    throw new RangeError("object cannot be modified outside of a change block")
  }
  const objPath = absoluteObjPath(doc, path, "updateSpans")
  const state = _state(doc, false)
  _clear_cache(doc)

  try {
    state.handle.updateSpans(objPath, newSpans)
  } catch (e) {
    throw new RangeError(`Cannot updateBlock: ${e}`)
  }
}

/**
 * Returns a cursor for the given position in a string.
 *
 * @remarks
 * A cursor represents a relative position, "before character X",
 * rather than an absolute position. As the document is edited, the
 * cursor remains stable relative to its context, just as you'd expect
 * from a cursor in a concurrent text editor.
 *
 * The string representation is shareable, and so you can use this both
 * to edit the document yourself (using {@link splice}) or to share multiple
 * collaborator's current cursor positions over the network.
 *
 * @typeParam T - The type of the value contained in the document
 * @param doc - The document
 * @param path - The path to the string
 * @param index - The current index of the position of the cursor
 */
export function getCursor<T>(
  doc: Doc<T>,
  path: stable.Prop[],
  index: number,
): Cursor {
  const objPath = absoluteObjPath(doc, path, "getCursor")
  const state = _state(doc, false)

  try {
    return state.handle.getCursor(objPath, index)
  } catch (e) {
    throw new RangeError(`Cannot getCursor: ${e}`)
  }
}

/**
 * Returns the current index of the cursor.
 *
 * @typeParam T - The type of the value contained in the document
 *
 * @param doc - The document
 * @param path - The path to the string
 * @param index - The cursor
 */
export function getCursorPosition<T>(
  doc: Doc<T>,
  path: stable.Prop[],
  cursor: Cursor,
): number {
  const objPath = absoluteObjPath(doc, path, "getCursorPosition")
  const state = _state(doc, false)

  try {
    return state.handle.getCursorPosition(objPath, cursor)
  } catch (e) {
    throw new RangeError(`Cannot getCursorPosition: ${e}`)
  }
}

export function mark<T>(
  doc: Doc<T>,
  path: stable.Prop[],
  range: MarkRange,
  name: string,
  value: MarkValue,
) {
  const objPath = absoluteObjPath(doc, path, "mark")
  if (!_is_proxy(doc)) {
    throw new RangeError("object cannot be modified outside of a change block")
  }
  const state = _state(doc, false)

  try {
    return state.handle.mark(objPath, range, name, value)
  } catch (e) {
    throw new RangeError(`Cannot mark: ${e}`)
  }
}

export function unmark<T>(
  doc: Doc<T>,
  path: stable.Prop[],
  range: MarkRange,
  name: string,
) {
  const objPath = absoluteObjPath(doc, path, "unmark")
  if (!_is_proxy(doc)) {
    throw new RangeError("object cannot be modified outside of a change block")
  }
  const state = _state(doc, false)

  try {
    return state.handle.unmark(objPath, range, name)
  } catch (e) {
    throw new RangeError(`Cannot unmark: ${e}`)
  }
}

export function marks<T>(doc: Doc<T>, path: stable.Prop[]): Mark[] {
  const objPath = absoluteObjPath(doc, path, "marks")
  const state = _state(doc, false)

  try {
    return state.handle.marks(objPath)
  } catch (e) {
    throw new RangeError(`Cannot call marks(): ${e}`)
  }
}

export function marksAt<T>(
  doc: Doc<T>,
  path: stable.Prop[],
  index: number,
): MarkSet {
  const objPath = absoluteObjPath(doc, path, "marksAt")
  const state = _state(doc, false)
  try {
    return state.handle.marksAt(objPath, index)
  } catch (e) {
    throw new RangeError(`Cannot call marksAt(): ${e}`)
  }
}

/**
 * Get the conflicts associated with a property
 *
 * The values of properties in a map in automerge can be conflicted if there
 * are concurrent "put" operations to the same key. Automerge chooses one value
 * arbitrarily (but deterministically, any two nodes who have the same set of
 * changes will choose the same value) from the set of conflicting values to
 * present as the value of the key.
 *
 * Sometimes you may want to examine these conflicts, in this case you can use
 * {@link getConflicts} to get the conflicts for the key.
 *
 * @example
 * ```
 * import * as automerge from "@automerge/automerge"
 *
 * type Profile = {
 *     pets: Array<{name: string, type: string}>
 * }
 *
 * let doc1 = automerge.init<Profile>("aaaa")
 * doc1 = automerge.change(doc1, d => {
 *     d.pets = [{name: "Lassie", type: "dog"}]
 * })
 * let doc2 = automerge.init<Profile>("bbbb")
 * doc2 = automerge.merge(doc2, automerge.clone(doc1))
 *
 * doc2 = automerge.change(doc2, d => {
 *     d.pets[0].name = "Beethoven"
 * })
 *
 * doc1 = automerge.change(doc1, d => {
 *     d.pets[0].name = "Babe"
 * })
 *
 * const doc3 = automerge.merge(doc1, doc2)
 *
 * // Note that here we pass `doc3.pets`, not `doc3`
 * let conflicts = automerge.getConflicts(doc3.pets[0], "name")
 *
 * // The two conflicting values are the keys of the conflicts object
 * assert.deepEqual(Object.values(conflicts), ["Babe", "Beethoven"])
 * ```
 */
export function getConflicts<T>(
  doc: Doc<T>,
  prop: stable.Prop,
): Conflicts | undefined {
  const state = _state(doc, false)
  if (!state.textV2) {
    throw new Error("use getConflicts for a stable document")
  }
  const objectId = _obj(doc)
  if (objectId != null) {
    return unstableConflictAt(state.handle, objectId, prop)
  } else {
    return undefined
  }
}

function absoluteObjPath(
  doc: Doc<unknown>,
  path: stable.Prop[],
  functionName: string,
): string {
  path = path.slice()
  const objectId = _obj(doc)
  if (!objectId) {
    throw new RangeError(`invalid object for ${functionName}`)
  }
  path.unshift(objectId)
  return path.join("/")
}
