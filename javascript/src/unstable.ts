/**
 * # The unstable API
 *
 * This module contains new features we are working on which are either not yet
 * ready for a stable release and/or which will result in backwards incompatible
 * API changes. The API of this module may change in arbitrary ways between
 * point releases - we will always document what these changes are in the
 * [CHANGELOG](#changelog) below, but only depend on this module if you are prepared to deal
 * with frequent changes.
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
  type MarkRange,
  type MarkValue,
  type AutomergeValue,
  type ScalarValue,
} from "./unstable_types"

import type { Mark, MarkRange, MarkValue } from "./unstable_types"

import type { ScalarValue, PatchCallback } from "./stable"

import { type UnstableConflicts as Conflicts } from "./conflicts"
import { unstableConflictAt } from "./conflicts"

export type {
  PutPatch,
  DelPatch,
  SpliceTextPatch,
  InsertPatch,
  IncPatch,
  SyncMessage,
  Heads,
} from "@automerge/automerge-wasm"

export type { ChangeOptions, ApplyOptions, ChangeFn } from "./stable"
export {
  view,
  free,
  getHeads,
  change,
  emptyChange,
  loadIncremental,
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
} from "./stable"

export type InitOptions<T> = {
  /** The actor ID to use for this document, a random one will be generated if `null` is passed */
  actor?: ActorId
  freeze?: boolean
  /** A callback which will be called with the initial patch once the document has finished loading */
  patchCallback?: PatchCallback<T>
}

import { ActorId, Doc } from "./stable"
import * as stable from "./stable"
export { RawString } from "./raw_string"

/** @hidden */
export const getBackend = stable.getBackend

import { _is_proxy, _state, _obj } from "./internal_state"

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
  _opts?: ActorId | InitOptions<T>
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
  _opts?: ActorId | InitOptions<T>
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
  _opts?: ActorId | InitOptions<T>
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
  _actor?: ActorId | InitOptions<T>
): stable.InitOptions<T> {
  if (typeof _actor === "object") {
    return _actor
  } else {
    return { actor: _actor }
  }
}

export function splice<T>(
  doc: Doc<T>,
  prop: stable.Prop,
  index: number,
  del: number,
  newText?: string
) {
  if (!_is_proxy(doc)) {
    throw new RangeError("object cannot be modified outside of a change block")
  }
  const state = _state(doc, false)
  const objectId = _obj(doc)
  if (!objectId) {
    throw new RangeError("invalid object for splice")
  }
  const value = `${objectId}/${prop}`
  try {
    return state.handle.splice(value, index, del, newText)
  } catch (e) {
    throw new RangeError(`Cannot splice: ${e}`)
  }
}

export function mark<T>(
  doc: Doc<T>,
  prop: stable.Prop,
  range: MarkRange,
  name: string,
  value: MarkValue
) {
  if (!_is_proxy(doc)) {
    throw new RangeError("object cannot be modified outside of a change block")
  }
  const state = _state(doc, false)
  const objectId = _obj(doc)
  if (!objectId) {
    throw new RangeError("invalid object for mark")
  }
  const obj = `${objectId}/${prop}`
  try {
    return state.handle.mark(obj, range, name, value)
  } catch (e) {
    throw new RangeError(`Cannot mark: ${e}`)
  }
}

export function unmark<T>(
  doc: Doc<T>,
  prop: stable.Prop,
  range: MarkRange,
  name: string
) {
  if (!_is_proxy(doc)) {
    throw new RangeError("object cannot be modified outside of a change block")
  }
  const state = _state(doc, false)
  const objectId = _obj(doc)
  if (!objectId) {
    throw new RangeError("invalid object for unmark")
  }
  const obj = `${objectId}/${prop}`
  try {
    return state.handle.unmark(obj, range, name)
  } catch (e) {
    throw new RangeError(`Cannot unmark: ${e}`)
  }
}

export function marks<T>(doc: Doc<T>, prop: stable.Prop): Mark[] {
  const state = _state(doc, false)
  const objectId = _obj(doc)
  if (!objectId) {
    throw new RangeError("invalid object for unmark")
  }
  const obj = `${objectId}/${prop}`
  try {
    return state.handle.marks(obj)
  } catch (e) {
    throw new RangeError(`Cannot call marks(): ${e}`)
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
 * assert.deepEqual(Object.values(conflicts), ["Babe", Beethoven"])
 * ```
 */
export function getConflicts<T>(
  doc: Doc<T>,
  prop: stable.Prop
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
