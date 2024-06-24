/** @hidden **/
export { /** @hidden */ uuid } from "./uuid.js"

import { rootProxy } from "./proxies.js"
import { STATE } from "./constants.js"

import {
  type AutomergeValue,
  Counter,
  type Doc,
  type PatchCallback,
  type Patch,
  type PatchSource,
} from "./types.js"
export {
  type AutomergeValue,
  Counter,
  type Doc,
  Int,
  Uint,
  Float64,
  type Patch,
  type PatchCallback,
  type ScalarValue,
  type PatchInfo,
  type PatchSource,
} from "./types.js"

import { Text } from "./text.js"
export { Text } from "./text.js"

import type {
  API as WasmAPI,
  Actor as ActorId,
  Prop,
  ObjID,
  Change,
  DecodedChange,
  Heads,
  MaterializeValue,
  JsSyncState,
  SyncMessage,
  DecodedSyncMessage,
} from "@automerge/automerge-wasm"
export type {
  PutPatch,
  DelPatch,
  SpliceTextPatch,
  InsertPatch,
  IncPatch,
  SyncMessage,
} from "@automerge/automerge-wasm"

/** @hidden **/
type API = WasmAPI

const SyncStateSymbol = Symbol("_syncstate")

/**
 * An opaque type tracking the state of sync with a remote peer
 */
type SyncState = JsSyncState & {
  /** @hidden */
  _opaque: typeof SyncStateSymbol
}

import { ApiHandler, type ChangeToEncode, UseApi } from "./low_level.js"

import { Automerge } from "@automerge/automerge-wasm"

import { RawString } from "./raw_string.js"

import { _state, _is_proxy, _trace, _obj } from "./internal_state.js"

import { stableConflictAt } from "./conflicts.js"

/** Options passed to {@link change}, and {@link emptyChange}
 * @typeParam T - The type of value contained in the document
 */
export type ChangeOptions<T> = {
  /** A message which describes the changes */
  message?: string
  /** The unix timestamp of the change (purely advisory, not used in conflict resolution) */
  time?: number
  /** A callback which will be called to notify the caller of any changes to the document */
  patchCallback?: PatchCallback<T>
}

/** Options passed to {@link loadIncremental}, {@link applyChanges}, and {@link receiveSyncMessage}
 * @typeParam T - The type of value contained in the document
 */
export type ApplyOptions<T> = { patchCallback?: PatchCallback<T> }

/**
 * A List is an extended Array that adds the two helper methods `deleteAt` and `insertAt`.
 */
export interface List<T> extends Array<T> {
  insertAt(index: number, ...args: T[]): List<T>
  deleteAt(index: number, numDelete?: number): List<T>
}

/**
 * Function for use in {@link change} which inserts values into a list at a given index
 * @param list
 * @param index
 * @param values
 */
export function insertAt<T>(list: T[], index: number, ...values: T[]) {
  if (!_is_proxy(list)) {
    throw new RangeError("object cannot be modified outside of a change block")
  }

  ;(list as List<T>).insertAt(index, ...values)
}

/**
 * Function for use in {@link change} which deletes values from a list at a given index
 * @param list
 * @param index
 * @param numDelete
 */
export function deleteAt<T>(list: T[], index: number, numDelete?: number) {
  if (!_is_proxy(list)) {
    throw new RangeError("object cannot be modified outside of a change block")
  }

  ;(list as List<T>).deleteAt(index, numDelete)
}

/**
 * Function which is called by {@link change} when making changes to a `Doc<T>`
 * @typeParam T - The type of value contained in the document
 *
 * This function may mutate `doc`
 */
export type ChangeFn<T> = (doc: T) => void

/** @hidden **/
export interface State<T> {
  change: DecodedChange
  snapshot: T
}

/** @hidden **/
export function use(api: API) {
  UseApi(api)
}

import * as wasm from "@automerge/automerge-wasm"
use(wasm)

/**
 * Options to be passed to {@link init} or {@link load}
 * @typeParam T - The type of the value the document contains
 */
export type InitOptions<T> = {
  /** The actor ID to use for this document, a random one will be generated if `null` is passed */
  actor?: ActorId
  freeze?: boolean
  /** A callback which will be called with the initial patch once the document has finished loading */
  patchCallback?: PatchCallback<T>
  /** @hidden */
  enableTextV2?: boolean
  /** @hidden */
  unchecked?: boolean
  /** Allow loading a document with missing changes */
  allowMissingChanges?: boolean
  /** @hidden */
  convertRawStringsToText?: boolean
}

/** @hidden */
export function getBackend<T>(doc: Doc<T>): Automerge {
  return _state(doc).handle
}

function importOpts<T>(_actor?: ActorId | InitOptions<T>): InitOptions<T> {
  if (typeof _actor === "object") {
    return _actor
  } else {
    return { actor: _actor }
  }
}

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
  const freeze = !!opts.freeze
  const patchCallback = opts.patchCallback
  const text_v1 = !(opts.enableTextV2 || false)
  const actor = opts.actor
  const handle = ApiHandler.create({ actor, text_v1 })
  handle.enableFreeze(!!opts.freeze)
  const textV2 = opts.enableTextV2 || false
  registerDatatypes(handle, textV2)
  const doc = handle.materialize("/", undefined, {
    handle,
    heads: undefined,
    freeze,
    patchCallback,
    textV2,
  }) as Doc<T>
  return doc
}

/**
 * Make an immutable view of an automerge document as at `heads`
 *
 * @remarks
 * The document returned from this function cannot be passed to {@link change}.
 * This is because it shares the same underlying memory as `doc`, but it is
 * consequently a very cheap copy.
 *
 * Note that this function will throw an error if any of the hashes in `heads`
 * are not in the document.
 *
 * @typeParam T - The type of the value contained in the document
 * @param doc - The document to create a view of
 * @param heads - The hashes of the heads to create a view at
 */
export function view<T>(doc: Doc<T>, heads: Heads): Doc<T> {
  const state = _state(doc)
  const handle = state.handle
  return state.handle.materialize("/", heads, {
    ...state,
    handle,
    heads,
  }) as Doc<T>
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
  const state = _state(doc)
  const heads = state.heads
  const opts = importOpts(_opts)
  const handle = state.handle.fork(opts.actor, heads)
  handle.updateDiffCursor()

  // `change` uses the presence of state.heads to determine if we are in a view
  // set it to undefined to indicate that this is a full fat document
  const { heads: _oldHeads, ...stateSansHeads } = state
  stateSansHeads.patchCallback = opts.patchCallback
  return handle.applyPatches(doc, { ...stateSansHeads, handle })
}

/** Explicity free the memory backing a document. Note that this is note
 * necessary in environments which support
 * [`FinalizationRegistry`](https://developer.mozilla.org/en-US/docs/Web/JavaScript/Reference/Global_Objects/FinalizationRegistry)
 */
export function free<T>(doc: Doc<T>) {
  return _state(doc).handle.free()
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
  return _change(init(_opts), "from", {}, d => Object.assign(d, initialState))
    .newDoc
}

/**
 * Update the contents of an automerge document
 * @typeParam T - The type of the value contained in the document
 * @param doc - The document to update
 * @param options - Either a message, an {@link ChangeOptions}, or a {@link ChangeFn}
 * @param callback - A `ChangeFn` to be used if `options` was a `string`
 *
 * Note that if the second argument is a function it will be used as the `ChangeFn` regardless of what the third argument is.
 *
 * @example A simple change
 * ```
 * let doc1 = automerge.init()
 * doc1 = automerge.change(doc1, d => {
 *     d.key = "value"
 * })
 * assert.equal(doc1.key, "value")
 * ```
 *
 * @example A change with a message
 *
 * ```
 * doc1 = automerge.change(doc1, "add another value", d => {
 *     d.key2 = "value2"
 * })
 * ```
 *
 * @example A change with a message and a timestamp
 *
 * ```
 * doc1 = automerge.change(doc1, {message: "add another value", time: 1640995200}, d => {
 *     d.key2 = "value2"
 * })
 * ```
 *
 * @example responding to a patch callback
 * ```
 * let patchedPath
 * let patchCallback = patch => {
 *    patchedPath = patch.path
 * }
 * doc1 = automerge.change(doc1, {message: "add another value", time: 1640995200, patchCallback}, d => {
 *     d.key2 = "value2"
 * })
 * assert.equal(patchedPath, ["key2"])
 * ```
 */
export function change<T>(
  doc: Doc<T>,
  options: string | ChangeOptions<T> | ChangeFn<T>,
  callback?: ChangeFn<T>,
): Doc<T> {
  if (typeof options === "function") {
    return _change(doc, "change", {}, options).newDoc
  } else if (typeof callback === "function") {
    if (typeof options === "string") {
      options = { message: options }
    }
    return _change(doc, "change", options, callback).newDoc
  } else {
    throw RangeError("Invalid args for change")
  }
}

/**
 * The type returned from {@link changeAt}
 */
export type ChangeAtResult<T> = {
  /** The updated document **/
  newDoc: Doc<T>
  /**
   * The heads resulting from the change
   *
   * @remarks
   * Note that this is _not_ the same as the heads of `newDoc`. The newly created
   * change will be added to the history of `newDoc` as if it was _concurrent_
   * with whatever the heads of the document were at the time of the change.
   * This means that `newHeads` will be the same as the heads of a fork of
   * `newDoc` at the given heads to which the change was applied.
   *
   * This field will be `null` if no change was made
   */
  newHeads: Heads | null
}

/**
 * Make a change to the document as it was at a particular point in history
 * @typeParam T - The type of the value contained in the document
 * @param doc - The document to update
 * @param scope - The heads representing the point in history to make the change
 * @param options - Either a message or a {@link ChangeOptions} for the new change
 * @param callback - A `ChangeFn` to be used if `options` was a `string`
 *
 * @remarks
 * This function is similar to {@link change} but allows you to make changes to
 * the document as if it were at a particular point in time. To understand this
 * imagine a document created with the following history:
 *
 * ```ts
 * let doc = automerge.from({..})
 * doc = automerge.change(doc, () => {...})
 *
 * const heads = automerge.getHeads(doc)
 *
 * // fork the document make a change
 * let fork = automerge.fork(doc)
 * fork = automerge.change(fork, () => {...})
 * const headsOnFork = automerge.getHeads(fork)
 *
 * // make a change on the original doc
 * doc = automerge.change(doc, () => {...})
 * const headsOnOriginal = automerge.getHeads(doc)
 *
 * // now merge the changes back to the original document
 * doc = automerge.merge(doc, fork)
 *
 * // The heads of the document will now be (headsOnFork, headsOnOriginal)
 * ```
 *
 * {@link ChangeAt} produces an equivalent history, but without having to
 * create a fork of the document. In particular the `newHeads` field of the
 * returned {@link ChangeAtResult} will be the same as `headsOnFork`.
 *
 * Why would you want this? It's typically used in conjunction with {@link diff}
 * to reconcile state which is managed concurrently with the document. For
 * example, if you have a text editor component which the user is modifying
 * and you can't send the changes to the document synchronously you might follow
 * a workflow like this:
 *
 * * On initialization save the current heads of the document in the text editor state
 * * Every time the user makes a change record the change in the text editor state
 *
 * Now from time to time reconcile the editor state and the document
 * * Load the last saved heads from the text editor state, call them `oldHeads`
 * * Apply all the unreconciled changes to the document using `changeAt(doc, oldHeads, ...)`
 * * Get the diff from the resulting document to the current document using {@link diff}
 *   passing the {@link ChangeAtResult.newHeads} as the `before` argument and the
 *   heads of the entire document as the `after` argument.
 * * Apply the diff to the text editor state
 * * Save the current heads of the document in the text editor state
 */
export function changeAt<T>(
  doc: Doc<T>,
  scope: Heads,
  options: string | ChangeOptions<T> | ChangeFn<T>,
  callback?: ChangeFn<T>,
): ChangeAtResult<T> {
  if (typeof options === "function") {
    return _change(doc, "changeAt", {}, options, scope)
  } else if (typeof callback === "function") {
    if (typeof options === "string") {
      options = { message: options }
    }
    return _change(doc, "changeAt", options, callback, scope)
  } else {
    throw RangeError("Invalid args for changeAt")
  }
}

function progressDocument<T>(
  doc: Doc<T>,
  source: PatchSource,
  heads: Heads | null,
  callback?: PatchCallback<T>,
): Doc<T> {
  if (heads == null) {
    return doc
  }
  const state = _state(doc)
  const nextState = { ...state, heads: undefined }

  const { value: nextDoc, patches } = state.handle.applyAndReturnPatches(
    doc,
    nextState,
  )

  if (patches.length > 0) {
    if (callback != null) {
      callback(patches, { before: doc, after: nextDoc, source })
    }

    const newState = _state(nextDoc)

    newState.mostRecentPatch = {
      before: _state(doc).heads,
      after: newState.handle.getHeads(),
      patches,
    }
  }

  state.heads = heads
  return nextDoc
}

function _change<T>(
  doc: Doc<T>,
  source: PatchSource,
  options: ChangeOptions<T>,
  callback: ChangeFn<T>,
  scope?: Heads,
): { newDoc: Doc<T>; newHeads: Heads | null } {
  if (typeof callback !== "function") {
    throw new RangeError("invalid change function")
  }

  const state = _state(doc)

  if (doc === undefined || state === undefined) {
    throw new RangeError("must be the document root")
  }
  if (state.heads) {
    throw new RangeError(
      "Attempting to change an outdated document.  Use Automerge.clone() if you wish to make a writable copy.",
    )
  }
  if (_is_proxy(doc)) {
    throw new RangeError("Calls to Automerge.change cannot be nested")
  }
  let heads = state.handle.getHeads()
  if (scope && headsEqual(scope, heads)) {
    scope = undefined
  }
  if (scope) {
    state.handle.isolate(scope)
    heads = scope
  }
  try {
    state.heads = heads
    const root: T = rootProxy(state.handle, state.textV2)
    callback(root)
    if (state.handle.pendingOps() === 0) {
      state.heads = undefined
      if (scope) {
        state.handle.integrate()
      }
      return {
        newDoc: doc,
        newHeads: null,
      }
    } else {
      const newHead = state.handle.commit(options.message, options.time)
      state.handle.integrate()
      return {
        newDoc: progressDocument(
          doc,
          source,
          heads,
          options.patchCallback || state.patchCallback,
        ),
        newHeads: newHead != null ? [newHead] : null,
      }
    }
  } catch (e) {
    state.heads = undefined
    state.handle.rollback()
    throw e
  }
}

/**
 * Make a change to a document which does not modify the document
 *
 * @param doc - The doc to add the empty change to
 * @param options - Either a message or a {@link ChangeOptions} for the new change
 *
 * Why would you want to do this? One reason might be that you have merged
 * changes from some other peers and you want to generate a change which
 * depends on those merged changes so that you can sign the new change with all
 * of the merged changes as part of the new change.
 */
export function emptyChange<T>(
  doc: Doc<T>,
  options: string | ChangeOptions<T> | void,
) {
  if (options === undefined) {
    options = {}
  }
  if (typeof options === "string") {
    options = { message: options }
  }

  const state = _state(doc)

  if (state.heads) {
    throw new RangeError(
      "Attempting to change an outdated document.  Use Automerge.clone() if you wish to make a writable copy.",
    )
  }
  if (_is_proxy(doc)) {
    throw new RangeError("Calls to Automerge.change cannot be nested")
  }

  const heads = state.handle.getHeads()
  state.handle.emptyChange(options.message, options.time)
  return progressDocument(doc, "emptyChange", heads)
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
  const actor = opts.actor
  const patchCallback = opts.patchCallback
  const text_v1 = !(opts.enableTextV2 || false)
  const unchecked = opts.unchecked || false
  const allowMissingDeps = opts.allowMissingChanges || false
  const convertRawStringsToText = opts.convertRawStringsToText || false
  const handle = ApiHandler.load(data, {
    text_v1,
    actor,
    unchecked,
    allowMissingDeps,
    convertRawStringsToText,
  })
  handle.enableFreeze(!!opts.freeze)
  const textV2 = opts.enableTextV2 || false
  registerDatatypes(handle, textV2)
  const doc = handle.materialize("/", undefined, {
    handle,
    heads: undefined,
    patchCallback,
    textV2,
  }) as Doc<T>
  return doc
}

/**
 * Load changes produced by {@link saveIncremental}, or partial changes
 *
 * @typeParam T - The type of the value which is contained in the document.
 *                Note that no validation is done to make sure this type is in
 *                fact the type of the contained value so be a bit careful
 * @param data  - The compressedchanges
 * @param opts  - an {@link ApplyOptions}
 *
 * This function is useful when staying up to date with a connected peer.
 * Perhaps the other end sent you a full compresed document which you loaded
 * with {@link load} and they're sending you the result of
 * {@link getLastLocalChange} every time they make a change.
 *
 * Note that this function will succesfully load the results of {@link save} as
 * well as {@link getLastLocalChange} or any other incremental change.
 */
export function loadIncremental<T>(
  doc: Doc<T>,
  data: Uint8Array,
  opts?: ApplyOptions<T>,
): Doc<T> {
  if (!opts) {
    opts = {}
  }
  const state = _state(doc)
  if (state.heads) {
    throw new RangeError(
      "Attempting to change an out of date document - set at: " + _trace(doc),
    )
  }
  if (_is_proxy(doc)) {
    throw new RangeError("Calls to Automerge.change cannot be nested")
  }
  const heads = state.handle.getHeads()
  state.handle.loadIncremental(data)
  return progressDocument(
    doc,
    "loadIncremental",
    heads,
    opts.patchCallback || state.patchCallback,
  )
}

/**
 * Create binary save data to be appended to a save file or fed into {@link loadIncremental}
 *
 * @typeParam T - The type of the value which is contained in the document.
 *                Note that no validation is done to make sure this type is in
 *                fact the type of the contained value so be a bit careful
 *
 * This function is useful for incrementally saving state.  The data can be appended to a
 * automerge save file, or passed to a document replicating its state.
 *
 */
export function saveIncremental<T>(doc: Doc<T>): Uint8Array {
  const state = _state(doc)
  if (state.heads) {
    throw new RangeError(
      "Attempting to change an out of date document - set at: " + _trace(doc),
    )
  }
  if (_is_proxy(doc)) {
    throw new RangeError("Calls to Automerge.change cannot be nested")
  }
  return state.handle.saveIncremental()
}

/**
 * Export the contents of a document to a compressed format
 *
 * @param doc - The doc to save
 *
 * The returned bytes can be passed to {@link load} or {@link loadIncremental}
 */
export function save<T>(doc: Doc<T>): Uint8Array {
  return _state(doc).handle.save()
}

/**
 * Merge `remote` into `local`
 * @typeParam T - The type of values contained in each document
 * @param local - The document to merge changes into
 * @param remote - The document to merge changes from
 *
 * @returns - The merged document
 *
 * Often when you are merging documents you will also need to clone them. Both
 * arguments to `merge` are frozen after the call so you can no longer call
 * mutating methods (such as {@link change}) on them. The symtom of this will be
 * an error which says "Attempting to change an out of date document". To
 * overcome this call {@link clone} on the argument before passing it to {@link
 * merge}.
 */
export function merge<T>(local: Doc<T>, remote: Doc<T>): Doc<T> {
  const localState = _state(local)

  if (localState.heads) {
    throw new RangeError(
      "Attempting to change an out of date document - set at: " + _trace(local),
    )
  }
  const heads = localState.handle.getHeads()
  const remoteState = _state(remote)
  const changes = localState.handle.getChangesAdded(remoteState.handle)
  localState.handle.applyChanges(changes)
  return progressDocument(local, "merge", heads, localState.patchCallback)
}

/**
 * Get the actor ID associated with the document
 */
export function getActorId<T>(doc: Doc<T>): ActorId {
  const state = _state(doc)
  return state.handle.getActorId()
}

/**
 * The type of conflicts for particular key or index
 *
 * Maps and sequences in automerge can contain conflicting values for a
 * particular key or index. In this case {@link getConflicts} can be used to
 * obtain a `Conflicts` representing the multiple values present for the property
 *
 * A `Conflicts` is a map from a unique (per property or index) key to one of
 * the possible conflicting values for the given property.
 */
type Conflicts = { [key: string]: AutomergeValue }

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
  prop: Prop,
): Conflicts | undefined {
  const state = _state(doc, false)
  if (state.textV2) {
    throw new Error("use unstable.getConflicts for an unstable document")
  }
  const objectId = _obj(doc)
  if (objectId != null) {
    return stableConflictAt(state.handle, objectId, prop)
  } else {
    return undefined
  }
}

/**
 * Get the binary representation of the last change which was made to this doc
 *
 * This is most useful when staying in sync with other peers, every time you
 * make a change locally via {@link change} you immediately call {@link
 * getLastLocalChange} and send the result over the network to other peers.
 */
export function getLastLocalChange<T>(doc: Doc<T>): Change | undefined {
  const state = _state(doc)
  return state.handle.getLastLocalChange() || undefined
}

/**
 * Return the object ID of an arbitrary javascript value
 *
 * This is useful to determine if something is actually an automerge document,
 * if `doc` is not an automerge document this will return null.
 */
// eslint-disable-next-line @typescript-eslint/no-explicit-any
export function getObjectId(doc: any, prop?: Prop): ObjID | null {
  if (prop) {
    const state = _state(doc, false)
    const objectId = _obj(doc)
    if (!state || !objectId) {
      return null
    }
    return state.handle.get(objectId, prop) as ObjID
  } else {
    return _obj(doc)
  }
}

/**
 * Get the changes which are in `newState` but not in `oldState`. The returned
 * changes can be loaded in `oldState` via {@link applyChanges}.
 *
 * Note that this will crash if there are changes in `oldState` which are not in `newState`.
 */
export function getChanges<T>(oldState: Doc<T>, newState: Doc<T>): Change[] {
  const n = _state(newState)
  return n.handle.getChanges(getHeads(oldState))
}

/**
 * Get all the changes in a document
 *
 * This is different to {@link save} because the output is an array of changes
 * which can be individually applied via {@link applyChanges}`
 *
 */
export function getAllChanges<T>(doc: Doc<T>): Change[] {
  const state = _state(doc)
  return state.handle.getChanges([])
}

/**
 * Apply changes received from another document
 *
 * `doc` will be updated to reflect the `changes`. If there are changes which
 * we do not have dependencies for yet those will be stored in the document and
 * applied when the depended on changes arrive.
 *
 * You can use the {@link ApplyOptions} to pass a patchcallback which will be
 * informed of any changes which occur as a result of applying the changes
 *
 */
export function applyChanges<T>(
  doc: Doc<T>,
  changes: Change[],
  opts?: ApplyOptions<T>,
): [Doc<T>] {
  const state = _state(doc)
  if (!opts) {
    opts = {}
  }
  if (state.heads) {
    throw new RangeError(
      "Attempting to change an outdated document.  Use Automerge.clone() if you wish to make a writable copy.",
    )
  }
  if (_is_proxy(doc)) {
    throw new RangeError("Calls to Automerge.change cannot be nested")
  }
  const heads = state.handle.getHeads()
  state.handle.applyChanges(changes)
  state.heads = heads
  return [
    progressDocument(
      doc,
      "applyChanges",
      heads,
      opts.patchCallback || state.patchCallback,
    ),
  ]
}

/** @hidden */
export function getHistory<T>(doc: Doc<T>): State<T>[] {
  const textV2 = _state(doc).textV2
  const history = getAllChanges(doc)
  return history.map((change, index) => ({
    get change() {
      return decodeChange(change)
    },
    get snapshot() {
      const [state] = applyChanges(
        init({ enableTextV2: textV2 }),
        history.slice(0, index + 1),
      )
      return <T>state
    },
  }))
}

/**
 * Create a set of patches representing the change from one set of heads to another
 *
 * If either of the heads are missing from the document the returned set of patches will be empty
 */
export function diff(doc: Doc<unknown>, before: Heads, after: Heads): Patch[] {
  checkHeads(before, "before")
  checkHeads(after, "after")
  const state = _state(doc)
  if (
    state.mostRecentPatch &&
    equals(state.mostRecentPatch.before, before) &&
    equals(state.mostRecentPatch.after, after)
  ) {
    return state.mostRecentPatch.patches
  }
  return state.handle.diff(before, after)
}

function headsEqual(heads1: Heads, heads2: Heads): boolean {
  if (heads1.length !== heads2.length) {
    return false
  }
  for (let i = 0; i < heads1.length; i++) {
    if (heads1[i] !== heads2[i]) {
      return false
    }
  }
  return true
}

function checkHeads(heads: Heads, fieldname: string) {
  if (!Array.isArray(heads)) {
    throw new Error(`${fieldname} must be an array`)
  }
}

/** @hidden */
// FIXME : no tests
// FIXME can we just use deep equals now?
export function equals(val1: unknown, val2: unknown): boolean {
  if (!isObject(val1) || !isObject(val2)) return val1 === val2
  const keys1 = Object.keys(val1).sort(),
    keys2 = Object.keys(val2).sort()
  if (keys1.length !== keys2.length) return false
  for (let i = 0; i < keys1.length; i++) {
    if (keys1[i] !== keys2[i]) return false
    if (!equals(val1[keys1[i]], val2[keys2[i]])) return false
  }
  return true
}

/**
 * encode a {@link SyncState} into binary to send over the network
 *
 * @group sync
 * */
export function encodeSyncState(state: SyncState): Uint8Array {
  const sync = ApiHandler.importSyncState(state)
  const result = ApiHandler.encodeSyncState(sync)
  sync.free()
  return result
}

/**
 * Decode some binary data into a {@link SyncState}
 *
 * @group sync
 */
export function decodeSyncState(state: Uint8Array): SyncState {
  const sync = ApiHandler.decodeSyncState(state)
  const result = ApiHandler.exportSyncState(sync)
  sync.free()
  return result as SyncState
}

/**
 * Generate a sync message to send to the peer represented by `inState`
 * @param doc - The doc to generate messages about
 * @param inState - The {@link SyncState} representing the peer we are talking to
 *
 * @group sync
 *
 * @returns An array of `[newSyncState, syncMessage | null]` where
 * `newSyncState` should replace `inState` and `syncMessage` should be sent to
 * the peer if it is not null. If `syncMessage` is null then we are up to date.
 */
export function generateSyncMessage<T>(
  doc: Doc<T>,
  inState: SyncState,
): [SyncState, SyncMessage | null] {
  const state = _state(doc)
  const syncState = ApiHandler.importSyncState(inState)
  const message = state.handle.generateSyncMessage(syncState)
  const outState = ApiHandler.exportSyncState(syncState) as SyncState
  return [outState, message]
}

/**
 * Update a document and our sync state on receiving a sync message
 *
 * @group sync
 *
 * @param doc     - The doc the sync message is about
 * @param inState - The {@link SyncState} for the peer we are communicating with
 * @param message - The message which was received
 * @param opts    - Any {@link ApplyOption}s, used for passing a
 *                  {@link PatchCallback} which will be informed of any changes
 *                  in `doc` which occur because of the received sync message.
 *
 * @returns An array of `[newDoc, newSyncState, syncMessage | null]` where
 * `newDoc` is the updated state of `doc`, `newSyncState` should replace
 * `inState` and `syncMessage` should be sent to the peer if it is not null. If
 * `syncMessage` is null then we are up to date.
 */
export function receiveSyncMessage<T>(
  doc: Doc<T>,
  inState: SyncState,
  message: SyncMessage,
  opts?: ApplyOptions<T>,
): [Doc<T>, SyncState, null] {
  const syncState = ApiHandler.importSyncState(inState)
  if (!opts) {
    opts = {}
  }
  const state = _state(doc)
  if (state.heads) {
    throw new RangeError(
      "Attempting to change an outdated document.  Use Automerge.clone() if you wish to make a writable copy.",
    )
  }
  if (_is_proxy(doc)) {
    throw new RangeError("Calls to Automerge.change cannot be nested")
  }
  const heads = state.handle.getHeads()
  state.handle.receiveSyncMessage(syncState, message)
  const outSyncState = ApiHandler.exportSyncState(syncState) as SyncState
  return [
    progressDocument(
      doc,
      "receiveSyncMessage",
      heads,
      opts.patchCallback || state.patchCallback,
    ),
    outSyncState,
    null,
  ]
}

/**
 * Create a new, blank {@link SyncState}
 *
 * When communicating with a peer for the first time use this to generate a new
 * {@link SyncState} for them
 *
 * @group sync
 */
export function initSyncState(): SyncState {
  return ApiHandler.exportSyncState(ApiHandler.initSyncState()) as SyncState
}

/** @hidden */
export function encodeChange(change: ChangeToEncode): Change {
  return ApiHandler.encodeChange(change)
}

/** @hidden */
export function decodeChange(data: Change): DecodedChange {
  return ApiHandler.decodeChange(data)
}

/** @hidden */
export function encodeSyncMessage(message: DecodedSyncMessage): SyncMessage {
  return ApiHandler.encodeSyncMessage(message)
}

/** @hidden */
export function decodeSyncMessage(message: SyncMessage): DecodedSyncMessage {
  return ApiHandler.decodeSyncMessage(message)
}

/**
 * Get any changes in `doc` which are not dependencies of `heads`
 */
export function getMissingDeps<T>(doc: Doc<T>, heads: Heads): Heads {
  const state = _state(doc)
  return state.handle.getMissingDeps(heads)
}

/**
 * Get the hashes of the heads of this document
 */
export function getHeads<T>(doc: Doc<T>): Heads {
  const state = _state(doc)
  return state.heads || state.handle.getHeads()
}

/** @hidden */
export function dump<T>(doc: Doc<T>) {
  const state = _state(doc)
  state.handle.dump()
}

/** @hidden */
export function toJS<T>(doc: Doc<T>): T {
  const state = _state(doc)
  const enabled = state.handle.enableFreeze(false)
  const result = state.handle.materialize()
  state.handle.enableFreeze(enabled)
  return result as T
}

export function isAutomerge(doc: unknown): boolean {
  if (typeof doc == "object" && doc !== null) {
    return getObjectId(doc) === "_root" && !!Reflect.get(doc, STATE)
  } else {
    return false
  }
}

function isObject(obj: unknown): obj is Record<string, unknown> {
  return typeof obj === "object" && obj !== null
}

export function saveSince(doc: Doc<unknown>, heads: Heads): Uint8Array {
  const state = _state(doc)
  const result = state.handle.saveSince(heads)
  return result
}

export type {
  API,
  SyncState,
  ActorId,
  Conflicts,
  Prop,
  Change,
  ObjID,
  DecodedChange,
  DecodedSyncMessage,
  Heads,
  MaterializeValue,
}

function registerDatatypes(handle: Automerge, textV2: boolean) {
  handle.registerDatatype(
    "counter",
    (n: number) => new Counter(n),
    n => {
      if (n instanceof Counter) {
        return n.value
      }
    },
  )
  if (textV2) {
    handle.registerDatatype(
      "str",
      (n: string) => {
        return new RawString(n)
      },
      s => {
        if (s instanceof RawString) {
          return s.val
        }
      },
    )
  } else {
    handle.registerDatatype(
      "text",
      (n: string) => new Text(n),
      t => {
        if (t instanceof Text) {
          return t.join("")
        }
      },
    )
  }
}
