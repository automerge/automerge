
/** @hidden **/
export {/** @hidden */ uuid} from './uuid'

import {rootProxy, listProxy, mapProxy} from "./proxies"
import {STATE, HEADS, TRACE, IS_PROXY, OBJECT_ID, READ_ONLY, FROZEN} from "./constants"

import {AutomergeValue, Text, Counter} from "./types"
export {AutomergeValue, Counter, Int, Uint, Float64, ScalarValue} from "./types"

import {type API, type Patch} from "@automerge/automerge-wasm";
export { type Patch, PutPatch, DelPatch, SplicePatch, IncPatch, SyncMessage, } from "@automerge/automerge-wasm"
import {ApiHandler, UseApi} from "./low_level"

import {Actor as ActorId, Prop, ObjID, Change, DecodedChange, Heads, Automerge, MaterializeValue} from "@automerge/automerge-wasm"
import {JsSyncState as SyncState, SyncMessage, DecodedSyncMessage} from "@automerge/automerge-wasm"

/** Options passed to {@link change}, and {@link emptyChange}
 * @typeParam T - The type of value contained in the document
 */
export type ChangeOptions<T> = {
    /** A message which describes the changes */
    message?: string,
    /** The unix timestamp of the change (purely advisory, not used in conflict resolution) */
    time?: number,
    /** A callback which will be called to notify the caller of any changes to the document */
    patchCallback?: PatchCallback<T>
}

/** Options passed to {@link loadIncremental}, {@link applyChanges}, and {@link receiveSyncMessage}
 * @typeParam T - The type of value contained in the document
 */
export type ApplyOptions<T> = {patchCallback?: PatchCallback<T>}

/** 
 * An automerge document.
 * @typeParam T - The type of the value contained in this document
 *
 * Note that this provides read only access to the fields of the value. To
 * modify the value use {@link change}
 */
export type Doc<T> = {readonly [P in keyof T]: T[P]}

/**
 * Function which is called by {@link change} when making changes to a `Doc<T>`
 * @typeParam T - The type of value contained in the document
 *
 * This function may mutate `doc`
 */
export type ChangeFn<T> = (doc: T) => void

/**
 * Callback which is called by various methods in this library to notify the
 * user of what changes have been made.
 * @param patch - A description of the changes made
 * @param before - The document before the change was made
 * @param after - The document after the change was made
 */
export type PatchCallback<T> = (patch: Patch, before: Doc<T>, after: Doc<T>) => void

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
    actor?: ActorId,
    freeze?: boolean,
    /** A callback which will be called with the initial patch once the document has finished loading */
    patchCallback?: PatchCallback<T>,
};


interface InternalState<T> {
    handle: Automerge,
    heads: Heads | undefined,
    freeze: boolean,
    patchCallback?: PatchCallback<T>
}

/** @hidden */
export function getBackend<T>(doc: Doc<T>): Automerge {
    return _state(doc).handle
}

function _state<T>(doc: Doc<T>, checkroot = true): InternalState<T> {
    if (typeof doc !== 'object') {
        throw new RangeError("must be the document root")
    }
    const state = Reflect.get(doc, STATE) as InternalState<T>
    if (state === undefined || state == null || (checkroot && _obj(doc) !== "_root")) {
        throw new RangeError("must be the document root")
    }
    return state
}

function _frozen<T>(doc: Doc<T>): boolean {
    return Reflect.get(doc, FROZEN) === true
}

function _trace<T>(doc: Doc<T>): string | undefined {
    return Reflect.get(doc, TRACE) as string
}

function _obj<T>(doc: Doc<T>): ObjID | null {
    if (!(typeof doc === 'object') || doc === null) {
        return null
    }
    return Reflect.get(doc, OBJECT_ID) as ObjID
}

function _readonly<T>(doc: Doc<T>): boolean {
    return Reflect.get(doc, READ_ONLY) !== false
}

function importOpts<T>(_actor?: ActorId | InitOptions<T>): InitOptions<T> {
    if (typeof _actor === 'object') {
        return _actor
    } else {
        return {actor: _actor}
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
    let opts = importOpts(_opts)
    let freeze = !!opts.freeze
    let patchCallback = opts.patchCallback
    const handle = ApiHandler.create(opts.actor)
    handle.enablePatches(true)
    handle.enableFreeze(!!opts.freeze)
    handle.registerDatatype("counter", (n) => new Counter(n))
    const doc = handle.materialize("/", undefined, {handle, heads: undefined, freeze, patchCallback}) as Doc<T>
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
    return state.handle.materialize("/", heads, { ...state, handle, heads }) as any
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
export function clone<T>(doc: Doc<T>, _opts?: ActorId | InitOptions<T>): Doc<T> {
    const state = _state(doc)
    const heads = state.heads
    const opts = importOpts(_opts)
    const handle = state.handle.fork(opts.actor, heads)

    // `change` uses the presence of state.heads to determine if we are in a view
    // set it to undefined to indicate that this is a full fat document
    const {heads: oldHeads, ...stateSansHeads} = state
    return handle.applyPatches(doc, { ... stateSansHeads, handle })
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
export function from<T extends Record<string, unknown>>(initialState: T | Doc<T>, actor?: ActorId): Doc<T> {
    return change(init(actor), (d) => Object.assign(d, initialState))
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
 * doc1 = automerge.change(doc1, {message: "add another value", timestamp: 1640995200}, d => {
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
 * doc1 = automerge.change(doc1, {message, "add another value", timestamp: 1640995200, patchCallback}, d => {
 *     d.key2 = "value2"
 * })
 * assert.equal(patchedPath, ["key2"])
 * ```
 */
export function change<T>(doc: Doc<T>, options: string | ChangeOptions<T> | ChangeFn<T>, callback?: ChangeFn<T>): Doc<T> {
    if (typeof options === 'function') {
        return _change(doc, {}, options)
    } else if (typeof callback === 'function') {
        if (typeof options === "string") {
            options = {message: options}
        }
        return _change(doc, options, callback)
    } else {
        throw RangeError("Invalid args for change")
    }
}

function progressDocument<T>(doc: Doc<T>, heads: Heads, callback?: PatchCallback<T>): Doc<T> {
    let state = _state(doc)
    let nextState = {...state, heads: undefined};
    let nextDoc = state.handle.applyPatches(doc, nextState, callback)
    state.heads = heads
    return nextDoc
}

function _change<T>(doc: Doc<T>, options: ChangeOptions<T>, callback: ChangeFn<T>): Doc<T> {


    if (typeof callback !== "function") {
        throw new RangeError("invalid change function");
    }

    const state = _state(doc)

    if (doc === undefined || state === undefined) {
        throw new RangeError("must be the document root");
    }
    if (state.heads) {
        throw new RangeError("Attempting to change an outdated document.  Use Automerge.clone() if you wish to make a writable copy.")
    }
    if (_readonly(doc) === false) {
        throw new RangeError("Calls to Automerge.change cannot be nested")
    }
    const heads = state.handle.getHeads()
    try {
        state.heads = heads
        const root: T = rootProxy(state.handle);
        callback(root)
        if (state.handle.pendingOps() === 0) {
            state.heads = undefined
            return doc
        } else {
            state.handle.commit(options.message, options.time)
            return progressDocument(doc, heads, options.patchCallback || state.patchCallback);
        }
    } catch (e) {
        //console.log("ERROR: ",e)
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
export function emptyChange<T>(doc: Doc<T>, options: string | ChangeOptions<T>) {
    if (options === undefined) {
        options = {}
    }
    if (typeof options === "string") {
        options = {message: options}
    }

    const state = _state(doc)

    if (state.heads) {
        throw new RangeError("Attempting to change an outdated document.  Use Automerge.clone() if you wish to make a writable copy.")
    }
    if (_readonly(doc) === false) {
        throw new RangeError("Calls to Automerge.change cannot be nested")
    }

    const heads = state.handle.getHeads()
    state.handle.commit(options.message, options.time)
    return progressDocument(doc, heads)
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
export function load<T>(data: Uint8Array, _opts?: ActorId | InitOptions<T>): Doc<T> {
    const opts = importOpts(_opts)
    const actor = opts.actor
    const patchCallback = opts.patchCallback
    const handle = ApiHandler.load(data, actor)
    handle.enablePatches(true)
    handle.enableFreeze(!!opts.freeze)
    handle.registerDatatype("counter", (n) => new Counter(n))
    const doc: any = handle.materialize("/", undefined, {handle, heads: undefined, patchCallback}) as Doc<T>
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
export function loadIncremental<T>(doc: Doc<T>, data: Uint8Array, opts?: ApplyOptions<T>): Doc<T> {
    if (!opts) {opts = {}}
    const state = _state(doc)
    if (state.heads) {
        throw new RangeError("Attempting to change an out of date document - set at: " + _trace(doc));
    }
    if (_readonly(doc) === false) {
        throw new RangeError("Calls to Automerge.change cannot be nested")
    }
    const heads = state.handle.getHeads()
    state.handle.loadIncremental(data)
    return progressDocument(doc, heads, opts.patchCallback || state.patchCallback)
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
 * Merge `local` into `remote`
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
        throw new RangeError("Attempting to change an out of date document - set at: " + _trace(local));
    }
    const heads = localState.handle.getHeads()
    const remoteState = _state(remote)
    const changes = localState.handle.getChangesAdded(remoteState.handle)
    localState.handle.applyChanges(changes)
    return progressDocument(local, heads, localState.patchCallback)
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
type Conflicts = {[key: string]: AutomergeValue}

function conflictAt(context: Automerge, objectId: ObjID, prop: Prop): Conflicts | undefined {
    const values = context.getAll(objectId, prop)
    if (values.length <= 1) {
        return
    }
    const result: Conflicts = {}
    for (const fullVal of values) {
        switch (fullVal[0]) {
            case "map":
                result[fullVal[1]] = mapProxy(context, fullVal[1], [prop], true)
                break;
            case "list":
                result[fullVal[1]] = listProxy(context, fullVal[1], [prop], true)
                break;
            case "text":
                result[fullVal[1]] = context.text(fullVal[1])
                break;
            //case "table":
            //case "cursor":
            case "str":
            case "uint":
            case "int":
            case "f64":
            case "boolean":
            case "bytes":
            case "null":
                result[fullVal[2]] = fullVal[1]
                break;
            case "counter":
                result[fullVal[2]] = new Counter(fullVal[1])
                break;
            case "timestamp":
                result[fullVal[2]] = new Date(fullVal[1])
                break;
            default:
                throw RangeError(`datatype ${fullVal[0]} unimplemented`)
        }
    }
    return result
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
export function getConflicts<T>(doc: Doc<T>, prop: Prop): Conflicts | undefined {
    const state = _state(doc, false)
    const objectId = _obj(doc)
    if (objectId != null) {
        return conflictAt(state.handle, objectId, prop)
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
export function getObjectId(doc: any, prop?: Prop): ObjID | null {
    if (prop) {
      const state = _state(doc, false)
      const objectId = _obj(doc)
      if (!state || !objectId) {
        throw new RangeError("invalid object for splice")
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
    const o = _state(oldState)
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
export function applyChanges<T>(doc: Doc<T>, changes: Change[], opts?: ApplyOptions<T>): [Doc<T>] {
    const state = _state(doc)
    if (!opts) {opts = {}}
    if (state.heads) {
        throw new RangeError("Attempting to change an outdated document.  Use Automerge.clone() if you wish to make a writable copy.")
    }
    if (_readonly(doc) === false) {
        throw new RangeError("Calls to Automerge.change cannot be nested")
    }
    const heads = state.handle.getHeads();
    state.handle.applyChanges(changes)
    state.heads = heads;
    return [progressDocument(doc, heads, opts.patchCallback || state.patchCallback)]
}

/** @hidden */
export function getHistory<T>(doc: Doc<T>): State<T>[] {
    const history = getAllChanges(doc)
    return history.map((change, index) => ({
        get change() {
            return decodeChange(change)
        },
        get snapshot() {
            const [state] = applyChanges(init(), history.slice(0, index + 1))
            return <T>state
        }
    })
    )
}

/** @hidden */
// FIXME : no tests
// FIXME can we just use deep equals now?
export function equals(val1: unknown, val2: unknown): boolean {
    if (!isObject(val1) || !isObject(val2)) return val1 === val2
    const keys1 = Object.keys(val1).sort(), keys2 = Object.keys(val2).sort()
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
    let sync = ApiHandler.decodeSyncState(state)
    let result = ApiHandler.exportSyncState(sync)
    sync.free()
    return result
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
export function generateSyncMessage<T>(doc: Doc<T>, inState: SyncState): [SyncState, SyncMessage | null] {
    const state = _state(doc)
    const syncState = ApiHandler.importSyncState(inState)
    const message = state.handle.generateSyncMessage(syncState)
    const outState = ApiHandler.exportSyncState(syncState)
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
export function receiveSyncMessage<T>(doc: Doc<T>, inState: SyncState, message: SyncMessage, opts?: ApplyOptions<T>): [Doc<T>, SyncState, null] {
    const syncState = ApiHandler.importSyncState(inState)
    if (!opts) {opts = {}}
    const state = _state(doc)
    if (state.heads) {
        throw new RangeError("Attempting to change an outdated document.  Use Automerge.clone() if you wish to make a writable copy.")
    }
    if (_readonly(doc) === false) {
        throw new RangeError("Calls to Automerge.change cannot be nested")
    }
    const heads = state.handle.getHeads()
    state.handle.receiveSyncMessage(syncState, message)
    const outSyncState = ApiHandler.exportSyncState(syncState)
    return [progressDocument(doc, heads, opts.patchCallback || state.patchCallback), outSyncState, null];
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
    return ApiHandler.exportSyncState(ApiHandler.initSyncState())
}

/** @hidden */
export function encodeChange(change: DecodedChange): Change {
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

export function splice<T>(doc: Doc<T>, prop: Prop, index: number, del: number, newText?: string) {
    if (!Reflect.get(doc, IS_PROXY)) {
      throw new RangeError("object cannot be modified outside of a change block")
    }
    const state = _state(doc, false)
    const objectId = _obj(doc)
    if (!objectId) {
      throw new RangeError("invalid object for splice")
    }
    const value = state.handle.getWithType(objectId, prop)
    if (value === null) {
        throw new RangeError("Cannot splice, not a valid value");
    } else if (value[0] === 'text') {
        return state.handle.splice(value[1], index, del, newText)
    } else {
        throw new RangeError(`Cannot splice, value is of type '${value[0]}', must be 'text'`);
    }
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
  return getObjectId(doc) === "_root" && !!Reflect.get(doc as Object, STATE)
}

function isObject(obj: unknown): obj is Record<string, unknown> {
    return typeof obj === 'object' && obj !== null
}

export type {API, SyncState, ActorId, Conflicts, Prop, Change, ObjID, DecodedChange, DecodedSyncMessage, Heads, MaterializeValue}
