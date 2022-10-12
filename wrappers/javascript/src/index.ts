
export { uuid } from './uuid'

import { rootProxy, listProxy, textProxy, mapProxy } from "./proxies"
import { STATE, HEADS, TRACE, OBJECT_ID, READ_ONLY, FROZEN  } from "./constants"

import { AutomergeValue, Text, Counter } from "./types"
export { AutomergeValue, Text, Counter, Int, Uint, Float64 } from "./types"

import { type API, type Patch } from "@automerge/automerge-wasm";
import { ApiHandler, UseApi } from "./low_level"

import { Actor as ActorId, Prop, ObjID, Change, DecodedChange, Heads, Automerge, MaterializeValue } from "@automerge/automerge-wasm"
import { JsSyncState as SyncState, SyncMessage, DecodedSyncMessage } from "@automerge/automerge-wasm"

export type ChangeOptions<T> = { message?: string, time?: number, patchCallback?: PatchCallback<T> }
export type ApplyOptions<T> = { patchCallback?: PatchCallback<T> }

export type Doc<T> = { readonly [P in keyof T]: T[P] }

export type ChangeFn<T> = (doc: T) => void

export type PatchCallback<T> = (patch: Patch, before: Doc<T>, after: Doc<T>) => void

export interface State<T> {
  change: DecodedChange
  snapshot: T
}

export function use(api: API) {
  UseApi(api)
}

import * as wasm from "@automerge/automerge-wasm"
use(wasm)

export type InitOptions<T> = {
    actor?: ActorId,
    freeze?: boolean,
    patchCallback?: PatchCallback<T>,
};


interface InternalState<T> {
  handle: Automerge,
  heads: Heads | undefined,
  freeze: boolean,
  patchCallback?: PatchCallback<T>
}

export function getBackend<T>(doc: Doc<T>) : Automerge {
  return _state(doc).handle
}

function _state<T>(doc: Doc<T>, checkroot = true) : InternalState<T> {
  const state = Reflect.get(doc,STATE)
  if (state === undefined || (checkroot && _obj(doc) !== "_root")) {
    throw new RangeError("must be the document root")
  }
  return state
}

function _frozen<T>(doc: Doc<T>) : boolean {
  return Reflect.get(doc,FROZEN) === true
}

function _trace<T>(doc: Doc<T>) : string | undefined {
  return Reflect.get(doc,TRACE)
}

function _set_heads<T>(doc: Doc<T>, heads: Heads) {
  _state(doc).heads = heads
}

function _clear_heads<T>(doc: Doc<T>) {
  Reflect.set(doc,HEADS,undefined)
  Reflect.set(doc,TRACE,undefined)
}

function _obj<T>(doc: Doc<T>) : ObjID {
  let proxy_objid = Reflect.get(doc,OBJECT_ID)
  if (proxy_objid) {
    return proxy_objid
  }
  if (Reflect.get(doc,STATE)) {
    return "_root"
  }
  throw new RangeError("invalid document passed to _obj()")
}

function _readonly<T>(doc: Doc<T>) : boolean {
  return Reflect.get(doc,READ_ONLY) !== false
}

function importOpts<T>(_actor?: ActorId | InitOptions<T>) : InitOptions<T> {
  if (typeof _actor === 'object') {
    return _actor
  } else {
    return { actor: _actor }
  }
}

export function init<T>(_opts?: ActorId | InitOptions<T>) : Doc<T>{
  let opts = importOpts(_opts)
  let freeze = !!opts.freeze
  let patchCallback = opts.patchCallback
  const handle = ApiHandler.create(opts.actor)
  handle.enablePatches(true)
  //@ts-ignore
  handle.registerDatatype("counter", (n) => new Counter(n))
  //@ts-ignore
  handle.registerDatatype("text", (n) => new Text(n))
  //@ts-ignore
  const doc = handle.materialize("/", undefined, { handle, heads: undefined, freeze, patchCallback })
  //@ts-ignore
  return doc
}

export function clone<T>(doc: Doc<T>) : Doc<T> {
  const state = _state(doc)
  const handle = state.heads ? state.handle.forkAt(state.heads) : state.handle.fork()
  //@ts-ignore
  const clonedDoc : any = handle.materialize("/", undefined, { ... state, handle })

  return clonedDoc
}

export function free<T>(doc: Doc<T>) {
  return _state(doc).handle.free()
}

export function from<T extends Record<string, unknown>>(initialState: T | Doc<T>, actor?: ActorId): Doc<T> {
    return change(init(actor), (d) => Object.assign(d, initialState))
}

export function change<T>(doc: Doc<T>, options: string | ChangeOptions<T> | ChangeFn<T>, callback?: ChangeFn<T>): Doc<T> {
  if (typeof options === 'function') {
    return _change(doc, {}, options)
  } else if (typeof callback === 'function') {
    if (typeof options === "string") {
      options = { message: options }
    }
    return _change(doc, options, callback)
  } else {
    throw RangeError("Invalid args for change")
  }
}

function progressDocument<T>(doc: Doc<T>, heads: Heads, callback?: PatchCallback<T>): Doc<T> {
  let state = _state(doc)
  let nextState = { ... state, heads: undefined };
  // @ts-ignore
  let nextDoc = state.handle.applyPatches(doc, nextState, callback)
  state.heads = heads
  if (nextState.freeze) { Object.freeze(nextDoc) }
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
    throw new RangeError("Attempting to use an outdated Automerge document")
  }
  if (_readonly(doc) === false) {
    throw new RangeError("Calls to Automerge.change cannot be nested")
  }
  const heads = state.handle.getHeads()
  try {
    state.heads = heads
    const root : T = rootProxy(state.handle);
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

export function emptyChange<T>(doc: Doc<T>, options: ChangeOptions<T>) {
  if (options === undefined) {
    options = {}
  }
  if (typeof options === "string") {
    options = { message: options }
  }

  const state = _state(doc)

  if (state.heads) {
    throw new RangeError("Attempting to use an outdated Automerge document")
  }
  if (_readonly(doc) === false) {
    throw new RangeError("Calls to Automerge.change cannot be nested")
  }

  const heads = state.handle.getHeads()
  state.handle.commit(options.message, options.time)
  return progressDocument(doc, heads)
}

export function load<T>(data: Uint8Array, _opts?: ActorId | InitOptions<T>) : Doc<T> {
  const opts = importOpts(_opts)
  const actor = opts.actor
  const patchCallback = opts.patchCallback
  const handle = ApiHandler.load(data, actor)
  handle.enablePatches(true)
  //@ts-ignore
  handle.registerDatatype("counter", (n) => new Counter(n))
  //@ts-ignore
  handle.registerDatatype("text", (n) => new Text(n))
  //@ts-ignore
  const doc : any = handle.materialize("/", undefined, { handle, heads: undefined, patchCallback })
  return doc
}

export function save<T>(doc: Doc<T>) : Uint8Array  {
  return _state(doc).handle.save()
}

export function merge<T>(local: Doc<T>, remote: Doc<T>) : Doc<T> {
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

export function getActorId<T>(doc: Doc<T>) : ActorId {
  const state = _state(doc)
  return state.handle.getActorId()
}

type Conflicts = { [key: string]: AutomergeValue }

function conflictAt(context : Automerge, objectId: ObjID, prop: Prop) : Conflicts | undefined {
      const values = context.getAll(objectId, prop)
      if (values.length <= 1) {
        return
      }
      const result : Conflicts = {}
      for (const fullVal of values) {
        switch (fullVal[0]) {
          case "map":
            result[fullVal[1]] = mapProxy(context, fullVal[1], [ prop ], true)
            break;
          case "list":
            result[fullVal[1]] = listProxy(context, fullVal[1], [ prop ], true)
            break;
          case "text":
            result[fullVal[1]] = textProxy(context, fullVal[1], [ prop ], true)
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

export function getConflicts<T>(doc: Doc<T>, prop: Prop) : Conflicts | undefined {
  const state = _state(doc, false)
  const objectId = _obj(doc)
  return conflictAt(state.handle, objectId, prop)
}

export function getLastLocalChange<T>(doc: Doc<T>) : Change | undefined {
  const state = _state(doc)
  return state.handle.getLastLocalChange() || undefined
}

export function getObjectId<T>(doc: Doc<T>) : ObjID {
  return _obj(doc)
}

export function getChanges<T>(oldState: Doc<T>, newState: Doc<T>) : Change[] {
  const o = _state(oldState)
  const n = _state(newState)
  return n.handle.getChanges(getHeads(oldState))
}

export function getAllChanges<T>(doc: Doc<T>) : Change[] {
  const state = _state(doc)
  return state.handle.getChanges([])
}

export function applyChanges<T>(doc: Doc<T>, changes: Change[], opts?: ApplyOptions<T>) : [Doc<T>] {
  const state = _state(doc)
  if (!opts) { opts = {} }
  if (state.heads) {
    throw new RangeError("Attempting to use an outdated Automerge document")
  }
  if (_readonly(doc) === false) {
    throw new RangeError("Calls to Automerge.change cannot be nested")
  }
  const heads = state.handle.getHeads();
  state.handle.applyChanges(changes)
  state.heads = heads;
  return [progressDocument(doc, heads, opts.patchCallback || state.patchCallback )]
}

export function getHistory<T>(doc: Doc<T>) : State<T>[] {
  const history = getAllChanges(doc)
  return history.map((change, index) => ({
      get change () {
        return decodeChange(change)
      },
      get snapshot () {
        const [state] = applyChanges(init(), history.slice(0, index + 1))
        return <T>state
      }
    })
  )
}

// FIXME : no tests
// FIXME can we just use deep equals now?
export function equals(val1: unknown, val2: unknown) : boolean {
  if (!isObject(val1) || !isObject(val2)) return val1 === val2
  const keys1 = Object.keys(val1).sort(), keys2 = Object.keys(val2).sort()
  if (keys1.length !== keys2.length) return false
  for (let i = 0; i < keys1.length; i++) {
    if (keys1[i] !== keys2[i]) return false
    if (!equals(val1[keys1[i]], val2[keys2[i]])) return false
  }
  return true
}

export function encodeSyncState(state: SyncState) : Uint8Array {
  return ApiHandler.encodeSyncState(ApiHandler.importSyncState(state))
}

export function decodeSyncState(state: Uint8Array) : SyncState {
  return ApiHandler.exportSyncState(ApiHandler.decodeSyncState(state))
}

export function generateSyncMessage<T>(doc: Doc<T>, inState: SyncState) : [ SyncState, SyncMessage | null ] {
  const state = _state(doc)
  const syncState = ApiHandler.importSyncState(inState)
  const message = state.handle.generateSyncMessage(syncState)
  const outState = ApiHandler.exportSyncState(syncState)
  return [ outState, message ]
}

export function receiveSyncMessage<T>(doc: Doc<T>, inState: SyncState, message: SyncMessage, opts?: ApplyOptions<T>) : [ Doc<T>, SyncState, null ] {
  const syncState = ApiHandler.importSyncState(inState)
  if (!opts) { opts = {} }
  const state = _state(doc)
  if (state.heads) {
    throw new RangeError("Attempting to change an out of date document - set at: " + _trace(doc));
  }
  if (_readonly(doc) === false) {
    throw new RangeError("Calls to Automerge.change cannot be nested")
  }
  const heads = state.handle.getHeads()
  state.handle.receiveSyncMessage(syncState, message)
  const outSyncState = ApiHandler.exportSyncState(syncState)
  return [progressDocument(doc, heads, opts.patchCallback || state.patchCallback), outSyncState, null];
}

export function initSyncState() : SyncState {
  return ApiHandler.exportSyncState(ApiHandler.initSyncState())
}

export function encodeChange(change: DecodedChange) : Change {
  return ApiHandler.encodeChange(change)
}

export function decodeChange(data: Change) : DecodedChange {
  return ApiHandler.decodeChange(data)
}

export function encodeSyncMessage(message: DecodedSyncMessage) : SyncMessage {
  return ApiHandler.encodeSyncMessage(message)
}

export function decodeSyncMessage(message: SyncMessage) : DecodedSyncMessage {
  return ApiHandler.decodeSyncMessage(message)
}

export function getMissingDeps<T>(doc: Doc<T>, heads: Heads) : Heads {
  const state = _state(doc)
  return state.handle.getMissingDeps(heads)
}

export function getHeads<T>(doc: Doc<T>) : Heads {
  const state = _state(doc)
  return state.heads || state.handle.getHeads()
}

export function dump<T>(doc: Doc<T>) {
  const state = _state(doc)
  state.handle.dump()
}

// FIXME - return T?
export function toJS<T>(doc: Doc<T>) : MaterializeValue {
  const state = _state(doc)
  // @ts-ignore
  return state.handle.materialize("_root", state.heads, state)
}


function isObject(obj: unknown) : obj is Record<string,unknown> {
  return typeof obj === 'object' && obj !== null
}

export type { API, SyncState, ActorId, Conflicts, Prop, Change, ObjID, DecodedChange, DecodedSyncMessage, Heads, MaterializeValue }
