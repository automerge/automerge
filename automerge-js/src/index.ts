
import { uuid } from './uuid'

export { uuid } from './uuid'

import { rootProxy, listProxy, textProxy, mapProxy } from "./proxies"
import { STATE, HEADS, OBJECT_ID, READ_ONLY, FROZEN  } from "./constants"
import { Counter  } from "./counter"
import { Text } from "./text"
import { Int, Uint, Float64  } from "./numbers"
import { isObject } from "./common"

export { Text } from "./text"
export { Counter  } from "./counter"
export { Int, Uint, Float64  } from "./numbers"

import { ApiHandler, LowLevelApi, UseApi } from "./low_level"
import { Actor as ActorId, Prop, ObjID, Change, DecodedChange, Heads, Automerge } from "./low_level"
import { JsSyncState as SyncState, SyncMessage, DecodedSyncMessage } from "./low_level"

export type ChangeOptions<T> = { message?: string, time?: number }

export type Doc<T> = { readonly [P in keyof T]: Doc<T[P]> }

export type ChangeFn<T> = (doc: T) => void

export interface State<T> {
  change: DecodedChange
  snapshot: T
}

export function use(api: LowLevelApi) {
  UseApi(api)
}

function _state<T>(doc: Doc<T>) : Automerge {
  const state = (<any>doc)[STATE]
  if (state == undefined) {
    throw new RangeError("must be the document root")
  }
  return state
}

function _frozen<T>(doc: Doc<T>) : boolean {
  return (<any>doc)[FROZEN] === true
}

function _heads<T>(doc: Doc<T>) : Heads | undefined {
  return (<any>doc)[HEADS]
}

function _obj<T>(doc: Doc<T>) : ObjID {
  return (<any>doc)[OBJECT_ID]
}

function _readonly<T>(doc: Doc<T>) : boolean {
  return (<any>doc)[READ_ONLY] === true
}

export function init<T>(actor?: ActorId) : Doc<T>{
  if (typeof actor !== "string") {
    actor = undefined
  }
  const state = ApiHandler.create(actor)
  return rootProxy(state, true);
}

export function clone<T>(doc: Doc<T>) : Doc<T> {
  const state = _state(doc).clone()
  return rootProxy(state, true);
}

export function free<T>(doc: Doc<T>) {
  return _state(doc).free()
}

export function from<T>(initialState: T | Doc<T>, actor?: ActorId): Doc<T> {
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

function _change<T>(doc: Doc<T>, options: ChangeOptions<T>, callback: ChangeFn<T>): Doc<T> {


  if (typeof callback !== "function") {
    throw new RangeError("invalid change function");
  }

  if (doc === undefined || _state(doc) === undefined || _obj(doc) !== "_root") {
    throw new RangeError("must be the document root");
  }
  if (_frozen(doc) === true) {
    throw new RangeError("Attempting to use an outdated Automerge document")
  }
  if (!!_heads(doc) === true) {
    throw new RangeError("Attempting to change an out of date document");
  }
  if (_readonly(doc) === false) {
    throw new RangeError("Calls to Automerge.change cannot be nested")
  }
  const state = _state(doc)
  const heads = state.getHeads()
  try {
    Reflect.set(doc,HEADS,heads)
    Reflect.set(doc,FROZEN,true)
    const root : T = rootProxy(state);
    callback(root)
    if (state.pendingOps() === 0) {
      Reflect.set(doc,FROZEN,false)
      Reflect.set(doc,HEADS,undefined)
      return doc
    } else {
      state.commit(options.message, options.time)
      return rootProxy(state, true);
    }
  } catch (e) {
    //console.log("ERROR: ",e)
    Reflect.set(doc,FROZEN,false)
    Reflect.set(doc,HEADS,undefined)
    state.rollback()
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

  if (doc === undefined || _state(doc) === undefined || _obj(doc) !== "_root") {
    throw new RangeError("must be the document root");
  }
  if (_frozen(doc) === true) {
    throw new RangeError("Attempting to use an outdated Automerge document")
  }
  if (_readonly(doc) === false) {
    throw new RangeError("Calls to Automerge.change cannot be nested")
  }

  const state = _state(doc)
  state.commit(options.message, options.time)
  return rootProxy(state, true);
}

export function load<T>(data: Uint8Array, actor: ActorId) : Doc<T> {
  const state = ApiHandler.load(data, actor)
  return rootProxy(state, true);
}

export function save<T>(doc: Doc<T>) : Uint8Array  {
  const state = _state(doc)
  return state.save()
}

export function merge<T>(local: Doc<T>, remote: Doc<T>) : Doc<T> {
  if (!!_heads(local) === true) {
    throw new RangeError("Attempting to change an out of date document");
  }
  const localState = _state(local)
  const heads = localState.getHeads()
  const remoteState = _state(remote)
  const changes = localState.getChangesAdded(remoteState)
  localState.applyChanges(changes)
  Reflect.set(local,HEADS,heads)
  return rootProxy(localState, true)
}

export function getActorId<T>(doc: Doc<T>) : ActorId {
  const state = _state(doc)
  return state.getActorId()
}

function conflictAt(context : Automerge, objectId: ObjID, prop: Prop) : any {
      const values = context.getAll(objectId, prop)
      if (values.length <= 1) {
        return
      }
      const result = {}
      for (const conflict of values) {
        const datatype = conflict[0]
        const value = conflict[1]
        switch (datatype) {
          case "map":
            //@ts-ignore
            result[value] = mapProxy(context, value, [ prop ], true)
            break;
          case "list":
            //@ts-ignore
            result[value] = listProxy(context, value, [ prop ], true)
            break;
          case "text":
            //@ts-ignore
            result[value] = textProxy(context, value, [ prop ], true)
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
            //@ts-ignore
            result[conflict[2]] = value
            break;
          case "counter":
            //@ts-ignore
            result[conflict[2]] = new Counter(value)
            break;
          case "timestamp":
            //@ts-ignore
            result[conflict[2]] = new Date(<number>value)
            break;
          default:
            throw RangeError(`datatype ${datatype} unimplemented`)
        }
      }
      return result
}

export function getConflicts<T>(doc: Doc<T>, prop: Prop) : any {
  const state = _state(doc)
  const objectId = _obj(doc)
  return conflictAt(state, objectId, prop)
}

export function getLastLocalChange<T>(doc: Doc<T>) : Change | undefined {
  const state = _state(doc)
  try {
    return state.getLastLocalChange()
  } catch (e) {
    return
  }
}

export function getObjectId<T>(doc: Doc<T>) : ObjID {
  return _obj(doc)
}

export function getChanges<T>(oldState: Doc<T>, newState: Doc<T>) : Change[] {
  const o = _state(oldState)
  const n = _state(newState)
  const heads = _heads(oldState)
  return n.getChanges(heads || o.getHeads())
}

export function getAllChanges<T>(doc: Doc<T>) : Change[] {
  const state = _state(doc)
  return state.getChanges([])
}

export function applyChanges<T>(doc: Doc<T>, changes: Change[]) : [Doc<T>] {
  if (doc === undefined || _obj(doc) !== "_root") {
    throw new RangeError("must be the document root");
  }
  if (_frozen(doc) === true) {
    throw new RangeError("Attempting to use an outdated Automerge document")
  }
  if (_readonly(doc) === false) {
    throw new RangeError("Calls to Automerge.change cannot be nested")
  }
  const state = _state(doc)
  const heads = state.getHeads()
  state.applyChanges(changes)
  Reflect.set(doc,HEADS,heads)
  return [rootProxy(state, true)];
}

export function getHistory<T>(doc: Doc<T>) : State<T>[] {
  const actor = getActorId(doc)
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
export function equals(val1: any, val2: any) : boolean {
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
  const message = state.generateSyncMessage(syncState)
  const outState = ApiHandler.exportSyncState(syncState)
  return [ outState, message ]
}

export function receiveSyncMessage<T>(doc: Doc<T>, inState: SyncState, message: SyncMessage) : [ Doc<T>, SyncState, null ] {
  const syncState = ApiHandler.importSyncState(inState)
  if (doc === undefined || _obj(doc) !== "_root") {
    throw new RangeError("must be the document root");
  }
  if (_frozen(doc) === true) {
    throw new RangeError("Attempting to use an outdated Automerge document")
  }
  if (!!_heads(doc) === true) {
    throw new RangeError("Attempting to change an out of date document");
  }
  if (_readonly(doc) === false) {
    throw new RangeError("Calls to Automerge.change cannot be nested")
  }
  const state = _state(doc)
  const heads = state.getHeads()
  state.receiveSyncMessage(syncState, message)
  Reflect.set(doc,HEADS,heads)
  const outState = ApiHandler.exportSyncState(syncState)
  return [rootProxy(state, true), outState, null];
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
  return state.getMissingDeps(heads)
}

export function getHeads<T>(doc: Doc<T>) : Heads {
  const state = _state(doc)
  return _heads(doc) || state.getHeads()
}

export function dump<T>(doc: Doc<T>) {
  const state = _state(doc)
  state.dump()
}

export function toJS(doc: any) : any {
  if (typeof doc === "object") {
    if (doc instanceof Uint8Array) {
      return doc
    }
    if (doc === null) {
      return doc
    }
    if (doc instanceof Array) {
      return doc.map((a) => toJS(a))
    }
    if (doc instanceof Text) {
      //@ts-ignore
      return doc.map((a: any) => toJS(a))
    }
    const tmp : any = {}
    for (const index in doc) {
      tmp[index] = toJS(doc[index])
    }
    return tmp
  } else {
    return doc
  }
}

