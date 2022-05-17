import * as AutomergeWASM from "automerge-wasm"

import { uuid } from './uuid'

import _init from "automerge-wasm"

export default _init

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

import { Actor as ActorId, Prop, ObjID, Change, DecodedChange, Heads, Automerge } from "automerge-wasm"
import { JsSyncState as SyncState, SyncMessage, DecodedSyncMessage } from "automerge-wasm"

function _state<T>(doc: Doc<T>) : Automerge {
  let state = (<any>doc)[STATE]
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
  const state = AutomergeWASM.create(actor)
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

export function change<D, T = Proxy<D>>(doc: D, options: ChangeOptions<T> | ChangeFn<T>, callback?: ChangeFn<T>): D {

  if (typeof options === 'function') {
    callback = options
    options = {}
  }

  if (typeof options === "string") {
    options = { message: options }
  }

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
    //@ts-ignore
    doc[HEADS] = heads
    //Object.defineProperty(doc, HEADS, { value: heads, configurable: true, writable: true })
    //@ts-ignore
    doc[FROZEN] = true
    let root = rootProxy(state);
    callback(root)
    if (state.pendingOps() === 0) {
      //@ts-ignore
      doc[FROZEN] = false
      //@ts-ignore
      doc[HEADS] = undefined
      return doc
    } else {
      state.commit(options.message, options.time)
      return rootProxy(state, true);
    }
  } catch (e) {
    //console.log("ERROR: ",e)
    //@ts-ignore
    doc[FROZEN] = false
    //@ts-ignore
    doc[HEADS] = undefined
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
  const state = AutomergeWASM.load(data, actor)
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
  //@ts-ignore
  local[HEADS] = heads
  return rootProxy(localState, true)
}

export function getActorId<T>(doc: Doc<T>) : ActorId {
  const state = _state(doc)
  return state.getActorId()
}

function conflictAt(context : Automerge, objectId: ObjID, prop: Prop) : any {
      let values = context.getAll(objectId, prop)
      if (values.length <= 1) {
        return
      }
      let result = {}
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
  //@ts-ignore
  doc[HEADS] = heads
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
  return AutomergeWASM.encodeSyncState(AutomergeWASM.importSyncState(state))
}

export function decodeSyncState(state: Uint8Array) : SyncState {
  return AutomergeWASM.exportSyncState(AutomergeWASM.decodeSyncState(state))
}

export function generateSyncMessage<T>(doc: Doc<T>, inState: SyncState) : [ SyncState, SyncMessage | null ] {
  const state = _state(doc)
  const syncState = AutomergeWASM.importSyncState(inState)
  const message = state.generateSyncMessage(syncState)
  const outState = AutomergeWASM.exportSyncState(syncState)
  return [ outState, message ]
}

export function receiveSyncMessage<T>(doc: Doc<T>, inState: SyncState, message: SyncMessage) : [ Doc<T>, SyncState, null ] {
  const syncState = AutomergeWASM.importSyncState(inState)
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
  //@ts-ignore
  doc[HEADS] = heads;
  const outState = AutomergeWASM.exportSyncState(syncState)
  return [rootProxy(state, true), outState, null];
}

export function initSyncState() : SyncState {
  return AutomergeWASM.exportSyncState(AutomergeWASM.initSyncState())
}

export function encodeChange(change: DecodedChange) : Change {
  return AutomergeWASM.encodeChange(change)
}

export function decodeChange(data: Change) : DecodedChange {
  return AutomergeWASM.decodeChange(data)
}

export function encodeSyncMessage(message: DecodedSyncMessage) : SyncMessage {
  return AutomergeWASM.encodeSyncMessage(message)
}

export function decodeSyncMessage(message: SyncMessage) : DecodedSyncMessage {
  return AutomergeWASM.decodeSyncMessage(message)
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
    let tmp : any = {}
    for (let index in doc) {
      tmp[index] = toJS(doc[index])
    }
    return tmp
  } else {
    return doc
  }
}

type ChangeOptions<T> =
    | string // = message
    | {
      message?: string
      time?: number
    }

type Doc<T> = FreezeObject<T>

/**
 * The argument pased to the callback of a `change` function is a mutable proxy of the original
 * type. `Proxy<D>` is the inverse of `Doc<T>`: `Proxy<Doc<T>>` is `T`, and `Doc<Proxy<D>>` is `D`.
 */
type Proxy<D> = D extends Doc<infer T> ? T : never

type ChangeFn<T> = (doc: T) => void

interface State<T> {
  change: DecodedChange
  snapshot: T
}

// custom CRDT types

/*
  class TableRow {
    readonly id: UUID
  }

  class Table<T> {
    constructor()
    add(item: T): UUID
    byId(id: UUID): T & TableRow
    count: number
    ids: UUID[]
    remove(id: UUID): void
    rows: (T & TableRow)[]
  }
*/

  class List<T> extends Array<T> {
    insertAt?(index: number, ...args: T[]): List<T>
    deleteAt?(index: number, numDelete?: number): List<T>
  }

/*

  class Text extends List<string> {
    constructor(text?: string | string[])
    get(index: number): string
    toSpans<T>(): (string | T)[]
  }

  // Note that until https://github.com/Microsoft/TypeScript/issues/2361 is addressed, we
  // can't treat a Counter like a literal number without force-casting it as a number.
  // This won't compile:
  //   `assert.strictEqual(c + 10, 13) // Operator '+' cannot be applied to types 'Counter' and '10'.ts(2365)`
  // But this will:
  //   `assert.strictEqual(c as unknown as number + 10, 13)`
  class Counter extends Number {
    constructor(value?: number)
    increment(delta?: number): void
    decrement(delta?: number): void
    toString(): string
    valueOf(): number
    value: number
  }

  class Int { constructor(value: number) }
  class Uint { constructor(value: number) }
  class Float64 { constructor(value: number) }

*/

  // Readonly variants

  //type ReadonlyTable<T> = ReadonlyArray<T> & Table<T>
  type ReadonlyList<T> = ReadonlyArray<T> & List<T>
  type ReadonlyText = ReadonlyList<string> & Text

// prettier-ignore
type Freeze<T> =
  T extends Function ? T
  : T extends Text ? ReadonlyText
//  : T extends Table<infer T> ? FreezeTable<T>
  : T extends List<infer T> ? FreezeList<T>
  : T extends Array<infer T> ? FreezeArray<T>
  : T extends Map<infer K, infer V> ? FreezeMap<K, V>
  : T extends string & infer O ? string & O
  : FreezeObject<T>

//interface FreezeTable<T> extends ReadonlyTable<Freeze<T>> {}
interface FreezeList<T> extends ReadonlyList<Freeze<T>> {}
interface FreezeArray<T> extends ReadonlyArray<Freeze<T>> {}
interface FreezeMap<K, V> extends ReadonlyMap<Freeze<K>, Freeze<V>> {}
type FreezeObject<T> = { readonly [P in keyof T]: Freeze<T[P]> }
