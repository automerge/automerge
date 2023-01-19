// @ts-nocheck 
import { Text } from "./text.ts"
import {
  Automerge,
  type Heads,
  type ObjID,
  type Prop,
} from "https://deno.land/x/automerge_wasm@0.1.22/automerge_wasm.js";

import type {
  AutomergeValue,
  ScalarValue,
  MapValue,
  ListValue,
  TextValue,
} from "./types.ts"
import { Counter, getWriteableCounter } from "./counter.ts"
import {
  STATE,
  TRACE,
  IS_PROXY,
  OBJECT_ID,
  COUNTER,
  INT,
  UINT,
  F64,
} from "./constants.ts"
import { RawString } from "./raw_string.ts"

type Target = {
  context: Automerge
  objectId: ObjID
  path: Array<Prop>
  readonly: boolean
  heads?: Array<string>
  cache: {}
  trace?: any
  frozen: boolean
  textV2: boolean
}

function parseListIndex(key) {
  if (typeof key === "string" && /^[0-9]+$/.test(key)) key = parseInt(key, 10)
  if (typeof key !== "number") {
    return key
  }
  if (key < 0 || isNaN(key) || key === Infinity || key === -Infinity) {
    throw new RangeError("A list index must be positive, but you passed " + key)
  }
  return key
}

function valueAt(target: Target, prop: Prop): AutomergeValue | undefined {
  const { context, objectId, path, readonly, heads, textV2 } = target
  const value = context.getWithType(objectId, prop, heads)
  if (value === null) {
    return
  }
  const datatype = value[0]
  const val = value[1]
  switch (datatype) {
    case undefined:
      return
    case "map":
      return mapProxy(
        context,
        val as ObjID,
        textV2,
        [...path, prop],
        readonly,
        heads
      )
    case "list":
      return listProxy(
        context,
        val as ObjID,
        textV2,
        [...path, prop],
        readonly,
        heads
      )
    case "text":
      if (textV2) {
        return context.text(val as ObjID, heads)
      } else {
        return textProxy(
          context,
          val as ObjID,
          [...path, prop],
          readonly,
          heads
        )
      }
    case "str":
      return val
    case "uint":
      return val
    case "int":
      return val
    case "f64":
      return val
    case "boolean":
      return val
    case "null":
      return null
    case "bytes":
      return val
    case "timestamp":
      return val
    case "counter": {
      if (readonly) {
        return new Counter(val as number)
      } else {
        return getWriteableCounter(val as number, context, path, objectId, prop)
      }
    }
    default:
      throw RangeError(`datatype ${datatype} unimplemented`)
  }
}

function import_value(value: any, textV2: boolean) {
  switch (typeof value) {
    case "object":
      if (value == null) {
        return [null, "null"]
      } else if (value[UINT]) {
        return [value.value, "uint"]
      } else if (value[INT]) {
        return [value.value, "int"]
      } else if (value[F64]) {
        return [value.value, "f64"]
      } else if (value[COUNTER]) {
        return [value.value, "counter"]
      } else if (value instanceof Date) {
        return [value.getTime(), "timestamp"]
      } else if (value instanceof RawString) {
        return [value.val, "str"]
      } else if (value instanceof Text) {
        return [value, "text"]
      } else if (value instanceof Uint8Array) {
        return [value, "bytes"]
      } else if (value instanceof Array) {
        return [value, "list"]
      } else if (Object.getPrototypeOf(value) === Object.getPrototypeOf({})) {
        return [value, "map"]
      } else if (value[OBJECT_ID]) {
        throw new RangeError(
          "Cannot create a reference to an existing document object"
        )
      } else {
        throw new RangeError(`Cannot assign unknown object: ${value}`)
      }
    case "boolean":
      return [value, "boolean"]
    case "number":
      if (Number.isInteger(value)) {
        return [value, "int"]
      } else {
        return [value, "f64"]
      }
    case "string":
      if (textV2) {
        return [value, "text"]
      } else {
        return [value, "str"]
      }
    default:
      throw new RangeError(`Unsupported type of value: ${typeof value}`)
  }
}

const MapHandler = {
  get(target: Target, key): AutomergeValue | { handle: Automerge } {
    const { context, objectId, cache } = target
    if (key === Symbol.toStringTag) {
      return target[Symbol.toStringTag]
    }
    if (key === OBJECT_ID) return objectId
    if (key === IS_PROXY) return true
    if (key === TRACE) return target.trace
    if (key === STATE) return { handle: context }
    if (!cache[key]) {
      cache[key] = valueAt(target, key)
    }
    return cache[key]
  },

  set(target: Target, key, val) {
    const { context, objectId, path, readonly, frozen, textV2 } = target
    target.cache = {} // reset cache on set
    if (val && val[OBJECT_ID]) {
      throw new RangeError(
        "Cannot create a reference to an existing document object"
      )
    }
    if (key === TRACE) {
      target.trace = val
      return true
    }
    const [value, datatype] = import_value(val, textV2)
    if (frozen) {
      throw new RangeError("Attempting to use an outdated Automerge document")
    }
    if (readonly) {
      throw new RangeError(`Object property "${key}" cannot be modified`)
    }
    switch (datatype) {
      case "list": {
        const list = context.putObject(objectId, key, [])
        const proxyList = listProxy(
          context,
          list,
          textV2,
          [...path, key],
          readonly
        )
        for (let i = 0; i < value.length; i++) {
          proxyList[i] = value[i]
        }
        break
      }
      case "text": {
        if (textV2) {
          context.putObject(objectId, key, value)
        } else {
          const text = context.putObject(objectId, key, "")
          const proxyText = textProxy(context, text, [...path, key], readonly)
          for (let i = 0; i < value.length; i++) {
            proxyText[i] = value.get(i)
          }
        }
        break
      }
      case "map": {
        const map = context.putObject(objectId, key, {})
        const proxyMap = mapProxy(
          context,
          map,
          textV2,
          [...path, key],
          readonly
        )
        for (const key in value) {
          proxyMap[key] = value[key]
        }
        break
      }
      default:
        context.put(objectId, key, value, datatype)
    }
    return true
  },

  deleteProperty(target: Target, key) {
    const { context, objectId, readonly } = target
    target.cache = {} // reset cache on delete
    if (readonly) {
      throw new RangeError(`Object property "${key}" cannot be modified`)
    }
    context.delete(objectId, key)
    return true
  },

  has(target: Target, key) {
    const value = this.get(target, key)
    return value !== undefined
  },

  getOwnPropertyDescriptor(target: Target, key) {
    // const { context, objectId } = target
    const value = this.get(target, key)
    if (typeof value !== "undefined") {
      return {
        configurable: true,
        enumerable: true,
        value,
      }
    }
  },

  ownKeys(target: Target) {
    const { context, objectId, heads } = target
    // FIXME - this is a tmp workaround until fix the dupe key bug in keys()
    const keys = context.keys(objectId, heads)
    return [...new Set<string>(keys)]
  },
}

const ListHandler = {
  get(target: Target, index) {
    const { context, objectId, heads } = target
    index = parseListIndex(index)
    if (index === Symbol.hasInstance) {
      return instance => {
        return Array.isArray(instance)
      }
    }
    if (index === Symbol.toStringTag) {
      return target[Symbol.toStringTag]
    }
    if (index === OBJECT_ID) return objectId
    if (index === IS_PROXY) return true
    if (index === TRACE) return target.trace
    if (index === STATE) return { handle: context }
    if (index === "length") return context.length(objectId, heads)
    if (typeof index === "number") {
      return valueAt(target, index)
    } else {
      return listMethods(target)[index]
    }
  },

  set(target: Target, index, val) {
    const { context, objectId, path, readonly, frozen, textV2 } = target
    index = parseListIndex(index)
    if (val && val[OBJECT_ID]) {
      throw new RangeError(
        "Cannot create a reference to an existing document object"
      )
    }
    if (index === TRACE) {
      target.trace = val
      return true
    }
    if (typeof index == "string") {
      throw new RangeError("list index must be a number")
    }
    const [value, datatype] = import_value(val, textV2)
    if (frozen) {
      throw new RangeError("Attempting to use an outdated Automerge document")
    }
    if (readonly) {
      throw new RangeError(`Object property "${index}" cannot be modified`)
    }
    switch (datatype) {
      case "list": {
        let list
        if (index >= context.length(objectId)) {
          list = context.insertObject(objectId, index, [])
        } else {
          list = context.putObject(objectId, index, [])
        }
        const proxyList = listProxy(
          context,
          list,
          textV2,
          [...path, index],
          readonly
        )
        proxyList.splice(0, 0, ...value)
        break
      }
      case "text": {
        if (textV2) {
          if (index >= context.length(objectId)) {
            context.insertObject(objectId, index, value)
          } else {
            context.putObject(objectId, index, value)
          }
        } else {
          let text
          if (index >= context.length(objectId)) {
            text = context.insertObject(objectId, index, "")
          } else {
            text = context.putObject(objectId, index, "")
          }
          const proxyText = textProxy(context, text, [...path, index], readonly)
          proxyText.splice(0, 0, ...value)
        }
        break
      }
      case "map": {
        let map
        if (index >= context.length(objectId)) {
          map = context.insertObject(objectId, index, {})
        } else {
          map = context.putObject(objectId, index, {})
        }
        const proxyMap = mapProxy(
          context,
          map,
          textV2,
          [...path, index],
          readonly
        )
        for (const key in value) {
          proxyMap[key] = value[key]
        }
        break
      }
      default:
        if (index >= context.length(objectId)) {
          context.insert(objectId, index, value, datatype)
        } else {
          context.put(objectId, index, value, datatype)
        }
    }
    return true
  },

  deleteProperty(target: Target, index) {
    const { context, objectId } = target
    index = parseListIndex(index)
    const elem = context.get(objectId, index)
    if (elem != null && elem[0] == "counter") {
      throw new TypeError(
        "Unsupported operation: deleting a counter from a list"
      )
    }
    context.delete(objectId, index)
    return true
  },

  has(target: Target, index) {
    const { context, objectId, heads } = target
    index = parseListIndex(index)
    if (typeof index === "number") {
      return index < context.length(objectId, heads)
    }
    return index === "length"
  },

  getOwnPropertyDescriptor(target: Target, index) {
    const { context, objectId, heads } = target

    if (index === "length")
      return { writable: true, value: context.length(objectId, heads) }
    if (index === OBJECT_ID)
      return { configurable: false, enumerable: false, value: objectId }

    index = parseListIndex(index)

    const value = valueAt(target, index)
    return { configurable: true, enumerable: true, value }
  },

  getPrototypeOf(target) {
    return Object.getPrototypeOf(target)
  },
  ownKeys(/*target*/): string[] {
    const keys: string[] = []
    // uncommenting this causes assert.deepEqual() to fail when comparing to a pojo array
    // but not uncommenting it causes for (i in list) {} to not enumerate values properly
    //const {context, objectId, heads } = target
    //for (let i = 0; i < target.context.length(objectId, heads); i++) { keys.push(i.toString()) }
    keys.push("length")
    return keys
  },
}

const TextHandler = Object.assign({}, ListHandler, {
  get(target: Target, index: any) {
    const { context, objectId, heads } = target
    index = parseListIndex(index)
    if (index === Symbol.hasInstance) {
      return (instance: any) => {
        return Array.isArray(instance)
      }
    }
    if (index === Symbol.toStringTag) {
      return target[Symbol.toStringTag]
    }
    if (index === OBJECT_ID) return objectId
    if (index === IS_PROXY) return true
    if (index === TRACE) return target.trace
    if (index === STATE) return { handle: context }
    if (index === "length") return context.length(objectId, heads)
    if (typeof index === "number") {
      return valueAt(target, index)
    } else {
      return textMethods(target)[index] || listMethods(target)[index]
    }
  },
  getPrototypeOf(/*target*/) {
    return Object.getPrototypeOf(new Text())
  },
})

export function mapProxy(
  context: Automerge,
  objectId: ObjID,
  textV2: boolean,
  path?: Prop[],
  readonly?: boolean,
  heads?: Heads
): MapValue {
  const target: Target = {
    context,
    objectId,
    path: path || [],
    readonly: !!readonly,
    frozen: false,
    heads,
    cache: {},
    textV2,
  }
  const proxied = {}
  Object.assign(proxied, target)
  let result = new Proxy(proxied, MapHandler)
  // conversion through unknown is necessary because the types are so different
  return result as unknown as MapValue
}

export function listProxy(
  context: Automerge,
  objectId: ObjID,
  textV2: boolean,
  path?: Prop[],
  readonly?: boolean,
  heads?: Heads
): ListValue {
  const target: Target = {
    context,
    objectId,
    path: path || [],
    readonly: !!readonly,
    frozen: false,
    heads,
    cache: {},
    textV2,
  }
  const proxied = []
  Object.assign(proxied, target)
  // @ts-ignore
  return new Proxy(proxied, ListHandler) as unknown as ListValue
}

export function textProxy(
  context: Automerge,
  objectId: ObjID,
  path?: Prop[],
  readonly?: boolean,
  heads?: Heads
): TextValue {
  const target: Target = {
    context,
    objectId,
    path: path || [],
    readonly: !!readonly,
    frozen: false,
    heads,
    cache: {},
    textV2: false,
  }
  return new Proxy(target, TextHandler) as unknown as TextValue
}

export function rootProxy<T>(
  context: Automerge,
  textV2: boolean,
  readonly?: boolean
): T {
  /* eslint-disable-next-line */
  return <any>mapProxy(context, "_root", textV2, [], !!readonly)
}

function listMethods(target: Target) {
  const { context, objectId, path, readonly, frozen, heads, textV2 } = target
  const methods = {
    deleteAt(index, numDelete) {
      if (typeof numDelete === "number") {
        context.splice(objectId, index, numDelete)
      } else {
        context.delete(objectId, index)
      }
      return this
    },

    fill(val: ScalarValue, start: number, end: number) {
      const [value, datatype] = import_value(val, textV2)
      const length = context.length(objectId)
      start = parseListIndex(start || 0)
      end = parseListIndex(end || length)
      for (let i = start; i < Math.min(end, length); i++) {
        if (datatype === "text" || datatype === "list" || datatype === "map") {
          context.putObject(objectId, i, value)
        } else {
          context.put(objectId, i, value, datatype)
        }
      }
      return this
    },

    indexOf(o, start = 0) {
      const length = context.length(objectId)
      for (let i = start; i < length; i++) {
        const value = context.getWithType(objectId, i, heads)
        if (value && (value[1] === o[OBJECT_ID] || value[1] === o)) {
          return i
        }
      }
      return -1
    },

    insertAt(index, ...values) {
      this.splice(index, 0, ...values)
      return this
    },

    pop() {
      const length = context.length(objectId)
      if (length == 0) {
        return undefined
      }
      const last = valueAt(target, length - 1)
      context.delete(objectId, length - 1)
      return last
    },

    push(...values) {
      const len = context.length(objectId)
      this.splice(len, 0, ...values)
      return context.length(objectId)
    },

    shift() {
      if (context.length(objectId) == 0) return
      const first = valueAt(target, 0)
      context.delete(objectId, 0)
      return first
    },

    splice(index, del, ...vals) {
      index = parseListIndex(index)
      del = parseListIndex(del)
      for (const val of vals) {
        if (val && val[OBJECT_ID]) {
          throw new RangeError(
            "Cannot create a reference to an existing document object"
          )
        }
      }
      if (frozen) {
        throw new RangeError("Attempting to use an outdated Automerge document")
      }
      if (readonly) {
        throw new RangeError(
          "Sequence object cannot be modified outside of a change block"
        )
      }
      const result: AutomergeValue[] = []
      for (let i = 0; i < del; i++) {
        const value = valueAt(target, index)
        if (value !== undefined) {
          result.push(value)
        }
        context.delete(objectId, index)
      }
      const values = vals.map(val => import_value(val, textV2))
      for (const [value, datatype] of values) {
        switch (datatype) {
          case "list": {
            const list = context.insertObject(objectId, index, [])
            const proxyList = listProxy(
              context,
              list,
              textV2,
              [...path, index],
              readonly
            )
            proxyList.splice(0, 0, ...value)
            break
          }
          case "text": {
            if (textV2) {
              context.insertObject(objectId, index, value)
            } else {
              const text = context.insertObject(objectId, index, "")
              const proxyText = textProxy(
                context,
                text,
                [...path, index],
                readonly
              )
              proxyText.splice(0, 0, ...value)
            }
            break
          }
          case "map": {
            const map = context.insertObject(objectId, index, {})
            const proxyMap = mapProxy(
              context,
              map,
              textV2,
              [...path, index],
              readonly
            )
            for (const key in value) {
              proxyMap[key] = value[key]
            }
            break
          }
          default:
            context.insert(objectId, index, value, datatype)
        }
        index += 1
      }
      return result
    },

    unshift(...values) {
      this.splice(0, 0, ...values)
      return context.length(objectId)
    },

    entries() {
      const i = 0
      const iterator = {
        next: () => {
          const value = valueAt(target, i)
          if (value === undefined) {
            return { value: undefined, done: true }
          } else {
            return { value: [i, value], done: false }
          }
        },
      }
      return iterator
    },

    keys() {
      let i = 0
      const len = context.length(objectId, heads)
      const iterator = {
        next: () => {
          let value: undefined | number = undefined
          if (i < len) {
            value = i
            i++
          }
          return { value, done: true }
        },
      }
      return iterator
    },

    values() {
      const i = 0
      const iterator = {
        next: () => {
          const value = valueAt(target, i)
          if (value === undefined) {
            return { value: undefined, done: true }
          } else {
            return { value, done: false }
          }
        },
      }
      return iterator
    },

    toArray(): AutomergeValue[] {
      const list: AutomergeValue = []
      let value
      do {
        value = valueAt(target, list.length)
        if (value !== undefined) {
          list.push(value)
        }
      } while (value !== undefined)

      return list
    },

    map<T>(f: (AutomergeValue, number) => T): T[] {
      return this.toArray().map(f)
    },

    toString(): string {
      return this.toArray().toString()
    },

    toLocaleString(): string {
      return this.toArray().toLocaleString()
    },

    forEach(f: (AutomergeValue, number) => undefined) {
      return this.toArray().forEach(f)
    },

    // todo: real concat function is different
    concat(other: AutomergeValue[]): AutomergeValue[] {
      return this.toArray().concat(other)
    },

    every(f: (AutomergeValue, number) => boolean): boolean {
      return this.toArray().every(f)
    },

    filter(f: (AutomergeValue, number) => boolean): AutomergeValue[] {
      return this.toArray().filter(f)
    },

    find(f: (AutomergeValue, number) => boolean): AutomergeValue | undefined {
      let index = 0
      for (const v of this) {
        if (f(v, index)) {
          return v
        }
        index += 1
      }
    },

    findIndex(f: (AutomergeValue, number) => boolean): number {
      let index = 0
      for (const v of this) {
        if (f(v, index)) {
          return index
        }
        index += 1
      }
      return -1
    },

    includes(elem: AutomergeValue): boolean {
      return this.find(e => e === elem) !== undefined
    },

    join(sep?: string): string {
      return this.toArray().join(sep)
    },

    // todo: remove the any
    reduce<T>(f: (any, AutomergeValue) => T, initalValue?: T): T | undefined {
      return this.toArray().reduce(f, initalValue)
    },

    // todo: remove the any
    reduceRight<T>(
      f: (any, AutomergeValue) => T,
      initalValue?: T
    ): T | undefined {
      return this.toArray().reduceRight(f, initalValue)
    },

    lastIndexOf(search: AutomergeValue, fromIndex = +Infinity): number {
      // this can be faster
      return this.toArray().lastIndexOf(search, fromIndex)
    },

    slice(index?: number, num?: number): AutomergeValue[] {
      return this.toArray().slice(index, num)
    },

    some(f: (AutomergeValue, number) => boolean): boolean {
      let index = 0
      for (const v of this) {
        if (f(v, index)) {
          return true
        }
        index += 1
      }
      return false
    },

    [Symbol.iterator]: function* () {
      let i = 0
      let value = valueAt(target, i)
      while (value !== undefined) {
        yield value
        i += 1
        value = valueAt(target, i)
      }
    },
  }
  return methods
}

function textMethods(target: Target) {
  const { context, objectId, heads } = target
  const methods = {
    set(index: number, value) {
      return (this[index] = value)
    },
    get(index: number): AutomergeValue {
      return this[index]
    },
    toString(): string {
      return context.text(objectId, heads).replace(/￼/g, "")
    },
    toSpans(): AutomergeValue[] {
      const spans: AutomergeValue[] = []
      let chars = ""
      const length = context.length(objectId)
      for (let i = 0; i < length; i++) {
        const value = this[i]
        if (typeof value === "string") {
          chars += value
        } else {
          if (chars.length > 0) {
            spans.push(chars)
            chars = ""
          }
          spans.push(value)
        }
      }
      if (chars.length > 0) {
        spans.push(chars)
      }
      return spans
    },
    toJSON(): string {
      return this.toString()
    },
    indexOf(o, start = 0) {
      const text = context.text(objectId)
      return text.indexOf(o, start)
    },
  }
  return methods
}
