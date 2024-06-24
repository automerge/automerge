/* eslint-disable  @typescript-eslint/no-explicit-any */
import { Text } from "./text.js"
import { Automerge, type ObjID, type Prop } from "@automerge/automerge-wasm"

import type {
  AutomergeValue,
  ScalarValue,
  MapValue,
  ListValue,
} from "./types.js"
import {
  type AutomergeValue as UnstableAutomergeValue,
  MapValue as UnstableMapValue,
  ListValue as UnstableListValue,
} from "./next_types.js"
import { Counter, getWriteableCounter } from "./counter.js"
import {
  STATE,
  TRACE,
  IS_PROXY,
  OBJECT_ID,
  CLEAR_CACHE,
  COUNTER,
  INT,
  UINT,
  F64,
} from "./constants.js"
import { RawString } from "./raw_string.js"

type TargetCommon = {
  context: Automerge
  objectId: ObjID
  path: Array<Prop>
  cache: object
  trace?: any
}

export type Text2Target = TargetCommon & { textV2: true }
export type Text1Target = TargetCommon & { textV2: false }
export type Target = Text1Target | Text2Target

export type ValueType<T extends Target> = T extends Text2Target
  ? UnstableAutomergeValue
  : T extends Text1Target
    ? AutomergeValue
    : never
type MapValueType<T extends Target> = T extends Text2Target
  ? UnstableMapValue
  : T extends Text1Target
    ? MapValue
    : never
type ListValueType<T extends Target> = T extends Text2Target
  ? UnstableListValue
  : T extends Text1Target
    ? ListValue
    : never

function parseListIndex(key: any) {
  if (typeof key === "string" && /^[0-9]+$/.test(key)) key = parseInt(key, 10)
  if (typeof key !== "number") {
    return key
  }
  if (key < 0 || isNaN(key) || key === Infinity || key === -Infinity) {
    throw new RangeError("A list index must be positive, but you passed " + key)
  }
  return key
}

function valueAt<T extends Target>(
  target: T,
  prop: Prop,
): ValueType<T> | undefined {
  const { context, objectId, path, textV2 } = target
  const value = context.getWithType(objectId, prop)
  if (value === null) {
    return
  }
  const datatype = value[0]
  const val = value[1]
  switch (datatype) {
    case undefined:
      return
    case "map":
      return mapProxy<T>(context, val as ObjID, textV2, [...path, prop])
    case "list":
      return listProxy<T>(context, val as ObjID, textV2, [...path, prop])
    case "text":
      if (textV2) {
        return context.text(val as ObjID) as ValueType<T>
      } else {
        return textProxy(context, val as ObjID, [
          ...path,
          prop,
        ]) as unknown as ValueType<T>
      }
    case "str":
      return val as ValueType<T>
    case "uint":
      return val as ValueType<T>
    case "int":
      return val as ValueType<T>
    case "f64":
      return val as ValueType<T>
    case "boolean":
      return val as ValueType<T>
    case "null":
      return null as ValueType<T>
    case "bytes":
      return val as ValueType<T>
    case "timestamp":
      return val as ValueType<T>
    case "counter": {
      const counter: Counter = getWriteableCounter(
        val as number,
        context,
        path,
        objectId,
        prop,
      )
      return counter as ValueType<T>
    }
    default:
      throw RangeError(`datatype ${datatype} unimplemented`)
  }
}

type ImportedValue =
  | [null, "null"]
  | [number, "uint"]
  | [number, "int"]
  | [number, "f64"]
  | [number, "counter"]
  | [number, "timestamp"]
  | [string, "str"]
  | [Text | string, "text"]
  | [Uint8Array, "bytes"]
  | [Array<any>, "list"]
  | [Record<string, any>, "map"]
  | [boolean, "boolean"]

function import_value(
  value: any,
  textV2: boolean,
  path: Prop[],
  context: Automerge,
): ImportedValue {
  const type = typeof value
  switch (type) {
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
        return [value.toString(), "str"]
      } else if (value instanceof Text) {
        return [value, "text"]
      } else if (value instanceof Uint8Array) {
        return [value, "bytes"]
      } else if (value instanceof Array) {
        return [value, "list"]
      } else if (Object.prototype.toString.call(value) === "[object Object]") {
        return [value, "map"]
      } else if (isSameDocument(value, context)) {
        throw new RangeError(
          "Cannot create a reference to an existing document object",
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
    case "undefined":
      throw new RangeError(
        [
          `Cannot assign undefined value at ${printPath(path)}, `,
          "because `undefined` is not a valid JSON data type. ",
          "You might consider setting the property's value to `null`, ",
          "or using `delete` to remove it altogether.",
        ].join(""),
      )
    default:
      throw new RangeError(
        [
          `Cannot assign ${type} value at ${printPath(path)}. `,
          `All JSON primitive datatypes (object, array, string, number, boolean, null) `,
          `are supported in an Automerge document; ${type} values are not. `,
        ].join(""),
      )
  }
}

// When we assign a value to a property in a proxy we recursively walk through
// the value we are assigning and copy it into the document. This is generally
// desirable behaviour. However, a very common bug is to accidentally assign a
// value which is already in the document to another key within the same
// document, this often leads to surprising behaviour where users expected to
// _move_ the object, but it is instead copied. To avoid this we check if the
// value is from the same document and if it is we throw an error, this means
// we require an explicit Object.assign call to copy the object, thus avoiding
// the footgun
function isSameDocument(val, context) {
  // Date is technically an object, but immutable, so allowing people to assign
  // a date from one place in the document to another place in the document is
  // not likely to be a bug
  if (val instanceof Date) {
    return false
  }

  // this depends on __wbg_ptr being the wasm pointer
  // a new version of wasm-bindgen will break this
  // but the tests should expose the break
  if (val && val[STATE]?.handle?.__wbg_ptr === context.__wbg_ptr) {
    return true
  }
  return false
}

const MapHandler = {
  get<T extends Target>(
    target: T,
    key: any,
  ): ValueType<T> | ObjID | boolean | { handle: Automerge; textV2: boolean } {
    const { context, objectId, cache } = target
    if (key === Symbol.toStringTag) {
      return target[Symbol.toStringTag]
    }
    if (key === OBJECT_ID) return objectId
    if (key === IS_PROXY) return true
    if (key === TRACE) return target.trace
    if (key === STATE) return { handle: context, textV2: target.textV2 }
    if (!cache[key]) {
      cache[key] = valueAt(target, key)
    }
    return cache[key]
  },

  set(target: Target, key: any, val: any) {
    const { context, objectId, path, textV2 } = target
    target.cache = {} // reset cache on set
    if (isSameDocument(val, context)) {
      throw new RangeError(
        "Cannot create a reference to an existing document object",
      )
    }
    if (key === TRACE) {
      target.trace = val
      return true
    }
    if (key === CLEAR_CACHE) {
      return true
    }

    const [value, datatype] = import_value(val, textV2, [...path, key], context)
    switch (datatype) {
      case "list": {
        const list = context.putObject(objectId, key, [])
        const proxyList = listProxy(context, list, textV2, [...path, key])
        for (let i = 0; i < value.length; i++) {
          proxyList[i] = value[i]
        }
        break
      }
      case "text": {
        if (textV2) {
          assertString(value)
          context.putObject(objectId, key, value)
        } else {
          assertText(value)
          const text = context.putObject(objectId, key, "")
          const proxyText = textProxy(context, text, [...path, key])
          proxyText.splice(0, 0, ...value)
        }
        break
      }
      case "map": {
        const map = context.putObject(objectId, key, {})
        const proxyMap = mapProxy(context, map, textV2, [...path, key])
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

  deleteProperty(target: Target, key: any) {
    const { context, objectId } = target
    target.cache = {} // reset cache on delete
    context.delete(objectId, key)
    return true
  },

  has(target: Target, key: any) {
    const value = this.get(target, key)
    return value !== undefined
  },

  getOwnPropertyDescriptor(target: Target, key: any) {
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
    const { context, objectId } = target
    // FIXME - this is a tmp workaround until fix the dupe key bug in keys()
    const keys = context.keys(objectId)
    return [...new Set<string>(keys)]
  },
}

const ListHandler = {
  get<T extends Target>(
    target: T,
    index: any,
  ):
    | ValueType<T>
    | boolean
    | ObjID
    | { handle: Automerge }
    | number
    | ((_: any) => boolean) {
    const { context, objectId } = target
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
    if (index === "length") return context.length(objectId)
    if (typeof index === "number") {
      return valueAt(target, index) as ValueType<T>
    } else {
      return listMethods(target)[index]
    }
  },

  set(target: Target, index: any, val: any) {
    const { context, objectId, path, textV2 } = target
    index = parseListIndex(index)
    if (isSameDocument(val, context)) {
      throw new RangeError(
        "Cannot create a reference to an existing document object",
      )
    }
    if (index === CLEAR_CACHE) {
      return true
    }
    if (index === TRACE) {
      target.trace = val
      return true
    }
    if (typeof index == "string") {
      throw new RangeError("list index must be a number")
    }
    const [value, datatype] = import_value(
      val,
      textV2,
      [...path, index],
      context,
    )
    switch (datatype) {
      case "list": {
        let list: ObjID
        if (index >= context.length(objectId)) {
          list = context.insertObject(objectId, index, [])
        } else {
          list = context.putObject(objectId, index, [])
        }
        const proxyList = listProxy(context, list, textV2, [...path, index])
        proxyList.splice(0, 0, ...value)
        break
      }
      case "text": {
        if (textV2) {
          assertString(value)
          if (index >= context.length(objectId)) {
            context.insertObject(objectId, index, value)
          } else {
            context.putObject(objectId, index, value)
          }
        } else {
          let text: ObjID
          assertText(value)
          if (index >= context.length(objectId)) {
            text = context.insertObject(objectId, index, "")
          } else {
            text = context.putObject(objectId, index, "")
          }
          const proxyText = textProxy(context, text, [...path, index])
          proxyText.splice(0, 0, ...value)
        }
        break
      }
      case "map": {
        let map: ObjID
        if (index >= context.length(objectId)) {
          map = context.insertObject(objectId, index, {})
        } else {
          map = context.putObject(objectId, index, {})
        }
        const proxyMap = mapProxy(context, map, textV2, [...path, index])
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

  deleteProperty(target: Target, index: any) {
    const { context, objectId } = target
    index = parseListIndex(index)
    const elem = context.get(objectId, index)
    if (elem != null && elem[0] == "counter") {
      throw new TypeError(
        "Unsupported operation: deleting a counter from a list",
      )
    }
    context.delete(objectId, index)
    return true
  },

  has(target: Target, index: any) {
    const { context, objectId } = target
    index = parseListIndex(index)
    if (typeof index === "number") {
      return index < context.length(objectId)
    }
    return index === "length"
  },

  getOwnPropertyDescriptor(target: Target, index: any) {
    const { context, objectId } = target

    if (index === "length")
      return { writable: true, value: context.length(objectId) }
    if (index === OBJECT_ID)
      return { configurable: false, enumerable: false, value: objectId }

    index = parseListIndex(index)

    const value = valueAt(target, index)
    return { configurable: true, enumerable: true, value }
  },

  getPrototypeOf(target: Target) {
    return Object.getPrototypeOf(target)
  },
  ownKeys(/*target*/): string[] {
    const keys: string[] = []
    // uncommenting this causes assert.deepEqual() to fail when comparing to a pojo array
    // but not uncommenting it causes for (i in list) {} to not enumerate values properly
    //const {context, objectId } = target
    //for (let i = 0; i < target.context.length(objectId); i++) { keys.push(i.toString()) }
    keys.push("length")
    return keys
  },
}

const TextHandler = Object.assign({}, ListHandler, {
  get(target: Target, index: any) {
    const { context, objectId } = target
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
    if (index === "length") return context.length(objectId)
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

export function mapProxy<T extends Target>(
  context: Automerge,
  objectId: ObjID,
  textV2: boolean,
  path: Prop[],
): MapValueType<T> {
  const target: Target = {
    context,
    objectId,
    path: path || [],
    cache: {},
    textV2,
  }
  const proxied = {}
  Object.assign(proxied, target)
  const result = new Proxy(proxied, MapHandler)
  // conversion through unknown is necessary because the types are so different
  return result as unknown as MapValueType<T>
}

export function listProxy<T extends Target>(
  context: Automerge,
  objectId: ObjID,
  textV2: boolean,
  path: Prop[],
): ListValueType<T> {
  const target: Target = {
    context,
    objectId,
    path: path || [],
    cache: {},
    textV2,
  }
  const proxied = []
  Object.assign(proxied, target)
  // eslint-disable-next-line @typescript-eslint/ban-ts-comment
  // @ts-ignore
  return new Proxy(proxied, ListHandler) as unknown as ListValue
}

interface TextProxy extends Text {
  splice: (index: any, del: any, ...vals: any[]) => void
}

export function textProxy(
  context: Automerge,
  objectId: ObjID,
  path: Prop[],
): TextProxy {
  const target: Target = {
    context,
    objectId,
    path: path || [],
    cache: {},
    textV2: false,
  }
  const proxied = {}
  Object.assign(proxied, target)
  return new Proxy(proxied, TextHandler) as unknown as TextProxy
}

export function rootProxy<T>(context: Automerge, textV2: boolean): T {
  /* eslint-disable-next-line */
  return <any>mapProxy(context, "_root", textV2, [])
}

function listMethods<T extends Target>(target: T) {
  const { context, objectId, path, textV2 } = target
  const methods = {
    deleteAt(index: number, numDelete: number) {
      if (typeof numDelete === "number") {
        context.splice(objectId, index, numDelete)
      } else {
        context.delete(objectId, index)
      }
      return this
    },

    fill(val: ScalarValue, start: number, end: number) {
      const [value, datatype] = import_value(
        val,
        textV2,
        [...path, start],
        context,
      )
      const length = context.length(objectId)
      start = parseListIndex(start || 0)
      end = parseListIndex(end || length)
      for (let i = start; i < Math.min(end, length); i++) {
        if (datatype === "list" || datatype === "map") {
          context.putObject(objectId, i, value)
        } else if (datatype === "text") {
          if (textV2) {
            assertString(value)
            context.putObject(objectId, i, value)
          } else {
            assertText(value)
            const text = context.putObject(objectId, i, "")
            const proxyText = textProxy(context, text, [...path, i])
            for (let i = 0; i < value.length; i++) {
              proxyText[i] = value.get(i)
            }
          }
        } else {
          context.put(objectId, i, value, datatype)
        }
      }
      return this
    },

    indexOf(o: any, start = 0) {
      const length = context.length(objectId)
      for (let i = start; i < length; i++) {
        const value = context.getWithType(objectId, i)
        if (value && (value[1] === o[OBJECT_ID] || value[1] === o)) {
          return i
        }
      }
      return -1
    },

    insertAt(index: number, ...values: any[]) {
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

    push(...values: any[]) {
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

    splice(index: any, del: any, ...vals: any[]) {
      index = parseListIndex(index)

      // if del is undefined, delete until the end of the list
      if (typeof del !== "number") {
        del = context.length(objectId) - index
      }

      del = parseListIndex(del)

      for (const val of vals) {
        if (isSameDocument(val, context)) {
          throw new RangeError(
            "Cannot create a reference to an existing document object",
          )
        }
      }
      const result: ValueType<T>[] = []
      for (let i = 0; i < del; i++) {
        const value = valueAt<T>(target, index)
        if (value !== undefined) {
          result.push(value)
        }
        context.delete(objectId, index)
      }
      const values = vals.map((val, index) => {
        try {
          return import_value(val, textV2, [...path], context)
        } catch (e) {
          if (e instanceof RangeError) {
            throw new RangeError(
              `${e.message} (at index ${index} in the input)`,
            )
          } else {
            throw e
          }
        }
      })
      for (const [value, datatype] of values) {
        switch (datatype) {
          case "list": {
            const list = context.insertObject(objectId, index, [])
            const proxyList = listProxy(context, list, textV2, [...path, index])
            proxyList.splice(0, 0, ...value)
            break
          }
          case "text": {
            if (textV2) {
              assertString(value)
              context.insertObject(objectId, index, value)
            } else {
              const text = context.insertObject(objectId, index, "")
              const proxyText = textProxy(context, text, [...path, index])
              proxyText.splice(0, 0, ...value)
            }
            break
          }
          case "map": {
            const map = context.insertObject(objectId, index, {})
            const proxyMap = mapProxy(context, map, textV2, [...path, index])
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

    unshift(...values: any) {
      this.splice(0, 0, ...values)
      return context.length(objectId)
    },

    entries() {
      let i = 0
      const iterator: IterableIterator<[number, ValueType<T>]> = {
        next: () => {
          const value = valueAt(target, i)
          if (value === undefined) {
            return { value: undefined, done: true }
          } else {
            return { value: [i++, value], done: false }
          }
        },
        [Symbol.iterator]() {
          return this
        },
      }
      return iterator
    },

    keys() {
      let i = 0
      const len = context.length(objectId)
      const iterator: IterableIterator<number> = {
        next: () => {
          if (i < len) {
            return { value: i++, done: false }
          }
          return { value: undefined, done: true }
        },
        [Symbol.iterator]() {
          return this
        },
      }
      return iterator
    },

    values() {
      let i = 0
      const iterator: IterableIterator<ValueType<T>> = {
        next: () => {
          const value = valueAt(target, i++)
          if (value === undefined) {
            return { value: undefined, done: true }
          } else {
            return { value, done: false }
          }
        },
        [Symbol.iterator]() {
          return this
        },
      }
      return iterator
    },

    toArray(): ValueType<T>[] {
      const list: Array<ValueType<T>> = []
      let value: ValueType<T> | undefined
      do {
        value = valueAt<T>(target, list.length)
        if (value !== undefined) {
          list.push(value)
        }
      } while (value !== undefined)

      return list
    },

    map<U>(f: (_a: ValueType<T>, _n: number) => U): U[] {
      return this.toArray().map(f)
    },

    toString(): string {
      return this.toArray().toString()
    },

    toLocaleString(): string {
      return this.toArray().toLocaleString()
    },

    forEach(f: (_a: ValueType<T>, _n: number) => undefined) {
      return this.toArray().forEach(f)
    },

    // todo: real concat function is different
    concat(other: ValueType<T>[]): ValueType<T>[] {
      return this.toArray().concat(other)
    },

    every(f: (_a: ValueType<T>, _n: number) => boolean): boolean {
      return this.toArray().every(f)
    },

    filter(f: (_a: ValueType<T>, _n: number) => boolean): ValueType<T>[] {
      return this.toArray().filter(f)
    },

    find(
      f: (_a: ValueType<T>, _n: number) => boolean,
    ): ValueType<T> | undefined {
      let index = 0
      for (const v of this) {
        if (f(v, index)) {
          return v
        }
        index += 1
      }
    },

    findIndex(f: (_a: ValueType<T>, _n: number) => boolean): number {
      let index = 0
      for (const v of this) {
        if (f(v, index)) {
          return index
        }
        index += 1
      }
      return -1
    },

    includes(elem: ValueType<T>): boolean {
      return this.find(e => e === elem) !== undefined
    },

    join(sep?: string): string {
      return this.toArray().join(sep)
    },

    reduce<U>(
      f: (acc: U, currentValue: ValueType<T>) => U,
      initialValue: U,
    ): U | undefined {
      return this.toArray().reduce<U>(f, initialValue)
    },

    reduceRight<U>(
      f: (acc: U, item: ValueType<T>) => U,
      initialValue: U,
    ): U | undefined {
      return this.toArray().reduceRight(f, initialValue)
    },

    lastIndexOf(search: ValueType<T>, fromIndex = +Infinity): number {
      // this can be faster
      return this.toArray().lastIndexOf(search, fromIndex)
    },

    slice(index?: number, num?: number): ValueType<T>[] {
      return this.toArray().slice(index, num)
    },

    some(f: (v: ValueType<T>, i: number) => boolean): boolean {
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
  const { context, objectId } = target
  const methods = {
    set(index: number, value: any) {
      return (this[index] = value)
    },
    get(index: number): AutomergeValue {
      return this[index]
    },
    toString(): string {
      return context.text(objectId).replace(/￼/g, "")
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
    indexOf(o: any, start = 0) {
      const text = context.text(objectId)
      return text.indexOf(o, start)
    },
    insertAt(index: number, ...values: any[]) {
      if (values.every(v => typeof v === "string")) {
        context.splice(objectId, index, 0, values.join(""))
      } else {
        listMethods(target).insertAt(index, ...values)
      }
    },
  }
  return methods
}

function assertText(value: Text | string): asserts value is Text {
  if (!(value instanceof Text)) {
    throw new Error("value was not a Text instance")
  }
}

function assertString(value: Text | string): asserts value is string {
  if (typeof value !== "string") {
    throw new Error("value was not a string")
  }
}

function printPath(path: Prop[]): string {
  // print the path as a json pointer
  const jsonPointerComponents = path.map(component => {
    // if its a number just turn it into a string
    if (typeof component === "number") {
      return component.toString()
    } else if (typeof component === "string") {
      // otherwise we have to escape `/` and `~` characters
      return component.replace(/~/g, "~0").replace(/\//g, "~1")
    }
  })
  if (path.length === 0) {
    return ""
  } else {
    return "/" + jsonPointerComponents.join("/")
  }
}
