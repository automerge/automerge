/* eslint-disable  @typescript-eslint/no-explicit-any */
import type { Automerge, ObjID, Prop } from "./wasm_types.js"

const MAX_I64 = BigInt("9223372036854775807") // 2n ** 63n - 1n;

import type {
  AutomergeValue,
  ScalarValue,
  MapValue,
  ListValue,
} from "./types.js"
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
  IMMUTABLE_STRING,
  TEXT,
} from "./constants.js"
import { ImmutableString } from "./immutable_string.js"

type Target = {
  context: Automerge
  objectId: ObjID
  path: Array<Prop>
  cache: object
  trace?: any
}

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

function valueAt(target: Target, prop: Prop): AutomergeValue | undefined {
  const { context, objectId, path } = target
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
      return mapProxy(context, val as ObjID, [...path, prop])
    case "list":
      return listProxy(context, val as ObjID, [...path, prop])
    case "text":
      return context.text(val as ObjID) as AutomergeValue
    case "str":
      return new ImmutableString(val as string) as AutomergeValue
    case "uint":
      return val as AutomergeValue
    case "int":
      return val as AutomergeValue
    case "f64":
      return val as AutomergeValue
    case "boolean":
      return val as AutomergeValue
    case "null":
      return null as AutomergeValue
    case "bytes":
      return val as AutomergeValue
    case "timestamp":
      return val as AutomergeValue
    case "counter": {
      const counter: Counter = getWriteableCounter(
        val as number,
        context,
        path,
        objectId,
        prop,
      )
      return counter as AutomergeValue
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
  | [string, "text"]
  | [Uint8Array, "bytes"]
  | [Array<any>, "list"]
  | [Record<string, any>, "map"]
  | [boolean, "boolean"]

function import_value(
  value: any,
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
      } else if (isImmutableString(value)) {
        return [value.toString(), "str"]
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
    case "bigint":
      if (value > MAX_I64) {
        return [value, "uint"]
      } else {
        return [value, "int"]
      }
    case "number":
      if (Number.isInteger(value)) {
        return [value, "int"]
      } else {
        return [value, "f64"]
      }
    case "string":
      return [value, "text"]
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
  ): AutomergeValue | ObjID | boolean | { handle: Automerge } {
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

  set(target: Target, key: any, val: any) {
    const { context, objectId, path } = target
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

    const [value, datatype] = import_value(val, [...path, key], context)
    switch (datatype) {
      case "list": {
        const list = context.putObject(objectId, key, [])
        const proxyList = listProxy(context, list, [...path, key])
        for (let i = 0; i < value.length; i++) {
          proxyList[i] = value[i]
        }
        break
      }
      case "text": {
        context.putObject(objectId, key, value)
        break
      }
      case "map": {
        const map = context.putObject(objectId, key, {})
        const proxyMap = mapProxy(context, map, [...path, key])
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
    | AutomergeValue
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
      return valueAt(target, index) as AutomergeValue
    } else {
      return listMethods(target)[index]
    }
  },

  set(target: Target, index: any, val: any) {
    const { context, objectId, path } = target
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
    const [value, datatype] = import_value(val, [...path, index], context)
    switch (datatype) {
      case "list": {
        let list: ObjID
        if (index >= context.length(objectId)) {
          list = context.insertObject(objectId, index, [])
        } else {
          list = context.putObject(objectId, index, [])
        }
        const proxyList = listProxy(context, list, [...path, index])
        proxyList.splice(0, 0, ...value)
        break
      }
      case "text": {
        if (index >= context.length(objectId)) {
          context.insertObject(objectId, index, value)
        } else {
          context.putObject(objectId, index, value)
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
        const proxyMap = mapProxy(context, map, [...path, index])
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

export function mapProxy(
  context: Automerge,
  objectId: ObjID,
  path: Prop[],
): MapValue {
  const target: Target = {
    context,
    objectId,
    path: path || [],
    cache: {},
  }
  const proxied = {}
  Object.assign(proxied, target)
  const result = new Proxy(proxied, MapHandler)
  // conversion through unknown is necessary because the types are so different
  return result as unknown as MapValue
}

export function listProxy(
  context: Automerge,
  objectId: ObjID,
  path: Prop[],
): ListValue {
  const target: Target = {
    context,
    objectId,
    path: path || [],
    cache: {},
  }
  const proxied = []
  Object.assign(proxied, target)
  // eslint-disable-next-line @typescript-eslint/ban-ts-comment
  // @ts-ignore
  return new Proxy(proxied, ListHandler) as unknown as ListValue
}

export function rootProxy<T>(context: Automerge): T {
  /* eslint-disable-next-line */
  return <any>mapProxy(context, "_root", [])
}

function listMethods(target: Target) {
  const { context, objectId, path } = target
  const methods = {
    at(index: number) {
      return valueAt(target, index)
    },

    deleteAt(index: number, numDelete: number) {
      if (typeof numDelete === "number") {
        context.splice(objectId, index, numDelete)
      } else {
        context.delete(objectId, index)
      }
      return this
    },

    fill(val: ScalarValue, start: number, end: number) {
      const [value, datatype] = import_value(val, [...path, start], context)
      const length = context.length(objectId)
      start = parseListIndex(start || 0)
      end = parseListIndex(end || length)
      for (let i = start; i < Math.min(end, length); i++) {
        if (datatype === "list" || datatype === "map") {
          context.putObject(objectId, i, value)
        } else if (datatype === "text") {
          context.putObject(objectId, i, value)
        } else {
          context.put(objectId, i, value, datatype)
        }
      }
      return this
    },

    indexOf(searchElement: any, start = 0) {
      const length = context.length(objectId)
      for (let i = start; i < length; i++) {
        const valueWithType = context.getWithType(objectId, i)
        if (!valueWithType) {
          continue
        }

        const [valType, value] = valueWithType

        // Either the target element is an object, and we return if we have found
        // the same object or it is a primitive value and we return if it matches
        // the current value
        const isObject = ["map", "list", "text"].includes(valType)

        if (!isObject) {
          // If the element is not an object, then check if the value is equal to the target
          if (value === searchElement) {
            return i
          } else {
            continue
          }
        }

        // if it's an object, but the type of the search element is a string, then we
        // need to check if the object is a text object with the same value as the search element
        if (valType === "text" && typeof searchElement === "string") {
          if (searchElement === valueAt(target, i)) {
            return i
          }
        }

        // The only possible match now is if the searchElement is an object already in the
        // automerge document with the same object ID as the value
        if (searchElement[OBJECT_ID] === value) {
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
      const result: AutomergeValue[] = []
      for (let i = 0; i < del; i++) {
        const value = valueAt(target, index)
        if (value !== undefined) {
          result.push(value)
        }
        context.delete(objectId, index)
      }
      const values = vals.map((val, index) => {
        try {
          return import_value(val, [...path], context)
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
            const proxyList = listProxy(context, list, [...path, index])
            proxyList.splice(0, 0, ...value)
            break
          }
          case "text": {
            context.insertObject(objectId, index, value)
            break
          }
          case "map": {
            const map = context.insertObject(objectId, index, {})
            const proxyMap = mapProxy(context, map, [...path, index])
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
      const iterator: IterableIterator<[number, AutomergeValue]> = {
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
      const iterator: IterableIterator<AutomergeValue> = {
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

    toArray(): AutomergeValue[] {
      const list: Array<AutomergeValue> = []
      let value: AutomergeValue | undefined
      do {
        value = valueAt(target, list.length)
        if (value !== undefined) {
          list.push(value)
        }
      } while (value !== undefined)

      return list
    },

    map<U>(f: (_a: AutomergeValue, _n: number) => U): U[] {
      return this.toArray().map(f)
    },

    toString(): string {
      return this.toArray().toString()
    },

    toLocaleString(): string {
      return this.toArray().toLocaleString()
    },

    forEach(f: (_a: AutomergeValue, _n: number) => undefined) {
      return this.toArray().forEach(f)
    },

    // todo: real concat function is different
    concat(other: AutomergeValue[]): AutomergeValue[] {
      return this.toArray().concat(other)
    },

    every(f: (_a: AutomergeValue, _n: number) => boolean): boolean {
      return this.toArray().every(f)
    },

    filter(f: (_a: AutomergeValue, _n: number) => boolean): AutomergeValue[] {
      return this.toArray().filter(f)
    },

    find(
      f: (_a: AutomergeValue, _n: number) => boolean,
    ): AutomergeValue | undefined {
      let index = 0
      for (const v of this) {
        if (f(v, index)) {
          return v
        }
        index += 1
      }
    },

    findIndex(f: (_a: AutomergeValue, _n: number) => boolean): number {
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

    reduce<U>(
      f: (acc: U, currentValue: AutomergeValue) => U,
      initialValue: U,
    ): U | undefined {
      return this.toArray().reduce<U>(f, initialValue)
    },

    reduceRight<U>(
      f: (acc: U, item: AutomergeValue) => U,
      initialValue: U,
    ): U | undefined {
      return this.toArray().reduceRight(f, initialValue)
    },

    lastIndexOf(search: AutomergeValue, fromIndex = +Infinity): number {
      // this can be faster
      return this.toArray().lastIndexOf(search, fromIndex)
    },

    slice(index?: number, num?: number): AutomergeValue[] {
      return this.toArray().slice(index, num)
    },

    some(f: (v: AutomergeValue, i: number) => boolean): boolean {
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

/*
 * Check if an object is a {@link ImmutableString}
 */
export function isImmutableString(obj: any): obj is ImmutableString {
  // We used to determine whether something was a ImmutableString by doing an instanceof check, but
  // this doesn't work if the automerge module is loaded twice somehow. Instead, use the presence
  // of a symbol to determine if something is a ImmutableString

  return (
    typeof obj === "object" &&
    obj !== null &&
    Object.prototype.hasOwnProperty.call(obj, IMMUTABLE_STRING)
  )
}

export function isCounter(obj: any): obj is Counter {
  // We used to determine whether something was a Counter by doing an instanceof check, but
  // this doesn't work if the automerge module is loaded twice somehow. Instead, use the presence
  // of a symbol to determine if something is a Counter

  return (
    typeof obj === "object" &&
    obj !== null &&
    Object.prototype.hasOwnProperty.call(obj, COUNTER)
  )
}
