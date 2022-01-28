
const AutomergeWASM = require("automerge-wasm")
const { Int, Uint, Float64 } = require("./numbers");
const { Counter, getWriteableCounter } = require("./counter");
const { Text } = require("./text");
const { STATE, HEADS, FROZEN, OBJECT_ID, READ_ONLY } = require("./constants")
const { MAP, LIST, TABLE, TEXT } = require("automerge-wasm")

function parseListIndex(key) {
  if (typeof key === 'string' && /^[0-9]+$/.test(key)) key = parseInt(key, 10)
  if (typeof key !== 'number') {
    // throw new TypeError('A list index must be a number, but you passed ' + JSON.stringify(key))
    return key
  }
  if (key < 0 || isNaN(key) || key === Infinity || key === -Infinity) {
    throw new RangeError('A list index must be positive, but you passed ' + key)
  }
  return key
}

function valueAt(target, prop) {
      const { context, objectId, path, readonly, heads} = target
      let value = context.value(objectId, prop, heads)
      if (value === undefined) {
        return
      }
      const datatype = value[0]
      const val = value[1]
      switch (datatype) {
        case undefined: return;
        case "map": return mapProxy(context, val, [ ... path, prop ], readonly, heads);
        case "list": return listProxy(context, val, [ ... path, prop ], readonly, heads);
        case "text": return textProxy(context, val, [ ... path, prop ], readonly, heads);
        //case "table":
        //case "cursor":
        case "str": return val;
        case "uint": return val;
        case "int": return val;
        case "f64": return val;
        case "boolean": return val;
        case "null": return null;
        case "bytes": return val;
        case "timestamp": return val;
        case "counter": {
          if (readonly) {
            return new Counter(val);
          } else {
            return getWriteableCounter(val, context, path, objectId, prop)
          }
        }
        default:
          throw RangeError(`datatype ${datatype} unimplemented`)
      }
}

function import_value(value) {
    switch (typeof value) {
      case 'object':
        if (value == null) {
          return [ null, "null"]
        } else if (value instanceof Uint) {
          return [ value.value, "uint" ]
        } else if (value instanceof Int) {
          return [ value.value, "int" ]
        } else if (value instanceof Float64) {
          return [ value.value, "f64" ]
        } else if (value instanceof Counter) {
          return [ value.value, "counter" ]
        } else if (value instanceof Date) {
          return [ value.getTime(), "timestamp" ]
        } else if (value instanceof Uint8Array) {
          return [ value, "bytes" ]
        } else if (value instanceof Array) {
          return [ value, "list" ]
        } else if (value instanceof Text) {
          return [ value, "text" ]
        } else if (value[OBJECT_ID]) {
          throw new RangeError('Cannot create a reference to an existing document object')
        } else {
          return [ value, "map" ]
        }
        break;
      case 'boolean':
        return [ value, "boolean" ]
      case 'number':
        if (Number.isInteger(value)) {
          return [ value, "int" ]
        } else {
          return [ value, "f64" ]
        }
        break;
      case 'string':
        return [ value ]
        break;
      default:
        throw new RangeError(`Unsupported type of value: ${typeof value}`)
    }
}

const MapHandler = {
  get (target, key) {
    const { context, objectId, path, readonly, frozen, heads, cache } = target
    if (key === Symbol.toStringTag) { return target[Symbol.toStringTag] }
    if (key === OBJECT_ID) return objectId
    if (key === READ_ONLY) return readonly
    if (key === FROZEN) return frozen
    if (key === HEADS) return heads
    if (key === STATE) return context;
    if (!cache[key]) {
      cache[key] = valueAt(target, key)
    }
    return cache[key]
  },

  set (target, key, val) {
    let { context, objectId, path, readonly, frozen} = target
    target.cache = {} // reset cache on set
    if (val && val[OBJECT_ID]) {
          throw new RangeError('Cannot create a reference to an existing document object')
    }
    if (key === FROZEN) {
      target.frozen = val
      return
    }
    if (key === HEADS) {
      target.heads = val
      return
    }
    let [ value, datatype ] = import_value(val)
    if (frozen) {
      throw new RangeError("Attempting to use an outdated Automerge document")
    }
    if (readonly) {
      throw new RangeError(`Object property "${key}" cannot be modified`)
    }
    switch (datatype) {
      case "list":
        const list = context.set(objectId, key, LIST)
        const proxyList = listProxy(context, list, [ ... path, key ], readonly );
        for (let i = 0; i < value.length; i++) {
          proxyList[i] = value[i]
        }
        break;
      case "text":
        const text = context.set(objectId, key, TEXT)
        const proxyText = textProxy(context, text, [ ... path, key ], readonly );
        for (let i = 0; i < value.length; i++) {
          proxyText[i] = value.get(i)
        }
        break;
      case "map":
        const map = context.set(objectId, key, MAP)
        const proxyMap = mapProxy(context, map, [ ... path, key ], readonly );
        for (const key in value) {
          proxyMap[key] = value[key]
        }
        break;
      default:
        context.set(objectId, key, value, datatype)
    }
    return true
  },

  deleteProperty (target, key) {
    const { context, objectId, path, readonly, frozen } = target
    target.cache = {} // reset cache on delete
    if (readonly) {
      throw new RangeError(`Object property "${key}" cannot be modified`)
    }
    context.del(objectId, key)
    return true
  },

  has (target, key) {
    const value = this.get(target, key)
    return value !== undefined
  },

  getOwnPropertyDescriptor (target, key) {
    const { context, objectId } = target
    const value = this.get(target, key)
    if (typeof value !== 'undefined') {
      return {
        configurable: true, enumerable: true, value
      }
    }
  },

  ownKeys (target) {
    const { context, objectId, heads} = target
    return context.keys(objectId, heads)
  },
}


const ListHandler = {
  get (target, index) {
    const {context, objectId, path, readonly, frozen, heads } = target
    index = parseListIndex(index)
    if (index === Symbol.hasInstance) { return (instance) => { return [].has(instance) } }
    if (index === Symbol.toStringTag) { return target[Symbol.toStringTag] }
    if (index === OBJECT_ID) return objectId
    if (index === READ_ONLY) return readonly
    if (index === FROZEN) return frozen
    if (index === HEADS) return heads
    if (index === STATE) return context;
    if (index === 'length') return context.length(objectId, heads);
    if (index === Symbol.iterator) {
      let i = 0;
      return function *() {
        // FIXME - ugly
        let value = valueAt(target, i)
        while (value !== undefined) {
            yield value
            i += 1
            value = valueAt(target, i)
        }
      }
    }
    if (typeof index === 'number') {
      return valueAt(target, index)
    } else {
      return listMethods(target)[index]
    }
  },

  set (target, index, val) {
    let {context, objectId, path, readonly, frozen } = target
    index = parseListIndex(index)
    if (val && val[OBJECT_ID]) {
      throw new RangeError('Cannot create a reference to an existing document object')
    }
    if (index === FROZEN) {
      target.frozen = val
      return
    }
    if (index === HEADS) {
      target.heads = val
      return
    }
    if (typeof index == "string") {
      throw new RangeError('list index must be a number')
    }
    const [ value, datatype] = import_value(val)
    if (frozen) {
      throw new RangeError("Attempting to use an outdated Automerge document")
    }
    if (readonly) {
      throw new RangeError(`Object property "${index}" cannot be modified`)
    }
    switch (datatype) {
      case "list":
        let list
        if (index >= context.length(objectId)) {
          list = context.insert(objectId, index, LIST)
        } else {
          list = context.set(objectId, index, LIST)
        }
        const proxyList = listProxy(context, list, [ ... path, index ], readonly);
        proxyList.splice(0,0,...value)
        break;
      case "text":
        let text
        if (index >= context.length(objectId)) {
          text = context.insert(objectId, index, TEXT)
        } else {
          text = context.set(objectId, index, TEXT)
        }
        const proxyText = textProxy(context, text, [ ... path, index ], readonly);
        proxyText.splice(0,0,...value)
        break;
      case "map":
        let map
        if (index >= context.length(objectId)) {
          map = context.insert(objectId, index, MAP)
        } else {
          map = context.set(objectId, index, MAP)
        }
        const proxyMap = mapProxy(context, map, [ ... path, index ], readonly);
        for (const key in value) {
          proxyMap[key] = value[key]
        }
        break;
      default:
        if (index >= context.length(objectId)) {
          context.insert(objectId, index, value, datatype)
        } else {
          context.set(objectId, index, value, datatype)
        }
    }
    return true
  },

  deleteProperty (target, index) {
    const {context, objectId} = target
    index = parseListIndex(index)
    if (context.value(objectId, index)[0] == "counter") {
      throw new TypeError('Unsupported operation: deleting a counter from a list')
    }
    context.del(objectId, index)
    return true
  },

  has (target, index) {
    const {context, objectId, heads} = target
    index = parseListIndex(index)
    if (typeof index === 'number') {
      return index < context.length(objectId, heads)
    }
    return index === 'length'
  },

  getOwnPropertyDescriptor (target, index) {
    const {context, objectId, path, readonly, frozen, heads} = target

    if (index === 'length') return {writable: true, value: context.length(objectId, heads) }
    if (index === OBJECT_ID) return {configurable: false, enumerable: false, value: objectId}

    index = parseListIndex(index)

    let value = valueAt(target, index)
    return { configurable: true, enumerable: true, value }
  },

  getPrototypeOf(target) { return Object.getPrototypeOf([]) },
  ownKeys (target) {
    const {context, objectId, heads } = target
    let keys = []
    // uncommenting this causes assert.deepEqual() to fail when comparing to a pojo array
    // but not uncommenting it causes for (i in list) {} to not enumerate values properly
    //for (let i = 0; i < target.context.length(objectId, heads); i++) { keys.push(i.toString()) }
    keys.push("length");
    return keys
  }
}

const TextHandler = Object.assign({}, ListHandler, {
  get (target, index) {
    // FIXME this is a one line change from ListHandler.get()
    const {context, objectId, path, readonly, frozen, heads } = target
    index = parseListIndex(index)
    if (index === Symbol.toStringTag) { return target[Symbol.toStringTag] }
    if (index === Symbol.hasInstance) { return (instance) => { return [].has(instance) } }
    if (index === OBJECT_ID) return objectId
    if (index === READ_ONLY) return readonly
    if (index === FROZEN) return frozen
    if (index === HEADS) return heads
    if (index === STATE) return context;
    if (index === 'length') return context.length(objectId, heads);
    if (index === Symbol.iterator) {
      let i = 0;
      return function *() {
        let value = valueAt(target, i)
        while (value !== undefined) {
            yield value
            i += 1
            value = valueAt(target, i)
        }
      }
    }
    if (typeof index === 'number') {
      return valueAt(target, index)
    } else {
      return textMethods(target)[index] || listMethods(target)[index]
    }
  },
  getPrototypeOf(target) {
    return Object.getPrototypeOf(new Text())
  },
})

function mapProxy(context, objectId, path, readonly, heads) {
  return new Proxy({context, objectId, path, readonly: !!readonly, frozen: false, heads, cache: {}}, MapHandler)
}

function listProxy(context, objectId, path, readonly, heads) {
  let target = []
  Object.assign(target, {context, objectId, path, readonly: !!readonly, frozen: false, heads, cache: {}})
  return new Proxy(target, ListHandler)
}

function textProxy(context, objectId, path, readonly, heads) {
  let target = []
  Object.assign(target, {context, objectId, path, readonly: !!readonly, frozen: false, heads, cache: {}})
  return new Proxy(target, TextHandler)
}

function rootProxy(context, readonly) {
  return mapProxy(context, "_root", [], readonly)
}

function listMethods(target) {
  const {context, objectId, path, readonly, frozen, heads} = target
  const methods = {
    deleteAt(index, numDelete) {
      if (typeof numDelete === 'number') {
        context.splice(objectId, index, numDelete)
      } else {
        context.del(objectId, index)
      }
      return this
    },

    fill(val, start, end) {
      // FIXME
      let list = context.getObject(objectId)
      let [value, datatype] = valueAt(target, index)
      for (let index = parseListIndex(start || 0); index < parseListIndex(end || list.length); index++) {
        context.set(objectId, index, value, datatype)
      }
      return this
    },

    indexOf(o, start = 0) {
      // FIXME
      const id = o[OBJECT_ID]
      if (id) {
        const list = context.getObject(objectId)
        for (let index = start; index < list.length; index++) {
          if (list[index][OBJECT_ID] === id) {
            return index
          }
        }
        return -1
      } else {
        return context.indexOf(objectId, o, start)
      }
    },

    insertAt(index, ...values) {
      this.splice(index, 0, ...values)
      return this
    },

    pop() {
      let length = context.length(objectId)
      if (length == 0) {
        return undefined
      }
      let last = valueAt(target, length - 1)
      context.del(objectId, length - 1)
      return last
    },

    push(...values) {
      let len = context.length(objectId)
      this.splice(len, 0, ...values)
      return context.length(objectId)
    },

    shift() {
      if (context.length(objectId) == 0) return
      const first = valueAt(target, 0)
      context.del(objectId, 0)
      return first
    },

    splice(index, del, ...vals) {
      index = parseListIndex(index)
      del = parseListIndex(del)
      for (let val of vals) {
        if (val && val[OBJECT_ID]) {
              throw new RangeError('Cannot create a reference to an existing document object')
        }
      }
      if (frozen) {
        throw new RangeError("Attempting to use an outdated Automerge document")
      }
      if (readonly) {
        throw new RangeError("Sequence object cannot be modified outside of a change block")
      }
      let result = []
      for (let i = 0; i < del; i++) {
        let value = valueAt(target, index)
        result.push(value)
        context.del(objectId, index)
      }
      const values = vals.map((val) => import_value(val))
      for (let [value,datatype] of values) {
        switch (datatype) {
          case "list":
            const list = context.insert(objectId, index, LIST)
            const proxyList = listProxy(context, list, [ ... path, index ], readonly);
            proxyList.splice(0,0,...value)
            break;
          case "text":
            const text = context.insert(objectId, index, TEXT)
            const proxyText = textProxy(context, text, [ ... path, index ], readonly);
            proxyText.splice(0,0,...value)
            break;
          case "map":
            const map = context.insert(objectId, index, MAP)
            const proxyMap = mapProxy(context, map, [ ... path, index ], readonly);
            for (const key in value) {
              proxyMap[key] = value[key]
            }
            break;
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
      let i = 0;
      const iterator = {
        next: () => {
          let value = valueAt(target, i)
          if (value === undefined) {
            return { value: undefined, done: true }
          } else {
            return { value: [ i, value ], done: false }
          }
        }
      }
      return iterator
    },

    keys() {
      let i = 0;
      let len = context.length(objectId, heads)
      const iterator = {
        next: () => {
          let value = undefined
          if (i < len) { value = i; i++ }
          return { value, done: true }
        }
      }
      return iterator
    },

    values() {
      let i = 0;
      const iterator = {
        next: () => {
          let value = valueAt(target, i)
          if (value === undefined) {
            return { value: undefined, done: true }
          } else {
            return { value, done: false }
          }
        }
      }
      return iterator
    }
  }

  // Read-only methods that can delegate to the JavaScript built-in implementations
  // FIXME - super slow
  for (let method of ['concat', 'every', 'filter', 'find', 'findIndex', 'forEach', 'includes',
                      'join', 'lastIndexOf', 'map', 'reduce', 'reduceRight',
                      'slice', 'some', 'toLocaleString', 'toString']) {
    methods[method] = (...args) => {
      const list = []
      while (true) {
        let value =  valueAt(target, list.length)
        if (value == undefined) {
          break
        }
        list.push(value)
      }

      return list[method](...args)
    }
  }

  return methods
}

function textMethods(target) {
  const {context, objectId, path, readonly, frozen} = target
  const methods = {
    set (index, value) {
      return this[index] = value
    },
    get (index) {
      return this[index]
    },
    toString () {
      let str = ''
      let length = this.length
      for (let i = 0; i < length; i++) {
        const value = this.get(i)
        if (typeof value === 'string') str += value
      }
      return str
    },
    toSpans () {
      let spans = []
      let chars = ''
      let length = this.length
      for (let i = 0; i < length; i++) {
        const value = this[i]
        if (typeof value === 'string') {
          chars += value
        } else {
          if (chars.length > 0) {
            spans.push(chars)
            chars = ''
          }
          spans.push(value)
        }
      }
      if (chars.length > 0) {
        spans.push(chars)
      }
      return spans
    },
    toJSON () {
      return this.toString()
    }
  }
  return methods
}


module.exports = { rootProxy, textProxy, listProxy, mapProxy, MapHandler, ListHandler, TextHandler }
