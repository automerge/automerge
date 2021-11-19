
const AutomergeWASM = require("automerge-wasm")
const { Int, Uint, Float64 } = require("./numbers");
const { Counter } = require("./counter");
const { STATE, FROZEN, OBJECT_ID, READ_ONLY } = require("./constants")
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

function valueAt(context, objectId, prop, path, readonly, conflicts) {
      let values = context.values(objectId, prop)
      if (values.length === 0) {
        return
      }
      let value = values[0]
      let local_conflict = values.length > 1
      const datatype = value[0]
      const val = value[1]
      switch (datatype) {
        case undefined: return;
        case "map": return mapProxy(context, val, [ ... path, prop ], readonly, conflicts || local_conflict);
        case "list": return listProxy(context, val, [ ... path, prop ], readonly, conflicts || local_conflict);
        //case "table":
        //case "text":
        //case "cursor":
        case "str": return val;
        case "uint": return val;
        case "int": return val;
        case "f64": return val;
        case "boolean": return val;
        case "null": return null;
        case "bytes": return val;
        case "counter": return new Counter(val);
        case "timestamp": return new Date(val);
        default:
          throw RangeError(`datatype ${datatype} unimplemented`)
      }
}

/*
function am2js(value, context, path, index, readonly) {
      const datatype = value[0]
      const val = value[1]
      switch (datatype) {
        case undefined: return;
        case "map": return mapProxy(context, val, [ ... path, index ], readonly);
        case "list": return listProxy(context, val, [ ... path, index ], readonly);
        //case "table":
        //case "text":
        //case "cursor":
        case "str": return val;
        case "uint": return val;
        case "int": return val;
        case "f64": return val;
        case "boolean": return val;
        case "null": return null;
        case "bytes": return val;
        case "counter": return new Counter(val);
        case "timestamp": return new Date(val);
        default:
          throw RangeError(`datatype ${datatype} unimplemented`)
      }
}
*/

function list_get(target, index) {
    const [context, objectId, path, readonly, frozen, conflicts] = target
    if (index === OBJECT_ID) return objectId
    if (index === READ_ONLY) return readonly
    if (index === FROZEN) return frozen
    if (index === STATE) return context;
    if (index === 'length') return context.length(objectId);
    if (index === Symbol.iterator) {
      let i = 0;
      return function *() {
        // FIXME - ugly
        let value = valueAt(context, objectId, i, path, readonly, conflicts)
        while (value !== undefined) {
            yield value
            i += 1
            value = valueAt(context, objectId, i, path, readonly, conflicts)
        }
      }
    }
    if (typeof index === 'number') {
      return valueAt(context, objectId, index, path, readonly, conflicts)
    } else {
      return listMethods(target)[index]
    }
}

function local_conflicts(context, objectId, key) {
    if (typeof key === "string" || typeof key === "number") {
      const c = context.values(objectId, key)
      return c.length > 1
    }
    return false
}

function map_get(target, key) {
    const { context, objectId, path, readonly, frozen } = target
    if (key === OBJECT_ID) return objectId
    if (key === READ_ONLY) return readonly
    if (key === FROZEN) return frozen
    if (key === STATE) return context;
    return valueAt(context, objectId, key, path, readonly)
    //const value = context.value(objectId, key)
    //return am2js(value, context, path, key, readonly)
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
    //console.log("MAP.GET", key)
    if (key === Symbol.toStringTag) { return target[Symbol.toStringTag] }
    return map_get(target, key)
  },

  set (target, key, val) {
    let { context, objectId, path, readonly, frozen, conflicts } = target
    if (val && val[OBJECT_ID]) {
          throw new RangeError('Cannot create a reference to an existing document object')
    }
    if (key === FROZEN) {
      target.frozen = val
      return
    }
    conflicts = conflicts || local_conflicts(context, objectId, key)
    let [ value, datatype ] = import_value(val)
    if (map_get(target,key) === val && !conflicts) {
      return
    }
    if (frozen) {
      throw new RangeError("Attempting to use an outdated Automerge document")
    }
    if (readonly) {
      throw new RangeError(`Object property "${key}" cannot be modified`)
    }
    switch (datatype) {
      case "list":
        const list = context.set(objectId, key, LIST)
        const proxyList = listProxy(context, list, [ ... path, key ], readonly, conflicts);
        // FIXME use splice
        for (let i = 0; i < value.length; i++) {
          proxyList[i] = value[i]
        }
        break;
      case "map":
        //const map = context.make(objectId, key, "map")
        const map = context.set(objectId, key, MAP)
        const proxyMap = mapProxy(context, map, [ ... path, key ], readonly, conflicts);
        for (const key in value) {
          proxyMap[key] = value[key]
        }
        break;
      default:
        context.set(objectId, key, value, datatype)
    }
    /*
    switch (typeof value) {
      case 'object':
        if (value == null) {
          context.set(objectId, key, null, "null");
        } else if (value instanceof Uint) {
          context.set(objectId, key, value.value, "uint");
        } else if (value instanceof Int) {
          context.set(objectId, key, value.value, "int");
        } else if (value instanceof Float64) {
          context.set(objectId, key, value.value, "f64");
        } else if (value instanceof Counter) {
          context.set(objectId, key, value.value, "counter");
        } else if (value instanceof Date) {
          context.set(objectId, key, value.getTime(), "timestamp");
        } else if (value instanceof Uint8Array) {
          context.set(objectId, key, value, "bytes");
        } else if (value instanceof Array) {
          const childID = context.makeList(objectId, key)
          const child = listProxy(context, childID, [ ... path, key ]);
          // FIXME use splice
          for (const i = 0; i < value.length; i++) {
            child[i] = value[i]
          }
        } else {
          const childID = context.makeMap(objectId, key)
          const child = mapProxy(context, childID, [ ... path, key ]);
          for (const key in value) {
            child[key] = value[key]
          }
        }
        break;
      case 'boolean':
        context.set(objectId, key, value, "boolean");
        break;
      case 'number':
        if (Number.isInteger(value)) {
          context.set(objectId, key, value, "int");
        } else {
          context.set(objectId, key, value, "f64");
        }
        break;
      case 'string':
        context.set(objectId, key, value);
        break;
      default:
        throw new RangeError(`cant handle value of type "${typeof value}"`)
    }
    */
    return true
  },

  deleteProperty (target, key) {
    const { context, objectId, path, readonly, frozen, conflicts } = target
    if (readonly) {
      throw new RangeError(`Object property "${key}" cannot be modified`)
    }
    context.del(objectId, key)
    return true
  },

  has (target, key) {
    //console.log("has",key);
    const value = map_get(target, key)
    return value !== undefined
  },

  getOwnPropertyDescriptor (target, key) {
    //console.log("getOwnPropertyDescriptor",key);
    const { context, objectId } = target
    const value = map_get(target, key)
    if (typeof value !== 'undefined') {
      return {
        configurable: true, enumerable: true, value
      }
    }
  },

  ownKeys (target) {
    const { context, objectId } = target
    return context.keys(objectId)
  },
}

function splice(target, index, del, vals) {
    const [context, objectId, path, readonly, frozen, conflicts] = target
    index = parseListIndex(index)
    for (let val of vals) {
      if (val && val[OBJECT_ID]) {
            throw new RangeError('Cannot create a reference to an existing document object')
      }
    }
    if (frozen) {
      throw new RangeError("Attempting to use an outdated Automerge document")
    }
    if (readonly) {
      throw new RangeError(`Object property "${index}" cannot be modified`)
    }
    let result = []
    for (let i = 0; i < del; i++) {
      let value = valueAt(context, objectId, index, path, readonly, conflicts)
      result.push(value)
      context.del(objectId, index)
    }
    const values = vals.map((val) => import_value(val))
    for (let [value,datatype] of values) {
      switch (datatype) {
        case "list":
          const list = context.insert(objectId, index, LIST)
          const proxyList = listProxy(context, list, [ ... path, index ], readonly, conflicts);
          // FIXME use splice
          for (let i = 0; i < value.length; i++) {
            proxyList[i] = value[i]
          }
          break;
        case "map":
          const map = context.insert(objectId, index, MAP)
          const proxyMap = mapProxy(context, map, [ ... path, index ], readonly, conflicts);
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
}

const ListHandler = {
  get (target, index) {
    index = parseListIndex(index)
    //console.log("GET", index)
    if (index === Symbol.toStringTag) { return [][Symbol.toStringTag] }
    return list_get(target, index)
  },

  set (target, index, val) {
    let [context, objectId, path, readonly, frozen , conflicts ] = target
    //console.log("SET", index, val, objectId)
    //console.log("len", context.length(objectId))
    index = parseListIndex(index)
    if (val && val[OBJECT_ID]) {
      throw new RangeError('Cannot create a reference to an existing document object')
    }
    if (index === FROZEN) {
      target.frozen = val
      return
    }
    if (typeof index == "string") {
      throw new RangeError('list index must be a number')
    }
    conflicts = conflicts || local_conflicts(context, objectId, index)
    const [ value, datatype] = import_value(val)
    if (list_get(target,index) === val && !conflicts) {
      return
    }
    if (frozen) {
      throw new RangeError("Attempting to use an outdated Automerge document")
    }
    if (readonly) {
      throw new RangeError(`Object property "${index}" cannot be modified`)
    }
    switch (datatype) {
      case "list":
        const list = context.set(objectId, index, LIST)
        const proxyList = listProxy(context, list, [ ... path, index ], readonly, conflicts);
        // FIXME use splice
        for (let i = 0; i < value.length; i++) {
          proxyList[i] = value[i]
        }
        break;
      case "map":
        const map = context.set(objectId, index, MAP)
        const proxyMap = mapProxy(context, map, [ ... path, index ], readonly, conflicts);
        for (const key in value) {
          proxyMap[key] = value[key]
        }
        break;
      default:
        context.set(objectId, index, value, datatype)
    }
    return true
  },

  deleteProperty (target, index) {
    const [context, objectId, /* path, readonly, frozen */] = target
    index = parseListIndex(index)
    context.del(objectId, index)
    return true
  },

  has (target, key) {
    console.log("HAS",key);
    const [context, objectId, /* path, readonly, frozen */] = target
    key = parseListIndex(key)
    if (typeof key === 'number') {
      return key < context.length(objectId)
    }
    return key === 'length'
  },

  getOwnPropertyDescriptor (target, index) {
    const [context, objectId, path, readonly, frozen ] = target

    if (index === 'length') return {writable: true, value: context.length(objectId) }
    if (index === OBJECT_ID) return {configurable: false, enumerable: false, value: objectId}

    index = parseListIndex(index)

    let value = valueAt(context, objectId, index, path, readonly)
    return { configurable: true, enumerable: true, value }
  },

  ownKeys (target) {
    return ['length']
  }
}

function mapProxy(context, objectId, path, readonly, conflicts) {
  return new Proxy({context, objectId, path, readonly: !!readonly, frozen: false, conflicts}, MapHandler)
}

function listProxy(context, objectId, path, readonly, conflict) {
  readonly = !!readonly
  let frozen = false
  return new Proxy([context, objectId, path, readonly, frozen, conflict], ListHandler)
}

function rootProxy(context, readonly) {
  //context.instantiateObject = instantiateProxy
  return mapProxy(context, AutomergeWASM.root(), [], readonly, false)
}

function listMethods(target) {
  const [context, objectId, path, readonly, frozen, conflicts] = target
  const methods = {
    deleteAt(index, numDelete) {
      context.del(objectId, parseListIndex(index))
      return this
    },

    fill(val, start, end) {
      let list = context.getObject(objectId)
      let [value, datatype] = valueAt(context, objectId, index, path, readonly)
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
      splice(target, parseListIndex(index), 0, values)
      return this
    },

    pop() {
      let length = context.length(objectId)
      if (length == 0) {
        return undefined
      }
      let last = valueAt(context, objectId, length - 1, path, readonly, conflicts)
      context.del(objectId, length - 1)
      return last
    },

    push(...values) {
      splice(target, context.length(objectId), 0, values)
      return context.length(objectId)
    },

    shift() {
      if (context.length(objectId) == 0) return
      const first = valueAt(context, objectId, 0, path, readonly, conflicts)
      context.del(objectId, 0)
      return first
    },

    splice(start, deleteCount, ...values) {
      return splice(target, start, deleteCount, values)
    },

    unshift(...values) {
      splice(target, 0, 0, values)
      return context.length(objectId)
    },

    entries() {
      let i = 0;
      const iterator = {
        next: () => {
          //let rawVal = context.value(objectId, i)
          //let value = am2js(rawVal, context, path, i, readonly)
          let value = valueAt(context, objectId, i, path, readonly, conflicts)
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
      const iterator = {
        next: () => {
          // TODO - use len not value
          let value = valueAt(context, objectId, i, path, readonly, conflicts)
          if (value === undefined) {
            return { value: undefined, done: true }
          } else {
            return { value: i, done: false }
          }
        }
      }
      return iterator
    },

    values() {
      console.log("values");
      let i = 0;
      const iterator = {
        next: () => {
          let value = valueAt(context, objectId, i, path, readonly, conflicts)
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
  for (let method of ['concat', 'every', 'filter', 'find', 'findIndex', 'forEach', 'includes',
                      'join', 'lastIndexOf', 'map', 'reduce', 'reduceRight',
                      'slice', 'some', 'toLocaleString', 'toString']) {
    methods[method] = (...args) => {
      //const list = context.getObject(objectId)
      // .map((item, index) => context.getObjectField(path, objectId, index))


      const list = []
      while (true) {
        //let rawVal = context.value(objectId, list.length)
        //let value = am2js(rawVal, context, path, list.length, readonly)
        let value =  valueAt(context, objectId, list.length, path, readonly, conflicts)
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

module.exports = { rootProxy, listProxy, mapProxy }
