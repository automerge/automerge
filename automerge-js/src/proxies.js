
const AutomergeWASM = require("automerge-wasm")
const { Int, Uint, Float64 } = require("./numbers");
const { Counter } = require("./counter");
const { STATE, FROZEN, OBJECT_ID, READ_ONLY } = require("./constants")

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
      let values = context.conflicts(objectId, prop)
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
      const c = context.conflicts(objectId, key)
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
    //console.log("MAP.SET", key, val)
    let { context, objectId, path, readonly, frozen, conflicts } = target
    let [ value, datatype] = import_value(val)
    if (key === FROZEN) {
      target.frozen = val
      return
    }
    conflicts = conflicts || local_conflicts(context, objectId, key)
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
        const list = context.make(objectId, key, "list")
        const proxyList = listProxy(context, list, [ ... path, key ], readonly, conflicts);
        // FIXME use splice
        for (let i = 0; i < value.length; i++) {
          proxyList[i] = value[i]
        }
        break;
      case "map":
        const map = context.make(objectId, key, "map")
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
    const { context, path, readonly } = target
    if (readonly) {
      throw new RangeError(`Object property "${key}" cannot be modified`)
    }
    context.deleteMapKey(obectId, key)
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

const ListHandler = {
  get (target, index) {
    index = parseListIndex(index)
    //console.log("GET", index)
    if (index === Symbol.toStringTag) { return [][Symbol.toStringTag] }
    return list_get(target, index)
  },

  set (target, index, val) {
    let [context, objectId, path, readonly, frozen , conflicts ] = target
    //console.log("SET", index, val)
    index = parseListIndex(index)
    const [ value, datatype] = import_value(val)
    if (index === FROZEN) {
      target.frozen = val
      return
    }
    conflicts = conflicts || local_conflicts(context, objectId, index)
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
        const list = context.makeAt(objectId, index, "list")
        const proxyList = listProxy(context, list, [ ... path, index ], readonly, conflicts);
        // FIXME use splice
        for (let i = 0; i < value.length; i++) {
          proxyList[i] = value[i]
        }
        break;
      case "map":
        const map = context.makeAt(objectId, index, "map")
        const proxyMap = mapProxy(context, map, [ ... path, index ], readonly, conflicts);
        for (const key in value) {
          proxyMap[key] = value[key]
        }
        break;
      default:
        context.setAt(objectId, parseListIndex(index), value, datatype)
    }
    return true
  },

  deleteProperty (target, index) {
    const [context, objectId, /* path, readonly, frozen */] = target
    index = parseListIndex(index)
    //context.splice(path, parseListIndex(key), 1, [])
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
      context.splice(path, parseListIndex(index), numDelete || 1, [])
      return this
    },

    fill(value, start, end) {
      let list = context.getObject(objectId)
      for (let index = parseListIndex(start || 0); index < parseListIndex(end || list.length); index++) {
        context.setListIndex(path, index, value)
      }
      return this
    },

    indexOf(o, start = 0) {
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
        return context.getObject(objectId).indexOf(o, start)
      }
    },

    insertAt(index, ...values) {
      context.splice(path, parseListIndex(index), 0, values)
      return this
    },

    pop() {
      console.log("POP");
      let list = context.getObject(objectId)
      if (list.length == 0) return
      const last = context.getObjectField(path, objectId, list.length - 1)
      context.splice(path, list.length - 1, 1, [])
      return last
    },

    push(...values) {
      console.log("PUSH");
      let list = context.getObject(objectId)
      context.splice(path, list.length, 0, values)
      // need to getObject() again because the list object above may be immutable
      return context.getObject(objectId).length
    },

    shift() {
      console.log("SHIFT");
      let list = context.getObject(objectId)
      if (list.length == 0) return
      const first = context.getObjectField(path, objectId, 0)
      context.splice(path, 0, 1, [])
      return first
    },

    splice(start, deleteCount, ...values) {
      let list = context.getObject(objectId)
      start = parseListIndex(start)
      if (deleteCount === undefined || deleteCount > list.length - start) {
        deleteCount = list.length - start
      }
      const deleted = []
      for (let n = 0; n < deleteCount; n++) {
        deleted.push(context.getObjectField(path, objectId, start + n))
      }
      context.splice(path, start, deleteCount, values)
      return deleted
    },

    unshift(...values) {
      console.log("UNSHIFT");
      context.splice(path, 0, 0, values)
      return context.getObject(objectId).length
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
