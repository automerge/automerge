
const AutomergeWASM = require("automerge-wasm")
const { Int, Uint, Float64 } = require("./numbers");
const { Counter } = require("./counter");
const { STATE, FROZEN, OBJECT_ID, READ_ONLY } = require("./constants")

function parseListIndex(key) {
  if (typeof key === 'string' && /^[0-9]+$/.test(key)) key = parseInt(key, 10)
  if (typeof key !== 'number') {
    throw new TypeError('A list index must be a number, but you passed ' + JSON.stringify(key))
  }
  if (key < 0 || isNaN(key) || key === Infinity || key === -Infinity) {
    throw new RangeError('A list index must be positive, but you passed ' + key)
  }
  return key
}

function valueAt(context, objectId, prop, path, readonly) {
      let value = context.value(objectId, prop)
      const datatype = value[0]
      const val = value[1]
      switch (datatype) {
        case undefined: return;
        case "map": return mapProxy(context, val, [ ... path, prop ], readonly);
        case "list": return listProxy(context, val, [ ... path, prop ], readonly);
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
    const [context, objectId, path, readonly, frozen] = target
    if (index === OBJECT_ID) return objectId
    if (index === READ_ONLY) return readonly
    if (index === FROZEN) return frozen
    if (index === STATE) return context;
    if (index === 'length') return context.length(objectId);
    if (typeof index === 'string' && /^[0-9]+$/.test(index)) {
      index = parseListIndex(index)
    }
    if (typeof index === 'number') {
      return valueAt(context, objectId, index, path, readonly)
    } else {
      return listMethods(target)[index]
    }
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
          //context.set(objectId, key, value.value, "uint");
        } else if (value instanceof Int) {
          return [ value.value, "int" ]
          //context.set(objectId, key, value.value, "int");
        } else if (value instanceof Float64) {
          return [ value.value, "f64" ]
          //context.set(objectId, key, value.value, "f64");
        } else if (value instanceof Counter) {
          return [ value.value, "counter" ]
          //context.set(objectId, key, value.value, "counter");
        } else if (value instanceof Date) {
          return [ value.getTime(), "timestamp" ]
          //context.set(objectId, key, value.getTime(), "timestamp");
        } else if (value instanceof Uint8Array) {
          return [ value, "bytes" ]
          //context.set(objectId, key, value, "bytes");
        } else if (value instanceof Array) {
          return [ value, "list" ]
          /*
          const childID = context.makeList(objectId, key)
          const child = listProxy(context, childID, [ ... path, key ]);
          // FIXME use splice
          for (const i = 0; i < value.length; i++) {
            child[i] = value[i]
          }
          */
        } else {
          return [ value, "map" ]
          /*
          const childID = context.makeMap(objectId, key)
          const child = mapProxy(context, childID, [ ... path, key ]);
          for (const key in value) {
            child[key] = value[key]
          }
          */
        }
        break;
      case 'boolean':
        return [ value, "boolean" ]
        //context.set(objectId, key, value, "boolean");
        //break;
      case 'number':
        if (Number.isInteger(value)) {
          return [ value, "int" ]
         // context.set(objectId, key, value, "int");
        } else {
          return [ value, "f64" ]
          //context.set(objectId, key, value, "f64");
        }
        break;
      case 'string':
        return [ value ]
        //context.set(objectId, key, value);
        break;
      default:
        throw new RangeError(`cant handle value of type "${typeof value}"`)
    }
}

const MapHandler = {
  get (target, key) {
    if (key === Symbol.toStringTag) { return target[Symbol.toStringTag] }
    return map_get(target, key)
  },

  set (target, key, val) {
    //console.log("set",key);
    const { context, objectId, path, readonly, frozen } = target
    if (frozen) {
      throw new RangeError("Attempting to use an outdated Automerge document")
    }
    if (key === FROZEN) {
      target.frozen = val
      return
    }
    if (readonly) {
      throw new RangeError(`Object property "${key}" cannot be modified`)
    }
    let [ value, datatype] = import_value(val)
    switch (datatype) {
      case "list":
        const list = context.makeList(objectId, key)
        const proxyList = listProxy(context, list, [ ... path, key ]);
        // FIXME use splice
        for (let i = 0; i < value.length; i++) {
          proxyList[i] = value[i]
        }
        break;
      case "map":
        const map = context.makeMap(objectId, key)
        const proxyMap = mapProxy(context, map, [ ... path, key ]);
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
    //console.log("ownKeys");
    const { context, objectId } = target
    return context.keys(objectId)
  },
}

const ListHandler = {
  get (target, key) {
    if (key === Symbol.toStringTag) { return [][Symbol.toStringTag] }
    return list_get(target, key)
  },

  set (target, index, val) {
    const [context, objectId, path, readonly, frozen ] = target
    if (frozen) {
      throw new RangeError("Attempting to use an outdated Automerge document")
    }
    if (index === FROZEN) {
      target.frozen = val
      return
    }
    if (readonly) {
      throw new RangeError(`Object property "${index}" cannot be modified`)
    }
    const [ value, datatype] = import_value(val)
    switch (datatype) {
      case "list":
        const list = context.makeList(objectId, index)
        const proxyList = listProxy(context, list, [ ... path, index ]);
        // FIXME use splice
        for (const i = 0; i < value.length; i++) {
          proxyList[i] = value[i]
        }
        break;
      case "map":
        const map = context.makeMap(objectId, index)
        const proxyMap = mapProxy(context, map, [ ... path, index ]);
        for (const key in value) {
          proxyMap[key] = value[key]
        }
        break;
      default:
        context.setAt(objectId, parseListIndex(index), value, datatype)
    }
    return true
  },

  deleteProperty (target, key) {
    const [context, objectId, /* path, readonly, frozen */] = target
    //context.splice(path, parseListIndex(key), 1, [])
    return true
  },

  has (target, key) {
    const [context, objectId, /* path, readonly, frozen */] = target
    if (typeof key === 'string' && /^[0-9]+$/.test(key)) {
      return parseListIndex(key) < context.length(objectId)
    }
    return key === 'length'
  },

  getOwnPropertyDescriptor (target, index) {
    const [context, objectId, path, readonly, frozen ] = target

    if (index === 'length') return {writable: true, value: context.length(objectId) }
    //if (index === OBJECT_ID) return {configurable: false, enumerable: false, value: objectId}

    if (typeof index === 'string' && /^[0-9]+$/.test(index)) {
      index = parseListIndex(index)
    }
    //if (index < object.length) {
      //let rawVal = context.value(objectId, index);
      //let value = am2js(rawVal, context, path, index, readonly)
    let value = valueAt(context, objectId, index, path, readonly)
    return { configurable: true, enumerable: true, value }
    //}
  },

  ownKeys (target) {
    //const [context, objectId, /* path, readonly, frozen */] = target
    //let keys = ['length']
    //return keys
    return ['length']
  }
}

function mapProxy(context, objectId, path, readonly) {
  return new Proxy({context, objectId, path, readonly: !!readonly, frozen: false}, MapHandler)
}

function listProxy(context, objectId, path, readonly) {
  readonly = !!readonly
  let frozen = false
  return new Proxy([context, objectId, path, readonly, frozen], ListHandler)
}

function rootProxy(context, readonly) {
  //context.instantiateObject = instantiateProxy
  return mapProxy(context, AutomergeWASM.root(), [], readonly)
}

function listMethods(target) {
  const [context, objectId, path, readonly, frozen] = target
  const methods = {
    deleteAt(index, numDelete) {
      console.log("DELETE AT");
      context.splice(path, parseListIndex(index), numDelete || 1, [])
      return this
    },

    fill(value, start, end) {
      console.log("FILL");
      let list = context.getObject(objectId)
      for (let index = parseListIndex(start || 0); index < parseListIndex(end || list.length); index++) {
        context.setListIndex(path, index, value)
      }
      return this
    },

    indexOf(o, start = 0) {
      console.log("INDEX OF");
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
      console.log("INSERT AT");
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
      console.log("SPLICE");
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
          let value = valueAt(context, objectId, i, path, readonly)
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
          let value = valueAt(context, objectId, i, path, readonly)
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
          //let rawVal = context.value(objectId, i)
          //let value = am2js(rawVal, context, path, i, readonly)
          let value = valueAt(context, objectId, i, path, readonly)
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
        let value =  valueAt(context, objectId, list.length, path, readonly)
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

module.exports = { rootProxy } 
