
const AutomergeWASM = require("automerge-wasm")
const { Int, Uint, Float64 } = require("./numbers");
const { Counter } = require("./counter");
const { STATE, FROZEN, OBJECT_ID, READ_ONLY } = require("./constants")

function map_get(target, key) {
    const { context, objectId, path, readonly, frozen } = target
    if (key === OBJECT_ID) return objectId
    if (key === READ_ONLY) return readonly
    if (key === FROZEN) return frozen
    //if (key === STATE) return {actorId: context.actorId}
    if (key === STATE) return context;
    const value = context.value(objectId, key)
    const datatype = value[0]
    const val = value[1]
    switch (datatype) {
      case undefined: return;
      case "map": return mapProxy(context, val, [ ... path, key ], readonly);
      //case "list":
      //case "table":
      //case "text":
      //case "bytes":
      //case "cursor":
      //case "timestamp":
      //case "counter":
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

const MapHandler = {
  get (target, key) {
    //console.log("get",key);
    if (key === Symbol.toStringTag) { return target[Symbol.toStringTag] }
    return map_get(target, key)
  },

  set (target, key, value) {
    //console.log("set",key);
    const { context, objectId, path, readonly, frozen } = target
    if (frozen) {
      throw new RangeError("Attempting to use an outdated Automerge document")
    }
    if (key === FROZEN) {
      target.frozen = value
      return
    }
    if (readonly) {
      throw new RangeError(`Object property "${key}" cannot be modified`)
    }
    switch (typeof value) {
      case 'object':
        if (value == null) {
          context.set(objectId, key, null, "null");
        } else if (value instanceof Array) {
          throw new RangeError("set array value unsupported");
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
    const [context, objectId, path] = target
    if (key === Symbol.iterator) return context.getObject(objectId)[Symbol.iterator]
    if (key === OBJECT_ID) return objectId
    if (key === CHANGE) return context
    if (key === 'length') return context.getObject(objectId).length
    if (typeof key === 'string' && /^[0-9]+$/.test(key)) {
      return context.getObjectField(path, objectId, parseListIndex(key))
    }
    return listMethods(context, objectId, path)[key]
  },

  set (target, key, value) {
    const [context, /* objectId */, path] = target
    context.setListIndex(path, parseListIndex(key), value)
    return true
  },

  deleteProperty (target, key) {
    const [context, /* objectId */, path] = target
    context.splice(path, parseListIndex(key), 1, [])
    return true
  },

  has (target, key) {
    const [context, objectId, /* path */] = target
    if (typeof key === 'string' && /^[0-9]+$/.test(key)) {
      return parseListIndex(key) < context.getObject(objectId).length
    }
    return ['length', OBJECT_ID, CHANGE].includes(key)
  },

  getOwnPropertyDescriptor (target, key) {
    const [context, objectId, /* path */] = target
    const object = context.getObject(objectId)

    if (key === 'length') return {writable: true, value: object.length}
    if (key === OBJECT_ID) return {configurable: false, enumerable: false, value: objectId}

    if (typeof key === 'string' && /^[0-9]+$/.test(key)) {
      const index = parseListIndex(key)
      if (index < object.length) return {
        configurable: true, enumerable: true,
        value: context.getObjectField(objectId, index)
      }
    }
  },

  ownKeys (target) {
    const [context, objectId, /* path */] = target
    const object = context.getObject(objectId)
    let keys = ['length']
    for (let key of Object.keys(object)) keys.push(key)
    return keys
  }
}

function mapProxy(context, objectId, path, readonly) {
  return new Proxy({context, objectId, path, readonly: !!readonly, frozen: false}, MapHandler)
}

function listProxy(context, objectId, path) {
  return new Proxy([context, objectId, path], ListHandler)
}

function rootProxy(context, readonly) {
  //context.instantiateObject = instantiateProxy
  return mapProxy(context, AutomergeWASM.root(), [], readonly)
}

module.exports = { rootProxy } 
