const AutomergeWASM = require("automerge-wasm")
const uuid = require('./uuid')

let { rootProxy, listProxy, textProxy, mapProxy } = require("./proxies")
let { Counter  } = require("./counter")
let { Text } = require("./text")
let { Int, Uint, Float64  } = require("./numbers")
let { STATE, HEADS, OBJECT_ID, READ_ONLY, FROZEN  } = require("./constants")

function init(actor) {
  const state = AutomergeWASM.init(actor)
  return rootProxy(state, true);
}

function clone(doc) {
  const state = doc[STATE].clone()
  return rootProxy(state, true);
}

function free(doc) {
  return doc[STATE].free()
}

function from(data, actor) {
    let doc1 = init(actor)
    let doc2 = change(doc1, (d) => Object.assign(d, data))
    return doc2
}

function change(doc, options, callback) {
  if (callback === undefined) {
    // FIXME implement options
    callback = options
    options = {}
  }
  if (typeof options === "string") {
    options = { message: options }
  }
  if (doc === undefined || doc[STATE] === undefined || doc[OBJECT_ID] !== "_root") {
    throw new RangeError("must be the document root");
  }
  if (doc[FROZEN] === true) {
    throw new RangeError("Attempting to use an outdated Automerge document")
  }
  if (!!doc[HEADS] === true) {
    console.log("HEADS", doc[HEADS])
    throw new RangeError("Attempting to change an out of date document");
  }
  if (doc[READ_ONLY] === false) {
    throw new RangeError("Calls to Automerge.change cannot be nested")
  }
  const state = doc[STATE]
  const heads = state.getHeads()
  try {
    doc[HEADS] = heads
    doc[FROZEN] = true
    let root = rootProxy(state);
    callback(root)
    if (state.pendingOps() === 0) {
      doc[FROZEN] = false
      doc[HEADS] = undefined
      return doc
    } else {
      state.commit(options.message, options.time)
      return rootProxy(state, true);
    }
  } catch (e) {
    //console.log("ERROR: ",e)
    doc[FROZEN] = false
    doc[HEADS] = undefined
    state.rollback()
    throw e
  }
}

function emptyChange(doc, options) {
  if (options === undefined) {
    options = {}
  }
  if (typeof options === "string") {
    options = { message: options }
  }

  if (doc === undefined || doc[STATE] === undefined || doc[OBJECT_ID] !== "_root") {
    throw new RangeError("must be the document root");
  }
  if (doc[FROZEN] === true) {
    throw new RangeError("Attempting to use an outdated Automerge document")
  }
  if (doc[READ_ONLY] === false) {
    throw new RangeError("Calls to Automerge.change cannot be nested")
  }

  const state = doc[STATE]
  state.commit(options.message, options.time)
  return rootProxy(state, true);
}

function load(data, actor) {
  const state = AutomergeWASM.load(data, actor)
  return rootProxy(state, true);
}

function save(doc) {
  const state = doc[STATE]
  return state.save()
}

function merge(local, remote) {
  if (local[HEADS] === true) {
    throw new RangeError("Attempting to change an out of date document");
  }
  const localState = local[STATE]
  const heads = localState.getHeads()
  const remoteState = remote[STATE]
  const changes = localState.getChangesAdded(remoteState)
  localState.applyChanges(changes)
  local[HEADS] = heads
  return rootProxy(localState, true)
}

function getActorId(doc) {
  const state = doc[STATE]
  return state.getActorId()
}

function conflictAt(context, objectId, prop) {
      let values = context.values(objectId, prop)
      if (values.length <= 1) {
        return
      }
      let result = {}
      for (const conflict of values) {
        const datatype = conflict[0]
        const value = conflict[1]
        switch (datatype) {
          case "map":
            result[value] = mapProxy(context, value, [ prop ], true, true)
            break;
          case "list":
            result[value] = listProxy(context, value, [ prop ], true, true)
            break;
          case "text":
            result[value] = textProxy(context, value, [ prop ], true, true)
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
            result[conflict[2]] = value
            break;
          case "counter":
            result[conflict[2]] = new Counter(value)
            break;
          case "timestamp":
            result[conflict[2]] = new Date(value)
            break;
          default:
            throw RangeError(`datatype ${datatype} unimplemented`)
        }
      }
      return result
}

function getConflicts(doc, prop) {
  const state = doc[STATE]
  const objectId = doc[OBJECT_ID]
  return conflictAt(state, objectId, prop)
}

function getLastLocalChange(doc) {
  const state = doc[STATE]
  return state.getLastLocalChange()
}

function getObjectId(doc) {
  return doc[OBJECT_ID]
}

function getChanges(oldState, newState) {
  const o = oldState[STATE]
  const n = newState[STATE]
  const heads = oldState[HEADS]
  return n.getChanges(heads || o.getHeads())
}

function getAllChanges(doc) {
  const state = doc[STATE]
  return state.getChanges([])
}

function applyChanges(doc, changes) {
  if (doc === undefined || doc[STATE] === undefined || doc[OBJECT_ID] !== "_root") {
    throw new RangeError("must be the document root");
  }
  if (doc[FROZEN] === true) {
    throw new RangeError("Attempting to use an outdated Automerge document")
  }
  if (doc[READ_ONLY] === false) {
    throw new RangeError("Calls to Automerge.change cannot be nested")
  }
  const state = doc[STATE]
  const heads = state.getHeads()
  state.applyChanges(changes)
  doc[HEADS] = heads
  return [rootProxy(state, true)];
}

function getHistory(doc) {
  const actor = getActorId(doc)
  const history = getAllChanges(doc)
  return history.map((change, index) => ({
      get change () {
        return decodeChange(change)
      },
      get snapshot () {
        const [state] = applyChanges(init(), history.slice(0, index + 1))
        return state
      }
    })
  )
}

function equals() {
  if (!isObject(val1) || !isObject(val2)) return val1 === val2
  const keys1 = Object.keys(val1).sort(), keys2 = Object.keys(val2).sort()
  if (keys1.length !== keys2.length) return false
  for (let i = 0; i < keys1.length; i++) {
    if (keys1[i] !== keys2[i]) return false
    if (!equals(val1[keys1[i]], val2[keys2[i]])) return false
  }
  return true
}

function encodeSyncMessage(msg) {
  return AutomergeWASM.encodeSyncMessage(msg)
}

function decodeSyncMessage(msg) {
  return AutomergeWASM.decodeSyncMessage(msg)
}

function encodeSyncState(state) {
  return AutomergeWASM.encodeSyncState(state)
}

function decodeSyncState() {
  return AutomergeWASM.decodeSyncState(state)
}

function generateSyncMessage(doc, syncState) {
  const state = doc[STATE]
  const result = state.generateSyncMessage(syncState)
  return result
}

function receiveSyncMessage(doc, syncState, message) {
  if (doc === undefined || doc[STATE] === undefined || doc[OBJECT_ID] !== "_root") {
    throw new RangeError("must be the document root");
  }
  if (doc[FROZEN] === true) {
    throw new RangeError("Attempting to use an outdated Automerge document")
  }
  if (!!doc[HEADS] === true) {
    throw new RangeError("Attempting to change an out of date document");
  }
  if (doc[READ_ONLY] === false) {
    throw new RangeError("Calls to Automerge.change cannot be nested")
  }
  const state = doc[STATE]
  const heads = state.getHeads()
  const newSyncState = state.receiveSyncMessage(syncState, message)
  doc[HEADS] = heads
  return [rootProxy(state, true), newSyncState, null];
}

function initSyncState() {
  return AutomergeWASM.initSyncState(change)
}

function encodeChange(change) {
  return AutomergeWASM.encodeChange(change)
}

function decodeChange(data) {
  return AutomergeWASM.decodeChange(data)
}

function encodeSyncMessage(change) {
  return AutomergeWASM.encodeSyncMessage(change)
}

function decodeSyncMessage(data) {
  return AutomergeWASM.decodeSyncMessage(data)
}

function encodeSyncState(change) {
  return AutomergeWASM.encodeSyncState(change)
}

function decodeSyncState(data) {
  return AutomergeWASM.decodeSyncState(data)
}

function getMissingDeps(doc, heads) {
  const state = doc[STATE]
  if (!heads) {
    heads = []
  }
  return state.getMissingDeps(heads)
}

function getHeads(doc) {
  const state = doc[STATE]
  return doc[HEADS] || state.getHeads()
}

function dump(doc) {
  const state = doc[STATE]
  state.dump()
}

function toJS(doc) {
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
      return doc.map((a) => toJS(a))
    }
    let tmp = {}
    for (index in doc) {
      tmp[index] = toJS(doc[index])
    }
    return tmp
  } else {
    return doc
  }
}

module.exports = {
    init, from, change, emptyChange, clone, free,
    load, save, merge, getChanges, getAllChanges, applyChanges,
    getLastLocalChange, getObjectId, getActorId, getConflicts,
    encodeChange, decodeChange, equals, getHistory, getHeads, uuid,
    generateSyncMessage, receiveSyncMessage, initSyncState,
    decodeSyncMessage, encodeSyncMessage, decodeSyncState, encodeSyncState,
    getMissingDeps,
    dump, Text, Counter, Int, Uint, Float64, toJS,
}

// depricated
// Frontend, setDefaultBackend, Backend

// more...
/*
for (let name of ['getObjectId', 'getObjectById',
       'setActorId',
       'Text', 'Table', 'Counter', 'Observable' ]) {
    module.exports[name] = Frontend[name]
}
*/
