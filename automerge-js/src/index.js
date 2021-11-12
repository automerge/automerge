
let AutomergeWASM = require("automerge-wasm")
const { encodeChange, decodeChange } = require('./columnar')

let { rootProxy  } = require("./proxies")
let { Counter  } = require("./counter")
let { Int, Uint, Float64  } = require("./numbers")
let { STATE, OBJECT_ID, READ_ONLY, FROZEN  } = require("./constants")

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
  if (doc === undefined || doc[STATE] === undefined || doc[OBJECT_ID] !== "_root") {
    throw new RangeError("must be the document root");
  }
  if (doc[FROZEN] === true) {
    throw new RangeError("Attempting to use an outdated Automerge document")
  }
  if (doc[READ_ONLY] === false) {
    throw new RangeError("Calls to Automerge.change cannot be nested")
  }
  const state = doc[STATE].clone()
  state.begin()
  try {
    doc[FROZEN] = true
    let root = rootProxy(state);
    callback(root)
    if (state.pending_ops() === 0) {
      state.rollback()
      doc[FROZEN] = false
      return doc
    } else {
      state.commit()
      return rootProxy(state, true);
    }
  } catch (e) {
    doc[FROZEN] = false
    state.rollback()
    throw e
  }
}

function emptyChange(doc, options) {
  // FIXME implement options

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
  state.begin()
  state.commit()
  return rootProxy(state, true);
}

function load() {
}

function save(doc) {
  const state = doc[STATE]
  return state.save()
}

function merge(local, remote) {
  const localState = local[STATE]
  const remoteState = remote[STATE]
  const changes = localState.getChangesAdded(remoteState)
  localState.applyChanges(changes)
  return rootProxy(localState, true);
}

function getActorId(doc) {
  const state = doc[STATE]
  return state.getActorId()
}

function getLastLocalChange(doc) {
  const state = doc[STATE]
  return state.getLastLocalChange()
}

function getObjectId(doc) {
  return doc[OBJECT_ID]
}

function getChanges(doc, heads) {
  const state = doc[STATE]
  return state.getChanges(heads)
}

function getAllChanges(doc) {
  return getChanges(doc, [])
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
  state.applyChanges(changes)
  return [rootProxy(state, true)];
}

function equals() {
}

function getHistory() {
}

function uuid() {
}

function generateSyncMessage() {
}

function receiveSyncMessage() {
}

function initSyncState() {
}

function dump(doc) {
  const state = doc[STATE]
  state.dump()
}

function ex(doc, datatype, value) {
  let val;
  switch (datatype) {
    case "map":
      val = {}
      for (const key of doc.keys(value)) {
        let subval = doc.value(value,key)
        val[key] = ex(doc, subval[0], subval[1])
      }
      return val
    case "list":
      val = []
      let len = doc.length(value);
      for (let i = 0; i < len; i++) {
        let subval = doc.value(value, i)
        val.push(ex(doc, subval[0], subval[1]))
      }
      return val
    case "bytes":
      return value
    case "counter":
      return new Counter(value)
    case "timestamp":
      return new Date(value)
    case "str":
    case "uint":
    case "int":
    case "f64":
    case "boolean":
      return value
    case "null":
      return null
    default:
      throw RangeError(`invalid datatype ${datatype}`)
  }
}

function toJS(doc) {
  const state = doc[STATE].clone()
  return ex(state, "map", "_root")
}

module.exports = {
    init, from, change, emptyChange, clone, free,
    load, save, merge, getChanges, getAllChanges, applyChanges,
    getLastLocalChange, getObjectId, getActorId,
    encodeChange, decodeChange, equals, getHistory, uuid,
    generateSyncMessage, receiveSyncMessage, initSyncState,
    toJS, dump, Counter, Int, Uint, Float64
}

// depricated
// Frontend, setDefaultBackend, Backend

// more...
/*
for (let name of ['getObjectId', 'getObjectById',
       'setActorId', 'getConflicts',
       'Text', 'Table', 'Counter', 'Observable' ]) {
    module.exports[name] = Frontend[name]
}
*/
