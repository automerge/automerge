
let AutomergeWASM = require("automerge-wasm")

let { rootProxy  } = require("./proxies")
let { Counter  } = require("./counter")
let { Int, Uint, Float64  } = require("./numbers")
let { STATE, FROZEN  } = require("./constants")

function init() {
  const state = AutomergeWASM.init()
  return rootProxy(state, true);
}

function clone(doc) {
  const state = doc[STATE].clone()
  return rootProxy(state, true);
}

function free(doc) {
  return doc[STATE].free()
}

function from(data) {
    let doc1 = init()
    let doc2 = change(doc1, (d) => {
      //Object.keys(data).forEach(key => d[key] = data[key])
      Object.assign(d, data)
    })
    return doc2
}

function change(doc, func) {
  const state = doc[STATE]
  // FREEZE
  state.begin()
  try {
    let root = rootProxy(state);
    func(root)
    state.commit()
    return rootProxy(state, true);
  } catch (e) {
    state.rollback()
    throw e 
  }
}

function emptyChange() {
}

function load() {
}

function save() {
}

function merge() {
}

function getChanges() {
}

function getAllChanges() {
}

function applyChanges() {
}

function encodeChange() {
}

function decodeChange() {
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
  switch (datatype) {
    case "map":
      let val = {}
      console.log("mapkeys",doc.keys(value))
      for (const key of doc.keys(value)) {
        let subval = doc.value(value,key)
        val[key] = ex(doc, subval[0], subval[1])
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
    encodeChange, decodeChange, equals, getHistory, uuid,
    generateSyncMessage, receiveSyncMessage, initSyncState,
    toJS, dump, Counter, Int, Uint, Float64
}

// depricated
// Frontend, setDefaultBackend, Backend

// more...
/*
for (let name of ['getObjectId', 'getObjectById', 'getActorId',
       'setActorId', 'getConflicts', 'getLastLocalChange',
       'Text', 'Table', 'Counter', 'Observable', 'Int', 'Uint', 'Float64']) {
    module.exports[name] = Frontend[name]
}
*/
