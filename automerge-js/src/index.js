
let AutomergeWASM = require("automerge-wasm")

let { rootProxy  } = require("./proxies")
let { Counter  } = require("./counter")
let { STATE, FROZEN  } = require("./constants")

function init() {
  const state = AutomergeWASM.init()
  //return rootProxy(state, true);
  return state
}

function clone(doc) {
  //const state = doc[STATE].clone()
  //return rootProxy(state, true);
  return doc.clone()
}

function free(doc) {
  //return doc[STATE].free()
  doc.free()
}

function from() {
}

function change(doc, func) {
  doc.begin()
  try {
    let root = rootProxy(doc);
    func(root)
    doc.commit()
    return doc
  } catch (e) {
    doc.rollback()
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
  doc.dump()
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
    case "counter":
      return new Counter(value)
    case "timestamp":
      return new Date(value)
    case "str":
    case "uint":
    case "int":
    case "bool":
      return value
    case "null":
      return null
    default:
      throw RangeError(`invalid datatype ${datatype}`)
  }
}

function toJS(doc) {
  return ex(doc, "map", "_root")
}

module.exports = {
    init, from, change, emptyChange, clone, free,
    load, save, merge, getChanges, getAllChanges, applyChanges,
    encodeChange, decodeChange, equals, getHistory, uuid,
    generateSyncMessage, receiveSyncMessage, initSyncState,
    toJS, dump, Counter
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
