
let AutomergeWASM = require("automerge-wasm")

let { rootProxy  } = require("./proxies")

function init() {
  return AutomergeWASM.init()
}

function clone(doc) {
  return doc.clone()
}

function free(doc) {
  return doc.free()
}

function from() {
}

function change(doc, func) {
  //console.log("BEGIN")
  doc.begin()
  try {
    let root = rootProxy(doc);
    func(root)
    //console.log("COMMIT")
    doc.commit()
    return doc
  } catch (e) {
    console.log("ROLLBACK", e)
    doc.rollback()
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

function dump(doc, datatype, value) {
  switch (datatype) {
    case "map":
      let val = {}
      for (const key of doc.keys(value)) {
        let subval = doc.value(value,key)
        //console.log(`dump key=${key} subval=${subval}`)
        val[key] = dump(doc, subval[0], subval[1])
      }
      return val
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
  return dump(doc, "map", "_root")
}

module.exports = {
    init, from, change, emptyChange, clone, free,
    load, save, merge, getChanges, getAllChanges, applyChanges,
    encodeChange, decodeChange, equals, getHistory, uuid,
    generateSyncMessage, receiveSyncMessage, initSyncState,
    toJS,
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
