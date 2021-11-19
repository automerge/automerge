
let AutomergeWASM = require("automerge-wasm")
const { encodeChange, decodeChange } = require('./columnar')

let { rootProxy, listProxy, mapProxy } = require("./proxies")
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
  const state = doc[STATE].clone()
  state.begin(options.message, options.time)
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
    //console.log("ERROR: ",e)
    doc[FROZEN] = false
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
  state.begin(options.message, options.time)
  state.commit()
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
  const localState = local[STATE].clone()
  const remoteState = remote[STATE]
  const changes = localState.getChangesAdded(remoteState)
  localState.applyChanges(changes)
  return rootProxy(localState, true);
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
          //case "table":
          //case "text":
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
  return n.getChanges(o.getHeads())
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
  const state = doc[STATE].clone()
  state.applyChanges(changes)
  return [rootProxy(state, true)];
}

function equals() {
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

function uuid() {
}

function generateSyncMessage() {
}

function receiveSyncMessage() {
}

function initSyncState() {
}

function getMissingDeps(doc, heads) {
  const state = doc[STATE]
  if (!heads) {
    heads = []
  }
  return state.getMissingDeps(heads)
}

function dump(doc) {
  const state = doc[STATE]
  state.dump()
}

module.exports = {
    init, from, change, emptyChange, clone, free,
    load, save, merge, getChanges, getAllChanges, applyChanges,
    getLastLocalChange, getObjectId, getActorId, getConflicts,
    encodeChange, decodeChange, equals, getHistory, uuid,
    generateSyncMessage, receiveSyncMessage, initSyncState,
    getMissingDeps,
    dump, Counter, Int, Uint, Float64
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
