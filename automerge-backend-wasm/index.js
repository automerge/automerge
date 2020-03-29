let Backend = require("./pkg")
let { fromJS, List } = require('immutable')
let util = require('util')

const { encodeChange, decodeChange } = require('./columnar')

function decodeChanges(binaryChanges) {
  let decoded = []
  for (let binaryChange of binaryChanges) {
    if (!(binaryChange instanceof Uint8Array)) {
      throw new RangeError(`Unexpected type of change: ${binaryChange}`)
    }
    for (let change of decodeChange(binaryChange)) decoded.push(change)
  }
  //console.log("CHANGES",decoded);
  return fromJS(decoded)
}


function toJS(obj) {
  if (List.isList(obj)) {
    return obj.toJS()
  }
  return obj
}

let init = () => {
  return { state: Backend.State.new(), clock: {}, frozen: false };
}

let clean = (backend) => {
  if (backend.frozen) {
    let state = backend.state.forkAt(backend.clock)
    backend.state = state
    backend.clock = state.getClock()
    backend.frozen = false
  }
  return backend.state
}

let mutate = (oldBackend,fn) => {
  let state = clean(oldBackend)
  let result = fn(state)
  oldBackend.frozen = true
  let newBackend = { state, clock: state.getClock(), frozen: false };
  return [ newBackend, result ]
}

let applyChanges = (backend,changes) => {
  return mutate(backend, (b) => b.applyChanges(decodeChanges(changes)));
}

let loadChanges = (backend,changes) => {
  let [newState,_] = mutate(backend, (b) => b.loadChanges(decodeChanges(changes)));
  return newState
}

let applyLocalChange = (backend,request) => {
  //console.log("LOCAL REQUEST",request)
  return mutate(backend, (b) => b.applyLocalChange(toJS(request)));
}

let merge = (backend1,backend2) => {
  return mutate(backend1, (b) => b.merge(clean(backend2)));
}

let getClock = (backend) => {
  return fromJS(backend.clock);
}

let getHistory = (backend) => {
  // TODO: I cant fromJS here b/c transit screws it up
  let history = clean(backend).getHistory();
  return history
}

let getUndoStack = (backend) => {
  let stack = clean(backend).getUndoStack();
  return fromJS(stack)
}

let getRedoStack = (backend) => {
  let stack = clean(backend).getRedoStack();
  return fromJS(stack)
}

let getPatch = (backend) => clean(backend).getPatch()
let getChanges = (backend,other) => clean(backend).getChanges(clean(other)).map(encodeChange)
let getChangesForActor = (backend,actor) => clean(backend).getChangesForActor(actor).map(encodeChange)
let getMissingChanges = (backend,clock) => clean(backend).getMissingChanges(clock).map(encodeChange)
let getMissingDeps = (backend) => clean(backend).getMissingDeps()
let _elemIds = (backend,obj_id) => clean(backend)._elemIds(obj_id)

module.exports = {
  init, applyChanges, applyLocalChange, getPatch,
  getChanges, getChangesForActor, getMissingChanges, getMissingDeps, merge, getClock,
  getHistory, getUndoStack, getRedoStack, loadChanges, _elemIds
}
