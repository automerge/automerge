let Backend = require("./automerge_backend_wasm")

const { encodeChange, decodeChange } = require('automerge/backend/columnar')

function decodeChanges(binaryChanges) {
  let decoded = []
  for (let binaryChange of binaryChanges) {
    if (!(binaryChange instanceof Uint8Array)) {
      throw new RangeError(`Unexpected type of change: ${binaryChange}`)
    }
    for (let change of decodeChange(binaryChange)) decoded.push(change)
  }
  return decoded
}


let init = () => {
  return { state: Backend.State.new(), clock: {}, frozen: false };
}

let clean = (backend) => {
  if (backend.frozen) {
    //throw new Error('do not fork')
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
  return mutate(backend, (b) => b.applyLocalChange(request));
}

let getClock = (backend) => {
  return backend.clock;
}

let getUndoStack = (backend) => {
  return clean(backend).getUndoStack();
}

let getRedoStack = (backend) => {
  return clean(backend).getRedoStack();
}

let getPatch = (backend) => clean(backend).getPatch()
let getChanges = (backend,clock) => clean(backend).getChanges(clock).map(encodeChange)
let getChangesForActor = (backend,actor) => clean(backend).getChangesForActor(actor).map(encodeChange)
let getMissingDeps = (backend) => clean(backend).getMissingDeps()

module.exports = {
  init, applyChanges, applyLocalChange, getPatch,
  getChanges, getChangesForActor, getMissingDeps,
  getClock, getUndoStack, getRedoStack, loadChanges
}
