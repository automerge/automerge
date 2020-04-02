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


function init() {
  return { state: Backend.State.new(), frozen: false }
}

function backendState(backend) {
  if (backend.frozen) {
    throw new Error(
      'Attempting to use an outdated Automerge document that has already been updated. ' +
      'Please use the latest document state, or call Automerge.clone() if you really ' +
      'need to use this old document state.'
    )
  }
  return backend.state
}

function clone(backend) {
  const state = backend.state.forkAt(backend.state.getClock())
  return { state, frozen: false }
}

function free(backend) {
  backend.state.free()
  backend.state = null
  backend.frozen = true
}

function applyChanges(backend, changes) {
  const state = backendState(backend)
  const patch = state.applyChanges(decodeChanges(changes))
  backend.frozen = true
  return [{ state, frozen: false }, patch]
}

function applyLocalChange(backend, request) {
  const state = backendState(backend)
  const patch = state.applyLocalChange(request)
  backend.frozen = true
  return [{ state, frozen: false }, patch]
}

function loadChanges(backend, changes) {
  const state = backendState(backend)
  state.loadChanges(decodeChanges(changes))
  backend.frozen = true
  return { state, frozen: false }
}

function getPatch(backend) {
  return backendState(backend).getPatch()
}

function getChanges(backend, clock) {
  return backendState(backend).getChanges(clock).map(encodeChange)
}

function getChangesForActor(backend, actor) {
  return backendState(backend).getChangesForActor(actor).map(encodeChange)
}

function getMissingDeps(backend) {
  return backendState(backend).getMissingDeps()
}

function getUndoStack(backend) {
  return backendState(backend).getUndoStack()
}

function getRedoStack(backend) {
  return backendState(backend).getRedoStack()
}

module.exports = {
  init, clone, free, applyChanges, applyLocalChange, loadChanges, getPatch,
  getChanges, getChangesForActor, getMissingDeps, getUndoStack, getRedoStack
}
