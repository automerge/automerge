let Backend = require("./pkg")
let { fromJS, List, Map } = require('immutable')

function toJS(obj) {
  if (List.isList(obj)) {
    return obj.toJS()
  }
  return obj
}

let init = () => {
  return Backend.State.new();
}

let applyChanges = (backend,changes) => {
  let patch = backend.applyChanges(toJS(changes));
  return [ backend, patch ]
}

let applyLocalChange = (backend,change) => {
  let patch = backend.applyLocalChange(change);
  return [ backend, patch ]
}

let merge = (backend1,backend2) => {
  let patch = backend1.merge(backend2);
  return [ backend1, patch ]
}

let getClock = (backend) => {
  let clock = backend.getClock();
  return fromJS(clock);
}

let getHistory = (backend) => {
  // TODO: I cant fromJS here b/c transit screws it up
  let history = backend.getHistory();
  return history
}

let getPatch = (backend) => backend.getPatch()
let getChanges = (backend,other) => backend.getChanges(other)
let getChangesForActor = (backend,actor) => backend.getChangesForActor(actor)
let getMissingChanges = (backend,clock) => backend.getMissingChanges(clock)
let getMissingDeps = (backend) => backend.getMissingDeps()

module.exports = {
  init, applyChanges, applyLocalChange, getPatch,
  getChanges, getChangesForActor, getMissingChanges, getMissingDeps, merge, getClock, getHistory
}
