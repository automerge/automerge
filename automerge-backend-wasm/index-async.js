let { fromJS, List } = require('immutable')

async function exports(resolve,reject) {
  let Backend = await import("./pkg")

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
    return mutate(backend, (b) => b.applyChanges(toJS(changes)));
  }

  let applyLocalChange = (backend,change) => {
    return mutate(backend, (b) => b.applyLocalChange(toJS(change)));
  }

  let merge = (backend1,backend2) => {
  //  let changes = backend2.getMissingChanges(backend1.clock)
  //  backend1.applyChanges(changes)
  //  let missing_changes = remote.get_missing_changes(self.op_set.clock.clone());
  //  self.apply_changes(missing_changes)
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
  let getChanges = (backend,other) => clean(backend).getChanges(clean(other))
  let getChangesForActor = (backend,actor) => clean(backend).getChangesForActor(actor)
  let getMissingChanges = (backend,clock) => clean(backend).getMissingChanges(clock)
  let getMissingDeps = (backend) => clean(backend).getMissingDeps()

  resolve({
    init, applyChanges, applyLocalChange, getPatch,
    getChanges, getChangesForActor, getMissingChanges, getMissingDeps, merge, getClock,
    getHistory, getUndoStack, getRedoStack
  })
}

module.exports = new Promise(exports);

