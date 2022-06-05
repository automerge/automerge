import { describe, it } from 'mocha';
//@ts-ignore
import assert from 'assert'
//@ts-ignore
import init, { create, load, SyncState, Automerge, encodeChange, decodeChange, initSyncState, decodeSyncMessage, decodeSyncState, encodeSyncState, encodeSyncMessage } from '..'
import { Prop } from '..';

function patchValue(patch: any) : any {
    switch (patch.datatype) {
      case "map":
        return {}
      case "list": 
      case "text":
        return []
      default:
        return patch.value
    }
}

function applyPatch(obj: any, path: Prop[], patch: any) : any {
  let prop = path.shift();
  if (typeof prop === 'number' && Array.isArray(obj)) {
    return applyPatchToArray(obj, prop, path, patch)
  }
  if (typeof prop === 'string' && typeof obj === 'object') {
    return applyPatchToObject(obj, prop, path, patch)
  }
  return obj
}

type Obj = { [key:string]: any }

function applyPatchToObject(obj: Obj, prop: string, path: Prop[], patch: any) : any {
  if (path.length === 0) {
    switch (patch.action) {
      case "increment":
        return { ... obj, [prop]: obj[prop] + patchValue(patch) }
      case "put":
        return { ... obj, [prop]: patchValue(patch) }
      case "delete":
        let tmp = { ... obj }
        delete tmp[prop]
        return tmp
      default: 
        throw new RangeError(`Invalid patch ${patch}`)
    }
  } else {
     return { ... obj, [prop]: applyPatch(obj[prop], path, patch) }
  }
}

function applyPatchToArray(obj: Array<any>, prop: number, path: Prop[], patch: any) : any {
  if (path.length === 0) {
    switch (patch.action) {
      case "increment":
        return [ ... obj.slice(0,prop), obj[prop] + patchValue(patch), ... obj.slice(prop + 1) ]
      case "put":
        return [ ... obj.slice(0,prop), patchValue(patch), ... obj.slice(prop + 1) ]
      case "insert":
        return [ ... obj.slice(0,prop), patchValue(patch), ... obj.slice(prop) ]
      case "delete":
        return [... obj.slice(0,prop), ... obj.slice(prop + 1) ]
      default: 
        throw new RangeError(`Invalid patch ${patch}`)
    }
  } else {
     return [ ... obj.slice(0,prop), applyPatch(obj[prop], path, patch), ... obj.slice(prop + 1) ]
  }
}

function applyPatches(obj: any, patches: any) {
  for (let patch of patches) {
    console.log("obj",obj)
    console.log("patch",patch)
    obj = applyPatch(obj, patch.path, patch)
  }
  console.log("obj",obj)
  return obj
}

describe('Automerge', () => {
  describe('patches', () => {
    it.only('can apply nested patches', () => {
      const doc1 = create()
      doc1.enablePatches(true)
      doc1.put("/", "str", "value")
      doc1.put("/", "num", 0)
      doc1.delete("/", "num")
      doc1.put("/", "counter", 0, "counter")
      doc1.increment("/", "counter", 100)
      doc1.increment("/", "counter", 1)
      doc1.put("/", "bin", new Uint8Array([1,2,3]))
      doc1.put("/", "bool", true)
      let sub = doc1.putObject("/", "sub", {})
      let list = doc1.putObject("/", "list", [1,2,3,4,5,6])
      doc1.push("/list", 100, "counter");
      doc1.increment("/list", 6, 10);
      let sublist = doc1.putObject("/sub", "list", [1,2,3,4,[ 1,2,3,[4,{ five: "six" } ] ] ])
      doc1.put(sub, "str", "value")
      doc1.put("/sub", "num", 0)
      doc1.put("/sub", "bin", new Uint8Array([1,2,3]))
      doc1.put("/sub", "bool", true)
      let subsub = doc1.putObject("/sub", "sub", {})
      doc1.put("/sub/sub", "num", 0)
      doc1.put("/sub/sub", "bin", new Uint8Array([1,2,3]))
      doc1.put("/sub/sub", "bool", true)
      let patches = doc1.popPatches()
      let js = applyPatches({}, patches)
      assert.deepEqual(js,doc1.materialize("/"))
    })
    it.only('can handle deletes with nested patches', () => {
      const doc1 = create()
      doc1.enablePatches(true)
      let list = doc1.putObject("/", "list", [1,2,3,['a','b','c']])
      //doc1.delete("/list", 1);
      doc1.push("/list", 'hello');
      let patches = doc1.popPatches()
      let js = applyPatches({}, patches)
      assert.deepEqual(js,doc1.materialize("/"))
    })
  })
})
