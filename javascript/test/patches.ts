import * as assert from "assert"
import { unstable as Automerge } from "../src"

describe("patches", () => {
  it("should provide access to before and after states", () => {
    const doc = Automerge.init<{ count: number }>()
    const headsBefore = Automerge.getHeads(doc)
    let headsAfter

    const newDoc = Automerge.change(
      doc,
      {
        patchCallback: (_, patchInfo) => {
          assert.deepEqual(Automerge.getHeads(patchInfo.before), headsBefore)
          headsAfter = Automerge.getHeads(patchInfo.after) // => error: recursive use of an object detected which would lead to unsafe aliasing in rust
        },
      },
      doc => {
        doc.count = 1
      }
    )
    assert.deepEqual(headsAfter, Automerge.getHeads(newDoc))
  })
})
