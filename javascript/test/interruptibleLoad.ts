import { default as assert } from "assert"
import * as Automerge from "../src/entrypoints/fullfat_node.js"

describe("loadInterruptible", () => {
  it("should allow loading a document in steps", () => {
    let doc = Automerge.from({ foo: "bar" })
    for (let i = 0; i < 100; i++) {
      doc = Automerge.change(doc, d => {
        d.foo = "bar" + i
      })
    }
    const saved = Automerge.save(doc)

    let state = Automerge.loadInterruptible<any>(saved)
    let result = state.step()
    let steps = 0
    while (!result.done) {
      steps++
      result = state.step()
    }
    // This assertion is a bit fragile but we know that the document
    // will be loaded in chunks, so it should take at least 2 steps (one for
    // the start, and then at least one chunk)
    assert(steps > 0)
    assert.deepEqual(result.doc, doc)
  })

  it("should handle options correctly", () => {
    let doc = Automerge.from({ foo: "bar" }, { actor: "aaaaaa" })
    const saved = Automerge.save(doc)

    let state = Automerge.loadInterruptible<any>(saved, { actor: "bbbbbb" })
    let result = state.step()
    while (!result.done) {
      result = state.step()
    }
    assert.equal(Automerge.getActorId(result.doc), "bbbbbb")
  })
})
