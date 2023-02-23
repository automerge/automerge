import * as assert from "assert"
import { unstable as Automerge } from "../src"
import * as WASM from "@automerge/automerge-wasm"

describe("Automerge", () => {
  describe("marks", () => {
    it.only("should allow marks that can be seen in patches", () => {
      let callbacks = []
      let doc1 = Automerge.init({
        patchCallback: (patches, before, after) => callbacks.push(patches),
      })
      doc1 = Automerge.change(doc1, d => {
        d.x = "the quick fox jumps over the lazy dog"
      })
      doc1 = Automerge.change(doc1, d => {
        Automerge.mark(d, "x", "font-weight", "[5..10]", "bold")
      })
      assert.deepStrictEqual(callbacks[1], [
        {
          action: "mark",
          path: ["x"],
          marks: [{ name: "font-weight", range: "5..10", value: "bold" }],
        },
      ])

      callbacks = []

      let doc2 = Automerge.init({
        patchCallback: (patches, before, after) => callbacks.push(patches),
      })
      doc2 = Automerge.loadIncremental(doc2, Automerge.save(doc1))

      assert.deepStrictEqual(callbacks[0][2], {
        action: "mark",
        path: ["x"],
        marks: [{ name: "font-weight", range: "5..10", value: "bold" }],
      })
    })
  })
})
