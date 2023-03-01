import * as assert from "assert"
import { unstable as Automerge } from "../src"
import * as WASM from "@automerge/automerge-wasm"

describe("Automerge", () => {
  describe("marks", () => {
    it("should allow marks that can be seen in patches", () => {
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

      doc1 = Automerge.change(doc1, d => {
        Automerge.unmark(d, "x", "font-weight", 7, 9)
      })

      assert.deepStrictEqual(callbacks[1], [
        {
          action: "mark",
          path: ["x"],
          marks: [{ key: "font-weight", start: 5, end: 10, value: "bold" }],
        },
      ])

      assert.deepStrictEqual(callbacks[2], [
        {
          action: "unmark",
          path: ["x"],
          key: "font-weight",
          start: 7,
          end: 9,
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
        marks: [
          { key: "font-weight", start: 5, end: 7, value: "bold" },
          { key: "font-weight", start: 9, end: 10, value: "bold" },
        ],
      })

      assert.deepStrictEqual(Automerge.marks(doc2, "x"), [
        { key: "font-weight", value: "bold", start: 5, end: 7 },
        { key: "font-weight", value: "bold", start: 9, end: 10 },
      ])
    })
  })
})
