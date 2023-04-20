import * as assert from "assert"
import { unstable as Automerge } from "../src"
import * as WASM from "@automerge/automerge-wasm"

describe("Automerge", () => {
  describe("marks", () => {
    it("should allow marks that can be seen in patches", () => {
      let value = "bold"
      let callbacks = []
      let doc1 = Automerge.init({
        patchCallback: (patches, info) => callbacks.push(patches),
      })
      doc1 = Automerge.change(doc1, d => {
        d.x = "the quick fox jumps over the lazy dog"
      })
      doc1 = Automerge.change(doc1, d => {
        Automerge.mark(
          d,
          "x",
          { start: 5, end: 10, expand: "none" },
          "font-weight",
          value
        )
      })

      doc1 = Automerge.change(doc1, d => {
        Automerge.unmark(d, "x", { start: 7, end: 9 }, "font-weight")
      })

      assert.deepStrictEqual(callbacks[1], [
        {
          action: "mark",
          path: ["x"],
          marks: [{ name: "font-weight", start: 5, end: 10, value }],
        },
      ])

      assert.deepStrictEqual(callbacks[2], [
        {
          action: "unmark",
          path: ["x"],
          name: "font-weight",
          start: 7,
          end: 9,
        },
      ])

      callbacks = []

      let doc2 = Automerge.init({
        patchCallback: (patches, info) => callbacks.push(patches),
      })
      doc2 = Automerge.loadIncremental(doc2, Automerge.save(doc1))

      assert.deepStrictEqual(callbacks[0][2], {
        action: "mark",
        path: ["x"],
        marks: [
          { name: "font-weight", start: 5, end: 7, value },
          { name: "font-weight", start: 9, end: 10, value },
        ],
      })

      assert.deepStrictEqual(Automerge.marks(doc2, "x"), [
        { name: "font-weight", value, start: 5, end: 7 },
        { name: "font-weight", value, start: 9, end: 10 },
      ])
    })
  })
})
