import * as assert from "assert"
import { next as Automerge } from "../src"
import * as WASM from "@automerge/automerge-wasm"

describe("Automerge", () => {
  describe("marks", () => {
    it("should allow marks that can be seen in patches", () => {
      let value = "bold"
      let callbacks: Automerge.Patch[][] = []
      let doc1 = Automerge.init<{ x: string }>({
        patchCallback: (patches, info) => callbacks.push(patches),
      })
      doc1 = Automerge.change<{ x: string }>(doc1, d => {
        d.x = "the quick fox jumps over the lazy dog"
      })
      doc1 = Automerge.change(doc1, d => {
        Automerge.mark(
          d,
          ["x"],
          { start: 5, end: 10, expand: "none" },
          "font-weight",
          value,
        )
      })

      doc1 = Automerge.change(doc1, d => {
        Automerge.unmark(d, ["x"], { start: 7, end: 9 }, "font-weight")
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
          action: "mark",
          path: ["x"],
          marks: [
            {
              name: "font-weight",
              start: 7,
              end: 9,
              value: null,
            },
          ],
        },
      ])

      callbacks = []

      let doc2 = Automerge.init({
        patchCallback: (patches, info) => callbacks.push(patches),
      })
      doc2 = Automerge.loadIncremental(doc2, Automerge.save(doc1))

      assert.deepStrictEqual(callbacks[0][2], {
        action: "splice",
        path: ["x", 5],
        value: "ui",
        marks: { "font-weight": "bold" },
      })

      assert.deepStrictEqual(Automerge.marks(doc2, ["x"]), [
        { name: "font-weight", value, start: 5, end: 7 },
        { name: "font-weight", value, start: 9, end: 10 },
      ])
    })
  })

  it("should do unicode sensibly", () => {
    let doc = Automerge.from({ content: "😀😀" })

    doc = Automerge.change(doc, d => {
      Automerge.mark(
        d,
        ["content"],
        { start: 2, end: 4, expand: "none" },
        "bold",
        true,
      )
      Automerge.splice(d, ["content"], 0, 0, "🙃")
    })
    assert.deepStrictEqual(Automerge.marks(doc, ["content"]), [
      {
        name: "bold",
        value: true,
        start: 4,
        end: 6,
      },
    ])
    doc = Automerge.change(doc, d => {
      Automerge.unmark(
        d,
        ["content"],
        { start: 4, end: 6, expand: "none" },
        "bold",
      )
    })
    assert.deepStrictEqual(Automerge.marks(doc, ["content"]), [])
  })
})
